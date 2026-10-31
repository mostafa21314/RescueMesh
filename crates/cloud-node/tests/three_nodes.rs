//! Bullet 1: three nodes on 127.0.0.1 in one process, driven by the real client.

mod support;

use client::{synth_incident, Client};
use common::config::CampusParams;
use common::rng::Rng;
use std::sync::Arc;
use std::time::Duration;
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_hundred_requests_all_answered_and_tables_equal() {
    limit(async {
        const N: usize = 500;
        let (nodes, addrs) = start_cluster().await;
        let client = Arc::new(Client::new(client_cfg(&addrs, 2_000, 5)));

        let mut rng = Rng::new(7);
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..N {
            let incident = synth_incident(&mut rng, CampusParams::default().zones(), "it");
            let c = client.clone();
            tasks.spawn(async move { c.submit(incident, 2).await });
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let outcomes = tasks.join_all().await;

        assert_eq!(outcomes.len(), N);
        let unanswered: Vec<_> = outcomes
            .iter()
            .filter(|o| !o.answered())
            .map(|o| o.request_id)
            .collect();
        assert!(unanswered.is_empty(), "unanswered: {unanswered:?}");
        // The first request elected one server for epoch 1; with no failures it
        // served everything.
        let server = outcomes[0].served_by.unwrap();
        assert!(outcomes
            .iter()
            .all(|o| o.served_by == Some(server) && o.epoch == Some(1)));

        let all: Vec<_> = nodes.iter().collect();
        wait_converged(&all, N, Duration::from_secs(20)).await;
        let tables: Vec<_> = nodes.iter().map(|n| n.results_snapshot()).collect();
        assert_eq!(tables[0].len(), N);
        assert!(tables[0] == tables[1], "node 1 and node 2 results differ");
        assert!(tables[0] == tables[2], "node 1 and node 3 results differ");
        for o in &outcomes {
            assert_eq!(Some(&tables[1][&o.request_id].plan), o.plan.as_ref());
        }

        let s = nodes[server as usize - 1].status();
        assert_eq!(s.sent.get("Replicate").map(|c| c.0), Some(2 * N as u64));
        // Concurrent workers may pick the same responder from the same snapshot
        // (counted as local_replans), but commits are serialized: no responder is
        // ever held by two plans at once.
        let mut holds: std::collections::HashMap<u32, Vec<(u64, u64)>> = Default::default();
        for r in tables[0].values() {
            for a in &r.plan.assignments {
                if let common::Assignee::Responder { responder_id, .. } = a.assignee {
                    holds
                        .entry(responder_id)
                        .or_default()
                        .push((a.created_at_ms, a.expires_at_ms));
                }
            }
        }
        for (id, mut spans) in holds {
            spans.sort();
            for w in spans.windows(2) {
                assert!(
                    w[1].0 >= w[0].1,
                    "responder {id} double-booked: {:?} overlaps {:?}",
                    w[0],
                    w[1]
                );
            }
        }
        println!("local replans on the server: {}", s.local_replans);
        let digests: Vec<u64> = nodes.iter().map(|n| n.status().results_digest).collect();
        assert!(digests.iter().all(|d| *d == digests[0]));
        for n in &nodes {
            let s = n.status();
            assert_eq!((s.epoch, s.server), (1, Some(server)));
            assert_eq!(s.elections_decided, 1, "exactly one election (startup)");
            assert_eq!(s.replicate_conflicts, 0);
            assert_eq!(s.requests, N);
        }
        for n in nodes {
            shutdown(n).await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_is_answered_from_results_table_by_any_node() {
    limit(async {
        let (nodes, addrs) = start_cluster().await;
        let client = Client::new(client_cfg(&addrs, 2_000, 3));
        let mut rng = Rng::new(9);
        let first = client.submit(synth_incident(&mut rng, 256, "it"), 1).await;
        assert!(first.answered());
        let server = first.served_by.unwrap();

        // Wait for replication, then stop the server: a retry of the same request
        // must still be answered, by a peer, from its results table.
        let others: Vec<_> = nodes.iter().filter(|n| n.node_id() != server).collect();
        wait_replicated(&others, 1, Duration::from_secs(10)).await;
        let peer_addr = others[0].local_addr().to_string();
        let mut nodes: Vec<_> = nodes.into_iter().collect();
        let idx = nodes.iter().position(|n| n.node_id() == server).unwrap();
        shutdown(nodes.remove(idx)).await;

        let mut s = tokio::net::TcpStream::connect(&peer_addr).await.unwrap();
        let retry = common::Message::SubmitIncident {
            request_id: first.request_id,
            attempt: 2,
            client_epoch: first.epoch.unwrap(),
            incident: synth_incident(&mut rng, 256, "it"),
            complexity: 1,
        };
        common::wire::write_frame(&mut s, &retry).await.unwrap();
        let (reply, _) =
            tokio::time::timeout(Duration::from_secs(5), common::wire::read_frame(&mut s))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        match reply {
            common::Message::PlanResponse {
                request_id,
                served_by,
                plan,
                ..
            } => {
                assert_eq!(request_id, first.request_id);
                assert_eq!(
                    served_by, server,
                    "the stored response names the node that served it"
                );
                assert_eq!(Some(plan), first.plan);
            }
            other => panic!("expected PlanResponse, got {other:?}"),
        }
        for n in nodes {
            shutdown(n).await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_queue_replies_busy() {
    limit(async {
        let (nodes, addrs) = start_cluster_with(|c| {
            c.workers = 1;
            c.queue_capacity = 2;
        })
        .await;
        // Elect a server first.
        let client = Client::new(client_cfg(&addrs, 5_000, 1));
        let mut rng = Rng::new(3);
        let warm = client.submit(synth_incident(&mut rng, 256, "it"), 1).await;
        let server = warm.served_by.expect("warm-up answered");

        let mut s = tokio::net::TcpStream::connect(&addrs[server as usize - 1])
            .await
            .unwrap();
        let client_id = uuid::Uuid::new_v4();
        for seq in 0..10 {
            let msg = common::Message::SubmitIncident {
                request_id: common::RequestId { client_id, seq },
                attempt: 1,
                client_epoch: warm.epoch.unwrap(),
                incident: synth_incident(&mut rng, 256, "it"),
                complexity: 200,
            };
            common::wire::write_frame(&mut s, &msg).await.unwrap();
        }
        // One running + two queued fit; the rest are refused.
        let (mut busy, mut plans) = (0, 0);
        while busy + plans < 10 {
            let (m, _) =
                tokio::time::timeout(Duration::from_secs(60), common::wire::read_frame(&mut s))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            match m {
                common::Message::Busy { .. } => busy += 1,
                common::Message::PlanResponse { .. } => plans += 1,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert!(busy >= 7, "busy {busy}, plans {plans}");
        assert!(plans >= 1);
        for n in nodes {
            shutdown(n).await;
        }
    })
    .await
}
