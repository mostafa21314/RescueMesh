//! Three nodes on 127.0.0.1 in one process, driven by the real client.

use client::{synth_incident, Client};
use cloud_node::{start_with_listener, NodeHandle};
use common::config::*;
use common::rng::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

fn node_cfg(node_id: u32, listen: &str, peers: Vec<PeerConfig>) -> NodeConfig {
    NodeConfig {
        node_id,
        listen: listen.to_string(),
        peers,
        election_mode: ElectionMode::OnDemand,
        policy: Policy::Composite,
        weights: Weights::default(),
        bid_timeout_ms: 100,
        degraded_after_ms: 3000,
        workers: 4,
        seed: 42,
        queue_capacity: 1024,
        table_ttl_ms: 300_000,
        campus: CampusParams::default(),
    }
}

async fn start_cluster() -> (Vec<NodeHandle>, Vec<String>) {
    start_cluster_with(|_| {}).await
}

async fn start_cluster_with(tweak: impl Fn(&mut NodeConfig)) -> (Vec<NodeHandle>, Vec<String>) {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addrs: Vec<String> = listeners.iter().map(|l| l.local_addr().unwrap().to_string()).collect();
    let mut nodes = Vec::new();
    for (i, l) in listeners.into_iter().enumerate() {
        let id = i as u32 + 1;
        let peers = (0..3)
            .filter(|j| *j != i)
            .map(|j| PeerConfig { id: j as u32 + 1, addr: addrs[j].clone() })
            .collect();
        let mut cfg = node_cfg(id, &addrs[i], peers);
        tweak(&mut cfg);
        nodes.push(start_with_listener(cfg, l).await.unwrap());
    }
    (nodes, addrs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_hundred_requests_all_answered_and_tables_equal() {
    const N: usize = 500;
    let (nodes, addrs) = start_cluster().await;
    let client = Arc::new(Client::new(ClientConfig {
        nodes: addrs,
        request_timeout_ms: 2_000,
        max_attempts: 5,
        campus: CampusParams::default(),
    }));

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
    let unanswered: Vec<_> = outcomes.iter().filter(|o| !o.answered()).map(|o| o.request_id).collect();
    assert!(unanswered.is_empty(), "unanswered: {unanswered:?}");
    // Placeholder until Part B: node 1 serves everything, in epoch 0.
    assert!(outcomes.iter().all(|o| o.served_by == Some(1) && o.epoch == Some(0)));

    // Replication is asynchronous; wait until every node holds every result.
    let deadline = Instant::now() + Duration::from_secs(20);
    while nodes.iter().any(|n| n.results_snapshot().len() < N) {
        assert!(Instant::now() < deadline, "replication did not finish: {:?}",
            nodes.iter().map(|n| n.results_snapshot().len()).collect::<Vec<_>>());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let tables: Vec<_> = nodes.iter().map(|n| n.results_snapshot()).collect();
    assert_eq!(tables[0].len(), N);
    assert!(tables[0] == tables[1], "node 1 and node 2 results differ");
    assert!(tables[0] == tables[2], "node 1 and node 3 results differ");

    // Each plan the client got is the one every node stores.
    for o in &outcomes {
        assert_eq!(Some(&tables[1][&o.request_id].plan), o.plan.as_ref());
    }

    let s1 = nodes[0].status();
    assert_eq!(s1.sent.get("Replicate").map(|c| c.0), Some(2 * N as u64));
    // Concurrent workers may pick the same responder from the same snapshot
    // (that's what local_replans counts), but commits are serialized: no
    // responder is ever held by two plans at once.
    let mut holds: std::collections::HashMap<u32, Vec<(u64, u64)>> = Default::default();
    for r in tables[0].values() {
        for a in &r.plan.assignments {
            if let common::Assignee::Responder { responder_id, .. } = a.assignee {
                holds.entry(responder_id).or_default().push((a.created_at_ms, a.expires_at_ms));
            }
        }
    }
    for (id, mut spans) in holds {
        spans.sort();
        for w in spans.windows(2) {
            assert!(w[1].0 >= w[0].1, "responder {id} double-booked: {:?} overlaps {:?}", w[0], w[1]);
        }
    }
    println!("local replans on node 1: {}", s1.local_replans);
    let digests: Vec<u64> = nodes.iter().map(|n| n.status().results_digest).collect();
    assert!(digests.iter().all(|d| *d == digests[0]));
    for n in &nodes {
        let s = n.status();
        assert_eq!(s.replicate_conflicts, 0);
        assert_eq!(s.requests, N);
    }

    for n in nodes {
        n.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_is_answered_from_results_table_by_any_node() {
    let (nodes, addrs) = start_cluster().await;
    let client = Client::new(ClientConfig {
        nodes: addrs.clone(),
        request_timeout_ms: 2_000,
        max_attempts: 3,
        campus: CampusParams::default(),
    });
    let mut rng = Rng::new(9);
    let first = client.submit(synth_incident(&mut rng, 256, "it"), 1).await;
    assert!(first.answered());

    // Wait for replication, then stop the server: a retry of the same request
    // must still be answered (by a peer, from its results table).
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes[1].results_snapshot().is_empty() || nodes[2].results_snapshot().is_empty() {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut nodes = nodes.into_iter();
    nodes.next().unwrap().shutdown();

    let mut s = tokio::net::TcpStream::connect(&addrs[1]).await.unwrap();
    let retry = common::Message::SubmitIncident {
        request_id: first.request_id,
        attempt: 2,
        client_epoch: 0,
        incident: synth_incident(&mut rng, 256, "it"),
        complexity: 1,
    };
    common::wire::write_frame(&mut s, &retry).await.unwrap();
    let (reply, _) = tokio::time::timeout(Duration::from_secs(5), common::wire::read_frame(&mut s))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match reply {
        common::Message::PlanResponse { request_id, served_by, plan, .. } => {
            assert_eq!(request_id, first.request_id);
            assert_eq!(served_by, 1, "the stored response names the node that served it");
            assert_eq!(Some(plan), first.plan);
        }
        other => panic!("expected PlanResponse, got {other:?}"),
    }
    for n in nodes {
        n.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_queue_replies_busy() {
    let (nodes, addrs) = start_cluster_with(|c| {
        c.workers = 1;
        c.queue_capacity = 2;
    })
    .await;
    let mut s = tokio::net::TcpStream::connect(&addrs[0]).await.unwrap();
    let mut rng = Rng::new(3);
    let client_id = uuid::Uuid::new_v4();
    for seq in 0..10 {
        let msg = common::Message::SubmitIncident {
            request_id: common::RequestId { client_id, seq },
            attempt: 1,
            client_epoch: 0,
            incident: synth_incident(&mut rng, 256, "it"),
            complexity: 200,
        };
        common::wire::write_frame(&mut s, &msg).await.unwrap();
    }
    // One running + two queued fit; at least the rest are refused.
    let (mut busy, mut plans) = (0, 0);
    while busy + plans < 10 {
        let (m, _) = tokio::time::timeout(Duration::from_secs(30), common::wire::read_frame(&mut s))
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
        n.shutdown();
    }
}
