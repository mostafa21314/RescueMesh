//! Bullet 2, in process: failover by retry-triggered election, and the
//! per-request election mode.

mod support;

use client::{synth_incident, Client};
use common::config::{CampusParams, ElectionMode};
use common::rng::Rng;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_the_server_mid_workload_elects_another_and_every_request_is_answered() {
    limit(async {
        const N: usize = 300;
        let (nodes, addrs) = start_cluster().await;
        let client = Arc::new(Client::new(client_cfg(&addrs, 1_000, 6)));
        let mut slots: Vec<Option<_>> = nodes.into_iter().map(Some).collect();

        let sender = {
            let client = client.clone();
            tokio::spawn(async move {
                let mut rng = Rng::new(11);
                let mut tasks = tokio::task::JoinSet::new();
                for _ in 0..N {
                    let incident = synth_incident(&mut rng, CampusParams::default().zones(), "it");
                    let c = client.clone();
                    tasks.spawn(async move { c.submit(incident, 2).await });
                    tokio::time::sleep(Duration::from_millis(3)).await;
                }
                tasks.join_all().await
            })
        };

        // Let the first ~100 requests go through, then stop whoever serves.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let first_server = slots[0]
            .as_ref()
            .unwrap()
            .status()
            .server
            .expect("a server was elected");
        let first_epoch = slots[0].as_ref().unwrap().status().epoch;
        let victim = slots[first_server as usize - 1].take().unwrap();
        shutdown(victim).await;
        let killed_at = std::time::Instant::now();

        let outcomes = sender.await.unwrap();
        let after_kill = killed_at.elapsed();
        let unanswered: Vec<(common::RequestId, u32)> = outcomes
            .iter()
            .filter(|o| !o.answered())
            .map(|o| (o.request_id, o.attempts))
            .collect();
        if !unanswered.is_empty() {
            for n in slots.iter().flatten() {
                for (r, _) in &unanswered {
                    println!("{r}: {}", n.describe_request(r));
                }
                println!("{:?}", n.status());
            }
            panic!("unanswered: {unanswered:?}");
        }

        let after: Vec<_> = outcomes
            .iter()
            .filter(|o| o.epoch > Some(first_epoch))
            .collect();
        assert!(
            !after.is_empty(),
            "some requests were served after a new election"
        );
        assert!(
            after.iter().all(|o| o.served_by != Some(first_server)),
            "the stopped server can't serve"
        );
        assert!(
            outcomes.iter().any(|o| o.attempts >= 2),
            "the stop forced retries"
        );

        // The survivors agree on the final epoch and server (allow a moment for
        // an election that was still deciding when the last request returned).
        let survivors: Vec<_> = slots.iter().flatten().collect();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let st = loop {
            let st: Vec<_> = survivors.iter().map(|n| n.status()).collect();
            if (st[0].epoch, st[0].server) == (st[1].epoch, st[1].server)
                || std::time::Instant::now() > deadline
            {
                break st;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!((st[0].epoch, st[0].server), (st[1].epoch, st[1].server));
        assert!(st[0].epoch > first_epoch);
        assert!(st[0].server.is_some_and(|s| s != first_server));

        // min_tenure (D34): a retry can move the role only after the server has
        // held it for min_tenure (1 s here), so after the crash there is at most
        // one election per min_tenure, plus the one that detects the crash.
        // (Here retries come from 1 s timeouts, which already satisfy the
        // tenure, so few or none are suppressed; Busy-driven retries are the
        // case the rule suppresses, covered by the unit test.)
        let suppressed: u64 = st.iter().map(|s| s.suppressed_triggers).sum();
        let elections_after_crash = st[0].epoch - first_epoch;
        let allowed = 1 + after_kill.as_millis() as u64 / 1_000;
        assert!(
            elections_after_crash <= allowed,
            "{elections_after_crash} elections in {after_kill:?} after the crash (allowed {allowed})"
        );
        println!(
            "first server {first_server} (epoch {first_epoch}) → server {:?} (epoch {}) in {after_kill:?}; \
             retried requests: {}; suppressed triggers: {suppressed}",
            st[0].server,
            st[0].epoch,
            outcomes.iter().filter(|o| o.attempts >= 2).count()
        );
        for n in slots.into_iter().flatten() {
            shutdown(n).await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_request_mode_spreads_work_and_tables_converge() {
    limit(async {
        const N: usize = 300;
        let (nodes, addrs) =
            start_cluster_with(|c| c.election_mode = ElectionMode::PerRequest).await;
        let client = Arc::new(Client::new(client_cfg(&addrs, 2_000, 5)));
        let mut rng = Rng::new(5);
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..N {
            let incident = synth_incident(&mut rng, CampusParams::default().zones(), "it");
            let c = client.clone();
            tasks.spawn(async move { c.submit(incident, 5).await });
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let outcomes = tasks.join_all().await;
        assert!(outcomes.iter().all(|o| o.answered()));
        let servers: BTreeSet<_> = outcomes.iter().filter_map(|o| o.served_by).collect();
        assert!(
            servers.len() >= 2,
            "a burst should not all go to one node: {servers:?}"
        );
        assert!(
            outcomes.iter().all(|o| o.epoch == Some(0)),
            "per-request mode has no epochs"
        );

        let all: Vec<_> = nodes.iter().collect();
        wait_converged(&all, N, Duration::from_secs(20)).await;
        // One election per request attempt at every node. Under heavy load a node
        // can time out with only its own bid (counted as alone); the client's
        // retry then runs a new election.
        for n in &nodes {
            let s = n.status();
            assert!(
                s.elections_decided + s.elections_alone >= N as u64,
                "one election per request: decided {} alone {}",
                s.elections_decided,
                s.elections_alone
            );
        }
        let per_node: Vec<usize> = (1..=3)
            .map(|id| outcomes.iter().filter(|o| o.served_by == Some(id)).count())
            .collect();
        println!("per-request mode, served per node: {per_node:?}");
        for n in nodes {
            shutdown(n).await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_node_serves_degraded_only_after_three_seconds() {
    limit(async {
        // Two addresses nobody listens on, for the absent peers.
        let dead: Vec<String> = (0..2)
            .map(|_| {
                std::net::UdpSocket::bind("127.0.0.1:0")
                    .unwrap()
                    .local_addr()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let listener = common::rr::Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peers = vec![
            common::config::PeerConfig {
                id: 2,
                addr: dead[0].clone(),
            },
            common::config::PeerConfig {
                id: 3,
                addr: dead[1].clone(),
            },
        ];
        init_logs();
        let node = cloud_node::start_with_listener(node_cfg(1, &addr, peers), listener)
            .await
            .unwrap();

        let client = Client::new(client_cfg(&[addr], 1_000, 6));
        let mut rng = Rng::new(2);
        let started = std::time::Instant::now();
        let o = client.submit(synth_incident(&mut rng, 256, "it"), 1).await;
        let took = started.elapsed();

        assert!(o.answered(), "served alone eventually");
        assert!(o.degraded, "the response is marked degraded");
        assert_eq!(
            (o.served_by, o.epoch),
            (Some(1), Some(0)),
            "the epoch does not change while degraded"
        );
        assert!(
            took >= Duration::from_millis(3_000),
            "not before 3 s alone: {took:?}"
        );
        assert!(
            o.attempts >= 4,
            "three retries elapse first: {}",
            o.attempts
        );
        let s = node.status();
        assert!(s.degraded && s.elections_alone >= 4 && s.elections_decided == 0);

        // While degraded, later requests are served at once.
        let o2 = client.submit(synth_incident(&mut rng, 256, "it"), 1).await;
        assert!(o2.degraded && o2.attempts == 1);
        shutdown(node).await;
    })
    .await
}
