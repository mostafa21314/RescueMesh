//! Bullet 5: a node process that is stopped and started again (cold restart,
//! empty memory) must rebuild its state from its peers before it bids or
//! serves, and converge with them.

mod support;

use client::{synth_incident, Client};
use cloud_node::{start_with_listener, NodeHandle};
use common::config::{PeerConfig, ReadyRule};
use common::rng::Rng;
use common::rr::Listener;
use common::{unix_ms, NodePhase};
use std::time::{Duration, Instant};
use support::*;

/// Starts node `id` again on its old address with a fresh process state.
async fn restart(id: u32, addrs: &[String]) -> NodeHandle {
    restart_with(id, addrs, ReadyRule::default()).await
}

async fn restart_with(id: u32, addrs: &[String], rule: ReadyRule) -> NodeHandle {
    let addr = &addrs[id as usize - 1];
    let peers = (1..=3u32)
        .filter(|j| *j != id)
        .map(|j| PeerConfig {
            id: j,
            addr: addrs[j as usize - 1].clone(),
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(5);
    let listener = loop {
        match Listener::bind(addr).await {
            Ok(l) => break l,
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("rebind {addr}: {e}"),
        }
    };
    let mut cfg = node_cfg(id, addr, peers);
    cfg.recovery.ready_rule = rule;
    start_with_listener(cfg, listener).await.unwrap()
}

async fn ready(node: &NodeHandle, within: Duration) {
    let deadline = Instant::now() + within;
    while node.status().phase != NodePhase::Ready {
        assert!(
            Instant::now() < deadline,
            "node did not recover: {:?}",
            node.status()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn submit_all(client: &Client, rng: &mut Rng, n: usize) {
    for _ in 0..n {
        let o = client.submit(synth_incident(rng, 256, "it"), 2).await;
        assert!(o.answered(), "{:?} unanswered", o.request_id);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_server_rebuilds_state_from_peers_before_participating() {
    limit(async {
        let (mut nodes, addrs) = start_cluster().await;
        assert!(
            nodes.iter().all(|n| n.status().phase == NodePhase::Ready),
            "a fresh cluster starts ready"
        );
        let client = Client::new(client_cfg(&addrs, 1_000, 6));
        let mut rng = Rng::new(21);
        submit_all(&client, &mut rng, 40).await;
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            40,
            Duration::from_secs(10),
        )
        .await;

        // Stop the server; the survivors elect a new one and keep serving.
        let server = nodes[0].status().server.expect("a server was elected");
        let old = nodes.remove(server as usize - 1);
        shutdown(old).await;
        submit_all(&client, &mut rng, 20).await;
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            60,
            Duration::from_secs(10),
        )
        .await;
        let view = nodes[0].status();

        // The restarted process has empty memory. It must not answer or bid
        // from that: either it is still recovering, or it already holds
        // everything the survivors hold.
        let back = restart(server, &addrs).await;
        let first = back.status();
        assert!(
            first.phase == NodePhase::Recovering
                || (first.phase == NodePhase::Ready && first.results == 60),
            "{first:?}"
        );
        ready(&back, Duration::from_secs(10)).await;
        let s = back.status();
        assert!(s.last_recovery_ms.is_some());
        assert_eq!((s.epoch, s.server), (view.epoch, view.server));
        assert_eq!(s.elections_decided, 0, "it never bid while recovering");
        assert_eq!(back.results_snapshot(), nodes[0].results_snapshot());
        let now = unix_ms();
        assert_eq!(back.ledger_snapshot(now), nodes[0].ledger_snapshot(now));

        // It participates normally afterwards, and stays converged.
        nodes.insert(server as usize - 1, back);
        submit_all(&client, &mut rng, 20).await;
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            80,
            Duration::from_secs(10),
        )
        .await;
        for n in nodes {
            shutdown(n).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_restarted_nodes_wait_for_a_ready_peer_and_skip_each_other() {
    // both_agree: as first written. The default, majority, resumes from the
    // one ready survivor instead (docs/recovery.md, R4):
    // `majority_rule_restarts_from_the_one_ready_peer` below.
    limit(async {
        let rule = ReadyRule::BothAgree;
        let (mut nodes, addrs) = start_cluster_with(|cfg| cfg.recovery.ready_rule = rule).await;
        let client = Client::new(client_cfg(&addrs, 1_000, 6));
        let mut rng = Rng::new(22);
        submit_all(&client, &mut rng, 30).await;
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            30,
            Duration::from_secs(10),
        )
        .await;

        // Stop nodes 1 and 3; node 2 keeps the state.
        let third = nodes.remove(2);
        let first = nodes.remove(0);
        shutdown(third).await;
        shutdown(first).await;
        let survivor = &nodes[0];

        // Node 1 comes back while node 3 is unreachable: one failure at a
        // time is the model, so it waits rather than trust node 2 alone.
        let one = restart_with(1, &addrs, rule).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(one.status().phase, NodePhase::Recovering);
        // Incremental pages can be installed safely before the role view is
        // accepted; the node must still neither elect nor serve.
        assert_eq!(one.status().elections_decided, 0);
        assert_eq!(one.status().epoch, 0);

        // Node 3 comes back: node 1 answers "not ready" (it is recovering
        // itself), so node 3 recovers from node 2; then node 1 can too.
        let three = restart_with(3, &addrs, rule).await;
        ready(&three, Duration::from_secs(10)).await;
        ready(&one, Duration::from_secs(10)).await;
        let want = survivor.results_snapshot();
        assert_eq!(want.len(), 30);
        assert_eq!(one.results_snapshot(), want);
        assert_eq!(three.results_snapshot(), want);
        let s = survivor.status();
        for n in [&one, &three] {
            assert_eq!((n.status().epoch, n.status().server), (s.epoch, s.server));
        }
        for n in [one, three] {
            shutdown(n).await;
        }
        shutdown(nodes.remove(0)).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn majority_rule_restarts_from_the_one_ready_peer() {
    limit(async {
        let (mut nodes, addrs) = start_cluster().await;
        let client = Client::new(client_cfg(&addrs, 1_000, 6));
        let mut rng = Rng::new(23);
        submit_all(&client, &mut rng, 30).await;
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            30,
            Duration::from_secs(10),
        )
        .await;

        // Stop nodes 1 and 3; node 2 keeps the state.
        let third = nodes.remove(2);
        let first = nodes.remove(0);
        shutdown(third).await;
        shutdown(first).await;
        let survivor = &nodes[0];

        // Node 1 comes back while node 3 is still down: itself + node 2 is
        // 2 of 3, so it recovers from node 2 without waiting for node 3.
        let one = restart_with(1, &addrs, ReadyRule::Majority).await;
        ready(&one, Duration::from_secs(5)).await;
        assert_eq!(one.results_snapshot(), survivor.results_snapshot());
        let r = one.status().last_recovery.unwrap();
        assert_eq!((r.cause.as_str(), r.snapshots_used), ("start", 1));
        let s = survivor.status();
        assert_eq!(
            (one.status().epoch, one.status().server),
            (s.epoch, s.server)
        );
        shutdown(one).await;
        shutdown(nodes.remove(0)).await;
    })
    .await;
}
