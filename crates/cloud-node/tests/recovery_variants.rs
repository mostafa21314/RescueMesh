//! The recovery choices in `[recovery]` (docs/recovery.md): every variant must
//! end with the same state as the baseline; they differ in cost and in when
//! the returning node may resume.

mod support;

use client::{synth_incident, Client};
use cloud_node::{start_with_listener, NodeHandle};
use common::config::*;
use common::rr::Listener;
use common::{rng::Rng, NodePhase};
use std::time::{Duration, Instant};
use support::*;

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

fn assert_same_ledgers(nodes: &[&NodeHandle]) {
    let now_ms = common::unix_ms();
    let snaps: Vec<_> = nodes.iter().map(|n| n.ledger_snapshot(now_ms)).collect();
    assert!(snaps.windows(2).all(|w| w[0] == w[1]), "ledgers differ");
}

async fn submit_n(client: &Client, rng: &mut Rng, n: usize, tag: &str) {
    for _ in 0..n {
        assert!(client
            .submit(synth_incident(rng, 256, tag), 1)
            .await
            .answered());
    }
}

/// A node restarted on its old address, with the same config.
async fn restart(cfg: NodeConfig) -> NodeHandle {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Listener::bind(&cfg.listen).await {
            Ok(l) => return start_with_listener(cfg, l).await.unwrap(),
            Err(e) => {
                assert!(Instant::now() < deadline, "rebind {}: {e}", cfg.listen);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

fn cfg_of(addrs: &[String], i: usize, tweak: impl Fn(&mut NodeConfig)) -> NodeConfig {
    let peers = (0..3)
        .filter(|j| *j != i)
        .map(|j| PeerConfig {
            id: j as u32 + 1,
            addr: addrs[j].clone(),
        })
        .collect();
    let mut cfg = node_cfg(i as u32 + 1, &addrs[i], peers);
    tweak(&mut cfg);
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_transfer_and_chunked_snapshots_reach_the_same_state_with_less_data() {
    limit(async {
        let tweak = |cfg: &mut NodeConfig| {
            cfg.recovery.transfer = Transfer::Delta;
            cfg.recovery.snapshot = SnapshotBuild::Chunked;
            cfg.recovery.delta_margin_ms = 200;
        };
        let (nodes, addrs) = start_cluster_with(tweak).await;
        let client = Client::new(client_cfg(&addrs, 500, 8));
        let mut rng = Rng::new(11);
        submit_n(&client, &mut rng, 1, "warm").await;
        let server = client
            .submit(synth_incident(&mut rng, 256, "warm"), 1)
            .await
            .served_by
            .unwrap();
        // History that predates the outage by more than the margin.
        submit_n(&client, &mut rng, 40, "old").await;
        let all: Vec<_> = nodes.iter().collect();
        wait_converged(&all, 42, Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(400)).await;

        // The server goes silent; the survivors take over and serve 15 more.
        let victim = &nodes[server as usize - 1];
        let served_before: u64 = nodes.iter().map(|n| n.status().snapshots_served).sum();
        victim.inject_failure(Duration::from_millis(1500)).unwrap();
        submit_n(&client, &mut rng, 15, "during").await;
        ready(victim, Duration::from_secs(10)).await;
        wait_converged(&all, 57, Duration::from_secs(10)).await;
        assert_same_ledgers(&all);

        let report = victim.status().last_recovery.expect("recovery report");
        assert_eq!(report.cause, "mute");
        assert_eq!(report.snapshots_used, 2);
        assert!(report.new_results >= 15, "{report:?}");
        // Each survivor sent only what changed since the outage began.
        assert!(
            report.records_received < 2 * 57 / 2,
            "delta sent most of the table: {report:?}"
        );
        let served: u64 = nodes.iter().map(|n| n.status().snapshots_served).sum();
        assert_eq!(served - served_before, 2);
        let s = victim.status();
        assert!(nodes.iter().all(|n| n.status().epoch == s.epoch));
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn majority_rule_recovers_with_one_survivor_down_both_agree_waits() {
    limit(async {
        for rule in [ReadyRule::Majority, ReadyRule::BothAgree] {
            let (mut nodes, addrs) = start_cluster_with(|cfg| cfg.recovery.ready_rule = rule).await;
            let client = Client::new(client_cfg(&addrs, 1000, 6));
            let mut rng = Rng::new(5);
            submit_n(&client, &mut rng, 5, "warm").await;
            wait_converged(&nodes.iter().collect::<Vec<_>>(), 5, Duration::from_secs(5)).await;
            nodes[0].inject_failure(Duration::from_millis(100)).unwrap();
            shutdown(nodes.remove(2)).await;
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let s = nodes[0].status();
            match rule {
                ReadyRule::Majority => {
                    assert_eq!(s.phase, NodePhase::Ready);
                    let r = s.last_recovery.unwrap();
                    assert_eq!(r.snapshots_used, 1);
                    assert_eq!(s.results, 5);
                }
                ReadyRule::BothAgree => assert_eq!(s.phase, NodePhase::Recovering),
            }
            for node in nodes {
                shutdown(node).await;
            }
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_process_recovers_from_peers_before_serving() {
    limit(async {
        let on_start = |cfg: &mut NodeConfig| cfg.recovery.on_start = true;
        // The whole cluster starts together: the startup probe finds no peer
        // with history, so every node starts Ready without recovering.
        let (mut nodes, addrs) = start_cluster_with(on_start).await;
        for n in &nodes {
            ready(n, Duration::from_secs(5)).await;
            assert!(n.status().last_recovery.is_none());
        }
        let client = Client::new(client_cfg(&addrs, 500, 8));
        let mut rng = Rng::new(21);
        let server = client
            .submit(synth_incident(&mut rng, 256, "warm"), 1)
            .await
            .served_by
            .unwrap();
        submit_n(&client, &mut rng, 19, "before").await;
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            20,
            Duration::from_secs(5),
        )
        .await;
        let epoch = nodes[0].status().epoch;

        // Crash a follower (its memory is lost) and start it again.
        let i = nodes.iter().position(|n| n.node_id() != server).unwrap();
        shutdown(nodes.remove(i)).await;
        submit_n(&client, &mut rng, 5, "while-down").await;
        let back = restart(cfg_of(&addrs, i, on_start)).await;
        ready(&back, Duration::from_secs(5)).await;
        let r = back.status().last_recovery.unwrap();
        assert_eq!(r.cause, "start");
        // Live replicas may arrive while the snapshots are being fetched.
        // new_results counts snapshot inserts only; readiness must expose
        // all results regardless of which path installed them.
        assert_eq!(back.status().results, 25);
        assert!(r.new_results <= 25);
        assert_same_ledgers(&[&nodes[0], &nodes[1], &back]);
        // It adopted the current role view instead of forcing an election.
        assert_eq!(back.status().epoch, epoch);
        assert_eq!(back.status().server, Some(server));
        nodes.insert(i, back);
        submit_n(&client, &mut rng, 5, "after").await;
        let all: Vec<_> = nodes.iter().collect();
        wait_converged(&all, 30, Duration::from_secs(5)).await;
        assert_same_ledgers(&all);
        assert!(nodes.iter().all(|n| n.status().epoch == epoch));
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}

/// The baseline, for comparison: without `on_start`, a restarted process is
/// Ready at once with nothing in its tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_on_start_a_restarted_process_serves_empty() {
    limit(async {
        let no_start = |cfg: &mut NodeConfig| cfg.recovery.on_start = false;
        let (mut nodes, addrs) = start_cluster_with(no_start).await;
        let client = Client::new(client_cfg(&addrs, 500, 8));
        let mut rng = Rng::new(3);
        submit_n(&client, &mut rng, 10, "before").await;
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            10,
            Duration::from_secs(5),
        )
        .await;
        shutdown(nodes.remove(2)).await;
        let back = restart(cfg_of(&addrs, 2, no_start)).await;
        let s = back.status();
        assert_eq!(s.phase, NodePhase::Ready);
        assert_eq!((s.results, s.epoch, s.server), (0, 0, None));
        nodes.push(back);
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}
