mod support;

use client::{inject_failure, query_status, synth_incident, Client};
use common::{config::CampusParams, rng::Rng, NodePhase};
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::*;

async fn ready(node: &cloud_node::NodeHandle, within: Duration) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_second_server_outage_keeps_clients_running_and_recovers_before_rejoin() {
    limit(async {
        let (nodes, addrs) = start_cluster_with(|cfg| {
            cfg.campus = CampusParams { grid: 6, responders: 40, resources: 25 };
            cfg.min_tenure_ms = 500;
        }).await;
        let client = Arc::new(Client::new(client_cfg(&addrs, 500, 12)));
        let mut rng = Rng::new(99);
        let mut warm_incident = synth_incident(&mut rng, 36, "warm");
        warm_incident.ttl_ms = 60_000;
        let warm = client.submit(warm_incident, 1).await;
        let server = warm.served_by.expect("warmup answered");
        let victim = &nodes[server as usize - 1];
        wait_converged(&nodes.iter().collect::<Vec<_>>(), 1, Duration::from_secs(5)).await;

        // Start expensive work so injection occurs with jobs already running.
        let mut in_flight = tokio::task::JoinSet::new();
        for _ in 0..6 {
            let c = client.clone();
            let mut incident = synth_incident(&mut rng, 36, "inflight");
            incident.ttl_ms = 60_000;
            in_flight.spawn(async move { c.submit(incident, 1000).await });
        }
        let until = Instant::now() + Duration::from_secs(3);
        while victim.status().running == 0 {
            assert!(Instant::now() < until, "no in-flight work reached server");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let failed_at = Instant::now();
        assert_eq!(inject_failure(&addrs[server as usize - 1], 20_000, Duration::from_secs(1)).await.unwrap(), server);
        assert_eq!(victim.status().phase, NodePhase::Muted);
        assert!(query_status(&addrs[server as usize - 1], Duration::from_millis(100)).await.is_err());

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..120 {
            let c = client.clone();
            let mut incident = synth_incident(&mut rng, 36, "continuous");
            incident.ttl_ms = 60_000;
            let phase_at_send = victim.status().phase;
            tasks.spawn(async move {
                let result = c.submit(incident, 2).await;
                (phase_at_send, Instant::now(), result)
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let outcomes = tasks.join_all().await;
        assert!(outcomes.iter().all(|(_, _, o)| o.answered()), "every workload request must be answered");
        let during: Vec<_> = outcomes.iter().filter(|(_, completed, _)| completed.duration_since(failed_at) < Duration::from_secs(20)).collect();
        assert!(during.len() >= 50, "survivors must serve throughout the mute interval");
        assert!(during.iter().all(|(_, _, o)| o.served_by != Some(server)));
        assert!(during.iter().all(|(_, _, o)| !o.degraded), "two survivors can elect normally");
        let first_failover = during.iter().map(|(_, time, _)| time.duration_since(failed_at)).min().unwrap();
        let inflight = in_flight.join_all().await;
        assert!(inflight.iter().all(|o| o.answered()), "requests interrupted by failure must retry transparently");
        assert!(inflight.iter().any(|o| o.attempts >= 2));
        ready(victim, Duration::from_secs(10)).await;
        let all: Vec<_> = nodes.iter().collect();
        wait_converged(&all, 127, Duration::from_secs(10)).await;
        let now_ms = common::unix_ms();
        let snapshots: Vec<_> = nodes.iter().map(|n| n.ledger_snapshot(now_ms)).collect();
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[0], snapshots[2]);
        let status = victim.status();
        assert_eq!(status.faults_injected, 1);
        assert!(status.last_recovery_ms.is_some());
        assert!(nodes.iter().all(|n| n.status().server == status.server && n.status().epoch == status.epoch));
        println!("20 s outage: {}/120 requests completed during outage; first survivor response {:?}; 127/127 answered; recovery {} ms; result tables and ledgers equal", during.len(), first_failover, status.last_recovery_ms.unwrap());
        for node in nodes { shutdown(node).await; }
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_node_waits_for_both_survivors_and_failure_controls_are_bounded() {
    limit(async {
        // The both_agree rule specifically (the default, majority, would
        // resume with one survivor: recovery_variants.rs).
        let (mut nodes, addrs) = start_cluster_with(|cfg| {
            cfg.recovery.ready_rule = common::config::ReadyRule::BothAgree
        })
        .await;
        let client = Client::new(client_cfg(&addrs, 1000, 6));
        let mut rng = Rng::new(4);
        assert!(client
            .submit(synth_incident(&mut rng, 256, "warm"), 1)
            .await
            .answered());
        wait_converged(&nodes.iter().collect::<Vec<_>>(), 1, Duration::from_secs(5)).await;
        assert!(nodes[0].inject_failure(Duration::ZERO).is_err());
        assert!(nodes[0].inject_failure(Duration::from_secs(21)).is_err());
        nodes[0].inject_failure(Duration::from_millis(100)).unwrap();
        assert!(nodes[0].inject_failure(Duration::from_millis(100)).is_err());
        shutdown(nodes.remove(2)).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(nodes[0].status().phase, NodePhase::Recovering);
        assert!(nodes[0].inject_failure(Duration::from_millis(100)).is_err());
        let recovery = nodes[0].status();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            nodes[0].status().elections_decided,
            recovery.elections_decided
        );
        assert_eq!(nodes[0].status().results, recovery.results);
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_follower_outages_recover_without_replacing_a_healthy_server() {
    limit(async {
        let (nodes, addrs) = start_cluster().await;
        let client = Client::new(client_cfg(&addrs, 1000, 6));
        let mut rng = Rng::new(7);
        let first = client
            .submit(synth_incident(&mut rng, 256, "warm"), 1)
            .await;
        let server = first.served_by.unwrap();
        wait_converged(&nodes.iter().collect::<Vec<_>>(), 1, Duration::from_secs(5)).await;
        let victim = nodes.iter().find(|n| n.node_id() != server).unwrap();
        for round in 1..=2 {
            victim.inject_failure(Duration::from_millis(200)).unwrap();
            assert!(client
                .submit(synth_incident(&mut rng, 256, "live"), 1)
                .await
                .answered());
            ready(victim, Duration::from_secs(5)).await;
            wait_converged(
                &nodes.iter().collect::<Vec<_>>(),
                1 + round,
                Duration::from_secs(5),
            )
            .await;
            assert_eq!(victim.status().server, Some(server));
            assert_eq!(victim.status().faults_injected, round as u64);
        }
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}
