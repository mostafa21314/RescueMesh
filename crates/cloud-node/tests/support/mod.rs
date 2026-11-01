//! Helpers shared by the in-process integration tests.
#![allow(dead_code)]

use cloud_node::{start_with_listener, NodeHandle};
use common::config::*;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

/// Node logs (elections, step-downs, takeovers) go to the test's captured
/// output, which cargo prints only when the test fails.
pub fn init_logs() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .try_init();
}

pub fn node_cfg(node_id: u32, listen: &str, peers: Vec<PeerConfig>) -> NodeConfig {
    NodeConfig {
        node_id,
        listen: listen.to_string(),
        peers,
        election_mode: ElectionMode::OnDemand,
        policy: Policy::Composite,
        weights: Weights::default(),
        bid_timeout_ms: 100,
        degraded_after_ms: 3000,
        min_tenure_ms: 1000,
        cpu_source: CpuSource::Machine,
        workers: 4,
        seed: 42,
        queue_capacity: 1024,
        table_ttl_ms: 300_000,
        campus: CampusParams::default(),
    }
}

/// Three nodes on 127.0.0.1 with OS-assigned ports.
pub async fn start_cluster_with(tweak: impl Fn(&mut NodeConfig)) -> (Vec<NodeHandle>, Vec<String>) {
    init_logs();
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addrs: Vec<String> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect();
    let mut nodes = Vec::new();
    for (i, l) in listeners.into_iter().enumerate() {
        let peers = (0..3)
            .filter(|j| *j != i)
            .map(|j| PeerConfig {
                id: j as u32 + 1,
                addr: addrs[j].clone(),
            })
            .collect();
        let mut cfg = node_cfg(i as u32 + 1, &addrs[i], peers);
        tweak(&mut cfg);
        nodes.push(start_with_listener(cfg, l).await.unwrap());
    }
    (nodes, addrs)
}

pub async fn start_cluster() -> (Vec<NodeHandle>, Vec<String>) {
    start_cluster_with(|_| {}).await
}

pub fn client_cfg(addrs: &[String], timeout_ms: u64, attempts: u32) -> ClientConfig {
    ClientConfig {
        nodes: addrs.to_vec(),
        request_timeout_ms: timeout_ms,
        max_attempts: attempts,
        campus: CampusParams::default(),
    }
}

/// Waits until every node in `nodes` holds `n` results.
pub async fn wait_replicated(nodes: &[&NodeHandle], n: usize, within: Duration) {
    let deadline = Instant::now() + within;
    while nodes.iter().any(|h| h.results_snapshot().len() < n) {
        assert!(
            Instant::now() < deadline,
            "replication did not finish: {:?}",
            nodes
                .iter()
                .map(|h| h.results_snapshot().len())
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn shutdown(node: NodeHandle) {
    tokio::task::spawn_blocking(move || node.shutdown())
        .await
        .unwrap();
}

/// Waits until all nodes hold identical results tables. A request computed
/// twice (a retry won by another node) converges only when both results have
/// reached every node, which can be after each table already has `n` entries.
pub async fn wait_converged(nodes: &[&NodeHandle], n: usize, within: Duration) {
    wait_replicated(nodes, n, within).await;
    let deadline = Instant::now() + within;
    loop {
        let tables: Vec<_> = nodes.iter().map(|h| h.results_snapshot()).collect();
        if tables.windows(2).all(|w| w[0] == w[1]) {
            return;
        }
        if Instant::now() > deadline {
            for (k, v) in &tables[0] {
                let views: Vec<_> = tables
                    .iter()
                    .map(|t| t.get(k).map(|r| (r.served_by, r.epoch)))
                    .collect();
                if tables.iter().any(|t| t.get(k) != Some(v)) {
                    println!("{k}: (served_by, epoch) per node {views:?}");
                }
            }
            panic!("results tables did not converge");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Hard time limit for one integration test: fails it instead of hanging.
pub const TEST_LIMIT: Duration = Duration::from_secs(60);

pub async fn limit<F: std::future::Future<Output = ()>>(body: F) {
    tokio::time::timeout(TEST_LIMIT, body)
        .await
        .unwrap_or_else(|_| panic!("test exceeded its {TEST_LIMIT:?} limit"));
}
