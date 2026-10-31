//! Bullet 2: `--burn-threads` on the node that would otherwise be elected
//! moves the election away from it.
//!
//! The nodes run as separate processes (the `cloud-node` binary) with
//! `cpu_source = "process"`: in one process, or with whole-machine CPU on one
//! machine, every node would see the same CPU reading.

use client::{synth_incident, Client};
use common::config::{CampusParams, ClientConfig};
use common::rng::Rng;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Hard time limit for the test: fails it instead of hanging. The cluster's
/// Drop kills the node processes either way.
const TEST_LIMIT: Duration = Duration::from_secs(90);

async fn limit<F: std::future::Future<Output = ()>>(body: F) {
    tokio::time::timeout(TEST_LIMIT, body)
        .await
        .unwrap_or_else(|_| panic!("test exceeded its {TEST_LIMIT:?} limit"));
}

struct Cluster {
    children: Vec<Child>,
    dir: PathBuf,
    addrs: Vec<String>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_cluster(tag: &str, burn_node1: usize) -> Cluster {
    let dir = std::env::temp_dir().join(format!("rescuemesh-burn-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect();
    let mut children = Vec::new();
    for i in 0..3 {
        let peers: Vec<String> = (0..3)
            .filter(|j| *j != i)
            .map(|j| format!("{{ id = {}, addr = \"{}\" }}", j + 1, addrs[j]))
            .collect();
        let cfg = format!(
            "node_id = {}\nlisten = \"{}\"\npeers = [{}]\nelection_mode = \"on_demand\"\npolicy = \"Composite\"\n\
             bid_timeout_ms = 100\ndegraded_after_ms = 3000\ncpu_source = \"process\"\nworkers = 2\nseed = 42\n",
            i + 1,
            addrs[i],
            peers.join(", ")
        );
        let path = dir.join(format!("node{}.toml", i + 1));
        std::fs::write(&path, cfg).unwrap();
        let log = std::fs::File::create(dir.join(format!("node{}.log", i + 1))).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_cloud-node"));
        cmd.arg("--config")
            .arg(&path)
            .stdout(Stdio::null())
            .stderr(log);
        if i == 0 && burn_node1 > 0 {
            cmd.arg("--burn-threads").arg(burn_node1.to_string());
        }
        children.push(cmd.spawn().unwrap());
    }
    Cluster {
        children,
        dir,
        addrs,
    }
}

async fn served_by(cluster: &Cluster) -> Vec<u32> {
    // Wait for the listeners, then for the CPU EWMAs to settle (8+ samples).
    let deadline = Instant::now() + Duration::from_secs(30);
    for a in &cluster.addrs {
        while tokio::net::TcpStream::connect(a).await.is_err() {
            assert!(Instant::now() < deadline, "node at {a} did not start");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    // The first request triggers the startup election (no incumbent).
    let client = Client::new(ClientConfig {
        nodes: cluster.addrs.clone(),
        request_timeout_ms: 3_000,
        max_attempts: 3,
        campus: CampusParams::default(),
    });
    let mut rng = Rng::new(1);
    let mut out = Vec::new();
    for _ in 0..5 {
        let o = client
            .submit(synth_incident(&mut rng, 256, "burn"), 1)
            .await;
        out.push(o.served_by.expect("answered"));
    }
    out
}

fn logs(cluster: &Cluster) -> String {
    (1..=3)
        .map(|i| {
            std::fs::read_to_string(cluster.dir.join(format!("node{i}.log"))).unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn burn_threads_move_the_election_away() {
    limit(async {
        // Control: all idle, scores tie, the lowest node_id (1) wins.
        let idle = spawn_cluster("idle", 0);
        let s = served_by(&idle).await;
        assert!(
            s.iter().all(|n| *n == 1),
            "idle cluster should elect node 1: {s:?}\n{}",
            logs(&idle)
        );
        drop(idle);

        // Node 1 burns half the cores: its CPU term is ~0.5, the others ~0.
        let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
        let hot = spawn_cluster("hot", (cores / 2).max(1));
        let s = served_by(&hot).await;
        assert!(
            s.iter().all(|n| *n != 1),
            "election should avoid the burning node: {s:?}\n{}",
            logs(&hot)
        );
        println!("idle → node 1; node 1 burning → node {}", s[0]);
    })
    .await
}
