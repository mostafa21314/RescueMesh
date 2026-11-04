//! Real UDP relays exercise sustained RTT, overflow repair and a 20 s outage.
mod support;
use client::{synth_incident, Client};
use cloud_node::{start_with_listener, NodeHandle};
use common::{config::*, rng::Rng, rr::Listener};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use support::*;
static NETWORK_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
use tokio::{
    net::UdpSocket,
    task::{JoinHandle, JoinSet},
};

struct Relay {
    addr: String,
    task: JoinHandle<()>,
}
impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn relay(target: SocketAddr, delay: Duration, block_bulk: Arc<AtomicBool>) -> Relay {
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = socket.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let mut routes = HashMap::new();
        let mut kinds = HashMap::new();
        let mut deliveries = JoinSet::new();
        let mut packet = [0u8; 1201];
        loop {
            tokio::select! {
                received = socket.recv_from(&mut packet) => {
                    // Windows reports delayed ICMP errors when a short-lived
                    // session closes. Keep the simulated network running.
                    let Ok((n, from)) = received else { continue };
                    if n < 37 { continue; }
                    let id = uuid::Uuid::from_slice(&packet[5..21]).unwrap();
                    let returning = from == target;
                    let destination = if returning {
                        let Some(addr) = routes.get(&id) else { continue }; *addr
                    } else { routes.insert(id, from); target };
                    if packet[4] == 0 {
                        let seq = u64::from_be_bytes(packet[21..29].try_into().unwrap());
                        let index = u32::from_be_bytes(packet[29..33].try_into().unwrap());
                        let key = (id, seq, returning);
                        if index == 0 {
                            let body = String::from_utf8_lossy(&packet[37..n]);
                            kinds.insert(key, body.starts_with("{\"type\":\"ReplicateBatch\"") || body.starts_with("{\"type\":\"SyncReply\""));
                        }
                        if block_bulk.load(Ordering::Relaxed) && *kinds.get(&key).unwrap_or(&true) { continue; }
                    }
                    let body = packet[..n].to_vec();
                    let socket = socket.clone();
                    deliveries.spawn(async move { tokio::time::sleep(delay).await; let _ = socket.send_to(&body, destination).await; });
                }
                Some(_) = deliveries.join_next(), if !deliveries.is_empty() => {}
            }
        }
    });
    Relay { addr, task }
}

async fn cluster(
    delay: Duration,
    paused: Arc<AtomicBool>,
) -> (Vec<NodeHandle>, Vec<String>, Vec<Relay>) {
    init_logs();
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(Listener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses: Vec<_> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect();
    let mut relays = Vec::new();
    for address in &addresses {
        relays.push(relay(address.parse().unwrap(), delay, paused.clone()).await);
    }
    let mut nodes = Vec::new();
    for (i, listener) in listeners.into_iter().enumerate() {
        let peers = (0..3)
            .filter(|j| *j != i)
            .map(|j| PeerConfig {
                id: j as u32 + 1,
                addr: relays[j].addr.clone(),
            })
            .collect();
        let mut config = node_cfg(i as u32 + 1, &addresses[i], peers);
        config.campus = CampusParams {
            grid: 4,
            responders: 20,
            resources: 16,
        };
        config.recovery.on_start = false;
        config.bid_timeout_ms = 200;
        config.queue_capacity = 64;
        nodes.push(start_with_listener(config, listener).await.unwrap());
    }
    (nodes, addresses, relays)
}

async fn workload(client: Arc<Client>, count: usize, rate: u64) -> usize {
    let mut rng = Rng::new(125);
    let mut clock = tokio::time::interval(Duration::from_micros(1_000_000 / rate));
    clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pending = JoinSet::new();
    for _ in 0..count {
        clock.tick().await;
        let incident = synth_incident(&mut rng, 16, "delayed-test");
        let client = client.clone();
        pending.spawn(async move { client.submit(incident, 1).await.answered() });
    }
    let mut answered = 0;
    while let Some(outcome) = pending.join_next().await {
        answered += usize::from(outcome.unwrap());
    }
    answered
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn sustained_replication_converges_with_forty_millisecond_round_trips() {
    let _serial = NETWORK_TEST.lock().await;
    limit(async {
        let (nodes, addresses, _relays) =
            cluster(Duration::from_millis(20), Arc::new(AtomicBool::new(false))).await;
        let client = Arc::new(Client::new(client_cfg(&addresses, 1000, 8)));
        assert_eq!(workload(client, 1600, 400).await, 1600);
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            1600,
            Duration::from_secs(15),
        )
        .await;
        assert!(nodes.iter().all(|n| n.status().queue_len <= 64));
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dropped_replication_is_repaired_without_a_restart_or_injected_failure() {
    let _serial = NETWORK_TEST.lock().await;
    limit(async {
        let pause = Arc::new(AtomicBool::new(true));
        let (nodes, addresses, _relays) = cluster(Duration::from_millis(1), pause.clone()).await;
        let client = Arc::new(Client::new(client_cfg(&addresses, 1500, 8)));
        assert_eq!(workload(client, 5200, 800).await, 5200);
        assert!(
            nodes.iter().any(|n| n
                .status()
                .peer_queues
                .iter()
                .any(|q| q.dropped_replication > 0)),
            "the test must actually overflow a replication queue"
        );
        pause.store(false, Ordering::Relaxed);
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            5200,
            Duration::from_secs(20),
        )
        .await;
        assert!(nodes
            .iter()
            .any(|n| n.status().sync.iter().any(|s| s.repaired_results > 0)));
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn loaded_incumbent_keeps_control_responsive_on_delayed_links() {
    let _serial = NETWORK_TEST.lock().await;
    limit(async {
        let (nodes, addresses, _relays) =
            cluster(Duration::from_millis(20), Arc::new(AtomicBool::new(false))).await;
        let client = Arc::new(Client::new(client_cfg(&addresses, 1000, 8)));
        let mut rng = Rng::new(18);
        let warm = client.submit(synth_incident(&mut rng, 16, "warm"), 1).await;
        let victim = warm.served_by.unwrap();
        let address = &addresses[victim as usize - 1];
        client::set_load(address, 2, 8000, Duration::from_secs(2))
            .await
            .unwrap();
        let work = tokio::spawn(workload(client, 400, 100));
        for _ in 0..8 {
            let probe = client::query_probe(address, Duration::from_secs(1))
                .await
                .expect("incumbent control traffic must remain responsive");
            assert!(probe.queue_len <= 64);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert_eq!(work.await.unwrap(), 400);
        client::set_load(address, 0, 0, Duration::from_secs(2))
            .await
            .unwrap();
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            401,
            Duration::from_secs(15),
        )
        .await;
        assert!(nodes
            .iter()
            .all(|n| n.status().epoch <= warm.epoch.unwrap() + 3));
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn delayed_links_keep_clients_running_through_a_twenty_second_outage() {
    let _serial = NETWORK_TEST.lock().await;
    limit(async {
        let (nodes, addresses, _relays) =
            cluster(Duration::from_millis(15), Arc::new(AtomicBool::new(false))).await;
        let client = Arc::new(Client::new(client_cfg(&addresses, 1000, 8)));
        let mut rng = Rng::new(7);
        let warm = client.submit(synth_incident(&mut rng, 16, "warm"), 1).await;
        let victim = warm.served_by.unwrap();
        let work = tokio::spawn(workload(client, 750, 30));
        tokio::time::sleep(Duration::from_secs(2)).await;
        nodes[victim as usize - 1]
            .inject_failure(Duration::from_secs(20))
            .unwrap();
        assert_eq!(work.await.unwrap(), 750);
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            751,
            Duration::from_secs(20),
        )
        .await;
        assert!(nodes.iter().all(|n| n.status().queue_len <= 64));
        assert!(
            nodes
                .iter()
                .all(|n| n.status().epoch <= warm.epoch.unwrap() + 3),
            "no election storm"
        );
        for node in nodes {
            shutdown(node).await;
        }
    })
    .await;
}
