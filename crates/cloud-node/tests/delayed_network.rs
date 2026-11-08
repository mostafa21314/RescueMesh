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
    cluster_mode(delay, paused, ElectionMode::OnDemand).await
}

async fn cluster_mode(
    delay: Duration,
    paused: Arc<AtomicBool>,
    mode: ElectionMode,
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
        config.election_mode = mode;
        nodes.push(start_with_listener(config, listener).await.unwrap());
    }
    (nodes, addresses, relays)
}

fn assert_response_reservations(outcomes: &[client::Outcome]) {
    use common::{Assignee, Availability};
    let campus = cloud_node::campus::Campus::generate(
        42,
        CampusParams {
            grid: 4,
            responders: 20,
            resources: 16,
        },
    );
    let mut resources: HashMap<u32, Vec<(u64, i64)>> = HashMap::new();
    let mut responders: HashMap<u32, Vec<(u64, u64)>> = HashMap::new();
    for o in outcomes {
        assert!(o.answered());
        let owner = o.served_by.unwrap();
        for a in &o.plan.as_ref().unwrap().assignments {
            match a.assignee {
                Assignee::Resource {
                    resource_id,
                    quantity,
                    ..
                } => {
                    assert_eq!(owner, resource_id % 3 + 1);
                    resources.entry(resource_id).or_default().extend([
                        (a.created_at_ms, quantity as i64),
                        (a.expires_at_ms, -(quantity as i64)),
                    ]);
                }
                Assignee::Responder { responder_id, .. } => {
                    assert_eq!(owner, responder_id % 3 + 1);
                    responders
                        .entry(responder_id)
                        .or_default()
                        .push((a.created_at_ms, a.expires_at_ms));
                }
            }
        }
    }
    assert!(
        !resources.is_empty() && !responders.is_empty(),
        "safety check must exercise actual assignments"
    );
    for (id, mut events) in resources {
        let cap = campus
            .resources
            .iter()
            .find(|r| r.resource_id == id && r.availability == Availability::Available)
            .unwrap()
            .quantity as i64;
        events.sort_unstable();
        let mut used = 0;
        for (_, delta) in events {
            used += delta;
            assert!(
                used <= cap && used >= 0,
                "returned plans overbook resource {id}"
            );
        }
    }
    for (id, mut intervals) in responders {
        intervals.sort_unstable();
        assert!(
            intervals.windows(2).all(|w| w[0].1 <= w[1].0),
            "returned plans double-book responder {id}"
        );
    }
}

async fn per_request_fault_safety(outage: u64) {
    let _serial = NETWORK_TEST.lock().await;
    limit(async {
        let (nodes, addresses, _relays) = cluster_mode(
            Duration::from_millis(20),
            Arc::new(AtomicBool::new(false)),
            ElectionMode::PerRequest,
        )
        .await;
        let duration = outage + 5;
        let mut senders = JoinSet::new();
        for pc in 0..3 {
            let client = Arc::new(Client::new(client_cfg(&addresses, 1000, 8)));
            senders.spawn(async move {
                let mut rng = Rng::new(200 + pc);
                let mut clock = tokio::time::interval(Duration::from_micros(1_000_000 / 30));
                clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut jobs = JoinSet::new();
                for _ in 0..duration * 30 {
                    clock.tick().await;
                    let inc = synth_incident(&mut rng, 16, "per-request-fault");
                    let c = client.clone();
                    jobs.spawn(async move { c.submit(inc, 5).await });
                }
                let mut outcomes = Vec::new();
                while let Some(o) = jobs.join_next().await {
                    outcomes.push(o.unwrap());
                }
                outcomes
            });
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        nodes[1]
            .inject_failure(Duration::from_secs(outage))
            .unwrap();
        let mut outcomes = Vec::new();
        while let Some(result) = senders.join_next().await {
            outcomes.extend(result.unwrap());
        }
        assert_response_reservations(&outcomes);
        wait_converged(
            &nodes.iter().collect::<Vec<_>>(),
            outcomes.len(),
            Duration::from_secs(20),
        )
        .await;
        assert!(
            nodes.iter().any(|n| n
                .status()
                .received
                .get("ElectBidBatch")
                .is_some_and(|c| c.0 > 0)),
            "exercise batched election traffic"
        );
        for n in &nodes {
            let status = n.status();
            assert_eq!(status.resource_overcommits, 0);
            assert_eq!(status.replicate_conflicts, 0);
            let allocation = status.allocation.unwrap();
            assert_eq!(
                (
                    allocation.capacity_violations,
                    allocation.responder_violations,
                    allocation.rejected_plans
                ),
                (0, 0, 0)
            );
        }
        for n in nodes {
            shutdown(n).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn per_request_five_second_fault_preserves_resource_capacity_on_delayed_links() {
    per_request_fault_safety(5).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn per_request_twenty_second_fault_preserves_resource_capacity_on_delayed_links() {
    per_request_fault_safety(20).await;
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
