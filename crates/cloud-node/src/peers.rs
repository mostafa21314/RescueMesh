//! Outbound links to the other cloud nodes.
//!
//! Servers talk to each other only to replicate and (Part B) to elect, so a
//! link opens on demand: its task connects when the first message is queued,
//! and reconnects with exponential backoff after a failure. There are no
//! heartbeats. Each direction of a node pair uses its own TCP connection;
//! inbound peer messages arrive on our listener like client traffic.
//!
//! Delivery is at-least-once while both ends are up: a frame whose write
//! failed is resent after reconnecting, and receivers apply messages
//! idempotently. A frame accepted by the kernel just before the peer dies is
//! lost; catching the peer up when it returns is the recovery protocol's job.

use common::config::PeerConfig;
use common::counters::Counters;
use common::wire::write_frame;
use common::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub const BACKOFF_MIN: Duration = Duration::from_millis(100);
pub const BACKOFF_MAX: Duration = Duration::from_secs(2);
pub const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
/// Messages buffered per peer while its link is down; beyond this they are
/// dropped and counted.
pub const QUEUE_LIMIT: usize = 4096;
const WRITE_TIMEOUT: Duration = Duration::from_secs(1);

struct Control {
    enabled: AtomicBool,
    generation: AtomicU64,
}

struct Envelope {
    generation: u64,
    message: Message,
}

pub struct PeerLinks {
    links: BTreeMap<NodeId, mpsc::Sender<Envelope>>,
    control: Arc<Control>,
    last_seen: Mutex<HashMap<NodeId, Instant>>,
    drops: AtomicU64,
}

impl PeerLinks {
    pub fn spawn(peers: &[PeerConfig], sent: Arc<Counters>) -> (Arc<Self>, Vec<JoinHandle<()>>) {
        let mut links = BTreeMap::new();
        let mut tasks = Vec::new();
        let control = Arc::new(Control {
            enabled: AtomicBool::new(true),
            generation: AtomicU64::new(0),
        });
        for p in peers {
            let (tx, rx) = mpsc::channel(QUEUE_LIMIT);
            links.insert(p.id, tx);
            tasks.push(tokio::spawn(link_task(
                p.id,
                p.addr.clone(),
                rx,
                sent.clone(),
                control.clone(),
            )));
        }
        let me = Arc::new(PeerLinks {
            links,
            last_seen: Mutex::new(HashMap::new()),
            drops: AtomicU64::new(0),
            control,
        });
        (me, tasks)
    }

    pub fn send(&self, to: NodeId, msg: Message) {
        if !self.control.enabled.load(Ordering::SeqCst) {
            return;
        }
        if let Some(tx) = self.links.get(&to) {
            let envelope = Envelope {
                generation: self.control.generation.load(Ordering::SeqCst),
                message: msg,
            };
            if tx.try_send(envelope).is_err() {
                self.drops.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(peer = to, "peer queue full or closed, message dropped");
            }
        }
    }

    pub fn suspend(&self) {
        self.control.enabled.store(false, Ordering::SeqCst);
        self.control.generation.fetch_add(1, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.control.enabled.store(true, Ordering::SeqCst);
    }

    pub fn broadcast(&self, msg: Message) {
        for id in self.links.keys() {
            self.send(*id, msg.clone());
        }
    }

    /// Notes that a message from `id` just arrived.
    pub fn heard_from(&self, id: NodeId) {
        if self.links.contains_key(&id) {
            self.last_seen.lock().unwrap().insert(id, Instant::now());
        }
    }

    pub fn last_seen_ms(&self) -> Vec<(NodeId, Option<u64>)> {
        let seen = self.last_seen.lock().unwrap();
        self.links
            .keys()
            .map(|id| (*id, seen.get(id).map(|t| t.elapsed().as_millis() as u64)))
            .collect()
    }

    pub fn drops(&self) -> u64 {
        self.drops.load(Ordering::Relaxed)
    }
}

async fn link_task(
    peer: NodeId,
    addr: String,
    mut rx: mpsc::Receiver<Envelope>,
    sent: Arc<Counters>,
    control: Arc<Control>,
) {
    let mut conn: Option<TcpStream> = None;
    let mut backoff = BACKOFF_MIN;
    let mut failures = 0u32;
    while let Some(envelope) = rx.recv().await {
        let msg = envelope.message;
        loop {
            if !control.enabled.load(Ordering::SeqCst)
                || envelope.generation != control.generation.load(Ordering::SeqCst)
            {
                conn = None;
                break;
            }
            if conn.is_none() {
                match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr)).await {
                    Ok(Ok(s)) => {
                        let _ = s.set_nodelay(true);
                        tracing::info!(peer, %addr, "peer link connected");
                        conn = Some(s);
                        backoff = BACKOFF_MIN;
                        failures = 0;
                    }
                    outcome => {
                        failures += 1;
                        let why = match outcome {
                            Ok(Err(e)) => e.to_string(),
                            _ => "connect timed out".to_string(),
                        };
                        if failures == 1 {
                            tracing::warn!(peer, %addr, %why, "peer unreachable, retrying with backoff");
                        } else {
                            tracing::debug!(peer, %addr, %why, ?backoff, "peer still unreachable");
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(BACKOFF_MAX);
                        continue;
                    }
                }
            }
            let stream = conn.as_mut().expect("connected above");
            match tokio::time::timeout(WRITE_TIMEOUT, write_frame(stream, &msg)).await {
                Ok(Ok(n)) => {
                    sent.add(msg.kind(), n);
                    break;
                }
                error => {
                    tracing::debug!(
                        peer,
                        ?error,
                        "peer link write failed or timed out, reconnecting"
                    );
                    conn = None;
                }
            }
        }
    }
}
