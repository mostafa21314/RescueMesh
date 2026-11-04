//! Outbound links to the other cloud nodes.
//!
//! Servers talk to each other only to replicate and (Part B) to elect, so a
//! link opens on demand: its task connects when the first message is queued,
//! and reconnects with exponential backoff after a failure. There are no
//! heartbeats. Each direction of a node pair uses its own UDP RR session;
//! inbound peer messages arrive on our listener like client traffic.
//!
//! Delivery is at-least-once while both ends are up: a frame whose write
//! failed is resent after reconnecting, and receivers apply messages
//! idempotently. A frame acknowledged just before the peer dies can still be
//! lost; catching the peer up when it returns is the recovery protocol's job.
//! When a peer pulls a recovery snapshot from us, everything still queued for
//! it is in that snapshot, so the queue is cleared and the link reconnects at
//! once ([`PeerLinks::reset`]).

use common::config::PeerConfig;
use common::counters::Counters;
use common::rr::write_frame;
use common::rr::Stream;
use common::*;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
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
    /// The link's reset count when queued; older envelopes are not sent.
    resets: u64,
    message: Message,
}

/// One peer's outbound queue, shared with its link task.
struct Link {
    queue: Mutex<VecDeque<Envelope>>,
    queued: Notify,
    resets: AtomicU64,
    reset: Notify,
}

pub struct PeerLinks {
    links: BTreeMap<NodeId, Arc<Link>>,
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
            let link = Arc::new(Link {
                queue: Mutex::new(VecDeque::new()),
                queued: Notify::new(),
                resets: AtomicU64::new(0),
                reset: Notify::new(),
            });
            links.insert(p.id, link.clone());
            tasks.push(tokio::spawn(link_task(
                p.id,
                p.addr.clone(),
                link,
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
        if let Some(link) = self.links.get(&to) {
            let mut queue = link.queue.lock().unwrap();
            if queue.len() >= QUEUE_LIMIT {
                drop(queue);
                self.drops.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(peer = to, "peer queue full, message dropped");
                return;
            }
            queue.push_back(Envelope {
                generation: self.control.generation.load(Ordering::SeqCst),
                resets: link.resets.load(Ordering::SeqCst),
                message: msg,
            });
            drop(queue);
            link.queued.notify_one();
        }
    }

    /// `to` is taking a recovery snapshot from us. Call it under the state
    /// lock that the snapshot is read under: every frame queued for `to` so
    /// far is then covered by the snapshot, so it is dropped (with the one
    /// being retried), and the link stops backing off and reconnects now.
    /// Without this a peer that was down long enough to fill its queue missed
    /// frames sent after its snapshot, while the backlog drained.
    pub fn reset(&self, to: NodeId) {
        if let Some(link) = self.links.get(&to) {
            let mut queue = link.queue.lock().unwrap();
            link.resets.fetch_add(1, Ordering::SeqCst);
            queue.clear();
            drop(queue);
            link.reset.notify_one();
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
    link: Arc<Link>,
    sent: Arc<Counters>,
    control: Arc<Control>,
) {
    let mut conn: Option<Stream> = None;
    let mut backoff = BACKOFF_MIN;
    let mut failures = 0u32;
    loop {
        let next = link.queue.lock().unwrap().pop_front();
        let Some(envelope) = next else {
            link.queued.notified().await;
            continue;
        };
        let msg = envelope.message;
        loop {
            if !control.enabled.load(Ordering::SeqCst)
                || envelope.generation != control.generation.load(Ordering::SeqCst)
            {
                conn = None;
                break;
            }
            if envelope.resets != link.resets.load(Ordering::SeqCst) {
                conn = None;
                backoff = BACKOFF_MIN;
                break;
            }
            if conn.is_none() {
                match tokio::time::timeout(CONNECT_TIMEOUT, Stream::connect(&addr)).await {
                    Ok(Ok(s)) => {
                        tracing::info!(peer, %addr, "peer link connected");
                        conn = Some(s);
                        backoff = BACKOFF_MIN;
                        failures = 0;
                        // A recovery reset may have happened while the UDP
                        // handshake was awaiting its reply. Recheck the envelope.
                        continue;
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
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {
                                backoff = (backoff * 2).min(BACKOFF_MAX);
                            }
                            _ = link.reset.notified() => backoff = BACKOFF_MIN,
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::rr::read_frame;
    use common::rr::Listener;

    #[tokio::test]
    async fn reset_drops_the_backlog_and_reconnects_without_waiting_out_the_backoff() {
        // A peer address that refuses connections until we bind it again.
        let addr = Listener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        let (links, tasks) = PeerLinks::spawn(
            &[PeerConfig {
                id: 2,
                addr: addr.clone(),
            }],
            Arc::new(Counters::default()),
        );
        for _ in 0..QUEUE_LIMIT + 10 {
            links.send(2, Message::StatusRequest);
        }
        assert!(links.drops() > 0, "a full queue drops new frames");

        // Backoff has grown to 1.6 s by now; the peer comes back.
        tokio::time::sleep(Duration::from_millis(1_600)).await;
        let listener = Listener::bind(&addr).await.unwrap();
        let before = links.drops();
        links.reset(2);
        links.send(2, Message::ProbeRequest);
        assert_eq!(links.drops(), before, "the reset made room");

        let first = tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                // An in-progress UDP handshake can finish just as reset runs.
                // It must carry no stale payload; skip the abandoned session.
                if let Ok(Ok(Some((message, _)))) =
                    tokio::time::timeout(Duration::from_millis(50), read_frame(&mut stream)).await
                {
                    break message;
                }
            }
        })
        .await
        .expect("fresh payload arrived before the backoff ran out");
        assert!(matches!(first, Message::ProbeRequest), "{first:?}");
        for t in tasks {
            t.abort();
        }
    }
}
