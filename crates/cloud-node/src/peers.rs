//! Independent control and batched replication links. Overflow is observable;
//! the retained result journal supplies incremental repair outside these queues.
use common::config::PeerConfig;
use common::counters::Counters;
use common::rr::{write_frame, Stream};
use common::*;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub const BACKOFF_MIN: Duration = Duration::from_millis(100);
pub const BACKOFF_MAX: Duration = Duration::from_secs(2);
pub const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
pub const QUEUE_LIMIT: usize = 4096;
const WRITE_TIMEOUT: Duration = Duration::from_secs(3);
struct Control {
    enabled: AtomicBool,
    generation: AtomicU64,
}
struct Envelope {
    generation: u64,
    resets: u64,
    message: Message,
}
#[derive(Default)]
struct Lane {
    queue: Mutex<VecDeque<Envelope>>,
    queued: Notify,
    dropped: AtomicU64,
    inflight: AtomicUsize,
}
#[derive(Default)]
struct Link {
    control: Lane,
    replication: Lane,
    resets: AtomicU64,
    reset: Notify,
    batches_sent: AtomicU64,
    reconnects: AtomicU64,
}
pub struct PeerLinks {
    links: BTreeMap<NodeId, Arc<Link>>,
    control: Arc<Control>,
    last_seen: Mutex<HashMap<NodeId, Instant>>,
}

impl PeerLinks {
    pub fn spawn(peers: &[PeerConfig], sent: Arc<Counters>) -> (Arc<Self>, Vec<JoinHandle<()>>) {
        let control = Arc::new(Control {
            enabled: AtomicBool::new(true),
            generation: AtomicU64::new(0),
        });
        let mut links = BTreeMap::new();
        let mut tasks = Vec::new();
        for p in peers {
            let link = Arc::new(Link::default());
            links.insert(p.id, link.clone());
            for data in [false, true] {
                tasks.push(tokio::spawn(link_task(
                    p.id,
                    p.addr.clone(),
                    link.clone(),
                    sent.clone(),
                    control.clone(),
                    data,
                )));
            }
        }
        (
            Arc::new(Self {
                links,
                control,
                last_seen: Mutex::new(HashMap::new()),
            }),
            tasks,
        )
    }
    pub fn send(&self, to: NodeId, message: Message) {
        if !self.control.enabled.load(Ordering::SeqCst) {
            return;
        }
        let Some(link) = self.links.get(&to) else {
            return;
        };
        let data = matches!(message, Message::Replicate { .. });
        let lane = if data {
            &link.replication
        } else {
            &link.control
        };
        let mut queue = lane.queue.lock().unwrap();
        if queue.len() >= QUEUE_LIMIT {
            let dropped = lane.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped.is_power_of_two() {
                tracing::warn!(
                    peer = to,
                    data,
                    dropped,
                    "peer queue overflow; retained results will be repaired"
                );
            }
            return;
        }
        queue.push_back(Envelope {
            generation: self.control.generation.load(Ordering::SeqCst),
            resets: link.resets.load(Ordering::SeqCst),
            message,
        });
        drop(queue);
        lane.queued.notify_one();
    }
    /// Only a FULL snapshot covers all queued history. A delta must not call this.
    pub fn reset(&self, to: NodeId) {
        if let Some(link) = self.links.get(&to) {
            let mut control = link.control.queue.lock().unwrap();
            let mut data = link.replication.queue.lock().unwrap();
            link.resets.fetch_add(1, Ordering::SeqCst);
            control.clear();
            data.clear();
            drop(data);
            drop(control);
            link.reset.notify_waiters();
            link.control.queued.notify_one();
            link.replication.queued.notify_one();
        }
    }
    pub fn suspend(&self) {
        self.control.enabled.store(false, Ordering::SeqCst);
        self.control.generation.fetch_add(1, Ordering::SeqCst);
    }
    pub fn resume(&self) {
        self.control.enabled.store(true, Ordering::SeqCst);
    }
    pub fn broadcast(&self, message: Message) {
        for id in self.links.keys() {
            self.send(*id, message.clone());
        }
    }
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
    pub fn queues(&self) -> Vec<PeerQueueStatus> {
        self.links
            .iter()
            .map(|(&node_id, link)| PeerQueueStatus {
                node_id,
                control_queued: link.control.queue.lock().unwrap().len()
                    + link.control.inflight.load(Ordering::Relaxed),
                replication_queued: link.replication.queue.lock().unwrap().len()
                    + link.replication.inflight.load(Ordering::Relaxed),
                dropped_control: link.control.dropped.load(Ordering::Relaxed),
                dropped_replication: link.replication.dropped.load(Ordering::Relaxed),
                batches_sent: link.batches_sent.load(Ordering::Relaxed),
                reconnects: link.reconnects.load(Ordering::Relaxed),
            })
            .collect()
    }
    pub fn drops(&self) -> u64 {
        self.links
            .values()
            .map(|l| {
                l.control.dropped.load(Ordering::Relaxed)
                    + l.replication.dropped.load(Ordering::Relaxed)
            })
            .sum()
    }
}

fn batch(envelopes: &[Envelope]) -> Message {
    Message::ReplicateBatch {
        records: envelopes
            .iter()
            .map(|e| {
                let Message::Replicate {
                    epoch,
                    origin,
                    request_id,
                    plan,
                    incident,
                    degraded,
                    retained_until_ms,
                } = &e.message
                else {
                    unreachable!()
                };
                RecoveryRecord {
                    request_id: *request_id,
                    attempt: 0,
                    epoch: *epoch,
                    incident: incident.clone(),
                    complexity: 0,
                    result: Some(RecoveryResult {
                        served_by: *origin,
                        epoch: *epoch,
                        degraded: *degraded,
                        plan: plan.clone(),
                        retained_until_ms: *retained_until_ms,
                    }),
                }
            })
            .collect(),
    }
}

async fn link_task(
    peer: NodeId,
    addr: String,
    link: Arc<Link>,
    sent: Arc<Counters>,
    control: Arc<Control>,
    data: bool,
) {
    let lane = if data {
        &link.replication
    } else {
        &link.control
    };
    let mut conn: Option<Stream> = None;
    let mut backoff = BACKOFF_MIN;
    loop {
        let mut envelopes = Vec::new();
        {
            let mut queue = lane.queue.lock().unwrap();
            let can_batch = data
                || queue
                    .front()
                    .is_some_and(|e| matches!(e.message, Message::ElectBid { .. }));
            let mut bytes = 0;
            while let Some(e) = queue.pop_front() {
                if e.generation != control.generation.load(Ordering::SeqCst)
                    || e.resets != link.resets.load(Ordering::SeqCst)
                {
                    continue;
                }
                if !data && !envelopes.is_empty() && !matches!(e.message, Message::ElectBid { .. })
                {
                    queue.push_front(e);
                    break;
                }
                bytes += serde_json::to_vec(&e.message)
                    .expect("serialize peer message")
                    .len();
                envelopes.push(e);
                if !can_batch || envelopes.len() >= 64 || bytes >= 192 * 1024 {
                    break;
                }
            }
        }
        if envelopes.is_empty() {
            lane.queued.notified().await;
            continue;
        }
        lane.inflight.store(envelopes.len(), Ordering::Relaxed);
        let message = if data {
            batch(&envelopes)
        } else if envelopes.len() > 1 {
            Message::ElectBidBatch {
                bids: envelopes.iter().map(|e| e.message.clone()).collect(),
            }
        } else {
            envelopes[0].message.clone()
        };
        loop {
            let first = &envelopes[0];
            if !control.enabled.load(Ordering::SeqCst)
                || first.generation != control.generation.load(Ordering::SeqCst)
                || first.resets != link.resets.load(Ordering::SeqCst)
            {
                conn = None;
                backoff = BACKOFF_MIN;
                break;
            }
            if conn.is_none() {
                match tokio::time::timeout(CONNECT_TIMEOUT, Stream::connect(&addr)).await {
                    Ok(Ok(stream)) => {
                        conn = Some(stream);
                        backoff = BACKOFF_MIN;
                        link.reconnects.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    _ => {
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => backoff = (backoff*2).min(BACKOFF_MAX),
                            _ = link.reset.notified() => backoff = BACKOFF_MIN,
                        }
                        continue;
                    }
                }
            }
            match tokio::time::timeout(WRITE_TIMEOUT, write_frame(conn.as_mut().unwrap(), &message))
                .await
            {
                Ok(Ok(bytes)) => {
                    if data {
                        link.batches_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    for (i, e) in envelopes.iter().enumerate() {
                        sent.add(
                            e.message.kind(),
                            bytes / envelopes.len() + usize::from(i < bytes % envelopes.len()),
                        );
                    }
                    break;
                }
                error => {
                    tracing::debug!(peer, data, ?error, "peer write failed; reconnecting");
                    conn = None;
                }
            }
        }
        lane.inflight.store(0, Ordering::Relaxed);
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
