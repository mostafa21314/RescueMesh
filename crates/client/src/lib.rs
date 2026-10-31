//! Client side of the protocol.
//!
//! Every request goes to all cloud nodes by unicast fan-out (one TCP
//! connection per node; the lab network likely blocks multicast). The first
//! PlanResponse wins. On timeout or Busy the client resends the same
//! request_id to all nodes with attempt + 1, up to `max_attempts`. Each
//! request carries the highest epoch the client has seen in any response.

use common::config::ClientConfig;
use common::counters::Counters;
use common::rng::Rng;
use common::wire::{read_frame, write_frame};
use common::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use uuid::Uuid;

pub const BACKOFF_MIN: Duration = Duration::from_millis(100);
pub const BACKOFF_MAX: Duration = Duration::from_secs(2);
/// After a Busy reply the client waits `BUSY_PAUSE × attempt` before resending,
/// so a full queue isn't hammered with instant retries.
pub const BUSY_PAUSE: Duration = Duration::from_millis(20);

/// What happened to one request.
#[derive(Clone, Debug)]
pub struct Outcome {
    pub request_id: RequestId,
    /// First send, Unix ms.
    pub sent_at_ms: u64,
    /// Arrival of the accepted PlanResponse, Unix ms.
    pub response_at_ms: Option<u64>,
    pub latency: Option<Duration>,
    pub served_by: Option<NodeId>,
    pub epoch: Option<Epoch>,
    pub degraded: bool,
    pub attempts: u32,
    pub busy_replies: u32,
    pub plan: Option<Plan>,
}

impl Outcome {
    pub fn answered(&self) -> bool {
        self.plan.is_some()
    }
}

/// Message type → (count, bytes), plus frames that could not be sent because
/// a node was unreachable.
#[derive(Clone, Debug)]
pub struct Traffic {
    pub sent: BTreeMap<String, (u64, u64)>,
    pub received: BTreeMap<String, (u64, u64)>,
    pub unsent: u64,
}

/// Shared by the client and its link tasks: routes replies to the waiting
/// request and tracks the highest epoch seen.
#[derive(Default)]
struct Dispatch {
    pending: Mutex<HashMap<RequestId, mpsc::UnboundedSender<Message>>>,
    epoch: AtomicU64,
    sent: Counters,
    received: Counters,
    unsent: AtomicU64,
}

impl Dispatch {
    fn deliver(&self, msg: Message) {
        let request_id = match &msg {
            Message::PlanResponse { request_id, epoch, .. } => {
                self.epoch.fetch_max(*epoch, Ordering::Relaxed);
                *request_id
            }
            Message::Busy { request_id } => *request_id,
            _ => return,
        };
        // Replies for requests already answered (late duplicates) are dropped.
        if let Some(tx) = self.pending.lock().unwrap().get(&request_id) {
            let _ = tx.send(msg);
        }
    }
}

pub struct Client {
    cfg: ClientConfig,
    client_id: Uuid,
    seq: AtomicU64,
    links: Vec<mpsc::UnboundedSender<Message>>,
    dispatch: Arc<Dispatch>,
}

impl Client {
    /// Must be called inside a tokio runtime; spawns one link task per node.
    pub fn new(cfg: ClientConfig) -> Self {
        let dispatch = Arc::new(Dispatch::default());
        let links = cfg
            .nodes
            .iter()
            .map(|addr| {
                let (tx, rx) = mpsc::unbounded_channel();
                let timeout = Duration::from_millis(cfg.request_timeout_ms);
                tokio::spawn(link_task(addr.clone(), timeout, rx, dispatch.clone()));
                tx
            })
            .collect();
        Client { cfg, client_id: Uuid::new_v4(), seq: AtomicU64::new(0), links, dispatch }
    }

    pub fn client_id(&self) -> Uuid {
        self.client_id
    }

    pub fn highest_epoch(&self) -> Epoch {
        self.dispatch.epoch.load(Ordering::Relaxed)
    }

    pub fn traffic(&self) -> Traffic {
        Traffic {
            sent: self.dispatch.sent.snapshot(),
            received: self.dispatch.received.snapshot(),
            unsent: self.dispatch.unsent.load(Ordering::Relaxed),
        }
    }

    /// Submits one incident and waits for a plan or for the attempts to run out.
    pub async fn submit(&self, incident: Incident, complexity: u32) -> Outcome {
        let request_id = RequestId { client_id: self.client_id, seq: self.seq.fetch_add(1, Ordering::Relaxed) };
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.dispatch.pending.lock().unwrap().insert(request_id, tx);

        let timeout = Duration::from_millis(self.cfg.request_timeout_ms);
        let sent_at_ms = unix_ms();
        let start = Instant::now();
        let mut out = Outcome {
            request_id,
            sent_at_ms,
            response_at_ms: None,
            latency: None,
            served_by: None,
            epoch: None,
            degraded: false,
            attempts: 0,
            busy_replies: 0,
            plan: None,
        };

        'attempts: for attempt in 1..=self.cfg.max_attempts {
            out.attempts = attempt;
            let msg = Message::SubmitIncident {
                request_id,
                attempt,
                client_epoch: self.highest_epoch(),
                incident: incident.clone(),
                complexity,
            };
            for link in &self.links {
                let _ = link.send(msg.clone());
            }
            let deadline = tokio::time::Instant::now() + timeout;
            let busy = loop {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(Message::PlanResponse { served_by, epoch, degraded, plan, .. })) => {
                        out.response_at_ms = Some(unix_ms());
                        out.latency = Some(start.elapsed());
                        out.served_by = Some(served_by);
                        out.epoch = Some(epoch);
                        out.degraded = degraded;
                        out.plan = Some(plan);
                        break 'attempts;
                    }
                    Ok(Some(Message::Busy { .. })) => {
                        out.busy_replies += 1;
                        break true;
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) | Err(_) => break false,
                }
            };
            if busy && attempt < self.cfg.max_attempts {
                tokio::time::sleep(BUSY_PAUSE * attempt).await;
            }
        }
        self.dispatch.pending.lock().unwrap().remove(&request_id);
        tracing::debug!(
            %request_id,
            answered = out.answered(),
            served_by = ?out.served_by,
            epoch = ?out.epoch,
            attempts = out.attempts,
            latency = ?out.latency,
            "request finished"
        );
        out
    }
}

/// One node's connection: opened on demand, reopened with backoff. While the
/// node is unreachable, frames for it are dropped (the retry rule covers it)
/// rather than queued, so a dead node never delays requests to live ones.
async fn link_task(addr: String, timeout: Duration, mut rx: mpsc::UnboundedReceiver<Message>, dispatch: Arc<Dispatch>) {
    let mut writer: Option<(tokio::net::tcp::OwnedWriteHalf, Arc<AtomicBool>, tokio::task::JoinHandle<()>)> = None;
    let mut backoff = BACKOFF_MIN;
    let mut next_try = Instant::now();
    while let Some(msg) = rx.recv().await {
        if writer.as_ref().is_some_and(|(_, alive, _)| !alive.load(Ordering::Relaxed)) {
            writer = None;
        }
        if writer.is_none() {
            if Instant::now() < next_try {
                dispatch.unsent.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            match tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
                Ok(Ok(stream)) => {
                    let _ = stream.set_nodelay(true);
                    let (rd, wr) = stream.into_split();
                    let alive = Arc::new(AtomicBool::new(true));
                    let reader = tokio::spawn(read_loop(rd, alive.clone(), dispatch.clone()));
                    writer = Some((wr, alive, reader));
                    backoff = BACKOFF_MIN;
                }
                _ => {
                    tracing::debug!(%addr, ?backoff, "node unreachable");
                    next_try = Instant::now() + backoff;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    dispatch.unsent.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
        }
        let (wr, _, _) = writer.as_mut().expect("connected above");
        match write_frame(wr, &msg).await {
            Ok(n) => dispatch.sent.add(msg.kind(), n),
            Err(e) => {
                tracing::debug!(%addr, %e, "write failed; will reconnect");
                dispatch.unsent.fetch_add(1, Ordering::Relaxed);
                writer = None;
            }
        }
    }
    if let Some((_, _, reader)) = writer {
        reader.abort();
    }
}

async fn read_loop(mut rd: tokio::net::tcp::OwnedReadHalf, alive: Arc<AtomicBool>, dispatch: Arc<Dispatch>) {
    while let Ok(Some((msg, n))) = read_frame(&mut rd).await {
        dispatch.received.add(msg.kind(), n);
        dispatch.deliver(msg);
    }
    alive.store(false, Ordering::Relaxed);
}

/// Asks one node for its status over a short-lived connection.
pub async fn query_status(addr: &str, timeout: Duration) -> anyhow::Result<NodeStatus> {
    let fut = async {
        let mut s = TcpStream::connect(addr).await?;
        write_frame(&mut s, &Message::StatusRequest).await?;
        loop {
            match read_frame(&mut s).await? {
                Some((Message::Status(st), _)) => return Ok(st),
                Some(_) => continue,
                None => anyhow::bail!("connection closed before Status"),
            }
        }
    };
    tokio::time::timeout(timeout, fut).await?
}

/// A synthetic incident at a random zone. Severity skews low (30/25/20/15/10 %).
pub fn synth_incident(rng: &mut Rng, zones: u32, reporter: &str) -> Incident {
    let severity = match rng.below(100) {
        0..=29 => 1,
        30..=54 => 2,
        55..=74 => 3,
        75..=89 => 4,
        _ => 5,
    };
    Incident {
        incident_id: Uuid::from_u64_pair(rng.next_u64(), rng.next_u64()),
        reporter: reporter.to_string(),
        category: Category::ALL[rng.below(Category::ALL.len() as u64) as usize],
        zone: rng.below(zones as u64) as u32,
        severity,
        timestamp_ms: unix_ms(),
        status: IncidentStatus::Reported,
        ttl_ms: rng.range(5_000, 30_000),
    }
}
