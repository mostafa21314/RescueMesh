//! Client side of the protocol.
//!
//! Every request goes to all cloud nodes by unicast fan-out (one UDP RR
//! session per node; the lab network likely blocks multicast). The first
//! PlanResponse wins. On timeout or Busy the client resends the same
//! request_id to all nodes with attempt + 1, up to `max_attempts`. Each
//! request carries the highest epoch the client has seen in any response.

pub mod stats;
pub mod stress;

use common::config::ClientConfig;
use common::counters::Counters;
use common::rng::{mix, Rng};
use common::rr::Stream;
use common::rr::{read_frame, write_frame};
use common::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

pub const BACKOFF_MIN: Duration = Duration::from_millis(100);
pub const BACKOFF_MAX: Duration = Duration::from_secs(2);
/// First pause after a Busy reply; it doubles with every attempt (see `busy_pause`).
pub const BUSY_BASE: Duration = Duration::from_millis(50);
/// Jitter added to a Busy pause: up to this share of the pause, in percent.
pub const BUSY_JITTER_PCT: u64 = 25;

/// How long to wait after attempt `attempt` (1-based) got Busy before
/// resending with `attempt + 1`: min(50 ms × 2^(attempt−1), timeout), plus
/// 0–25 % jitter so clients turned away together don't come back together.
/// `salt` picks the jitter (per request and attempt); it is a hash, so a run
/// with the same seed retries at the same times.
pub fn busy_pause(attempt: u32, timeout: Duration, salt: u64) -> Duration {
    let doubled = BUSY_BASE.saturating_mul(1u32 << attempt.saturating_sub(1).min(20));
    let base = doubled.min(timeout);
    let per_mille = mix(salt) % (BUSY_JITTER_PCT * 10 + 1); // 0..=250 ‰
    base + base * per_mille as u32 / 1000
}

/// What happened to one request.
#[derive(Clone, Debug)]
pub struct Outcome {
    /// Time until success or retry exhaustion; failures are not zero latency.
    pub elapsed: Duration,
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
            Message::PlanResponse {
                request_id, epoch, ..
            } => {
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
    links: Vec<mpsc::Sender<Message>>,
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
                let (tx, rx) = mpsc::channel(4096);
                let timeout = Duration::from_millis(cfg.request_timeout_ms);
                tokio::spawn(link_task(addr.clone(), timeout, rx, dispatch.clone()));
                tx
            })
            .collect();
        Client {
            cfg,
            client_id: Uuid::new_v4(),
            seq: AtomicU64::new(0),
            links,
            dispatch,
        }
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
        let request_id = RequestId {
            client_id: self.client_id,
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.dispatch.pending.lock().unwrap().insert(request_id, tx);

        let timeout = Duration::from_millis(self.cfg.request_timeout_ms);
        let sent_at_ms = unix_ms();
        let start = Instant::now();
        let mut out = Outcome {
            elapsed: Duration::ZERO,
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
                if link.try_send(msg.clone()).is_err() {
                    self.dispatch.unsent.fetch_add(1, Ordering::Relaxed);
                }
            }
            let deadline = tokio::time::Instant::now() + timeout;
            let busy = loop {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(Message::PlanResponse {
                        served_by,
                        epoch,
                        degraded,
                        plan,
                        ..
                    })) => {
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
            // A Busy retry still raises the attempt number, so a server that
            // stays overloaded can be replaced (subject to min_tenure_ms).
            if busy && attempt < self.cfg.max_attempts {
                let salt = self.client_id.as_u64_pair().0
                    ^ request_id.seq.rotate_left(17)
                    ^ attempt as u64;
                tokio::time::sleep(busy_pause(attempt, timeout, salt)).await;
            }
        }
        out.elapsed = start.elapsed();
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
async fn link_task(
    addr: String,
    timeout: Duration,
    mut rx: mpsc::Receiver<Message>,
    dispatch: Arc<Dispatch>,
) {
    let mut writer: Option<(
        common::rr::WriteHalf,
        Arc<AtomicBool>,
        tokio::task::JoinHandle<()>,
    )> = None;
    let mut backoff = BACKOFF_MIN;
    let mut next_try = Instant::now();
    while let Some(msg) = rx.recv().await {
        if let Message::SubmitIncident { request_id, .. } = &msg {
            if !dispatch.pending.lock().unwrap().contains_key(request_id) {
                continue;
            }
        }
        if writer
            .as_ref()
            .is_some_and(|(_, alive, _)| !alive.load(Ordering::Relaxed))
        {
            if let Some((_, _, reader)) = writer.take() {
                reader.abort();
            }
        }
        if writer.is_none() {
            if Instant::now() < next_try {
                dispatch.unsent.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            match tokio::time::timeout(timeout, Stream::connect(&addr)).await {
                Ok(Ok(stream)) => {
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
        match tokio::time::timeout(timeout, write_frame(wr, &msg)).await {
            Ok(Ok(n)) => dispatch.sent.add(msg.kind(), n),
            error => {
                tracing::debug!(%addr, ?error, "write failed; will reconnect");
                dispatch.unsent.fetch_add(1, Ordering::Relaxed);
                if let Some((_, _, reader)) = writer.take() {
                    reader.abort();
                }
            }
        }
    }
    if let Some((_, _, reader)) = writer {
        reader.abort();
    }
}

async fn read_loop(mut rd: common::rr::ReadHalf, alive: Arc<AtomicBool>, dispatch: Arc<Dispatch>) {
    while let Ok(Some((msg, n))) = read_frame(&mut rd).await {
        dispatch.received.add(msg.kind(), n);
        dispatch.deliver(msg);
    }
    alive.store(false, Ordering::Relaxed);
}

/// Asks one node for its status over a short-lived connection.
pub async fn query_status(addr: &str, timeout: Duration) -> anyhow::Result<NodeStatus> {
    let fut = async {
        let mut s = Stream::connect(addr).await?;
        write_frame(&mut s, &Message::StatusRequest).await?;
        loop {
            match read_frame(&mut s).await? {
                Some((Message::Status(st), _)) => return Ok(*st),
                Some(_) => continue,
                None => anyhow::bail!("connection closed before Status"),
            }
        }
    };
    tokio::time::timeout(timeout, fut).await?
}

/// Asks one node for a cheap load sample (no results digest); the stress
/// harness polls this during runs.
pub async fn query_probe(addr: &str, timeout: Duration) -> anyhow::Result<NodeProbe> {
    let fut = async {
        let mut s = Stream::connect(addr).await?;
        write_frame(&mut s, &Message::ProbeRequest).await?;
        loop {
            match read_frame(&mut s).await? {
                Some((Message::Probe(p), _)) => return Ok(p),
                Some(_) => continue,
                None => anyhow::bail!("connection closed before Probe"),
            }
        }
    };
    tokio::time::timeout(timeout, fut).await?
}

/// Demo control only; ordinary workload clients never need to call this.
pub async fn inject_failure(
    addr: &str,
    duration_ms: u64,
    timeout: Duration,
) -> anyhow::Result<NodeId> {
    anyhow::ensure!(
        (1..=20_000).contains(&duration_ms),
        "failure duration must be 1..=20,000 ms"
    );
    tokio::time::timeout(timeout, async {
        let mut stream = Stream::connect(addr).await?;
        write_frame(&mut stream, &Message::InjectFailure { duration_ms }).await?;
        match read_frame(&mut stream).await? {
            Some((
                Message::FailureAccepted {
                    node_id,
                    duration_ms: accepted,
                },
                _,
            )) if accepted == duration_ms => Ok(node_id),
            Some((Message::FailureRejected { reason }, _)) => anyhow::bail!(reason),
            _ => anyhow::bail!("failure was not acknowledged"),
        }
    })
    .await?
}

/// Set or stop artificial CPU load on a configured lab server.
pub async fn set_load(
    addr: &str,
    threads: usize,
    duration_ms: u64,
    timeout: Duration,
) -> anyhow::Result<NodeId> {
    tokio::time::timeout(timeout, async {
        let mut stream = Stream::connect(addr).await?;
        write_frame(
            &mut stream,
            &Message::SetLoad {
                threads,
                duration_ms,
            },
        )
        .await?;
        match read_frame(&mut stream).await? {
            Some((
                Message::LoadAccepted {
                    node_id,
                    threads: got,
                    duration_ms: duration,
                },
                _,
            )) if got == threads && duration == duration_ms => Ok(node_id),
            Some((Message::LoadRejected { reason }, _)) => anyhow::bail!(reason),
            _ => anyhow::bail!("load was not acknowledged"),
        }
    })
    .await?
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busy_pause_doubles_caps_at_timeout_and_jitters_up_to_25_percent() {
        let timeout = Duration::from_millis(1000);
        for attempt in 1..=6u32 {
            let base = (50u64 << (attempt - 1)).min(1000);
            let mut seen = std::collections::BTreeSet::new();
            for salt in 0..500u64 {
                let p = busy_pause(attempt, timeout, salt).as_millis() as u64;
                assert!(
                    p >= base && p <= base + base / 4,
                    "attempt {attempt}: {p} ms vs base {base}"
                );
                seen.insert(p);
            }
            assert!(seen.len() > 1, "jitter varies with the salt");
        }
        // Same inputs, same pause.
        assert_eq!(busy_pause(3, timeout, 42), busy_pause(3, timeout, 42));
        // Huge attempt numbers don't overflow; the cap holds.
        assert!(busy_pause(u32::MAX, timeout, 7) <= timeout + timeout / 4);
        // Over the default 5 attempts the pauses add up to ≥ 750 ms (50+100+200+400),
        // most of a takeover backlog's drain time (≈1 s measured).
        let total: Duration = (1..5).map(|a| busy_pause(a, timeout, 0)).sum();
        assert!(total >= Duration::from_millis(750));
    }
}
