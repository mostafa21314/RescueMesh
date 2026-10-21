//! A RescueMesh cloud node.
//!
//! Process model (per node, one OS process):
//!   * tokio multi-threaded runtime for all network I/O;
//!   * one listener port shared by clients and peers (the first frame decides:
//!     `Hello` = peer link, anything else = client session);
//!   * one outbound link task per peer (reconnects with backoff, fed by a bounded
//!     channel);
//!   * a heartbeat task (load report to every peer), a prune task, an election GC
//!     task and a CPU calibration probe task;
//!   * CPU-bound planning runs on the blocking pool, bounded by a semaphore of
//!     `workers` permits. Requests waiting for a permit form the node's queue,
//!     which is what `queue_len` in the load report measures.
//!
//! Executor selection per request is either the load-bidding election
//! (`election.rs`, default) or the load-blind hash baseline (`assign.rs`).

use crate::assign::{hash_choose, AssignerKind};
use crate::election::{self, Elections, ServiceEstimator};
use crate::campus::{seed_resources, seed_responders, Campus};
use crate::model::*;
use crate::planner::{self, PlannerConfig};
use crate::proto::{LoadReport, Msg, StatusReport};
use crate::state::{ClusterState, Pending, ReplOp};
use crate::wire::{self, now_ms, Wire};
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, info, warn};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub id: NodeId,
    pub listen: String,
    pub peers: Vec<(NodeId, String)>,
    pub workers: usize,
    pub heartbeat_ms: u64,
    pub peer_timeout_ms: u64,
    pub seed: u64,
    pub campus_side: u32,
    pub responders: usize,
    pub resources: usize,
    pub planner: PlannerConfig,
    pub assigner: AssignerKind,
    /// Election: decide on the bids received so far after this long.
    pub bid_timeout_ms: u64,
    /// Election: service-time prior until the first completion.
    pub prior_service_ms: f64,
    /// Election: CPU calibration probe period.
    pub probe_ms: u64,
    /// Injected slowdown at startup (1.0 = none); also settable via `rm-client slow`.
    pub slow_factor: f64,
}

/// Artificial load injection: emulates a CPU `factor`x slower on this node. After any
/// timed CPU work (a plan or a probe) that took `t`, the thread sleeps `(factor-1)·t`
/// while still holding its planner permit. The election sees this only through real
/// measurement (longer service times, slower probes). Sleeping rather than spinning
/// keeps the co-located nodes of the one-machine testbed unaffected, as if node and
/// hog were on their own machine.
struct Slowdown {
    factor_milli: AtomicU64,
    until_ms: AtomicU64,
}

impl Slowdown {
    fn set(&self, factor: f64, secs: u64) {
        self.factor_milli.store((factor.max(1.0) * 1000.0).round() as u64, Relaxed);
        self.until_ms.store(if secs == 0 { 0 } else { now_ms() + secs * 1000 }, Relaxed);
    }

    fn factor(&self) -> f64 {
        let until = self.until_ms.load(Relaxed);
        if until != 0 && now_ms() >= until {
            return 1.0;
        }
        self.factor_milli.load(Relaxed) as f64 / 1000.0
    }

    fn stretch(&self, work: Duration) {
        let f = self.factor();
        if f > 1.0 {
            std::thread::sleep(work.mul_f64(f - 1.0));
        }
    }
}

/// RAII gauge: +1 while alive. Keeps `queue_len`/`inflight` exact on every exit path,
/// so the load metric cannot drift.
struct Gauge<'a>(&'a AtomicU32);

impl<'a> Gauge<'a> {
    fn new(c: &'a AtomicU32) -> Self {
        c.fetch_add(1, Relaxed);
        Gauge(c)
    }
}

impl Drop for Gauge<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

struct PeerHandle {
    tx: mpsc::Sender<Msg>,
    last_seen_ms: AtomicU64,
    load: Mutex<Option<LoadReport>>,
}

pub struct Node {
    pub cfg: NodeConfig,
    campus: Campus,
    state: Mutex<ClusterState>,
    peers: HashMap<NodeId, PeerHandle>,
    workers: Arc<Semaphore>,
    elections: Mutex<Elections>,
    estimator: Mutex<ServiceEstimator>,
    slow: Slowdown,
    /// Monotonic clock origin for election timings.
    t0: Instant,
    queue_len: AtomicU32,
    inflight: AtomicU32,
    completed: AtomicU64,
    executed_here: AtomicU64,
    dropped: AtomicU64,
    /// Requests being executed here -> client sessions waiting for the result
    /// (a retry arriving mid-execution joins the waiters instead of re-running).
    waiters: Mutex<HashMap<RequestId, Vec<mpsc::Sender<Msg>>>>,
}

pub async fn run(cfg: NodeConfig) -> anyhow::Result<()> {
    let campus = Campus::generate(cfg.seed, cfg.campus_side);
    let state = ClusterState::new(
        seed_responders(cfg.seed, &campus, cfg.responders),
        seed_resources(cfg.seed, &campus, cfg.resources),
    );

    let mut peers = HashMap::new();
    let mut links = Vec::new();
    for (pid, addr) in &cfg.peers {
        let (tx, rx) = mpsc::channel(4096);
        peers.insert(*pid, PeerHandle { tx, last_seen_ms: AtomicU64::new(0), load: Mutex::new(None) });
        links.push((*pid, addr.clone(), rx));
    }

    let node = Arc::new(Node {
        workers: Arc::new(Semaphore::new(cfg.workers)),
        cfg: cfg.clone(),
        campus,
        state: Mutex::new(state),
        peers,
        elections: Mutex::new(Elections::new(cfg.id)),
        estimator: Mutex::new(ServiceEstimator::new(cfg.prior_service_ms * 1000.0)),
        slow: Slowdown { factor_milli: AtomicU64::new(1000), until_ms: AtomicU64::new(0) },
        t0: Instant::now(),
        queue_len: AtomicU32::new(0),
        inflight: AtomicU32::new(0),
        completed: AtomicU64::new(0),
        executed_here: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
        waiters: Mutex::new(HashMap::new()),
    });

    for (pid, addr, rx) in links {
        tokio::spawn(peer_link(cfg.id, pid, addr, rx));
    }
    tokio::spawn(heartbeat_loop(node.clone()));
    tokio::spawn(prune_loop(node.clone()));
    node.slow.set(cfg.slow_factor, 0);
    if cfg.assigner == AssignerKind::Election {
        tokio::spawn(probe_loop(node.clone()));
        tokio::spawn(election_gc_loop(node.clone()));
    }

    let listener = TcpListener::bind(&cfg.listen).await?;
    info!(
        node = cfg.id,
        listen = %cfg.listen,
        workers = cfg.workers,
        assigner = cfg.assigner.name(),
        "cloud node up"
    );
    loop {
        let (stream, addr) = listener.accept().await?;
        let node = node.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(node, stream).await {
                debug!(%addr, "connection closed: {e}");
            }
        });
    }
}

/// Outbound link to one peer. Messages queue in `rx` while disconnected; the
/// channel is bounded and senders use try_send, so a long outage drops messages
/// (counted in `dropped`) instead of blocking request handling.
async fn peer_link(me: NodeId, peer: NodeId, addr: String, mut rx: mpsc::Receiver<Msg>) {
    let mut backoff = Duration::from_millis(100);
    loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                backoff = Duration::from_millis(100);
                let mut w = wire::framed(stream);
                if wire::send(&mut w, &Msg::Hello { node: me }).await.is_err() {
                    continue;
                }
                info!(peer, %addr, "peer link connected");
                while let Some(msg) = rx.recv().await {
                    if let Err(e) = wire::send(&mut w, &msg).await {
                        warn!(peer, "peer link lost: {e}");
                        break;
                    }
                }
                if rx.is_closed() {
                    return;
                }
            }
            Err(_) => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        }
    }
}

async fn handle_conn(node: Arc<Node>, stream: TcpStream) -> anyhow::Result<()> {
    let mut w = wire::framed(stream);
    let first = match wire::recv(&mut w).await? {
        Some(m) => m,
        None => return Ok(()),
    };
    match first {
        Msg::Hello { node: peer } => peer_inbound(node, peer, w).await,
        other => client_session(node, w, other).await,
    }
}

async fn peer_inbound(node: Arc<Node>, peer: NodeId, mut w: Wire) -> anyhow::Result<()> {
    info!(peer, "peer inbound connected");
    while let Some(msg) = wire::recv(&mut w).await? {
        if let Some(h) = node.peers.get(&peer) {
            h.last_seen_ms.store(now_ms(), Relaxed);
        }
        match msg {
            Msg::Heartbeat { node: from, load } => {
                if let Some(h) = node.peers.get(&from) {
                    *h.load.lock() = Some(load);
                }
            }
            Msg::Bid { request_id, attempt, node: from, score_us } => node.on_peer_bid(request_id, attempt, from, score_us),
            Msg::Replicate { from, op } => {
                let id = op.op_id().to_string();
                if node.state.lock().apply(op) {
                    debug!(from, op = %id, "applied replicated op");
                }
            }
            other => warn!(peer, "unexpected peer message: {other:?}"),
        }
    }
    Ok(())
}

async fn client_session(node: Arc<Node>, w: Wire, first: Msg) -> anyhow::Result<()> {
    let (mut sink, mut stream) = w.split();
    let (tx, mut rx) = mpsc::channel::<Msg>(1024);
    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if wire::send(&mut sink, &m).await.is_err() {
                break;
            }
        }
    });

    let mut next = Some(first);
    loop {
        let msg = match next.take() {
            Some(m) => m,
            None => match wire::recv(&mut stream).await? {
                Some(m) => m,
                None => break,
            },
        };
        match msg {
            Msg::Submit { request_id, client_id, incident } => {
                let node = node.clone();
                let tx = tx.clone();
                tokio::spawn(async move { node.handle_submit(request_id, client_id, incident, tx).await });
            }
            Msg::StatusQuery => {
                let _ = tx.send(Msg::Status(node.status())).await;
            }
            Msg::Slow { factor, secs } => {
                node.slow.set(factor, secs);
                info!(factor, secs, "slowdown injected");
                let _ = tx.send(Msg::Ack { message: format!("node {} slow factor {factor} for {secs}s", node.cfg.id) }).await;
            }
            other => {
                let _ = tx
                    .send(Msg::Error { request_id: None, message: format!("unexpected message: {other:?}") })
                    .await;
            }
        }
    }
    drop(tx);
    let _ = writer.await;
    Ok(())
}

impl Node {
    /// Sorted list of nodes considered alive (always includes self).
    pub fn live_nodes(&self) -> Vec<NodeId> {
        let now = now_ms();
        let mut live: Vec<NodeId> = self
            .peers
            .iter()
            .filter(|(_, h)| now.saturating_sub(h.last_seen_ms.load(Relaxed)) <= self.cfg.peer_timeout_ms)
            .map(|(id, _)| *id)
            .collect();
        live.push(self.cfg.id);
        live.sort();
        live
    }

    fn mono_us(&self) -> u64 {
        self.t0.elapsed().as_micros() as u64
    }

    /// Requests this node has committed to but not finished: waiting for a worker,
    /// being planned, or won in an election and not yet enqueued.
    fn outstanding(&self) -> u32 {
        self.queue_len.load(Relaxed) + self.inflight.load(Relaxed) + self.elections.lock().reserved()
    }

    /// The bid: estimated completion time (µs) of one more request here, now.
    fn score_now(&self) -> u64 {
        let est = self.estimator.lock().estimate_us();
        election::score_us(est, self.outstanding(), self.cfg.workers as u32)
    }

    fn load_report(&self) -> LoadReport {
        let (est, ratio) = {
            let e = self.estimator.lock();
            (e.estimate_us(), e.probe_ratio())
        };
        // Locals, not inline: a lock guard temporary inside the struct literal would
        // still be held while score_now() locks the same mutex.
        let reserved = self.elections.lock().reserved();
        let score_us = self.score_now();
        LoadReport {
            queue_len: self.queue_len.load(Relaxed),
            inflight: self.inflight.load(Relaxed),
            workers: self.cfg.workers as u32,
            reserved,
            completed: self.completed.load(Relaxed),
            ewma_service_ms: est / 1000.0,
            probe_ratio: ratio,
            score_us,
            slow_factor: self.slow.factor(),
            sent_ms: now_ms(),
        }
    }

    /// Queue `msg` to every peer link; returns how many accepted it.
    fn broadcast(&self, msg: Msg) -> usize {
        let mut sent = 0;
        for h in self.peers.values() {
            if h.tx.try_send(msg.clone()).is_err() {
                self.dropped.fetch_add(1, Relaxed);
            } else {
                sent += 1;
            }
        }
        sent
    }

    pub fn status(&self) -> StatusReport {
        let now = now_ms();
        let load = self.load_report();
        let election = self.elections.lock().stats();
        let st = self.state.lock();
        StatusReport {
            node: self.cfg.id,
            version: st.version,
            active_incidents: st.incidents.values().filter(|i| i.is_active(now)).count(),
            plans: st.plans.len(),
            plans_digest: format!("{:016x}", st.plans_digest),
            pending: st.pending.len(),
            executed_here: self.executed_here.load(Relaxed),
            load,
            live: self.live_nodes(),
            assigner: self.cfg.assigner.name().to_string(),
            dropped_peer_msgs: self.dropped.load(Relaxed),
            election,
            duplicate_executions: st.duplicate_executions,
        }
    }

    async fn handle_submit(self: Arc<Self>, rid: RequestId, client: String, incident: Incident, reply: mpsc::Sender<Msg>) {
        let me = self.cfg.id;

        // Idempotency: an already-committed request is answered from the cache by
        // the node that executed it; the others stay silent (and do not bid).
        if self.reply_if_committed(&rid, &reply).await {
            return;
        }

        self.state.lock().observe_incident(&incident);
        let executor = match self.cfg.assigner {
            AssignerKind::Hash => hash_choose(&rid, &self.live_nodes()),
            AssignerKind::Election => self.elect(&rid).await,
        };
        if executor != me {
            debug!(request = %rid, executor, "not elected; tracking as pending");
            let mut st = self.state.lock();
            // The executor's commit may already have arrived while we were deciding.
            if !st.plans.contains_key(&rid) {
                st.pending.insert(rid, Pending { executor, attempt: 0, received_ms: now_ms(), incident });
            }
            return;
        }
        self.execute(rid, client, incident, reply).await;
    }

    /// Answer from the committed-plan cache if this node executed the request.
    /// Returns true if the request is already committed (by anyone).
    async fn reply_if_committed(&self, rid: &RequestId, reply: &mpsc::Sender<Msg>) -> bool {
        let cached = self.state.lock().plans.get(rid).cloned();
        match cached {
            Some(plan) => {
                if plan.executed_by == self.cfg.id {
                    let _ = reply.send(Msg::PlanResult { request_id: rid.clone(), plan, duplicate: true }).await;
                }
                true
            }
            None => false,
        }
    }

    /// One election round for `rid` (attempt 0): bid, then wait for the full bid set
    /// or the bid timeout. A repeated Submit (client retry) joins the same round.
    async fn elect(&self, rid: &RequestId) -> NodeId {
        let now = self.mono_us();
        let score = self.score_now();
        let live = self.live_nodes();
        let (fresh, decided, rx) = {
            let mut el = self.elections.lock();
            let fresh = el.local_bid(rid, 0, score, now);
            let decided = el.decision(rid).or_else(|| el.try_decide(rid, &live, now));
            let rx = if decided.is_none() { el.subscribe(rid) } else { None };
            (fresh, decided, rx)
        };
        if fresh {
            let msg = Msg::Bid { request_id: rid.clone(), attempt: 0, node: self.cfg.id, score_us: score };
            let bytes = serde_json::to_vec(&msg).map(|v| v.len() + 4).unwrap_or(0);
            let sent = self.broadcast(msg);
            self.elections.lock().note_sent(sent, sent * bytes);
        }
        if let Some(w) = decided {
            return w;
        }
        if let Some(rx) = rx {
            if let Ok(Ok(w)) = tokio::time::timeout(Duration::from_millis(self.cfg.bid_timeout_ms), rx).await {
                return w;
            }
        }
        let now = self.mono_us();
        let mut el = self.elections.lock();
        // None only if the round was garbage-collected, which takes seconds.
        el.decide_on_timeout(rid, now).unwrap_or_else(|| hash_choose(rid, &live))
    }

    fn on_peer_bid(&self, rid: RequestId, attempt: u32, from: NodeId, score_us: u64) {
        if self.cfg.assigner != AssignerKind::Election {
            return;
        }
        let committed = self.state.lock().plans.contains_key(&rid);
        let live = self.live_nodes();
        let now = self.mono_us();
        let mut el = self.elections.lock();
        if committed {
            el.note_late();
            return;
        }
        el.peer_bid(&rid, attempt, from, score_us, now);
        if let Some(w) = el.try_decide(&rid, &live, now) {
            debug!(request = %rid, winner = w, "elected on full bid set");
        }
    }

    async fn execute(self: Arc<Self>, rid: RequestId, client: String, incident: Incident, reply: mpsc::Sender<Msg>) {
        let me = self.cfg.id;

        // Duplicate arriving while we are still computing: wait for the same result.
        // If it finished meanwhile (or a duplicate executor's plan already arrived),
        // answer from the cache instead of planning again.
        let committed = {
            let mut w = self.waiters.lock();
            if let Some(v) = w.get_mut(&rid) {
                v.push(reply);
                return;
            }
            let committed = self.state.lock().plans.contains_key(&rid);
            if !committed {
                w.insert(rid.clone(), vec![reply.clone()]);
            }
            committed
        };
        if committed {
            self.elections.lock().start_execution(&rid);
            self.reply_if_committed(&rid, &reply).await;
            return;
        }
        drop(reply);

        let queued = Gauge::new(&self.queue_len);
        self.elections.lock().start_execution(&rid);
        let permit = self.workers.clone().acquire_owned().await.expect("semaphore closed");
        drop(queued);
        let inflight = Gauge::new(&self.inflight);

        let snap = self.state.lock().snapshot(now_ms());
        let node = self.clone();
        let (inc2, rid2) = (incident.clone(), rid.clone());
        let res = tokio::task::spawn_blocking(move || {
            let t = Instant::now();
            let mut plan = planner::plan(&node.cfg.planner, &node.campus, &snap, &inc2, &rid2, me);
            node.slow.stretch(t.elapsed());
            // Service time as the client and the load metric see it (incl. injected slowdown).
            plan.compute_us = t.elapsed().as_micros() as u64;
            plan
        })
        .await;
        drop(permit);
        drop(inflight);
        let plan = match res {
            Ok(p) => p,
            Err(e) => {
                warn!(request = %rid, "planner failed: {e}");
                let txs = self.waiters.lock().remove(&rid).unwrap_or_default();
                for tx in txs {
                    let _ = tx.send(Msg::Error { request_id: Some(rid.clone()), message: "planner failed".into() }).await;
                }
                return;
            }
        };
        self.completed.fetch_add(1, Relaxed);
        self.executed_here.fetch_add(1, Relaxed);
        self.estimator.lock().on_completion(plan.compute_us as f64, self.mono_us());

        let plan = Arc::new(plan);
        let op = ReplOp::PlanCommitted {
            op_id: format!("plan:{rid}"),
            attempt: 0,
            incident: Arc::new(incident),
            plan: plan.clone(),
        };
        self.state.lock().apply(op.clone());
        self.broadcast(Msg::Replicate { from: me, op });
        debug!(request = %rid, client = %client, compute_us = plan.compute_us, "plan committed");

        let txs = self.waiters.lock().remove(&rid).unwrap_or_default();
        for (i, tx) in txs.into_iter().enumerate() {
            let _ = tx
                .send(Msg::PlanResult { request_id: rid.clone(), plan: plan.clone(), duplicate: i > 0 })
                .await;
        }
    }
}

/// Fixed CPU micro-benchmark (~0.3 ms at full speed). Median of three runs, so one
/// preemption does not register as a slowdown but sustained contention does.
fn probe_us(slow: &Slowdown) -> f64 {
    let mut runs = [0.0f64; 3];
    for r in runs.iter_mut() {
        let t = Instant::now();
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for i in 0..300_000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x = x.wrapping_add(i);
        }
        std::hint::black_box(x);
        slow.stretch(t.elapsed());
        *r = t.elapsed().as_secs_f64() * 1e6;
    }
    runs.sort_by(f64::total_cmp);
    runs[1]
}

async fn probe_loop(node: Arc<Node>) {
    let mut tick = tokio::time::interval(Duration::from_millis(node.cfg.probe_ms));
    loop {
        tick.tick().await;
        let n = node.clone();
        if let Ok(us) = tokio::task::spawn_blocking(move || probe_us(&n.slow)).await {
            node.estimator.lock().on_probe(us, node.mono_us());
        }
        // Cold start: borrow the peers' work-per-request estimate (heartbeat data is
        // stale, but this only seeds a prior) so a node that has not won anything yet
        // does not bid with an arbitrary default.
        let peer_work = node
            .peers
            .values()
            .filter_map(|h| h.load.lock().clone())
            .filter(|l| l.completed >= 5 && l.probe_ratio > 0.0)
            .map(|l| l.ewma_service_ms * 1000.0 / l.probe_ratio)
            .min_by(f64::total_cmp);
        if let Some(w) = peer_work {
            node.estimator.lock().seed_prior(w);
        }
    }
}

async fn election_gc_loop(node: Arc<Node>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        // Decided rounds: 10 s (covers late bids and client retries); orphans: 30 s.
        node.elections.lock().gc(node.mono_us(), 10_000_000, 30_000_000);
    }
}

async fn heartbeat_loop(node: Arc<Node>) {
    let mut tick = tokio::time::interval(Duration::from_millis(node.cfg.heartbeat_ms));
    loop {
        tick.tick().await;
        node.broadcast(Msg::Heartbeat { node: node.cfg.id, load: node.load_report() });
    }
}

async fn prune_loop(node: Arc<Node>) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        node.state.lock().prune(now_ms());
    }
}
