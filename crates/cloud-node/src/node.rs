//! A cloud node: accepts client and peer connections on one port, records
//! every request, runs elections (see `election`), plans the requests it
//! serves on the worker pool, replies, and replicates results to its peers.

use crate::campus::Campus;
use crate::election::{self, Action, Bid, Core, Outcome, Record};
use crate::ledger::Ledger;
use crate::load::LoadMonitor;
use crate::peers::PeerLinks;
use crate::planner;
use crate::pool::Pool;
use crate::tables::{Offer, Response, Tables};
use common::config::{ElectionMode, NodeConfig, ReadyRule, SnapshotBuild, Transfer};
use common::counters::Counters;
use common::wire::{read_frame, write_frame};
use common::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

/// How often expired table entries are swept.
const SWEEP_EVERY: Duration = Duration::from_secs(1);
/// Planning attempts on a fresh snapshot before planning under the ledger lock.
const OPTIMISTIC_PLANS: u32 = 2;
pub const MAX_FAILURE: Duration = Duration::from_secs(20);

type Reply = mpsc::UnboundedSender<Message>;

struct Job {
    generation: u64,
    request_id: RequestId,
    incident: Incident,
    complexity: u32,
    /// Epoch and degraded flag when the job was queued; the response carries them.
    epoch: Epoch,
    degraded: bool,
}

struct Done {
    job: Job,
    plan: Option<Plan>,
}

struct State {
    phase: NodePhase,
    faults_injected: u64,
    last_recovery_ms: Option<u64>,
    last_recovery: Option<RecoveryReport>,
    /// When the current or last outage started; a delta transfer asks for
    /// what changed since then.
    muted_at: Option<Instant>,
    recovery_epoch: Epoch,
    tables: Tables,
    core: Core,
    /// The connection each request last arrived on; the node answers there
    /// when it finishes the request, including after a takeover.
    reply_to: HashMap<RequestId, Reply>,
    /// Requests queued or running on this node's pool.
    inflight: HashSet<RequestId>,
    /// Per-request mode: elections won before the client's message arrived.
    pending_wins: HashSet<RequestId>,
}

#[derive(Default)]
struct Stats {
    sent: Arc<Counters>,
    received: Counters,
    replicate_conflicts: AtomicU64,
    duplicate_replicates: AtomicU64,
    local_replans: Arc<AtomicU64>,
    resource_overcommits: AtomicU64,
    late_results: AtomicU64,
    dropped_on_step_down: AtomicU64,
    snapshots_served: AtomicU64,
    snapshot_lock_max_us: AtomicU64,
}

struct Shared {
    campus: Arc<Campus>,
    generation: Arc<AtomicU64>,
    stopped: AtomicBool,
    cfg: NodeConfig,
    ledger: Arc<Mutex<Ledger>>,
    state: Mutex<State>,
    pool: Pool<Job>,
    load: Arc<LoadMonitor>,
    peers: Arc<PeerLinks>,
    stats: Stats,
}

pub struct NodeHandle {
    shared: Arc<Shared>,
    addr: SocketAddr,
    tasks: Vec<JoinHandle<()>>,
}

impl NodeHandle {
    /// Silences application traffic, then actively recovers before serving again.
    pub fn inject_failure(&self, duration: Duration) -> anyhow::Result<()> {
        inject_failure(&self.shared, duration)
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn node_id(&self) -> NodeId {
        self.shared.cfg.node_id
    }

    pub fn results_snapshot(&self) -> BTreeMap<RequestId, Response> {
        self.shared.state.lock().unwrap().tables.results_snapshot()
    }

    pub fn status(&self) -> NodeStatus {
        status(&self.shared)
    }

    pub fn ledger_snapshot(&self, now_ms: u64) -> crate::ledger::Snapshot {
        self.shared.ledger.lock().unwrap().snapshot(now_ms)
    }

    /// Diagnostic: this node's view of one request (for test failure output).
    pub fn describe_request(&self, request_id: &RequestId) -> String {
        let st = self.shared.state.lock().unwrap();
        format!(
            "node {} epoch {} server {:?} degraded {} electing {}: entry {:?} result {:?} inflight {} reply_to {}",
            self.shared.cfg.node_id,
            st.core.epoch(),
            st.core.server(),
            st.core.degraded(),
            st.core.electing(),
            st.tables.request(request_id).map(|e| (e.last_attempt, e.last_epoch, e.completed)),
            st.tables.result(request_id).map(|r| (r.served_by, r.epoch)),
            st.inflight.contains(request_id),
            st.reply_to.contains_key(request_id),
        )
    }

    /// Stops accepting, closes every connection and peer link, and joins the
    /// planner threads after their current job. Blocks; from async code call
    /// it via `spawn_blocking`.
    pub fn shutdown(self) {
        self.shared.stopped.store(true, Ordering::SeqCst);
        self.shared.generation.fetch_add(1, Ordering::SeqCst);
        for t in &self.tasks {
            t.abort();
        }
        self.shared.load.stop();
        self.shared.pool.shutdown();
    }
}

/// Binds `cfg.listen` and starts the node.
pub async fn start(cfg: NodeConfig) -> anyhow::Result<NodeHandle> {
    let listener = TcpListener::bind(&cfg.listen).await?;
    start_with_listener(cfg, listener).await
}

pub async fn start_with_listener(
    cfg: NodeConfig,
    listener: TcpListener,
) -> anyhow::Result<NodeHandle> {
    cfg.validate()?;
    let addr = listener.local_addr()?;
    let campus = Arc::new(Campus::generate(cfg.seed, cfg.campus));
    let ledger = Arc::new(Mutex::new(Ledger::new(&campus)));
    let load = Arc::new(LoadMonitor::start(cfg.cpu_source));
    let stats = Stats::default();
    let (peers, mut tasks) = PeerLinks::spawn(&cfg.peers, stats.sent.clone());
    let (done_tx, done_rx) = mpsc::unbounded_channel();
    let generation = Arc::new(AtomicU64::new(0));

    let pool = {
        let (campus, ledger, load) = (campus.clone(), ledger.clone(), load.clone());
        let replans = stats.local_replans.clone();
        let generation = generation.clone();
        Pool::new(cfg.workers, cfg.queue_capacity, move |job: Job| {
            let started = Instant::now();
            let plan = plan_and_commit(&campus, &ledger, &replans, &job, &generation);
            load.record_service(started.elapsed(), planner::expected_cost(job.complexity));
            let _ = done_tx.send(Done { job, plan });
        })
    };

    let core = Core::new(election::Params {
        me: cfg.node_id,
        cluster_size: cfg.peers.len() + 1,
        mode: cfg.election_mode,
        policy: cfg.policy,
        bid_timeout: Duration::from_millis(cfg.bid_timeout_ms),
        degraded_after: Duration::from_millis(cfg.degraded_after_ms),
        min_tenure: Duration::from_millis(cfg.min_tenure_ms),
    });
    let shared = Arc::new(Shared {
        campus: campus.clone(),
        generation,
        stopped: AtomicBool::new(false),
        state: Mutex::new(State {
            phase: if cfg.recovery.on_start {
                NodePhase::Recovering
            } else {
                NodePhase::Ready
            },
            faults_injected: 0,
            last_recovery_ms: None,
            last_recovery: None,
            muted_at: None,
            recovery_epoch: 0,
            tables: Tables::default(),
            core,
            reply_to: HashMap::new(),
            inflight: HashSet::new(),
            pending_wins: HashSet::new(),
        }),
        cfg,
        ledger,
        pool,
        load,
        peers,
        stats,
    });

    // No server is known at startup. The first request triggers the
    // election (trigger "no_server"), rather than boot, when peers are often
    // not up yet and the election would hear only its own bid.
    // Temporary outages use the pull-recovery path below. With
    // `recovery.on_start`, a (re)started process uses it too before it serves;
    // without it, a restarted node starts Ready and empty (docs/recovery.md, R5).
    // TODO(discovery): peers come from the config file for now.
    tasks.push(tokio::spawn(accept_loop(shared.clone(), listener)));
    tasks.push(tokio::spawn(completion_loop(shared.clone(), done_rx)));
    tasks.push(tokio::spawn(sweep_loop(shared.clone())));
    if shared.cfg.recovery.on_start {
        tasks.push(tokio::spawn(recover(shared.clone(), Cause::Start)));
    }
    tracing::info!(
        node = shared.cfg.node_id,
        %addr,
        workers = shared.cfg.workers,
        mode = ?shared.cfg.election_mode,
        policy = ?shared.cfg.policy,
        cpu_source = ?shared.cfg.cpu_source,
        zones = campus.zones(),
        responders = campus.responders.len(),
        "node started"
    );
    Ok(NodeHandle {
        shared,
        addr,
        tasks,
    })
}

fn inject_failure(shared: &Arc<Shared>, duration: Duration) -> anyhow::Result<()> {
    anyhow::ensure!(
        !duration.is_zero() && duration <= MAX_FAILURE,
        "failure duration must be 1..=20,000 ms"
    );
    let mut st = shared.state.lock().unwrap();
    anyhow::ensure!(
        st.phase == NodePhase::Ready && !shared.stopped.load(Ordering::SeqCst),
        "node is not ready"
    );
    st.phase = NodePhase::Muted;
    st.faults_injected += 1;
    st.last_recovery_ms = None;
    st.muted_at = Some(Instant::now());
    st.recovery_epoch = st.core.epoch();
    shared.generation.fetch_add(1, Ordering::SeqCst);
    shared.peers.suspend();
    shared.pool.drop_queued();
    st.inflight.clear();
    st.reply_to.clear();
    st.pending_wins.clear();
    tracing::warn!(
        node = shared.cfg.node_id,
        duration_ms = duration.as_millis(),
        "temporary failure started"
    );
    let sh = shared.clone();
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        if sh.stopped.load(Ordering::SeqCst) {
            return;
        }
        sh.state.lock().unwrap().phase = NodePhase::Recovering;
        recover(sh, Cause::Mute).await;
    });
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cause {
    /// Rejoining after a temporary failure: state is in memory, but stale.
    Mute,
    /// The process just started: state is empty.
    Start,
}

impl Cause {
    fn as_str(self) -> &'static str {
        match self {
            Cause::Mute => "mute",
            Cause::Start => "start",
        }
    }
}

/// What one round of transfers brought back.
#[derive(Default)]
struct Round {
    snapshots: Vec<crate::recovery::Snapshot>,
    /// Peers that answered but are not Ready, with and without state.
    unavailable_with_state: usize,
    unavailable_empty: usize,
}

/// Pulls snapshots from the peers. Both rules ask both survivors at once;
/// `Majority` stops waiting `majority_wait_ms` after the first usable one.
async fn transfer_round(shared: &Arc<Shared>, since_ms: Option<u64>) -> Round {
    let rp = shared.cfg.recovery;
    let (tx, mut rx) = mpsc::unbounded_channel();
    for p in &shared.cfg.peers {
        let (tx, addr, id, me) = (tx.clone(), p.addr.clone(), p.id, shared.cfg.node_id);
        tokio::spawn(async move {
            let _ = tx.send(crate::recovery::fetch(&addr, me, id, since_ms).await);
        });
    }
    drop(tx);
    let mut round = Round::default();
    let mut deadline: Option<tokio::time::Instant> = None;
    loop {
        let next = match deadline {
            Some(d) => match tokio::time::timeout_at(d, rx.recv()).await {
                Ok(n) => n,
                Err(_) => break, // majority: stopped waiting for the second
            },
            None => rx.recv().await,
        };
        let Some(fetched) = next else { break };
        match fetched {
            Ok(crate::recovery::Fetched::Snapshot(snap)) => {
                if rp.ready_rule == ReadyRule::Majority && !snap.electing && deadline.is_none() {
                    deadline = Some(
                        tokio::time::Instant::now() + Duration::from_millis(rp.majority_wait_ms),
                    );
                }
                round.snapshots.push(snap);
            }
            Ok(crate::recovery::Fetched::Unavailable { has_state: true }) => {
                round.unavailable_with_state += 1
            }
            Ok(crate::recovery::Fetched::Unavailable { has_state: false }) => {
                round.unavailable_empty += 1
            }
            Err(e) => tracing::debug!(%e, "recovery transfer failed"),
        }
    }
    round
}

/// The role view (epoch, server) to adopt from a round, or `None` if the
/// round does not meet the ready rule.
fn accepted_view(shared: &Shared, round: &Round) -> Option<(Epoch, Option<NodeId>)> {
    let valid = |s: &crate::recovery::Snapshot| {
        !s.electing
            && s.server.is_none_or(|id| {
                id == shared.cfg.node_id || shared.cfg.peers.iter().any(|p| p.id == id)
            })
    };
    match shared.cfg.recovery.ready_rule {
        // A temporary single-node failure leaves two ready survivors. Do not
        // resume on one arbitrary or still-electing survivor's stale view.
        // A peer that answered "starting, empty" holds nothing to agree on,
        // so it is left out; otherwise two nodes starting next to one Ready
        // node would each wait for the other forever.
        ReadyRule::BothAgree => {
            let needed = shared.cfg.peers.len() - round.unavailable_empty;
            let first = round.snapshots.first()?;
            (needed > 0
                && round.snapshots.len() == needed
                && round.snapshots.iter().all(|s| {
                    valid(s) && s.epoch == first.epoch && s.server == first.server
                }))
            .then_some((first.epoch, first.server))
        }
        // Itself plus one survivor is 2 of 3. Of the usable views, the
        // highest epoch is the most recent role decision.
        ReadyRule::Majority => round
            .snapshots
            .iter()
            .filter(|s| valid(s))
            .max_by_key(|s| (s.epoch, std::cmp::Reverse(s.server)))
            .map(|s| (s.epoch, s.server)),
    }
}

async fn recover(shared: Arc<Shared>, cause: Cause) {
    let rp = shared.cfg.recovery;
    let started = Instant::now();
    let mut report = RecoveryReport {
        cause: cause.as_str().into(),
        ..Default::default()
    };
    tracing::info!(
        node = shared.cfg.node_id,
        cause = cause.as_str(),
        ?rp,
        "recovery started; election participation disabled"
    );
    // Old planners must finish/cancel before replacing their derived ledger.
    while shared.pool.load().running > 0 {
        if shared.stopped.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    report.drain_ms = started.elapsed().as_millis() as u64;
    let boot_deadline = started + Duration::from_millis(rp.boot_wait_ms);
    loop {
        if shared.stopped.load(Ordering::SeqCst) {
            return;
        }
        // Delta: what changed since the outage began, plus a margin for replicas
        // that were in flight when it began. A process start has nothing to
        // build on, so it always takes everything.
        let muted_at = shared.state.lock().unwrap().muted_at;
        let since_ms = match (rp.transfer, cause, muted_at) {
            (Transfer::Delta, Cause::Mute, Some(t)) => {
                Some(t.elapsed().as_millis() as u64 + rp.delta_margin_ms)
            }
            _ => None,
        };
        report.rounds += 1;
        let fetch_started = Instant::now();
        let round = transfer_round(&shared, since_ms).await;
        report.fetch_ms = fetch_started.elapsed().as_millis() as u64;

        let view = accepted_view(&shared, &round);
        // Process start with no Ready peer: either the whole cluster is
        // starting (every peer answered, none has state) or no survivor is
        // reachable for boot_wait_ms. Nobody holds state we could fetch.
        let bootstrap = cause == Cause::Start
            && round.snapshots.is_empty()
            && ((round.unavailable_empty == shared.cfg.peers.len())
                || Instant::now() >= boot_deadline);
        if view.is_none() && !bootstrap {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let install_started = Instant::now();
        let mut st = shared.state.lock().unwrap();
        if shared.stopped.load(Ordering::SeqCst) {
            return;
        }
        let (epoch, server) = view.unwrap_or((st.core.epoch(), None));
        // A newer live replica can overtake either snapshot transfer.
        // Never install an older role view over that observed epoch.
        if epoch < st.recovery_epoch {
            drop(st);
            continue;
        }
        let now = Instant::now();
        // What we might hold that a survivor lacks: everything we hold (full),
        // or what changed shortly before the outage (delta). Taken before the
        // merge, which marks merged entries as changed now.
        let candidates =
            st.tables
                .recovery_keys(since_ms.map(|ms| {
                    now.checked_sub(Duration::from_millis(ms)).unwrap_or(started)
                }));
        let mut peer_versions = Vec::new();
        for snap in round.snapshots {
            report.snapshots_used += 1;
            report.records_received += snap.records.len() as u64;
            report.bytes_received += snap.bytes;
            let versions: HashMap<RequestId, (Epoch, NodeId)> = snap
                .records
                .iter()
                .filter_map(|r| r.result.as_ref().map(|x| (r.request_id, (x.epoch, x.served_by))))
                .collect();
            peer_versions.push((snap.peer, versions));
            report.new_results += st.tables.merge_recovery(snap.records, now).stored;
        }
        let mut ledger = Ledger::new(&shared.campus);
        for record in st.tables.recovery_records(None) {
            if let Some(result) = &record.result {
                ledger.apply(record.request_id, &result.plan, unix_ms());
            }
        }
        *shared.ledger.lock().unwrap() = ledger;
        st.core.recover_view(now, epoch, server);
        st.inflight.clear();
        st.phase = NodePhase::Ready;
        shared.peers.resume();
        // Also publish retained results that may have completed just before
        // the outage without reaching a survivor.
        for id in candidates {
            let Some(record) = st.tables.recovery_record(&id) else {
                continue;
            };
            let Some(r) = record.result else { continue };
            let message = Message::Replicate {
                epoch: r.epoch,
                origin: r.served_by,
                request_id: id,
                plan: r.plan,
                incident: record.incident,
                degraded: r.degraded,
            };
            for (peer, versions) in &peer_versions {
                if versions.get(&id) != Some(&(r.epoch, r.served_by)) {
                    shared.peers.send(*peer, message.clone());
                    report.republished += 1;
                }
            }
        }
        if st.core.serving() {
            run_actions(&shared, &mut st, vec![Action::BecameServer]);
        }
        report.bootstrap = view.is_none();
        report.install_ms = install_started.elapsed().as_millis() as u64;
        report.total_ms = started.elapsed().as_millis() as u64;
        st.last_recovery_ms = Some(report.total_ms);
        tracing::info!(
            node = shared.cfg.node_id,
            cause = cause.as_str(),
            total_ms = report.total_ms,
            drain_ms = report.drain_ms,
            fetch_ms = report.fetch_ms,
            install_ms = report.install_ms,
            rounds = report.rounds,
            records = report.records_received,
            bytes = report.bytes_received,
            new_results = report.new_results,
            republished = report.republished,
            bootstrap = report.bootstrap,
            results = st.tables.len().1,
            epoch = st.core.epoch(),
            "recovery complete; node ready"
        );
        st.last_recovery = Some(report);
        return;
    }
}

/// Answers a returning peer's RecoveryRequest on its connection.
fn serve_snapshot(shared: &Shared, since_ms: Option<u64>, reply: &Reply) {
    // Each hold is timed from acquiring the lock to releasing it.
    let lock = || {
        let st = shared.state.lock().unwrap();
        (st, Instant::now())
    };
    let mut max_hold = 0u64;
    let note = |t: Instant, max: &mut u64| *max = (*max).max(t.elapsed().as_micros() as u64);
    let since = |now: Instant| since_ms.and_then(|ms| now.checked_sub(Duration::from_millis(ms)));

    let (st, t) = lock();
    if st.phase != NodePhase::Ready {
        let _ = reply.send(Message::RecoveryUnavailable {
            node_id: shared.cfg.node_id,
            phase: st.phase,
            has_state: st.tables.len() != (0, 0),
        });
        return;
    }
    let count = match shared.cfg.recovery.snapshot {
        SnapshotBuild::Locked => {
            // Build and queue the whole snapshot in one hold of the lock.
            let records = st.tables.recovery_records(since(Instant::now()));
            let count = records.len();
            for chunk in records.chunks(64) {
                let _ = reply.send(Message::RecoveryChunk {
                    records: chunk.to_vec(),
                });
            }
            note(t, &mut max_hold);
            drop(st);
            count
        }
        SnapshotBuild::Chunked => {
            // Only the key list under the first hold; then 64 records per
            // hold, so request handling interleaves with the transfer. Entries
            // swept in between are skipped; entries changed in between are
            // sent as they are now, which the result rule makes safe.
            let keys = st.tables.recovery_keys(since(Instant::now()));
            note(t, &mut max_hold);
            drop(st);
            let mut count = 0;
            for chunk in keys.chunks(64) {
                let (st, t) = lock();
                let records: Vec<RecoveryRecord> =
                    chunk.iter().filter_map(|id| st.tables.recovery_record(id)).collect();
                note(t, &mut max_hold);
                drop(st);
                count += records.len();
                let _ = reply.send(Message::RecoveryChunk { records });
            }
            count
        }
    };
    // The role view is read last, so it is at least as new as the records.
    let (st, t) = lock();
    let _ = reply.send(Message::RecoveryComplete {
        node_id: shared.cfg.node_id,
        epoch: st.core.epoch(),
        server: st.core.server(),
        electing: st.core.electing(),
        records: count,
    });
    note(t, &mut max_hold);
    drop(st);
    shared.stats.snapshots_served.fetch_add(1, Ordering::Relaxed);
    shared
        .stats
        .snapshot_lock_max_us
        .fetch_max(max_hold, Ordering::Relaxed);
}

/// Plans on a snapshot and commits if nothing it chose was taken meanwhile;
/// after `OPTIMISTIC_PLANS` collisions, plans while holding the ledger lock.
/// Either way commits happen one at a time, so two workers never assign the
/// same responder.
fn plan_and_commit(
    campus: &Campus,
    ledger: &Mutex<Ledger>,
    replans: &AtomicU64,
    job: &Job,
    generation: &AtomicU64,
) -> Option<Plan> {
    for _ in 0..OPTIMISTIC_PLANS {
        let now = unix_ms();
        let snap = ledger.lock().unwrap().snapshot(now);
        let plan = planner::plan(campus, &snap, &job.incident, job.complexity, now);
        let mut l = ledger.lock().unwrap();
        if generation.load(Ordering::SeqCst) != job.generation {
            return None;
        }
        if l.can_commit(job.request_id, &plan, now) {
            l.apply(job.request_id, &plan, now);
            return Some(plan);
        }
        replans.fetch_add(1, Ordering::Relaxed);
    }
    let now = unix_ms();
    let mut l = ledger.lock().unwrap();
    if generation.load(Ordering::SeqCst) != job.generation {
        return None;
    }
    let plan = planner::plan(campus, &l.snapshot(now), &job.incident, job.complexity, now);
    if generation.load(Ordering::SeqCst) != job.generation {
        return None;
    }
    l.apply(job.request_id, &plan, now);
    Some(plan)
}

/// This node's load score, computed fresh.
fn score(shared: &Shared) -> u32 {
    let pending = shared.pool.load().pending_cost;
    shared
        .load
        .terms(pending, shared.cfg.workers)
        .score(shared.cfg.policy, &shared.cfg.weights)
}

async fn accept_loop(shared: Arc<Shared>, listener: TcpListener) {
    // Connection tasks live in the JoinSet, so aborting this task closes them.
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    if shared.state.lock().unwrap().phase == NodePhase::Muted { continue; }
                    let _ = stream.set_nodelay(true);
                    conns.spawn(serve_conn(shared.clone(), stream));
                }
                Err(e) => tracing::warn!(%e, "accept failed"),
            },
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
}

async fn serve_conn(shared: Arc<Shared>, stream: TcpStream) {
    let (mut rd, mut wr) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let writer = async {
        while let Some(m) = rx.recv().await {
            if shared.state.lock().unwrap().phase == NodePhase::Muted
                && !matches!(m, Message::FailureAccepted { .. })
            {
                continue;
            }
            match write_frame(&mut wr, &m).await {
                Ok(n) => shared.stats.sent.add(m.kind(), n),
                Err(_) => break,
            }
        }
    };
    let reader = async {
        loop {
            match read_frame(&mut rd).await {
                Ok(Some((msg, n))) => {
                    shared.stats.received.add(msg.kind(), n);
                    handle(&shared, msg, &tx);
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!(%e, "connection closed with error");
                    break;
                }
            }
        }
    };
    tokio::select! {
        _ = reader => {}
        _ = writer => {}
    }
}

fn handle(shared: &Arc<Shared>, msg: Message, reply: &Reply) {
    let phase = shared.state.lock().unwrap().phase;
    if phase == NodePhase::Muted {
        return;
    }
    if phase == NodePhase::Recovering
        && !matches!(
            msg,
            Message::Replicate { .. } | Message::StatusRequest | Message::RecoveryRequest { .. }
        )
    {
        return;
    }
    match msg {
        Message::InjectFailure { duration_ms } => {
            let response = match inject_failure(shared, Duration::from_millis(duration_ms)) {
                Ok(()) => Message::FailureAccepted {
                    node_id: shared.cfg.node_id,
                    duration_ms,
                },
                Err(error) => Message::FailureRejected {
                    reason: error.to_string(),
                },
            };
            let _ = reply.send(response);
        }
        Message::RecoveryRequest { node_id, since_ms } => {
            if shared.cfg.peers.iter().any(|p| p.id == node_id) {
                serve_snapshot(shared, since_ms, reply);
            }
        }
        Message::SubmitIncident {
            request_id,
            attempt,
            client_epoch,
            incident,
            complexity,
        } => on_submit(
            shared,
            request_id,
            attempt,
            client_epoch,
            incident,
            complexity,
            reply,
        ),
        Message::Replicate {
            epoch,
            origin,
            request_id,
            plan,
            incident,
            degraded,
        } => on_replicate(shared, epoch, origin, request_id, plan, incident, degraded),
        Message::ElectBid {
            target_epoch,
            node_id,
            score: s,
            incumbent,
            request_id,
            attempt,
        } => {
            shared.peers.heard_from(node_id);
            let mut guard = shared.state.lock().unwrap();
            let st = &mut *guard;
            if st.phase != NodePhase::Ready {
                return;
            }
            let bid = Bid {
                node_id,
                score: s,
                incumbent,
            };
            let acts = st.core.on_bid(
                Instant::now(),
                target_epoch,
                bid,
                request_id,
                attempt,
                &|| score(shared),
            );
            run_actions(shared, st, acts);
        }
        Message::StatusRequest => {
            let _ = reply.send(Message::Status(status(shared)));
        }
        other => tracing::debug!(kind = other.kind(), "ignoring message not meant for a node"),
    }
}

fn on_submit(
    shared: &Arc<Shared>,
    request_id: RequestId,
    attempt: u32,
    client_epoch: Epoch,
    incident: Incident,
    complexity: u32,
    reply: &Reply,
) {
    let now = Instant::now();
    let mut guard = shared.state.lock().unwrap();
    let st = &mut *guard;
    if st.phase != NodePhase::Ready {
        return;
    }
    let mut acts = st.core.observe_client_epoch(now, client_epoch);
    let prev = st.tables.record(
        request_id,
        attempt,
        st.core.epoch(),
        &incident,
        complexity,
        now,
    );

    if let Some(r) = st.tables.result(&request_id) {
        tracing::debug!(%request_id, attempt, "answering from results table");
        let _ = reply.send(r.to_message(request_id));
        run_actions(shared, st, acts);
        return;
    }
    st.reply_to.insert(request_id, reply.clone());
    acts.extend(
        st.core
            .on_submit(now, request_id, attempt, prev, &|| score(shared)),
    );
    if st.pending_wins.remove(&request_id) {
        acts.push(Action::Execute(request_id));
    }
    run_actions(shared, st, acts);

    // On-demand mode: the server executes what reaches it (the takeover above
    // may already have queued this request).
    if st.core.serving()
        && !st.inflight.contains(&request_id)
        && st.tables.result(&request_id).is_none()
        && !submit(shared, st, request_id, false)
    {
        tracing::debug!(%request_id, attempt, "queue full, replying Busy");
        let _ = reply.send(Message::Busy { request_id });
    }
}

/// Queues a recorded request on the pool. False if the queue is full
/// (`force` ignores the limit: takeover).
fn submit(shared: &Shared, st: &mut State, request_id: RequestId, force: bool) -> bool {
    let Some(e) = st.tables.request(&request_id) else {
        return false;
    };
    let job = Job {
        generation: shared.generation.load(Ordering::SeqCst),
        request_id,
        incident: e.incident.clone(),
        complexity: e.complexity,
        epoch: st.core.epoch(),
        degraded: st.core.degraded(),
    };
    let cost = planner::expected_cost(e.complexity);
    let queued = if force {
        shared.pool.force_submit(cost, job)
    } else {
        shared.pool.try_submit(cost, job)
    };
    if queued.is_ok() {
        st.inflight.insert(request_id);
        true
    } else {
        false
    }
}

fn run_actions(shared: &Arc<Shared>, st: &mut State, actions: Vec<Action>) {
    for a in actions {
        match a {
            Action::Broadcast(m) => shared.peers.broadcast(m),
            Action::Timer(key, after) => {
                let sh = shared.clone();
                let generation = shared.generation.load(Ordering::SeqCst);
                tokio::spawn(async move {
                    tokio::time::sleep(after).await;
                    let mut guard = sh.state.lock().unwrap();
                    let st = &mut *guard;
                    if st.phase != NodePhase::Ready
                        || sh.stopped.load(Ordering::SeqCst)
                        || sh.generation.load(Ordering::SeqCst) != generation
                    {
                        return;
                    }
                    let acts = st.core.on_timeout(Instant::now(), key);
                    run_actions(&sh, st, acts);
                });
            }
            Action::StepDown => {
                // Queued jobs are dropped; running ones finish, and their late
                // results are harmless duplicates (see completion_loop).
                let dropped = shared.pool.drop_queued();
                for j in &dropped {
                    st.inflight.remove(&j.request_id);
                }
                shared
                    .stats
                    .dropped_on_step_down
                    .fetch_add(dropped.len() as u64, Ordering::Relaxed);
                tracing::info!(
                    node = shared.cfg.node_id,
                    epoch = st.core.epoch(),
                    dropped = dropped.len(),
                    "stepped down"
                );
            }
            Action::BecameServer => {
                // Every not-completed request, oldest first, even past the
                // queue limit: they were already accepted by the cluster.
                let mut queued = 0;
                for rid in st.tables.pending_oldest_first() {
                    if !st.inflight.contains(&rid) && submit(shared, st, rid, true) {
                        queued += 1;
                    }
                }
                tracing::info!(
                    node = shared.cfg.node_id,
                    epoch = st.core.epoch(),
                    degraded = st.core.degraded(),
                    took_over = queued,
                    "now serving"
                );
            }
            Action::Execute(rid) => {
                if st.inflight.contains(&rid) || st.tables.result(&rid).is_some() {
                    continue;
                }
                if !st.tables.has_request(&rid) {
                    st.pending_wins.insert(rid); // the client's message hasn't arrived yet
                } else if !submit(shared, st, rid, false) {
                    if let Some(r) = st.reply_to.get(&rid) {
                        let _ = r.send(Message::Busy { request_id: rid });
                    }
                }
            }
            Action::Log(rec) => log_election(shared.cfg.node_id, &rec),
            Action::Split {
                epoch,
                other,
                server,
            } => tracing::warn!(
                node = shared.cfg.node_id,
                epoch,
                other,
                server,
                "two servers in one epoch (split winners): lower node_id keeps the role"
            ),
            Action::Suppressed {
                request_id,
                epoch,
                server,
                held_for,
            } => tracing::info!(
                node = shared.cfg.node_id,
                %request_id,
                epoch,
                server,
                held_for_ms = held_for.as_millis() as u64,
                min_tenure_ms = shared.cfg.min_tenure_ms,
                "retry trigger suppressed: server's tenure below min_tenure"
            ),
        }
    }
}

fn log_election(me: NodeId, r: &Record) {
    let (outcome, winner) = match r.outcome {
        Outcome::Won(w) => ("decided", Some(w)),
        Outcome::Alone => ("alone", None),
        Outcome::Degraded => ("degraded", Some(me)),
        Outcome::Abandoned => ("abandoned", None),
    };
    let line = format!(
        "election {} trigger={} trigger_request={} bids={:?} incumbent={:?} outcome={} winner={:?} duration_ms={:.2} messages_sent={}",
        r.key,
        r.trigger.as_str(),
        r.trigger_request.map(|x| x.to_string()).unwrap_or_else(|| "-".into()),
        r.bids,
        r.incumbents,
        outcome,
        winner,
        r.duration.as_secs_f64() * 1e3,
        r.messages_sent
    );
    match r.outcome {
        Outcome::Won(_) | Outcome::Degraded => tracing::info!(node = me, "{line}"),
        Outcome::Alone | Outcome::Abandoned => tracing::warn!(node = me, "{line}"),
    }
}

/// Records a finished plan, answers the client, then replicates.
async fn completion_loop(shared: Arc<Shared>, mut done_rx: mpsc::UnboundedReceiver<Done>) {
    let me = shared.cfg.node_id;
    while let Some(Done { job, plan }) = done_rx.recv().await {
        let request_id = job.request_id;
        let mut incident = job.incident;
        incident.status = IncidentStatus::Planned;
        let mut guard = shared.state.lock().unwrap();
        let st = &mut *guard;
        if st.phase != NodePhase::Ready
            || job.generation != shared.generation.load(Ordering::SeqCst)
        {
            continue;
        }
        let Some(plan) = plan else {
            continue;
        };
        st.inflight.remove(&request_id);

        // A plan finished after this node stopped serving its epoch is late.
        // It is stored and replicated under the result rule (higher epoch,
        // then lower served_by), so every node converges on one result, but
        // not sent to the client: the current server answers it.
        let late = shared.cfg.election_mode == ElectionMode::OnDemand
            && !(st.core.server() == Some(me) && st.core.epoch() == job.epoch);
        if late {
            shared.stats.late_results.fetch_add(1, Ordering::Relaxed);
        }

        let response = Response {
            served_by: me,
            epoch: job.epoch,
            degraded: job.degraded,
            plan,
        };
        match st
            .tables
            .offer(request_id, response.clone(), &incident, Instant::now())
        {
            Offer::Kept => {
                // A preferred result is already held: undo ours, keep theirs.
                let kept = st.tables.result(&request_id).expect("kept").clone();
                {
                    let mut l = shared.ledger.lock().unwrap();
                    l.release(request_id, &response.plan, unix_ms());
                    l.apply(request_id, &kept.plan, unix_ms());
                }
                if let Some(r) = st.reply_to.remove(&request_id) {
                    if !late {
                        let _ = r.send(kept.to_message(request_id));
                    }
                }
                tracing::debug!(%request_id, "our result lost to one already held");
            }
            offer => {
                if let Offer::Replaced(old) = offer {
                    let mut l = shared.ledger.lock().unwrap();
                    l.release(request_id, &old.plan, unix_ms());
                    l.apply(request_id, &response.plan, unix_ms());
                }
                if let Some(r) = st.reply_to.remove(&request_id) {
                    if !late {
                        let _ = r.send(response.to_message(request_id));
                    }
                }
                tracing::debug!(%request_id, epoch = job.epoch, late, priority = response.plan.priority, "served");
                shared.peers.broadcast(Message::Replicate {
                    epoch: response.epoch,
                    origin: me,
                    request_id,
                    plan: response.plan,
                    incident,
                    degraded: response.degraded,
                });
            }
        }
    }
}

fn on_replicate(
    shared: &Arc<Shared>,
    epoch: Epoch,
    origin: NodeId,
    request_id: RequestId,
    plan: Plan,
    incident: Incident,
    degraded: bool,
) {
    shared.peers.heard_from(origin);
    let now = Instant::now();
    let mut guard = shared.state.lock().unwrap();
    let st = &mut *guard;
    if st.phase == NodePhase::Muted {
        return;
    }
    if !shared.cfg.peers.iter().any(|p| p.id == origin) {
        return;
    }
    st.recovery_epoch = st.recovery_epoch.max(epoch);
    if st.phase == NodePhase::Ready {
        let acts = st.core.on_replicate(now, epoch, origin);
        run_actions(shared, st, acts);
    }

    let response = Response {
        served_by: origin,
        epoch,
        degraded,
        plan,
    };
    let applied = match st
        .tables
        .offer(request_id, response.clone(), &incident, now)
    {
        Offer::Kept => {
            shared
                .stats
                .duplicate_replicates
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        Offer::Stored => shared
            .ledger
            .lock()
            .unwrap()
            .apply(request_id, &response.plan, unix_ms()),
        Offer::Replaced(old) => {
            let mut l = shared.ledger.lock().unwrap();
            l.release(request_id, &old.plan, unix_ms());
            l.apply(request_id, &response.plan, unix_ms())
        }
    };
    // The origin answered the client.
    st.reply_to.remove(&request_id);

    for c in &applied.conflicts {
        shared
            .stats
            .replicate_conflicts
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            responder = c.responder_id,
            kept = %c.kept,
            dropped = %c.dropped,
            "replicated plan assigns an already-assigned responder; higher priority keeps it"
        );
    }
    if applied.resource_overcommits > 0 {
        shared
            .stats
            .resource_overcommits
            .fetch_add(applied.resource_overcommits as u64, Ordering::Relaxed);
        tracing::warn!(%request_id, units = applied.resource_overcommits, "replicated plan overcommits resources");
    }
}

async fn sweep_loop(shared: Arc<Shared>) {
    let ttl = Duration::from_millis(shared.cfg.table_ttl_ms);
    let mut tick = tokio::time::interval(SWEEP_EVERY);
    loop {
        tick.tick().await;
        let mut guard = shared.state.lock().unwrap();
        let st = &mut *guard;
        st.tables.sweep(Instant::now(), ttl);
        let tables = &st.tables;
        st.reply_to.retain(|r, _| tables.has_request(r));
        st.pending_wins.retain(|r| tables.has_request(r));
    }
}

fn status(shared: &Shared) -> NodeStatus {
    let load = shared.pool.load();
    let st = shared.state.lock().unwrap();
    let (requests, results) = st.tables.len();
    let c = st.core.counters;
    NodeStatus {
        node_id: shared.cfg.node_id,
        phase: st.phase,
        faults_injected: st.faults_injected,
        last_recovery_ms: st.last_recovery_ms,
        last_recovery: st.last_recovery.clone(),
        snapshots_served: shared.stats.snapshots_served.load(Ordering::Relaxed),
        snapshot_lock_max_us: shared.stats.snapshot_lock_max_us.load(Ordering::Relaxed),
        epoch: st.core.epoch(),
        server: st.core.server(),
        degraded: st.core.degraded(),
        elections_decided: c.decided,
        elections_alone: c.alone,
        split_resolutions: c.splits,
        step_downs: c.step_downs,
        suppressed_triggers: c.suppressed,
        dropped_on_step_down: shared.stats.dropped_on_step_down.load(Ordering::Relaxed),
        late_results: shared.stats.late_results.load(Ordering::Relaxed),
        requests,
        results,
        results_digest: st.tables.results_digest(),
        queue_len: load.queued,
        running: load.running,
        pending_cost: load.pending_cost,
        replicate_conflicts: shared.stats.replicate_conflicts.load(Ordering::Relaxed),
        duplicate_replicates: shared.stats.duplicate_replicates.load(Ordering::Relaxed),
        local_replans: shared.stats.local_replans.load(Ordering::Relaxed),
        resource_overcommits: shared.stats.resource_overcommits.load(Ordering::Relaxed),
        peer_queue_drops: shared.peers.drops(),
        peer_last_seen_ms: shared.peers.last_seen_ms(),
        sent: shared.stats.sent.snapshot(),
        received: shared.stats.received.snapshot(),
    }
}
