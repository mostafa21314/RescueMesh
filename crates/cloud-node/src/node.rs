//! A cloud node: accepts client and peer connections on one port, records
//! every request, plans the ones it serves on the worker pool, replies, and
//! replicates results to its peers.

use crate::campus::Campus;
use crate::ledger::Ledger;
use crate::peers::PeerLinks;
use crate::planner;
use crate::pool::Pool;
use crate::tables::{Response, Tables};
use common::config::NodeConfig;
use common::counters::Counters;
use common::wire::{read_frame, write_frame};
use common::*;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

/// PLACEHOLDER until Part B (election): node 1 always serves, in epoch 0.
/// Part B replaces this with an elected server per epoch.
pub const PLACEHOLDER_SERVER: NodeId = 1;
pub const PLACEHOLDER_EPOCH: Epoch = 0;

/// How often expired table entries are swept.
const SWEEP_EVERY: Duration = Duration::from_secs(1);
/// Planning attempts on a fresh snapshot before planning under the ledger lock.
const OPTIMISTIC_PLANS: u32 = 2;

type Reply = mpsc::UnboundedSender<Message>;

struct Job {
    request_id: RequestId,
    incident: Incident,
    complexity: u32,
}

struct Done {
    request_id: RequestId,
    incident: Incident,
    plan: Plan,
}

struct State {
    tables: Tables,
    epoch: Epoch,
    server: Option<NodeId>,
    /// Connections waiting for a request this node is planning right now.
    waiters: HashMap<RequestId, Vec<Reply>>,
}

#[derive(Default)]
struct Stats {
    sent: Arc<Counters>,
    received: Counters,
    replicate_conflicts: AtomicU64,
    duplicate_replicates: AtomicU64,
    local_replans: Arc<AtomicU64>,
    resource_overcommits: AtomicU64,
}

struct Shared {
    cfg: NodeConfig,
    ledger: Arc<Mutex<Ledger>>,
    state: Mutex<State>,
    pool: Pool<Job>,
    peers: Arc<PeerLinks>,
    stats: Stats,
}

pub struct NodeHandle {
    shared: Arc<Shared>,
    addr: SocketAddr,
    tasks: Vec<JoinHandle<()>>,
}

impl NodeHandle {
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

    /// Stops accepting, closes every connection and peer link, and joins the
    /// planner threads after their current job.
    pub fn shutdown(self) {
        for t in &self.tasks {
            t.abort();
        }
        self.shared.pool.shutdown();
    }
}

/// Binds `cfg.listen` and starts the node.
pub async fn start(cfg: NodeConfig) -> anyhow::Result<NodeHandle> {
    let listener = TcpListener::bind(&cfg.listen).await?;
    start_with_listener(cfg, listener).await
}

pub async fn start_with_listener(cfg: NodeConfig, listener: TcpListener) -> anyhow::Result<NodeHandle> {
    cfg.validate()?;
    let addr = listener.local_addr()?;
    let campus = Arc::new(Campus::generate(cfg.seed, cfg.campus));
    let ledger = Arc::new(Mutex::new(Ledger::new(&campus)));
    let stats = Stats::default();
    let (peers, mut tasks) = PeerLinks::spawn(&cfg.peers, stats.sent.clone());
    let (done_tx, done_rx) = mpsc::unbounded_channel();

    let pool = {
        let (campus, ledger, replans) = (campus.clone(), ledger.clone(), stats.local_replans.clone());
        Pool::new(cfg.workers, cfg.queue_capacity, move |job: Job| {
            let plan = plan_and_commit(&campus, &ledger, &replans, &job);
            let _ = done_tx.send(Done { request_id: job.request_id, incident: job.incident, plan });
        })
    };

    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            tables: Tables::default(),
            epoch: PLACEHOLDER_EPOCH,
            server: Some(PLACEHOLDER_SERVER),
            waiters: HashMap::new(),
        }),
        cfg,
        ledger,
        pool,
        peers,
        stats,
    });

    // TODO(recovery/sync): a node that (re)starts with peers already holding
    // results must fetch them actively before serving; nothing pushes them.
    // TODO(discovery): peers come from the config file for now.
    tasks.push(tokio::spawn(accept_loop(shared.clone(), listener)));
    tasks.push(tokio::spawn(completion_loop(shared.clone(), done_rx)));
    tasks.push(tokio::spawn(sweep_loop(shared.clone())));
    tracing::info!(
        node = shared.cfg.node_id,
        %addr,
        workers = shared.cfg.workers,
        zones = campus.zones(),
        responders = campus.responders.len(),
        server = PLACEHOLDER_SERVER,
        "node started (placeholder: node {PLACEHOLDER_SERVER} always serves)"
    );
    Ok(NodeHandle { shared, addr, tasks })
}

/// Plans on a snapshot and commits if nothing it chose was taken meanwhile;
/// after `OPTIMISTIC_PLANS` collisions, plans while holding the ledger lock.
/// Either way commits happen one at a time, so two workers never assign the
/// same responder.
fn plan_and_commit(campus: &Campus, ledger: &Mutex<Ledger>, replans: &AtomicU64, job: &Job) -> Plan {
    for _ in 0..OPTIMISTIC_PLANS {
        let now = unix_ms();
        let snap = ledger.lock().unwrap().snapshot(now);
        let plan = planner::plan(campus, &snap, &job.incident, job.complexity, now);
        let mut l = ledger.lock().unwrap();
        if l.can_commit(job.request_id, &plan, now) {
            l.apply(job.request_id, &plan, now);
            return plan;
        }
        replans.fetch_add(1, Ordering::Relaxed);
    }
    let now = unix_ms();
    let mut l = ledger.lock().unwrap();
    let plan = planner::plan(campus, &l.snapshot(now), &job.incident, job.complexity, now);
    l.apply(job.request_id, &plan, now);
    plan
}

async fn accept_loop(shared: Arc<Shared>, listener: TcpListener) {
    // Connection tasks live in the JoinSet, so aborting this task closes them.
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    // TODO(fault injection): the mute switch refuses connections here.
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
                    // TODO(fault injection): the mute switch drops inbound frames here.
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

fn handle(shared: &Shared, msg: Message, reply: &Reply) {
    match msg {
        Message::SubmitIncident { request_id, attempt, client_epoch, incident, complexity } => {
            on_submit(shared, request_id, attempt, client_epoch, incident, complexity, reply)
        }
        Message::Replicate { epoch, origin, request_id, plan, incident } => {
            on_replicate(shared, epoch, origin, request_id, plan, incident)
        }
        Message::StatusRequest => {
            let _ = reply.send(Message::Status(status(shared)));
        }
        other => tracing::debug!(kind = other.kind(), "ignoring message not meant for a node"),
    }
}

fn on_submit(
    shared: &Shared,
    request_id: RequestId,
    attempt: u32,
    _client_epoch: Epoch,
    incident: Incident,
    complexity: u32,
    reply: &Reply,
) {
    // TODO(Part B): adopt a higher client_epoch, and trigger an election on a
    // retry whose previous attempt arrived in the current epoch.
    let me = shared.cfg.node_id;
    let mut st = shared.state.lock().unwrap();
    let epoch = st.epoch;
    st.tables.record(request_id, attempt, epoch, &incident, complexity, Instant::now());

    if let Some(r) = st.tables.result(&request_id) {
        tracing::debug!(%request_id, attempt, "answering from results table");
        let _ = reply.send(r.to_message(request_id));
        return;
    }
    if st.server != Some(me) {
        return; // recorded only
    }
    if let Some(ws) = st.waiters.get_mut(&request_id) {
        ws.push(reply.clone()); // already queued or running
        return;
    }
    let job = Job { request_id, incident, complexity };
    match shared.pool.try_submit(planner::expected_cost(complexity), job) {
        Ok(()) => {
            st.waiters.insert(request_id, vec![reply.clone()]);
        }
        Err(_) => {
            tracing::debug!(%request_id, attempt, "queue full, replying Busy");
            let _ = reply.send(Message::Busy { request_id });
        }
    }
}

/// Records a finished plan, answers the waiting connections, then replicates.
async fn completion_loop(shared: Arc<Shared>, mut done_rx: mpsc::UnboundedReceiver<Done>) {
    let me = shared.cfg.node_id;
    while let Some(Done { request_id, mut incident, plan }) = done_rx.recv().await {
        incident.status = IncidentStatus::Planned;
        let (response, waiters) = {
            let mut st = shared.state.lock().unwrap();
            // TODO(Part B): the job's own epoch, and count late results after step-down.
            let response = Response { served_by: me, epoch: st.epoch, degraded: false, plan };
            st.tables.complete(request_id, response.clone(), &incident, Instant::now());
            (response, st.waiters.remove(&request_id).unwrap_or_default())
        };
        let msg = response.to_message(request_id);
        for w in waiters {
            let _ = w.send(msg.clone());
        }
        tracing::debug!(%request_id, priority = response.plan.priority, "served");
        shared.peers.broadcast(Message::Replicate {
            epoch: response.epoch,
            origin: me,
            request_id,
            plan: response.plan,
            incident,
        });
    }
}

fn on_replicate(shared: &Shared, epoch: Epoch, origin: NodeId, request_id: RequestId, plan: Plan, incident: Incident) {
    shared.peers.heard_from(origin);
    // TODO(Part B): adopt a higher epoch; resolve two servers of one epoch.
    let response = Response { served_by: origin, epoch, degraded: false, plan };
    let waiters = {
        let mut st = shared.state.lock().unwrap();
        if !st.tables.complete(request_id, response.clone(), &incident, Instant::now()) {
            shared.stats.duplicate_replicates.fetch_add(1, Ordering::Relaxed);
            return;
        }
        st.waiters.remove(&request_id).unwrap_or_default()
    };
    let msg = response.to_message(request_id);
    for w in waiters {
        let _ = w.send(msg.clone());
    }

    let applied = shared.ledger.lock().unwrap().apply(request_id, &response.plan, unix_ms());
    for c in &applied.conflicts {
        shared.stats.replicate_conflicts.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            responder = c.responder_id,
            kept = %c.kept,
            dropped = %c.dropped,
            "replicated plan assigns an already-assigned responder; higher priority keeps it"
        );
    }
    if applied.resource_overcommits > 0 {
        shared.stats.resource_overcommits.fetch_add(applied.resource_overcommits as u64, Ordering::Relaxed);
        tracing::warn!(%request_id, units = applied.resource_overcommits, "replicated plan overcommits resources");
    }
}

async fn sweep_loop(shared: Arc<Shared>) {
    let ttl = Duration::from_millis(shared.cfg.table_ttl_ms);
    let mut tick = tokio::time::interval(SWEEP_EVERY);
    loop {
        tick.tick().await;
        shared.state.lock().unwrap().tables.sweep(Instant::now(), ttl);
    }
}

fn status(shared: &Shared) -> NodeStatus {
    let load = shared.pool.load();
    let st = shared.state.lock().unwrap();
    let (requests, results) = st.tables.len();
    NodeStatus {
        node_id: shared.cfg.node_id,
        epoch: st.epoch,
        server: st.server,
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
