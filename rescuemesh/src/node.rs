//! A RescueMesh cloud node.
//!
//! Process model (per node, one OS process):
//!   * tokio multi-threaded runtime for all network I/O;
//!   * one listener port shared by clients and peers (the first frame decides:
//!     `Hello` = peer link, anything else = client session);
//!   * one outbound link task per peer (reconnects with backoff, fed by a bounded
//!     channel);
//!   * a heartbeat task (load report to every peer) and a prune task;
//!   * CPU-bound planning runs on the blocking pool, bounded by a semaphore of
//!     `workers` permits. Requests waiting for a permit form the node's queue,
//!     which is what `queue_len` in the load report measures.

use crate::assign::{Assigner, HashAssigner};
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
use std::time::Duration;
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
    assigner: Box<dyn Assigner>,
    workers: Arc<Semaphore>,
    queue_len: AtomicU32,
    inflight: AtomicU32,
    completed: AtomicU64,
    executed_here: AtomicU64,
    dropped: AtomicU64,
    ewma_service_ms: Mutex<f64>,
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
        assigner: Box::new(HashAssigner),
        queue_len: AtomicU32::new(0),
        inflight: AtomicU32::new(0),
        completed: AtomicU64::new(0),
        executed_here: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
        ewma_service_ms: Mutex::new(0.0),
        waiters: Mutex::new(HashMap::new()),
    });

    for (pid, addr, rx) in links {
        tokio::spawn(peer_link(cfg.id, pid, addr, rx));
    }
    tokio::spawn(heartbeat_loop(node.clone()));
    tokio::spawn(prune_loop(node.clone()));

    let listener = TcpListener::bind(&cfg.listen).await?;
    info!(
        node = cfg.id,
        listen = %cfg.listen,
        workers = cfg.workers,
        assigner = node.assigner.name(),
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

    fn load_report(&self) -> LoadReport {
        LoadReport {
            queue_len: self.queue_len.load(Relaxed),
            inflight: self.inflight.load(Relaxed),
            workers: self.cfg.workers as u32,
            completed: self.completed.load(Relaxed),
            ewma_service_ms: *self.ewma_service_ms.lock(),
            sent_ms: now_ms(),
        }
    }

    fn broadcast(&self, msg: Msg) {
        for h in self.peers.values() {
            if h.tx.try_send(msg.clone()).is_err() {
                self.dropped.fetch_add(1, Relaxed);
            }
        }
    }

    pub fn status(&self) -> StatusReport {
        let now = now_ms();
        let st = self.state.lock();
        StatusReport {
            node: self.cfg.id,
            version: st.version,
            active_incidents: st.incidents.values().filter(|i| i.is_active(now)).count(),
            plans: st.plans.len(),
            plans_digest: format!("{:016x}", st.plans_digest),
            pending: st.pending.len(),
            executed_here: self.executed_here.load(Relaxed),
            load: self.load_report(),
            live: self.live_nodes(),
            assigner: self.assigner.name().to_string(),
            dropped_peer_msgs: self.dropped.load(Relaxed),
        }
    }

    async fn handle_submit(self: Arc<Self>, rid: RequestId, client: String, incident: Incident, reply: mpsc::Sender<Msg>) {
        let me = self.cfg.id;

        // Idempotency: an already-committed request is answered from the cache by
        // the node that executed it; the others stay silent.
        let cached = self.state.lock().plans.get(&rid).cloned();
        if let Some(plan) = cached {
            if plan.executed_by == me {
                let _ = reply.send(Msg::PlanResult { request_id: rid, plan, duplicate: true }).await;
            }
            return;
        }

        self.state.lock().observe_incident(&incident);
        let live = self.live_nodes();
        let executor = self.assigner.choose(&rid, &live);
        if executor != me {
            debug!(request = %rid, executor, "not selected; tracking as pending");
            self.state.lock().pending.insert(
                rid,
                Pending { executor, received_ms: now_ms(), incident },
            );
            return;
        }

        // Duplicate arriving while we are still computing: wait for the same result.
        {
            let mut w = self.waiters.lock();
            if let Some(v) = w.get_mut(&rid) {
                v.push(reply);
                return;
            }
            w.insert(rid.clone(), vec![reply]);
        }

        self.queue_len.fetch_add(1, Relaxed);
        let permit = self.workers.clone().acquire_owned().await.expect("semaphore closed");
        self.queue_len.fetch_sub(1, Relaxed);
        self.inflight.fetch_add(1, Relaxed);

        let snap = self.state.lock().snapshot(now_ms());
        let node = self.clone();
        let (inc2, rid2) = (incident.clone(), rid.clone());
        let plan = tokio::task::spawn_blocking(move || {
            planner::plan(&node.cfg.planner, &node.campus, &snap, &inc2, &rid2, me)
        })
        .await
        .expect("planner panicked");
        drop(permit);
        self.inflight.fetch_sub(1, Relaxed);
        self.completed.fetch_add(1, Relaxed);
        self.executed_here.fetch_add(1, Relaxed);
        {
            let mut e = self.ewma_service_ms.lock();
            let ms = plan.compute_us as f64 / 1000.0;
            *e = if *e == 0.0 { ms } else { 0.8 * *e + 0.2 * ms };
        }

        let plan = Arc::new(plan);
        let op = ReplOp::PlanCommitted {
            op_id: format!("plan:{rid}"),
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
