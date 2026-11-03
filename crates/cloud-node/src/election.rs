//! Load-aware election, as a sans-IO state machine.
//!
//! `Core` holds who serves (epoch, server, degraded) and the elections in
//! progress. The node feeds it events (a request arrived, a bid arrived, a bid
//! timer fired, a Replicate arrived) with the current time; it returns
//! `Action`s for the node to carry out (send a bid, arm a timer, step down,
//! take over, execute a request, log an election). No sockets, clocks, or
//! threads in here, so every rule is unit-tested below.
//!
//! On-demand mode (default): one node serves per epoch. An election for
//! `epoch + 1` starts when there is no known server, or when a client retries a
//! not-completed request whose previous attempt arrived in the current epoch.
//! Each node bids once per election; a decision needs ≥ 2 of 3 bids; the
//! incumbent can't win unless it is the only candidate; the winner is the
//! lowest (score, node_id).
//!
//! Per-request mode (comparison): every request attempt gets its own bid
//! exchange, keyed by (request_id, attempt), with no incumbent rule and no
//! epochs.

use common::config::{ElectionMode, Policy};
use common::rng::{fnv1a, mix};
use common::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use crate::tables::Previous;

/// How long per-request election keys are remembered after deciding, so a
/// late bid can't restart a finished election.
pub const FINISHED_KEEP: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    Epoch(Epoch),
    Request(RequestId, u32),
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Epoch(e) => write!(f, "epoch {e}"),
            Key::Request(r, a) => write!(f, "request {r} attempt {a}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// No known server (startup, after stepping down, after adopting an epoch
    /// whose server is unknown).
    NoServer,
    /// A retry of a request whose previous attempt arrived in this epoch.
    Retry,
    /// Per-request mode: a request attempt.
    PerRequest,
    /// Joined because a peer's bid arrived first.
    PeerBid,
}

impl Trigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Trigger::NoServer => "no_server",
            Trigger::Retry => "retry",
            Trigger::PerRequest => "per_request",
            Trigger::PeerBid => "peer_bid",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bid {
    pub node_id: NodeId,
    pub score: u32,
    pub incumbent: Option<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Won(NodeId),
    /// Only this node's bid: no decision.
    Alone,
    /// Alone for ≥ degraded_after: now serving alone.
    Degraded,
    /// Replaced by a higher election, or overtaken by a message with a
    /// higher epoch.
    Abandoned,
}

/// One line per election (logged by the node).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub key: Key,
    pub trigger: Trigger,
    pub trigger_request: Option<RequestId>,
    pub bids: Vec<(NodeId, u32)>,
    pub incumbents: Vec<NodeId>,
    pub outcome: Outcome,
    pub duration: Duration,
    pub messages_sent: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Broadcast(Message),
    /// Call `on_timeout(key)` after this long.
    Timer(Key, Duration),
    /// Stop serving: drop queued-not-started jobs.
    StepDown,
    /// Became the server (elected or degraded): execute every not-completed
    /// request in the request table, oldest first.
    BecameServer,
    /// Per-request mode: this node won the request's election.
    Execute(RequestId),
    Log(Record),
    /// Two servers of one epoch were seen; `server` now holds the role.
    Split {
        epoch: Epoch,
        other: NodeId,
        server: NodeId,
    },
    /// A retry that would have triggered an election, ignored because the
    /// server has held the role for less than min_tenure.
    Suppressed {
        request_id: RequestId,
        epoch: Epoch,
        server: NodeId,
        held_for: Duration,
    },
}

#[derive(Clone, Debug)]
struct Election {
    key: Key,
    started: Instant,
    trigger: Trigger,
    trigger_request: Option<RequestId>,
    bids: BTreeMap<NodeId, Bid>,
    sent: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct Params {
    pub me: NodeId,
    pub cluster_size: usize,
    pub mode: ElectionMode,
    pub policy: Policy,
    pub bid_timeout: Duration,
    pub degraded_after: Duration,
    /// A retry can trigger an election only after the current server has held
    /// the role this long (hysteresis; 0 disables). "No server" is exempt.
    pub min_tenure: Duration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub decided: u64,
    pub alone: u64,
    pub splits: u64,
    pub step_downs: u64,
    /// Retry triggers ignored because the server's tenure was below min_tenure.
    pub suppressed: u64,
}

pub struct Core {
    p: Params,
    epoch: Epoch,
    server: Option<NodeId>,
    degraded: bool,
    /// Start of the current run of elections that heard only our own bid.
    alone_since: Option<Instant>,
    /// On-demand mode: the election in progress, if any.
    active: Option<Election>,
    /// Per-request mode: elections in progress, and recently finished keys.
    per_request: HashMap<Key, Election>,
    finished: HashMap<Key, Instant>,
    /// On-demand mode: the bid this node sent for each target epoch, and
    /// when. Rejoining an election it already bid in (its own timer fired
    /// before a peer's bid arrived) re-sends the same bid, so every node
    /// decides from the same bids.
    own_bids: BTreeMap<Epoch, Bid>,
    /// When this node learned that `server` holds the role in `epoch`.
    tenure: Option<(NodeId, Epoch, Instant)>,
    pub counters: Counters,
}

impl Core {
    /// Discard pre-outage elections and install the view confirmed by survivors.
    /// The caller keeps the node gated until its tables and ledger are rebuilt.
    pub fn recover_view(&mut self, now: Instant, epoch: Epoch, server: Option<NodeId>) {
        self.epoch = epoch;
        self.server = server;
        self.degraded = false;
        self.alone_since = None;
        self.active = None;
        self.per_request.clear();
        self.finished.clear();
        self.own_bids.clear();
        self.tenure = server.map(|s| (s, epoch, now));
    }
    pub fn new(p: Params) -> Self {
        Core {
            p,
            epoch: 0,
            server: None,
            degraded: false,
            alone_since: None,
            active: None,
            per_request: HashMap::new(),
            finished: HashMap::new(),
            own_bids: BTreeMap::new(),
            tenure: None,
            counters: Counters::default(),
        }
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn server(&self) -> Option<NodeId> {
        self.server
    }

    pub fn degraded(&self) -> bool {
        self.degraded
    }

    /// On-demand mode: this node executes attempt-1 requests.
    pub fn serving(&self) -> bool {
        self.p.mode == ElectionMode::OnDemand
            && self.server == Some(self.p.me)
            && self.active.is_none()
    }

    pub fn electing(&self) -> bool {
        self.active.is_some()
    }

    fn quorum(&self) -> usize {
        self.p.cluster_size / 2 + 1
    }

    // ---------------------------------------------------------------- events

    /// A request's `client_epoch`. A higher epoch than ours means an election
    /// we missed: adopt it, with the server unknown.
    pub fn observe_client_epoch(&mut self, now: Instant, client_epoch: Epoch) -> Vec<Action> {
        let mut out = Vec::new();
        if self.p.mode != ElectionMode::OnDemand || client_epoch <= self.epoch {
            return out;
        }
        if self
            .active
            .as_ref()
            .is_some_and(|e| target(e.key) >= client_epoch)
        {
            return out; // our election for that epoch will decide it
        }
        self.adopt(now, client_epoch, None, &mut out);
        out
    }

    /// A SubmitIncident with no stored result. `prev` is the request-table
    /// entry as it was before this attempt was recorded.
    pub fn on_submit(
        &mut self,
        now: Instant,
        request_id: RequestId,
        attempt: u32,
        prev: Option<Previous>,
        score: &dyn Fn() -> u32,
    ) -> Vec<Action> {
        let mut out = Vec::new();
        match self.p.mode {
            ElectionMode::OnDemand => {
                if self.degraded {
                    return out; // serving alone; nobody to elect
                }
                let retry = attempt >= 2
                    && prev.is_some_and(|p| {
                        p.attempt < attempt && !p.completed && p.epoch == self.epoch
                    });
                if let (true, Some(server)) = (retry, self.server) {
                    let held_for = self.held_for(now, server);
                    if held_for < self.p.min_tenure {
                        // Hysteresis: a new server gets one client timeout to
                        // work through the backlog it inherited before a
                        // retry counts against it.
                        self.counters.suppressed += 1;
                        out.push(Action::Suppressed {
                            request_id,
                            epoch: self.epoch,
                            server,
                            held_for,
                        });
                    } else {
                        self.trigger_epoch(now, Trigger::Retry, Some(request_id), score, &mut out);
                    }
                } else if retry {
                    // No known server: exempt from the tenure rule.
                    self.trigger_epoch(now, Trigger::Retry, Some(request_id), score, &mut out);
                } else if self.server.is_none() && self.active.is_none() {
                    self.trigger_epoch(now, Trigger::NoServer, Some(request_id), score, &mut out);
                }
            }
            ElectionMode::PerRequest => {
                if prev.is_some_and(|p| p.attempt >= attempt) {
                    return out; // duplicate of an attempt already handled
                }
                if self.degraded {
                    out.push(Action::Execute(request_id));
                    return out;
                }
                let key = Key::Request(request_id, attempt);
                if !self.per_request.contains_key(&key) && !self.finished.contains_key(&key) {
                    self.start(
                        now,
                        key,
                        Trigger::PerRequest,
                        Some(request_id),
                        None,
                        score,
                        &mut out,
                    );
                }
            }
        }
        out
    }

    /// An ElectBid from a peer.
    #[allow(clippy::too_many_arguments)]
    pub fn on_bid(
        &mut self,
        now: Instant,
        target_epoch: Epoch,
        bid: Bid,
        request_id: Option<RequestId>,
        attempt: u32,
        score: &dyn Fn() -> u32,
    ) -> Vec<Action> {
        let mut out = Vec::new();
        self.peer_heard();
        match (self.p.mode, request_id) {
            (ElectionMode::OnDemand, None) => {
                if target_epoch <= self.epoch {
                    return out; // old election
                }
                match self.active.as_ref().map(|e| target(e.key)) {
                    Some(t) if t > target_epoch => return out,
                    Some(t) if t == target_epoch => {}
                    _ => {
                        // Not joined yet (or joined a lower one): join this one.
                        if let Some(old) = self.active.take() {
                            out.push(Action::Log(self.record(&old, Outcome::Abandoned, now)));
                        }
                        if target_epoch > self.epoch + 1 {
                            self.adopt(now, target_epoch - 1, None, &mut out);
                        }
                        self.trigger_epoch(now, Trigger::PeerBid, None, score, &mut out);
                    }
                }
                let e = self.active.as_mut().expect("joined above");
                e.bids.insert(bid.node_id, bid);
                if e.bids.len() >= self.p.cluster_size {
                    self.decide(now, Key::Epoch(target_epoch), &mut out);
                }
            }
            (ElectionMode::PerRequest, Some(rid)) => {
                let key = Key::Request(rid, attempt);
                if self.finished.contains_key(&key) {
                    return out;
                }
                if !self.per_request.contains_key(&key) {
                    self.start(now, key, Trigger::PeerBid, Some(rid), None, score, &mut out);
                }
                let e = self.per_request.get_mut(&key).expect("started above");
                e.bids.insert(bid.node_id, bid);
                if e.bids.len() >= self.p.cluster_size {
                    self.decide(now, key, &mut out);
                }
            }
            (mode, _) => tracing::warn!(
                ?mode,
                from = bid.node_id,
                "bid for the other election mode ignored"
            ),
        }
        out
    }

    /// The bid timer for `key` fired: decide with the bids collected.
    pub fn on_timeout(&mut self, now: Instant, key: Key) -> Vec<Action> {
        let mut out = Vec::new();
        let open = match key {
            Key::Epoch(_) => self.active.as_ref().is_some_and(|e| e.key == key),
            Key::Request(..) => self.per_request.contains_key(&key),
        };
        if open {
            self.decide(now, key, &mut out);
        }
        if let Key::Request(..) = key {
            self.finished
                .retain(|_, t| now.duration_since(*t) < FINISHED_KEEP);
        }
        out
    }

    /// A Replicate from `origin` for `epoch`.
    pub fn on_replicate(&mut self, now: Instant, epoch: Epoch, origin: NodeId) -> Vec<Action> {
        let mut out = Vec::new();
        self.peer_heard();
        if self.p.mode != ElectionMode::OnDemand {
            return out;
        }
        if epoch > self.epoch {
            self.adopt(now, epoch, Some(origin), &mut out);
        } else if epoch == self.epoch && self.active.is_none() {
            let me = self.p.me;
            match self.server {
                Some(s) if s == origin => {}
                Some(s) if s == me => {
                    // Split winners: both believe they won this epoch. The
                    // lower node_id keeps the role.
                    self.counters.splits += 1;
                    let keeper = me.min(origin);
                    if keeper == origin {
                        self.step_down(&mut out);
                        self.server = Some(origin);
                    }
                    out.push(Action::Split {
                        epoch,
                        other: origin,
                        server: keeper,
                    });
                }
                Some(s) => {
                    self.counters.splits += 1;
                    let keeper = s.min(origin);
                    self.server = Some(keeper);
                    out.push(Action::Split {
                        epoch,
                        other: s.max(origin),
                        server: keeper,
                    });
                }
                None => self.server = Some(origin),
            }
        }
        out
    }

    // --------------------------------------------------------------- helpers

    /// How long `server` has held the role in the current epoch, as this node
    /// knows it. A server learned of without a timestamp (e.g. via a split
    /// resolution) starts its tenure now.
    fn held_for(&mut self, now: Instant, server: NodeId) -> Duration {
        match self.tenure {
            Some((s, e, since)) if s == server && e == self.epoch => now.duration_since(since),
            _ => {
                self.tenure = Some((server, self.epoch, now));
                Duration::ZERO
            }
        }
    }

    /// Any message from a peer ends degraded mode and the "alone" run.
    fn peer_heard(&mut self) {
        self.alone_since = None;
        if self.degraded {
            self.degraded = false;
            if self.server == Some(self.p.me) {
                self.server = None;
            }
            tracing::info!(
                node = self.p.me,
                epoch = self.epoch,
                "peer heard: leaving degraded mode"
            );
        }
    }

    /// Moves to a higher `epoch`, stepping down if we were serving.
    fn adopt(&mut self, now: Instant, epoch: Epoch, server: Option<NodeId>, out: &mut Vec<Action>) {
        if self.server == Some(self.p.me) {
            self.step_down(out);
        }
        self.epoch = epoch;
        self.own_bids.retain(|target, _| *target > epoch);
        self.server = server;
        self.tenure = server.map(|s| (s, epoch, now));
        self.degraded = false;
        if self.active.as_ref().is_some_and(|e| target(e.key) <= epoch) {
            let old = self.active.take().expect("checked");
            out.push(Action::Log(self.record(&old, Outcome::Abandoned, now)));
        }
    }

    fn step_down(&mut self, out: &mut Vec<Action>) {
        self.counters.step_downs += 1;
        out.push(Action::StepDown);
    }

    /// Starts (or joins) the election for `epoch + 1`.
    fn trigger_epoch(
        &mut self,
        now: Instant,
        trigger: Trigger,
        request_id: Option<RequestId>,
        score: &dyn Fn() -> u32,
        out: &mut Vec<Action>,
    ) {
        let target_epoch = self.epoch + 1;
        if self
            .active
            .as_ref()
            .is_some_and(|e| target(e.key) >= target_epoch)
        {
            return; // all triggers for one target join one election
        }
        let incumbent = self.server;
        if incumbent == Some(self.p.me) {
            self.step_down(out);
        }
        self.server = None;
        self.start(
            now,
            Key::Epoch(target_epoch),
            trigger,
            request_id,
            incumbent,
            score,
            out,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn start(
        &mut self,
        now: Instant,
        key: Key,
        trigger: Trigger,
        trigger_request: Option<RequestId>,
        incumbent: Option<NodeId>,
        score: &dyn Fn() -> u32,
        out: &mut Vec<Action>,
    ) {
        let me = self.p.me;
        let reuse = match key {
            Key::Epoch(t) => self.own_bids.get(&t).copied(),
            Key::Request(..) => None,
        };
        let own = reuse.unwrap_or_else(|| Bid {
            node_id: me,
            score: score(),
            incumbent,
        });
        if let (Key::Epoch(t), None) = (key, reuse) {
            self.own_bids.insert(t, own);
        }
        let incumbent = own.incumbent;
        let (target_epoch, request_id, attempt) = match key {
            Key::Epoch(t) => (t, None, 0),
            Key::Request(r, a) => (0, Some(r), a),
        };
        out.push(Action::Broadcast(Message::ElectBid {
            target_epoch,
            node_id: me,
            score: own.score,
            incumbent,
            request_id,
            attempt,
        }));
        out.push(Action::Timer(key, self.p.bid_timeout));
        let e = Election {
            key,
            started: now,
            trigger,
            trigger_request,
            bids: BTreeMap::from([(me, own)]),
            sent: (self.p.cluster_size - 1) as u32,
        };
        match key {
            Key::Epoch(_) => self.active = Some(e),
            Key::Request(..) => {
                self.per_request.insert(key, e);
            }
        }
    }

    fn decide(&mut self, now: Instant, key: Key, out: &mut Vec<Action>) {
        let e = match key {
            Key::Epoch(_) => self.active.take(),
            Key::Request(..) => {
                self.finished.insert(key, now);
                self.per_request.remove(&key)
            }
        };
        let Some(e) = e else { return };
        let me = self.p.me;

        if e.bids.len() >= self.quorum() {
            let winner = decide_winner(self.p.mode, self.p.policy, key, &e.bids);
            self.counters.decided += 1;
            self.alone_since = None;
            match key {
                Key::Epoch(t) => {
                    self.epoch = t;
                    self.own_bids.retain(|k, _| *k > t);
                    self.server = Some(winner);
                    self.tenure = Some((winner, t, now));
                    self.degraded = false;
                    if winner == me {
                        out.push(Action::BecameServer);
                    }
                }
                Key::Request(rid, _) => {
                    if winner == me {
                        out.push(Action::Execute(rid));
                    }
                }
            }
            out.push(Action::Log(self.record(&e, Outcome::Won(winner), now)));
            return;
        }

        // Only our own bid.
        self.counters.alone += 1;
        let since = *self.alone_since.get_or_insert(e.started);
        if !self.degraded && now.duration_since(since) >= self.p.degraded_after {
            // Serve alone. The epoch does not change while degraded.
            self.degraded = true;
            match key {
                Key::Epoch(_) => {
                    self.server = Some(me);
                    out.push(Action::BecameServer);
                }
                Key::Request(rid, _) => out.push(Action::Execute(rid)),
            }
            out.push(Action::Log(self.record(&e, Outcome::Degraded, now)));
        } else {
            out.push(Action::Log(self.record(&e, Outcome::Alone, now)));
        }
    }

    fn record(&self, e: &Election, outcome: Outcome, now: Instant) -> Record {
        Record {
            key: e.key,
            trigger: e.trigger,
            trigger_request: e.trigger_request,
            bids: e.bids.values().map(|b| (b.node_id, b.score)).collect(),
            incumbents: incumbents(self.p.mode, &e.bids).into_iter().collect(),
            outcome,
            duration: now.duration_since(e.started),
            messages_sent: e.sent,
        }
    }
}

fn target(key: Key) -> Epoch {
    match key {
        Key::Epoch(t) => t,
        Key::Request(..) => 0,
    }
}

/// Every incumbent named by any bid (on-demand mode only). Bids normally
/// agree; naming them all keeps the decision identical on every node that
/// holds the same bids.
fn incumbents(mode: ElectionMode, bids: &BTreeMap<NodeId, Bid>) -> BTreeSet<NodeId> {
    match mode {
        ElectionMode::OnDemand => bids.values().filter_map(|b| b.incumbent).collect(),
        ElectionMode::PerRequest => BTreeSet::new(),
    }
}

/// The winner among `bids`: bidders minus incumbents (unless that leaves
/// none), then the lowest (score, node_id) — or, under RoundRobin, a
/// deterministic hash of the election key mod the number of candidates.
pub fn decide_winner(
    mode: ElectionMode,
    policy: Policy,
    key: Key,
    bids: &BTreeMap<NodeId, Bid>,
) -> NodeId {
    let excluded = incumbents(mode, bids);
    let mut cands: Vec<&Bid> = bids
        .values()
        .filter(|b| !excluded.contains(&b.node_id))
        .collect();
    if cands.is_empty() {
        cands = bids.values().collect();
    }
    match policy {
        Policy::RoundRobin => {
            cands.sort_by_key(|b| b.node_id);
            let h = match key {
                Key::Epoch(t) => mix(t),
                Key::Request(r, a) => {
                    let mut bytes = r.client_id.as_bytes().to_vec();
                    bytes.extend_from_slice(&r.seq.to_be_bytes());
                    mix(fnv1a(&bytes) ^ a as u64)
                }
            };
            cands[(h % cands.len() as u64) as usize].node_id
        }
        _ => {
            cands
                .iter()
                .min_by_key(|b| (b.score, b.node_id))
                .expect("at least one bid")
                .node_id
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    const TIMEOUT: Duration = Duration::from_millis(100);
    const DEGRADED: Duration = Duration::from_secs(3);

    fn core(me: NodeId, mode: ElectionMode) -> Core {
        Core::new(Params {
            me,
            cluster_size: 3,
            mode,
            policy: Policy::Composite,
            bid_timeout: TIMEOUT,
            degraded_after: DEGRADED,
            min_tenure: Duration::ZERO,
        })
    }

    fn core_with_tenure(me: NodeId, min_tenure: Duration) -> Core {
        let mut c = core(me, ElectionMode::OnDemand);
        c.p.min_tenure = min_tenure;
        c
    }

    fn rid(seq: u64) -> RequestId {
        RequestId {
            client_id: Uuid::from_u64_pair(1, 2),
            seq,
        }
    }

    fn bid(node_id: NodeId, score: u32, incumbent: Option<NodeId>) -> Bid {
        Bid {
            node_id,
            score,
            incumbent,
        }
    }

    fn prev(attempt: u32, epoch: Epoch) -> Option<Previous> {
        Some(Previous {
            attempt,
            epoch,
            completed: false,
        })
    }

    fn sent_bid(actions: &[Action]) -> Option<(Epoch, u32, Option<NodeId>)> {
        actions.iter().find_map(|a| match a {
            Action::Broadcast(Message::ElectBid {
                target_epoch,
                score,
                incumbent,
                ..
            }) => Some((*target_epoch, *score, *incumbent)),
            _ => None,
        })
    }

    fn winner(actions: &[Action]) -> Option<NodeId> {
        actions.iter().find_map(|a| match a {
            Action::Log(Record {
                outcome: Outcome::Won(w),
                ..
            }) => Some(*w),
            _ => None,
        })
    }

    /// Runs a 3-bid election on `c` that `server` wins (lowest score).
    fn elect(c: &mut Core, now: Instant, server: NodeId) {
        let t = c.epoch() + 1;
        let me = c.p.me;
        let s = move |n: NodeId| if n == server { 0 } else { 900 };
        for n in (1..=3).filter(|n| *n != me) {
            c.on_bid(now, t, bid(n, s(n), None), None, 0, &|| s(me));
        }
        assert_eq!((c.epoch(), c.server()), (t, Some(server)));
    }

    #[test]
    fn same_winner_for_any_bid_order() {
        let bids = [bid(1, 300, None), bid(2, 120, None), bid(3, 500, None)];
        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for me in 1..=3 {
            for order in orders {
                let mut c = core(me, ElectionMode::OnDemand);
                let now = Instant::now();
                let own = bids[(me - 1) as usize].score;
                let mut acts = Vec::new();
                for i in order {
                    let b = bids[i];
                    if b.node_id != me {
                        acts.extend(c.on_bid(now, 1, b, None, 0, &|| own));
                    }
                }
                assert_eq!(winner(&acts), Some(2), "me {me} order {order:?}");
                assert_eq!((c.epoch(), c.server()), (1, Some(2)));
            }
        }
    }

    #[test]
    fn tie_breaks_by_lower_node_id() {
        let mut m = BTreeMap::new();
        for n in [3, 1, 2] {
            m.insert(n, bid(n, 250, None));
        }
        assert_eq!(
            decide_winner(ElectionMode::OnDemand, Policy::Composite, Key::Epoch(1), &m),
            1
        );
        m.insert(1, bid(1, 251, None));
        assert_eq!(
            decide_winner(ElectionMode::OnDemand, Policy::Composite, Key::Epoch(1), &m),
            2
        );
    }

    #[test]
    fn incumbent_excluded_unless_only_candidate() {
        let mut m = BTreeMap::new();
        m.insert(1, bid(1, 0, Some(1))); // the incumbent, idle (e.g. client link broken)
        m.insert(2, bid(2, 700, Some(1)));
        m.insert(3, bid(3, 800, Some(1)));
        assert_eq!(
            decide_winner(ElectionMode::OnDemand, Policy::Composite, Key::Epoch(2), &m),
            2
        );
        // Per-request mode has no incumbent rule.
        assert_eq!(
            decide_winner(
                ElectionMode::PerRequest,
                Policy::Composite,
                Key::Epoch(2),
                &m
            ),
            1
        );
        // Only the incumbent left: it may win.
        let only: BTreeMap<_, _> = [(1, bid(1, 0, Some(1)))].into();
        assert_eq!(
            decide_winner(
                ElectionMode::OnDemand,
                Policy::Composite,
                Key::Epoch(2),
                &only
            ),
            1
        );
    }

    #[test]
    fn decides_with_two_bids_after_timeout() {
        let mut c = core(2, ElectionMode::OnDemand);
        let t0 = Instant::now();
        let acts = c.on_submit(t0, rid(1), 1, None, &|| 400);
        assert_eq!(sent_bid(&acts), Some((1, 400, None)));
        assert!(acts.contains(&Action::Timer(Key::Epoch(1), TIMEOUT)));
        let acts = c.on_bid(t0, 1, bid(3, 100, None), None, 0, &|| 400);
        assert!(
            winner(&acts).is_none(),
            "2 of 3 bids: wait for the third or the timeout"
        );
        let acts = c.on_timeout(t0 + TIMEOUT, Key::Epoch(1));
        assert_eq!(winner(&acts), Some(3));
        assert_eq!((c.epoch(), c.server()), (1, Some(3)));
    }

    #[test]
    fn one_bid_is_not_a_decision() {
        let mut c = core(1, ElectionMode::OnDemand);
        let t0 = Instant::now();
        c.on_submit(t0, rid(1), 1, None, &|| 0);
        let acts = c.on_timeout(t0 + TIMEOUT, Key::Epoch(1));
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::Log(Record {
                outcome: Outcome::Alone,
                ..
            })
        )));
        assert_eq!((c.epoch(), c.server(), c.degraded()), (0, None, false));
    }

    #[test]
    fn rejoining_an_election_resends_the_same_bid() {
        // Node 2 bids 100, its timer fires before node 3's bid arrives (no
        // decision). Node 3 decided from node 2's bid of 100; when node 3's
        // late bid reaches node 2, node 2 must decide from the same bids.
        let mut c = core(2, ElectionMode::OnDemand);
        let t0 = Instant::now();
        let acts = c.on_submit(t0, rid(1), 1, None, &|| 100);
        assert_eq!(sent_bid(&acts), Some((1, 100, None)));
        c.on_timeout(t0 + TIMEOUT, Key::Epoch(1));
        assert_eq!(c.server(), None);
        let later = t0 + TIMEOUT * 3;
        let acts = c.on_bid(later, 1, bid(3, 150, None), None, 0, &|| 900);
        assert_eq!(
            sent_bid(&acts),
            Some((1, 100, None)),
            "same bid as before, not a fresh 900"
        );
        let acts = c.on_timeout(later + TIMEOUT, Key::Epoch(1));
        assert_eq!(winner(&acts), Some(2));
        // Even after a long delay, the same target epoch must keep its bid.
        let mut c = core(2, ElectionMode::OnDemand);
        c.on_submit(t0, rid(1), 1, None, &|| 100);
        c.on_timeout(t0 + TIMEOUT, Key::Epoch(1));
        let acts = c.on_submit(t0 + Duration::from_secs(2), rid(2), 1, None, &|| 700);
        assert_eq!(sent_bid(&acts), Some((1, 100, None)));
    }

    #[test]
    fn delayed_bid_beyond_the_old_cache_window_cannot_make_survivors_follow_each_other() {
        let t = Instant::now();
        let mut a = core(1, ElectionMode::OnDemand);
        let mut b = core(2, ElectionMode::OnDemand);
        a.on_submit(t, rid(1), 1, None, &|| 0);
        a.on_timeout(t + TIMEOUT, Key::Epoch(1));
        let later = t + Duration::from_millis(1100);
        b.on_bid(later, 1, bid(1, 0, None), None, 0, &|| 10);
        b.on_timeout(later + TIMEOUT, Key::Epoch(1));
        a.on_bid(later + TIMEOUT, 1, bid(2, 10, None), None, 0, &|| 20);
        a.on_timeout(later + TIMEOUT * 2, Key::Epoch(1));
        assert_eq!(a.server(), Some(1));
        assert_eq!(b.server(), Some(1));
        assert!(a.serving());
    }

    #[test]
    fn retry_triggers_wait_for_min_tenure_but_no_server_is_exempt() {
        let tenure = Duration::from_secs(1);
        let t0 = Instant::now();
        let mut c = core_with_tenure(2, tenure);
        elect(&mut c, t0, 1); // server 1 since t0, epoch 1

        // A retry 300 ms into server 1's tenure: suppressed, no bid, no step-down.
        let early = t0 + Duration::from_millis(300);
        let acts = c.on_submit(early, rid(1), 2, prev(1, 1), &|| 0);
        assert!(sent_bid(&acts).is_none());
        assert_eq!(
            acts,
            vec![Action::Suppressed {
                request_id: rid(1),
                epoch: 1,
                server: 1,
                held_for: Duration::from_millis(300)
            }]
        );
        assert_eq!((c.server(), c.counters.suppressed), (Some(1), 1));

        // After the tenure: the retry triggers an election naming server 1.
        let late = t0 + tenure;
        let acts = c.on_submit(late, rid(2), 2, prev(1, 1), &|| 900);
        assert_eq!(sent_bid(&acts), Some((2, 900, Some(1))));

        // The new server (3) wins epoch 2 at `late`: its tenure restarts.
        c.on_bid(late, 2, bid(3, 0, Some(1)), None, 0, &|| 900);
        c.on_bid(late, 2, bid(1, 900, Some(1)), None, 0, &|| 900);
        assert_eq!((c.epoch(), c.server()), (2, Some(3)));
        let acts = c.on_submit(
            late + Duration::from_millis(10),
            rid(3),
            2,
            prev(1, 2),
            &|| 0,
        );
        assert!(matches!(
            acts[..],
            [Action::Suppressed {
                server: 3,
                epoch: 2,
                ..
            }]
        ));

        // "No server known" is exempt: a fresh node elects at once.
        let mut c = core_with_tenure(2, tenure);
        let acts = c.on_submit(t0, rid(4), 1, None, &|| 0);
        assert_eq!(sent_bid(&acts), Some((1, 0, None)));
        // So is a retry while no server is known (server stepped down / unknown).
        let mut c = core_with_tenure(2, tenure);
        c.observe_client_epoch(t0, 3);
        let acts = c.on_submit(t0, rid(5), 2, prev(1, 3), &|| 0);
        assert_eq!(sent_bid(&acts), Some((4, 0, None)));
    }

    #[test]
    fn winner_takes_over() {
        let mut c = core(1, ElectionMode::OnDemand);
        let t0 = Instant::now();
        c.on_submit(t0, rid(1), 1, None, &|| 0);
        c.on_bid(t0, 1, bid(2, 500, None), None, 0, &|| 0);
        let acts = c.on_bid(t0, 1, bid(3, 500, None), None, 0, &|| 0);
        assert!(acts.contains(&Action::BecameServer));
        assert!(c.serving());
    }

    #[test]
    fn retry_in_current_epoch_triggers_old_epoch_retry_does_not() {
        let now = Instant::now();
        let mut c = core(2, ElectionMode::OnDemand);
        elect(&mut c, now, 1); // epoch 1, server 1

        // Attempt 1 arrived in epoch 0 (an older epoch): no election.
        assert!(c.on_submit(now, rid(7), 2, prev(1, 0), &|| 0).is_empty());
        // A duplicate of an attempt already seen: no election.
        assert!(c.on_submit(now, rid(8), 2, prev(2, 1), &|| 0).is_empty());
        // Completed requests never trigger.
        let done = Some(Previous {
            attempt: 1,
            epoch: 1,
            completed: true,
        });
        assert!(c.on_submit(now, rid(9), 2, done, &|| 0).is_empty());

        // Attempt 1 arrived in this epoch: election for epoch 2, naming 1.
        let acts = c.on_submit(now, rid(10), 2, prev(1, 1), &|| 0);
        assert_eq!(sent_bid(&acts), Some((2, 0, Some(1))));
        assert_eq!(c.server(), None);
        // Further retries join the same election.
        assert!(c.on_submit(now, rid(11), 2, prev(1, 1), &|| 0).is_empty());
    }

    #[test]
    fn server_steps_down_on_retry_and_on_any_higher_epoch() {
        let now = Instant::now();

        // A retry reaching the server itself: it steps down and bids as incumbent.
        let mut c = core(1, ElectionMode::OnDemand);
        elect(&mut c, now, 1);
        let acts = c.on_submit(now, rid(1), 2, prev(1, 1), &|| 0);
        assert!(acts.contains(&Action::StepDown));
        assert_eq!(sent_bid(&acts), Some((2, 0, Some(1))));
        assert!(!c.serving());

        // A higher-epoch bid.
        let mut c = core(1, ElectionMode::OnDemand);
        elect(&mut c, now, 1);
        let acts = c.on_bid(now, 2, bid(3, 0, Some(1)), None, 0, &|| 0);
        assert!(acts.contains(&Action::StepDown));
        assert_eq!(sent_bid(&acts), Some((2, 0, Some(1))));

        // A higher-epoch Replicate: adopt the epoch and its server.
        let mut c = core(1, ElectionMode::OnDemand);
        elect(&mut c, now, 1);
        let acts = c.on_replicate(now, 4, 2);
        assert!(acts.contains(&Action::StepDown));
        assert_eq!((c.epoch(), c.server()), (4, Some(2)));

        // A higher client_epoch: adopt it, server unknown.
        let mut c = core(1, ElectionMode::OnDemand);
        elect(&mut c, now, 1);
        let acts = c.observe_client_epoch(now, 3);
        assert!(acts.contains(&Action::StepDown));
        assert_eq!((c.epoch(), c.server()), (3, None));
        // ...and the next request elects a server for epoch 4.
        let acts = c.on_submit(now, rid(2), 1, None, &|| 0);
        assert_eq!(sent_bid(&acts), Some((4, 0, None)));
    }

    #[test]
    fn old_bids_are_ignored_higher_target_replaces() {
        let now = Instant::now();
        let mut c = core(3, ElectionMode::OnDemand);
        elect(&mut c, now, 1);
        assert!(c.on_bid(now, 1, bid(2, 0, None), None, 0, &|| 0).is_empty());
        c.on_submit(now, rid(1), 2, prev(1, 1), &|| 0); // election for 2
        let acts = c.on_bid(now, 5, bid(2, 0, None), None, 0, &|| 0); // jump to 5
        assert!(acts.iter().any(|a| matches!(
            a,
            Action::Log(Record {
                outcome: Outcome::Abandoned,
                key: Key::Epoch(2),
                ..
            })
        )));
        assert_eq!(sent_bid(&acts).map(|b| b.0), Some(5));
        assert_eq!(c.epoch(), 4);
    }

    #[test]
    fn split_winner_lower_node_id_keeps_the_role() {
        let now = Instant::now();
        // Node 2 thinks it won epoch 1; a Replicate from node 1 for epoch 1 arrives.
        let mut c = core(2, ElectionMode::OnDemand);
        elect(&mut c, now, 2);
        let acts = c.on_replicate(now, 1, 1);
        assert!(acts.contains(&Action::StepDown));
        assert!(acts.contains(&Action::Split {
            epoch: 1,
            other: 1,
            server: 1
        }));
        assert_eq!(c.server(), Some(1));

        // Node 1 thinks it won; a Replicate from node 3: node 1 keeps it.
        let mut c = core(1, ElectionMode::OnDemand);
        elect(&mut c, now, 1);
        let acts = c.on_replicate(now, 1, 3);
        assert!(!acts.contains(&Action::StepDown));
        assert_eq!(c.server(), Some(1));
        assert_eq!(c.counters.splits, 1);

        // A bystander that saw server 3 then hears from 2: picks 2.
        let mut c = core(1, ElectionMode::OnDemand);
        elect(&mut c, now, 3);
        c.on_replicate(now, 1, 2);
        assert_eq!(c.server(), Some(2));
    }

    #[test]
    fn degraded_only_after_three_seconds_alone() {
        let mut c = core(1, ElectionMode::OnDemand);
        let t0 = Instant::now();
        let mut seq = 0;
        let mut run = |c: &mut Core, at: Duration| {
            seq += 1;
            c.on_submit(t0 + at, rid(seq), 1, None, &|| 0);
            c.on_timeout(t0 + at + TIMEOUT, Key::Epoch(1))
        };
        for s in [0, 1, 2] {
            run(&mut c, Duration::from_secs(s));
            assert!(!c.degraded(), "alone for {s}.1 s is not enough");
            assert_eq!(c.server(), None);
        }
        let acts = run(&mut c, Duration::from_millis(2_950)); // ends at 3.05 s
        assert!(c.degraded());
        assert!(acts.contains(&Action::BecameServer));
        assert!(c.serving());
        assert_eq!(c.epoch(), 0, "the epoch does not change while degraded");
        // Retries don't trigger elections while degraded.
        assert!(c
            .on_submit(t0 + Duration::from_secs(4), rid(99), 2, prev(1, 0), &|| 0)
            .is_empty());
        // Any peer message ends degraded mode.
        c.on_replicate(t0 + Duration::from_secs(5), 0, 2);
        assert!(!c.degraded());
        assert_eq!(c.server(), Some(2));
    }

    #[test]
    fn a_two_bid_election_resets_the_alone_clock() {
        let mut c = core(1, ElectionMode::OnDemand);
        let t0 = Instant::now();
        c.on_submit(t0, rid(1), 1, None, &|| 0);
        c.on_timeout(t0 + TIMEOUT, Key::Epoch(1)); // alone since t0
        c.on_bid(
            t0 + Duration::from_secs(2),
            1,
            bid(2, 0, None),
            None,
            0,
            &|| 0,
        );
        c.on_timeout(t0 + Duration::from_secs(2) + TIMEOUT, Key::Epoch(1)); // decided, 1 wins
        assert_eq!(c.server(), Some(1));
        assert!(!c.degraded());
    }

    #[test]
    fn per_request_mode_elects_per_attempt_without_incumbent_rule() {
        let now = Instant::now();
        let mut c = core(2, ElectionMode::PerRequest);
        let acts = c.on_submit(now, rid(1), 1, None, &|| 100);
        let key = Key::Request(rid(1), 1);
        assert!(acts.contains(&Action::Timer(key, TIMEOUT)));
        c.on_bid(now, 0, bid(1, 300, None), Some(rid(1)), 1, &|| 100);
        let acts = c.on_bid(now, 0, bid(3, 200, None), Some(rid(1)), 1, &|| 100);
        assert!(acts.contains(&Action::Execute(rid(1))));
        assert_eq!(c.epoch(), 0);
        // A late bid for the finished key doesn't restart it.
        assert!(c
            .on_bid(now, 0, bid(3, 0, None), Some(rid(1)), 1, &|| 100)
            .is_empty());
        // A peer's bid for a request we haven't seen yet: we join.
        let acts = c.on_bid(now, 0, bid(1, 0, None), Some(rid(2)), 1, &|| 100);
        assert!(sent_bid(&acts).is_some());
    }

    #[test]
    fn round_robin_is_deterministic_and_spreads() {
        let bids: BTreeMap<_, _> = (1..=3).map(|n| (n, bid(n, 0, None))).collect();
        let pick = |k| decide_winner(ElectionMode::PerRequest, Policy::RoundRobin, k, &bids);
        let mut seen = BTreeSet::new();
        for s in 0..60 {
            let k = Key::Request(rid(s), 1);
            assert_eq!(pick(k), pick(k));
            seen.insert(pick(k));
        }
        assert_eq!(seen.len(), 3);
    }
}
