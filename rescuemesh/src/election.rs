//! Leaderless per-request load-bidding election (Phase 1, bullet 2).
//!
//! Every node that receives a client's `Submit` computes a load score *at that moment*
//! (the estimated completion time of one more request on this node) and broadcasts it
//! to its peers as `Bid{request_id, attempt, node, score_us}`. A node decides once it
//! holds a bid from every node in its live set, or when the bid timeout expires.
//! The winner is the minimum of `(score_bucket(score), tiebreak(request_id, node))`
//! over the received bids, a pure function of the bid set, so every node that saw the
//! same bids elects the same executor without a coordinator or a second round.
//!
//! This module is sans-IO: the node feeds it bids and clock readings (monotonic µs)
//! and acts on the decisions. It also holds the load estimator behind the score.
//!
//! Ideas borrowed from the unmerged teammate PR #2 (`feature/election`): keep the
//! first bid per node, order bids by a deterministic `(score, tiebreak)` key, and
//! carry a request+attempt pair so a later takeover round can be fenced.

use crate::assign::fnv1a;
use crate::model::{NodeId, RequestId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Decision function
// ---------------------------------------------------------------------------

/// Mantissa bits kept when comparing scores: scores within ~6-12% of each other fall
/// in the same bucket and count as a tie. Differences that small are below the noise
/// of the service-time estimate, and treating them as ties lets the per-request
/// tiebreak spread work evenly instead of always favouring whichever node's EWMA is
/// a hair lower.
const SCORE_MANTISSA_BITS: u32 = 3;

/// Integer log-scale bucket of a score (a tiny float: exponent + 3 mantissa bits).
/// Monotone non-decreasing and integer-only, so every node computes it identically.
pub fn score_bucket(score_us: u64) -> u64 {
    let m = SCORE_MANTISSA_BITS;
    if score_us < (1 << (m + 1)) {
        return score_us;
    }
    let e = 63 - score_us.leading_zeros(); // index of the top set bit, >= m + 1
    let mantissa = (score_us >> (e - m)) & ((1 << m) - 1);
    (((e - m) as u64 + 1) << m) | mantissa
}

/// Per-request pseudo-random order of the nodes. Raw node ids would hand every tie to
/// node 1; FNV-1a of `request_id # node` (plus a splitmix finalizer, because FNV-1a
/// barely mixes the last input byte into the high bits) rotates ties evenly.
pub fn tiebreak(request_id: &str, node: NodeId) -> u64 {
    let mut b = Vec::with_capacity(request_id.len() + 5);
    b.extend_from_slice(request_id.as_bytes());
    b.push(b'#');
    b.extend_from_slice(&node.to_be_bytes());
    let mut z = fnv1a(&b);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The election rule: lowest `(bucket(score), tiebreak, node)` wins.
pub fn winner(request_id: &str, bids: impl IntoIterator<Item = (NodeId, u64)>) -> Option<NodeId> {
    bids.into_iter()
        .min_by_key(|&(n, s)| (score_bucket(s), tiebreak(request_id, n), n))
        .map(|(n, _)| n)
}

// ---------------------------------------------------------------------------
// Load metric
// ---------------------------------------------------------------------------

/// Estimated completion time (µs) of one more request on a node: its own service
/// time plus the queueing delay behind `outstanding` requests shared over `workers`
/// planner threads. Strictly increasing in `outstanding`, so a burst spreads out.
pub fn score_us(service_est_us: f64, outstanding: u32, workers: u32) -> u64 {
    let w = workers.max(1) as f64;
    (service_est_us.max(1.0) * (1.0 + outstanding as f64 / w)).round() as u64
}

/// Service-time estimate = (work per request) × (current CPU slowness).
///
/// * Work per request is an EWMA (α = 0.2) of measured planner wall time divided by
///   the CPU slowness at the time of the sample, i.e. in "baseline CPU" units.
/// * CPU slowness is `median(last 5 probes) / lowest such median seen`, where the
///   probe times a fixed CPU loop every 200 ms. The rolling median ignores single
///   outliers (e.g. a probe landing on an efficiency core of a big.LITTLE CPU) and
///   reacts to a sustained change within 3 probes. It moves even when the node completes nothing, so a node that was
///   slow and has been avoided is picked again as soon as its CPU recovers (a plain
///   completion-driven EWMA would freeze at the slow value and starve the node).
///   It also captures external CPU contention when nodes run on separate machines.
/// * Time decay: samples that complete during the probe's reaction lag inflate the
///   work EWMA, and an avoided node would never correct it. So after `IDLE_US`
///   without a completion the work estimate decays (half-life 1 s) toward the lowest
///   work estimate this node has had; the next request it wins re-measures it.
#[derive(Clone, Debug)]
pub struct ServiceEstimator {
    work_us: f64,
    min_work_us: f64,
    probe_base_us: f64,
    probes: std::collections::VecDeque<f64>,
    ratio: f64,
    last_completion_us: u64,
    last_probe_us: u64,
    pub samples: u64,
}

const SERVICE_ALPHA: f64 = 0.2;
const IDLE_US: u64 = 1_000_000;
const DECAY_HALF_LIFE_US: f64 = 1_000_000.0;
/// Samples before the work EWMA may set the decay floor.
const MIN_WARMUP: u64 = 5;
const PROBE_WINDOW: usize = 5;

impl ServiceEstimator {
    /// `prior_us` stands in for the work per request until the first completion.
    pub fn new(prior_us: f64) -> Self {
        let w = prior_us.max(1.0);
        ServiceEstimator {
            work_us: w,
            min_work_us: w,
            probe_base_us: f64::INFINITY,
            probes: std::collections::VecDeque::with_capacity(PROBE_WINDOW),
            ratio: 1.0,
            last_completion_us: 0,
            last_probe_us: 0,
            samples: 0,
        }
    }

    pub fn on_probe(&mut self, probe_us: f64, now_us: u64) {
        if probe_us > 0.0 && probe_us.is_finite() {
            if self.probes.len() == PROBE_WINDOW {
                self.probes.pop_front();
            }
            self.probes.push_back(probe_us);
            let mut w: Vec<f64> = self.probes.iter().copied().collect();
            w.sort_by(f64::total_cmp);
            let median = w[w.len() / 2];
            self.probe_base_us = self.probe_base_us.min(median);
            self.ratio = (median / self.probe_base_us).max(1.0);
        }
        let dt = now_us.saturating_sub(self.last_probe_us.max(self.last_completion_us + IDLE_US));
        if now_us >= self.last_completion_us + IDLE_US && dt > 0 {
            let keep = 0.5f64.powf(dt as f64 / DECAY_HALF_LIFE_US);
            self.work_us = self.min_work_us + (self.work_us - self.min_work_us) * keep;
        }
        self.last_probe_us = now_us;
    }

    pub fn on_completion(&mut self, service_us: f64, now_us: u64) {
        let work = service_us / self.ratio;
        self.work_us = if self.samples == 0 { work } else { (1.0 - SERVICE_ALPHA) * self.work_us + SERVICE_ALPHA * work };
        self.samples += 1;
        if self.samples == MIN_WARMUP {
            self.min_work_us = self.work_us;
        } else if self.samples > MIN_WARMUP {
            self.min_work_us = self.min_work_us.min(self.work_us);
        }
        self.last_completion_us = now_us;
    }

    /// A node with no completions of its own assumes the work per request its peers
    /// measure (requests are statistically identical whichever node plans them).
    pub fn seed_prior(&mut self, work_us: f64) {
        if self.samples == 0 && work_us > 0.0 && work_us.is_finite() {
            self.work_us = work_us;
            self.min_work_us = work_us;
        }
    }

    /// Work per request in this node's baseline-CPU units.
    pub fn work_us(&self) -> f64 {
        self.work_us
    }

    pub fn estimate_us(&self) -> f64 {
        self.work_us * self.ratio
    }

    pub fn probe_ratio(&self) -> f64 {
        self.ratio
    }
}

// ---------------------------------------------------------------------------
// Round state machine
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BidOutcome {
    Recorded,
    /// A second bid from the same node for the same round (the first one is kept).
    Duplicate,
    /// Arrived after the decision, or for another attempt: ignored except in metrics.
    Late,
}

struct Round {
    attempt: u32,
    created_us: u64,
    /// When our own Submit arrived and we bid. A node without the Submit never bids
    /// and never decides (it could not execute the request anyway).
    local_us: Option<u64>,
    bids: BTreeMap<NodeId, u64>,
    decided: Option<(NodeId, u64)>,
    /// We won and the executor has not enqueued the work yet; counted in the score.
    reserved: bool,
    waiters: Vec<oneshot::Sender<NodeId>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ElectionStats {
    pub decided: u64,
    pub won: u64,
    pub decided_full: u64,
    pub decided_timeout: u64,
    pub bids_sent: u64,
    pub bid_bytes_sent: u64,
    pub bids_received: u64,
    pub duplicate_bids: u64,
    pub late_bids: u64,
    pub orphan_rounds: u64,
    pub open_rounds: usize,
    /// Election duration: local Submit receipt -> decision.
    pub duration_mean_us: f64,
    pub duration_p50_us: u64,
    pub duration_p99_us: u64,
    pub duration_max_us: u64,
}

const DURATION_RING: usize = 1 << 16;

pub struct Elections {
    me: NodeId,
    rounds: HashMap<RequestId, Round>,
    reserved: u32,
    stats: ElectionStats,
    durations: Vec<u64>,
    ring_next: usize,
    duration_sum: u128,
}

impl Elections {
    pub fn new(me: NodeId) -> Self {
        Elections {
            me,
            rounds: HashMap::new(),
            reserved: 0,
            stats: ElectionStats::default(),
            durations: Vec::new(),
            ring_next: 0,
            duration_sum: 0,
        }
    }

    fn round(&mut self, rid: &str, attempt: u32, now_us: u64) -> &mut Round {
        self.rounds.entry(rid.to_string()).or_insert_with(|| Round {
            attempt,
            created_us: now_us,
            local_us: None,
            bids: BTreeMap::new(),
            decided: None,
            reserved: false,
            waiters: Vec::new(),
        })
    }

    /// Our Submit arrived: record our own bid. Returns true if it is new and must be
    /// broadcast; false for a repeated Submit (client retry), which re-uses the round.
    pub fn local_bid(&mut self, rid: &str, attempt: u32, score_us: u64, now_us: u64) -> bool {
        let me = self.me;
        let r = self.round(rid, attempt, now_us);
        if r.local_us.is_some() || r.attempt != attempt {
            return false;
        }
        r.local_us = Some(now_us);
        r.bids.insert(me, score_us);
        true
    }

    pub fn peer_bid(&mut self, rid: &str, attempt: u32, node: NodeId, score_us: u64, now_us: u64) -> BidOutcome {
        self.stats.bids_received += 1;
        let r = self.round(rid, attempt, now_us);
        let outcome = if r.decided.is_some() || r.attempt != attempt {
            BidOutcome::Late
        } else if r.bids.contains_key(&node) {
            BidOutcome::Duplicate
        } else {
            r.bids.insert(node, score_us);
            BidOutcome::Recorded
        };
        match outcome {
            BidOutcome::Late => self.stats.late_bids += 1,
            BidOutcome::Duplicate => self.stats.duplicate_bids += 1,
            BidOutcome::Recorded => {}
        }
        outcome
    }

    /// A bid for a request that is already committed here (counted, not stored).
    pub fn note_late(&mut self) {
        self.stats.bids_received += 1;
        self.stats.late_bids += 1;
    }

    pub fn note_sent(&mut self, msgs: usize, bytes: usize) {
        self.stats.bids_sent += msgs as u64;
        self.stats.bid_bytes_sent += bytes as u64;
    }

    /// Decide if we have bid and every node in `live` has bid too.
    pub fn try_decide(&mut self, rid: &str, live: &[NodeId], now_us: u64) -> Option<NodeId> {
        let r = self.rounds.get(rid)?;
        if r.decided.is_some() || r.local_us.is_none() || !live.iter().all(|n| r.bids.contains_key(n)) {
            return None;
        }
        Some(self.finish(rid, true, now_us))
    }

    /// Bid timeout: decide on the bids received so far (always includes our own).
    pub fn decide_on_timeout(&mut self, rid: &str, now_us: u64) -> Option<NodeId> {
        let r = self.rounds.get(rid)?;
        if let Some((w, _)) = r.decided {
            return Some(w);
        }
        r.local_us?;
        Some(self.finish(rid, false, now_us))
    }

    fn finish(&mut self, rid: &str, full: bool, now_us: u64) -> NodeId {
        let me = self.me;
        let r = self.rounds.get_mut(rid).expect("round exists");
        let w = winner(rid, r.bids.iter().map(|(n, s)| (*n, *s))).expect("own bid present");
        r.decided = Some((w, now_us));
        for tx in r.waiters.drain(..) {
            let _ = tx.send(w);
        }
        let took = now_us.saturating_sub(r.local_us.unwrap_or(now_us));
        if w == me {
            r.reserved = true;
            self.reserved += 1;
            self.stats.won += 1;
        }
        self.stats.decided += 1;
        if full {
            self.stats.decided_full += 1;
        } else {
            self.stats.decided_timeout += 1;
        }
        self.duration_sum += took as u128;
        self.stats.duration_max_us = self.stats.duration_max_us.max(took);
        if self.durations.len() < DURATION_RING {
            self.durations.push(took);
        } else {
            self.durations[self.ring_next] = took;
            self.ring_next = (self.ring_next + 1) % DURATION_RING;
        }
        w
    }

    pub fn decision(&self, rid: &str) -> Option<NodeId> {
        self.rounds.get(rid).and_then(|r| r.decided.map(|(w, _)| w))
    }

    /// Wake-up for a task waiting on an undecided round.
    pub fn subscribe(&mut self, rid: &str) -> Option<oneshot::Receiver<NodeId>> {
        let r = self.rounds.get_mut(rid)?;
        let (tx, rx) = oneshot::channel();
        match r.decided {
            Some((w, _)) => {
                let _ = tx.send(w);
            }
            None => r.waiters.push(tx),
        }
        Some(rx)
    }

    /// The executor has enqueued (or skipped) a request it won: release the
    /// reservation. Idempotent.
    pub fn start_execution(&mut self, rid: &str) {
        if let Some(r) = self.rounds.get_mut(rid) {
            if std::mem::take(&mut r.reserved) {
                self.reserved -= 1;
            }
        }
    }

    /// Requests won here but not yet enqueued.
    pub fn reserved(&self) -> u32 {
        self.reserved
    }

    /// Drop decided rounds after `decided_ttl_us` and never-decided ones (bids for a
    /// Submit we never got) after `orphan_ttl_us`, so memory stays bounded.
    pub fn gc(&mut self, now_us: u64, decided_ttl_us: u64, orphan_ttl_us: u64) {
        let mut released = 0;
        let mut orphans = 0;
        self.rounds.retain(|_, r| {
            let keep = match r.decided {
                Some((_, at)) => now_us.saturating_sub(at) < decided_ttl_us,
                None => now_us.saturating_sub(r.created_us) < orphan_ttl_us,
            };
            if !keep {
                released += r.reserved as u32;
                orphans += r.decided.is_none() as u64;
            }
            keep
        });
        self.reserved -= released;
        self.stats.orphan_rounds += orphans;
    }

    pub fn stats(&self) -> ElectionStats {
        let mut s = self.stats.clone();
        s.open_rounds = self.rounds.len();
        if !self.durations.is_empty() {
            let mut d = self.durations.clone();
            d.sort_unstable();
            let pct = |p: f64| d[((p / 100.0) * (d.len() - 1) as f64).round() as usize];
            s.duration_p50_us = pct(50.0);
            s.duration_p99_us = pct(99.0);
            s.duration_mean_us = self.duration_sum as f64 / s.decided.max(1) as f64;
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [NodeId; 3] = [1, 2, 3];

    #[test]
    fn lowest_score_wins() {
        assert_eq!(winner("r1", [(1, 9_000), (2, 3_000), (3, 6_000)]), Some(2));
        assert_eq!(winner("r1", [(1, 9_000), (3, 6_000)]), Some(3));
        assert_eq!(winner("r1", std::iter::empty()), None);
    }

    #[test]
    fn same_bids_same_winner_regardless_of_order() {
        let a = winner("req-42", [(1, 5_000), (2, 5_100), (3, 9_000)]);
        let b = winner("req-42", [(3, 9_000), (2, 5_100), (1, 5_000)]);
        assert_eq!(a, b);
    }

    #[test]
    fn ties_spread_evenly() {
        let mut wins = [0u32; 4];
        for i in 0..6_000 {
            let rid = format!("run-w{}-r{}", i % 7, i);
            wins[winner(&rid, ALL.map(|n| (n, 10_000))).unwrap() as usize] += 1;
        }
        for n in ALL {
            assert!((1_800..=2_200).contains(&wins[n as usize]), "{wins:?}");
        }
    }

    #[test]
    fn near_equal_scores_are_ties_but_real_gaps_are_not() {
        assert_eq!(score_bucket(10_000), score_bucket(10_200));
        assert!(score_bucket(10_000) < score_bucket(13_000));
        let mut prev = 0;
        for s in (0..200_000).step_by(7) {
            let b = score_bucket(s);
            assert!(b >= prev);
            prev = b;
        }
    }

    #[test]
    fn score_increases_with_every_outstanding_request() {
        for w in [1, 3, 10] {
            let mut prev = 0;
            for n in 0..50 {
                let s = score_us(10_000.0, n, w);
                assert!(s > prev, "w={w} n={n}");
                prev = s;
            }
        }
        assert!(score_us(40_000.0, 0, 3) > score_us(10_000.0, 8, 3));
        assert!(score_us(10_000.0, 2, 3) < score_us(20_000.0, 2, 3));
    }

    #[test]
    fn probe_tracks_slowdown_and_recovery_without_completions() {
        let mut e = ServiceEstimator::new(10_000.0);
        for _ in 0..5 {
            e.on_probe(300.0, 0);
        }
        e.on_completion(10_000.0, 0);
        assert!((e.estimate_us() - 10_000.0).abs() < 1.0);
        e.on_probe(9_000.0, 100_000); // one outlier probe is ignored
        assert!((e.estimate_us() - 10_000.0).abs() < 1.0);
        for k in 0..3 {
            e.on_probe(1_200.0, 200_000 + k); // CPU now 4x slower (sustained)
        }
        assert!((e.estimate_us() - 40_000.0).abs() < 1.0);
        e.on_completion(40_000.0, 300_000); // a slowed sample is still the same work
        assert!((e.estimate_us() - 40_000.0).abs() < 1.0);
        for k in 0..5 {
            e.on_probe(300.0, 400_000 + k); // recovered, with no completions in between
        }
        assert!((e.estimate_us() - 10_000.0).abs() < 1.0);
    }

    #[test]
    fn prior_comes_from_peers_until_own_samples() {
        let mut e = ServiceEstimator::new(10_000.0);
        e.seed_prior(4_000.0);
        assert!((e.estimate_us() - 4_000.0).abs() < 1.0);
        e.on_completion(6_000.0, 1);
        e.seed_prior(1_000.0);
        assert!((e.estimate_us() - 6_000.0).abs() < 1.0);
    }

    #[test]
    fn idle_node_decays_toward_its_best_estimate() {
        let mut e = ServiceEstimator::new(10_000.0);
        let mut t = 0;
        for _ in 0..10 {
            t += 10_000;
            e.on_completion(4_000.0, t);
        }
        // Samples slowed 4x arrive before the probe notices: the work EWMA inflates.
        for _ in 0..20 {
            t += 10_000;
            e.on_completion(16_000.0, t);
        }
        assert!(e.estimate_us() > 14_000.0);
        e.on_probe(300.0, t + 500_000); // not idle long enough yet: no decay
        assert!(e.estimate_us() > 14_000.0);
        let mut prev = e.estimate_us();
        for k in 1..=30 {
            e.on_probe(300.0, t + 1_000_000 + k * 200_000);
            assert!(e.estimate_us() <= prev);
            prev = e.estimate_us();
        }
        assert!(e.estimate_us() < 4_500.0, "{}", e.estimate_us());
    }

    #[test]
    fn full_bid_set_decides_and_reserves_for_winner() {
        let mut el = Elections::new(2);
        assert!(el.local_bid("r", 0, 1_000, 10));
        assert_eq!(el.try_decide("r", &ALL, 11), None);
        assert_eq!(el.peer_bid("r", 0, 1, 5_000, 12), BidOutcome::Recorded);
        assert_eq!(el.peer_bid("r", 0, 3, 7_000, 13), BidOutcome::Recorded);
        assert_eq!(el.try_decide("r", &ALL, 14), Some(2));
        assert_eq!(el.reserved(), 1);
        el.start_execution("r");
        el.start_execution("r");
        assert_eq!(el.reserved(), 0);
        let s = el.stats();
        assert_eq!((s.decided, s.won, s.decided_full, s.duration_max_us), (1, 1, 1, 4));
    }

    #[test]
    fn bid_before_local_submit() {
        let mut el = Elections::new(1);
        el.peer_bid("r", 0, 2, 100, 1);
        el.peer_bid("r", 0, 3, 200, 2);
        // Peers' bids alone never decide: we have not seen the Submit.
        assert_eq!(el.try_decide("r", &ALL, 3), None);
        assert!(el.local_bid("r", 0, 900_000, 4));
        assert_eq!(el.try_decide("r", &ALL, 4), Some(2));
        assert_eq!(el.reserved(), 0);
    }

    #[test]
    fn duplicate_and_late_bids_are_ignored() {
        let mut el = Elections::new(1);
        el.local_bid("r", 0, 5_000, 0);
        assert!(!el.local_bid("r", 0, 1, 1), "client retry must not re-bid");
        el.peer_bid("r", 0, 2, 9_000, 1);
        assert_eq!(el.peer_bid("r", 0, 2, 1, 2), BidOutcome::Duplicate);
        el.peer_bid("r", 0, 3, 9_000, 3);
        assert_eq!(el.try_decide("r", &ALL, 4), Some(1));
        assert_eq!(el.peer_bid("r", 0, 3, 1, 5), BidOutcome::Late);
        assert_eq!(el.decision("r"), Some(1));
        let s = el.stats();
        assert_eq!((s.duplicate_bids, s.late_bids, s.bids_received), (1, 1, 4));
    }

    #[test]
    fn timeout_decides_on_partial_bid_set() {
        let mut el = Elections::new(3);
        el.local_bid("r", 0, 8_000, 0);
        el.peer_bid("r", 0, 1, 2_000, 1); // node 2 is down: its bid never comes
        assert_eq!(el.try_decide("r", &ALL, 50), None);
        let mut rx = el.subscribe("r").unwrap();
        assert_eq!(el.decide_on_timeout("r", 100_000), Some(1));
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(el.decide_on_timeout("r", 100_001), Some(1), "decision is stable");
        let s = el.stats();
        assert_eq!((s.decided, s.decided_timeout, s.won), (1, 1, 0));
    }

    #[test]
    fn gc_drops_old_rounds_and_releases_reservations() {
        let mut el = Elections::new(1);
        el.local_bid("won", 0, 1, 0);
        el.try_decide("won", &[1], 0);
        el.peer_bid("orphan", 0, 2, 5, 0);
        assert_eq!((el.reserved(), el.stats().open_rounds), (1, 2));
        el.gc(1_000, 5_000, 30_000);
        assert_eq!(el.stats().open_rounds, 2);
        el.gc(40_000, 5_000, 30_000);
        assert_eq!((el.reserved(), el.stats().open_rounds, el.stats().orphan_rounds), (0, 0, 1));
    }
}
