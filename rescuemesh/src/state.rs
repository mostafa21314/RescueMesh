//! Replicated cloud state held by every node.
//!
//! Consistency approach (Phase 1): state is a set of immutable, uniquely-identified
//! records (incidents, plans, assignments). Every change is a `ReplOp` with a unique
//! `op_id`; applying an op twice is a no-op, and applying ops in different orders
//! gives the same result because:
//!   * incidents / plans / assignments are inserted by unique key (set union),
//!   * responder commitment and resource availability are *derived* from the kept
//!     plans' assignments (busy-until = latest commitment; available = capacity minus
//!     unexpired allocations) instead of being updated in place.
//! So replicas converge once they have applied the same set of ops.
//!
//! Duplicate execution: during membership changes two nodes can both win the
//! election for one request and both commit a plan. Every replica keeps the plan
//! whose `(attempt, tiebreak(request_id, executed_by))` ranks lowest, whatever order
//! the two arrive in, and swaps the loser's assignments out. Because commitments are
//! derived, the swap is exact and replicas still converge. What this does
//! NOT prevent is two nodes over-allocating the same resource concurrently; that is
//! the lease/reservation problem handled later (Phase 2 conflict handling).

use crate::model::*;
use crate::planner::Snapshot;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ReplOp {
    /// `attempt` is the election attempt that chose the executor (0 until takeover
    /// exists); one op per (request, executor).
    PlanCommitted {
        op_id: String,
        #[serde(default)]
        attempt: u32,
        incident: Arc<Incident>,
        plan: Arc<ResponsePlan>,
    },
}

impl ReplOp {
    pub fn op_id(&self) -> &str {
        match self {
            ReplOp::PlanCommitted { op_id, .. } => op_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Pending {
    pub executor: NodeId,
    /// Election attempt that chose `executor` (input for takeover/re-election).
    pub attempt: u32,
    pub received_ms: u64,
    pub incident: Incident,
}

pub struct ClusterState {
    pub incidents: HashMap<String, Arc<Incident>>,
    /// Seeded responders; `busy_until_ms` here is the static base value.
    pub responders: Vec<Responder>,
    /// responder id -> assignment id -> expires_ms (from the kept plans)
    commitments: HashMap<String, HashMap<String, u64>>,
    pub resources: Vec<Resource>,
    /// resource id -> assignment id -> (expires_ms, quantity)
    allocations: HashMap<String, HashMap<String, (u64, u32)>>,
    pub assignments: HashMap<String, Assignment>,
    /// Idempotency cache: request id -> committed plan.
    pub plans: HashMap<RequestId, Arc<ResponsePlan>>,
    /// Attempt of each kept plan (part of its rank).
    plan_attempt: HashMap<RequestId, u32>,
    /// Second plans received for an already-planned request (only one is kept).
    pub duplicate_executions: u64,
    /// Requests this node saw but another node was chosen to execute.
    pub pending: HashMap<RequestId, Pending>,
    applied: HashSet<String>,
    /// Ops in local apply order (input for rejoin/state transfer later).
    pub log: Vec<ReplOp>,
    /// Order-independent digest of the kept plans: XOR of FNV(request_id @ executed_by),
    /// so replicas that kept different executors' plans show different digests.
    pub plans_digest: u64,
    pub version: u64,
}

impl ClusterState {
    pub fn new(responders: Vec<Responder>, resources: Vec<Resource>) -> Self {
        ClusterState {
            incidents: HashMap::new(),
            responders,
            commitments: HashMap::new(),
            resources,
            allocations: HashMap::new(),
            assignments: HashMap::new(),
            plans: HashMap::new(),
            plan_attempt: HashMap::new(),
            duplicate_executions: 0,
            pending: HashMap::new(),
            applied: HashSet::new(),
            log: Vec::new(),
            plans_digest: 0,
            version: 0,
        }
    }

    /// Record an incident seen directly from a client multicast (not replicated:
    /// every node receives the multicast; the committed plan carries it anyway).
    pub fn observe_incident(&mut self, inc: &Incident) {
        self.incidents.entry(inc.id.clone()).or_insert_with(|| Arc::new(inc.clone()));
    }

    /// Apply a replicated op. Returns false if it was already applied.
    pub fn apply(&mut self, op: ReplOp) -> bool {
        let ReplOp::PlanCommitted { op_id, attempt, incident, plan } = &op;
        if !self.applied.insert(format!("{op_id}@{}", plan.executed_by)) {
            return false;
        }
        let mut inc = (**incident).clone();
        inc.status = IncidentStatus::Planned;
        self.incidents.insert(inc.id.clone(), Arc::new(inc));
        let rid = &plan.request_id;
        match self.plans.get(rid).cloned() {
            None => self.install(plan, *attempt),
            Some(kept) => {
                self.duplicate_executions += 1;
                let kept_rank = plan_rank(rid, self.plan_attempt[rid], kept.executed_by);
                if plan_rank(rid, *attempt, plan.executed_by) < kept_rank {
                    self.uninstall(&kept);
                    self.install(plan, *attempt);
                }
            }
        }
        self.pending.remove(rid);
        self.log.push(op);
        self.version += 1;
        true
    }

    fn install(&mut self, plan: &Arc<ResponsePlan>, attempt: u32) {
        for a in &plan.assignments {
            match &a.assignee {
                Assignee::Responder { id } => {
                    self.commitments.entry(id.clone()).or_default().insert(a.id.clone(), a.expires_ms);
                }
                Assignee::Resource { id, quantity } => {
                    self.allocations.entry(id.clone()).or_default().insert(a.id.clone(), (a.expires_ms, *quantity));
                }
            }
            self.assignments.insert(a.id.clone(), a.clone());
        }
        self.plans.insert(plan.request_id.clone(), plan.clone());
        self.plan_attempt.insert(plan.request_id.clone(), attempt);
        self.plans_digest ^= plan_hash(plan);
    }

    fn uninstall(&mut self, plan: &ResponsePlan) {
        for a in &plan.assignments {
            let (Assignee::Responder { id } | Assignee::Resource { id, .. }) = &a.assignee;
            if let Some(m) = self.commitments.get_mut(id) {
                m.remove(&a.id);
            }
            if let Some(m) = self.allocations.get_mut(id) {
                m.remove(&a.id);
            }
            self.assignments.remove(&a.id);
        }
        self.plans_digest ^= plan_hash(plan);
    }

    /// Latest commitment of a responder (base value if it has none).
    pub fn busy_until(&self, r: &Responder) -> u64 {
        let committed = self.commitments.get(&r.id).and_then(|m| m.values().max().copied()).unwrap_or(0);
        r.busy_until_ms.max(committed)
    }

    pub fn available(&self, r: &Resource, now_ms: u64) -> u32 {
        let used: u32 = self
            .allocations
            .get(&r.id)
            .map(|m| m.values().filter(|(exp, _)| *exp > now_ms).map(|(_, q)| *q).sum())
            .unwrap_or(0);
        r.capacity.saturating_sub(used)
    }

    pub fn snapshot(&self, now_ms: u64) -> Snapshot {
        Snapshot {
            now_ms,
            incidents: self.incidents.values().filter(|i| i.is_active(now_ms)).cloned().collect(),
            responders: self
                .responders
                .iter()
                .map(|r| Responder { busy_until_ms: self.busy_until(r), ..r.clone() })
                .collect(),
            resources: self.resources.iter().map(|r| (r.clone(), self.available(r, now_ms))).collect(),
        }
    }

    /// Drop expired incidents, assignments and allocations (plans and the log are kept).
    pub fn prune(&mut self, now_ms: u64) {
        self.incidents.retain(|_, i| i.is_active(now_ms));
        self.assignments.retain(|_, a| a.expires_ms > now_ms);
        for m in self.commitments.values_mut() {
            m.retain(|_, exp| *exp > now_ms);
        }
        for m in self.allocations.values_mut() {
            m.retain(|_, (exp, _)| *exp > now_ms);
        }
    }
}

/// Rank of a committed plan among duplicates for the same request (lowest is kept).
fn plan_rank(rid: &str, attempt: u32, executed_by: NodeId) -> (u32, u64, NodeId) {
    (attempt, crate::election::tiebreak(rid, executed_by), executed_by)
}

fn plan_hash(plan: &ResponsePlan) -> u64 {
    crate::assign::fnv1a(format!("{}@{}", plan.request_id, plan.executed_by).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campus::*;
    use crate::planner::{self, PlannerConfig};

    fn fixture() -> (Campus, ClusterState) {
        let c = Campus::generate(7, 8);
        let st = ClusterState::new(seed_responders(7, &c, 40), seed_resources(7, &c, 30));
        (c, st)
    }

    fn op(c: &Campus, st: &ClusterState, inc: &Incident, node: NodeId) -> ReplOp {
        let plan = planner::plan(&PlannerConfig::default(), c, &st.snapshot(2_000), inc, "req-1", node);
        ReplOp::PlanCommitted {
            op_id: "plan:req-1".into(),
            attempt: 0,
            incident: Arc::new(inc.clone()),
            plan: Arc::new(plan),
        }
    }

    #[test]
    fn duplicate_executions_converge_regardless_of_order() {
        let (c, mut a) = fixture();
        let (_, mut b) = fixture();
        let inc = Incident {
            id: "i1".into(),
            reporter: "u".into(),
            category: Category::MedicalAssist,
            zone: 5,
            severity: 4,
            created_ms: 1_000,
            status: IncidentStatus::Open,
            ttl_ms: 600_000,
        };
        let (p1, p2) = (op(&c, &a, &inc, 1), op(&c, &a, &inc, 2));
        assert!(a.apply(p1.clone()) && a.apply(p2.clone()));
        assert!(b.apply(p2.clone()) && b.apply(p1.clone()));
        assert!(!a.apply(p1), "same (request, executor) op is idempotent");
        assert_eq!(a.plans_digest, b.plans_digest);
        assert_eq!(a.plans["req-1"].executed_by, b.plans["req-1"].executed_by);
        assert_eq!((a.duplicate_executions, b.duplicate_executions), (1, 1));
        let busy = |s: &ClusterState| s.snapshot(2_000).responders.iter().map(|r| r.busy_until_ms).collect::<Vec<_>>();
        assert_eq!(busy(&a), busy(&b));
        let mut ids_a: Vec<_> = a.assignments.keys().cloned().collect();
        let mut ids_b: Vec<_> = b.assignments.keys().cloned().collect();
        ids_a.sort();
        ids_b.sort();
        assert_eq!(ids_a, ids_b);
    }
}
