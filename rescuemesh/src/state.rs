//! Replicated cloud state held by every node.
//!
//! Consistency approach (Phase 1): state is a set of immutable, uniquely-identified
//! records (incidents, plans, assignments). Every change is a `ReplOp` with a unique
//! `op_id`; applying an op twice is a no-op, and applying ops in different orders
//! gives the same result because:
//!   * incidents / plans / assignments are inserted by unique key (set union),
//!   * responder commitment is `busy_until = max(old, new)`,
//!   * resource availability is *derived* (capacity minus unexpired allocations)
//!     instead of being decremented in place.
//! So replicas converge once they have applied the same set of ops. What this does
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
    PlanCommitted { op_id: String, incident: Arc<Incident>, plan: Arc<ResponsePlan> },
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
    pub received_ms: u64,
    pub incident: Incident,
}

pub struct ClusterState {
    pub incidents: HashMap<String, Arc<Incident>>,
    pub responders: Vec<Responder>,
    responder_idx: HashMap<String, usize>,
    pub resources: Vec<Resource>,
    /// resource id -> assignment id -> (expires_ms, quantity)
    allocations: HashMap<String, HashMap<String, (u64, u32)>>,
    pub assignments: HashMap<String, Assignment>,
    /// Idempotency cache: request id -> committed plan.
    pub plans: HashMap<RequestId, Arc<ResponsePlan>>,
    /// Requests this node saw but another node was chosen to execute.
    pub pending: HashMap<RequestId, Pending>,
    applied: HashSet<String>,
    /// Ops in local apply order (input for rejoin/state transfer later).
    pub log: Vec<ReplOp>,
    /// Order-independent digest of committed plan ids (XOR of FNV hashes).
    pub plans_digest: u64,
    pub version: u64,
}

impl ClusterState {
    pub fn new(responders: Vec<Responder>, resources: Vec<Resource>) -> Self {
        let responder_idx = responders.iter().enumerate().map(|(i, r)| (r.id.clone(), i)).collect();
        ClusterState {
            incidents: HashMap::new(),
            responders,
            responder_idx,
            resources,
            allocations: HashMap::new(),
            assignments: HashMap::new(),
            plans: HashMap::new(),
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
        if !self.applied.insert(op.op_id().to_string()) {
            return false;
        }
        match &op {
            ReplOp::PlanCommitted { incident, plan, .. } => {
                let mut inc = (**incident).clone();
                inc.status = IncidentStatus::Planned;
                self.incidents.insert(inc.id.clone(), Arc::new(inc));
                for a in &plan.assignments {
                    match &a.assignee {
                        Assignee::Responder { id } => {
                            if let Some(&i) = self.responder_idx.get(id) {
                                let r = &mut self.responders[i];
                                r.busy_until_ms = r.busy_until_ms.max(a.expires_ms);
                            }
                        }
                        Assignee::Resource { id, quantity } => {
                            self.allocations
                                .entry(id.clone())
                                .or_default()
                                .insert(a.id.clone(), (a.expires_ms, *quantity));
                        }
                    }
                    self.assignments.insert(a.id.clone(), a.clone());
                }
                if self.plans.insert(plan.request_id.clone(), plan.clone()).is_none() {
                    self.plans_digest ^= crate::assign::fnv1a(plan.request_id.as_bytes());
                }
                self.pending.remove(&plan.request_id);
            }
        }
        self.log.push(op);
        self.version += 1;
        true
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
            responders: self.responders.clone(),
            resources: self.resources.iter().map(|r| (r.clone(), self.available(r, now_ms))).collect(),
        }
    }

    /// Drop expired incidents, assignments and allocations (plans and the log are kept).
    pub fn prune(&mut self, now_ms: u64) {
        self.incidents.retain(|_, i| i.is_active(now_ms));
        self.assignments.retain(|_, a| a.expires_ms > now_ms);
        for m in self.allocations.values_mut() {
            m.retain(|_, (exp, _)| *exp > now_ms);
        }
    }
}
