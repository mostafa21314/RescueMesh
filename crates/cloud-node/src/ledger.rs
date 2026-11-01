//! The assignment ledger: which responders and resource units are committed to
//! which request, until when.
//!
//! The serving node commits one plan at a time under the ledger lock, so its
//! own workers never hand out the same responder twice. Replicated plans go
//! through the same `apply`, which resolves a responder claimed by two
//! requests deterministically: the higher-priority incident keeps it, ties go
//! to the lower request_id. Because the rule depends only on the two claims,
//! replicas that apply the same plans in any order end up with the same holder.

use crate::campus::Campus;
use common::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hold {
    pub request_id: RequestId,
    pub priority: u32,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResourceHold {
    request_id: RequestId,
    quantity: u32,
    expires_at_ms: u64,
}

/// What the planner sees: who is busy and how many units each offer has left.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub busy: HashSet<ResponderId>,
    pub remaining: HashMap<ResourceId, u32>,
}

/// A responder claimed by two requests; `kept` holds it, `dropped` lost it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    pub responder_id: ResponderId,
    pub kept: RequestId,
    pub dropped: RequestId,
}

#[derive(Debug, Default)]
pub struct Applied {
    pub conflicts: Vec<Conflict>,
    /// Resource units handed out beyond an offer's quantity (possible only
    /// when two servers planned concurrently).
    pub resource_overcommits: u32,
}

#[derive(Debug)]
pub struct Ledger {
    responders: HashMap<ResponderId, Hold>,
    resources: HashMap<ResourceId, Vec<ResourceHold>>,
    /// Units each usable offer starts with (offline or lapsed offers: absent).
    capacity: HashMap<ResourceId, u32>,
}

impl Ledger {
    pub fn new(campus: &Campus) -> Self {
        let capacity = campus
            .resources
            .iter()
            .filter(|r| r.availability == Availability::Available)
            .map(|r| (r.resource_id, r.quantity))
            .collect();
        Ledger { responders: HashMap::new(), resources: HashMap::new(), capacity }
    }

    pub fn snapshot(&self, now_ms: u64) -> Snapshot {
        let busy = self
            .responders
            .iter()
            .filter(|(_, h)| h.expires_at_ms > now_ms)
            .map(|(id, _)| *id)
            .collect();
        let remaining = self
            .capacity
            .iter()
            .map(|(id, cap)| (*id, cap.saturating_sub(self.used(*id, now_ms))))
            .collect();
        Snapshot { busy, remaining }
    }

    fn used(&self, id: ResourceId, now_ms: u64) -> u32 {
        self.resources
            .get(&id)
            .map_or(0, |hs| hs.iter().filter(|h| h.expires_at_ms > now_ms).map(|h| h.quantity).sum())
    }

    /// True if every responder and resource unit in `plan` is still free (or
    /// already held by this same request).
    pub fn can_commit(&self, request_id: RequestId, plan: &Plan, now_ms: u64) -> bool {
        plan.assignments.iter().all(|a| match a.assignee {
            Assignee::Responder { responder_id, .. } => match self.responders.get(&responder_id) {
                Some(h) => h.expires_at_ms <= now_ms || h.request_id == request_id,
                None => true,
            },
            Assignee::Resource { resource_id, quantity, .. } => {
                let held = self
                    .resources
                    .get(&resource_id)
                    .is_some_and(|hs| hs.iter().any(|h| h.request_id == request_id));
                held || self.capacity.get(&resource_id).copied().unwrap_or(0)
                    >= self.used(resource_id, now_ms) + quantity
            }
        })
    }

    /// Records `plan`'s assignments. Idempotent per request_id.
    pub fn apply(&mut self, request_id: RequestId, plan: &Plan, now_ms: u64) -> Applied {
        let mut out = Applied::default();
        for a in &plan.assignments {
            match a.assignee {
                Assignee::Responder { responder_id, .. } => {
                    let new = Hold { request_id, priority: plan.priority, expires_at_ms: a.expires_at_ms };
                    match self.responders.get(&responder_id) {
                        None => {
                            self.responders.insert(responder_id, new);
                        }
                        Some(old) if old.request_id == request_id => {}
                        Some(old) if old.expires_at_ms <= now_ms => {
                            self.responders.insert(responder_id, new);
                        }
                        Some(old) => {
                            let new_wins = (new.priority, std::cmp::Reverse(new.request_id))
                                > (old.priority, std::cmp::Reverse(old.request_id));
                            let (kept, dropped) =
                                if new_wins { (request_id, old.request_id) } else { (old.request_id, request_id) };
                            if new_wins {
                                self.responders.insert(responder_id, new);
                            }
                            out.conflicts.push(Conflict { responder_id, kept, dropped });
                        }
                    }
                }
                Assignee::Resource { resource_id, quantity, .. } => {
                    let used = self.used(resource_id, now_ms);
                    let cap = self.capacity.get(&resource_id).copied().unwrap_or(0);
                    let holds = self.resources.entry(resource_id).or_default();
                    holds.retain(|h| h.expires_at_ms > now_ms);
                    if holds.iter().any(|h| h.request_id == request_id) {
                        continue;
                    }
                    if used + quantity > cap {
                        out.resource_overcommits += 1;
                    }
                    holds.push(ResourceHold { request_id, quantity, expires_at_ms: a.expires_at_ms });
                }
            }
        }
        out
    }

    pub fn holder(&self, responder_id: ResponderId) -> Option<&Hold> {
        self.responders.get(&responder_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::CampusParams;
    use uuid::Uuid;

    fn rid(seq: u64) -> RequestId {
        RequestId { client_id: Uuid::nil(), seq }
    }

    fn plan_with(priority: u32, responder_id: ResponderId) -> Plan {
        Plan {
            incident_id: Uuid::nil(),
            priority,
            ranked_responders: vec![],
            assignments: vec![Assignment {
                incident_id: Uuid::nil(),
                assignee: Assignee::Responder { responder_id, role: Capability::FirstAid },
                created_at_ms: 0,
                expires_at_ms: 10_000,
            }],
            route: vec![],
            route_cost: 0,
            shortfall: vec![],
            planned_at_ms: 0,
        }
    }

    fn ledger() -> Ledger {
        Ledger::new(&Campus::generate(1, CampusParams { grid: 3, responders: 5, resources: 3 }))
    }

    #[test]
    fn higher_priority_keeps_responder_in_any_order() {
        let (lo, hi) = (plan_with(300, 4), plan_with(700, 4));
        for order in [[(rid(1), &lo), (rid(2), &hi)], [(rid(2), &hi), (rid(1), &lo)]] {
            let mut l = ledger();
            let mut conflicts = 0;
            for (r, p) in order {
                conflicts += l.apply(r, p, 0).conflicts.len();
            }
            assert_eq!(conflicts, 1);
            assert_eq!(l.holder(4).unwrap().request_id, rid(2));
        }
    }

    #[test]
    fn equal_priority_goes_to_lower_request_id() {
        let p = plan_with(500, 2);
        let mut l = ledger();
        l.apply(rid(9), &p, 0);
        let c = l.apply(rid(3), &p, 0).conflicts;
        assert_eq!(c, vec![Conflict { responder_id: 2, kept: rid(3), dropped: rid(9) }]);
    }

    #[test]
    fn apply_is_idempotent_and_expiry_frees() {
        let p = plan_with(500, 1);
        let mut l = ledger();
        assert!(l.apply(rid(1), &p, 0).conflicts.is_empty());
        assert!(l.apply(rid(1), &p, 0).conflicts.is_empty());
        assert!(!l.can_commit(rid(2), &p, 5_000));
        assert!(l.can_commit(rid(2), &p, 10_000));
    }
}
