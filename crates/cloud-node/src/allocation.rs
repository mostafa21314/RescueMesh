//! Exclusive asset partitions avoid cross-node double booking without adding a
//! synchronous network round to each plan. Partition ownership never moves on
//! election/failure. Issued leases survive result replacement and temporary mute.
use crate::{
    campus::Campus,
    ledger::{Ledger, Snapshot},
};
use common::*;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub struct Allocator {
    me: NodeId,
    members: Vec<NodeId>,
    responders: HashSet<ResponderId>,
    resources: HashMap<ResourceId, (ResourceType, u32)>,
    issued: Ledger,
    quarantine: Option<Instant>,
    rejected: u64,
}

impl Allocator {
    pub fn new(campus: &Campus, me: NodeId, mut members: Vec<NodeId>) -> Self {
        members.sort_unstable();
        members.dedup();
        assert!(members.contains(&me));
        Self {
            me,
            members,
            responders: campus.responders.iter().map(|r| r.id).collect(),
            resources: campus
                .resources
                .iter()
                .filter(|r| r.availability == Availability::Available)
                .map(|r| (r.resource_id, (r.kind, r.quantity)))
                .collect(),
            issued: Ledger::new(campus),
            quarantine: None,
            rejected: 0,
        }
    }

    fn owner(&self, asset: u32) -> NodeId {
        self.members[asset as usize % self.members.len()]
    }

    pub fn validate(&self, origin: NodeId, plan: &Plan) -> bool {
        if !self.members.contains(&origin) {
            return false;
        }
        let (mut responders, mut resources) = (HashSet::new(), HashSet::new());
        plan.assignments.iter().all(|a| {
            if a.incident_id != plan.incident_id
                || a.created_at_ms > a.expires_at_ms
                || a.expires_at_ms - a.created_at_ms > MAX_ASSIGNMENT_LEASE_MS
            {
                return false;
            }
            match a.assignee {
                Assignee::Responder { responder_id, .. } => {
                    self.responders.contains(&responder_id)
                        && self.owner(responder_id) == origin
                        && responders.insert(responder_id)
                }
                Assignee::Resource {
                    resource_id,
                    kind,
                    quantity,
                } => {
                    self.owner(resource_id) == origin
                        && self
                            .resources
                            .get(&resource_id)
                            .is_some_and(|(k, cap)| *k == kind && quantity > 0 && quantity <= *cap)
                        && resources.insert(resource_id)
                }
            }
        })
    }

    pub fn reject(&mut self) {
        self.rejected += 1;
    }

    pub fn snapshot(&self, now: u64) -> Snapshot {
        let mut snap = self.issued.snapshot(now);
        let blocked = self.quarantine.is_some_and(|until| until > Instant::now());
        let eligible: HashSet<_> = self
            .responders
            .iter()
            .copied()
            .filter(|id| !blocked && self.owner(*id) == self.me)
            .collect();
        snap.busy.extend(
            self.responders
                .iter()
                .filter(|id| !eligible.contains(id))
                .copied(),
        );
        for (id, quantity) in &mut snap.remaining {
            if blocked || self.owner(*id) != self.me {
                *quantity = 0;
            }
        }
        snap.eligible_responders = Some(eligible);
        snap
    }

    pub fn reserve(&mut self, id: RequestId, plan: &Plan, now: u64) -> bool {
        if !self.validate(self.me, plan) {
            self.reject();
            return false;
        }
        if self.quarantine.is_some_and(|until| until > Instant::now())
            && !plan.assignments.is_empty()
        {
            return false;
        }
        if !self.issued.can_commit(id, plan, now) {
            return false;
        }
        let applied = self.issued.apply(id, plan, now);
        debug_assert!(applied.conflicts.is_empty() && applied.resource_overcommits == 0);
        true
    }

    /// A fresh process cannot know every lease its previous incarnation issued.
    /// Keep that budget closed for the maximum lease, while returning explicit
    /// shortfalls and allowing other nodes to keep using their own partitions.
    pub fn cold_rejoin(&mut self) {
        self.quarantine = Some(Instant::now() + Duration::from_millis(MAX_ASSIGNMENT_LEASE_MS));
    }

    pub fn restore_known(&mut self, id: RequestId, origin: NodeId, plan: &Plan, now: u64) {
        if origin == self.me && self.validate(origin, plan) {
            self.issued.apply(id, plan, now);
        }
    }

    pub fn status(&self, now: u64, canonical: &Ledger) -> AllocationStatus {
        let (own_resources, own_responders) = self.issued.violations(now);
        let (resources, responders) = canonical.violations(now);
        AllocationStatus {
            policy: ALLOCATION_POLICY.into(),
            members: self.members.clone(),
            rejected_plans: self.rejected,
            capacity_violations: resources + own_resources,
            responder_violations: responders + own_responders,
            quarantine_remaining_ms: self.quarantine.map_or(0, |until| {
                until.saturating_duration_since(Instant::now()).as_millis() as u64
            }),
            owned_responders: self
                .responders
                .iter()
                .filter(|id| self.owner(**id) == self.me)
                .count(),
            owned_resource_offers: self
                .resources
                .keys()
                .filter(|id| self.owner(**id) == self.me)
                .count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::CampusParams;
    use uuid::Uuid;

    fn campus() -> Campus {
        Campus::generate(
            42,
            CampusParams {
                grid: 3,
                responders: 15,
                resources: 30,
            },
        )
    }
    fn rid(seq: u64) -> RequestId {
        RequestId {
            client_id: Uuid::nil(),
            seq,
        }
    }
    fn resource_plan(c: &Campus, owner: u32, start: u64, end: u64) -> Plan {
        let r = c
            .resources
            .iter()
            .find(|r| r.availability == Availability::Available && r.resource_id % 3 == owner - 1)
            .unwrap();
        Plan {
            incident_id: Uuid::nil(),
            priority: 500,
            ranked_responders: vec![],
            assignments: vec![Assignment {
                incident_id: Uuid::nil(),
                assignee: Assignee::Resource {
                    resource_id: r.resource_id,
                    kind: r.kind,
                    quantity: r.quantity,
                },
                created_at_ms: start,
                expires_at_ms: end,
            }],
            route: vec![],
            route_cost: 0,
            shortfall: vec![],
            planned_at_ms: start,
        }
    }

    #[test]
    fn concurrent_issuers_use_disjoint_budgets_and_replicas_accept_every_order() {
        let c = campus();
        let plans: Vec<_> = (1..=3)
            .map(|owner| resource_plan(&c, owner, 0, 10_000))
            .collect();
        for owner in 1..=3 {
            let mut allocator = Allocator::new(&c, owner, vec![3, 1, 2]);
            assert!(allocator.reserve(rid(owner as u64), &plans[(owner - 1) as usize], 0));
            assert!(!allocator.reserve(rid(20), &plans[(owner - 1) as usize], 0));
            let snap = allocator.snapshot(0);
            assert!(c
                .responders
                .iter()
                .all(|r| snap.busy.contains(&r.id) || r.id % 3 == owner - 1));
        }
        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
            let mut replica = Ledger::new(&c);
            for i in order {
                assert_eq!(
                    replica
                        .apply(rid(i as u64), &plans[i], 0)
                        .resource_overcommits,
                    0
                );
            }
            assert_eq!(replica.violations(0), (0, 0));
        }
    }

    #[test]
    fn issued_lease_is_not_freed_by_canonical_replacement_or_rollback() {
        let c = campus();
        let mut allocator = Allocator::new(&c, 1, vec![1, 2, 3]);
        let p = resource_plan(&c, 1, 0, 10_000);
        assert!(allocator.reserve(rid(1), &p, 0));
        let mut canonical = Ledger::new(&c);
        canonical.apply(rid(1), &p, 0);
        canonical.release(rid(1), &p, 1); // Another origin's result replaced it.
        assert!(!allocator.reserve(rid(2), &p, 1));
        let next = resource_plan(&c, 1, 10_000, 20_000);
        assert!(allocator.reserve(rid(2), &next, 10_000));
        assert_eq!(allocator.status(10_000, &canonical).capacity_violations, 0);
    }

    #[test]
    fn provenance_oversized_leases_duplicates_and_capacity_are_checked() {
        let c = campus();
        let allocator = Allocator::new(&c, 1, vec![1, 2, 3]);
        let mut p = resource_plan(&c, 1, 0, 10_000);
        assert!(!allocator.validate(2, &p));
        p.assignments[0].expires_at_ms = MAX_ASSIGNMENT_LEASE_MS + 1;
        assert!(!allocator.validate(1, &p));
        p = resource_plan(&c, 1, 0, 10_000);
        p.assignments.push(p.assignments[0].clone());
        assert!(!allocator.validate(1, &p));
    }

    #[test]
    fn cold_rejoin_returns_shortfalls_until_previous_lease_budget_is_safe() {
        let c = campus();
        let mut allocator = Allocator::new(&c, 1, vec![1, 2, 3]);
        allocator.cold_rejoin();
        let snap = allocator.snapshot(0);
        assert!(snap.eligible_responders.unwrap().is_empty());
        assert!(snap.remaining.values().all(|q| *q == 0));
        assert!(!allocator.reserve(rid(1), &resource_plan(&c, 1, 0, 10_000), 0));
    }

    #[test]
    fn delayed_adjacent_leases_do_not_generate_clock_skew_false_positives() {
        let c = campus();
        let mut l = Ledger::new(&c);
        let a = resource_plan(&c, 1, 10_000, 20_000);
        let b = resource_plan(&c, 1, 20_000, 30_000);
        assert_eq!(l.apply(rid(2), &b, 9_000).resource_overcommits, 0);
        assert_eq!(l.apply(rid(1), &a, 9_000).resource_overcommits, 0);
        assert_eq!(l.violations(9_000), (0, 0));
        let overlapping = resource_plan(&c, 1, 15_000, 25_000);
        assert!(l.apply(rid(3), &overlapping, 9_000).resource_overcommits > 0);
        assert_eq!(l.violations(9_000).0, 1);
    }
}
