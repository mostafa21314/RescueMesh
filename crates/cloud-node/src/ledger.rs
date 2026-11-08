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
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResourceHold {
    request_id: RequestId,
    quantity: u32,
    created_at_ms: u64,
    expires_at_ms: u64,
}

/// What the planner sees: who is busy and how many units each offer has left.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub busy: HashSet<ResponderId>,
    pub remaining: HashMap<ResourceId, u32>,
    /// Restricted candidate set for the local node's exclusive asset partition.
    pub eligible_responders: Option<HashSet<ResponderId>>,
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
    /// Retain losing claims too, so replacing a winning plan restores the next
    /// valid holder rather than incorrectly making the responder free.
    claims: HashMap<ResponderId, HashMap<RequestId, Hold>>,
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
        Ledger {
            responders: HashMap::new(),
            claims: HashMap::new(),
            resources: HashMap::new(),
            capacity,
        }
    }

    pub fn snapshot(&self, now_ms: u64) -> Snapshot {
        let busy = self
            .claims
            .iter()
            .filter(|(_, claims)| {
                claims
                    .values()
                    .any(|h| h.created_at_ms <= now_ms && h.expires_at_ms > now_ms)
            })
            .map(|(id, _)| *id)
            .collect();
        let remaining = self
            .capacity
            .iter()
            .map(|(id, cap)| (*id, cap.saturating_sub(self.used(*id, now_ms))))
            .collect();
        Snapshot {
            busy,
            remaining,
            eligible_responders: None,
        }
    }

    fn used(&self, id: ResourceId, now_ms: u64) -> u32 {
        self.resources.get(&id).map_or(0, |hs| {
            hs.iter()
                .filter(|h| h.created_at_ms <= now_ms && h.expires_at_ms > now_ms)
                .map(|h| h.quantity)
                .sum()
        })
    }

    /// True if every responder and resource unit in `plan` is still free (or
    /// already held by this same request).
    pub fn can_commit(&self, request_id: RequestId, plan: &Plan, now_ms: u64) -> bool {
        let mut responders = HashSet::new();
        let mut resources = HashSet::new();
        if plan.assignments.iter().any(|a| match a.assignee {
            Assignee::Responder { responder_id, .. } => !responders.insert(responder_id),
            Assignee::Resource {
                resource_id,
                quantity,
                ..
            } => quantity == 0 || !resources.insert(resource_id),
        }) {
            return false;
        }
        plan.assignments.iter().all(|a| {
            a.expires_at_ms <= now_ms
                || match a.assignee {
                    Assignee::Responder { responder_id, .. } => {
                        self.claims.get(&responder_id).is_none_or(|claims| {
                            claims.values().all(|h| {
                                h.request_id == request_id
                                    || !overlap(
                                        h.created_at_ms,
                                        h.expires_at_ms,
                                        a.created_at_ms.max(now_ms),
                                        a.expires_at_ms,
                                    )
                            })
                        })
                    }
                    Assignee::Resource {
                        resource_id,
                        quantity,
                        ..
                    } => {
                        self.capacity.get(&resource_id).copied().unwrap_or(0) as u64
                            >= self.resource_peak_until(
                                resource_id,
                                Some(request_id),
                                a.created_at_ms.max(now_ms),
                                a.expires_at_ms,
                            ) + quantity as u64
                    }
                }
        })
    }

    /// Records `plan`'s assignments. Idempotent per request_id.
    pub fn apply(&mut self, request_id: RequestId, plan: &Plan, now_ms: u64) -> Applied {
        let mut out = Applied::default();
        for a in &plan.assignments {
            if a.expires_at_ms <= now_ms {
                continue;
            }
            match a.assignee {
                Assignee::Responder { responder_id, .. } => {
                    let mut new = Hold {
                        request_id,
                        priority: plan.priority,
                        created_at_ms: a.created_at_ms,
                        expires_at_ms: a.expires_at_ms,
                    };
                    let old = self
                        .claims
                        .get(&responder_id)
                        .and_then(|claims| {
                            claims
                                .values()
                                .filter(|h| {
                                    h.request_id != request_id
                                        && overlap(
                                            h.created_at_ms,
                                            h.expires_at_ms,
                                            new.created_at_ms,
                                            new.expires_at_ms,
                                        )
                                })
                                .max_by_key(|h| (h.priority, std::cmp::Reverse(h.request_id)))
                        })
                        .cloned();
                    if let Some(existing) = self
                        .claims
                        .get(&responder_id)
                        .and_then(|claims| claims.get(&request_id))
                    {
                        new.created_at_ms = new.created_at_ms.min(existing.created_at_ms);
                        new.expires_at_ms = new.expires_at_ms.max(existing.expires_at_ms);
                    }
                    self.claims
                        .entry(responder_id)
                        .or_default()
                        .insert(request_id, new.clone());
                    if let Some(old) = old.filter(|old| old.request_id != request_id) {
                        let new_wins = (new.priority, std::cmp::Reverse(new.request_id))
                            > (old.priority, std::cmp::Reverse(old.request_id));
                        let (kept, dropped) = if new_wins {
                            (request_id, old.request_id)
                        } else {
                            (old.request_id, request_id)
                        };
                        out.conflicts.push(Conflict {
                            responder_id,
                            kept,
                            dropped,
                        });
                    }
                    self.refresh(responder_id, now_ms);
                }
                Assignee::Resource {
                    resource_id,
                    quantity,
                    ..
                } => {
                    let used = self.resource_peak_until(
                        resource_id,
                        Some(request_id),
                        a.created_at_ms,
                        a.expires_at_ms,
                    );
                    let cap = self.capacity.get(&resource_id).copied().unwrap_or(0);
                    let holds = self.resources.entry(resource_id).or_default();
                    holds.retain(|h| h.expires_at_ms > now_ms);
                    if let Some(held) = holds.iter_mut().find(|h| h.request_id == request_id) {
                        held.quantity = held.quantity.max(quantity);
                        held.created_at_ms = held.created_at_ms.min(a.created_at_ms);
                        held.expires_at_ms = held.expires_at_ms.max(a.expires_at_ms);
                        continue;
                    }
                    if used + quantity as u64 > cap as u64 {
                        out.resource_overcommits += 1;
                    }
                    holds.push(ResourceHold {
                        request_id,
                        quantity,
                        created_at_ms: a.created_at_ms,
                        expires_at_ms: a.expires_at_ms,
                    });
                }
            }
        }
        out
    }

    /// Undoes `plan`'s holds for `request_id` (a result that lost to a
    /// preferred one for the same request). Holds of other requests stay.
    pub fn release(&mut self, request_id: RequestId, plan: &Plan, now_ms: u64) {
        for a in &plan.assignments {
            match a.assignee {
                Assignee::Responder { responder_id, .. } => {
                    if let Some(claims) = self.claims.get_mut(&responder_id) {
                        claims.remove(&request_id);
                    }
                    self.refresh(responder_id, now_ms);
                }
                Assignee::Resource { resource_id, .. } => {
                    if let Some(hs) = self.resources.get_mut(&resource_id) {
                        hs.retain(|h| h.request_id != request_id);
                    }
                }
            }
        }
    }

    pub fn holder(&self, responder_id: ResponderId) -> Option<&Hold> {
        self.responders.get(&responder_id)
    }

    /// Reservation interval audit; replica receipt time cannot create a false
    /// overlap when one issuer's adjacent leases arrive at a slower clock.
    pub fn resource_peak(&self, id: ResourceId, exclude: Option<RequestId>, from: u64) -> u64 {
        self.resource_peak_until(id, exclude, from, u64::MAX)
    }

    fn resource_peak_until(
        &self,
        id: ResourceId,
        exclude: Option<RequestId>,
        from: u64,
        until: u64,
    ) -> u64 {
        peak(
            self.resources
                .get(&id)
                .into_iter()
                .flatten()
                .filter(|h| Some(h.request_id) != exclude)
                .map(|h| (h.created_at_ms, h.expires_at_ms.min(until), h.quantity)),
            from,
        )
    }

    pub fn violations(&self, from: u64) -> (u64, u64) {
        let resources = self
            .capacity
            .iter()
            .filter(|(id, cap)| self.resource_peak(**id, None, from) > **cap as u64)
            .count() as u64;
        let responders = self
            .claims
            .values()
            .filter(|claims| {
                peak(
                    claims
                        .values()
                        .map(|h| (h.created_at_ms, h.expires_at_ms, 1)),
                    from,
                ) > 1
            })
            .count() as u64;
        (resources, responders)
    }

    fn winner(&self, responder: ResponderId, now_ms: u64) -> Option<&Hold> {
        self.claims
            .get(&responder)?
            .values()
            .filter(|h| h.created_at_ms <= now_ms && h.expires_at_ms > now_ms)
            .max_by_key(|h| (h.priority, std::cmp::Reverse(h.request_id)))
    }

    fn refresh(&mut self, responder: ResponderId, now_ms: u64) {
        if let Some(claims) = self.claims.get_mut(&responder) {
            claims.retain(|_, h| h.expires_at_ms > now_ms);
        }
        match self.winner(responder, now_ms).cloned() {
            Some(hold) => {
                self.responders.insert(responder, hold);
            }
            None => {
                self.responders.remove(&responder);
            }
        }
    }
}

fn overlap(a: u64, b: u64, c: u64, d: u64) -> bool {
    a < d && c < b
}

fn peak(spans: impl Iterator<Item = (u64, u64, u32)>, from: u64) -> u64 {
    let mut events = Vec::new();
    for (start, end, quantity) in spans {
        let start = start.max(from);
        if start < end {
            events.push((start, quantity as i64));
            events.push((end, -(quantity as i64)));
        }
    }
    events.sort_unstable(); // Expirations precede starts at an equal timestamp.
    let (mut used, mut maximum) = (0i64, 0i64);
    for (_, delta) in events {
        used += delta;
        maximum = maximum.max(used);
    }
    maximum as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::CampusParams;
    use uuid::Uuid;

    fn rid(seq: u64) -> RequestId {
        RequestId {
            client_id: Uuid::nil(),
            seq,
        }
    }

    fn plan_with(priority: u32, responder_id: ResponderId) -> Plan {
        Plan {
            incident_id: Uuid::nil(),
            priority,
            ranked_responders: vec![],
            assignments: vec![Assignment {
                incident_id: Uuid::nil(),
                assignee: Assignee::Responder {
                    responder_id,
                    role: Capability::FirstAid,
                },
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
        Ledger::new(&Campus::generate(
            1,
            CampusParams {
                grid: 3,
                responders: 5,
                resources: 3,
            },
        ))
    }

    #[test]
    fn higher_priority_keeps_responder_in_any_order() {
        let (lo, hi) = (plan_with(300, 4), plan_with(700, 4));
        for order in [
            [(rid(1), &lo), (rid(2), &hi)],
            [(rid(2), &hi), (rid(1), &lo)],
        ] {
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
        assert_eq!(
            c,
            vec![Conflict {
                responder_id: 2,
                kept: rid(3),
                dropped: rid(9)
            }]
        );
    }

    #[test]
    fn release_frees_only_this_requests_holds() {
        let mut l = ledger();
        l.apply(rid(1), &plan_with(500, 1), 0);
        l.apply(rid(2), &plan_with(500, 2), 0);
        l.release(rid(1), &plan_with(500, 1), 0);
        l.release(rid(1), &plan_with(500, 2), 0); // held by request 2: untouched
        assert!(l.holder(1).is_none());
        assert_eq!(l.holder(2).unwrap().request_id, rid(2));
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

    #[test]
    fn expired_delayed_claim_cannot_erase_a_live_reservation() {
        let mut l = ledger();
        let mut live = plan_with(100, 1);
        live.assignments[0].expires_at_ms = 30_000;
        l.apply(rid(2), &live, 20_000);
        l.apply(rid(1), &plan_with(900, 1), 20_000);
        assert_eq!(l.holder(1).unwrap().request_id, rid(2));
        assert!(l.snapshot(20_000).busy.contains(&1));
        assert!(!l.can_commit(rid(3), &live, 20_000));
    }

    #[test]
    fn replacing_a_winning_plan_restores_the_displaced_claim() {
        let low = plan_with(100, 1);
        let old = plan_with(900, 1);
        let new = plan_with(900, 2);
        let mut a = ledger();
        a.apply(rid(1), &low, 0);
        a.apply(rid(2), &old, 0);
        a.release(rid(2), &old, 0);
        a.apply(rid(2), &new, 0);
        let mut b = ledger();
        b.apply(rid(2), &new, 0);
        b.apply(rid(1), &low, 0);
        assert_eq!(a.holder(1), b.holder(1));
        assert_eq!(a.holder(2), b.holder(2));
        assert_eq!(a.snapshot(0), b.snapshot(0));
    }
}
