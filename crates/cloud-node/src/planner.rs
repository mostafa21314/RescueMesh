//! The planner: incident → response plan.
//!
//! 1. Priority from severity, age and scarcity.
//! 2. `complexity` travel-time scenarios: in each, every edge's cost is
//!    scaled by a seeded factor in [1.0, 1.5] (congestion, blocked paths), and
//!    Dijkstra runs from each responder's zone to the incident zone. A
//!    responder's ETA is its mean over the scenarios. This is where the CPU
//!    cost goes, and it grows linearly with `complexity`.
//! 3. Cost matrix, roles × free responders, from ETA, capability match and
//!    priority.
//! 4. Hungarian assignment (our own solver) of roles to responders.
//! 5. Nearest available unit of each required resource type, and the lead
//!    responder's route on the base graph.
//!
//! `plan` is a pure function of (campus, snapshot, incident, complexity,
//! now_ms): no clocks, no randomness beyond seeds derived from the incident.

use crate::campus::Campus;
use crate::hungarian;
use crate::ledger::Snapshot;
use common::rng::mix;
use common::*;

/// Upper bound on scenarios, so one request can't pin a worker for minutes.
pub const MAX_COMPLEXITY: u32 = 1_000;
/// How many responders the ranked list keeps.
pub const RANKED_LEN: usize = 10;
/// Cost added for a responder lacking the role's capability, at priority 0.
const MISMATCH_PENALTY: i64 = 800;

/// Roles the incident needs: 1 at severity 1–2, 2 at 3–4, 3 at 5, cycling
/// through the category's capabilities.
pub fn roles_for(incident: &Incident) -> Vec<Capability> {
    let caps = incident.category.required_capabilities();
    let n = 1 + (incident.severity.clamp(1, 5) as usize - 1) / 2;
    (0..n).map(|i| caps[i % caps.len()]).collect()
}

/// Expected cost of a job in work units; the pool sums these as pending work.
pub fn expected_cost(complexity: u32) -> u64 {
    complexity.clamp(1, MAX_COMPLEXITY) as u64
}

/// Priority in 0..=1000: 0.5·severity + 0.2·age + 0.3·scarcity.
pub fn priority(campus: &Campus, snap: &Snapshot, incident: &Incident, now_ms: u64) -> u32 {
    let sev = incident.severity.clamp(1, 5) as u64 * 200;
    let age = now_ms.saturating_sub(incident.timestamp_ms);
    let age = (age * 1000)
        .checked_div(incident.ttl_ms)
        .unwrap_or(1000)
        .min(1000);

    // Scarcity: share of capable responders already busy, and share of
    // required resource types with no unit left.
    let caps = incident.category.required_capabilities();
    let capable: Vec<_> = campus
        .responders
        .iter()
        .filter(|r| {
            snap.eligible_responders
                .as_ref()
                .is_none_or(|ids| ids.contains(&r.id))
        })
        .filter(|r| r.capabilities.iter().any(|c| caps.contains(c)))
        .collect();
    let busy_share = if capable.is_empty() {
        1000
    } else {
        capable.iter().filter(|r| snap.busy.contains(&r.id)).count() as u64 * 1000
            / capable.len() as u64
    };
    let kinds = incident.category.required_resources();
    let short = kinds
        .iter()
        .filter(|k| best_resource(campus, snap, **k, 0).is_none())
        .count() as u64;
    let res_share = short * 1000 / kinds.len().max(1) as u64;
    let scarcity = (2 * busy_share + res_share) / 3;

    ((500 * sev + 200 * age + 300 * scarcity) / 1000) as u32
}

fn best_resource<'a>(
    campus: &'a Campus,
    snap: &Snapshot,
    kind: ResourceType,
    zone: ZoneId,
) -> Option<&'a ResourceOffer> {
    campus
        .resources
        .iter()
        .filter(|r| r.kind == kind && snap.remaining.get(&r.resource_id).copied().unwrap_or(0) > 0)
        .min_by_key(|r| {
            (
                campus.base_dist[r.zone as usize][zone as usize],
                r.resource_id,
            )
        })
}

/// Mean travel cost from each zone in `from` to `target` over `scenarios`
/// perturbed copies of the graph.
fn scenario_etas(
    campus: &Campus,
    from: &[ZoneId],
    target: ZoneId,
    scenarios: u32,
    salt: u64,
) -> Vec<u32> {
    let g = &campus.graph;
    let mut sum = vec![0u64; from.len()];
    for s in 0..scenarios {
        let seed = mix(salt ^ mix(s as u64 + 1));
        let weight = |e: usize| {
            let factor = 1000 + mix(seed ^ e as u64) % 501; // per mille, 1.0–1.5
            ((g.edges[e].w as u64 * factor).div_ceil(1000)) as u32
        };
        for (i, &z) in from.iter().enumerate() {
            sum[i] += g.dijkstra(z, Some(target), weight).dist[target as usize] as u64;
        }
    }
    sum.iter().map(|t| (t / scenarios as u64) as u32).collect()
}

pub fn plan(
    campus: &Campus,
    snap: &Snapshot,
    incident: &Incident,
    complexity: u32,
    now_ms: u64,
) -> Plan {
    let zone = incident.zone % campus.zones();
    let prio = priority(campus, snap, incident, now_ms);
    let roles = roles_for(incident);
    let scenarios = complexity.clamp(1, MAX_COMPLEXITY);
    let created = now_ms;
    let expires = now_ms.saturating_add(incident.ttl_ms.min(MAX_ASSIGNMENT_LEASE_MS));

    // Dijkstra once per distinct responder zone: responders in the same zone
    // share an ETA.
    let mut zones: Vec<ZoneId> = campus.responders.iter().map(|r| r.zone).collect();
    zones.sort_unstable();
    zones.dedup();
    let salt = incident.incident_id.as_u64_pair().0 ^ incident.incident_id.as_u64_pair().1;
    let etas = scenario_etas(campus, &zones, zone, scenarios, salt);
    let eta_of = |z: ZoneId| etas[zones.binary_search(&z).expect("responder zone")];

    // Urgent incidents weigh travel time more and skill mismatch less.
    let p = prio as i64;
    let cost = |r: &Responder, role: Capability| -> i64 {
        let travel = eta_of(r.zone) as i64 * 10 * (1000 + p) / 1000;
        let mismatch = if r.capabilities.contains(&role) {
            0
        } else {
            MISMATCH_PENALTY * (2000 - p) / 1000
        };
        travel + mismatch
    };

    let free: Vec<&Responder> = campus
        .responders
        .iter()
        .filter(|r| !snap.busy.contains(&r.id))
        .collect();
    let filled = roles.len().min(free.len());
    let matrix: Vec<Vec<i64>> = roles[..filled]
        .iter()
        .map(|&role| free.iter().map(|r| cost(r, role)).collect())
        .collect();
    let (cols, _) = hungarian::solve(&matrix);

    let mut assignments: Vec<Assignment> = cols
        .iter()
        .zip(&roles)
        .map(|(&j, &role)| Assignment {
            incident_id: incident.incident_id,
            assignee: Assignee::Responder {
                responder_id: free[j].id,
                role,
            },
            created_at_ms: created,
            expires_at_ms: expires,
        })
        .collect();
    let mut shortfall: Vec<Shortfall> = roles[filled..]
        .iter()
        .map(|&role| Shortfall::Responder { role })
        .collect();

    let mut ranked: Vec<RankedResponder> = free
        .iter()
        .map(|r| RankedResponder {
            responder_id: r.id,
            cost: roles.iter().map(|&role| cost(r, role)).min().unwrap_or(0),
            eta: eta_of(r.zone),
        })
        .collect();
    ranked.sort_by_key(|r| (r.cost, r.responder_id));
    ranked.truncate(RANKED_LEN);

    for &kind in incident.category.required_resources() {
        match best_resource(campus, snap, kind, zone) {
            Some(offer) => assignments.push(Assignment {
                incident_id: incident.incident_id,
                assignee: Assignee::Resource {
                    resource_id: offer.resource_id,
                    kind,
                    quantity: 1,
                },
                created_at_ms: created,
                expires_at_ms: expires,
            }),
            None => shortfall.push(Shortfall::Resource { kind }),
        }
    }

    let (route, route_cost) = match cols.first() {
        Some(&j) => {
            let sp = campus.graph.base_dijkstra(free[j].zone);
            (sp.path_to(zone), sp.dist[zone as usize])
        }
        None => (Vec::new(), 0),
    };

    Plan {
        incident_id: incident.incident_id,
        priority: prio,
        ranked_responders: ranked,
        assignments,
        route,
        route_cost,
        shortfall,
        planned_at_ms: now_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::CampusParams;
    use uuid::Uuid;

    fn incident(seed: u64, severity: u8) -> Incident {
        Incident {
            incident_id: Uuid::from_u64_pair(seed, seed * 7 + 1),
            reporter: "test".into(),
            category: Category::ALL[(seed % 6) as usize],
            zone: (seed * 37 % 256) as u32,
            severity,
            timestamp_ms: 1_000,
            status: IncidentStatus::Reported,
            ttl_ms: 20_000,
        }
    }

    #[test]
    fn deterministic() {
        let c = Campus::generate(42, CampusParams::default());
        let snap = crate::ledger::Ledger::new(&c).snapshot(0);
        for s in 0..20 {
            let inc = incident(s, (s % 5 + 1) as u8);
            let a = plan(&c, &snap, &inc, 3, 5_000);
            let b = plan(
                &Campus::generate(42, CampusParams::default()),
                &snap,
                &inc,
                3,
                5_000,
            );
            assert_eq!(a, b);
            // A JSON round trip keeps the plan identical (integers only).
            let back: Plan = serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
            assert_eq!(a, back);
        }
    }

    #[test]
    fn assigns_distinct_free_responders() {
        let c = Campus::generate(42, CampusParams::default());
        let mut snap = crate::ledger::Ledger::new(&c).snapshot(0);
        snap.busy.extend(0..140); // only 10 responders left
        for s in 0..30 {
            let p = plan(&c, &snap, &incident(s, 5), 1, 5_000);
            let ids: Vec<_> = p.responder_ids().collect();
            let mut dedup = ids.clone();
            dedup.sort();
            dedup.dedup();
            assert_eq!(ids.len(), dedup.len());
            assert!(ids.iter().all(|id| !snap.busy.contains(id)));
            assert_eq!(ids.len(), 3);
        }
    }

    #[test]
    fn shortfall_when_no_one_is_free() {
        let c = Campus::generate(42, CampusParams::default());
        let mut snap = crate::ledger::Ledger::new(&c).snapshot(0);
        snap.busy.extend(0..150);
        let p = plan(&c, &snap, &incident(1, 3), 1, 5_000);
        assert_eq!(p.responder_ids().count(), 0);
        assert_eq!(
            p.shortfall
                .iter()
                .filter(|s| matches!(s, Shortfall::Responder { .. }))
                .count(),
            2
        );
        assert!(p.route.is_empty());
    }

    #[test]
    fn route_ends_at_incident_zone() {
        let c = Campus::generate(42, CampusParams::default());
        let snap = crate::ledger::Ledger::new(&c).snapshot(0);
        let inc = incident(5, 4);
        let p = plan(&c, &snap, &inc, 2, 5_000);
        assert_eq!(*p.route.last().unwrap(), inc.zone);
    }

    #[test]
    fn severity_raises_priority() {
        let c = Campus::generate(42, CampusParams::default());
        let snap = crate::ledger::Ledger::new(&c).snapshot(0);
        let lo = priority(&c, &snap, &incident(1, 1), 1_000);
        let hi = priority(&c, &snap, &incident(1, 5), 1_000);
        assert!(hi > lo);
    }
}
