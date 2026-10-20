//! Incident priority and response-plan computation (Service 1).
//!
//! The plan for one incident is computed *jointly* with the other active incidents,
//! so a cheap local greedy pick cannot steal responders that a more urgent incident
//! needs. Steps:
//!
//! 1. Priority of every active incident = weighted severity + age + capability scarcity.
//! 2. Keep the `top_k` most urgent incidents (the target is always kept).
//! 3. Shortest paths (Dijkstra) from each kept incident's zone over the campus graph.
//! 4. Build a slots x responders cost matrix (distance, capability match, current
//!    commitment, divided by incident priority) and solve it optimally with the
//!    Hungarian algorithm, O(slots^2 * responders).
//! 5. Sensitivity analysis: re-solve `sensitivity_rounds` times with perturbed weights
//!    and report how stable the target's picks are.
//! 6. Pick nearest available resources of the required types, and a route.
//!
//! Everything is deterministic for a given snapshot, so runs are repeatable, and the
//! work per request is tunable through `top_k` and `sensitivity_rounds`.

use crate::campus::Campus;
use crate::model::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannerConfig {
    pub w_severity: f64,
    pub w_age: f64,
    pub w_scarcity: f64,
    pub w_distance: f64,
    pub w_capability: f64,
    pub w_busy: f64,
    pub top_k: usize,
    pub sensitivity_rounds: u32,
    pub assignment_ttl_ms: u64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        PlannerConfig {
            w_severity: 0.55,
            w_age: 0.25,
            w_scarcity: 0.20,
            w_distance: 0.6,
            w_capability: 0.3,
            w_busy: 0.1,
            top_k: 40,
            sensitivity_rounds: 6,
            assignment_ttl_ms: 60_000,
        }
    }
}

/// Read-only copy of the state the planner needs. Taken under the state lock, then
/// planning runs without holding it.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub now_ms: u64,
    pub incidents: Vec<Arc<Incident>>,
    pub responders: Vec<Responder>,
    /// (resource, units currently available)
    pub resources: Vec<(Resource, u32)>,
}

fn capability_match(r: &Responder, inc: &Incident) -> f64 {
    let req = inc.category.required_capabilities();
    let have = req.iter().filter(|c| r.capabilities.contains(c)).count();
    have as f64 / req.len() as f64
}

/// Per capability: share of responders with it that are currently committed (0 = plenty).
fn capability_scarcity(snap: &Snapshot) -> HashMap<Capability, f64> {
    let mut total: HashMap<Capability, usize> = HashMap::new();
    let mut busy: HashMap<Capability, usize> = HashMap::new();
    for r in &snap.responders {
        for c in &r.capabilities {
            *total.entry(*c).or_default() += 1;
            if r.busy_until_ms > snap.now_ms {
                *busy.entry(*c).or_default() += 1;
            }
        }
    }
    Capability::ALL
        .iter()
        .map(|c| {
            let t = *total.get(c).unwrap_or(&0);
            let s = if t == 0 { 1.0 } else { *busy.get(c).unwrap_or(&0) as f64 / t as f64 };
            (*c, s)
        })
        .collect()
}

pub fn priority(cfg: &PlannerConfig, inc: &Incident, now_ms: u64, scarcity: &HashMap<Capability, f64>) -> f64 {
    let sev = inc.severity.clamp(1, 5) as f64 / 5.0;
    let age = (now_ms.saturating_sub(inc.created_ms) as f64 / inc.ttl_ms.max(1) as f64).min(1.0);
    let req = inc.category.required_capabilities();
    let scar = req.iter().map(|c| scarcity.get(c).copied().unwrap_or(1.0)).sum::<f64>() / req.len() as f64;
    cfg.w_severity * sev + cfg.w_age * age + cfg.w_scarcity * scar
}

/// Optimal assignment, rows <= cols. Returns row -> col. (Kuhn-Munkres with potentials.)
pub fn hungarian(a: &[Vec<f64>]) -> Vec<usize> {
    let n = a.len();
    if n == 0 {
        return vec![];
    }
    let m = a[0].len();
    assert!(n <= m, "hungarian: rows must not exceed cols");
    let inf = f64::INFINITY;
    let mut u = vec![0.0; n + 1];
    let mut v = vec![0.0; m + 1];
    let mut p = vec![0usize; m + 1];
    let mut way = vec![0usize; m + 1];
    for i in 1..=n {
        p[0] = i;
        let mut j0 = 0usize;
        let mut minv = vec![inf; m + 1];
        let mut used = vec![false; m + 1];
        loop {
            used[j0] = true;
            let i0 = p[j0];
            let mut delta = inf;
            let mut j1 = 0usize;
            for j in 1..=m {
                if !used[j] {
                    let cur = a[i0 - 1][j - 1] - u[i0] - v[j];
                    if cur < minv[j] {
                        minv[j] = cur;
                        way[j] = j0;
                    }
                    if minv[j] < delta {
                        delta = minv[j];
                        j1 = j;
                    }
                }
            }
            for j in 0..=m {
                if used[j] {
                    u[p[j]] += delta;
                    v[j] -= delta;
                } else {
                    minv[j] -= delta;
                }
            }
            j0 = j1;
            if p[j0] == 0 {
                break;
            }
        }
        loop {
            let j1 = way[j0];
            p[j0] = p[j1];
            j0 = j1;
            if j0 == 0 {
                break;
            }
        }
    }
    let mut ans = vec![usize::MAX; n];
    for j in 1..=m {
        if p[j] != 0 {
            ans[p[j] - 1] = j - 1;
        }
    }
    ans
}

struct Problem<'a> {
    /// (priority, incident) for each kept incident, most urgent first
    incs: Vec<(f64, &'a Incident)>,
    /// slot -> index into incs
    slots: Vec<usize>,
    /// dist[k][zone] from incident k's zone
    dist: Vec<Vec<u64>>,
    max_dist: f64,
}

fn build_cost(cfg: &PlannerConfig, pb: &Problem, snap: &Snapshot, wd: f64, wc: f64) -> Vec<Vec<f64>> {
    let horizon = cfg.assignment_ttl_ms.max(1) as f64;
    pb.slots
        .iter()
        .map(|&k| {
            let (p, inc) = pb.incs[k];
            snap.responders
                .iter()
                .map(|r| {
                    let d = pb.dist[k][r.zone as usize];
                    let dn = if d == u64::MAX { 10.0 } else { d as f64 / pb.max_dist };
                    let busy = (r.busy_until_ms.saturating_sub(snap.now_ms) as f64 / horizon).min(1.0);
                    let raw = wd * dn + wc * (1.0 - capability_match(r, inc)) + cfg.w_busy * busy;
                    raw / (0.25 + p)
                })
                .collect()
        })
        .collect()
}

pub fn plan(
    cfg: &PlannerConfig,
    campus: &Campus,
    snap: &Snapshot,
    target: &Incident,
    request_id: &str,
    node: NodeId,
) -> ResponsePlan {
    let t0 = Instant::now();
    let now = snap.now_ms;
    let scarcity = capability_scarcity(snap);

    // 1-2. score active incidents, keep the most urgent top_k (always keep the target)
    let mut scored: Vec<(f64, &Incident)> = snap
        .incidents
        .iter()
        .map(|a| a.as_ref())
        .filter(|i| i.id != target.id && i.is_active(now))
        .chain(std::iter::once(target))
        .map(|i| (priority(cfg, i, now, &scarcity), i))
        .collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.id.cmp(&b.1.id)));
    let considered = scored.len();
    let rank = scored.iter().position(|(_, i)| i.id == target.id).unwrap() + 1;
    let target_priority = scored[rank - 1].0;
    let keep = cfg.top_k.max(1);
    if scored.len() > keep {
        if rank > keep {
            let t = scored[rank - 1];
            scored.truncate(keep - 1);
            scored.push(t);
        } else {
            scored.truncate(keep);
        }
    }
    let tk = scored.iter().position(|(_, i)| i.id == target.id).unwrap();

    // 3. shortest paths from each kept incident (cached by zone)
    let mut by_zone: HashMap<ZoneId, (Vec<u64>, Vec<u32>)> = HashMap::new();
    for (_, inc) in &scored {
        by_zone.entry(inc.zone).or_insert_with(|| campus.dijkstra(inc.zone));
    }
    let dist: Vec<Vec<u64>> = scored.iter().map(|(_, i)| by_zone[&i.zone].0.clone()).collect();
    let max_dist = dist
        .iter()
        .flat_map(|d| d.iter().copied().filter(|&x| x != u64::MAX))
        .max()
        .unwrap_or(1)
        .max(1) as f64;

    // slots in priority order; at most one slot per responder
    let mut slots = Vec::new();
    for (k, (_, inc)) in scored.iter().enumerate() {
        for _ in 0..inc.responders_needed() {
            slots.push(k);
        }
    }
    let m = snap.responders.len();
    if slots.len() > m {
        // drop lowest-priority slots, but never the target's
        let mut kept: Vec<usize> = slots.iter().copied().filter(|&k| k == tk).collect();
        for &k in &slots {
            if kept.len() >= m {
                break;
            }
            if k != tk {
                kept.push(k);
            }
        }
        kept.truncate(m);
        kept.sort();
        slots = kept;
    }
    let pb = Problem { incs: scored, slots, dist, max_dist };

    // 4. optimal joint assignment
    let mut target_picks: Vec<(usize, f64)> = Vec::new(); // (responder idx, cost)
    if m > 0 && !pb.slots.is_empty() {
        let cost = build_cost(cfg, &pb, snap, cfg.w_distance, cfg.w_capability);
        let sol = hungarian(&cost);
        for (s, &k) in pb.slots.iter().enumerate() {
            if k == tk && sol[s] != usize::MAX {
                target_picks.push((sol[s], cost[s][sol[s]]));
            }
        }
    }
    target_picks.sort_by(|a, b| a.1.total_cmp(&b.1));

    // 5. sensitivity analysis under perturbed weights
    let perturb = [(1.2, 0.8), (0.8, 1.2), (1.4, 1.0), (1.0, 1.4), (0.7, 1.0), (1.0, 0.7)];
    let mut kept_hits = 0usize;
    let mut kept_total = 0usize;
    if m > 0 && !target_picks.is_empty() {
        for r in 0..cfg.sensitivity_rounds as usize {
            let (fd, fc) = perturb[r % perturb.len()];
            let scale = 1.0 + 0.05 * (r / perturb.len()) as f64;
            let cost = build_cost(cfg, &pb, snap, cfg.w_distance * fd * scale, cfg.w_capability * fc);
            let sol = hungarian(&cost);
            let alt: Vec<usize> = pb
                .slots
                .iter()
                .enumerate()
                .filter(|(_, &k)| k == tk)
                .map(|(s, _)| sol[s])
                .collect();
            kept_total += target_picks.len();
            kept_hits += target_picks.iter().filter(|(ri, _)| alt.contains(ri)).count();
        }
    }
    let stability = if kept_total == 0 { 1.0 } else { kept_hits as f64 / kept_total as f64 };

    let tdist = &pb.dist[tk];
    let (_, tprev) = &by_zone[&target.zone];
    let responders: Vec<RankedResponder> = target_picks
        .iter()
        .map(|&(ri, c)| {
            let r = &snap.responders[ri];
            RankedResponder {
                responder_id: r.id.clone(),
                zone: r.zone,
                travel_cost: tdist[r.zone as usize],
                capability_match: capability_match(r, target),
                cost: c,
            }
        })
        .collect();

    // 6. resources: nearest offer with spare units for each required type
    let need_units = 1 + (target.severity.clamp(1, 5) as u32 - 1) / 2;
    let mut resources = Vec::new();
    let mut shortfall = Vec::new();
    for rt in target.category.required_resources() {
        let best = snap
            .resources
            .iter()
            .filter(|(r, avail)| r.rtype == *rt && *avail > 0 && r.offer_expires_ms > now)
            .min_by(|a, b| tdist[a.0.zone as usize].cmp(&tdist[b.0.zone as usize]).then_with(|| a.0.id.cmp(&b.0.id)));
        match best {
            Some((r, avail)) => {
                let q = need_units.min(*avail);
                resources.push(ResourceAllocation {
                    resource_id: r.id.clone(),
                    rtype: *rt,
                    quantity: q,
                    travel_cost: tdist[r.zone as usize],
                });
                if q < need_units {
                    shortfall.push(*rt);
                }
            }
            None => shortfall.push(*rt),
        }
    }

    // route for the lead responder
    let (route, route_cost) = match responders.first() {
        Some(lead) => (Campus::path_to_source(tprev, lead.zone), lead.travel_cost),
        None => (vec![target.zone], 0),
    };

    let expires = now + cfg.assignment_ttl_ms;
    let mut assignments = Vec::new();
    for r in &responders {
        assignments.push(Assignment {
            id: format!("{request_id}/R/{}", r.responder_id),
            incident_id: target.id.clone(),
            assignee: Assignee::Responder { id: r.responder_id.clone() },
            created_ms: now,
            expires_ms: expires,
        });
    }
    for a in &resources {
        assignments.push(Assignment {
            id: format!("{request_id}/X/{}", a.resource_id),
            incident_id: target.id.clone(),
            assignee: Assignee::Resource { id: a.resource_id.clone(), quantity: a.quantity },
            created_ms: now,
            expires_ms: expires,
        });
    }

    let mut actions = Vec::new();
    match responders.first() {
        Some(lead) => actions.push(format!(
            "Dispatch {} from zone {} to zone {} ({} m, {} hops)",
            lead.responder_id,
            lead.zone,
            target.zone,
            lead.travel_cost,
            route.len().saturating_sub(1)
        )),
        None => actions.push("No responder available: escalate".to_string()),
    }
    for r in responders.iter().skip(1) {
        actions.push(format!("Support: {} ({} m)", r.responder_id, r.travel_cost));
    }
    for a in &resources {
        actions.push(format!("Deliver {} x {:?} from {}", a.quantity, a.rtype, a.resource_id));
    }
    for s in &shortfall {
        actions.push(format!("Shortfall: {:?} not fully covered, request more from peers", s));
    }

    ResponsePlan {
        request_id: request_id.to_string(),
        incident_id: target.id.clone(),
        executed_by: node,
        priority: target_priority,
        rank,
        incidents_considered: considered,
        responders,
        resources,
        shortfall,
        route,
        route_cost,
        stability,
        actions,
        assignments,
        created_ms: now,
        compute_us: t0.elapsed().as_micros() as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campus::*;

    #[test]
    fn hungarian_small() {
        let a = vec![vec![4.0, 1.0, 3.0], vec![2.0, 0.0, 5.0], vec![3.0, 2.0, 2.0]];
        let s = hungarian(&a);
        let total: f64 = s.iter().enumerate().map(|(i, &j)| a[i][j]).sum();
        assert_eq!(total, 5.0);
    }

    #[test]
    fn hungarian_rectangular() {
        let a = vec![vec![9.0, 1.0, 9.0, 9.0], vec![9.0, 9.0, 9.0, 2.0]];
        assert_eq!(hungarian(&a), vec![1, 3]);
    }

    fn inc(id: &str, zone: u32, sev: u8) -> Incident {
        Incident {
            id: id.into(),
            reporter: "u".into(),
            category: Category::MedicalAssist,
            zone,
            severity: sev,
            created_ms: 1_000,
            status: IncidentStatus::Open,
            ttl_ms: 600_000,
        }
    }

    #[test]
    fn plan_is_deterministic_and_complete() {
        let c = Campus::generate(7, 8);
        let snap = Snapshot {
            now_ms: 2_000,
            incidents: (0..10).map(|i| Arc::new(inc(&format!("i{i}"), i * 3, (i % 5 + 1) as u8))).collect(),
            responders: seed_responders(7, &c, 30),
            resources: seed_resources(7, &c, 30).into_iter().map(|r| { let a = r.capacity; (r, a) }).collect(),
        };
        let t = inc("target", 20, 5);
        let cfg = PlannerConfig::default();
        let p1 = plan(&cfg, &c, &snap, &t, "req-1", 1);
        let p2 = plan(&cfg, &c, &snap, &t, "req-1", 1);
        assert_eq!(p1.responders.len(), 3);
        assert_eq!(
            p1.responders.iter().map(|r| &r.responder_id).collect::<Vec<_>>(),
            p2.responders.iter().map(|r| &r.responder_id).collect::<Vec<_>>()
        );
        assert_eq!(*p1.route.last().unwrap(), 20);
        assert_eq!(p1.route[0], p1.responders[0].zone);
    }
}
