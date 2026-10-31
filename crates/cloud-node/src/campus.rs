//! Seeded synthetic campus. Every node generates it locally from the shared
//! seed, so all three start from identical state with nothing transferred.

use crate::graph::Graph;
use common::config::CampusParams;
use common::rng::Rng;
use common::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Campus {
    pub params: CampusParams,
    pub graph: Graph,
    pub responders: Vec<Responder>,
    pub resources: Vec<ResourceOffer>,
    /// All-pairs shortest travel cost on the base graph: base_dist[a][b].
    /// Used for resource placement and routes; responder ETAs are computed per
    /// request under perturbed travel times (see planner).
    pub base_dist: Vec<Vec<u32>>,
}

impl Campus {
    pub fn generate(seed: u64, params: CampusParams) -> Self {
        let mut rng = Rng::new(seed);
        let g = params.grid;
        let n = params.zones();

        // Lattice of walkways, travel cost 1..=9 per hop, plus `grid` longer
        // shortcuts (tunnels, bridges) so shortest paths aren't just Manhattan.
        let mut graph = Graph::new(n as usize);
        for r in 0..g {
            for c in 0..g {
                let z = r * g + c;
                if c + 1 < g {
                    graph.add_edge(z, z + 1, rng.range(1, 9) as u32);
                }
                if r + 1 < g {
                    graph.add_edge(z, z + g, rng.range(1, 9) as u32);
                }
            }
        }
        for _ in 0..g {
            let a = rng.below(n as u64) as u32;
            let b = rng.below(n as u64) as u32;
            if a != b {
                graph.add_edge(a, b, rng.range(10, 30) as u32);
            }
        }

        let responders = (0..params.responders)
            .map(|id| {
                let k = rng.range(1, 3) as usize;
                let mut caps: Vec<Capability> = Vec::with_capacity(k);
                while caps.len() < k {
                    let c = Capability::ALL[rng.below(Capability::ALL.len() as u64) as usize];
                    if !caps.contains(&c) {
                        caps.push(c);
                    }
                }
                caps.sort();
                Responder { id, zone: rng.below(n as u64) as u32, capabilities: caps }
            })
            .collect();

        let resources = (0..params.resources)
            .map(|resource_id| ResourceOffer {
                resource_id,
                owner: format!("dept-{}", rng.range(1, 8)),
                kind: ResourceType::ALL[rng.below(ResourceType::ALL.len() as u64) as usize],
                quantity: rng.range(1, 4) as u32,
                availability: if rng.chance(100) { Availability::Offline } else { Availability::Available },
                zone: rng.below(n as u64) as u32,
                lease_expiry_ms: None,
            })
            .collect();

        let base_dist = (0..n).map(|z| graph.base_dijkstra(z).dist).collect();

        Campus { params, graph, responders, resources, base_dist }
    }

    pub fn zones(&self) -> u32 {
        self.params.zones()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::UNREACHABLE;

    #[test]
    fn same_seed_same_state() {
        let p = CampusParams::default();
        assert_eq!(Campus::generate(42, p), Campus::generate(42, p));
    }

    #[test]
    fn different_seed_different_state() {
        let p = CampusParams::default();
        assert_ne!(Campus::generate(42, p), Campus::generate(43, p));
    }

    #[test]
    fn campus_is_connected_and_sized() {
        let p = CampusParams::default();
        let c = Campus::generate(7, p);
        assert_eq!(c.graph.len(), p.zones() as usize);
        assert_eq!(c.responders.len(), p.responders as usize);
        assert_eq!(c.resources.len(), p.resources as usize);
        assert!(c.base_dist.iter().all(|row| row.iter().all(|d| *d != UNREACHABLE)));
    }
}
