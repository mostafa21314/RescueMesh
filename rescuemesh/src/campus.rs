//! Synthetic campus: a weighted zone graph plus seeded responders and resource offers.
//! Every cloud node generates the same campus from the same seed, so the initial
//! state is identical everywhere without any transfer.

use crate::model::*;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub const NO_PREV: u32 = u32::MAX;

#[derive(Clone, Debug)]
pub struct Campus {
    pub side: u32,
    /// adj[z] = (neighbour, travel cost in metres)
    pub adj: Vec<Vec<(ZoneId, u64)>>,
}

fn add_edge(adj: &mut [Vec<(ZoneId, u64)>], a: u32, b: u32, w: u64) {
    adj[a as usize].push((b, w));
    adj[b as usize].push((a, w));
}

impl Campus {
    /// side x side grid of zones, random edge lengths, plus `side` random shortcuts.
    pub fn generate(seed: u64, side: u32) -> Self {
        let n = side * side;
        let mut rng = StdRng::seed_from_u64(seed);
        let mut adj = vec![Vec::new(); n as usize];
        for r in 0..side {
            for c in 0..side {
                let id = r * side + c;
                if c + 1 < side {
                    let w = rng.gen_range(50..=250);
                    add_edge(&mut adj, id, id + 1, w);
                }
                if r + 1 < side {
                    let w = rng.gen_range(50..=250);
                    add_edge(&mut adj, id, id + side, w);
                }
            }
        }
        for _ in 0..side {
            let a = rng.gen_range(0..n);
            let b = rng.gen_range(0..n);
            if a != b {
                let w = rng.gen_range(150..=400);
                add_edge(&mut adj, a, b, w);
            }
        }
        Campus { side, adj }
    }

    pub fn zones(&self) -> u32 {
        self.side * self.side
    }

    /// Single-source shortest paths. Returns (dist, prev); prev points back toward `src`.
    pub fn dijkstra(&self, src: ZoneId) -> (Vec<u64>, Vec<u32>) {
        let n = self.adj.len();
        let mut dist = vec![u64::MAX; n];
        let mut prev = vec![NO_PREV; n];
        let mut heap = BinaryHeap::new();
        dist[src as usize] = 0;
        heap.push(Reverse((0u64, src)));
        while let Some(Reverse((d, u))) = heap.pop() {
            if d > dist[u as usize] {
                continue;
            }
            for &(v, w) in &self.adj[u as usize] {
                let nd = d + w;
                if nd < dist[v as usize] {
                    dist[v as usize] = nd;
                    prev[v as usize] = u;
                    heap.push(Reverse((nd, v)));
                }
            }
        }
        (dist, prev)
    }

    /// Path from `from` to the Dijkstra source, following `prev`.
    pub fn path_to_source(prev: &[u32], from: ZoneId) -> Vec<ZoneId> {
        let mut path = vec![from];
        let mut cur = from;
        while prev[cur as usize] != NO_PREV {
            cur = prev[cur as usize];
            path.push(cur);
        }
        path
    }
}

pub fn seed_responders(seed: u64, campus: &Campus, n: usize) -> Vec<Responder> {
    let mut rng = StdRng::seed_from_u64(seed ^ 0xA11C_E5ED);
    (0..n)
        .map(|i| {
            let mut caps = vec![Capability::General];
            for _ in 0..rng.gen_range(1..=2) {
                let c = Capability::ALL[rng.gen_range(0..Capability::ALL.len())];
                if !caps.contains(&c) {
                    caps.push(c);
                }
            }
            caps.sort();
            Responder {
                id: format!("resp-{i:04}"),
                zone: rng.gen_range(0..campus.zones()),
                capabilities: caps,
                busy_until_ms: 0,
            }
        })
        .collect()
}

pub fn seed_resources(seed: u64, campus: &Campus, n: usize) -> Vec<Resource> {
    let mut rng = StdRng::seed_from_u64(seed ^ 0x2E50_02CE);
    (0..n)
        .map(|i| Resource {
            id: format!("res-{i:04}"),
            owner: format!("provider-{:02}", i % 20),
            rtype: ResourceType::ALL[rng.gen_range(0..ResourceType::ALL.len())],
            capacity: rng.gen_range(1..=5),
            zone: rng.gen_range(0..campus.zones()),
            offer_expires_ms: u64::MAX,
        })
        .collect()
}
