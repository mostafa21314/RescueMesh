//! Undirected weighted zone graph and Dijkstra.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub const UNREACHABLE: u32 = u32::MAX;
pub const NO_PARENT: u32 = u32::MAX;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Edge {
    pub a: u32,
    pub b: u32,
    pub w: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Graph {
    pub edges: Vec<Edge>,
    /// adj[zone] = (neighbour, index into `edges`)
    pub adj: Vec<Vec<(u32, usize)>>,
}

pub struct ShortestPaths {
    pub dist: Vec<u32>,
    pub parent: Vec<u32>,
}

impl Graph {
    pub fn new(n: usize) -> Self {
        Graph {
            edges: Vec::new(),
            adj: vec![Vec::new(); n],
        }
    }

    pub fn len(&self) -> usize {
        self.adj.len()
    }

    pub fn is_empty(&self) -> bool {
        self.adj.is_empty()
    }

    pub fn add_edge(&mut self, a: u32, b: u32, w: u32) {
        let i = self.edges.len();
        self.edges.push(Edge { a, b, w });
        self.adj[a as usize].push((b, i));
        self.adj[b as usize].push((a, i));
    }

    /// Dijkstra from `src` with per-edge weights from `weight(edge_index)`.
    /// With `stop_at = Some(t)` it returns as soon as `t` is settled; other
    /// distances may then be incomplete.
    pub fn dijkstra(
        &self,
        src: u32,
        stop_at: Option<u32>,
        weight: impl Fn(usize) -> u32,
    ) -> ShortestPaths {
        let n = self.len();
        let mut dist = vec![UNREACHABLE; n];
        let mut parent = vec![NO_PARENT; n];
        let mut heap = BinaryHeap::new();
        dist[src as usize] = 0;
        heap.push(Reverse((0u32, src)));
        while let Some(Reverse((d, u))) = heap.pop() {
            if d > dist[u as usize] {
                continue;
            }
            if stop_at == Some(u) {
                break;
            }
            for &(v, e) in &self.adj[u as usize] {
                let nd = d.saturating_add(weight(e));
                if nd < dist[v as usize] {
                    dist[v as usize] = nd;
                    parent[v as usize] = u;
                    heap.push(Reverse((nd, v)));
                }
            }
        }
        ShortestPaths { dist, parent }
    }

    pub fn base_dijkstra(&self, src: u32) -> ShortestPaths {
        self.dijkstra(src, None, |e| self.edges[e].w)
    }
}

impl ShortestPaths {
    /// Zones from the source to `dst`, inclusive; empty if unreachable.
    pub fn path_to(&self, dst: u32) -> Vec<u32> {
        if self.dist[dst as usize] == UNREACHABLE {
            return Vec::new();
        }
        let mut path = vec![dst];
        let mut cur = dst;
        while self.parent[cur as usize] != NO_PARENT {
            cur = self.parent[cur as usize];
            path.push(cur);
        }
        path.reverse();
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_cheaper_detour() {
        // 0 -10- 1, 0 -1- 2 -1- 1
        let mut g = Graph::new(3);
        g.add_edge(0, 1, 10);
        g.add_edge(0, 2, 1);
        g.add_edge(2, 1, 1);
        let sp = g.base_dijkstra(0);
        assert_eq!(sp.dist, vec![0, 2, 1]);
        assert_eq!(sp.path_to(1), vec![0, 2, 1]);
    }
}
