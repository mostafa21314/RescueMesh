//! Executor selection: which cloud node performs a multicast request.
//!
//! PHASE 1 / BULLET 1 PLACEHOLDER. This trait is the swap-in point for the
//! distributed load-ranking election chosen in the Phase 0 survey (bullet 2).
//! `HashAssigner` is deterministic but deliberately load-blind: every node that
//! sees the same request and the same live set picks the same executor, which is
//! enough to exercise the three-node service end to end.

use crate::model::NodeId;

pub trait Assigner: Send + Sync {
    /// `live` is sorted and always contains the local node.
    fn choose(&self, request_id: &str, live: &[NodeId]) -> NodeId;
    fn name(&self) -> &'static str;
}

/// 64-bit FNV-1a: stable across machines and Rust versions (unlike std's hasher).
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

pub struct HashAssigner;

impl Assigner for HashAssigner {
    fn choose(&self, request_id: &str, live: &[NodeId]) -> NodeId {
        live[(fnv1a(request_id.as_bytes()) % live.len() as u64) as usize]
    }
    fn name(&self) -> &'static str {
        "hash-placeholder"
    }
}
