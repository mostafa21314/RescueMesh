//! Executor selection policy: which cloud node performs a multicast request.
//!
//! Two policies, chosen with `rm-node --assigner`:
//!   * `election` (default): the leaderless per-request load-bidding election in
//!     `election.rs`. Load-aware; costs one bid exchange per request.
//!   * `hash`: the bullet-1 baseline, `fnv1a(request_id) % live.len()`. Load-blind and
//!     free (no messages); kept so experiments can compare against it.

use crate::model::NodeId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum AssignerKind {
    Election,
    Hash,
}

impl AssignerKind {
    pub fn name(self) -> &'static str {
        match self {
            AssignerKind::Election => "election",
            AssignerKind::Hash => "hash",
        }
    }
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

/// Baseline policy. `live` is sorted and always contains the local node; every node
/// with the same live view picks the same executor, but the choice ignores load and
/// remaps when membership changes.
pub fn hash_choose(request_id: &str, live: &[NodeId]) -> NodeId {
    live[(fnv1a(request_id.as_bytes()) % live.len() as u64) as usize]
}
