//! Message formats. One JSON object per frame, frames are length-prefixed
//! (4-byte big-endian length, see `wire.rs`). The `type` field selects the variant.

use crate::election::ElectionStats;
use crate::model::*;
use crate::state::ReplOp;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LoadReport {
    /// Requests waiting for a worker.
    pub queue_len: u32,
    /// Requests currently being planned.
    pub inflight: u32,
    pub workers: u32,
    /// Requests won in an election but not yet enqueued.
    pub reserved: u32,
    pub completed: u64,
    /// Current service-time estimate (ms): work EWMA x CPU probe ratio.
    pub ewma_service_ms: f64,
    /// CPU probe time / fastest probe seen (1.0 = full speed).
    pub probe_ratio: f64,
    /// The bid this node would make right now (µs).
    pub score_us: u64,
    /// Injected slowdown factor (1.0 = none); see `rm-client slow`.
    pub slow_factor: f64,
    pub sent_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusReport {
    pub node: NodeId,
    pub version: u64,
    pub active_incidents: usize,
    pub plans: usize,
    pub plans_digest: String,
    pub pending: usize,
    pub executed_here: u64,
    pub load: LoadReport,
    pub live: Vec<NodeId>,
    pub assigner: String,
    pub dropped_peer_msgs: u64,
    pub election: ElectionStats,
    /// Second plans received for an already-planned request (see state.rs).
    pub duplicate_executions: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Msg {
    // ---- client -> cloud (multicast to every node) ----
    Submit { request_id: RequestId, client_id: String, incident: Incident },
    StatusQuery,
    /// Admin (experiments/demo): emulate a CPU `factor`x slower on the receiving node
    /// for `secs` seconds (0 = until changed; factor 1 = off). Answered with `Ack`.
    Slow { factor: f64, secs: u64 },

    // ---- cloud -> client ----
    /// Sent only by the executing node. `duplicate` = answered from the idempotency cache.
    PlanResult { request_id: RequestId, plan: Arc<ResponsePlan>, duplicate: bool },
    Status(StatusReport),
    Error { request_id: Option<RequestId>, message: String },
    Ack { message: String },

    // ---- cloud <-> cloud ----
    /// First frame on an outbound peer connection; identifies it as a peer link.
    Hello { node: NodeId },
    Heartbeat { node: NodeId, load: LoadReport },
    Replicate { from: NodeId, op: ReplOp },
    /// Election bid: `node`'s estimated completion time for `request_id`, computed
    /// when its Submit arrived. Sent once per (request, attempt) to every peer.
    Bid { request_id: RequestId, attempt: u32, node: NodeId, score_us: u64 },
}
