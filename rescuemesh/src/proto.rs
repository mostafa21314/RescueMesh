//! Message formats. One JSON object per frame, frames are length-prefixed
//! (4-byte big-endian length, see `wire.rs`). The `type` field selects the variant.

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
    pub completed: u64,
    /// EWMA of planner service time (ms).
    pub ewma_service_ms: f64,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Msg {
    // ---- client -> cloud (multicast to every node) ----
    Submit { request_id: RequestId, client_id: String, incident: Incident },
    StatusQuery,

    // ---- cloud -> client ----
    /// Sent only by the executing node. `duplicate` = answered from the idempotency cache.
    PlanResult { request_id: RequestId, plan: Arc<ResponsePlan>, duplicate: bool },
    Status(StatusReport),
    Error { request_id: Option<RequestId>, message: String },

    // ---- cloud <-> cloud ----
    /// First frame on an outbound peer connection; identifies it as a peer link.
    Hello { node: NodeId },
    Heartbeat { node: NodeId, load: LoadReport },
    Replicate { from: NodeId, op: ReplOp },
}
