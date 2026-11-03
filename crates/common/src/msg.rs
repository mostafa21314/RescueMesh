//! Wire messages. Each frame carries one `Message` as JSON; the `type` field
//! names the variant. docs/protocol.md documents each message, who sends it,
//! and the timeouts and retry rules around it.

use crate::model::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePhase {
    #[default]
    Ready,
    Muted,
    Recovering,
}

/// One retained request and its optional completed result, used during rejoin.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecoveryRecord {
    pub request_id: RequestId,
    pub attempt: u32,
    pub epoch: Epoch,
    pub incident: Incident,
    pub complexity: u32,
    pub result: Option<RecoveryResult>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecoveryResult {
    pub served_by: NodeId,
    pub epoch: Epoch,
    pub degraded: bool,
    pub plan: Plan,
}

/// What one recovery did and how long each part took.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RecoveryReport {
    /// "mute" (rejoin after a temporary failure) or "start" (process start).
    pub cause: String,
    /// From the end of the outage (or process start) to Ready.
    pub total_ms: u64,
    /// Waiting for this node's own planners to finish.
    pub drain_ms: u64,
    /// The last, successful round of snapshot transfers.
    pub fetch_ms: u64,
    /// Merging the snapshots, rebuilding the ledger, republishing.
    pub install_ms: u64,
    /// Transfer rounds, including failed or rejected ones.
    pub rounds: u32,
    /// Snapshots used, records and payload bytes received in them.
    pub snapshots_used: u32,
    pub records_received: u64,
    pub bytes_received: u64,
    /// Results this node did not hold before the merge.
    pub new_results: u64,
    /// Results republished to a survivor that lacked them.
    pub republished: u64,
    /// Started Ready and empty because no peer was Ready (process start only).
    pub bootstrap: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    /// Client → every node. Retries reuse `request_id` with `attempt + 1`.
    SubmitIncident {
        request_id: RequestId,
        attempt: u32,
        /// Highest epoch the client has seen in any response.
        client_epoch: Epoch,
        incident: Incident,
        /// Scales the planner's CPU cost (number of travel-time scenarios).
        complexity: u32,
    },
    /// Serving node (or any node holding the result) → client.
    PlanResponse {
        request_id: RequestId,
        served_by: NodeId,
        epoch: Epoch,
        degraded: bool,
        plan: Plan,
    },
    /// Serving node → both peers, after it has replied to the client.
    Replicate {
        epoch: Epoch,
        origin: NodeId,
        request_id: RequestId,
        plan: Plan,
        incident: Incident,
        /// The origin computed the plan while serving alone (degraded mode).
        #[serde(default)]
        degraded: bool,
    },
    /// Node → both peers: a bid in an election.
    ///
    /// On-demand mode: `request_id` is `None` and the election is for
    /// `target_epoch`. Per-request mode: the election is for
    /// (`request_id`, `attempt`) and `target_epoch` is unused (0).
    ElectBid {
        target_epoch: Epoch,
        node_id: NodeId,
        /// Load score in milli-units, 0..=1000; lower is less loaded.
        score: u32,
        /// The server whose failure triggered this election, as the bidder
        /// knows it (`None` at startup or when unknown).
        incumbent: Option<NodeId>,
        #[serde(default)]
        request_id: Option<RequestId>,
        #[serde(default)]
        attempt: u32,
    },
    /// Serving node → client: the planning queue is full.
    Busy {
        request_id: RequestId,
    },
    /// Demo control: temporarily silence this node (1..=20,000 ms).
    InjectFailure {
        duration_ms: u64,
    },
    FailureAccepted {
        node_id: NodeId,
        duration_ms: u64,
    },
    FailureRejected {
        reason: String,
    },
    /// Returning node pulls a fixed snapshot from each ready survivor.
    ///
    /// `since_ms`: send only entries changed in the last `since_ms` ms, as
    /// measured on the survivor's own clock (a duration, so clock skew between
    /// machines doesn't matter). `None`: send everything (full transfer).
    RecoveryRequest {
        node_id: NodeId,
        #[serde(default)]
        since_ms: Option<u64>,
    },
    RecoveryChunk {
        records: Vec<RecoveryRecord>,
    },
    RecoveryComplete {
        node_id: NodeId,
        epoch: Epoch,
        server: Option<NodeId>,
        electing: bool,
        records: usize,
    },
    /// Survivor → returning node: not Ready, so it has no trustworthy
    /// snapshot. `has_state` is false for a process that just started empty.
    RecoveryUnavailable {
        node_id: NodeId,
        phase: NodePhase,
        has_state: bool,
    },
    /// Diagnostic: client → node. Not part of request processing.
    StatusRequest,
    /// Diagnostic: node → client.
    Status(NodeStatus),
}

impl Message {
    /// Stable name used for traffic counters and logs.
    pub fn kind(&self) -> &'static str {
        match self {
            Message::SubmitIncident { .. } => "SubmitIncident",
            Message::PlanResponse { .. } => "PlanResponse",
            Message::Replicate { .. } => "Replicate",
            Message::Busy { .. } => "Busy",
            Message::ElectBid { .. } => "ElectBid",
            Message::StatusRequest => "StatusRequest",
            Message::Status(_) => "Status",
            Message::InjectFailure { .. } => "InjectFailure",
            Message::FailureAccepted { .. } => "FailureAccepted",
            Message::FailureRejected { .. } => "FailureRejected",
            Message::RecoveryRequest { .. } => "RecoveryRequest",
            Message::RecoveryChunk { .. } => "RecoveryChunk",
            Message::RecoveryComplete { .. } => "RecoveryComplete",
            Message::RecoveryUnavailable { .. } => "RecoveryUnavailable",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node_id: NodeId,
    #[serde(default)]
    pub phase: NodePhase,
    #[serde(default)]
    pub faults_injected: u64,
    #[serde(default)]
    pub last_recovery_ms: Option<u64>,
    /// Breakdown of this node's most recent recovery.
    #[serde(default)]
    pub last_recovery: Option<RecoveryReport>,
    /// Snapshots this node served to returning peers, and the longest single
    /// hold of its state lock while building one, in microseconds.
    #[serde(default)]
    pub snapshots_served: u64,
    #[serde(default)]
    pub snapshot_lock_max_us: u64,
    pub epoch: Epoch,
    pub server: Option<NodeId>,
    pub degraded: bool,
    /// Elections decided with at least 2 bids / ended with only this node's bid.
    pub elections_decided: u64,
    pub elections_alone: u64,
    /// Times this node saw two servers in one epoch and resolved it.
    pub split_resolutions: u64,
    pub step_downs: u64,
    /// Retry triggers ignored because the server's tenure was below min_tenure.
    pub suppressed_triggers: u64,
    /// Queued jobs dropped on stepping down.
    pub dropped_on_step_down: u64,
    /// Plans finished after this node stopped serving their epoch.
    pub late_results: u64,
    pub requests: usize,
    pub results: usize,
    /// Order-independent fingerprint of the results table; equal tables give
    /// equal digests.
    pub results_digest: u64,
    pub queue_len: usize,
    pub running: usize,
    pub pending_cost: u64,
    pub replicate_conflicts: u64,
    pub duplicate_replicates: u64,
    pub local_replans: u64,
    pub resource_overcommits: u64,
    pub peer_queue_drops: u64,
    /// (peer, milliseconds since last heard from; `None` = never). A list,
    /// not a map: integer map keys don't survive the tagged-enum decoding.
    pub peer_last_seen_ms: Vec<(NodeId, Option<u64>)>,
    /// Message type → (count, bytes), as sent and received by this node.
    pub sent: BTreeMap<String, (u64, u64)>,
    pub received: BTreeMap<String, (u64, u64)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips_through_json() {
        let st = NodeStatus {
            node_id: 1,
            phase: NodePhase::Ready,
            faults_injected: 0,
            last_recovery_ms: None,
            last_recovery: None,
            snapshots_served: 0,
            snapshot_lock_max_us: 0,
            epoch: 0,
            server: Some(1),
            degraded: false,
            elections_decided: 1,
            elections_alone: 0,
            split_resolutions: 0,
            step_downs: 0,
            suppressed_triggers: 0,
            dropped_on_step_down: 0,
            late_results: 0,
            requests: 3,
            results: 3,
            results_digest: 99,
            queue_len: 0,
            running: 0,
            pending_cost: 0,
            replicate_conflicts: 0,
            duplicate_replicates: 0,
            local_replans: 0,
            resource_overcommits: 0,
            peer_queue_drops: 0,
            peer_last_seen_ms: vec![(2, Some(5)), (3, None)],
            sent: BTreeMap::from([("Replicate".to_string(), (6, 600))]),
            received: BTreeMap::new(),
        };
        let msg = Message::Status(st);
        let back: Message = serde_json::from_slice(&serde_json::to_vec(&msg).unwrap()).unwrap();
        assert_eq!(back, msg);
    }
}
