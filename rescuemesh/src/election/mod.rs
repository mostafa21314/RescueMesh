//! Public contract for coordinator election and per-request worker selection.
//!
//! The runtime owns networking, timers, storage, and load measurement. An
//! implementation consumes events and returns actions for the runtime to perform.
//! Worker proposals must be committed by the replication/scheduling layer before
//! execution. This interface alone does not implement Raft log replication.

use std::time::Duration;

pub mod config;
pub use config::{
    CoordinatorConfig, CoordinatorConfigError, ElectionConfig, ElectionConfigError,
    WorkerSelectionConfig, WorkerSelectionConfigError,
};
mod coordinator;
pub use coordinator::Coordinator;
mod worker_selection;
pub use worker_selection::WorkerSelector;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// Must remain the same across client retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId(pub u128);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Term(pub u64);

/// Compare term first, then index when checking candidate eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogPosition {
    pub term: Term,
    pub index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionStatus {
    pub node: NodeId,
    pub term: Term,
    pub role: Role,
    pub coordinator: Option<NodeId>,
}

/// Durable election metadata, restored before participating after a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

/// Identifies one bid round. Retries must use a new attempt or coordinator term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SelectionId {
    pub request: RequestId,
    pub coordinator_term: Term,
    pub attempt: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadBid {
    pub selection: SelectionId,
    pub node: NodeId,
    /// Lower is better; every node must use the same units and scoring policy.
    pub score: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionMessage {
    RequestVote { term: Term, last_log: LogPosition },
    VoteResponse { term: Term, granted: bool },
    RequestLoadBid { selection: SelectionId },
    LoadBid(LoadBid),
}

/// Generations allow implementations to ignore callbacks from cancelled timers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimerId {
    Election(u64),
    Heartbeat(u64),
    Bids(SelectionId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionEvent {
    /// Runtime forwards the local Coordinator's StatusChanged action to its selector.
    /// This is trusted local state, never a status supplied by a remote peer.
    CoordinatorStatusUpdated(ElectionStatus),
    /// `from` comes from the transport's identified peer connection.
    Message {
        from: NodeId,
        message: ElectionMessage,
    },
    TimerElapsed(TimerId),
    /// Supplied by the replication layer after validating leader communication.
    CoordinatorContact {
        from: NodeId,
        term: Term,
    },
    /// Supplied after the local durable log changes, before handling more votes.
    LocalLogUpdated(LogPosition),
    /// Starts a bid round; only the current coordinator may initiate selection.
    SelectWorker {
        selection: SelectionId,
    },
    /// Answer to a local load-measurement action; included as a normal bid.
    LocalLoadMeasured(LoadBid),
    /// Sent only after the corresponding metadata write succeeds durably.
    StatePersisted {
        write_id: u64,
    },
    /// A recovering node becomes eligible only after state synchronization.
    EligibilityChanged {
        node: NodeId,
        eligible: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionAction {
    Send {
        to: NodeId,
        message: ElectionMessage,
    },
    ArmTimer {
        timer: TimerId,
        after: Duration,
    },
    CancelTimer(TimerId),
    /// Do not send dependent votes or announcements until StatePersisted arrives.
    PersistState {
        write_id: u64,
        state: HardState,
    },
    /// Ask the replication layer to send its normal leader heartbeat.
    RequestHeartbeat {
        term: Term,
    },
    MeasureLocalLoad {
        selection: SelectionId,
    },
    StatusChanged(ElectionStatus),
    /// Advisory output, not an authorized assignment. Break equal scores by NodeId.
    WorkerProposed {
        selection: SelectionId,
        worker: NodeId,
    },
    /// No safe proposal was possible, for example because too few bids arrived.
    SelectionUnavailable {
        selection: SelectionId,
    },
}

/// Implemented by the election state machine; no network or storage I/O here.
///
/// Feed events serially for each node. Implementations must retain the fixed
/// voting membership when peers time out, ignore stale rounds/timers, and require
/// a majority for coordinator election. Returning WorkerProposed does not grant
/// permission to execute: the caller must first commit an assignment.
pub trait Election {
    fn handle(&mut self, event: ElectionEvent) -> Vec<ElectionAction>;

    fn status(&self) -> ElectionStatus;
}
