//! Per-request load bidding, separate from coordinator election and assignment
//! commitment. Every node runs a selector alongside its Coordinator.
//!
//! Forward local Coordinator StatusChanged actions as CoordinatorStatusUpdated
//! events BEFORE delivering selection events. Route selection messages, Bids
//! timers, load callbacks, and worker eligibility updates here. Execute returned
//! actions through the runtime. WorkerProposed must go through replicated
//! assignment commitment before any worker executes it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::{
    Election, ElectionAction, ElectionEvent, ElectionMessage, ElectionStatus, LoadBid, NodeId,
    RequestId, Role, SelectionId, Term, TimerId, WorkerSelectionConfig, WorkerSelectionConfigError,
};

struct Round {
    id: SelectionId,
    coordinator: NodeId,
    participants: BTreeSet<NodeId>,
    bids: BTreeMap<NodeId, u64>,
    local_score: Option<u64>,
    finished: bool,
}

/// Collects one fixed score per node per round, using node ID to break ties.
///
/// The coordinator waits for all eligible participants, or the bid deadline. At
/// the deadline, a fixed-membership majority of valid bids is sufficient to
/// propose a worker. This bid threshold is NOT assignment consensus.
///
/// Retry with a strictly larger attempt number in the same term. After a restart,
/// recover coordinator/request state first; do not reuse old selection IDs.
/// Scores use a common policy supplied by the runtime, including queued, active,
/// and newly committed work. Scores are frozen within a round; load changes are
/// reflected in subsequent rounds. No sockets, clocks, or load sampling run here.
pub struct WorkerSelector {
    status: ElectionStatus,
    members: BTreeSet<NodeId>,
    eligible: BTreeSet<NodeId>,
    config: WorkerSelectionConfig,
    rounds: HashMap<RequestId, Round>,
}

impl WorkerSelector {
    pub fn new(
        node: NodeId,
        members: impl IntoIterator<Item = NodeId>,
        config: WorkerSelectionConfig,
    ) -> Result<Self, WorkerSelectionConfigError> {
        let membership: Vec<_> = members.into_iter().collect();
        let members: BTreeSet<_> = membership.iter().copied().collect();
        if members.len() != membership.len() || !members.contains(&node) {
            return Err(WorkerSelectionConfigError(
                "membership must be unique and include this node",
            ));
        }
        config.validate()?;
        Ok(Self {
            status: ElectionStatus {
                node,
                term: Term(0),
                role: Role::Follower,
                coordinator: None,
            },
            eligible: members.clone(),
            members,
            config,
            rounds: HashMap::new(),
        })
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    fn is_coordinator(&self) -> bool {
        self.status.role == Role::Leader && self.status.coordinator == Some(self.status.node)
    }

    fn finish(&mut self, request: RequestId, deadline: bool, actions: &mut Vec<ElectionAction>) {
        let quorum = self.quorum();
        let Some(round) = self.rounds.get_mut(&request) else {
            return;
        };
        if round.finished {
            return;
        }
        let collecting = round.coordinator == self.status.node;
        if !deadline
            && (!collecting
                || round.bids.len() < quorum
                || round.bids.len() != round.participants.len())
        {
            return;
        }
        round.finished = true;
        actions.push(ElectionAction::CancelTimer(TimerId::Bids(round.id)));
        if !collecting {
            return;
        }
        if round.bids.len() >= quorum {
            let worker = round
                .bids
                .iter()
                .min_by_key(|(node, score)| (**score, **node))
                .map(|(&node, _)| node)
                .expect("majority has at least one bid");
            actions.push(ElectionAction::WorkerProposed {
                selection: round.id,
                worker,
            });
        } else {
            actions.push(ElectionAction::SelectionUnavailable {
                selection: round.id,
            });
        }
    }

    fn cancel_round(round: &mut Round, node: NodeId, actions: &mut Vec<ElectionAction>) {
        if round.finished {
            return;
        }
        round.finished = true;
        actions.push(ElectionAction::CancelTimer(TimerId::Bids(round.id)));
        if round.coordinator == node {
            actions.push(ElectionAction::SelectionUnavailable {
                selection: round.id,
            });
        }
    }

    fn update_status(&mut self, status: ElectionStatus, actions: &mut Vec<ElectionAction>) {
        if status.node != self.status.node
            || status.term < self.status.term
            || status
                .coordinator
                .is_some_and(|id| !self.members.contains(&id))
            || (status.role == Role::Leader && status.coordinator != Some(status.node))
            || (status.role != Role::Leader && status.coordinator == Some(status.node))
            || (status.role == Role::Candidate && status.coordinator.is_some())
            || status == self.status
        {
            return;
        }
        for round in self.rounds.values_mut() {
            Self::cancel_round(round, self.status.node, actions);
        }
        if status.term > self.status.term {
            self.rounds.clear();
        }
        self.status = status;
    }

    fn begin(
        &mut self,
        selection: SelectionId,
        coordinator: NodeId,
        actions: &mut Vec<ElectionAction>,
    ) {
        if selection.coordinator_term != self.status.term {
            return;
        }
        if !self.eligible.contains(&self.status.node) {
            if coordinator == self.status.node {
                actions.push(ElectionAction::SelectionUnavailable { selection });
            }
            return;
        }
        if let Some(previous) = self.rounds.get_mut(&selection.request) {
            if selection.attempt < previous.id.attempt {
                return;
            }
            if selection.attempt == previous.id.attempt {
                // Replay the original score rather than measuring again.
                if !previous.finished
                    && coordinator != self.status.node
                    && let Some(score) = previous.local_score
                {
                    actions.push(ElectionAction::Send {
                        to: coordinator,
                        message: ElectionMessage::LoadBid(LoadBid {
                            selection,
                            node: self.status.node,
                            score,
                        }),
                    });
                }
                return;
            }
            Self::cancel_round(previous, self.status.node, actions);
        } else if self.rounds.len() >= self.config.max_tracked_requests {
            if coordinator == self.status.node {
                actions.push(ElectionAction::SelectionUnavailable { selection });
            }
            return;
        }
        let insufficient = coordinator == self.status.node && self.eligible.len() < self.quorum();
        self.rounds.insert(
            selection.request,
            Round {
                id: selection,
                coordinator,
                participants: self.eligible.clone(),
                bids: BTreeMap::new(),
                local_score: None,
                finished: insufficient,
            },
        );
        if insufficient {
            actions.push(ElectionAction::SelectionUnavailable { selection });
            return;
        }
        actions.push(ElectionAction::ArmTimer {
            timer: TimerId::Bids(selection),
            after: self.config.bid_timeout,
        });
        actions.push(ElectionAction::MeasureLocalLoad { selection });
        if coordinator == self.status.node {
            for &to in &self.eligible {
                if to != self.status.node {
                    actions.push(ElectionAction::Send {
                        to,
                        message: ElectionMessage::RequestLoadBid { selection },
                    });
                }
            }
        }
    }

    fn receive_bid(&mut self, bid: LoadBid, actions: &mut Vec<ElectionAction>) {
        if !self.is_coordinator() {
            return;
        }
        let Some(round) = self.rounds.get_mut(&bid.selection.request) else {
            return;
        };
        if round.finished || round.id != bid.selection || !round.participants.contains(&bid.node) {
            return;
        }
        // First accepted score wins, even if a duplicate carries a changed value.
        round.bids.entry(bid.node).or_insert(bid.score);
        self.finish(bid.selection.request, false, actions);
    }

    fn local_load(&mut self, bid: LoadBid, actions: &mut Vec<ElectionAction>) {
        if bid.node != self.status.node || !self.eligible.contains(&bid.node) {
            return;
        }
        let Some(round) = self.rounds.get_mut(&bid.selection.request) else {
            return;
        };
        if round.finished
            || round.id != bid.selection
            || round.local_score.is_some()
            || !round.participants.contains(&bid.node)
        {
            return;
        }
        round.local_score = Some(bid.score);
        if round.coordinator == self.status.node {
            self.receive_bid(bid, actions);
        } else {
            actions.push(ElectionAction::Send {
                to: round.coordinator,
                message: ElectionMessage::LoadBid(bid),
            });
        }
    }

    fn eligibility(&mut self, node: NodeId, eligible: bool, actions: &mut Vec<ElectionAction>) {
        if !self.members.contains(&node) {
            return;
        }
        if eligible {
            // A recovered node can join future rounds, not alter a frozen round.
            self.eligible.insert(node);
            return;
        }
        self.eligible.remove(&node);
        let requests: Vec<_> = self.rounds.keys().copied().collect();
        for request in requests {
            let round = self.rounds.get_mut(&request).expect("existing request");
            if round.finished {
                continue;
            }
            round.participants.remove(&node);
            round.bids.remove(&node);
            if node == self.status.node {
                Self::cancel_round(round, node, actions);
            } else {
                self.finish(request, false, actions);
            }
        }
    }
}

impl Election for WorkerSelector {
    fn handle(&mut self, event: ElectionEvent) -> Vec<ElectionAction> {
        let mut actions = Vec::new();
        match event {
            ElectionEvent::CoordinatorStatusUpdated(status) => {
                self.update_status(status, &mut actions)
            }
            ElectionEvent::SelectWorker { selection } if self.is_coordinator() => {
                self.begin(selection, self.status.node, &mut actions);
            }
            ElectionEvent::Message { from, message }
                if from != self.status.node && self.members.contains(&from) =>
            {
                match message {
                    ElectionMessage::RequestLoadBid { selection }
                        if self.status.coordinator == Some(from)
                            && self.status.role == Role::Follower =>
                    {
                        self.begin(selection, from, &mut actions);
                    }
                    ElectionMessage::LoadBid(bid) if bid.node == from => {
                        self.receive_bid(bid, &mut actions)
                    }
                    _ => {}
                }
            }
            ElectionEvent::LocalLoadMeasured(bid) => self.local_load(bid, &mut actions),
            ElectionEvent::TimerElapsed(TimerId::Bids(selection)) => {
                if self
                    .rounds
                    .get(&selection.request)
                    .is_some_and(|round| round.id == selection)
                {
                    self.finish(selection.request, true, &mut actions);
                }
            }
            ElectionEvent::EligibilityChanged { node, eligible } => {
                self.eligibility(node, eligible, &mut actions)
            }
            _ => {}
        }
        actions
    }

    fn status(&self) -> ElectionStatus {
        self.status
    }
}
