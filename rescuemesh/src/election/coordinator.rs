//! Coordinator election only. Replication validates leader contact and implements
//! AppendEntries/log commitment; worker selection is handled separately.

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use super::{
    CoordinatorConfig, CoordinatorConfigError, Election, ElectionAction, ElectionEvent,
    ElectionMessage, ElectionStatus, HardState, LogPosition, NodeId, Role, Term, TimerId,
};

struct PendingWrite {
    id: u64,
    after_persist: Vec<ElectionAction>,
}

/// A fixed-membership, crash-recovery coordinator election state machine.
///
/// Construct from durable metadata and the last durable log position, after local
/// recovery. Call `start()` and execute its actions, then feed events through
/// `handle()`. `handle()` also starts the machine if necessary.
///
/// Pass an independently chosen seed per node/process start for timeout jitter.
/// The small PRNG is for scheduling only, not security. PersistState writes must
/// be atomic and durable before acknowledging them. On storage failure, stop the
/// node; do not report StatePersisted. Events arriving during a write are queued,
/// so the runtime must apply backpressure if storage stops making progress.
pub struct Coordinator {
    node: NodeId,
    members: BTreeSet<NodeId>,
    config: CoordinatorConfig,
    hard: HardState,
    last_log: LogPosition,
    role: Role,
    leader: Option<NodeId>,
    votes: BTreeSet<NodeId>,
    eligible: bool,
    started: bool,
    random_state: u64,
    generation: u64,
    election_timer: Option<TimerId>,
    heartbeat_timer: Option<TimerId>,
    next_write: u64,
    pending: Option<PendingWrite>,
    queued: VecDeque<ElectionEvent>,
}

impl Coordinator {
    pub fn new(
        node: NodeId,
        members: impl IntoIterator<Item = NodeId>,
        restored: HardState,
        last_log: LogPosition,
        config: CoordinatorConfig,
        random_seed: u64,
    ) -> Result<Self, CoordinatorConfigError> {
        let membership: Vec<_> = members.into_iter().collect();
        let members: BTreeSet<_> = membership.iter().copied().collect();
        if members.len() != membership.len() || !members.contains(&node) {
            return Err(CoordinatorConfigError(
                "membership must be unique and include this node",
            ));
        }
        if restored.voted_for.is_some_and(|id| !members.contains(&id)) {
            return Err(CoordinatorConfigError(
                "restored vote is outside membership",
            ));
        }
        if last_log.term > restored.term {
            return Err(CoordinatorConfigError(
                "log term exceeds restored election term",
            ));
        }
        config.validate()?;
        Ok(Self {
            node,
            members,
            config,
            hard: restored,
            last_log,
            role: Role::Follower,
            leader: None,
            votes: BTreeSet::new(),
            eligible: true,
            started: false,
            random_state: if random_seed == 0 {
                0x9e3779b97f4a7c15
            } else {
                random_seed
            },
            generation: 0,
            election_timer: None,
            heartbeat_timer: None,
            next_write: 0,
            pending: None,
            queued: VecDeque::new(),
        })
    }

    /// Idempotent startup: announces follower status and arms the election timer.
    pub fn start(&mut self) -> Vec<ElectionAction> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        let mut actions = vec![ElectionAction::StatusChanged(self.status())];
        self.reset_election_timer(&mut actions);
        actions
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    fn cancel(timer: &mut Option<TimerId>, actions: &mut Vec<ElectionAction>) {
        if let Some(timer) = timer.take() {
            actions.push(ElectionAction::CancelTimer(timer));
        }
    }

    fn reset_election_timer(&mut self, actions: &mut Vec<ElectionAction>) {
        Self::cancel(&mut self.election_timer, actions);
        if !self.eligible {
            return;
        }
        self.generation = self
            .generation
            .checked_add(1)
            .expect("timer generation exhausted");
        self.random_state ^= self.random_state << 13;
        self.random_state ^= self.random_state >> 7;
        self.random_state ^= self.random_state << 17;
        let min = self.config.election_timeout_min.as_nanos() as u64;
        let max = self.config.election_timeout_max.as_nanos() as u64;
        let after = Duration::from_nanos(min + self.random_state % (max - min));
        let timer = TimerId::Election(self.generation);
        self.election_timer = Some(timer);
        actions.push(ElectionAction::ArmTimer { timer, after });
    }

    fn arm_heartbeat(&mut self, actions: &mut Vec<ElectionAction>) {
        Self::cancel(&mut self.heartbeat_timer, actions);
        self.generation = self
            .generation
            .checked_add(1)
            .expect("timer generation exhausted");
        let timer = TimerId::Heartbeat(self.generation);
        self.heartbeat_timer = Some(timer);
        actions.push(ElectionAction::ArmTimer {
            timer,
            after: self.config.heartbeat_interval,
        });
    }

    fn follow(&mut self, term: Term, leader: Option<NodeId>, actions: &mut Vec<ElectionAction>) {
        let previous = self.status();
        let was_follower = self.role == Role::Follower;
        if term > self.hard.term {
            self.hard = HardState {
                term,
                voted_for: None,
            };
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        Self::cancel(&mut self.heartbeat_timer, actions);
        if !was_follower {
            self.reset_election_timer(actions);
        }
        if self.status() != previous {
            actions.push(ElectionAction::StatusChanged(self.status()));
        }
    }

    fn promote_if_elected(&mut self, actions: &mut Vec<ElectionAction>) {
        if self.role != Role::Candidate || self.votes.len() < self.quorum() {
            return;
        }
        self.role = Role::Leader;
        self.leader = Some(self.node);
        Self::cancel(&mut self.election_timer, actions);
        actions.push(ElectionAction::StatusChanged(self.status()));
        actions.push(ElectionAction::RequestHeartbeat {
            term: self.hard.term,
        });
        self.arm_heartbeat(actions);
    }

    fn campaign(&mut self, actions: &mut Vec<ElectionAction>) {
        self.hard = HardState {
            term: Term(self.hard.term.0.checked_add(1).expect("term exhausted")),
            voted_for: Some(self.node),
        };
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.node);
        self.reset_election_timer(actions);
        actions.push(ElectionAction::StatusChanged(self.status()));
        for &to in &self.members {
            if to != self.node {
                actions.push(ElectionAction::Send {
                    to,
                    message: ElectionMessage::RequestVote {
                        term: self.hard.term,
                        last_log: self.last_log,
                    },
                });
            }
        }
        // Self-vote can only establish leadership after it is durable.
    }

    fn process(&mut self, event: ElectionEvent, actions: &mut Vec<ElectionAction>) {
        match event {
            ElectionEvent::Message { from, message } => {
                if from == self.node || !self.members.contains(&from) {
                    return;
                }
                match message {
                    ElectionMessage::RequestVote { term, last_log } => {
                        if term > self.hard.term {
                            self.follow(term, None, actions);
                        }
                        let granted = self.eligible
                            && term == self.hard.term
                            && self.hard.voted_for.is_none_or(|id| id == from)
                            && last_log >= self.last_log;
                        if granted {
                            self.hard.voted_for = Some(from);
                            self.reset_election_timer(actions);
                        }
                        actions.push(ElectionAction::Send {
                            to: from,
                            message: ElectionMessage::VoteResponse {
                                term: self.hard.term,
                                granted,
                            },
                        });
                    }
                    ElectionMessage::VoteResponse { term, granted } => {
                        if term > self.hard.term {
                            self.follow(term, None, actions);
                        }
                        if self.role == Role::Candidate && term == self.hard.term && granted {
                            self.votes.insert(from);
                            self.promote_if_elected(actions);
                        }
                    }
                    // These belong to the worker-selection state machine.
                    ElectionMessage::RequestLoadBid { .. } | ElectionMessage::LoadBid(_) => {}
                }
            }
            ElectionEvent::CoordinatorContact { from, term } => {
                if from == self.node || !self.members.contains(&from) || term < self.hard.term {
                    return;
                }
                self.follow(term, Some(from), actions);
                self.reset_election_timer(actions);
            }
            ElectionEvent::TimerElapsed(timer) => {
                if Some(timer) == self.election_timer && self.eligible && self.role != Role::Leader
                {
                    self.election_timer = None;
                    self.campaign(actions);
                } else if Some(timer) == self.heartbeat_timer && self.role == Role::Leader {
                    self.heartbeat_timer = None;
                    actions.push(ElectionAction::RequestHeartbeat {
                        term: self.hard.term,
                    });
                    self.arm_heartbeat(actions);
                }
            }
            ElectionEvent::LocalLogUpdated(position) => self.last_log = position,
            ElectionEvent::EligibilityChanged { node, eligible } if node == self.node => {
                if self.eligible == eligible {
                    return;
                }
                self.eligible = eligible;
                if eligible {
                    self.reset_election_timer(actions);
                } else {
                    self.follow(self.hard.term, None, actions);
                    Self::cancel(&mut self.election_timer, actions);
                }
            }
            // Routing code should deliver selection events to its worker selector.
            _ => {}
        }
    }

    fn process_with_persistence(
        &mut self,
        event: ElectionEvent,
        actions: &mut Vec<ElectionAction>,
    ) {
        let before = self.hard;
        let mut produced = Vec::new();
        self.process(event, &mut produced);
        if self.hard == before {
            actions.extend(produced);
            return;
        }
        let mut after_persist = Vec::new();
        for action in produced {
            match action {
                ElectionAction::Send { .. } | ElectionAction::RequestHeartbeat { .. } => {
                    after_persist.push(action)
                }
                // Step down and cancel timers immediately, even during storage I/O.
                _ => actions.push(action),
            }
        }
        self.next_write = self.next_write.checked_add(1).expect("write ID exhausted");
        self.pending = Some(PendingWrite {
            id: self.next_write,
            after_persist,
        });
        actions.push(ElectionAction::PersistState {
            write_id: self.next_write,
            state: self.hard,
        });
    }
}

impl Election for Coordinator {
    fn handle(&mut self, event: ElectionEvent) -> Vec<ElectionAction> {
        let mut actions = self.start();
        if let ElectionEvent::StatePersisted { write_id } = event {
            if self
                .pending
                .as_ref()
                .is_none_or(|pending| pending.id != write_id)
            {
                return actions;
            }
            let pending = self.pending.take().expect("pending write checked above");
            actions.extend(pending.after_persist);
            self.promote_if_elected(&mut actions);
            while self.pending.is_none() {
                let Some(next) = self.queued.pop_front() else {
                    break;
                };
                self.process_with_persistence(next, &mut actions);
            }
        } else if self.pending.is_some() {
            self.queued.push_back(event);
        } else {
            self.process_with_persistence(event, &mut actions);
        }
        actions
    }

    fn status(&self) -> ElectionStatus {
        ElectionStatus {
            node: self.node,
            term: self.hard.term,
            role: self.role,
            coordinator: self.leader,
        }
    }
}
