use std::time::Duration;

use rescuemesh::election::{
    Coordinator, CoordinatorConfig, Election, ElectionAction, ElectionEvent, ElectionMessage,
    HardState, LogPosition, NodeId, Role, Term, TimerId,
};

fn log(term: u64, index: u64) -> LogPosition {
    LogPosition {
        term: Term(term),
        index,
    }
}

fn node(id: u64, members: &[u64], restored: HardState, last_log: LogPosition) -> Coordinator {
    Coordinator::new(
        NodeId(id),
        members.iter().copied().map(NodeId),
        restored,
        last_log,
        CoordinatorConfig::default(),
        id * 1234567,
    )
    .unwrap()
}

fn fresh(id: u64) -> Coordinator {
    node(
        id,
        &[1, 2, 3],
        HardState {
            term: Term(0),
            voted_for: None,
        },
        log(0, 0),
    )
}

fn election_timer(actions: &[ElectionAction]) -> TimerId {
    actions
        .iter()
        .find_map(|action| match action {
            ElectionAction::ArmTimer {
                timer: timer @ TimerId::Election(_),
                ..
            } => Some(*timer),
            _ => None,
        })
        .expect("election timer")
}

fn persist(node: &mut Coordinator, actions: &[ElectionAction]) -> Vec<ElectionAction> {
    let write_id = actions
        .iter()
        .find_map(|action| match action {
            ElectionAction::PersistState { write_id, .. } => Some(*write_id),
            _ => None,
        })
        .expect("pending write");
    node.handle(ElectionEvent::StatePersisted { write_id })
}

fn vote_request(from: u64, term: u64, last_log: LogPosition) -> ElectionEvent {
    ElectionEvent::Message {
        from: NodeId(from),
        message: ElectionMessage::RequestVote {
            term: Term(term),
            last_log,
        },
    }
}

fn vote(from: u64, term: u64) -> ElectionEvent {
    ElectionEvent::Message {
        from: NodeId(from),
        message: ElectionMessage::VoteResponse {
            term: Term(term),
            granted: true,
        },
    }
}

fn has_vote(actions: &[ElectionAction], granted: bool) -> bool {
    actions.iter().any(|action| {
        matches!(action,
        ElectionAction::Send { message: ElectionMessage::VoteResponse { granted: value, .. }, .. }
        if *value == granted)
    })
}

fn elect(node: &mut Coordinator) -> Vec<ElectionAction> {
    let timer = election_timer(&node.start());
    let actions = node.handle(ElectionEvent::TimerElapsed(timer));
    persist(node, &actions);
    node.handle(vote(2, 1))
}

#[test]
fn election_waits_for_durable_self_vote_then_a_majority() {
    let mut a = fresh(1);
    let timer = election_timer(&a.start());
    assert!(a.start().is_empty());
    let actions = a.handle(ElectionEvent::TimerElapsed(timer));
    assert_eq!(a.status().role, Role::Candidate);
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, ElectionAction::Send { .. }))
    );
    assert!(
        a.handle(ElectionEvent::StatePersisted { write_id: 999 })
            .is_empty()
    );
    let messages = persist(&mut a, &actions);
    assert_eq!(
        messages
            .iter()
            .filter(|a| matches!(a, ElectionAction::Send { .. }))
            .count(),
        2
    );
    assert_eq!(a.status().role, Role::Candidate);
    let actions = a.handle(vote(2, 1));
    assert_eq!(a.status().role, Role::Leader);
    assert_eq!(a.status().coordinator, Some(NodeId(1)));
    assert!(actions.contains(&ElectionAction::RequestHeartbeat { term: Term(1) }));
}

#[test]
fn grant_is_durable_and_a_second_candidate_is_denied_even_after_restart() {
    let mut b = fresh(2);
    let actions = b.handle(vote_request(1, 1, log(0, 0)));
    assert!(!has_vote(&actions, true));
    let restored = actions
        .iter()
        .find_map(|a| match a {
            ElectionAction::PersistState { state, .. } => Some(*state),
            _ => None,
        })
        .unwrap();
    assert!(has_vote(&persist(&mut b, &actions), true));
    let mut restarted = node(2, &[1, 2, 3], restored, log(0, 0));
    assert!(has_vote(
        &restarted.handle(vote_request(3, 1, log(0, 0))),
        false
    ));
    assert!(has_vote(
        &restarted.handle(vote_request(1, 1, log(0, 0))),
        true
    ));
}

#[test]
fn concurrent_vote_requests_are_serialized_behind_storage() {
    let mut b = fresh(2);
    let pending = b.handle(vote_request(1, 1, log(0, 0)));
    assert!(b.handle(vote_request(3, 1, log(0, 0))).is_empty());
    let actions = persist(&mut b, &pending);
    assert!(has_vote(&actions, true));
    assert!(has_vote(&actions, false));
}

#[test]
fn log_freshness_compares_term_before_index() {
    let restored = HardState {
        term: Term(3),
        voted_for: None,
    };
    let mut b = node(2, &[1, 2, 3], restored, log(2, 10));
    assert!(has_vote(&b.handle(vote_request(1, 3, log(1, 1000))), false));
    assert!(has_vote(&b.handle(vote_request(1, 3, log(2, 9))), false));
    let pending = b.handle(vote_request(1, 3, log(3, 1)));
    assert!(has_vote(&persist(&mut b, &pending), true));
}

#[test]
fn duplicate_unknown_and_old_votes_do_not_form_a_majority() {
    let mut a = node(
        1,
        &[1, 2, 3, 4, 5],
        HardState {
            term: Term(0),
            voted_for: None,
        },
        log(0, 0),
    );
    let timer = election_timer(&a.start());
    let pending = a.handle(ElectionEvent::TimerElapsed(timer));
    persist(&mut a, &pending);
    for event in [vote(2, 1), vote(2, 1), vote(99, 1), vote(3, 0), vote(1, 1)] {
        a.handle(event);
    }
    assert_eq!(a.status().role, Role::Candidate);
    a.handle(vote(3, 1));
    assert_eq!(a.status().role, Role::Leader);
}

#[test]
fn higher_term_steps_down_and_cancels_heartbeats_before_storage_completes() {
    let mut a = fresh(1);
    let leader_actions = elect(&mut a);
    let heartbeat = leader_actions
        .iter()
        .find_map(|a| match a {
            ElectionAction::ArmTimer {
                timer: timer @ TimerId::Heartbeat(_),
                ..
            } => Some(*timer),
            _ => None,
        })
        .unwrap();
    let actions = a.handle(vote_request(3, 2, log(0, 0)));
    assert_eq!(a.status().role, Role::Follower);
    assert!(actions.contains(&ElectionAction::CancelTimer(heartbeat)));
    assert!(!has_vote(&actions, true));
    let queued = a.handle(ElectionEvent::TimerElapsed(heartbeat));
    assert!(queued.is_empty());
    let completed = persist(&mut a, &actions);
    assert!(has_vote(&completed, true));
    assert!(
        !completed
            .iter()
            .any(|a| matches!(a, ElectionAction::RequestHeartbeat { .. }))
    );
}

#[test]
fn validated_contact_resets_timeout_and_ignores_old_timer_and_old_terms() {
    let mut b = fresh(2);
    let old_timer = election_timer(&b.start());
    let actions = b.handle(ElectionEvent::CoordinatorContact {
        from: NodeId(1),
        term: Term(1),
    });
    let current_timer = election_timer(&actions);
    persist(&mut b, &actions);
    assert!(b.handle(ElectionEvent::TimerElapsed(old_timer)).is_empty());
    assert!(
        b.handle(ElectionEvent::CoordinatorContact {
            from: NodeId(3),
            term: Term(0)
        })
        .is_empty()
    );
    assert_eq!(b.status().coordinator, Some(NodeId(1)));
    b.handle(ElectionEvent::TimerElapsed(current_timer));
    assert_eq!(b.status().term, Term(2));
    assert_eq!(b.status().role, Role::Candidate);
}

#[test]
fn timeout_retries_a_split_vote_in_a_new_term() {
    let mut a = fresh(1);
    let timer = election_timer(&a.start());
    let first = a.handle(ElectionEvent::TimerElapsed(timer));
    let retry = election_timer(&first);
    persist(&mut a, &first);
    let second = a.handle(ElectionEvent::TimerElapsed(retry));
    assert_eq!(a.status().term, Term(2));
    assert!(
        !second
            .iter()
            .any(|a| matches!(a, ElectionAction::Send { .. }))
    );
    persist(&mut a, &second);
    a.handle(vote(2, 1));
    assert_eq!(a.status().role, Role::Candidate);
    a.handle(vote(2, 2));
    assert_eq!(a.status().role, Role::Leader);
}

#[test]
fn recovery_disables_local_participation_without_shrinking_membership() {
    let mut a = fresh(1);
    let timer = election_timer(&a.start());
    a.handle(ElectionEvent::EligibilityChanged {
        node: NodeId(1),
        eligible: false,
    });
    assert!(a.handle(ElectionEvent::TimerElapsed(timer)).is_empty());
    let pending = a.handle(vote_request(2, 1, log(0, 0)));
    assert!(has_vote(&persist(&mut a, &pending), false));
    let ready = a.handle(ElectionEvent::EligibilityChanged {
        node: NodeId(1),
        eligible: true,
    });
    let pending = a.handle(ElectionEvent::TimerElapsed(election_timer(&ready)));
    persist(&mut a, &pending);
    a.handle(ElectionEvent::EligibilityChanged {
        node: NodeId(2),
        eligible: false,
    });
    a.handle(ElectionEvent::EligibilityChanged {
        node: NodeId(3),
        eligible: false,
    });
    assert_eq!(a.status().role, Role::Candidate);
}

#[test]
fn singleton_waits_for_persistence_before_becoming_leader() {
    let mut a = node(
        1,
        &[1],
        HardState {
            term: Term(0),
            voted_for: None,
        },
        log(0, 0),
    );
    let timer = election_timer(&a.start());
    let pending = a.handle(ElectionEvent::TimerElapsed(timer));
    assert_eq!(a.status().role, Role::Candidate);
    persist(&mut a, &pending);
    assert_eq!(a.status().role, Role::Leader);
}

#[test]
fn independent_seeds_produce_timeouts_inside_the_configured_range() {
    let mut delays = Vec::new();
    for id in 1..=3 {
        let mut a = fresh(id);
        for action in a.start() {
            if let ElectionAction::ArmTimer { after, .. } = action {
                assert!(after >= Duration::from_millis(500));
                assert!(after < Duration::from_millis(1000));
                delays.push(after);
            }
        }
    }
    delays.sort();
    delays.dedup();
    assert!(delays.len() > 1);
}

#[test]
fn invalid_configuration_is_rejected() {
    let restored = HardState {
        term: Term(0),
        voted_for: None,
    };
    for members in [vec![NodeId(2)], vec![NodeId(1), NodeId(1)]] {
        assert!(
            Coordinator::new(
                NodeId(1),
                members,
                restored,
                log(0, 0),
                CoordinatorConfig::default(),
                1
            )
            .is_err()
        );
    }
    let config = CoordinatorConfig {
        heartbeat_interval: Duration::ZERO,
        ..CoordinatorConfig::default()
    };
    assert!(Coordinator::new(NodeId(1), [NodeId(1)], restored, log(0, 0), config, 1).is_err());
}

#[test]
fn surviving_peers_elect_a_new_coordinator_and_returning_leader_steps_down() {
    let mut a = fresh(1);
    let mut b = fresh(2);
    let mut c = fresh(3);
    b.start();
    c.start();
    elect(&mut a);
    let pending = b.handle(ElectionEvent::CoordinatorContact {
        from: NodeId(1),
        term: Term(1),
    });
    let b_timer = election_timer(&pending);
    persist(&mut b, &pending);
    let pending = c.handle(ElectionEvent::CoordinatorContact {
        from: NodeId(1),
        term: Term(1),
    });
    persist(&mut c, &pending);
    // A is now unreachable; B campaigns and C provides its durable vote.
    let pending = b.handle(ElectionEvent::TimerElapsed(b_timer));
    let requests = persist(&mut b, &pending);
    let request = requests
        .into_iter()
        .find_map(|action| match action {
            ElectionAction::Send {
                to: NodeId(3),
                message,
            } => Some(message),
            _ => None,
        })
        .unwrap();
    let pending = c.handle(ElectionEvent::Message {
        from: NodeId(2),
        message: request,
    });
    let responses = persist(&mut c, &pending);
    for action in responses {
        if let ElectionAction::Send {
            to: NodeId(2),
            message,
        } = action
        {
            b.handle(ElectionEvent::Message {
                from: NodeId(3),
                message,
            });
        }
    }
    assert_eq!(b.status().role, Role::Leader);
    assert_eq!(b.status().term, Term(2));
    let pending = a.handle(ElectionEvent::CoordinatorContact {
        from: NodeId(2),
        term: Term(2),
    });
    assert_eq!(a.status().role, Role::Follower);
    persist(&mut a, &pending);
    assert_eq!(a.status().coordinator, Some(NodeId(2)));
}
