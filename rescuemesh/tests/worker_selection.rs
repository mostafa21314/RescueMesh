use rescuemesh::election::{
    Election, ElectionAction, ElectionEvent, ElectionMessage, ElectionStatus, LoadBid, NodeId,
    RequestId, Role, SelectionId, Term, TimerId, WorkerSelectionConfig, WorkerSelector,
};
use std::time::Duration;

fn selection(request: u128, attempt: u64) -> SelectionId {
    SelectionId {
        request: RequestId(request),
        coordinator_term: Term(1),
        attempt,
    }
}

fn selector(node: u64, config: WorkerSelectionConfig) -> WorkerSelector {
    let mut selector =
        WorkerSelector::new(NodeId(node), [NodeId(1), NodeId(2), NodeId(3)], config).unwrap();
    selector.handle(ElectionEvent::CoordinatorStatusUpdated(ElectionStatus {
        node: NodeId(node),
        term: Term(1),
        role: if node == 1 {
            Role::Leader
        } else {
            Role::Follower
        },
        coordinator: Some(NodeId(1)),
    }));
    selector
}

fn leader() -> WorkerSelector {
    selector(1, WorkerSelectionConfig::default())
}

fn start(selector: &mut WorkerSelector, id: SelectionId) -> Vec<ElectionAction> {
    selector.handle(ElectionEvent::SelectWorker { selection: id })
}

fn bid(
    selector: &mut WorkerSelector,
    id: SelectionId,
    node: u64,
    score: u64,
) -> Vec<ElectionAction> {
    let bid = LoadBid {
        selection: id,
        node: NodeId(node),
        score,
    };
    selector.handle(if node == 1 {
        ElectionEvent::LocalLoadMeasured(bid)
    } else {
        ElectionEvent::Message {
            from: NodeId(node),
            message: ElectionMessage::LoadBid(bid),
        }
    })
}

fn deadline(selector: &mut WorkerSelector, id: SelectionId) -> Vec<ElectionAction> {
    selector.handle(ElectionEvent::TimerElapsed(TimerId::Bids(id)))
}

fn proposal(id: SelectionId, node: u64) -> ElectionAction {
    ElectionAction::WorkerProposed {
        selection: id,
        worker: NodeId(node),
    }
}

fn request(from: u64, id: SelectionId) -> ElectionEvent {
    ElectionEvent::Message {
        from: NodeId(from),
        message: ElectionMessage::RequestLoadBid { selection: id },
    }
}

#[test]
fn all_bids_select_lowest_score_before_deadline_and_finish_once() {
    let mut a = leader();
    let id = selection(10, 0);
    let actions = start(&mut a, id);
    assert!(actions.contains(&ElectionAction::MeasureLocalLoad { selection: id }));
    assert_eq!(
        actions
            .iter()
            .filter(|a| matches!(a, ElectionAction::Send { .. }))
            .count(),
        2
    );
    assert!(bid(&mut a, id, 1, 30).is_empty());
    assert!(bid(&mut a, id, 2, 20).is_empty());
    let actions = bid(&mut a, id, 3, 10);
    assert!(actions.contains(&proposal(id, 3)));
    assert!(actions.contains(&ElectionAction::CancelTimer(TimerId::Bids(id))));
    assert!(deadline(&mut a, id).is_empty());
    assert!(start(&mut a, id).is_empty());
    assert!(bid(&mut a, id, 2, 0).is_empty());
}

#[test]
fn partial_bids_wait_for_deadline_and_equal_scores_use_node_id() {
    let mut a = leader();
    let id = selection(10, 0);
    start(&mut a, id);
    bid(&mut a, id, 3, 5);
    assert!(bid(&mut a, id, 2, 5).is_empty());
    assert!(deadline(&mut a, id).contains(&proposal(id, 2)));
}

#[test]
fn one_bid_cannot_produce_a_worker_even_when_others_are_ineligible() {
    let mut a = leader();
    let id = selection(10, 0);
    start(&mut a, id);
    bid(&mut a, id, 1, 1);
    assert!(deadline(&mut a, id).contains(&ElectionAction::SelectionUnavailable { selection: id }));
    for node in [2, 3] {
        a.handle(ElectionEvent::EligibilityChanged {
            node: NodeId(node),
            eligible: false,
        });
    }
    let next = selection(11, 0);
    let actions = start(&mut a, next);
    assert_eq!(
        actions,
        vec![ElectionAction::SelectionUnavailable { selection: next }]
    );
}

#[test]
fn spoofed_unknown_and_duplicate_bids_do_not_change_the_first_sample() {
    let mut a = leader();
    let id = selection(10, 0);
    start(&mut a, id);
    for (from, node) in [(2, 3), (99, 99), (1, 1)] {
        assert!(
            a.handle(ElectionEvent::Message {
                from: NodeId(from),
                message: ElectionMessage::LoadBid(LoadBid {
                    selection: id,
                    node: NodeId(node),
                    score: 0
                }),
            })
            .is_empty()
        );
    }
    bid(&mut a, id, 2, 20);
    bid(&mut a, id, 2, 0);
    bid(&mut a, id, 1, 10);
    assert!(deadline(&mut a, id).contains(&proposal(id, 1)));
}

#[test]
fn newer_attempt_cancels_previous_round_and_rejects_old_callbacks() {
    let mut a = leader();
    let old = selection(10, 0);
    let new = selection(10, 1);
    start(&mut a, old);
    let actions = start(&mut a, new);
    assert!(actions.contains(&ElectionAction::SelectionUnavailable { selection: old }));
    assert!(actions.contains(&ElectionAction::CancelTimer(TimerId::Bids(old))));
    assert!(start(&mut a, old).is_empty());
    assert!(bid(&mut a, old, 2, 0).is_empty());
    assert!(bid(&mut a, old, 1, 0).is_empty());
    assert!(deadline(&mut a, old).is_empty());
    bid(&mut a, new, 1, 5);
    bid(&mut a, new, 2, 10);
    assert!(deadline(&mut a, new).contains(&proposal(new, 1)));
}

#[test]
fn concurrent_requests_keep_their_bids_separate() {
    let mut a = leader();
    let x = selection(10, 0);
    let y = selection(11, 0);
    start(&mut a, x);
    start(&mut a, y);
    bid(&mut a, x, 1, 20);
    bid(&mut a, y, 1, 1);
    bid(&mut a, x, 2, 5);
    bid(&mut a, y, 2, 10);
    assert!(deadline(&mut a, x).contains(&proposal(x, 2)));
    assert!(deadline(&mut a, y).contains(&proposal(y, 1)));
}

#[test]
fn follower_only_answers_known_coordinator_and_replays_its_fixed_score() {
    let mut b = selector(2, WorkerSelectionConfig::default());
    let id = selection(10, 0);
    assert!(start(&mut b, id).is_empty());
    assert!(b.handle(request(3, id)).is_empty());
    let actions = b.handle(request(1, id));
    assert!(actions.contains(&ElectionAction::MeasureLocalLoad { selection: id }));
    assert!(b.handle(request(1, id)).is_empty());
    let local = LoadBid {
        selection: id,
        node: NodeId(2),
        score: 10,
    };
    let expected = vec![ElectionAction::Send {
        to: NodeId(1),
        message: ElectionMessage::LoadBid(local),
    }];
    assert_eq!(b.handle(ElectionEvent::LocalLoadMeasured(local)), expected);
    assert!(
        b.handle(ElectionEvent::LocalLoadMeasured(LoadBid {
            score: 0,
            ..local
        }))
        .is_empty()
    );
    assert_eq!(b.handle(request(1, id)), expected);
    deadline(&mut b, id);
    assert!(b.handle(request(1, id)).is_empty());
    assert!(b.handle(ElectionEvent::LocalLoadMeasured(local)).is_empty());
}

#[test]
fn coordinator_change_cancels_rounds_and_old_term_inputs_are_ignored() {
    let mut a = leader();
    let old = selection(10, 0);
    start(&mut a, old);
    let actions = a.handle(ElectionEvent::CoordinatorStatusUpdated(ElectionStatus {
        node: NodeId(1),
        term: Term(2),
        role: Role::Follower,
        coordinator: Some(NodeId(2)),
    }));
    assert!(actions.contains(&ElectionAction::SelectionUnavailable { selection: old }));
    assert!(deadline(&mut a, old).is_empty());
    assert!(bid(&mut a, old, 2, 0).is_empty());
    assert!(a.handle(request(2, old)).is_empty());
    let current = SelectionId {
        coordinator_term: Term(2),
        ..old
    };
    assert!(
        a.handle(request(2, current))
            .contains(&ElectionAction::MeasureLocalLoad { selection: current })
    );
    a.handle(ElectionEvent::CoordinatorStatusUpdated(ElectionStatus {
        node: NodeId(1),
        term: Term(1),
        role: Role::Leader,
        coordinator: Some(NodeId(1)),
    }));
    assert_eq!(a.status().term, Term(2));
}

#[test]
fn same_term_stepdown_cancels_selection_and_blocks_late_results() {
    let mut a = leader();
    let id = selection(10, 0);
    start(&mut a, id);
    let actions = a.handle(ElectionEvent::CoordinatorStatusUpdated(ElectionStatus {
        node: NodeId(1),
        term: Term(1),
        role: Role::Follower,
        coordinator: Some(NodeId(2)),
    }));
    assert!(actions.contains(&ElectionAction::SelectionUnavailable { selection: id }));
    assert!(bid(&mut a, id, 2, 0).is_empty());
    assert!(deadline(&mut a, id).is_empty());
}

#[test]
fn ineligible_worker_is_removed_and_recovery_only_affects_future_rounds() {
    let mut a = leader();
    let id = selection(10, 0);
    start(&mut a, id);
    bid(&mut a, id, 2, 0);
    a.handle(ElectionEvent::EligibilityChanged {
        node: NodeId(2),
        eligible: false,
    });
    a.handle(ElectionEvent::EligibilityChanged {
        node: NodeId(2),
        eligible: true,
    });
    assert!(bid(&mut a, id, 2, 0).is_empty());
    bid(&mut a, id, 1, 10);
    assert!(bid(&mut a, id, 3, 20).contains(&proposal(id, 1)));
    let next = selection(11, 0);
    let actions = start(&mut a, next);
    assert!(actions.contains(&ElectionAction::Send {
        to: NodeId(2),
        message: ElectionMessage::RequestLoadBid { selection: next },
    }));
}

#[test]
fn local_recovery_cancels_round_and_rejects_new_work() {
    let mut a = leader();
    let id = selection(10, 0);
    start(&mut a, id);
    let actions = a.handle(ElectionEvent::EligibilityChanged {
        node: NodeId(1),
        eligible: false,
    });
    assert!(actions.contains(&ElectionAction::SelectionUnavailable { selection: id }));
    assert!(bid(&mut a, id, 1, 0).is_empty());
    let next = selection(11, 0);
    assert!(
        start(&mut a, next).contains(&ElectionAction::SelectionUnavailable { selection: next })
    );
}

#[test]
fn request_capacity_preserves_history_but_allows_higher_attempts() {
    let config = WorkerSelectionConfig {
        max_tracked_requests: 1,
        ..WorkerSelectionConfig::default()
    };
    let mut a = selector(1, config);
    let first = selection(10, 0);
    start(&mut a, first);
    deadline(&mut a, first);
    let second = selection(11, 0);
    assert_eq!(
        start(&mut a, second),
        vec![ElectionAction::SelectionUnavailable { selection: second }]
    );
    let retry = selection(10, 1);
    assert!(start(&mut a, retry).contains(&ElectionAction::MeasureLocalLoad { selection: retry }));
    assert!(start(&mut a, first).is_empty());
}

#[test]
fn configuration_rejects_invalid_membership_timeouts_and_capacity() {
    assert!(WorkerSelector::new(NodeId(1), [NodeId(2)], WorkerSelectionConfig::default()).is_err());
    assert!(
        WorkerSelector::new(
            NodeId(1),
            [NodeId(1), NodeId(1)],
            WorkerSelectionConfig::default()
        )
        .is_err()
    );
    for config in [
        WorkerSelectionConfig {
            bid_timeout: Duration::ZERO,
            ..WorkerSelectionConfig::default()
        },
        WorkerSelectionConfig {
            max_tracked_requests: 0,
            ..WorkerSelectionConfig::default()
        },
    ] {
        assert!(WorkerSelector::new(NodeId(1), [NodeId(1)], config).is_err());
    }
}

#[test]
fn messages_and_local_load_callbacks_work_end_to_end_between_selectors() {
    let mut a = leader();
    let mut b = selector(2, WorkerSelectionConfig::default());
    let mut c = selector(3, WorkerSelectionConfig::default());
    let id = selection(10, 0);
    let requests = start(&mut a, id);
    bid(&mut a, id, 1, 100);
    let mut final_actions = Vec::new();
    for action in requests {
        if let ElectionAction::Send { to, message } = action {
            let (peer, score) = if to == NodeId(2) {
                (&mut b, 10)
            } else {
                (&mut c, 20)
            };
            let actions = peer.handle(ElectionEvent::Message {
                from: NodeId(1),
                message,
            });
            assert!(actions.contains(&ElectionAction::MeasureLocalLoad { selection: id }));
            for response in peer.handle(ElectionEvent::LocalLoadMeasured(LoadBid {
                selection: id,
                node: to,
                score,
            })) {
                if let ElectionAction::Send {
                    to: NodeId(1),
                    message,
                } = response
                {
                    final_actions.extend(a.handle(ElectionEvent::Message { from: to, message }));
                }
            }
        }
    }
    assert!(final_actions.contains(&proposal(id, 2)));
}
