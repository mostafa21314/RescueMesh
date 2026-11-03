//! Request table and results table. Every node keeps both, whether or not it
//! serves: the request table is what a new server takes over (Part B), and
//! the results table lets any node answer a retry.

use common::rng::fnv1a;
use common::*;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct RequestEntry {
    pub first_seen: Instant,
    pub last_attempt: u32,
    /// Node epoch when the highest attempt so far arrived.
    pub last_epoch: Epoch,
    pub completed: bool,
    pub incident: Incident,
    pub complexity: u32,
}

/// What a node stores and resends as a PlanResponse.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Response {
    pub served_by: NodeId,
    pub epoch: Epoch,
    pub degraded: bool,
    pub plan: Plan,
}

impl Response {
    pub fn to_message(&self, request_id: RequestId) -> Message {
        Message::PlanResponse {
            request_id,
            served_by: self.served_by,
            epoch: self.epoch,
            degraded: self.degraded,
            plan: self.plan.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Offer {
    /// First result for the request.
    Stored,
    /// Preferred over the result held, which is returned.
    Replaced(Response),
    /// The result held is preferred (or identical); the offer is ignored.
    Kept,
}

/// True if `new` should replace `old`: higher epoch, then lower served_by.
pub fn preferred(new: &Response, old: &Response) -> bool {
    (new.epoch, std::cmp::Reverse(new.served_by)) > (old.epoch, std::cmp::Reverse(old.served_by))
}

/// The entry as it was before `record` updated it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Previous {
    pub attempt: u32,
    pub epoch: Epoch,
    pub completed: bool,
}

#[derive(Default)]
pub struct Tables {
    requests: HashMap<RequestId, RequestEntry>,
    results: HashMap<RequestId, (Instant, Response)>,
}

impl Tables {
    /// Sorted, fixed snapshot; later live replicas are merged by the same result rule.
    pub fn recovery_records(&self) -> Vec<RecoveryRecord> {
        let mut records: BTreeMap<RequestId, RecoveryRecord> = self
            .requests
            .iter()
            .map(|(&id, e)| {
                (
                    id,
                    RecoveryRecord {
                        request_id: id,
                        attempt: e.last_attempt,
                        epoch: e.last_epoch,
                        incident: e.incident.clone(),
                        complexity: e.complexity,
                        result: None,
                    },
                )
            })
            .collect();
        for (&id, (_, r)) in &self.results {
            // Normally the request entry exists. Keep results even if the request
            // entry's independently measured TTL expired slightly earlier.
            if let Some(record) = records.get_mut(&id) {
                record.result = Some(RecoveryResult {
                    served_by: r.served_by,
                    epoch: r.epoch,
                    degraded: r.degraded,
                    plan: r.plan.clone(),
                });
            }
        }
        records.into_values().collect()
    }

    pub fn merge_recovery(&mut self, records: Vec<RecoveryRecord>, now: Instant) {
        for r in records {
            self.record(
                r.request_id,
                r.attempt,
                r.epoch,
                &r.incident,
                r.complexity,
                now,
            );
            if let Some(result) = r.result {
                self.offer(
                    r.request_id,
                    Response {
                        served_by: result.served_by,
                        epoch: result.epoch,
                        degraded: result.degraded,
                        plan: result.plan,
                    },
                    &r.incident,
                    now,
                );
            }
        }
    }
    /// Records an arriving attempt. Returns the entry's previous state, or
    /// `None` if the request is new to this node.
    pub fn record(
        &mut self,
        request_id: RequestId,
        attempt: u32,
        epoch: Epoch,
        incident: &Incident,
        complexity: u32,
        now: Instant,
    ) -> Option<Previous> {
        match self.requests.get_mut(&request_id) {
            Some(e) => {
                let prev = Previous {
                    attempt: e.last_attempt,
                    epoch: e.last_epoch,
                    completed: e.completed,
                };
                if attempt > e.last_attempt {
                    e.last_attempt = attempt;
                    e.last_epoch = epoch;
                }
                Some(prev)
            }
            None => {
                self.requests.insert(
                    request_id,
                    RequestEntry {
                        first_seen: now,
                        last_attempt: attempt,
                        last_epoch: epoch,
                        completed: self.results.contains_key(&request_id),
                        incident: incident.clone(),
                        complexity,
                    },
                );
                None
            }
        }
    }

    /// Offers a result for `request_id` and marks the request completed,
    /// creating its request entry if this node never saw the client's message.
    ///
    /// Two different results for one request (a late result from a server
    /// that stepped down, or two servers in one epoch) are resolved the same
    /// way on every node, whatever order they arrive in: the higher epoch
    /// wins, then the lower `served_by`.
    pub fn offer(
        &mut self,
        request_id: RequestId,
        response: Response,
        incident: &Incident,
        now: Instant,
    ) -> Offer {
        self.requests
            .entry(request_id)
            .or_insert_with(|| RequestEntry {
                first_seen: now,
                last_attempt: 0,
                last_epoch: response.epoch,
                completed: true,
                incident: incident.clone(),
                complexity: 0,
            })
            .completed = true;
        match self.results.get_mut(&request_id) {
            None => {
                self.results.insert(request_id, (now, response));
                Offer::Stored
            }
            Some((_, old)) if preferred(&response, old) => {
                Offer::Replaced(std::mem::replace(old, response))
            }
            Some(_) => Offer::Kept,
        }
    }

    /// Not-completed requests, oldest first: what a new server takes over.
    pub fn pending_oldest_first(&self) -> Vec<RequestId> {
        let mut v: Vec<(Instant, RequestId)> = self
            .requests
            .iter()
            .filter(|(_, e)| !e.completed)
            .map(|(r, e)| (e.first_seen, *r))
            .collect();
        v.sort();
        v.into_iter().map(|(_, r)| r).collect()
    }

    pub fn result(&self, request_id: &RequestId) -> Option<&Response> {
        self.results.get(request_id).map(|(_, r)| r)
    }

    pub fn request(&self, request_id: &RequestId) -> Option<&RequestEntry> {
        self.requests.get(request_id)
    }

    /// Drops entries older than `ttl`.
    pub fn sweep(&mut self, now: Instant, ttl: Duration) {
        self.results
            .retain(|_, (t, _)| now.duration_since(*t) < ttl);
        self.requests.retain(|id, e| {
            now.duration_since(e.first_seen) < ttl || self.results.contains_key(id)
        });
    }

    pub fn has_request(&self, request_id: &RequestId) -> bool {
        self.requests.contains_key(request_id)
    }

    pub fn len(&self) -> (usize, usize) {
        (self.requests.len(), self.results.len())
    }

    pub fn results_snapshot(&self) -> BTreeMap<RequestId, Response> {
        self.results
            .iter()
            .map(|(k, (_, r))| (*k, r.clone()))
            .collect()
    }

    /// Order-independent fingerprint: wrapping sum of per-entry hashes.
    pub fn results_digest(&self) -> u64 {
        self.results.iter().fold(0u64, |acc, (k, (_, r))| {
            let bytes = serde_json::to_vec(&(k, r)).expect("serialize result");
            acc.wrapping_add(fnv1a(&bytes))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn incident() -> Incident {
        Incident {
            incident_id: Uuid::nil(),
            reporter: "t".into(),
            category: Category::Fire,
            zone: 0,
            severity: 3,
            timestamp_ms: 0,
            status: IncidentStatus::Reported,
            ttl_ms: 1000,
        }
    }

    #[test]
    fn record_tracks_highest_attempt_and_its_epoch() {
        let mut t = Tables::default();
        let r = RequestId {
            client_id: Uuid::nil(),
            seq: 1,
        };
        let now = Instant::now();
        assert_eq!(t.record(r, 1, 0, &incident(), 1, now), None);
        assert_eq!(
            t.record(r, 2, 3, &incident(), 1, now),
            Some(Previous {
                attempt: 1,
                epoch: 0,
                completed: false
            })
        );
        // A late, lower attempt doesn't move the entry back.
        assert_eq!(
            t.record(r, 1, 4, &incident(), 1, now),
            Some(Previous {
                attempt: 2,
                epoch: 3,
                completed: false
            })
        );
        assert_eq!(t.request(&r).unwrap().last_epoch, 3);
    }

    fn response(served_by: NodeId, epoch: Epoch) -> Response {
        Response {
            served_by,
            epoch,
            degraded: false,
            plan: Plan {
                incident_id: Uuid::nil(),
                priority: served_by,
                ranked_responders: vec![],
                assignments: vec![],
                route: vec![],
                route_cost: 0,
                shortfall: vec![],
                planned_at_ms: 0,
            },
        }
    }

    #[test]
    fn conflicting_results_converge_in_any_order() {
        let r = RequestId {
            client_id: Uuid::nil(),
            seq: 1,
        };
        let now = Instant::now();
        let offers = [response(3, 1), response(1, 2), response(2, 2)];
        for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2], [2, 0, 1]] {
            let mut t = Tables::default();
            for i in order {
                t.offer(r, offers[i].clone(), &incident(), now);
            }
            assert_eq!(t.result(&r), Some(&response(1, 2)), "order {order:?}");
            assert!(t.request(&r).unwrap().completed);
        }
        let mut t = Tables::default();
        assert_eq!(t.offer(r, response(1, 1), &incident(), now), Offer::Stored);
        assert_eq!(t.offer(r, response(1, 1), &incident(), now), Offer::Kept);
        assert_eq!(
            t.offer(r, response(2, 2), &incident(), now),
            Offer::Replaced(response(1, 1))
        );
    }

    #[test]
    fn takeover_order_is_oldest_first_and_skips_completed() {
        let mut t = Tables::default();
        let t0 = Instant::now();
        let r = |seq| RequestId {
            client_id: Uuid::nil(),
            seq,
        };
        t.record(r(5), 1, 0, &incident(), 1, t0 + Duration::from_millis(2));
        t.record(r(9), 1, 0, &incident(), 1, t0);
        t.record(r(1), 1, 0, &incident(), 1, t0 + Duration::from_millis(1));
        t.record(r(2), 1, 0, &incident(), 1, t0 + Duration::from_millis(3));
        t.offer(r(1), response(1, 1), &incident(), t0);
        assert_eq!(t.pending_oldest_first(), vec![r(9), r(5), r(2)]);
    }

    #[test]
    fn sweep_expires_old_entries() {
        let mut t = Tables::default();
        let r = RequestId {
            client_id: Uuid::nil(),
            seq: 1,
        };
        let then = Instant::now();
        t.record(r, 1, 0, &incident(), 1, then);
        t.sweep(then + Duration::from_secs(1), Duration::from_secs(10));
        assert_eq!(t.len(), (1, 0));
        t.sweep(then + Duration::from_secs(11), Duration::from_secs(10));
        assert_eq!(t.len(), (0, 0));
    }
}
