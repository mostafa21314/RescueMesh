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
                let prev = Previous { attempt: e.last_attempt, epoch: e.last_epoch, completed: e.completed };
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

    /// Stores a result and marks the request completed, creating its request
    /// entry if this node never saw the client's message. Returns false if a
    /// result was already held (the new one is ignored).
    pub fn complete(&mut self, request_id: RequestId, response: Response, incident: &Incident, now: Instant) -> bool {
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
        if self.results.contains_key(&request_id) {
            return false;
        }
        self.results.insert(request_id, (now, response));
        true
    }

    pub fn result(&self, request_id: &RequestId) -> Option<&Response> {
        self.results.get(request_id).map(|(_, r)| r)
    }

    pub fn request(&self, request_id: &RequestId) -> Option<&RequestEntry> {
        self.requests.get(request_id)
    }

    /// Drops entries older than `ttl`.
    pub fn sweep(&mut self, now: Instant, ttl: Duration) {
        self.requests.retain(|_, e| now.duration_since(e.first_seen) < ttl);
        self.results.retain(|_, (t, _)| now.duration_since(*t) < ttl);
    }

    pub fn len(&self) -> (usize, usize) {
        (self.requests.len(), self.results.len())
    }

    pub fn results_snapshot(&self) -> BTreeMap<RequestId, Response> {
        self.results.iter().map(|(k, (_, r))| (*k, r.clone())).collect()
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
        let r = RequestId { client_id: Uuid::nil(), seq: 1 };
        let now = Instant::now();
        assert_eq!(t.record(r, 1, 0, &incident(), 1, now), None);
        assert_eq!(t.record(r, 2, 3, &incident(), 1, now), Some(Previous { attempt: 1, epoch: 0, completed: false }));
        // A late, lower attempt doesn't move the entry back.
        assert_eq!(t.record(r, 1, 4, &incident(), 1, now), Some(Previous { attempt: 2, epoch: 3, completed: false }));
        assert_eq!(t.request(&r).unwrap().last_epoch, 3);
    }

    #[test]
    fn sweep_expires_old_entries() {
        let mut t = Tables::default();
        let r = RequestId { client_id: Uuid::nil(), seq: 1 };
        let then = Instant::now();
        t.record(r, 1, 0, &incident(), 1, then);
        t.sweep(then + Duration::from_secs(1), Duration::from_secs(10));
        assert_eq!(t.len(), (1, 0));
        t.sweep(then + Duration::from_secs(11), Duration::from_secs(10));
        assert_eq!(t.len(), (0, 0));
    }
}
