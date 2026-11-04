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
    /// Last time this entry or its result changed: what a delta recovery
    /// transfer selects on.
    pub changed: Instant,
    pub last_interest: Instant,
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
    pub retained_until_ms: u64,
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

impl From<RecoveryResult> for Response {
    fn from(result: RecoveryResult) -> Self {
        Self {
            served_by: result.served_by,
            epoch: result.epoch,
            degraded: result.degraded,
            plan: result.plan,
            retained_until_ms: result.retained_until_ms,
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
    /// `results_digest`, kept up to date on every change so a status query
    /// doesn't re-serialize the whole table under the state lock.
    digest: u64,
    revision: u64,
    changes: BTreeMap<u64, RequestId>,
    versions: HashMap<RequestId, u64>,
    hashes: HashMap<RequestId, u64>,
}

fn entry_hash(k: &RequestId, r: &Response) -> u64 {
    fnv1a(&serde_json::to_vec(&(k, r)).expect("serialize result"))
}

/// What `merge_recovery` changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Merged {
    /// Results this node did not hold.
    pub stored: u64,
    /// Results that replaced a held one under the result rule.
    pub replaced: u64,
}

impl Tables {
    /// Keys a recovery snapshot covers, sorted: every request entry, or with
    /// `since`, only those changed at or after it.
    pub fn recovery_keys(&self, since: Option<Instant>) -> Vec<RequestId> {
        let mut keys: Vec<RequestId> = self
            .requests
            .iter()
            .filter(|(_, e)| since.is_none_or(|t| e.changed >= t))
            .map(|(id, _)| *id)
            .collect();
        keys.sort();
        keys
    }

    /// One request entry with its result, as sent in a recovery snapshot.
    pub fn recovery_record(&self, id: &RequestId) -> Option<RecoveryRecord> {
        let e = self.requests.get(id)?;
        Some(RecoveryRecord {
            request_id: *id,
            attempt: e.last_attempt,
            epoch: e.last_epoch,
            incident: e.incident.clone(),
            complexity: e.complexity,
            result: self.results.get(id).map(|(_, r)| RecoveryResult {
                served_by: r.served_by,
                epoch: r.epoch,
                degraded: r.degraded,
                plan: r.plan.clone(),
                retained_until_ms: r.retained_until_ms,
            }),
        })
    }

    /// Sorted, fixed snapshot; later live replicas are merged by the same result rule.
    pub fn recovery_records(&self, since: Option<Instant>) -> Vec<RecoveryRecord> {
        self.recovery_keys(since)
            .iter()
            .filter_map(|id| self.recovery_record(id))
            .collect()
    }

    pub fn merge_recovery(&mut self, records: Vec<RecoveryRecord>, now: Instant) -> Merged {
        let mut merged = Merged::default();
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
                let response = Response {
                    served_by: result.served_by,
                    epoch: result.epoch,
                    degraded: result.degraded,
                    plan: result.plan,
                    retained_until_ms: result.retained_until_ms,
                };
                match self.offer(r.request_id, response, &r.incident, now) {
                    Offer::Stored => merged.stored += 1,
                    Offer::Replaced(_) => merged.replaced += 1,
                    Offer::Kept => {}
                }
            }
        }
        merged
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
                e.last_interest = now;
                let prev = Previous {
                    attempt: e.last_attempt,
                    epoch: e.last_epoch,
                    completed: e.completed,
                };
                if attempt > e.last_attempt {
                    e.last_attempt = attempt;
                    e.last_epoch = epoch;
                    e.changed = now;
                }
                Some(prev)
            }
            None => {
                self.requests.insert(
                    request_id,
                    RequestEntry {
                        first_seen: now,
                        changed: now,
                        last_interest: now,
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
        let entry = self
            .requests
            .entry(request_id)
            .or_insert_with(|| RequestEntry {
                first_seen: now,
                changed: now,
                last_interest: now,
                last_attempt: 0,
                last_epoch: response.epoch,
                completed: true,
                incident: incident.clone(),
                complexity: 0,
            });
        entry.completed = true;
        match self.results.get_mut(&request_id) {
            None => {
                entry.changed = now;
                let hash = entry_hash(&request_id, &response);
                self.digest = self.digest.wrapping_add(hash);
                self.hashes.insert(request_id, hash);
                self.results.insert(request_id, (now, response));
                self.changed(request_id);
                Offer::Stored
            }
            Some((_, old)) if preferred(&response, old) => {
                entry.changed = now;
                let hash = entry_hash(&request_id, &response);
                self.digest = self
                    .digest
                    .wrapping_sub(self.hashes[&request_id])
                    .wrapping_add(hash);
                self.hashes.insert(request_id, hash);
                let old = std::mem::replace(old, response);
                self.changed(request_id);
                Offer::Replaced(old)
            }
            Some(_) => Offer::Kept,
        }
    }

    fn changed(&mut self, request_id: RequestId) {
        self.revision += 1;
        if let Some(previous) = self.versions.insert(request_id, self.revision) {
            self.changes.remove(&previous);
        }
        self.changes.insert(self.revision, request_id);
    }

    /// A bounded page of the latest retained version of changed results.
    /// Expired/overwritten revisions are holes, not missing retained state.
    pub fn sync_page(&self, after: u64) -> (u64, u64, Vec<RecoveryRecord>) {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut records = Vec::new();
        let mut bytes = 0;
        let mut next = after;
        for (&revision, id) in self.changes.range((Excluded(after), Unbounded)) {
            let Some(record) = self.recovery_record(id) else {
                continue;
            };
            let size = serde_json::to_vec(&record).expect("serialize record").len();
            if !records.is_empty() && (records.len() >= 64 || bytes + size > 256 * 1024) {
                return (next, self.revision, records);
            }
            bytes += size;
            next = revision;
            records.push(record);
        }
        (self.revision, self.revision, records)
    }

    pub fn scoped_digest(
        &self,
        clients: &[uuid::Uuid],
        min_retained_until_ms: u64,
    ) -> (usize, u64) {
        let mut count = 0;
        let mut digest = 0u64;
        for (id, (_, response)) in &self.results {
            if (!clients.is_empty() && !clients.contains(&id.client_id))
                || (response.retained_until_ms > 0
                    && response.retained_until_ms <= min_retained_until_ms)
            {
                continue;
            }
            count += 1;
            digest = digest.wrapping_add(self.hashes[id]);
        }
        (count, digest)
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

    pub fn iter_results(&self) -> impl Iterator<Item = (RequestId, &Response)> {
        self.results
            .iter()
            .map(|(id, (_, response))| (*id, response))
    }

    pub fn request(&self, request_id: &RequestId) -> Option<&RequestEntry> {
        self.requests.get(request_id)
    }

    /// Drops entries older than `ttl`.
    pub fn sweep(&mut self, now: Instant, ttl: Duration) {
        let mut digest = self.digest;
        let wall = unix_ms();
        let versions = &mut self.versions;
        let changes = &mut self.changes;
        let hashes = &mut self.hashes;
        self.results.retain(|k, (t, r)| {
            let keep = if r.retained_until_ms == 0 {
                now.duration_since(*t) < ttl
            } else {
                wall < r.retained_until_ms
            };
            if !keep {
                digest = digest.wrapping_sub(hashes.remove(k).expect("result hash"));
                if let Some(version) = versions.remove(k) {
                    changes.remove(&version);
                }
            }
            keep
        });
        self.digest = digest;
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
        self.digest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    fn r(seq: u64) -> RequestId {
        RequestId {
            client_id: Uuid::nil(),
            seq,
        }
    }

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
            retained_until_ms: 0,
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
    fn repair_journal_keeps_latest_versions_and_advances_across_expiration() {
        let now = Instant::now();
        let mut tables = Tables::default();
        let mut first = response(1, 1);
        first.retained_until_ms = unix_ms().saturating_sub(1);
        tables.offer(r(1), first, &incident(), now);
        tables.offer(r(2), response(1, 1), &incident(), now);
        tables.offer(r(2), response(1, 2), &incident(), now);
        let (next, latest, records) = tables.sync_page(0);
        assert_eq!((next, latest, records.len()), (3, 3, 2));
        assert_eq!(records[1].result.as_ref().unwrap().epoch, 2);
        tables.sweep(now, Duration::from_secs(10));
        let (next, latest, records) = tables.sync_page(1);
        assert_eq!((next, latest, records.len()), (3, 3, 1));
        assert_eq!(records[0].request_id, r(2));
        assert!(tables.sync_page(3).2.is_empty());
    }

    #[test]
    fn cohort_digest_ignores_other_clients_and_soon_expiring_history() {
        let now = Instant::now();
        let mut tables = Tables::default();
        let mut soon = response(1, 1);
        soon.retained_until_ms = unix_ms() + 1000;
        tables.offer(r(1), soon, &incident(), now);
        let mut other = r(2);
        other.client_id = uuid::Uuid::new_v4();
        let mut later = response(1, 1);
        later.retained_until_ms = unix_ms() + 60_000;
        tables.offer(other, later, &incident(), now);
        assert_eq!(tables.scoped_digest(&[], 0).0, 2);
        assert_eq!(tables.scoped_digest(&[], unix_ms() + 10_000).0, 1);
        assert_eq!(tables.scoped_digest(&[other.client_id], 0).0, 1);
        assert_eq!(tables.scoped_digest(&[uuid::Uuid::new_v4()], 0).0, 0);
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
    fn incremental_digest_matches_a_full_recount() {
        let recount = |t: &Tables| {
            t.results
                .iter()
                .fold(0u64, |acc, (k, (_, r))| acc.wrapping_add(entry_hash(k, r)))
        };
        let r = |seq| RequestId {
            client_id: Uuid::nil(),
            seq,
        };
        let t0 = Instant::now();
        let mut t = Tables::default();
        t.offer(r(1), response(1, 1), &incident(), t0);
        t.offer(
            r(2),
            response(2, 1),
            &incident(),
            t0 + Duration::from_secs(5),
        );
        t.offer(r(1), response(1, 1), &incident(), t0); // kept
        t.offer(r(1), response(3, 2), &incident(), t0); // replaced
        assert_eq!(t.results_digest(), recount(&t));
        t.sweep(t0 + Duration::from_secs(3), Duration::from_secs(2));
        assert_eq!(t.len().1, 1);
        assert_eq!(t.results_digest(), recount(&t));
    }

    #[test]
    fn recovery_keys_since_selects_entries_changed_after_it() {
        let r = |seq| RequestId {
            client_id: Uuid::nil(),
            seq,
        };
        let t0 = Instant::now();
        let later = t0 + Duration::from_secs(10);
        let mut t = Tables::default();
        t.record(r(1), 1, 0, &incident(), 1, t0);
        t.record(r(2), 1, 0, &incident(), 1, t0);
        t.record(r(3), 1, 0, &incident(), 1, later);
        // An old request completed late, and an old one retried late, count.
        t.offer(r(1), response(1, 1), &incident(), later);
        t.record(r(2), 2, 1, &incident(), 1, later);
        assert_eq!(t.recovery_keys(None), vec![r(1), r(2), r(3)]);
        assert_eq!(
            t.recovery_keys(Some(t0 + Duration::from_secs(5))),
            vec![r(1), r(2), r(3)]
        );
        t.record(r(5), 1, 0, &incident(), 1, t0);
        assert_eq!(
            t.recovery_keys(Some(t0 + Duration::from_secs(5))),
            vec![r(1), r(2), r(3)]
        );
        let rec = t.recovery_record(&r(1)).unwrap();
        assert_eq!(rec.result.unwrap().served_by, 1);
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
