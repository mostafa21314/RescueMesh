//! Bounded planning queue served by a fixed set of worker threads.
//!
//! Planning is CPU-bound, so it runs on dedicated OS threads, not on the tokio
//! runtime that handles network I/O. The pool tracks pending work as the sum
//! of the expected costs of queued and running jobs (the load signal the
//! election ranks on). Queued-but-not-started jobs can be dropped (step-down
//! in Part B); running jobs always finish.

use common::config::{SchedulingParams, SchedulingPolicy};
use common::WorkerStats;
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolLoad {
    pub queued: usize,
    pub running: usize,
    /// Expected cost of queued + running jobs.
    pub pending_cost: u64,
}

struct Queue<J> {
    jobs: VecDeque<Queued<J>>,
    queued_cost: u64,
    running_cost: u64,
    running: usize,
    shutdown: bool,
    stats: WorkerStats,
}

struct Queued<J> {
    cost: u64,
    job: J,
    priority: u32,
    entered: Instant,
}

struct Inner<J> {
    q: Mutex<Queue<J>>,
    cv: Condvar,
    capacity: usize,
    scheduling: SchedulingParams,
}

pub struct Pool<J> {
    inner: Arc<Inner<J>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<J: Send + 'static> Pool<J> {
    pub fn new<F>(workers: usize, capacity: usize, handler: F) -> Self
    where
        F: Fn(J) + Send + Sync + 'static,
    {
        Self::with_scheduler(
            workers,
            capacity,
            SchedulingParams {
                policy: SchedulingPolicy::Fifo,
                ..Default::default()
            },
            handler,
        )
    }

    pub fn with_scheduler<F>(
        workers: usize,
        capacity: usize,
        scheduling: SchedulingParams,
        handler: F,
    ) -> Self
    where
        F: Fn(J) + Send + Sync + 'static,
    {
        let inner = Arc::new(Inner {
            q: Mutex::new(Queue {
                jobs: VecDeque::new(),
                queued_cost: 0,
                running_cost: 0,
                running: 0,
                shutdown: false,
                stats: WorkerStats::default(),
            }),
            cv: Condvar::new(),
            capacity,
            scheduling,
        });
        let handler = Arc::new(handler);
        let threads = (0..workers)
            .map(|i| {
                let inner = inner.clone();
                let handler = handler.clone();
                std::thread::Builder::new()
                    .name(format!("planner-{i}"))
                    .spawn(move || worker(&inner, &*handler))
                    .expect("spawn planner thread")
            })
            .collect();
        Pool {
            inner,
            threads: Mutex::new(threads),
        }
    }

    /// Queues `job`, or hands it back if the queue is full.
    pub fn try_submit(&self, cost: u64, job: J) -> Result<(), J> {
        self.try_submit_ranked(cost, 0, job)
    }

    pub fn try_submit_ranked(&self, cost: u64, priority: u32, job: J) -> Result<(), J> {
        let mut q = self.inner.q.lock().unwrap();
        if q.shutdown || q.jobs.len() >= self.inner.capacity {
            return Err(job);
        }
        q.queued_cost += cost;
        q.jobs.push_back(Queued {
            cost,
            job,
            priority: priority.min(1000),
            entered: Instant::now(),
        });
        drop(q);
        self.inner.cv.notify_one();
        Ok(())
    }

    /// Removes every job not yet started and returns them, oldest first.
    pub fn drop_queued(&self) -> Vec<J> {
        let mut q = self.inner.q.lock().unwrap();
        q.queued_cost = 0;
        q.jobs.drain(..).map(|j| j.job).collect()
    }

    pub fn stats(&self) -> WorkerStats {
        self.inner.q.lock().unwrap().stats.clone()
    }

    pub fn free_slots(&self) -> usize {
        self.inner
            .capacity
            .saturating_sub(self.inner.q.lock().unwrap().jobs.len())
    }

    pub fn discard_where(&self, predicate: impl Fn(&J) -> bool) -> Vec<J> {
        let mut q = self.inner.q.lock().unwrap();
        let mut removed = Vec::new();
        let mut kept = VecDeque::new();
        while let Some(item) = q.jobs.pop_front() {
            if predicate(&item.job) {
                q.queued_cost -= item.cost;
                removed.push(item.job);
            } else {
                kept.push_back(item);
            }
        }
        q.jobs = kept;
        removed
    }

    pub fn load(&self) -> PoolLoad {
        let q = self.inner.q.lock().unwrap();
        PoolLoad {
            queued: q.jobs.len(),
            running: q.running,
            pending_cost: q.queued_cost + q.running_cost,
        }
    }

    /// Stops the workers after their current job; queued jobs are discarded.
    pub fn shutdown(&self) {
        self.inner.q.lock().unwrap().shutdown = true;
        self.inner.cv.notify_all();
        for t in self.threads.lock().unwrap().drain(..) {
            let _ = t.join();
        }
    }
}

fn worker<J>(inner: &Inner<J>, handler: &(dyn Fn(J) + Send + Sync)) {
    loop {
        let (cost, job) = {
            let mut q = inner.q.lock().unwrap();
            loop {
                if q.shutdown {
                    return;
                }
                if !q.jobs.is_empty() {
                    let now = Instant::now();
                    let index = if inner.scheduling.policy == SchedulingPolicy::Fifo {
                        0
                    } else {
                        q.jobs
                            .iter()
                            .enumerate()
                            .max_by(|(ia, a), (ib, b)| {
                                priority_score(
                                    a.priority,
                                    now.duration_since(a.entered).as_secs_f64() * 1000.0,
                                    inner.scheduling,
                                )
                                .total_cmp(&priority_score(
                                    b.priority,
                                    now.duration_since(b.entered).as_secs_f64() * 1000.0,
                                    inner.scheduling,
                                ))
                                .then_with(|| ib.cmp(ia))
                            })
                            .map_or(0, |(i, _)| i)
                    };
                    let item = q.jobs.remove(index).unwrap();
                    let waited = now.duration_since(item.entered).as_micros() as u64;
                    q.stats.started += 1;
                    q.stats.queue_wait_total_us += waited;
                    q.stats.queue_wait_max_us = q.stats.queue_wait_max_us.max(waited);
                    q.queued_cost -= item.cost;
                    q.running_cost += item.cost;
                    q.running += 1;
                    break (item.cost, item.job);
                }
                q = inner.cv.wait(q).unwrap();
            }
        };
        if catch_unwind(AssertUnwindSafe(|| handler(job))).is_err() {
            tracing::error!("planner job panicked");
        }
        let mut q = inner.q.lock().unwrap();
        q.running_cost -= cost;
        q.running -= 1;
    }
}

/// Age grows without bound; the initial urgency advantage decays exponentially.
/// An arrival cannot postpone an older low-priority job beyond the boost window.
fn priority_score(priority: u32, age_ms: f64, p: SchedulingParams) -> f64 {
    age_ms
        + p.priority_boost_ms as f64 * f64::from(priority.min(1000)) / 1000.0
            * (-age_ms / p.priority_half_life_ms as f64).exp2()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    fn priority_order(age_low: bool, policy: SchedulingPolicy) -> Vec<u32> {
        let (started_tx, started_rx) = mpsc::channel();
        let (gate_tx, gate_rx) = mpsc::channel();
        let gate_rx = Mutex::new(gate_rx);
        let pool = Pool::with_scheduler(
            1,
            3,
            SchedulingParams {
                policy,
                ..Default::default()
            },
            move |id| {
                started_tx.send(id).unwrap();
                gate_rx.lock().unwrap().recv().unwrap();
            },
        );
        pool.try_submit_ranked(1, 0, 0u32).unwrap();
        assert_eq!(started_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 0);
        pool.try_submit_ranked(1, 0, 1).unwrap();
        if age_low {
            pool.inner.q.lock().unwrap().jobs[0].entered -= Duration::from_millis(500);
        }
        pool.try_submit_ranked(1, 1000, 2).unwrap();
        let mut order = Vec::new();
        for _ in 0..2 {
            gate_tx.send(()).unwrap();
            order.push(started_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        }
        gate_tx.send(()).unwrap();
        pool.shutdown();
        order
    }

    #[test]
    fn urgency_overtakes_recent_low_priority_but_waiting_prevents_starvation() {
        assert_eq!(
            priority_order(false, SchedulingPolicy::PriorityDecay),
            vec![2, 1]
        );
        assert_eq!(
            priority_order(true, SchedulingPolicy::PriorityDecay),
            vec![1, 2]
        );
        assert_eq!(priority_order(false, SchedulingPolicy::Fifo), vec![1, 2]);
    }

    #[test]
    fn full_queue_rejects_and_drop_queued_keeps_running_job() {
        let (gate_tx, gate_rx) = mpsc::channel::<()>();
        let gate_rx = Mutex::new(gate_rx);
        let (done_tx, done_rx) = mpsc::channel::<u32>();
        let done_tx = Mutex::new(done_tx);
        let pool = Pool::new(1, 2, move |j: u32| {
            gate_rx.lock().unwrap().recv().unwrap();
            done_tx.lock().unwrap().send(j).unwrap();
        });

        pool.try_submit(5, 1).unwrap();
        // Wait until the worker has taken job 1.
        while pool.load().running == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        pool.try_submit(3, 2).unwrap();
        pool.try_submit(4, 3).unwrap();
        assert_eq!(pool.try_submit(1, 4), Err(4), "queue of 2 is full");
        assert_eq!(
            pool.load(),
            PoolLoad {
                queued: 2,
                running: 1,
                pending_cost: 12
            }
        );

        assert_eq!(pool.drop_queued(), vec![2, 3]);
        assert_eq!(pool.load().pending_cost, 5);
        gate_tx.send(()).unwrap();
        assert_eq!(done_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
        drop(gate_tx);
        pool.shutdown();
        assert_eq!(pool.load(), PoolLoad::default());
    }
}
