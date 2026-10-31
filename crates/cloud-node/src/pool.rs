//! Bounded planning queue served by a fixed set of worker threads.
//!
//! Planning is CPU-bound, so it runs on dedicated OS threads, not on the tokio
//! runtime that handles network I/O. The pool tracks pending work as the sum
//! of the expected costs of queued and running jobs (the load signal the
//! election ranks on). Queued-but-not-started jobs can be dropped (step-down
//! in Part B); running jobs always finish.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolLoad {
    pub queued: usize,
    pub running: usize,
    /// Expected cost of queued + running jobs.
    pub pending_cost: u64,
}

struct Queue<J> {
    jobs: VecDeque<(u64, J)>,
    queued_cost: u64,
    running_cost: u64,
    running: usize,
    shutdown: bool,
}

struct Inner<J> {
    q: Mutex<Queue<J>>,
    cv: Condvar,
    capacity: usize,
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
        let inner = Arc::new(Inner {
            q: Mutex::new(Queue { jobs: VecDeque::new(), queued_cost: 0, running_cost: 0, running: 0, shutdown: false }),
            cv: Condvar::new(),
            capacity,
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
        Pool { inner, threads: Mutex::new(threads) }
    }

    /// Queues `job`, or hands it back if the queue is full.
    pub fn try_submit(&self, cost: u64, job: J) -> Result<(), J> {
        let mut q = self.inner.q.lock().unwrap();
        if q.shutdown || q.jobs.len() >= self.inner.capacity {
            return Err(job);
        }
        q.queued_cost += cost;
        q.jobs.push_back((cost, job));
        drop(q);
        self.inner.cv.notify_one();
        Ok(())
    }

    /// Removes every job not yet started and returns them, oldest first.
    pub fn drop_queued(&self) -> Vec<J> {
        let mut q = self.inner.q.lock().unwrap();
        q.queued_cost = 0;
        q.jobs.drain(..).map(|(_, j)| j).collect()
    }

    pub fn load(&self) -> PoolLoad {
        let q = self.inner.q.lock().unwrap();
        PoolLoad { queued: q.jobs.len(), running: q.running, pending_cost: q.queued_cost + q.running_cost }
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
                if let Some(item) = q.jobs.pop_front() {
                    q.queued_cost -= item.0;
                    q.running_cost += item.0;
                    q.running += 1;
                    break item;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

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
        assert_eq!(pool.load(), PoolLoad { queued: 2, running: 1, pending_cost: 12 });

        assert_eq!(pool.drop_queued(), vec![2, 3]);
        assert_eq!(pool.load().pending_cost, 5);
        gate_tx.send(()).unwrap();
        assert_eq!(done_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
        drop(gate_tx);
        pool.shutdown();
        assert_eq!(pool.load(), PoolLoad::default());
    }
}
