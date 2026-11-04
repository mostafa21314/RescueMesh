//! Load measurement and the election's load score.
//!
//! score = w_pending · pending + w_cpu · cpu + w_svc · service_time, each term
//! normalized to [0, 1] and the weighted sum divided by the weight total, then
//! quantized to milli-units (0..=1000) so every node compares the same integers.
//!
//! * pending      – pending expected cost per worker, x / (x + PENDING_HALF)
//! * cpu          – CPU use in [0, 1], sampled every 250 ms, EWMA
//! * service_time – planning time per complexity unit (EWMA), x / (x + SERVICE_HALF_MS)
//!
//! `x / (x + h)` maps [0, ∞) onto [0, 1) and reaches ½ at `h`. Unlike
//! `min(x / cap, 1)` it never saturates, so of two overloaded nodes the more
//! loaded one still scores higher.

use common::config::{CpuSource, Policy, Weights};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const CPU_SAMPLE_EVERY: Duration = Duration::from_millis(250);
/// EWMA weight of a new sample (CPU and service time).
pub const EWMA_ALPHA: f64 = 0.3;
/// Pending work per worker, in complexity units, that scores ½.
pub const PENDING_HALF: f64 = 10.0;
/// Planning time per complexity unit that scores ½ (measured ≈1 ms/unit in release).
pub const SERVICE_HALF_MS: f64 = 2.0;

/// The three normalized terms, for logs and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Terms {
    pub pending: f64,
    pub cpu: f64,
    pub service_time: f64,
}

impl Terms {
    pub fn new(pending_cost: u64, workers: usize, cpu: f64, ms_per_unit: f64) -> Self {
        let per_worker = pending_cost as f64 / workers.max(1) as f64;
        Terms {
            pending: per_worker / (per_worker + PENDING_HALF),
            cpu: cpu.clamp(0.0, 1.0),
            service_time: ms_per_unit / (ms_per_unit + SERVICE_HALF_MS),
        }
    }

    /// Score in milli-units under `policy`. RoundRobin ignores scores; it
    /// bids the composite so the logs still show load.
    pub fn score(&self, policy: Policy, w: &Weights) -> u32 {
        let x = match policy {
            Policy::QueueOnly => self.pending,
            Policy::CpuOnly => self.cpu,
            Policy::Composite | Policy::RoundRobin => {
                let total = w.pending + w.cpu + w.service_time;
                if total <= 0.0 {
                    0.0
                } else {
                    (w.pending * self.pending
                        + w.cpu * self.cpu
                        + w.service_time * self.service_time)
                        / total
                }
            }
        };
        (x.clamp(0.0, 1.0) * 1000.0).round() as u32
    }
}

fn ewma(old: Option<f64>, sample: f64) -> f64 {
    match old {
        Some(o) => o + EWMA_ALPHA * (sample - o),
        None => sample,
    }
}

#[derive(Default)]
struct Ewmas {
    cpu: Option<f64>,
    ms_per_unit: Option<f64>,
}

/// CPU sampler thread plus the planning-time EWMA. The sampler also reads
/// this process's resident memory, for `Status`.
pub struct LoadMonitor {
    ewmas: Arc<Mutex<Ewmas>>,
    rss: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

impl LoadMonitor {
    pub fn start(source: CpuSource) -> Self {
        let ewmas = Arc::new(Mutex::new(Ewmas::default()));
        let rss = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (e, r, s) = (ewmas.clone(), rss.clone(), stop.clone());
        std::thread::Builder::new()
            .name("cpu-sampler".into())
            .spawn(move || sample_cpu(source, &e, &r, &s))
            .expect("spawn cpu sampler");
        LoadMonitor { ewmas, rss, stop }
    }

    /// CPU use in [0, 1] (EWMA), as the score uses it.
    pub fn cpu(&self) -> f64 {
        self.ewmas.lock().unwrap().cpu.unwrap_or(0.0)
    }

    /// Resident memory of this process, bytes, as of the last sample.
    pub fn rss_bytes(&self) -> u64 {
        self.rss.load(Ordering::Relaxed)
    }

    /// Records one finished plan.
    pub fn record_service(&self, took: Duration, complexity_units: u64) {
        let ms = took.as_secs_f64() * 1e3 / complexity_units.max(1) as f64;
        let mut e = self.ewmas.lock().unwrap();
        e.ms_per_unit = Some(ewma(e.ms_per_unit, ms));
    }

    pub fn terms(&self, pending_cost: u64, workers: usize) -> Terms {
        let e = self.ewmas.lock().unwrap();
        Terms::new(
            pending_cost,
            workers,
            e.cpu.unwrap_or(0.0),
            e.ms_per_unit.unwrap_or(0.0),
        )
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn sample_cpu(source: CpuSource, ewmas: &Mutex<Ewmas>, rss: &AtomicU64, stop: &AtomicBool) {
    let mut sys = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().ok();
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get()) as f64;
    // The first reading only primes the counters.
    sys.refresh_cpu();
    if let Some(pid) = pid {
        sys.refresh_process(pid);
    }
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(CPU_SAMPLE_EVERY);
        let (memory, process_cpu) = match pid {
            Some(pid) if sys.refresh_process(pid) => sys.process(pid).map_or((0, 0.0), |p| {
                (p.memory(), p.cpu_usage() as f64 / 100.0 / cores)
            }),
            _ => (0, 0.0),
        };
        rss.store(memory, Ordering::Relaxed);
        let busy = match source {
            CpuSource::Machine => {
                sys.refresh_cpu();
                sys.global_cpu_info().cpu_usage() as f64 / 100.0
            }
            CpuSource::Process => process_cpu,
        };
        let mut e = ewmas.lock().unwrap();
        e.cpu = Some(ewma(e.cpu, busy.clamp(0.0, 1.0)));
    }
}

/// Starts `n` threads that spin forever: CPU load the queue and service-time
/// terms don't see (`--burn-threads`).
pub fn burn(n: usize) {
    for i in 0..n {
        std::thread::Builder::new()
            .name(format!("burn-{i}"))
            .spawn(|| {
                let mut x: u64 = 1;
                loop {
                    x = std::hint::black_box(x.wrapping_mul(6364136223846793005).wrapping_add(1));
                }
            })
            .expect("spawn burn thread");
    }
}

/// Reversible CPU load for demonstrations, bounded in duration and thread count.
#[derive(Default)]
pub struct LoadGenerator {
    active: Mutex<Option<BurnRun>>,
}

type BurnRun = (Arc<AtomicBool>, Vec<std::thread::JoinHandle<()>>);

impl LoadGenerator {
    pub fn set(&self, threads: usize, duration: Duration) -> anyhow::Result<()> {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        anyhow::ensure!(
            threads <= cores * 2,
            "load threads must be <= twice the logical CPU count ({})",
            cores * 2
        );
        anyhow::ensure!(
            threads == 0 || (!duration.is_zero() && duration <= Duration::from_secs(3600)),
            "load duration must be 1..=3,600,000 ms"
        );
        let mut active = self.active.lock().unwrap();
        if let Some((stop, handles)) = active.take() {
            stop.store(true, Ordering::Relaxed);
            for handle in handles {
                let _ = handle.join();
            }
        }
        if threads == 0 {
            return Ok(());
        }
        let stop = Arc::new(AtomicBool::new(false));
        let until = std::time::Instant::now() + duration;
        let handles = (0..threads)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut x = 1u64;
                    while !stop.load(Ordering::Relaxed) && std::time::Instant::now() < until {
                        for _ in 0..1000 {
                            x = std::hint::black_box(
                                x.wrapping_mul(6364136223846793005).wrapping_add(1),
                            );
                        }
                    }
                })
            })
            .collect();
        *active = Some((stop, handles));
        Ok(())
    }
}

impl Drop for LoadGenerator {
    fn drop(&mut self) {
        let _ = self.set(0, Duration::ZERO);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_scores_zero_and_terms_are_bounded() {
        let w = Weights::default();
        assert_eq!(Terms::new(0, 4, 0.0, 0.0).score(Policy::Composite, &w), 0);
        let t = Terms::new(1_000_000, 4, 5.0, 1e9);
        assert!(t.pending < 1.0 && t.cpu == 1.0 && t.service_time < 1.0);
        assert!(t.score(Policy::Composite, &w) <= 1000);
    }

    #[test]
    fn more_pending_scores_higher_even_when_overloaded() {
        let w = Weights::default();
        let a = Terms::new(4_000, 4, 0.2, 1.0).score(Policy::Composite, &w);
        let b = Terms::new(8_000, 4, 0.2, 1.0).score(Policy::Composite, &w);
        assert!(b > a);
    }

    #[test]
    fn single_term_policies() {
        let w = Weights::default();
        let t = Terms::new(40, 4, 0.5, 0.0); // pending ½, cpu ½
        assert_eq!(t.score(Policy::QueueOnly, &w), 500);
        assert_eq!(t.score(Policy::CpuOnly, &w), 500);
        assert_eq!(t.score(Policy::Composite, &w), 400); // 0.5·½ + 0.3·½ + 0.2·0
    }

    #[test]
    fn ewma_moves_toward_samples() {
        assert_eq!(ewma(None, 1.0), 1.0);
        assert!((ewma(Some(0.0), 1.0) - EWMA_ALPHA).abs() < 1e-12);
    }
}
