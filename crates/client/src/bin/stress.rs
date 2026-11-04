//! Stress harness (Phase 1, bullet 6); method and results in
//! docs/stress-testing.md.
//!
//! Sweeps the number of concurrent client processes (each one the `client`
//! binary: open loop at `--rate`, with its own client id, connections and
//! retries), repeats every level `--reps` times in a shuffled order, and with
//! `--fault-ms` mutes the elected server once in every run. Per run:
//!
//! 1. wait until every node is Ready with nothing queued or running;
//! 2. full `Status` of every node (counters before);
//! 3. start the processes; a sampler polls the cheap `Probe` every `--sample-ms`;
//! 4. with `--fault-ms`: at `--fault-at-ms`, mute the elected server, then
//!    poll until it is Ready again;
//! 5. wait for the processes, then until every node is idle and the results
//!    digests agree (the convergence time);
//! 6. full `Status` again: node counters are the differences.
//!
//! Output in `--out`: runs.csv (one row per run), levels.csv (per level: mean
//! ± 95 % CI over the runs, and percentiles pooled over all their requests),
//! summary.md, samples.csv (every probe), requests.csv (every request),
//! meta.txt, clients.log (the client processes' stderr).

use clap::Parser;
use client::stats::{ci95, jain, percentile, Ci};
use client::stress::{parse_client_csv, run_metrics, Fault, Req, RunMetrics};
use client::{inject_failure, query_probe, query_status, set_load};
use common::config::ClientConfig;
use common::msg::{NodePhase, NodeProbe, NodeStatus};
use common::rng::Rng;
use common::{unix_ms, NodeId};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const PROBE_TIMEOUT: Duration = Duration::from_millis(500);
/// A full Status serializes the results table; allow for large tables.
const STATUS_TIMEOUT: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(20);

/// RescueMesh stress harness: sweeps concurrent client processes against the cloud.
#[derive(Parser, Debug)]
struct Args {
    /// Client config (node addresses, timeout, attempts), as for `client`.
    #[arg(long)]
    config: PathBuf,
    /// Load levels: concurrent client processes per run, comma-separated.
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8,16,32,48,64")]
    clients: Vec<u32>,
    /// Runs per level.
    #[arg(long, default_value_t = 5)]
    reps: u32,
    /// Incidents per client process per run.
    #[arg(long, default_value_t = 100)]
    requests: u64,
    /// Requests per second per client process (open loop).
    #[arg(long, default_value_t = 10.0)]
    rate: f64,
    /// Planner complexity per request.
    #[arg(long, default_value_t = 5)]
    complexity: u32,
    /// Mute the elected server for this long in every run.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=20_000))]
    fault_ms: Option<u64>,
    /// When to inject the failure, ms after the run's processes start.
    #[arg(long, default_value_t = 5000)]
    fault_at_ms: u64,
    /// Probe sampling interval, ms.
    #[arg(long, default_value_t = 250)]
    sample_ms: u64,
    /// Pause between runs, ms (lets the CPU average decay).
    #[arg(long, default_value_t = 1000)]
    pause_ms: u64,
    /// Seed for the run order and the processes' incidents.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Output directory (created; files in it are overwritten).
    #[arg(long)]
    out: PathBuf,
    /// The `client` binary (default: next to this one).
    #[arg(long)]
    client_bin: Option<PathBuf>,
    /// Tokio worker threads per client process.
    #[arg(long, default_value_t = 2)]
    client_threads: usize,
    /// Give up waiting for idle, recovery or convergence after this long.
    #[arg(long, default_value_t = 60)]
    settle_secs: u64,
    /// Free text stored in meta.txt and summary.md (machines, build, ...).
    #[arg(long, default_value = "")]
    label: String,
    /// Shell command run before every run, followed by a warm-up burst that
    /// elects a server. With a cluster restart (`scripts/run_local.sh`) every
    /// run starts from empty tables, so runs are independent replications.
    #[arg(long)]
    before_run: Option<String>,
    /// With --fault-ms: instead of muting the elected server, run this shell
    /// command, `{node}` replaced by its id and `{ms}` by --fault-ms. It must
    /// stop the node at once and start it again after {ms}, e.g. a crash and
    /// cold restart: `scripts/restart_node.sh {node} {ms}`.
    #[arg(long, requires = "fault_ms")]
    fault_cmd: Option<String>,
    /// Start a fresh owned local cluster per run, using node1..3.toml here.
    #[arg(long, conflicts_with = "before_run")]
    local_nodes: Option<PathBuf>,
    /// Apply temporary CPU load to this node id during each run.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=3))]
    load_node: Option<u32>,
    #[arg(long, default_value_t = 2)]
    load_threads: usize,
}

/// One probe answer (or `None`: no answer within `PROBE_TIMEOUT`).
struct Sample {
    t_ms: u64,
    node: usize,
    probe: Option<NodeProbe>,
}

/// One run's measurements: client-side metrics, node counter deltas summed
/// over the nodes, sampler aggregates, convergence and fault figures.
struct RunRow {
    workload_wall_ms: f64,
    expected_requests: usize,
    fault_requested: bool,
    udp_sent: u64,
    udp_bytes: u64,
    udp_retransmissions: u64,
    udp_duplicates: u64,
    udp_timeouts: u64,
    run: u32,
    rep: u32,
    clients: u32,
    offered_rps: f64,
    m: RunMetrics,
    /// Node ids, in config order, for the per-node shares.
    nodes: Vec<NodeId>,
    elections: u64,
    epoch_after: u64,
    election_ms_mean: f64,
    electbid_msgs: u64,
    electbid_bytes: u64,
    replicate_msgs: u64,
    replicate_bytes: u64,
    node_sent_bytes: u64,
    suppressed: u64,
    step_downs: u64,
    late_results: u64,
    dropped_on_step_down: u64,
    replicate_conflicts: u64,
    peer_drops: u64,
    overcommits: u64,
    results_before: usize,
    queue_max: usize,
    queue_mean: f64,
    cpu_mean: f64,
    cpu_max: f64,
    rss_max_mb: f64,
    score_max: u32,
    converged: bool,
    converge_ms: f64,
    fault: Option<Fault>,
    run_start_ms: u64,
    recovery_ms: f64,
    exit_failures: u32,
}

impl RunRow {
    fn served_share(&self) -> Vec<f64> {
        self.nodes
            .iter()
            .map(|n| *self.m.served.get(n).unwrap_or(&0) as f64)
            .collect()
    }

    /// Share of the answers given by the busiest node.
    fn top_share(&self) -> f64 {
        let s = self.served_share();
        let total: f64 = s.iter().sum();
        if total == 0.0 {
            f64::NAN
        } else {
            s.iter().copied().fold(0.0, f64::max) / total
        }
    }

    fn fields(&self) -> Vec<(&'static str, String)> {
        let m = &self.m;
        let fm = m.fault;
        let fault =
            |g: &dyn Fn(&client::stress::FaultMetrics) -> f64| fm.as_ref().map_or(f64::NAN, g);
        let rel = |t: Option<u64>| t.map_or(f64::NAN, |t| t as f64 - self.run_start_ms as f64);
        let served = self
            .nodes
            .iter()
            .map(|n| format!("{n}:{}", m.served.get(n).unwrap_or(&0)))
            .collect::<Vec<_>>()
            .join(";");
        vec![
            ("run", self.run.to_string()),
            ("rep", self.rep.to_string()),
            ("clients", self.clients.to_string()),
            ("offered_rps", f(self.offered_rps)),
            ("requests", self.expected_requests.to_string()),
            ("observed_requests", m.requests.to_string()),
            ("answered", m.answered.to_string()),
            (
                "failed",
                self.expected_requests
                    .saturating_sub(m.answered)
                    .to_string(),
            ),
            ("fault_requested", self.fault_requested.to_string()),
            ("fault_injected", self.fault.is_some().to_string()),
            ("udp_datagrams_sent", self.udp_sent.to_string()),
            ("udp_bytes_sent", self.udp_bytes.to_string()),
            ("udp_retransmissions", self.udp_retransmissions.to_string()),
            ("udp_duplicate_fragments", self.udp_duplicates.to_string()),
            ("udp_delivery_timeouts", self.udp_timeouts.to_string()),
            ("retried", m.retried.to_string()),
            ("busy_replies", m.busy.to_string()),
            ("span_ms", f(m.span_ms)),
            ("throughput_rps", f(m.throughput)),
            ("workload_wall_ms", f(self.workload_wall_ms)),
            (
                "goodput_rps",
                f(m.answered as f64 * 1000.0 / self.workload_wall_ms.max(1.0)),
            ),
            ("lat_mean", f(m.latency.mean)),
            ("lat_p50", f(m.latency.p50)),
            ("lat_p95", f(m.latency.p95)),
            ("lat_p99", f(m.latency.p99)),
            ("lat_max", f(m.latency.max)),
            ("served", served),
            ("top_share", f(self.top_share())),
            ("jain_served", f(jain(&self.served_share()))),
            ("max_gap_ms", f(m.max_gap_ms)),
            ("elections", self.elections.to_string()),
            ("epoch_after", self.epoch_after.to_string()),
            ("election_ms_mean", f(self.election_ms_mean)),
            ("electbid_msgs", self.electbid_msgs.to_string()),
            ("electbid_bytes", self.electbid_bytes.to_string()),
            ("replicate_msgs", self.replicate_msgs.to_string()),
            ("replicate_bytes", self.replicate_bytes.to_string()),
            ("node_sent_bytes", self.node_sent_bytes.to_string()),
            ("suppressed_triggers", self.suppressed.to_string()),
            ("step_downs", self.step_downs.to_string()),
            ("late_results", self.late_results.to_string()),
            (
                "dropped_on_step_down",
                self.dropped_on_step_down.to_string(),
            ),
            ("replicate_conflicts", self.replicate_conflicts.to_string()),
            ("peer_queue_drops", self.peer_drops.to_string()),
            ("overcommits", self.overcommits.to_string()),
            ("results_before", self.results_before.to_string()),
            ("queue_max", self.queue_max.to_string()),
            ("queue_mean", f(self.queue_mean)),
            ("cpu_mean", f(self.cpu_mean)),
            ("cpu_max", f(self.cpu_max)),
            ("rss_max_mb", f(self.rss_max_mb)),
            ("score_max", self.score_max.to_string()),
            ("converged", self.converged.to_string()),
            ("converge_ms", f(self.converge_ms)),
            (
                "victim",
                self.fault.map(|x| x.victim.to_string()).unwrap_or_default(),
            ),
            ("inject_rel_ms", f(rel(self.fault.map(|x| x.inject_ms)))),
            ("ready_rel_ms", f(rel(self.fault.and_then(|x| x.ready_ms)))),
            ("recovery_ms", f(self.recovery_ms)),
            (
                "out_of_service_ms",
                f(self
                    .fault
                    .and_then(|x| x.ready_ms.map(|r| r.saturating_sub(x.inject_ms) as f64))
                    .unwrap_or(f64::NAN)),
            ),
            ("failover_ms", f(fault(&|x| x.failover_ms))),
            ("p50_before", f(fault(&|x| x.before.p50))),
            ("p99_before", f(fault(&|x| x.before.p99))),
            ("p50_during", f(fault(&|x| x.during.p50))),
            ("p99_during", f(fault(&|x| x.during.p99))),
            ("p50_after", f(fault(&|x| x.after.p50))),
            ("p99_after", f(fault(&|x| x.after.p99))),
            (
                "failed_during",
                fm.map(|x| x.failed_during.to_string()).unwrap_or_default(),
            ),
            ("client_exit_failures", self.exit_failures.to_string()),
        ]
    }
}

/// CSV number: 3 decimals, empty for NaN.
fn f(x: f64) -> String {
    if x.is_finite() {
        format!("{x:.3}")
    } else {
        String::new()
    }
}

/// Markdown number: fewer decimals for bigger values, "–" for NaN.
fn num(x: f64) -> String {
    match x.abs() {
        a if !a.is_finite() => "–".into(),
        a if a >= 100.0 => format!("{x:.0}"),
        a if a >= 10.0 => format!("{x:.1}"),
        _ => format!("{x:.2}"),
    }
}

/// Markdown "mean ± half-width".
fn pm(c: Ci) -> String {
    if c.half.is_finite() {
        format!("{} ± {}", num(c.mean), num(c.half))
    } else {
        num(c.mean)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let cfg = Arc::new(ClientConfig::load(&args.config)?);
    anyhow::ensure!(!args.clients.is_empty() && args.reps > 0, "nothing to run");
    anyhow::ensure!(
        args.rate.is_finite() && args.rate > 0.0,
        "--rate must be finite and > 0 (open loop)"
    );
    anyhow::ensure!(
        args.requests > 0 && args.clients.iter().all(|c| *c > 0),
        "requests and clients must be positive"
    );
    anyhow::ensure!(
        args.sample_ms > 0 && args.client_threads > 0 && args.settle_secs > 0,
        "sample-ms, client-threads and settle-secs must be positive"
    );
    anyhow::ensure!(
        !args.out.join("runs.csv").exists(),
        "output already contains runs.csv; choose a new directory"
    );
    let run_ms = args.requests as f64 / args.rate * 1e3;
    if let Some(ms) = args.fault_ms {
        anyhow::ensure!(
            run_ms > (args.fault_at_ms + ms) as f64,
            "a run lasts about {run_ms:.0} ms: --fault-at-ms + --fault-ms must end before that"
        );
    }
    let client_bin = match &args.client_bin {
        Some(p) => p.clone(),
        None => std::env::current_exe()?
            .with_file_name(format!("client{}", std::env::consts::EXE_SUFFIX)),
    };
    anyhow::ensure!(
        client_bin.exists(),
        "client binary {} not found",
        client_bin.display()
    );
    std::fs::create_dir_all(args.out.join("tmp"))?;
    write_meta(&args, &cfg)?;

    // Rep-major order with the levels shuffled in every rep, so a drift over
    // the sweep (table growth, thermal throttling) is not confounded with load.
    let mut rng = Rng::new(args.seed);
    let mut schedule = Vec::new();
    for rep in 1..=args.reps {
        let mut levels = args.clients.clone();
        for i in (1..levels.len()).rev() {
            levels.swap(i, rng.below(i as u64 + 1) as usize);
        }
        schedule.extend(levels.into_iter().map(|c| (rep, c)));
    }

    let mut runs_csv = None::<BufWriter<File>>;
    let mut samples_csv = BufWriter::new(File::create(args.out.join("samples.csv"))?);
    writeln!(samples_csv, "run,t_ms,node,reachable,phase,epoch,server,queue_len,running,pending_cost,load_score,cpu_load,rss_mb,results")?;
    let mut requests_csv = BufWriter::new(File::create(args.out.join("requests.csv"))?);
    writeln!(
        requests_csv,
        "run,clients,rep,client,seq,sent_rel_ms,latency_ms,served_by,epoch,attempts,busy,answered"
    )?;

    let mut rows = Vec::new();
    let mut pooled: BTreeMap<u32, Vec<f64>> = BTreeMap::new();
    let total = schedule.len();
    let mut failure = None;
    for (i, &(rep, clients)) in schedule.iter().enumerate() {
        let run = i as u32 + 1;
        tokio::time::sleep(Duration::from_millis(args.pause_ms)).await;
        let (row, reqs, samples) = match run_once(&args, &cfg, &client_bin, run, rep, clients).await
        {
            Ok(x) => x,
            Err(e) => {
                // Summarize the runs so far before reporting the error.
                failure = Some(e.context(format!("run {run}")));
                break;
            }
        };
        println!(
            "run {run}/{total} rep {rep} clients {clients}: {}/{} answered, {:.1} req/s, p50 {:.2} p99 {:.2} ms, elections {}, converged {} ({:.0} ms){}",
            row.m.answered,
            row.m.requests,
            row.m.throughput,
            row.m.latency.p50,
            row.m.latency.p99,
            row.elections,
            row.converged,
            row.converge_ms,
            match row.m.fault {
                Some(fm) => format!(
                    ", victim {:?} failover {:.0} ms recovery {:.0} ms",
                    row.fault.map(|x| x.victim),
                    fm.failover_ms,
                    row.recovery_ms
                ),
                None if args.fault_ms.is_some() => ", fault NOT injected".into(),
                None => String::new(),
            }
        );
        let fields = row.fields();
        let w = match &mut runs_csv {
            Some(w) => w,
            None => {
                let mut w = BufWriter::new(File::create(args.out.join("runs.csv"))?);
                let header: Vec<&str> = fields.iter().map(|(k, _)| *k).collect();
                writeln!(w, "{}", header.join(","))?;
                runs_csv.insert(w)
            }
        };
        let values: Vec<&str> = fields.iter().map(|(_, v)| v.as_str()).collect();
        writeln!(w, "{}", values.join(","))?;
        w.flush()?;
        write_samples(&mut samples_csv, run, &row.nodes, &samples)?;
        write_requests(
            &mut requests_csv,
            run,
            clients,
            rep,
            row.run_start_ms,
            &reqs,
        )?;
        pooled
            .entry(clients)
            .or_default()
            .extend(reqs.iter().filter_map(|r| r.latency_ms));
        rows.push(row);
    }
    samples_csv.flush()?;
    requests_csv.flush()?;
    let _ = std::fs::remove_dir_all(args.out.join("tmp"));

    if !rows.is_empty() {
        write_levels(&args, &rows, &mut pooled)?;
        println!("wrote {}", args.out.display());
    }
    failure.map_or(Ok(()), Err)
}

/// `sh -c line` (`cmd /C line` on Windows), stdout discarded.
fn shell(line: &str) -> Command {
    let mut c = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.args(["/C", line]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", line]);
        c
    };
    c.stdout(Stdio::null());
    c
}

/// Runs the `--before-run` hook if there is one, waits until the cloud is
/// idle, then sends a 20-request burst so a server is elected before measuring.
async fn prepare(
    args: &Args,
    cfg: &ClientConfig,
    client_bin: &Path,
    run: u32,
) -> anyhow::Result<()> {
    let limit = Duration::from_secs(args.settle_secs);
    wait_idle(cfg, limit).await?;
    let csv = args.out.join(format!("tmp/warmup{run}.csv"));
    let mut warm = spawn_client(args, client_bin, 20, 0.0, args.seed, &csv)?;
    let status = tokio::task::spawn_blocking(move || warm.wait()).await??;
    anyhow::ensure!(status.success(), "warm-up client failed: {status}");
    wait_idle(cfg, limit).await
}

fn spawn_client(
    args: &Args,
    bin: &Path,
    count: u64,
    rate: f64,
    seed: u64,
    csv: &Path,
) -> anyhow::Result<std::process::Child> {
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(args.out.join("clients.log"))?;
    Ok(Command::new(bin)
        .arg("--config")
        .arg(&args.config)
        .args(["--count", &count.to_string()])
        .args(["--rate", &rate.to_string()])
        .args(["--complexity", &args.complexity.to_string()])
        .args(["--seed", &seed.to_string()])
        .arg("--out")
        .arg(csv)
        .env("TOKIO_WORKER_THREADS", args.client_threads.to_string())
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()?)
}

/// Own only the children this run created. Drop also cleans up on errors.
struct LocalNodes(Vec<std::process::Child>);
impl Drop for LocalNodes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn start_local(args: &Args, run: u32) -> anyhow::Result<LocalNodes> {
    let mut children = LocalNodes(Vec::new());
    let Some(dir) = &args.local_nodes else {
        return Ok(children);
    };
    let client_cfg = ClientConfig::load(&args.config)?;
    let mut configs = Vec::new();
    for id in 1..=3 {
        let path = dir.join(format!("node{id}.toml"));
        let cfg = common::config::NodeConfig::load(&path)?;
        let addr: std::net::SocketAddr = cfg.listen.parse()?;
        anyhow::ensure!(
            addr.ip().is_loopback(),
            "--local-nodes requires loopback listeners"
        );
        anyhow::ensure!(
            client_cfg.nodes.contains(&cfg.listen),
            "client config must point at this local cluster"
        );
        // Reserve all UDP ports before starting anything; never attach a test
        // to an unrelated cluster which happens to be running on these ports.
        configs.push((path, std::net::UdpSocket::bind(addr)?));
    }
    let bin = std::env::current_exe()?
        .with_file_name(format!("cloud-node{}", std::env::consts::EXE_SUFFIX));
    for (i, (path, reservation)) in configs.into_iter().enumerate() {
        drop(reservation);
        let log = File::create(args.out.join(format!("run{run}-node{}.log", i + 1)))?;
        children.0.push(
            Command::new(&bin)
                .arg("--config")
                .arg(path)
                .env("RUST_LOG", "info")
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()?,
        );
    }
    Ok(children)
}

async fn run_once(
    args: &Args,
    cfg: &Arc<ClientConfig>,
    client_bin: &Path,
    run: u32,
    rep: u32,
    clients: u32,
) -> anyhow::Result<(RunRow, Vec<Req>, Vec<Sample>)> {
    let limit = Duration::from_secs(args.settle_secs);
    let _local = start_local(args, run)?;
    if let Some(hook) = &args.before_run {
        let mut cmd = shell(hook);
        let status = tokio::task::spawn_blocking(move || cmd.status()).await??;
        anyhow::ensure!(status.success(), "--before-run failed: {status}");
    }
    wait_idle(cfg, limit).await?;
    let loaded = if let Some(id) = args.load_node {
        let statuses = status_all(cfg).await;
        let i = statuses
            .iter()
            .position(|s| s.as_ref().is_some_and(|s| s.node_id == id))
            .ok_or_else(|| anyhow::anyhow!("load node {id} did not answer"))?;
        let duration =
            (args.requests as f64 / args.rate * 1000.0) as u64 + args.settle_secs * 1000 + 15_000;
        set_load(&cfg.nodes[i], args.load_threads, duration, STATUS_TIMEOUT).await?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        Some(cfg.nodes[i].clone())
    } else {
        None
    };
    prepare(args, cfg, client_bin, run).await?;
    wait_idle(cfg, limit).await?;
    let mut before = status_all(cfg).await;
    for _ in 0..2 {
        if before.iter().all(Option::is_some) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        before = status_all(cfg).await;
    }
    anyhow::ensure!(
        before.iter().all(Option::is_some),
        "a node did not answer Status before run {run}"
    );
    let before: Vec<NodeStatus> = before.into_iter().flatten().collect();

    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let run_start_ms = unix_ms();
    let sampler = tokio::spawn(sample(
        cfg.clone(),
        Duration::from_millis(args.sample_ms),
        t0,
        stop.clone(),
    ));
    let fault_task = args.fault_ms.map(|ms| {
        tokio::spawn(inject(
            cfg.clone(),
            t0 + Duration::from_millis(args.fault_at_ms),
            ms,
            args.fault_cmd.clone(),
            limit,
        ))
    });
    let mut children = Vec::new();
    for i in 0..clients {
        let csv = args.out.join(format!("tmp/run{run}_c{i}.csv"));
        let seed = args.seed.wrapping_mul(1_000_003) + u64::from(run) * 1000 + u64::from(i);
        let child = spawn_client(args, client_bin, args.requests, args.rate, seed, &csv)?;
        children.push((i, csv, child));
    }
    let waited = tokio::task::spawn_blocking(move || {
        children
            .into_iter()
            .map(|(i, csv, mut c)| (i, csv, c.wait()))
            .collect::<Vec<_>>()
    })
    .await?;
    let workload_wall_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let fault = match fault_task {
        Some(t) => match t.await? {
            Ok(x) => Some(x),
            Err(e) => {
                tracing::warn!(run, error = %e, "failure not injected");
                None
            }
        },
        None => None,
    };
    stop.store(true, Ordering::Relaxed);
    let samples = sampler.await?;
    let (converge, after) = converge(cfg, limit).await;
    if let Some(addr) = loaded {
        set_load(&addr, 0, 0, STATUS_TIMEOUT).await?;
    }

    let mut reqs = Vec::new();
    let mut exit_failures = 0;
    for (i, csv, status) in waited {
        if !matches!(status, Ok(s) if s.success()) {
            exit_failures += 1;
        }
        if let Ok(text) = std::fs::read_to_string(&csv) {
            reqs.extend(parse_client_csv(&text, i)?);
        }
        let _ = std::fs::remove_file(&csv);
    }
    let m = run_metrics(&reqs, fault.as_ref());

    // Node counters: after − before, summed over the nodes that answered.
    let pairs: Vec<(&NodeStatus, &NodeStatus)> = before
        .iter()
        .zip(&after)
        .filter_map(|(b, a)| a.as_ref().map(|a| (b, a)))
        .collect();
    let delta = |g: &dyn Fn(&NodeStatus) -> u64| -> u64 {
        pairs.iter().map(|(b, a)| g(a).saturating_sub(g(b))).sum()
    };
    let sent = |kind: &'static str, bytes: bool| {
        move |s: &NodeStatus| {
            s.sent
                .get(kind)
                .map_or(0, |&(n, b)| if bytes { b } else { n })
        }
    };
    let epoch_before = before.iter().map(|s| s.epoch).max().unwrap_or(0);
    let epoch_after = pairs
        .iter()
        .map(|(_, a)| a.epoch)
        .max()
        .unwrap_or(epoch_before);
    let recorded = delta(&|s| s.elections_recorded);
    let election_us = delta(&|s| s.election_time_us);
    let recovery_ms = fault
        .and_then(|x| {
            pairs
                .iter()
                .find(|(_, a)| a.node_id == x.victim)
                .and_then(|(_, a)| a.last_recovery_ms)
        })
        .map_or(f64::NAN, |ms| ms as f64);

    // Sampler aggregates, per tick over the nodes that answered.
    let mut ticks: BTreeMap<u64, Vec<&NodeProbe>> = BTreeMap::new();
    for s in &samples {
        let tick = ticks.entry(s.t_ms).or_default();
        tick.extend(s.probe.as_ref());
    }
    let per_tick = |g: &dyn Fn(&[&NodeProbe]) -> f64| -> Vec<f64> {
        ticks
            .values()
            .filter(|t| !t.is_empty())
            .map(|t| g(t))
            .collect()
    };
    let probes = || samples.iter().filter_map(|s| s.probe.as_ref());
    let queue_sums = per_tick(&|t| t.iter().map(|p| p.queue_len as f64).sum());
    let cpu_tops = per_tick(&|t| t.iter().map(|p| p.cpu_load).fold(0.0, f64::max));

    let row = RunRow {
        workload_wall_ms,
        expected_requests: clients as usize * args.requests as usize,
        fault_requested: args.fault_ms.is_some(),
        udp_sent: delta(&|s| s.transport.datagrams_sent),
        udp_bytes: delta(&|s| s.transport.bytes_sent),
        udp_retransmissions: delta(&|s| s.transport.retransmissions),
        udp_duplicates: delta(&|s| s.transport.duplicate_fragments),
        udp_timeouts: delta(&|s| s.transport.delivery_timeouts),
        run,
        rep,
        clients,
        offered_rps: f64::from(clients) * args.rate,
        nodes: before.iter().map(|s| s.node_id).collect(),
        elections: epoch_after.saturating_sub(epoch_before),
        epoch_after,
        election_ms_mean: if recorded == 0 {
            f64::NAN
        } else {
            election_us as f64 / recorded as f64 / 1e3
        },
        electbid_msgs: delta(&sent("ElectBid", false)),
        electbid_bytes: delta(&sent("ElectBid", true)),
        replicate_msgs: delta(&sent("Replicate", false)),
        replicate_bytes: delta(&sent("Replicate", true)),
        node_sent_bytes: delta(&|s| s.sent.values().map(|&(_, b)| b).sum()),
        suppressed: delta(&|s| s.suppressed_triggers),
        step_downs: delta(&|s| s.step_downs),
        late_results: delta(&|s| s.late_results),
        dropped_on_step_down: delta(&|s| s.dropped_on_step_down),
        replicate_conflicts: delta(&|s| s.replicate_conflicts),
        peer_drops: delta(&|s| s.peer_queue_drops),
        overcommits: delta(&|s| s.resource_overcommits),
        results_before: before.iter().map(|s| s.results).max().unwrap_or(0),
        queue_max: probes().map(|p| p.queue_len).max().unwrap_or(0),
        queue_mean: client::stats::mean(&queue_sums),
        cpu_mean: client::stats::mean(&cpu_tops),
        cpu_max: cpu_tops.iter().copied().fold(f64::NAN, f64::max),
        rss_max_mb: probes().map(|p| p.rss_bytes).max().unwrap_or(0) as f64 / 1048576.0,
        score_max: probes().map(|p| p.load_score).max().unwrap_or(0),
        converged: converge.is_some(),
        converge_ms: converge.map_or(f64::NAN, |d| d.as_secs_f64() * 1e3),
        fault,
        run_start_ms,
        recovery_ms,
        exit_failures,
        m,
    };
    Ok((row, reqs, samples))
}

async fn probe_all(cfg: &ClientConfig) -> Vec<Option<NodeProbe>> {
    let mut set = JoinSet::new();
    for (i, addr) in cfg.nodes.iter().enumerate() {
        let addr = addr.clone();
        set.spawn(async move { (i, query_probe(&addr, PROBE_TIMEOUT).await.ok()) });
    }
    let mut out = vec![None; cfg.nodes.len()];
    while let Some(Ok((i, p))) = set.join_next().await {
        out[i] = p;
    }
    out
}

async fn status_all(cfg: &ClientConfig) -> Vec<Option<NodeStatus>> {
    let mut set = JoinSet::new();
    for (i, addr) in cfg.nodes.iter().enumerate() {
        let addr = addr.clone();
        set.spawn(async move { (i, query_status(&addr, STATUS_TIMEOUT).await.ok()) });
    }
    let mut out = vec![None; cfg.nodes.len()];
    while let Some(Ok((i, s))) = set.join_next().await {
        out[i] = s;
    }
    out
}

fn idle(probes: &[Option<NodeProbe>]) -> bool {
    probes.iter().all(|p| {
        p.as_ref()
            .is_some_and(|p| p.phase == NodePhase::Ready && p.queue_len == 0 && p.running == 0)
    })
}

/// Polls until every node is Ready with nothing queued or running, on two
/// polls in a row (the second covers a job that just finished).
async fn wait_idle(cfg: &ClientConfig, limit: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + limit;
    let mut quiet = 0;
    loop {
        let probes = probe_all(cfg).await;
        quiet = if idle(&probes) { quiet + 1 } else { 0 };
        if quiet >= 2 {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "cloud not idle after {limit:?}: {probes:?}"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// After the workload: time until every node is idle and all results digests
/// are equal (`None` if not within `limit`), and the final statuses. The
/// resolution is one probe round plus one Status round.
async fn converge(
    cfg: &ClientConfig,
    limit: Duration,
) -> (Option<Duration>, Vec<Option<NodeStatus>>) {
    let t0 = Instant::now();
    loop {
        if idle(&probe_all(cfg).await) {
            let st = status_all(cfg).await;
            let digests: Option<Vec<u64>> = st
                .iter()
                .map(|s| s.as_ref().map(|s| s.results_digest))
                .collect();
            if digests.is_some_and(|d| d.windows(2).all(|w| w[0] == w[1])) {
                return (Some(t0.elapsed()), st);
            }
        }
        if t0.elapsed() >= limit {
            return (None, status_all(cfg).await);
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn sample(
    cfg: Arc<ClientConfig>,
    every: Duration,
    t0: Instant,
    stop: Arc<AtomicBool>,
) -> Vec<Sample> {
    let mut out = Vec::new();
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !stop.load(Ordering::Relaxed) {
        tick.tick().await;
        let t_ms = t0.elapsed().as_millis() as u64;
        for (node, probe) in probe_all(&cfg).await.into_iter().enumerate() {
            out.push(Sample { t_ms, node, probe });
        }
    }
    out
}

/// At `at`, mutes the server most nodes name (or runs `fault_cmd` on it),
/// then polls it until it is Ready again (or `limit` after the outage ends).
async fn inject(
    cfg: Arc<ClientConfig>,
    at: Instant,
    ms: u64,
    fault_cmd: Option<String>,
    limit: Duration,
) -> anyhow::Result<Fault> {
    tokio::time::sleep_until(at.into()).await;
    let probes = probe_all(&cfg).await;
    let mut votes: BTreeMap<NodeId, usize> = BTreeMap::new();
    for s in probes.iter().flatten().filter_map(|p| p.server) {
        *votes.entry(s).or_default() += 1;
    }
    let victim = votes
        .into_iter()
        .max_by_key(|&(id, n)| (n, Reverse(id)))
        .map(|(id, _)| id)
        .ok_or_else(|| anyhow::anyhow!("no node knows a server"))?;
    let idx = probes
        .iter()
        .position(|p| p.as_ref().is_some_and(|p| p.node_id == victim))
        .ok_or_else(|| anyhow::anyhow!("server {victim} did not answer a probe"))?;
    let (inject_ms, script) = match fault_cmd {
        Some(line) => {
            let line = line
                .replace("{node}", &victim.to_string())
                .replace("{ms}", &ms.to_string());
            let child = shell(&line).spawn()?;
            let script = tokio::task::spawn_blocking(move || {
                let mut c = child;
                c.wait()
            });
            // The outage starts when the node stops answering, not when the
            // command starts (a shell can take a while to get to the kill).
            let deadline = Instant::now() + Duration::from_secs(5);
            while query_probe(&cfg.nodes[idx], PROBE_TIMEOUT).await.is_ok() {
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "--fault-cmd did not stop node {victim}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            (unix_ms(), Some(script))
        }
        None => {
            let sent = unix_ms();
            let acked = inject_failure(
                &cfg.nodes[idx],
                ms,
                Duration::from_millis(cfg.request_timeout_ms),
            )
            .await?;
            anyhow::ensure!(
                acked == victim,
                "node {acked} acknowledged, expected {victim}"
            );
            // The node mutes between our send and its acknowledgement.
            ((sent + unix_ms()) / 2, None)
        }
    };
    tokio::time::sleep(Duration::from_millis(ms)).await;
    let deadline = Instant::now() + limit;
    let ready_ms = loop {
        if let Ok(p) = query_probe(&cfg.nodes[idx], PROBE_TIMEOUT).await {
            if p.phase == NodePhase::Ready {
                break Some(unix_ms());
            }
        }
        if Instant::now() >= deadline {
            break None;
        }
        tokio::time::sleep(POLL).await;
    };
    if let Some(script) = script {
        let status = script.await??;
        anyhow::ensure!(status.success(), "--fault-cmd failed: {status}");
    }
    Ok(Fault {
        victim,
        inject_ms,
        ready_ms,
    })
}

fn write_meta(args: &Args, cfg: &ClientConfig) -> anyhow::Result<()> {
    let mut w = File::create(args.out.join("meta.txt"))?;
    writeln!(w, "label: {}", args.label)?;
    writeln!(w, "started_unix_ms: {}", unix_ms())?;
    writeln!(
        w,
        "harness_machine: {} {}, {} logical cores",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism().map_or(0, |n| n.get())
    )?;
    writeln!(w, "nodes: {:?}", cfg.nodes)?;
    writeln!(
        w,
        "request_timeout_ms: {} max_attempts: {}",
        cfg.request_timeout_ms, cfg.max_attempts
    )?;
    writeln!(w, "args: {args:?}")?;
    Ok(())
}

fn write_samples(
    w: &mut impl Write,
    run: u32,
    nodes: &[NodeId],
    samples: &[Sample],
) -> std::io::Result<()> {
    for s in samples {
        match &s.probe {
            Some(p) => writeln!(
                w,
                "{run},{},{},true,{:?},{},{},{},{},{},{},{:.3},{:.1},{}",
                s.t_ms,
                p.node_id,
                p.phase,
                p.epoch,
                p.server.map(|x| x.to_string()).unwrap_or_default(),
                p.queue_len,
                p.running,
                p.pending_cost,
                p.load_score,
                p.cpu_load,
                p.rss_bytes as f64 / 1048576.0,
                p.results
            )?,
            None => writeln!(
                w,
                "{run},{},{},false,,,,,,,,,,",
                s.t_ms,
                nodes.get(s.node).copied().unwrap_or_default()
            )?,
        }
    }
    Ok(())
}

fn write_requests(
    w: &mut impl Write,
    run: u32,
    clients: u32,
    rep: u32,
    start_ms: u64,
    reqs: &[Req],
) -> std::io::Result<()> {
    let opt = |v: Option<u64>| v.map(|x| x.to_string()).unwrap_or_default();
    for r in reqs {
        writeln!(
            w,
            "{run},{clients},{rep},{},{},{},{},{},{},{},{},{}",
            r.client,
            r.seq,
            r.sent_ms as i64 - start_ms as i64,
            r.latency_ms.map(|l| format!("{l:.3}")).unwrap_or_default(),
            opt(r.served_by.map(u64::from)),
            opt(r.epoch),
            r.attempts,
            r.busy,
            r.answered()
        )?;
    }
    Ok(())
}

/// levels.csv and summary.md: per level, the mean ± 95 % CI of each per-run
/// figure over the level's runs, and percentiles over all its requests.
fn write_levels(
    args: &Args,
    rows: &[RunRow],
    pooled: &mut BTreeMap<u32, Vec<f64>>,
) -> anyhow::Result<()> {
    let mut csv = BufWriter::new(File::create(args.out.join("levels.csv"))?);
    let mut md = String::new();
    let fault_run = args.fault_ms.is_some();
    let requests: usize = rows.iter().map(|r| r.expected_requests).sum();
    let answered: usize = rows.iter().map(|r| r.m.answered).sum();
    writeln!(md, "# Stress results: {}\n", args.out.display())?;
    if !args.label.is_empty() {
        writeln!(md, "{}\n", args.label)?;
    }
    writeln!(
        md,
        "{} runs ({} levels × {} reps, shuffled within each rep), {} requests per process per run at {} req/s, complexity {}{}. {requests} requests in total, {answered} answered ({:.2} %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.\n",
        rows.len(),
        args.clients.len(),
        args.reps,
        args.requests,
        args.rate,
        args.complexity,
        match args.fault_ms {
            Some(ms) => match &args.fault_cmd {
                Some(cmd) => format!(", the elected server taken down by `{cmd}` for {ms} ms at {} ms in every run", args.fault_at_ms),
                None => format!(", the elected server muted for {ms} ms at {} ms in every run", args.fault_at_ms),
            },
            None => String::new(),
        },
        100.0 * answered as f64 / requests.max(1) as f64
    )?;

    let mut header = vec![
        "clients",
        "offered_rps",
        "runs",
        "requests",
        "answered_pct",
        "retried_pct",
        "thr_mean",
        "thr_ci",
        "p50_mean",
        "p50_ci",
        "p95_mean",
        "p95_ci",
        "p99_mean",
        "p99_ci",
        "pooled_mean",
        "pooled_p50",
        "pooled_p95",
        "pooled_p99",
        "pooled_p999",
        "pooled_max",
        "elections_mean",
        "elections_ci",
        "electbid_msgs_mean",
        "election_ms_mean",
        "replicate_bytes_per_answer",
        "suppressed_mean",
        "queue_max_mean",
        "cpu_mean",
        "converged_runs",
        "converge_ms_mean",
        "converge_ms_ci",
        "top_share_mean",
    ];
    if fault_run {
        header.extend([
            "failover_ms_mean",
            "failover_ms_ci",
            "max_gap_ms_mean",
            "max_gap_ms_ci",
            "recovery_ms_mean",
            "recovery_ms_ci",
            "out_of_service_ms_mean",
            "out_of_service_ms_ci",
            "p99_before_mean",
            "p99_during_mean",
            "p99_during_ci",
            "p99_after_mean",
            "failed_during_total",
        ]);
    }
    writeln!(csv, "{}", header.join(","))?;

    writeln!(md, "## Latency and throughput\n")?;
    writeln!(md, "| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |")?;
    writeln!(md, "|---|---|---|---|---|---|---|---|---|")?;
    let mut coord = String::new();
    writeln!(coord, "\n## Coordination overhead and node state\n")?;
    writeln!(coord, "| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |")?;
    writeln!(coord, "|---|---|---|---|---|---|---|---|---|---|")?;
    let mut faults = String::new();
    if fault_run {
        writeln!(faults, "\n## Injected failure of the elected server\n")?;
        writeln!(faults, "| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |")?;
        writeln!(faults, "|---|---|---|---|---|---|---|")?;
    }

    let mut levels: Vec<u32> = args.clients.clone();
    levels.sort_unstable();
    levels.dedup();
    for c in levels {
        let rs: Vec<&RunRow> = rows.iter().filter(|r| r.clients == c).collect();
        if rs.is_empty() {
            continue;
        }
        let col = |g: &dyn Fn(&RunRow) -> f64| -> Vec<f64> { rs.iter().map(|r| g(r)).collect() };
        let fcol = |g: &dyn Fn(&client::stress::FaultMetrics) -> f64| -> Vec<f64> {
            rs.iter()
                .map(|r| r.m.fault.as_ref().map_or(f64::NAN, g))
                .collect()
        };
        let req: usize = rs.iter().map(|r| r.expected_requests).sum();
        let ans: usize = rs.iter().map(|r| r.m.answered).sum();
        let retried: usize = rs.iter().map(|r| r.m.retried).sum();
        let repl: u64 = rs.iter().map(|r| r.replicate_bytes).sum();
        let lat = pooled.entry(c).or_default();
        lat.sort_by(|a, b| a.total_cmp(b));
        let pooled_mean = client::stats::mean(lat);
        let thr = ci95(&col(&|r| r.m.throughput));
        let p50 = ci95(&col(&|r| r.m.latency.p50));
        let p95 = ci95(&col(&|r| r.m.latency.p95));
        let p99 = ci95(&col(&|r| r.m.latency.p99));
        let elections = ci95(&col(&|r| r.elections as f64));
        let bids = ci95(&col(&|r| r.electbid_msgs as f64));
        let el_ms = ci95(&col(&|r| r.election_ms_mean));
        let suppressed = ci95(&col(&|r| r.suppressed as f64));
        let qmax = ci95(&col(&|r| r.queue_max as f64));
        let cpu = ci95(&col(&|r| r.cpu_mean));
        let conv = ci95(&col(&|r| r.converge_ms));
        let converged = rs.iter().filter(|r| r.converged).count();
        let top = ci95(&col(&|r| r.top_share()));
        let answered_pct = 100.0 * ans as f64 / req.max(1) as f64;
        let retried_pct = 100.0 * retried as f64 / req.max(1) as f64;
        let repl_per = repl as f64 / ans.max(1) as f64;
        let mut vals = vec![
            c.to_string(),
            f(f64::from(c) * args.rate),
            rs.len().to_string(),
            req.to_string(),
            f(answered_pct),
            f(retried_pct),
            f(thr.mean),
            f(thr.half),
            f(p50.mean),
            f(p50.half),
            f(p95.mean),
            f(p95.half),
            f(p99.mean),
            f(p99.half),
            f(pooled_mean),
            f(percentile(lat, 0.50)),
            f(percentile(lat, 0.95)),
            f(percentile(lat, 0.99)),
            f(percentile(lat, 0.999)),
            f(percentile(lat, 1.0)),
            f(elections.mean),
            f(elections.half),
            f(bids.mean),
            f(el_ms.mean),
            f(repl_per),
            f(suppressed.mean),
            f(qmax.mean),
            f(cpu.mean),
            converged.to_string(),
            f(conv.mean),
            f(conv.half),
            f(top.mean),
        ];
        writeln!(
            md,
            "| {c} | {} | {} | {} | {} | {} | {} | {} / {} | {} |",
            num(f64::from(c) * args.rate),
            pm(thr),
            num(answered_pct),
            pm(p50),
            pm(p95),
            pm(p99),
            num(percentile(lat, 0.99)),
            num(percentile(lat, 0.999)),
            num(retried_pct),
        )?;
        writeln!(
            coord,
            "| {c} | {} | {} | {} | {} | {} | {} | {} | {converged}/{} | {} |",
            pm(elections),
            num(bids.mean),
            num(el_ms.mean),
            num(repl_per),
            num(suppressed.mean),
            num(qmax.mean),
            num(cpu.mean),
            rs.len(),
            pm(conv),
        )?;
        if fault_run {
            let failover = ci95(&fcol(&|x| x.failover_ms));
            let gap = ci95(&col(&|r| r.m.max_gap_ms));
            let recovery = ci95(&col(&|r| r.recovery_ms));
            let out = ci95(&col(&|r| {
                r.fault
                    .and_then(|x| x.ready_ms.map(|t| t.saturating_sub(x.inject_ms) as f64))
                    .unwrap_or(f64::NAN)
            }));
            let before = ci95(&fcol(&|x| x.before.p99));
            let during = ci95(&fcol(&|x| x.during.p99));
            let after = ci95(&fcol(&|x| x.after.p99));
            let failed: usize = rs
                .iter()
                .filter_map(|r| r.m.fault.map(|x| x.failed_during))
                .sum();
            vals.extend([
                f(failover.mean),
                f(failover.half),
                f(gap.mean),
                f(gap.half),
                f(recovery.mean),
                f(recovery.half),
                f(out.mean),
                f(out.half),
                f(before.mean),
                f(during.mean),
                f(during.half),
                f(after.mean),
                failed.to_string(),
            ]);
            writeln!(
                faults,
                "| {c} | {} | {} | {} | {} | {} / {} / {} | {failed} |",
                pm(failover),
                pm(gap),
                pm(recovery),
                pm(out),
                num(before.mean),
                pm(during),
                num(after.mean),
            )?;
        }
        writeln!(csv, "{}", vals.join(","))?;
    }
    md.push_str(&coord);
    md.push_str(&faults);

    let mut served: BTreeMap<NodeId, usize> = BTreeMap::new();
    for r in rows {
        for (n, k) in &r.m.served {
            *served.entry(*n).or_default() += k;
        }
    }
    let total: usize = served.values().sum();
    let shares: Vec<String> = served
        .iter()
        .map(|(n, k)| format!("node {n} {:.1} %", 100.0 * *k as f64 / total.max(1) as f64))
        .collect();
    writeln!(
        md,
        "\nAnswers by serving node over the whole sweep: {}.",
        shares.join(", ")
    )?;
    let anomalies: Vec<String> = [
        (
            "client process exit failures",
            rows.iter().map(|r| r.exit_failures as u64).sum::<u64>(),
        ),
        ("peer queue drops", rows.iter().map(|r| r.peer_drops).sum()),
        (
            "resource overcommits",
            rows.iter().map(|r| r.overcommits).sum(),
        ),
        (
            "runs not converged",
            rows.iter().filter(|r| !r.converged).count() as u64,
        ),
    ]
    .iter()
    .map(|(k, v)| format!("{k}: {v}"))
    .collect();
    writeln!(md, "\nChecks: {}.", anomalies.join(", "))?;
    csv.flush()?;
    std::fs::write(args.out.join("summary.md"), md)?;
    Ok(())
}
