//! Test client for the cloud: multicasts incident requests to every node, measures
//! end-to-end latency, retries with the same request id, and checks that all
//! replicas hold the same committed plans.

use clap::{Parser, Subcommand};
use futures::stream::SplitSink;
use futures::StreamExt;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rescuemesh::model::*;
use rescuemesh::proto::Msg;
use rescuemesh::wire::{self, now_ms, Wire};
use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

#[derive(Parser)]
struct Cli {
    /// Cloud nodes, comma separated HOST:PORT list.
    #[arg(long, default_value = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", global = true)]
    nodes: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Closed-loop load: CONCURRENCY workers each send REQUESTS incidents.
    Load {
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        #[arg(long, default_value_t = 50)]
        requests: usize,
        #[arg(long, default_value_t = 144)]
        zones: u32,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 5000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = 3)]
        attempts: u32,
        #[arg(long, default_value_t = 0)]
        think_ms: u64,
        /// Write one CSV row per request here.
        #[arg(long)]
        out: Option<String>,
        /// Print the first plan in full.
        #[arg(long)]
        show_plan: bool,
    },
    /// Ask every node for its status and compare replicas. Election metrics are
    /// printed as key=value pairs (one line per node) for scripts.
    Status,
    /// Artificial load: make node N emulate a CPU FACTOR times slower for SECS
    /// seconds (0 = until changed; --factor 1 turns it off).
    Slow {
        #[arg(long)]
        node: usize,
        #[arg(long, default_value_t = 4.0)]
        factor: f64,
        #[arg(long, default_value_t = 30)]
        secs: u64,
    },
}

struct Row {
    request_id: String,
    worker: usize,
    sent_ms: u64,
    latency_us: u64,
    executor: Option<u32>,
    compute_us: u64,
    priority: f64,
    attempts: u32,
    ok: bool,
}

type Sinks = Vec<Option<SplitSink<Wire, bytes::Bytes>>>;

async fn connect_all(addrs: &[String], tx: &mpsc::Sender<Msg>) -> Sinks {
    let mut sinks = Vec::new();
    for a in addrs {
        sinks.push(connect_one(a, tx).await);
    }
    sinks
}

async fn connect_one(addr: &str, tx: &mpsc::Sender<Msg>) -> Option<SplitSink<Wire, bytes::Bytes>> {
    let s = tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await.ok()?.ok()?;
    let (sink, mut stream) = wire::framed(s).split();
    let tx = tx.clone();
    tokio::spawn(async move {
        while let Ok(Some(m)) = wire::recv(&mut stream).await {
            if tx.send(m).await.is_err() {
                break;
            }
        }
    });
    Some(sink)
}

fn gen_incident(rng: &mut StdRng, id: String, reporter: &str, zones: u32) -> Incident {
    let sev = match rng.gen_range(0..100) {
        0..=34 => 1,
        35..=59 => 2,
        60..=79 => 3,
        80..=93 => 4,
        _ => 5,
    };
    Incident {
        id,
        reporter: reporter.to_string(),
        category: Category::ALL[rng.gen_range(0..Category::ALL.len())],
        zone: rng.gen_range(0..zones),
        severity: sev,
        created_ms: now_ms(),
        status: IncidentStatus::Open,
        ttl_ms: 600_000,
    }
}

#[allow(clippy::too_many_arguments)]
async fn worker(
    w: usize,
    addrs: Vec<String>,
    run: String,
    requests: usize,
    zones: u32,
    seed: u64,
    timeout: Duration,
    attempts: u32,
    think: Duration,
    show_plan: bool,
) -> Vec<Row> {
    let (tx, mut rx) = mpsc::channel::<Msg>(1024);
    let mut sinks = connect_all(&addrs, &tx).await;
    let mut rng = StdRng::seed_from_u64(seed.wrapping_mul(1_000_003).wrapping_add(w as u64));
    let client_id = format!("{run}-w{w}");
    let mut rows = Vec::with_capacity(requests);

    for k in 0..requests {
        let rid = format!("{client_id}-r{k}");
        let inc = gen_incident(&mut rng, format!("{client_id}-i{k}"), &format!("user-{w}"), zones);
        let msg = Msg::Submit { request_id: rid.clone(), client_id: client_id.clone(), incident: inc };
        let t0 = Instant::now();
        let sent_ms = now_ms();
        let mut row = Row {
            request_id: rid.clone(),
            worker: w,
            sent_ms,
            latency_us: 0,
            executor: None,
            compute_us: 0,
            priority: 0.0,
            attempts: 0,
            ok: false,
        };
        'attempt: for attempt in 1..=attempts {
            row.attempts = attempt;
            // multicast to every node (reconnecting any lost link first)
            for (i, s) in sinks.iter_mut().enumerate() {
                if s.is_none() {
                    *s = connect_one(&addrs[i], &tx).await;
                }
                if let Some(sink) = s {
                    if wire::send(sink, &msg).await.is_err() {
                        *s = None;
                    }
                }
            }
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(Msg::PlanResult { request_id, plan, .. })) if request_id == rid => {
                        row.latency_us = t0.elapsed().as_micros() as u64;
                        row.executor = Some(plan.executed_by);
                        row.compute_us = plan.compute_us;
                        row.priority = plan.priority;
                        row.ok = true;
                        if show_plan && w == 0 && k == 0 {
                            println!("{}", serde_json::to_string_pretty(&*plan).unwrap());
                        }
                        break 'attempt;
                    }
                    Ok(Some(_)) => continue, // late reply to an earlier request
                    Ok(None) => break 'attempt,
                    Err(_) => break, // timed out: retry with the same request id
                }
            }
        }
        rows.push(row);
        if !think.is_zero() {
            tokio::time::sleep(think).await;
        }
    }
    rows
}

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx] as f64 / 1000.0
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let addrs: Vec<String> = cli.nodes.split(',').map(|s| s.trim().to_string()).collect();

    match cli.cmd {
        Cmd::Load { concurrency, requests, zones, seed, timeout_ms, attempts, think_ms, out, show_plan } => {
            let run = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
            let started = Instant::now();
            let handles: Vec<_> = (0..concurrency)
                .map(|w| {
                    tokio::spawn(worker(
                        w,
                        addrs.clone(),
                        run.clone(),
                        requests,
                        zones,
                        seed,
                        Duration::from_millis(timeout_ms),
                        attempts,
                        Duration::from_millis(think_ms),
                        show_plan,
                    ))
                })
                .collect();
            let mut rows = Vec::new();
            for h in handles {
                rows.extend(h.await?);
            }
            let wall = started.elapsed().as_secs_f64();

            if let Some(path) = out {
                let mut f = std::fs::File::create(&path)?;
                writeln!(f, "request_id,worker,sent_ms,latency_us,executor,compute_us,priority,attempts,ok")?;
                for r in &rows {
                    writeln!(
                        f,
                        "{},{},{},{},{},{},{:.4},{},{}",
                        r.request_id,
                        r.worker,
                        r.sent_ms,
                        r.latency_us,
                        r.executor.map(|e| e.to_string()).unwrap_or_default(),
                        r.compute_us,
                        r.priority,
                        r.attempts,
                        r.ok
                    )?;
                }
                println!("wrote {} rows to {path}", rows.len());
            }

            let ok: Vec<&Row> = rows.iter().filter(|r| r.ok).collect();
            let mut lat: Vec<u64> = ok.iter().map(|r| r.latency_us).collect();
            lat.sort();
            let mean = lat.iter().sum::<u64>() as f64 / lat.len().max(1) as f64 / 1000.0;
            let mean_compute = ok.iter().map(|r| r.compute_us).sum::<u64>() as f64 / ok.len().max(1) as f64 / 1000.0;
            let mut by_exec: HashMap<u32, usize> = HashMap::new();
            for r in &ok {
                *by_exec.entry(r.executor.unwrap()).or_default() += 1;
            }
            let mut by_exec: Vec<_> = by_exec.into_iter().collect();
            by_exec.sort();
            let retried = rows.iter().filter(|r| r.attempts > 1).count();
            println!("run {run}: {} requests, {} ok, {} failed, {} retried", rows.len(), ok.len(), rows.len() - ok.len(), retried);
            println!("throughput {:.1} req/s over {:.2} s", ok.len() as f64 / wall, wall);
            println!(
                "latency ms: mean {:.2}  p50 {:.2}  p95 {:.2}  p99 {:.2}  max {:.2}   (planner compute mean {:.2})",
                mean,
                pct(&lat, 50.0),
                pct(&lat, 95.0),
                pct(&lat, 99.0),
                pct(&lat, 100.0),
                mean_compute
            );
            println!("executed by node: {by_exec:?}");
        }
        Cmd::Status => {
            let mut digests = Vec::new();
            for a in &addrs {
                let s = match TcpStream::connect(a).await {
                    Ok(s) => s,
                    Err(e) => {
                        println!("{a}: unreachable ({e})");
                        continue;
                    }
                };
                let mut w = wire::framed(s);
                wire::send(&mut w, &Msg::StatusQuery).await?;
                match tokio::time::timeout(Duration::from_secs(2), wire::recv(&mut w)).await {
                    Ok(Ok(Some(Msg::Status(st)))) => {
                        println!(
                            "node {} @ {a}: version {} plans {} digest {} pending {} active_incidents {} executed_here {} queue {} inflight {} reserved {} service_est {:.1}ms probe_ratio {:.2} score {}us slow {} live {:?} dropped {} assigner {}",
                            st.node, st.version, st.plans, st.plans_digest, st.pending, st.active_incidents,
                            st.executed_here, st.load.queue_len, st.load.inflight, st.load.reserved,
                            st.load.ewma_service_ms, st.load.probe_ratio, st.load.score_us, st.load.slow_factor,
                            st.live, st.dropped_peer_msgs, st.assigner
                        );
                        let e = &st.election;
                        println!(
                            "  election node={} decided={} won={} full={} timeout={} bids_sent={} bid_bytes_sent={} bids_recv={} dup_bids={} late_bids={} orphans={} open={} dur_mean_us={:.0} dur_p50_us={} dur_p99_us={} dur_max_us={} executed={} duplicate_executions={}",
                            st.node, e.decided, e.won, e.decided_full, e.decided_timeout, e.bids_sent, e.bid_bytes_sent,
                            e.bids_received, e.duplicate_bids, e.late_bids, e.orphan_rounds, e.open_rounds,
                            e.duration_mean_us, e.duration_p50_us, e.duration_p99_us, e.duration_max_us,
                            st.executed_here, st.duplicate_executions
                        );
                        digests.push((st.plans, st.plans_digest));
                    }
                    other => println!("{a}: bad reply {other:?}"),
                }
            }
            if digests.len() > 1 {
                let same = digests.windows(2).all(|w| w[0] == w[1]);
                println!("replicas consistent: {same}");
            }
        }
        Cmd::Slow { node, factor, secs } => {
            let a = addrs.get(node.wrapping_sub(1)).ok_or_else(|| anyhow::anyhow!("--node must be 1..={}", addrs.len()))?;
            let mut w = wire::framed(TcpStream::connect(a).await?);
            wire::send(&mut w, &Msg::Slow { factor, secs }).await?;
            match tokio::time::timeout(Duration::from_secs(2), wire::recv(&mut w)).await {
                Ok(Ok(Some(Msg::Ack { message }))) => println!("{message}"),
                other => anyhow::bail!("{a}: bad reply {other:?}"),
            }
        }
    }
    Ok(())
}
