use clap::Parser;
use client::{inject_failure, query_status, synth_incident, Client, Outcome};
use common::config::ClientConfig;
use common::msg::{NodePhase, NodeStatus};
use common::rng::Rng;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// RescueMesh client: submits synthetic incidents to every cloud node.
#[derive(Parser)]
struct Args {
    /// Refuse to run a client binary from a different application protocol.
    #[arg(long, default_value_t = common::PROTOCOL_VERSION)]
    require_protocol_version: u32,
    /// Client config, e.g. config/client.toml or config/local/client.toml.
    #[arg(long)]
    config: PathBuf,
    /// Number of incidents to submit.
    #[arg(long, default_value_t = 100)]
    count: u64,
    /// Requests per second (open loop); 0 sends them all at once.
    #[arg(long, default_value_t = 50.0)]
    rate: f64,
    /// Planner complexity per request (travel-time scenarios; CPU cost ∝ this).
    #[arg(long, default_value_t = 5)]
    complexity: u32,
    /// Seed for the synthetic incidents (default: random).
    #[arg(long)]
    seed: Option<u64>,
    /// Scheduled Unix-millisecond start, used by the six-PC study barrier.
    #[arg(long, conflicts_with_all = ["status", "inject_failure"])]
    start_at_ms: Option<u64>,
    /// Stable synthetic reporter label for matched workload comparisons.
    #[arg(long)]
    reporter: Option<String>,
    /// Per-request CSV log.
    #[arg(long, default_value = "client_run.csv")]
    out: PathBuf,
    /// Print each node's status and whether their results tables agree, then exit.
    #[arg(long)]
    status: bool,
    /// Demo: silence the currently elected server, then wait for its recovery.
    #[arg(long, conflicts_with = "status")]
    inject_failure: bool,
    #[arg(long, default_value_t = 20_000, value_parser = clap::value_parser!(u64).range(1..=20_000))]
    failure_ms: u64,
    /// Repeat the demo sequentially; each round waits until all nodes are ready.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    fault_rounds: u32,
    /// With --status: first wait, up to SECS (default 10), until no node has
    /// queued or running jobs, so in-flight replication doesn't skew the digests.
    #[arg(long, value_name = "SECS", num_args = 0..=1, default_missing_value = "10", requires = "status")]
    settle: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    anyhow::ensure!(
        args.require_protocol_version == common::PROTOCOL_VERSION,
        "client protocol {} does not satisfy required protocol {}",
        common::PROTOCOL_VERSION,
        args.require_protocol_version
    );
    anyhow::ensure!(
        args.rate.is_finite() && args.rate >= 0.0,
        "rate must be finite and nonnegative"
    );
    anyhow::ensure!(args.count > 0, "count must be positive");
    let cfg = ClientConfig::load(&args.config)?;
    if args.inject_failure {
        return run_faults(&cfg, args.failure_ms, args.fault_rounds).await;
    }
    if args.status {
        return print_status(&cfg, args.settle.map(Duration::from_secs)).await;
    }

    let zones = cfg.campus.zones();
    let client = Arc::new(Client::new(cfg));
    let seed = args
        .seed
        .unwrap_or_else(|| client.client_id().as_u64_pair().0);
    let mut rng = Rng::new(seed);
    let reporter = args
        .reporter
        .unwrap_or_else(|| format!("client-{}", &client.client_id().simple().to_string()[..8]));
    tracing::info!(client = %client.client_id(), count = args.count, rate = args.rate, complexity = args.complexity, seed, "starting run");

    let scheduled_start = args.start_at_ms.unwrap_or_else(common::unix_ms);
    let now = common::unix_ms();
    anyhow::ensure!(
        scheduled_start <= now.saturating_add(60_000),
        "start must be within the next 60 seconds"
    );
    if scheduled_start > now {
        tokio::time::sleep(Duration::from_millis(scheduled_start - now)).await;
    }
    let start = Instant::now()
        .checked_sub(Duration::from_millis(
            common::unix_ms().saturating_sub(scheduled_start),
        ))
        .unwrap_or_else(Instant::now);
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..args.count {
        let offset = if args.rate > 0.0 {
            Duration::from_secs_f64(i as f64 / args.rate)
        } else {
            Duration::ZERO
        };
        let due = start + offset;
        if args.rate > 0.0 {
            tokio::time::sleep_until(due.into()).await;
        }
        let incident = synth_incident(&mut rng, zones, &reporter);
        let c = client.clone();
        let complexity = args.complexity;
        let scheduled_at_ms = scheduled_start.saturating_add(offset.as_millis() as u64);
        tasks.spawn(async move {
            let dispatch_lag_ms =
                Instant::now().saturating_duration_since(due).as_secs_f64() * 1000.0;
            (
                c.submit(incident, complexity).await,
                scheduled_at_ms,
                dispatch_lag_ms,
            )
        });
    }
    let mut logged = tasks.join_all().await;
    let wall = start.elapsed();
    logged.sort_by_key(|o| o.0.request_id.seq);

    write_csv(&args.out, &logged)?;
    let outcomes: Vec<_> = logged.into_iter().map(|o| o.0).collect();
    summarize(&outcomes, wall, &client);
    tracing::info!(file = %args.out.display(), "per-request log written");
    Ok(())
}

fn write_csv(path: &PathBuf, outcomes: &[(Outcome, u64, f64)]) -> anyhow::Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    writeln!(f, "client_id,seq,sent_at_ms,response_at_ms,latency_ms,served_by,epoch,degraded,attempts,busy_replies,answered,elapsed_ms,scheduled_at_ms,dispatch_lag_ms,allocation_policy,assignments,resource_assignments,responder_assignments,resource_shortfalls,responder_shortfalls")?;
    for (o, scheduled_at_ms, dispatch_lag_ms) in outcomes {
        let opt = |v: Option<u64>| v.map(|x| x.to_string()).unwrap_or_default();
        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{:.3},{},{:.3},{},{},{},{},{},{}",
            o.request_id.client_id,
            o.request_id.seq,
            o.sent_at_ms,
            opt(o.response_at_ms),
            o.latency
                .map(|d| format!("{:.3}", d.as_secs_f64() * 1e3))
                .unwrap_or_default(),
            opt(o.served_by.map(u64::from)),
            opt(o.epoch),
            o.degraded,
            o.attempts,
            o.busy_replies,
            o.answered(),
            o.elapsed.as_secs_f64() * 1000.0,
            scheduled_at_ms,
            dispatch_lag_ms,
            common::ALLOCATION_POLICY,
            o.plan.as_ref().map_or(0, |p| p.assignments.len()),
            o.plan.as_ref().map_or(0, |p| p
                .assignments
                .iter()
                .filter(|a| matches!(a.assignee, common::Assignee::Resource { .. }))
                .count()),
            o.plan.as_ref().map_or(0, |p| p
                .assignments
                .iter()
                .filter(|a| matches!(a.assignee, common::Assignee::Responder { .. }))
                .count()),
            o.plan.as_ref().map_or(0, |p| p
                .shortfall
                .iter()
                .filter(|s| matches!(s, common::Shortfall::Resource { .. }))
                .count()),
            o.plan.as_ref().map_or(0, |p| p
                .shortfall
                .iter()
                .filter(|s| matches!(s, common::Shortfall::Responder { .. }))
                .count())
        )?;
    }
    Ok(())
}

fn summarize(outcomes: &[Outcome], wall: Duration, client: &Client) {
    let answered: Vec<&Outcome> = outcomes.iter().filter(|o| o.answered()).collect();
    let mut lat: Vec<f64> = answered
        .iter()
        .filter_map(|o| o.latency)
        .map(|d| d.as_secs_f64() * 1e3)
        .collect();
    lat.sort_by(|a, b| a.total_cmp(b));
    let pct = |p: f64| {
        if lat.is_empty() {
            f64::NAN
        } else {
            lat[((lat.len() - 1) as f64 * p).round() as usize]
        }
    };
    let mut served: BTreeMap<u32, usize> = BTreeMap::new();
    let mut attempts: BTreeMap<u32, usize> = BTreeMap::new();
    for o in &answered {
        *served.entry(o.served_by.unwrap()).or_default() += 1;
    }
    for o in outcomes {
        *attempts.entry(o.attempts).or_default() += 1;
    }
    let busy: u32 = outcomes.iter().map(|o| o.busy_replies).sum();
    println!(
        "requests {}  answered {}  failed {}  wall {:.2}s  throughput {:.1} req/s",
        outcomes.len(),
        answered.len(),
        outcomes.len() - answered.len(),
        wall.as_secs_f64(),
        answered.len() as f64 / wall.as_secs_f64()
    );
    println!(
        "latency ms  p50 {:.2}  p95 {:.2}  p99 {:.2}  max {:.2}",
        pct(0.5),
        pct(0.95),
        pct(0.99),
        pct(1.0)
    );
    println!(
        "served_by {served:?}  attempts {attempts:?}  busy replies {busy}  highest epoch {}",
        client.highest_epoch()
    );
    let t = client.traffic();
    println!(
        "sent {:?}\nreceived {:?}\nunsent frames (node down) {}",
        t.sent, t.received, t.unsent
    );
}

async fn query_all(cfg: &ClientConfig) -> Vec<anyhow::Result<NodeStatus>> {
    let timeout = Duration::from_millis(cfg.request_timeout_ms);
    let mut out = Vec::new();
    for addr in &cfg.nodes {
        out.push(query_status(addr, timeout).await);
    }
    out
}

async fn run_faults(cfg: &ClientConfig, duration_ms: u64, rounds: u32) -> anyhow::Result<()> {
    for round in 1..=rounds {
        let statuses = query_all(cfg)
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            statuses.len() == 3 && statuses.iter().all(|s| s.phase == NodePhase::Ready),
            "all three nodes must be ready before injecting one failure"
        );
        let server = statuses[0]
            .server
            .ok_or_else(|| anyhow::anyhow!("run a warm-up request first to elect a server"))?;
        anyhow::ensure!(
            statuses
                .iter()
                .all(|s| s.server == Some(server) && s.epoch == statuses[0].epoch),
            "wait for the election views to agree"
        );
        let index = statuses
            .iter()
            .position(|s| s.node_id == server)
            .ok_or_else(|| anyhow::anyhow!("elected server missing from configuration"))?;
        let timeout = Duration::from_millis(cfg.request_timeout_ms);
        inject_failure(&cfg.nodes[index], duration_ms, timeout).await?;
        println!("round {round}: elected server {server} muted for {duration_ms} ms; workload clients continue unchanged");
        // Stay outside the known mute window, then poll readiness. Only one demo
        // controller should run, so faults cannot overlap across rounds.
        tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(s) = query_status(&cfg.nodes[index], timeout).await {
                if s.phase == NodePhase::Ready {
                    println!(
                        "node {server} recovered: {} ms, epoch {}, server {:?}, {} results",
                        s.last_recovery_ms.unwrap_or(0),
                        s.epoch,
                        s.server,
                        s.results
                    );
                    break;
                }
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "node {server} remains unavailable/recovering; inspect survivor connectivity"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

/// Polls every node until all reachable ones report no queued or running jobs
/// on two polls in a row (the second covers a job that just finished but whose
/// `Replicate` isn't sent yet), or until `limit`. Returns the last poll.
async fn settled(cfg: &ClientConfig, limit: Duration) -> Vec<anyhow::Result<NodeStatus>> {
    const POLL: Duration = Duration::from_millis(100);
    let deadline = Instant::now() + limit;
    let mut quiet_polls = 0;
    loop {
        let statuses = query_all(cfg).await;
        let idle = statuses
            .iter()
            .flatten()
            .all(|s| s.queue_len == 0 && s.running == 0);
        quiet_polls = if idle { quiet_polls + 1 } else { 0 };
        if quiet_polls >= 2 {
            return statuses;
        }
        if Instant::now() >= deadline {
            println!("not settled after {limit:?}: jobs still queued or running; comparing anyway");
            return statuses;
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn print_status(cfg: &ClientConfig, settle: Option<Duration>) -> anyhow::Result<()> {
    let statuses = match settle {
        Some(limit) => settled(cfg, limit).await,
        None => query_all(cfg).await,
    };
    let mut digests = Vec::new();
    for (addr, status) in cfg.nodes.iter().zip(statuses) {
        match status {
            Ok(s) => {
                println!(
                    "{addr}: node {} epoch {} server {:?} requests {} results {} digest {:016x} queue {} running {} pending_cost {} conflicts {} dup_replicates {} replans {} overcommits {} peer_drops {} last_seen {:?}",
                    s.node_id, s.epoch, s.server, s.requests, s.results, s.results_digest, s.queue_len, s.running,
                    s.pending_cost, s.replicate_conflicts, s.duplicate_replicates, s.local_replans,
                    s.resource_overcommits, s.peer_queue_drops, s.peer_last_seen_ms
                );
                println!(
                    "    phase {:?} faults {} last_recovery_ms {:?}",
                    s.phase, s.faults_injected, s.last_recovery_ms
                );
                println!(
                    "    degraded {} elections decided {} alone {} splits {} step_downs {} suppressed_triggers {} dropped_on_step_down {} late_results {}",
                    s.degraded, s.elections_decided, s.elections_alone, s.split_resolutions, s.step_downs,
                    s.suppressed_triggers,
                    s.dropped_on_step_down, s.late_results
                );
                println!("    sent {:?}\n    received {:?}", s.sent, s.received);
                digests.push((s.results, s.results_digest));
            }
            Err(e) => println!("{addr}: unreachable ({e})"),
        }
    }
    if digests.len() > 1 {
        let same = digests.windows(2).all(|w| w[0] == w[1]);
        println!(
            "results tables {}",
            if same {
                "IDENTICAL on all reachable nodes"
            } else {
                "DIFFER"
            }
        );
    }
    anyhow::ensure!(
        digests.len() == cfg.nodes.len(),
        "some configured nodes did not answer UDP Status"
    );
    anyhow::ensure!(
        digests.windows(2).all(|w| w[0] == w[1]),
        "results tables have not converged"
    );
    Ok(())
}
