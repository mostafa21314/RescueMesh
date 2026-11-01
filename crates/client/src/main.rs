use clap::Parser;
use client::{query_status, synth_incident, Client, Outcome};
use common::config::ClientConfig;
use common::rng::Rng;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// RescueMesh client: submits synthetic incidents to every cloud node.
#[derive(Parser)]
struct Args {
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
    /// Per-request CSV log.
    #[arg(long, default_value = "client_run.csv")]
    out: PathBuf,
    /// Print each node's status and whether their results tables agree, then exit.
    #[arg(long)]
    status: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let cfg = ClientConfig::load(&args.config)?;
    if args.status {
        return print_status(&cfg).await;
    }

    let zones = cfg.campus.zones();
    let client = Arc::new(Client::new(cfg));
    let seed = args.seed.unwrap_or_else(|| client.client_id().as_u64_pair().0);
    let mut rng = Rng::new(seed);
    let reporter = format!("client-{}", &client.client_id().simple().to_string()[..8]);
    tracing::info!(client = %client.client_id(), count = args.count, rate = args.rate, complexity = args.complexity, seed, "starting run");

    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..args.count {
        if args.rate > 0.0 {
            tokio::time::sleep_until((start + Duration::from_secs_f64(i as f64 / args.rate)).into()).await;
        }
        let incident = synth_incident(&mut rng, zones, &reporter);
        let c = client.clone();
        let complexity = args.complexity;
        tasks.spawn(async move { c.submit(incident, complexity).await });
    }
    let mut outcomes: Vec<Outcome> = tasks.join_all().await;
    let wall = start.elapsed();
    outcomes.sort_by_key(|o| o.request_id.seq);

    write_csv(&args.out, &outcomes)?;
    summarize(&outcomes, wall, &client);
    tracing::info!(file = %args.out.display(), "per-request log written");
    Ok(())
}

fn write_csv(path: &PathBuf, outcomes: &[Outcome]) -> anyhow::Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    writeln!(f, "client_id,seq,sent_at_ms,response_at_ms,latency_ms,served_by,epoch,degraded,attempts,busy_replies,answered")?;
    for o in outcomes {
        let opt = |v: Option<u64>| v.map(|x| x.to_string()).unwrap_or_default();
        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{}",
            o.request_id.client_id,
            o.request_id.seq,
            o.sent_at_ms,
            opt(o.response_at_ms),
            o.latency.map(|d| format!("{:.3}", d.as_secs_f64() * 1e3)).unwrap_or_default(),
            opt(o.served_by.map(u64::from)),
            opt(o.epoch),
            o.degraded,
            o.attempts,
            o.busy_replies,
            o.answered()
        )?;
    }
    Ok(())
}

fn summarize(outcomes: &[Outcome], wall: Duration, client: &Client) {
    let answered: Vec<&Outcome> = outcomes.iter().filter(|o| o.answered()).collect();
    let mut lat: Vec<f64> = answered.iter().filter_map(|o| o.latency).map(|d| d.as_secs_f64() * 1e3).collect();
    lat.sort_by(|a, b| a.total_cmp(b));
    let pct = |p: f64| if lat.is_empty() { f64::NAN } else { lat[((lat.len() - 1) as f64 * p).round() as usize] };
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
    println!("latency ms  p50 {:.2}  p95 {:.2}  p99 {:.2}  max {:.2}", pct(0.5), pct(0.95), pct(0.99), pct(1.0));
    println!("served_by {served:?}  attempts {attempts:?}  busy replies {busy}  highest epoch {}", client.highest_epoch());
    let t = client.traffic();
    println!("sent {:?}\nreceived {:?}\nunsent frames (node down) {}", t.sent, t.received, t.unsent);
}

async fn print_status(cfg: &ClientConfig) -> anyhow::Result<()> {
    let mut digests = Vec::new();
    for addr in &cfg.nodes {
        match query_status(addr, Duration::from_millis(cfg.request_timeout_ms)).await {
            Ok(s) => {
                println!(
                    "{addr}: node {} epoch {} server {:?} requests {} results {} digest {:016x} queue {} running {} pending_cost {} conflicts {} dup_replicates {} replans {} overcommits {} peer_drops {} last_seen {:?}",
                    s.node_id, s.epoch, s.server, s.requests, s.results, s.results_digest, s.queue_len, s.running,
                    s.pending_cost, s.replicate_conflicts, s.duplicate_replicates, s.local_replans,
                    s.resource_overcommits, s.peer_queue_drops, s.peer_last_seen_ms
                );
                println!("    sent {:?}\n    received {:?}", s.sent, s.received);
                digests.push((s.results, s.results_digest));
            }
            Err(e) => println!("{addr}: unreachable ({e})"),
        }
    }
    if digests.len() > 1 {
        let same = digests.windows(2).all(|w| w[0] == w[1]);
        println!("results tables {}", if same { "IDENTICAL on all reachable nodes" } else { "DIFFER" });
    }
    Ok(())
}
