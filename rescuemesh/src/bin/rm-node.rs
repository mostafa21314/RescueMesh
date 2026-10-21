use clap::Parser;
use rescuemesh::assign::AssignerKind;
use rescuemesh::node::{run, NodeConfig};
use rescuemesh::planner::PlannerConfig;

/// RescueMesh cloud node (Incident Processing and Coordination Service).
#[derive(Parser, Debug)]
struct Args {
    /// This node's id (1, 2 or 3).
    #[arg(long)]
    id: u32,
    /// Address to listen on for clients and peers.
    #[arg(long, default_value = "0.0.0.0:7001")]
    listen: String,
    /// Peer as ID@HOST:PORT; repeat for each peer.
    #[arg(long = "peer", value_parser = parse_peer)]
    peers: Vec<(u32, String)>,
    /// Planner worker threads (default: number of CPUs).
    #[arg(long)]
    workers: Option<usize>,
    #[arg(long, default_value_t = 250)]
    heartbeat_ms: u64,
    /// A peer is considered live if heard from within this window.
    #[arg(long, default_value_t = 1500)]
    peer_timeout_ms: u64,
    /// Campus/responder/resource seed. Must match on all nodes.
    #[arg(long, default_value_t = 4411)]
    seed: u64,
    /// Campus is SIDE x SIDE zones. Must match on all nodes.
    #[arg(long, default_value_t = 12)]
    campus_side: u32,
    #[arg(long, default_value_t = 160)]
    responders: usize,
    #[arg(long, default_value_t = 120)]
    resources: usize,
    /// Planner: most-urgent incidents solved jointly per request.
    #[arg(long, default_value_t = 40)]
    top_k: usize,
    /// Planner: extra perturbed re-solves per request (tunes CPU cost).
    #[arg(long, default_value_t = 6)]
    sensitivity_rounds: u32,
    /// Executor selection: load-bidding election, or the load-blind hash baseline.
    #[arg(long, value_enum, default_value_t = AssignerKind::Election)]
    assigner: AssignerKind,
    /// Election: decide on the bids received so far after this many ms.
    #[arg(long, default_value_t = 100)]
    bid_timeout_ms: u64,
    /// Election: assumed service time (ms) until the first plan completes.
    #[arg(long, default_value_t = 10.0)]
    prior_service_ms: f64,
    /// Election: CPU calibration probe period (ms).
    #[arg(long, default_value_t = 200)]
    probe_ms: u64,
    /// Emulate a CPU this many times slower (artificial load; 1 = off).
    #[arg(long, default_value_t = 1.0)]
    slow_factor: f64,
}

fn parse_peer(s: &str) -> Result<(u32, String), String> {
    let (id, addr) = s.split_once('@').ok_or("expected ID@HOST:PORT")?;
    Ok((id.parse().map_err(|_| "bad peer id")?, addr.to_string()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let a = Args::parse();
    let planner = PlannerConfig { top_k: a.top_k, sensitivity_rounds: a.sensitivity_rounds, ..Default::default() };
    run(NodeConfig {
        id: a.id,
        listen: a.listen,
        peers: a.peers,
        workers: a.workers.unwrap_or_else(num_cpus::get).max(1),
        heartbeat_ms: a.heartbeat_ms,
        peer_timeout_ms: a.peer_timeout_ms,
        seed: a.seed,
        campus_side: a.campus_side,
        responders: a.responders,
        resources: a.resources,
        planner,
        assigner: a.assigner,
        bid_timeout_ms: a.bid_timeout_ms,
        prior_service_ms: a.prior_service_ms,
        probe_ms: a.probe_ms,
        slow_factor: a.slow_factor,
    })
    .await
}
