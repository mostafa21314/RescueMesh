use clap::Parser;
use common::config::NodeConfig;
use std::path::PathBuf;

/// RescueMesh cloud node (Incident Processing and Coordination Service).
#[derive(Parser)]
struct Args {
    /// Node config, e.g. config/node1.toml or config/local/node1.toml.
    #[arg(long)]
    config: PathBuf,
    /// Start N busy-loop threads: CPU load that only the score's CPU term sees.
    #[arg(long, default_value_t = 0)]
    burn_threads: usize,
    /// Planner threads; omitted uses config, zero chooses for this machine.
    #[arg(long)]
    workers: Option<usize>,
    /// Append server events to this file as well as the terminal.
    #[arg(long)]
    log_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    if let Some(path) = &args.log_file {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        use tracing_subscriber::fmt::writer::MakeWriterExt;
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(file).and(std::io::stdout))
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    let mut cfg = NodeConfig::load(&args.config)?;
    if let Some(workers) = args.workers {
        cfg.workers = if workers == 0 {
            common::config::default_workers()
        } else {
            workers
        };
    }
    if args.burn_threads > 0 {
        tracing::warn!(threads = args.burn_threads, "burning CPU");
        cloud_node::load::burn(args.burn_threads);
    }
    let node = cloud_node::start(cfg).await?;
    tokio::signal::ctrl_c().await?;
    tracing::info!(status = ?node.status(), "shutting down");
    node.shutdown();
    Ok(())
}
