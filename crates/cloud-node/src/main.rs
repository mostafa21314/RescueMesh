use clap::Parser;
use common::config::NodeConfig;
use std::path::PathBuf;

/// RescueMesh cloud node (Incident Processing and Coordination Service).
#[derive(Parser)]
struct Args {
    /// Node config, e.g. config/node1.toml or config/local/node1.toml.
    #[arg(long)]
    config: PathBuf,
    // TODO(Part B): --burn-threads N (external CPU load only the CPU term sees).
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let cfg = NodeConfig::load(&args.config)?;
    let node = cloud_node::start(cfg).await?;
    tokio::signal::ctrl_c().await?;
    tracing::info!(status = ?node.status(), "shutting down");
    node.shutdown();
    Ok(())
}
