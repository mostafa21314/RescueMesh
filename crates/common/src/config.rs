//! Node and client configuration, loaded from TOML (see config/).
//!
//! Addresses are `host:port` strings and are resolved when a link connects, so
//! several nodes may share one machine and a node can move by editing its IP.

use crate::model::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Read { path: String, source: std::io::Error },
    #[error("parsing {path}: {source}")]
    Parse { path: String, source: toml::de::Error },
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerConfig {
    pub id: NodeId,
    pub addr: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElectionMode {
    OnDemand,
    PerRequest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Policy {
    Composite,
    QueueOnly,
    CpuOnly,
    RoundRobin,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Weights {
    pub pending: f64,
    pub cpu: f64,
    pub service_time: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Weights { pending: 0.5, cpu: 0.3, service_time: 0.2 }
    }
}

/// Size of the synthetic campus. Must match on every node (and the client
/// uses `zones()` to pick incident locations).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CampusParams {
    /// The zone graph is a `grid × grid` lattice plus random shortcuts.
    pub grid: u32,
    pub responders: u32,
    pub resources: u32,
}

impl Default for CampusParams {
    fn default() -> Self {
        CampusParams { grid: 16, responders: 150, resources: 100 }
    }
}

impl CampusParams {
    pub fn zones(&self) -> u32 {
        self.grid * self.grid
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: NodeId,
    /// Address to bind, e.g. "0.0.0.0:7001".
    pub listen: String,
    /// The other two nodes.
    pub peers: Vec<PeerConfig>,
    pub election_mode: ElectionMode,
    pub policy: Policy,
    #[serde(default)]
    pub weights: Weights,
    pub bid_timeout_ms: u64,
    pub degraded_after_ms: u64,
    /// Planner worker threads.
    pub workers: usize,
    /// Seed for the synthetic campus; must match on every node.
    pub seed: u64,
    /// Queued-but-not-started planning jobs before the node replies Busy.
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    /// How long request-table and results-table entries are kept.
    #[serde(default = "default_table_ttl_ms")]
    pub table_ttl_ms: u64,
    #[serde(default)]
    pub campus: CampusParams,
}

fn default_queue_capacity() -> usize {
    64
}

fn default_table_ttl_ms() -> u64 {
    300_000
}

impl NodeConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let cfg: NodeConfig = load_toml(path.as_ref())?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut ids = HashSet::from([self.node_id]);
        for p in &self.peers {
            if !ids.insert(p.id) {
                return Err(ConfigError::Invalid(format!("node id {} appears twice", p.id)));
            }
        }
        if self.workers == 0 {
            return Err(ConfigError::Invalid("workers must be at least 1".into()));
        }
        if self.queue_capacity == 0 {
            return Err(ConfigError::Invalid("queue_capacity must be at least 1".into()));
        }
        if self.campus.grid < 2 || self.campus.responders == 0 {
            return Err(ConfigError::Invalid("campus needs grid >= 2 and responders >= 1".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientConfig {
    /// `host:port` of every cloud node.
    pub nodes: Vec<String>,
    /// How long to wait for a PlanResponse before resending with attempt + 1.
    pub request_timeout_ms: u64,
    pub max_attempts: u32,
    #[serde(default)]
    pub campus: CampusParams,
}

impl ClientConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let cfg: ClientConfig = load_toml(path.as_ref())?;
        if cfg.nodes.is_empty() || cfg.max_attempts == 0 {
            return Err(ConfigError::Invalid("need at least one node and max_attempts >= 1".into()));
        }
        Ok(cfg)
    }
}

fn load_toml<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, ConfigError> {
    let shown = path.display().to_string();
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read { path: shown.clone(), source })?;
    toml::from_str(&text).map_err(|source| ConfigError::Parse { path: shown, source })
}
