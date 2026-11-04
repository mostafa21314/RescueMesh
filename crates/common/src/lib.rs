//! Types shared by the cloud nodes and the client: the synthetic data model,
//! request ids, wire messages and their framing, configuration, a seeded PRNG,
//! and per-message-type traffic counters.

pub mod config;
pub mod counters;
pub mod model;
pub mod msg;
pub mod rng;
pub mod rr;
pub mod wire;

pub use model::*;
pub use msg::*;

/// Wall-clock time in milliseconds since the Unix epoch.
pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
