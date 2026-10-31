//! RescueMesh cloud node: the Incident Processing and Coordination Service.
//!
//! * `election`  – load-aware election state machine (sans-IO)
//! * `load`      – CPU sampler, service-time EWMA, load score
//! * `campus`    – seeded synthetic campus (zone graph, responders, resources)
//! * `graph`     – Dijkstra over the zone graph
//! * `hungarian` – our minimum-cost assignment solver
//! * `planner`   – incident → response plan (deterministic, CPU cost ∝ complexity)
//! * `ledger`    – which responder/resource units are committed to which request
//! * `pool`      – bounded planning queue and worker threads
//! * `tables`    – request table and results table
//! * `peers`     – on-demand peer links with reconnect and backoff
//! * `node`      – listener, request handling, replication

pub mod campus;
pub mod election;
pub mod graph;
pub mod hungarian;
pub mod ledger;
pub mod load;
pub mod node;
pub mod peers;
pub mod planner;
pub mod pool;
pub mod tables;

pub use node::{start, start_with_listener, NodeHandle};
