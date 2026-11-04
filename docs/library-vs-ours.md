# What libraries provide vs. what we built

No consensus, Raft or Paxos crate is used. Election, replication, failure detection
and conflict resolution are our code.

## Libraries

| Crate | Used for | Not used for |
|---|---|---|
| `tokio` | Async runtime, UDP sockets, timers, channels, Ctrl-C | Any protocol logic |
| `serde`, `serde_json` | Deriving and encoding messages as JSON | Framing (ours, below) |
| `toml` | Parsing config files | |
| `uuid` | Client ids (v4) and incident ids | |
| `clap` | Command-line flags | |
| `tracing`, `tracing-subscriber` | Structured logs, `RUST_LOG` filtering | |
| `anyhow`, `thiserror` | Error plumbing | |
| `sysinfo` | Reading CPU usage (whole machine or this process) for the load score's CPU term | Sampling schedule, EWMA, normalization (ours) |
| `matplotlib` (Python, outside the Rust build) | Drawing stress plots and exporting PDF/PNG reports | The new report's statistics are computed by `phase1_report.py`; run metrics by the Rust harness |
| `tokio-util` | Allowed, not used | UDP RR protocol is implemented in `common::rr` |

## Ours

| Area | Where |
|---|---|
| Wire protocol: message set, UDP sessions, ACKs, retransmission, fragmentation, ordering, duplicate suppression and size limits | `common::msg`, `common::rr`, `scripts/udp_rr.py`, docs/protocol.md |
| Seeded PRNG (SplitMix64) so every node builds the same campus on any platform | `common::rng` |
| Synthetic campus: zone graph, responders, resource offers | `cloud-node::campus` |
| Dijkstra | `cloud-node::graph` |
| Hungarian algorithm (minimum-cost rectangular assignment) | `cloud-node::hungarian` |
| Planner: priority model, travel-time scenarios, cost matrix, resource choice, route | `cloud-node::planner` |
| Worker pool: bounded queue, pending-cost accounting, dropping queued jobs | `cloud-node::pool` |
| Assignment ledger: serialized commits, deterministic conflict rule for replicated plans | `cloud-node::ledger` |
| Request table and results table, TTL sweep, order-independent digest | `cloud-node::tables` |
| Peer links: on-demand connect, reconnect with backoff, last-seen tracking, traffic counters | `cloud-node::peers`, `common::counters` |
| Request handling, deduplication of in-flight retries, replication | `cloud-node::node` |
| Client: unicast fan-out, first-response-wins, retry with the same request_id, epoch stamping | `client` |
| Load score: CPU sampler thread, EWMAs, normalization, policies, `--burn-threads` | `cloud-node::load` |
| Election: triggers, bids, 2-of-3 decision, incumbent exclusion, epochs and step-down, takeover, split-winner resolution, degraded mode, per-request mode | `cloud-node::election` (sans-IO), wired in `cloud-node::node` |
| One-result-per-request rule and ledger release for superseded results | `cloud-node::tables`, `cloud-node::ledger` |
| Stress harness: process sweep, sampler, fault timing, metrics, percentiles, Student-t confidence intervals, Jain's index | `client::stress`, `client::stats`, `client` bin `stress` |
| Control/data separation, bounded replication batches and revision-cursor state repair | `cloud-node::peers`, `cloud-node::node::anti_entropy`, `cloud-node::tables` |
| Worker priority with exponential decay and aging, bounded takeover and client-interest leases | `cloud-node::pool`, `cloud-node::node`, `common::config::SchedulingParams` |
| Retention-safe baseline validation and per-run cohort convergence | `client` bin `stress`, `common::msg::ResultsDigest`, `cloud-node::tables` |
