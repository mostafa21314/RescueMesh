# RescueMesh — Phase 1, Part 1: Three-node cloud + Incident Processing Service

This milestone implements the first Phase 1 bullet: **the three-node cloud and the
Incident Processing and Coordination Service**. The load-aware election from the
Phase 0 survey (bullet 2) plugs into a single, marked interface (`src/assign.rs`);
until then a deterministic, load-blind placeholder picks the executor.

## Build and run

```bash
cargo build --release
cargo test --release --lib          # planner unit tests

# all three nodes on one machine
scripts/run_local.sh                 # ports 7001-7003, logs in logs/
target/release/rm-client status      # per-node state + replica consistency check
target/release/rm-client load --concurrency 8 --requests 500 --out run.csv
scripts/stop_local.sh
```

On the three cloud machines (example IPs):

```bash
# machine A
rm-node --id 1 --listen 0.0.0.0:7001 --peer 2@10.0.0.2:7001 --peer 3@10.0.0.3:7001
# machine B
rm-node --id 2 --listen 0.0.0.0:7001 --peer 1@10.0.0.1:7001 --peer 3@10.0.0.3:7001
# machine C
rm-node --id 3 --listen 0.0.0.0:7001 --peer 1@10.0.0.1:7001 --peer 2@10.0.0.2:7001
# any client machine
rm-client --nodes 10.0.0.1:7001,10.0.0.2:7001,10.0.0.3:7001 load --concurrency 32 --requests 1000
```

`--seed`, `--campus-side`, `--responders` and `--resources` must match on all nodes
(they generate the identical synthetic campus locally).

## Architecture

```
            client (rm-client / later the client middleware)
          Submit{request_id, incident}  multicast to all three
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
     ┌─────────┐    ┌─────────┐    ┌─────────┐
     │ node 1  │◄──►│ node 2  │◄──►│ node 3  │   full mesh of peer links:
     └─────────┘    └─────────┘    └─────────┘   Heartbeat(load), Replicate(op)
          │  each node: same request → Assigner.choose(request_id, live set)
          │  executor plans, commits, replicates, replies; others record pending
          ▼
     PlanResult (only from the executor)
```

| Source file | Responsibility |
|---|---|
| `model.rs` | Incident, Resource, Responder, Assignment, ResponsePlan (fields per the brief) |
| `campus.rs` | Seeded synthetic campus: 12×12 zone graph, 160 responders, 120 resource offers |
| `planner.rs` | Priority + joint optimal assignment + sensitivity analysis + route |
| `state.rs` | Replicated state, idempotent op application, convergence digest |
| `assign.rs` | **Election swap-in point** (`Assigner` trait); `HashAssigner` placeholder |
| `node.rs` | Listener, peer links, heartbeats, request handling, replication |
| `proto.rs`, `wire.rs` | Message formats and framing |
| `bin/rm-node.rs`, `bin/rm-client.rs` | Node process and test/load client |

## Request lifecycle

1. The client multicasts `Submit{request_id, client_id, incident}` to every node.
2. Each node checks its idempotency cache. If the request is already committed, only
   the node that executed it re-sends the cached plan (`duplicate: true`).
3. Each node records the incident and calls `assigner.choose(request_id, live)`, where
   `live` is itself plus every peer heard from within `--peer-timeout-ms` (1500 ms).
   All nodes with the same live view pick the same executor.
4. Non-executors store the request in `pending` (input for failover detection later).
5. The executor waits for a planner worker, takes a snapshot of state, runs the
   planner on the blocking pool, applies `PlanCommitted` locally, broadcasts it to
   peers as `Replicate`, and replies `PlanResult`.
6. Peers apply the op (idempotent by `op_id = "plan:<request_id>"`), which also
   clears their `pending` entry.

## The planning computation

Required inputs per the brief — severity, age, location/distance, resource
availability, responder capability — all feed the decision:

* **Priority** = 0.55·severity/5 + 0.25·min(age/TTL, 1) + 0.20·scarcity, where
  scarcity is the share of responders with the required capabilities that are already
  committed.
* The target and the other active incidents are ranked; the **top-k (40)** are solved
  jointly so a low-priority incident cannot take responders a critical one needs.
* **Dijkstra** from each kept incident's zone over the campus graph.
* Cost matrix, slots × responders (severity decides 1–3 slots per incident):
  `(0.6·distance_norm + 0.3·(1 − capability_match) + 0.1·busy_norm) / (0.25 + priority)`.
  Solved optimally with the **Hungarian algorithm**, O(slots²·responders).
* **Sensitivity analysis:** re-solved 6 times with perturbed distance/capability
  weights; `stability` = share of the target's picks that survive.
* Nearest offer with free units for each required resource type; shortfalls listed.
* Route for the lead responder from the shortest-path tree.

The planner is deterministic for a given snapshot, so workloads are repeatable.
`--top-k` and `--sensitivity-rounds` tune the CPU cost per request for the
load-balancing experiments (measured 6–24 ms per plan on the test sandbox).

## Replicated state and consistency (Phase 1 scope)

State is a set of uniquely identified, immutable records. Every change is a
`ReplOp` with a unique `op_id`; applying it twice does nothing, and ops commute:

* incidents, plans and assignments are inserted by unique key (set union);
* responder commitment is `busy_until = max(old, new)`;
* resource availability is **derived** (capacity minus unexpired allocations), never
  decremented in place.

So any two replicas that have applied the same set of ops hold the same state,
regardless of order. `plans_digest` (XOR of FNV-1a hashes of plan ids) is an
order-independent fingerprint; `rm-client status` compares it across nodes.

**Known limit (by design, for later bullets/Phase 2):** two executors planning
concurrently can both allocate the last unit of a resource. Detecting and reconciling
that is the lease/reservation conflict problem the brief assigns to Phase 2.

## Message formats

Length-prefixed frames (4-byte big-endian length) carrying one JSON object; the
`type` field selects the variant. One port serves clients and peers: a connection
whose first frame is `Hello` is a peer link.

| Message | Direction | Fields |
|---|---|---|
| `Submit` | client → all nodes | `request_id`, `client_id`, `incident` |
| `PlanResult` | executor → client | `request_id`, `plan`, `duplicate` |
| `StatusQuery` / `Status` | client ↔ node | node state, load, digest, live set |
| `Error` | node → client | `request_id?`, `message` |
| `Hello` | node → peer (first frame) | `node` |
| `Heartbeat` | node → peers, every 250 ms | `node`, `load` (queue_len, inflight, workers, completed, ewma_service_ms) |
| `Replicate` | executor → peers | `from`, `op` (`PlanCommitted{op_id, incident, plan}`) |

**Timeouts and retries.** Peer liveness window 1500 ms. Peer links reconnect with
exponential backoff (100 ms → 2 s). The client retries with the **same**
`request_id` after `--timeout-ms` (default 5 s), up to `--attempts` (default 3).
**Duplicates** are suppressed at two points: the committed-plan cache, and a waiter
list for a request that is still being computed (the retry joins it instead of
starting a second computation).

## Parallelization model

**Cloud node:** one process; tokio multi-threaded runtime for all network I/O; one
task per client connection and one per received request; CPU-bound planning on the
blocking thread pool, capped by a semaphore of `--workers` permits (default: CPU
count). Requests waiting for a permit form the node's queue; `queue_len`,
`inflight` and the EWMA service time are reported in every heartbeat — these are the
load signals the election in bullet 2 will rank on. The state lock is held only to
take a snapshot or apply an op, never during planning.

**Client:** N independent workers, each a closed loop (send, wait, repeat) with its
own connection to every node. Throughput is varied by changing N. Each worker merges
replies from the three connections and matches them by `request_id`.

## Verification so far (single sandbox, 2 CPUs shared by all three nodes)

| Workers | Requests | Failed | Throughput | p50 | p95 | p99 | Executed by node 1/2/3 |
|---|---|---|---|---|---|---|---|
| 1 | 300 | 0 | 161 req/s | 6.5 ms | 9.5 ms | 10.9 ms | 101 / 99 / 100 |
| 4 | 1,200 | 0 | 221 req/s | 17.3 ms | 30.7 ms | 37.6 ms | 404 / 401 / 395 |
| 16 | 4,800 | 0 | 210 req/s | 67.5 ms | 154.5 ms | 181.4 ms | 1592 / 1586 / 1622 |

After all runs every node reported 6,301 plans, the same digest, and 0 pending.
A forced-retry run (3 ms timeout, 5 attempts each, 1,000 sends for 200 requests)
produced exactly 200 plans: no request executed twice.

These numbers are smoke tests, not the Phase 1 experiments: with three nodes on two
CPUs, throughput plateaus at the shared CPU. Real figures come from the six-machine
setup.

## Next bullets — where they plug in

| Bullet | Hook already in place |
|---|---|
| 2. Load-aware election | Replace `HashAssigner` in `assign.rs`; load reports already flow via `Heartbeat` |
| 3. Concurrency docs | Model described above; workers/queue are configurable and reported |
| 4. Fault injection | Add a "silenced" flag in `node.rs` that drops inbound/outbound frames |
| 5. Rejoin & recovery | `state.log` (ordered ops) and `pending` are the transfer/catch-up inputs |
| 6. Stress experiments | `rm-client load --out` writes per-request CSV rows |

## Design log (this milestone)

| Decision | Alternatives considered | Reason |
|---|---|---|
| TCP + length-prefixed JSON | gRPC/tonic, UDP multicast | Every mechanism stays visible in our code; JSON is easy to inspect in logs; TCP gives reliable ordered per-link delivery |
| One port for clients and peers | Separate ports | Simpler deployment; the first frame identifies the connection type |
| Commutative, idempotent ops instead of a replicated log with ordering | Raft log for all state | Per-request plans are independent records; set-union state converges without ordering. Ordering is only needed for long-lived roles (Raft, per the survey) |
| Derived resource availability | Decrement on assignment | Decrements do not commute and break convergence under concurrent executors |
| Hungarian joint assignment + sensitivity rounds | Greedy nearest responder | Meets the brief's "substantial enough to create meaningful load"; deterministic; cost is tunable |
| Seeded identical campus on every node | Transfer initial state | No bootstrap protocol needed; all data synthetic as required |
| Placeholder hash assigner | Implement election now | Keeps bullet 1 testable in isolation; one interface to replace |

## Libraries vs. our own code

Libraries: `tokio` (async runtime, TCP), `tokio-util` (length-delimited framing),
`serde`/`serde_json` (serialization), `clap` (CLI), `tracing` (logs), `rand`
(seeded synthetic data), `parking_lot` (mutex), `uuid` (client run ids).
Team-designed and implemented: the message protocol, peer mesh and liveness
tracking, executor selection, replication and idempotency scheme, convergence
digest, and the planning algorithm (priority model, Dijkstra, Hungarian solver,
sensitivity analysis).
