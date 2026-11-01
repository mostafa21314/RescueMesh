# RescueMesh — Phase 1

CSCE 4411 Distributed Systems term project (Group 7), in Rust.

Phase 1 status:

| Bullet | State |
|---|---|
| 1. Three-node cloud + Incident Processing and Coordination Service | **done (Part A)** |
| 2. Distributed election for workload assignment, with a load metric | **done (Part B)**: on-demand, retry-triggered election; per-request mode for comparison |
| 3–6. Concurrency write-up, fault injection, recovery, stress tests | later |

## Layout

```
crates/common      data model, request ids, messages, framing, config, PRNG, counters
crates/cloud-node  the node: campus, Dijkstra, Hungarian, planner, pool, ledger, tables, peers,
                   load score, election
crates/client      client library + CLI (fan-out to all nodes, retries)
config/            lab configs (node1-3.toml, client.toml); config/local/ = all on 127.0.0.1
scripts/           run_local: start or stop 3 nodes locally; lab: set_ips, start_node,
                   open_ports (Windows), check_cluster; package.sh builds dist/<target>/
docs/              protocol.md, design-log.md, library-vs-ours.md, deployment.md
```

## Build and test

```bash
cargo build --release
cargo test            # unit tests + integration tests (500 requests, failover, per-request,
                      # --burn-threads with real processes)
```

## Run locally (three nodes on 127.0.0.1)

```bash
scripts/run_local.sh                    # builds, starts nodes on 7001-7003, logs in logs/
target/release/client --config config/local/client.toml --count 500 --rate 100 --complexity 5
target/release/client --config config/local/client.toml --status   # per-node state, tables equal?
target/release/client --config config/local/client.toml --status --settle   # same, after waiting (≤10 s) for in-flight jobs
scripts/run_local.sh stop
```

Windows: `.\scripts\run_local.ps1` and `.\scripts\run_local.ps1 stop`.

Election experiments:

```bash
BURN_NODE=1 BURN_THREADS=4 scripts/run_local.sh   # node 1 also burns 4 CPU threads
kill $(cat logs/node1.pid)                         # crash a node mid-run
grep election logs/node*.log                       # one line per election
```

Mode and policy are per-node config: `election_mode = "on_demand" | "per_request"`,
`policy = "Composite" | "QueueOnly" | "CpuOnly" | "RoundRobin"`,
`cpu_source = "machine" | "process"` (on one machine, only `"process"` tells the
nodes' CPU load apart), `min_tenure_ms` (a retry can trigger an election only
after the server has held the role this long; keep it equal to the client's
`request_timeout_ms`). All three nodes must use the same mode and policy.

Client flags: `--count` incidents, `--rate` per second (0 = all at once),
`--complexity` planner cost per request, `--seed` for repeatable incidents, `--out`
per-request CSV (default `client_run.csv`: send time, response time, latency,
served_by, epoch, attempts, Busy replies).

## Run in the lab

Step by step for each machine (Windows and Linux lab PCs, Mac and Windows client
laptops), builds, firewall, lab-day order, test sequence and troubleshooting:
[docs/deployment.md](docs/deployment.md). In short, current topology: node1 and
node2 on machine A (ports 7001, 7002), node3 on machine B (7003).

```bash
scripts/set_ips.sh A_IP B_IP [C_IP]     # every machine, same arguments (C_IP: node2 moves to C)
scripts/start_node.sh 1                 # machine A (and 2, in a second terminal); machine B: 3
scripts/check_cluster.sh                # any machine: reachable / refused / timeout per node, then --status
target/release/client --config config/client.toml --count 1000 --rate 100 --complexity 5
```

Windows: the same scripts as `.ps1`, run with
`powershell -ExecutionPolicy Bypass -File scripts\<name>.ps1 ...`;
`scripts\open_ports.ps1` (admin) opens 7001-7003 in the firewall.

`seed` and `[campus]` must match on every node: each one generates the same synthetic
campus locally. Log level: `RUST_LOG=debug` for per-request lines.

## Docs

- [docs/protocol.md](docs/protocol.md): messages, timeouts, retry rules
- [docs/design-log.md](docs/design-log.md): each decision, its alternatives, and why
- [docs/library-vs-ours.md](docs/library-vs-ours.md): what crates provide vs. our code
- [docs/deployment.md](docs/deployment.md): running on lab PCs and laptops
