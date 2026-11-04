# RescueMesh — Phase 1

CSCE 4411 Distributed Systems term project (Group 7), in Rust.

**Current transport: UDP Request–Reply.** Any Windows/Linux PC can be a server,
client or test controller. Start with the
[Windows/Linux Phase 1 guide](docs/udp-phase1.md) for machine roles, LAN setup,
normal/imbalance/fault controls, severity presets and reliability limits.

```text
cargo build --release
python -m pip install -r scripts/requirements-report.txt
python scripts/phase1.py test --local --scenario all --severity 2 --duration 30 --reps 5
```

The test writes raw CSVs and an offline HTML report with PNG/PDF figures under
`results/phase1/`. Use `server --node N` or `client` in the same launcher to
select this PC's role; add `--config-dir config/site` for a shared LAN config.
The older stress/recovery studies below describe TCP measurements and must not
be presented as measurements of the new UDP transport.

Phase 1 status:

| Bullet | State |
|---|---|
| 1. Three-node cloud + Incident Processing and Coordination Service | **done (Part A)** |
| 2. Distributed election for workload assignment, with a load metric | **done (Part B)**: on-demand, retry-triggered election; per-request mode for comparison |
| 3. Concurrent processing and parallelization write-up | **done**: async client/node I/O, fixed planner threads; see the document below |
| 4. Temporary server failures up to 20 seconds | **implemented and locally tested**: mute, survivor election, automatic pull recovery before rejoin |
| 5. Server rejoin and state recovery/convergence | **done and measured**: a node back from a temporary outage or a crash and restart pulls snapshots from its ready peers before taking part; four recovery choices (delta transfer, chunked snapshots, majority ready rule, recovery on start) compared over 170 runs ([docs/recovery.md](docs/recovery.md)) |
| 6. Stress across increasing client loads, statistically meaningful data | **done**: `stress` harness; method, results and analysis in [docs/stress-testing.md](docs/stress-testing.md) |

## Layout

```
crates/common      data model, request ids, messages, framing, config, PRNG, counters
crates/cloud-node  the node: campus, Dijkstra, Hungarian, planner, pool, ledger, tables, peers,
                   load score, election
crates/client      client library + CLI (fan-out to all nodes, retries); `stress` harness
config/            lab configs (node1-3.toml, client.toml); config/local/ = all on 127.0.0.1
scripts/           run_local: start or stop 3 nodes locally; lab: set_ips, start_node,
                   open_ports (Windows), check_cluster; package.sh builds dist/<target>/;
                   stress_local.sh and plot_stress.py: the stress experiments;
                   recovery_bench.py, recovery_report.py, lab_agent.py: the recovery experiments
results/stress/    stress results: CSVs, summary.md, node logs, plots
experiments/recovery/  recovery experiment results (summary rows, summary.md)
docs/              protocol.md, design-log.md, library-vs-ours.md, deployment.md,
                   concurrency-and-failures.md, recovery.md, stress-testing.md, lab-experiments.md
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
scripts/restart_node.sh 1 5000                     # or crash it and start it again 5 s later (rejoin)
grep election logs/node*.log                       # one line per election
```

Mode and policy are per-node config: `election_mode = "on_demand" | "per_request"`,
`policy = "Composite" | "QueueOnly" | "CpuOnly" | "RoundRobin"`,
`cpu_source = "machine" | "process"` (on one machine, only `"process"` tells the
nodes' CPU load apart), `min_tenure_ms` (a retry can trigger an election only
after the server has held the role this long; keep it equal to the client's
`request_timeout_ms`). All three nodes must use the same mode and policy.

Stress experiments (a load sweep, a mute sweep and a crash-and-restart sweep; about 50 min):

```bash
scripts/stress_local.sh                                      # results/stress/local-<date>/{load,fault,restart}
python3 scripts/plot_stress.py results/stress/local-<date>   # PNGs (needs matplotlib)
target/release/stress --help                                 # one sweep: any levels, any cluster
```

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

- [docs/concurrency-and-failures.md](docs/concurrency-and-failures.md): parallelization models, election choice, 20-second failure demo, recovery and limitations
- [docs/recovery.md](docs/recovery.md): server recovery: state inventory, protocol diagrams, decisions with alternatives, measured comparison of the variants
- [docs/lab-experiments.md](docs/lab-experiments.md): running the recovery experiments on the six lab PCs
- [docs/protocol.md](docs/protocol.md): messages, timeouts, retry rules
- [docs/design-log.md](docs/design-log.md): each decision, its alternatives, and why
- [docs/library-vs-ours.md](docs/library-vs-ours.md): what crates provide vs. our code
- [docs/deployment.md](docs/deployment.md): running on lab PCs and laptops
- [docs/recovery.md](docs/recovery.md): server rejoin and state recovery, after a mute or a cold restart
- [docs/stress-testing.md](docs/stress-testing.md): stress harness, metrics, results and analysis
