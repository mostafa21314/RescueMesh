# Phase 1: Windows/Linux roles, UDP Request–Reply and reliability experiments

The current implementation uses application protocol 3. Read
[the allocation safety update](allocation-v3.md) and
[the reliability and scheduling update](reliability-v2.md) before upgrading the
three-server setup; it includes the new baseline checks and decay scheduler.
The historical UDP measurements at the end of this guide predate that update.

The current six-PC `.62–.67` layout and paired election-mode experiments are in
[the six-PC study guide](six-pc-study.md).

The application now uses **Request–Reply over UDP** for clients, cloud peers,
elections, replication, diagnostics and recovery. RR means Request–Reply here;
the load-aware election is retained. Phase 2 discovery and services are outside
this change. The project PDF supplies the Phase 1 requirements; the requested
UDP transport and selectable machine roles are the user's changes to that design.

## Choose the role of each PC

Any supported Windows or Linux PC can run a cloud peer, a client or the test
controller. Roles do not depend on CPU brand or OS. Build a native executable
for each OS/architecture: a Windows `.exe` is not a Linux executable. Different
processors can run identical source/configuration but will have different
capacity and timing. The server launcher selects 1–8 worker threads from the
logical CPU count; `--workers N` overrides it. Configured worker counts still
apply when invoking `cloud-node` directly.

Phase 1 has **three cloud peers**, IDs 1–3. The election chooses which peer
processes work; a machine started in `server` mode may therefore be a follower.
Client PCs do not participate in that election. All machines must use the same
campus seed, campus dimensions, election policy and peer addresses.

Install a native Rust toolchain and its linker, plus Python 3.11 or newer.
Build from this repository on each platform:

```text
cargo build --release
python -m pip install -r scripts/requirements-report.txt
```

On Linux use `python3` if `python` is unavailable. Python/matplotlib are needed
for the launcher and figures, not for running the Rust binaries directly. This
Windows workspace uses the GNU Rust toolchain with GCC; MSVC users need the
Visual Studio C++ build tools. Linux builds need a C toolchain, for example
`build-essential` on Ubuntu. No OS-specific CPU assignment is hardcoded.

Generate a LAN configuration once, replacing these example addresses with your
three server PCs' reachable LAN IPs:

```text
python scripts/phase1.py setup --nodes 192.168.1.10:7001 192.168.1.11:7002 192.168.1.12:7003 --directory config/site
```

Copy `config/site` to every participating PC. Each node binds all local
interfaces on its configured UDP port; clients use the advertised IPs. Setup
refuses to overwrite existing configuration. IPv6 endpoints use `[IP]:PORT`.

On the Windows 11 PC selected as node 1:

```text
python scripts/phase1.py server --node 1 --config-dir config/site
```

On the other two server PCs, Windows or Linux:

```text
python3 scripts/phase1.py server --node 2 --config-dir config/site
python3 scripts/phase1.py server --node 3 --config-dir config/site
```

Run each command on the PC that owns that node's IP. Stop with Ctrl+C. To make
a PC a client instead, run:

```text
python scripts/phase1.py control status --config-dir config/site
python scripts/phase1.py client --config-dir config/site --severity 2 --count 1000 --out client-run.csv
```

Start clients on as many PCs as needed. This launcher does not install a Windows
Service/systemd service or automatically start at boot; the server process stays
in its terminal. Native binaries are in `target/release/` (`.exe` on Windows).

## Networking

Allow inbound **UDP**, not TCP, on the server's port. In an administrator
PowerShell, the supplied `scripts/open_ports.ps1` opens UDP 7001–7003. Or scope a
rule to your lab subnet and this machine's port, for example:

```powershell
New-NetFirewallRule -DisplayName RescueMesh-UDP -Direction Inbound -Action Allow -Protocol UDP -LocalPort 7001 -RemoteAddress 192.168.1.0/24
Get-NetUDPEndpoint -LocalPort 7001
```

Linux with an enabled ufw firewall, for node 2:

```bash
sudo ufw allow from 192.168.1.0/24 to any port 7002 proto udp
ss -lunp
```

Use the actual subnet and port. Clients use ephemeral UDP source ports. All
peers must reach one another, not just the clients. Check with `control status`
or the native client's `--status --settle`; a TCP connect test cannot diagnose
this application. A successful socket bind alone does not prove LAN reachability.
Control messages are intended for a trusted lab LAN and are not authenticated.
The optional `lab_agent.py` HTTP management service still uses HTTP/TCP; the
application traffic and the new Phase 1 controls use UDP.

## Repeated stress tests and live controls

For a self-contained test on this Windows PC, with fresh owned server processes
for every repetition:

```text
python scripts/phase1.py test --local --scenario all --severity 2 --duration 30 --reps 5
```

Stop any manually started local nodes first: the harness refuses occupied ports.
Local configurations use process CPU measurements so three processes sharing a
PC can be distinguished. LAN configurations use machine CPU measurements, which
include competing applications on that server PC.

To bombard the running LAN servers, run on a client PC:

```text
python scripts/phase1.py test --config-dir config/site --scenario all --severity 3 --duration 60 --reps 5 --out results/phase1/lan-run-1
```

`all` runs normal, imbalance and faulty experiments separately. `combined`
injects both load and a fault in the same experiment. Output directories must
be new/empty to prevent accidental mixing or overwriting.

| Setting | Client processes per sweep | Requests/s per process | Planner complexity |
|---|---|---|---|
| Severity 1 | 1, 2 | 10 | 1 |
| Severity 2 | 2, 4, 8 | 20 | 3 |
| Severity 3 | 4, 8, 16 | 30 | 5 |
| Severity 4 | 8, 16, 32 | 50 | 8 |
| Severity 5 | 16, 32, 64 | 75 | 12 |

These are offered-load presets, not calibrated utilization percentages. Override
them with `--clients 1,4,16 --rate 40 --complexity 5`. For example, 16 clients at
40 requests/s offer 640 logical requests/s; every request is sent to all three
peers and retries add traffic. `--duration` controls the submission window;
draining retries and recovering can make a run take longer.

- **Normal:** ordinary traffic with no injected fault/load.
- **Imbalance:** `--load-node 1 --load-threads 2` burns CPU on one peer, starts
  before warm-up, and resets after the run. The load also has an expiry timer.
  A machine with many cores may need more threads to create noticeable imbalance
  (maximum twice its logical CPU count).
- **Faulty:** silence the elected server after `--fault-at-ms 5000` for
  `--fault-ms 10000`. The limit is 20,000 ms. Other peers take over; the returning
  node recovers before participating. The submission window must extend past
  the outage. This models temporary application unavailability, not a random
  packet-loss percentage or a power outage.
- **Combined:** both injections. Severity changes workload, while the above
  flags independently control fault duration and artificial load.

The default on-demand election retains a healthy incumbent until a retry
triggers an election. It does not continuously redistribute every request.
Consequently a normal run can legitimately have one server handling most work.
To compare assignment on every request, set `election_mode = "per_request"` on
all three nodes and use a separate output directory.

During an independently running client workload, toggle conditions from another
terminal (add `--config-dir config/site` for LAN nodes):

```text
python scripts/phase1.py control imbalance --node 1 --threads 2 --duration-ms 30000
python scripts/phase1.py control normal
python scripts/phase1.py control fault --node 2 --duration-ms 10000
python scripts/phase1.py control status
```

`normal` stops artificial CPU load on reachable nodes. An already muted node
returns when its fault timer expires; this command does not cancel an outage.
The automated harness records injection acknowledgements and convergence.

LAN tests leave independently started servers running, so their retained history
can affect subsequent repetitions. Restart all three between independent
experiments for a clean baseline. Only `--local` guarantees fresh nodes per run.
For multiple physical generators, use separate output directories and designate
one controller to inject faults. Each report measures that generator's requests;
server-wide counters include traffic from all generators. They must not simply
be summed across generators. The existing optional lab-agent recovery benchmark
can coordinate remote processes; see [lab-experiments.md](lab-experiments.md).

## Data and figures

Every test writes `experiment.json` (parameters and generator environment),
per-scenario `runs.csv`, `requests.csv`, `samples.csv`, summaries and client logs.
Local tests also keep server logs. `requests` in run summaries is the planned
denominator: failed client processes or missing CSV records cannot inflate the
success percentage. `observed_requests` shows how many records were actually read.

Reports are generated automatically; regenerate or copy collected results to
another machine and run:

```text
python scripts/phase1.py report results/phase1/lan-run-1
```

Open `report.html` locally. Outputs include `report.md`, `reliability.csv`, and
PNG/PDF figures for:

- successful-request latency histogram and empirical CDF, median/p95/p99;
- success/failure rates, missing records, retries and failed client processes;
- goodput across client counts, with 95% confidence intervals across repetitions;
- per-server completed-work shares and failover/recovery/convergence times;
- request latency, unavailable probes, queue depth and CPU over time;
- UDP fragment retransmissions, duplicate fragments, delivery timeouts, dropped
  peer messages and resource overcommit counts.

Latency distributions include successful requests only; always report failures
alongside them. Goodput uses the full workload wall time, including final retry
drain. Confidence intervals need repeated runs; one run is marked N/A. The Wilson
success interval assumes independent requests, which shared outages violate;
run-level results and confidence intervals are also provided. A 100% observed
success rate is evidence for that experiment, not a universal reliability guarantee.

The timeline explicitly selects the run with the highest p99. Synchronize LAN
clocks for resource-expiry semantics; measured request durations use the client's
monotonic clock. Record each server's CPU, RAM, OS, worker count and network
conditions separately when reporting a mixed-machine experiment.

## Does UDP affect reliability?

**Yes. UDP itself does not guarantee delivery, ordering or duplicate protection.**
Replacing TCP with bare UDP would remove those transport properties
([RFC 768](https://www.rfc-editor.org/rfc/rfc768.html)). This implementation
therefore supplies session IDs, sequence numbers, per-fragment acknowledgements,
bounded retransmission with backoff, ordered reassembly and duplicate suppression.
The existing logical request IDs, response matching, retries, replica merge rules
and recovery handle failures above that transport layer.

Messages are JSON with explicit-width, network-order headers, so mixed OS/CPU
peers use the same wire format. Datagrams are at most 1,200 bytes; large messages
are fragmented at the application layer and capped at 1 MiB. See
[protocol.md](protocol.md) for exact framing and limits. Bounded windows and
backoff limit bursts, but this lab transport does not implement TCP's mature
adaptive congestion control. It is designed for controlled LAN experiments;
Internet deployment would need more work following
[RFC 8085](https://www.rfc-editor.org/rfc/rfc8085.html).

The remaining limits matter to a reliability report:

- Retries can still expire under persistent loss, congestion, overload or multiple
  unavailable peers. UDP reliability depends on this protocol and its parameters.
- A reply is sent before replication completes. If the only holder dies then,
  an acknowledged result can be lost. There is no durable replicated commit.
- Duplicate suppression is bounded by sessions and the request-table TTL;
  retries can recompute work after expiry/restart. Do not claim exactly-once
  execution. Conflicting results converge by the project's deterministic rule.
- Election and recovery provide eventual convergence, not consensus or full
  partition safety. Degraded/split operation can overcommit resources; the
  counters expose this rather than proving it impossible.
- State is in memory. Losing every replica loses history. A returning peer with
  no reachable authoritative peer cannot establish that its empty state is current.

The earlier TCP experiment results in `results/stress` and `experiments/recovery`
remain historical evidence. They do not measure this UDP implementation. Run the
same workloads under both transports on the same machines to quantify any
performance/reliability difference; the new UDP smoke tests alone cannot do that.

## Validation on this Windows PC

The release build, all 83 Rust tests, the Python/Rust UDP control integration
test, formatting and Clippy checks passed. Tests include a 20-second temporary
outage, restart recovery, 500-request convergence, and a UDP relay that drops,
duplicates and reorders packets. Python controls receive a fragmented Rust
status response and exercise load/failure controls against actual node processes.

Two short, repeated local experiments were run with fresh servers each time:

| Experiment | Offered logical requests/s | Requests answered | Converged repetitions |
|---|---|---|---|
| Normal, imbalance, faulty; 1 and 4 clients, complexity 2 | 20, 80 | 3,600 / 3,600 (100%) | 12 / 12 |
| Combined load + fault; 4 clients, complexity 5 | 400 | 4,800 / 4,800 (100%) | 2 / 2 |
| Combined load + fault; 16 clients, complexity 5 | 1,600 | 7,085 / 19,200 (36.90%) | 2 / 2 |

The combined high-load runs recorded 47,556 Busy replies, peak queues near
4,800, 186 server UDP retransmissions and 32 transport delivery timeouts.
Both converged after clients stopped, but many clients had already exhausted
their retry budget. This demonstrates why convergence and request success are
reported separately. A takeover can enqueue retained work beyond the usual
admission queue limit, as described in the election protocol.

These use six-second submission windows and two repetitions per configuration;
they validate the harness and reveal overload behavior, not long-term service
availability. The injected outages here were two seconds; the separate automated
test exercises the 20-second requirement. Longer runs and at least five
repetitions per level are recommended for the final study.

Open the [normal/imbalance/fault report](../results/phase1/windows-udp-verified/report.html)
and [combined-load report](../results/phase1/windows-udp-combined/report.html).
The adjacent raw CSVs and metadata are retained. Linux execution and a physical
mixed-OS LAN experiment were not performed in this workspace; run the same
checks on those PCs before making cross-platform performance claims.
