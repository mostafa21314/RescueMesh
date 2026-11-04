# Six-PC comparison: on-demand and per-request

Application traffic uses protocol 2 over UDP Request–Reply. Python agents use
HTTP/TCP 7400 for process management and file collection. No dashboard is involved.

| Role | ZeroTier IP | Application port |
|---|---|---|
| Server 1 | 10.26.210.62 | UDP 7001 |
| Server 2 | 10.26.210.63 | UDP 7002 |
| Server 3 | 10.26.210.64 | UDP 7003 |
| Client PC 1 / controller | 10.26.210.65 | outgoing UDP |
| Client PC 2 | 10.26.210.66 | outgoing UDP |
| Client PC 3 | 10.26.210.67 | outgoing UDP |

These replace the old four-PC addresses. `config/study-lab.json` and
`config/site` already contain this layout. The controller can also run on a
seventh PC that reaches all six addresses.

## Prepare every PC

Copy the same updated source to **all six PCs**. The source bundle is
`dist/six-pc-study-source.zip`; extract into a new directory. Python 3.11+ is
required. From the project directory on every PC:

```bash
cargo build --release
```

The client needs the new `--start-at-ms` option. Agents reject older clients;
the controller compares source fingerprints and records binary hashes/OS/CPU.
Stop old manually started servers with Ctrl+C. Agents refuse an occupied UDP
listener and only stop their own processes. **Do not separately start servers
with `phase1.py server`: the test coordinator starts/restarts them for each run.**

If UFW is active, allow agent traffic on every PC:

```bash
sudo ufw allow from 10.26.210.0/24 to any port 7400 proto tcp
```

On Servers 1, 2 and 3 respectively, also allow their own UDP port:

```bash
sudo ufw allow from 10.26.210.0/24 to any port 7001 proto udp
sudo ufw allow from 10.26.210.0/24 to any port 7002 proto udp
sudo ufw allow from 10.26.210.0/24 to any port 7003 proto udp
```

## Start one agent on every PC

Choose one shared study token. Enter that **same value** on every PC and in the
controller terminal. This prompt avoids echoing it or storing it in shell history:

```bash
read -rsp 'Shared study token: ' RESCUEMESH_LAB_TOKEN; echo
export RESCUEMESH_LAB_TOKEN
```

Run the matching command and leave it running:

```bash
# Server 1 (.62)
python3 scripts/study_agent.py --role server --bind 10.26.210.62
# Server 2 (.63)
python3 scripts/study_agent.py --role server --bind 10.26.210.63
# Server 3 (.64)
python3 scripts/study_agent.py --role server --bind 10.26.210.64
# Client PC 1 (.65)
python3 scripts/study_agent.py --role client --bind 10.26.210.65
# Client PC 2 (.66)
python3 scripts/study_agent.py --role client --bind 10.26.210.66
# Client PC 3 (.67)
python3 scripts/study_agent.py --role client --bind 10.26.210.67
```

Agents bind only the supplied IP. Work files remain under the OS temporary
directory `rescuemesh-study-7400` and are also collected by the controller.
Leave several GB free across the controller and client PCs for the full study.
`--insecure` supports tokenless isolated loopback rehearsals.

## Controller: check and rehearse

Open a second terminal on Client PC 1 (.65), enter/export the same token, then:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r scripts/requirements-report.txt
python scripts/phase1_study.py check --lab config/study-lab.json
python scripts/phase1_study.py plan --lab config/study-lab.json
python scripts/phase1_study.py run --lab config/study-lab.json --profile smoke --out results/phase1/six-pc-smoke
```

`check` is read-only. Smoke runs both modes, both load targets, five- and
twenty-second outages and short soaks: 12 runs with one process per client PC,
30 requests/s/process and complexity 1. Fault runs include post-outage traffic.
Allow several minutes. Inspect `report.html`, `measurement_valid`, `converged`
and the logs before the long study. Windows controllers use
`.venv\Scripts\Activate.ps1` and `$env:RESCUEMESH_LAB_TOKEN` instead.

## Main comparison

```bash
python scripts/phase1_study.py run --lab config/study-lab.json --out results/phase1/six-pc-study-1
```

| Stage | Conditions | Runs |
|---|---|---|
| Normal | Both modes × rates 30, 50, 75, 100, 150, 200 × processes/PC 1, 2, 4, 8, 16; 60 seconds; 5 repetitions | 300 |
| Resilience | Both modes × up to 3 common load points × busy/idle CPU load and 5/20-second faults; 5 repetitions | up to 120 |
| Soak | Both modes at a verified common sustainable point; 30 minutes | 2 |

Total processes: 3, 6, 12, 24, 48. Cluster offered rate: 90–9,600 requests/s.
Normal schedules 33,759,000 requests. With three distinct resilience points,
there are 422 runs and at least eight hours of workload time; startup, warm-up,
settling and collection add to wall time. Deduplicated points reduce run count.

To run stages separately, use this sequence instead of the all-stage command:

```bash
python scripts/phase1_study.py run --lab config/study-lab.json --stage normal --out results/phase1/six-pc-study-1
python scripts/phase1_study.py run --lab config/study-lab.json --stage resilience --resume --out results/phase1/six-pc-study-1
python scripts/phase1_study.py run --lab config/study-lab.json --stage soak --resume --out results/phase1/six-pc-study-1
```

Resume an interrupted all-stage run by repeating its command with `--resume`.
Completed cases are skipped; incomplete attempts are preserved and retried from
fresh server state. Settings, addresses and source must match. After a source
change, rebuild/copy everywhere and use a new study directory.

## Method

- Each independent run restarts three servers, verifies empty Ready state,
  sends 15 warm-up requests at one request/s/process, verifies every reply, and
  requires idle workers, drained outgoing queues and matching results/roles.
  Warm-up uses a separate seed and is excluded from measurement.
- Mode pairs share incident seeds/reporter labels but use unique request UUIDs.
  Conditions and mode order are randomized within repetitions. Defaults are
  Composite scoring, three workers, queue capacity 64, identical priority decay
  and retries. Settings are frozen; full server configurations are saved.
- Client processes launch before a scheduled barrier. Clock-offset samples align
  timestamps; monotonic dispatch delay measures generator lag. Planned and
  achieved rates are separate metrics.
- At 15 seconds, load uses half a target's logical CPUs for 25 seconds, then is
  removed. On-demand targets the incumbent or a follower. Per-request targets
  the highest/lowest recent worker-start activity. Targets/reasons/snapshots are
  saved. Target hardware can differ between modes in these role-based tests.
- Resilience selects the lowest/highest common sustainable point and the highest
  offered point. Sustainability requires valid measurements, >=99% success and
  convergence in all normal repetitions of both modes. Duplicate points collapse.
  `--points 1:30,4:75,16:200` can explicitly choose resilience points; retain that
  option on every resume. Soak requires a verified common sustainable point.
- Failed convergence is retained. Every next independent run still requires a
  fresh healthy baseline. Infrastructure/baseline errors stop and preserve logs
  and `error.json`; resume retries incomplete cases. Soaks are labelled separately.
- Server logging defaults to `warn` equally in both modes: INFO logging for every
  per-request election can dominate the measurement. Counts, durations, bytes,
  snapshots and warnings remain available. Use INFO in a separate diagnostic study.
- A measurement-invalid run is retained. Generator-invalid means missing records,
  <95% dispatched in the scheduled interval, or dispatch p95 >100 ms. It does not
  establish server capacity at the nominal offered rate. UUID uniqueness, process
  exits and unchanged server incarnations are checked. Missing work stays in the
  availability denominator.
- Client latency uses monotonic clocks. Cross-PC windows use sampled offsets;
  `time_alignment_valid` is false above 50 ms uncertainty/observed offset change.
  Keep clocks synchronized for result/assignment expiry. Host network counters
  can include both virtual and physical interfaces.
- Final convergence checks the current run's UUID cohort and matching Ready/idle
  role views. Whole-table polling overlaps file collection; convergence time is
  an observed upper bound after clients finish. First-survivor reply requires a
  request submitted after injection and computed by a non-victim.
- Jain fairness describes distribution and is not a correctness test alone.
  Election decisions are per-node decisions, not unique elections. Resource
  conflicts/overcommits are reported separately from availability.

## Statistics and outputs

- `runs.csv`: mean, sample latency SD, p50/p95/p99/max, success/failure/missing,
  failed elapsed, goodput, achieved rate, dispatch lag, load shares, election
  overhead, queues, repair, resource conflicts, recovery, convergence and validity.
- `summary.csv`: mean, sample SD and 95% Student-t CI **between repetitions**.
  Means of run percentiles are distinguished from pooled request percentiles.
- `paired-differences.csv`: `per_request − on_demand`, with SD/CI, using valid pairs.
- `clients.csv`, `processes.csv`, `windows.csv`: per-PC/process statistics and
  before/during/after successful latency distributions and failure counts.
- `cases/.../requests.csv.gz`: merged raw requests with PC/process/UUID, original
  failed elapsed, aligned timestamps and dispatch lag. Native CSVs are separately
  preserved as `pc*-process*.csv.gz` without clock correction.
- `samples.jsonl`, `hosts.jsonl`: server probes plus six-PC CPU/memory/network
  counters. Missing probes stay missing. `entry/before/after`, clock, intervention
  and launch JSON plus compressed node/client logs preserve context for each run.
- `report.html`, `report.md`, `plots/*.png`, `plots/*.pdf`: offline tables, CDFs,
  success/p99 heatmaps, latency/goodput/election comparisons and server/host timelines.

Latency SD uses the sample `n−1` denominator within each run. Summary SD/CI use
independent repetitions. Fewer than two observations gives N/A for SD/CI where
undefined. Percentiles use nearest sorted index `round((n−1)×p)`, consistent with
the Rust client. Failed elapsed stays separate from successful latency. CDFs
use bins; exact run percentiles remain in CSVs. Reports read compact summaries
instead of loading all 33.8 million raw requests.

Regenerate reports after at least one run completes:

```bash
python scripts/phase1_study.py report --out results/phase1/six-pc-study-1
```

## Verification

After building, run the unit checks and full six-agent loopback smoke rehearsal:

```bash
python -m unittest discover -s scripts -p test_six_pc_study.py -v
```

This checks both modes, all interventions, authenticated access, existing-listener
refusal, raw statistics, convergence and resume using real binaries. One-host
orchestration verification does not replace the actual six-PC ZeroTier study.
