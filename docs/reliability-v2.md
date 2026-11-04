# Reliability fixes and worker priority scheduling (protocol 2)

This change addresses the failures in the first mixed-OS ZeroTier study.
Application traffic continues to use UDP Request–Reply. The `RMR1` datagram
header remains; `protocol_version = 2` identifies the new application messages.
Rebuild all servers and clients together before using the updated harness.

## Delivery and repair

Each peer now has two independent UDP sessions: one for control messages and
one for replication. Election bids no longer wait behind the replication
backlog. The replication sender groups up to 64 updates into a bounded batch,
amortizing the acknowledgement round trip. Both queues remain bounded at
4,096 messages; overflow counters distinguish control from replication.

A separate incremental repair task polls each ready peer every 250 ms. Each
node maintains a monotonic result revision index, with one latest revision per
retained result. Replies contain at most 64 records, normally within 256 KiB.
The requester installs pages idempotently and advances its cursor afterward.
Queue overflow therefore does not permanently erase a result that remains in
another node's retained tables. Updated versions propagate through the same
deterministic result preference rule as ordinary replication.

The repair cursor includes the source process's incarnation UUID. A changed
incarnation restarts that source at revision zero. Default `transfer = "delta"`
recovery now uses these validated revisions instead of relying on a recent-time
window. Pages can be installed while Recovering, with progress preserved across
transfer timeouts. Readiness still requires the configured accepted peer role
view; partially recovered startup state is not treated as an authoritative
survivor. Stable peer views also repair missed role messages while no new client
work is running.

Legacy `RecoveryRequest {since_ms}` remains available. A time-filtered snapshot
preserves older queued messages; only a full snapshot can reset that backlog.
Rejoin republishes a bounded number of immediate updates and lets journal repair
deliver the rest. Ledger reconstruction iterates retained results without
cloning a whole result table.

Short-lived UDP sessions send a best-effort CLOSE control datagram (kind 5).
The receiver drains messages already accepted before reporting EOF, and rejects
new DATA on a closed session. The idle timeout remains the fallback when CLOSE
is lost. This keeps frequent probes from occupying the session table for a minute.

## Worker scheduling with decay

Server election chooses the machine. The worker scheduler chooses the next
queued job within that machine. All server configurations now show:

```toml
[scheduling]
policy = "priority_decay"       # priority_decay | fifo
priority_boost_ms = 250
priority_half_life_ms = 1000
request_lease_ms = 10000
```

The base priority uses the existing incident priority calculation (severity,
incident age and resource scarcity), sampled at admission. With queue waiting
time `age_ms`, the next job maximizes:

```text
score = age_ms
      + priority_boost_ms * (base_priority / 1000)
        * 2 ** (-age_ms / priority_half_life_ms)
```

Urgency provides an initial advantage; that advantage halves every configured
half-life while waiting time continues to grow. With the defaults, a low-priority
job waiting over 250 ms outranks any newly arriving job. Older queued jobs
therefore cannot be postponed indefinitely by new arrivals. Equal scores use
FIFO order. Running jobs finish normally; scheduling does not preempt them.

`fifo` is available for comparison. The boost must be below the request lease,
the half-life must be positive, and the lease is bounded to 1..300,000 ms.
The defaults preserve a short urgency preference within the client's normal
retry window. Worker statistics report dequeue counts, accumulated/max queue
wait, and jobs whose admission lease expired before planning began.

Takeover uses the same queue capacity as ordinary admission. It considers
recent, live client interests rather than enqueuing an entire retained request
table. Requests that do not fit rely on normal retry/admission. Queued jobs have
a finite lease, so abandoned history does not monopolize workers indefinitely.

Clients send best-effort per-request finish notifications, normally piggybacked
on later submissions. These clear obsolete response subscriptions and queued
work. Notifications apply to the sending session, so an old session cannot
cancel a newer subscription for the same request. Session closure and the lease
provide fallbacks. Stored results remain available for duplicate requests and
repair; a completed assignment is not rolled back by a finish notification.

## Experiment baselines and retention

Before every run, the harness requires three distinct nodes using protocol 2,
matching campus/election configuration, sufficient retention, ready/idle
workers, matching role views, drained outbound queues and an agreed result
digest. The comparison excludes old records that will expire during the run's
comparison window. An unhealthy baseline is recorded and stops the experiment.

Result retention uses the origin-selected expiry deadline, carried through
replication and repair, rather than restarting the TTL on every receiver.
Post-run convergence compares only the current run's client UUIDs, requires
equal counts/digests and matching ready role views, and includes at least every
answered request. Old records expiring during a long sweep no longer invalidate
this current-run comparison. The harness rejects a retention interval too short
for the workload, settling and retries.

By default, failed post-run convergence stops subsequent runs. To deliberately
study continued degraded operation, use `--allow-unconverged`; reports show
failed baseline counts and metadata identifies soak mode. This is distinct from
fresh, independent repetitions. Remote servers still retain history unless the
operator restarts them, or supplies an appropriate `--before-run` restart hook.

Outputs now include:

- `entry-runN.json`, `before-runN.json`, `after-runN.json`: node configuration,
  protocol/incarnation, clock samples, state comparisons, queue and repair
  counters, and worker statistics;
- merged `requests.csv`: client UUID and elapsed time for failed requests;
- `samples.csv`: separate outgoing control/replication queue depths and repair lag;
- `runs.csv`/`reliability.csv`: baseline validity, convergence scope, repairs,
  split drops, selected load node, expired jobs and worker queue waiting time;
- report figures for outgoing queues and repair progress, plus baseline status
  and failure duration diagnostics in the tables.

The role launcher writes server events to `logs/nodeN.log` and the terminal.
For a native invocation use `--log-file logs/nodeN.log`. Preserve the logs from
all three PCs alongside the controller's result directory. Local owned-server
runs already collect per-node logs in their output directories.
Logging defaults to `info`; an existing `RUST_LOG` environment variable controls
the level. Use `RUST_LOG=info` when collecting experiment logs.

## Update the three-server setup

Stop the old server/client processes, copy the updated project to all four PCs,
then build on each machine:

```text
cargo build --release
```

The source-only deployment bundle is `dist/reliability-v2-source.zip`. Extract
it into a new directory on each PC and build there. It includes `config/site`
with the shared addresses, but no old results or binaries. Windows validation
binaries are staged separately in `target/reliability-v2/release`; the ordinary
`target/release` folder may still contain the previous build until rebuilt.

Keep the shared ZeroTier addresses in `config/site`. The existing addresses and
election policy remain suitable. On Windows Node 1:

```text
python scripts/phase1.py server --node 1 --config-dir config/site
```

On Ubuntu Nodes 2 and 3, respectively:

```bash
./target/release/cloud-node --config config/site/node2.toml --workers 0 --log-file logs/node2.log
./target/release/cloud-node --config config/site/node3.toml --workers 0 --log-file logs/node3.log
```

On Client 1, activate the plotting environment and check all nodes:

```bash
source .venv/bin/activate
python scripts/phase1.py control status --config-dir config/site
```

Each status must show `protocol_version: 2`. Begin with separate short cases and
new output directories, for example:

```bash
python scripts/phase1.py test --config-dir config/site --scenario normal --clients 4,8 --rate 30 --duration 10 --reps 2 --out results/phase1/v2-normal
python scripts/phase1.py test --config-dir config/site --scenario imbalance --load-target follower --load-threads 12 --clients 4 --duration 10 --reps 2 --out results/phase1/v2-follower
python scripts/phase1.py test --config-dir config/site --scenario imbalance --load-target incumbent --load-threads 12 --clients 4 --duration 10 --reps 2 --out results/phase1/v2-incumbent
python scripts/phase1.py test --config-dir config/site --scenario faulty --clients 4 --duration 30 --reps 2 --fault-at-ms 5000 --fault-ms 20000 --out results/phase1/v2-fault
```

`fixed` (the default) uses `--load-node`; `incumbent` and `follower` select an
actual role after warm-up. The selected node is recorded in `runs.csv`. A loaded
incumbent need not be replaced if it continues meeting deadlines: election is
still on demand. The server receiving artificial load must have enough logical
CPUs for the selected thread count (maximum twice its logical CPU count).

## Validation and limits

Regression tests cover sustained traffic with 40 ms peer round trips, actual
replication queue overflow followed by repair without restarting a node, and
continued client service plus convergence through a 20-second outage on delayed
links. A separate delayed-link case loads the incumbent's CPU while checking
its control responsiveness and result convergence. Unit tests exercise priority
order, aging, FIFO comparison, revision
replacement/expiration, cohort filtering, and prompt UDP close with queued-data
delivery. Existing election, restart, resource ledger and recovery checks also run.

Windows validation on November 4, 2026:

- 94 Rust tests passed (the full 93-test suite plus the added delayed-link
  incumbent-load test); Clippy passed with warnings denied. Python controls
  passed the Rust UDP interoperability test, including fragmented status,
  load/fault controls and log capture.
- The first release-binary sweep completed 21,600/21,600 requests and converged
  in all 12 runs across normal, imbalance and fault cases. See
  [the local validation report](../results/phase1/v2-local-validation/report.html).
- Final separate incumbent/follower load checks each completed 480/480
  requests. A 20-second outage completed 3,360/3,360 requests, failed over in
  1,121 ms and recovered in 56 ms. All three runs converged.
- An intentional extreme overload (16 clients, nominal 100 requests/s each,
  complexity 1,000) completed only 22/4,800 requests. Its queue stayed within
  64, but convergence did not complete within the 20-second settling budget;
  one node's final status was unavailable. The harness saved all 4,778 failed
  elapsed times/UUIDs and stopped. This remains a failed capacity test, not
  evidence of reliable service at that load. See
  [the final checks report](../results/phase1/v2-final-checks/report.html).
- A deliberately divergent baseline was rejected before any measured run.
  Explicit soak mode recorded the failed baseline instead of calling it healthy.

Local validation is evidence for these cases. The updated version still needs
the mixed-OS ZeroTier study to measure its performance on the actual PCs.
Retained state remains in memory, client replies precede replicated commit,
and the project uses eventual reconciliation rather than a consensus protocol.
These changes improve delivery, repair and scheduling without establishing
durable exactly-once execution or partition-safe resource allocation.
