# Concurrent processing and temporary failures

## Client parallelization

Each client process uses Tokio's multithreaded asynchronous runtime. The workload
generator starts a separate task for every incident; it does not wait for one
response before submitting the next incident. `--rate` controls the offered
arrival rate, while `--rate 0` submits the whole burst immediately.

Requests share one outbound link task per cloud node. Each link serializes frames
on its own TCP connection; links to different nodes operate independently. A
dispatcher routes replies to the task waiting for that request ID. Each task has
its own timeout and attempt counter. Retries reuse the request ID, increase the
attempt number, and fan out to all nodes. A Busy response uses exponential backoff
with jitter. Responses are accepted on a first-response-wins basis.

The client therefore supports many concurrent requests without creating an OS
thread or a new connection for each one. The workload generator's task count and
outbound channels are not bounded; use a finite count and controlled rate for
experiments rather than claiming bounded memory under unlimited offered load.

## Cloud-node parallelization

Every node runs the same executable and keeps request and result tables. Tokio
handles the listener, connection tasks, peer links, completion processing, timers,
and table sweeps. Each connection has an asynchronous reader and writer. A short
state mutex serializes updates to request tables and election state.

CPU-heavy response planning runs on a fixed pool of `workers` OS threads, outside
the Tokio executor. A worker takes a ledger snapshot, computes Dijkstra/Hungarian
planning work, and checks and commits assignments under the ledger mutex. If two
workers selected the same resources, the losing worker replans. After two failed
optimistic attempts, it plans under the ledger mutex to guarantee local progress;
this fallback can temporarily serialize planning and delay ledger users.

`queue_capacity` bounds ordinary queued jobs. A full queue produces Busy; retries
already in flight are deduplicated. **Takeover deliberately bypasses this bound**
for already-recorded unfinished requests, so the total backlog is not strictly
bounded during failover. Pending-cost measurements include queued and running
work. In the default on-demand mode, planning threads on the elected server do
the work; the other two nodes replicate state and can take over. Per-request mode
can distribute simultaneous computations across several nodes.

## Election choice

Keep the project's on-demand, load-aware election for the single temporary-node
failure experiment. A client retry detects lost service; two surviving bidders
select a replacement using the existing score and node-ID tie-breaker. Minimum
tenure limits repeated elections under overload. A node's bid is now fixed for
the entire target epoch, including retries after a long delay.

This choice preserves the architecture and the professor-guided on-demand model.
Raft would be appropriate if the project were redesigned around a majority-
committed assignment log, but replacing this election alone with Raft voting
would not provide that log or solve recovery. The current protocol remains
asynchronous replication with deterministic reconciliation, not Raft consensus.

## Temporary failure and recovery

The demo controller uses the current election outcome to choose the serving node
as the failure victim. It requires all three configured nodes to be ready and to
agree on that server and epoch. Only one controller should run. For multiple
rounds, it waits for recovery before injecting the next outage.

1. Inject a duration from 1 to 20,000 ms. The node acknowledges the control
   request, then enters `muted`. New connections are closed and normal inbound
   messages and outbound responses are suppressed. Its peer links stop sending;
   envelopes from the old outbound generation are discarded.
2. Queued jobs are discarded. Running planners may finish their calculations,
   but a generation check prevents cancelled jobs from committing new effects or
   publishing results. Old election timers cannot run after the outage.
3. Ordinary clients keep using the same endpoints and retry automatically. The
   two survivors elect a replacement and take over recorded unfinished work.
4. At the end of the mute interval, the node enters `recovering`. It cannot bid,
   handle client work, or serve recovery snapshots. It can receive current
   replicas and report diagnostic status.
5. The node waits for its old planners to drain, then pulls fixed snapshots over
   dedicated connections from both ready survivors. Snapshots contain retained
   requests, attempt/epoch metadata, incidents, complexity, and completed plans.
   Transfers use chunks of at most 64 records and validate the trailer count.
   Each peer transfer has a 10-second limit and a one-million-record limit.
6. Both survivors must report the same epoch/server and no active on-demand
   election. Partial or interrupted snapshots are rejected and retried. Live
   replicas observed during recovery cannot be overwritten by an older epoch.
   Results merge by higher epoch, then lower original server ID. The node
   reconstructs the assignment ledger from the retained plans, ignoring expired
   assignments, before becoming `ready`. Missing survivor results are republished.

`Status` exposes `phase`, `faults_injected`, and `last_recovery_ms`. Recovery time
starts after the mute interval; it is separate from the configured outage time.
The node stays recovering if both survivors cannot be contacted. This is an
intentional restriction of the single-failure demonstration.

This simulates communication failure in a live process, not a power loss. State
is still in memory. Cold restart/persistent storage, arbitrary partitions,
linearizable claims, and an acknowledged replication log remain separate work.
A recovery snapshot is a point-in-time cut of each survivor, not an atomic
cluster-wide snapshot; live replication continues and table convergence is
checked after the workload settles. Retention must cover the experiment.

## Reproduce the demo

Start the local cluster with `scripts/run_local.ps1` (or `scripts/run_local.sh`).
Run this workload in a second terminal; the first requests elect a server:

```powershell
.\target\release\client.exe --config config/local/client.toml --count 2000 --rate 30 --complexity 5 --out failure_run.csv
```

While it runs, use a third terminal:

```powershell
.\target\release\client.exe --config config/local/client.toml --inject-failure --failure-ms 20000
.\target\release\client.exe --config config/local/client.toml --status --settle
```

`--fault-rounds 3` repeats sequential outages, each targeting the then-elected
server. Use a workload long enough to span all rounds. On Linux/macOS, use
`./target/release/client` instead. For the six-machine demo, use the lab config
paths and measure again; local measurements do not establish lab performance.

The workload CSV records answered/failed requests, latency, serving node, epoch,
attempts, and Busy replies. Node logs record elections, outage start, and recovery
completion. Transparent continuation means no endpoint/configuration change or
manual retry; requests can still experience the failure-detection delay, and
finite retry budgets can be exhausted under sufficiently heavy overload.

## Automated checks

```text
cargo test -p cloud-node --test temporary_failure -- --nocapture
cargo test --workspace --locked
```

The 20-second test injects failure with jobs in flight, submits requests throughout
and after the outage, requires all 127 requests to be answered, verifies normal
survivor responses during the outage, then compares result tables and active
ledger snapshots. Additional tests cover repeated follower outages, duration
bounds, and remaining in recovery when a survivor is unavailable. Regression
tests cover long-delayed bids, expired claims, and restoring displaced claims.
