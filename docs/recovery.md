# Server rejoin and state recovery (Phase 1, bullet 5)

How a cloud node that was out of service gets back to a state consistent with
its peers before it takes part again: what it copies, replays, rebuilds and
throws away, and how it knows it is current. Two ways to be out of service:

| | Temporary outage (mute) | Crash and cold restart |
|---|---|---|
| Caused by | `InjectFailure` (`client --inject-failure`, `stress --fault-ms`): the spec's fault model, up to 20 s | the process stops (`kill -9`, Ctrl+C, power) and is started again (`scripts/restart_node.sh N MS` locally) |
| Memory | kept: tables, ledger, election view as they were at the mute | lost: the node starts empty |
| Path back | `muted` → `recovering` → `ready` | `starting` → `recovering` → `ready` |
| Implemented in | PR #38 (bullet 4) | this bullet: the startup decision, and recovery from whichever peers are ready |

Both use the same recovery step (`recover` in `crates/cloud-node/src/node.rs`,
transfers in `recovery.rs`). Muting itself, the demo controller and the
parallelization models are in [concurrency-and-failures.md](concurrency-and-failures.md).

## A node's state

| State | Where | After an outage |
|---|---|---|
| Request table: request id → incident, complexity, last attempt and its epoch, completed? | `tables.rs` | **copied** from peers, merged |
| Results table: request id → plan, serving node, epoch, degraded | `tables.rs` | **copied** from peers, merged by the one-result rule; results the peers lack are **replayed** to them |
| Assignment ledger: responder and resource holds with expiry | `ledger.rs` | **rebuilt** from the merged results' plans; expired holds are skipped |
| Election view: epoch, server, tenure, own bids, election in progress | `election.rs` | **installed** from the peers' agreed view (`Core::recover_view`); no election in progress, own old bids dropped |
| Queued planner jobs, in-flight set, reply routes, election timers, pending wins | `node.rs`, `pool.rs` | **discarded** (a muted node bumps its generation, so old jobs and timers can't commit; a restarted node never had them) |
| Outbound peer frames queued before the outage | `peers.rs` | **discarded**: on the muted node by its generation; on each survivor when the node pulls its snapshot (step 2) |

## The startup decision (cold restart)

A node starts in phase `Starting`: it answers `ProbeRequest`, `StatusRequest` and
`RecoveryRequest` (with `RecoveryUnavailable`) and nothing else, so it can't bid
or take client work. It probes both peers (300 ms each, in parallel):

- a peer is `Ready` and has seen an election or a request, or a peer is itself
  `Recovering` → the cluster has been running: go to `Recovering` and recover;
- otherwise (peers unreachable, starting, or ready but empty) → the whole cluster
  is starting: go to `Ready` with empty tables, as before.

So `scripts/run_local.sh` or starting all lab nodes gives a fresh cluster, while
restarting one node of a running cluster gives a rejoin. Without this, a
restarted node knew no server, triggered a `no_server` election on the first
request, and with its empty queue had the lowest bid: it could win and serve
from empty tables and an empty ledger (design log, limitation b).

## The recovery step

1. **Wait for local planners** to drain (a muted node's old jobs; their results
   are discarded by the generation check).
2. **Pull a fixed snapshot from every peer** over a dedicated connection
   (`RecoveryRequest` → `RecoveryChunk` × n, ≤ 64 records each →
   `RecoveryComplete {epoch, server, electing, records}`). The trailer's count
   must match; a partial transfer is discarded; each transfer has 10 s and
   1 000 000 records at most. Under the lock it reads the snapshot under, the
   peer also clears its outbound queue to this node (everything in it is in the
   snapshot) and wakes that link from its reconnect backoff, so frames sent after
   the snapshot reach the node at once instead of queueing behind, or being
   dropped behind, a stale backlog.
3. **Every peer must answer.** A ready peer sends a snapshot. A peer that is
   starting or recovering itself answers `RecoveryUnavailable` and is skipped:
   it holds nothing authoritative. An unreachable peer (refused, closed, timed
   out) keeps the node recovering and the step is retried every 100 ms: the
   single-failure model leaves two ready survivors, and the node must not resume
   on one arbitrary survivor's possibly stale view. At least one snapshot is
   needed.
4. **The snapshots must agree** on epoch and server, none taken during an
   election, and the server must be a configured node; otherwise retry.
5. **Not older than what it has seen live.** While recovering, the node accepts
   live `Replicate`s (merged into its tables) and remembers the highest epoch in
   them. A snapshot view older than that is refused and the pull retried.
6. **Install**, under the state lock: merge every snapshot into the tables
   (higher epoch wins, then lower serving node, the same rule as live
   replication, so the order of snapshots and replicas doesn't matter); rebuild
   the ledger from the merged plans; install the agreed election view; clear
   in-flight state; resume peer links; become `Ready`; record
   `last_recovery_ms`.
7. **Replay:** send each source peer, as `Replicate`, every result the node holds
   that the peer's snapshot lacked (results a muted server completed just before
   the outage, before replicating them).
8. If the installed view names this node as the server (it can be, when the
   survivors had not replaced it yet), it takes over the not-completed requests
   like a newly elected server.

From then on it is an ordinary node: it bids in the next election and serves if
it wins. It is current in this sense: it holds everything every ready peer held
at its snapshot, plus every replica that arrived during the transfer, and its
election view is the one all ready peers agree on outside an election. It is
not an atomic cluster-wide cut: a result replicated between a snapshot and the
install arrives as a live `Replicate` and is merged by the same rule.

## In-flight requests

Requests that were in flight at the failed node are not tracked by the cluster
as in flight; the client is the failure detector. Every request goes to all
three nodes, so the survivors already have it in their request tables. After
`request_timeout_ms` (1 s) the client resends it with the same request id and
attempt + 1; that retry triggers an election among the survivors (once the old
server's tenure reached `min_tenure_ms`), and the new server takes over every
not-completed request in its table, oldest first. A request that was completed
somewhere is answered from that node's results table. Executing a request twice
(a takeover racing a late result) is harmless: the one-result rule keeps one
plan per request on every node and releases the loser's ledger holds.

## Limits

- **Memory only.** A node that crashes loses results it computed but had not yet
  replicated (limitation c): its client got the answer, the cluster did not. A
  retry of such a request is recomputed and may get a different plan. Restarting
  all three nodes loses everything; that is a fresh cluster.
- **One failure at a time.** With one peer unreachable the node stays
  recovering (test `two_restarted_nodes_wait_for_a_ready_peer_and_skip_each_other`),
  and if every peer is starting or recovering it has no source; restarting the
  whole cluster is the way out.
- **Survivors' backlogs.** While a node is down, each survivor's link to it
  keeps up to 4096 frames and drops newer ones (limitation a). Recovery doesn't
  depend on them: the snapshot covers them, and the survivor clears the queue
  when the snapshot is taken (step 2). Before that reset, a 10 s outage at
  480 req/s nearly filled the queue, frames sent after the snapshot overflowed
  it while the link was still backing off, and the node never converged (design
  log, November 3).
- **A recovering node does no client work.** Clients keep using the other two
  through the same fan-out; it adds no capacity until ready.

## Measured

Recovery time (`last_recovery_ms`: end of the outage, or process start, to
`ready`), time out of service, failover and convergence under increasing load,
for both kinds of outage: [stress-testing.md](stress-testing.md#failures-under-load).

## Tests

| Test | What it shows |
|---|---|
| `tests/cold_restart.rs::restarted_server_rebuilds_state_from_peers_before_participating` | the server is stopped, 20 more requests are served by the survivors, the server is started again: it is `Recovering` or already holds all 60 results, never bids while recovering, ends with the survivors' epoch, server, results and ledger, then serves and converges |
| `tests/cold_restart.rs::two_restarted_nodes_wait_for_a_ready_peer_and_skip_each_other` | nodes 1 and 3 stopped; node 1 restarted alone stays recovering with nothing installed; node 3 restarted recovers from node 2, skipping node 1 (`RecoveryUnavailable`); then node 1 recovers; all three equal |
| `tests/temporary_failure.rs` (PR #38) | 20 s outage of the serving node under load; repeated follower outages; staying recovering when a survivor is gone |
| every other integration test | a fresh cluster still starts `Ready` (the startup probe finds no history) |
