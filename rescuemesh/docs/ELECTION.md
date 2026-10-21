# Distributed election for workload assignment (Phase 1, bullet 2)

**Policy:** a leaderless, per-request *load-bidding election*. Each cloud node that receives a
client's multicast `Submit` bids its **estimated completion time** for that request, and all nodes
apply the same deterministic rule to the same bids, so they elect the same executor without a
coordinator. Code: `src/election.rs` (sans-IO decision rule, round state machine, load estimator),
wired into `src/node.rs`. `rm-node --assigner hash` keeps the bullet-1 load-blind baseline for
comparison.

## 1. Algorithm

For every request `rid` (attempt 0), each node runs:

1. **Submit arrives.** If `rid` is already committed: the executor re-sends the cached plan and the
   others stay silent; no bid. Otherwise the node computes `score_us` *now* from local counters,
   records it as its own bid, and sends `Bid{rid, attempt, node, score_us}` to both peers.
2. **Collect.** The round state (`HashMap<rid, Round>`) is created by whichever comes first, the
   local Submit or a peer's Bid (a peer can be faster). Only the **first bid per node** counts:
   duplicates are ignored, and bids that arrive after the decision are only counted in metrics.
3. **Decide** once the round holds a bid from **every node in the local live set**, or when
   **`--bid-timeout-ms` (100 ms)** expires, whichever comes first. A node that never got the
   Submit never bids and never decides, since it could not execute the request anyway.
4. **Winner** = the bid with the lowest `(score_bucket(score_us), tiebreak(rid, node), node)`.
   * `score_bucket` keeps 3 mantissa bits of the integer score (an integer log scale, ~6–12 %
     steps). Differences inside one bucket are below the noise of the estimate and count as ties.
   * `tiebreak = splitmix(fnv1a(rid ‖ '#' ‖ node_be32))` gives a different pseudo-random node
     order per request, so ties (an idle cluster, equal nodes) spread evenly and do not always
     favour node 1. The finalizer is needed because FNV-1a barely mixes the last input byte into
     the high bits: without it node 1 won 50 % of ties.
5. The **winner** runs the existing execute path: it waits for a planner permit, plans, commits
   `PlanCommitted`, replicates it and replies. The **others** store a `pending` entry holding the
   elected executor and the attempt, which is the input for a later takeover. Rounds are
   garbage-collected 10 s after the decision, and orphans (peer bids without a local Submit) after
   30 s.

The rule is a pure function of the bid set, so **every node that sees the same bids elects the same
node**. Even when bid sets differ (Section 4), **at least one node always executes**: the node `m`
holding the globally lowest bid has its own bid in its set, and the minimum of any set that
contains the global minimum is `m`.

**Example message flow** (illustrative: node 2 slowed; scores in µs):

```
t=0      client ──Submit(r7)──► n1, n2, n3                     (multicast, 3 frames)
t+20µs   n1: score 6 020  ──Bid(r7,0,n1,6020)──►  n2, n3
t+25µs   n3: score 5 870  ──Bid(r7,0,n3,5870)──►  n1, n2
t+30µs   n2: score 23 700 ──Bid(r7,0,n2,23700)─►  n1, n3        (6 bid frames in total)
t+~60µs  each node now holds {n1,n2,n3}: buckets 6020→83, 5870→83, 23700→99
         n1 and n3 tie (within ~12 %); tiebreak("r7",3) < tiebreak("r7",1)  → winner n3
         n3: enqueue → plan → PlanCommitted ──Replicate──► n1, n2 ; PlanResult ──► client
         n1, n2: pending[r7] = {executor: n3, attempt: 0}; cleared when the Replicate arrives
```

## 2. Messages, timeouts, parameters

| Item | Value | Notes |
|---|---|---|
| `Bid` | `{type:"Bid", request_id, attempt, node, score_us}` | JSON in a 4-byte length-prefixed frame. ~87 B on the wire (measured: 521–526 B per request for 6 bids) |
| `Slow` / `Ack` | `{factor, secs}` / `{message}` | Admin message for load injection: `rm-client slow --node 2 --factor 4 --secs 30` |
| Bid timeout | 100 ms (`--bid-timeout-ms`) | ≫ measured election p99 (0.24–2.3 ms) and LAN RTT, so on a healthy cluster it practically never fires (1 in 12,000 measured). It is the delay a request pays while a crashed peer is still inside the 1.5 s liveness window. ≪ the client's 5 s retry timeout |
| Liveness window | 1500 ms, heartbeats every 250 ms | unchanged from bullet 1 |
| CPU probe | every 200 ms (`--probe-ms`), median of 3 × ~0.3 ms loops | ~0.5 % of one core |
| Service prior | 10 ms (`--prior-service-ms`), replaced by peers' measured work per request (from heartbeats) until the node's first completion | |
| Round GC | decided: 10 s; orphan: 30 s; sweep every 1 s | bounds memory to ~10 s of requests |

## 3. Load metric and its justification

```
score_us = S_est × (1 + (queued + inflight + reserved) / workers)
S_est    = work_ewma × cpu_ratio
```

* **What it estimates:** the completion time of *one more* request at this node, i.e. its own
  service time plus the queueing delay behind the requests it already owns, shared over its
  `workers` planner threads. The policy is *minimum expected completion time*, the quantity the
  client actually experiences. It is strictly increasing in every outstanding request, so a burst
  spreads over the nodes instead of piling onto the momentarily lightest one.
* **`reserved`** = requests this node has *won but not yet enqueued*. Without it, rounds decided
  back-to-back would all see the old load and herd onto the same node.
* **`work_ewma`**: EWMA (α = 0.2) of the measured planner wall time, divided by `cpu_ratio` at the
  moment of the sample. This is the work per request in baseline-CPU units.
* **`cpu_ratio`** = median of the last 5 probes ÷ the lowest such median seen. The probe times a
  fixed CPU loop, so it measures how fast this node's CPU is *right now*, including contention from
  other processes. It moves even when the node completes nothing. That is what keeps an avoided,
  once-slow node from starving: a completion-driven EWMA freezes at the slow value forever. The
  rolling median ignores single outliers (on the test machine's 4 performance + 6 efficiency cores,
  a lone probe can land on a slow core).
* **Time decay:** samples that complete during the probe's ~0.4 s reaction lag inflate
  `work_ewma`. So after 1 s without a completion, the work estimate decays (half-life 1 s) toward
  the lowest work estimate the node has had, and the next request it wins re-measures it. In
  practice this sends about one exploratory request per second to an avoided node.
* **Counters are exact.** `queued` and `inflight` are RAII gauges, decremented on every exit path,
  and a planner panic (`JoinError`) is handled: waiters get an `Error` and are removed. This
  replaces the bullet-1 `.expect`, which leaked `inflight` and `waiters`, so the metric cannot
  drift.
* **Freshness:** the bid is computed from local counters at Submit time and travels with the
  request. Election never uses the 250 ms-old heartbeat loads, which only seed the cold-start
  prior.

**Factors considered and not used.** Planning is CPU-bound: it reads an in-memory snapshot and
does no disk I/O, and the clients sit on the same LAN.

| Factor | Decision | Reason |
|---|---|---|
| Queue length, recent service time | **used** | Directly determine completion time (the formula above) |
| CPU utilisation | **used indirectly** via the probe | Raw CPU % saturates at 100 % and cannot tell a queue of 1 from a queue of 20. On the one-machine testbed it is identical for all three nodes. The probe measures what CPU load *does* to this node's speed |
| RTT / delay to the client | not used | All nodes get the same multicast over the same LAN. Measured node↔node bid exchange is p50 ≈ 50–120 µs against ~5 ms of service time, below the ~6–12 % score resolution. Would be added as `+ rtt_us` to the score for WAN clients |
| Reliability with the client | implicit | A node that does not receive the Submit does not bid and cannot win |
| I/O bandwidth | not used | No disk I/O. Per request each node sends one reply or replicate frame and two 87 B bids, far below LAN capacity |

## 4. Behaviour under slow, down and rejoining nodes

* **Slow node** (`rm-client slow`, or real CPU contention on its machine): its planner samples
  and probes slow down, `S_est` rises within ~0.4 s, and it stops winning except when the others'
  queues make their completion times equally bad. Once the slowdown ends, the probe and the
  time-decay bring it back within 1–2 s (timeline below).
* **Node down:** its peers still count it as live for up to 1.5 s and decide each round on the
  timeout (100 ms of extra latency, 2 bids per request). After that it leaves the live set and
  rounds decide on 2 bids immediately. Requests it had already won are **not** taken over: that is
  the fault-tolerance bullet. The `pending` entries record `{executor, attempt}` for that work, and
  `Bid.attempt` exists so that a takeover round (attempt 1) is fenced from attempt 0.
* **Rejoined/restarted node:** it starts with empty counters and seeds its work estimate from the
  peers' heartbeats, so it is neither starved by a high prior nor flooded by a zero one. Its
  `reserved` and queue counters make it take its share without herding.
* **Inconsistent views** (a peer's bid arrives at one node before its timeout and at another
  after, or the live sets differ): two nodes can both elect themselves and both execute. This is
  made **harmless and convergent**. When a replica receives a second `PlanCommitted` for the same
  request, it keeps the plan with the lowest `(attempt, tiebreak(rid, executed_by))` whatever the
  arrival order, and swaps the loser's assignments out. Responder commitments are now derived from
  the kept plans (busy-until = latest unexpired commitment) instead of `max()`ed in place, so the
  swap is exact. The count appears as `duplicate_executions`, and `plans_digest` now hashes
  `request_id@executed_by`, so a replica that kept a different executor's plan shows a different
  digest in `rm-client status`. A unit test checks that both arrival orders give identical plans,
  digest, assignments and responder state.

## 5. Message complexity and cost

Per request with n nodes: **n(n−1) bid messages = 6 for n = 3**, sent in one parallel round and
always exactly that many (Bully/Ring-style leader elections cost O(n²) or O(n) *per leader
change*, not per request). Measured on the bid path: **6.00 bids/request, ~525 B/request**,
election duration (Submit receipt → decision) **p50 47–123 µs, p99 0.24–2.3 ms**. That is
**one election per request**, i.e. 1,100–1,500 elections/s in these runs. Planning itself takes
~4–5 ms, so the election adds ~1–2 % to the median latency.

## 6. Alternatives considered

| Alternative | Why not |
|---|---|
| Hash of the request id (bullet-1 baseline) | Free and consistent, but load-blind: a slow node keeps getting 1/3 of the work. Measured p99 is 6× worse when one node is slowed (below). It also remaps on any membership change |
| Each node picks the least-loaded node from heartbeat loads (no bids) | Loads are up to 250 ms stale, and each node's copy is of a different age, so nodes disagree. All requests within one heartbeat interval also herd onto the same "least loaded" node |
| Coordinator or leader decides, e.g. the Raft-style coordinator of teammate PR #2 | A single decision means no disagreement. But it adds an extra hop on every request, makes the leader a per-request bottleneck, and causes an election outage whenever the leader fails. PR #2 was reviewed and not integrated; its ideas were reused (credit below) |
| Bully / Ring | They elect a *long-lived leader* after a failure and say nothing about load. Used per request they degenerate into the coordinator design plus O(n²)/O(n) messages |

## 7. Measured evidence

**Testbed:** one Apple M5 laptop (4 performance + 6 efficiency cores) running all three nodes
(`scripts/run_local.sh`, `--workers 3` each) and the closed-loop client. Planner defaults (top-k 40,
6 sensitivity rounds) take ~4–5 ms per plan. Each configuration uses a fresh cluster, a
150-request warm-up, then 3,000 measured requests. "slow2" = node 2 at `--factor 4` (Section 8).
Command: `scripts/experiment.sh`. Raw CSVs and status dumps are in `results/` (git-ignored).

| config | share n1/n2/n3 (%) | mean ms | p50 | p95 | p99 | max | req/s |
|---|---|---|---|---|---|---|---|
| hash, balanced, c=6 | 33.4/32.8/33.8 | 5.18 | 4.91 | 7.20 | 8.62 | 11.51 | 1146 |
| **election**, balanced, c=6 | 30.9/34.9/34.3 | 4.86 | 4.75 | 6.11 | **6.63** | 8.27 | 1227 |
| hash, balanced, c=24 | 33.7/33.1/33.2 | 15.16 | 13.11 | 30.94 | 35.55 | 39.87 | 1547 |
| **election**, balanced, c=24 | 33.2/33.4/33.4 | 15.70 | 15.82 | 19.60 | **22.25** | 30.23 | 1509 |
| hash, node 2 ×4, c=6 | 33.7/33.1/33.2 | 12.51 | 4.42 | 34.66 | 38.30 | 50.20 | 476 |
| **election**, node 2 ×4, c=6 | 50.9/**0.1**/49.0 | 5.23 | 5.10 | 6.47 | **7.25** | 22.93 | 1132 |
| hash, node 2 ×4, c=24 | 33.4/33.2/33.4 | 48.99 | 4.53 | 151.59 | 169.10 | 197.44 | 472 |
| **election**, node 2 ×4, c=24 | 49.3/**1.1**/49.6 | 19.71 | 19.69 | 23.84 | **27.34** | 31.99 | 1207 |

All 24,000 requests succeeded with 0 retries, 0 duplicate executions and 0 pending. All replicas
reported the same digest after every run.

**Election cost** (measured runs, summed over nodes):

| config | elections | full-set / timeout decisions | bids/request | bid bytes/request | duration p50 / p99 / max (µs) |
|---|---|---|---|---|---|
| balanced, c=6 | 3000 | 9000 / 0 | 6.00 | 521 | 49 / 242 / 1532 |
| balanced, c=24 | 3000 | 8999 / 1 | 6.00 | 526 | 123 / 2306 / 101257 |
| node 2 ×4, c=6 | 3000 | 9000 / 0 | 6.00 | 523 | 47 / 272 / 1715 |
| node 2 ×4, c=24 | 3000 | 9000 / 0 | 6.00 | 526 | 78 / 1921 / 5812 |

(full + timeout counts one decision per node per election. The single timeout was one bid that
arrived >100 ms late while the CPU was saturated. It did not change the winner.)

**Load change during a run** (demo scenario 1): election, c=6, node 2 slowed ×4 from ~3 s to ~7 s.
Requests executed per node per second of send time:

| second | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
|---|---|---|---|---|---|---|---|---|---|---|
| n1 | 211 | 223 | 194 | 265 | 282 | 265 | 290 | 232 | 169 | 181 |
| n2 | 189 | 215 | 185 | **1** | **0** | **0** | **0** | 93 | 197 | 208 |
| n3 | 191 | 142 | 185 | 302 | 288 | 291 | 272 | 225 | 184 | 167 |
| mean ms | 4.10 | 4.27 | 4.57 | 4.42 | 4.42 | 4.58 | 4.58 | 4.81 | 4.83 | 4.66 |

**Is the policy fair and effective?**
* *Effective:* with a slowed node, election cuts p99 6× (169 → 27 ms at c=24, 38 → 7 ms at c=6)
  and keeps 2.5× the throughput of hash. Hash sends 1/3 of the work to the slow node, where
  requests average 139 ms. Under balanced load, election still beats hash on the tail (p99 22 vs
  36 ms at c=24) because it steers around transient queues that hash collides into. The median at
  c=24 is slightly higher (15.8 vs 13.1 ms) because election evens out the queues instead of
  leaving some short and some long.
* *Fair:* on equal nodes the shares are within ±2.5 percentage points of 1/3 (c=24: 33.2/33.4/33.4,
  c=6: 30.9/34.9/34.3). With a slow node the policy is deliberately not share-fair. It equalises
  **per-request completion time** instead: at c=24 the 1.1 % of requests node 2 still won averaged
  22.4 ms, against 19.7 ms on nodes 1 and 3. Under hash, per-node mean latency was 4.4 / 138.6 /
  4.3 ms.

## 8. Load injection (demo scenario 2)

`rm-client slow --node N --factor F --secs S` (or `rm-node --slow-factor F`) makes node N emulate a
CPU F× slower. After any timed CPU work, a plan or a probe, that took t, the thread sleeps (F−1)·t
while still holding its planner permit. The election only sees this through real measurement:
longer service samples and slower probes. The score is never touched. Sleep is used instead of
spinning so that, on a one-machine testbed, the injected load does not also slow the co-located
nodes (an external CPU hog would load all three equally). On separate machines, a real hog such as
`stress` is also picked up, through the probe and the service times.

## 9. Limitations

* Agreement needs the same live view, and every bid must arrive before the timeout. Otherwise a
  duplicate execution is possible: it is detected, counted and convergent, but the client may have
  received the losing plan. There is at-least-once execution among nodes that got the Submit, but
  no takeover yet if the elected executor crashes.
* Rounds that are open concurrently see the same load (`reserved` only covers won-not-enqueued
  work). Near-simultaneous requests can therefore still cluster, bounded by the ~0.1 ms bid
  window.
* The estimator assumes requests are statistically alike across nodes, so a node's work per
  request differs only through CPU speed. Heavier request mixes are learnt only via completions.
* Testbed caveats: all nodes share one heterogeneous CPU, the slowdown is emulated, and the
  duration percentiles include the 150 warm-up rounds.
* Concurrent executors can still over-allocate the last unit of a resource. This is the Phase 2
  lease/conflict problem and is unchanged.

## 10. Library-provided vs team-implemented

**Libraries:** `tokio` (TCP, timers, `oneshot` used to wake the waiting Submit task, blocking
pool), `serde`/`serde_json` (bid encoding), `clap` (flags), `parking_lot` (mutexes).
**Team-implemented:** the election protocol and decision rule, score bucketing and tiebreak,
round state machine and GC, the load metric (work EWMA, CPU probe, time-decay, peer prior,
reservations), panic-safe gauges, duplicate-execution convergence in `state.rs`, the slowdown
injection, the metrics and the experiment and summary scripts. **Credit:** the frozen first bid per
node, the deterministic `(score, tiebreak)` order and request+attempt fencing are ideas from
teammate MrSykoPath's unmerged PR #2 (`feature/election`). Its code is not used.
