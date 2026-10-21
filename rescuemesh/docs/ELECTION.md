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
     steps). Differences inside one bucket count as ties. This resolution is a design choice, not the
     noise floor: idle, unslowed nodes on the test machine showed probe ratios of 1.5–1.9, far
     wider than one bucket. The buckets only stop a few-percent EWMA difference from deciding
     every tie the same way.
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
node**. Even when bid sets differ (Section 4), at least one node elects itself: the node `m`
holding the globally lowest bid has its own bid in its set, and the minimum of any set that
contains the global minimum is `m`. That node executes the request **unless it crashes before
committing**. Then the request is lost: a client retry within the 10 s round TTL re-uses the
decided round, which still names the dead node, and no re-election exists yet (Section 9).

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
| Bid timeout | 100 ms (`--bid-timeout-ms`) | ≫ measured election p99 (0.29–3.3 ms) and LAN RTT, so on a healthy cluster it practically never fires (0–1 in 12,000 measured decisions per run). It is the delay a request pays while a crashed peer is still inside the 1.5 s liveness window. ≪ the client's 5 s retry timeout |
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
  time-decay bring it back within ~1 s (timeline below). This also holds if the node *booted*
  slowed (`--slow-factor`): the probe baseline is then learnt slowed, and when it later drops, the
  learnt work estimate and decay floor are rescaled by `new_base/old_base` (measured: node 2 back
  to its full share within ~1 s of `--factor 1`).
* **Node down:** its peers still count it as live for up to 1.5 s and decide each round on the
  timeout (100 ms of extra latency, 2 bids per request). After that it leaves the live set and
  rounds decide on 2 bids immediately. Requests it had already won are **not** taken over: that is
  the fault-tolerance bullet. The `pending` entries record `{executor, attempt}` for that work, and
  `Bid.attempt` exists so that a takeover round (attempt 1) is fenced from attempt 0.
* **Rejoined/restarted node:** peers count as live for one liveness window after boot, so a
  fresh node waits for their bids (or the 100 ms timeout) instead of electing itself. An inbound
  `Hello` from a restarted peer also wakes our outbound link to it out of reconnect backoff. Before
  both fixes the restarted node decided alone and duplicated work. Churn check (node 3 killed at
  ~1 s, restarted at ~3 s, 6,000 requests): **218 duplicate executions before, 58 with the
  boot-liveness fix alone, 0 with both**. It seeds its work estimate from the peers' heartbeats.
  It **does win disproportionately** after a rejoin, though, for an honest reason: without state
  transfer its replica is empty, so its plans are genuinely cheaper (2.8–3.5 ms vs 4.2–4.5 ms) and
  it won ~80–90 % of requests for the rest of the run. This persists until state transfer (the
  recovery bullet) exists.
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
election duration (Submit receipt → decision) **p50 44–117 µs, p99 0.29–3.3 ms**. That is
**one election per request**, i.e. 1,150–1,600 elections/s in these runs. Planning itself takes
~4–5 ms, so the election adds ~1–2 % to the median latency.

## 6. Alternatives considered

| Alternative | Why not |
|---|---|
| Hash of the request id (bullet-1 baseline) | Free and consistent, but load-blind: a slow node keeps getting 1/3 of the work. Measured p99 is 5–7× worse when one node is slowed (below). It also remaps on any membership change |
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
| hash, balanced, c=6 | 33.6/32.9/33.4 | 5.15 | 4.82 | 7.20 | 8.62 | 10.31 | 1159 |
| **election**, balanced, c=6 | 33.5/34.2/32.3 | 4.97 | 4.78 | 6.41 | **6.90** | 9.70 | 1198 |
| hash, balanced, c=24 | 32.8/33.4/33.8 | 14.44 | 12.61 | 28.32 | 31.73 | 36.86 | 1610 |
| **election**, balanced, c=24 | 33.6/32.7/33.7 | 14.89 | 14.67 | 19.28 | **25.12** | 58.60 | 1592 |
| hash, node 2 ×4, c=6 | 33.4/33.5/33.1 | 13.68 | 4.21 | 38.66 | 43.19 | 61.84 | 435 |
| **election**, node 2 ×4, c=6 | 51.6/**0.0**/48.4 | 5.16 | 4.90 | 6.88 | **8.06** | 10.45 | 1154 |
| hash, node 2 ×4, c=24 | 33.0/33.3/33.7 | 51.46 | 4.07 | 161.52 | 178.24 | 196.59 | 448 |
| **election**, node 2 ×4, c=24 | 49.6/**0.6**/49.8 | 19.18 | 19.22 | 23.01 | **25.36** | 48.63 | 1237 |

All 24,000 requests succeeded with 0 retries, 0 duplicate executions and 0 pending. All replicas
reported the same digest after every run. (A first run before the review fixes gave the same
picture within a few ms.)

**Election cost** (measured runs, summed over nodes):

| config | elections | full-set / timeout decisions | bids/request | bid bytes/request | duration p50 / p99 / max (µs) |
|---|---|---|---|---|---|
| balanced, c=6 | 3000 | 9000 / 0 | 6.00 | 521 | 48 / 288 / 101860* |
| balanced, c=24 | 3000 | 9000 / 0 | 6.00 | 526 | 117 / 3274 / 10830 |
| node 2 ×4, c=6 | 3000 | 9000 / 0 | 6.00 | 523 | 44 / 396 / 2240 |
| node 2 ×4, c=24 | 3000 | 9000 / 0 | 6.00 | 526 | 84 / 1673 / 4228 |

(full + timeout counts one decision per node per election. \*The one 100 ms decision was in the
warm-up: a bid sent before the peer links were up. A first run had one timeout in 12,000
measured decisions, while the CPU was saturated, and it did not change the winner.)

**Load change during a run** (demo scenario 1): election, c=6, node 2 slowed ×4 from ~3 s to ~7 s.
Requests executed per node per second of send time:

| second | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
|---|---|---|---|---|---|---|---|---|---|---|
| n1 | 212 | 211 | 196 | 285 | 279 | 279 | 280 | 232 | 183 | 187 |
| n2 | 146 | 157 | 166 | **2** | **0** | **0** | **0** | 95 | 169 | 189 |
| n3 | 221 | 196 | 201 | 278 | 277 | 280 | 279 | 227 | 188 | 173 |
| mean ms | 4.14 | 4.31 | 4.35 | 4.38 | 4.43 | 4.49 | 4.43 | 4.60 | 4.89 | 4.68 |

**Is the policy fair and effective?**
* *Effective:* with a slowed node, election cuts p99 by 5–7× (178 → 25 ms at c=24, 43 → 8 ms at
  c=6) and keeps 2.6× the throughput of hash. Hash sends 1/3 of the work to the slow node, where
  requests average 146 ms. Under balanced load, election still beats hash on the tail at c=6
  (p99 6.9 vs 8.6 ms) and at p95 at c=24 (19.3 vs 28.3 ms). At c=24 its p99 is lower too
  (25.1 vs 31.7 ms), but its median is higher (14.7 vs 12.6 ms): it evens out the queues instead
  of leaving some short and some long.
* *Fair:* on equal nodes the shares are within ±1.1 percentage points of 1/3. With a slow node the
  policy is deliberately not share-fair. It equalises **per-request completion time** instead: at
  c=24 the 18 requests node 2 still won averaged 27.3 ms, against 19.1–19.2 ms on nodes 1 and 3.
  Under hash, per-node mean latency was 4.0 / 146.4 / 4.0 ms.

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
  received the losing plan.
* **No takeover when the elected executor crashes mid-request.** The request is lost, and client
  retries within the 10 s round TTL re-use the decision naming the dead node. This is the hook for
  the fault-tolerance bullet. Non-executors hold `pending{executor, attempt}`, `Bid` carries
  `attempt`, and replicated plans are deduplicated per `(request, executor, attempt)`. On executor
  failure, the nodes would open an attempt-1 round for the pending requests.
* A rejoined node with empty state wins most requests until state transfer exists (Section 4).
* The decay floor `min_work_us` is the all-time minimum work estimate. If the request mix becomes
  permanently heavier, an idle node's estimate decays toward an optimistic value. Each exploratory
  win corrects it, so the cost is about one mis-sent request per second per avoided node.
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
