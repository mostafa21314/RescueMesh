# RescueMesh wire protocol (Phase 1)

Status: bullets 1 and 2 implemented (Part A: cloud and Incident Processing Service;
Part B: on-demand load-aware election, with per-request election as a comparison mode).

## Transport and framing

- TCP. One listening port per node (`listen` in its config) serves clients and peers
  alike; a node tells messages apart by their `type`, not by who connected.
- Each frame is a 4-byte big-endian body length followed by the body. The body is
  one JSON object: a `Message`, with its variant in the `type` field
  (`{"type":"Busy","request_id":{...}}`).
- A body larger than 16 MiB (`wire::MAX_FRAME`) is rejected and the connection closed.
- Every number inside a plan is an integer, so a plan survives a JSON round trip
  unchanged, and replicas holding the same plans compare equal byte for byte. Load
  scores are integers too (milli-units).

## Identifiers

| Name | Type | Meaning |
|---|---|---|
| `request_id` | `{client_id: UUID, seq: u64}` | One logical client request. Retries reuse it. |
| `attempt` | u32, starts at 1 | Incremented on every resend of the same `request_id`. |
| `epoch` | u64, starts at 0 | Period during which one node serves. Epoch 0 has no server; the first election creates epoch 1. Per-request mode stays at 0. |
| `node_id` | u32 | From the node's config. |

## Messages

### `SubmitIncident` (client → every node)

```
{ type, request_id, attempt, client_epoch, incident, complexity }
```

- `client_epoch`: highest `epoch` the client has seen in any `PlanResponse` (0 at start).
- `complexity`: planner cost knob (number of travel-time scenarios), clamped to 1..=1000.
- Receiving node, in order:
  1. If `client_epoch` is higher than its epoch (and it isn't already electing that
     epoch), it adopts `client_epoch` with the server unknown, stepping down if it
     was serving.
  2. It records the attempt in its **request table**: `first_seen`, highest
     `attempt`, the node's epoch when that attempt arrived, `completed`.
  3. If it holds a result for `request_id`, it replies with that `PlanResponse`
     (whatever the attempt number, server or not) and stops.
  4. **Election triggers** (on-demand mode, not while degraded):
     - a retry: `attempt ≥ 2`, the previous attempt was lower, the request is not
       completed, and the previous attempt arrived in the **current** epoch. Retries
       of requests whose previous attempt arrived in an older epoch never trigger.
       **Hysteresis:** if a server is known and has held the role (as this node
       knows it) for less than `min_tenure_ms`, the trigger is suppressed, counted
       (`suppressed_triggers`) and logged; the request is handled normally.
     - otherwise, if no server is known and no election is running: trigger
       `no_server` (this is how the startup election happens: on the first request).
  5. If it is the server: queue the request for planning, or reply `Busy` if the
     queue is full. A request already queued or running is not queued twice. Other
     nodes only record it.
- Per-request mode: step 4 instead starts a bid exchange for
  (`request_id`, `attempt`), unless one is already running or finished; the winner
  queues the request.

### `PlanResponse` (server, or any node holding the result → client)

```
{ type, request_id, served_by, epoch, degraded, plan }
```

- `served_by`/`epoch`: the node that computed the plan and the epoch its job was
  queued in. A node answering a retry from its results table returns the stored
  response unchanged, so `served_by` still names the original server.
- `degraded`: computed by a node serving alone (degraded mode).
- `plan`: priority (0..=1000), top-10 ranked responders, assignments (responders
  and resource units, with `created_at_ms`/`expires_at_ms`), the lead responder's
  route and its cost, shortfalls, and `planned_at_ms`.

### `Replicate` (server → both peers)

```
{ type, epoch, origin, request_id, plan, incident, degraded }
```

- Sent after the server has replied to the client (and also for late results; below).
- Role effects (on-demand mode):
  - `epoch` higher than the receiver's: the receiver adopts it with
    `server = origin`, stepping down if it was serving.
  - `epoch` equal, and no election is running at the receiver: if the receiver
    believes it is the server itself, **the lower node_id of the two keeps the
    role** and the other steps down (split winners); a bystander that believed in
    a different server takes the lower of the two. Counted (`split_resolutions`).
- Data effects: the receiver offers `PlanResponse{served_by: origin, epoch,
  degraded, plan}` to its results table, marks the request completed (creating
  the request-table entry if the client's message never arrived), and applies the
  assignments to its ledger.
- **One result per request, the same on every node:** if a node already holds a
  different result for the request, the one with the **higher epoch** wins, then the
  **lower `served_by`**. The loser's ledger holds are released. Because the rule
  depends only on the two results, replicas converge in any arrival order. An
  identical or losing `Replicate` is ignored and counted (`duplicate_replicates`).
- If a replicated plan assigns a responder that another unexpired request already
  holds, the **higher-priority incident keeps it**; equal priority goes to the
  **lower `request_id`**. Counted (`replicate_conflicts`) and logged.
- Resource units are added per request; units handed out beyond an offer's
  quantity are counted (`resource_overcommits`).
- `degraded` defaults to `false` if absent.

### `ElectBid` (node → both peers)

```
{ type, target_epoch, node_id, score, incumbent, request_id, attempt }
```

- `score`: the sender's load score, computed fresh when bidding, in milli-units
  (0..=1000, lower = less loaded). See the design log (D18, D24).
- `incumbent`: the server whose failure triggered the election, as the sender
  knows it; `None` for `no_server` elections.
- On-demand mode (`request_id` absent): an election for `target_epoch` =
  sender's epoch + 1.
  - A bid for a target the receiver already decided (≤ its epoch) is ignored.
  - A bid for a target the receiver hasn't joined makes it join: it steps down
    if it was serving (adopting `target_epoch − 1` first if it was further
    behind), sends its own bid to both peers, and starts collecting. A bid for a
    higher target replaces the election in progress.
  - Every trigger for the same target joins the one election; each node bids
    once per election. A node that rejoins an election it already bid in
    (within 10 × `bid_timeout_ms`) re-sends that same bid, so all nodes decide
    from identical bids.
- Per-request mode (`request_id`, `attempt` present, `target_epoch` = 0): the bid
  exchange for that request attempt. Keys already decided are remembered for 60 s,
  so late bids don't restart them.
- **Decision**, at every node independently: collect bids until all 3 have
  arrived or `bid_timeout_ms` has passed.
  - With ≥ 2 bids: candidates = bidders minus every incumbent named in the
    bids (unless that leaves none); winner = lowest (score, node_id). Under
    `policy = RoundRobin`: candidates sorted by node_id, index =
    hash(key) mod count, where key = target epoch (on-demand) or
    (request_id, attempt) (per-request). On-demand: epoch = target, server =
    winner, and the winner **takes over**: it queues every not-completed request in
    its request table, oldest first (even past the queue limit).
  - With only its own bid: no decision. If the node's elections have heard only
    its own bid for ≥ `degraded_after_ms`, it enters **degraded mode**: it serves
    alone, marks responses `degraded`, does not change the epoch, and doesn't run
    elections. Any message from a peer, or adopting a higher epoch, ends degraded
    mode.
- Each election is logged as one line: key (target epoch or request), trigger
  (`no_server`, `retry`, `per_request`, `peer_bid`), trigger request_id, bids
  received (node:score), incumbents, outcome (`decided`, `alone`, `degraded`,
  `abandoned`), winner, duration, messages sent.

### Step-down

When a server learns of a higher epoch (any of the cases above), or triggers a
retry election as incumbent, it **drops its queued-not-started jobs** and lets
running ones finish. A job that finishes after its node stopped serving that
epoch is a **late result**: it is stored and replicated under the
one-result rule (so all nodes converge), but not sent to the client, because the
current server answers. Counted (`late_results`).

### `Busy` (server → client)

```
{ type, request_id }
```

The planning queue (`queue_capacity` jobs not yet started) is full. The client
treats it as a reason to retry (below). In on-demand mode that retry can trigger an
election (see design log D34).

### `StatusRequest` / `Status` (diagnostic, client ↔ node)

Not part of request processing. `Status` reports epoch, server, degraded,
election counters (decided, alone, splits, step-downs, suppressed triggers, jobs
dropped on step-down, late results), table sizes, an order-independent digest of the results table,
queue length, running jobs, pending cost, conflict/duplicate/replan counters,
when each peer was last heard from, and messages and bytes per type, sent and
received. `client --status` queries every node and reports whether the digests
agree. Replication is asynchronous (reply first, replicate after), so while
requests are in flight one node can be a result ahead; `--status --settle [SECS]`
(default 10 s) first polls every 100 ms until every reachable node shows queue 0
and running 0 on two polls in a row, then compares.

## Timeouts, retries, backoff

| What | Value | Where | Rule |
|---|---|---|---|
| Client request timeout | `request_timeout_ms` = 1000 | client.toml | No `PlanResponse` within it → resend the same `request_id` to all nodes with `attempt + 1`. To be re-set to ~2× the measured p99 normal latency. |
| Client attempts | `max_attempts` = 5 | client.toml | After the last attempt times out, the request is reported unanswered. |
| Busy pause | min(50 ms × 2^(attempt−1), request timeout) + 0–25 % jitter | `client::busy_pause` | On `Busy` for attempt *n*, wait this long, then resend with attempt *n* + 1 (it counts as an attempt, so a persistently overloaded server can still be replaced, subject to `min_tenure_ms`). |
| Client connect timeout | = request timeout | client | A node that doesn't accept in time is skipped for this attempt. |
| Client reconnect backoff | 100 ms doubling to 2 s | `client::BACKOFF_*` | While a node is in backoff, frames for it are dropped (counted as `unsent`), not queued: the retry rule covers them, and a dead node never delays the live ones. |
| Bid timeout | `bid_timeout_ms` = 100 | node.toml | Decide when all 3 bids have arrived or this passes. |
| Degraded after | `degraded_after_ms` = 3000 | node.toml | Elections that heard only the node's own bid for this long → serve alone. |
| Minimum tenure | `min_tenure_ms` = 1000 | node.toml | A retry triggers an election only after the current server has held the role this long (= the client's request timeout; 0 disables). "No server known" is exempt. |
| CPU sampling | every 250 ms, EWMA α = 0.3 | `load` | Feeds the score's CPU term. |
| Peer connect timeout | 500 ms | `peers::CONNECT_TIMEOUT` | |
| Peer reconnect backoff | 100 ms doubling to 2 s | `peers::BACKOFF_*` | A peer link opens when its first message is queued; a frame whose write failed is resent after reconnecting (at-least-once while both ends are up). |
| Peer queue | 4096 messages | `peers::QUEUE_LIMIT` | Beyond it, messages to that peer are dropped and counted (`peer_queue_drops`). |
| Finished per-request keys | 60 s | `election::FINISHED_KEEP` | Late bids for them are ignored. |
| Table TTL | `table_ttl_ms` = 1 800 000 (30 min, D38) | node.toml | Request- and results-table entries older than this are removed (swept every second); a retry after that is recomputed, not answered from the table. Must be much longer than a client's full retry window (5 × 1 s). |

No heartbeats: nodes send each other nothing except `Replicate` and `ElectBid`.
A client's retry is the failure detector.

## Request lifecycle (on-demand mode)

1. The client sends `SubmitIncident` (attempt 1) to all three nodes.
2. Every node records it. If no server is known, the nodes elect one (epoch 1 at
   startup); the winner takes the recorded requests over.
3. The server queues the request. A worker plans on a snapshot of the ledger, then
   commits under the ledger lock if every chosen responder and resource unit is
   still free; otherwise it replans (after two collisions it plans while holding
   the lock).
4. The server stores the result, replies `PlanResponse` on the connection the
   request last arrived on, then sends `Replicate` to both peers.
5. Peers store the result and apply the assignments.
6. If the client times out (server crashed, overloaded, or unreachable) it resends
   with `attempt + 1`. A node holding the result answers from its results table.
   Otherwise, since the previous attempt arrived in the current epoch, the nodes
   elect a new server for the next epoch, excluding the incumbent, and the winner
   takes over every not-completed request.
