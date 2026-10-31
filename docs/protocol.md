# RescueMesh wire protocol (Phase 1)

Status: **Part A implemented** (bullet 1). Election messages and rules (bullet 2) are
marked *Part B* and are not implemented yet.

## Transport and framing

- TCP. One listening port per node (`listen` in its config) serves clients and peers
  alike; a node tells messages apart by their `type`, not by who connected.
- Each frame is a 4-byte big-endian body length followed by the body. The body is
  one JSON object: a `Message`, with its variant in the `type` field
  (`{"type":"Busy","request_id":{...}}`).
- A body larger than 16 MiB (`wire::MAX_FRAME`) is rejected and the connection closed.
- Every number inside a plan is an integer, so a plan survives a JSON round trip
  unchanged, and replicas holding the same plans compare equal byte for byte.

## Identifiers

| Name | Type | Meaning |
|---|---|---|
| `request_id` | `{client_id: UUID, seq: u64}` | One logical client request. Retries reuse it. |
| `attempt` | u32, starts at 1 | Incremented on every resend of the same `request_id`. |
| `epoch` | u64 | Period during which one node serves. Part A: always `0`. |
| `node_id` | u32 | From the node's config. |

## Messages

### `SubmitIncident` (client → every node)

```
{ type, request_id, attempt, client_epoch, incident, complexity }
```

- `client_epoch`: highest `epoch` the client has seen in any `PlanResponse` (0 at start).
- `complexity`: planner cost knob (number of travel-time scenarios), clamped to 1..=1000.
- Every receiving node records the attempt in its **request table**
  (`first_seen`, highest `attempt`, the node's epoch when that attempt arrived,
  `completed`).
- If the node holds a result for `request_id`, it replies with that `PlanResponse`
  (whatever the attempt number, and whether or not it is the server).
- Otherwise only the server acts: it queues the request for planning, or joins the
  waiting list if the request is already queued or running (no second
  computation), or replies `Busy` if the queue is full. Other nodes only record it.
- *Part B*: a higher `client_epoch` makes the node adopt that epoch; a retry
  (`attempt ≥ 2`) of a not-completed request whose previous attempt arrived in the
  current epoch triggers an election.

### `PlanResponse` (server, or any node holding the result → client)

```
{ type, request_id, served_by, epoch, degraded, plan }
```

- `served_by`/`epoch`: the node that computed the plan and its epoch then. A peer
  answering a retry from its results table returns the stored response unchanged,
  so `served_by` still names the original server.
- `degraded`: *Part B* (serving alone). Always `false` in Part A.
- `plan`: priority (0..=1000), top-10 ranked responders, assignments (responders
  and resource units, with `created_at_ms`/`expires_at_ms`), lead responder's
  route and its cost, shortfalls, and `planned_at_ms`.

### `Replicate` (server → both peers)

```
{ type, epoch, origin, request_id, plan, incident }
```

- Sent after the server has replied to the client.
- The receiver stores `PlanResponse{served_by: origin, epoch, degraded: false, plan}`
  in its results table, marks the request completed (creating the request-table
  entry if the client's message never arrived), and applies the assignments to
  its ledger.
- Idempotent: a second `Replicate` for the same `request_id` is ignored and
  counted (`duplicate_replicates`).
- If a replicated plan assigns a responder that another unexpired request already
  holds, the **higher-priority incident keeps it**; equal priority goes to the
  **lower `request_id`**. The conflict is counted (`replicate_conflicts`) and
  logged. The rule depends only on the two claims, so replicas converge whatever
  order they apply plans in. In Part A only one node serves, so it can't
  happen; it becomes possible with two servers in one epoch (Part B split winners).
- Resource units are added per request without decrementing anything; units
  handed out beyond an offer's quantity are counted (`resource_overcommits`).
- *Part B*: a `Replicate` with a higher epoch makes the receiver adopt it; a
  `Replicate` from another server of the receiver's own epoch resolves split
  winners.

### `Busy` (server → client)

```
{ type, request_id }
```

The planning queue (`queue_capacity` jobs not yet started) is full. Treated by
the client as a reason to retry (below).

### `StatusRequest` / `Status` (diagnostic, client ↔ node)

Not part of request processing. `Status` reports the node's epoch and server, table
sizes, an order-independent digest of the results table, queue length, running
jobs, pending cost, conflict/duplicate/replan counters, when each peer was last
heard from, and messages and bytes sent and received per type.
`client --status` queries every node and reports whether the digests agree.

### *Part B*: `ElectBid`

`{ type, target_epoch, node_id, score, incumbent }`, sent node → both peers.
Specified in the Part B design; not implemented yet.

## Timeouts, retries, backoff

| What | Value | Where | Rule |
|---|---|---|---|
| Client request timeout | `request_timeout_ms` = 1000 | client.toml | No `PlanResponse` within it → resend the same `request_id` to all nodes with `attempt + 1`. To be re-set to ~2× the measured p99 normal latency. |
| Client attempts | `max_attempts` = 5 | client.toml | After the 5th attempt times out, the request is reported unanswered. |
| Busy pause | 20 ms × attempt | `client::BUSY_PAUSE` | On `Busy`, wait this long, then resend with `attempt + 1` (counts as an attempt). |
| Client connect timeout | = request timeout | client | A node that doesn't accept in time is skipped for this attempt. |
| Client reconnect backoff | 100 ms doubling to 2 s | `client::BACKOFF_*` | While a node is in backoff, frames for it are dropped (counted as `unsent`), not queued: the retry rule covers them, and a dead node never delays the live ones. |
| Peer connect timeout | 500 ms | `peers::CONNECT_TIMEOUT` | |
| Peer reconnect backoff | 100 ms doubling to 2 s | `peers::BACKOFF_*` | A peer link opens when its first message is queued; a frame whose write failed is resent after reconnecting (at-least-once while both ends are up). |
| Peer queue | 4096 messages | `peers::QUEUE_LIMIT` | Beyond it, messages to that peer are dropped and counted (`peer_queue_drops`). |
| Table TTL | `table_ttl_ms` = 300 000 | node.toml | Request- and results-table entries older than this are removed (swept every second). Must be much longer than a client's full retry window (5 × 1 s). |
| *Part B* bid timeout | `bid_timeout_ms` = 100 | node.toml | Collect bids until all 3 arrive or this passes. |
| *Part B* degraded after | `degraded_after_ms` = 3000 | node.toml | Elections that hear only the node's own bid for this long → serve alone, `degraded = true`. |

No heartbeats: nodes send each other nothing except `Replicate` (and, in Part B,
`ElectBid`). A client's retry is the failure detector.

## Request lifecycle (Part A)

1. The client sends `SubmitIncident` (attempt 1) to all three nodes.
2. Every node records it. The server (node 1 until Part B) queues it on its planner pool.
3. A worker plans on a snapshot of the ledger, then commits under the ledger lock if
   every chosen responder and resource unit is still free; otherwise it replans
   (after two collisions it plans while holding the lock). Commits are one at a
   time, so two workers never assign the same responder.
4. The server stores the result, replies `PlanResponse` to every waiting connection,
   then sends `Replicate` to both peers.
5. Peers store the result and apply the assignments.
6. A retry reaching any node that holds the result is answered from its results table.
