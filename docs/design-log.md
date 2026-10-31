# Design log

One entry per decision: the alternatives considered, the choice, and the reason.
Entries marked *(Part B)* are decided but not implemented yet. Values marked *to be
measured* are first guesses, to be re-set from lab measurements.

## Architecture

### D1. Three equal peers vs. a fixed primary
- **Alternatives:** a fixed primary with two backups; three equal peers, one of which
  serves per epoch.
- **Choice:** three equal peers. Every node runs the same binary, keeps the same
  tables, and can serve.
- **Reason:** a fixed primary is a single point of failure and needs its own failover
  path anyway. With equal peers, failover is just "elect someone else", and the same
  code serves in every role. Until Part B, node 1 serves as a marked placeholder
  (`node::PLACEHOLDER_SERVER`).

### D2. Unicast fan-out vs. multicast
- **Alternatives:** IP multicast to a group address; client sends to one node, which
  forwards; client sends to all three by unicast.
- **Choice:** the client opens one TCP connection per node and sends every request on
  all three.
- **Reason:** the lab network likely blocks multicast, and multicast is UDP
  (unreliable, unordered). Fan-out costs 3 small frames per request (≈350 bytes
  each, measured), and every node learns about every request directly, which the
  retry-triggered election needs: a non-server must already have seen attempt 1 to
  recognize attempt 2 as a retry. Forwarding through one node would make that node
  a single point of failure.

### D3. TCP + length-prefixed JSON
- **Alternatives:** gRPC/protobuf; a binary encoding (bincode); UDP datagrams;
  JSON over TCP.
- **Choice:** TCP, a 4-byte big-endian length, then a JSON body with a `type` field.
- **Reason:** TCP gives reliable, ordered delivery per link, so we only handle
  connection loss, not reordering. JSON can be read straight from logs and packet
  captures, which matters while we debug the election. The cost is size (a
  Replicate is ≈830 bytes, a PlanResponse ≈650) and encode time, which is small
  next to planning. A binary encoding is a drop-in change in `common::wire` if
  the experiments show encoding matters.

### D4. Our own framing instead of `tokio-util`'s codec
- **Alternatives:** `tokio_util::codec::LengthDelimitedCodec`; our own.
- **Choice:** our own `read_frame`/`write_frame` (30 lines).
- **Reason:** the codec's `Framed` stream needs `futures`' `StreamExt`/`SinkExt`
  to use, and `futures` is not on the allowed crate list. The framing is trivial,
  and owning it makes the size limit and byte counting explicit.

### D5. Integers only inside a plan
- **Alternatives:** floating-point costs; enabling serde_json's exact float round trip.
- **Choice:** every plan field is an integer (priority 0..=1000, costs as i64,
  travel costs as u32).
- **Reason:** the server stores the plan it computed; peers store the plan they
  decoded from JSON. Floats can change by one unit in the last place on a round trip
  unless a non-default parser feature is enabled, which would make equal replicas
  compare unequal. Integers also make the Hungarian solver's comparisons exact, so
  it can be tested against brute force.

## Processing

### D6. Seeded synthetic campus on every node
- **Alternatives:** one node generates and ships the state; load it from a file;
  every node generates it from a shared seed.
- **Choice:** `Campus::generate(seed, params)` on every node, with our own SplitMix64 PRNG.
- **Reason:** identical starting state with no bootstrap protocol. Our PRNG doesn't
  change between library versions or platforms the way a library generator can.
  Tested: same seed gives an identical campus; a different seed doesn't.

### D7. Planner workload and CPU knob
- **Alternatives:** greedy nearest responder; joint assignment over many open
  incidents; one incident's roles × responders, solved under several travel-time
  scenarios.
- **Choice:** priority (0.5 severity + 0.2 age + 0.3 scarcity) → `complexity`
  travel-time scenarios, each with every edge scaled by a seeded factor in [1.0,
  1.5] and Dijkstra from each distinct responder zone to the incident → mean ETA →
  cost matrix (roles × free responders: ETA weighted up by priority, capability
  mismatch weighted down by priority) → our Hungarian solver → top-10 ranked list,
  nearest available unit of each required resource, lead responder's route.
- **Reason:** the brief asks for a computation heavy enough to create real load. The
  scenarios model travel-time uncertainty and give a linear CPU knob
  (`complexity`). They are request-specific (seeded from the incident id), so they
  can't be precomputed. Honest note: with undirected edges, one reverse Dijkstra
  from the incident zone per scenario would give the same ETAs much faster; the
  per-responder runs follow the specified algorithm and are deliberately the load
  generator. Measured on one laptop (release build, three nodes on one machine,
  20 req/s, idle cluster): p50 latency 3.3 / 12.7 / 23.8 / 53.8 ms at complexity
  1 / 5 / 20 / 50; at complexity 100, 20 req/s exceeds 4 workers' capacity and
  latency grows to seconds. *To be measured in the lab.*

### D8. Worker-pool model
- **Alternatives:** plan on the async runtime; `spawn_blocking` per request; a fixed
  pool of `workers` OS threads behind a bounded queue.
- **Choice:** a fixed pool, its own queue (`queue_capacity`), and pending work exposed
  as the sum of queued and running jobs' expected cost (`complexity` units).
- **Reason:** planning is CPU-bound; running it on the async runtime would stall
  network I/O, and `spawn_blocking`'s pool grows without a useful bound. A fixed
  pool makes capacity explicit, gives the election an exact pending-work figure, and
  lets a stepping-down server drop queued-not-started jobs while running ones finish
  (Part B).

### D9. Serialized commits
- **Alternatives:** hold the ledger lock while planning (fully serial); plan on a
  snapshot and commit if still valid (optimistic); let conflicts happen and resolve
  them afterwards.
- **Choice:** optimistic. Plan on a snapshot, then under the ledger lock check that
  every chosen responder and unit is still free and commit. After two collisions,
  plan while holding the lock.
- **Reason:** keeps workers parallel and still guarantees that one server never
  assigns a responder twice. Collisions are counted (`local_replans`): 45 of 500 in
  the debug-build test, 5 of 500 in release. The integration test checks that no
  responder is ever held by two plans at once.

### D10. Busy reply
- **Alternatives:** an unbounded queue; drop silently and let the client time out; an
  explicit `Busy`.
- **Choice:** reply `Busy` when the queue is full; the client waits 20 ms × attempt
  and resends with `attempt + 1`.
- **Reason:** an unbounded queue turns overload into unbounded latency; dropping
  silently costs the client a whole timeout. `Busy` tells the client right away,
  and because it counts as a retry, in Part B it can move work away from an
  overloaded server. The short pause keeps a full queue from being hammered.

### D11. Request table on every node, results answered by anyone
- **Alternatives:** only the server tracks requests; every node records every request.
- **Choice:** every node records every attempt (`first_seen`, highest attempt, its
  epoch, completed) and every node keeps the results it computed or received by
  `Replicate`. Any node holding a result answers a retry with it.
- **Reason:** the request table is what a new server takes over (Part B), and what
  tells a retry apart from a first attempt. Answering from results means a retry
  after the server's crash is served without recomputing or reassigning. Entries
  expire after `table_ttl_ms` = 300 s, much longer than a client's retry window
  (5 × 1 s), so a late retry still finds its result.

### D12. Conflict rule for replicated plans
- **Alternatives:** last writer wins; first writer wins; a deterministic rule on the
  claims.
- **Choice:** higher-priority incident keeps the responder; ties go to the lower
  request_id; counted and logged.
- **Reason:** last/first writer depends on arrival order, which differs between
  replicas. A rule on the two claims alone gives the same holder on every replica
  in any order (unit-tested both orders). It only matters when two nodes served in
  the same epoch (Part B split winners).

### D13. Peer links on demand, one TCP connection per direction
- **Alternatives:** a permanent mesh with keep-alives; one shared connection per pair;
  connect on demand.
- **Choice:** each node opens an outbound link to a peer when it first has something
  to send, and reconnects with backoff (100 ms → 2 s). Inbound peer traffic arrives
  on the listener. Up to 4096 messages per peer are buffered; beyond that, they
  are dropped and counted.
- **Reason:** servers talk only to replicate or elect, so a link that isn't needed
  shouldn't exist. Two one-way connections avoid deciding who dials whom. A peer
  that is down for long needs the recovery protocol to catch up anyway (bullet 5),
  so the buffer is bounded rather than unbounded.

## Election *(Part B)*

### D14. DECISION CHANGED: per-request election → on-demand, retry-triggered election
- **Alternatives:** our survey's per-request election (every request triggers a bid
  exchange); a long-lived leader chosen by Raft-style terms and heartbeats; an
  on-demand election triggered by client retries.
- **Choice:** one node serves for a whole epoch; a new one is elected only when needed:
  at startup or after stepping down (no known server), or when a client retries a
  request (same request_id, higher attempt) whose previous attempt arrived in the
  current epoch. The survey's per-request election is kept as
  `election_mode = "per_request"` for comparison.
- **Reason:** professor's guidance. Per-request election adds a bid round (3 messages
  and a wait) to every request, even when nothing is wrong. With an on-demand
  election, the normal path costs no election traffic and elections happen only
  when the client sees a problem. Keeping the per-request mode lets the experiments
  measure the difference rather than assert it.

### D15. No heartbeats: the client retry is the failure detector
- **Alternatives:** periodic heartbeats between servers; the client's retry.
- **Choice:** no heartbeats. A retry of a request first seen in the current epoch means
  the current server crashed, is overloaded, or is cut off from the client.
- **Reason:** zero background traffic, and it detects exactly the failures clients
  notice, including ones heartbeats miss (a server that answers its peers but not its
  clients). **Limits:** a server that fails while idle goes unnoticed until the next
  request is retried, so the first request after an idle failure pays a timeout. A
  node returning from a failure hears nothing from its peers, so it must sync
  actively (recovery protocol, bullet 5).

### D16. Epochs, and step-down on a higher epoch
- **Alternatives:** no epochs (whoever thinks it's server serves); epochs with explicit
  hand-over messages; epochs carried by every message.
- **Choice:** every election targets `epoch + 1`. Any message carrying a higher epoch (a
  bid, a Replicate, or a request's `client_epoch`) makes a node adopt it; a node
  that was server steps down: it drops queued-not-started jobs, lets running ones
  finish, and treats their late results as harmless, counted duplicates. The new
  server immediately executes every not-completed request in its request table,
  oldest first.
- **Reason:** epochs give "who serves" a version number, so stale servers learn they
  are stale from any message that reaches them, with no extra protocol. Stamping
  the client's highest epoch into requests spreads the news through the client,
  even when servers can't reach each other.

### D17. Incumbent exclusion
- **Alternatives:** let every bidder win on score; exclude the incumbent.
- **Choice:** the server whose failure triggered the election may not win it unless it is
  the only bidder.
- **Reason:** a retry means the incumbent failed this client, whether by crash,
  overload, or a bad network path to the client. Its load score can still look good
  (a server with a broken client link is idle), so without the rule the election
  could re-elect the node that just failed.

### D18. Load-score terms
- **Alternatives:** queue length only; CPU only; round-robin; a weighted composite;
  adding network RTT.
- **Choice:** `score = 0.5 · pending + 0.3 · cpu + 0.2 · service_time`, each term
  normalized to [0, 1], computed fresh when bidding. `policy` selects Composite,
  QueueOnly, CpuOnly or RoundRobin for comparison.
  - **pending** = pending expected cost / workers: the work already promised,
    per worker. It is the most direct predictor of how long a new request waits.
  - **cpu** = machine CPU use (sampled every 250 ms, EWMA): load the queue can't see,
    such as other processes or another node on the same machine (tested with
    `--burn-threads`).
  - **service_time** = recent planning time (EWMA): catches a node that is slow per
    job (thermal throttling, a slower machine), which neither the queue nor CPU
    share shows directly.
  - **RTT excluded:** all servers share one LAN, so RTT differences are tiny next
    to planning times and would mostly add noise.
- **Reason:** each term covers a failure mode the others miss; the weights put the
  most direct signal first. The single-term policies exist so experiments can test
  whether the composite is worth it.

### D19. 2-of-3 rule; split-winner limitation
- **Alternatives:** decide on one bid (own); wait for all three; a majority, 2 of 3.
- **Choice:** collect bids until all 3 arrive or `bid_timeout_ms` passes; decide only
  with ≥ 2 bids; winner = minimum (score, node_id) among the bidders minus the
  incumbent.
- **Reason:** waiting for all three would block forever when one node is down; one bid
  would let a partitioned node elect itself. **Known limitation:** bids can arrive
  differently at different nodes, so two nodes can each believe they won the
  same epoch. This is not consensus and we do not claim it cannot happen. When a
  node sees a Replicate from another server of the same epoch, the lower node_id
  keeps the role and the other steps down; occurrences are counted, and the
  ledger's conflict rule (D12) keeps assignments consistent meanwhile.

### D20. Degraded mode after 3 s alone, vs. the spec's 20 s silence
- **Alternatives:** never serve alone; serve alone immediately; serve alone after a wait.
- **Choice:** `degraded_after_ms` = 3000. If a node's elections hear only its own bid
  for ≥ 3 s, it serves alone with `degraded = true` and doesn't change the epoch
  while degraded. Any peer message ends degraded mode.
- **Reason:** the 20 s in the spec is the length of the injected silence, which the
  spec sets. The 3 s is how long a lonely node waits before serving alone. It spans
  about three client retries and is ~1000× the lab RTT, so one lost message can't
  trigger it, and requests don't hang for the whole 20 s silence. Not changing the
  epoch while degraded keeps a cut-off node from racing the epoch ahead of the
  others. *To be confirmed by measurement.*

## Timing parameters

### D21. bid_timeout_ms = 100
- **Alternatives:** 10 ms; 100 ms; 1 s.
- **Choice:** 100 ms.
- **Reason:** expected lab RTT is well under 1 ms, so 100 ms is >100× RTT: a
  slow-but-alive peer still gets its bid in, while an election with a dead peer
  costs only 100 ms on top of the client's timeout. *To be measured:* lab RTT
  distribution, then re-set to a comfortable multiple of its p99.

### D22. request_timeout_ms = 1000, max_attempts = 5
- **Alternatives:** a fixed short timeout; a fixed long one; one derived from measured
  latency.
- **Choice:** 1000 ms for now, to be re-set to about 2× the measured p99 normal latency.
- **Reason:** the timeout is the failure detector (D15). Too short, and a merely busy
  server triggers elections; too long, and a crash costs that much latency. The
  local smoke run measured p99 ≈ 10 ms at complexity 5 and 200 req/s, so 1000 ms
  is very conservative until lab numbers exist. Five attempts span ≈ 5 s, longer
  than the 3 s degraded threshold (D20).
