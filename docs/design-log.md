# Design log

One entry per decision: the alternatives considered, the choice, and the reason.
Values marked *to be measured* are first guesses, to be re-set from lab
measurements. Entries marked *(my call — please review)* were decided during
Part B without the team's input, choosing the option most consistent with the
agreed design.

## November 3: cold-restart rejoin (Phase 1, bullet 5)

Details: [recovery.md](recovery.md).

- **Problem:** limitation b below. A restarted node starts empty, knows no
  server, and can win the next election and serve from empty tables.
- **Alternatives:** (1) every node recovers at startup: a cluster starting
  together would wait on each other forever; (2) an operator flag (`--rejoin`)
  on the restarted node: easy to forget, and wrong flags are silent; (3) a disk
  log replayed at start: the project keeps state in memory, and the peers hold
  it anyway; (4) probe the peers at startup and decide.
- **Choice:** (4). A new `Starting` phase; if any peer is ready with history or
  recovering, the node recovers through PR #38's snapshot pull before it bids
  or serves; otherwise it starts empty. A peer that is starting or recovering
  answers `RecoveryUnavailable` and is skipped, so two nodes restarted together
  recover one after the other instead of waiting on each other. An unreachable
  peer still blocks recovery, as PR #38 decided for the single-failure model.
- **Trade-off:** a node that hears from no peer within 300 ms starts empty, as
  before; under a partition at startup that is wrong, and is not handled.
- **Measured:** the restart sweep in [stress-testing.md](stress-testing.md).

## November 3: reset a peer's link when it pulls a snapshot (Phase 1, bullet 5)

- **Problem:** found by the restart sweep. At 48 clients (480 req/s) a 10 s
  outage leaves about 4000 frames in a survivor's 4096-frame queue to the dead
  node. The node restarts and pulls its snapshot within ~50 ms, but the
  survivor's link is still in its backoff (up to 2 s) and then drains the stale
  backlog first; meanwhile the queue overflows and new frames are dropped
  (360–390 per run, all in the 1–2 s after the rejoin). Those were `Replicate`s
  for results made after the snapshot, and the rejoined node never converged
  (0 of 5 runs; kept in `results/stress/local-2026-11-03/before-link-reset/`).
- **Alternatives:** (1) a larger queue: moves the threshold, and 2 s of backoff
  at high load can still overflow it; (2) drop the oldest frame instead of the
  newest: right for this case, but the newest frames still wait out the
  backoff; (3) sequence numbers per link and a re-pull on a gap: general, and
  more protocol; (4) on serving a `RecoveryRequest`, clear that peer's queue and
  wake its link.
- **Choice:** (4). The snapshot is read under the state lock, and every
  `Replicate` and `ElectBid` is queued under the same lock after its state
  change, so a frame queued before the reset is in the snapshot, and one queued
  after it is delivered. The link queue became a bounded deque (so it can be
  cleared) with a second wake-up for resets that cuts the backoff short.
  Unit test: `peers::tests::reset_drops_the_backlog_and_reconnects_without_waiting_out_the_backoff`.
- **Limit:** frames to a peer that is down but not recovering are still dropped
  past 4096; it recovers by snapshot when it returns, so nothing depends on them.

## November 3: stress harness and measurements (Phase 1, bullet 6)

Details, metric definitions and results: [stress-testing.md](stress-testing.md).

- **Simulated users are OS processes.** The harness (`stress`) starts N copies of
  the unchanged `client` binary per run, each with its own client id, connections,
  retry state and Tokio runtime (2 worker threads). *Alternative:* N `Client`
  objects in one process: cheaper, but they share one runtime and one set of
  sockets, which is not what the lab setup ("many simulated user processes") is.
- **Open loop.** Each process sends at a fixed rate whatever the responses do, so
  offered load = N × rate. *Alternative:* closed loop (send after the previous
  answer): it throttles itself at saturation and hides the overload behaviour
  (Busy replies, retries, elections) we want to measure.
- **Run order:** every level once per rep, levels shuffled within each rep (seeded).
  The tables keep 30 min of results (D38), so state grows over a sweep; a
  level-by-level order would confound that drift with load.
- **A restarted cluster for every run** (`--before-run`, locally
  `scripts/run_local.sh`, then a 20-request warm-up). The first full sweep showed
  that an overloaded run leaves thousands of abandoned requests pending for the
  TTL, and later runs at low load collapse because each election takes them over
  again (stress-testing.md, Overload). With a restart, runs are independent
  replications; the shuffle remains for machine drift.
- **Statistics:** 5 runs per level. Per-run p50/p95/p99 and throughput are
  reported as mean ± 95 % Student-t half-width over the runs (the runs are the
  independent samples); tails (p99.9) are also pooled over all requests of a
  level. Throughput is (n − 1) / (last answer − first answer): n / span
  overstates a short run's rate by n / (n − 1).
- **`ProbeRequest` / `Probe`.** `Status` computes the results digest by
  serializing every result under the state lock; polling it every 250 ms would
  add that pause to the work being measured. The probe returns only cheap fields.
  `Status` gained the summed election duration, CPU, resident memory and score.
- **Fault sweep:** a fixed 10 s outage at 8 s into every 25 s run, so loads are
  comparable; the spec's random up-to-20 s outage is still available as
  `client --inject-failure`, and `stress --fault-ms` takes any 1..=20 000 ms.
- **Convergence** is measured after the workload: time until every node is idle
  and the three digests are equal, polled every 20 ms. Its resolution is one
  probe round plus one Status round.
- **Limitation:** the committed results come from one machine (nodes, harness
  and up to 128 client processes share 10 cores and the loopback interface).
  They rank configurations and show the shape of the curves; lab numbers must
  be measured on the six machines (deployment.md, test 5).

## November 2: concurrency documentation and temporary-failure implementation

- **Choice:** retain the on-demand load-aware serving-node election. The new
  demo controller selects the currently elected server as victim, verifies all
  nodes ready, injects a bounded outage, and waits for recovery before another
  round. This keeps the professor-guided architecture and avoids introducing a
  second coordinator algorithm solely for failure simulation.
- **Failure model:** live process with communication muted for 1..=20,000 ms.
  Ready/muted/recovering phases gate client work and elections. Outage generations
  invalidate old jobs, election timers, and outgoing peer envelopes. Cold process
  restart/disk durability is not claimed by this implementation.
- **Recovery:** pull retained request/result snapshots in chunks from both ready
  survivors over dedicated connections. Require matching non-electing role views,
  merge results deterministically, rebuild the ledger, then enable participation.
  A newer epoch observed in live replication forces another recovery round.
  These cuts are not an atomic cluster snapshot or a Raft commit protocol.
- **Corrections required for delayed messages/rejoin:** retain the same bid for
  the whole target epoch (supersedes D36's time window); ignore expired incoming
  assignments; retain losing responder claims so replacing the winner restores
  the next valid claimant. Keep request metadata while its result is retained,
  ensuring snapshots can reconstruct both.
- **Documentation:** [concurrency-and-failures.md](concurrency-and-failures.md)
  describes the actual client tasks, connection tasks, worker threads, mutexes,
  overload behavior, demo commands, and remaining limits. Historical decisions
  below describe earlier versions; this entry supersedes their temporary-rejoin
  TODOs, not their cold-restart limitations.
- **Validation:** all 60 workspace tests passed locally on Windows, including a
  real-time 20-second serving-node outage. In the initial standalone run all
  127 requests were answered, 98 workload requests completed during the outage,
  the first survivor response arrived after about 1.08 s, and rejoin took 10 ms.
  Results and active ledger snapshots agreed after settling. These are small
  local regression measurements, not six-machine or statistical stress results.

## Architecture

### D1. Three equal peers vs. a fixed primary
- **Alternatives:** a fixed primary with two backups; three equal peers, one of which
  serves per epoch.
- **Choice:** three equal peers. Every node runs the same binary, keeps the same
  tables, and can serve.
- **Reason:** a fixed primary is a single point of failure and needs its own failover
  path anyway. With equal peers, failover is just "elect someone else", and the same
  code serves in every role. (Part A used node 1 as a fixed placeholder server;
  Part B replaced it with the election.)

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
- **Choice:** reply `Busy` when the queue is full; the client waits, then resends with
  `attempt + 1`. (The wait was 20 ms × attempt; D37 replaced it with exponential
  backoff plus jitter.)
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
  expire after `table_ttl_ms` (30 min since D38; first 300 s), much longer than a
  client's retry window (5 × 1 s), so a late retry still finds its result.

### D12. Conflict rule for replicated plans
- **Alternatives:** last writer wins; first writer wins; a deterministic rule on the
  claims.
- **Choice:** higher-priority incident keeps the responder; ties go to the lower
  request_id; counted and logged.
- **Reason:** last/first writer depends on arrival order, which differs between
  replicas. A rule on the two claims alone gives the same holder on every replica
  in any order (unit-tested both orders). It only matters when two nodes served in
  the same epoch (split winners, D19) or a late result races its replacement (D27).

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

## Election

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
  measure the difference rather than assert it. First measurement (three nodes on
  one laptop, 1000 requests at 200 req/s, complexity 5, no faults): on-demand
  sent 6 ElectBid frames in total (one startup election), per-request 6000
  (≈1 MB); p50 latency 4.9 vs 5.0 ms, p99 9.9 vs 8.1 ms. On loopback the bid round
  costs well under a millisecond, so the latency difference is noise there; in
  the lab it adds one RTT plus the wait for the slowest bid to every request.

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
  keeps the role and the other steps down; a bystander that believed in a third
  server takes the lower of the two. Occurrences are counted
  (`split_resolutions`); meanwhile the ledger rule (D12) and the result rule (D27)
  keep assignments and results consistent.

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
  costs only 100 ms on top of the client's timeout. Measured locally: a 3-bid
  election decides in 0.3–0.7 ms; a 2-bid one (dead peer) takes the full
  ≈101 ms. *To be measured:* lab RTT distribution, then re-set to a comfortable
  multiple of its p99.

### D22. request_timeout_ms = 1000, max_attempts = 5
- **Alternatives:** a fixed short timeout; a fixed long one; one derived from measured
  latency.
- **Choice:** 1000 ms for now, to be re-set to about 2× the measured p99 normal latency.
- **Reason:** the timeout is the failure detector (D15). Too short, and a merely busy
  server triggers elections; too long, and a crash costs that much latency. The
  local smoke run measured p99 ≈ 10 ms at complexity 5 and 200 req/s, so 1000 ms
  is very conservative until lab numbers exist. Five attempts span ≈ 5 s, longer
  than the 3 s degraded threshold (D20).

## Part B implementation decisions

### D23. Startup election on the first request, not at boot *(my call — please review)*
- **Alternatives:** elect as soon as the process starts; elect when the first
  request arrives.
- **Choice:** a node starts with no known server and no election. The first
  request it receives triggers `no_server`; all three nodes get that request
  (fan-out), so all three trigger the same election for epoch 1.
- **Reason:** at boot the peers are often not up yet, so the election would hear
  only its own bid and decide nothing; nothing needs a server before a request
  exists. It is still trigger (a) of the design ("no known server, at startup"),
  and it keeps the system silent while idle (D15).

### D24. Score normalization and quantization *(my call — please review)*
- **Alternatives:** raw terms; `min(x / cap, 1)`; `x / (x + h)`; float vs.
  integer scores.
- **Choice:** pending = p / (p + 10), where p is pending expected cost per worker in
  complexity units; cpu = EWMA of CPU use in [0, 1]; service_time = s / (s + 2),
  where s is the EWMA of planning time **per complexity unit** in ms. The weighted
  sum is divided by the weight total and quantized to milli-units (0..=1000).
- **Reason:** `x / (x + h)` maps [0, ∞) onto [0, 1) and never saturates, so of two
  overloaded nodes the more loaded still scores higher (`min(x/cap, 1)` would tie
  them). Half-points: 10 units per worker ≈ 10 ms of queued work at the measured
  ≈1 ms/unit; 2 ms/unit is twice the measured planning speed. Time per *unit*
  keeps a node that happened to serve heavy requests from looking slow.
  Integer scores: every node compares the same values, whereas a float that
  went through JSON could differ by one unit in the last place from the sender's
  copy. Quantizing also makes idle nodes tie exactly, so the node_id tie-break
  decides instead of measurement noise. *Constants to be revisited with lab data.*

### D25. CPU term source: machine vs. process *(my call — please review)*
- **Alternatives:** whole-machine CPU only; this process's CPU only; configurable.
- **Choice:** `cpu_source = "machine"` (default) or `"process"`.
- **Reason:** machine CPU sees external load (other programs), which is what the
  CPU term is for, but nodes sharing a machine (node1 and node2 on machine A for
  now) see the same value, and that's correct: they really share the CPU.
  Process CPU (this node's threads as a share of all cores) tells co-located nodes
  apart but is blind to other programs. The `--burn-threads` integration test
  runs three node processes on one machine with `"process"`; with `"machine"` it
  could not distinguish them.

### D26. RoundRobin key *(my call — please review)*
- **Alternatives:** hash(request_id) in both modes; target epoch in on-demand mode.
- **Choice:** per-request mode: hash(request_id, attempt) mod candidates, as
  specified. On-demand mode: hash(target_epoch) mod candidates.
- **Reason:** in on-demand mode several retries (different request_ids) can trigger
  the same election, and each node may have seen a different one first; the
  target epoch is the one key every bidder agrees on.

### D27. One result per request: higher epoch, then lower served_by *(my call — please review)*
- **Alternatives:** first result wins; last result wins; a rule on the results.
- **Choice:** when a node holds two different results for one request (a late
  result from a server that stepped down, or two servers in one epoch), it keeps
  the one with the higher epoch, then the lower `served_by`, and releases the
  loser's ledger holds. Late results are stored and replicated under this rule,
  but not sent to the client.
- **Reason:** first/last-wins depends on arrival order, so replicas could end up
  holding different plans for the same request. With an order-independent rule
  they converge (unit-tested in four orders). The higher epoch is the newer
  server's view of the ledger. **Limitation:** during a handover, a node holding a
  late result can answer a client's retry with it before the newer result
  arrives, so a client may hold a plan the cluster later supersedes.

### D28. Which incumbent is excluded *(my call — please review)*
- **Alternatives:** the incumbent named by the triggering node; every incumbent named
  in any bid.
- **Choice:** every incumbent named in the bids received is excluded (unless that
  leaves no candidate).
- **Reason:** bids normally agree on the incumbent. When they don't (after a split),
  excluding all of them still gives every node that holds the same bids the same
  decision.

### D29. A bid for the next epoch makes the server step down at once *(my call — please review)*
- **Alternatives:** keep serving until the decision; step down on the first bid.
- **Choice:** a node that receives a bid for an epoch it hasn't joined steps down
  immediately (and adopts `target − 1` if it was further behind), then bids.
- **Reason:** the design lists "a bid" among the messages that carry a higher epoch
  and cause step-down. A bid means a peer believes the current server failed. The
  incumbent can't win (D17), so serving on until the decision would only produce
  late results.

### D30. Adopting a higher client_epoch leaves the server unknown *(my call — please review)*
- **Alternatives:** guess the server; ask a peer; treat the server as unknown.
- **Choice:** adopt the epoch with server = unknown, which then triggers a `no_server`
  election on the next request. A Replicate with a higher epoch names its origin
  as the server, so it is adopted with a known server.
- **Reason:** the client's epoch doesn't say who serves, and asking a peer would add
  a protocol step. **Cost:** a node that missed an election (e.g. it was down)
  can force one extra election when a client tells it about the new epoch. Part
  of the rejoin problem (bullet 5).

### D31. Degraded mode details *(my call — please review)*
- **Choice:** while degraded, retries do not trigger elections (there is nobody to
  elect, and the node must not step down from serving alone). Degraded mode ends on
  any peer message or on adopting a higher epoch; the node's server becomes
  unknown unless that message names one. `Replicate` carries a `degraded` flag
  (an addition to the message as first specified), so peers store exactly the
  response the client got.
- **Reason:** follows "serves alone ... never changes the epoch while degraded ...
  any peer message ends degraded mode", and keeps the results tables equal after
  a degraded period.

### D32. Per-request mode details *(my call — please review)*
- **Choice:** `ElectBid` carries optional `request_id` and `attempt` (an addition to
  the message); a retry is a new election (new key). Decided keys are remembered for
  60 s so a late bid can't restart them. A node that wins before the client's
  message reaches it queues the request when the message arrives. The winner
  queues the request at once, so its pending cost already includes it when it
  bids on the next request.
- **Reason:** the same bid exchange as on-demand mode, keyed per request; no
  incumbent rule and no epochs, as specified.

### D33. Takeover ignores the queue limit *(my call — please review)*
- **Alternatives:** take over only what fits in the queue and leave the rest to client
  retries; take over everything.
- **Choice:** the new server queues every not-completed request, oldest first, even
  past `queue_capacity`. `Busy` still applies to new arrivals.
- **Reason:** the design says the new server "immediately executes every not-completed
  request". Leaving some to retries also produced more Busy replies, more
  elections (D34) and lower throughput: in the local failover run below,
  throughput went from 120 to 169 req/s, and p50 latency from 1.5 s to 0.69 s.

### D34. Hysteresis: a retry moves the role only after `min_tenure_ms` (decided)
- **Problem:** after a server crash, the two survivors handed the role back and forth:
  epochs 2→8 in ≈0.8 s (three local processes, 600 requests at 200 req/s, complexity
  5, `queue_capacity` 64, server killed 1.2 s in). Every request was still answered,
  but six of the seven elections were wasted.
- **Cause:** the backlog moves with the role. The new server inherits every
  not-completed request (D33), so its queue overflows and new arrivals get `Busy`.
  Their retry arrives 20 ms later, in the same epoch, which is trigger (b).
  Incumbent exclusion then hands the role, and the whole backlog, to the other
  survivor, and the cycle repeats every ≈125 ms until the backlog drains.
- **Alternatives:** never treat Busy-caused retries as triggers (gives up moving work
  off an overloaded server); size `queue_capacity` to the takeover backlog (only
  hides it); hysteresis on the role.
- **Choice:** hysteresis, per our survey §5.7. A retry can trigger an election only
  if the current server has held the role for at least `min_tenure_ms` (new config
  field; default 1000 ms = the client's `request_timeout_ms`; 0 disables).
  Elections triggered by "no server known" are exempt, and so is a retry while no
  server is known. Tenure is measured from when this node learned the server's role
  (decided the election, or adopted the epoch from that server's Replicate).
  Suppressed triggers are counted (`suppressed_triggers` in Status) and logged one
  line each; the retried request is then handled normally (queued, or Busy).
- **Cost:** a server that crashes within `min_tenure_ms` of being elected is detected
  up to one client timeout later than without the rule. Under sustained overload,
  retry elections still happen, but at most one per `min_tenure_ms` instead of one
  per Busy retry.
- **Measured** (same scenario, same release build; `min_tenure_ms = 0` vs `1000`,
  3 runs each):

  | min_tenure_ms | elections after the crash | final epoch | suppressed triggers | answered | throughput | p50 / p99 latency |
  |---|---|---|---|---|---|---|
  | 0 (before) | 7, 7, 7 | 8 | 0 | 600/600 every run | 167–170 req/s | 702–712 ms / 2003 ms |
  | 1000 (after) | 1, 1, 1 | 2 | 316, 320, 284 | **593, 590, 597** | 197–199 req/s | 17–18 ms / 1084–1099 ms |
  | 1000 + D37 Busy backoff | 1, 1, 1 | 2 | 256, 188, 192 | **600, 600, 600** | 200 req/s | 18–69 ms / 1097–1099 ms |

  One election now detects the crash, and nothing follows. The second row lost 3–10
  requests per run to Busy exhausting the client's attempts; D37 fixed that (third
  row: no request needed more than 4 of its 5 attempts). In the in-process failover test (debug build, no Busy
  because the queue holds 1024), elections after the crash went from ≈125 ms apart
  to at most one per second (3–4 elections in 4.5–5 s while the new server worked
  through the backlog); the test asserts that rate.

### D35. The election as a sans-IO state machine
- **Alternatives:** election logic inside the networking code; a pure state machine
  driven by events with explicit time.
- **Choice:** `election::Core` takes events (request, bid, timer, Replicate) with
  `now` and returns actions (send bid, arm timer, step down, take over, execute,
  log). The node carries out the actions.
- **Reason:** every rule (order independence, tie-break, incumbent exclusion,
  2-of-3 after timeout, old-epoch retries, step-down, split winners, degraded
  after 3 s) is unit-tested with synthetic times, with no sockets or sleeps.

### D36. Rejoining an election re-sends the same bid (accepted)
- **Alternatives:** bid afresh whenever (re)joining; re-send the bid already sent
  for that target epoch.
- **Choice:** a node remembers the bid it sent for each target epoch. If it rejoins
  that election within 10 × `bid_timeout_ms` (its own timer fired before a peer's
  bid arrived, so it decided nothing), it re-sends the same bid. After that window
  it bids afresh.
- **Reason:** found by the failover test under heavy parallel test load, where bids
  took up to ≈340 ms against a 100 ms timeout. Node B decided from A's first bid;
  A timed out alone, then rejoined when B's late bid arrived, with a *fresh*
  score. The two nodes had then decided from different bids, could pick different
  winners, and could each believe the other was serving, so nobody served;
  repeated, two requests ran out of attempts. Re-sending the same bid is the
  design's "each node bids once per election", and makes both nodes decide from
  identical bids. It mirrors Raft, where a node votes at most once per term: a
  bid is the node's single statement for that epoch, so it must not change when
  the node rejoins; re-sending it unchanged keeps every node deciding from the
  same values. The window keeps a lonely node from re-sending a stale score
  forever. (B ignores A's re-sent bid because it already decided that epoch, but
  A now reaches the same winner.)

### D37. Exponential backoff with jitter after Busy (decided)
- **Problem:** with D34 in place, 3–10 of 600 requests per run went unanswered in the
  failover scenario (table in D34). Every one got `Busy` on all 5 attempts.
- **Cause:** the client paused only 20 ms × attempt after `Busy` (D10), so its 5
  attempts ran out in ≈200 ms, while the new server's queue held the inherited
  backlog for ≈1 s. Before D34, each Busy retry moved the role to a server with a
  fresh queue, which hid this.
- **Alternatives:**
  1. Client: back off exponentially on `Busy`.
  2. Server: requests taken over (D33) don't count against `queue_capacity`.
  3. `Busy` doesn't consume an attempt.
- **Choice:** option 1. After attempt *n* gets `Busy`, the client waits
  min(50 ms × 2^(n−1), `request_timeout_ms`) plus 0–25 % jitter, then resends with
  attempt *n* + 1. Over the default 5 attempts the pauses add up to ≥ 750 ms
  (50 + 100 + 200 + 400, plus jitter), most of a takeover backlog's measured drain
  time. The jitter keeps clients that were turned away together from coming back
  together. It is a hash of (client, request, attempt), so a run with the same seed
  retries at the same times. Busy retries still raise the attempt number, so a
  server that stays overloaded can still be replaced (subject to `min_tenure_ms`).
- **Why not option 2:** it removes the queue bound during a takeover. The new server
  would accept new work without limit on top of the backlog it inherited, which is
  the unbounded-queue overload `Busy` exists to prevent (D10).
- **Why not option 3:** if `Busy` didn't consume an attempt, a Busy retry would never
  carry a higher attempt number, so it could never trigger a re-election. That
  removes the high-queue trigger of our election design: an overloaded server
  could only be replaced via timeouts, never via its own `Busy` replies.
- **Measured:** third row of the D34 table. 600/600 answered in all 3 runs, 1 election
  after the crash, p50 18–69 ms, p99 ≈1.1 s, throughput at the offered 200 req/s;
  94–128 Busy replies per run, at most 4 attempts for any request.

### D38. table_ttl_ms = 1 800 000 (30 min), was 300 000 (decided)
- **Problem:** in local test runs with 3 nodes and the client, the 5-minute TTL
  expired results tables mid-session. Each node stamps a result when *it* stores
  it (computed or received by `Replicate`), so the nodes expire the same result at
  slightly different moments, and a status taken around then showed different
  result counts and digests for reasons unrelated to replication.
- **Alternatives:** keep 300 s and take status within 5 min of a run; disable the
  sweep; a longer TTL.
- **Choice:** `table_ttl_ms = 1800000` in every node config (`config/` and
  `config/local/`), and as the code default.
- **Reason:** 30 min covers a test session, so digests compare replication and
  nothing else. Memory is not a concern at these volumes (thousands of entries).
- **Side effect:** a retry arriving after its entries expired is not answered from
  the results table. The node no longer knows the request, so it is treated as new
  and recomputed: the plan can differ from the first one, and the first plan's
  ledger holds are not released by the sweep. With a 5 s retry window this needs a
  client that resends after 30 min, which ours never does.

## Local test results (Mac, preliminary)

3 local nodes and the client on one Mac (`config/local/`), release build. Preliminary:
one machine, no network between the nodes; to be repeated in the lab.

- **Normal run:** 200 requests, all 200 answered on attempt 1; p50 7.9 ms, p99
  11.5 ms. 1 election; 6 ElectBid messages in total, against 1,200 if we elected per
  request (3 nodes × 2 peers × 200).
- **The first election was decided by load:** bids node1 = 37, node2 = 39,
  node3 = 39 (milli-units, D24), so node 1 won. The election took 0.46 ms, and
  each node sent 2 messages (its bid to each peer).
- **Failover:** the current server was killed mid-run (3,000 requests at
  50 req/s), twice. Both times the two survivors elected a new server in a new
  epoch and agreed on it; about 8 requests needed a retry; the results tables
  were identical after the run.
- **Status during a run:** a status taken while requests are in flight can show
  one node a result ahead of the others, because replication is asynchronous:
  the server replies to the client first and replicates after. Digests only
  match once nothing is in flight. `client --status --settle` now waits for that
  before comparing (below).
- **`--settle`:** `client --status --settle [SECS]` (default 10 s) polls every
  100 ms until every reachable node reports `queue 0` and `running 0` on two
  consecutive polls, then prints the status and compares digests. Two polls in a
  row cover the moment between a job finishing and its `Replicate` being sent. If
  the cluster doesn't settle in time, it says so before printing. It doesn't see
  frames still in a peer link's queue (limitation a, below).

## Known limitations (for bullet 5; status on November 3)

Status: **a** no longer matters for recovery (the returning node pulls what it
needs, and the peer clears its backlog to it when the snapshot is taken; see
"reset a peer's link" above); **b** fixed (a
restarted node is `Starting`/`Recovering` until it has copied its peers' state,
and doesn't bid); **c** remains (memory only). Details in [recovery.md](recovery.md).


a. **The peer link's backlog has no useful limit.** While a peer is down, its link
   keeps every undelivered message (`Replicate`, but also `ElectBid`) and sends
   them all when the peer returns. That gave node 3 a partial catch-up after a
   restart, but it is a side effect, not a protocol: the queue is bounded only by
   count (`QUEUE_LIMIT` = 4096 frames per peer; beyond that new frames are dropped
   and counted in `peer_drops`), not by age or bytes. At 50 req/s that is
   ≈80 s of results held in memory; anything beyond is lost silently to the
   returning node, and stale bids from long-finished elections are delivered too.
   Needs a real bound (age or bytes, and only `Replicate`), or replacing by the
   recovery/sync protocol, where the returning node pulls what it missed
   (`TODO(recovery/sync)` in `node.rs`).
b. **A restarted node can win an election while stale.** It starts knowing no
   server, so its first request can trigger an immediate `no_server` election
   (D23), and with an empty queue its load bid is the lowest, so it may win and
   serve from a ledger and results table that are missing everything it didn't
   see. Rejoining nodes must not bid or serve until caught up (`ready = false`,
   issue #28).
c. **A restarted node loses the results it served before crashing.** Tables and
   the ledger are in memory only. Its peers hold the results it replicated; any
   it computed but hadn't yet replicated are gone, and it gets nothing back
   except what limitation a happens to redeliver.
