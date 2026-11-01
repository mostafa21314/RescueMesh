# Part B summary: on-demand, load-aware election (Phase 1, bullet 2)

Branch `phase1/load-election`, on top of the Part A commit `87de5e8`. Nothing is
committed: all changes are in the working tree.

## What was built

| Piece | Where | What it does |
|---|---|---|
| Load score | `crates/cloud-node/src/load.rs` | CPU sampler thread (sysinfo, every 250 ms, EWMA α = 0.3), planning time per complexity unit (EWMA), `score = w_pending·pending + w_cpu·cpu + w_svc·service_time`. Each term is mapped to [0, 1]; the score is an integer in milli-units. Policies: Composite, QueueOnly, CpuOnly, RoundRobin. `--burn-threads N`. |
| Election state machine | `crates/cloud-node/src/election.rs` | Pure logic with no I/O ("sans-IO"): events in (request, bid, timer, Replicate) with explicit time; actions out (send bid, arm timer, step down, take over, execute, log). Details below. |
| Node wiring | `crates/cloud-node/src/node.rs` | Carries out the actions. Step-down drops queued jobs. Takeover queues every not-completed request, oldest first. Late results and suppressed triggers are counted. One log line per election and per suppressed trigger. |
| One result per request | `tables.rs`, `ledger.rs` | When a node holds two results for one request, it keeps the higher epoch, then the lower served_by, and releases the loser's ledger holds, so replicas converge in any order. |
| Protocol | `crates/common/src/msg.rs` | New `ElectBid {target_epoch, node_id, score, incumbent, request_id?, attempt}`; `Replicate` gains `degraded`; `Status` gains election counters, including `suppressed_triggers`. |
| Client Busy backoff | `crates/client/src/lib.rs` | After `Busy` for attempt *n*: wait min(50 ms × 2^(n−1), request timeout) + 0–25 % jitter, then resend with attempt *n* + 1 (D37). |
| Config / CLI | `config/*.toml`, `scripts/run_local.*`, `client` | `min_tenure_ms` (default 1000 ms), `cpu_source = "machine" \| "process"`; `--burn-threads N` (`BURN_NODE=1 BURN_THREADS=4 scripts/run_local.sh`); `client --status` prints the election counters. |
| Docs | `docs/protocol.md` (rewritten), `docs/design-log.md` (D23–D37 added; D1, D10, D12, D14, D19, D21 updated), `docs/library-vs-ours.md`, `README.md` | |

The election state machine implements:
- **Triggers:** no known server; or a retry of a not-completed request whose
  previous attempt arrived in the current epoch.
- **Hysteresis (D34):** a retry triggers an election only once the current server
  has held the role for `min_tenure_ms`. "No server known" is exempt.
- **Bidding:** one election per target epoch; a bid for a higher target replaces
  the current election; a node that rejoins an election re-sends its original bid.
- **Decision:** at least 2 of 3 bids; the incumbent is excluded; winner = lowest
  (score, node_id).
- **Epochs:** step-down on any higher epoch (bid, Replicate, or client_epoch).
- **Split winners:** the lower node_id keeps the role.
- **Degraded mode** after 3 s of elections that heard only the node's own bid.
- **Per-request mode**, for comparison.

## Test results

Final run, each step under a hard time limit:
- `cargo fmt --all -- --check`: clean.
- `cargo clippy --all-targets -- -D warnings`: clean.
- `cargo test`: **54 passed, 0 failed** (23 s): 41 cloud-node unit tests (16 of
  them for the election), 1 client unit test, 4 common unit tests, 1 config test,
  7 integration tests.

Stability:
- **40/40** consecutive runs of the election integration tests
  (`cargo test -p cloud-node --test election`: failover, per-request, degraded),
  after the D36 fix. Before it, the binary failed about once in 10–20 runs. That
  was before the D34 change.
- 5 full-suite debug runs and 1 release run: 52/52 each time (before D34).
- After D34: 5 runs of the election tests, all passing; after D37, the final full
  run above.
- Every integration test has a hard limit inside the test (60 s; 90 s for the
  process-based burn test). Every command and script run by hand was killed at a
  hard deadline if it overran.

| Spec test | Test |
|---|---|
| same winner for any bid order | `election::tests::same_winner_for_any_bid_order` (all 6 orders, at each of the 3 nodes) |
| tie-break by node_id | `election::tests::tie_breaks_by_lower_node_id` |
| incumbent excluded | `election::tests::incumbent_excluded_unless_only_candidate` |
| 2-bid decision after timeout | `election::tests::decides_with_two_bids_after_timeout`, `one_bid_is_not_a_decision` |
| old-epoch retry does not trigger | `election::tests::retry_in_current_epoch_triggers_old_epoch_retry_does_not` |
| step-down on a higher epoch | `election::tests::server_steps_down_on_retry_and_on_any_higher_epoch` (bid, Replicate, client_epoch, own retry) |
| takeover of pending requests | `election::tests::winner_takes_over`, `tables::tests::takeover_order_is_oldest_first_and_skips_completed`, the failover integration test |
| split-winner resolution | `election::tests::split_winner_lower_node_id_keeps_the_role` |
| degraded mode only after 3 s alone | `election::tests::degraded_only_after_three_seconds_alone`, plus the integration test `a_lone_node_serves_degraded_only_after_three_seconds` (answered at ≈3.1 s, `degraded = true`, epoch unchanged) |
| integration: stop the server mid-workload | `tests/election.rs::stopping_the_server_mid_workload_elects_another_and_every_request_is_answered`: every request answered, and at most one election per `min_tenure_ms` after the crash (observed 3–4 in 4.5–5 s, debug build under parallel test load) |
| integration: --burn-threads moves the election | `tests/burn.rs::burn_threads_move_the_election_away`: 3 real `cloud-node` processes with `cpu_source = "process"`. Idle cluster → node 1 wins the tie; node 1 burning half the cores → node 2 wins. |

Tests for the hysteresis (D34):
- `election::tests::retry_triggers_wait_for_min_tenure_but_no_server_is_exempt`:
  - a retry 300 ms into a server's tenure is suppressed (no bid, no step-down);
  - after 1 s it triggers an election;
  - a newly elected server's tenure starts over;
  - "no server known" is exempt, including a retry while no server is known.
- The failover integration test's rate bound (above).

Test for the Busy backoff (D37): `client::tests::busy_pause_doubles_caps_at_timeout_and_jitters_up_to_25_percent`.
It checks the pause per attempt (base 50 ms doubling, capped at the timeout, jitter
between 0 and 25 %), that the jitter varies, that the pause is deterministic for the
same inputs, that huge attempt numbers don't overflow, and that 5 attempts span at
least 750 ms.

Other extra tests:
- Per-request mode spreads a burst over at least 2 nodes, and the tables converge.
- Rejoining an election re-sends the same bid (D36).
- Round-robin is deterministic.
- The load score's terms.
- The one-result rule converges in any order.
- `Ledger::release`.
- `Status` survives a JSON round trip.

Measured with real processes (one laptop, release build):
- **No faults, 1000 requests at 200 req/s, complexity 5.** On-demand: 1 election,
  6 ElectBid frames in total, p50 4.9 ms / p99 9.9 ms. Per-request: 6000 ElectBid
  frames (≈1 MB), p50 5.0 ms / p99 8.1 ms. Tables identical in both.
- **Server killed 1.2 s into a 600-request run at 200 req/s, complexity 5,
  queue_capacity 64**, 3 runs each:

  | min_tenure_ms | elections after the crash | final epoch | suppressed triggers | answered | throughput | p50 / p99 |
  |---|---|---|---|---|---|---|
  | 0 (before) | 7, 7, 7 | 8 | 0 | 600/600 | 167–170 req/s | ≈710 ms / 2003 ms |
  | 1000 (after) | 1, 1, 1 | 2 | 284–320 | **590–597** | 197–199 req/s | ≈17 ms / ≈1090 ms |
  | 1000 + D37 Busy backoff | 1, 1, 1 | 2 | 188–256 | **600, 600, 600** | 200 req/s | 18–69 ms / 1097–1099 ms |

  With the backoff, 94–128 Busy replies per run; no request needed more than 4 of
  its 5 attempts.

## Unfinished / open

1. **Not tested end to end:** split winners (unit-tested only); the PowerShell
   script (no Windows machine here).
2. **Stubs for later bullets:**
   - `TODO(fault injection)`: mute-switch points in `node.rs` and `peers.rs`.
   - `TODO(recovery/sync)`: a node that returns must fetch the results it
     missed (D15, D30).
   - `TODO(discovery)`.
   - No stress harness yet.
3. **Constants still to measure in the lab:** `bid_timeout_ms`,
   `request_timeout_ms` and `min_tenure_ms` (kept equal), the score's half-points
   (`PENDING_HALF`, `SERVICE_HALF_MS`), `queue_capacity`, the 10 × bid-timeout
   window for re-sending a bid (D36), and the Busy backoff base and jitter (D37).
4. `cargo fmt` reformatted several Part A files (campus, graph, hungarian, rng,
   wire and others). Those diffs are formatting only.

## Decisions

Decided with you:
- **D34** Hysteresis per survey §5.7: `min_tenure_ms`, "no server known" exempt,
  suppressed triggers counted and logged.
- **D36** (accepted) Rejoining an election re-sends the same bid, like Raft's one
  vote per term.
- **D37** Exponential backoff with jitter after `Busy`; Busy retries still raise the
  attempt number. Rejected: not counting taken-over requests against the queue
  (removes the queue bound during a takeover), and Busy not consuming an attempt
  (Busy retries could then never trigger re-election, which removes the high-queue
  trigger of the election design).

My own calls, marked *(my call — please review)* in `docs/design-log.md`:
- **D23** The startup election happens on the first request, not at boot.
- **D24** Score normalization `x / (x + h)`; service time per complexity unit;
  scores as integer milli-units.
- **D25** `cpu_source = machine | process` (default machine); the burn test uses process.
- **D26** RoundRobin uses hash(target_epoch) in on-demand mode, and hash(request_id,
  attempt) in per-request mode.
- **D27** One result per request: higher epoch, then lower served_by. Late results
  are replicated but not sent to the client.
- **D28** Every incumbent named in any bid is excluded.
- **D29** A bid for the next epoch makes the server step down immediately.
- **D30** Adopting a higher client_epoch leaves the server unknown.
- **D31** No elections while degraded; `Replicate` carries `degraded`.
- **D32** Per-request bids carry `request_id` and `attempt`; finished keys are
  remembered for 60 s.
- **D33** Takeover ignores the queue limit.
- **D35** The election is a sans-IO state machine.
- Within D34: tenure is measured from when *this node* learned the server's role
  (decided the election, or adopted the epoch from the server's Replicate), and a
  retry while no server is known is exempt, like "no server".

## Suggested commit message

```
Phase 1 Part B: on-demand load-aware election

- load score: pending cost per worker, CPU (sysinfo, 250 ms EWMA) and
  planning time per unit, normalized and quantized; Composite, QueueOnly,
  CpuOnly and RoundRobin policies; --burn-threads
- election as a sans-IO state machine: no-server and retry triggers,
  one election per target epoch, 2-of-3 bids, incumbent exclusion,
  min(score, node_id), epochs with step-down on any higher epoch,
  takeover of not-completed requests, split-winner resolution, degraded
  mode after 3 s alone, same bid re-sent on rejoin (one bid per epoch,
  as in Raft's one vote per term); per-request mode for comparison
- hysteresis (survey 5.7): a retry triggers an election only after the
  server has held the role for min_tenure_ms (default 1000); no-server
  elections exempt; suppressed triggers counted and logged. After a crash:
  1 election instead of 7 (epochs 2->8 before)
- client: after Busy, wait min(50 ms x 2^(attempt-1), request timeout)
  plus 0-25% jitter; Busy retries still raise the attempt number. With
  the hysteresis: 600/600 answered in the failover scenario (590-597
  with the old fixed pause)
- ElectBid message; Replicate carries degraded; election counters in Status
- one result per request (higher epoch, then lower served_by) with
  ledger release, so replicas converge after handovers
- tests: election rules, tenure, Busy backoff, failover, degraded
  mode, per-request mode, --burn-threads with real processes; hard time
  limit in every integration test
- docs: protocol rewritten for the election, design log D23-D37
```
