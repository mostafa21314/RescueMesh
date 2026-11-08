# Protocol 3: bounded exclusive reservations and an explicit correctness gate

The six-PC smoke completed every request and converged, but per-request fault
runs detected 30 resource overcommitments and 61 responder conflicts summed
across replicas. A local planner mutex cannot prevent two different servers
from choosing the same globally shared resource before replication arrives.

## Reservation policy

Protocol 3 uses `exclusive_asset_partitions_v1` in **both election modes**.
Sort the configured server IDs. Responder/resource ID `x` belongs to
`members[x % members.len()]`. With IDs 1,2,3, asset 0 belongs to Node 1, asset 1
to Node 2, asset 2 to Node 3, and so on. A resource offer's entire quantity is
owned by its designated allocator; capacity is not duplicated at every node.

The elected processing node still computes the response plan. Its planning
snapshot exposes only responders and offers in its own asset partition. It
uses local serialized reservation checks before issuing a plan, and commits
assignment timestamps at reservation time rather than at the earlier planning
snapshot. Planning cost still runs the configured travel-time scenarios on
the campus graph. Priority scarcity considers the eligible responder subset.

The important trade-off is **safety versus resource utilization**: nodes never
borrow another node's assets just because that node is idle or unavailable.
On-demand processing can therefore leave other nodes' partitions unused;
per-request processing can use several partitions concurrently. Report the
new shortfall and assignment metrics when comparing the policies. This is an
explicit design change from the smoke's protocol-2 shared asset pool, so rerun
both modes on protocol 3 rather than mixing results across versions.

An issued reservation remains protected until its lease expires, even when a
different result replaces the node's canonical result for that request. This
prevents a preferred-result merge from releasing an assignment the client may
already have received. Temporary mute/recovery preserves these local issued
leases and restores known retained own-node reservations. The replicated
canonical result tables can still converge independently.

Assignment leases are capped at 30 seconds. After **cold rejoin** into a running
cluster, a restarted process quarantines its asset partition for 30 seconds,
covering issued leases it may have lost. It can answer requests during this
period with explicit resource/responder shortfalls. It cannot allocate another
node's assets to compensate. Fresh empty test clusters do not incur this cold
rejoin quarantine. A temporary mute retains memory and does not need it.

These guarantees assume fixed consistent membership/campus configuration, one
live process per node ID and stable synchronized clocks for lease timestamps.
They do not establish Raft consensus, durable exactly-once execution, safety
against Byzantine peers, or dynamic ownership transfer.
All-node memory loss is outside the single-node failure model; fresh empty
test clusters begin a new simulation rather than recovering external live leases.
If resources must be
accessible through every processing node, an owner-mediated or quorum-backed
reservation protocol is a different design with additional coordination cost.

## Validation and diagnostics

Received/recovered plans are checked for issuer ownership, known assets and
resource types, duplicate assignments, positive bounded quantities and lease
length. Invalid replicated plans are rejected and counted, not silently
accepted. The study treats these rejections as correctness failures.

Reservation audits compare the actual `[created_at, expires_at)` intervals.
Adjacent leases do not overlap. A slower replica clock receiving the next
valid lease before the previous one expires locally no longer creates a false
double-booking warning. Real overlapping capacity remains detectable.

Every server status includes `allocation`: policy, membership, owned assets,
quarantine remaining, rejected-plan count and current/future interval violations.
Existing historical overcommit and responder-conflict counters remain visible.

Study results separate:

- `measurement_valid`: complete workload/CSV/timing instrumentation;
- `converged`: final cohort agreement and matching Ready/idle role views;
- `resource_correctness_known`: complete allocation diagnostics available;
- `resource_correctness_pass`: compatible policy/membership, zero rejections,
  no interval violations and no new overcommit/responder-conflict detections;
- `correctness_pass`: convergence **and** resource correctness.

Unknown diagnostics never count as a pass. Confirmed resource violations save
the run and stop the campaign. Unassessed/nonconvergent overload measurements
remain labelled, and the next independent run still requires a fresh healthy
baseline. Sustainable-point selection and paired comparisons require resource
correctness as well as valid measurements. Legacy reports show unknown/failing
resource verdicts rather than inferring safety from successful replies.

The client CSV additionally records allocation policy, assignment counts,
resource/responder counts, and resource/responder shortfalls. A valid computed
plan with shortages is distinguishable from a fully supplied response plan.
Successful reply rate alone is not a fulfillment rate.

## Election throughput

Election/control and replication still have separate bounded UDP RR lanes.
The control sender now groups up to 64 queued election bids into one bounded
`ElectBidBatch`, retaining each bid's request/attempt and the logical message
counts. Nested batches and non-bid control messages inside a bid batch are
rejected. ACKs, retries, backoff, queue bounds and single-message compatibility
remain. No extra reservation network round is introduced.

This improves the sustained bid delivery capacity of per-request mode on
delayed networks. It does not promise identical latency to on-demand mode or
eliminate election waits when a peer is unavailable. UDP datagram/byte/retry
counters are recorded separately from logical election messages.

## Upgrade and rerun

Copy the updated source to all six PCs, rebuild with `cargo build --release`,
and restart all six study agents using the same commands/token in
[six-pc-study.md](six-pc-study.md). Every server must report protocol 3 and the
same asset policy/membership. Previous study directories cannot be resumed
across the source/protocol change; preserve them as the defect baseline.

```bash
python scripts/phase1_study.py check --lab config/study-lab.json
python scripts/phase1_study.py run --lab config/study-lab.json --profile smoke --out results/phase1/six-pc-smoke-v3
```

Then run a short repeated, multi-process complexity-5 pilot:

```bash
python scripts/phase1_study.py run --lab config/study-lab.json --stage normal --processes 1,2 --rates 30,50 --reps 2 --duration 15 --at-seconds 3 --complexity 5 --out results/phase1/six-pc-pilot-v3
```

Inspect the separate correctness flags, latency distributions, shortfalls and
achieved rate before starting the full campaign in a new directory:

```bash
python scripts/phase1_study.py run --lab config/study-lab.json --out results/phase1/six-pc-study-v3
```

The real six-PC smoke and pilot are still necessary. Local delayed-UDP and
six-agent tests verify implementation cases; they do not certify the actual
ZeroTier network or the full 30–200 requests/s/process load range.

## Local verification completed

- All 101 Rust tests passed, including cold restart, recovery, transport,
  allocation provenance/lease/interval checks and both delayed per-request
  fault regressions. Clippy passed with warnings denied.
- The new 5/20-second per-request regressions use three independent clients,
  complexity 5 and delayed peer links. They inspect the actual leases returned
  to clients, not only the final result digest, and require real assignments,
  no interval overbooking, exercised bid batches and zero violation counters.
- The six-agent smoke plus repeated complexity-5 multi-process pilot and final
  protocol-guard checks covered 18 local runs and 14,220 requests. All completed,
  converged and passed resource checks with zero reported overcommitments or
  responder conflicts. Python statistics/gate/resume and UDP interoperability
  checks passed; reporting with zero successful requests was also checked.
- Local synthetic workloads do report many shortfalls because the finite
  asset partitions remain leased. This is recorded explicitly and is not
  counted as full resource fulfillment just because a plan was returned.

Local artifacts: [smoke](../results/phase1/six-pc-local-smoke-941d24/report.html),
[multi-process pilot](../results/phase1/six-pc-local-smoke-941d24-multiple/report.html),
[final checks](../results/phase1/six-pc-v3-final-normal/report.html).
