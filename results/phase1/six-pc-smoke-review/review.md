# Review of the six-PC ZeroTier smoke test

Source: `C:\Users\G3\Downloads\six-pc-smoke.zip`. The archive was read as data and preserved unchanged. Application code was not modified.

**Recommendation: do not start the complete main campaign yet.** Orchestration, delivery and final result-table convergence passed; per-request fault runs exposed resource-allocation correctness warnings.

All 10,440 requests were answered in all 12 runs. All three client PCs contributed to every run. Raw UUID/sequence uniqueness, empty starts, 15-result warm-ups, final cohort counts/digests, unchanged node incarnations, and p50/p95/p99/sample SD were independently verified.

## Checks that passed

- Twelve healthy independent baselines and twelve converged final result cohorts; no missing or duplicate raw request records.
- Matching reported source fingerprint on all six PCs; agent roles and addresses match the intended layout.
- All measurement, generator, intervention, incarnation and clock-alignment flags are true.
- Achieved rate is 90 requests/s in every case; dispatch p95 is about 2.1–2.3 ms and maximum launch skew is about 1.5 ms.
- Zero reported control/replication queue drops. Load injections used 12 threads, lasted about two seconds and raised the target machine CPU toward 43–45%.

## Correctness finding

| Per-request scenario | Answered | Overcommit counter increase | Responder-conflict counter increase |
|---|---:|---:|---:|
| fault5 | 810/810 | 3 | 2 |
| fault20 | 2160/2160 | 27 | 59 |

These are counter increases summed across replicas, not counts of unique incidents. Server logs explicitly report replicated plans exceeding resource capacity and responders claimed by competing requests. The counters are historical detections, not a measurement of how much capacity remained overallocated at the final snapshot.

A plausible mechanism is concurrent planners committing against temporarily different resource snapshots. This is an inference, not a proven root cause from these files. The archive does not include all plan/ledger contents needed to reconstruct every allocation. Result-table convergence does not establish resource-allocation safety.

`measurement_valid=true` validates the measurement setup; it does not require zero resource overcommitments. It must not be interpreted as an overall correctness pass.

## Normal-operation latency

| Mode | p50 ms | p95 ms | p99 ms | Sample SD ms | Retried requests |
|---|---:|---:|---:|---:|---:|
| per_request | 12.58 | 414.66 | 1060.12 | 251.95 | 22 |
| on_demand | 8.78 | 14.93 | 19.70 | 3.09 | 0 |

The first per-request normal run retried 22/450 requests, with no Busy replies, zero sampled worker queue and zero unreachable probes. The long tail should be reproduced; these samples do not establish its precise cause. Other per-request non-fault cases have much smaller p99 values.

On-demand split-winner warnings occur during the normal run warm-up, before the measured workload. The verified baseline and measured final role views agree afterward.

## Before the full campaign

1. Investigate or repair per-request resource-capacity handling during failure/recovery; distinguish detected/resolved conflicts from unsafe accepted allocations.
2. Add an explicit resource-correctness decision alongside the existing measurement-valid and convergence flags.
3. Repeat the fault smoke cases and verify overcommit findings are resolved or explicitly justified by the documented consistency model.
4. Run a short multi-process pilot at complexity 5; this smoke exercised only one process per client PC, complexity 1 and 90 cluster requests/s. Its ten-second soaks do not establish long-term behavior.

The existing main study can characterize these defects as an explicitly labelled baseline, but this review does not certify the current per-request implementation as ready for a clean reliability comparison.
