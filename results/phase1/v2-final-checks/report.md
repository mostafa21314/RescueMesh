# Phase 1 UDP reliability report

| Scenario | Clients | Runs | Requests | Success % [Wilson 95%] | p50 / p95 / p99 ms | Goodput ± CI | Converged | Baseline verified / failed / unknown | Faults injected/requested | Missing | Exit failures | Peer drops / overcommits |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fault20-faulty | 4 | 1 | 3360 | 100.00 [99.89, 100.00] | 5.42 / 12.14 / 1009.89 | 118.70 ± N/A | 1/1 | 1 / 0 / 0 | 1/1 | 0 | 0 | 0 / 0 |
| follower-imbalance | 4 | 1 | 480 | 100.00 [99.21, 100.00] | 5.22 / 8.03 / 9.28 | 111.05 ± N/A | 1/1 | 1 / 0 / 0 | 0/0 | 0 | 0 | 0 / 0 |
| incumbent-imbalance | 4 | 1 | 480 | 100.00 [99.21, 100.00] | 5.16 / 8.05 / 11.20 | 110.63 ± N/A | 1/1 | 1 / 0 / 0 | 0/0 | 0 | 0 | 0 / 0 |
| overload-normal | 16 | 1 | 4800 | 0.46 [0.30, 0.69] | 3004.39 / 6145.83 / 7318.56 | 2.01 ± N/A | 0/1 | 1 / 0 / 0 | 0/0 | 0 | 0 | 0 / 0 |

## Load and transport diagnostics

| Scenario | Clients | Retried requests | Busy replies | Peak queue | UDP retransmissions | UDP delivery timeouts | Peak node RSS MiB | Control / replication drops | Repaired results | Expired jobs | Worker queue wait ms | Failed elapsed p95 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fault20-faulty | 4 | 89 | 2 | 1 | 0 | 0 | 31.79 | 0 / 0 | 2409 | 0 | 1.23 | N/A |
| follower-imbalance | 4 | 0 | 0 | 0 | 0 | 0 | 28.96 | 0 / 0 | 0 | 0 | 0.02 | N/A |
| incumbent-imbalance | 4 | 0 | 0 | 0 | 0 | 0 | 29.29 | 0 / 0 | 0 | 0 | 0.02 | N/A |
| overload-normal | 16 | 4800 | 32922 | 64 | 264 | 40 | 31.22 | 0 / 0 | 6 | 7 | 3400.75 | 5184.13 |

## Interpretation

- Latency distributions include successful replies only; failed and missing operations remain in the success-rate denominator.
- Goodput includes process startup, retries and waiting for failures to exhaust. The older throughput_rps column measures the spacing between successful replies.
- 95% Student-t intervals describe variation across repeated runs. N/A means fewer than two runs. Fresh local clusters isolate repetitions; remote clusters retain history unless explicitly restarted.
- Wilson intervals are an approximate binomial reference; requests share servers and are correlated, so they do not establish real-world reliability. Zero observed failures is not proof of zero failure probability.
- A requested fault without a recorded victim is an invalid fault experiment. Inspect client exit failures, missing records, peer queue drops, resource overcommits and convergence failures.
- Protocol 2 convergence compares the current run client UUIDs after processing is idle, including at least every answered result. Legacy runs compare whole retained tables. See convergence_scope in reliability.csv.
- A verified baseline requires matching retention-safe digests and role views. Baseline unknown means legacy data; a failed baseline indicates explicit soak mode. Shared remote history still prevents claiming fresh independent repetitions.
- Protocol 2 writes baseline/final node JSON, preserves failed-request elapsed times and client UUIDs, and records split control/replication drops, repairs and worker queue waits. Repairs and drops are per-node counters rather than counts of unique incidents.
- Repair lag compares installed cursors with the last revisions observed from peers; it cannot reveal updates created while a peer is unreachable. Zero known lag alone does not establish convergence.
- Node counters are process-wide and include diagnostics/ACKs. Retransmissions indicate missing acknowledgements, which may be caused by data loss, ACK loss or delayed processing.
- These are measurements of the named machines, workload and network only. Multi-PC tests should use synchronized clocks; client latency itself uses a monotonic clock.

## Figures

![comparison](plots/comparison.png)
![distribution-recovery](plots/distribution-recovery.png)
![fault20-faulty-distribution](plots/fault20-faulty-distribution.png)
![fault20-faulty-timeline](plots/fault20-faulty-timeline.png)
![fault20-faulty-peer-queues](plots/fault20-faulty-peer-queues.png)
![follower-imbalance-distribution](plots/follower-imbalance-distribution.png)
![follower-imbalance-timeline](plots/follower-imbalance-timeline.png)
![follower-imbalance-peer-queues](plots/follower-imbalance-peer-queues.png)
![incumbent-imbalance-distribution](plots/incumbent-imbalance-distribution.png)
![incumbent-imbalance-timeline](plots/incumbent-imbalance-timeline.png)
![incumbent-imbalance-peer-queues](plots/incumbent-imbalance-peer-queues.png)
![overload-normal-distribution](plots/overload-normal-distribution.png)
![overload-normal-timeline](plots/overload-normal-timeline.png)
![overload-normal-peer-queues](plots/overload-normal-peer-queues.png)
