# Phase 1 UDP reliability report

| Scenario | Clients | Runs | Requests | Success % [Wilson 95%] | p50 / p95 / p99 ms | Goodput ± CI | Converged | Faults injected/requested | Missing | Exit failures | Peer drops / overcommits |
|---|---|---|---|---|---|---|---|---|---|---|---|
| faulty | 1 | 2 | 240 | 100.00 [98.42, 100.00] | 7.46 / 822.60 / 1070.15 | 19.79 ± 0.04 | 2/2 | 2/2 | 0 | 0 | 0 / 0 |
| faulty | 4 | 2 | 960 | 100.00 [99.60, 100.00] | 5.65 / 834.61 / 1077.00 | 76.45 ± 0.42 | 2/2 | 2/2 | 0 | 0 | 0 / 0 |
| imbalance | 1 | 2 | 240 | 100.00 [98.42, 100.00] | 2.22 / 3.09 / 3.71 | 19.82 ± 0.14 | 2/2 | 0/0 | 0 | 0 | 0 / 0 |
| imbalance | 4 | 2 | 960 | 100.00 [99.60, 100.00] | 2.12 / 3.19 / 4.43 | 76.28 ± 0.67 | 2/2 | 0/0 | 0 | 0 | 0 / 0 |
| normal | 1 | 2 | 240 | 100.00 [98.42, 100.00] | 7.29 / 9.91 / 10.64 | 19.79 ± 0.03 | 2/2 | 0/0 | 0 | 0 | 0 / 0 |
| normal | 4 | 2 | 960 | 100.00 [99.60, 100.00] | 5.05 / 8.20 / 9.24 | 76.31 ± 0.19 | 2/2 | 0/0 | 0 | 0 | 0 / 0 |

## Load and transport diagnostics

| Scenario | Clients | Retried requests | Busy replies | Peak queue | UDP retransmissions | UDP delivery timeouts | Peak node RSS MiB |
|---|---|---|---|---|---|---|---|
| faulty | 1 | 6 | 0 | 0 | 0 | 0 | 28.25 |
| faulty | 4 | 24 | 4 | 0 | 0 | 0 | 28.66 |
| imbalance | 1 | 0 | 0 | 0 | 0 | 0 | 27.86 |
| imbalance | 4 | 0 | 0 | 0 | 0 | 0 | 28.30 |
| normal | 1 | 0 | 0 | 0 | 0 | 0 | 27.64 |
| normal | 4 | 0 | 0 | 1 | 0 | 0 | 28.34 |

## Interpretation

- Latency distributions include successful replies only; failed and missing operations remain in the success-rate denominator.
- Goodput includes process startup, retries and waiting for failures to exhaust. The older throughput_rps column measures the spacing between successful replies.
- 95% Student-t intervals describe variation across repeated runs. N/A means fewer than two runs. Fresh local clusters isolate repetitions; remote clusters retain history unless explicitly restarted.
- Wilson intervals are an approximate binomial reference; requests share servers and are correlated, so they do not establish real-world reliability. Zero observed failures is not proof of zero failure probability.
- A requested fault without a recorded victim is an invalid fault experiment. Inspect client exit failures, missing records, peer queue drops, resource overcommits and convergence failures.
- Convergence means equal retained result counts/digests after processing becomes idle. It is separate from client success and from the returning node becoming Ready.
- Node counters are process-wide and include diagnostics/ACKs. Retransmissions indicate missing acknowledgements, which may be caused by data loss, ACK loss or delayed processing.
- These are measurements of the named machines, workload and network only. Multi-PC tests should use synchronized clocks; client latency itself uses a monotonic clock.

## Figures

![comparison](plots/comparison.png)
![distribution-recovery](plots/distribution-recovery.png)
![faulty-distribution](plots/faulty-distribution.png)
![faulty-timeline](plots/faulty-timeline.png)
![imbalance-distribution](plots/imbalance-distribution.png)
![imbalance-timeline](plots/imbalance-timeline.png)
![normal-distribution](plots/normal-distribution.png)
![normal-timeline](plots/normal-timeline.png)
