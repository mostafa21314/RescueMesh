# Stress results: E:\RescueMesh\results\phase1\windows-udp-verified\imbalance

imbalance; severity=1; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 120 requests per process per run at 20 req/s, complexity 2. 1200 requests in total, 1200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 1 | 20.0 | 20.0 ± 0.02 | 100 | 2.21 ± 0.51 | 3.03 ± 0.98 | 3.92 ± 3.38 | 3.71 / 4.21 | 0.00 |
| 4 | 80.0 | 77.5 ± 0.08 | 100 | 2.12 ± 0.15 | 3.24 ± 1.64 | 4.54 ± 1.55 | 4.43 / 5.88 | 0.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.00 ± 0.00 | 0.00 | – | 2620 | 0.00 | 0.00 | 0.08 | 2/2 | 0.99 ± 1.24 |
| 4 | 0.00 ± 0.00 | 0.00 | – | 1710 | 0.00 | 0.00 | 0.08 | 2/2 | 1.04 ± 1.93 |

Answers by serving node over the whole sweep: node 2 100.0 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
