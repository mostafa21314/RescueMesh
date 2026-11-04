# Stress results: E:\RescueMesh\results\phase1\v2-local-validation\imbalance

imbalance; severity=3; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 180 requests per process per run at 30 req/s, complexity 5. 7200 requests in total, 7200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 120 | 116 ± 2.48 | 100 | 5.21 ± 0.40 | 8.05 ± 1.21 | 10.7 ± 12.3 | 11.2 / 15.7 | 0.00 |
| 16 | 480 | 395 ± 27.9 | 100 | 7.30 ± 0.50 | 13.2 ± 1.68 | 15.6 ± 1.87 | 15.6 / 22.8 | 0.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 0.00 ± 0.00 | 0.00 | – | 1810 | 0.00 | 0.00 | 0.33 | 2/2 | 1.04 ± 0.20 |
| 16 | 0.00 ± 0.00 | 0.00 | – | 1623 | 0.00 | 5.00 | 0.33 | 2/2 | 1.17 ± 0.86 |

Answers by serving node over the whole sweep: node 2 100.0 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
