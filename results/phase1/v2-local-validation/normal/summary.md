# Stress results: E:\RescueMesh\results\phase1\v2-local-validation\normal

normal; severity=3; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 180 requests per process per run at 30 req/s, complexity 5. 7200 requests in total, 7200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 120 | 116 ± 2.38 | 100 | 5.27 ± 0.97 | 7.45 ± 1.14 | 10.1 ± 4.22 | 10.4 / 16.1 | 0.00 |
| 16 | 480 | 404 ± 0.36 | 100 | 6.31 ± 0.24 | 12.5 ± 6.78 | 14.4 ± 2.68 | 14.6 / 20.9 | 0.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 0.00 ± 0.00 | 0.00 | – | 1810 | 0.00 | 0.00 | 0.02 | 2/2 | 1.83 ± 3.85 |
| 16 | 0.00 ± 0.00 | 0.00 | – | 1621 | 0.00 | 5.50 | 0.07 | 2/2 | 1.17 ± 0.64 |

Answers by serving node over the whole sweep: node 1 100.0 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
