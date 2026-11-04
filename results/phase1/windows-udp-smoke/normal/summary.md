# Stress results: results\phase1\windows-udp-smoke\normal

normal; severity=1; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 120 requests per process per run at 20 req/s, complexity 2. 1200 requests in total, 1200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 1 | 20.0 | 20.0 ± 0.04 | 100 | 6.89 ± 4.73 | 9.72 ± 1.06 | 10.8 ± 3.82 | 10.9 / 12.0 | 0.00 |
| 4 | 80.0 | 77.6 ± 0.16 | 100 | 5.25 ± 1.08 | 7.98 ± 3.21 | 8.83 ± 4.80 | 8.94 / 10.3 | 0.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.00 ± 0.00 | 0.00 | – | 2620 | 0.00 | 0.00 | 0.07 | 2/2 | 3.52 ± 1.55 |
| 4 | 0.00 ± 0.00 | 0.00 | – | 1709 | 0.00 | 0.00 | 0.07 | 2/2 | 2.25 ± 13.3 |

Answers by serving node over the whole sweep: node 1 100.0 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
