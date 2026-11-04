# Stress results: E:\RescueMesh\results\phase1\v2-final-checks\incumbent\imbalance

imbalance; severity=2; UDP RR; Windows-10-10.0.26200-SP0

1 runs (1 levels × 1 reps, shuffled within each rep), 120 requests per process per run at 30 req/s, complexity 5. 480 requests in total, 480 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 120 | 114 | 100 | 5.16 | 8.05 | 11.2 | 11.2 / 15.2 | 0.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 0.00 | 0.00 | – | 1927 | 0.00 | 0.00 | 0.35 | 1/1 | 1.05 |

Answers by serving node over the whole sweep: node 1 100.0 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
