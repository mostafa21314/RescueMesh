# Stress results: E:\RescueMesh\results\phase1\v2-final-checks\fault20\faulty

faulty; severity=2; UDP RR; Windows-10-10.0.26200-SP0

1 runs (1 levels × 1 reps, shuffled within each rep), 840 requests per process per run at 30 req/s, complexity 5, the elected server muted for 20000 ms at 5000 ms in every run. 3360 requests in total, 3360 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 120 | 119 | 100 | 5.42 | 12.1 | 1010 | 1010 / 1122 | 2.65 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 1.00 | 4.00 | 115 | 1658 | 4.00 | 1.00 | 0.03 | 1/1 | 1.32 |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 4 | 1121 | 1140 | 56.0 | 20093 | 10.6 / 1015 / 11.0 | 0 |

Answers by serving node over the whole sweep: node 1 17.2 %, node 2 82.8 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
