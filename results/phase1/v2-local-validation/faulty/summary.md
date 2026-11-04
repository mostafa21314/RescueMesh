# Stress results: E:\RescueMesh\results\phase1\v2-local-validation\faulty

faulty; severity=3; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 180 requests per process per run at 30 req/s, complexity 5, the elected server muted for 2000 ms at 1500 ms in every run. 7200 requests in total, 7200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 120 | 116 ± 0.00 | 100 | 5.75 ± 0.06 | 1009 ± 2.34 | 1092 ± 148 | 1093 / 1146 | 12.1 |
| 16 | 480 | 404 ± 0.00 | 100 | 9.85 ± 3.63 | 1127 ± 16.6 | 1294 ± 118 | 1293 / 1566 | 26.9 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 1.50 ± 6.35 | 7.00 | 78.3 | 2110 | 3.00 | 0.00 | 0.02 | 2/2 | 1.01 ± 1.37 |
| 16 | 2.00 ± 0.00 | 8.00 | 104 | 1683 | 690 | 64.0 | 0.06 | 2/2 | 1.06 ± 1.86 |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 4 | 1118 ± 69.9 | 1128 ± 95.3 | 7.00 ± 0.00 | 2092 ± 527 | 11.6 / 1113 ± 12.9 / 9.62 | 0 |
| 16 | 1110 ± 0.00 | 1116 ± 6.35 | 21.5 ± 6.35 | 2576 ± 940 | 14.3 / 1393 ± 66.9 / 51.2 | 0 |

Answers by serving node over the whole sweep: node 1 15.6 %, node 2 55.1 %, node 3 29.2 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
