# Stress results: E:\RescueMesh\results\phase1\windows-udp-verified\faulty

faulty; severity=1; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 120 requests per process per run at 20 req/s, complexity 2, the elected server muted for 2000 ms at 1500 ms in every run. 1200 requests in total, 1200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 1 | 20.0 | 20.0 ± 0.02 | 100 | 7.44 ± 2.03 | 822 ± 3.95 | 1067 ± 40.3 | 1070 / 1119 | 2.50 |
| 4 | 80.0 | 77.6 ± 0.16 | 100 | 5.35 ± 12.0 | 832 ± 31.6 | 1077 ± 3.51 | 1077 / 1126 | 2.50 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.00 ± 0.00 | 4.00 | 101 | 2620 | 0.00 | 0.00 | 0.00 | 2/2 | 3.66 ± 6.71 |
| 4 | 1.00 ± 0.00 | 4.00 | 105 | 1713 | 4.00 | 0.00 | 0.01 | 2/2 | 2.79 ± 21.6 |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 1 | 1144 ± 57.2 | 1160 ± 6.35 | 8.50 ± 44.5 | 2046 ± 19.1 | 10.4 / 1116 ± 43.8 / 11.0 | 0 |
| 4 | 1132 ± 108 | 1138 ± 25.4 | 11.0 ± 12.7 | 2040 ± 69.9 | 566 / 1092 ± 178 / 8.97 | 0 |

Answers by serving node over the whole sweep: node 1 22.5 %, node 2 77.5 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
