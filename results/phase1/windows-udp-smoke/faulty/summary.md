# Stress results: results\phase1\windows-udp-smoke\faulty

faulty; severity=1; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 120 requests per process per run at 20 req/s, complexity 2, the elected server muted for 2000 ms at 1500 ms in every run. 1200 requests in total, 1200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 1 | 20.0 | 20.0 ± 0.30 | 100 | 6.91 ± 7.11 | 820 ± 122 | 1067 ± 94.0 | 1075 / 1121 | 2.50 |
| 4 | 80.0 | 77.6 ± 0.32 | 100 | 3.76 ± 8.60 | 832 ± 4.26 | 1072 ± 1.40 | 1071 / 1119 | 2.50 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.00 ± 0.00 | 4.00 | 102 | 2620 | 0.00 | 0.00 | 0.03 | 2/2 | 1.21 ± 1.57 |
| 4 | 1.00 ± 0.00 | 4.00 | 109 | 1711 | 4.00 | 0.00 | 0.06 | 2/2 | 1.18 ± 2.12 |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 1 | 1141 ± 50.8 | 1158 ± 31.8 | 9.00 ± 12.7 | 2042 ± 63.5 | 9.89 / 1115 ± 77.7 / 10.0 | 0 |
| 4 | 1118 ± 0.00 | 1134 ± 19.1 | 9.50 ± 6.35 | 2017 ± 0.00 | 564 / 1079 ± 90.9 / 8.44 | 0 |

Answers by serving node over the whole sweep: node 1 22.5 %, node 2 15.2 %, node 3 62.3 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
