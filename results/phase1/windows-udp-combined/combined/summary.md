# Stress results: E:\RescueMesh\results\phase1\windows-udp-combined\combined

combined; severity=3; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 600 requests per process per run at 100 req/s, complexity 5, the elected server muted for 2000 ms at 1500 ms in every run. 24000 requests in total, 11885 answered (49.52 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 400 | 385 ± 0.78 | 100 | 6.26 ± 1.02 | 954 ± 72.3 | 1079 ± 44.4 | 1079 / 1111 | 12.3 |
| 16 | 1600 | 466 ± 11.0 | 36.9 | 1652 ± 26.3 | 3343 ± 130 | 3788 ± 4135 | 4100 / 4134 | 91.8 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 2.00 ± 0.00 | 10.0 | 41.4 | 1441 | 1100 | 298 | 0.11 | 2/2 | 1.24 ± 0.38 |
| 16 | 9.00 ± 0.00 | 50.0 | 17.1 | 3761 | 47916 | 4804 | 0.12 | 2/2 | 1377 ± 529 |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 4 | 1110 ± 82.6 | 1118 ± 31.8 | 26.0 ± 38.1 | 2038 ± 63.5 | 570 / 1092 ± 88.5 / 13.7 | 0 |
| 16 | 1038 ± 6.35 | 782 ± 807 | 67.5 ± 19.1 | 2082 ± 12.7 | 1474 / 4116 ± 50.5 / 3420 | 2448 |

Answers by serving node over the whole sweep: node 1 32.3 %, node 2 36.4 %, node 3 31.4 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
