# Stress results: results/stress/local-2026-11-03/restart

local: 3 nodes + all client processes on one Darwin arm64 machine, 10 cores, release build 08206b5

20 runs (4 levels × 5 reps, shuffled within each rep), 250 requests per process per run at 10 req/s, complexity 5, the elected server taken down by `scripts/restart_node.sh {node} {ms}` for 10000 ms at 8000 ms in every run. 125000 requests in total, 124963 answered (99.97 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 40.0 | 40.1 ± 0.01 | 100 | 8.35 ± 0.18 | 30.8 ± 2.38 | 862 ± 52.8 | 857 / 1116 | 0.60 |
| 16 | 160 | 160 ± 0.05 | 100 | 14.2 ± 0.57 | 135 ± 35.7 | 973 ± 10.3 | 970 / 1138 | 1.41 |
| 32 | 320 | 321 ± 0.12 | 100 | 15.7 ± 1.04 | 432 ± 19.0 | 1019 ± 5.92 | 1019 / 1156 | 2.96 |
| 48 | 480 | 481 ± 0.12 | 99.9 | 15.1 ± 6.57 | 662 ± 28.4 | 1047 ± 13.3 | 1046 / 1154 | 4.91 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 1.00 ± 0.00 | 4.00 | 101 | 703 | 0.00 | 0.00 | 0.16 | 5/5 | 2.54 ± 0.39 |
| 16 | 1.00 ± 0.00 | 4.00 | 101 | 639 | 88.2 | 68.2 | 0.20 | 5/5 | 5.24 ± 0.86 |
| 32 | 1.40 ± 0.68 | 4.80 | 101 | 629 | 640 | 257 | 0.26 | 5/5 | 8.76 ± 1.83 |
| 48 | 2.00 ± 0.00 | 6.00 | 101 | 626 | 1998 | 426 | 0.31 | 5/5 | 10.1 ± 0.48 |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 4 | 1090 ± 6.60 | 1101 ± 5.30 | 7.20 ± 3.21 | 10033 ± 16.7 | 27.2 / 937 ± 12.5 / 27.9 | 0 |
| 16 | 1094 ± 6.97 | 1100 ± 6.14 | 15.0 ± 0.88 | 10037 ± 12.4 | 681 / 980 ± 10.9 / 35.0 | 0 |
| 32 | 1093 ± 4.93 | 1098 ± 4.44 | 29.4 ± 1.88 | 10048 ± 2.57 | 908 / 1041 ± 32.4 / 34.6 | 0 |
| 48 | 1094 ± 4.73 | 1101 ± 2.91 | 49.4 ± 7.98 | 10070 ± 2.96 | 53.9 / 1096 ± 9.12 / 41.1 | 37 |

Answers by serving node over the whole sweep: node 1 30.8 %, node 2 35.4 %, node 3 33.8 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
