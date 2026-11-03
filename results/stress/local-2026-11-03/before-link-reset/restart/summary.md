# Stress results: results/stress/local-2026-11-03/restart

local: 3 nodes + all client processes on one Darwin arm64 machine, 10 cores, release build 9eae4e9

20 runs (4 levels × 5 reps, shuffled within each rep), 250 requests per process per run at 10 req/s, complexity 5, the elected server taken down by `scripts/restart_node.sh {node} {ms}` for 10000 ms at 8000 ms in every run. 125000 requests in total, 124953 answered (99.96 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 40.0 | 40.1 ± 0.01 | 100 | 7.91 ± 0.59 | 35.5 ± 18.4 | 902 ± 66.1 | 932 / 1124 | 0.68 |
| 16 | 160 | 160 ± 0.03 | 100 | 14.1 ± 0.94 | 126 ± 36.0 | 970 ± 16.6 | 963 / 1136 | 1.39 |
| 32 | 320 | 321 ± 0.16 | 100 | 13.8 ± 5.96 | 427 ± 9.54 | 1011 ± 16.0 | 1008 / 1150 | 2.85 |
| 48 | 480 | 480 ± 0.79 | 99.9 | 18.0 ± 4.49 | 653 ± 11.0 | 1041 ± 18.0 | 1038 / 1151 | 5.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 1.00 ± 0.00 | 4.00 | 101 | 987 | 0.00 | 13.4 | 0.17 | 5/5 | 2.89 ± 0.65 |
| 16 | 1.00 ± 0.00 | 4.00 | 101 | 907 | 85.6 | 65.0 | 0.19 | 5/5 | 5.04 ± 0.50 |
| 32 | 1.00 ± 0.00 | 4.00 | 101 | 893 | 622 | 251 | 0.24 | 5/5 | 7.70 ± 0.83 |
| 48 | 2.20 ± 0.56 | 8.80 | 94.3 | 918 | 2038 | 421 | 0.31 | 0/5 | – |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 4 | 1131 ± 62.2 | 1137 ± 64.0 | 7.20 ± 2.04 | 10043 ± 11.0 | 24.7 / 981 ± 67.7 / 28.1 | 0 |
| 16 | 1094 ± 3.87 | 1100 ± 4.08 | 15.8 ± 2.39 | 10036 ± 12.5 | 466 / 977 ± 16.8 / 33.8 | 0 |
| 32 | 1096 ± 7.32 | 1102 ± 5.16 | 31.8 ± 2.83 | 10050 ± 6.27 | 465 / 1055 ± 33.9 / 31.2 | 0 |
| 48 | 1092 ± 9.41 | 1075 ± 59.0 | 48.8 ± 5.30 | 10072 ± 2.57 | 70.2 / 1089 ± 15.4 / 46.2 | 47 |

Answers by serving node over the whole sweep: node 1 38.0 %, node 2 28.1 %, node 3 34.0 %.

Checks: client process exit failures: 0, peer queue drops: 1885, resource overcommits: 1, runs not converged: 5.
