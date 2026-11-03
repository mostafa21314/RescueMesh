# Stress results: results/stress/local-2026-11-03/fault

local: 3 nodes + all client processes on one Darwin arm64 machine, 10 cores, release build 08206b5

20 runs (4 levels × 5 reps, shuffled within each rep), 250 requests per process per run at 10 req/s, complexity 5, the elected server muted for 10000 ms at 8000 ms in every run. 125000 requests in total, 124941 answered (99.95 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 40.0 | 40.1 ± 0.01 | 100 | 7.75 ± 0.46 | 43.1 ± 26.6 | 937 ± 9.01 | 940 / 1119 | 0.80 |
| 16 | 160 | 160 ± 0.04 | 100 | 13.1 ± 1.20 | 130 ± 34.0 | 974 ± 14.9 | 975 / 1144 | 1.53 |
| 32 | 320 | 321 ± 0.12 | 100 | 16.6 ± 1.83 | 441 ± 8.33 | 1022 ± 15.6 | 1019 / 1155 | 3.02 |
| 48 | 480 | 480 ± 0.47 | 99.9 | 16.3 ± 5.53 | 690 ± 33.6 | 1050 ± 20.4 | 1046 / 1161 | 5.48 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 1.00 ± 0.00 | 4.00 | 101 | 1577 | 0.00 | 0.00 | 0.18 | 5/5 | 3.52 ± 1.47 |
| 16 | 1.00 ± 0.00 | 4.00 | 101 | 1368 | 95.2 | 65.2 | 0.22 | 5/5 | 4.88 ± 0.18 |
| 32 | 1.20 ± 0.56 | 4.80 | 101 | 1331 | 674 | 259 | 0.26 | 5/5 | 7.91 ± 0.53 |
| 48 | 2.60 ± 0.68 | 11.2 | 83.9 | 1320 | 2172 | 424 | 0.32 | 5/5 | 10.3 ± 0.19 |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 4 | 1110 ± 1.11 | 1197 ± 6.47 | 8.80 ± 2.69 | 10025 ± 1.84 | 23.5 / 1036 ± 6.27 / 26.8 | 0 |
| 16 | 1114 ± 8.47 | 1188 ± 7.70 | 26.0 ± 2.48 | 10035 ± 13.1 | 28.8 / 1062 ± 14.6 / 32.9 | 0 |
| 32 | 1114 ± 3.51 | 1173 ± 2.91 | 36.6 ± 5.16 | 10045 ± 2.54 | 35.9 / 1097 ± 14.4 / 34.7 | 0 |
| 48 | 1110 ± 2.04 | 1155 ± 3.21 | 52.2 ± 4.60 | 10063 ± 10.9 | 40.7 / 1112 ± 16.3 / 82.9 | 59 |

Answers by serving node over the whole sweep: node 1 40.8 %, node 2 41.6 %, node 3 17.6 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
