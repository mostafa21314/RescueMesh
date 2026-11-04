# Stress results: /home/g5/Desktop/RescueMesh/results/phase1/zerotier-study-1/imbalance

imbalance; severity=3; UDP RR; Linux-7.0.0-42-generic-x86_64-with-glibc2.43

15 runs (3 levels × 5 reps, shuffled within each rep), 1800 requests per process per run at 30 req/s, complexity 5. 252000 requests in total, 252000 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 120 | 120 ± 0.08 | 100 | 19.2 ± 0.41 | 114 ± 4.18 | 146 ± 16.4 | 141 / 250 | 0.00 |
| 8 | 240 | 240 ± 0.10 | 100 | 24.6 ± 1.20 | 118 ± 3.04 | 147 ± 13.2 | 145 / 215 | 0.00 |
| 16 | 480 | 480 ± 0.30 | 100 | 27.2 ± 2.29 | 133 ± 8.47 | 191 ± 27.1 | 196 / 257 | 0.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 0.00 ± 0.00 | 0.00 | – | 1444 | 0.00 | 4.60 | 0.52 | 0/5 | – |
| 8 | 0.00 ± 0.00 | 0.00 | – | 1318 | 0.00 | 23.6 | 0.52 | 0/5 | – |
| 16 | 0.00 ± 0.00 | 0.00 | – | 668 | 0.00 | 55.6 | 0.52 | 0/5 | – |

Answers by serving node over the whole sweep: node 2 100.0 %.

Checks: client process exit failures: 0, peer queue drops: 159012, resource overcommits: 0, runs not converged: 15.
