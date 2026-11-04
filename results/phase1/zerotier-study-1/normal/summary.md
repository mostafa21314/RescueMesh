# Stress results: /home/g5/Desktop/RescueMesh/results/phase1/zerotier-study-1/normal

normal; severity=3; UDP RR; Linux-7.0.0-42-generic-x86_64-with-glibc2.43

15 runs (3 levels × 5 reps, shuffled within each rep), 1800 requests per process per run at 30 req/s, complexity 5. 252000 requests in total, 225690 answered (89.56 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 120 | 120 ± 0.02 | 100 | 20.1 ± 1.77 | 114 ± 5.80 | 140 ± 11.6 | 138 / 220 | 0.00 |
| 8 | 240 | 240 ± 0.03 | 100 | 23.2 ± 4.23 | 121 ± 11.8 | 161 ± 39.1 | 162 / 250 | 0.00 |
| 16 | 480 | 450 ± 83.3 | 81.7 | 28.9 ± 2.92 | 205 ± 215 | 458 ± 783 | 189 / 537 | 18.3 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 0.00 ± 0.00 | 0.00 | – | 1445 | 0.00 | 4.60 | 0.07 | 0/5 | – |
| 8 | 0.00 ± 0.00 | 0.00 | – | 1352 | 0.00 | 16.6 | 0.15 | 0/5 | – |
| 16 | 2.20 ± 6.11 | 513 | 102 | 1015 | 598 | 3571 | 0.20 | 0/5 | – |

Answers by serving node over the whole sweep: node 2 89.3 %, node 3 10.7 %.

Checks: client process exit failures: 0, peer queue drops: 149326, resource overcommits: 429, runs not converged: 15.
