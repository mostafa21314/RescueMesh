# Stress results: /home/g5/Desktop/RescueMesh/results/phase1/zerotier-study-1/faulty

faulty; severity=3; UDP RR; Linux-7.0.0-42-generic-x86_64-with-glibc2.43

15 runs (3 levels × 5 reps, shuffled within each rep), 1800 requests per process per run at 30 req/s, complexity 5, the elected server muted for 20000 ms at 15000 ms in every run. 252000 requests in total, 83512 answered (33.14 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 4 | 120 | 103 ± 42.2 | 71.6 | 21.4 ± 5.38 | 3015 ± 4922 | 3589 ± 4983 | 7707 / 8225 | 37.8 |
| 8 | 240 | 218 ± 63.9 | 30.1 | 50.8 ± 85.9 | 1615 ± 4135 | 1729 ± 4292 | 7740 / 7978 | 74.9 |
| 16 | 480 | 482 ± 1.69 | 25.1 | 24.8 ± 4.82 | 193 ± 151 | 237 ± 160 | 409 / 486 | 74.9 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 4 | 6.60 ± 6.49 | 668 | 101 | 3149 | 562 | 19577 | 0.07 | 0/5 | – |
| 8 | 11.8 ± 3.33 | 1537 | 101 | 6741 | 2644 | 34612 | 0.09 | 0/5 | – |
| 16 | 11.4 ± 2.26 | 1492 | 101 | 4314 | 6498 | 39489 | 0.10 | 0/5 | – |

## Injected failure of the elected server

| clients | failover ms | longest gap without answers ms | recovery ms | out of service ms | p99 before / during / after ms | unanswered (window) |
|---|---|---|---|---|---|---|
| 4 | 1556 ± 1437 | 6783 ± 11375 | 3942 ± 7571 | 23954 ± 7571 | 144 / 4650 ± 6411 / 2419 | 6622 |
| 8 | 6157 | 3822 ± 10186 | 8518 ± 5638 | 28543 ± 5647 | 174 / 7976 / 7948 | 32891 |
| 16 | – | 140 ± 7.78 | 7742 ± 7164 | 27781 ± 7149 | 237 / – / – | 66684 |

Answers by serving node over the whole sweep: node 1 47.1 %, node 2 29.3 %, node 3 23.5 %.

Checks: client process exit failures: 0, peer queue drops: 581629, resource overcommits: 6230, runs not converged: 15.
