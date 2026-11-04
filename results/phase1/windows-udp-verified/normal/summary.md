# Stress results: E:\RescueMesh\results\phase1\windows-udp-verified\normal

normal; severity=1; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 120 requests per process per run at 20 req/s, complexity 2. 1200 requests in total, 1200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 1 | 20.0 | 20.0 ± 0.04 | 100 | 7.38 ± 3.67 | 9.79 ± 2.97 | 10.5 ± 2.01 | 10.6 / 10.8 | 0.00 |
| 4 | 80.0 | 77.5 ± 1.04 | 100 | 4.99 ± 1.40 | 8.17 ± 1.07 | 9.19 ± 1.59 | 9.24 / 10.2 | 0.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.00 ± 0.00 | 0.00 | – | 2620 | 0.00 | 0.00 | 0.01 | 2/2 | 1.55 ± 3.24 |
| 4 | 0.00 ± 0.00 | 0.00 | – | 1709 | 0.00 | 0.50 | 0.01 | 2/2 | 1.48 ± 7.54 |

Answers by serving node over the whole sweep: node 1 100.0 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
