# Stress results: results\phase1\windows-udp-smoke\imbalance

imbalance; severity=1; UDP RR; Windows-10-10.0.26200-SP0

4 runs (2 levels × 2 reps, shuffled within each rep), 120 requests per process per run at 20 req/s, complexity 2. 1200 requests in total, 1200 answered (100.00 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 1 | 20.0 | 20.0 ± 0.02 | 100 | 2.22 ± 0.19 | 3.25 ± 1.70 | 3.66 ± 1.44 | 3.78 / 4.03 | 0.00 |
| 4 | 80.0 | 77.6 ± 1.28 | 100 | 2.17 ± 0.10 | 3.17 ± 0.34 | 4.52 ± 1.68 | 4.65 / 5.55 | 0.00 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.00 ± 0.00 | 0.00 | – | 2620 | 0.00 | 0.50 | 0.15 | 2/2 | 0.98 ± 0.11 |
| 4 | 0.00 ± 0.00 | 0.00 | – | 1710 | 0.00 | 0.00 | 0.13 | 2/2 | 1.04 ± 1.66 |

Answers by serving node over the whole sweep: node 1 10.0 %, node 2 50.0 %, node 3 40.0 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 0.
