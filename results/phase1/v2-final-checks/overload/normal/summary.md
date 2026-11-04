# Stress results: E:\RescueMesh\results\phase1\v2-final-checks\overload\normal

normal; severity=2; UDP RR; Windows-10-10.0.26200-SP0

1 runs (1 levels × 1 reps, shuffled within each rep), 300 requests per process per run at 100 req/s, complexity 1000. 4800 requests in total, 22 answered (0.46 %). ± is the 95 % confidence half-width over the runs of a level (Student t); pooled percentiles are over every request of the level.

## Latency and throughput

| clients | offered req/s | throughput req/s | answered % | p50 ms | p95 ms | p99 ms | pooled p99 / p99.9 ms | retried % |
|---|---|---|---|---|---|---|---|---|
| 16 | 1600 | 3.26 | 0.46 | 3004 | 6146 | 7319 | 7319 / 7319 | 100 |

## Coordination overhead and node state

| clients | elections / run | ElectBid frames / run | election ms | Replicate bytes / answer | suppressed triggers / run | max queue | CPU (EWMA) | converged | convergence ms |
|---|---|---|---|---|---|---|---|---|---|
| 16 | 9.00 | 30.0 | 58.5 | 9104 | 29039 | 64.0 | 0.13 | 0/1 | – |

Answers by serving node over the whole sweep: node 1 40.9 %, node 2 36.4 %, node 3 22.7 %.

Checks: client process exit failures: 0, peer queue drops: 0, resource overcommits: 0, runs not converged: 1.
