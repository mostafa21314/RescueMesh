# Six-PC election comparison

12 completed runs.

| Stage / scenario | Mode | Processes/PC | Rate/process | Runs valid / total | Success % | p50 / p95 / p99 ms | Mean within-run latency SD ms | Goodput mean ± SD | Converged |
|---|---|---|---|---|---|---|---|---|---|
| normal/normal | on_demand | 1 | 30 | 1/1 | 100.00 | 4.22 / 5.99 / 8.14 | 1.58 | 90.00 ± N/A | 1/1 |
| normal/normal | per_request | 1 | 30 | 1/1 | 100.00 | 5.03 / 7.25 / 10.00 | 1.97 | 90.00 ± N/A | 1/1 |
| resilience/fault20 | on_demand | 1 | 30 | 1/1 | 100.00 | 4.37 / 10.05 / 1006.49 | 185.86 | 90.00 ± N/A | 1/1 |
| resilience/fault20 | per_request | 1 | 30 | 1/1 | 100.00 | 105.88 / 120.65 / 122.52 | 38.94 | 90.00 ± N/A | 1/1 |
| resilience/fault5 | on_demand | 1 | 30 | 1/1 | 100.00 | 4.72 / 1004.54 / 1050.17 | 289.65 | 90.00 ± N/A | 1/1 |
| resilience/fault5 | per_request | 1 | 30 | 1/1 | 100.00 | 103.44 / 119.55 / 121.90 | 51.38 | 90.00 ± N/A | 1/1 |
| resilience/load_busy | on_demand | 1 | 30 | 1/1 | 100.00 | 1.62 / 5.74 / 7.16 | 1.77 | 90.00 ± N/A | 1/1 |
| resilience/load_busy | per_request | 1 | 30 | 1/1 | 100.00 | 2.89 / 7.02 / 7.56 | 2.20 | 90.00 ± N/A | 1/1 |
| resilience/load_idle | on_demand | 1 | 30 | 1/1 | 100.00 | 1.69 / 6.71 / 8.62 | 2.24 | 90.00 ± N/A | 1/1 |
| resilience/load_idle | per_request | 1 | 30 | 1/1 | 100.00 | 2.21 / 7.63 / 9.28 | 2.48 | 90.00 ± N/A | 1/1 |
| soak/soak | on_demand | 1 | 30 | 1/1 | 100.00 | 1.69 / 5.38 / 6.76 | 1.27 | 90.00 ± N/A | 1/1 |
| soak/soak | per_request | 1 | 30 | 1/1 | 100.00 | 6.07 / 7.77 / 9.38 | 1.67 | 90.00 ± N/A | 1/1 |

## Measurement definitions

- Rates are per process; processes are per client PC. Total offered load = 3 × processes/PC × rate/process.
- Latency describes successful requests. Failed and missing requests remain in the availability denominator; failed elapsed times are recorded separately.
- lat_sd_ms is sample SD within a run (n−1). Summary SD/CI describe independent repetitions; p50/p95/p99 means are means of run percentiles, not pooled percentiles. Fewer than two observations/runs gives N/A for SD/CI.
- Percentiles use the nearest sorted index round((n−1)×p), matching the existing Rust client. CDF figures use histogram bins; exact run percentiles are in runs.csv.
- CI uses conservative two-sided Student-t 95% intervals. Shared machines/network still limit generalization beyond this experiment.
- A measurement-invalid run is retained and shown. Generator-invalid means missing scheduled sends, <95% dispatched within the workload window, or dispatch p95 >100 ms. Inspect achieved_send_rps before interpreting server capacity.
- Paired differences use only measurement-valid pairs with the same condition, seed and repetition. Positive differences mean per_request minus on_demand.
- Every independent run restarts all servers and requires an empty healthy baseline, then converged warm-up. Nonconvergent runs remain failures; the next run must establish a fresh baseline.
- Soak observations are continuous operation and are analyzed separately. A common sustainable normal point is required before soak execution.
- Election decisions/durations are summed across node decisions, not unique logical elections. Per-request elections do not change the persistent-server epoch.
- Fault target is the incumbent in on_demand, or the most active measured worker in per_request. First-survivor latency requires a request submitted after acknowledged injection and computed by a non-victim.
- Recovery time excludes the requested mute interval and includes polling uncertainty. Convergence time is an observed upper bound after client completion; collection overlaps provisional convergence polling.
- Wall-clock alignment uses measured agent offsets. Inspect uncertainty and offset drift before interpreting cross-PC intervention windows; request latency and dispatch lag use monotonic clocks.
- Repair lag uses the latest peer revision observed, so zero known lag while disconnected does not prove convergence. Host network counters can include both virtual and physical interfaces.
- Server logging defaults to warn in both modes to avoid per-request election log I/O dominating the comparison. Node counters, full snapshots, warnings and raw samples are retained.

## Figures

![normal-success_pct_mean](plots/normal-success_pct_mean.png)
![normal-lat_p99_ms_mean](plots/normal-lat_p99_ms_mean.png)
![comparison-goodput_rps](plots/comparison-goodput_rps.png)
![comparison-lat_p50_ms](plots/comparison-lat_p50_ms.png)
![comparison-lat_p95_ms](plots/comparison-lat_p95_ms.png)
![comparison-lat_p99_ms](plots/comparison-lat_p99_ms.png)
![comparison-electbid_messages_per_request](plots/comparison-electbid_messages_per_request.png)
![comparison-fairness_jain](plots/comparison-fairness_jain.png)
![fault20-cdf](plots/fault20-cdf.png)
![fault5-cdf](plots/fault5-cdf.png)
![load_busy-cdf](plots/load_busy-cdf.png)
![load_idle-cdf](plots/load_idle-cdf.png)
![normal-cdf](plots/normal-cdf.png)
![soak-cdf](plots/soak-cdf.png)
![fault20-on_demand-timeline](plots/fault20-on_demand-timeline.png)
![fault20-on_demand-host-cpu](plots/fault20-on_demand-host-cpu.png)
![fault20-per_request-timeline](plots/fault20-per_request-timeline.png)
![fault20-per_request-host-cpu](plots/fault20-per_request-host-cpu.png)
![fault5-on_demand-timeline](plots/fault5-on_demand-timeline.png)
![fault5-on_demand-host-cpu](plots/fault5-on_demand-host-cpu.png)
![fault5-per_request-timeline](plots/fault5-per_request-timeline.png)
![fault5-per_request-host-cpu](plots/fault5-per_request-host-cpu.png)
![load_busy-on_demand-timeline](plots/load_busy-on_demand-timeline.png)
![load_busy-on_demand-host-cpu](plots/load_busy-on_demand-host-cpu.png)
![load_busy-per_request-timeline](plots/load_busy-per_request-timeline.png)
![load_busy-per_request-host-cpu](plots/load_busy-per_request-host-cpu.png)
![load_idle-on_demand-timeline](plots/load_idle-on_demand-timeline.png)
![load_idle-on_demand-host-cpu](plots/load_idle-on_demand-host-cpu.png)
![load_idle-per_request-timeline](plots/load_idle-per_request-timeline.png)
![load_idle-per_request-host-cpu](plots/load_idle-per_request-host-cpu.png)
![normal-on_demand-timeline](plots/normal-on_demand-timeline.png)
![normal-on_demand-host-cpu](plots/normal-on_demand-host-cpu.png)
![normal-per_request-timeline](plots/normal-per_request-timeline.png)
![normal-per_request-host-cpu](plots/normal-per_request-host-cpu.png)
![soak-on_demand-timeline](plots/soak-on_demand-timeline.png)
![soak-on_demand-host-cpu](plots/soak-on_demand-host-cpu.png)
![soak-per_request-timeline](plots/soak-per_request-timeline.png)
![soak-per_request-host-cpu](plots/soak-per_request-host-cpu.png)
