# Six-PC smoke v3 review

**Hold the full main campaign. Resource safety diagnostics passed; availability during a 20-second node failure did not.**

Archive: `C:\Users\G3\Downloads\six-pc-smoke-v3.zip`. Read as data, unchanged. No application code was modified.

Independently checked all 10,440 raw request records: unique client UUID/sequence pairs, all three client PCs in every case, answer/failure counts, successful-request p50/p95/p99 and sample standard deviation. Empty starts, 15-result warmups, unchanged node incarnations, matching source fingerprints, and final cohort agreement also verified.

## Availability and latency

Across 12 runs, 9,131/10,440 requests were answered (87.46%). Ten runs answered everything. Both 20-second fault runs lost requests.

| Scenario | Mode | Answered / requested | Success % | p50 ms | p95 ms | p99 ms | Sample SD ms |
|---|---|---:|---:|---:|---:|---:|---:|
| normal | per_request | 450/450 | 100.00 | 106.36 | 287.62 | 449.21 | 90.94 |
| normal | on_demand | 450/450 | 100.00 | 5.43 | 88.43 | 128.52 | 30.10 |
| fault5 | on_demand | 810/810 | 100.00 | 7.34 | 2007.88 | 2103.63 | 655.30 |
| fault5 | per_request | 810/810 | 100.00 | 105.09 | 110.33 | 114.19 | 47.89 |
| fault20 | per_request | 1143/2160 | 52.92 | 4109.70 | 7480.19 | 7955.42 | 2876.14 |
| fault20 | on_demand | 1868/2160 | 86.48 | 5.54 | 7684.46 | 7844.16 | 2215.53 |
| load_idle | on_demand | 450/450 | 100.00 | 6.03 | 8.73 | 11.44 | 1.59 |
| load_idle | per_request | 450/450 | 100.00 | 14.29 | 117.63 | 304.37 | 102.86 |
| load_busy | on_demand | 450/450 | 100.00 | 179.20 | 2957.36 | 3125.01 | 1134.90 |
| load_busy | per_request | 450/450 | 100.00 | 106.76 | 1163.65 | 2093.59 | 448.44 |
| soak | on_demand | 900/900 | 100.00 | 5.73 | 1529.10 | 2123.23 | 548.29 |
| soak | per_request | 900/900 | 100.00 | 12.05 | 101.78 | 115.94 | 26.70 |

Latency and SD above describe successful replies only. Failed per-request fault20 requests waited approximately eight seconds before exhausting the retry budget. They must remain in the availability denominator.

## Findings

- All 12 runs report resource correctness known/pass, zero resource overcommits, zero responder conflicts, and zero peer queue drops. Final allocation diagnostics independently show compatible policy/membership and zero rejection/capacity/responder violations. This supports the resource fix for this workload; it is not a proof for arbitrary failures.
- `correctness_pass=true` means final convergence and resource checks passed. It does not require 100% answered requests. All runs passing this flag does not certify availability.
- Per-request fault20: 1,017 failed requests, 18,282 UDP retransmissions, 13 delivery timeouts, sampled control queue maximum 432 and replication queue maximum 315. Logs include elections ending alone without a winner on the surviving nodes. Sampled worker queue peaked at nine and node CPU at about 3.1%. This points toward peer delivery/election progress rather than CPU saturation, but does not isolate a root cause.
- On-demand fault20: 292 failed requests, 14,683 UDP retransmissions, 13 delivery timeouts and sampled replication queue maximum 1,510. All 2,160 measured requests eventually exist in the converged cohort despite only 1,868 client successes. Computation/replication completion therefore did not ensure a timely reply.
- Per-request fault20 final cohort contains 1,188 of 2,160 requests despite 1,143 client successes: at least 45 failed-client requests obtained stored results, while 972 measured requests have no stored cohort result at the final check. Convergence means agreement on the stored subset, not completion of every submitted request.
- Five runs fail clock alignment: normal on-demand, fault20 on-demand, busy-load in both modes, and on-demand short soak. The largest estimated offset change is 154.65 ms; largest uncertainty is 219.38 ms. This can include network measurement asymmetry/jitter and does not prove a physical clock jump. Monotonic client latency remains useful; precise cross-PC fault/recovery timing needs qualification.
- Approximately 91–98% of answered plans report at least one resource/responder shortfall. Many plans contain no assignments. These are successful plan replies, not fully fulfilled incidents. The partitioned ownership policy and finite assets intentionally allow shortfalls; include this metric beside reply success.

## Next step

Investigate sustained peer delivery and election/reply progress during 20-second failures, repeat those cases, and resolve or explicitly qualify clock alignment before the full campaign. Then run a multi-process complexity-5 pilot. This smoke used only 30 requests/s per process, one process on each of three client PCs (90 total requests/s), complexity 1, and ten-second soaks; it does not establish the planned maximum-load or long-duration behavior.

The previous smoke had no unanswered requests but unsafe allocation diagnostics. V3 improves the observed safety result while this trial has worse fault availability. Different trial/network conditions and no repeated matched runs prevent attributing the regression solely to a particular code change.
