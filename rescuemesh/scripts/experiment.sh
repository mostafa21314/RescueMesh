#!/usr/bin/env bash
# Phase 1 / bullet 2 experiment: executor-selection policy under balanced and skewed load.
#
#   {hash, election} x {balanced, node 2 slowed xFACTOR} x {CONC levels}, REQS requests
#   each, on a fresh local 3-node cluster per configuration (so EWMAs and counters start
#   clean), plus one election "timeline" run where node 2 is slowed mid-run for a few
#   seconds (demo: different peers elected as load conditions change).
#
# Output: $OUT/<config>.csv (one row per request, from rm-client load), *.status.txt
# (rm-client status before/after the measured run), timeline.csv, and summary.md.
#
# Usage: scripts/experiment.sh           env: REQS=3000 CONC="6 24" WORKERS=3 FACTOR=4 OUT=results/<stamp>
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release --quiet

REQS=${REQS:-3000}
CONC=${CONC:-"6 24"}
WORKERS=${WORKERS:-3}
FACTOR=${FACTOR:-4}
OUT=${OUT:-results/$(date +%Y%m%d-%H%M%S)}
C=target/release/rm-client
mkdir -p "$OUT"
trap 'scripts/stop_local.sh >/dev/null' EXIT

start_cluster() { # $1 = assigner
  scripts/stop_local.sh >/dev/null
  sleep 0.5
  scripts/run_local.sh --workers "$WORKERS" --assigner "$1" >/dev/null
  for _ in $(seq 50); do
    [[ $($C status 2>/dev/null | grep -c 'live \[1, 2, 3\]') == 3 ]] && return 0
    sleep 0.1
  done
  echo "cluster did not come up" >&2
  exit 1
}

for conc in $CONC; do
  per=$(((REQS + conc - 1) / conc))
  for assigner in hash election; do
    for scen in balanced slow2; do
      name="${assigner}_${scen}_c${conc}"
      echo "== $name ($((per * conc)) requests)"
      start_cluster "$assigner"
      [[ $scen == slow2 ]] && $C slow --node 2 --factor "$FACTOR" --secs 0 >/dev/null
      $C load --concurrency 3 --requests 50 >/dev/null # warm-up: fills service estimates
      $C status >"$OUT/$name.status-pre.txt"
      $C load --concurrency "$conc" --requests "$per" --out "$OUT/$name.csv" >"$OUT/$name.load.txt"
      $C status >"$OUT/$name.status.txt"
      tail -2 "$OUT/$name.load.txt"
    done
  done
done

echo "== timeline (election, c=6, node 2 slowed x$FACTOR from ~3 s to ~7 s)"
start_cluster election
$C load --concurrency 3 --requests 50 >/dev/null
(sleep 3 && $C slow --node 2 --factor "$FACTOR" --secs 4 >/dev/null) &
$C load --concurrency 6 --requests 1000 --think-ms 5 --out "$OUT/timeline.csv" >"$OUT/timeline.load.txt"
wait
scripts/stop_local.sh >/dev/null

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
pctl() { # sorted values on stdin, $1 = percentile
  awk -v p="$1" '{v[NR]=$1} END{i=int(p/100*(NR-1)+0.5)+1; printf "%.2f", v[i]/1000}'
}

{
  echo "# Election experiment ($(date '+%Y-%m-%d %H:%M'), $WORKERS planner workers/node, slow factor $FACTOR)"
  echo
  echo "| config | ok/total | share n1/n2/n3 (%) | mean ms | p50 | p95 | p99 | max | req/s | dup exec |"
  echo "|---|---|---|---|---|---|---|---|---|---|"
  for f in "$OUT"/*_c*.csv; do
    name=$(basename "$f" .csv)
    lat=$(awk -F, 'NR>1 && $9=="true"{print $4}' "$f" | sort -n)
    stats=$(awk -F, 'NR>1{n++; if($9=="true"){ok++; s+=$4; c[$5]++; e=$3+$4/1000; if(e>end)end=e}
                          if(start==""||$3<start)start=$3}
      END{printf "%d/%d | %.1f/%.1f/%.1f | %.2f | %.0f", ok, n, 100*c[1]/ok, 100*c[2]/ok, 100*c[3]/ok,
          s/ok/1000, ok/((end-start)/1000)}' "$f")
    dup=$(grep -o 'duplicate_executions=[0-9]*' "$OUT/$name.status.txt" | cut -d= -f2 | sort -n | tail -1)
    tput=${stats##* | }
    echo "| $name | ${stats% | *} | $(pctl 50 <<<"$lat") | $(pctl 95 <<<"$lat") | $(pctl 99 <<<"$lat") | $(pctl 100 <<<"$lat") | $tput | ${dup:-0} |"
  done
  echo
  echo "Election cost (measured run only: post minus pre counters, summed over nodes; durations are per-node"
  echo "histograms incl. the 150 warm-up rounds, shown as the max over nodes):"
  echo
  echo "| config | elections | full / timeout | bids sent | bids/request | bid bytes/request | dur mean us | p50 | p99 | max |"
  echo "|---|---|---|---|---|---|---|---|---|---|"
  for f in "$OUT"/election_*_c*.csv; do
    name=$(basename "$f" .csv)
    reqs=$(awk -F, 'NR>1{n++} END{print n}' "$f")
    awk -v name="$name" -v reqs="$reqs" '
      function kv(line, key,   i, a, n, t) { n = split(line, a, " "); for (i = 1; i <= n; i++) { split(a[i], t, "="); if (t[1] == key) return t[2] } return 0 }
      FNR==1 { file++ }
      /election node=/ {
        sign = (file == 1) ? -1 : 1
        for (k in K) tot[K[k]] += sign * kv($0, K[k])
        if (file == 2) {
          if (kv($0, "dur_mean_us") > dm) dm = kv($0, "dur_mean_us")
          if (kv($0, "dur_p50_us") > d50) d50 = kv($0, "dur_p50_us")
          if (kv($0, "dur_p99_us") > d99) d99 = kv($0, "dur_p99_us")
          if (kv($0, "dur_max_us") > dmax) dmax = kv($0, "dur_max_us")
        }
      }
      BEGIN { split("decided full timeout bids_sent bid_bytes_sent", K, " ") }
      END {
        printf "| %s | %d | %d / %d | %d | %.2f | %.0f | %.0f | %d | %d | %d |\n", name, tot["decided"] / 3,
          tot["full"], tot["timeout"], tot["bids_sent"], tot["bids_sent"] / reqs, tot["bid_bytes_sent"] / reqs, dm, d50, d99, dmax
      }' "$OUT/$name.status-pre.txt" "$OUT/$name.status.txt"
  done
  echo
  echo "Timeline (election, c=6): share of requests executed per node, by 1 s of send time"
  echo
  echo "| second | n1 | n2 | n3 | mean ms |"
  echo "|---|---|---|---|---|"
  awk -F, 'NR>1 && $9=="true"{ if(t0==""||$3<t0)t0=$3; r[NR]=$0 }
    END{ for (i in r) { split(r[i], f, ","); b=int((f[3]-t0)/1000); c[b,f[5]]++; n[b]++; s[b]+=f[4]; if(b>mb)mb=b }
         for (b=0; b<=mb; b++) if (n[b] > 0) printf "| %d | %d | %d | %d | %.2f |\n", b, c[b,1], c[b,2], c[b,3], s[b]/n[b]/1000 }' "$OUT/timeline.csv"
} | tee "$OUT/summary.md"
