#!/usr/bin/env bash
# Phase 1 stress experiments on one machine (three nodes on 127.0.0.1).
# Restarts the cluster before every run (stress --before-run), so each run starts
# from empty tables and runs are independent: without it, requests abandoned in
# an overloaded run stay pending for the table TTL and every later election
# takes them over again (docs/stress-testing.md, "Overload").
#
#   scripts/stress_local.sh [NAME]      results in results/stress/NAME (default local-YYYY-MM-DD)
#
#   1. load:    1..128 client processes × 5 reps, 100 requests each at 10 req/s
#   2. fault:   4..48 client processes × 5 reps, 250 requests each at 10 req/s,
#               the elected server muted for 10 s at t = 8 s in every run
#   3. restart: as 2, but the elected server is killed (kill -9) and started
#               again 10 s later: a cold restart (scripts/restart_node.sh)
#
# Levels and reps: LOAD_CLIENTS, LOAD_REPS, FAULT_CLIENTS, FAULT_REPS (fault and
# restart); EXTRA_ARGS goes to every sweep (e.g. "--complexity 8"); SWEEPS picks
# the sweeps (default "load fault restart"). About 50 min.
# A quick check:
#   LOAD_CLIENTS=1,8 LOAD_REPS=1 FAULT_CLIENTS=8 FAULT_REPS=1 scripts/stress_local.sh quick
# Plots (needs matplotlib): python3 scripts/plot_stress.py results/stress/NAME
set -euo pipefail
cd "$(dirname "$0")/.."
name=${1:-local-$(date +%Y-%m-%d)}
out=results/stress/$name
mkdir -p "$out"

cargo build --release --quiet
cfg=config/local/client.toml
# The commit, marked -dirty only if the code (not docs) differs from it.
build=$(git rev-parse --short HEAD 2>/dev/null || echo '?')$(git diff --quiet HEAD -- crates Cargo.toml Cargo.lock 2>/dev/null || echo '-dirty')
label="local: 3 nodes + all client processes on one $(uname -sm) machine, $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo '?') cores, release build $build"

sweep() { # sweep DIR ARGS...
  local dir=$1; shift
  mkdir -p "$out/$dir"
  rm -f logs/node*.log "$out/$dir/nodes.log"
  # Each restart truncates the node logs: keep them first.
  local keep="cat logs/node1.log logs/node2.log logs/node3.log >> '$out/$dir/nodes.log' 2>/dev/null"
  target/release/stress --config "$cfg" --out "$out/$dir" --label "$label" \
    --before-run "$keep; scripts/run_local.sh" "$@"
  sh -c "$keep" || true
  gzip -f "$out/$dir/requests.csv" "$out/$dir/nodes.log"
}

trap 'scripts/run_local.sh stop >/dev/null' EXIT
sweeps=" ${SWEEPS:-load fault restart} "
# shellcheck disable=SC2086 # EXTRA_ARGS is a word list
# Overloaded runs may not drain within 30 s; the next run restarts the cluster.
[[ $sweeps == *" load "* ]] && sweep load --clients "${LOAD_CLIENTS:-1,2,4,8,16,32,48,64,80,96,128}" --reps "${LOAD_REPS:-5}" \
  --requests 100 --rate 10 --settle-secs 30 ${EXTRA_ARGS:-}
faults=(--clients "${FAULT_CLIENTS:-4,16,32,48}" --reps "${FAULT_REPS:-5}"
  --requests 250 --rate 10 --fault-ms 10000 --fault-at-ms 8000)
# shellcheck disable=SC2086
[[ $sweeps == *" fault "* ]] && sweep fault "${faults[@]}" ${EXTRA_ARGS:-}
# shellcheck disable=SC2086
[[ $sweeps == *" restart "* ]] && sweep restart "${faults[@]}" --fault-cmd "scripts/restart_node.sh {node} {ms}" ${EXTRA_ARGS:-}
echo "results in $out"
