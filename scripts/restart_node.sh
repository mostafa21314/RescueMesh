#!/usr/bin/env bash
# Crash one local node (started by scripts/run_local.sh) and start it again
# after MS milliseconds: kill -9, so it loses everything in memory, then a
# cold start that must rebuild its state from its peers before participating.
#
#   scripts/restart_node.sh N [MS]      e.g. scripts/restart_node.sh 2 10000
#
# Used by scripts/stress_local.sh (stress --fault-cmd) for the restart sweep.
set -euo pipefail
cd "$(dirname "$0")/.."
i=$1
ms=${2:-0}
kill -9 "$(cat "logs/node$i.pid")"
sleep "$(awk "BEGIN { print $ms / 1000 }")"
RUST_LOG=${RUST_LOG:-info} target/release/cloud-node --config "config/local/node$i.toml" \
  >> "logs/node$i.log" 2>&1 &
echo $! > "logs/node$i.pid"
