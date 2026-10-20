#!/usr/bin/env bash
# Start the three cloud nodes on one machine (ports 7001-7003). Logs go to logs/.
# Usage: scripts/run_local.sh [extra rm-node flags...]   Stop: scripts/stop_local.sh
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --release --quiet
mkdir -p logs
BIN=target/release/rm-node
for i in 1 2 3; do
  peers=()
  for j in 1 2 3; do
    [[ $j == "$i" ]] || peers+=(--peer "$j@127.0.0.1:700$j")
  done
  RUST_LOG=${RUST_LOG:-info} "$BIN" --id "$i" --listen "127.0.0.1:700$i" "${peers[@]}" "$@" \
    > "logs/node$i.log" 2>&1 &
  echo $! > "logs/node$i.pid"
  echo "node $i started (pid $!), log logs/node$i.log"
done
