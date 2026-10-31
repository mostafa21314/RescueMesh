#!/usr/bin/env bash
# Start the three cloud nodes on 127.0.0.1 (ports 7001-7003) from config/local/.
# Logs and pids go to logs/.
#
#   scripts/run_local.sh          build (release) and start all three
#   scripts/run_local.sh stop     stop them
#   BURN_NODE=1 BURN_THREADS=4 scripts/run_local.sh   node 1 also burns 4 CPU threads
#
# Then, for example:
#   target/release/client --config config/local/client.toml --count 500 --rate 100 --complexity 5
#   target/release/client --config config/local/client.toml --status
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

stop() {
  for i in 1 2 3; do
    if [[ -f logs/node$i.pid ]]; then
      kill "$(cat logs/node$i.pid)" 2>/dev/null && echo "node $i stopped" || true
      rm -f logs/node$i.pid
    fi
  done
}

if [[ "${1:-}" == "stop" ]]; then
  stop
  exit 0
fi

stop
cargo build --release --quiet
for i in 1 2 3; do
  extra=()
  [[ "${BURN_NODE:-}" == "$i" ]] && extra=(--burn-threads "${BURN_THREADS:-4}")
  RUST_LOG=${RUST_LOG:-info} target/release/cloud-node --config config/local/node$i.toml ${extra[@]+"${extra[@]}"} \
    > logs/node$i.log 2>&1 &
  echo $! > logs/node$i.pid
  echo "node $i started (pid $!), log logs/node$i.log"
done
