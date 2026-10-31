#!/usr/bin/env bash
# Start one cloud node in the foreground with config/node<ID>.toml.
#
#   scripts/start_node.sh ID [cloud-node options]
#   scripts/start_node.sh 1
#   scripts/start_node.sh 2 --burn-threads 4      # node 2 also burns 4 CPU threads
#
# The binary is the first of: target/release/cloud-node (built here),
# scripts/cloud-node, ./cloud-node (copied, e.g. a dist/ folder). Stop the node
# with Ctrl+C. RUST_LOG=debug for more detail.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)

id=${1:-}
[[ $id =~ ^[123]$ ]] || { echo "usage: scripts/start_node.sh ID [cloud-node options]   (ID = 1, 2 or 3)" >&2; exit 2; }
shift

bin=""
for b in "$root/target/release/cloud-node" "$here/cloud-node" "$root/cloud-node"; do
  if [[ -f $b ]]; then bin=$b; break; fi
done
if [[ -z $bin ]]; then
  echo "error: no cloud-node binary in target/release/, scripts/ or $root" >&2
  echo "       build it (cargo build --release) or copy it next to the scripts folder" >&2
  exit 1
fi
# Copies from a USB stick often lose the execute bit.
[[ -x $bin ]] || chmod +x "$bin"

cfg="$root/config/node$id.toml"
[[ -f $cfg ]] || { echo "error: $cfg not found" >&2; exit 1; }
if grep -v '^[[:space:]]*#' "$cfg" | grep -q 'MACHINE_[A-C]_IP'; then
  echo "error: $cfg still has MACHINE_*_IP placeholders; run scripts/set_ips.sh A_IP B_IP [C_IP] first" >&2
  exit 1
fi

echo "node $id: $bin --config $cfg $*"
grep -E '^(listen|  \{ id)' "$cfg" | sed 's/^/  /'
echo "stop with Ctrl+C"
export RUST_LOG=${RUST_LOG:-info}
exec "$bin" --config "$cfg" "$@"
