#!/usr/bin/env bash
# One command per machine: build if needed, write the configs, open the port,
# start this machine's node.
#
#   bash scripts/up.sh THIS_NODE NODE1_IP NODE2_IP NODE3_IP
#   bash scripts/up.sh 2 10.25.8.25 10.25.8.40 10.25.8.41
#
# The IPs are given in node order (1, 2, 3) and must be the same on every
# machine; this calls set_ips.sh with them in its A B C order. Extra options
# after the IPs go to cloud-node (e.g. --burn-threads 8).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)

usage() { echo "usage: bash scripts/up.sh THIS_NODE NODE1_IP NODE2_IP NODE3_IP [cloud-node options]" >&2; exit 2; }
[[ $# -ge 4 ]] || usage
id=$1; ip1=$2; ip2=$3; ip3=$4; shift 4
[[ $id =~ ^[123]$ ]] || usage
ips=("" "$ip1" "$ip2" "$ip3")
mine=${ips[$id]}

# Warn if this machine doesn't have the IP its node is configured for.
if command -v ip >/dev/null; then
  have=$(ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1)
  grep -qx "$mine" <<<"$have" || echo "warning: node $id is set to $mine, but this machine has: $(echo $have)" >&2
fi

if [[ ! -f $root/target/release/cloud-node && ! -f $here/cloud-node && ! -f $root/cloud-node ]]; then
  [[ -f $HOME/.cargo/env ]] && source "$HOME/.cargo/env"
  echo "building (cargo build --release)..."
  (cd "$root" && cargo build --release)
fi

bash "$here/set_ips.sh" "$ip1" "$ip3" "$ip2"

port=700$id
if command -v ufw >/dev/null && sudo -n true 2>/dev/null; then
  if sudo ufw status | grep -q 'Status: active'; then
    sudo ufw allow "$port/udp" >/dev/null && echo "ufw: allowed $port/udp"
  fi
else
  echo "note: if a firewall is active, allow inbound UDP $port (e.g. sudo ufw allow $port/udp)"
fi

exec bash "$here/start_node.sh" "$id" "$@"
