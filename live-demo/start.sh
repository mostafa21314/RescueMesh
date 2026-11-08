#!/usr/bin/env bash
# Start this PC's part of the six-PC live demo.
#
#   bash live-demo/start.sh PC_NUMBER IP1 IP2 IP3 IP4 IP5 IP6   # first time (saves the IPs)
#   bash live-demo/start.sh PC_NUMBER                            # afterwards
#
# PC 1-3 run cloud nodes 1-3, PC 4-6 run clients. Same six IPs, same order, on every PC.
set -euo pipefail
cd "$(dirname "$0")/.."

usage() { sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }
[ $# -ge 1 ] || usage
PC=$1; shift
case "$PC" in [1-6]) ;; *) echo "PC_NUMBER must be 1-6"; usage ;; esac

LAB=live-demo/lab.json
if [ $# -eq 6 ]; then
  printf '{"pcs": ["%s", "%s", "%s", "%s", "%s", "%s"], "agent_port": 7500}\n' "$@" > "$LAB"
  echo "saved the six IPs to $LAB"
elif [ $# -ne 0 ]; then
  echo "give all six IPs (PC 1..6), or none to reuse $LAB"; usage
fi
[ -f "$LAB" ] || { echo "no $LAB yet: run with the six IPs once"; usage; }

command -v python3 >/dev/null || { echo "python3 missing: sudo apt install -y python3"; exit 1; }

# Build (fast when nothing changed).
if ! command -v cargo >/dev/null && [ -f "$HOME/.cargo/env" ]; then . "$HOME/.cargo/env"; fi
if command -v cargo >/dev/null; then
  echo "building (first time takes a few minutes)..."
  cargo build --release --quiet --bin cloud-node --bin client
elif [ ! -x target/release/cloud-node ] || [ ! -x target/release/client ]; then
  echo "Rust is not installed. Install it (no sudo needed), then run this again:"
  echo "  sudo apt install -y build-essential curl"
  echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal"
  echo "  source ~/.cargo/env"
  exit 1
fi

# This machine should own the IP given for its PC number.
MY_IP=$(python3 -c "import json;print(json.load(open('$LAB'))['pcs'][$PC-1])")
if command -v hostname >/dev/null && ! hostname -I 2>/dev/null | tr ' ' '\n' | grep -qx "$MY_IP"; then
  echo "WARNING: this PC has $(hostname -I 2>/dev/null) but lab.json says PC $PC is $MY_IP"
fi

# Firewall: agent TCP 7500 everywhere, node UDP port on PC 1-3.
if command -v ufw >/dev/null && sudo -n true 2>/dev/null && sudo -n ufw status 2>/dev/null | grep -q 'Status: active'; then
  sudo -n ufw allow 7500/tcp >/dev/null
  [ "$PC" -le 3 ] && sudo -n ufw allow $((7000 + PC))/udp >/dev/null
  echo "ufw: opened 7500/tcp$([ "$PC" -le 3 ] && echo " and $((7000 + PC))/udp")"
fi

exec python3 -u live-demo/agent.py --pc "$PC"
