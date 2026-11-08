#!/usr/bin/env bash
# RescueMesh six-PC live demo: one command per Ubuntu PC.
#
#   bash oneclick.sh N        N = this PC's number: 1-3 run nodes 1-3, 4-6 run clients
#
# On a fresh PC, without downloading anything first:
#   curl -fsSL https://raw.githubusercontent.com/mostafa21314/RescueMesh/demo/six-pc-live/live-demo/oneclick.sh | bash -s N
#
# It installs what's missing (Rust, compiler), downloads or updates the code in
# ~/RescueMesh-demo, builds, finds the other five PCs on the network by itself
# (no IPs to type), starts this PC's part and opens the dashboard. It also puts a
# "RescueMesh PC N" launcher on the Desktop, so next time it's a double-click.
set -euo pipefail

# Everything sits in main() so `curl ... | bash` reads the whole script before running it.
main() {
REPO_URL=https://github.com/mostafa21314/RescueMesh.git
BRANCH=demo/six-pc-live
DIR="$HOME/RescueMesh-demo"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
die() { printf '\n\033[31mERROR: %s\033[0m\n' "$*"; exit 1; }

PC=${1:-}
if [ -z "$PC" ]; then
  read -rp "Which PC is this? (1, 2, 3 = nodes; 4, 5, 6 = clients): " PC </dev/tty
fi
case "$PC" in [1-6]) ;; *) die "PC number must be 1-6 (got '$PC')" ;; esac

# 1. Tools. Only asks for sudo if something is missing.
missing=()
command -v git >/dev/null || missing+=(git)
command -v curl >/dev/null || missing+=(curl)
command -v python3 >/dev/null || missing+=(python3)
command -v cc >/dev/null || missing+=(build-essential)
if [ ${#missing[@]} -gt 0 ]; then
  say "Installing ${missing[*]} (needs sudo)"
  sudo apt-get update -qq && sudo apt-get install -y "${missing[@]}" \
    || die "could not install ${missing[*]}; ask the lab admin or install them, then run this again"
fi
if ! command -v cargo >/dev/null; then
  [ -f "$HOME/.cargo/env" ] || {
    say "Installing Rust into ~/.cargo (no sudo)"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  }
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
fi

# 2. Code: this checkout if the script runs from one, else ~/RescueMesh-demo.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" 2>/dev/null && pwd || true)
if [ -n "$HERE" ] && [ -f "$HERE/agent.py" ] && [ -f "$HERE/../Cargo.toml" ]; then
  ROOT=$(cd "$HERE/.." && pwd)
  git -C "$ROOT" pull -q --ff-only 2>/dev/null || true
else
  ROOT=$DIR
  if [ -d "$ROOT/.git" ]; then
    say "Updating $ROOT"
    git -C "$ROOT" fetch -q origin "$BRANCH" && git -C "$ROOT" checkout -q "$BRANCH" \
      && git -C "$ROOT" reset -q --hard "origin/$BRANCH"
  else
    say "Downloading the code to $ROOT"
    git clone -q -b "$BRANCH" "$REPO_URL" "$ROOT"
  fi
fi
cd "$ROOT"

# 3. Build (a few minutes the first time, seconds after).
say "Building"
cargo build --release --quiet --bin cloud-node --bin client

# 4. Firewall, if ufw is on and sudo works without a password.
if command -v ufw >/dev/null && sudo -n true 2>/dev/null && sudo -n ufw status 2>/dev/null | grep -q 'Status: active'; then
  sudo -n ufw allow 7500 >/dev/null
  [ "$PC" -le 3 ] && sudo -n ufw allow $((7000 + PC))/udp >/dev/null
  echo "firewall: opened 7500 (tcp+udp)$([ "$PC" -le 3 ] && echo " and $((7000 + PC))/udp")"
elif command -v ufw >/dev/null && sudo -n ufw status 2>/dev/null | grep -q 'Status: active'; then
  echo "NOTE: ufw is on; if PCs can't see each other run: sudo ufw allow 7500 && sudo ufw allow 7001:7003/udp"
fi

# 5. Desktop launcher for next time.
DESK=$(xdg-user-dir DESKTOP 2>/dev/null || echo "$HOME/Desktop")
if [ -d "$DESK" ]; then
  L="$DESK/rescuemesh-pc$PC.desktop"
  cat > "$L" <<EOF
[Desktop Entry]
Type=Application
Name=RescueMesh PC $PC
Comment=Start this PC's part of the RescueMesh six-PC demo
Exec=bash -c 'bash "$ROOT/live-demo/oneclick.sh" $PC; echo; read -rp "Press Enter to close"'
Terminal=true
Icon=network-workgroup
EOF
  chmod +x "$L"
  gio set "$L" metadata::trusted true 2>/dev/null || true
fi

# 6. Run. The dashboard opens once the agent is up.
say "Starting PC $PC ($([ "$PC" -le 3 ] && echo "node $PC" || echo client)). Keep this window open; Ctrl+C stops it."
if command -v xdg-open >/dev/null && [ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ]; then
  ( for _ in $(seq 30); do
      curl -fs -o /dev/null http://localhost:7500/api/state && { xdg-open http://localhost:7500 >/dev/null 2>&1; exit; }
      sleep 1
    done ) &
fi
exec python3 -u live-demo/agent.py --pc "$PC" --discover
}

main "$@"
