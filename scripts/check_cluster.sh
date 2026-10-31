#!/usr/bin/env bash
# From any machine: test TCP reachability of every node in config/client.toml,
# then run `client --status`.
#
#   scripts/check_cluster.sh [--settle]     # --settle is passed on to client --status
#   CHECK_TIMEOUT=5 scripts/check_cluster.sh
#
# Per node, one of:
#   reachable     something accepts connections there (the node, if the port is right)
#   refused       the machine answered but nothing listens: node not running, or wrong port
#   timeout       no answer within CHECK_TIMEOUT s (default 3): firewall or network drops it
#   unreachable   no route to host: wrong IP, a different network, or a firewall
#                 that rejects (firewalld's default)
# The client binary is the first of target/release/client, scripts/client, ./client.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
cfg="$root/config/client.toml"
limit=${CHECK_TIMEOUT:-3}

[[ -f $cfg ]] || { echo "error: $cfg not found" >&2; exit 1; }
line=$(grep -E '^nodes[[:space:]]*=' "$cfg" | head -1 | tr -d '\r')
addrs=()
rest=$line
while [[ $rest =~ \"([^\"]+)\"(.*)$ ]]; do
  addrs+=("${BASH_REMATCH[1]}")
  rest=${BASH_REMATCH[2]}
done
[[ ${#addrs[@]} -gt 0 ]] || { echo "error: no 'nodes = [\"host:port\", ...]' line in $cfg" >&2; exit 1; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# probe HOST PORT: prints a verdict. bash's /dev/tcp opens the connection; the
# attempt runs in the background so it can be abandoned at the time limit.
probe() {
  local host=$1 port=$2 err="$tmp/err" pid i
  (exec 3<>"/dev/tcp/$host/$port") 2>"$err" &
  pid=$!
  for ((i = 0; i < limit * 10; i++)); do
    if ! kill -0 "$pid" 2>/dev/null; then
      if wait "$pid"; then
        echo "reachable"
      elif grep -qi 'refused' "$err"; then
        echo "refused (node not running, or wrong port)"
      elif grep -qiE 'no route|unreachable|host is down' "$err"; then
        echo "unreachable (wrong IP, other network, or a rejecting firewall)"
      elif grep -qi 'timed out' "$err"; then
        echo "timeout (blocked by firewall or network)"
      else
        echo "error: $(sed 's/.*: //' "$err" | head -1)"
      fi
      return
    fi
    sleep 0.1
  done
  kill "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  echo "timeout after ${limit}s (blocked by firewall or network)"
}

echo "TCP check (limit ${limit}s per node), from $cfg:"
ok=0
n=1
for a in "${addrs[@]}"; do
  host=${a%:*}
  port=${a##*:}
  if [[ $host == MACHINE_*_IP ]]; then
    verdict="not set: run scripts/set_ips.sh A_IP B_IP [C_IP] first"
  else
    verdict=$(probe "$host" "$port")
  fi
  [[ $verdict == reachable ]] && ok=$((ok + 1))
  printf '  node %s  %-21s %s\n' "$n" "$a" "$verdict"
  n=$((n + 1))
done
echo "$ok of ${#addrs[@]} nodes reachable"

client=""
for b in "$root/target/release/client" "$here/client" "$root/client"; do
  if [[ -f $b ]]; then client=$b; break; fi
done
if [[ -z $client ]]; then
  echo "no client binary found (target/release/, scripts/ or $root); skipping client --status"
  exit 1
fi
[[ -x $client ]] || chmod +x "$client"
if [[ $ok -eq 0 ]]; then
  echo "no node reachable; skipping client --status"
  exit 1
fi
echo
echo "client --status:"
"$client" --config "$cfg" --status "$@"
