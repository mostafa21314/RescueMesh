#!/usr/bin/env bash
# Write the machines' IP addresses into config/node1-3.toml and config/client.toml.
#
#   scripts/set_ips.sh A_IP B_IP [C_IP]
#
# node1 runs on machine A, node3 on machine B, node2 on machine A, or on machine
# C if C_IP is given. Safe to re-run: every peer address and the client's node
# list are rewritten from the arguments (placeholders or old IPs alike), with
# each node's port taken from its own `listen` line. scripts/set_ips.ps1 does the
# same and writes byte-identical files (UTF-8, LF line endings).
set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
  echo "usage: scripts/set_ips.sh A_IP B_IP [C_IP]" >&2
  echo "  node1 -> A, node2 -> A (or C if given), node3 -> B" >&2
  exit 2
}
[[ $# -eq 2 || $# -eq 3 ]] || usage

valid_ipv4() {
  [[ $1 =~ ^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$ ]] || return 1
  local i
  for i in 1 2 3 4; do
    ((10#${BASH_REMATCH[$i]} <= 255)) || return 1
  done
}
for ip in "$@"; do
  valid_ipv4 "$ip" || { echo "error: '$ip' is not an IPv4 address (e.g. 10.40.12.7)" >&2; exit 2; }
  if [[ $ip == 127.* ]]; then
    echo "warning: $ip is a loopback address; other machines can't reach it" >&2
  fi
done

A=$1
B=$2
C=${3:-$1}
hosts=("" "$A" "$C" "$B")
machines=("" "A" "$([[ $# -eq 3 ]] && echo C || echo A)" "B")

# Each node's port, from its own listen line.
ports=("")
for k in 1 2 3; do
  line=$(grep -E '^listen = "' "config/node$k.toml" | head -1 || true)
  [[ $line =~ :([0-9]+)\" ]] || { echo "error: no 'listen = \"...:PORT\"' line in config/node$k.toml" >&2; exit 1; }
  ports+=("${BASH_REMATCH[1]}")
done

# Rewrites one file to "$2.new"; lines are read with any trailing CR removed and
# written with LF.
rewrite() {
  local file=$1 out=$2 line
  : > "$out"
  while IFS= read -r line || [[ -n $line ]]; do
    line=${line%$'\r'}
    if [[ $line =~ ^([[:space:]]*\{[[:space:]]*id[[:space:]]*=[[:space:]]*)([0-9]+)(,[[:space:]]*addr[[:space:]]*=[[:space:]]*\")[^\"]*(\".*)$ ]]; then
      local head=${BASH_REMATCH[1]} id=${BASH_REMATCH[2]} mid=${BASH_REMATCH[3]} tail=${BASH_REMATCH[4]}
      case $id in
        1 | 2 | 3) line="${head}${id}${mid}${hosts[$id]}:${ports[$id]}${tail}" ;;
      esac
    elif [[ $line =~ ^nodes[[:space:]]*=[[:space:]]*\[[^]]*\](.*)$ ]]; then
      line="nodes = [\"${hosts[1]}:${ports[1]}\", \"${hosts[2]}:${ports[2]}\", \"${hosts[3]}:${ports[3]}\"]${BASH_REMATCH[1]}"
    fi
    printf '%s\n' "$line" >> "$out"
  done < "$file"
}

files=(config/node1.toml config/node2.toml config/node3.toml config/client.toml)
# Write every file to .new first, then move them all, so a failure leaves the
# configs untouched.
for f in "${files[@]}"; do rewrite "$f" "$f.new"; done
for f in "${files[@]}"; do mv "$f.new" "$f"; done

echo "node  machine  address"
for k in 1 2 3; do
  printf '%-5s %-8s %s\n' "$k" "${machines[$k]}" "${hosts[$k]}:${ports[$k]}"
done
echo "wrote ${files[*]}"
