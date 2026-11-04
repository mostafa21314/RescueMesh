#!/usr/bin/env bash
# Query UDP application replies, not TCP connection acceptance.
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

client=""
for b in "$root/target/release/client" "$here/client" "$root/client"; do
  if [[ -f $b ]]; then client=$b; break; fi
done
if [[ -z $client ]]; then
  echo "no client binary found (target/release/, scripts/ or $root); skipping client --status"
  exit 1
fi
[[ -x $client ]] || chmod +x "$client"

echo
echo "client --status:"
"$client" --config "$cfg" --status "$@"
