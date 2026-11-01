# Quick setup: one node per machine

The short path that worked on 2026-10-31: node 1 on a Windows lab PC, nodes 2
and 3 on Ubuntu lab PCs, the client on a Mac, all on a phone hotspot. For the
full background (firewalls, test sequence, troubleshooting) see
[deployment.md](deployment.md).

| Machine | Runs | Port |
|---|---|---|
| Windows lab PC | node 1 | 7001 |
| Ubuntu PC #1 | node 2 | 7002 |
| Ubuntu PC #2 | node 3 | 7003 |
| Mac | client | none |

## 0. Network

The campus network blocked node traffic (between subnets, and inbound to the
Windows PC), so put every machine on one hotspot. Then find each machine's IP
on it:

| OS | Command |
|---|---|
| Windows | `(Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias "Wi-Fi*").IPAddress` |
| Ubuntu | `hostname -I` (or `ip route get 1.1.1.1`, the address after `src`) |
| Mac | `ipconfig getifaddr en0` |

All IPs should share the same prefix (e.g. `10.25.8.x`). Below, `IP1 IP2 IP3`
are the IPs of the node 1, node 2 and node 3 machines.

**Argument order.** `up.sh` / `up.ps1` take the IPs in node order:
`IP1 IP2 IP3`. `set_ips` takes them in machine order A B C, which is
`IP1 IP3 IP2`. Mixing these up makes nodes 2 and 3 look for each other at
their own address (`peer unreachable ... connection refused` in the node 2
log, `last_seen ... None` in `check_cluster`).

## 1. Windows PC: node 1

First time only:

1. Install Rust with the GNU toolchain (no admin needed). Get `rustup-init.exe`
   from <https://rustup.rs>, then in a new PowerShell window:
   ```powershell
   rustup toolchain install stable-x86_64-pc-windows-gnu
   rustup default stable-x86_64-pc-windows-gnu
   ```
2. Install MinGW-w64. The GNU toolchain needs its `dlltool` and `as`
   (without it the build fails with "error calling dlltool" or
   "dlltool ... CreateProcess"):
   ```powershell
   winget install --id BrechtSanders.WinLibs.POSIX.UCRT -e
   ```
3. Clone and build. `up.ps1` builds automatically (and adds WinLibs to PATH)
   if there's no binary yet.

Every time:

```powershell
cd <repo>\RescueMesh
powershell -ExecutionPolicy Bypass -File scripts\up.ps1 1 IP1 IP2 IP3
```

When the Windows Security Alert appears, tick **Private and Public**, then
**Allow access**. The hotspot is a Public network. Cancelling, or ticking only
Private, leaves a Block rule for that exe path, which a non-admin can't remove.
If that happened (`check_cluster` from another machine shows node 1
`unreachable (deadline has elapsed)` or `timeout`), run a copy from a new
path, which gets a fresh alert:

```powershell
New-Item -ItemType Directory -Force bin1 | Out-Null
Copy-Item target\release\cloud-node.exe bin1\cloud-node.exe
$env:RUST_LOG = "info"
.\bin1\cloud-node.exe --config config\node1.toml
```

From then on start node 1 with that last line (after `up.ps1` or `set_ips`
has written the configs). `bin1/` isn't in git.

Check port 7001 is held by `cloud-node`, not something else (e.g. a test
listener), before starting: `Get-NetTCPConnection -LocalPort 7001 -State Listen`.

## 2. Ubuntu PCs: nodes 2 and 3

First time only:

```bash
sudo apt install -y build-essential curl git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source ~/.cargo/env
git clone -b phase1/load-election https://github.com/mostafa21314/RescueMesh.git
```

Every time:

```bash
cd ~/RescueMesh
git pull
bash scripts/up.sh 2 IP1 IP2 IP3      # node 3 PC: bash scripts/up.sh 3 IP1 IP2 IP3
```

`up.sh` builds if needed, writes the configs, allows the node's port in ufw
(when ufw is active and sudo works without a password; otherwise run
`sudo ufw allow 7001:7003/tcp` yourself) and starts the node. It warns if this
machine doesn't have the IP given for its node.

## 3. Mac: client

```bash
cd <repo>/RescueMesh
cargo build --release
bash scripts/set_ips.sh IP1 IP3 IP2   # note the order
bash scripts/check_cluster.sh
target/release/client --config config/client.toml --count 200 --rate 50 --complexity 5 --out t1_normal.csv
bash scripts/check_cluster.sh --settle
```

## 4. What "working" looks like

`check_cluster` before a run: 3 of 3 `reachable`, each node with a status
line (not `deadline has elapsed`).

After a run, with `--settle`:

- every node's `last_seen` has a time for both other nodes (no `None`);
- all three show the same `epoch 1 server Some(N)`, `results 200` and digest;
- the last line is `results tables IDENTICAL on all reachable nodes`;
- the client shows `failed 0`, `attempts {1: 200}`, `highest epoch 1`, and one
  node in `served_by`.

If two nodes each name themselves server and the tables differ, those two
can't reach each other: check their configs with
`grep -E 'listen|addr' config/node*.toml` on each machine, and the node logs
for `peer unreachable`.

Restart all three nodes (Ctrl+C, then the start command again) before each
test, so every node starts with empty tables. If an IP changes, run `up` (or
`set_ips`) again with the new IPs on every machine and restart the nodes.
