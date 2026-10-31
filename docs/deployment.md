# Running RescueMesh on physical machines

This guide takes you from a USB stick (or a fresh clone) to a running three-node
cloud with clients on laptops. Find your role in the list below, follow its
section from top to bottom, then go to [Lab day](#lab-day).

| Role | Section |
|---|---|
| Windows lab PC running a cloud node | [a](#a-windows-lab-pc-running-a-cloud-node) |
| Linux lab PC running a cloud node | [b](#b-linux-lab-pc-running-a-cloud-node) |
| The Mac running the client | [c](#c-the-mac-running-the-client) |
| A Windows laptop running the client | [d](#d-a-windows-laptop-running-the-client) |

## The setup

| Machine | Runs | Listens on (TCP, inbound) |
|---|---|---|
| A (lab PC) | node 1 and node 2 | 7001, 7002 |
| B (lab PC) | node 3 | 7003 |
| C (lab PC, later) | node 2, when it moves off machine A | 7002 |
| Laptops (Mac, Windows) | the client | nothing: clients only connect out |

- Node *N* listens on port 700*N*.
- Every node connects to the other two, and every client connects to all three
  nodes. So every node machine must accept inbound TCP on its nodes' ports.
  Laptops need no open ports.
- The configs ship with placeholders (`MACHINE_A_IP`, `MACHINE_B_IP`).
  `set_ips` replaces them with real addresses.
- Every machine must end up with the **same** `config/` files. Either run
  `set_ips` with the same arguments on each machine (the `.sh` and `.ps1`
  versions write byte-identical files), or run it once and copy `config/`.

### What's in a package folder

`scripts/package.sh` creates one folder per target in `dist/`, e.g.
`dist/x86_64-pc-windows-gnu/`:

```
cloud-node(.exe)   the cloud node
client(.exe)       the client
config/            node1-3.toml, client.toml (+ local/ for one-machine runs)
scripts/           set_ips, start_node, check_cluster, open_ports
DEPLOYMENT.md      this file
BUILD.txt          target, commit, compiler, build date
```

The scripts look for the binaries first in `target/release/` (built in a
clone), then in `scripts/`, then in the folder above `scripts/` (a package
folder). So the same commands work in a clone and in a package folder.

### Running the scripts

| OS | How |
|---|---|
| Windows | `powershell -ExecutionPolicy Bypass -File scripts\<name>.ps1 ...`, from the package or repo folder. `-ExecutionPolicy Bypass` applies to that one run and needs no admin rights. It's required where PowerShell refuses with "running scripts is disabled on this system". |
| Linux, macOS | `bash scripts/<name>.sh ...`. Use `bash` explicitly: a copy from a USB stick often loses the execute bit. |

## Release builds

Pick one way per platform. Every way produces `cloud-node` and `client`
(`.exe` on Windows).

**Linux lab PC**
- *Build there.* rustup installs into your home folder and needs no root:
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  source ~/.cargo/env
  cargo build --release          # -> target/release/cloud-node, target/release/client
  ```
  This needs a C linker (`cc`), which most lab images have. If `cargo build`
  fails with "linker `cc` not found" and you can't install one, use the static
  binary below.
- *Static binary, built on any x86_64 Linux machine.* It has no library
  dependencies, so it runs on any x86_64 Linux, whatever its glibc:
  ```bash
  rustup target add x86_64-unknown-linux-musl
  cargo build --release --target x86_64-unknown-linux-musl
  # -> target/x86_64-unknown-linux-musl/release/{cloud-node,client}
  ```
  `scripts/package.sh` on a Linux machine does this and packages the result.

**Windows lab PC or laptop**
- *Build there with the GNU toolchain* (no Visual Studio needed, no admin). Get
  `rustup-init.exe` from <https://rustup.rs>, then in PowerShell:
  ```powershell
  .\rustup-init.exe -y --default-host x86_64-pc-windows-gnu --profile minimal
  # open a new PowerShell window so PATH includes %USERPROFILE%\.cargo\bin
  cargo build --release          # -> target\release\cloud-node.exe, client.exe
  ```
  If rustup is already installed with the MSVC toolchain:
  `rustup toolchain install stable-x86_64-pc-windows-gnu` and
  `rustup default stable-x86_64-pc-windows-gnu`.
- *Cross-compile on the Mac* and copy the `.exe` files with `config/` and
  `scripts/`. This needs rustup (Homebrew's `rust` formula can't add targets)
  and the mingw-w64 linker:
  ```bash
  brew install rustup mingw-w64
  rustup default stable          # PATH must find rustup's cargo before Homebrew's: rustc -vV must not say "Homebrew"
  rustup target add x86_64-pc-windows-gnu
  scripts/package.sh             # -> dist/x86_64-pc-windows-gnu/
  ```

**macOS**
- `cargo build --release` (`target/release/client`), or `scripts/package.sh`
  (`dist/aarch64-apple-darwin/` on Apple silicon).

**`scripts/package.sh`** always builds this machine's own target. It also tries
`x86_64-pc-windows-gnu` and `x86_64-unknown-linux-musl`, and skips each one,
saying what's missing (the target's Rust library, or its linker), if it can't be
built here. Each built target goes to `dist/<target>/` (see
[What's in a package folder](#whats-in-a-package-folder)). Copy that folder to
the USB stick.

## a. Windows lab PC running a cloud node

1. **Get the binary.** Copy `dist\x86_64-pc-windows-gnu\` from the USB stick to
   a local folder, e.g. `C:\Users\<you>\rescuemesh\`. Don't run it from the
   stick. Or build in a clone ([Release builds](#release-builds)). Open
   PowerShell there: `cd C:\Users\<you>\rescuemesh`.
2. **Find this machine's IP:** `ipconfig`. Take the "IPv4 Address" under the
   **Ethernet adapter** (wired), e.g. `10.40.12.7`. Ignore `169.254.x.x` (no
   network address assigned), `127.0.0.1`, and adapters named vEthernet, WSL,
   VirtualBox or VMware. Note it as machine A or B, and collect the other lab
   PC's IP.
3. **Fill in the IPs** (same arguments on every machine):
   ```powershell
   powershell -ExecutionPolicy Bypass -File scripts\set_ips.ps1 <A_IP> <B_IP>
   ```
   Add `<C_IP>` as a third argument once node 2 moves to machine C. It prints
   the table it wrote:
   ```
   node  machine  address
   1     A        10.40.12.7:7001
   2     A        10.40.12.7:7002
   3     B        10.40.12.8:7003
   wrote config/node1.toml config/node2.toml config/node3.toml config/client.toml
   ```
4. **Open the ports.**
   - *With admin rights:* in an administrator PowerShell (right-click
     PowerShell > Run as administrator), run
     `powershell -ExecutionPolicy Bypass -File scripts\open_ports.ps1`. It allows
     inbound TCP 7001-7003 on all network profiles. `-Remove` deletes the rule
     after the lab.
   - *Without admin rights:* see
     [Windows firewall without admin](#windows-firewall-without-admin).
5. **Start the node(s)**, one PowerShell window per node. On machine A, nodes 1
   and 2 run in two windows:
   ```powershell
   powershell -ExecutionPolicy Bypass -File scripts\start_node.ps1 1
   ```
   It prints the node's addresses, then the node's log. `node started node=1
   addr=0.0.0.0:7001` means it's listening. Ctrl+C stops it.
6. **Verify** from the *other* lab PC and from a laptop with `check_cluster`.
   A check from this machine to its own IP doesn't go through its firewall, so
   it proves the node runs, not that others can reach it:
   ```powershell
   powershell -ExecutionPolicy Bypass -File scripts\check_cluster.ps1
   ```

### Windows firewall without admin

The first time `cloud-node.exe` starts listening, Windows may show **"Windows
Defender Firewall has blocked some features of this app"**:
- Tick **both** "Private networks" and "Public networks", then click **Allow
  access**. The campus network is often classified as Public, so ticking only
  Private leaves the node blocked.
- On an account without admin rights, Allow access asks for an administrator's
  password. If nobody can enter one, the node runs but other machines can't
  connect to it (`check_cluster` from elsewhere shows **timeout**).
- Clicking Cancel can leave a block rule for `cloud-node.exe`. An admin can
  delete it later under Windows Defender Firewall with Advanced Security >
  Inbound Rules.
- If no alert appears, the firewall is either off or managed centrally. Only
  `check_cluster` from another machine tells you which.

If a Windows PC can't accept inbound connections, run its nodes on a machine
that can (e.g. a Linux PC), and use the Windows PC for a client. Clients only
connect out, which the firewall allows.

## b. Linux lab PC running a cloud node

1. **Get the binary.** Copy the Linux package folder from the USB stick to your
   home folder (`cp -r /media/$USER/<stick>/x86_64-unknown-linux-musl ~/rescuemesh`)
   and `cd ~/rescuemesh`. Or build in a clone ([Release builds](#release-builds)).
   `start_node.sh` restores the execute bit on the binary if the copy lost it.
2. **Find this machine's IP:** `ip -4 addr show` (or `hostname -I`). Take the
   `inet` address of the wired interface (`eth0`, `enp…`, `eno…`), e.g.
   `10.40.12.8`. Ignore `127.0.0.1`, `docker0` (`172.17.x.x`) and `virbr0`.
3. **Fill in the IPs** (same arguments on every machine):
   `bash scripts/set_ips.sh <A_IP> <B_IP> [<C_IP>]`. It prints the same table as
   in [a](#a-windows-lab-pc-running-a-cloud-node).
4. **Open the ports** (needs sudo). First see which firewall is active:
   ```bash
   sudo ufw status                 # "Status: active" -> ufw
   sudo firewall-cmd --state       # "running"        -> firewalld
   ```
   | Firewall | Allow 7001-7003 |
   |---|---|
   | ufw | `sudo ufw allow 7001:7003/tcp` |
   | firewalld | `sudo firewall-cmd --add-port=7001-7003/tcp` (until reboot); add `--permanent` and run `sudo firewall-cmd --reload` to keep it |
   | plain iptables | `sudo iptables -I INPUT -p tcp --dport 7001:7003 -j ACCEPT` |
   | none active | nothing to do |

   Without sudo you can't change the firewall. `check_cluster` from another
   machine shows whether it blocks: **timeout** for ufw/iptables drops,
   **unreachable** for firewalld's default reject.
5. **Start the node:** `bash scripts/start_node.sh 3` (machine B). Add
   `--burn-threads N` for the CPU test. The log starts with `node started
   node=3 addr=0.0.0.0:7003`. Ctrl+C stops it.
6. **Verify** from the other lab PC and a laptop: `bash scripts/check_cluster.sh`.

## c. The Mac running the client

1. **Get the binary:** in the repo, `cargo build --release`
   (`target/release/client`).
2. **Find the Mac's IP** (only for the troubleshooting notes; the client needs
   no inbound ports): `ipconfig getifaddr en0` (Wi-Fi).
3. **Fill in the IPs** with the lab PCs' addresses (the same arguments as on the
   lab PCs): `scripts/set_ips.sh <A_IP> <B_IP> [<C_IP>]`.
4. **Ports:** nothing to open. The client only connects out.
5. **Run the client:**
   ```bash
   target/release/client --config config/client.toml --count 200 --rate 50 --complexity 5 --out run.csv
   ```
6. **Verify first:** `scripts/check_cluster.sh` (add `--settle` after a run).

## d. A Windows laptop running the client

1. **Get the binary.** Copy `dist\x86_64-pc-windows-gnu\` to a local folder and
   `cd` there, or build in a clone ([Release builds](#release-builds)). If a
   downloaded copy is blocked ("Windows protected your PC"), choose More info >
   Run anyway, or run `Unblock-File .\client.exe`.
2. **Find the laptop's IP** (only for troubleshooting): `ipconfig`, then the
   "IPv4 Address" under **Wireless LAN adapter Wi-Fi**.
3. **Fill in the IPs** with the lab PCs' addresses:
   `powershell -ExecutionPolicy Bypass -File scripts\set_ips.ps1 <A_IP> <B_IP> [<C_IP>]`.
4. **Ports:** nothing to open.
5. **Run the client:**
   ```powershell
   .\client.exe --config config\client.toml --count 200 --rate 50 --complexity 5 --out run.csv
   ```
   In a clone the binary is `.\target\release\client.exe`.
6. **Verify first:**
   `powershell -ExecutionPolicy Bypass -File scripts\check_cluster.ps1`
   (add `-Settle` after a run).

## Checking the cluster

`check_cluster` reads the node addresses from `config/client.toml` and tries a
TCP connection to each, then runs `client --status`. It prints one verdict per
node:

| Verdict | Meaning |
|---|---|
| `reachable` | something accepts connections there: the node, if the address is right |
| `refused` | the machine answered, but nothing listens on that port: the node isn't running, or the port is wrong |
| `timeout` | no answer within 3 s: a firewall or the network drops the connection |
| `unreachable` | no route to the host: wrong IP, a different network, or a firewall that rejects (firewalld's default) |
| `not set` | `client.toml` still has placeholders: run `set_ips` |

Example (node 2 stopped):

```
TCP check (limit 3s per node), from .../config/client.toml:
  node 1  10.40.12.7:7001       reachable
  node 2  10.40.12.7:7002       refused (node not running, or wrong port)
  node 3  10.40.12.8:7003       reachable
2 of 3 nodes reachable

client --status:
10.40.12.7:7001: node 1 epoch 1 server Some(1) requests 200 results 200 digest 6bdddb34e273cc0b queue 0 running 0 ...
10.40.12.7:7002: unreachable (Connection refused ...)
10.40.12.8:7003: node 3 epoch 1 server Some(1) requests 200 results 200 digest 6bdddb34e273cc0b queue 0 running 0 ...
results tables IDENTICAL on all reachable nodes
```

Use `--settle` (`-Settle` in PowerShell) after a run: `client --status` then
waits, for up to 10 s, until no node has queued or running jobs before it
compares the digests. Replication is asynchronous, so while requests are in
flight one node can be a result ahead.

## Lab day

1. **Before the lab:** on the Mac, `scripts/package.sh`; copy each `dist/<target>/`
   folder to the USB stick. Check `BUILD.txt` names the commit you mean to run.
2. **On machines A and B:** copy the package folder locally
   ([a](#a-windows-lab-pc-running-a-cloud-node) step 1,
   [b](#b-linux-lab-pc-running-a-cloud-node) step 1).
3. **Collect the IPs** of A and B (step 2 of a/b). Lab PCs get addresses by
   DHCP, so check them again each lab day.
4. **Run `set_ips`** with the same `<A_IP> <B_IP>` on every machine, laptops
   included. Or run it once and copy `config/` to the others.
5. **Open the ports** on A and B (step 4 of a/b).
6. **Start the nodes:** node 1 and node 2 on A (two windows), node 3 on B.
7. **Check lab to lab:** `check_cluster` on A (tests node 3 across the
   network) and on B (tests nodes 1 and 2 across the network). Every node must
   be `reachable`.
8. **Check from every laptop:** `check_cluster`. If lab to lab works but the
   laptops time out, see the last row of [Troubleshooting](#troubleshooting).
9. **Record each machine** before measuring, because mixed hardware changes the
   load scores:

   | OS | CPU model | OS version |
   |---|---|---|
   | Windows | `(Get-CimInstance Win32_Processor).Name` | `(Get-CimInstance Win32_OperatingSystem).Caption` |
   | Linux | `lscpu \| grep 'Model name'` | `grep PRETTY_NAME /etc/os-release` |
   | macOS | `sysctl -n machdep.cpu.brand_string` | `sw_vers -productVersion` |

   Also note which node ran on which machine, the network (wired or Wi-Fi), and
   `BUILD.txt`.
10. **Run the [test sequence](#test-sequence).**

**Measurements count for the report only once no two nodes share a machine**
(node 2 on machine C). While nodes 1 and 2 share machine A, they compete for
its CPU and see the same machine CPU load (`cpu_source = "machine"`), so their
scores and latencies aren't independent. Runs on two machines are rehearsals.

## Test sequence

Run the client from a laptop (or a lab PC). Keep the node windows visible, since
the election lines are in their logs. Before each test, **restart all three
nodes** (Ctrl+C, then `start_node` again) so each starts from empty tables.
Restarting only one node isn't supported yet: a restarted node has lost its
tables and can win an election while stale (design log, "Known limitations").

Commands below are for macOS/Linux; on Windows use `.\client.exe` and
`config\client.toml`.

### 1. Normal run

```bash
client --config config/client.toml --count 200 --rate 50 --complexity 5 --out t1_normal.csv
check_cluster --settle
```

Expected:
- Client: `requests 200  answered 200  failed 0`; `attempts {1: 200}`;
  `served_by` names one node; `highest epoch 1`. Latency is the network round
  trip plus a few ms of planning, and is higher over Wi-Fi.
- Each node logs one election, with three bids. The lowest score wins, ties go
  to the lower node id:
  ```
  election epoch 1 trigger=no_server ... bids=[(1, 37), (2, 39), (3, 39)] incumbent=[] outcome=decided winner=Some(1) duration_ms=... messages_sent=2
  ```
  The winner also logs `now serving node=1 epoch=1 degraded=false`.
- `check_cluster --settle`: 3 reachable, each node `epoch 1 server Some(N)
  requests 200 results 200` with the same digest; `results tables IDENTICAL`.

### 2. Kill the current server mid-run

Restart all nodes, then start a 60 s run:

```bash
client --config config/client.toml --count 3000 --rate 50 --complexity 5 --out t2_failover.csv
```

Find the server: its window logs `now serving`, or `check_cluster` shows
`server Some(N)`. About 20 s into the run, press Ctrl+C in that node's window.
Closing the window, or `kill -9` / Task Manager > End task, is an even harder
stop.

Expected:
- The two survivors log an election in the next epoch, triggered by a client
  retry. It has two bids, because the dead node doesn't bid. The dead server is
  the excluded incumbent, and the election takes about 100 ms (the bid timeout):
  ```
  election epoch 2 trigger=retry ... bids=[(2, 75), (3, 74)] incumbent=[1] outcome=decided winner=Some(3) duration_ms=101.17 messages_sent=2
  now serving node=3 epoch=2 degraded=false took_over=...
  ```
- Client: `answered 3000  failed 0`. A handful of requests in `attempts` have 2
  (about 8 in our local runs). `served_by` names two nodes. `highest epoch 2`.
  Max latency is about 1.1 s (one request timeout plus the election).
- `check_cluster --settle`: the killed node `refused`; the two survivors
  agree on epoch and server, `results 3000`, `IDENTICAL on all reachable nodes`.

### 3. CPU burner on the current server

Restart all nodes. Start the node that won test 1 with a burner on all of its
machine's cores (`nproc` on Linux, `$env:NUMBER_OF_PROCESSORS` on Windows):

```bash
bash scripts/start_node.sh 1 --burn-threads 8                                         # Linux/macOS
powershell -ExecutionPolicy Bypass -File scripts\start_node.ps1 1 -BurnThreads 8     # Windows
```

Its log shows `burning CPU threads=8`. Then run the normal run from test 1.

Expected:
- The epoch-1 election's bids show the burning node with the highest score, and
  another node wins.
- With `cpu_source = "machine"` (the default), the CPU term measures the whole
  machine. While nodes 1 and 2 share machine A, a burner in node 1 raises both
  their scores, so node 3 wins. That's correct, and it's one reason the
  measurements count only on separate machines.
- The score is read only at an election. Starting a burner on a server that's
  already running changes nothing until a retry triggers the next election (the
  on-demand design). That's why the burner starts with the node.

### 4. Two nodes down (degraded)

Restart all nodes, then stop two of them (say nodes 1 and 2). Run a short test:

```bash
client --config config/client.toml --count 20 --rate 2 --complexity 5 --out t4_degraded.csv
check_cluster
```

Expected:
- Node 3 logs elections that hear only its own bid (`outcome=alone`, warnings).
  After 3 s alone it serves in degraded mode:
  `outcome=degraded winner=Some(3)` and `now serving node=3 ... degraded=true`.
- Client: `answered 20  failed 0`. The first request takes just over 3 s and 4
  or more attempts. The rest take 1 attempt. `served_by {3: 20}`. The CSV's
  `degraded` column is `true`. The epoch doesn't change while degraded.
- `check_cluster`: nodes 1 and 2 `refused`; node 3 `degraded true`.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `refused` for a node | Node not running on that machine, or wrong port | Start it with `start_node <id>`. Check it logged `node started`. Check the port in `client.toml` is 700*id* |
| `timeout` for a node | Firewall drops inbound connections, or the network does | Open the ports ([a](#a-windows-lab-pc-running-a-cloud-node) step 4, [b](#b-linux-lab-pc-running-a-cloud-node) step 4). Windows without admin: [firewall without admin](#windows-firewall-without-admin) |
| `unreachable` for a node | Wrong IP, machine on another network, or firewalld rejecting | Re-check the IP with `ipconfig` / `ip addr`. Re-run `set_ips` everywhere |
| `not set` | `set_ips` not run on this machine | Run `set_ips` with the same arguments as everywhere else |
| Node exits with "Address already in use" | That node is already running on this machine (another window), or the port is taken | Close the other copy, or use the right node id |
| `start_node`: "still has MACHINE_*_IP placeholders" | `set_ips` not run | Run `set_ips` |
| Nodes reach each other, but results or elections look as if a node is missing | The configs differ between machines | Re-run `set_ips` with identical arguments everywhere (or copy one `config/`) |
| PowerShell: "running scripts is disabled on this system" | Execution policy | Use `powershell -ExecutionPolicy Bypass -File ...` |
| Linux: "Permission denied" running a script or binary | Execute bit lost on copy | `bash scripts/...` for scripts; `chmod +x cloud-node client` (`start_node.sh` does this) |
| Linux: "cannot execute binary file" or a GLIBC version error | Wrong binary for this machine | Use the `x86_64-unknown-linux-musl` package |
| Windows: "Windows protected your PC" or antivirus quarantines the `.exe` | Unsigned binary from a download | More info > Run anyway, `Unblock-File`, or build it on that machine |
| Lab to lab works, laptop to lab times out | Campus network separation: Wi-Fi clients are kept apart from the wired lab network | Fall back to one network every machine joins: a travel router, or a phone hotspot. Then take the new IPs from `ipconfig` / `ip addr`, re-run `set_ips` everywhere, and re-check. Note the network change with the results |
