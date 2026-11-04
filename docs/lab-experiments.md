# Running the experiments on the six lab PCs

Application and control requests now use UDP RR; the optional management agent
continues to use HTTP/TCP. New measurements go under `experiments/recovery/udp/lab/`.
For choosing roles without agents and running the normal/imbalance/fault sweep,
see [udp-phase1.md](udp-phase1.md).

The recovery experiments (docs/recovery.md) were first run on one Mac. This is
how to run the same experiments on the real setup: three PCs with one cloud
node each, three PCs running the client workload. One command on the
controller runs everything; every PC only runs a small agent.

```
                 controller (PC 4, e.g. the Mac): recovery_bench.py --lab lab.json
                     │  HTTP :7400 to every agent        │  node protocol :7001-7003
   ┌─────────────────┼─────────────────┐                 │  (status, InjectFailure)
 PC 1 agent        PC 2 agent        PC 3 agent          │
   └ cloud-node 1    └ cloud-node 2    └ cloud-node 3  ◄─┘
 PC 4 agent        PC 5 agent        PC 6 agent
   └ client          └ client          └ client      (each sends 1/3 of the load)
```

The agent (`scripts/lab_agent.py`) starts and kills its PC's `cloud-node` with
the config the controller sends, or runs a client workload and returns its
per-request CSV. The controller corrects for clock differences between the
PCs (it measures each agent's clock offset), so latency windows line up.

## 1. Every PC (once)

1. Same network for all six. Last time the campus network blocked node
   traffic, so use one phone hotspot (docs/quick-setup.md, step 0). Note each
   PC's IP: Ubuntu `hostname -I`, Mac `ipconfig getifaddr en0`, Windows
   `(Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias "Wi-Fi*").IPAddress`.
2. Rust and the code, on **this branch** (or `main` once it is merged):
   ```bash
   git clone https://github.com/mostafa21314/RescueMesh.git
   cd RescueMesh
   git checkout phase1/recovery-study
   cargo build --release
   ```
   (Windows needs the GNU toolchain and MinGW: docs/quick-setup.md, step 1.)
3. Python 3: preinstalled on Ubuntu and macOS. Windows:
   `winget install -e --id Python.Python.3.12`, then use `py` instead of
   `python3` below.
4. Open the ports:
   - every PC: **7400** (agent);
   - node PCs also **7001–7003** (nodes).

   Ubuntu: `sudo ufw allow 7400/tcp && sudo ufw allow 7001:7003/udp`.
   Windows: when the security alert appears for `python.exe` (and later
   `cloud-node.exe`), tick **Private and Public**, then **Allow**.

## 2. Every PC: start the agent

```bash
python3 scripts/lab_agent.py
```

It prints `lab agent listening on 0.0.0.0:7400`. Leave it running. Do **not**
start nodes with `up.sh` for the experiments: the agent starts them, with a
fresh config and empty tables for every run.

## 3. Controller (PC 4): describe the lab

```bash
cp config/lab.example.json lab.json
```

Replace `IP_PC1` … `IP_PC6` with the real IPs (node 1 on PC 1, etc.). Then
check every agent answers:

```bash
curl http://IP_PC1:7400/time
```

(repeat for each IP; each prints `{"ms": ...}`).

## 4. Controller: a short dry run first (≈ 2 min)

```bash
python3 scripts/recovery_bench.py restart --reps 1 --lab lab.json
```

It prints each agent's clock offset, then two lines like
`[restart 1/2] 25s recovered=True ... agree=True failed=0`. On Windows node PCs
this is also when the firewall alert for `cloud-node.exe` appears: allow it,
then re-run. If a run says `FAILED ... node 2 start`, look at that PC's
`rescuemesh-lab-7400/node.log` in its temp folder.

## 5. Controller: the experiments

In order of importance; each line can be run on its own.

| Command | Runs | Time (approx.) | Shows |
|---|---|---|---|
| `python3 scripts/recovery_bench.py restart --reps 5 --lab lab.json` | 10 | 5 min | R5: crash + restart, on_start off vs on |
| `python3 scripts/recovery_bench.py ready_rule --reps 5 --lab lab.json` | 10 | 5 min | R4: second failure, both_agree vs majority |
| `python3 scripts/recovery_bench.py transfer --reps 3 --lab lab.json` | 36 | 25 min | R2/R3: full vs delta, locked vs chunked, real network |
| `python3 scripts/recovery_bench.py load --reps 3 --lab lab.json` | 54 | 30 min | failover and recovery vs rate and outage |

Or everything at once: `python3 scripts/recovery_bench.py all --reps 3 --lab lab.json`.
If something interrupts a sweep, re-run the same command with `--resume`.

## 6. Controller: tables and figures

```bash
python3 scripts/recovery_report.py --lab
```

Writes `experiments/recovery/lab/summary.md` and `docs/img/recovery/lab/*.png`,
next to the one-machine results, so the report can show both.

## What runs where

| What | Where |
|---|---|
| Unit and integration tests (`cargo test`) | one machine; they start their own nodes in one process. Run them on any PC before the lab session. |
| Recovery experiments | all six PCs, with this guide |
| Demo scenarios (normal, load imbalance, failure, recovery) | all six PCs, with docs/quick-setup.md and docs/deployment.md "Test sequence" (nodes started with `up.sh`, not the agent) |
