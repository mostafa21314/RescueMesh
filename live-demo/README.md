# RescueMesh live demo on six Ubuntu PCs

The real system, not a simulation: three PCs run the cloud nodes, three PCs run
clients that send incidents, and a dashboard in the browser shows the elected
server, epochs, queues, latency and which node served each request. From the
dashboard you can switch election mode, set the load, mute a node for up to
20 s, put CPU load on it, kill it and start it again.

| PC | Runs | Port |
|---|---|---|
| PC 1 | node 1 | UDP 7001 |
| PC 2 | node 2 | UDP 7002 |
| PC 3 | node 3 | UDP 7003 |
| PC 4, 5, 6 | client (load generator) | outgoing UDP |
| all six | demo agent + dashboard | TCP 7500 |

## 1. Network

All six PCs must reach each other (same lab subnet, one hotspot, or ZeroTier).
On each PC, note its IP:

```bash
hostname -I
```

Write down the six IPs in order: `IP1` (PC 1) … `IP6` (PC 6).

## 2. First time on each PC

```bash
sudo apt install -y build-essential curl git python3
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source ~/.cargo/env
git clone -b demo/six-pc-live https://github.com/mostafa21314/RescueMesh.git
cd RescueMesh
```

No sudo? Skip the `apt` line if `gcc`, `curl` and `python3` are already there;
rustup installs into your home folder.

## 3. Start each PC

Same six IPs, same order, on every PC; only the first number changes:

```bash
bash live-demo/start.sh 1 IP1 IP2 IP3 IP4 IP5 IP6     # on PC 1
bash live-demo/start.sh 2 IP1 IP2 IP3 IP4 IP5 IP6     # on PC 2
bash live-demo/start.sh 3 IP1 IP2 IP3 IP4 IP5 IP6     # on PC 3
bash live-demo/start.sh 4 IP1 IP2 IP3 IP4 IP5 IP6     # on PC 4
bash live-demo/start.sh 5 IP1 IP2 IP3 IP4 IP5 IP6     # on PC 5
bash live-demo/start.sh 6 IP1 IP2 IP3 IP4 IP5 IP6     # on PC 6
```

The first run builds the Rust binaries (a few minutes) and saves the IPs to
`live-demo/lab.json`; after that `bash live-demo/start.sh N` is enough. Leave the
terminal open. Ctrl+C stops that PC (and its node).

If the firewall is on and the script could not open it (it only does so when
`sudo` works without a password), run once:

```bash
sudo ufw allow 7500/tcp
sudo ufw allow 7001:7003/udp
```

## 4. Open the dashboard

On any of the six PCs open **http://localhost:7500** in the browser (Firefox is
fine). From a laptop on the same network: `http://IP4:7500` (any PC's IP works).

- **Start all clients** sends load from PCs 4–6 (req/s per PC). Each client card
  shows completed req/s, p50/p95 latency, failures, retries and the share of
  requests each node served.
- **Mute 5 s / 20 s** silences a node; mute the SERVER node to watch the others
  elect a new one, the clients retry and fail over, and the muted node recover
  and rejoin.
- **CPU load 25 s** burns half the node's CPU threads (changes its load score).
- **Kill** crashes the node process; **Start** restarts it and it pulls state
  from its peers before serving.
- **Election mode** + **Restart nodes in this mode** switches all three nodes
  between `on_demand` and `per_request` (restarting empties their tables).
- **Verify convergence** compares the three nodes' results tables (count and
  digest). Let traffic stop for a few seconds first.

## Troubleshooting

- A card says **agent down**: that PC's `start.sh` isn't running, the IP in
  `lab.json` is wrong, or TCP 7500 is blocked. Test from another PC:
  `curl http://IPn:7500/api/state`.
- Nodes **disagree on server** or `results` differ for long: the nodes can't
  reach each other on UDP 7001–7003; check the firewall and the IPs.
- `WARNING: this PC has ... but lab.json says ...`: wrong PC number or IP order.
- Wrong IPs saved: run `start.sh N` again with all six IPs.
- Logs: `live-demo/run/pcN/nodeN.log` on PCs 1–3.

## Rehearse on one machine

```bash
cargo build --release
for i in 1 2 3 4 5 6; do python3 live-demo/agent.py --pc $i --lab live-demo/lab.local.json & done
# dashboard: http://localhost:7504   stop: kill %1 %2 %3 %4 %5 %6
```

## How it works

`agent.py` (Python standard library only) runs on every PC. On PCs 1–3 it
starts the real `cloud-node` with a config written from `lab.json`, reads its
state over the UDP control protocol (`ProbeRequest`, `StatusRequest`) and sends
`InjectFailure` / `SetLoad` for mute and CPU load. On PCs 4–6 it runs the real
`client` binary in overlapping 2-second batches and reads each batch's
per-request CSV. The agent that serves the dashboard asks the other five for
their state every second and forwards button presses to them. The agent has no
authentication: run it only on the lab network.
