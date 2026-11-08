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

All six PCs must be on the same network (same lab subnet, one hotspot, or one
ZeroTier network). The PCs find each other by broadcast, so there are no IPs to
type.

## 2. One command on each PC

Open a terminal and paste this, with the PC's number at the end
(1, 2, 3 = nodes; 4, 5, 6 = clients). Each PC gets a different number:

```bash
curl -fsSL https://raw.githubusercontent.com/mostafa21314/RescueMesh/demo/six-pc-live/live-demo/oneclick.sh | bash -s 1
```

That's it. It installs what's missing (Rust into your home folder; it asks for
sudo only if `git`, `curl`, `python3` or a C compiler are missing), downloads the
code to `~/RescueMesh-demo`, builds (a few minutes the first time), finds the
other PCs, starts this PC's part and opens the dashboard. Leave the terminal
open; Ctrl+C stops that PC. Nodes start as soon as PCs 1, 2 and 3 have all
found each other.

**Next time:** double-click **RescueMesh PC N** on the Desktop (it updates the
code and starts again). If Ubuntu asks, choose "Trust and Launch". Or run
`bash ~/RescueMesh-demo/live-demo/oneclick.sh N`.

If the firewall is on (`sudo ufw status` says active) and the script could not
open it, run once:

```bash
sudo ufw allow 7500 && sudo ufw allow 7001:7003/udp
```

### If broadcast is blocked: give the IPs instead

Some networks drop broadcasts, and then the cards stay "not found yet". Get each
PC's IP with `hostname -I`, then on every PC (inside `~/RescueMesh-demo`), with
the same six IPs in PC order and only the first number changing:

```bash
bash live-demo/start.sh 1 IP1 IP2 IP3 IP4 IP5 IP6
```

## 3. Open the dashboard

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

- A card says **not found yet**: that PC isn't running, or broadcasts don't get
  through (use `start.sh` with IPs, above).
- A card says **agent down**: that PC stopped, or TCP 7500 is blocked. Test from
  another PC: `curl http://IPn:7500/api/state`.
- Red banner **two PCs claim the same number**: two PCs were given the same
  number; stop one and start it with the right number.
- Nodes **disagree on server** or `results` differ for long: the nodes can't
  reach each other on UDP 7001–7003; check the firewall and the IPs.
- With `start.sh`: `WARNING: this PC has ... but lab.json says ...` means a wrong
  PC number or IP order; run it again with all six IPs to fix them.
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
their state every second and forwards button presses to them. With
`oneclick.sh` each agent broadcasts "I am PC N" on UDP 7500 every second and
learns the other PCs' IPs from theirs. The agent has no
authentication: run it only on the lab network.
