#!/usr/bin/env python3
"""Recovery experiments (docs/recovery.md, "Experiments").

Each run starts three real cloud-node processes on 127.0.0.1, fills their
tables to a target size, runs an open-loop client workload, fails the elected
server in the middle of it (mute = temporary failure, kill = process crash
and restart), and records one summary row: the returning node's recovery
report, the survivors' snapshot lock holds, client latency before / during /
after the outage, elections, and whether all three results tables agree at
the end.

    python3 scripts/recovery_bench.py transfer      # R2/R3: full vs delta, locked vs chunked
    python3 scripts/recovery_bench.py load          # recovery vs request rate and outage length
    python3 scripts/recovery_bench.py ready_rule    # R4: a second node fails during recovery
    python3 scripts/recovery_bench.py restart       # R5: crash + restart, on_start off/on
    python3 scripts/recovery_bench.py all --reps 3  # everything, fewer repetitions

Summary rows go to experiments/recovery/<experiment>.csv (appended, so an
interrupted sweep can be resumed with --resume); per-request client logs go to
experiments/recovery/raw/. Run `cargo build --release` first.

Lab mode: `--lab lab.json` runs the same experiments on real machines. Each
lab PC runs scripts/lab_agent.py; the nodes are started, killed and restarted
through their agents, and the workload is split over the client PCs' agents.
Results go to experiments/recovery/lab/. See docs/lab-experiments.md.
"""

import argparse
import csv
import itertools
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target" / "release"
OUT = ROOT / "experiments" / "recovery"
RAW = OUT / "raw"
BASE_PORT = 7301
REQUEST_TIMEOUT_MS = 1000
MAX_ATTEMPTS = 8  # 8 attempts > 20 s outage + recovery at 1 s per attempt

# ---------------------------------------------------------------- wire

def call(addr, msg, timeout=2.0):
    """One request/response over the node's framing: 4-byte length + JSON.
    `addr` is (host, port)."""
    with socket.create_connection(addr, timeout=timeout) as s:
        body = json.dumps(msg).encode()
        s.sendall(struct.pack(">I", len(body)) + body)
        n = struct.unpack(">I", _read(s, 4))[0]
        return json.loads(_read(s, n))


def _read(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf


def status(addr, timeout=2.0):
    try:
        return call(addr, {"type": "StatusRequest"}, timeout)
    except (OSError, ConnectionError, ValueError, struct.error):
        return None


# ---------------------------------------------------------------- cluster

def node_toml(i, recovery, addrs, listen_host="127.0.0.1"):
    peers = ", ".join(
        f'{{ id = {k + 1}, addr = "{addrs[k][0]}:{addrs[k][1]}" }}' for k in range(3) if k != i
    )
    rec = "\n".join(f"{k} = {json.dumps(v)}" for k, v in recovery.items())
    return f"""node_id = {i + 1}
listen = "{listen_host}:{addrs[i][1]}"
peers = [{peers}]
election_mode = "on_demand"
policy = "Composite"
bid_timeout_ms = 100
degraded_after_ms = 3000
min_tenure_ms = {REQUEST_TIMEOUT_MS}
cpu_source = "process"
workers = 3
seed = 42
queue_capacity = 4096
table_ttl_ms = 1800000

[recovery]
{rec}
"""


def client_toml(addrs):
    return ("nodes = [%s]\nrequest_timeout_ms = %d\nmax_attempts = %d\n"
            % (", ".join(f'"{h}:{p}"' for h, p in addrs), REQUEST_TIMEOUT_MS, MAX_ATTEMPTS))


class Cluster:
    """Three nodes and the client as processes on this machine."""

    def __init__(self, recovery, workdir):
        self.dir = Path(workdir)
        self.addrs = [("127.0.0.1", BASE_PORT + k) for k in range(3)]
        self.ports = [p for _, p in self.addrs]
        self.procs = [None] * 3
        self.logs = []
        for i in range(3):
            (self.dir / f"node{i + 1}.toml").write_text(node_toml(i, recovery, self.addrs))
        (self.dir / "client.toml").write_text(client_toml(self.addrs))

    def start(self, i):
        log = open(self.dir / f"node{i + 1}.log", "a")
        self.logs.append(log)
        self.procs[i] = subprocess.Popen(
            [str(BIN / "cloud-node"), "--config", str(self.dir / f"node{i + 1}.toml")],
            stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "RUST_LOG": "info"},
        )
        wait_for(lambda: status(self.addrs[i], 0.5) is not None, 10, f"node {i + 1} start")

    def kill(self, i):
        self.procs[i].send_signal(signal.SIGKILL)
        self.procs[i].wait()
        self.procs[i] = None

    def stop(self):
        for p in self.procs:
            if p:
                p.send_signal(signal.SIGKILL)
                p.wait()
        for log in self.logs:
            log.close()
        # Wait until the ports are free for the next run.
        for port in self.ports:
            wait_for(lambda: _port_free(port), 10, f"port {port} free")

    def statuses(self):
        return [status(a) for a in self.addrs]

    def client(self, count, rate, complexity, out, seed):
        return subprocess.Popen(
            [str(BIN / "client"), "--config", str(self.dir / "client.toml"),
             "--count", str(count), "--rate", str(rate), "--complexity", str(complexity),
             "--seed", str(seed), "--out", str(out)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env={**os.environ, "RUST_LOG": "error"},
        )


def agent(addr, path, body=None, timeout=60):
    """One call to a lab agent (scripts/lab_agent.py)."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(f"http://{addr}{path}", data=data,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def clock_offset(addr):
    """Agent clock minus this machine's clock, in ms (midpoint of a round trip)."""
    best = None
    for _ in range(5):
        t0 = time.time() * 1000
        ms = agent(addr, "/time", timeout=5)["ms"]
        t1 = time.time() * 1000
        if best is None or t1 - t0 < best[0]:
            best = (t1 - t0, ms - (t0 + t1) / 2)
    return best[1]


class LabCluster:
    """Three nodes on lab PCs, driven through their agents; the workload is split
    over the client PCs' agents. Same interface as Cluster."""

    def __init__(self, recovery, workdir, lab):
        self.dir = Path(workdir)
        self.addrs = [(n["host"], n["port"]) for n in lab["nodes"]]
        self.agents = [n.get("agent", f"{n['host']}:7400") for n in lab["nodes"]]
        self.client_agents = lab["clients"]
        self.configs = [node_toml(i, recovery, self.addrs, listen_host="0.0.0.0") for i in range(3)]

    def start(self, i):
        agent(self.agents[i], "/node/start", {"config": self.configs[i], "node_port": self.addrs[i][1]})
        wait_for(lambda: status(self.addrs[i], 0.5) is not None, 15, f"node {i + 1} start")

    def kill(self, i):
        agent(self.agents[i], "/node/kill", {})

    def stop(self):
        for i in range(3):
            try:
                self.kill(i)
            except OSError as e:
                print(f"warning: could not stop node {i + 1}: {e}", flush=True)

    def statuses(self):
        return [status(a) for a in self.addrs]

    def client(self, count, rate, complexity, out, seed):
        return LabClients(self, count, rate, complexity, out, seed)


class LabClients:
    """One workload split evenly over the client PCs; `wait()` merges their CSVs
    into `out`, with every timestamp moved onto this machine's clock."""

    def __init__(self, c, count, rate, complexity, out, seed):
        self.out = Path(out)
        n = len(c.client_agents)
        self.parts = []
        for k, a in enumerate(c.client_agents):
            share = count // n + (1 if k < count % n else 0)
            if share == 0:
                continue
            offset = clock_offset(a)
            job = agent(a, "/client/start", {
                "config": client_toml(c.addrs), "count": share, "rate": rate / n,
                "complexity": complexity, "seed": seed * 10 + k})["job"]
            self.parts.append((a, job, offset))

    def wait(self):
        header, rows = None, []
        for a, job, offset in self.parts:
            text = agent(a, "/client/wait", {"job": job}, timeout=None)["csv"]
            lines = text.splitlines()
            if not lines:
                continue
            header = lines[0]
            cols = header.split(",")
            for line in lines[1:]:
                v = line.split(",")
                for name in ("sent_at_ms", "response_at_ms"):
                    j = cols.index(name)
                    if v[j]:
                        v[j] = str(int(int(v[j]) - offset))
                rows.append(",".join(v))
        self.out.write_text("\n".join([header or ""] + rows) + "\n")


def _port_free(port):
    with socket.socket() as s:
        # Like the node's own bind (tokio sets SO_REUSEADDR): TIME_WAIT
        # connections from the last run don't block it.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def wait_for(cond, limit_s, what):
    deadline = time.time() + limit_s
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.05)
    raise TimeoutError(what)


def now_ms():
    return int(time.time() * 1000)


# ---------------------------------------------------------------- one run

def full_recovery(recovery):
    return {"transfer": "full", "snapshot": "locked", "ready_rule": "both_agree",
            "on_start": False, **recovery}


def run_tag(exp, rep, *, recovery, rate, outage_ms, table, mode="mute", **_):
    rec = full_recovery(recovery)
    return (f"{exp}_{rec['transfer']}_{rec['snapshot']}_{rec['ready_rule']}_"
            f"{'start' if rec['on_start'] else 'nostart'}_{mode}_r{rate}_o{outage_ms}_t{table}_{rep}")


def run(exp, rep, *, recovery, rate, outage_ms, table, mode="mute",
        kill_survivor=False, complexity=5, pre_s=3.0, post_s=6.0, ready_limit_s=30.0):
    """One experiment run; returns the summary row."""
    rec = full_recovery(recovery)
    tag = run_tag(exp, rep, recovery=recovery, rate=rate, outage_ms=outage_ms, table=table, mode=mode)
    RAW.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        c = LabCluster(rec, tmp, LAB) if LAB else Cluster(rec, tmp)
        try:
            for i in range(3):
                c.start(i)
            wait_for(lambda: all(s and s["phase"] == "ready" for s in c.statuses()), 15, "cluster ready")
            # Fill the tables: cheap requests, as fast as the cluster takes them.
            if table:
                pre = c.client(table, 0, 1, Path(tmp) / "preload.csv", 1000 + rep)
                pre.wait()
            settle(c)
            warm = c.client(1, 0, 1, Path(tmp) / "warm.csv", 2000 + rep)
            warm.wait()
            settle(c)
            before = c.statuses()
            server = before[0]["server"]
            victim = server - 1
            epoch_before = max(s["epoch"] for s in before)

            # Open-loop workload spanning the outage and the recovery.
            span = pre_s + outage_ms / 1000 + post_s
            count = int(rate * span)
            csv_path = RAW / f"{tag}.csv"
            load = c.client(count, rate, complexity, csv_path, 3000 + rep)
            t_load = now_ms()
            time.sleep(pre_s)

            t_fault = now_ms()
            if mode == "mute":
                ack = call(c.addrs[victim], {"type": "InjectFailure", "duration_ms": outage_ms})
                assert ack["type"] == "FailureAccepted", ack
            else:
                c.kill(victim)
            if kill_survivor:
                # A second, independent failure while the first node is away.
                survivor = next(k for k in range(3) if k != victim)
                time.sleep(min(0.5, outage_ms / 2000))
                c.kill(survivor)
            time.sleep(outage_ms / 1000)
            t_back = now_ms()
            if mode == "kill":
                c.start(victim)

            # Ready, or not within the limit (e.g. both_agree with a survivor down).
            recovered = True
            try:
                wait_for(lambda: (s := status(c.addrs[victim], 0.5)) is not None
                         and s["phase"] == "ready", ready_limit_s, "victim ready")
            except TimeoutError:
                recovered = False
            t_ready = now_ms()
            load.wait()
            after = settle(c)
            row = summarize(
                exp, rep, rec, mode, rate, outage_ms, table, complexity, kill_survivor,
                before, after, victim, epoch_before, recovered,
                t_load, t_fault, t_back, t_ready, csv_path, count)
            return row
        finally:
            c.stop()


def settle(c, limit_s=15):
    """Polls until no reachable node has queued or running work, twice in a row."""
    quiet = 0
    deadline = time.time() + limit_s
    sts = c.statuses()
    while time.time() < deadline and quiet < 2:
        time.sleep(0.15)
        sts = c.statuses()
        busy = any(s and (s["queue_len"] or s["running"]) for s in sts)
        quiet = 0 if busy else quiet + 1
    time.sleep(0.3)  # replicas sent after the last job finished
    return c.statuses()


def pct(xs, p):
    if not xs:
        return ""
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(round((len(xs) - 1) * p)))], 2)


def summarize(exp, rep, rec, mode, rate, outage_ms, table, complexity, kill_survivor,
              before, after, victim, epoch_before, recovered,
              t_load, t_fault, t_back, t_ready, csv_path, count):
    rows = list(csv.DictReader(open(csv_path)))
    ans = [r for r in rows if r["answered"] == "true"]
    lat = lambda rs: [float(r["latency_ms"]) for r in rs if r["latency_ms"]]
    sent = lambda r: int(r["sent_at_ms"])
    pre = [r for r in ans if sent(r) < t_fault]
    during = [r for r in ans if t_fault <= sent(r) < t_back]
    # Requests sent while the victim recovers, plus 1 s: where a survivor's
    # snapshot lock hold would show as client latency.
    rec_win = [r for r in ans if t_back <= sent(r) < t_ready + 1000]
    post = [r for r in ans if sent(r) >= t_ready + 1000]
    # Failover: from the fault until a survivor first answers a request sent
    # after the fault.
    first = [int(r["response_at_ms"]) for r in ans
             if sent(r) >= t_fault and r["served_by"] and int(r["served_by"]) != victim + 1]
    failover_ms = (min(first) - t_fault) if first else ""
    v_after = after[victim]
    survivors = [s for k, s in enumerate(after) if k != victim and s]
    rep_ = (v_after or {}).get("last_recovery") or {}
    live = [s for s in after if s]
    digests = {s["results_digest"] for s in live}
    return {
        "experiment": exp, "rep": rep, "mode": mode,
        "transfer": rec["transfer"], "snapshot": rec["snapshot"],
        "ready_rule": rec["ready_rule"], "on_start": rec["on_start"],
        "rate": rate, "outage_ms": outage_ms, "table": table, "complexity": complexity,
        "kill_survivor": kill_survivor, "requests": count,
        "answered": len(ans), "failed": count - len(ans),
        "recovered": recovered,
        "recovery_total_ms": rep_.get("total_ms", ""),
        "recovery_drain_ms": rep_.get("drain_ms", ""),
        "recovery_fetch_ms": rep_.get("fetch_ms", ""),
        "recovery_install_ms": rep_.get("install_ms", ""),
        "recovery_rounds": rep_.get("rounds", ""),
        "snapshots_used": rep_.get("snapshots_used", ""),
        "records_received": rep_.get("records_received", ""),
        "bytes_received": rep_.get("bytes_received", ""),
        "new_results": rep_.get("new_results", ""),
        "republished": rep_.get("republished", ""),
        "observed_ready_ms": t_ready - t_back if recovered else "",
        "survivor_lock_max_us": max((s.get("snapshot_lock_max_us", 0) for s in survivors), default=""),
        "failover_ms": failover_ms,
        "lat_pre_p50": pct(lat(pre), .5), "lat_pre_p99": pct(lat(pre), .99),
        "lat_outage_p50": pct(lat(during), .5), "lat_outage_p99": pct(lat(during), .99),
        "lat_outage_max": pct(lat(during), 1),
        "lat_recovery_p50": pct(lat(rec_win), .5), "lat_recovery_p99": pct(lat(rec_win), .99),
        "lat_recovery_max": pct(lat(rec_win), 1),
        "lat_post_p50": pct(lat(post), .5), "lat_post_p99": pct(lat(post), .99),
        "throughput": round(len(ans) / max(1e-3, (max(int(r["response_at_ms"]) for r in ans) - t_load) / 1000), 1) if ans else "",
        "max_attempts_used": max((int(r["attempts"]) for r in ans), default=""),
        "elections_after_fault": max(s["epoch"] for s in live) - epoch_before,
        "victim_results": (v_after or {}).get("results", ""),
        "survivor_results": max((s["results"] for s in survivors), default=""),
        "live_nodes": len(live),
        # Among the nodes still running (a run may kill a survivor on purpose).
        "tables_agree": len(digests) == 1,
        "replicate_conflicts": sum(s["replicate_conflicts"] for s in live),
        "victim_served_after_back": sum(
            1 for r in ans if sent(r) >= t_back and r["served_by"] == str(victim + 1)),
        "raw": csv_path.name,
    }


# ---------------------------------------------------------------- experiments

def transfer(reps):
    for table, (tr, sn), rep in itertools.product(
            [2000, 20000, 60000],
            [("full", "locked"), ("full", "chunked"), ("delta", "locked"), ("delta", "chunked")],
            range(reps)):
        yield dict(recovery={"transfer": tr, "snapshot": sn}, rate=100, outage_ms=10000, table=table), rep


def load(reps):
    for (tr, sn), rate, outage, rep in itertools.product(
            [("full", "locked"), ("delta", "chunked")], [50, 100, 200], [5000, 10000, 20000], range(reps)):
        yield dict(recovery={"transfer": tr, "snapshot": sn}, rate=rate, outage_ms=outage, table=2000), rep


def ready_rule(reps):
    for rule, rep in itertools.product(["both_agree", "majority"], range(reps)):
        yield dict(recovery={"ready_rule": rule}, rate=50, outage_ms=3000, table=1000,
                   kill_survivor=True, ready_limit_s=15.0, post_s=4.0), rep


def restart(reps):
    for on_start, rep in itertools.product([False, True], range(reps)):
        yield dict(recovery={"on_start": on_start}, rate=100, outage_ms=5000, table=2000, mode="kill"), rep


EXPERIMENTS = {"transfer": transfer, "load": load, "ready_rule": ready_rule, "restart": restart}
LAB = None  # the parsed lab.json in lab mode


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("experiment", choices=[*EXPERIMENTS, "all"])
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--resume", action="store_true", help="skip runs already in the CSV")
    ap.add_argument("--lab", type=Path, help="lab.json: run on lab PCs through their agents")
    args = ap.parse_args()
    global LAB, OUT, RAW
    if args.lab:
        LAB = json.loads(args.lab.read_text())
        OUT = OUT / "lab"
        RAW = OUT / "raw"
        for a in [n.get("agent", f"{n['host']}:7400") for n in LAB["nodes"]] + LAB["clients"]:
            try:
                print(f"agent {a}: clock offset {clock_offset(a):+.0f} ms", flush=True)
            except OSError as e:
                sys.exit(f"agent {a} unreachable: {e}")
    names = list(EXPERIMENTS) if args.experiment == "all" else [args.experiment]
    OUT.mkdir(parents=True, exist_ok=True)
    for name in names:
        path = OUT / f"{name}.csv"
        done = set()
        if args.resume and path.exists():
            done = {r["raw"] for r in csv.DictReader(open(path))}
        elif path.exists():
            path.unlink()
        plan = list(EXPERIMENTS[name](args.reps))
        for k, (params, rep) in enumerate(plan, 1):
            label = json.dumps({**params, "rep": rep})
            if run_tag(name, rep, **params) + ".csv" in done:
                continue
            started = time.time()
            try:
                row = run(name, rep, **params)
            except Exception as e:  # keep the sweep going; the row is missing
                print(f"[{name} {k}/{len(plan)}] FAILED {label}: {e!r}", flush=True)
                continue
            new = not path.exists()
            with open(path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(row))
                if new:
                    w.writeheader()
                w.writerow(row)
            print(f"[{name} {k}/{len(plan)}] {time.time() - started:.0f}s "
                  f"recovered={row['recovered']} total={row['recovery_total_ms']}ms "
                  f"records={row['records_received']} lock={row['survivor_lock_max_us']}us "
                  f"rec_p99={row['lat_recovery_p99']} agree={row['tables_agree']} "
                  f"failed={row['failed']}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
