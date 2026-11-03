#!/usr/bin/env python3
"""Lab agent: lets scripts/recovery_bench.py drive this PC during experiments.

Run one on every lab PC (docs/lab-experiments.md):

    python3 scripts/lab_agent.py            # listens on 0.0.0.0:7400

The controller (recovery_bench.py --lab lab.json) then asks it, over HTTP, to
start / kill this PC's cloud-node with a config it sends, or to run a client
workload and send back the client's per-request CSV. It also reports this
PC's clock, so the controller can line up timestamps taken on different
machines. Standard library only; works on Windows, Linux and macOS.

Endpoints (all JSON):
    GET  /time                       {"ms": unix ms now}
    POST /node/start  {"config": "<node toml>", "node_port": 7001}
    POST /node/kill
    POST /client/start {"config": "<client toml>", "count", "rate", "complexity", "seed"} -> {"job"}
    POST /client/wait  {"job"}       -> {"csv": "<file contents>"}  (blocks until done)
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXE = ".exe" if os.name == "nt" else ""
BIN = ROOT / "target" / "release"
WORK = Path(tempfile.gettempdir()) / "rescuemesh-lab"

lock = threading.Lock()
node = {"proc": None, "log": None}
jobs = {}


def reachable(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def kill_node():
    p = node["proc"]
    if p and p.poll() is None:
        p.kill()
        p.wait()
    node["proc"] = None
    if node["log"]:
        node["log"].close()
        node["log"] = None


def start_node(config, node_port):
    kill_node()
    WORK.mkdir(parents=True, exist_ok=True)
    cfg = WORK / "node.toml"
    cfg.write_text(config)
    # The port can stay busy for a moment after a kill: retry the start.
    deadline = time.time() + 15
    while time.time() < deadline:
        node["log"] = open(WORK / "node.log", "a")
        node["proc"] = subprocess.Popen(
            [str(BIN / f"cloud-node{EXE}"), "--config", str(cfg)],
            stdout=node["log"], stderr=subprocess.STDOUT,
            env={**os.environ, "RUST_LOG": "info"})
        for _ in range(40):
            if node["proc"].poll() is not None:
                break  # exited: port still in use; try again
            if reachable(node_port):
                return
            time.sleep(0.05)
        kill_node()
        time.sleep(0.3)
    raise RuntimeError(f"cloud-node did not start on port {node_port}; see {WORK / 'node.log'}")


def run_client(job, req):
    WORK.mkdir(parents=True, exist_ok=True)
    cfg = WORK / f"client_{job}.toml"
    out = WORK / f"client_{job}.csv"
    cfg.write_text(req["config"])
    p = subprocess.run(
        [str(BIN / f"client{EXE}"), "--config", str(cfg),
         "--count", str(req["count"]), "--rate", str(req["rate"]),
         "--complexity", str(req["complexity"]), "--seed", str(req["seed"]),
         "--out", str(out)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env={**os.environ, "RUST_LOG": "error"})
    jobs[job]["csv"] = out.read_text() if out.exists() else ""
    jobs[job]["code"] = p.returncode
    jobs[job]["done"].set()


class Handler(BaseHTTPRequestHandler):
    def reply(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/time":
            return self.reply({"ms": int(time.time() * 1000)})
        self.reply({"error": "not found"}, 404)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        req = json.loads(self.rfile.read(n) or b"{}")
        try:
            if self.path == "/node/start":
                with lock:
                    start_node(req["config"], req["node_port"])
                return self.reply({"ok": True})
            if self.path == "/node/kill":
                with lock:
                    kill_node()
                return self.reply({"ok": True})
            if self.path == "/client/start":
                job = f"{time.time_ns()}"
                jobs[job] = {"done": threading.Event()}
                threading.Thread(target=run_client, args=(job, req), daemon=True).start()
                return self.reply({"job": job})
            if self.path == "/client/wait":
                j = jobs[req["job"]]
                j["done"].wait()
                jobs.pop(req["job"], None)
                return self.reply({"csv": j["csv"], "code": j["code"]})
        except Exception as e:  # report to the controller instead of dying
            return self.reply({"error": repr(e)}, 500)
        self.reply({"error": "not found"}, 404)

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[agent] {self.address_string()} {fmt % args}\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=7400)
    args = ap.parse_args()
    # Per agent port, so several agents on one machine (a local rehearsal) don't
    # overwrite each other's files.
    global WORK
    WORK = WORK.with_name(f"rescuemesh-lab-{args.port}")
    for b in ("cloud-node", "client"):
        if not (BIN / f"{b}{EXE}").exists():
            print(f"warning: {BIN / (b + EXE)} not found; run `cargo build --release` first", file=sys.stderr)
    print(f"lab agent listening on 0.0.0.0:{args.port} (repo {ROOT})", flush=True)
    try:
        ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()
    finally:
        kill_node()


if __name__ == "__main__":
    main()
