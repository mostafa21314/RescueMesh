#!/usr/bin/env python3
"""RescueMesh six-PC live demo agent (Python 3.8+, standard library only).

Run one agent on each of the six PCs:

    PC 1-3  run the real cloud node (node 1-3, UDP 7001-7003)
    PC 4-6  run the real client binary as a load generator

Every agent serves the dashboard on TCP 7500, so open http://localhost:7500
on any of the six PCs. The dashboard's agent asks the other five for their
state and forwards button presses to them. See live-demo/README.md.
"""
import argparse
import csv
import json
import os
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import request as urlreq

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from udp_rr import call  # noqa: E402  (UDP control protocol, scripts/udp_rr.py)

BIN = Path(os.environ.get('RESCUEMESH_BIN_DIR', str(ROOT / 'target' / 'release')))
NODE_PORT = {1: 7001, 2: 7002, 3: 7003}
BATCH_S = 2.0          # each client process sends BATCH_S seconds of requests
WINDOW_S = 10.0        # live stats window
MODES = ('on_demand', 'per_request')

NODE_TOML = """# Written by live-demo/agent.py; edit live-demo/lab.json, not this file.
node_id = {n}
listen = "0.0.0.0:{port}"
peers = [{peers}]
election_mode = "{mode}"
policy = "Composite"
bid_timeout_ms = 100
degraded_after_ms = 3000
min_tenure_ms = 1000
cpu_source = "machine"
workers = 3
seed = 42
queue_capacity = 4096
table_ttl_ms = 1800000

[recovery]
transfer = "delta"
snapshot = "chunked"
ready_rule = "majority"
on_start = true

[scheduling]
policy = "priority_decay"
priority_boost_ms = 250
priority_half_life_ms = 1000
request_lease_ms = 10000
"""

CLIENT_TOML = """# Written by live-demo/agent.py
nodes = [{nodes}]
request_timeout_ms = 1000
max_attempts = 8
"""

EVENT_RE = re.compile(r'elect|server|mute|fail|recover|rejoin|step|ready|start|degrad|WARN|ERROR', re.I)
ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')


def now_ms():
    return int(time.time() * 1000)


class Lab:
    """The six PCs' IPs: fixed (lab.json) or found by LAN broadcast (--discover)."""

    def __init__(self, pcs=None, agent_ports=None, discover=False):
        self.pcs = list(pcs) if pcs else [None] * 6
        self.agent_ports = agent_ports or [7500] * 6
        self.discover = discover
        self.ids = [None] * 6
        self.seen = [0.0] * 6
        self.conflicts = {}
        self.lock = threading.Lock()

    @classmethod
    def load(cls, path):
        data = json.loads(Path(path).read_text())
        if len(data['pcs']) != 6:
            raise SystemExit(f'{path}: "pcs" must list six IPs (PC 1..6)')
        port = int(data.get('agent_port', 7500))
        return cls(data['pcs'], data.get('agent_ports') or [port] * 6)

    def saw(self, pc, ip, iid):
        """A beacon from PC `pc` (instance `iid`) arrived from `ip`."""
        now, i = time.time(), pc - 1
        with self.lock:
            same_owner = self.ids[i] in (None, iid) or self.pcs[i] == ip
            if same_owner or now - self.seen[i] > 6:
                # A new owner (or the old one gone quiet for 6 s) takes the slot;
                # the same owner beaconing from a second interface keeps its first IP.
                if self.pcs[i] is None or (self.ids[i] != iid and self.pcs[i] != ip):
                    print(f'found PC {pc} at {ip}' + (f' (was {self.pcs[i]})' if self.pcs[i] else ''), flush=True)
                    self.pcs[i] = ip
                self.ids[i] = iid
                self.seen[i] = now
                self.conflicts.pop(pc, None)
            else:
                if pc not in self.conflicts:
                    print(f'WARNING: two machines say they are PC {pc}: {self.pcs[i]} and {ip}', flush=True)
                self.conflicts[pc] = [self.pcs[i], ip]

    def view(self):
        return {'pcs': self.pcs, 'discover': self.discover,
                'conflicts': {str(k): v for k, v in self.conflicts.items()}}

    def agent(self, pc):
        ip = self.pcs[pc - 1]
        return None if ip is None else f'{ip}:{self.agent_ports[pc - 1]}'

    def node(self, n):
        ip = self.pcs[n - 1]
        if ip is None:
            raise LookupError(f'PC {n} (node {n}) not found yet: is its oneclick.sh running?')
        return (ip, NODE_PORT[n])

    def nodes_known(self):
        return all(self.pcs[:3])


def broadcast_addrs():
    out = {'255.255.255.255'}
    try:
        txt = subprocess.run(['ip', '-o', '-4', 'addr', 'show'], capture_output=True,
                             text=True, timeout=2).stdout
        out.update(re.findall(r'\bbrd (\d+\.\d+\.\d+\.\d+)', txt))
    except (OSError, subprocess.SubprocessError):
        pass
    return sorted(out)


class Beacon:
    """Every second, broadcast 'I am PC n' on UDP agent_port; record everyone else's."""

    def __init__(self, lab, pc, port):
        self.lab, self.pc, self.port = lab, pc, port
        self.iid = '%016x' % random.getrandbits(64)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind(('', port))
        threading.Thread(target=self.send_loop, daemon=True).start()
        threading.Thread(target=self.recv_loop, daemon=True).start()

    def send_loop(self):
        msg = json.dumps({'rescuemesh_demo': 1, 'pc': self.pc, 'id': self.iid}).encode()
        addrs, k = broadcast_addrs(), 0
        while True:
            k += 1
            if k % 10 == 0:
                addrs = broadcast_addrs()
            for a in addrs:
                try:
                    self.sock.sendto(msg, (a, self.port))
                except OSError:
                    pass
            time.sleep(1)

    def recv_loop(self):
        while True:
            try:
                data, (ip, _) = self.sock.recvfrom(2048)
                m = json.loads(data)
                if m.get('rescuemesh_demo') == 1 and 1 <= int(m['pc']) <= 6 and not ip.startswith('127.'):
                    self.lab.saw(int(m['pc']), ip, str(m.get('id')))
            except (OSError, ValueError, KeyError, TypeError):
                time.sleep(.1)


class Node:
    """Owns this PC's cloud-node process."""

    def __init__(self, n, lab, run_dir):
        self.n, self.lab, self.run = n, lab, run_dir
        self.mode = 'on_demand'
        self.proc = None
        self.lock = threading.Lock()
        self.log = run_dir / f'node{n}.log'
        self.cfg = run_dir / f'node{n}.toml'
        self.started_ms = None
        self.note = ''

    def write_config(self):
        peers = ', '.join(
            '{{ id = {i}, addr = "{h}:{p}" }}'.format(i=i, h=self.lab.node(i)[0], p=NODE_PORT[i])
            for i in (1, 2, 3) if i != self.n)
        self.cfg.write_text(NODE_TOML.format(n=self.n, port=NODE_PORT[self.n], peers=peers, mode=self.mode))

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, mode=None):
        with self.lock:
            if mode:
                if mode not in MODES:
                    raise ValueError(f'mode must be one of {MODES}')
                self.mode = mode
            self._stop()
            self.write_config()
            exe = BIN / 'cloud-node'
            if not exe.exists():
                raise FileNotFoundError(f'{exe} missing: run cargo build --release')
            with open(self.log, 'a') as f:
                f.write(f'--- live-demo: start node {self.n} mode {self.mode} ---\n')
            self.proc = subprocess.Popen(
                [str(exe), '--config', str(self.cfg), '--log-file', str(self.log)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env={**os.environ, 'RUST_LOG': os.environ.get('RUST_LOG', 'info')},
                start_new_session=True)
            self.started_ms = now_ms()
            self.note = 'started'
        return {'ok': True, 'mode': self.mode}

    def _stop(self):
        if self.running():
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(3)
        self.proc = None

    def kill(self):
        """Crash it: SIGKILL, no clean shutdown."""
        with self.lock:
            if self.running():
                self.proc.kill()
                self.proc.wait(3)
            self.proc = None
            self.note = 'killed'
        return {'ok': True}

    def udp(self, msg, timeout=1.0):
        return call(('127.0.0.1', NODE_PORT[self.n]), msg, timeout)

    def events(self, limit=14):
        try:
            with open(self.log, 'rb') as f:
                f.seek(0, 2)
                f.seek(max(0, f.tell() - 60000))
                lines = f.read().decode('utf-8', 'replace').splitlines()[1:]
        except OSError:
            return []
        out = [ANSI_RE.sub('', l) for l in lines if EVENT_RE.search(l)]
        return out[-limit:]

    def state(self):
        st = {'running': self.running(), 'mode': self.mode, 'pid': self.proc.pid if self.running() else None,
              'started_ms': self.started_ms, 'note': self.note, 'events': self.events()}
        if self.running():
            try:
                st['probe'] = self.udp({'type': 'ProbeRequest'}, .6)
            except Exception as e:  # noqa: BLE001 - surfaced on the dashboard
                st['error'] = str(e)
        return st


class Load:
    """Drives the real client binary in overlapping BATCH_S-second batches."""

    def __init__(self, pc, lab, run_dir):
        self.pc, self.lab, self.run = pc, lab, run_dir
        self.cfg = run_dir / 'client.toml'
        self.rate, self.complexity = 50.0, 5
        self.active = False
        self.lock = threading.Lock()
        self.rows = deque()            # (collected_s, ok, latency_ms, served_by, epoch, attempts, busy)
        self.batches = deque(maxlen=90)
        self.totals = Counter()
        self.error = ''
        self.procs = []
        self.seq = 0
        threading.Thread(target=self.loop, daemon=True).start()

    def start(self, rate=None, complexity=None):
        exe = BIN / 'client'
        if not exe.exists():
            raise FileNotFoundError(f'{exe} missing: run cargo build --release')
        nodes = ', '.join('"{}:{}"'.format(*self.lab.node(n)) for n in (1, 2, 3))
        self.cfg.write_text(CLIENT_TOML.format(nodes=nodes))
        with self.lock:
            if rate is not None:
                self.rate = max(1.0, min(2000.0, float(rate)))
            if complexity is not None:
                self.complexity = max(1, min(50, int(complexity)))
            self.active = True
        return {'ok': True, 'rate': self.rate}

    def stop(self):
        self.active = False
        return {'ok': True}

    def reset(self):
        with self.lock:
            self.rows.clear()
            self.batches.clear()
            self.totals.clear()
            self.error = ''
        return {'ok': True}

    def loop(self):
        next_t = time.monotonic()
        while True:
            t = time.monotonic()
            if self.active and t >= next_t and len(self.procs) < 8:
                self.spawn()
                next_t = t + BATCH_S
            elif not self.active:
                next_t = t
            for item in list(self.procs):
                proc, out = item
                if proc.poll() is not None:
                    self.procs.remove(item)
                    self.collect(proc, out)
            time.sleep(.05)

    def spawn(self):
        self.seq += 1
        out = self.run / f'batch-{self.seq % 16}.csv'
        count = max(1, int(round(self.rate * BATCH_S)))
        proc = subprocess.Popen(
            [str(BIN / 'client'), '--config', str(self.cfg), '--count', str(count),
             '--rate', str(self.rate), '--complexity', str(self.complexity),
             '--seed', str(random.getrandbits(48)), '--reporter', f'pc{self.pc}', '--out', str(out)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env={**os.environ, 'RUST_LOG': 'error'})
        self.procs.append((proc, out))

    def collect(self, proc, out):
        err = proc.stderr.read().decode('utf-8', 'replace').strip() if proc.stderr else ''
        try:
            with open(out, newline='') as f:
                rows = list(csv.DictReader(f))
            out.unlink()
        except OSError:
            self.error = (err.splitlines() or ['client produced no CSV'])[-1][:300]
            return
        t = time.time()
        lat, served = [], Counter()
        ok = failed = retried = busy = 0
        epoch = 0
        with self.lock:
            for r in rows:
                good = r['answered'] == 'true'
                ms = float(r['latency_ms']) if r['latency_ms'] else None
                by = r['served_by'] or '-'
                att = int(r['attempts'] or 0)
                bz = int(r['busy_replies'] or 0)
                ep = int(r['epoch'] or 0)
                self.rows.append((t, good, ms, by, ep, att, bz))
                ok += good
                failed += not good
                retried += att > 1
                busy += bz
                epoch = max(epoch, ep)
                if good and ms is not None:
                    lat.append(ms)
                    served[by] += 1
            self.totals.update(sent=len(rows), ok=ok, failed=failed, retried=retried, busy=busy)
            lat.sort()
            self.batches.append({'t': now_ms(), 'ok': ok, 'failed': failed,
                                 'p50': pct(lat, .5), 'p95': pct(lat, .95), 'served_by': dict(served),
                                 'epoch': epoch})
            self.error = '' if not err or rows else err.splitlines()[-1][:300]

    def state(self):
        with self.lock:
            cut = time.time() - WINDOW_S
            while self.rows and self.rows[0][0] < cut:
                self.rows.popleft()
            lat = sorted(r[2] for r in self.rows if r[1] and r[2] is not None)
            served = Counter(r[3] for r in self.rows if r[1])
            win = {'sent': len(self.rows), 'ok': sum(r[1] for r in self.rows),
                   'failed': sum(not r[1] for r in self.rows), 'retried': sum(r[5] > 1 for r in self.rows),
                   'p50': pct(lat, .5), 'p95': pct(lat, .95), 'p99': pct(lat, .99),
                   'served_by': dict(served), 'epoch': max((r[4] for r in self.rows), default=0),
                   'seconds': WINDOW_S}
            return {'active': self.active, 'rate': self.rate, 'complexity': self.complexity,
                    'in_flight_batches': len(self.procs), 'window': win, 'totals': dict(self.totals),
                    'batches': list(self.batches)[-60:], 'error': self.error}


def pct(sorted_vals, q):
    if not sorted_vals:
        return None
    return round(sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))], 2)


class Agent:
    def __init__(self, pc, lab):
        self.pc, self.lab = pc, lab
        self.run = HERE / 'run' / f'pc{pc}'
        self.run.mkdir(parents=True, exist_ok=True)
        self.node = Node(pc, lab, self.run) if pc <= 3 else None
        self.load = Load(pc, lab, self.run) if pc >= 4 else None
        self.pool = ThreadPoolExecutor(12)

    def autostart(self, mode):
        """Start the node once PCs 1-3 are all known (at once with lab.json)."""
        if not self.lab.nodes_known():
            print('waiting to find PCs 1-3 on the network...', flush=True)
        while not self.lab.nodes_known():
            time.sleep(.5)
        try:
            self.node.start(mode)
            print(f'node {self.pc} started ({mode})', flush=True)
        except Exception as e:  # noqa: BLE001
            print(f'could not start node {self.pc}: {e}', flush=True)
            self.node.note = str(e)

    def state(self):
        st = {'pc': self.pc, 'ip': self.lab.pcs[self.pc - 1], 'role': 'node' if self.node else 'client',
              'time_ms': now_ms(), 'lab': self.lab.view()}
        if self.node:
            st['node_id'] = self.pc
            st['node'] = self.node.state()
        else:
            st['client'] = self.load.state()
        return st

    def cluster(self):
        def one(pc):
            if pc == self.pc:
                return self.state()
            if self.lab.agent(pc) is None:
                return {'pc': pc, 'ip': None, 'role': 'node' if pc <= 3 else 'client',
                        'unreachable': f'not found yet: run oneclick.sh {pc} on PC {pc}'}
            try:
                return http_json(f'http://{self.lab.agent(pc)}/api/state', timeout=1.5)
            except Exception as e:  # noqa: BLE001
                return {'pc': pc, 'ip': self.lab.pcs[pc - 1], 'role': 'node' if pc <= 3 else 'client',
                        'unreachable': str(e)}
        return {'pcs': list(self.pool.map(one, range(1, 7))), 'served_from': self.pc, 'time_ms': now_ms()}

    def act(self, body):
        pc = int(body.get('pc', self.pc))
        if pc != self.pc:
            if self.lab.agent(pc) is None:
                raise LookupError(f'PC {pc} not found yet')
            return http_json(f'http://{self.lab.agent(pc)}/api/act', body, timeout=6)
        a = body.get('action')
        if self.node:
            if a == 'start':
                return self.node.start(body.get('mode'))
            if a == 'kill':
                return self.node.kill()
            if a == 'mute':
                ms = max(1, min(20000, int(body.get('ms', 20000))))
                return self.node.udp({'type': 'InjectFailure', 'duration_ms': ms}, 3)
            if a == 'cpu':
                threads = int(body.get('threads', max(1, (os.cpu_count() or 2) // 2)))
                ms = int(body.get('ms', 25000))
                return self.node.udp({'type': 'SetLoad', 'threads': threads, 'duration_ms': ms}, 3)
            if a == 'digest':
                s = self.node.udp({'type': 'StatusRequest'}, 3)
                return {k: s.get(k) for k in ('node_id', 'epoch', 'server', 'results', 'results_digest', 'phase')}
        else:
            if a == 'start':
                return self.load.start(body.get('rate'), body.get('complexity'))
            if a == 'stop':
                return self.load.stop()
            if a == 'reset':
                return self.load.reset()
        raise ValueError(f'unknown action {a!r} for PC {self.pc}')

    def shutdown(self):
        if self.load:
            self.load.stop()
        if self.node:
            with self.node.lock:
                self.node._stop()


def http_json(url, body=None, timeout=2.0):
    data = None if body is None else json.dumps(body).encode()
    req = urlreq.Request(url, data=data, headers={'Content-Type': 'application/json'})
    with urlreq.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def make_handler(agent):
    page = HERE / 'dashboard.html'

    class Handler(BaseHTTPRequestHandler):
        def send(self, code, body, ctype='application/json'):
            raw = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(code)
            self.send_header('Content-Type', ctype)
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            try:
                if self.path in ('/', '/index.html'):
                    return self.send(200, page.read_bytes(), 'text/html; charset=utf-8')
                if self.path == '/api/state':
                    return self.send(200, agent.state())
                if self.path == '/api/cluster':
                    return self.send(200, agent.cluster())
                self.send(404, {'error': 'not found'})
            except Exception as e:  # noqa: BLE001
                self.send(500, {'error': str(e)})

        def do_POST(self):
            try:
                n = int(self.headers.get('Content-Length') or 0)
                body = json.loads(self.rfile.read(n) or b'{}')
                if self.path == '/api/act':
                    return self.send(200, agent.act(body))
                self.send(404, {'error': 'not found'})
            except Exception as e:  # noqa: BLE001
                self.send(400, {'error': f'{type(e).__name__}: {e}'})

        def log_message(self, fmt, *args):
            pass

    return Handler


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--pc', type=int, required=True, choices=range(1, 7), help='this PC: 1-3 node, 4-6 client')
    ap.add_argument('--lab', default=str(HERE / 'lab.json'), help='six IPs (written by start.sh)')
    ap.add_argument('--discover', action='store_true',
                    help='find the other PCs by LAN broadcast instead of reading --lab (oneclick.sh does this)')
    ap.add_argument('--mode', choices=MODES, default='on_demand', help='election mode for the first start')
    ap.add_argument('--no-autostart', action='store_true', help="don't start the node at launch")
    args = ap.parse_args()

    discover = args.discover or not Path(args.lab).exists()
    lab = Lab(discover=True) if discover else Lab.load(args.lab)
    agent = Agent(args.pc, lab)
    port = lab.agent_ports[args.pc - 1]
    server = ThreadingHTTPServer(('0.0.0.0', port), make_handler(agent))
    server.daemon_threads = True
    if discover:
        Beacon(lab, args.pc, port)
    if agent.node and not args.no_autostart:
        threading.Thread(target=agent.autostart, args=(args.mode,), daemon=True).start()
    role = f'node {args.pc} (UDP {NODE_PORT[args.pc]})' if agent.node else 'client (load generator)'
    print(f'PC {args.pc}: {role}; agent on TCP {port}' + ('; finding the other PCs by broadcast' if discover else ''))
    print(f'Dashboard: http://localhost:{port}')
    print('Ctrl+C stops this PC.', flush=True)

    def bye(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, bye)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        agent.shutdown()
        print('stopped')


if __name__ == '__main__':
    main()
