#!/usr/bin/env python3
"""Six-PC study process agent. Application traffic remains UDP RR.

Set RESCUEMESH_LAB_TOKEN to the same secret on the controller and all agents.
Run with --role server or --role client and --bind THIS_PC_ZEROTIER_IP.
Only processes started by this agent can be stopped through its API.
"""
import argparse
import hashlib
import hmac
import json
import math
import os
import platform
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXE = '.exe' if os.name == 'nt' else ''
API_VERSION = 1


def stamp():
    return int(time.time() * 1000)


def source_id():
    digest = hashlib.sha256()
    paths = [ROOT / 'Cargo.toml', ROOT / 'Cargo.lock']
    paths += sorted((ROOT / 'crates').rglob('*.rs'))
    paths += sorted((ROOT / 'crates').rglob('Cargo.toml'))
    paths += [ROOT / 'scripts' / n for n in ('study_agent.py', 'phase1_study.py', 'study_metrics.py')]
    for path in paths:
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(path.read_bytes().replace(b'\r\n', b'\n'))
    return digest.hexdigest()


def host_metrics():
    """Cumulative counters: Linux /proc or Windows system CPU/memory APIs."""
    result = {'at_ms': stamp(), 'cpu_total': None, 'cpu_idle': None, 'memory_available_bytes': None,
              'network_rx_bytes': None, 'network_tx_bytes': None}
    if sys.platform.startswith('linux'):
        cpu = [int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
        result.update(cpu_total=sum(cpu[:8]), cpu_idle=cpu[3] + cpu[4])
        mem = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        result['memory_available_bytes'] = int(mem['MemAvailable'].split()[0]) * 1024
        interfaces = [line.split(':', 1) for line in Path('/proc/net/dev').read_text().splitlines()[2:]]
        fields = [v.split() for name, v in interfaces if name.strip() != 'lo']
        result['network_rx_bytes'] = sum(int(v[0]) for v in fields)
        result['network_tx_bytes'] = sum(int(v[8]) for v in fields)
    elif os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        idle, kernel, user = (wintypes.FILETIME() for _ in range(3))
        if ctypes.windll.kernel32.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)):
            value = lambda t: (t.dwHighDateTime << 32) | t.dwLowDateTime
            result.update(cpu_idle=value(idle), cpu_total=value(kernel) + value(user))
    return result


class Agent:
    def __init__(self, role, directory, binary_directory, token):
        self.role, self.directory, self.bin, self.token = role, Path(directory), Path(binary_directory), token
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.node = None
        self.node_log = None
        self.jobs = {}
        self.fingerprint = source_id()
        self.cpu_model = platform.processor()
        if sys.platform.startswith('linux'):
            self.cpu_model = next((line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
                                   if line.startswith('model name')), self.cpu_model)
        binary = self.bin / ('cloud-node' if role == 'server' else 'client')
        binary = binary.with_name(binary.name + EXE)
        if not binary.is_file():
            raise ValueError(f'{binary} missing; run cargo build --release')
        if role == 'client':
            help_text = subprocess.check_output([str(binary), '--help'], text=True, encoding='utf-8')
            if '--start-at-ms' not in help_text:
                raise ValueError('Client binary is outdated; rebuild before starting the agent')
        self.binary_metadata = {'path': str(binary), 'bytes': binary.stat().st_size,
                                'sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}

    def health(self):
        return {'api_version': API_VERSION, 'role': self.role, 'ms': stamp(), 'source_id': self.fingerprint,
                'os': platform.platform(), 'cpu_model': self.cpu_model, 'logical_cpus': os.cpu_count(), 'binary': self.binary_metadata,
                'node_pid': self.node.pid if self.node and self.node.poll() is None else None,
                'metrics': host_metrics()}

    @staticmethod
    def spawn(command, log, **kwargs):
        return subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT,
                                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0), **kwargs)

    def stop_node(self):
        with self.lock:
            if self.node and self.node.poll() is None:
                self.node.kill()
                self.node.wait(timeout=10)
            self.node = None
            if self.node_log:
                self.node_log.close()
                self.node_log = None

    def start_node(self, req):
        if self.role != 'server':
            raise ValueError('This agent is configured as a client PC')
        cfg = tomllib.loads(req['config'])
        port = int(cfg['listen'].rsplit(':', 1)[1])
        if not 1 <= port <= 65535:
            raise ValueError('Invalid UDP listener')
        with self.lock:
            self.stop_node()
            # Never attach to or terminate somebody else's listener.
            family = socket.AF_INET6 if cfg['listen'].startswith('[') else socket.AF_INET
            with socket.socket(family, socket.SOCK_DGRAM) as reservation:
                if os.name == 'nt':
                    reservation.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                reservation.bind(('::' if family == socket.AF_INET6 else '0.0.0.0', port))
            self.node_path = self.directory / f'node-{uuid.uuid4().hex}.log'
            path = self.node_path.with_suffix('.toml')
            path.write_text(req['config'], encoding='utf-8')
            self.node_log = self.node_path.open('wb')
            self.node = self.spawn([str(self.bin / f'cloud-node{EXE}'), '--config', str(path)], self.node_log,
                                   env={**os.environ, 'RUST_LOG': req.get('log_level', 'warn')})
            return {'pid': self.node.pid}

    def start_batch(self, req):
        if self.role != 'client':
            raise ValueError('This agent is configured as a server PC')
        processes, count, rate = int(req['processes']), int(req['count']), float(req['rate'])
        complexity = int(req['complexity'])
        if not (1 <= processes <= 64 and 1 <= count <= 1_000_000 and math.isfinite(rate)
                and 0 < rate <= 10_000 and 1 <= complexity <= 1000):
            raise ValueError('Invalid processes/count/rate/complexity')
        start = int(req['start_at_ms'])
        if not stamp() - 1000 <= start <= stamp() + 60_000:
            raise ValueError('Start must be within the next 60 seconds')
        tomllib.loads(req['config'])
        with self.lock:
            if any(any(p['proc'].poll() is None for p in j['parts']) for j in self.jobs.values()):
                raise ValueError('This client PC already has an active batch')
            job = uuid.uuid4().hex
            directory = self.directory / job
            directory.mkdir()
            cfg = directory / 'client.toml'
            cfg.write_text(req['config'], encoding='utf-8')
            parts = []
            self.jobs[job] = {'parts': parts, 'start_at_ms': start, 'campaign': req.get('campaign')}
            try:
                for i in range(processes):
                    out, log = directory / f'{i}.csv', directory / f'{i}.log'
                    with log.open('wb') as stream:
                        proc = self.spawn([str(self.bin / f'client{EXE}'), '--config', str(cfg), '--count', str(count),
                            '--rate', str(rate), '--complexity', str(complexity), '--seed', str(int(req['seed']) + i),
                            '--reporter', f"study-pc{req.get('pc', 1)}-process{i}", '--start-at-ms', str(start),
                            '--out', str(out)], stream,
                            env={**os.environ, 'RUST_LOG': 'warn', 'TOKIO_WORKER_THREADS': '2'})
                    parts.append({'proc': proc, 'csv': out, 'log': log})
            except BaseException:
                self.stop_batch(job)
                raise
            return {'job': job, 'processes': len(parts)}

    def stop_batch(self, job):
        with self.lock:
            for part in self.jobs[job]['parts']:
                if part['proc'].poll() is None:
                    part['proc'].kill()
                part['proc'].wait(timeout=10)

    def dispatch(self, path, req):
        if path == '/campaign/stop':
            with self.lock:
                for job in list(self.jobs):
                    if self.jobs[job]['campaign'] == req['campaign']:
                        self.stop_batch(job)
                        del self.jobs[job]
            return {'ok': True}
        if path == '/node/start':
            return self.start_node(req)
        if path == '/node/stop':
            self.stop_node()
            return {'ok': True}
        if path == '/node/log':
            return {'text': self.node_path.read_text(encoding='utf-8', errors='replace') if hasattr(self, 'node_path') else ''}
        if path == '/batch/start':
            return self.start_batch(req)
        with self.lock:
            job = self.jobs[req['job']]
            if path == '/batch/status':
                codes = [p['proc'].poll() for p in job['parts']]
                return {'done': all(c is not None for c in codes), 'exit_codes': codes}
            if path == '/batch/stop':
                self.stop_batch(req['job'])
                return {'ok': True}
            if path == '/batch/file':
                if req['kind'] not in ('csv', 'log'):
                    raise ValueError('Unknown file kind')
                part = job['parts'][int(req['process'])]
                path = part[req['kind']]
                return {'text': path.read_text(encoding='utf-8', errors='replace') if path.exists() else ''}
            if path == '/batch/release':
                if any(p['proc'].poll() is None for p in job['parts']):
                    raise ValueError('Stop or finish the batch before releasing it')
                del self.jobs[req['job']]
                return {'ok': True}
        raise ValueError('Unknown API path')

    def close(self):
        for job in list(self.jobs):
            self.stop_batch(job)
        self.stop_node()


class Server(ThreadingHTTPServer):
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    def reply(self, body, code=200):
        data = json.dumps(body, allow_nan=False).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def authorized(self):
        token = self.server.agent.token
        if token and not hmac.compare_digest(self.headers.get('Authorization', ''), f'Bearer {token}'):
            self.reply({'error': 'Invalid study token'}, 401)
            return False
        return True

    def do_GET(self):
        if not self.authorized():
            return
        if self.path == '/time':
            self.reply({'ms': stamp()})
        elif self.path == '/health':
            self.reply(self.server.agent.health())
        elif self.path == '/metrics':
            self.reply(host_metrics())
        else:
            self.reply({'error': 'Unknown API path'}, 404)

    def do_POST(self):
        if not self.authorized():
            return
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 2 * 1024 * 1024:
                raise ValueError('Invalid request body size')
            req = json.loads(self.rfile.read(size))
            self.reply(self.server.agent.dispatch(self.path, req))
        except (ValueError, KeyError, IndexError, OSError, subprocess.SubprocessError) as error:
            self.reply({'error': str(error)}, 400)

    def log_message(self, *_):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role', choices=['server', 'client'], required=True)
    parser.add_argument('--bind', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=7400)
    parser.add_argument('--work-dir', type=Path)
    parser.add_argument('--bin-dir', type=Path, default=Path(os.environ.get('RESCUEMESH_BIN_DIR', ROOT / 'target/release')))
    parser.add_argument('--insecure', action='store_true', help='Allow tokenless isolated local rehearsals')
    args = parser.parse_args()
    token = os.environ.get('RESCUEMESH_LAB_TOKEN', '')
    if not token and not args.insecure:
        parser.error('Set RESCUEMESH_LAB_TOKEN to the shared study secret')
    agent = Agent(args.role, args.work_dir or Path(tempfile.gettempdir()) / f'rescuemesh-study-{args.port}',
                  args.bin_dir.resolve(), token)
    server = Server((args.bind, args.port), Handler)
    server.agent = agent
    print(f'Study agent ({args.role}) listening on {args.bind}:{server.server_port}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        agent.close()


if __name__ == '__main__':
    main()
