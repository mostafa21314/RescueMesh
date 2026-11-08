#!/usr/bin/env python3
"""Choose this PC's Phase 1 role on Windows or Linux (Python 3.11+)."""
import argparse
import datetime as dt
import json
import math
import os
import platform
import subprocess
import sys
import tomllib
import uuid
from pathlib import Path
from udp_rr import call

ROOT = Path(__file__).resolve().parent.parent
PRESETS = {
    1: ('1,2', 10, 1),
    2: ('2,4,8', 20, 3),
    3: ('4,8,16', 30, 5),
    4: ('8,16,32', 50, 8),
    5: ('16,32,64', 75, 12),
}


def binary(name):
    suffix = '.exe' if os.name == 'nt' else ''
    directories = [Path(os.environ['RESCUEMESH_BIN_DIR']).resolve()] if os.environ.get('RESCUEMESH_BIN_DIR') else [ROOT / 'target' / 'release', ROOT]
    for directory in directories:
        path = directory / (name + suffix)
        if path.is_file():
            return str(path)
    raise FileNotFoundError(f'{name}{suffix} is missing. Run cargo build --release on this PC.')


def run(command, **kwargs):
    print(subprocess.list2cmdline([str(x) for x in command]), flush=True)
    return subprocess.run(command, check=True, cwd=ROOT, **kwargs)


def endpoint(value):
    host, port = value.rsplit(':', 1)
    if not host or not 1 <= int(port) <= 65535:
        raise ValueError(f'invalid endpoint {value}; use IP:PORT')
    return host.strip('[]'), int(port)


def setup(args):
    from recovery_bench import node_toml, client_toml
    addrs = [endpoint(v) for v in args.nodes]
    if len(set(addrs)) != 3:
        raise ValueError('give three distinct server endpoints')
    args.directory.mkdir(parents=True, exist_ok=True)
    files = {f'node{i + 1}.toml': node_toml(i, {
        'transfer': 'delta', 'snapshot': 'chunked', 'ready_rule': 'majority', 'on_start': True,
    }, addrs, '::' if ':' in addrs[i][0] else '0.0.0.0').replace('cpu_source = "process"', 'cpu_source = "machine"')
        for i in range(3)}
    files['client.toml'] = client_toml(addrs)
    if any((args.directory / name).exists() for name in files):
        raise FileExistsError('configuration exists; choose a new directory or edit the existing files')
    for name, content in files.items():
        (args.directory / name).write_text(content, encoding='utf-8')
    print(f'Created {args.directory}. Copy this directory to every participating PC.')


def control(args):
    config = tomllib.loads((args.config_dir / 'client.toml').read_text(encoding='utf-8'))
    selected = config['nodes'] if args.node is None else [config['nodes'][args.node - 1]]
    errors = []
    for value in selected:
        message = {'type': 'StatusRequest'}
        if args.action == 'normal':
            message = {'type': 'SetLoad', 'threads': 0, 'duration_ms': 0}
        elif args.action == 'imbalance':
            message = {'type': 'SetLoad', 'threads': args.threads, 'duration_ms': args.duration_ms}
        elif args.action == 'fault':
            message = {'type': 'InjectFailure', 'duration_ms': args.duration_ms}
        try:
            reply = call(endpoint(value), message, timeout=5)
            print(value, json.dumps(reply, indent=2))
            expected = {'status': 'Status', 'normal': 'LoadAccepted',
                        'imbalance': 'LoadAccepted', 'fault': 'FailureAccepted'}[args.action]
            if reply.get('type') != expected:
                raise RuntimeError(reply.get('reason', 'unexpected control reply'))
        except (OSError, ValueError, RuntimeError) as error:
            # A muted peer must not prevent a stop/status command from reaching
            # the remaining servers.
            errors.append(f'{value}: {error}')
    if errors:
        raise RuntimeError('\n'.join(errors))


def test(args):
    clients, rate, complexity = PRESETS[args.severity]
    clients = args.clients or clients
    rate = args.rate if args.rate is not None else rate
    complexity = args.complexity if args.complexity is not None else complexity
    if not math.isfinite(rate) or not math.isfinite(args.duration) or rate <= 0 or args.duration <= 0 or args.reps <= 0:
        raise ValueError('rate, duration and repetitions must be positive and finite')
    if args.fault_at_ms < 0 or args.load_threads <= 0 or args.settle_secs <= 0:
        raise ValueError('fault-at-ms must be nonnegative; load-threads and settle-secs must be positive')
    if args.local and args.before_run:
        raise ValueError('--local owns its servers; --before-run is for externally managed runs')
    if not all(int(v) > 0 for v in clients.split(',')) or not 1 <= complexity <= 1000:
        raise ValueError('client counts must be positive; complexity must be 1..1000')
    if args.out.exists() and any(args.out.iterdir()):
        raise FileExistsError('output directory is not empty; choose a new directory')
    scenarios = ['normal', 'imbalance', 'faulty'] if args.scenario == 'all' else [args.scenario]
    if any(s in ('faulty', 'combined') for s in scenarios):
        if not 1 <= args.fault_ms <= 20_000:
            raise ValueError('fault-ms must be 1..20000')
        if args.fault_at_ms + args.fault_ms >= args.duration * 1000:
            raise ValueError('duration must leave workload after fault-at-ms + fault-ms')
    args.out.mkdir(parents=True, exist_ok=True)
    config = args.config_dir / 'client.toml'
    metadata = {**vars(args), 'os': platform.platform(), 'cpu': platform.processor(),
                'logical_cpus': os.cpu_count(), 'transport': 'UDP-RR RMR1', 'protocol_version': 3,
                'clients': clients, 'rate_per_client': rate, 'complexity': complexity,
                'started_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
                'isolated_runs': args.local}
    (args.out / 'experiment.json').write_text(json.dumps(metadata, default=str, indent=2), encoding='utf-8')
    try:
        for scenario in scenarios:
            command = [binary('stress'), '--config', str(config), '--out', str(args.out / scenario),
                       '--clients', clients, '--rate', str(rate), '--complexity', str(complexity),
                       '--requests', str(math.ceil(rate * args.duration)), '--reps', str(args.reps),
                       '--seed', str(args.seed), '--settle-secs', str(args.settle_secs),
                       '--label', f'{scenario}; severity={args.severity}; UDP RR; {platform.platform()}']
            if args.local:
                command += ['--local-nodes', str(args.config_dir)]
            if args.allow_unconverged:
                command += ['--allow-unconverged']
            if args.before_run:
                command += ['--before-run', args.before_run]
            if scenario in ('faulty', 'combined'):
                command += ['--fault-ms', str(args.fault_ms), '--fault-at-ms', str(args.fault_at_ms)]
            if scenario in ('imbalance', 'combined'):
                if args.load_target == 'fixed':
                    command += ['--load-node', str(args.load_node)]
                else:
                    command += ['--load-' + args.load_target]
                command += ['--load-threads', str(args.load_threads)]
            run(command)
    finally:
        # A remote server's load has a timer too, but reset it promptly if a run
        # is interrupted. Local children are owned/cleaned up by the Rust harness.
        if not args.local and any(s in ('imbalance', 'combined') for s in scenarios):
            nodes = tomllib.loads(config.read_text(encoding='utf-8'))['nodes']
            for addr in nodes:
                try:
                    call(endpoint(addr), {'type': 'SetLoad', 'threads': 0, 'duration_ms': 0}, 5)
                except OSError as error:
                    print(f'{addr}: load reset unavailable; its timer will expire: {error}', file=sys.stderr)
        if not args.no_report and list(args.out.rglob('runs.csv')):
            run([sys.executable, str(ROOT / 'scripts' / 'phase1_report.py'), str(args.out)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    p = sub.add_parser('setup', help='generate shared LAN configurations')
    p.add_argument('--nodes', nargs=3, required=True, metavar='IP:PORT')
    p.add_argument('--directory', type=Path, default=ROOT / 'config' / 'site')
    p = sub.add_parser('server', help='run this PC as one cloud peer')
    p.add_argument('--node', type=int, choices=(1, 2, 3), required=True)
    p.add_argument('--workers', type=int, default=0, help='0 selects a suitable count for this PC')
    p.add_argument('--log-file', type=Path, help='default: logs/nodeN.log')
    p = sub.add_parser('client', help='send synthetic incidents; any PC can be a client')
    p.add_argument('--severity', type=int, choices=range(1, 6), default=2)
    p.add_argument('--count', type=int, default=1000)
    p.add_argument('--out', type=Path, default=Path('client_run.csv'))
    p = sub.add_parser('control', help='inspect nodes or toggle load/failure while clients run')
    p.add_argument('action', choices=('status', 'normal', 'imbalance', 'fault'))
    p.add_argument('--node', type=int, choices=(1, 2, 3))
    p.add_argument('--threads', type=int, default=2)
    p.add_argument('--duration-ms', type=int, default=10_000)
    p = sub.add_parser('test', help='run repeated workloads and generate figures')
    p.add_argument('--scenario', choices=('normal', 'imbalance', 'faulty', 'combined', 'all'), default='all')
    p.add_argument('--severity', type=int, choices=range(1, 6), default=2)
    p.add_argument('--local', action='store_true', help='own and restart three local servers for every run')
    p.add_argument('--clients', help='override process counts, e.g. 1,4,16')
    p.add_argument('--rate', type=float, help='override requests/s per client process')
    p.add_argument('--complexity', type=int)
    p.add_argument('--duration', type=float, default=30)
    p.add_argument('--reps', type=int, default=5)
    p.add_argument('--fault-ms', type=int, default=10_000)
    p.add_argument('--fault-at-ms', type=int, default=5000)
    p.add_argument('--load-node', type=int, choices=(1, 2, 3), default=1)
    p.add_argument('--load-target', choices=('fixed', 'incumbent', 'follower'), default='fixed')
    p.add_argument('--load-threads', type=int, default=2)
    p.add_argument('--settle-secs', type=int, default=60)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--no-report', action='store_true')
    p.add_argument('--allow-unconverged', action='store_true', help='explicit soak mode; retain failed baselines and continue after divergent runs')
    p.add_argument('--before-run', help='run this restart/setup command before every repetition')
    stamp = dt.datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]
    p.add_argument('--out', type=Path, default=ROOT / 'results' / 'phase1' / stamp)
    p = sub.add_parser('report', help='generate or regenerate figures from collected CSVs')
    p.add_argument('directory', type=Path)
    for name in ('server', 'client', 'control', 'test'):
        sub.choices[name].add_argument('--config-dir', type=Path, default=ROOT / 'config' / 'local')
    args = parser.parse_args()
    # Resolve paths before run() changes to the repository directory.
    for key, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, key, value.resolve())
    if args.mode == 'setup':
        setup(args)
    elif args.mode == 'server':
        log = args.log_file or ROOT / 'logs' / f'node{args.node}.log'
        run([binary('cloud-node'), '--config', str(args.config_dir / f'node{args.node}.toml'),
             '--workers', str(args.workers), '--log-file', str(log)])
    elif args.mode == 'client':
        _, rate, complexity = PRESETS[args.severity]
        run([binary('client'), '--config', str(args.config_dir / 'client.toml'), '--count', str(args.count),
             '--rate', str(rate), '--complexity', str(complexity), '--out', str(args.out)])
    elif args.mode == 'control':
        if args.action in ('fault', 'imbalance') and args.node is None:
            parser.error('--node is required for fault/imbalance')
        control(args)
    elif args.mode == 'test':
        test(args)
    elif args.mode == 'report':
        run([sys.executable, str(ROOT / 'scripts' / 'phase1_report.py'), str(args.directory)])


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
    except KeyboardInterrupt:
        sys.exit(130)
