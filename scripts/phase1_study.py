#!/usr/bin/env python3
"""Paired on-demand/per-request study on three server PCs and three client PCs.

Commands: plan (offline), check (read-only), run, report.
See docs/six-pc-study.md. Agents: scripts/study_agent.py, port 7400/TCP.
Application requests and server controls use UDP Request-Reply.
"""
import argparse
import concurrent.futures as futures
import csv
import gzip
import io
import json
import math
import os
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from pathlib import Path

from recovery_bench import client_toml, node_toml
from study_agent import source_id, stamp
from study_metrics import ALLOCATION_POLICY, PROTOCOL_VERSION, analyze_requests, node_deltas, resource_verdict, write_rows
from udp_rr import call

ROOT = Path(__file__).resolve().parents[1]
MODES = ['on_demand', 'per_request']
SCENARIOS = ['load_busy', 'load_idle', 'fault5', 'fault20']


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False), encoding='utf-8')


def parallel(function, items):
    with futures.ThreadPoolExecutor(max_workers=max(1, min(12, len(items)))) as pool:
        return list(pool.map(function, items))


def api(address, path, body=None, timeout=10):
    token = os.environ.get('RESCUEMESH_LAB_TOKEN', '')
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    req = urllib.request.Request(f'http://{address}{path}', headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        # Overlay addresses should not be routed through an environment HTTP proxy.
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors='replace')
        raise RuntimeError(f'Agent {address}{path}: HTTP {error.code}: {detail}') from error


def clock(address):
    readings = []
    for _ in range(5):
        t0 = time.time() * 1000
        remote = api(address, '/time')['ms']
        t1 = time.time() * 1000
        readings.append({'offset_ms': remote - (t0 + t1) / 2, 'uncertainty_ms': (t1 - t0) / 2})
    return min(readings, key=lambda x: x['uncertainty_ms'])


def endpoints(lab):
    return [(n['host'], int(n['port'])) for n in lab['nodes']]


def node_query(address, message, timeout=1):
    try:
        return call(address, message, timeout)
    except (OSError, ValueError):
        return None


def statuses(lab):
    return parallel(lambda a: node_query(a, {'type': 'StatusRequest'}, 3), endpoints(lab))


def idle_agreed(states, mode, minimum=0, drained=True):
    if len(states) != 3 or any(not s for s in states):
        return False
    if not all(s['phase'] == 'ready' and not s['degraded'] and s['queue_len'] == 0 and s['running'] == 0 for s in states):
        return False
    if any(not s.get('allocation') or s['allocation']['policy'] != ALLOCATION_POLICY or
           s['allocation'].get('members') != sorted(p['node_id'] for p in states) or
           any(s['allocation'].get(k, 0) for k in ('capacity_violations', 'responder_violations', 'rejected_plans', 'quarantine_remaining_ms')) or
           s.get('resource_overcommits', 0) or s.get('replicate_conflicts', 0) for s in states):
        return False
    if drained and any(q['control_queued'] or q['replication_queued'] for s in states for q in s.get('peer_queues', [])):
        return False
    if len({(s['epoch'], s['server'], s['results'], s['results_digest']) for s in states}) != 1:
        return False
    return states[0]['results'] >= minimum and (mode != 'on_demand' or minimum == 0 or states[0]['server'] is not None)


def wait_baseline(lab, mode, minimum, seconds):
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        last = statuses(lab)
        if any(s and s.get('protocol_version') != PROTOCOL_VERSION for s in last):
            raise RuntimeError('All server/client PCs must be rebuilt for allocation protocol 3')
        if idle_agreed(last, mode, minimum):
            if any(s.get('configuration', {}).get('election_mode') != mode for s in last):
                raise RuntimeError('Server version or election mode mismatch')
            return last
        time.sleep(.25)
    raise RuntimeError(f'Healthy {mode} baseline was not established: {last}')


def preflight(lab, allow_local=False):
    if len(lab['nodes']) != 3 or len(lab['clients']) != 3:
        raise ValueError('The study requires exactly three server agents and three client agents')
    addresses = [n['agent'] for n in lab['nodes']] + lab['clients']
    if len(set(addresses)) != 6 or len(set(endpoints(lab))) != 3:
        raise ValueError('Need six distinct agents and three distinct UDP listeners')
    if not allow_local and len({a.rsplit(':', 1)[0] for a in addresses}) != 6:
        raise ValueError('Need six physical PC addresses; --allow-local is only for rehearsals')
    health = parallel(lambda a: api(a, '/health'), addresses)
    for i, info in enumerate(health):
        if info.get('api_version') != 1 or info['role'] != ('server' if i < 3 else 'client'):
            raise ValueError(f'Agent {addresses[i]} has the wrong version or role')
        if info['source_id'] != source_id():
            raise ValueError(f'Agent {addresses[i]} uses different source; copy/rebuild the same project on every PC')
    offsets = parallel(clock, addresses)
    return {'at_ms': stamp(), 'agents': dict(zip(addresses, health)), 'clocks': dict(zip(addresses, offsets))}


def case_id(case):
    return f"{case['stage']}-{case['scenario']}-p{case['processes']}-r{case['rate']:g}-rep{case['rep']}-{case['mode']}"


def make_cases(args, stage, points=None):
    rng = random.Random(args.seed + {'normal': 0, 'resilience': 1, 'soak': 2}[stage])
    if stage == 'normal':
        conditions = [(p, r, 'normal') for p in args.processes for r in args.rates]
    elif stage == 'resilience':
        conditions = [(p, r, s) for p, r in points for s in SCENARIOS]
    else:
        conditions = [(points[0][0], points[0][1], 'soak')]
    cases = []
    for rep in range(1, (1 if stage == 'soak' else args.reps) + 1):
        shuffled = list(conditions)
        rng.shuffle(shuffled)
        for p, rate, scenario in shuffled:
            modes = list(MODES)
            rng.shuffle(modes)
            # Matching seeds/reporter labels within a pair; UUIDs remain unique.
            seed = rng.randrange(1, 2**52)
            duration = args.soak_seconds if stage == 'soak' else args.duration
            if scenario.startswith('fault'):
                duration = max(duration, args.at_seconds + int(scenario[5:]) + 3)
            for mode in modes:
                case = {'stage': stage, 'scenario': scenario, 'processes': p, 'rate': rate,
                        'duration': duration, 'rep': rep, 'mode': mode, 'seed': seed,
                        'complexity': args.complexity}
                case['id'] = case_id(case)
                cases.append(case)
    return cases


def all_results(out):
    return [json.loads(path.read_text(encoding='utf-8')) for path in sorted((out / 'cases').glob('*/result.json'))]


def select_points(results, args):
    if args.points:
        points = [tuple(map(float, p.split(':'))) for p in args.points.split(',')]
        return [(int(p), r) for p, r in points]
    # Select common sustainable, near-capacity and high offered-load points.
    groups = {}
    for result in results:
        if result['stage'] == 'normal':
            groups.setdefault((result['processes'], result['rate']), []).append(result)
    ordered = sorted(groups, key=lambda x: (3 * x[0] * x[1], x))
    if not ordered:
        raise ValueError('Run the normal stage first, or supply --points processes:rate,...')
    stable = [p for p in ordered if len(groups[p]) == 2 * args.reps and all(
        r.get('measurement_valid') and r.get('resource_correctness_pass') and r['success_pct'] >= 99 and r['converged'] for r in groups[p])]
    low = stable[0] if stable else ordered[0]
    near = stable[-1] if stable else ordered[len(ordered) // 2]
    return list(dict.fromkeys([low, near, ordered[-1]]))


def start_servers(lab, case, args):
    addresses = endpoints(lab)
    recovery = {'transfer': 'delta', 'snapshot': 'chunked', 'ready_rule': 'majority', 'on_start': True}
    def start(item):
        i, n = item
        cfg = node_toml(i, recovery, addresses, listen_host='0.0.0.0')
        cfg = cfg.replace('election_mode = "on_demand"', f'election_mode = "{case["mode"]}"')
        cfg = cfg.replace('cpu_source = "process"', 'cpu_source = "machine"')
        cfg = cfg.replace('workers = 3', f'workers = {args.workers}')
        cfg = cfg.replace('queue_capacity = 4096', f'queue_capacity = {args.queue_capacity}')
        ttl = max(3_600_000, math.ceil((case['duration'] + 2 * args.settle_seconds + 120) * 1000))
        cfg = cfg.replace('table_ttl_ms = 1800000', f'table_ttl_ms = {ttl}')
        api(n['agent'], '/node/start', {'config': cfg, 'log_level': args.node_log_level})
    parallel(start, list(enumerate(lab['nodes'])))


def launch(lab, case, campaign, clocks, at_ms):
    def start(item):
        pc, address = item
        reply = api(address, '/batch/start', {'config': client_toml(endpoints(lab)), 'processes': case['processes'],
            'count': math.ceil(case['rate'] * case['duration']), 'rate': case['rate'], 'complexity': case['complexity'],
            'seed': case['seed'] + pc * 1_000_000, 'pc': pc + 1, 'campaign': campaign,
            'start_at_ms': round(at_ms + clocks[address]['offset_ms'])}, timeout=30)
        return {'pc': pc + 1, 'address': address, 'job': reply['job'], 'processes': reply['processes']}
    return parallel(start, list(enumerate(lab['clients'])))


def batch_status(job):
    return api(job['address'], '/batch/status', {'job': job['job']})


def finish_job(job, stop=False):
    if stop:
        api(job['address'], '/batch/stop', {'job': job['job']})
    api(job['address'], '/batch/release', {'job': job['job']})


def intervention(lab, case, args, before, cancel, start_ms):
    scenario = case['scenario']
    if scenario not in SCENARIOS:
        return {}
    if cancel.wait(max(0, (start_ms + args.at_seconds * 1000 - stamp()) / 1000)):
        return {'error': 'Intervention cancelled'}
    current = statuses(lab)
    candidates = [s for s in current if s and s['phase'] == 'ready']
    if len(candidates) != 3:
        return {'error': 'Not all servers were reachable at intervention time'}
    if case['mode'] == 'on_demand':
        roles = Counter(s['server'] for s in candidates if s['server'] is not None)
        if not roles:
            return {'error': 'No incumbent at intervention time'}
        incumbent = roles.most_common(1)[0][0]
        target = min(s['node_id'] for s in candidates if s['node_id'] != incumbent) if scenario == 'load_idle' else incumbent
        reason = 'follower' if scenario == 'load_idle' else 'incumbent'
    else:
        scores = [(s['workers']['started'] - before[s['node_id'] - 1]['workers']['started'], s['node_id']) for s in candidates]
        target = min(scores)[1] if scenario == 'load_idle' else max(scores, key=lambda x: (x[0], -x[1]))[1]
        reason = 'least_recent_worker_activity' if scenario == 'load_idle' else 'most_recent_worker_activity'
    event = {'target': target, 'target_reason': reason, 'selection_statuses': current}
    address = endpoints(lab)[target - 1]
    if scenario.startswith('fault'):
        ms = int(scenario[5:]) * 1000
        sent = stamp()
        reply = node_query(address, {'type': 'InjectFailure', 'duration_ms': ms}, 3)
        if not reply or reply.get('type') != 'FailureAccepted':
            return {**event, 'error': f'Failure not acknowledged: {reply}'}
        event.update(kind='fault', at_ms=(sent + stamp()) / 2, duration_ms=ms, ready_ms=None)
        if cancel.wait(ms / 1000):
            return event
        deadline = time.monotonic() + args.settle_seconds
        while time.monotonic() < deadline and not cancel.is_set():
            probe = node_query(address, {'type': 'ProbeRequest'}, .5)
            if probe and probe['phase'] == 'ready':
                event['ready_ms'] = stamp()
                break
            cancel.wait(.25)
    else:
        cores = args.health['agents'][lab['nodes'][target - 1]['agent']]['logical_cpus'] or 1
        threads = max(1, math.ceil(cores * args.load_fraction))
        seconds = max(.2, min(args.load_seconds, case['duration'] - args.at_seconds - 1))
        reply = node_query(address, {'type': 'SetLoad', 'threads': threads, 'duration_ms': math.ceil(seconds * 1000)}, 3)
        if not reply or reply.get('type') != 'LoadAccepted':
            return {**event, 'error': f'Load not acknowledged: {reply}'}
        event.update(kind='load', at_ms=stamp(), threads=threads)
        cancel.wait(seconds)
        reply = node_query(address, {'type': 'SetLoad', 'threads': 0, 'duration_ms': 0}, 3)
        event['end_ms'] = stamp()
        if not reply or reply.get('type') != 'LoadAccepted':
            event['error'] = 'Load reset not acknowledged; its finite timer remains the fallback'
    return event


def provisional_convergence(lab, mode, seconds, cancel):
    t0 = time.monotonic()
    while time.monotonic() - t0 < seconds and not cancel.is_set():
        states = statuses(lab)
        if idle_agreed(states, mode):
            return {'at_ms': stamp(), 'statuses': states}
        time.sleep(.25)
    return None


def scope_converged(lab, ids, answered, mode, seconds):
    t0 = time.monotonic()
    last, digests = [], []
    while True:
        last = statuses(lab)
        digests = parallel(lambda a: node_query(a, {'type': 'ResultsDigestRequest', 'client_ids': ids,
                                  'min_retained_until_ms': 0}, 3), endpoints(lab))
        healthy = len(last) == 3 and all(s and s['phase'] == 'ready' and not s['degraded'] and
                    s['queue_len'] == 0 and s['running'] == 0 for s in last)
        roles = {(s['epoch'], s['server']) for s in last if s}
        equal = all(d for d in digests) and len({(d['count'], d['digest']) for d in digests if d}) == 1
        if healthy and len(roles) == 1 and equal and digests[0]['count'] >= answered:
            return True, last, digests
        if time.monotonic() - t0 >= seconds:
            return False, last, digests
        time.sleep(.25)


def run_case(lab, case, args, manifest):
    parent = args.out / 'cases' / case['id']
    parent.mkdir(parents=True, exist_ok=True)
    directory = parent / f'attempt-{len(list(parent.glob("attempt-*"))) + 1}'
    directory.mkdir()
    save(directory / 'case.json', case)
    jobs, samples, hosts, cancel = [], [], [], threading.Event()
    pool = futures.ThreadPoolExecutor(max_workers=2)
    try:
        # Clean up only this campaign's orphaned client jobs on a resumed run.
        parallel(lambda a: api(a, '/campaign/stop', {'campaign': manifest['campaign']}), lab['clients'])
        start_servers(lab, case, args)
        entry = wait_baseline(lab, case['mode'], 0, args.settle_seconds)
        if any(s['results'] or s['requests'] for s in entry):
            raise RuntimeError('Freshly started nodes are not empty')
        save(directory / 'entry.json', entry)
        clocks = dict(zip(lab['clients'], parallel(clock, lab['clients'])))
        # Warm up gently even when per-request election links have substantial
        # RTT. Warm-up must not itself become an overload test.
        warm = {**case, 'processes': 1, 'rate': 1, 'duration': 5, 'complexity': 1,
                'seed': case['seed'] + 100_000_000_000}
        warm_jobs = launch(lab, warm, manifest['campaign'], clocks, stamp() + 1500)
        jobs.extend(warm_jobs)
        deadline = time.monotonic() + args.settle_seconds
        while True:
            states = parallel(batch_status, warm_jobs)
            if all(s['done'] for s in states):
                if any(c != 0 for s in states for c in s['exit_codes']):
                    raise RuntimeError('Warm-up client failed')
                break
            if time.monotonic() > deadline:
                raise RuntimeError('Warm-up timed out')
            time.sleep(.1)
        for job in warm_jobs:
            text = api(job['address'], '/batch/file', {'job': job['job'], 'process': 0, 'kind': 'csv'})['text']
            rows = list(csv.DictReader(io.StringIO(text)))
            if len(rows) != 5 or any(r['answered'] != 'true' for r in rows):
                (directory / f'warmup-pc{job["pc"]}.csv').write_text(text, encoding='utf-8')
                raise RuntimeError('Warm-up requests did not all receive replies; baseline is unhealthy')
        before = wait_baseline(lab, case['mode'], 15, args.settle_seconds)
        for job in warm_jobs:
            finish_job(job)
            jobs.remove(job)
        save(directory / 'before.json', before)
        # Measure clocks for all six PCs at both ends of the measured run.
        addresses = [n['agent'] for n in lab['nodes']] + lab['clients']
        clock_before = dict(zip(addresses, parallel(clock, addresses)))
        start_ms = stamp() + math.ceil(args.lead_seconds * 1000)
        jobs = launch(lab, case, manifest['campaign'], clock_before, start_ms)
        save(directory / 'launch.json', {'start_ms': start_ms, 'jobs': jobs, 'clocks': clock_before})
        event_future = pool.submit(intervention, lab, case, args, before, cancel, start_ms)
        deadline = time.monotonic() + args.lead_seconds + case['duration'] + args.settle_seconds + 20
        next_metrics, done = 0, False
        sample_path, host_path = directory / 'samples.jsonl', directory / 'hosts.jsonl'
        with sample_path.open('w', encoding='utf-8') as probe_log, host_path.open('w', encoding='utf-8') as host_log:
            while not done:
                tick = time.monotonic()
                probes = parallel(lambda a: node_query(a, {'type': 'ProbeRequest'}, .5), endpoints(lab))
                for i, probe in enumerate(probes):
                    row = {'t_ms': stamp() - start_ms, 'node': i + 1, 'reachable': probe is not None,
                           **(probe or {})}
                    samples.append(row)
                    probe_log.write(json.dumps(row) + '\n')
                if tick >= next_metrics:
                    def metric(address):
                        try:
                            return api(address, '/metrics', timeout=2)
                        except (OSError, RuntimeError):
                            return None
                    for address, value in zip(addresses, parallel(metric, addresses)):
                        row = {'t_ms': stamp() - start_ms, 'agent': address, 'metrics': value}
                        hosts.append(row)
                        host_log.write(json.dumps(row) + '\n')
                    states = parallel(batch_status, jobs)
                    done = all(s['done'] for s in states)
                    next_metrics = tick + 1
                if time.monotonic() > deadline:
                    raise RuntimeError('Client batch exceeded its workload/retry budget')
                if not done:
                    time.sleep(max(0, args.sample_ms / 1000 - (time.monotonic() - tick)))
        clients_done_ms = stamp()
        event = event_future.result(timeout=args.settle_seconds + 25)
        save(directory / 'intervention.json', event)
        provisional = pool.submit(provisional_convergence, lab, case['mode'], args.settle_seconds, cancel)
        parts = []
        for job in jobs:
            for process in range(job['processes']):
                for kind in ('csv', 'log'):
                    text = api(job['address'], '/batch/file', {'job': job['job'], 'process': process, 'kind': kind}, timeout=60)['text']
                    path = directory / f'pc{job["pc"]}-process{process}.{kind}.gz'
                    with gzip.open(path, 'wt', encoding='utf-8', newline='') as stream:
                        stream.write(text)
                    if kind == 'csv':
                        parts.append((job['pc'], process, path, clock_before[job['address']]['offset_ms']))
        metrics, ids = analyze_requests(directory, case, parts, start_ms, event)
        if metrics['uuid_valid']:
            success, after, digests = scope_converged(lab, ids, metrics['answered'], case['mode'], args.settle_seconds)
        else:
            success, after, digests = False, statuses(lab), []
        collected_at = stamp()
        early = provisional.result(timeout=args.settle_seconds + 10)
        observed_convergence = collected_at
        if success and early and all(a and e and (a['results'], a['results_digest']) == (e['results'], e['results_digest'])
                for a, e in zip(after, early['statuses'])):
            observed_convergence = early['at_ms']
        save(directory / 'after.json', {'statuses': after, 'cohort_digests': digests, 'client_ids': ids})
        clock_after = dict(zip(addresses, parallel(clock, addresses)))
        save(directory / 'clocks.json', {'before': clock_before, 'after': clock_after})
        node_logs = parallel(lambda n: api(n['agent'], '/node/log', {}, timeout=60)['text'], lab['nodes'])
        for i, text in enumerate(node_logs):
            with gzip.open(directory / f'node{i+1}.log.gz', 'wt', encoding='utf-8') as stream:
                stream.write(text)
        codes = [c for s in states for c in s['exit_codes']]
        intervention_valid = case['scenario'] not in SCENARIOS or (bool(event) and not event.get('error') and
                             (event.get('kind') != 'fault' or event.get('ready_ms') is not None))
        incarnation_valid = all(a and b['incarnation'] == a['incarnation'] for b, a in zip(before, after))
        clock_shift = max(abs(clock_after[a]['offset_ms'] - clock_before[a]['offset_ms']) for a in addresses)
        clock_uncertainty = max(v['uncertainty_ms'] for v in clock_before.values())
        row = {**case, **metrics, **node_deltas(before, after, [s for s in samples if s['t_ms'] >= 0]), **resource_verdict(before, after),
               'artifacts': str(directory.relative_to(args.out)).replace('\\', '/'), 'baseline_agreed': True,
               'converged': success, 'convergence_ms': max(0, observed_convergence - clients_done_ms) if success else None,
               'client_exit_failures': sum(c != 0 for c in codes), 'intervention_valid': intervention_valid,
               'incarnation_valid': incarnation_valid, 'clock_offset_change_max_ms': clock_shift,
               'clock_uncertainty_max_ms': clock_uncertainty, 'time_alignment_valid': clock_shift <= 50 and clock_uncertainty <= 50,
               'target': event.get('target'), 'target_reason': event.get('target_reason'),
               'recovery_ms': max(0, event['ready_ms'] - event['at_ms'] - event['duration_ms'])
                    if event.get('kind') == 'fault' and event.get('ready_ms') else None}
        row['measurement_valid'] = (row['missing_records'] == 0 and row['uuid_valid'] and row['load_generation_valid']
            and row['client_exit_failures'] == 0 and intervention_valid and incarnation_valid and row['allocation_fields_present'])
        row['correctness_pass'] = row['converged'] and row['resource_correctness_pass']
        row['electbid_messages_per_request'] = row['electbid_messages'] / row['requests']
        save(parent / 'result.json', row)
        return row
    except BaseException as error:
        save(directory / 'error.json', {'error': str(error), 'type': type(error).__name__})
        for i, n in enumerate(lab['nodes']):
            try:
                text = api(n['agent'], '/node/log', {}, timeout=5)['text']
                (directory / f'node{i+1}-error.log').write_text(text, encoding='utf-8')
            except (OSError, RuntimeError):
                pass
        raise
    finally:
        cancel.set()
        pool.shutdown(wait=True, cancel_futures=True)
        for job in jobs:
            try:
                finish_job(job, stop=True)
            except (OSError, RuntimeError):
                pass
        for address in lab['clients']:
            try:
                api(address, '/campaign/stop', {'campaign': manifest['campaign']})
            except (OSError, RuntimeError):
                pass
        for n in lab['nodes']:
            try:
                api(n['agent'], '/node/stop', {})
            except (OSError, RuntimeError):
                pass


def execute(args, lab):
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / 'experiment.json'
    settings = {k: getattr(args, k) for k in ['profile', 'rates', 'processes', 'reps', 'duration', 'complexity', 'workers',
                'queue_capacity', 'seed', 'at_seconds', 'load_seconds', 'load_fraction', 'soak_seconds', 'node_log_level',
                'settle_seconds', 'sample_ms', 'lead_seconds', 'points']}
    if path.exists():
        if not args.resume:
            raise ValueError('Output exists; use a new directory or --resume')
        manifest = json.loads(path.read_text(encoding='utf-8'))
        if manifest['settings'] != settings or manifest['lab'] != lab or manifest['source_id'] != source_id():
            raise ValueError('Resume requires identical settings, lab and source')
    else:
        if any(args.out.iterdir()):
            raise ValueError('Output directory is not empty')
        manifest = {'campaign': uuid.uuid4().hex, 'source_id': source_id(), 'settings': settings,
                    'lab': lab, 'created_ms': stamp(), 'protocol_version': PROTOCOL_VERSION, 'allocation_policy': ALLOCATION_POLICY}
        save(path, manifest)
    args.health = preflight(lab, args.allow_local)
    save(args.out / 'preflight.json', args.health)
    stages = ['normal', 'resilience', 'soak'] if args.stage == 'all' else [args.stage]
    for stage in stages:
        results = all_results(args.out)
        if stage == 'soak':
            points = select_points(results, args)
            normal = [r for r in results if r['stage'] == 'normal']
            stable = [p for p in points if len([r for r in normal if (r['processes'], r['rate']) == p]) == 2 * args.reps and
                all(r['measurement_valid'] and r.get('resource_correctness_pass') and r['success_pct'] >= 99 and r['converged'] for r in normal
                    if (r['processes'], r['rate']) == p)]
            if not stable:
                raise RuntimeError('No verified common sustainable point for a soak; inspect the normal-stage results')
            points = [stable[0]]
        elif stage == 'resilience':
            points = select_points(results, args)
        else:
            points = None
        plan_file = args.out / f'plan-{stage}.json'
        cases = make_cases(args, stage, points)
        if plan_file.exists() and json.loads(plan_file.read_text(encoding='utf-8')) != cases:
            raise ValueError('Selected points changed; preserve this study and use a new output directory')
        save(plan_file, cases)
        done = {r['id'] for r in results}
        print(f'{stage}: {len(cases)} runs, {sum(c["id"] in done for c in cases)} already completed', flush=True)
        for case in cases:
            if case['id'] in done:
                continue
            print(f'Starting {case["id"]}: {3 * case["processes"]} processes, {3 * case["processes"] * case["rate"]:g} offered req/s', flush=True)
            row = run_case(lab, case, args, manifest)
            write_rows(args.out / 'runs.csv', all_results(args.out))
            print(f'  answered {row["answered"]}/{row["requests"]}; p50/p95/p99={row["lat_p50_ms"]}/{row["lat_p95_ms"]}/{row["lat_p99_ms"]} ms; '
                  f'SD={row["lat_sd_ms"]}; valid={row["measurement_valid"]}; converged={row["converged"]}; '
                  f'resources={row["resource_correctness_pass"] if row["resource_correctness_known"] else "unknown"}; '
                  f'shortfall_pct={row["shortfall_pct"]}', flush=True)
            if row['resource_correctness_known'] and not row['resource_correctness_pass']:
                if not args.no_report:
                    from phase1_study_report import generate
                    generate(args.out)
                raise RuntimeError(f'Resource correctness failed; saved {row["id"]}. Stop and investigate: {row["resource_correctness_reasons"]}')
            # A nonconvergent measurement is retained; the next run must still
            # pass its fresh empty baseline. Infrastructure errors stop the study.
    if not args.no_report:
        from phase1_study_report import generate
        generate(args.out)


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['plan', 'check', 'run', 'report'])
    parser.add_argument('--lab', type=Path, default=ROOT / 'config/study-lab.json')
    parser.add_argument('--out', type=Path, default=ROOT / 'results/phase1/six-pc-study-1')
    parser.add_argument('--stage', choices=['all', 'normal', 'resilience', 'soak'], default='all')
    parser.add_argument('--profile', choices=['standard', 'smoke'], default='standard')
    parser.add_argument('--rates', default='30,50,75,100,150,200')
    parser.add_argument('--processes', default='1,2,4,8,16', help='Client processes PER client PC')
    parser.add_argument('--reps', type=int, default=5)
    parser.add_argument('--duration', type=float, default=60)
    parser.add_argument('--complexity', type=int, default=5)
    parser.add_argument('--workers', type=int, default=3)
    parser.add_argument('--queue-capacity', type=int, default=64)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--at-seconds', type=float, default=15)
    parser.add_argument('--load-seconds', type=float, default=25)
    parser.add_argument('--load-fraction', type=float, default=.5)
    parser.add_argument('--soak-seconds', type=float, default=1800)
    parser.add_argument('--settle-seconds', type=float, default=60)
    parser.add_argument('--lead-seconds', type=float, default=5)
    parser.add_argument('--sample-ms', type=int, default=500)
    parser.add_argument('--node-log-level', default='warn', choices=['warn', 'info', 'debug'])
    parser.add_argument('--points', help='Explicit resilience points, e.g. 1:30,4:75,16:200')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--no-report', action='store_true')
    parser.add_argument('--allow-local', action='store_true', help='Six loopback agents for isolated rehearsals')
    parser.add_argument('--insecure', action='store_true', help='Tokenless isolated local rehearsal')
    args = parser.parse_args(argv)
    if args.profile == 'smoke':
        args.rates, args.processes, args.reps, args.duration = '30', '1', 1, 5
        args.complexity, args.at_seconds, args.load_seconds, args.soak_seconds = 1, 1, 2, 10
    args.rates = [float(x) for x in args.rates.split(',')]
    args.processes = [int(x) for x in args.processes.split(',')]
    if (not args.rates or not args.processes or len(set(args.rates)) != len(args.rates) or
        len(set(args.processes)) != len(args.processes) or any(not math.isfinite(r) or not 0 < r <= 10_000 for r in args.rates) or
        any(not 1 <= p <= 64 for p in args.processes) or args.reps < 1 or not 1 <= args.complexity <= 1000 or
        args.workers < 1 or args.queue_capacity < 1 or args.sample_ms < 50 or not 1 <= args.lead_seconds <= 30 or
        not all(math.isfinite(v) and v > 0 for v in [args.duration, args.soak_seconds, args.settle_seconds,
                                                   args.at_seconds, args.load_seconds, args.load_fraction]) or
        args.load_fraction > 2 or args.at_seconds >= args.duration):
        parser.error('Invalid workload, timing, worker or load settings')
    return args


def main():
    args = arguments()
    if args.command == 'report':
        from phase1_study_report import generate
        generate(args.out)
        return
    lab = json.loads(args.lab.read_text(encoding='utf-8'))
    if args.command == 'plan':
        cases = make_cases(args, 'normal')
        print(json.dumps({'normal_runs': len(cases), 'normal_requests': sum(3 * c['processes'] * math.ceil(c['rate'] * c['duration']) for c in cases),
            'resilience_runs_up_to': min(3, len(args.rates) * len(args.processes)) * 4 * args.reps * 2, 'soak_runs': 2,
            'normal_workload_hours': sum(c['duration'] for c in cases) / 3600,
            'client_processes_per_pc': args.processes, 'total_processes': [3*p for p in args.processes],
            'rates_per_process': args.rates, 'lab': lab}, indent=2))
        return
    if not os.environ.get('RESCUEMESH_LAB_TOKEN') and not args.insecure:
        raise ValueError('Set RESCUEMESH_LAB_TOKEN to the same secret as the six agents')
    if args.command == 'check':
        info = preflight(lab, args.allow_local)
        for address, health in info['agents'].items():
            print(f"{address}: {health['role']}, {health['logical_cpus']} CPUs, {health['os']}, clock uncertainty +/-{info['clocks'][address]['uncertainty_ms']:.1f} ms")
    else:
        execute(args, lab)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, futures.TimeoutError) as error:
        sys.exit(str(error))
    except KeyboardInterrupt:
        sys.exit(130)
