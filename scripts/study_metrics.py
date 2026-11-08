"""Statistics for the six-PC study; successful latency and failed elapsed stay separate."""
import csv
import gzip
import math
import statistics
import uuid
from collections import Counter, defaultdict

ALLOCATION_POLICY = 'exclusive_asset_partitions_v1'
PROTOCOL_VERSION = 3


def resource_verdict(before, after):
    reasons = []
    known = len(before) == len(after) == 3 and all(s and s.get('allocation') for s in before + after)
    if not known:
        return {'resource_correctness_known': False, 'resource_correctness_pass': False,
                'resource_correctness_reasons': ['Missing allocation diagnostics; safety was not assessed']}
    for b, a in zip(before, after):
        for s in (b, a):
            allocation = s['allocation']
            if s.get('protocol_version') != PROTOCOL_VERSION or allocation['policy'] != ALLOCATION_POLICY:
                reasons.append(f'node {s["node_id"]}: incompatible allocation protocol/policy')
            if allocation.get('members') != [1, 2, 3]:
                reasons.append(f'node {s["node_id"]}: inconsistent allocation membership')
            for key in ('rejected_plans', 'capacity_violations', 'responder_violations'):
                if allocation.get(key, 0):
                    reasons.append(f'node {s["node_id"]}: {key}={allocation[key]}')
        for key in ('resource_overcommits', 'replicate_conflicts'):
            if a.get(key, 0) > b.get(key, 0):
                reasons.append(f'node {a["node_id"]}: {key} increased')
    return {'resource_correctness_known': True, 'resource_correctness_pass': not reasons,
            'resource_correctness_reasons': sorted(set(reasons))}


def quantile_sorted(values, p):
    return values[int((len(values) - 1) * p + .5)] if values else None


def distribution(values):
    values = sorted(v for v in values if math.isfinite(v))
    return {'n': len(values), 'mean_ms': statistics.mean(values) if values else None,
            'sd_ms': statistics.stdev(values) if len(values) >= 2 else None,
            'p50_ms': quantile_sorted(values, .5), 'p95_ms': quantile_sorted(values, .95),
            'p99_ms': quantile_sorted(values, .99), 'max_ms': values[-1] if values else None}


def ci95(values):
    values = [v for v in values if v is not None and math.isfinite(v)]
    if not values:
        return {'n': 0, 'mean': None, 'sd': None, 'ci95_half': None}
    mean = statistics.mean(values)
    if len(values) < 2:
        return {'n': 1, 'mean': mean, 'sd': None, 'ci95_half': None}
    critical = [12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262,
                2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101,
                2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048,
                2.045, 2.042]
    df = len(values) - 1
    t = critical[df - 1] if df <= 30 else 2.042 if df < 40 else 2.021 if df < 60 else 2 if df < 120 else 1.980
    sd = statistics.stdev(values)
    return {'n': len(values), 'mean': mean, 'sd': sd, 'ci95_half': t * sd / math.sqrt(len(values))}


def write_rows(path, rows):
    rows = list(rows)
    if not rows:
        return
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def analyze_requests(directory, case, parts, start_ms, event):
    """Read one process at a time, retain numeric latency arrays, gzip merged raw rows.

    parts: (pc, process, native CSV path, agent-minus-controller clock offset).
    Each native file is preserved; only timestamps in the merged CSV are shifted.
    """
    expected_per_process = math.ceil(case['rate'] * case['duration'])
    expected = 3 * case['processes'] * expected_per_process
    latencies, failed, lags, responses = [], [], [], []
    phases, hosts, hist = defaultdict(list), defaultdict(list), Counter()
    host_counts = defaultdict(Counter)
    process_rows = []
    windows = defaultdict(lambda: {'requests': 0, 'answered': 0, 'failed': 0})
    served, ids = Counter(), set()
    retried = busy = observed = in_window = 0
    first_sends, ends, first_survivor, after_fault_success = [], [start_ms + case['duration'] * 1000], [], 0
    uuid_valid, launch_by_pc = True, defaultdict(list)
    allocation_fields_present = True
    assignment_totals = Counter()
    before = event.get('at_ms')
    after = event.get('ready_ms') if event.get('kind') == 'fault' else event.get('end_ms')
    allocation_fields = ['allocation_policy', 'assignments', 'resource_assignments', 'responder_assignments', 'resource_shortfalls', 'responder_shortfalls']
    header = ['pc', 'process', 'client_id', 'seq', 'sent_at_ms', 'response_at_ms', 'latency_ms', 'served_by',
              'epoch', 'degraded', 'attempts', 'busy_replies', 'answered', 'elapsed_ms', 'scheduled_at_ms', 'dispatch_lag_ms'] + allocation_fields
    with gzip.open(directory / 'requests.csv.gz', 'wt', newline='', encoding='utf-8') as merged:
        writer = csv.DictWriter(merged, fieldnames=header)
        writer.writeheader()
        for pc, process, path, offset in parts:
            seen = bytearray(expected_per_process)
            file_ids, earliest = set(), None
            file_latency, file_failed, file_lag = [], [], []
            if not path.exists() or not path.stat().st_size:
                continue
            with gzip.open(path, 'rt', newline='', encoding='utf-8') as stream:
                for row in csv.DictReader(stream):
                    identity, seq = str(uuid.UUID(row['client_id'])), int(row['seq'])
                    if not 0 <= seq < expected_per_process or seen[seq]:
                        raise ValueError(f'Duplicate or invalid request sequence in {path}')
                    seen[seq] = 1
                    file_ids.add(identity)
                    sent = float(row['sent_at_ms']) - offset
                    response = float(row['response_at_ms']) - offset if row['response_at_ms'] else None
                    elapsed, lag = float(row['elapsed_ms']), float(row['dispatch_lag_ms'])
                    if not all(math.isfinite(v) and v >= 0 for v in (elapsed, lag)):
                        raise ValueError(f'Invalid elapsed or dispatch delay in {path}')
                    answered = row['answered'] == 'true'
                    if answered != (response is not None):
                        raise ValueError(f'Inconsistent response fields in {path}')
                    observed += 1
                    host_counts[pc]['observed_requests'] += 1
                    host_counts[pc]['in_window'] += int(start_ms - 100 <= sent < start_ms + case['duration'] * 1000)
                    in_window += int(start_ms - 100 <= sent < start_ms + case['duration'] * 1000)
                    earliest = min(earliest, sent) if earliest is not None else sent
                    ends.append(sent + elapsed)
                    lags.append(lag)
                    file_lag.append(lag)
                    retried += int(int(row['attempts']) > 1)
                    busy += int(row['busy_replies'])
                    phase = 'whole' if before is None else 'before' if sent < before else 'during' if after is None or sent < after else 'after'
                    windows[phase]['requests'] += 1
                    if answered:
                        policy_ok = row.get('allocation_policy') == ALLOCATION_POLICY and all(row.get(k, '') != '' for k in allocation_fields)
                        allocation_fields_present &= policy_ok
                        if policy_ok:
                            for k in allocation_fields[1:]:
                                assignment_totals[k] += int(row[k])
                            assignment_totals['plans_with_shortfall'] += int(int(row['resource_shortfalls']) + int(row['responder_shortfalls']) > 0)
                            assignment_totals['plans_without_assignments'] += int(row['assignments'] == '0')
                        latency = float(row['latency_ms'])
                        if not math.isfinite(latency) or latency < 0:
                            raise ValueError(f'Invalid latency in {path}')
                        latencies.append(latency)
                        responses.append(response)
                        phases[phase].append(latency)
                        hosts[pc].append(latency)
                        file_latency.append(latency)
                        host_counts[pc]['answered'] += 1
                        served[int(row['served_by'])] += 1
                        hist[int(math.floor(math.log2(max(latency, 2 ** -10)) * 4))] += 1
                        windows[phase]['answered'] += 1
                        if event.get('kind') == 'fault' and sent >= before:
                            after_fault_success += 1
                            if int(row['served_by']) != event['target']:
                                first_survivor.append(response)
                    else:
                        failed.append(elapsed)
                        file_failed.append(elapsed)
                        windows[phase]['failed'] += 1
                    for field in ('sent_at_ms', 'response_at_ms', 'scheduled_at_ms'):
                        if row.get(field):
                            row[field] = f'{float(row[field]) - offset:.3f}'
                    writer.writerow({'pc': pc, 'process': process, **{k: row.get(k, '') for k in header[2:]}})
            uuid_valid &= len(file_ids) == 1 and not bool(file_ids & ids)
            ids.update(file_ids)
            if earliest is not None:
                launch_by_pc[pc].append(earliest)
                first_sends.append(earliest)
            process_rows.append({'pc': pc, 'process': process, 'requests': expected_per_process,
                'observed_requests': sum(seen), 'answered': len(file_latency),
                'failed': expected_per_process - len(file_latency), **distribution(file_latency),
                'dispatch_lag_p95_ms': distribution(file_lag)['p95_ms'],
                'failed_elapsed_p95_ms': distribution(file_failed)['p95_ms']})
    answered = len(latencies)
    duration = case['duration']
    wall_s = max(.001, (max(ends) - start_ms) / 1000)
    successful_in_window = sum(start_ms <= t <= start_ms + duration * 1000 for t in responses)
    ordered = sorted([start_ms, start_ms + duration * 1000] +
                     [t for t in responses if start_ms <= t <= start_ms + duration * 1000])
    total_served = sum(served.values())
    lat = distribution(latencies)
    result = {'requests': expected, 'observed_requests': observed, 'answered': answered, 'failed': expected - answered,
              'missing_records': max(0, expected - observed), 'success_pct': 100 * answered / expected,
              'offered_rps': 3 * case['processes'] * case['rate'], 'achieved_send_rps': in_window / duration,
              'throughput_rps': successful_in_window / duration, 'goodput_rps': answered / wall_s,
              'workload_wall_s': wall_s, 'retried': retried, 'busy_replies': busy,
              'lat_mean_ms': lat['mean_ms'], 'lat_sd_ms': lat['sd_ms'], 'lat_p50_ms': lat['p50_ms'],
              'lat_p95_ms': lat['p95_ms'], 'lat_p99_ms': lat['p99_ms'], 'lat_max_ms': lat['max_ms'],
              'failed_elapsed_mean_ms': distribution(failed)['mean_ms'],
              'failed_elapsed_p95_ms': distribution(failed)['p95_ms'],
              'dispatch_lag_p95_ms': distribution(lags)['p95_ms'],
              'launch_skew_ms': max(first_sends) - min(first_sends) if first_sends else None,
              'max_service_gap_ms': max(b - a for a, b in zip(ordered, ordered[1:])),
              'fairness_jain': total_served ** 2 / (3 * sum(v*v for v in served.values())) if total_served else None,
              'served_1': served[1], 'served_2': served[2], 'served_3': served[3],
              'uuid_valid': uuid_valid and len(ids) == 3 * case['processes'],
              'post_fault_answered': after_fault_success if event.get('kind') == 'fault' else None,
              'first_survivor_reply_ms': min(first_survivor) - before if first_survivor else None}
    result['load_generation_valid'] = (observed == expected and in_window >= .95 * expected and
        result['dispatch_lag_p95_ms'] is not None and result['dispatch_lag_p95_ms'] <= 100)
    result['allocation_fields_present'] = allocation_fields_present
    result.update({k: assignment_totals[k] if allocation_fields_present else None for k in allocation_fields[1:] + ['plans_with_shortfall', 'plans_without_assignments']})
    result['shortfall_pct'] = 100 * assignment_totals['plans_with_shortfall'] / answered if answered and allocation_fields_present else None
    write_rows(directory / 'windows.csv', [{'window': phase, **counts, **distribution(phases[phase])}
                                          for phase, counts in sorted(windows.items())])
    write_rows(directory / 'clients.csv', [{'pc': pc, 'requests': expected // 3,
                'observed_requests': host_counts[pc]['observed_requests'], 'answered': host_counts[pc]['answered'],
                'failed': expected // 3 - host_counts[pc]['answered'],
                'achieved_send_rps': host_counts[pc]['in_window'] / duration,
                **distribution(hosts[pc]), 'first_send_ms': min(launch_by_pc[pc]) if launch_by_pc[pc] else None}
                for pc in range(1, 4)])
    write_rows(directory / 'processes.csv', process_rows)
    write_rows(directory / 'histogram.csv', [{'lower_ms': 2 ** (bucket / 4), 'upper_ms': 2 ** ((bucket + 1) / 4),
                'count': count} for bucket, count in sorted(hist.items())])
    return result, sorted(ids)


def node_deltas(before, after, samples):
    def delta(key):
        return sum(max(0, a.get(key, 0) - b.get(key, 0)) for b, a in zip(before, after) if b and a)
    result = {key: delta(key) for key in ['elections_decided', 'step_downs', 'late_results', 'replicate_conflicts',
               'resource_overcommits', 'peer_queue_drops', 'suppressed_triggers', 'elections_recorded']}
    result['node_sent_bytes'] = sum(max(0, sum(v[1] for v in a.get('sent', {}).values()) -
        sum(v[1] for v in b.get('sent', {}).values())) for b, a in zip(before, after) if b and a)
    for key, index in [('electbid_messages', 0), ('electbid_bytes', 1)]:
        result[key] = sum(max(0, a.get('sent', {}).get('ElectBid', [0, 0])[index] -
            b.get('sent', {}).get('ElectBid', [0, 0])[index]) for b, a in zip(before, after) if b and a)
    duration = delta('election_time_us')
    result['election_mean_ms'] = duration / result['elections_recorded'] / 1000 if result['elections_recorded'] else None
    for key in ['dropped_control', 'dropped_replication']:
        result[key] = sum(max(0, sum(p.get(key, 0) for p in a.get('peer_queues', [])) -
            sum(p.get(key, 0) for p in b.get('peer_queues', []))) for b, a in zip(before, after) if b and a)
    result['repaired_results'] = sum(max(0, sum(p.get('repaired_results', 0) for p in a.get('sync', [])) -
        sum(p.get('repaired_results', 0) for p in b.get('sync', []))) for b, a in zip(before, after) if b and a)
    started = sum(max(0, a.get('workers', {}).get('started', 0) - b.get('workers', {}).get('started', 0))
                  for b, a in zip(before, after) if b and a)
    waits = sum(max(0, a.get('workers', {}).get('queue_wait_total_us', 0) -
                    b.get('workers', {}).get('queue_wait_total_us', 0)) for b, a in zip(before, after) if b and a)
    result['worker_queue_wait_mean_ms'] = waits / started / 1000 if started else None
    result['worker_expired'] = sum(max(0, a.get('workers', {}).get('expired_before_start', 0) -
        b.get('workers', {}).get('expired_before_start', 0)) for b, a in zip(before, after) if b and a)
    for key in ['datagrams_sent', 'bytes_sent', 'retransmissions', 'delivery_timeouts']:
        result['udp_' + key] = sum(max(0, a.get('transport', {}).get(key, 0) - b.get('transport', {}).get(key, 0))
                                  for b, a in zip(before, after) if b and a)
    for key in ['queue_len', 'peer_control_queued', 'peer_replication_queued', 'sync_lag', 'rss_bytes', 'cpu_load']:
        values = [s[key] for s in samples if s.get('reachable') and key in s]
        result[f'{key}_max'] = max(values) if values else None
    result['unreachable_probes'] = sum(not s.get('reachable') for s in samples)
    return result
