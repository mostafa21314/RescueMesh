#!/usr/bin/env python3
"""Offline six-PC comparison report; reads compact summaries, not millions of raw rows."""
import csv
import html
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

from study_metrics import ci95, write_rows

METRICS = ['success_pct', 'goodput_rps', 'throughput_rps', 'achieved_send_rps', 'lat_mean_ms', 'lat_sd_ms',
           'lat_p50_ms', 'lat_p95_ms', 'lat_p99_ms', 'lat_max_ms', 'fairness_jain', 'electbid_messages_per_request',
           'election_mean_ms', 'max_service_gap_ms', 'first_survivor_reply_ms', 'recovery_ms', 'convergence_ms',
           'dispatch_lag_p95_ms', 'worker_queue_wait_mean_ms', 'node_sent_bytes', 'peer_queue_drops',
           'resource_overcommits', 'replicate_conflicts', 'queue_len_max', 'rss_bytes_max', 'cpu_load_max',
           'repaired_results', 'dropped_control', 'dropped_replication', 'peer_replication_queued_max', 'sync_lag_max']


def summarize(results):
    groups = defaultdict(list)
    for row in results:
        groups[(row['stage'], row['scenario'], row['processes'], row['rate'], row['mode'])].append(row)
    rows = []
    for (stage, scenario, processes, rate, mode), runs in sorted(groups.items()):
        row = {'stage': stage, 'scenario': scenario, 'processes_per_pc': processes, 'total_processes': 3 * processes,
               'rate_per_process': rate, 'offered_rps': 3 * processes * rate, 'mode': mode, 'runs': len(runs),
               'valid_runs': sum(r['measurement_valid'] for r in runs), 'converged_runs': sum(r['converged'] for r in runs),
               'requested': sum(r['requests'] for r in runs), 'answered': sum(r['answered'] for r in runs),
               'failed': sum(r['failed'] for r in runs), 'missing_records': sum(r['missing_records'] for r in runs),
               'generator_invalid_runs': sum(not r['load_generation_valid'] for r in runs),
               'intervention_invalid_runs': sum(not r['intervention_valid'] for r in runs)}
        for metric in METRICS:
            summary = ci95([r.get(metric) for r in runs])
            row.update({f'{metric}_{k}': v for k, v in summary.items()})
        valid = ci95([r['goodput_rps'] for r in runs if r['measurement_valid']])
        row.update({f'valid_goodput_rps_{k}': v for k, v in valid.items()})
        rows.append(row)
    return rows


def paired(results):
    blocks = defaultdict(dict)
    for row in results:
        blocks[(row['stage'], row['scenario'], row['processes'], row['rate'], row['rep'])][row['mode']] = row
    groups = defaultdict(list)
    for key, modes in blocks.items():
        if len(modes) != 2 or not all(r['measurement_valid'] for r in modes.values()):
            continue
        for metric in METRICS:
            a, b = modes['on_demand'].get(metric), modes['per_request'].get(metric)
            if a is not None and b is not None:
                groups[(*key[:4], metric)].append(b - a)
    return [{'stage': key[0], 'scenario': key[1], 'processes_per_pc': key[2], 'rate_per_process': key[3],
             'metric': key[4], 'difference': 'per_request minus on_demand', **ci95(values)}
            for key, values in sorted(groups.items())]


def value(v):
    return f'{v:.2f}' if v is not None and isinstance(v, (int, float)) and math.isfinite(v) else 'N/A'


def table(headers, rows):
    return '<div class="scroll"><table><thead><tr>' + ''.join(f'<th>{html.escape(str(h))}</th>' for h in headers) + \
        '</tr></thead><tbody>' + ''.join('<tr>' + ''.join(f'<td>{html.escape(str(v))}</td>' for v in row) + '</tr>' for row in rows) + '</tbody></table></div>'


def generate(root):
    root = Path(root)
    results = [json.loads(p.read_text(encoding='utf-8')) for p in sorted((root / 'cases').glob('*/result.json'))]
    if not results:
        raise ValueError('No completed run summaries found')
    summary, differences = summarize(results), paired(results)
    write_rows(root / 'summary.csv', summary)
    write_rows(root / 'paired-differences.csv', differences)
    if not differences:
        (root / 'paired-differences.csv').write_text('stage,scenario,processes_per_pc,rate_per_process,metric,difference,n,mean,sd,ci95_half\n', encoding='utf-8')
    write_rows(root / 'runs.csv', results)
    # Collect per-PC and per-window statistics without loading raw request tables.
    for filename in ['clients.csv', 'processes.csv', 'windows.csv']:
        merged = []
        for result in results:
            path = root / result['artifacts'] / filename
            if path.exists():
                with path.open(newline='', encoding='utf-8') as stream:
                    merged.extend({'case': result['id'], 'mode': result['mode'], **r} for r in csv.DictReader(stream))
        write_rows(root / filename, merged)
    figures = []
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as error:
        raise RuntimeError('Install reports: python -m pip install -r scripts/requirements-report.txt') from error
    plots = root / 'plots'
    plots.mkdir(exist_ok=True)
    colors = {'on_demand': '#2474b5', 'per_request': '#d96b27'}

    def export(fig, name):
        fig.tight_layout()
        fig.savefig(plots / f'{name}.png', dpi=150)
        fig.savefig(plots / f'{name}.pdf')
        plt.close(fig)
        figures.append(name)

    normal = [r for r in summary if r['stage'] == 'normal']
    if normal:
        rates = sorted({r['rate_per_process'] for r in normal})
        levels = sorted({r['processes_per_pc'] for r in normal})
        for metric, label in [('success_pct_mean', 'Successful requests (%)'), ('lat_p99_ms_mean', 'Mean run p99 latency (ms)')]:
            fig, axes = plt.subplots(1, 2, figsize=(13, 4), squeeze=False)
            for mode, ax in zip(colors, axes[0]):
                grid = np.full((len(levels), len(rates)), np.nan)
                selected = {(r['processes_per_pc'], r['rate_per_process']): r for r in normal if r['mode'] == mode}
                for i, p in enumerate(levels):
                    for j, rate in enumerate(rates):
                        row = selected.get((p, rate))
                        if row and row.get(metric) is not None:
                            grid[i, j] = row[metric]
                image = ax.imshow(grid, aspect='auto', cmap='viridis', vmin=0, vmax=100 if metric.startswith('success') else None)
                ax.set_xticks(range(len(rates)), [f'{r:g}' for r in rates])
                ax.set_yticks(range(len(levels)), [str(p) for p in levels])
                ax.set(xlabel='Requests/s per process', ylabel='Processes per client PC', title=mode)
                for i, p in enumerate(levels):
                    for j, rate in enumerate(rates):
                        row = selected.get((p, rate))
                        if row:
                            marker = ' !' if row['valid_runs'] != row['runs'] else ''
                            ax.text(j, i, f'{value(row.get(metric))}{marker}', ha='center', va='center', fontsize=8,
                                    color='white', bbox={'facecolor': 'black', 'alpha': .3, 'pad': 1, 'edgecolor': 'none'})
                fig.colorbar(image, ax=ax, label=label)
            fig.suptitle(label + '; ! = at least one measurement-invalid run')
            export(fig, 'normal-' + metric)
        for metric, label in [('goodput_rps', 'Goodput (successful requests/s)'), ('lat_p50_ms', 'p50 latency (ms)'),
                              ('lat_p95_ms', 'p95 latency (ms)'), ('lat_p99_ms', 'p99 latency (ms)'),
                              ('electbid_messages_per_request', 'Election messages / scheduled request'),
                              ('fairness_jain', 'Jain workload distribution index')]:
            fig, axes = plt.subplots(1, len(levels), figsize=(max(7, 3.4*len(levels)), 3.5), squeeze=False)
            for p, ax in zip(levels, axes[0]):
                for mode, color in colors.items():
                    rows = sorted([r for r in normal if r['mode'] == mode and r['processes_per_pc'] == p], key=lambda r: r['offered_rps'])
                    x = [r['offered_rps'] for r in rows]
                    y = [r.get(metric + '_mean') if r.get(metric + '_mean') is not None else float('nan') for r in rows]
                    err = [r.get(metric + '_ci95_half') or 0 for r in rows]
                    ax.errorbar(x, y, yerr=err, color=color, marker='o', label=mode, capsize=3)
                ax.set(title=f'{p}/PC ({3*p} total)', xlabel='Offered requests/s', ylabel=label)
                ax.grid(alpha=.25)
            axes[0][0].legend(fontsize=8)
            fig.suptitle('Mean over runs; error bars = 95% Student-t CI (omitted for one run)')
            export(fig, 'comparison-' + metric)
    # Compact, binned pooled distributions. Exact percentiles remain in CSVs.
    for scenario in sorted({r['scenario'] for r in results}):
        chosen = [r for r in results if r['scenario'] == scenario]
        condition = min({(r['processes'], r['rate']) for r in chosen}, key=lambda x: x[0] * x[1])
        chosen = [r for r in chosen if (r['processes'], r['rate']) == condition]
        fig, ax = plt.subplots(figsize=(7, 4))
        for mode, color in colors.items():
            counts = Counter()
            for result in chosen:
                if result['mode'] != mode:
                    continue
                histogram = root / result['artifacts'] / 'histogram.csv'
                if not histogram.exists():
                    continue
                with histogram.open(newline='', encoding='utf-8') as stream:
                    for row in csv.DictReader(stream):
                        counts[float(row['upper_ms'])] += int(row['count'])
            total, cumulative, x, y = sum(counts.values()), 0, [], []
            for upper, count in sorted(counts.items()):
                cumulative += count
                x.append(upper)
                y.append(cumulative / total)
            if total:
                ax.step(x, y, where='post', label=f'{mode} ({total:,} successes)', color=color)
        ax.set(xlabel='Successful latency (ms), logarithmic', ylabel='Cumulative fraction', xscale='log',
               title=f'{scenario}: binned CDF, {condition[0]} processes/PC, {condition[1]:g} req/s/process')
        ax.grid(alpha=.25)
        if ax.lines:
            ax.legend()
        export(fig, f'{scenario}-cdf')
    # One representative run per mode/scenario; raw samples exist for every run.
    for scenario in sorted({r['scenario'] for r in results}):
        for mode in colors:
            candidates = [r for r in results if r['scenario'] == scenario and r['mode'] == mode]
            if not candidates:
                continue
            result = min(candidates, key=lambda r: (r['rep'], r['processes']*r['rate']))
            directory = root / result['artifacts']
            samples = [json.loads(line) for line in (directory / 'samples.jsonl').read_text().splitlines()]
            event = json.loads((directory / 'intervention.json').read_text())
            launch = json.loads((directory / 'launch.json').read_text())
            fig, axes = plt.subplots(4, 1, figsize=(11, 9), sharex=True)
            for node, color in zip(range(1, 4), ['#2474b5', '#d96b27', '#20876d']):
                rows = [s for s in samples if s['node'] == node]
                t = [s['t_ms']/1000 for s in rows]
                for ax, key in zip(axes, ['queue_len', 'cpu_load', 'peer_replication_queued', 'sync_lag']):
                    ax.plot(t, [s.get(key, float('nan')) if s['reachable'] else float('nan') for s in rows],
                            label=f'node {node}', color=color)
            for ax, label in zip(axes, ['Planner queue', 'Machine CPU fraction', 'Outgoing replication messages', 'Known repair revision lag']):
                ax.set_ylabel(label)
                ax.grid(alpha=.25)
                if event.get('at_ms'):
                    start = (event['at_ms'] - launch['start_ms']) / 1000
                    end = event.get('ready_ms') or event.get('end_ms')
                    ax.axvspan(start, (end - launch['start_ms']) / 1000 if end else result['duration'], color='grey', alpha=.15)
            axes[0].legend(ncol=3)
            axes[0].set_title(f'{scenario} / {mode}: {result["id"]}; gaps = unanswered probes')
            axes[-1].set_xlabel('Time from scheduled workload start (s)')
            export(fig, f'{scenario}-{mode}-timeline')
            hosts_path = directory / 'hosts.jsonl'
            observations = [json.loads(line) for line in hosts_path.read_text().splitlines()]
            by_agent = defaultdict(list)
            for observation in observations:
                if observation['metrics']:
                    by_agent[observation['agent']].append(observation)
            fig, ax = plt.subplots(figsize=(11, 4))
            for address, rows in sorted(by_agent.items()):
                x, y = [], []
                for a, b in zip(rows, rows[1:]):
                    left, right = a['metrics'], b['metrics']
                    if left['cpu_total'] is None or right['cpu_total'] is None:
                        continue
                    total = right['cpu_total'] - left['cpu_total']
                    if total > 0:
                        x.append(b['t_ms']/1000)
                        y.append(100*(1 - (right['cpu_idle'] - left['cpu_idle'])/total))
                if x:
                    ax.plot(x, y, label=address)
            ax.set(xlabel='Time from scheduled start (s)', ylabel='Host CPU (%)', title=f'{scenario} / {mode}: all six PCs')
            ax.grid(alpha=.25)
            if ax.lines:
                ax.legend(fontsize=8, ncol=2)
            export(fig, f'{scenario}-{mode}-host-cpu')
    notes = [
        'Rates are per process; processes are per client PC. Total offered load = 3 × processes/PC × rate/process.',
        'Latency describes successful requests. Failed and missing requests remain in the availability denominator; failed elapsed times are recorded separately.',
        'lat_sd_ms is sample SD within a run (n−1). Summary SD/CI describe independent repetitions; p50/p95/p99 means are means of run percentiles, not pooled percentiles. Fewer than two observations/runs gives N/A for SD/CI.',
        'Percentiles use the nearest sorted index round((n−1)×p), matching the existing Rust client. CDF figures use histogram bins; exact run percentiles are in runs.csv.',
        'CI uses conservative two-sided Student-t 95% intervals. Shared machines/network still limit generalization beyond this experiment.',
        'A measurement-invalid run is retained and shown. Generator-invalid means missing scheduled sends, <95% dispatched within the workload window, or dispatch p95 >100 ms. Inspect achieved_send_rps before interpreting server capacity.',
        'Paired differences use only measurement-valid pairs with the same condition, seed and repetition. Positive differences mean per_request minus on_demand.',
        'Every independent run restarts all servers and requires an empty healthy baseline, then converged warm-up. Nonconvergent runs remain failures; the next run must establish a fresh baseline.',
        'Soak observations are continuous operation and are analyzed separately. A common sustainable normal point is required before soak execution.',
        'Election decisions/durations are summed across node decisions, not unique logical elections. Per-request elections do not change the persistent-server epoch.',
        'Fault target is the incumbent in on_demand, or the most active measured worker in per_request. First-survivor latency requires a request submitted after acknowledged injection and computed by a non-victim.',
        'Recovery time excludes the requested mute interval and includes polling uncertainty. Convergence time is an observed upper bound after client completion; collection overlaps provisional convergence polling.',
        'Wall-clock alignment uses measured agent offsets. Inspect uncertainty and offset drift before interpreting cross-PC intervention windows; request latency and dispatch lag use monotonic clocks.',
        'Repair lag uses the latest peer revision observed, so zero known lag while disconnected does not prove convergence. Host network counters can include both virtual and physical interfaces.',
        'Server logging defaults to warn in both modes to avoid per-request election log I/O dominating the comparison. Node counters, full snapshots, warnings and raw samples are retained.',
    ]
    headers = ['Stage / scenario', 'Mode', 'Processes/PC', 'Rate/process', 'Runs valid / total', 'Success %',
               'p50 / p95 / p99 ms', 'Mean within-run latency SD ms', 'Goodput mean ± SD', 'Converged']
    cells = [[f'{r["stage"]}/{r["scenario"]}', r['mode'], r['processes_per_pc'], f'{r["rate_per_process"]:g}',
              f'{r["valid_runs"]}/{r["runs"]}', value(r['success_pct_mean']),
              ' / '.join(value(r[f'lat_p{p}_ms_mean']) for p in [50, 95, 99]), value(r['lat_sd_ms_mean']),
              f'{value(r["goodput_rps_mean"])} ± {value(r["goodput_rps_sd"])}', f'{r["converged_runs"]}/{r["runs"]}'] for r in summary]
    document = '<!doctype html><html lang="en"><meta charset="utf-8"><title>Six-PC election comparison</title>'
    document += '<style>body{font:16px system-ui;margin:32px;color:#17212b}table{border-collapse:collapse;font-size:13px}th,td{padding:8px;border:1px solid #cbd5df}th{background:#edf4fa}.scroll{overflow:auto}img{max-width:100%;display:block;margin:24px 0}li{margin:10px 0}</style>'
    document += f'<h1>RescueMesh · Six-PC election comparison</h1><p>{len(results)} completed runs; three server PCs and three client PCs. UDP Request–Reply, application protocol 2.</p>'
    document += table(headers, cells) + '<h2>Interpretation and measurement definitions</h2><ul>' + ''.join(f'<li>{html.escape(n)}</li>' for n in notes) + '</ul>'
    diagnostic_headers = ['Scenario', 'Mode', 'Processes/PC', 'Rate', 'Generator-invalid runs', 'Invalid interventions',
                          'Mean peer drops', 'Mean overcommit detections', 'Mean resource conflicts', 'Missing rows']
    diagnostics = [[r['scenario'], r['mode'], r['processes_per_pc'], r['rate_per_process'], r['generator_invalid_runs'],
                    r['intervention_invalid_runs'], value(r['peer_queue_drops_mean']), value(r['resource_overcommits_mean']),
                    value(r['replicate_conflicts_mean']), r['missing_records']] for r in summary]
    document += '<h2>Validity and resource correctness diagnostics</h2>' + table(diagnostic_headers, diagnostics)
    document += '<p>Availability, state convergence and resource-allocation correctness are separate findings. Conflict/overcommit counters are per-node detections, not unique incidents.</p>'
    document += '<p>Exact tables: <a href="runs.csv">runs.csv</a>, <a href="summary.csv">summary.csv</a>, <a href="paired-differences.csv">paired differences</a>, clients.csv, processes.csv, windows.csv. Raw compressed requests and logs are under cases/.</p>'
    document += ''.join(f'<a href="plots/{name}.pdf"><img src="plots/{name}.png" alt="{html.escape(name)}"></a>' for name in figures)
    (root / 'report.html').write_text(document + '</html>', encoding='utf-8')
    md = ['# Six-PC election comparison', '', f'{len(results)} completed runs.', '',
          '| ' + ' | '.join(headers) + ' |', '|' + '|'.join(['---']*len(headers)) + '|']
    md += ['| ' + ' | '.join(map(str, row)) + ' |' for row in cells]
    md += ['', '## Validity and resource diagnostics', '', '| ' + ' | '.join(diagnostic_headers) + ' |',
           '|' + '|'.join(['---']*len(diagnostic_headers)) + '|']
    md += ['| ' + ' | '.join(map(str, row)) + ' |' for row in diagnostics]
    md += ['', '## Measurement definitions', ''] + ['- ' + n for n in notes]
    md += ['', '## Figures', ''] + [f'![{name}](plots/{name}.png)' for name in figures]
    (root / 'report.md').write_text('\n'.join(md) + '\n', encoding='utf-8')
    print(f'{root / "report.html"}: {len(figures)} PNG/PDF figures; summary.csv includes SD and 95% CI', flush=True)
