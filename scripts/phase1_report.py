#!/usr/bin/env python3
"""Export Phase 1 reliability tables, PNG/PDF figures and an offline HTML report.

Usage: python scripts/phase1_report.py RESULTS_DIRECTORY
Dependencies: python -m pip install -r scripts/requirements-report.txt
"""
import argparse
import csv
import gzip
import html
import math
import statistics
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
except ImportError:
    raise SystemExit('Install plotting dependencies: python -m pip install -r scripts/requirements-report.txt')

COLORS = ['#2474b5', '#d96b27', '#20876d', '#a14683']


def rows(path):
    if not path.exists() and path.with_suffix(path.suffix + '.gz').exists():
        path = path.with_suffix(path.suffix + '.gz')
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def number(row, key, fallback=math.nan):
    try:
        value = float(row.get(key, ''))
        return value if math.isfinite(value) else fallback
    except (ValueError, TypeError):
        return fallback


def quantile(values, p):
    if not values:
        return math.nan
    v = sorted(values)
    return v[int((len(v) - 1) * p + .5)]


def ci(values):
    v = [x for x in values if math.isfinite(x)]
    if not v:
        return math.nan, math.nan
    if len(v) < 2:
        return v[0], math.nan
    # Same conservative Student-t table used by the Rust harness.
    critical = [12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262,
                2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101,
                2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052,
                2.048, 2.045, 2.042]
    df = len(v) - 1
    t = critical[df - 1] if df <= 30 else (2.042 if df < 40 else 2.021 if df < 60 else 2.000 if df < 120 else 1.980)
    return statistics.mean(v), t * statistics.stdev(v) / math.sqrt(len(v))


def wilson(successes, total):
    if not total:
        return math.nan, math.nan
    z = 1.95996398454
    p = successes / total
    scale = 1 + z*z/total
    centre = (p + z*z/(2*total)) / scale
    half = z * math.sqrt(p*(1-p)/total + z*z/(4*total*total)) / scale
    return max(0, centre-half), min(1, centre+half)


def fmt(value):
    return f'{value:.2f}' if math.isfinite(value) else 'N/A'


def save(fig, directory, name):
    fig.tight_layout()
    fig.savefig(directory / f'{name}.png', dpi=150)
    fig.savefig(directory / f'{name}.pdf')
    plt.close(fig)
    return f'plots/{name}.png'


def summarize(scenario, runs, requests):
    total = int(sum(number(r, 'requests', 0) for r in runs))
    answered = int(sum(number(r, 'answered', 0) for r in runs))
    latency = [number(r, 'latency_ms') for r in requests if r.get('answered') == 'true']
    latency = [v for v in latency if math.isfinite(v)]
    goodput = ci([number(r, 'goodput_rps', number(r, 'throughput_rps')) for r in runs])
    success_run_ci = ci([100 * number(r, 'answered', 0) / max(1, number(r, 'requests', 0)) for r in runs])
    low, high = wilson(answered, total)
    fault_runs = [r for r in runs if r.get('fault_requested') == 'true' or r.get('victim')]
    return {
        'scenario': scenario, 'clients': int(runs[0]['clients']), 'runs': len(runs),
        'requests': total, 'answered': answered, 'failed': total-answered,
        'success_pct': 100 * answered / max(1, total),
        'wilson_low_pct': 100 * low, 'wilson_high_pct': 100 * high,
        'run_success_mean_pct': success_run_ci[0], 'run_success_ci95': success_run_ci[1],
        'missing_records': max(0, total-len(requests)),
        'mean_ms': statistics.mean(latency) if latency else math.nan,
        'p50_ms': quantile(latency, .5), 'p95_ms': quantile(latency, .95), 'p99_ms': quantile(latency, .99),
        'goodput_mean_rps': goodput[0], 'goodput_ci95': goodput[1],
        'converged_runs': sum(r.get('converged') == 'true' for r in runs),
        'baseline_verified_runs': sum(r.get('baseline_agreed') == 'true' for r in runs),
        'baseline_failed_runs': sum(r.get('baseline_agreed') == 'false' for r in runs),
        'baseline_unknown_runs': sum('baseline_agreed' not in r for r in runs),
        'convergence_scope': ';'.join(sorted({r.get('convergence_scope', 'legacy_whole_table') for r in runs})),
        'fault_runs_requested': len(fault_runs),
        'fault_runs_injected': sum(bool(r.get('victim')) for r in fault_runs),
        'client_exit_failures': int(sum(number(r, 'client_exit_failures', 0) for r in runs)),
        'retries': sum(number(r, 'attempts', 1) > 1 for r in requests),
        'busy_replies': int(sum(number(r, 'busy_replies', 0) for r in runs)),
        'peak_queue': int(max(number(r, 'queue_max', 0) for r in runs)),
        'peak_rss_mb': max(number(r, 'rss_max_mb', 0) for r in runs),
        'udp_retransmissions': int(sum(number(r, 'udp_retransmissions', 0) for r in runs)),
        'udp_duplicate_fragments': int(sum(number(r, 'udp_duplicate_fragments', 0) for r in runs)),
        'udp_delivery_timeouts': int(sum(number(r, 'udp_delivery_timeouts', 0) for r in runs)),
        'peer_queue_drops': int(sum(number(r, 'peer_queue_drops', 0) for r in runs)),
        'dropped_control': int(sum(number(r, 'dropped_control', 0) for r in runs)),
        'dropped_replication': int(sum(number(r, 'dropped_replication', 0) for r in runs)),
        'repaired_results': int(sum(number(r, 'repaired_results', 0) for r in runs)),
        'worker_expired': int(sum(number(r, 'worker_expired', 0) for r in runs)),
        'worker_queue_wait_mean_ms': ci([number(r, 'worker_queue_wait_mean_ms') for r in runs])[0],
        'failed_elapsed_p95_ms': quantile([number(r, 'elapsed_ms') for r in requests
            if r.get('answered') != 'true' and math.isfinite(number(r, 'elapsed_ms'))], .95),
        'resource_overcommits': int(sum(number(r, 'overcommits', 0) for r in runs)),
    }


def scenario_plots(name, runs, requests, samples, out):
    images = []
    by_level = defaultdict(list)
    for row in requests:
        if row['answered'] == 'true' and row['latency_ms']:
            by_level[int(row['clients'])].append(float(row['latency_ms']))
    fig, (a, b) = plt.subplots(1, 2, figsize=(12, 4))
    for level, lat in sorted(by_level.items()):
        lat = sorted(lat)
        a.hist(lat, bins=40, histtype='step', density=True, label=f'{level} clients')
        b.step(lat, [(i+1)/len(lat) for i in range(len(lat))], where='post', label=f'{level} clients')
    for ax in (a, b):
        ax.set_xscale('symlog', linthresh=1)
        ax.set_xlabel('Successful request latency (ms)')
        ax.grid(alpha=.2)
        if by_level:
            ax.legend()
    a.set(ylabel='Probability density', title=f'{name}: latency distribution')
    b.set(ylabel='Fraction of successful requests', title='Empirical CDF (failures excluded)')
    images.append(save(fig, out, f'{name}-distribution'))

    # Representative run: highest recorded p99, explicitly labeled to avoid
    # presenting a handpicked timeline as an aggregate.
    chosen = max(runs, key=lambda r: number(r, 'lat_p99', -1))
    selected = [r for r in requests if r['run'] == chosen['run']]
    selected_samples = [r for r in samples if r['run'] == chosen['run']]
    fig, axes = plt.subplots(4, 1, figsize=(12, 10), sharex=True)
    nodes = sorted({r['node'] for r in samples} | {'1', '2', '3'})
    for i, node in enumerate(nodes):
        c = COLORS[i % len(COLORS)]
        points = [r for r in selected if r['served_by'] == node and r['answered'] == 'true']
        axes[0].scatter([number(r, 'sent_rel_ms')/1000 for r in points],
                        [number(r, 'latency_ms') for r in points], s=9, color=c, label=f'node {node}')
        probes = [r for r in selected_samples if r['node'] == node]
        t = [number(r, 't_ms')/1000 for r in probes]
        axes[2].plot(t, [number(r, 'queue_len') for r in probes], color=c, label=f'node {node}')
        axes[3].plot(t, [100*number(r, 'cpu_load') for r in probes], color=c)
        unavailable = [number(r, 't_ms')/1000 for r in probes if r['reachable'] != 'true']
        axes[1].scatter(unavailable, [int(node)] * len(unavailable), marker='x', color=c)
    failed = [r for r in selected if r['answered'] != 'true']
    axes[1].scatter([number(r, 'sent_rel_ms')/1000 for r in failed], [0]*len(failed),
                    color='#b52531', marker='x', label='unanswered request (row 0)')
    for ax in axes:
        for key, label, color in [('inject_rel_ms', 'fault injected', '#b52531'), ('ready_rel_ms', 'node ready', '#20876d')]:
            t = number(chosen, key)
            if math.isfinite(t):
                ax.axvline(t/1000, color=color, linestyle='--', label=label)
        ax.grid(alpha=.2)
    axes[0].set(yscale='symlog', ylabel='Latency (ms)', title=f'{name}: run {chosen["run"]}, {chosen["clients"]} clients (highest run p99)')
    axes[0].set_ylim(bottom=0)
    axes[0].legend(ncol=5, fontsize=8)
    axes[1].set(ylabel='Failure / node', yticks=[0, 1, 2, 3], title='Unanswered requests (0) and unavailable node probes (1–3)')
    axes[2].set_ylabel('Queued jobs')
    axes[2].set_ylim(bottom=0, top=max(1, axes[2].get_ylim()[1]))
    axes[2].legend(ncol=3, fontsize=8)
    axes[3].set(xlabel='Time from workload start (s)', ylabel='CPU EWMA (%)')
    axes[3].set_ylim(bottom=0)
    images.append(save(fig, out, f'{name}-timeline'))
    if samples and 'peer_replication_queued' in samples[0]:
        fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True)
        for i, node in enumerate(nodes):
            probes = [r for r in selected_samples if r['node'] == node]
            times = [number(r, 't_ms')/1000 for r in probes]
            for ax, key, label in zip(axes, ('peer_replication_queued', 'peer_control_queued', 'sync_lag'),
                                      ('Replication messages', 'Control messages', 'Known unpulled revisions')):
                ax.plot(times, [number(r,key) for r in probes], color=COLORS[i % len(COLORS)], label=f'node {node}')
                ax.set_ylabel(label)
                ax.grid(alpha=.2)
                ax.set_ylim(bottom=0)
        axes[0].set_title(f'{name}: run {chosen["run"]}, outgoing queues and repair progress')
        axes[0].legend(ncol=3)
        axes[-1].set_xlabel('Time from workload start (s)')
        images.append(save(fig, out, f'{name}-peer-queues'))
    return images


def generate(root):
    sweeps = sorted(root.rglob('runs.csv'))
    if not sweeps:
        raise ValueError(f'No runs.csv found under {root}')
    out = root / 'plots'
    out.mkdir(exist_ok=True)
    all_summary, datasets, images = [], [], []
    for path in sweeps:
        name = str(path.parent.relative_to(root)).replace('\\', '-').replace('/', '-')
        if name == '.':
            name = path.parent.name
        runs = rows(path)
        if not runs:
            continue
        requests = rows(path.parent / 'requests.csv')
        samples = rows(path.parent / 'samples.csv')
        datasets.append((name, runs, requests))
        for level in sorted({r['clients'] for r in runs}, key=int):
            all_summary.append(summarize(name, [r for r in runs if r['clients'] == level],
                                         [r for r in requests if r['clients'] == level]))
        images += scenario_plots(name, runs, requests, samples, out)
    if not all_summary:
        raise ValueError('No completed runs found')
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    for i, (name, runs, requests) in enumerate(datasets):
        summary = [r for r in all_summary if r['scenario'] == name]
        x = [r['clients'] for r in summary]
        color = COLORS[i % len(COLORS)]
        def series(ax, key, label, error=None):
            e = [r[error] if math.isfinite(r[error]) else 0 for r in summary] if error else None
            ax.errorbar(x, [r[key] for r in summary], yerr=e, capsize=3, marker='o', label=name, color=color)
            ax.set(xlabel='Concurrent client processes on this generator', ylabel=label)
            ax.grid(alpha=.2)
        series(axes[0, 0], 'goodput_mean_rps', 'Successful replies / workload wall second', 'goodput_ci95')
        series(axes[0, 1], 'success_pct', 'Successful requests (%)')
        series(axes[1, 0], 'p99_ms', 'Pooled successful-request p99 (ms)')
        series(axes[1, 1], 'udp_retransmissions', 'Server UDP fragment retransmissions (sum)')
    axes[0, 0].set_title('Goodput: mean ± 95% CI across runs')
    axes[0, 1].set(ylim=(-1, 101), title='Reliability: denominator includes missing records')
    axes[1, 0].set(title='Tail latency (always read alongside success rate)', ylim=(0, None))
    axes[1, 1].set_title('Transport overhead (includes server control traffic)')
    axes[0, 0].legend()
    images.insert(0, save(fig, out, 'comparison'))

    fig, (a, b) = plt.subplots(1, 2, figsize=(12, 4.5))
    labels = [f'{r["scenario"]}\n{r["clients"]} clients' for r in all_summary]
    bottom = [0.] * len(labels)
    for node in ('1', '2', '3'):
        counts = []
        for summary in all_summary:
            requests = next(req for name, _, req in datasets if name == summary['scenario'])
            n = sum(r['served_by'] == node and int(r['clients']) == summary['clients'] for r in requests)
            counts.append(100*n/max(1, summary['answered']))
        a.bar(labels, counts, bottom=bottom, label=f'node {node}')
        bottom = [x+y for x, y in zip(bottom, counts)]
    a.set(ylabel='Share of successful replies (%)', title='Work distribution (fairness depends on capacity)')
    a.legend()
    for i, key in enumerate(('failover_ms', 'recovery_ms', 'converge_ms')):
        values, errors = [], []
        for summary in all_summary:
            runs = next(runs for name, runs, _ in datasets if name == summary['scenario'])
            mean, half = ci([number(r, key) for r in runs if int(r['clients']) == summary['clients']])
            values.append(mean)
            errors.append(half if math.isfinite(half) else 0)
        b.errorbar([j + .15*(i-1) for j in range(len(labels))], values, yerr=errors,
                   marker='o', linestyle='none', capsize=3, label=key)
    b.set(xticks=range(len(labels)), xticklabels=labels, ylabel='Time (ms)', yscale='symlog', title='Failover, recovery and post-workload convergence')
    b.legend(fontsize=8)
    for ax in (a, b):
        ax.tick_params(axis='x', labelsize=8, rotation=30)
    images.insert(1, save(fig, out, 'distribution-recovery'))

    with (root / 'reliability.csv').open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(all_summary[0]))
        writer.writeheader()
        writer.writerows(all_summary)
    notes = [
        'Latency distributions include successful replies only; failed and missing operations remain in the success-rate denominator.',
        'Goodput includes process startup, retries and waiting for failures to exhaust. The older throughput_rps column measures the spacing between successful replies.',
        '95% Student-t intervals describe variation across repeated runs. N/A means fewer than two runs. Fresh local clusters isolate repetitions; remote clusters retain history unless explicitly restarted.',
        'Wilson intervals are an approximate binomial reference; requests share servers and are correlated, so they do not establish real-world reliability. Zero observed failures is not proof of zero failure probability.',
        'A requested fault without a recorded victim is an invalid fault experiment. Inspect client exit failures, missing records, peer queue drops, resource overcommits and convergence failures.',
        'Protocol 2 convergence compares the current run client UUIDs after processing is idle, including at least every answered result. Legacy runs compare whole retained tables. See convergence_scope in reliability.csv.',
        'A verified baseline requires matching retention-safe digests and role views. Baseline unknown means legacy data; a failed baseline indicates explicit soak mode. Shared remote history still prevents claiming fresh independent repetitions.',
        'Protocol 2 writes baseline/final node JSON, preserves failed-request elapsed times and client UUIDs, and records split control/replication drops, repairs and worker queue waits. Repairs and drops are per-node counters rather than counts of unique incidents.',
        'Repair lag compares installed cursors with the last revisions observed from peers; it cannot reveal updates created while a peer is unreachable. Zero known lag alone does not establish convergence.',
        'Node counters are process-wide and include diagnostics/ACKs. Retransmissions indicate missing acknowledgements, which may be caused by data loss, ACK loss or delayed processing.',
        'These are measurements of the named machines, workload and network only. Multi-PC tests should use synchronized clocks; client latency itself uses a monotonic clock.',
    ]
    headers = ['Scenario', 'Clients', 'Runs', 'Requests', 'Success % [Wilson 95%]', 'p50 / p95 / p99 ms', 'Goodput ± CI', 'Converged', 'Baseline verified / failed / unknown', 'Faults injected/requested', 'Missing', 'Exit failures', 'Peer drops / overcommits']
    table = []
    for r in all_summary:
        table.append([r['scenario'], str(r['clients']), str(r['runs']), str(r['requests']),
                      f"{fmt(r['success_pct'])} [{fmt(r['wilson_low_pct'])}, {fmt(r['wilson_high_pct'])}]",
                      ' / '.join(fmt(r[k]) for k in ('p50_ms', 'p95_ms', 'p99_ms')),
                      f"{fmt(r['goodput_mean_rps'])} ± {fmt(r['goodput_ci95'])}",
                      f"{r['converged_runs']}/{r['runs']}",
                      f"{r['baseline_verified_runs']} / {r['baseline_failed_runs']} / {r['baseline_unknown_runs']}",
                      f"{r['fault_runs_injected']}/{r['fault_runs_requested']}",
                      str(r['missing_records']), str(r['client_exit_failures']),
                      f"{r['peer_queue_drops']} / {r['resource_overcommits']}"])
    md = ['# Phase 1 UDP reliability report', '', '| ' + ' | '.join(headers) + ' |',
          '|' + '|'.join(['---']*len(headers)) + '|']
    md += ['| ' + ' | '.join(row) + ' |' for row in table]
    diagnostic_headers = ['Scenario', 'Clients', 'Retried requests', 'Busy replies', 'Peak queue',
                          'UDP retransmissions', 'UDP delivery timeouts', 'Peak node RSS MiB',
                          'Control / replication drops', 'Repaired results', 'Expired jobs', 'Worker queue wait ms', 'Failed elapsed p95 ms']
    diagnostics = [[r['scenario'], str(r['clients']), str(r['retries']), str(r['busy_replies']),
                    str(r['peak_queue']), str(r['udp_retransmissions']), str(r['udp_delivery_timeouts']),
                    fmt(r['peak_rss_mb']), f"{r['dropped_control']} / {r['dropped_replication']}",
                    str(r['repaired_results']), str(r['worker_expired']), fmt(r['worker_queue_wait_mean_ms']),
                    fmt(r['failed_elapsed_p95_ms'])] for r in all_summary]
    md += ['', '## Load and transport diagnostics', '', '| ' + ' | '.join(diagnostic_headers) + ' |',
           '|' + '|'.join(['---']*len(diagnostic_headers)) + '|']
    md += ['| ' + ' | '.join(row) + ' |' for row in diagnostics]
    md += ['', '## Interpretation', ''] + ['- ' + n for n in notes]
    md += ['', '## Figures', ''] + [f'![{Path(p).stem}]({p})' for p in images]
    (root / 'report.md').write_text('\n'.join(md)+'\n', encoding='utf-8')
    head = ''.join('<th>'+html.escape(v)+'</th>' for v in headers)
    body = ''.join('<tr>'+''.join('<td>'+html.escape(v)+'</td>' for v in row)+'</tr>' for row in table)
    document = '<!doctype html><html lang="en"><meta charset="utf-8"><title>Phase 1 reliability report</title>'
    document += '<style>body{font:16px system-ui;margin:32px;color:#17212b}table{border-collapse:collapse;font-size:13px}td,th{padding:9px;border:1px solid #ccd5df}th{background:#eef4fa}img{max-width:100%;display:block;margin:28px 0}li{margin:10px 0}</style>'
    document += '<h1>Phase 1 · UDP Request–Reply</h1><p>Measured reliability, workload distribution and recovery. Raw data: reliability.csv and per-scenario CSV files.</p>'
    document += f'<div style="overflow:auto"><table><tr>{head}</tr>{body}</table></div><h2>Interpretation</h2><ul>'
    document += ''.join('<li>'+html.escape(n)+'</li>' for n in notes)+'</ul>'
    diagnostic_head = ''.join('<th>'+html.escape(v)+'</th>' for v in diagnostic_headers)
    diagnostic_body = ''.join('<tr>'+''.join('<td>'+html.escape(v)+'</td>' for v in row)+'</tr>' for row in diagnostics)
    document += f'<h2>Load and transport diagnostics</h2><div style="overflow:auto"><table><tr>{diagnostic_head}</tr>{diagnostic_body}</table></div>'
    document += ''.join(f'<a href="{p.replace(".png", ".pdf")}"><img src="{p}" alt="{html.escape(Path(p).stem)}"></a>' for p in images)
    document += '</html>'
    (root / 'report.html').write_text(document, encoding='utf-8')
    print(root / 'report.html')
    print(f'{len(images)} figures exported as PNG and PDF; tables in reliability.csv and report.md')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    generate(parser.parse_args().directory.resolve())
