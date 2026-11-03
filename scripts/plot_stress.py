#!/usr/bin/env python3
"""Plots for the stress harness's output (docs/stress-testing.md).

    python3 scripts/plot_stress.py results/stress/NAME

NAME holds `load`, `fault` and/or `restart` sweeps (each written by target/release/stress);
a single sweep directory works too. PNGs go to NAME/plots/. Needs matplotlib
(pip install matplotlib); everything else is the standard library.
"""
import csv
import gzip
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

COLORS = {"1": "tab:blue", "2": "tab:orange", "3": "tab:green"}


def num(v):
    return float(v) if v not in ("", None) else float("nan")


def rows(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def requests(sweep):
    gz = sweep / "requests.csv.gz"
    opener = (lambda: gzip.open(gz, "rt", newline="")) if gz.exists() else (
        lambda: open(sweep / "requests.csv", newline=""))
    with opener() as f:
        yield from csv.DictReader(f)


def errorbar(ax, x, levels, key, label, **kw):
    y = [num(r[f"{key}_mean"]) for r in levels]
    e = [num(r[f"{key}_ci"]) for r in levels]
    e = [0 if v != v else v for v in e]  # NaN half-width (one run) → no bar
    ax.errorbar(x, y, yerr=e, marker="o", capsize=3, label=label, **kw)


def save(fig, out, name):
    fig.tight_layout()
    fig.savefig(out / name, dpi=130)
    plt.close(fig)
    print(out / name)


def load_plots(sweep, out):
    levels = rows(sweep / "levels.csv")
    runs = rows(sweep / "runs.csv")
    clients = [int(r["clients"]) for r in levels]
    offered = [num(r["offered_rps"]) for r in levels]

    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4))
    errorbar(a, offered, levels, "thr", "measured (mean ± 95 % CI)")
    top = max(offered)
    a.plot([0, top], [0, top], "k--", lw=0.8, label="throughput = offered")
    a.set(xlabel="offered load (req/s)", ylabel="throughput (answers/s)",
          title="Throughput vs offered load")
    a.legend()
    b.plot(clients, [num(r["answered_pct"]) for r in levels], "o-", label="answered %")
    b.plot(clients, [num(r["retried_pct"]) for r in levels], "s-", label="needed a retry %")
    b.set(xlabel="concurrent client processes", ylabel="% of requests",
          title="Answered and retried requests")
    b.legend()
    save(fig, out, "load_throughput.png")

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for key, label in (("p50", "p50"), ("p95", "p95"), ("p99", "p99")):
        errorbar(ax, clients, levels, key, label)
    ax.plot(clients, [num(r["pooled_p999"]) for r in levels], "k:", marker="x",
            label="p99.9 (pooled)")
    ax.set(xlabel="concurrent client processes (10 req/s each)", ylabel="latency (ms)",
           yscale="log", title="Request latency vs load (per-run percentiles, mean ± 95 % CI)")
    ax.legend()
    save(fig, out, "load_latency.png")

    by_level = defaultdict(list)
    for r in requests(sweep):
        if r["latency_ms"]:
            by_level[int(r["clients"])].append(float(r["latency_ms"]))
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for c in sorted(by_level):
        v = sorted(by_level[c])
        ax.plot(v, [(i + 1) / len(v) for i in range(len(v))], label=f"{c} clients")
    ax.set(xscale="log", xlabel="latency (ms)", ylabel="fraction of answered requests",
           title="Latency CDF, all requests of a level pooled")
    ax.legend(fontsize=8)
    save(fig, out, "load_latency_cdf.png")

    fig, (a, b, c) = plt.subplots(1, 3, figsize=(14, 4))
    errorbar(a, clients, levels, "elections", "elections / run")
    a.set(xlabel="client processes", ylabel="elections per run",
          title="Election frequency")
    a.plot(clients, [num(r["electbid_msgs_mean"]) for r in levels], "s--",
           label="ElectBid frames / run")
    a.legend()
    b.plot(clients, [num(r["suppressed_mean"]) for r in levels], "o-")
    b.set(xlabel="client processes", ylabel="per run",
          title="Retry triggers suppressed by min_tenure")
    c.plot(clients, [num(r["queue_max_mean"]) for r in levels], "o-", label="max queued jobs")
    c2 = c.twinx()
    c2.plot(clients, [100 * num(r["cpu_mean"]) for r in levels], "s--", color="tab:red",
            label="CPU % (machine)")
    c.set(xlabel="client processes", ylabel="queued jobs (max over run)",
          title="Queue and CPU")
    c2.set_ylabel("CPU %")
    c.legend(loc="upper left")
    c2.legend(loc="lower right")
    save(fig, out, "load_coordination.png")

    # Per-run scatter: shows the run-to-run spread behind the means.
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for key, m in (("lat_p50", "o"), ("lat_p99", "^")):
        ax.scatter([int(r["clients"]) for r in runs], [num(r[key]) for r in runs],
                   marker=m, alpha=0.6, label=key.replace("lat_", ""))
    ax.set(xscale="log", yscale="log", xlabel="client processes", ylabel="latency (ms)",
           title="Every run's p50 and p99")
    ax.legend()
    save(fig, out, "load_runs_scatter.png")


def fault_plots(sweep, out, prefix):
    """Plots for a sweep with an injected failure; files are named PREFIX_*.png."""
    levels = rows(sweep / "levels.csv")
    runs = rows(sweep / "runs.csv")
    clients = [int(r["clients"]) for r in levels]

    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4))
    errorbar(a, clients, levels, "failover_ms", "failover: injection → first survivor answer")
    errorbar(a, clients, levels, "max_gap_ms", "longest gap without any answer")
    a.set(xlabel="client processes (10 req/s each)", ylabel="ms", title="Failover")
    a.legend(fontsize=8)
    errorbar(b, clients, levels, "recovery_ms", "rejoin: outage end → ready")
    b.plot(clients, [num(r["converge_ms_mean"]) for r in levels], "s--",
           label="convergence after the workload")
    b.set(xlabel="client processes", ylabel="ms", title="Recovery and convergence")
    b.legend(fontsize=8)
    save(fig, out, f"{prefix}_summary.png")

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for key, label in (("p99_before", "before"), ("p99_during", "during"), ("p99_after", "after")):
        ax.plot(clients, [num(r[f"{key}_mean"]) for r in levels], "o-", label=label)
    ax.set(xlabel="client processes", ylabel="p99 latency (ms)", yscale="log",
           title="p99 by window (sent before / during / after the outage)")
    ax.legend()
    save(fig, out, f"{prefix}_windows.png")

    # Timeline of the first rep of every level: each request's latency at its
    # send time, coloured by serving node, and the nodes' queues.
    first = {}
    for r in runs:
        c = int(r["clients"])
        if c not in first or int(r["rep"]) < int(first[c]["rep"]):
            first[c] = r
    picked = {r["run"]: r for r in first.values()}
    points = defaultdict(list)
    for r in requests(sweep):
        if r["run"] in picked:
            points[r["run"]].append(r)
    queues = defaultdict(lambda: defaultdict(list))
    for s in rows(sweep / "samples.csv"):
        if s["run"] in picked and s["reachable"] == "true":
            queues[s["run"]][s["node"]].append((num(s["t_ms"]) / 1e3, num(s["queue_len"])))
    order = sorted(picked.values(), key=lambda r: int(r["clients"]))
    fig, axes = plt.subplots(len(order), 1, figsize=(11, 2.8 * len(order)), squeeze=False)
    for ax, run in zip(axes[:, 0], order):
        pts = points[run["run"]]
        for node, color in COLORS.items():
            sel = [p for p in pts if p["served_by"] == node]
            ax.scatter([num(p["sent_rel_ms"]) / 1e3 for p in sel],
                       [num(p["latency_ms"]) for p in sel], s=3, color=color,
                       label=f"served by node {node}")
        lost = [p for p in pts if p["answered"] != "true"]
        if lost:
            ax.scatter([num(p["sent_rel_ms"]) / 1e3 for p in lost], [1] * len(lost),
                       marker="x", color="red", label="unanswered")
        for key, style, label in (("inject_rel_ms", "r-", "down"),
                                  ("ready_rel_ms", "g-", "ready again")):
            if run[key]:
                ax.axvline(num(run[key]) / 1e3, color=style[0], lw=1, label=label)
        ax.set(yscale="log", ylabel="latency (ms)",
               title=f"{run['clients']} clients, run {run['run']}: victim node {run['victim']}")
        q = ax.twinx()
        for node, series in queues[run["run"]].items():
            q.plot([t for t, _ in series], [v for _, v in series], lw=0.8,
                   color=COLORS.get(node, "grey"), alpha=0.5)
        q.set_ylabel("queued jobs (lines)")
        ax.legend(fontsize=7, loc="upper right", markerscale=3)
    axes[-1, 0].set_xlabel("time since the run started (s)")
    save(fig, out, f"{prefix}_timeline.png")


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    root = Path(sys.argv[1])
    out = root / "plots"
    out.mkdir(exist_ok=True)
    sweeps = [root / d for d in ("load", "fault", "restart") if (root / d / "runs.csv").exists()]
    if not sweeps and (root / "runs.csv").exists():
        sweeps = [root]
    if not sweeps:
        sys.exit(f"no runs.csv under {root}")
    for s in sweeps:
        has_fault = "failover_ms_mean" in rows(s / "levels.csv")[0]
        if has_fault:
            fault_plots(s, out, "fault" if s == root else s.name)
        else:
            load_plots(s, out)


if __name__ == "__main__":
    main()
