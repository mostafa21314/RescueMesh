#!/usr/bin/env python3
"""Turns experiments/recovery/udp/*.csv (from recovery_bench.py) into tables and plots.

    python3 scripts/recovery_report.py          # results from this machine
    python3 scripts/recovery_report.py --lab    # results from the lab PCs

Writes experiments/recovery/udp/summary.md (mean ± 95 % CI, median and max over the
repetitions of each cell) and docs/img/recovery/udp/*.png. Historical TCP
figures embedded in docs/recovery.md are left unchanged.
"""

import argparse
import csv
import math
import statistics as st
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "experiments" / "recovery" / "udp"
IMG = ROOT / "docs" / "img" / "recovery" / "udp"

# Categorical slots in fixed order (validated: CVD-separable; the two lighter
# hues are below 3:1 contrast, so every series also has its own marker and a
# direct label, and every figure has a table in summary.md).
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
MARKERS = ["o", "s", "^", "D"]
VARIANTS = [("full", "locked"), ("full", "chunked"), ("delta", "locked"), ("delta", "chunked")]
INK, MUTED, GRID, SURFACE = "#1f1f1e", "#6b6a64", "#e6e5e0", "#fcfcfb"

# Two-sided 95 % t quantiles by degrees of freedom.
T95 = {1: 12.71, 2: 4.30, 3: 3.18, 4: 2.78, 5: 2.57, 6: 2.45, 7: 2.36, 8: 2.31, 9: 2.26}


def load(name):
    path = DATA / f"{name}.csv"
    if not path.exists():
        return []
    rows = list(csv.DictReader(open(path)))
    for r in rows:
        for k, v in r.items():
            try:
                r[k] = float(v) if v not in ("", None) and k != "raw" else v
            except ValueError:
                pass
    return rows


def stats(xs):
    xs = [x for x in xs if isinstance(x, float)]
    if not xs:
        return None
    m = st.mean(xs)
    ci = T95.get(len(xs) - 1, 1.96) * st.stdev(xs) / math.sqrt(len(xs)) if len(xs) > 1 else 0.0
    return {"n": len(xs), "mean": m, "ci": ci, "median": st.median(xs), "max": max(xs)}


def fmt(s, scale=1.0, digits=1):
    if not s:
        return "–"
    return f"{s['mean'] * scale:.{digits}f} ± {s['ci'] * scale:.{digits}f}"


def group(rows, keys):
    g = defaultdict(list)
    for r in rows:
        g[tuple(r[k] for k in keys)].append(r)
    return g


def style(ax, title, xlabel, ylabel):
    ax.set_title(title, loc="left", fontsize=11, color=INK, pad=10)
    ax.set_xlabel(xlabel, color=MUTED, fontsize=9)
    ax.set_ylabel(ylabel, color=MUTED, fontsize=9)
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=8)


def line(ax, xs, ss, i, label, scale=1.0):
    pts = [(x, s) for x, s in zip(xs, ss) if s]
    if not pts:
        return
    x = [p[0] for p in pts]
    y = [p[1]["mean"] * scale for p in pts]
    e = [p[1]["ci"] * scale for p in pts]
    ax.errorbar(x, y, yerr=e, color=COLORS[i], marker=MARKERS[i], markersize=6,
                linewidth=2, capsize=3, label=label,
                markeredgecolor=SURFACE, markeredgewidth=1.5)
    # Direct label at the line's end, nudged down past labels already placed
    # there (two variants often end at nearly the same value).
    ax.figure.canvas.draw()
    py = ax.transData.transform((x[-1], y[-1]))[1]
    placed = ax.__dict__.setdefault("_end_labels", [])
    dy = 0
    while any(abs(py + dy - q) < 11 for q in placed):
        dy -= 11
    placed.append(py + dy)
    ax.annotate(label, (x[-1], y[-1]), xytext=(6, dy), textcoords="offset points",
                va="center", fontsize=8, color=INK)


def save(fig, name):
    IMG.mkdir(parents=True, exist_ok=True)
    fig.patch.set_facecolor(SURFACE)
    fig.tight_layout()
    fig.savefig(IMG / name, dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------- experiments

def transfer(out):
    rows = load("transfer")
    if not rows:
        return
    g = group(rows, ["transfer", "snapshot", "table"])
    tables = sorted({r["table"] for r in rows})
    out.append("## Transfer and snapshot build (R2, R3)\n")
    out.append(f"Mute of the elected server for 10 s at 100 req/s, complexity 5; the table "
               f"is filled first. {len(rows)} runs. Mean ± 95 % CI over the repetitions.\n")
    out.append("| transfer | snapshot | table | n | recovery ms | fetch ms | install ms | "
               "records received | MB received | survivor max lock hold ms | client p99 during recovery ms | tables agree |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for tr, sn in VARIANTS:
        for t in tables:
            rs = g.get((tr, sn, t), [])
            if not rs:
                continue
            col = lambda k: stats([r[k] for r in rs])
            agree = sum(1 for r in rs if r["tables_agree"] == "True")
            out.append(
                f"| {tr} | {sn} | {int(t):,} | {len(rs)} | {fmt(col('recovery_total_ms'))} | "
                f"{fmt(col('recovery_fetch_ms'))} | {fmt(col('recovery_install_ms'))} | "
                f"{fmt(col('records_received'), digits=0)} | {fmt(col('bytes_received'), 1e-6)} | "
                f"{fmt(col('survivor_lock_max_us'), 1e-3, 2)} | {fmt(col('lat_recovery_p99'))} | {agree}/{len(rs)} |")
    out.append("")

    for metric, scale, ylabel, fname, title in [
        ("recovery_total_ms", 1, "ms (log scale)", "transfer_recovery_time.png",
         "Recovery time (end of outage → Ready) vs table size"),
        ("bytes_received", 1e-6, "MB (log scale)", "transfer_bytes.png",
         "Snapshot data received by the returning node"),
        ("survivor_lock_max_us", 1e-3, "ms (log scale)", "transfer_lock_hold.png",
         "Longest survivor state-lock hold while serving a snapshot"),
    ]:
        fig, ax = plt.subplots(figsize=(7, 4))
        for i, (tr, sn) in enumerate(VARIANTS):
            line(ax, tables, [stats([r[metric] for r in g.get((tr, sn, t), [])]) for t in tables],
                 i, f"{tr} + {sn}", scale)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xticks(tables)
        ax.set_xticklabels([f"{int(t):,}" for t in tables])
        ax.legend(frameon=False, fontsize=8, loc="upper left")
        style(ax, title, "requests in the table before the outage (log scale)", ylabel)
        ax.set_xlim(tables[0] * 0.7, tables[-1] * 2.2)
        save(fig, fname)


def load_exp(out):
    rows = load("load")
    if not rows:
        return
    g = group(rows, ["transfer", "snapshot", "rate", "outage_ms"])
    rates = sorted({r["rate"] for r in rows})
    outages = sorted({r["outage_ms"] for r in rows})
    variants = [v for v in VARIANTS if any(k[:2] == v for k in g)]
    out.append("## Recovery and failover vs load and outage length\n")
    out.append(f"Mute of the elected server; 2,000-request table; complexity 5. {len(rows)} runs.\n")
    out.append("| transfer | snapshot | req/s | outage s | n | failover ms | recovery ms | records received | "
               "new results | client p99 during outage ms | max attempts | failed | tables agree |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for tr, sn in variants:
        for rate in rates:
            for o in outages:
                rs = g.get((tr, sn, rate, o), [])
                if not rs:
                    continue
                col = lambda k: stats([r[k] for r in rs])
                agree = sum(1 for r in rs if r["tables_agree"] == "True")
                out.append(
                    f"| {tr} | {sn} | {int(rate)} | {int(o / 1000)} | {len(rs)} | {fmt(col('failover_ms'), digits=0)} | "
                    f"{fmt(col('recovery_total_ms'))} | {fmt(col('records_received'), digits=0)} | "
                    f"{fmt(col('new_results'), digits=0)} | {fmt(col('lat_outage_p99'), digits=0)} | "
                    f"{int(max(r['max_attempts_used'] for r in rs))} | {int(sum(r['failed'] for r in rs))} | {agree}/{len(rs)} |")
    out.append("")

    for metric, ylabel, fname, title in [
        ("recovery_total_ms", "ms", "load_recovery_time.png", "Recovery time vs request rate"),
        ("records_received", "records", "load_records.png", "Records transferred vs request rate"),
    ]:
        fig, axes = plt.subplots(1, len(outages), figsize=(10, 3.6), sharey=True)
        for ax, o in zip(axes, outages):
            for i, v in enumerate(VARIANTS):
                if v not in variants:
                    continue
                line(ax, rates, [stats([r[metric] for r in g.get((*v, rate, o), [])]) for rate in rates],
                     i, f"{v[0]} + {v[1]}")
            style(ax, f"{int(o / 1000)} s outage", "requests / s", ylabel if ax is axes[0] else "")
            ax.set_xticks(rates)
            ax.set_xlim(rates[0] - 20, rates[-1] + 90)
        axes[0].legend(frameon=False, fontsize=8, loc="upper left")
        fig.suptitle(title, x=0.01, ha="left", fontsize=11, color=INK)
        save(fig, fname)

    # Failover does not depend on the recovery variant: pool them.
    fig, ax = plt.subplots(figsize=(7, 4))
    for i, o in enumerate(outages):
        ss = [stats([r["failover_ms"] for r in rows if r["rate"] == rate and r["outage_ms"] == o])
              for rate in rates]
        line(ax, rates, ss, i, f"{int(o / 1000)} s outage")
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    style(ax, "Failover: fault → first answer from a survivor", "requests / s", "ms")
    ax.set_xticks(rates)
    ax.set_xlim(rates[0] - 20, rates[-1] + 60)
    save(fig, "load_failover.png")


def ready_rule(out):
    rows = load("ready_rule")
    if not rows:
        return
    out.append("## Ready rule with a second failure during recovery (R4)\n")
    out.append("The elected server is muted for 3 s; 0.5 s later one survivor is killed. "
               "The returning node has 15 s to become Ready.\n")
    out.append("| ready rule | n | recovered | recovery ms (recovered runs) | snapshots used | rounds | failed requests | tables agree (live nodes) |")
    out.append("|---|---|---|---|---|---|---|---|")
    for rule, rs in sorted(group(rows, ["ready_rule"]).items()):
        ok = [r for r in rs if r["recovered"] == "True"]
        col = lambda k: stats([r[k] for r in ok])
        agree = sum(1 for r in rs if r["tables_agree"] == "True")
        out.append(f"| {rule[0]} | {len(rs)} | {len(ok)}/{len(rs)} | {fmt(col('recovery_total_ms'))} | "
                   f"{fmt(col('snapshots_used'))} | {fmt(col('recovery_rounds'))} | "
                   f"{int(sum(r['failed'] for r in rs))} | {agree}/{len(rs)} |")
    out.append("")


def restart(out):
    rows = load("restart")
    if not rows:
        return
    out.append("## Process crash and restart (R5)\n")
    out.append("The elected server's process is killed (memory lost) and restarted 5 s later; "
               "100 req/s, 2,000-request table.\n")
    out.append("| on_start | n | recovery ms | results on restarted node | results on survivors | "
               "requests served by restarted node | replicate conflicts | elections after fault | tables agree |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    for key, rs in sorted(group(rows, ["on_start"]).items()):
        col = lambda k: stats([r[k] for r in rs])
        agree = sum(1 for r in rs if r["tables_agree"] == "True")
        out.append(f"| {key[0]} | {len(rs)} | {fmt(col('recovery_total_ms'))} | {fmt(col('victim_results'), digits=0)} | "
                   f"{fmt(col('survivor_results'), digits=0)} | {fmt(col('victim_served_after_back'), digits=0)} | "
                   f"{fmt(col('replicate_conflicts'), digits=0)} | {fmt(col('elections_after_fault'))} | {agree}/{len(rs)} |")
    out.append("")


def main():
    global DATA, IMG
    ap = argparse.ArgumentParser()
    ap.add_argument("--lab", action="store_true", help="read experiments/recovery/udp/lab/")
    args = ap.parse_args()
    if args.lab:
        DATA, IMG = DATA / "lab", IMG / "lab"
        setup = ("Setup: six lab PCs (three cloud nodes, one per PC; the workload split over "
                 "three client PCs), driven by scripts/lab_agent.py")
    else:
        setup = "Setup: three cloud-node processes and clients on the experiment host; see run metadata"
    out = ["# Recovery experiments: summary",
           "",
           "Generated by `scripts/recovery_report.py` from the CSVs in this folder. "
           f"{setup}, release build, 3 planner workers per node, request timeout 1 s. "
           "Cells show mean ± 95 % confidence interval (t distribution) over the repetitions.",
           ""]
    transfer(out)
    load_exp(out)
    ready_rule(out)
    restart(out)
    (DATA / "summary.md").write_text("\n".join(out) + "\n")
    print(f"wrote {DATA / 'summary.md'} and {IMG}/")


if __name__ == "__main__":
    main()
