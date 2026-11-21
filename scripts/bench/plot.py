#!/usr/bin/env python3
"""Plot the benchmark runs of run_benchmarks.sh for the report.

    python3 scripts/bench/plot.py results/<run>/results/runs.jsonl --out results/<run>/plots
    python3 scripts/bench/plot.py --client results/<run>/client.jsonl --out results/<run>/plots

From the job runs it writes three PNG figures, one column per job (needs
matplotlib):

* strong-scaling.png: weak-W on 1 .. W workers, the median run time (the
  bar goes from the fastest to the slowest run) and the speedup T(1) / T(n);
* weak-scaling.png: weak-n on n workers, the median run time and the
  efficiency T(weak-1 on 1) / T(weak-n on n);
* go-vs-pyspark.png: every configuration, the runs of both clients as the
  difference from the median of Go.

From the client benchmark (run_clientbench.sh) it writes client-overhead.png:
the median time of starting and connecting, of SELECT 1 on an open session,
and of bringing results of growing size back as rows and as Arrow.

Go is blue and PySpark orange in all of them. summarize.py and
summarize_client.py print the same numbers as tables.
"""

import argparse
import math
import os
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import FixedLocator, FuncFormatter, MaxNLocator, NullLocator  # noqa: E402

import summarize_client  # noqa: E402
from summarize import CLIENTS, JOBS, TITLES, load  # noqa: E402

COLORS = {"go": "#2a78d6", "pyspark": "#eb6834"}
MARKERS = {"go": "o", "pyspark": "D"}
SIZES = {"go": 6, "pyspark": 5}
INK, SECONDARY, MUTED, GRID, AXIS = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
DODGE = 0.05  # the clients are drawn this far left and right of each point

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Segoe UI", "Arial", "DejaVu Sans"],
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.titleweight": "semibold",
    "axes.titlelocation": "left",
    "axes.titlecolor": INK,
    "axes.labelcolor": SECONDARY,
    "axes.edgecolor": AXIS,
    "axes.linewidth": 0.8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "axes.grid.axis": "y",
    "axes.axisbelow": True,
    "grid.color": GRID,
    "grid.linewidth": 0.6,
    "xtick.color": AXIS,
    "ytick.color": AXIS,
    "xtick.labelcolor": SECONDARY,
    "ytick.labelcolor": SECONDARY,
    "legend.frameon": False,
    "figure.facecolor": "white",
    "savefig.dpi": 200,
})


def clients(runs):
    return [c for c in CLIENTS if any(k[1] == c for k in runs)]


def offset(c, cs):
    """Where client c is drawn relative to the point, so both stay visible."""
    return 0 if len(cs) == 1 else (-DODGE if c == cs[0] else DODGE)


def times(ax, runs, job, points):
    """Median run time with the range of the runs; points is [(input, workers)]."""
    cs = clients(runs)
    for c in cs:
        xs, med, below, above = [], [], [], []
        for name, w in points:
            t = runs.get((job, c, name, w))
            if t:
                m = statistics.median(t)
                xs.append(w + offset(c, cs))
                med.append(m)
                below.append(m - min(t))
                above.append(max(t) - m)
        ax.errorbar(xs, med, yerr=[below, above], color=COLORS[c], lw=2, elinewidth=1.2, capsize=0,
                    marker=MARKERS[c], ms=SIZES[c], mec="white", mew=1.2, label=TITLES[c], zorder=3)
    ax.set_ylim(0, None)
    ax.set_ylabel("run time (s)")


def ratios(ax, runs, job, points, value, ideal, label):
    """value(t1, t, w) per client, with the ideal line."""
    workers = [w for _, w in points]
    ax.plot(workers, [ideal(w) for w in workers], color=MUTED, lw=1, zorder=2)
    ax.annotate("ideal", (workers[-1], ideal(workers[-1])), xytext=(4, 0), textcoords="offset points",
                color=MUTED, va="center", fontsize=8)
    cs = clients(runs)
    for c in cs:
        xs, ys = [], []
        base = runs.get((job, c) + points[0])
        for name, w in points:
            t = runs.get((job, c, name, w))
            if t and base:
                xs.append(w + offset(c, cs))
                ys.append(value(statistics.median(base), statistics.median(t), w))
        ax.plot(xs, ys, color=COLORS[c], lw=2, marker=MARKERS[c], ms=SIZES[c], mec="white", mew=1.2,
                label=TITLES[c], zorder=3)
    ax.set_ylabel(label)


def workers_axis(ax, points, label):
    workers = [w for _, w in points]
    ax.set_xticks(workers)
    ax.set_xlim(workers[0] - 0.3, workers[-1] + 0.45)
    ax.set_xlabel(label)


def legend(fig, cs, lines=True):
    """One legend for the figure, at its top left."""
    handles = [Line2D([], [], color=COLORS[c], lw=2 if lines else 0, marker=MARKERS[c], ms=SIZES[c],
                      mec="white", mew=1.2) for c in cs]
    fig.legend(handles, [TITLES[c] for c in cs], loc="upper left", ncol=len(cs), bbox_to_anchor=(0.01, 1.0),
               labelcolor=SECONDARY, handlelength=2.2 if lines else 1)


def scaling_figure(runs, jobs, points, top, bottom, xlabel, path):
    fig, axes = plt.subplots(2, len(jobs), figsize=(3.4 * len(jobs), 5.6), squeeze=False)
    for col, job in enumerate(jobs):
        times(axes[0][col], runs, job, points[job])
        axes[0][col].set_title(TITLES[job])
        ratios(axes[1][col], runs, job, points[job], *top)
        axes[1][col].set_ylim(*bottom)
        for row in axes:
            workers_axis(row[col], points[job], xlabel)
    legend(fig, clients(runs))
    fig.tight_layout(rect=(0, 0, 1, 0.95), h_pad=2)
    fig.savefig(path)
    plt.close(fig)


def comparison_figure(runs, jobs, path):
    cs = clients(runs)
    rows = sorted({(k[2], k[3]) for k in runs})  # top to bottom, as in the tables of summarize.py
    fig, axes = plt.subplots(1, len(jobs), figsize=(3.4 * len(jobs), 0.45 * len(rows) + 1.3), sharex=True,
                             sharey=True, squeeze=False)
    for col, job in enumerate(jobs):
        ax = axes[0][col]
        ax.grid(False)
        ax.grid(True, axis="x")
        ax.axvline(0, color=AXIS, lw=0.8, zorder=1)
        for y, (name, w) in enumerate(rows):
            go = runs.get((job, "go", name, w))
            for c in cs:
                t = runs.get((job, c, name, w))
                if not (t and go):
                    continue
                pct = [100 * (x / statistics.median(go) - 1) for x in t]
                m = statistics.median(pct)
                ax.errorbar([m], [y + 3 * offset(c, cs)], xerr=[[m - min(pct)], [max(pct) - m]], color=COLORS[c],
                            elinewidth=1.2, capsize=0, marker=MARKERS[c], ms=SIZES[c], mec="white", mew=1.2,
                            ls="none", zorder=3)
        ax.tick_params(axis="y", length=0)
        ax.spines["left"].set_visible(False)
        ax.set_title(TITLES[job])
        ax.set_xlabel("difference from the Go median")
        ax.xaxis.set_major_locator(MaxNLocator(nbins=6, steps=[1, 2, 5, 10]))
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:+.0f}%" if v else "0"))
    ax = axes[0][0]
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([f"{name}, {w} worker{'s' if w > 1 else ''}" for name, w in rows])
    ax.set_ylim(len(rows) - 0.4, -0.6)
    legend(fig, cs, lines=False)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.35 / fig.get_figheight()))
    fig.savefig(path)
    plt.close(fig)


def duration(ms):
    return f"{ms / 1000:.2f} s" if ms >= 1000 else f"{ms:.0f} ms" if ms >= 10 else f"{ms:.1f} ms"


def compact(n):
    for size, suffix in ((10**9, "G"), (10**6, "M"), (10**3, "K")):
        if n >= size and n % size == 0:
            return f"{n // size}{suffix}"
    return f"{n:,}"


def median_bars(ax, samples, cs, cases, title):
    """Horizontal bars of the median of every case, one per client, the first case on top."""
    ax.grid(False)
    ax.grid(True, axis="x")
    height, top = 0.34, 0
    for y, (case, _) in enumerate(cases):
        for c in cs:
            v = samples.get((case, c, 1))
            if not v:
                continue
            m = statistics.median(v)
            top = max(top, m)
            yy = y + (0 if len(cs) == 1 else (-1 if c == cs[0] else 1) * (height / 2 + 0.02))
            ax.barh(yy, m, height=height, color=COLORS[c], zorder=3)
            ax.annotate(duration(m), (m, yy), xytext=(4, 0), textcoords="offset points", va="center",
                        color=SECONDARY, fontsize=8)
    ax.set_yticks(range(len(cases)))
    ax.set_yticklabels([label for _, label in cases])
    # room for at least two cases, so that the bars are as thick in every panel
    center, half = (len(cases) - 1) / 2, max(len(cases), 2) / 2 + 0.05
    ax.set_ylim(center + half, center - half)
    ax.set_xlim(0, top * 1.3)
    ax.tick_params(axis="y", length=0)
    ax.set_xlabel("median time (ms)")
    ax.set_title(title)


def collect_lines(ax, samples, cs, case, title):
    """Median time against the size of the result, both on log scales."""
    sizes = sorted({k[2] for k in samples if k[0] == case})
    for c in cs:
        xs = [n for n in sizes if samples.get((case, c, n))]
        ax.plot(xs, [statistics.median(samples[(case, c, n)]) for n in xs], color=COLORS[c], lw=2,
                marker=MARKERS[c], ms=SIZES[c], mec="white", mew=1.2, zorder=3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.xaxis.set_major_locator(FixedLocator(sizes))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: compact(int(round(v)))))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}" if v >= 1 else f"{v:g}"))
    ax.set_xlim(sizes[0] / 2, sizes[-1] * 2)
    ax.set_xlabel("rows in the result")
    ax.set_ylabel("median time (ms)")
    ax.set_title(title)


def client_figure(samples, path):
    cs = [c for c in CLIENTS if any(k[1] == c for k in samples)]
    fig, ((start, latency), (rows, arrow)) = plt.subplots(2, 2, figsize=(6.8, 5.8))
    median_bars(start, samples, cs, [("process", "new process\nto first result"),
                                     ("connect", "connect and\nfirst query")], "Start-up")
    median_bars(latency, samples, cs, [("latency", "SELECT 1")], "Query on an open session")
    collect_lines(rows, samples, cs, "rows", "Result as rows")
    collect_lines(arrow, samples, cs, "arrow", "Result as Arrow")
    arrow.sharey(rows)
    medians = [statistics.median(v) for k, v in samples.items() if k[0] in ("rows", "arrow")]
    if medians:  # from the decade below the fastest, so that it has a labelled gridline
        rows.set_ylim(10 ** math.floor(math.log10(min(medians))), max(medians) * 1.5)
    legend(fig, cs)
    fig.tight_layout(rect=(0, 0, 1, 0.95), h_pad=2, w_pad=2)
    fig.savefig(path)
    plt.close(fig)


def job_figures(metrics, out):
    runs = load(metrics)
    if not runs:
        sys.exit(f"{metrics}: no runs with a workers=<n> label")
    jobs = [j for j in JOBS if any(k[0] == j for k in runs)]
    written = []

    strong, weak = {}, {}
    for job in jobs:
        configs = {(k[2], k[3]) for k in runs if k[0] == job}
        top = max(w for _, w in configs)
        strong[job] = sorted((name, w) for name, w in configs if name == f"weak-{top}")
        weak[job] = sorted(((name, w) for name, w in configs if name == f"weak-{w}"), key=lambda p: p[1])
    if all(len(strong[j]) > 1 for j in jobs):
        path = os.path.join(out, "strong-scaling.png")
        top = strong[jobs[0]][0][0]
        scaling_figure(runs, jobs, strong, (lambda t1, t, w: t1 / t, lambda w: w, "speedup T(1) / T(n)"),
                       (0, None), f"workers (input {top})", path)
        written.append(path)
    if all(len(weak[j]) > 1 for j in jobs):
        path = os.path.join(out, "weak-scaling.png")
        scaling_figure(runs, jobs, weak, (lambda t1, t, w: t1 / t, lambda w: 1, "efficiency T(1) / T(n)"),
                       (0, 1.15), "workers n (input weak-n)", path)
        written.append(path)
    if len(clients(runs)) == 2:
        path = os.path.join(out, "go-vs-pyspark.png")
        comparison_figure(runs, jobs, path)
        written.append(path)
    return written


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("metrics", nargs="?", help="JSON-lines file of the job runs, written with -metrics")
    p.add_argument("--client", help="JSON-lines file of run_clientbench.sh")
    p.add_argument("--out", default=".", help="directory for the figures (default: current)")
    args = p.parse_args()
    if not (args.metrics or args.client):
        p.error("give the job runs, --client or both")
    os.makedirs(args.out, exist_ok=True)
    written = []
    if args.client:
        samples = summarize_client.load(args.client)
        if not samples:
            sys.exit(f"{args.client}: no client benchmark lines")
        path = os.path.join(args.out, "client-overhead.png")
        client_figure(samples, path)
        written.append(path)
    if args.metrics:
        written += job_figures(args.metrics, args.out)
    print("\n".join(written) if written else "nothing to plot")


if __name__ == "__main__":
    main()
