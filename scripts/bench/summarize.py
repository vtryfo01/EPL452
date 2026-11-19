#!/usr/bin/env python3
"""Summarize benchmark runs: the -metrics JSON lines of run_benchmarks.sh.

    python3 scripts/bench/summarize.py ~/results/runs.jsonl [--csv summary.csv]

For every job it prints Markdown tables of the median time in seconds:

* all runs: every input and number of workers, each client, PySpark / Go;
* strong scaling: weak-W on 1 .. W workers, speedup T(1) / T(n) and
  efficiency T(1) / (n T(n));
* weak scaling: weak-n on n workers, efficiency T(weak-1 on 1) / T(weak-n on n).

Only runs whose label has "workers=<n>" are used. --csv also writes one row
per (job, client, input, workers) with the number of runs, median, mean,
standard deviation, minimum and maximum, for plotting.
"""

import argparse
import collections
import csv
import json
import statistics
import sys

CLIENTS = ("go", "pyspark")
JOBS = ("wordcount", "invertedindex")
TITLES = {"wordcount": "WordCount", "invertedindex": "Inverted Index", "go": "Go", "pyspark": "PySpark"}


def load(path):
    """Returns {(job, client, input, workers): [seconds, ...]}."""
    runs = collections.defaultdict(list)
    with open(path, encoding="utf-8") as f:
        for number, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                m = json.loads(line)
            except json.JSONDecodeError as e:
                sys.exit(f"{path}:{number}: {e}")
            label = dict(kv.split("=", 1) for kv in m.get("label", "").split() if "=" in kv)
            if "workers" not in label:
                continue
            name = m["input"].rstrip("/").rsplit("/", 1)[-1]
            runs[(m["job"], m.get("client", "go"), name, int(label["workers"]))].append(m["elapsed_ms"] / 1000)
    return runs


def table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def fmt(x, digits=1):
    return "" if x is None else f"{x:.{digits}f}"


def summarize(runs):
    med = {key: statistics.median(times) for key, times in runs.items()}
    count = {key: len(times) for key, times in runs.items()}
    clients = [c for c in CLIENTS if any(k[1] == c for k in runs)]
    out = []
    for job in JOBS:
        configs = sorted({(k[2], k[3]) for k in runs if k[0] == job}, key=lambda c: (c[0], c[1]))
        if not configs:
            continue
        out.append(f"## {TITLES[job]}\n")

        header = ["input", "workers"] + [f"{TITLES[c]} (s, runs)" for c in clients]
        if len(clients) == 2:
            header.append("PySpark / Go")
        rows = []
        for name, w in configs:
            row = [name, str(w)]
            for c in clients:
                key = (job, c, name, w)
                row.append(f"{fmt(med[key])} ({count[key]})" if key in med else "")
            go, py = med.get((job, "go", name, w)), med.get((job, "pyspark", name, w))
            if len(clients) == 2:
                row.append(fmt(py / go, 2) if go and py else "")
            rows.append(row)
        out.append("All runs (median):\n\n" + table(header, rows) + "\n")

        top = max(w for _, w in configs)
        strong = f"weak-{top}"
        workers = sorted(w for name, w in configs if name == strong)
        if len(workers) > 1:
            header, rows = ["workers"], []
            for c in clients:
                header += [f"{TITLES[c]} (s)", "speedup", "efficiency"]
            for w in workers:
                row = [str(w)]
                for c in clients:
                    t, t1 = med.get((job, c, strong, w)), med.get((job, c, strong, workers[0]))
                    speedup = t1 / t * workers[0] if t and t1 else None
                    row += [fmt(t), fmt(speedup, 2), fmt(speedup / w if speedup else None, 2)]
                rows.append(row)
            out.append(f"Strong scaling ({strong}):\n\n" + table(header, rows) + "\n")

        weak = sorted(w for name, w in configs if name == f"weak-{w}")
        if len(weak) > 1:
            header, rows = ["workers", "input"], []
            for c in clients:
                header += [f"{TITLES[c]} (s)", "efficiency"]
            for w in weak:
                row = [str(w), f"weak-{w}"]
                for c in clients:
                    t, t1 = med.get((job, c, f"weak-{w}", w)), med.get((job, c, f"weak-{weak[0]}", weak[0]))
                    row += [fmt(t), fmt(t1 / t if t and t1 else None, 2)]
                rows.append(row)
            out.append("Weak scaling (weak-n on n workers):\n\n" + table(header, rows) + "\n")
    return "\n".join(out)


def write_csv(runs, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["job", "client", "input", "workers", "runs", "median_s", "mean_s", "stdev_s", "min_s", "max_s"])
        for (job, client, name, workers), times in sorted(runs.items()):
            stdev = statistics.stdev(times) if len(times) > 1 else 0.0
            w.writerow([job, client, name, workers, len(times), f"{statistics.median(times):.3f}",
                        f"{statistics.mean(times):.3f}", f"{stdev:.3f}", f"{min(times):.3f}", f"{max(times):.3f}"])


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("metrics", help="JSON-lines file written with -metrics")
    p.add_argument("--csv", help="also write the per-configuration statistics to this CSV file")
    args = p.parse_args()
    runs = load(args.metrics)
    if not runs:
        sys.exit(f"{args.metrics}: no runs with a workers=<n> label")
    print(summarize(runs))
    if args.csv:
        write_csv(runs, args.csv)


if __name__ == "__main__":
    main()
