#!/usr/bin/env python3
"""Summarize the client benchmark: the JSON lines of run_clientbench.sh.

    python3 scripts/bench/summarize_client.py ~/results/client.jsonl [--csv client-summary.csv]

It prints one Markdown table with, for every case and result size, the
median and the 95th percentile in milliseconds of each client and the ratio
of the medians, PySpark / Go. The cases are:

* process: a new process that connects and runs SELECT 1, timed from outside
  (includes starting the program; for PySpark, Python and the imports);
* connect: inside the program, creating the session and the first SELECT 1;
* latency: SELECT 1 on an open session;
* rows, arrow: bringing a result of n rows back as rows or as an Arrow table.

Only lines with "bench": "client" are used. --csv also writes one row per
(case, client, rows) with the number of samples, median, 95th percentile,
mean, minimum and maximum, for plotting.
"""

import argparse
import collections
import csv
import json
import math
import statistics
import sys

CASES = ("process", "connect", "latency", "rows", "arrow")
CLIENTS = ("go", "pyspark")
TITLES = {"go": "Go", "pyspark": "PySpark"}


def load(path):
    """Returns {(case, client, rows): [ms, ...]}."""
    samples = collections.defaultdict(list)
    with open(path, encoding="utf-8") as f:
        for number, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                m = json.loads(line)
            except json.JSONDecodeError as e:
                sys.exit(f"{path}:{number}: {e}")
            if m.get("bench") == "client":
                samples[(m["case"], m["client"], int(m["rows"]))].append(float(m["ms"]))
    return samples


def percentile(values, p):
    """Nearest-rank percentile."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p / 100 * len(ordered)) - 1)]


def ms(x):
    return f"{x:.2f}" if x < 10 else f"{x:.1f}" if x < 1000 else f"{x:.0f}"


def keys(samples):
    """(case, rows) in the order of CASES, then by size."""
    order = {c: i for i, c in enumerate(CASES)}
    return sorted({(k[0], k[2]) for k in samples}, key=lambda k: (order.get(k[0], len(order)), k[0], k[1]))


def summarize(samples):
    clients = [c for c in CLIENTS if any(k[1] == c for k in samples)]
    header = ["case", "rows"]
    for c in clients:
        header += [f"{TITLES[c]} median (ms)", "p95", "n"]
    if len(clients) == 2:
        header.append("PySpark / Go")
    lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
    for case, rows in keys(samples):
        row, med = [case, f"{rows:,}"], {}
        for c in clients:
            v = samples.get((case, c, rows))
            if v:
                med[c] = statistics.median(v)
                row += [ms(med[c]), ms(percentile(v, 95)), str(len(v))]
            else:
                row += ["", "", ""]
        if len(clients) == 2:
            row.append(f"{med['pyspark'] / med['go']:.2f}" if len(med) == 2 and med["go"] else "")
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def write_csv(samples, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["case", "client", "rows", "samples", "median_ms", "p95_ms", "mean_ms", "min_ms", "max_ms"])
        order = {c: i for i, c in enumerate(CASES)}
        for (case, client, rows), v in sorted(samples.items(), key=lambda kv: (order.get(kv[0][0], 99),) + kv[0]):
            w.writerow([case, client, rows, len(v), f"{statistics.median(v):.3f}", f"{percentile(v, 95):.3f}",
                        f"{statistics.mean(v):.3f}", f"{min(v):.3f}", f"{max(v):.3f}"])


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("metrics", help="JSON-lines file written by run_clientbench.sh")
    p.add_argument("--csv", help="also write the per-case statistics to this CSV file")
    args = p.parse_args()
    samples = load(args.metrics)
    if not samples:
        sys.exit(f"{args.metrics}: no client benchmark lines")
    print(summarize(samples))
    if args.csv:
        write_csv(samples, args.csv)


if __name__ == "__main__":
    main()
