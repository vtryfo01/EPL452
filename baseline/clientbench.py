#!/usr/bin/env python3
"""Measure the work of the Spark Connect client (PySpark baseline).

    clientbench.py -remote sc://localhost:15002 -metrics client.jsonl -label rep=1

The same cases, queries, flags and JSON lines as cmd/clientbench, with
"client": "pyspark": connect (create the session and run SELECT 1), latency
(SELECT 1), rows (collect a WordCount-shaped result of n rows) and arrow (the
same result with toArrow).
"""

import argparse
import json
import os
import sys
import time

from pyspark.sql import SparkSession

DEFAULT_REMOTE = "sc://localhost:15002"
CASES = ("connect", "latency", "rows", "arrow")
WARMUPS = 3


def select_one(spark):
    rows = spark.sql("SELECT 1").collect()
    if len(rows) != 1:
        raise SystemExit(f"clientbench: SELECT 1 returned {len(rows)} rows")


def fetch(spark, how, n):
    """Brings the n-row result back as rows or as an Arrow table."""
    df = spark.sql(f"SELECT concat('term', id) AS term, id AS count FROM range({n})")
    got = len(df.collect()) if how == "rows" else df.toArrow().num_rows
    if got != n:
        raise SystemExit(f"clientbench: {how}: expected {n} rows, got {got}")


def parse_args(argv):
    p = argparse.ArgumentParser(prog="clientbench.py", allow_abbrev=False,
                                description="Measure the Spark Connect client (PySpark baseline).")
    p.add_argument("-remote", default=os.environ.get("SPARK_REMOTE") or DEFAULT_REMOTE,
                   help="Spark Connect URL (default $SPARK_REMOTE)")
    p.add_argument("-cases", default=",".join(CASES), help="comma-separated cases to measure")
    p.add_argument("-queries", type=int, default=200, help="timed SELECT 1 queries of latency")
    p.add_argument("-sizes", default="1000,10000,100000,1000000",
                   help="comma-separated result sizes (rows) of rows and arrow")
    p.add_argument("-samples", type=int, default=5, help="timed collects of every size")
    p.add_argument("-metrics", default="", help="append the JSON lines to this file instead of printing them")
    p.add_argument("-label", default="", help='free-form label stored with every measurement, e.g. "rep=1"')
    args = p.parse_args(argv)
    args.cases = args.cases.split(",")
    for c in args.cases:
        if c not in CASES:
            p.error(f"unknown case {c!r} ({', '.join(CASES)})")
    try:
        args.sizes = [int(s) for s in args.sizes.split(",")]
    except ValueError:
        p.error(f"invalid -sizes {args.sizes!r}")
    if min(args.sizes) < 1:
        p.error("sizes must be positive")
    return args


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    out = open(args.metrics, "a", encoding="utf-8") if args.metrics else sys.stdout

    def record(case, rows, seconds):
        m = {"bench": "client", "client": "pyspark", "case": case, "rows": rows}
        if args.label:
            m["label"] = args.label
        m["ms"] = round(seconds * 1000, 3)
        out.write(json.dumps(m, separators=(",", ":")) + "\n")

    start = time.perf_counter()
    spark = SparkSession.builder.remote(args.remote).getOrCreate()
    try:
        select_one(spark)
        if "connect" in args.cases:
            record("connect", 1, time.perf_counter() - start)

        if "latency" in args.cases:
            for i in range(WARMUPS + args.queries):
                start = time.perf_counter()
                select_one(spark)
                if i >= WARMUPS:
                    record("latency", 1, time.perf_counter() - start)

        for how in ("rows", "arrow"):
            if how not in args.cases:
                continue
            for n in args.sizes:
                for i in range(WARMUPS + args.samples):
                    start = time.perf_counter()
                    fetch(spark, how, n)
                    if i >= WARMUPS:
                        record(how, n, time.perf_counter() - start)
    finally:
        spark.stop()
        if out is not sys.stdout:
            out.close()


if __name__ == "__main__":
    main()
