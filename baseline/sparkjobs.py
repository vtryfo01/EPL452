"""PySpark baseline: WordCount and Inverted Index through Spark Connect.

The query plans are the same as the Go programs' (internal/jobs), and the
command line, the printed output and the -metrics JSON lines are the same as
theirs (internal/cli), so the benchmark scripts run both clients the same way
and the results can be compared with cmd/verify. The semantics are described
in docs/workloads.md.

    python3 baseline/wordcount.py -input hdfs://node0:9000/datasets/classics-60 -show 10
    python3 baseline/invertedindex.py -input $IN -format noop -metrics runs.jsonl -label "workers=3 rep=1"
"""

import argparse
import datetime
import json
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Same as textproc.TokenPattern in Go (test_sparkjobs.py checks it).
TOKEN_PATTERN = "[^a-z0-9]+"
DEFAULT_REMOTE = "sc://localhost:15002"


def records(spark, path):
    """One row per input line with columns doc_id and text."""
    lines = spark.read.format("text").load(path)
    return lines.select(F.split_part("value", F.lit("\t"), F.lit(1)).alias("doc_id"),
                        F.split_part("value", F.lit("\t"), F.lit(2)).alias("text"))


def terms(spark, path):
    """One (doc_id, term) row per term occurrence."""
    words = F.split(F.lower("text"), TOKEN_PATTERN)
    return (records(spark, path).select("doc_id", F.explode(words).alias("term"))
            .filter(F.col("term") != ""))


def wordcount(spark, path):
    """(term, count): the number of occurrences of every term."""
    return terms(spark, path).groupBy("term").agg(F.count("term").alias("count"))


def invertedindex(spark, path):
    """(term, docs, doc_freq): the sorted ids of the documents that contain
    every term and how many there are."""
    return (terms(spark, path).groupBy("term").agg(F.array_sort(F.collect_set("doc_id")).alias("docs"))
            .withColumn("doc_freq", F.size("docs")))


def dump_counts(rows, out):
    for row in sorted(rows, key=lambda r: r["term"]):
        out.write(f"{row['term']}\t{row['count']}\n")


def dump_index(rows, out):
    for row in sorted(rows, key=lambda r: r["term"]):
        if row["doc_freq"] != len(row["docs"]):
            raise SystemExit(f"term {row['term']!r}: doc_freq {row['doc_freq']} but {len(row['docs'])} documents")
        out.write(f"{row['term']}\t{row['doc_freq']}\t{','.join(row['docs'])}\n")


# name -> (build, order of -show, dump, column summed by -stats)
JOBS = {
    "wordcount": (wordcount, ("count", "term"), dump_counts, "count"),
    "invertedindex": (invertedindex, ("doc_freq", "term"), dump_index, "doc_freq"),
}


def log(name, msg):
    print(f"{name}: {msg}", file=sys.stderr)


def cell(value):
    """Formats a value like the Go programs do (fmt.Sprint)."""
    text = "[" + " ".join(value) + "]" if isinstance(value, list) else str(value)
    return text[:77] + "..." if len(text) > 80 else text


def first_rows(df, n, order):
    rows = df.orderBy(F.col(order[0]).desc(), F.col(order[1]).asc()).limit(n).collect()
    for row in rows:
        print("\t".join(cell(v) for v in row), file=sys.stderr)


def parse_args(name, argv):
    p = argparse.ArgumentParser(prog=name, allow_abbrev=False,
                                description=f"Run {name} on Spark through Spark Connect (PySpark baseline).")
    p.add_argument("-remote", default=os.environ.get("SPARK_REMOTE") or DEFAULT_REMOTE,
                   help="Spark Connect URL (default $SPARK_REMOTE)")
    p.add_argument("-input", required=True, help="input corpus: a file or directory, local or hdfs://")
    p.add_argument("-output", default="", help="write the result to this path (overwritten)")
    p.add_argument("-format", default="parquet", choices=("parquet", "json", "noop"),
                   help="output format, or noop to run the job without storing the result")
    p.add_argument("-show", type=int, default=-1,
                   help="print the first n rows of the result (default 20 unless -output or -dump is set)")
    p.add_argument("-dump", action="store_true", help="print the whole result as sorted TSV (small inputs only)")
    p.add_argument("-stats", action="store_true", help="also compute the number of terms and the total count")
    p.add_argument("-metrics", default="", help="append a JSON line with the timing to this file")
    p.add_argument("-label", default="", help='free-form label stored in the metrics, e.g. "workers=3 rep=1"')
    args = p.parse_args(argv)
    write = bool(args.output) or args.format == "noop"
    if args.format == "noop" and args.output:
        p.error("-format noop does not store anything; drop -output")
    if args.dump and write:
        p.error("-dump cannot be combined with -output or -format noop")
    if args.stats and args.format == "noop":
        p.error("-stats needs a stored result; use -output instead of -format noop")
    if args.show < 0:
        args.show = 0 if write or args.dump else 20
    return args, write


def main(name, argv=None):
    args, write = parse_args(name, sys.argv[1:] if argv is None else argv)
    build, order, dump, sum_column = JOBS[name]
    spark = SparkSession.builder.remote(args.remote).getOrCreate()
    try:
        result = build(spark, args.input)
        m = {"job": name, "client": "pyspark", "label": args.label, "remote": args.remote, "input": args.input}
        start = datetime.datetime.now(datetime.timezone.utc)
        begin = time.perf_counter()
        if write:
            m.update(action="write", format=args.format)
            if args.output:
                m["output"] = args.output
            writer = result.write.mode("overwrite").format(args.format)
            writer.save(args.output) if args.output else writer.save()
        elif args.dump:
            m["action"] = "collect"
            dump(result.collect(), sys.stdout)
        else:
            # Sorting needs the complete result, so this still runs the whole job.
            m["action"] = "show"
            first_rows(result, args.show, order)
        elapsed_ms = int((time.perf_counter() - begin) * 1000)
        log(name, f"{m['action']} of {args.input} took {elapsed_ms / 1000:.2f}s")
        m.update(start=start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), elapsed_ms=elapsed_ms)

        if write and args.format != "noop":
            # Inspect what was stored instead of recomputing the job.
            result = spark.read.format(args.format).load(args.output)
            if args.show > 0:
                first_rows(result, args.show, order)
        if args.stats:
            row = result.agg(F.count("term").alias("terms"), F.sum(sum_column).alias("total")).first()
            m["stats"] = {"terms": row["terms"], "total": row["total"] or 0}
            log(name, f"terms={m['stats']['terms']} total={m['stats']['total']}")
    finally:
        spark.stop()

    if args.metrics:
        if not m["label"]:
            del m["label"]
        with open(args.metrics, "a", encoding="utf-8") as f:
            f.write(json.dumps(m, separators=(",", ":")) + "\n")
