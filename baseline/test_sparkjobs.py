#!/usr/bin/env python3
"""Tests of the PySpark baseline.

    python3 baseline/test_sparkjobs.py                     # no Spark needed
    SPARK_REMOTE=sc://localhost:15002 python3 baseline/test_sparkjobs.py
    SPARK_REMOTE=sc://node0:15002 SPARK_TEST_INPUT=hdfs://node0:9000/datasets/corpus python3 baseline/test_sparkjobs.py

With SPARK_REMOTE set, both jobs run on the small corpus and must print
exactly testdata/small/expected, like the Go programs (internal/jobs tests).
"""

import contextlib
import io
import json
import os
import re
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import sparkjobs  # noqa: E402


class OfflineTest(unittest.TestCase):
    def test_token_pattern_matches_go(self):
        with open(os.path.join(REPO, "internal", "textproc", "textproc.go"), encoding="utf-8") as f:
            go = re.search(r'const TokenPattern = "([^"]*)"', f.read()).group(1)
        self.assertEqual(sparkjobs.TOKEN_PATTERN, go)

    def test_flags(self):
        args, write = sparkjobs.parse_args("wordcount", ["-input", "x", "-format", "noop", "-label", "workers=1"])
        self.assertEqual((args.input, args.format, args.label, args.show, write), ("x", "noop", "workers=1", 0, True))
        args, write = sparkjobs.parse_args("wordcount", ["-input", "x"])
        self.assertEqual((args.show, write), (20, False))
        for bad in (["-input", "x", "-format", "noop", "-output", "o"], ["-input", "x", "-dump", "-output", "o"],
                    ["-input", "x", "-stats", "-format", "noop"], ["-input", "x", "-format", "csv"], []):
            with self.subTest(bad=bad), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                sparkjobs.parse_args("wordcount", bad)

    def test_cell(self):
        self.assertEqual(sparkjobs.cell(["doc1", "doc2"]), "[doc1 doc2]")
        self.assertEqual(sparkjobs.cell(12), "12")
        self.assertEqual(len(sparkjobs.cell("x" * 100)), 80)


@unittest.skipUnless(os.environ.get("SPARK_REMOTE"), "SPARK_REMOTE is not set")
class SparkTest(unittest.TestCase):
    input = os.environ.get("SPARK_TEST_INPUT") or "file://" + os.path.join(REPO, "testdata", "small", "corpus")

    def run_job(self, name, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            sparkjobs.main(name, ["-input", self.input, *args])
        return out.getvalue()

    def test_dump_equals_expected(self):
        for name in ("wordcount", "invertedindex"):
            with self.subTest(job=name):
                with open(os.path.join(REPO, "testdata", "small", "expected", name + ".tsv"), encoding="utf-8") as f:
                    self.assertEqual(self.run_job(name, "-dump"), f.read())

    def test_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            metrics = os.path.join(tmp, "runs.jsonl")
            self.run_job("invertedindex", "-format", "noop", "-metrics", metrics, "-label", "workers=1 rep=1")
            if not self.input.startswith("hdfs://"):
                out = "file://" + os.path.join(tmp, "result")
                self.run_job("wordcount", "-output", out, "-stats", "-metrics", metrics)
            with open(metrics, encoding="utf-8") as f:
                runs = [json.loads(line) for line in f]
        self.assertEqual(runs[0]["job"], "invertedindex")
        self.assertEqual(runs[0]["client"], "pyspark")
        self.assertEqual(runs[0]["label"], "workers=1 rep=1")
        self.assertEqual((runs[0]["action"], runs[0]["format"]), ("write", "noop"))
        self.assertGreater(runs[0]["elapsed_ms"], 0)
        self.assertTrue(runs[0]["start"].endswith("Z"))
        if len(runs) > 1:
            self.assertEqual(runs[1]["stats"], {"terms": 29, "total": 42})
            self.assertNotIn("label", runs[1])


if __name__ == "__main__":
    unittest.main()
