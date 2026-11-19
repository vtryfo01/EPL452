#!/usr/bin/env python3
"""Tests of summarize.py: python3 scripts/bench/test_summarize.py"""

import csv
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import summarize  # noqa: E402


def run(job, client, name, workers, seconds, label=None):
    m = {"job": job, "client": client, "input": f"hdfs://node0:9000/datasets/{name}", "action": "write",
         "elapsed_ms": int(seconds * 1000), "label": label or f"workers={workers} rep=1"}
    return json.dumps(m)


class SummarizeTest(unittest.TestCase):
    def test_tables(self):
        lines = [
            # strong: weak-2 on 1 and 2 workers; weak: weak-1 on 1, weak-2 on 2 (shared)
            run("wordcount", "go", "weak-2", 1, 100), run("wordcount", "go", "weak-2", 1, 110),
            run("wordcount", "go", "weak-2", 1, 90),
            run("wordcount", "go", "weak-2", 2, 50),
            run("wordcount", "go", "weak-1", 1, 48),
            run("wordcount", "pyspark", "weak-2", 1, 120), run("wordcount", "pyspark", "weak-2", 2, 60),
            run("wordcount", "pyspark", "weak-1", 1, 60),
            run("wordcount", "go", "weak-2", 2, 1, label="manual run"),  # ignored: no workers=
            "",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "runs.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            runs = summarize.load(path)
            self.assertEqual(runs[("wordcount", "go", "weak-2", 1)], [100, 110, 90])
            self.assertEqual(runs[("wordcount", "go", "weak-2", 2)], [50])
            text = summarize.summarize(runs)
            summarize.write_csv(runs, os.path.join(tmp, "s.csv"))
            with open(os.path.join(tmp, "s.csv"), encoding="utf-8") as f:
                rows = list(csv.DictReader(f))

        self.assertNotIn("Inverted Index", text)
        self.assertIn("| weak-2 | 1 | 100.0 (3) | 120.0 (1) | 1.20 |", text)
        self.assertIn("Strong scaling (weak-2):", text)
        self.assertIn("| 2 | 50.0 | 2.00 | 1.00 | 60.0 | 2.00 | 1.00 |", text)
        self.assertIn("| 2 | weak-2 | 50.0 | 0.96 | 60.0 | 1.00 |", text)
        self.assertEqual(len(rows), 6)
        self.assertEqual((rows[0]["client"], rows[0]["input"], rows[0]["runs"]), ("go", "weak-1", "1"))
        first = [r for r in rows if r["client"] == "go" and r["input"] == "weak-2" and r["workers"] == "1"][0]
        self.assertEqual((first["median_s"], first["min_s"], first["max_s"], first["stdev_s"]),
                         ("100.000", "90.000", "110.000", "10.000"))


if __name__ == "__main__":
    unittest.main()
