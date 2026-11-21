#!/usr/bin/env python3
"""Tests of summarize_client.py: python3 scripts/bench/test_summarize_client.py"""

import csv
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import summarize_client  # noqa: E402


def sample(case, client, rows, ms):
    return json.dumps({"bench": "client", "client": client, "case": case, "rows": rows, "label": "rep=1", "ms": ms})


class SummarizeClientTest(unittest.TestCase):
    def test_tables(self):
        lines = [
            sample("rows", "go", 1000, 4.0), sample("rows", "go", 1000, 6.0), sample("rows", "go", 1000, 5.0),
            sample("rows", "pyspark", 1000, 10.0),
            sample("rows", "go", 100, 2.0),
            sample("process", "go", 1, 50.0), sample("process", "pyspark", 1, 1500.0),
            sample("latency", "go", 1, 3.0),
            json.dumps({"job": "wordcount", "client": "go", "label": "workers=1", "elapsed_ms": 5}),  # ignored
            "",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "client.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            samples = summarize_client.load(path)
            text = summarize_client.summarize(samples)
            summarize_client.write_csv(samples, os.path.join(tmp, "s.csv"))
            with open(os.path.join(tmp, "s.csv"), encoding="utf-8") as f:
                rows = list(csv.DictReader(f))

        self.assertEqual(samples[("rows", "go", 1000)], [4.0, 6.0, 5.0])
        self.assertEqual(len(samples), 6)
        table = text.splitlines()
        self.assertEqual(table[0], "| case | rows | Go median (ms) | p95 | n | PySpark median (ms) | p95 | n "
                                   "| PySpark / Go |")
        # cases in a fixed order, sizes ascending
        self.assertEqual([r.split(" | ")[0] for r in table[2:]], ["| process", "| latency", "| rows", "| rows"])
        self.assertIn("| process | 1 | 50.0 | 50.0 | 1 | 1500 | 1500 | 1 | 30.00 |", text)
        self.assertIn("| rows | 1,000 | 5.00 | 6.00 | 3 | 10.0 | 10.0 | 1 | 2.00 |", text)
        self.assertIn("| latency | 1 | 3.00 | 3.00 | 1 |  |  |  |  |", text)
        self.assertEqual(rows[0]["case"], "process")
        self.assertEqual([(r["client"], r["rows"], r["median_ms"]) for r in rows if r["case"] == "rows"],
                         [("go", "100", "2.000"), ("go", "1000", "5.000"), ("pyspark", "1000", "10.000")])

    def test_percentile(self):
        values = list(range(1, 101))
        self.assertEqual(summarize_client.percentile(values, 95), 95)
        self.assertEqual(summarize_client.percentile([7.0], 95), 7.0)
        self.assertEqual(summarize_client.percentile([1, 2, 3, 4], 50), 2)


if __name__ == "__main__":
    unittest.main()
