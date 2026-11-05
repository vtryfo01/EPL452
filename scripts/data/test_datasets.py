#!/usr/bin/env python3
"""Unit tests for the dataset scripts: python3 scripts/data/test_datasets.py"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import dslib  # noqa: E402
import preprocess  # noqa: E402

BOOK = """﻿The Project Gutenberg eBook of Test Book

This header must be removed.

*** START OF THE PROJECT GUTENBERG EBOOK TEST BOOK ***

Chapter I.\tThe Café

“Don’t,” said Æthelred—naïvely—to the ﬁsh.

*** END OF THE PROJECT GUTENBERG EBOOK TEST BOOK ***
License text that must be removed.
"""


class NormalizeTest(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(preprocess.normalize("Chapter I.\tThe Café"), "Chapter I. The Cafe")
        self.assertEqual(preprocess.normalize("“Don’t,” said Æthelred—naïvely—to the ﬁsh."),
                         "Don t, said AEthelred naively to the fish.")
        self.assertEqual(preprocess.normalize("   \x0c  "), "")

    def test_body_lines(self):
        text, _ = preprocess.decode(BOOK.encode("utf-8"))
        lines, markers = preprocess.body_lines(text)
        self.assertTrue(markers)
        body = [line for line in lines if line.strip()]
        self.assertEqual(len(body), 2)
        self.assertTrue(body[0].startswith("Chapter I."))

    def test_without_markers_keeps_everything(self):
        lines, markers = preprocess.body_lines("just\ntext\n")
        self.assertFalse(markers)
        self.assertEqual(lines, ["just", "text"])

    def test_latin1_fallback(self):
        text, encoding = preprocess.decode("Caf\xe9".encode("latin-1"))
        self.assertEqual((text, encoding), ("Café", "latin-1"))


class SizeTest(unittest.TestCase):
    def test_parse_size(self):
        self.assertEqual(dslib.parse_size("4096"), 4096)
        self.assertEqual(dslib.parse_size("128M"), 128 << 20)
        self.assertEqual(dslib.parse_size("1.5G"), 3 << 29)
        self.assertEqual(dslib.parse_size("2GiB"), 2 << 30)
        with self.assertRaises(ValueError):
            dslib.parse_size("lots")


class PipelineTest(unittest.TestCase):
    def run_script(self, *args):
        subprocess.run([sys.executable, *args], check=True, capture_output=True)

    def test_preprocess_and_scale(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = os.path.join(tmp, "raw")
            os.makedirs(raw)
            for book in (7, 12):
                with open(os.path.join(raw, f"pg{book}.txt"), "w", encoding="utf-8") as f:
                    f.write(BOOK)
            out = os.path.join(tmp, "processed")
            # A tiny part size forces one document per part file.
            self.run_script(os.path.join(HERE, "preprocess.py"), "--raw", raw, "--out", out, "--part-size", "10")

            manifest = dslib.read_manifest(out)
            self.assertEqual(manifest["totals"]["docs"], 2)
            self.assertEqual(manifest["totals"]["parts"], 2)
            with open(os.path.join(out, "part-00000.tsv"), encoding="utf-8") as f:
                records = f.read().splitlines()
            self.assertEqual(records, [
                "pg7\tChapter I. The Cafe",
                "pg7\tDon t, said AEthelred naively to the fish.",
            ])
            for line in records:
                self.assertEqual(line.count("\t"), 1)
                line.encode("ascii")  # raises if not ASCII

            # Just over 2 copies (the ".r1" ids of copy 2 add 3 bytes per
            # record): the third copy is cut after its first document.
            scaled = os.path.join(tmp, "scaled")
            size = manifest["totals"]["bytes"] * 2 + 4 * 3 + 1
            self.run_script(os.path.join(HERE, "scale_dataset.py"), "--src", out, "--size", str(size), "--out", scaled)
            scaled_manifest = dslib.read_manifest(scaled)
            self.assertEqual(scaled_manifest["source"]["copies"], 3)
            self.assertEqual(scaled_manifest["source"]["complete_copies"], 2)
            docs = []
            for part in scaled_manifest["parts"]:
                with open(os.path.join(scaled, part["file"]), encoding="utf-8") as f:
                    for line in f:
                        doc = line.split("\t", 1)[0]
                        if not docs or docs[-1] != doc:
                            docs.append(doc)
            self.assertEqual(docs, ["pg7", "pg12", "pg7.r1", "pg12.r1", "pg7.r2"])
            self.assertEqual(json.dumps(scaled_manifest["totals"]["docs"]), "5")


if __name__ == "__main__":
    unittest.main()
