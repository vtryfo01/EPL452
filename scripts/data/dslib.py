"""Helpers shared by the dataset scripts.

A processed dataset is a directory of part files plus a manifest:

    part-00000.tsv   one record per line: <doc_id>\t<text>
    part-00001.tsv   a long document may continue in the next part file
    ...
    _MANIFEST.json   provenance, sizes and checksums

Spark (and internal/reference) skip files whose name starts with "_", so the
directory can be copied to HDFS as is and passed to the jobs with -input.
Part files are filled up to the HDFS block size (128 MiB) and never past it,
so each one is stored as a single block and read by one Spark task, instead of
thousands of tiny files or a tiny second block per file.
"""

import datetime
import hashlib
import json
import os
import re

MANIFEST = "_MANIFEST.json"
RECORD_FORMAT = "<doc_id>\\t<text>, one record per line, UTF-8 (ASCII after normalization)"
DEFAULT_PART_SIZE = "128M"

_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]?)i?B?\s*$", re.I)


def parse_size(text):
    """Parse sizes such as 4096, 128M, 1.5G (binary units)."""
    m = _SIZE.match(text)
    if not m:
        raise ValueError(f"invalid size {text!r}")
    exponent = " KMGT".index(m.group(2).upper() or " ")
    return int(float(m.group(1)) * 1024**exponent)


def human_size(n):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TiB"


def read_id_list(path):
    """Read a book list: one numeric id per line, '#' starts a comment."""
    ids = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                ids.append(int(line))
    return ids


def read_manifest(directory):
    with open(os.path.join(directory, MANIFEST), encoding="utf-8") as f:
        return json.load(f)


class PartWriter:
    """Writes documents into part files of roughly part_size bytes."""

    def __init__(self, out_dir, part_size):
        if os.path.exists(out_dir) and os.listdir(out_dir):
            raise SystemExit(f"{out_dir} is not empty; remove it first")
        os.makedirs(out_dir, exist_ok=True)
        self.out_dir = out_dir
        self.part_size = part_size
        self.parts = []
        self.docs = []
        self._file = None
        self._doc = None

    @property
    def bytes(self):
        """Bytes written so far."""
        current = self._part["bytes"] if self._file is not None else 0
        return sum(p["bytes"] for p in self.parts) + current

    def start_doc(self, doc_id, **info):
        """Start a new document."""
        self._end_doc()
        if self._file is None:
            self._new_part()
        self._doc = {"id": doc_id, "records": 0, "bytes": 0, **info}
        self._part["docs"] += 1

    def write(self, text):
        """Write one record of the current document. A part file never grows
        past part_size (unless a single record is larger): the document then
        continues in the next part file."""
        data = f"{self._doc['id']}\t{text}\n".encode("utf-8")
        if self._part["bytes"] and self._part["bytes"] + len(data) > self.part_size:
            self._new_part()
            self._part["docs"] += 1
        self._file.write(data)
        self._hash.update(data)
        self._doc["records"] += 1
        self._doc["bytes"] += len(data)
        self._part["records"] += 1
        self._part["bytes"] += len(data)

    def close(self, name, source, keep_docs=True):
        """Finish the last part and write the manifest; returns it."""
        self._end_doc()
        self._close_part()
        manifest = {
            "name": name,
            "format": RECORD_FORMAT,
            "created_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": source,
            "part_size_target": self.part_size,
            "totals": {
                "docs": len(self.docs),
                "records": sum(p["records"] for p in self.parts),
                "bytes": sum(p["bytes"] for p in self.parts),
                "parts": len(self.parts),
            },
            "parts": self.parts,
        }
        if keep_docs:
            manifest["docs"] = self.docs
        with open(os.path.join(self.out_dir, MANIFEST), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=1)
            f.write("\n")
        return manifest

    def _end_doc(self):
        if self._doc is not None:
            self.docs.append(self._doc)
            self._doc = None

    def _new_part(self):
        self._close_part()
        name = f"part-{len(self.parts):05d}.tsv"
        self._file = open(os.path.join(self.out_dir, name), "wb")
        self._hash = hashlib.sha256()
        self._part = {"file": name, "bytes": 0, "records": 0, "docs": 0}

    def _close_part(self):
        if self._file is not None:
            self._file.close()
            self._part["sha256"] = self._hash.hexdigest()
            self.parts.append(self._part)
            self._file = None
