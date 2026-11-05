#!/usr/bin/env python3
"""Build a dataset of a given size by replicating (or truncating) a processed
dataset, for the weak-scaling experiments.

  # 1 GiB per worker for 4 workers
  scale_dataset.py --src data/processed/sample-2000 --size 4G --out data/processed/weak-4

Copy 0 keeps the original document ids; copy k renames every document to
"<id>.r<k>" so that the copies are distinct documents for the inverted index.
Whole documents are written until the output reaches --size, so the result
is at most one document larger than requested.

Replication keeps the vocabulary and the shape of the term distribution of
the source (realistic skew for the shuffle), while the amount of work grows
with the size. For a dataset made of complete copies, every word count is
exactly the number of copies times the source count, which is a cheap
correctness check on large runs.
"""

import argparse
import itertools
import os
import sys

from dslib import DEFAULT_PART_SIZE, PartWriter, human_size, parse_size, read_manifest


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--src", required=True, help="processed source dataset")
    p.add_argument("--size", required=True, help="target size, e.g. 512M, 4G")
    p.add_argument("--out", required=True, help="output directory (must be empty or missing)")
    p.add_argument("--name", help="dataset name (default: basename of --out)")
    p.add_argument("--part-size", default=DEFAULT_PART_SIZE, help="target part file size (default %(default)s)")
    args = p.parse_args()

    src = read_manifest(args.src)
    if src["totals"]["bytes"] == 0:
        sys.exit(f"{args.src} is empty")
    target = parse_size(args.size)
    writer = PartWriter(args.out, parse_size(args.part_size))

    copies = complete = 0
    for copy in itertools.count():
        if writer.bytes >= target:
            break
        copies += 1
        suffix = f".r{copy}" if copy else ""
        current = None
        for line in source_records(args.src, src):
            doc_id, _, text = line.partition("\t")
            if doc_id + suffix != current:
                if writer.bytes >= target:
                    break
                current = doc_id + suffix
                writer.start_doc(current)
            writer.write(text)
        else:
            complete += 1

    name = args.name or os.path.basename(os.path.normpath(args.out))
    manifest = writer.close(name, {
        "kind": "scaled",
        "from": src["name"],
        "from_bytes": src["totals"]["bytes"],
        "target_bytes": target,
        "copies": copies,
        "complete_copies": complete,
    }, keep_docs=False)
    t = manifest["totals"]
    print(f"{name}: {t['docs']} documents, {human_size(t['bytes'])} in {t['parts']} part files "
          f"({copies} copies of {src['name']} started) -> {args.out}", file=sys.stderr)


def source_records(directory, manifest):
    for part in manifest["parts"]:
        with open(os.path.join(directory, part["file"]), encoding="utf-8") as f:
            for line in f:
                yield line.rstrip("\n")


if __name__ == "__main__":
    main()
