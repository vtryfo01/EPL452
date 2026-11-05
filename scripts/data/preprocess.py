#!/usr/bin/env python3
"""Turn downloaded Gutenberg books into a processed dataset (see dslib.py).

  preprocess.py --raw data/raw/gutenberg --list data/lists/classics-60.txt \\
      --out data/processed/classics-60

For every book (document id "pg<id>"):
  * decode as UTF-8 (falling back to Latin-1) and drop the byte-order mark;
  * keep only the text between the "*** START OF ... PROJECT GUTENBERG" and
    "*** END OF ... PROJECT GUTENBERG" markers (license and header removed);
  * fold the text to printable ASCII: expand ligatures, strip accents (NFKD),
    replace every other character, tabs and control characters by a space,
    collapse runs of spaces and trim;
  * drop empty lines and write one "<doc_id>\\t<line>" record per line.

Folding to ASCII makes lower-casing and the [^a-z0-9]+ tokenization behave
identically in Spark (Java), Go and Python.
"""

import argparse
import os
import re
import sys
import unicodedata

from dslib import DEFAULT_PART_SIZE, PartWriter, human_size, parse_size, read_id_list

START = re.compile(r"^\s*\*{3}\s*START OF (THE|THIS) PROJECT GUTENBERG", re.I)
END = re.compile(r"^\s*\*{3}\s*END OF (THE|THIS) PROJECT GUTENBERG", re.I)
NOT_PRINTABLE_ASCII = re.compile(r"[^\x20-\x7e]")
SPACES = re.compile(r" {2,}")
LIGATURES = str.maketrans({
    "æ": "ae", "Æ": "AE", "œ": "oe", "Œ": "OE", "ß": "ss", "ø": "o", "Ø": "O",
    "ł": "l", "Ł": "L", "đ": "d", "Đ": "D", "þ": "th", "Þ": "Th", "ð": "d", "Ð": "D",
    "ı": "i",
})
BOOK_FILE = re.compile(r"^pg(\d+)\.txt$")


def decode(data):
    try:
        return data.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        return data.decode("latin-1"), "latin-1"


def body_lines(text):
    """Return the lines between the Gutenberg markers and whether both were found."""
    lines = text.splitlines()
    start = next((i + 1 for i, line in enumerate(lines) if START.match(line)), 0)
    end = next((i for i in range(start, len(lines)) if END.match(lines[i])), len(lines))
    return lines[start:end], start > 0 and end < len(lines)


def normalize(line):
    line = unicodedata.normalize("NFKD", line.translate(LIGATURES))
    line = "".join(ch for ch in line if not unicodedata.combining(ch))
    line = NOT_PRINTABLE_ASCII.sub(" ", line)
    return SPACES.sub(" ", line).strip()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw", default="data/raw/gutenberg", help="directory with pg<id>.txt files")
    p.add_argument("--list", help="only process the books of this list (default: every pg<id>.txt)")
    p.add_argument("--out", required=True, help="output directory (must be empty or missing)")
    p.add_argument("--name", help="dataset name (default: basename of --out)")
    p.add_argument("--part-size", default=DEFAULT_PART_SIZE, help="target part file size (default %(default)s)")
    args = p.parse_args()

    if args.list:
        ids = sorted(set(read_id_list(args.list)))
    else:
        ids = sorted(int(m.group(1)) for m in map(BOOK_FILE.match, os.listdir(args.raw)) if m)
    available = [i for i in ids if os.path.exists(os.path.join(args.raw, f"pg{i}.txt"))]
    if len(available) < len(ids):
        print(f"warning: {len(ids) - len(available)} listed books were not downloaded", file=sys.stderr)
    if not available:
        sys.exit("no books to process")

    writer = PartWriter(args.out, parse_size(args.part_size))
    no_markers = 0
    for book in available:
        source = f"pg{book}.txt"
        with open(os.path.join(args.raw, source), "rb") as f:
            text, encoding = decode(f.read())
        lines, has_markers = body_lines(text)
        no_markers += not has_markers
        writer.start_doc(f"pg{book}", source=source, encoding=encoding, markers=has_markers)
        for line in lines:
            line = normalize(line)
            if line:
                writer.write(line)

    name = args.name or os.path.basename(os.path.normpath(args.out))
    manifest = writer.close(name, {"kind": "gutenberg", "list": args.list, "raw": args.raw})
    t = manifest["totals"]
    print(f"{name}: {t['docs']} documents, {t['records']} records, {human_size(t['bytes'])} "
          f"in {t['parts']} part files -> {args.out}", file=sys.stderr)
    if no_markers:
        print(f"warning: {no_markers} books had no START/END markers and were kept whole", file=sys.stderr)


if __name__ == "__main__":
    main()
