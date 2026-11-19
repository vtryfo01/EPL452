#!/usr/bin/env python3
"""Build an inverted index (term -> documents that contain it) on Spark through
Spark Connect (PySpark baseline).

    invertedindex.py -input hdfs://node0:9000/datasets/classics-60 -format noop -metrics runs.jsonl

Flags as in the Go program: invertedindex.py -h.
"""

import sparkjobs

if __name__ == "__main__":
    sparkjobs.main("invertedindex")
