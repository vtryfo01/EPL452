#!/usr/bin/env python3
"""Count the terms of a corpus on Spark through Spark Connect (PySpark baseline).

    wordcount.py -input hdfs://node0:9000/datasets/classics-60 -format noop -metrics runs.jsonl

Flags as in the Go program: wordcount.py -h.
"""

import sparkjobs

if __name__ == "__main__":
    sparkjobs.main("wordcount")
