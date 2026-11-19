# Datasets and HDFS

Code: `scripts/data`, `scripts/hdfs`, `data/lists`.

## Dataset choice

| candidate | size | documents | preprocessing | verdict |
| --- | --- | --- | --- | --- |
| **Project Gutenberg** books | any number of books, ~0.4 MB each | one per book | strip license header/footer, fold to ASCII | **chosen** |
| English Wikipedia dump | ~22 GB compressed | one per article | XML parsing and wikitext removal (heavy) | fallback if we need more distinct text |
| 1 Billion Word benchmark | ~4 GB | none (shuffled sentences) | none | rejected: no documents for the inverted index |
| Common Crawl WET files | unbounded | one per page | language filtering, heavy noise | rejected: too noisy for exact correctness checks |

Gutenberg is public domain, plain text, and a book is a natural document
for the inverted index. The selection is reproducible: books are picked from
the official catalog (`pg_catalog.csv`) by explicit id or by a seeded random
sample, and every list is committed with titles.

| dataset | list | use |
| --- | --- | --- |
| `testdata/small/corpus` | hand-written, 4 documents | exact expected outputs, edge cases (see docs/workloads.md) |
| `classics-60` | `data/lists/classics-60.txt`, 60 well-known books (55.8 MiB processed, 928,771 records) | full comparison against the reference; first cluster runs |
| `sample-2000` | `data/lists/sample-2000.txt`, 2000 random English texts (seed 452); 1,999 downloaded, #32857 has no plain-text version (777.9 MiB, 12,089,988 records) | base corpus for scaling |
| `weak-<n>` | `scale_dataset.py --copies 5n` on `sample-2000` (about 4 GiB per worker) | weak scaling with *n* workers; `weak-3` also for strong scaling |

For weak scaling, `weak-<n>` is 5*n* complete copies of `sample-2000` (copy
*k* renames document `pg84` to `pg84.r<k>`), so every worker gets about
4 GiB. Replication keeps the vocabulary and the skew of the term
distribution, so the shuffle behaves like real text, and every count is
exactly the number of copies times the source count, which `verify -copies`
checks (docs/workloads.md).

Why 4 GiB per worker: Spark reads the input in 128 MiB splits, one task each,
and a worker on our nodes (`m510`, 8 cores, 16 hardware threads) runs 16 tasks
at once. 4 GiB is about 32 splits per worker, two full waves, so no core waits
for work in the read stage, and the jobs run long enough that the fixed cost of
starting a job (a few seconds) is small. With replication 2, `weak-1` to
`weak-3` (24 copies of 4 GiB in total) take about 48 GiB of HDFS, 16 GiB per
worker, of the 56 GB free on each.

## Pipeline

```sh
# 1. Choose books (the catalog is downloaded once to data/raw/pg_catalog.csv)
python3 scripts/data/select_gutenberg.py --count 2000 --seed 452 --out data/lists/sample-2000.txt

# 2. Download them (2 s between requests; resumable; failures in _MISSING.txt)
python3 scripts/data/fetch_gutenberg.py data/lists/classics-60.txt --out data/raw/gutenberg

# 3. Clean and pack into ~128 MiB part files with a manifest
python3 scripts/data/preprocess.py --raw data/raw/gutenberg --list data/lists/classics-60.txt \
    --out data/processed/classics-60

# 4. Optional: build a scaled input, by size or as exactly k complete copies
python3 scripts/data/scale_dataset.py --src data/processed/sample-2000 --size 4G --out data/processed/big
python3 scripts/data/scale_dataset.py --src data/processed/sample-2000 --copies 15 --out data/processed/weak-3

# Tests of the scripts
python3 scripts/data/test_datasets.py
```

On the cluster two scripts on `node0` do all of this, load the results into
HDFS and check both Go jobs on them (run after `scripts/cloudlab/deploy.sh`;
data under `/mydata/datasets`):

```sh
scripts/data/cluster_datasets.sh      # classics-60 and sample-2000 (about 45 min, mostly the download)
scripts/data/weak_datasets.sh 5       # weak-1 to weak-3: 5, 10 and 15 copies of sample-2000
```

On an experiment started from our CloudLab profile, its `datasets` parameter
runs both scripts automatically after the setup (docs/deployment.md).
`cluster_datasets.sh` downloads from a Gutenberg mirror (the main site blocks
bulk downloads) and takes the few books the mirror lacks from the main site.
Both scripts can be re-run and end with a summary of sizes and check times.

Processed layout: `part-NNNNN.tsv` files of `<doc_id>\t<text>` records (empty
lines dropped; a long document may continue in the next part file, which the
jobs handle since documents are identified by their id) and `_MANIFEST.json`
with the source, the per-document and per-part sizes, and a SHA-256 per part.
Part files are filled up to the 128 MiB HDFS block size and never past it.
This avoids the small-files problem (thousands of 400 KB books would mean
thousands of tasks and NameNode objects), and each part is a single HDFS block
read by one Spark task on a node that stores it. The scripts only use the
Python standard library.

Preprocessing of a book: decode UTF-8 (Latin-1 fallback), keep the text
between the `*** START OF ... PROJECT GUTENBERG` and `*** END OF ...` markers,
expand ligatures (æ → ae), strip accents (NFKD), turn every other non-printable
or non-ASCII character and tabs into spaces, collapse spaces, drop empty lines.

## HDFS

Placement (see docs/deployment.md, which runs these scripts on the cluster):
NameNode on the master node (`node0`, next to the Spark master and the Spark
Connect server), one DataNode on every worker machine, replication 2, 128 MiB
blocks. Executors then read most blocks locally. For strong scaling we suggest
keeping the DataNodes on all workers in every run and varying only the
number of Spark workers, so that the input placement is identical across runs.

```sh
# on node0 (master)
scripts/hdfs/setup_hdfs.sh namenode node0
# on every worker
scripts/hdfs/setup_hdfs.sh datanode node0
# on any node: wait for N DataNodes and do a write/read test
scripts/hdfs/check_hdfs.sh N

# load a dataset; prints the URI for -input (hdfs://node0:9000/datasets/classics-60)
scripts/hdfs/load_hdfs.sh data/processed/classics-60
```

`setup_hdfs.sh` installs OpenJDK 11 for the daemons if missing (Hadoop 3.4
supports Java 8/11 at runtime; Spark 4 keeps its own Java 17), downloads Hadoop
3.4.3 into `/opt/hadoop`, writes `core-site.xml`/`hdfs-site.xml`, formats the
NameNode once and starts the daemons with `hdfs --daemon start` (no SSH between
nodes needed). It is idempotent, so it can run again after a restart.
All settings can be overridden with environment variables (see the header of
the script). `load_hdfs.sh` compares the size of every file with the local
copy and requires `fsck` to report the directory HEALTHY.

`scripts/cloudlab/deploy.sh` runs `setup_hdfs.sh` on each node and then
`check_hdfs.sh <workers>`. On the class experiment the LAN names `node0`,
`node1`, ... resolve on every node, and `/mydata` is a directory on the root
disk, with about 50 GB free per worker. DataNodes report their LAN address, the
name Spark uses for executors, so that tasks can run next to their blocks.

Spark reads `hdfs://node0:9000/...` URIs with the Hadoop client bundled in the
Spark distribution, so the Spark configuration needs no HDFS settings.
