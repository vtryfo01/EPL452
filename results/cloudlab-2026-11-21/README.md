# CloudLab run, 21 November 2026

Strong and weak scaling of WordCount and Inverted Index with the Go client and
the PySpark baseline (docs/benchmarks.md). 80 runs, none failed. Times in the
logs are UTC (Cyprus is UTC+2).

## Setup

* CloudLab Utah, experiment from our profile (`profile.py`, datasets `all`),
  4 `m510` nodes (Intel Xeon D-1548, 8 cores / 16 threads, 64 GB, NVMe):
  `node0` runs the HDFS NameNode, the Spark master and the Spark Connect
  server; `node1` to `node3` each run a DataNode and a Spark worker with one
  16-core executor (46 GB).
* Spark 4.0.5, HDFS 3.4.3 (replication 2, 128 MiB blocks), the Go client of
  `go.mod`, `pyspark-client` 4.0.5. Settings in `cluster-info.txt`.
* Code: commit `f6b1d9c`, copied to `node0` by hand, plus the `-s` option of
  `run_benchmarks.sh` for repetition 4 (next commit). The experiment booted
  from `a1edb44`, the commit that `runs.env.txt` names.

## What ran

1. 09:25 to 09:29: `startup.sh` set up HDFS and Spark and built the Go
   programs (`startup.log`). The experiment could only be booked for 3 hours.
2. The download of `sample-2000` one book at a time took about 4 s per book,
   over 2 hours for 2000 books, so it was stopped (startup service exit 143)
   and `pipeline.sh` downloaded with 8 parallel jobs: 2051 books in 10 min.
3. The main Gutenberg site refused the books the mirror lacks (HTTP 503), so
   `pipeline2.sh` took 5 of them from a second mirror. #12664 and #32857 were
   on neither: `sample-2000` has 1,998 books (777.8 MiB, 12,088,635 records;
   week 10 had 1,999). Both fixes are now in `cluster_datasets.sh`.
4. To fit in the time left, `weak-n` is **2n copies** of `sample-2000`
   instead of 5n: `weak-1` 1.6 GiB, `weak-2` 3.1 GiB, `weak-3` 4.7 GiB.
   `verify` confirmed both Go jobs on every input (`results/pipeline.log`).
5. 10:06 to 11:28: `run_benchmarks.sh -r 3`; 11:28 to 11:56:
   `run_benchmarks.sh -s 4 -r 1` (`rep4.sh`). One repetition took 27 min.

## Results

All tables are in `summary.md` (medians of 4 runs) and `summary.csv` (also
mean, standard deviation, minimum and maximum), and the figures in `plots/`:
`strong-scaling.png`, `weak-scaling.png` and `go-vs-pyspark.png`.

**Go and PySpark take the same time.** The medians differ by at most 2%
(PySpark / Go between 0.98 and 1.01) in all 10 configurations. Both clients
only build the plan and send it, and Spark runs the same plan, so the client
language does not show in jobs of 23 to 230 s.

**Strong scaling** (`weak-3` on 1, 2 and 3 workers, Go; PySpark is within 0.03):

| job | 1 worker | 2 workers | 3 workers | speedup on 3 | efficiency |
| --- | --- | --- | --- | --- | --- |
| WordCount | 67.7 s | 41.1 s | 27.3 s | 2.48 | 0.83 |
| Inverted Index | 227.4 s | 132.1 s | 83.8 s | 2.71 | 0.90 |

**Weak scaling** (`weak-n` on n workers, Go):

| job | 1 worker | 2 workers | 3 workers | efficiency on 3 |
| --- | --- | --- | --- | --- |
| WordCount | 23.5 s | 25.1 s | 27.3 s | 0.86 |
| Inverted Index | 74.4 s | 79.6 s | 83.8 s | 0.89 |

**The first run after the workers change is slower**, whichever client it is:
by 0.5 to 4.5 s for WordCount and 4.5 to 8.3 s for Inverted Index on
`weak-3`, the first input after every change (the second input shows no
consistent difference).
After 3 repetitions Go had run first twice and looked up to 10% slower on 3
workers; repetition 4, with PySpark first, balanced it. The driver now runs 4
repetitions by default and docs/benchmarks.md explains why the number should
be even. A warm-up that runs both jobs on every executor would remove the
effect instead.

## Files

| file | content |
| --- | --- |
| `results/runs.jsonl` | one JSON line per run (label `workers=<n> rep=<r>`) |
| `results/runs.env.txt` | node, Spark version and arguments of both invocations |
| `results/bench.log` | log of the driver, ending with the summary |
| `results/pipeline.log` | download, datasets and their checks |
| `results/setup_pyspark.log` | install and test of the PySpark baseline |
| `startup.log` | automatic setup of `node0` |
| `pipeline.sh`, `pipeline2.sh`, `rep4.sh` | the scripts run by hand on `node0` |
| `cluster-info.txt` | sizes in HDFS and `spark-defaults.conf` |
| `summary.md`, `summary.csv` | `scripts/bench/summarize.py results/runs.jsonl --csv summary.csv` |
| `plots/*.png` | `scripts/bench/plot.py results/runs.jsonl --out plots` |
