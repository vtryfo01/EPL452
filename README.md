# CS452 Team 5: Spark Connect Client for Go

A reproducible CloudLab deployment of Apache Spark with Spark Connect and
HDFS, Go programs (Spark Connect Go client) for WordCount and Inverted Index,
and strong/weak scaling experiments compared with an equivalent PySpark
baseline.

## Repository layout

| path | contents |
| --- | --- |
| `cmd/wordcount`, `cmd/invertedindex` | Go programs that run the jobs through Spark Connect |
| `cmd/verify` | checks a Spark result against the reference implementation |
| `internal/textproc` | record format and tokenization rules (shared definition) |
| `internal/jobs` | the Spark Connect query plans; Spark integration tests |
| `internal/reference` | single-machine WordCount / Inverted Index used as the oracle |
| `internal/cli` | flags, timing and JSON-lines metrics for the benchmark runs |
| `testdata/small` | hand-checked correctness corpus and expected outputs |
| `data/lists` | reproducible Project Gutenberg book selections |
| `scripts/data` | dataset selection, download, preprocessing, scaling |
| `scripts/hdfs` | HDFS installation/configuration, health check, dataset loading |
| `profile.py` | CloudLab profile: `node0` and the workers on one LAN, set up automatically at boot |
| `scripts/spark` | Spark master, worker and Spark Connect server setup, number of workers |
| `scripts/cloudlab` | setup at boot (`startup.sh`) or over SSH (`deploy.sh`), cluster check |
| `baseline` | PySpark baseline: the same jobs, flags and metrics through the PySpark Spark Connect client |
| `scripts/bench` | PySpark client setup, benchmark driver, summary tables |
| `results` | benchmark results, one directory per cluster run |
| plots, report | to be added |

Documentation: [docs/workloads.md](docs/workloads.md) (semantics, running,
correctness testing), [docs/datasets-and-hdfs.md](docs/datasets-and-hdfs.md)
(dataset choice and pipeline, HDFS placement and scripts),
[docs/deployment.md](docs/deployment.md) (CloudLab cluster) and
[docs/benchmarks.md](docs/benchmarks.md) (PySpark baseline, benchmark runs).

## Quick start

Requirements: Go 1.23+, Python 3.8+, a Spark 4.x Spark Connect server.

```sh
go test ./...                                    # unit tests, no Spark needed
go build -o bin/ ./cmd/...

# local Spark Connect server: $SPARK_HOME/sbin/start-connect-server.sh
SPARK_REMOTE=sc://localhost:15002 go test ./internal/jobs     # jobs on Spark vs expected outputs
bin/wordcount -input file://$PWD/testdata/small/corpus -show 5
bin/invertedindex -input file://$PWD/testdata/small/corpus -dump
```

## Status (week 10)

Done:

* Datasets selected and documented; `classics-60` and `sample-2000` lists
  generated from the Gutenberg catalog; download, preprocessing and
  replication scripts with unit tests.
* HDFS scripts for a NameNode on the master and DataNodes on the workers,
  plus a health check and a verified loader.
* WordCount and Inverted Index in Go on Spark Connect, with a reference
  implementation, hand-checked expected outputs, Spark integration tests and a
  verification tool for large runs.
* Tested end to end on one machine (Spark 4.0.5 Spark Connect, single-node
  HDFS from `scripts/hdfs`): `classics-60` loaded into HDFS, both jobs run from
  Go, results stored in HDFS and equal to the reference on all 85,015 terms
  (details in docs/workloads.md).

* CloudLab cluster deployed and checked with one command: Spark master, 3
  workers, Spark Connect and HDFS, with both Go jobs correct on the small corpus
  (details in docs/deployment.md).
* Real datasets on the cluster (`scripts/data/cluster_datasets.sh`):
  `classics-60` and `sample-2000` (1,999 books, 778 MiB) loaded into HDFS, and
  both Go jobs equal to the reference on every term (85,015 and 541,397).
* Weak-scaling inputs (`scripts/data/weak_datasets.sh 5`): `weak-1` to
  `weak-3`, 5, 10 and 15 complete copies of `sample-2000` (about 4 GiB per
  worker), checked with `verify -copies` (docs/datasets-and-hdfs.md).
* PySpark baseline with the same plans, flags and metrics as the Go programs,
  tested against the same expected outputs, and a benchmark driver for strong
  scaling, weak scaling and Go vs PySpark with a summary of the results
  (docs/benchmarks.md).
* First cluster benchmark (21 November, 4 `m510` nodes, 4 repetitions, 80
  runs, `weak-n` = 2n copies of `sample-2000`): Go and PySpark within 2% of
  each other everywhere; on 3 workers a strong-scaling speedup of 2.48
  (WordCount) and 2.71 (Inverted Index) and a weak-scaling efficiency of 0.86
  and 0.89 (results/cloudlab-2026-11-21).

Next:

* The plots and the report; if there is cluster time, the same runs with the
  planned 5 copies per worker.
