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
| PySpark baseline, benchmark scripts, plots, report | to be added |

Documentation: [docs/workloads.md](docs/workloads.md) (semantics, running,
correctness testing), [docs/datasets-and-hdfs.md](docs/datasets-and-hdfs.md)
(dataset choice and pipeline, HDFS placement and scripts) and
[docs/deployment.md](docs/deployment.md) (CloudLab cluster).

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

Next:

* PySpark baseline with the same semantics (docs/workloads.md) and the
  benchmark driver that collects the `-metrics` JSON lines.
* Strong scaling (`weak-3` on 1 to 3 workers), weak scaling (`weak-n` on *n*
  workers) and Go vs PySpark runs, then the plots.
