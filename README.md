# CS452 Team 5: Spark Connect Client for Go

Andreas Papandreou and Varnavas Tryfonos. Instructor: Dr Haris Volos.

A reproducible CloudLab deployment of Apache Spark with Spark Connect and
HDFS, Go programs (Spark Connect Go client) for WordCount and Inverted Index,
and strong/weak scaling experiments compared with an equivalent PySpark
baseline.

## Repository layout

| path | contents | owner |
| --- | --- | --- |
| `cmd/wordcount`, `cmd/invertedindex` | Go programs that run the jobs through Spark Connect | Varnavas |
| `cmd/verify` | checks a Spark result against the reference implementation | Varnavas |
| `internal/textproc` | record format and tokenization rules (shared definition) | Varnavas |
| `internal/jobs` | the Spark Connect query plans; Spark integration tests | Varnavas |
| `internal/reference` | single-machine WordCount / Inverted Index used as the oracle | Varnavas |
| `internal/cli` | flags, timing and JSON-lines metrics for the benchmark runs | Varnavas |
| `testdata/small` | hand-checked correctness corpus and expected outputs | Varnavas |
| `data/lists` | reproducible Project Gutenberg book selections | Varnavas |
| `scripts/data` | dataset selection, download, preprocessing, scaling | Varnavas |
| `scripts/hdfs` | HDFS installation/configuration, health check, dataset loading | Varnavas |
| CloudLab profile, Spark and Spark Connect setup | to be added | Andreas |
| PySpark baseline, benchmark scripts, plots, report | to be added | both |

Documentation: [docs/workloads.md](docs/workloads.md) (semantics, running,
correctness testing) and [docs/datasets-and-hdfs.md](docs/datasets-and-hdfs.md)
(dataset choice and pipeline, HDFS placement and scripts).

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

## Status (week 9)

Done (Varnavas):

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

Next:

* Integrate `scripts/hdfs` into the CloudLab profile once it is available,
  load `classics-60` and `sample-2000`, and run `cmd/verify` on the cluster.
* Fix the per-worker data size for weak scaling after the first cluster runs.
* PySpark baseline with the same semantics (docs/workloads.md) and the
  benchmark driver that collects the `-metrics` JSON lines.
