# Client overhead, local run, 21 November 2026

`cmd/clientbench` (Go) and `baseline/clientbench.py` (PySpark) against a Spark
Connect server on the same machine (docs/benchmarks.md, "Client overhead"):
the work of the clients themselves, which the cluster benchmarks
(results/cloudlab-2026-11-21) do not show. 2,008 measurements, none failed.

## Setup

* A laptop: AMD Ryzen 7 5800H (8 cores / 16 threads), WSL 2 with Ubuntu 22.04
  and 7 GB of memory. Server and clients run in WSL and talk over the loopback
  interface, so there is no network in the times.
* Spark 4.0.5 (Java 17), a Spark Connect server in local mode:
  `$SPARK_HOME/sbin/start-connect-server.sh --master "local[8]" --conf spark.driver.memory=2g --conf spark.ui.enabled=false`.
  A few test runs had warmed it up before.
* Go client: spark-connect-go of `go.mod`, built with `CGO_ENABLED=0`.
  PySpark client: `pyspark-client` 4.0.5, Python 3.10, pyarrow 25.0.1,
  pandas 2.3.3.
* `scripts/bench/run_clientbench.sh` with the defaults: 4 repetitions
  (alternating which client goes first), 5 new processes per repetition and
  client, 200 `SELECT 1`, results of 1,000 to 1,000,000 rows 5 times each.
  The code is that of the commit that adds this directory (the copy that ran
  had no `.git`, hence "commit: unknown" in the env files).

## Results

Medians in ms (`summary.md` also has the 95th percentile; `summary.csv` the
mean, minimum and maximum), server `sc://127.0.0.1:15002`:

| case | Go | PySpark | PySpark / Go |
| --- | --- | --- | --- |
| new process to first result (`process`) | 29.9 | 561 | 18.8 |
| connect and first query (`connect`) | 23.9 | 373 | 15.6 |
| `SELECT 1` on an open session (`latency`) | 8.9 | 14.1 | 1.6 |
| 1,000 rows as rows | 15.3 | 26.1 | 1.7 |
| 100,000 rows as rows | 58.7 | 279 | 4.8 |
| 1,000,000 rows as rows | 475 | 2,550 | 5.4 |
| 1,000 rows as Arrow | 12.5 | 23.9 | 1.9 |
| 1,000,000 rows as Arrow | 139 | 166 | 1.2 |

Figure: `plots/client-overhead.png`.

**Start-up: Go is ready about 0.5 s sooner.** A new Go process has its first
result after 30 ms, a PySpark one after 0.56 s. Inside the program, about
250 ms of PySpark's 373 ms are importing pyarrow and pandas, which PySpark
loads while connecting and running the first query; creating the session takes about 80 ms and the first
`SELECT 1` about 30 ms (measured separately, 3 times). The rest of `process`
is starting Python and importing `pyspark`.

**Per query, PySpark adds about 5 ms:** `SELECT 1` on an open session takes
8.9 ms with Go and 14.1 ms with PySpark, which also shows in the smallest
results.

**Results as rows: Go is 5 times faster for large results.** One million
WordCount-shaped rows (`term`, `count`) arrive as rows in 0.47 s with Go and in
2.55 s with PySpark, which builds a Python `Row` object for every row.

**Results as Arrow: almost the same.** Both clients decode Arrow in compiled
code (arrow-go, pyarrow), and at one million rows the difference is 139 ms
against 166 ms. Turning the Arrow data into rows costs Go about 0.34 s and
PySpark about 2.4 s per million rows.

**Host names and the Go client.** With `sc://localhost:15002`
(`client-localhost.jsonl`, `summary-localhost.md`) Go needed a median of
909 ms to connect instead of 24 ms, while PySpark stayed at 358 ms: gRPC-Go
looks up a DNS TXT record for the host before the first query, and this
machine's DNS server takes about 0.9 s to answer (docs/benchmarks.md). The
other cases of that first run were a little slower for both clients (e.g.
`SELECT 1` 12.1 and 16.8 ms), as the server had run fewer queries before, and
their ratios are similar to the ones above.

Together with the cluster run: for jobs of 20 s and more, the client does not
matter (within 2%). The Go client is clearly faster for short programs and
interactive use, many small queries, and results that come back to the client.

Limits: one machine, where the clients and the server share the CPUs; the
loopback interface instead of a network; Spark in local mode; 4 repetitions.
The first PySpark process of repetitions 1 and 2 took 3.4 to 3.7 s (the
95th percentile of `process`); the medians are not affected.

## Files

| file | content |
| --- | --- |
| `client.jsonl`, `client.env.txt` | the run with `sc://127.0.0.1:15002`: one JSON line per measurement, machine and versions |
| `summary.md`, `summary.csv` | `scripts/bench/summarize_client.py client.jsonl --csv summary.csv` |
| `plots/client-overhead.png` | `scripts/bench/plot.py --client client.jsonl --out plots` |
| `client-localhost.jsonl`, `client-localhost.env.txt`, `summary-localhost.md` | the first run, with `sc://localhost:15002` (written as `client.jsonl` and renamed; its PySpark version, missing in the env file, was 4.0.5) |
