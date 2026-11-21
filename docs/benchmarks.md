# Benchmarks: strong scaling, weak scaling, Go vs PySpark

Code: `baseline` (PySpark baseline), `scripts/bench` (drivers, summaries and
figures), `cmd/clientbench` (client overhead, at the end).

## PySpark baseline

`baseline/wordcount.py` and `baseline/invertedindex.py` run the same Spark
plans as the Go programs (docs/workloads.md) through the PySpark Spark Connect
client (`pyspark-client`, the version of the server). They take the same flags
(`-input`, `-output`, `-format`, `-show`, `-dump`, `-stats`, `-metrics`,
`-label`) and print and record the same things; the metrics have
`"client": "pyspark"` instead of `"go"`. Since both clients only send a plan,
the comparison measures what the client language changes: building and sending
the plan, and the Python or Go process around it.

`scripts/bench/setup_pyspark.sh` installs the client into `/opt/pyspark` on
`node0` and runs `baseline/test_sparkjobs.py`, which checks both jobs against
`testdata/small/expected`, like the Go integration tests. On a profile
experiment `startup.sh` runs it at the end; the driver below also runs it.

```sh
/opt/pyspark/bin/python baseline/wordcount.py -input hdfs://node0:9000/datasets/classics-60 -show 5
```

For larger inputs, store a result and check it with `verify -result`:

```sh
IN=hdfs://node0:9000/datasets/sample-2000
/opt/pyspark/bin/python baseline/invertedindex.py -input $IN -output hdfs://node0:9000/results/py-ii
bin/verify -job invertedindex -input $IN -local /mydata/datasets/processed/sample-2000 -result hdfs://node0:9000/results/py-ii
```

## Running

On `node0`, after the datasets (`weak-1` to `weak-3`) are in HDFS:

```sh
cd ~/EPL452
mkdir -p ~/results
nohup scripts/bench/run_benchmarks.sh > ~/results/bench.log 2>&1 &
tail -f ~/results/bench.log
```

| option | meaning | default |
| --- | --- | --- |
| `-r` | repetitions of every run (keep it even, see Method) | 4 |
| `-s` | number of the first repetition, to add repetitions to a file later | 1 |
| `-w` | largest number of workers W | all alive workers |
| `-c` | clients | `go,pyspark` |
| `-j` | jobs | `wordcount,invertedindex` |
| `-e` | `strong`: `weak-W` on 1 .. W workers; `weak`: `weak-n` on n workers | `strong,weak` |
| `-o` | metrics file (appended to) | `~/results/runs.jsonl` |
| `-n` | only print the runs | |

With 3 workers a repetition is 5 configurations (`weak-3` on 1, 2 and 3
workers, `weak-1` on 1, `weak-2` on 2; `weak-3` on 3 belongs to both
experiments and runs once), times 2 jobs and 2 clients: 20 runs. On `m510`
nodes with 2 copies of `sample-2000` per worker (`weak_datasets.sh 2`), a
repetition took 27 minutes (results/cloudlab-2026-11-21); with 5 copies per
worker it takes about 2.5 times as long, mostly Inverted Index on one worker.
With less time, run `-j wordcount` first or fewer repetitions; repetitions can
be added to the same file later, e.g. `-s 3 -r 2` after two.

## Method

* Every run is a fresh client process that executes the whole job and
  discards the result (`-format noop`), so writing the output does not hide
  the scaling. `elapsed_ms` is the time of that action seen by the client
  (docs/workloads.md).
* The number of workers is set with `scripts/spark/use_workers.sh N`: one
  executor with all cores on each of N workers. The HDFS DataNodes stay on all
  workers, so every run reads the same blocks from the same disks.
* After each change of workers, an untimed WordCount on `classics-60` absorbs
  the slow first job after a Spark restart (docs/deployment.md).
* Repetitions are the outer loop and the order of the two clients alternates
  between them, so slow drifts in the cluster affect both clients alike. The
  summary reports medians.
* The first run of a job after the workers change is slower than the second
  (on 3 workers by 3 to 4.5 s for WordCount and 5 to 8 s for Inverted Index
  on `weak-3`), whichever client it is: most likely the executors' JVMs are still
  warming up, as the warm-up job is small, may not reach every executor and
  does not run Inverted Index. The alternating order cancels this out only with
  an even number of repetitions, so that each client runs first equally often.
* The inputs fit in the workers' memory (at most 12 GiB over 3 workers with
  62 GB each), so after the first read they come from the page cache. The
  first repetition can therefore be slower; it is kept, and the median of 3
  or more repetitions is not affected by it.

## Results

`~/results/runs.jsonl` gets one line per run (label `workers=<n> rep=<r>`), and
`~/results/runs.env.txt` the node type, Spark version and commit of every
invocation. The driver ends with the summary; to make it again or export it:

```sh
python3 scripts/bench/summarize.py ~/results/runs.jsonl --csv ~/results/summary.csv
scp '<user>@<node0 host>:results/*' results/      # from your machine
```

The summary has, per job, the median of every configuration with the
PySpark / Go ratio, the strong-scaling table (speedup T(1) / T(n) and
efficiency T(1) / (n T(n)) on `weak-W`) and the weak-scaling table (efficiency
T(`weak-1` on 1) / T(`weak-n` on n)). The CSV has the number of runs, median,
mean, standard deviation, minimum and maximum of every configuration.

The figures for the report (needs `pip install matplotlib`):

```sh
python3 scripts/bench/plot.py results/runs.jsonl --out results/plots
```

`strong-scaling.png` and `weak-scaling.png` have one column per job, with the
median run time (and the range of the runs) above and the speedup or the
efficiency, next to the ideal, below; `go-vs-pyspark.png` shows the runs of
both clients in every configuration as the difference from the median of Go.

Tests (no cluster needed): `python3 scripts/bench/test_summarize.py`,
`python3 scripts/bench/test_summarize_client.py`, `python3 baseline/test_sparkjobs.py`.

## Client overhead

The job benchmarks time Spark more than the client: both clients send a plan
of a few kilobytes and wait. `cmd/clientbench` and `baseline/clientbench.py`
measure the work of the client itself, with the same SQL queries in both:

| case | what is timed |
| --- | --- |
| `process` | a new process that connects and runs `SELECT 1`, timed from outside by the driver, so it includes starting the program (for PySpark, Python and the imports of `pyspark`) |
| `connect` | inside the program: creating the session and the first `SELECT 1` |
| `latency` | `SELECT 1` on an open session (submit the SQL, collect its row), 200 times |
| `rows` | collecting `SELECT concat('term', id) AS term, id AS count FROM range(n)`, a result shaped like WordCount's, as rows (Go `Collect`, PySpark `collect()`), for n = 1,000 to 1,000,000, 5 times each |
| `arrow` | the same result as an Arrow table (Go `ToArrow`, PySpark `toArrow()`) |

Before `latency` and every size, 3 untimed queries warm the client and the
server up. `scripts/bench/run_clientbench.sh` runs both clients in turn,
alternating which goes first, and appends one JSON line per measurement:

```sh
PYSPARK_VENV=~/pyspark-venv scripts/bench/run_clientbench.sh -o ~/results/client.jsonl  # sc://127.0.0.1:15002
python3 scripts/bench/summarize_client.py ~/results/client.jsonl --csv ~/results/client-summary.csv
python3 scripts/bench/plot.py --client ~/results/client.jsonl --out ~/results/plots
```

| option | meaning | default |
| --- | --- | --- |
| `-r` | repetitions (the order of the clients alternates) | 4 |
| `-p` | new processes timed per repetition and client (`process`) | 5 |
| `-o` | metrics file (appended to) | `~/results/client.jsonl` |
| `-- flags` | passed to both clients: `-queries`, `-sizes`, `-samples`, `-cases` | |

The summary has the median and 95th percentile of every case and size for
each client, and the ratio of the medians. Running it next to the server
(on one machine, or on `node0`) leaves the network out, so what remains is
the clients' own work and Spark's fixed cost per query.

Give the server as an IP address (`SPARK_REMOTE`, default
`sc://127.0.0.1:15002`). For a host name, the Go client connects through
gRPC-Go's DNS resolver (`grpc.NewClient` in spark-connect-go), which also
looks up the TXT record `_grpc_config.<host>` before the first query and
waits for the answer. With a slow DNS server this dominates `connect`: on the
machine of results/local-2026-11-21-client, `sc://localhost:15002` took a
median of 0.9 s, the time its DNS server needed to answer that the record
does not exist, and `sc://127.0.0.1:15002` about 30 ms.
`GRPC_ENABLE_TXT_SERVICE_CONFIG=false` turns the lookup off. The PySpark
client (gRPC C core) does not make it. The job benchmarks are not affected:
they time the job after connecting.
