# Benchmarks: strong scaling, weak scaling, Go vs PySpark

Code: `baseline` (PySpark baseline), `scripts/bench` (driver and summary).

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

Tests (no cluster needed): `python3 scripts/bench/test_summarize.py`,
`python3 baseline/test_sparkjobs.py`.
