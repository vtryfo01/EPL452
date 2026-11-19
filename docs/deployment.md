# Cluster deployment

Code: `profile.py`, `scripts/spark`, `scripts/cloudlab`.

An experiment started from our CloudLab profile sets itself up: every node
installs and starts its part of HDFS and Spark at boot, `node0` checks the
cluster and can then prepare the datasets. On other experiments one command,
`deploy.sh`, does the same setup over SSH.

## Layout

| node | runs |
| --- | --- |
| `node0` | Spark master (7077, UI 8080), Spark Connect server (15002), HDFS NameNode (9000, UI 9870), Go programs |
| `node1` ... `nodeN` | Spark worker, one executor using all cores, HDFS DataNode |

Spark and HDFS traffic uses the experiment LAN (`10.10.1.x`). The public
interface only accepts SSH and CloudLab's event service, because Spark and HDFS
have no authentication. To open a UI, use a tunnel:
`ssh -L 8080:node0:8080 user@<node0 host>`.

## Experiment

The profile is `profile.py` in this repository. Create it once in the CloudLab
portal (Experiments, Create Experiment Profile, Git Repo) with the URL
`https://github.com/vtryfo01/EPL452`. CloudLab clones the repository without
logging in, so it must be public. On the profile page, Update pulls the latest
commit (or add the GitHub webhook shown there to update on every push).

Parameters when starting an experiment:

| parameter | meaning |
| --- | --- |
| nodes | `node0` plus the workers (default 4) |
| hardware | node type, all nodes the same (default `m400`) |
| datasets | `none`; `base`: `classics-60` and `sample-2000` (about 45 min); `all` (default): also `weak-1` to `weak-N` (about 1.5 h in total) |

All nodes run Ubuntu 22.04 on one LAN, `node0` to `nodeN` at `10.10.1.1`
onwards. At every boot each node runs `scripts/cloudlab/startup.sh` from the
clone in `/local/repository`, as the experiment user:

1. copies the repository to `~/EPL452` and runs `prepare_node.sh` (Java, firewall);
2. a worker waits for the NameNode and the Spark master on `node0`, then starts
   its DataNode and Spark worker;
3. `node0` starts the NameNode, the Spark master and Spark Connect, builds the
   Go programs, waits (up to 30 min) for every worker and runs
   `check_cluster.sh`;
4. `node0` prepares the chosen datasets with `cluster_datasets.sh` and
   `weak_datasets.sh 5` (docs/datasets-and-hdfs.md). This happens once: a
   reboot restarts the daemons but does not repeat it;
5. `node0` installs the PySpark baseline (`setup_pyspark.sh`,
   docs/benchmarks.md).

Progress on each node:

```sh
cat ~/startup.status     # running: <step>, OK, or FAILED: <step>
tail -f ~/startup.log
```

The cluster is ready when `node0` says `OK`, or `running: preparing ...` if
you only need the cluster.

## Deploy

For experiments not made from the profile (such as the class experiment), or to
start again with `--clean`. On a profile experiment, wait until
`~/startup.status` says `OK` first. Requirements on your machine: `ssh`, `tar`,
and the experiment's SSH key in an agent. The first host must be `node0`, as shown in the experiment's List View.

```sh
scripts/cloudlab/deploy.sh user@<node0 host> user@<node1 host> user@<node2 host> ...
```

The script copies the repository to `~/EPL452` on every node, runs
`prepare_node.sh`, builds the Go programs on `node0`, starts the daemons and runs
`check_cluster.sh`. It fails if any step fails.

| flag | effect |
| --- | --- |
| none | keeps the data, restarts Spark, starts any missing HDFS daemon |
| `--clean` | first removes all previous installs and **all data in HDFS** |

## Check

`scripts/cloudlab/check_cluster.sh <workers>` runs on `node0`, also on its own. It
requires all DataNodes live, HDFS writable, all Spark workers alive, both Go jobs
equal to `testdata/small/expected` when run through Spark Connect, and an
executor on every worker.

## Use

```sh
ssh user@<node0 host>
cd ~/EPL452
export SPARK_REMOTE=sc://node0:15002
bin/wordcount -input hdfs://node0:9000/datasets/corpus -show 5
```

## Number of workers

For strong scaling, run `scripts/spark/use_workers.sh N` on `node0`. It restarts
the Spark Connect server so that jobs use N workers, one executor with all
cores on each. HDFS keeps its DataNodes on all workers, so the input stays where
it is. Without N it goes back to all workers, which `check_cluster.sh` expects.

## Limits

* Data lives on the root disk, in `/mydata`, with about 50 GB free per worker.
  With replication 2 this limits the input to roughly 75 GB on 3 workers, before
  shuffle files and results.
* Executor memory is 75% of the RAM of `node0`, so all nodes must be the same type.
* The daemons and the firewall rule do not survive a reboot. On a profile
  experiment `startup.sh` sets them up again at boot; otherwise run `deploy.sh`
  again. Neither has been tested with a real reboot.

## Tested

On `m400` nodes: `node0` and 3 workers, each with 8 ARM cores and 62 GB RAM, on
Ubuntu 22.04.

| test | result | time |
| --- | --- | --- |
| first deployment on new nodes from `profile.py` | passed | 10 min 26 s |
| `--clean` deployment, twice | passed | 7 min 50 s, 6 min 16 s |
| daemons killed on `node0` and a worker, then re-run without `--clean` | passed, file in HDFS kept | 2 min 57 s |
| one Spark worker stopped | `check_cluster.sh` fails as it should | |
| 1.5 GB WordCount scan | 24 of 24 tasks `NODE_LOCAL`, 8 on each worker | |
| Spark and HDFS ports from outside the experiment | blocked, SSH open | |
| `use_workers.sh` with 1, 2 and 3 workers | executors on that many workers, WordCount correct | |

The first job after a Spark restart took about 22 s on the small corpus. The
cause is unknown, so benchmarks should not time that job.
