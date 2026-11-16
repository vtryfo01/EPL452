# Cluster deployment

Code: `profile.py`, `scripts/spark`, `scripts/cloudlab`.

One command sets up HDFS and Spark on a CloudLab experiment over SSH and checks
the result.

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

In the CloudLab portal, create a profile from `profile.py` (Experiments, Create
Experiment Profile) and start an experiment from it. Choose the number of nodes
(`node0` plus the workers) and the hardware type (`m400` by default). All nodes
run Ubuntu 22.04 on one LAN, `node0` to `nodeN` at `10.10.1.1` onwards.

## Deploy

Requirements on your machine: `ssh`, `tar`, and the experiment's SSH key in an
agent. The first host must be `node0`, as shown in the experiment's List View.

```sh
scripts/cloudlab/deploy.sh user@<node0 host> user@<node1 host> user@<node2 host> ...
```

The script copies the repository to `~/EPL452` on every node, installs Java,
builds the Go programs on `node0`, starts the daemons and runs
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
* After a reboot run `deploy.sh` again: the daemons and the firewall rule do not
  survive it. This path has not been tested with a real reboot.

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
