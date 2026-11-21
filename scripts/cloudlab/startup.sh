#!/usr/bin/env bash
# Set up this node when an experiment created from profile.py boots: every
# node does its own part of deploy.sh, then node0 checks the cluster and, if
# asked, prepares the datasets.
#
#   startup.sh <workers> [none|base|all]
#
# profile.py runs it from /local/repository at every boot, so the daemons come
# back after a reboot; the datasets are prepared only once. It copies the
# repository to ~/EPL452 (files that exist only there, such as results, are
# kept) and runs as the experiment's creator, switching to that user when
# CloudLab starts it as geniuser or root, so everything is in the home
# directory the creator logs in to. Progress is in ~/startup.log, and
# ~/startup.status holds "running: <step>", "OK" or "FAILED: <step>".
#
# Datasets: base = classics-60 and sample-2000 (cluster_datasets.sh), all =
# also weak-1 .. weak-<workers> (weak_datasets.sh 5). Last, node0 installs the
# PySpark baseline (setup_pyspark.sh).
# Settings: MASTER [node0], STARTUP_TIMEOUT [1800 s to wait for the other nodes],
# DATA_ROOT [/mydata/datasets], HADOOP_HOME [/opt/hadoop].
set -euo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKERS=${1:?usage: startup.sh <workers> [none|base|all]}
DATASETS=${2:-none}
[[ $DATASETS =~ ^(none|base|all)$ ]] || { echo "startup: datasets must be none, base or all" >&2; exit 2; }
MASTER=${MASTER:-node0}
TIMEOUT=${STARTUP_TIMEOUT:-1800}
DATA_ROOT=${DATA_ROOT:-/mydata/datasets}
HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
export DATA_ROOT HADOOP_HOME
src=$(cd "$(dirname "$0")/../.." && pwd)

# The HDFS daemons must not run as root, and the creator should find the
# cluster in their own home: run as the experiment's creator. CloudLab runs
# startup services as geniuser (urn:...+user+<creator> gives the creator).
me=$(id -un)
if ((EUID == 0)) || [[ $me == geniuser ]]; then
  creator=$(geni-get user_urn 2>/dev/null | awk -F+ '{print $NF}') || true
  if [[ -n $creator && $creator != "$me" ]] && id "$creator" >/dev/null 2>&1; then
    exec sudo -n -u "$creator" -H bash "$src/scripts/cloudlab/startup.sh" "$@"
  fi
  ((EUID != 0)) || { echo "startup: cannot find the experiment's creator" >&2; exit 1; }
fi
HOME=$(getent passwd "$(id -un)" | cut -d: -f6)
export HOME
cd "$HOME"
repo=$HOME/EPL452

exec >>"$HOME/startup.log" 2>&1
step=start
status() { echo "$*" >"$HOME/startup.status"; echo "startup: $(date -u +%FT%TZ) $*"; }
begin() { step=$1; status "running: $step"; }
trap 'rc=$?; ((rc == 0)) || status "FAILED: $step (exit $rc)"' EXIT

# wait_for runs a command every 10 s until it succeeds, for at most TIMEOUT s.
wait_for() {
  local deadline=$((SECONDS + TIMEOUT))
  until "$@"; do
    ((SECONDS < deadline)) || return 1
    sleep 10
  done
}

port_open() { timeout 1 bash -c "</dev/tcp/$MASTER/$1" 2>/dev/null; }
master_ready() { port_open 9000 && port_open 7077; }

# workers_ready: WORKERS live DataNodes and WORKERS alive Spark workers.
workers_ready() {
  local datanodes spark
  datanodes=$("$HADOOP_HOME/bin/hdfs" dfsadmin -report -live 2>/dev/null | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p') || true
  spark=$(curl -fsS "http://$MASTER:8080/json/" 2>/dev/null | python3 -c 'import json, sys
print(sum(w["state"] == "ALIVE" for w in json.load(sys.stdin)["workers"]))' 2>/dev/null) || true
  ((${datanodes:-0} >= WORKERS && ${spark:-0} >= WORKERS))
}

master_ip=$(getent ahostsv4 "$MASTER" | awk 'NR == 1 {print $1}')
[[ -n $master_ip ]] || { begin "resolving $MASTER"; exit 1; }
role=worker
ip -4 -o addr | grep -qw "$master_ip" && role=master
replication=$((WORKERS >= 2 ? 2 : 1))
status "starting as $role ($WORKERS workers, datasets: $DATASETS)"

if [[ $src != "$repo" ]]; then
  begin "copying the repository to $repo"
  mkdir -p "$repo"
  tar -C "$src" --exclude=.git -c . | tar -x -C "$repo"
fi

begin "installing Java"
"$repo/scripts/cloudlab/prepare_node.sh" "$MASTER"

if [[ $role == worker ]]; then
  begin "waiting for the HDFS NameNode and the Spark master on $MASTER"
  wait_for master_ready
  begin "starting the HDFS DataNode and the Spark worker"
  HDFS_REPLICATION=$replication "$repo/scripts/hdfs/setup_hdfs.sh" datanode "$MASTER"
  "$repo/scripts/spark/setup_spark.sh" worker "$MASTER"
  status OK
  exit 0
fi

begin "starting the HDFS NameNode and the Spark master"
HDFS_REPLICATION=$replication "$repo/scripts/hdfs/setup_hdfs.sh" namenode "$MASTER"
"$repo/scripts/spark/setup_spark.sh" master "$MASTER"

begin "building the Go programs"
"$repo/scripts/cloudlab/build_go.sh"

begin "waiting for $WORKERS DataNodes and Spark workers"
wait_for workers_ready

begin "checking the cluster"
"$repo/scripts/cloudlab/check_cluster.sh" "$WORKERS"

if [[ $DATASETS != none && ! -e $DATA_ROOT/.base-done ]]; then
  begin "preparing classics-60 and sample-2000"
  "$repo/scripts/data/cluster_datasets.sh"
  touch "$DATA_ROOT/.base-done"
fi
if [[ $DATASETS == all && ! -e $DATA_ROOT/.weak-done ]]; then
  begin "preparing weak-1 to weak-$WORKERS"
  "$repo/scripts/data/weak_datasets.sh" 5 "$WORKERS"
  touch "$DATA_ROOT/.weak-done"
fi

begin "installing the PySpark baseline"
"$repo/scripts/bench/setup_pyspark.sh"
status OK
