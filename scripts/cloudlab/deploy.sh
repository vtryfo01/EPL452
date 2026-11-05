#!/usr/bin/env bash
# Deploy HDFS and Spark on a CloudLab experiment over SSH, then check the cluster.
#
#   deploy.sh user@node0 user@node1 [user@node2 ...]
#
# The first host must be node0 (master); the others are the workers.
# A re-run keeps the data, restarts the Spark daemons and starts any missing
# HDFS daemon.
set -euo pipefail

(($# >= 2)) || { sed -n '4p' "$0" | sed 's/^# *//'; exit 2; }
master=$1
shift
workers=("$@")
MASTER_NAME=node0
repo=$(cd "$(dirname "$0")/../.." && pwd)
SSH=(ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

# on runs a command on a host and prefixes its output with the host name.
on() {
  local host=$1
  shift
  "${SSH[@]}" "$host" "$*" 2>&1 | sed -u "s/^/[${host#*@}] /"
}

# each runs a function on every worker at once and fails if any of them fails.
each() {
  local pids=() status=0
  for w in "${workers[@]}"; do "$1" "$w" & pids+=($!); done
  for p in "${pids[@]}"; do wait "$p" || status=1; done
  return $status
}

prepare_node() {
  tar -C "$repo" -cz scripts testdata cmd internal go.mod go.sum | "${SSH[@]}" "$1" 'rm -rf ~/EPL452 && mkdir ~/EPL452 && tar -xz -C ~/EPL452'
  on "$1" 'sudo -n apt-get -o DPkg::Lock::Timeout=300 update -q >/dev/null &&
    sudo -n env DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y -q openjdk-11-jre-headless openjdk-17-jre-headless >/dev/null'
}

start_worker() {
  on "$1" "HDFS_REPLICATION=$replication ~/EPL452/scripts/hdfs/setup_hdfs.sh datanode $MASTER_NAME &&
    ~/EPL452/scripts/spark/setup_spark.sh worker $MASTER_NAME"
}

# Every node must already resolve node0, and the first host must be it.
on "$master" "ip -4 -o addr | grep -qw \$(getent ahostsv4 $MASTER_NAME | awk 'NR == 1 {print \$1}')" ||
  { echo "deploy: $master is not $MASTER_NAME" >&2; exit 1; }

prepare_node "$master"
each prepare_node
on "$master" '~/EPL452/scripts/cloudlab/build_go.sh'

replication=$((${#workers[@]} >= 2 ? 2 : 1))
on "$master" "HDFS_REPLICATION=$replication ~/EPL452/scripts/hdfs/setup_hdfs.sh namenode $MASTER_NAME &&
  ~/EPL452/scripts/spark/setup_spark.sh master $MASTER_NAME"
each start_worker
on "$master" "~/EPL452/scripts/cloudlab/check_cluster.sh ${#workers[@]}"
