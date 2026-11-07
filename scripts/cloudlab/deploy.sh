#!/usr/bin/env bash
# Deploy HDFS and Spark on a CloudLab experiment over SSH, then check the cluster.
#
#   deploy.sh [--clean] user@node0 user@node1 [user@node2 ...]
#
# The first host must be node0 (master); the others are the workers.
# --clean first removes everything a previous deployment left on the nodes,
# including the data in HDFS. Without it, a re-run keeps the data, restarts the
# Spark daemons and starts any missing HDFS daemon.
set -euo pipefail

clean=false
if [[ ${1:-} == --clean ]]; then clean=true; shift; fi
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

clean_node() {
  on "$1" "pkill -f 'org.apache.[s]park' ; pkill -f 'org.apache.[h]adoop' ; sleep 2 ;
    sudo -n rm -rf /opt/spark /opt/hadoop /opt/go /mydata/spark /mydata/hdfs ~/EPL452 ~/go /tmp/spark-* /tmp/hadoop-* /tmp/go1* /etc/profile.d/hadoop.sh"
}

prepare_node() {
  tar -C "$repo" -cz scripts testdata data/lists cmd internal go.mod go.sum | "${SSH[@]}" "$1" 'rm -rf ~/EPL452 && mkdir ~/EPL452 && tar -xz -C ~/EPL452'
  on "$1" 'sudo -n apt-get -o DPkg::Lock::Timeout=300 update -q >/dev/null &&
    sudo -n env DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y -q openjdk-11-jre-headless openjdk-17-jre-headless >/dev/null'
  # Spark and HDFS have no authentication: on the public interface allow only
  # SSH and CloudLab's event service.
  on "$1" "pub=\$(ip -4 route show default | awk '{print \$5; exit}')
    lan=\$(ip -4 -o route get \$(getent ahostsv4 $MASTER_NAME | awk 'NR == 1 {print \$1}') | sed -nE 's/.* dev ([^ ]+).*/\1/p')
    if [[ \$pub != \$lan ]]; then
      rule=(INPUT -i \$pub -p tcp --syn -m multiport ! --dports 22,16505 -j DROP)
      sudo -n iptables -C \"\${rule[@]}\" 2>/dev/null || sudo -n iptables -I \"\${rule[@]}\"
    fi"
}

start_worker() {
  on "$1" "HDFS_REPLICATION=$replication ~/EPL452/scripts/hdfs/setup_hdfs.sh datanode $MASTER_NAME &&
    ~/EPL452/scripts/spark/setup_spark.sh worker $MASTER_NAME"
}

# Every node must already resolve node0, and the first host must be it.
on "$master" "ip -4 -o addr | grep -qw \$(getent ahostsv4 $MASTER_NAME | awk 'NR == 1 {print \$1}')" ||
  { echo "deploy: cannot reach $master, or it is not $MASTER_NAME" >&2; exit 1; }

if $clean; then
  clean_node "$master"
  each clean_node
fi

prepare_node "$master"
each prepare_node
on "$master" '~/EPL452/scripts/cloudlab/build_go.sh'

replication=$((${#workers[@]} >= 2 ? 2 : 1))
on "$master" "HDFS_REPLICATION=$replication ~/EPL452/scripts/hdfs/setup_hdfs.sh namenode $MASTER_NAME &&
  ~/EPL452/scripts/spark/setup_spark.sh master $MASTER_NAME"
each start_worker
on "$master" "~/EPL452/scripts/cloudlab/check_cluster.sh ${#workers[@]}"
