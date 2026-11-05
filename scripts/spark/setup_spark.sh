#!/usr/bin/env bash
# Install, configure and (re)start Spark on one node.
#
#   setup_spark.sh master <master-host>   # standalone master
#   setup_spark.sh worker <master-host>   # worker
#
# Run as the experiment user. Needs Java 17 (installed by deploy.sh).
# Settings: SPARK_VERSION [4.0.5], SPARK_HOME [/opt/spark],
# SPARK_DATA_ROOT [/mydata/spark], SPARK_DRIVER_MEMORY [8g],
# SPARK_EXECUTOR_MEMORY [75% of this node's RAM; nodes must be the same type].
set -euo pipefail

ROLE=${1:-}
MASTER=${2:-}
[[ $ROLE =~ ^(master|worker)$ && -n $MASTER ]] || { sed -n '4,5p' "$0" | sed 's/^# *//'; exit 2; }

SPARK_VERSION=${SPARK_VERSION:-4.0.5}
SPARK_HOME=${SPARK_HOME:-/opt/spark}
DATA=${SPARK_DATA_ROOT:-/mydata/spark}
DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-8g}
EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-$(($(awk '/MemTotal/ {print $2}' /proc/meminfo) * 3 / 4 / 1048576))g}
JAVA=/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)
export SPARK_HOME

log() { echo "setup_spark: $*" >&2; }
die() { log "error: $*"; exit 1; }
make_dir() { sudo -n mkdir -p "$1" && sudo -n chown "$(id -un):" "$1"; }

[[ -x $JAVA/bin/java ]] || die "no Java 17 at $JAVA"

if [[ ! -x $SPARK_HOME/sbin/start-master.sh ]]; then
  name=spark-$SPARK_VERSION-bin-hadoop3
  tarball=/tmp/$name.tgz
  if [[ ! -s $tarball ]]; then
    for mirror in https://dlcdn.apache.org/spark https://archive.apache.org/dist/spark; do
      log "downloading $mirror/spark-$SPARK_VERSION/$name.tgz"
      curl -fsSL -o "$tarball.part" "$mirror/spark-$SPARK_VERSION/$name.tgz" && mv "$tarball.part" "$tarball" && break
    done
    [[ -s $tarball ]] || die "could not download Spark $SPARK_VERSION"
  fi
  make_dir "$SPARK_HOME"
  tar -xzf "$tarball" -C "$SPARK_HOME" --strip-components=1
fi

make_dir "$DATA"
mkdir -p "$DATA"/{logs,pids,work,local}

# Bind to the experiment LAN: the name of this node resolves to its public address.
master_ip=$(getent ahostsv4 "$MASTER" | awk 'NR == 1 {print $1}')
[[ -n $master_ip ]] || die "cannot resolve $MASTER"
local_ip=$(ip -4 -o route get "$master_ip" | sed -E 's/.* src ([0-9.]+).*/\1/')

cat > "$SPARK_HOME/conf/spark-env.sh" <<ENV
export JAVA_HOME=$JAVA
export SPARK_LOCAL_IP=$local_ip
export SPARK_LOCAL_HOSTNAME=$local_ip
export SPARK_MASTER_HOST=$MASTER
export SPARK_LOG_DIR=$DATA/logs
export SPARK_PID_DIR=$DATA/pids
export SPARK_WORKER_DIR=$DATA/work
ENV

cat > "$SPARK_HOME/conf/spark-defaults.conf" <<CONF
spark.master                       spark://$MASTER:7077
spark.local.dir                    $DATA/local
spark.driver.memory                $DRIVER_MEMORY
spark.executor.memory              $EXECUTOR_MEMORY
CONF

case $ROLE in
  master)
    "$SPARK_HOME/sbin/stop-master.sh" >/dev/null 2>&1 || true
    "$SPARK_HOME/sbin/start-master.sh"
    ;;
  worker)
    "$SPARK_HOME/sbin/stop-worker.sh" >/dev/null 2>&1 || true
    "$SPARK_HOME/sbin/start-worker.sh" "spark://$MASTER:7077"
    ;;
esac
log "done: $ROLE on $local_ip"
