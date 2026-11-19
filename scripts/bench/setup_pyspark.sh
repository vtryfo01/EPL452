#!/usr/bin/env bash
# Install the PySpark baseline's client (baseline/requirements.txt) in a
# virtual environment on this node, then run the baseline's tests, which run
# both jobs through Spark Connect on the small corpus. Re-running it is cheap.
#
#   setup_pyspark.sh
#
# Settings: PYSPARK_VENV [/opt/pyspark], MASTER [node0],
# SPARK_REMOTE [sc://$MASTER:15002], CHECK_INPUT [the small corpus in HDFS,
# hdfs://$MASTER:9000/datasets/corpus, loaded by check_cluster.sh].
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
VENV=${PYSPARK_VENV:-/opt/pyspark}
MASTER=${MASTER:-node0}
export SPARK_REMOTE=${SPARK_REMOTE:-sc://$MASTER:15002}
CHECK_INPUT=${CHECK_INPUT:-hdfs://$MASTER:9000/datasets/corpus}

log() { echo "setup_pyspark: $*" >&2; }

if [[ ! -x $VENV/bin/pip ]]; then
  mkdir -p "$VENV" 2>/dev/null || { sudo -n mkdir -p "$VENV" && sudo -n chown "$(id -un):" "$VENV"; }
  if ! python3 -m venv --clear "$VENV" >/dev/null 2>&1; then
    # Ubuntu ships venv (ensurepip) in a separate package.
    log "installing python3-venv"
    sudo -n apt-get -o DPkg::Lock::Timeout=300 update -q >/dev/null
    sudo -n env DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y -q python3-venv >/dev/null
    python3 -m venv --clear "$VENV"
  fi
fi
log "installing $(grep -v '^#' "$repo/baseline/requirements.txt" | tr '\n' ' ')into $VENV"
"$VENV/bin/pip" install -q --disable-pip-version-check -r "$repo/baseline/requirements.txt"

log "testing the baseline on $CHECK_INPUT through $SPARK_REMOTE"
if ! out=$(SPARK_TEST_INPUT=$CHECK_INPUT "$VENV/bin/python" "$repo/baseline/test_sparkjobs.py" 2>&1); then
  echo "$out" >&2
  log "error: the baseline tests failed"
  exit 1
fi
echo "$out" | tail -3 >&2
log "OK: run the baseline with $VENV/bin/python baseline/wordcount.py ..."
