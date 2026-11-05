#!/usr/bin/env bash
# Check a deployed cluster from the master: HDFS, the Spark workers, and both
# Go jobs against the expected outputs of the small corpus.
#
#   check_cluster.sh <workers>
set -euo pipefail

WORKERS=${1:?usage: check_cluster.sh <workers>}
MASTER=${MASTER:-node0}
repo=$(cd "$(dirname "$0")/../.." && pwd)
expected=$repo/testdata/small/expected
export SPARK_REMOTE=sc://$MASTER:15002

log() { echo "check_cluster: $*" >&2; }
die() { log "FAILED: $*"; exit 1; }

"$repo/scripts/hdfs/check_hdfs.sh" "$WORKERS"
input=$("$repo/scripts/hdfs/load_hdfs.sh" -f "$repo/testdata/small/corpus")

# spark_workers prints "<alive workers> <alive workers running an executor>".
spark_workers() {
  curl -fsS "http://$MASTER:8080/json/" | python3 -c '
import json, sys
alive = [w for w in json.load(sys.stdin)["workers"] if w["state"] == "ALIVE"]
print(len(alive), sum(w["coresused"] > 0 for w in alive))'
}

for _ in $(seq 60); do
  alive=$(spark_workers 2>/dev/null | cut -d' ' -f1) || alive=0
  ((${alive:-0} >= WORKERS)) && break
  sleep 2
done
((${alive:-0} == WORKERS)) || die "$alive Spark workers alive, expected $WORKERS"
log "$alive Spark workers alive"

for _ in $(seq 60); do
  timeout 1 bash -c "</dev/tcp/$MASTER/15002" 2>/dev/null && break
  sleep 2
done

diff <("$repo/bin/wordcount" -input "$input" -dump) "$expected/wordcount.tsv" || die "wordcount output differs"
log "wordcount matches the expected output"
diff <("$repo/bin/invertedindex" -input "$input" -dump) "$expected/invertedindex.tsv" || die "invertedindex output differs"
log "invertedindex matches the expected output"

used=$(spark_workers | cut -d' ' -f2)
((used == WORKERS)) || die "executors on $used workers, expected $WORKERS"
log "executors on all $used workers"
log "OK"
