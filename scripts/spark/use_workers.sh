#!/usr/bin/env bash
# Restart Spark Connect so that jobs run on N workers, one executor on each.
# Run on node0. Without N, all workers are used.
#
#   use_workers.sh [N]
set -e

# info prints a value computed from the alive workers (w) of the Spark master.
info() {
  curl -s http://node0:8080/json/ | python3 -c "import json, sys
w = [x for x in json.load(sys.stdin)['workers'] if x['state'] == 'ALIVE']
print($1)"
}

ALIVE=$(info "len(w)")
CORES=$(info "w[0]['cores']")
N=${1:-$ALIVE}
if [ "$N" -lt 1 ] || [ "$N" -gt "$ALIVE" ]; then
  echo "error: asked for $N workers, $ALIVE alive"
  exit 1
fi

/opt/spark/sbin/stop-connect-server.sh
/opt/spark/sbin/start-connect-server.sh --conf spark.executor.cores=$CORES --conf spark.cores.max=$((N * CORES))

for i in $(seq 60); do
  USED=$(info "sum(x['coresused'] > 0 for x in w)")
  if [ "$USED" = "$N" ] && timeout 1 bash -c "</dev/tcp/node0/15002" 2>/dev/null; then
    echo "Spark Connect uses $N of $ALIVE workers"
    exit 0
  fi
  sleep 2
done
echo "error: executors on $USED workers, expected $N"
exit 1
