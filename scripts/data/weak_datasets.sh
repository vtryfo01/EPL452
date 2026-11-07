#!/usr/bin/env bash
# Build the weak-scaling inputs on a deployed cluster, load them into HDFS and
# check both Go jobs on them. Run on node0 after cluster_datasets.sh.
#
#   weak_datasets.sh <copies-per-worker> [max-workers]    (default 3 workers)
#
# weak-<n> (n = 1 .. max-workers) is n * <copies-per-worker> complete copies of
# sample-2000 (scale_dataset.py --copies), so every worker gets the same amount
# of data. It is loaded into HDFS as /datasets/weak-<n> and checked with
# verify -copies against the reference of sample-2000, so the large local copy
# is deleted after loading. The last lines printed are a summary.
#
# Settings: DATA_ROOT [/mydata/datasets], SOURCE [sample-2000], MASTER [node0].
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
DATA_ROOT=${DATA_ROOT:-/mydata/datasets}
SOURCE=${SOURCE:-sample-2000}
MASTER=${MASTER:-node0}
export SPARK_REMOTE=${SPARK_REMOTE:-sc://$MASTER:15002}

log() { echo "weak_datasets: $*" >&2; }
die() { log "error: $*"; exit 1; }

per_worker=${1:-}
max_workers=${2:-3}
[[ $per_worker =~ ^[1-9][0-9]*$ && $max_workers =~ ^[1-9][0-9]*$ ]] ||
  { sed -n '5p' "$0" | sed 's/^# *//'; exit 2; }
src=$DATA_ROOT/processed/$SOURCE
[[ -f $src/_MANIFEST.json ]] || die "no $src; run cluster_datasets.sh first"

# seconds runs a command with its output on stderr and prints how long it took.
seconds() {
  local start=$SECONDS
  "$@" >&2 || return
  echo $((SECONDS - start))
}

summary=()
for ((n = 1; n <= max_workers; n++)); do
  copies=$((n * per_worker))
  out=$DATA_ROOT/processed/weak-$n
  log "weak-$n: $copies copies of $SOURCE"
  rm -rf "$out"
  python3 "$repo/scripts/data/scale_dataset.py" --src "$src" --copies "$copies" --out "$out"
  bytes=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["totals"]["bytes"])' "$out/_MANIFEST.json")
  uri=$("$repo/scripts/hdfs/load_hdfs.sh" -f "$out")
  rm -rf "$out"

  times=()
  for job in wordcount invertedindex; do
    log "weak-$n: checking $job"
    t=$(seconds "$repo/bin/verify" -job "$job" -input "$uri" -local "$src" -copies "$copies" -mode sample) ||
      die "weak-$n: $job does not match $copies copies of the reference (see above)"
    times+=("$job ${t}s")
  done
  summary+=("weak-$n: $copies copies, $((bytes / 1048576)) MiB, $uri, verify OK: ${times[*]}")
done

log "done"
printf '%s\n' "${summary[@]}"
