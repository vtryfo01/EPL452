#!/usr/bin/env bash
# Prepare the book datasets on a deployed cluster and check both Go jobs on
# them. Run on node0 after scripts/cloudlab/deploy.sh.
#
#   cluster_datasets.sh [dataset ...]    (default: classics-60 sample-2000)
#
# For each dataset (book list data/lists/<dataset>.txt):
#   1. download the books (resumable; already downloaded books are skipped;
#      books missing from the mirror are fetched from a second mirror, then
#      from the main site; books none of them has are left out);
#   2. preprocess them into $DATA_ROOT/processed/<dataset> (kept if present);
#   3. load that directory into HDFS as /datasets/<dataset> (replaced);
#   4. run WordCount and Inverted Index through Spark Connect and compare
#      them with the reference implementation (cmd/verify).
# The last lines printed are a summary with sizes and verify times.
#
# Settings: DATA_ROOT [/mydata/datasets], GUTENBERG_URL [a Gutenberg mirror,
# as the main site blocks bulk downloads], DOWNLOAD_JOBS [8 books at a time
# from it], DOWNLOAD_DELAY [1 s between the books of a job], MASTER [node0].
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
DATA_ROOT=${DATA_ROOT:-/mydata/datasets}
mirror='https://aleph.pglaf.org/cache/epub/{id}/pg{id}.txt'
second_mirror='http://mirrors.xmission.com/gutenberg/{dirs}/{id}/{id}-0.txt'
main_site='https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt'
GUTENBERG_URL=${GUTENBERG_URL:-$mirror}
DOWNLOAD_JOBS=${DOWNLOAD_JOBS:-8}
DOWNLOAD_DELAY=${DOWNLOAD_DELAY:-1}
MASTER=${MASTER:-node0}
export SPARK_REMOTE=${SPARK_REMOTE:-sc://$MASTER:15002}

log() { echo "cluster_datasets: $*" >&2; }
die() { log "error: $*"; exit 1; }

datasets=("$@")
((${#datasets[@]})) || datasets=(classics-60 sample-2000)
for d in "${datasets[@]}"; do
  [[ -f $repo/data/lists/$d.txt ]] || die "no book list $repo/data/lists/$d.txt"
done
[[ -x $repo/bin/verify ]] || die "no $repo/bin/verify; run deploy.sh (or scripts/cloudlab/build_go.sh) first"
[[ -d $DATA_ROOT ]] || sudo -n mkdir -p "$DATA_ROOT"
[[ -w $DATA_ROOT ]] || sudo -n chown "$(id -un):" "$DATA_ROOT"

# seconds runs a command with its output on stderr and prints how long it took.
seconds() {
  local start=$SECONDS
  "$@" >&2 || return
  echo $((SECONDS - start))
}

summary=()
for d in "${datasets[@]}"; do
  raw=$DATA_ROOT/raw/gutenberg
  out=$DATA_ROOT/processed/$d

  list=$repo/data/lists/$d.txt
  log "$d: downloading the books into $raw"
  python3 "$repo/scripts/data/fetch_gutenberg.py" "$list" --out "$raw" \
    --jobs "$DOWNLOAD_JOBS" --delay "$DOWNLOAD_DELAY" --url-template "$GUTENBERG_URL" ||
    log "$d: some downloads failed; trying the other sources"
  # The mirror lags behind the main site: take the few books it lacks from a
  # mirror with the original layout, then from the main site, which often
  # refuses cloud machines (HTTP 503).
  python3 "$repo/scripts/data/fetch_gutenberg.py" "$list" --out "$raw" --delay 1 --url-template "$second_mirror" ||
    log "$d: some downloads from the second mirror failed"
  python3 "$repo/scripts/data/fetch_gutenberg.py" "$list" --out "$raw" --delay 2 --url-template "$main_site" ||
    log "$d: some downloads from the main site failed"
  absent=$(python3 -c 'import os, sys
sys.path.insert(0, sys.argv[1])
from dslib import read_id_list
print(*(i for i in read_id_list(sys.argv[2]) if not os.path.exists(f"{sys.argv[3]}/pg{i}.txt")))' \
    "$repo/scripts/data" "$list" "$raw")
  [[ -z $absent ]] || log "$d: left out, as no source has them: $absent"

  if [[ -f $out/_MANIFEST.json ]]; then
    log "$d: keeping the processed dataset in $out"
  else
    log "$d: preprocessing into $out"
    rm -rf "$out.tmp"
    python3 "$repo/scripts/data/preprocess.py" --raw "$raw" --list "$repo/data/lists/$d.txt" \
      --out "$out.tmp" --name "$d"
    mv "$out.tmp" "$out"
  fi
  read -r docs bytes < <(python3 -c 'import json, sys
t = json.load(open(sys.argv[1]))["totals"]
print(t["docs"], t["bytes"])' "$out/_MANIFEST.json")

  log "$d: loading into HDFS"
  uri=$("$repo/scripts/hdfs/load_hdfs.sh" -f "$out")

  times=()
  for job in wordcount invertedindex; do
    log "$d: checking $job"
    t=$(seconds "$repo/bin/verify" -job "$job" -input "$uri" -local "$out") ||
      die "$d: $job does not match the reference (see above)"
    times+=("$job ${t}s")
  done
  summary+=("$d: $docs books, $((bytes / 1048576)) MiB, $uri, verify OK: ${times[*]}")
done

log "done"
printf '%s\n' "${summary[@]}"
