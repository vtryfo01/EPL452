#!/usr/bin/env bash
# Run as geniuser on node0 (nohup). The experiment ends at 14:23 Cyprus time,
# so: parallel download, base datasets, weak inputs with 2 copies per worker,
# then the benchmarks (stopped by hand before the end).
set -uo pipefail
cd ~/EPL452 || exit 1
export SPARK_REMOTE=sc://node0:15002
raw=/mydata/datasets/raw/gutenberg
mirror='https://aleph.pglaf.org/cache/epub/{id}/pg{id}.txt'
FETCHERS=${FETCHERS:-8}
COPIES=${COPIES:-2}
mkdir -p "$raw" ~/results
log() { echo "pipeline: $(date -u +%T) $*"; }

log "downloading with $FETCHERS parallel fetchers"
tmp=$(mktemp -d)
grep -hv '^#' data/lists/classics-60.txt data/lists/sample-2000.txt > "$tmp/all.txt"
split -n r/"$FETCHERS" "$tmp/all.txt" "$tmp/part-"
for f in "$tmp"/part-*; do
  python3 scripts/data/fetch_gutenberg.py "$f" --out "$raw" --delay 1 --url-template "$mirror" > "$f.log" 2>&1 &
done
wait
tail -qn1 "$tmp"/part-*.log
log "download done: $(ls "$raw" | grep -c '^pg') books"

log "base datasets"
scripts/data/cluster_datasets.sh classics-60 sample-2000 || { log "FAILED: cluster_datasets"; exit 1; }
touch /mydata/datasets/.base-done

log "weak inputs, $COPIES copies of sample-2000 per worker"
scripts/data/weak_datasets.sh "$COPIES" 3 || { log "FAILED: weak_datasets"; exit 1; }

log "benchmarks"
scripts/bench/run_benchmarks.sh -r 3 > ~/results/bench.log 2>&1
log "benchmarks ended with $?"
