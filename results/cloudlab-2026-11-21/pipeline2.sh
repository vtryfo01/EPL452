#!/usr/bin/env bash
# Continue the pipeline after the main Gutenberg site refused (503): fetch the
# 5 books the pglaf mirror lacks from the xmission mirror, then do the rest of
# cluster_datasets.sh for sample-2000 without its main-site download pass
# (classics-60 is already loaded and verified), the weak inputs and the
# benchmarks.
set -uo pipefail
cd ~/EPL452 || exit 1
export SPARK_REMOTE=sc://node0:15002
raw=/mydata/datasets/raw/gutenberg
COPIES=${COPIES:-2}
log() { echo "pipeline: $(date -u +%T) $*"; }

for id in 1090 1169 1173 1222 1318; do
  d=$(echo "${id%?}" | sed 's/./&\//g')
  curl -fsS -m 60 -o "$raw/pg$id.txt.tmp" "http://mirrors.xmission.com/gutenberg/$d$id/$id-0.txt" &&
    mv "$raw/pg$id.txt.tmp" "$raw/pg$id.txt" && log "pg$id from xmission: $(stat -c %s "$raw/pg$id.txt") bytes"
done

d=sample-2000
out=/mydata/datasets/processed/$d
log "$d: preprocessing"
rm -rf "$out.tmp"
python3 scripts/data/preprocess.py --raw "$raw" --list "data/lists/$d.txt" --out "$out.tmp" --name "$d" &&
  mv "$out.tmp" "$out" || { log "FAILED: preprocess"; exit 1; }
python3 -c 'import json, sys; t = json.load(open(sys.argv[1]))["totals"]; print("pipeline:", t)' "$out/_MANIFEST.json"
log "$d: loading into HDFS"
uri=$(scripts/hdfs/load_hdfs.sh -f "$out") || { log "FAILED: load"; exit 1; }
for job in wordcount invertedindex; do
  log "$d: checking $job"
  start=$SECONDS
  bin/verify -job "$job" -input "$uri" -local "$out" || { log "FAILED: verify $job on $d"; exit 1; }
  log "$d: $job verify OK in $((SECONDS - start)) s"
done
touch /mydata/datasets/.base-done

log "weak inputs, $COPIES copies of sample-2000 per worker"
scripts/data/weak_datasets.sh "$COPIES" 3 || { log "FAILED: weak_datasets"; exit 1; }

log "benchmarks"
scripts/bench/run_benchmarks.sh -r 3 > ~/results/bench.log 2>&1
log "benchmarks ended with $?"
