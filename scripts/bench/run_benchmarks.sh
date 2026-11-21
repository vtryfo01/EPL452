#!/usr/bin/env bash
# Run the scaling benchmarks on node0: both jobs with the Go programs and the
# PySpark baseline on 1 .. W Spark workers, one JSON line per run appended to
# a metrics file, then print the tables of summarize.py.
#
#   run_benchmarks.sh [-r repeats] [-s first] [-w W] [-c clients] [-j jobs] [-e experiments] [-o file] [-n]
#
#   -r  repetitions of every run [4]; keep it even, as the first run of a job
#       after the workers change is slower and each client should be first equally often
#   -s  number of the first repetition [1], to add repetitions to a file later
#   -w  largest number of workers W [all alive workers]
#   -c  clients [go,pyspark]
#   -j  jobs [wordcount,invertedindex]
#   -e  experiments [strong,weak]: strong = weak-W on 1 .. W workers,
#       weak = weak-n on n workers (n = 1 .. W)
#   -o  metrics file, appended to [~/results/runs.jsonl]
#   -n  only print the runs
#
# Every run executes the whole job and discards the result (-format noop).
# The number of workers is set with scripts/spark/use_workers.sh, followed by
# an untimed warm-up job. weak-W on W workers belongs to both experiments and
# runs once. The repetitions are the outer loop, so an interrupted benchmark
# still has complete rounds, and the order of the clients alternates between
# repetitions. A failed run is reported and skipped. It takes hours: run it in
# the background, e.g.
#
#   nohup scripts/bench/run_benchmarks.sh > ~/results/bench.log 2>&1 &
#
# Settings: MASTER [node0], DATA_URI [hdfs://$MASTER:9000/datasets],
# WARMUP [classics-60, the input of the warm-up job], PYSPARK_VENV [/opt/pyspark].
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
MASTER=${MASTER:-node0}
DATA_URI=${DATA_URI:-hdfs://$MASTER:9000/datasets}
WARMUP=${WARMUP:-classics-60}
VENV=${PYSPARK_VENV:-/opt/pyspark}
export SPARK_REMOTE=${SPARK_REMOTE:-sc://$MASTER:15002}

repeats=4 first=1 max_workers="" clients=go,pyspark jobs=wordcount,invertedindex experiments=strong,weak
out=$HOME/results/runs.jsonl dry=false
while getopts r:s:w:c:j:e:o:n opt; do
  case $opt in
    r) repeats=$OPTARG ;;
    s) first=$OPTARG ;;
    w) max_workers=$OPTARG ;;
    c) clients=$OPTARG ;;
    j) jobs=$OPTARG ;;
    e) experiments=$OPTARG ;;
    o) out=$OPTARG ;;
    n) dry=true ;;
    *) sed -n '6,17p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
  esac
done

log() { echo "run_benchmarks: $(date -u +%T) $*" >&2; }
die() { log "error: $*"; exit 1; }

# split_list <array> <name> <value> <allowed...> splits a comma-separated option.
split_list() {
  local -n words=$1
  local name=$2 value=$3 v
  shift 3
  words=()
  for v in ${value//,/ }; do
    [[ " $* " == *" $v "* ]] || die "unknown $name '$v' (allowed: $*)"
    words+=("$v")
  done
}
split_list client_list client "$clients" go pyspark
split_list job_list job "$jobs" wordcount invertedindex
split_list experiment_list experiment "$experiments" strong weak
((${#client_list[@]} && ${#job_list[@]} && ${#experiment_list[@]})) || die "nothing to run"
[[ $repeats =~ ^[1-9][0-9]*$ ]] || die "-r must be a positive number"
[[ $first =~ ^[1-9][0-9]*$ ]] || die "-s must be a positive number"
has() { [[ " ${experiment_list[*]} " == *" $1 "* ]]; }

if [[ -z $max_workers ]]; then
  max_workers=$(curl -fsS "http://$MASTER:8080/json/" | python3 -c 'import json, sys
print(sum(w["state"] == "ALIVE" for w in json.load(sys.stdin)["workers"]))') ||
    die "cannot ask the Spark master how many workers are alive; give -w"
fi
[[ $max_workers =~ ^[1-9][0-9]*$ ]] || die "-w must be a positive number"

# plan: "<workers> <input>" in the order they run within a repetition.
plan=()
for ((w = 1; w <= max_workers; w++)); do
  if has strong; then plan+=("$w weak-$max_workers"); fi
  if has weak && ! { has strong && ((w == max_workers)); }; then plan+=("$w weak-$w"); fi
done
total=$((repeats * ${#plan[@]} * ${#job_list[@]} * ${#client_list[@]}))
log "$total runs: $repeats x (workers:input ${plan[*]/ /:}) x (${job_list[*]}) x (${client_list[*]})"

# exists checks that an input directory is there, in HDFS or on the local disk.
exists() {
  case $DATA_URI in
    hdfs://*) "${HADOOP_HOME:-/opt/hadoop}/bin/hdfs" dfs -test -d "$DATA_URI/$1" ;;
    file://*) [[ -d ${DATA_URI#file://}/$1 ]] ;;
    *) [[ -d $DATA_URI/$1 ]] ;;
  esac
}

run_job() { # run_job <client> <job> <input> <extra flags...>
  local client=$1 job=$2 input=$3
  shift 3
  case $client in
    go) "$repo/bin/$job" -input "$DATA_URI/$input" "$@" ;;
    pyspark) "$VENV/bin/python" "$repo/baseline/$job.py" -input "$DATA_URI/$input" "$@" ;;
  esac
}

set_workers() {
  log "using $1 workers"
  "$repo/scripts/spark/use_workers.sh" "$1" >&2 || die "could not switch to $1 workers"
  run_job go wordcount "$WARMUP" -format noop 2>/dev/null || die "the warm-up job on $WARMUP failed"
}

if ! $dry; then
  [[ -x $repo/bin/wordcount && -x $repo/bin/invertedindex ]] || "$repo/scripts/cloudlab/build_go.sh"
  if [[ " ${client_list[*]} " == *" pyspark "* ]]; then
    "$repo/scripts/bench/setup_pyspark.sh"
  fi
  inputs=$(printf '%s\n' "$WARMUP" "${plan[@]#* }" | sort -u)
  for input in $inputs; do
    exists "$input" || die "no input $DATA_URI/$input (see scripts/data/weak_datasets.sh)"
  done
  mkdir -p "$(dirname "$out")"
  {
    echo "== $(date -u +%FT%TZ) run_benchmarks.sh $*"
    echo "metrics: $out"
    echo "node: $(hostname), $(nproc) hardware threads, $(awk '/MemTotal/ {printf "%.0f GB", $2 / 1048576}' /proc/meminfo), $(lscpu | sed -n 's/^Model name: *//p' | head -1)"
    echo "spark: $(head -1 "${SPARK_HOME:-/opt/spark}/RELEASE" 2>/dev/null)"
    echo "commit: $(git -C /local/repository rev-parse HEAD 2>/dev/null || echo unknown)"
  } >>"${out%.jsonl}.env.txt"
fi

n=0 failed=0 current=""
for ((rep = first; rep < first + repeats; rep++)); do
  order=("${client_list[@]}")
  if ((rep % 2 == 0)); then
    for ((i = 0; i < ${#client_list[@]}; i++)); do order[i]=${client_list[${#client_list[@]} - 1 - i]}; done
  fi
  for item in "${plan[@]}"; do
    read -r w input <<<"$item"
    if [[ $w != "$current" ]]; then
      $dry || set_workers "$w"
      current=$w
    fi
    for job in "${job_list[@]}"; do
      for client in "${order[@]}"; do
        n=$((n + 1))
        log "[$n/$total] repetition $rep: $job on $input, $w workers, $client"
        $dry && continue
        if ! run_job "$client" "$job" "$input" -format noop -metrics "$out" -label "workers=$w rep=$rep"; then
          failed=$((failed + 1))
          log "FAILED: $client $job on $input with $w workers (repetition $rep)"
        fi
      done
    done
  done
done
$dry && exit 0

"$repo/scripts/spark/use_workers.sh" >&2 || log "warning: could not switch back to all workers"
python3 "$repo/scripts/bench/summarize.py" "$out"
((failed == 0)) || die "$failed of $total runs failed"
log "done: $total runs in $out"
