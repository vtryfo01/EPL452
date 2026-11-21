#!/usr/bin/env bash
# Run the client benchmark (cmd/clientbench and baseline/clientbench.py)
# against a Spark Connect server, then print the tables of summarize_client.py.
#
#   run_clientbench.sh [-r repeats] [-p starts] [-o file] [-- clientbench flags]
#
#   -r  repetitions [4]; the order of the clients alternates between them
#   -p  new processes timed per repetition and client [5]
#   -o  metrics file, appended to [~/results/client.jsonl]
#
# In every repetition each client first runs -p new processes that only
# connect and run SELECT 1, each timed from outside ("process": it includes
# starting the program, for PySpark Python and its imports), then one process
# with all the cases of clientbench. Flags after -- go to both clients, e.g.
# -- -queries 50 -sizes 1000,100000. It takes about 10 minutes with the defaults.
#
# Give the server as an IP address: for a host name, the Go client (gRPC-Go)
# also looks up a DNS TXT record before its first query, which takes as long
# as the DNS server needs to answer (docs/benchmarks.md).
#
# Settings: SPARK_REMOTE [sc://127.0.0.1:15002], PYSPARK_VENV [/opt/pyspark],
# GO_HOME [/opt/go, where build_go.sh installs Go if bin/clientbench is missing].
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
VENV=${PYSPARK_VENV:-/opt/pyspark}
export SPARK_REMOTE=${SPARK_REMOTE:-sc://127.0.0.1:15002}

repeats=4 starts=5 out=$HOME/results/client.jsonl
while getopts r:p:o: opt; do
  case $opt in
    r) repeats=$OPTARG ;;
    p) starts=$OPTARG ;;
    o) out=$OPTARG ;;
    *) sed -n '5,9p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

log() { echo "run_clientbench: $(date -u +%T) $*" >&2; }
die() { log "error: $*"; exit 1; }

[[ $repeats =~ ^[1-9][0-9]*$ ]] || die "-r must be a positive number"
[[ $starts =~ ^[0-9]+$ ]] || die "-p must be a number"
[[ -x $repo/bin/clientbench ]] || "$repo/scripts/cloudlab/build_go.sh"
[[ -x $VENV/bin/python ]] || die "no PySpark in $VENV: run scripts/bench/setup_pyspark.sh or set PYSPARK_VENV"

client() { # client <go|pyspark> <flags...>
  local c=$1
  shift
  case $c in
    go) "$repo/bin/clientbench" "$@" ;;
    pyspark) "$VENV/bin/python" "$repo/baseline/clientbench.py" "$@" ;;
  esac
}

mkdir -p "$(dirname "$out")"
{
  echo "== $(date -u +%FT%TZ) run_clientbench.sh -r $repeats -p $starts -o $out -- $*"
  echo "remote: $SPARK_REMOTE"
  echo "node: $(hostname), $(nproc) hardware threads, $(awk '/MemTotal/ {printf "%.0f GB", $2 / 1048576}' /proc/meminfo), $(lscpu | sed -n 's/^Model name: *//p' | head -1)"
  # not from the repository, whose profile.py would hide Python's profile module
  echo "pyspark: $(cd / && "$VENV/bin/python" -c 'import pyspark; print(pyspark.__version__)'), $("$VENV/bin/python" --version 2>&1)"
  echo "commit: $(git -C "$repo" rev-parse HEAD 2>/dev/null || echo unknown)"
} >>"${out%.jsonl}.env.txt"

clients=(go pyspark)
for ((rep = 1; rep <= repeats; rep++)); do
  order=("${clients[@]}")
  ((rep % 2)) || order=(pyspark go)
  for c in "${order[@]}"; do
    log "repetition $rep/$repeats: $c"
    for ((i = 0; i < starts; i++)); do
      begin=$(date +%s%N)
      client "$c" -cases connect -metrics "$out" -label "rep=$rep" 2>/dev/null || die "$c: connecting failed"
      end=$(date +%s%N)
      printf '{"bench":"client","client":"%s","case":"process","rows":1,"label":"rep=%d","ms":%d.%03d}\n' \
        "$c" "$rep" $(((end - begin) / 1000000)) $(((end - begin) / 1000 % 1000)) >>"$out"
    done
    client "$c" -metrics "$out" -label "rep=$rep" "$@" || die "$c: the benchmark failed"
  done
done

log "done: $out"
python3 "$repo/scripts/bench/summarize_client.py" "$out"
