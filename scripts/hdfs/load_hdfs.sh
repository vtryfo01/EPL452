#!/usr/bin/env bash
# Copy a processed dataset directory into HDFS and check the copy.
#
#   load_hdfs.sh [-f] <local-dataset-dir> [hdfs-dir]
#
#   -f          replace hdfs-dir if it already exists
#   hdfs-dir    default /datasets/<basename of the local directory>
#
# Every file is copied, including _MANIFEST.json (Spark ignores files that
# start with "_"). After the copy the size of every file is compared with the
# local one and fsck must report the directory HEALTHY. The last line printed
# is the URI to pass to the Go jobs with -input.
#
# Uses HADOOP_HOME [/opt/hadoop]; HDFS_REPLICATION overrides the replication
# of the copied files.
set -euo pipefail

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
hdfs() { "$HADOOP_HOME/bin/hdfs" "$@"; }
log() { echo "load_hdfs: $*" >&2; }
die() { log "error: $*"; exit 1; }

force=false
if [[ ${1:-} == -f ]]; then force=true; shift; fi
[[ $# -ge 1 && $# -le 2 ]] || { sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
src=${1%/}
[[ -d $src ]] || die "$src is not a directory"
dest=${2:-/datasets/$(basename "$src")}

shopt -s nullglob
files=("$src"/*)
((${#files[@]} > 0)) || die "$src is empty"

if hdfs dfs -test -e "$dest"; then
  $force || die "$dest already exists in HDFS (use -f to replace it)"
  log "removing the old $dest"
  hdfs dfs -rm -r -skipTrash "$dest" >/dev/null
fi

opts=()
[[ -n ${HDFS_REPLICATION:-} ]] && opts=(-D "dfs.replication=$HDFS_REPLICATION")
local_bytes=$(du -cb "${files[@]}" | tail -1 | cut -f1)
log "copying ${#files[@]} files ($local_bytes bytes) from $src to $dest"
start=$(date +%s%N)
hdfs dfs -mkdir -p "$dest"
hdfs dfs "${opts[@]}" -put "${files[@]}" "$dest/"
log "copied in $((($(date +%s%N) - start) / 1000000)) ms"

# Compare file sizes one by one.
declare -A remote
while read -r name size; do remote[$name]=$size; done < <(hdfs dfs -stat '%n %b' "$dest/*")
for f in "${files[@]}"; do
  name=$(basename "$f")
  [[ ${remote[$name]:-missing} == $(stat -c %s "$f") ]] ||
    die "$name: HDFS has ${remote[$name]:-no file}, local file has $(stat -c %s "$f") bytes"
done
((${#remote[@]} == ${#files[@]})) || die "$dest has ${#remote[@]} files, expected ${#files[@]}"

report=$(hdfs fsck "$dest" 2>/dev/null) || true
grep -q 'is HEALTHY' <<<"$report" || die "fsck does not report $dest as HEALTHY"
sed -n '/Replicated Blocks:/,/Erasure Coded/p' <<<"$report" |
  grep -E 'Total (size|files|blocks)|Average block replication' | sed 's/^ */  /' >&2
log "OK"
echo "$(hdfs getconf -confKey fs.defaultFS)$dest"
