#!/usr/bin/env bash
# Wait until HDFS reports the expected number of live DataNodes, then write,
# read back and delete a small file.
#
#   check_hdfs.sh [expected-datanodes]    (default 1)
#
# Uses HADOOP_HOME [/opt/hadoop]; run it on any node configured by setup_hdfs.sh.
set -euo pipefail

EXPECTED=${1:-1}
HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
hdfs() { "$HADOOP_HOME/bin/hdfs" "$@"; }
log() { echo "check_hdfs: $*" >&2; }

live=0
for _ in $(seq 60); do
  live=$(hdfs dfsadmin -report -live 2>/dev/null | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p')
  live=${live:-0}
  ((live >= EXPECTED)) && break
  sleep 2
done
if ((live < EXPECTED)); then
  log "error: $live live DataNodes, expected $EXPECTED"
  hdfs dfsadmin -report >&2 || true
  exit 1
fi
log "$live live DataNodes"
hdfs dfsadmin -report -live | grep -E '^(Name|Hostname|DFS Remaining):' >&2
# After a restart the NameNode leaves safe mode ~30 s after enough blocks are reported.
timeout 300 "$HADOOP_HOME/bin/hdfs" dfsadmin -safemode wait >&2 || { log "error: still in safe mode"; exit 1; }

file=/tmp/check_hdfs.$$
msg="written by check_hdfs.sh on $(hostname) at $(date -u +%FT%TZ)"
echo "$msg" | hdfs dfs -put -f - "$file"
[[ $(hdfs dfs -cat "$file") == "$msg" ]] || { log "error: read back a different content"; exit 1; }
hdfs dfs -rm -skipTrash "$file" >/dev/null
log "OK: $(hdfs getconf -confKey fs.defaultFS) is writable and readable"
