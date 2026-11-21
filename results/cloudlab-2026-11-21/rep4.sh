#!/usr/bin/env bash
# Queue repetition 4 after the running benchmarks (pipeline2.sh) end, if they
# end early enough for it to finish before the experiment expires.
cd ~/EPL452 || exit 1
log() { echo "pipeline: $(date -u +%T) $*"; }
while pgrep -u geniuser -f "bash ./pipeline2.sh" >/dev/null; do sleep 20; done
if ((10#$(date -u +%H%M) <= 1133)); then
  log "repetition 4"
  scripts/bench/run_benchmarks.sh -s 4 -r 1 >> ~/results/bench.log 2>&1
  log "repetition 4 ended with $?"
else
  log "too late for repetition 4; skipped"
fi
