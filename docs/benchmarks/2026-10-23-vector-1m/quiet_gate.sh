#!/bin/zsh
# Quiet-machine gate for the 1M measurement runs (2026-10-23).
#
# Blocks until the machine is quiet, then prints one line and exits 0:
#   load average < 3.5 AND no single process above 80% CPU,
#   both conditions stable for 60 consecutive seconds (12 x 5s samples).
# The user's machine routinely carries parallel load; every recorded
# stage run starts only after this gate passes (phase-0 lesson: single
# benchmarks, exclusive machine).
#
# Usage: ./quiet_gate.sh [max_wait_minutes]

set -u
LOAD_MAX=3.5
CPU_MAX=80
STABLE_SAMPLES=12   # 12 x 5s = 60s
max_wait=${1:-180}
deadline=$(( $(date +%s) + max_wait * 60 ))

stable=0
while true; do
    load=$(sysctl -n vm.loadavg | awk '{print $2}')
    topcpu=$(ps -Arcceo %cpu -o comm | awk 'NR>1 {if ($1+0 > m) m=$1+0} END {print m+0}')
    if (( $(echo "$load < $LOAD_MAX" | bc -l) )) && (( $(echo "$topcpu < $CPU_MAX" | bc -l) )); then
        stable=$(( stable + 1 ))
    else
        stable=0
    fi
    if (( stable >= STABLE_SAMPLES )); then
        echo "quiet gate passed at $(date '+%H:%M:%S') (load=$load topcpu=${topcpu}%)"
        exit 0
    fi
    if (( $(date +%s) > deadline )); then
        echo "quiet gate TIMED OUT after ${max_wait}min (load=$load topcpu=${topcpu}%)" >&2
        exit 1
    fi
    sleep 5
done
