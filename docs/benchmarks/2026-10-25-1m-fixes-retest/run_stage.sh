#!/bin/zsh
# Retest runner (2026-10-25): same shape as 2026-10-23-vector-1m/run_stage.sh,
# logging into this directory. Branch: fix/1m-findings @ 522de17.
set -eu
cd "$(dirname "$0")/../../.."
stage=$1
timeout=$2
shift 2
here="docs/benchmarks/2026-10-25-1m-fixes-retest"
log="$here/logs/$stage.txt"
{
    echo "=== stage $stage (fix branch $(git rev-parse --short HEAD)) ==="
    date
    uptime
    ps -Arcceo %cpu,time,comm | head -8
    df -h /System/Volumes/Data | tail -1
    echo "--- quiet gate ---"
    "$here/quiet_gate.sh" 900
    date
    echo "--- run ---"
} >> "$log"
env VECTRA_1M=1 VECTRA_1M_STAGE="$stage" "$@" \
    go test ./bench/ -run TestVector1M -v -count=1 -timeout "$timeout" >> "$log" 2>&1
echo "stage $stage exit=0" >> "$log"
tail -3 "$log"
