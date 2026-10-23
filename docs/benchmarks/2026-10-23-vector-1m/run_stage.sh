#!/bin/zsh
# Drives one 1M measurement stage: waits for the quiet gate, captures the
# environment (uptime, top CPU consumers, disk free) and the raw go test
# output side by side, so numbers and conditions land in the same file.
#
# Usage: ./run_stage.sh <stage> <timeout> [extra env assignments...]
#   stage   build|chain|compact|rebuild
#   timeout go test -timeout value, e.g. 7h
#
# Env (defaults are the recorded run's): VECTRA_1M_DIR, VECTRA_1M_N,
# VECTRA_1M_CP_BYTES, VECTRA_1M_CHAIN_VECS, VECTRA_1M_REBUILD_FROM,
# VECTRA_1M_REBUILD_MAX_SECS; see bench/vector_1m_test.go for the full
# knob list.
#
# Recorded sequence (each after the previous stage's library landed):
#   ./run_stage.sh build   8h VECTRA_1M_DIR=/tmp/vectra-1m
#   cp -R /tmp/vectra-1m /tmp/vectra-1m-asbuilt   # rebuild pairing input
#   ./run_stage.sh chain   8h VECTRA_1M_DIR=/tmp/vectra-1m
#   ./run_stage.sh compact 3h VECTRA_1M_DIR=/tmp/vectra-1m
#   ./run_stage.sh rebuild 2h VECTRA_1M_DIR=/tmp/vectra-1m \
#       VECTRA_1M_REBUILD_FROM=/tmp/vectra-1m-asbuilt \
#       VECTRA_1M_REBUILD_MAX_SECS=1800

set -eu
cd "$(dirname "$0")/../../.."   # repo root

stage=$1
timeout=$2
shift 2

here="docs/benchmarks/2026-10-23-vector-1m"
log="$here/logs/$stage.txt"

{
    echo "=== stage $stage ==="
    date
    uptime
    echo "--- top CPU consumers ---"
    ps -Arcceo %cpu,time,comm | head -8
    echo "--- disk ---"
    df -h /System/Volumes/Data | tail -1
    echo "--- quiet gate ---"
    "$here/quiet_gate.sh"
    date
    echo "--- run ---"
} >> "$log"

env VECTRA_1M=1 VECTRA_1M_STAGE="$stage" "$@" \
    go test ./bench/ -run TestVector1M -v -count=1 -timeout "$timeout" >> "$log" 2>&1

tail -5 "$log"
