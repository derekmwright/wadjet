#!/usr/bin/env bash
# profile-local-query.sh
#
# Runs the tpch-harness local S3 mode for one query while sampling CPU
# profiles from the coordinator + workers during active query time.
#
# Usage:
#   ./scripts/profile-local-query.sh q01
#
# Outputs under /tmp/wadjet-profiles/<query>-<timestamp>/:
#   coord.cpu.prof     — 30 s CPU profile from the coordinator
#   worker-N.cpu.prof  — per-worker CPU profiles
#   goroutines.txt     — coord goroutine dump
#   summary.txt        — pprof -top summary for each profile

set -euo pipefail

QUERY="${1:-q01}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="/tmp/wadjet-profiles/${QUERY}-${TS}"
mkdir -p "$OUT"

BIN="/home/dwright/Projects/caelum/wadjet_bin"
HARNESS_OUT="${OUT}/harness.json"

# Fire off the harness in the background so we can attach profilers while
# the query runs.
AWS_PROFILE=citc /home/dwright/Projects/caelum/tpch-harness \
    --mode=local --source=s3 \
    --bucket=wadjet-bench-sf10-use2 --region=us-east-2 \
    --endpoint=s3.us-east-2.amazonaws.com --ssl --data-prefix=tables/ \
    --wadjet-bin="$BIN" --pg-addr=:15499 --no-compare \
    --use-native-dag --queries="$QUERY" --out="$HARNESS_OUT" \
    > "${OUT}/harness.log" 2>&1 &
HARNESS_PID=$!
echo "harness pid=${HARNESS_PID} — waiting for cluster ready..."

# Wait until we see the "cluster ready" log line (data priming done).
while ! grep -q "cluster ready" "${OUT}/harness.log" 2>/dev/null; do
    if ! kill -0 "$HARNESS_PID" 2>/dev/null; then
        echo "harness exited before cluster ready"
        exit 1
    fi
    sleep 5
done
echo "cluster ready — sampling 30 s profiles"

# Discover the coord HTTP port from its process args.
COORD_HTTP=$(pgrep -af "wadjet_bin serve --mode=standalone" | \
    grep -oP -- '--http-addr=:\K\d+' | head -1)

# Grab coord goroutine dump + CPU profile in parallel.
if [ -n "$COORD_HTTP" ]; then
    echo "  coord http=:${COORD_HTTP}"
    curl -s "http://localhost:${COORD_HTTP}/debug/pprof/goroutine?debug=1" \
        > "${OUT}/goroutines.txt" &
    curl -s -o "${OUT}/coord.cpu.prof" \
        "http://localhost:${COORD_HTTP}/debug/pprof/profile?seconds=30" &
fi

# Worker metrics-addr ports.
i=0
for PORT in $(pgrep -af "wadjet_bin serve --mode=worker" | \
    grep -oP -- '--metrics-addr=:\K\d+'); do
    echo "  worker-${i} metrics=:${PORT}"
    curl -s -o "${OUT}/worker-${i}.cpu.prof" \
        "http://localhost:${PORT}/debug/pprof/profile?seconds=30" &
    i=$((i + 1))
done

wait
echo "profiles captured — waiting for harness to finish"

# Let harness finish normally.
wait "$HARNESS_PID" || true

# Emit -top summaries.
{
    echo "=== Wall-time + summary ==="
    tail -5 "${OUT}/harness.log"
    echo ""
    for prof in "${OUT}"/*.cpu.prof; do
        [ -s "$prof" ] || continue
        echo "=== ${prof##*/} ==="
        go tool pprof -top -cum -nodecount 20 "$prof" 2>&1 | head -30
        echo ""
    done
} > "${OUT}/summary.txt"

echo "done: ${OUT}"
ls -la "$OUT"
