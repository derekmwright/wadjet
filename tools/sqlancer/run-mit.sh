#!/usr/bin/env bash
# Run the query generator against an isolated MIT embedded server.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUN_DIR="${SQLANCER_RUN_DIR:-$(mktemp -d /tmp/sqlancer-mit.XXXXXX)}"
mkdir -p "$RUN_DIR"
RUN_DIR="$(cd "$RUN_DIR" && pwd)"
case "$RUN_DIR/" in "$ROOT/"*) echo 'Choose a scratch run directory' >&2; exit 1;; esac
export SQLANCER_RUN_DIR="$RUN_DIR"
# Resolve the jar before changing the server's working directory.
if [[ -n "${SQLANCER_JAR:-}" ]]; then
    export SQLANCER_JAR="$(realpath "$SQLANCER_JAR")"
fi
cd "$RUN_DIR"
if (echo > /dev/tcp/127.0.0.1/15432) 2>/dev/null; then
    echo 'Port 15432 is already in use' >&2
    exit 1
fi
"$ROOT/dist/wadjet" serve --pg-addr=:15432 --storage-type=file \
    --data-dir="$RUN_DIR/data" --nats-store-dir="$RUN_DIR/nats" --nats-port=-1 --query-timeout=8s > server.log 2>&1 &
server_pid=$!
cleanup() {
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
ready=false
for ((attempt=0; attempt<100; attempt++)); do
    if ! kill -0 "$server_pid" 2>/dev/null; then
        cat server.log >&2
        exit 1
    fi
    if (echo > /dev/tcp/127.0.0.1/15432) 2>/dev/null; then
        ready=true
        break
    fi
    sleep 0.1
done
if [[ "$ready" != true ]]; then
    echo "Server did not listen within 10 seconds; see $RUN_DIR/server.log" >&2
    exit 1
fi
"$ROOT/tools/sqlancer/run.sh" --username wadjet --password wadjet "$@" \
    --test-collations=false --connection-url postgresql://localhost:15432/wadjet
