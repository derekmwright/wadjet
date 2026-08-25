#!/usr/bin/env bash
# Run one SQLancer invocation, always from a dedicated scratch working
# directory — never the caller's cwd.
#
# SQLancer writes its per-round reproduction logs to logs/wadjet/*.log,
# relative to wherever it's launched from. Launching it from an arbitrary
# cwd instead of the SQLancer build directory (this README used to just
# say "cd there first" — easy to miss, and a soak's own supervisor script
# missed it) landed ~800 stray log files in the main repo checkout, twice,
# during the 2026-08-25 standing soak (wadjet#289). This wrapper makes
# that impossible to forget: it always cds into a scratch dir before
# invoking java, so a caller that skips this script is the only way to
# reintroduce the mistake.
#
# Usage: tools/sqlancer/run.sh <args to pass to "java -jar $SQLANCER_JAR">
#
#   tools/sqlancer/run.sh \
#     --num-threads 1 --random-seed 42 --num-queries 1000 \
#     --num-tries 5000 --timeout-seconds 300 --query-timeout 8 \
#     --username wadjet --password wadjet \
#     wadjet --oracle NOREC --test-collations=false \
#     --connection-url postgresql://localhost:15432/wadjet
#
# env:
#   SQLANCER_JAR      default /tmp/sqlancer-wadjet/target/sqlancer-2.0.0.jar
#   SQLANCER_RUN_DIR  default a fresh `mktemp -d`; printed to stderr so you
#                     can find logs/wadjet/*.log there afterward
set -euo pipefail

JAR="${SQLANCER_JAR:-/tmp/sqlancer-wadjet/target/sqlancer-2.0.0.jar}"
if [ ! -f "$JAR" ]; then
    echo "SQLancer jar not found at $JAR — run tools/sqlancer/build.sh first (or set SQLANCER_JAR)" >&2
    exit 1
fi
if [ "$#" -eq 0 ]; then
    echo "usage: $0 <java -jar \$SQLANCER_JAR args...> — see this script's header comment" >&2
    exit 1
fi

# Resolve JAR to an absolute path BEFORE the cd below — a relative
# SQLANCER_JAR (or the relative default, if this script is ever invoked
# from somewhere the default doesn't hold) would otherwise be looked up
# relative to $RUN_DIR instead of the caller's original cwd, since it's
# only expanded when "java -jar $JAR" finally runs.
case "$JAR" in
    /*) ;;
    *) JAR="$(cd "$(dirname "$JAR")" && pwd)/$(basename "$JAR")" ;;
esac

RUN_DIR="${SQLANCER_RUN_DIR:-$(mktemp -d /tmp/sqlancer-run.XXXXXX)}"
mkdir -p "$RUN_DIR"
echo "tools/sqlancer/run.sh: running from $RUN_DIR (logs/wadjet/*.log lands there, never the repo checkout)" >&2

cd "$RUN_DIR"
exec java -jar "$JAR" "$@"
