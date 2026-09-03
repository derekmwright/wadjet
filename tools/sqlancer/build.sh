#!/usr/bin/env bash
# Clone SQLancer, apply the wadjet dialect patch, install the WadjetProvider
# adapter sources, and build the runnable jar.
#
# Usage: tools/sqlancer/build.sh [target-dir]
#   target-dir defaults to /tmp/sqlancer-wadjet (kept out of the repo: this
#   clones a third-party GPLv3 tool, never vendored here).
#
# Prerequisites: java 17+ and mvn on PATH (see README.md "Prerequisites" for
# how to install both without root/apt on a locked-down box).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${1:-/tmp/sqlancer-wadjet}"

if ! command -v java >/dev/null 2>&1; then
    echo "java not found on PATH — see README.md Prerequisites" >&2
    exit 1
fi
if ! command -v mvn >/dev/null 2>&1; then
    echo "mvn not found on PATH — see README.md Prerequisites" >&2
    exit 1
fi

if [ -d "$TARGET_DIR/.git" ]; then
    echo "Reusing existing clone at $TARGET_DIR"
else
    echo "Cloning sqlancer/sqlancer into $TARGET_DIR"
    git clone --depth 1 https://github.com/sqlancer/sqlancer.git "$TARGET_DIR"
fi

for patch in "$SCRIPT_DIR"/patches/*.patch; do
    echo "Applying $patch"
    ( cd "$TARGET_DIR" && git apply --check "$patch" 2>/dev/null ) \
        && ( cd "$TARGET_DIR" && git apply "$patch" ) \
        || echo "  (already applied — skipping)"
done

echo "Installing WadjetProvider adapter sources"
mkdir -p "$TARGET_DIR/src/sqlancer/wadjet"
cp "$SCRIPT_DIR/adapter-src/sqlancer/wadjet/"*.java "$TARGET_DIR/src/sqlancer/wadjet/"

echo "Building (mvn package -DskipTests)"
( cd "$TARGET_DIR" && mvn -q package -DskipTests )

JAR="$TARGET_DIR/target/sqlancer-2.0.0.jar"
if [ -f "$JAR" ]; then
    echo "Built: $JAR"
else
    echo "Build finished but $JAR is missing — check the SQLancer version in target/ matches sqlancer-2.0.0.jar in this script" >&2
    exit 1
fi
