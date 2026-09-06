#!/usr/bin/env bash
#
# Start a Wadjet worker node for distributed benchmarking.
# Run this on each worker instance after Terraform provisioning.
#
# Usage:
#   ./start-worker.sh <coordinator-private-ip>
#
# Example:
#   ./start-worker.sh 10.0.1.50

set -euo pipefail

COORD_IP="${1:?Usage: $0 <coordinator-private-ip>}"
NATS_URL="nats://${COORD_IP}:4222"

BUCKET="${WADJET_BUCKET:?Set WADJET_BUCKET}"
REGION="${WADJET_REGION:?Set WADJET_REGION}"
S3_ENDPOINT="s3.${REGION}.amazonaws.com"

echo "[$(date +%H:%M:%S)] Starting worker, connecting to coordinator at ${COORD_IP}..."

# A benchmark node must never mutate its read bucket. --mode=worker does not
# run the compaction sweep today (only standalone/coordinator do), so this is
# defence in depth against a future change or a mis-set mode — see #921, where
# a harness STANDALONE coordinator's default-on sweep tripled the SF10 fixture.
exec wadjet serve \
  --mode=worker \
  --nats-url="${NATS_URL}" \
  --storage.endpoint="${S3_ENDPOINT}" \
  --bucket="${BUCKET}" \
  --storage-type=s3 \
  --background-compaction=false \
  ${MEMORY_BUDGET:+--memory-budget=$MEMORY_BUDGET} \
  ${SPILL_DIR:+--spill-dir=$SPILL_DIR}
