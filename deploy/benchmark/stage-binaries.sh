#!/usr/bin/env bash
#
# Cross-compile wadjet + tpch-bench + security-bench for arm64 and stage on S3.
#
# Usage:
#   ./stage-binaries.sh                          # Stage to default bucket
#   ./stage-binaries.sh my-bucket us-east-2      # Stage to specific bucket/region
#
# Uploads to:
#   s3://<bucket>/bin/<git-sha>/wadjet
#   s3://<bucket>/bin/<git-sha>/tpch-bench
#   s3://<bucket>/bin/<git-sha>/security-bench
#   s3://<bucket>/bin/latest/wadjet
#   s3://<bucket>/bin/latest/tpch-bench
#   s3://<bucket>/bin/latest/security-bench

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUCKET="${1:-wadjet-bench-sf10-use2}"
REGION="${2:-us-east-2}"
GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
OUTDIR="$(mktemp -d)"

echo "Building wadjet + tpch-bench + security-bench for linux/arm64 (${GIT_SHA})..."
cd "$REPO_ROOT"
GOOS=linux GOARCH=arm64 go build -o "${OUTDIR}/wadjet" ./cmd/wadjet
GOOS=linux GOARCH=arm64 go build -o "${OUTDIR}/tpch-bench" ./cmd/tpch-bench
GOOS=linux GOARCH=arm64 go build -o "${OUTDIR}/security-bench" ./cmd/security-bench

# amd64 variants (ClickBench arc: the official listing hardware is
# c6a.4xlarge x86). Staged under bin/<ver>/amd64/; the terraform arch
# knob selects the path at instance bootstrap.
echo "Building linux/amd64 variants..."
mkdir -p "${OUTDIR}/amd64"
GOOS=linux GOARCH=amd64 go build -o "${OUTDIR}/amd64/wadjet" ./cmd/wadjet
GOOS=linux GOARCH=amd64 go build -o "${OUTDIR}/amd64/tpch-bench" ./cmd/tpch-bench

echo "Uploading binaries to s3://${BUCKET}/bin/{${GIT_SHA},latest}/..."
for ver in "$GIT_SHA" "latest"; do
  aws s3 cp "${OUTDIR}/wadjet" "s3://${BUCKET}/bin/${ver}/wadjet" --region "$REGION" --quiet
  aws s3 cp "${OUTDIR}/tpch-bench" "s3://${BUCKET}/bin/${ver}/tpch-bench" --region "$REGION" --quiet
  aws s3 cp "${OUTDIR}/security-bench" "s3://${BUCKET}/bin/${ver}/security-bench" --region "$REGION" --quiet
  aws s3 cp "${OUTDIR}/amd64/wadjet" "s3://${BUCKET}/bin/${ver}/amd64/wadjet" --region "$REGION" --quiet
  aws s3 cp "${OUTDIR}/amd64/tpch-bench" "s3://${BUCKET}/bin/${ver}/amd64/tpch-bench" --region "$REGION" --quiet
done

# Upload deploy scripts alongside binaries so instances don't depend on git clone
echo "Uploading deploy scripts to s3://${BUCKET}/bin/{${GIT_SHA},latest}/scripts/..."
for ver in "$GIT_SHA" "latest"; do
  aws s3 sync "${REPO_ROOT}/deploy/benchmark/" "s3://${BUCKET}/bin/${ver}/scripts/" \
    --region "$REGION" --quiet \
    --exclude "terraform/*" --exclude "*.tfstate*" --exclude ".terraform*"
  # Upload benchmark profiles (read by tpch-bench --config)
  aws s3 sync "${REPO_ROOT}/deploy/benchmark/profiles/" "s3://${BUCKET}/bin/${ver}/profiles/" \
    --region "$REGION" --quiet
done

WADJET_SIZE=$(du -h "${OUTDIR}/wadjet" | cut -f1)
TPCH_SIZE=$(du -h "${OUTDIR}/tpch-bench" | cut -f1)
SEC_SIZE=$(du -h "${OUTDIR}/security-bench" | cut -f1)
rm -rf "$OUTDIR"

echo "Done. wadjet=${WADJET_SIZE}, tpch-bench=${TPCH_SIZE}, security-bench=${SEC_SIZE}"
echo "  s3://${BUCKET}/bin/${GIT_SHA}/"
echo "  s3://${BUCKET}/bin/latest/"
