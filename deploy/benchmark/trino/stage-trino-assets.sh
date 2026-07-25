#!/usr/bin/env bash
# Stage the Trino comparison assets (runner, DDL, queries) to S3 where the
# terraform-trino bootstrap pulls them.
#
# Usage: ./stage-trino-assets.sh [bucket] [region]
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
BUCKET="${1:-wadjet-bench-sf100-use2}"
REGION="${2:-us-east-2}"
aws s3 sync "$DIR/" "s3://${BUCKET}/bin/trino-assets/" \
  --region "$REGION" --delete \
  --exclude "stage-trino-assets.sh"
echo "staged to s3://${BUCKET}/bin/trino-assets/"
