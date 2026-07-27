#!/usr/bin/env bash
#
# Grab per-worker journals to S3 BEFORE tearing down a benchmark cluster.
# Worker logs carry the per-query decode-ahead sweep (pressure_stall_ms
# per query = page-cache sensor tax attribution), refault episode
# counters, upload-queue timing, and fragment phase lines — none of which
# survive teardown otherwise. The 2026-07-27 window-variance
# investigation stalled on exactly this: five comparable runs, zero
# worker logs.
#
# Usage:
#   ./grab-worker-logs.sh <results-prefix> [region] [profile]
#   e.g. ./grab-worker-logs.sh results/20260727-202225 us-east-2 citc
#
# Requires: workers tagged Name=wadjet-bench-worker-* still RUNNING
# (run this after the coordinator self-stops, before tofu destroy) and
# worker instance roles with s3:PutObject on the data bucket.

set -euo pipefail

PREFIX="${1:?usage: grab-worker-logs.sh <results-prefix> [region] [profile]}"
REGION="${2:-us-east-2}"
PROFILE="${3:-citc}"
BUCKET="${WADJET_BUCKET:-wadjet-bench-sf100-use2}"

CMD_ID=$(aws ssm send-command \
  --profile "$PROFILE" --region "$REGION" \
  --document-name AWS-RunShellScript \
  --targets "Key=tag:Name,Values=wadjet-bench-worker-0,wadjet-bench-worker-1,wadjet-bench-worker-2" \
  --timeout-seconds 600 \
  --parameters "commands=[\"IID=\$(curl -s -H \\\"X-aws-ec2-metadata-token: \$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')\\\" http://169.254.169.254/latest/meta-data/instance-id)\",\"journalctl --no-pager | gzip -c > /root/wlog-\$IID.gz\",\"aws s3 cp /root/wlog-\$IID.gz s3://$BUCKET/$PREFIX/wlogs/wlog-\$IID.gz --region $REGION\",\"echo WGRAB DONE \$IID\"]" \
  --query Command.CommandId --output text)

echo "grab dispatched: $CMD_ID — waiting for 3 uploads under s3://$BUCKET/$PREFIX/wlogs/"
for _ in $(seq 1 30); do
  N=$(aws s3 ls --profile "$PROFILE" "s3://$BUCKET/$PREFIX/wlogs/" 2>/dev/null | wc -l)
  [ "$N" -ge 3 ] && { echo "done: $N worker logs uploaded"; exit 0; }
  sleep 10
done
echo "WARNING: only $N worker logs after 5 min — check SSM invocation $CMD_ID" >&2
exit 1
