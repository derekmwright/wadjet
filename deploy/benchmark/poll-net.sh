#!/usr/bin/env bash
#
# One-shot NIC/ENA sample across running bench workers via SSM: cumulative
# rx/tx bytes plus ENA allowance-exceeded counters per worker. Use this
# for LIVE checks while an arm runs (is the cluster throttling right
# now?). For analysis-grade series, don't use this — worker user-data
# journals the same counters every 30s under the `ena-poll` tag and
# grab-worker-logs.sh ships them in wlogs (zcat wlog-*.gz | grep ena-poll).
#
# Interpretation (2026-08-09 diagnosis, docs/benchmarks/
# network-bound-diagnosis-2026-08-09.md): bw_in/out_allowance_exceeded
# climbing hundreds/sec = ENA-throttled; wall deltas without this context
# are uninterpretable on burst-networked instance types.
#
# Usage: ./poll-net.sh [name-tag-values] [region] [profile]
#   e.g. ./poll-net.sh                       # all 3 workers, us-east-2, citc
#        ./poll-net.sh wadjet-bench-coordinator

set -uo pipefail

TAGS="${1:-wadjet-bench-worker-0,wadjet-bench-worker-1,wadjet-bench-worker-2}"
REGION="${2:-us-east-2}"
PROFILE="${3:-citc}"

CMD_ID=$(aws ssm send-command \
  --profile "$PROFILE" --region "$REGION" \
  --document-name AWS-RunShellScript \
  --targets "Key=tag:Name,Values=$TAGS" \
  --parameters 'commands=["IF=$(ls /sys/class/net | grep -E \"^(eth|ens|enp)\" | head -1); echo \"$(hostname) $(date -u +%H:%M:%S) rx=$(cat /sys/class/net/$IF/statistics/rx_bytes) tx=$(cat /sys/class/net/$IF/statistics/tx_bytes) $(ethtool -S $IF 2>/dev/null | grep -E \"allowance_exceeded\" | tr -s \" \" | tr \"\\n\" \";\")\""]' \
  --query Command.CommandId --output text)

sleep 8
for iid in $(aws ec2 describe-instances --profile "$PROFILE" --region "$REGION" \
    --filters "Name=tag:Name,Values=$TAGS" Name=instance-state-name,Values=running \
    --query 'Reservations[].Instances[].InstanceId' --output text); do
  aws ssm get-command-invocation --profile "$PROFILE" --region "$REGION" \
    --command-id "$CMD_ID" --instance-id "$iid" \
    --query StandardOutputContent --output text 2>/dev/null | head -2
done
