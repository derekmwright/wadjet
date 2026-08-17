#!/usr/bin/env bash
#
# Log-tailing SF100 watcher — the FALLBACK for deploys whose harness is not
# wired to the run-event queue (no --notify-sqs-url). Prefer watch-events.sh:
# it consumes what the harness pushes, so it cannot drift from the log format.
#
# Everything this script greps is pinned to the real emitters:
#   per-query line   cmd/tpch-bench/main.go runBenchmark's fmt.Printf:
#                    "Q%02d %-30s %8s  %5d rows  heap %+4dMB  gc %4.1fms  %s"
#                    (FAIL prints "FAIL: <err>", so the status is not anchored
#                    to end-of-line; see LINE_RE below)
#   run banner       "=== Run %d/%d ==="
#   run total        the Summary table's TOTAL row
#   coordinator log  /root/benchmark.log — `tee` target in the terraform
#                    coordinator user_data
#   bootstrap FATAL  "FATAL: $* failed after $max attempts" from the retry()
#                    helper in the same user_data, in /var/log/cloud-init-output.log
#
# Phases:
#   1. Startup verification (fail fast at t+5m): no bootstrap FATAL, the
#      binaries are staged, /root/benchmark.log exists and is advancing.
#   2. Tail: print each new query/run/TOTAL line as it appears.
#   3. End: the coordinator leaves the running state (run-benchmark.sh ends
#      with `shutdown -h now`; the coordinator's shutdown behavior is stop).
#
# Usage:
#   ./watch-sf100.sh <coordinator-instance-id> [region]
#   ./watch-sf100.sh "$(tofu -chdir=deploy/benchmark/terraform output -raw coordinator_instance_id)"
#
# Env: AWS_PROFILE (default citc), POLL_SECONDS (default 30)
#
# Exit: 0 when the coordinator stops (run over), 1 on a startup failure.

set -uo pipefail

IID="${1:?usage: watch-sf100.sh <coordinator-instance-id> [region]}"
REGION="${2:-us-east-2}"
PROFILE="${AWS_PROFILE:-citc}"
POLL="${POLL_SECONDS:-30}"
STARTUP_GRACE_SEC="${STARTUP_GRACE_SEC:-300}"

BENCH_LOG=/root/benchmark.log
CLOUD_INIT_LOG=/var/log/cloud-init-output.log
# Anchored to the emitters listed in the header comment. The duration class
# is deliberately loose ([0-9][0-9a-zµ.]*s) because time.Duration.String()
# renders 1.234s, 12ms, 1m32.5s and 1h2m3s from the same call site; the
# status is not end-anchored because a failure prints "FAIL: <err>".
# Verified against real runBenchmark output, including the vsig and SKIPPED
# lines it must NOT match.
LINE_RE='^Q[0-9]{2} .* [0-9][0-9a-zµ.]*s +[0-9]+ rows .*(OK|FAIL)|^=== Run [0-9]+/[0-9]+ ===|^ +TOTAL '

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# ssm_run executes a shell snippet on the coordinator and echoes its stdout.
# The snippet is passed as JSON (file://) rather than inline shorthand — the
# quoting in these commands does not survive the CLI's list parser.
ssm_run() {
  python3 -c 'import json,sys; json.dump({"commands":[sys.argv[1]]}, open(sys.argv[2],"w"))' "$1" "$WORK/params.json"
  local cmd_id
  cmd_id=$(aws ssm send-command \
    --profile "$PROFILE" --region "$REGION" \
    --instance-ids "$IID" \
    --document-name AWS-RunShellScript \
    --parameters "file://$WORK/params.json" \
    --timeout-seconds 60 \
    --query Command.CommandId --output text 2>"$WORK/ssm-err") || {
    echo "WARNING: send-command failed: $(cat "$WORK/ssm-err")" >&2
    return 1
  }
  local status
  for _ in $(seq 1 30); do
    sleep 2
    status=$(aws ssm get-command-invocation --profile "$PROFILE" --region "$REGION" \
      --command-id "$cmd_id" --instance-id "$IID" \
      --query Status --output text 2>/dev/null)
    case "$status" in
      Success|Failed|Cancelled|TimedOut) break ;;
    esac
  done
  aws ssm get-command-invocation --profile "$PROFILE" --region "$REGION" \
    --command-id "$cmd_id" --instance-id "$IID" \
    --query StandardOutputContent --output text 2>/dev/null
}

instance_state() {
  aws ec2 describe-instances --profile "$PROFILE" --region "$REGION" \
    --instance-ids "$IID" \
    --query 'Reservations[].Instances[].State.Name' --output text 2>/dev/null
}

field() { printf '%s\n' "$1" | grep "^$2=" | head -1 | cut -d= -f2-; }

echo "watching coordinator $IID (region=$REGION profile=$PROFILE)"
echo "startup verification, fail-fast at t+$((STARTUP_GRACE_SEC / 60))m..."

# ---- Phase 1: startup verification ----

# Escaped $( ... ) so the substitutions run on the coordinator, not here.
PROBE="printf 'fatal=%s\n' \"\$(grep -c FATAL $CLOUD_INIT_LOG 2>/dev/null | head -1)\"; printf 'bins=%s\n' \"\$(ls /usr/local/bin/tpch-bench /usr/local/bin/wadjet 2>/dev/null | wc -l)\"; printf 'lines=%s\n' \"\$(wc -l < $BENCH_LOG 2>/dev/null || echo 0)\""

START=$SECONDS
FIRST_LINES=-1
while true; do
  OUT=$(ssm_run "$PROBE")
  FATALS=$(field "$OUT" fatal)
  BINS=$(field "$OUT" bins)
  LINES=$(field "$OUT" lines)
  : "${FATALS:=0}" "${BINS:=0}" "${LINES:=0}"
  echo "  t+$((SECONDS - START))s: cloud-init FATALs=$FATALS binaries=$BINS/2 benchmark.log lines=$LINES"

  if [ "$FATALS" -gt 0 ] 2>/dev/null; then
    echo "== bootstrap FATAL on the coordinator:" >&2
    ssm_run "grep -n FATAL $CLOUD_INIT_LOG | tail -20" >&2
    echo "== fix the bootstrap and redeploy; tear down first" >&2
    exit 1
  fi
  if [ "$BINS" -ge 2 ] && [ "$LINES" -gt 0 ]; then
    if [ "$FIRST_LINES" -lt 0 ]; then
      FIRST_LINES=$LINES
    elif [ "$LINES" -gt "$FIRST_LINES" ]; then
      echo "== startup verified: binaries staged, benchmark.log advancing"
      break
    fi
  fi
  if [ $((SECONDS - START)) -ge "$STARTUP_GRACE_SEC" ]; then
    # Fail fast only on a genuine startup failure: no binaries or no log at
    # all. A log that merely hasn't grown between two probes is normal here —
    # catalog snapshot restore and the worker rendezvous are both quiet — so
    # that case falls through to the tail loop with a warning.
    if [ "$BINS" -ge 2 ] && [ "$LINES" -gt 0 ]; then
      echo "== binaries staged and benchmark.log exists but has not grown; tailing anyway" >&2
      break
    fi
    echo "== startup NOT verified after $((STARTUP_GRACE_SEC / 60))m (FATALs=$FATALS binaries=$BINS/2 lines=$LINES)" >&2
    echo "== last 30 lines of $CLOUD_INIT_LOG:" >&2
    ssm_run "tail -30 $CLOUD_INIT_LOG" >&2
    exit 1
  fi
  sleep "$POLL"
done

# ---- Phase 2: tail the harness output ----

CURSOR=0
IDLE=0
while true; do
  # $LINE_RE and the cursor expand here; $(wc ...) is escaped so it runs on
  # the coordinator.
  TAIL_CMD="printf 'lines=%s\n' \"\$(wc -l < $BENCH_LOG 2>/dev/null || echo 0)\"; tail -n +$((CURSOR + 1)) $BENCH_LOG 2>/dev/null | grep -E '$LINE_RE'"
  OUT=$(ssm_run "$TAIL_CMD")
  NEW_LINES=$(field "$OUT" lines)
  : "${NEW_LINES:=$CURSOR}"

  BODY=$(printf '%s\n' "$OUT" | grep -v '^lines=')
  if [ -n "$(printf '%s' "$BODY" | tr -d '[:space:]')" ]; then
    printf '%s\n' "$BODY" | while IFS= read -r l; do
      [ -n "$l" ] && echo "[$(date +%H:%M:%S)] $l"
    done
    IDLE=0
  else
    IDLE=$((IDLE + POLL))
    echo "[$(date +%H:%M:%S)] (no new query lines for ${IDLE}s; log at $NEW_LINES lines)"
  fi
  CURSOR=$NEW_LINES

  STATE=$(instance_state)
  if [ -n "$STATE" ] && [ "$STATE" != "running" ]; then
    echo "== coordinator is $STATE — run complete. Results: s3://<bucket>/results/<ts>/"
    echo "== grab worker journals BEFORE teardown: ./grab-worker-logs.sh results/<ts>"
    exit 0
  fi
  sleep "$POLL"
done
