#!/usr/bin/env bash
#
# Canonical benchmark watcher: a blocking long-poll receive loop on the
# run-event queue the harness PUSHES to (internal/benchnotify, --notify-sqs-url).
#
# This exists because session-written polling watchers grep a remembered log
# format and drift from the harness that writes it. Here the producer
# (cmd/tpch-bench, cmd/clickbench-bench) and this consumer ship in the same
# commit, and the payload is JSON with a versioned field set — nothing to
# mis-grep. Use watch-sf100.sh only when a deploy predates the queue wiring.
#
# Usage:
#   ./watch-events.sh <queue-url> [region] [profile]
#   ./watch-events.sh "$(tofu -chdir=deploy/benchmark/terraform output -raw notify_queue_url)"
#
# Env:
#   AWS_PROFILE      — AWS profile (default: citc; the 3rd arg wins)
#   MAX_IDLE_MIN     — give up after this many minutes with no event
#                      (default 0 = wait forever)
#
# Output: one line per event, "<local HH:MM:SS> <event JSON>". Messages are
# deleted (one delete-message-batch call) BEFORE they are printed, so two
# watchers on one queue SPLIT the stream — run one.
#
# Exit: 0 on suite_completed, 1 on fatal, 2 on MAX_IDLE_MIN with no terminal
# event (the teardown signal).
#
# Delivery is at-least-once (standard SQS, not FIFO): a message can arrive
# more than once, with no ordering guarantee. Observed directly on the first
# SF100 run using this emitter (2026-08-19, run_id 20260819-112820) — one
# run_started delivered twice, 30 seconds apart, same run_id, same ts. This
# script only prints and deletes, so a duplicate is harmless here. Any OTHER
# consumer that counts events, accumulates timings, or treats an event as an
# edge (e.g. run_started resetting state) MUST dedupe on (run_id, event,
# query) before acting — see internal/benchnotify's package doc.
#
# Wait-loop trap: this script's OWN startup banner (two lines up) contains
# the literal words "suite_completed" and "fatal", so an external waiter
# doing a naive `grep -q "suite_completed\|fatal"` over this script's output
# fires immediately on startup, before any real event. Match the JSON event
# line instead, e.g. `grep -q '"event":"suite_completed"\|"event":"fatal"'`.
#
# Delete-after-visibility-expiry trap (#388): the first release-run use of
# this script deleted every message with `aws sqs delete-message`, one CLI
# invocation per handle, called AFTER the whole batch had already been
# parsed and printed. Each `aws` invocation is a fresh process (SSO/STS
# credential resolution, TLS handshake, no connection reuse), so a batch of
# 10 could easily spend several seconds just parsing+printing before the
# delete loop even started, then several more running 10 serial deletes.
# SQS's --visibility-timeout clock starts the moment receive-message
# returns; once it expires the message is redelivered to the next
# long-poll, which mints it a NEW receipt handle. A delete-message call
# using the OLD (now-stale) handle after that point returns HTTP 200 —
# success — but deletes nothing, silently. That is exactly what was
# observed: every `aws sqs delete-message` exited 0, yet the queue kept
# redelivering every ~30s, indefinitely. Two changes close this: delete
# happens as ONE delete-message-batch call immediately after receive
# returns (before any parsing/printing work), and the batch response's
# `Failed` list — which single delete-message has no equivalent of — is
# checked, so a stale-handle no-op is now loud instead of invisible.

set -uo pipefail

QUEUE_URL="${1:?usage: watch-events.sh <queue-url> [region] [profile]}"
REGION="${2:-us-east-2}"
PROFILE="${3:-${AWS_PROFILE:-citc}}"
MAX_IDLE_MIN="${MAX_IDLE_MIN:-0}"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "watching $QUEUE_URL (region=$REGION profile=$PROFILE)"
echo "exit: 0 = suite_completed, 1 = fatal, 2 = idle timeout"

LAST_EVENT=$SECONDS
while true; do
  # WaitTimeSeconds=20 is the long poll: this call blocks on the server
  # until a message arrives, so the loop costs one request per 20s idle.
  RESP=$(aws sqs receive-message \
    --profile "$PROFILE" --region "$REGION" \
    --queue-url "$QUEUE_URL" \
    --max-number-of-messages 10 \
    --wait-time-seconds 20 \
    --visibility-timeout 30 \
    --output json 2>"$WORK/err") || {
    echo "WARNING: receive-message failed: $(cat "$WORK/err")" >&2
    sleep 5
    continue
  }

  rm -f "$WORK/entries.json" "$WORK/lines" "$WORK/terminal"
  if [ -n "$RESP" ]; then
    # Parse once: build the delete-batch entries (Id/ReceiptHandle pairs)
    # and the buffered print lines in the same pass, but do NOT print here
    # — printing happens after the delete below, per the trap above.
    printf '%s' "$RESP" | python3 -c '
import json, sys, time
raw = sys.stdin.read().strip()
resp = json.loads(raw) if raw else {}
entries, lines, terminal = [], [], None
for i, m in enumerate(resp.get("Messages", [])):
    entries.append({"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]})
    body = m.get("Body", "")
    try:
        ev = json.loads(body)
    except ValueError:
        lines.append(time.strftime("%H:%M:%S") + " UNPARSEABLE " + body)
        continue
    # Reprint compactly: the producer already emits one line, this just
    # normalizes whitespace and drops any key ordering surprise.
    lines.append(time.strftime("%H:%M:%S") + " " + json.dumps(ev, separators=(",", ":")))
    name = ev.get("event")
    if name in ("suite_completed", "fatal"):
        terminal = name
with open(sys.argv[1], "w") as f:
    json.dump(entries, f)
with open(sys.argv[2], "w") as f:
    for line in lines:
        f.write(line + "\n")
if terminal:
    with open(sys.argv[3], "w") as f:
        f.write(terminal)
' "$WORK/entries.json" "$WORK/lines" "$WORK/terminal"
  fi

  # Delete FIRST, in one batch call, before printing anything or acting on a
  # terminal event — see the #388 trap above. delete-message-batch also
  # reports failures in its response, unlike single delete-message, so a
  # stale-handle no-op is caught here instead of vanishing.
  if [ -s "$WORK/entries.json" ] && [ "$(cat "$WORK/entries.json")" != "[]" ]; then
    LAST_EVENT=$SECONDS
    DELRESP=$(aws sqs delete-message-batch --profile "$PROFILE" --region "$REGION" \
      --queue-url "$QUEUE_URL" --entries "file://$WORK/entries.json" \
      --output json 2>"$WORK/delerr") || {
      echo "WARNING: delete-message-batch failed: $(cat "$WORK/delerr") (events will redeliver in 30s)" >&2
      DELRESP=""
    }
    if [ -n "$DELRESP" ]; then
      FAILED=$(printf '%s' "$DELRESP" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(json.dumps(d.get("Failed", [])))
')
      if [ "$FAILED" != "[]" ]; then
        echo "WARNING: delete-message-batch reported failed deletions (events will redeliver in 30s): $FAILED" >&2
      fi
    fi
  fi

  [ -s "$WORK/lines" ] && cat "$WORK/lines"

  if [ -s "$WORK/terminal" ]; then
    TERMINAL=$(cat "$WORK/terminal")
    echo "== $TERMINAL — watcher done"
    [ "$TERMINAL" = "fatal" ] && exit 1
    exit 0
  fi

  if [ "$MAX_IDLE_MIN" -gt 0 ] && [ $((SECONDS - LAST_EVENT)) -gt $((MAX_IDLE_MIN * 60)) ]; then
    echo "== no events for ${MAX_IDLE_MIN}m — giving up (check the coordinator, then tear down)" >&2
    exit 2
  fi
done
