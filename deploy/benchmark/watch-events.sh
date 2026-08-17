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
# deleted as they are printed, so two watchers on one queue SPLIT the stream —
# run one.
#
# Exit: 0 on suite_completed, 1 on fatal, 2 on MAX_IDLE_MIN with no terminal
# event (the teardown signal).

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

  rm -f "$WORK/handles" "$WORK/terminal"
  if [ -n "$RESP" ]; then
    printf '%s' "$RESP" | python3 -c '
import json, sys, time
raw = sys.stdin.read().strip()
resp = json.loads(raw) if raw else {}
handles, terminal = [], None
for m in resp.get("Messages", []):
    handles.append(m["ReceiptHandle"])
    body = m.get("Body", "")
    try:
        ev = json.loads(body)
    except ValueError:
        print(time.strftime("%H:%M:%S"), "UNPARSEABLE", body)
        continue
    # Reprint compactly: the producer already emits one line, this just
    # normalizes whitespace and drops any key ordering surprise.
    print(time.strftime("%H:%M:%S"), json.dumps(ev, separators=(",", ":")))
    name = ev.get("event")
    if name in ("suite_completed", "fatal"):
        terminal = name
sys.stdout.flush()
with open(sys.argv[1], "w") as f:
    f.write("\n".join(handles))
if terminal:
    with open(sys.argv[2], "w") as f:
        f.write(terminal)
' "$WORK/handles" "$WORK/terminal"
  fi

  # Delete before acting on a terminal event so a re-run of this script
  # does not replay the finished suite.
  if [ -s "$WORK/handles" ]; then
    LAST_EVENT=$SECONDS
    while IFS= read -r h; do
      [ -n "$h" ] || continue
      aws sqs delete-message --profile "$PROFILE" --region "$REGION" \
        --queue-url "$QUEUE_URL" --receipt-handle "$h" >/dev/null 2>&1 ||
        echo "WARNING: delete-message failed (event will redeliver in 30s)" >&2
    done < "$WORK/handles"
  fi

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
