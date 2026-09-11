# Benchnotify event delivery contract

Source: internal/benchnotify/benchnotify.go — package benchnotify, moved 2026-09-11 (#1026)
Superseded: The proposed (RunID, Event, Query, Try) key omits RunIndex, which the current TPC-H emitter supplies on query_completed and run_completed events across repeated runs. Include it to avoid dropping legitimate events. Shipping producer and consumer together does not structurally prevent schema drift.

Package benchnotify pushes benchmark lifecycle events to an SQS queue so
an operator can watch a remote run with a blocking receive loop instead of
grepping a remembered log format over SSM. Producer (the bench binaries)
and consumer (deploy/benchmark/watch-events.sh) ship in the same commit,
so the two cannot drift.

# Event schema

One JSON object per SQS message, single line, no envelope:

	{
	  "run_id":        "20260817-142233",  // results timestamp id: results/<run_id>/
	  "event":         "run_started" | "query_completed" | "run_completed" |
	                   "suite_completed" | "fatal",
	  "query":         "Q01",     // query_completed
	  "try":           1,         // query_completed, clickbench only (1 = cold)
	  "wall_seconds":  12.345,    // query_completed
	  "rows":          4,         // query_completed, tpch only
	  "ok":            true,      // query_completed
	  "run_index":     2,         // run_completed (1-based)
	  "total_runs":    3,         // run_started, run_completed
	  "total_seconds": 198.4,     // run_completed, suite_completed
	  "cold_seconds":  256.5,     // suite_completed, clickbench only (sum of try 1)
	  "hot_seconds":   164.4,     // suite_completed, clickbench only (sum of min(try2,try3))
	  "error":         "...",     // fatal
	  "ts":            "2026-08-17T14:22:33Z"  // RFC3339, always present
	}

Omitted fields are absent from the JSON, not zero-valued — a consumer
should treat a missing field as "not applicable to this event".

Event order for a TPC-H run: one run_started, then per run N
query_completed events followed by a run_completed, then one
suite_completed. For ClickBench: one run_started, then per query one
query_completed per try, then one suite_completed. Either suite can end
on a fatal instead — a fatal is always terminal.

# Delivery is at-least-once

The queue is a standard SQS queue, so delivery is at-least-once, not
exactly-once: a message can arrive more than once, with no ordering
guarantee across messages. Observed directly on the first SF100 run using
this emitter (2026-08-19, run_id 20260819-112820) — one run_started
delivered twice, 30 seconds apart, same run_id, same ts:

	07:28:36 {"run_id":"20260819-112820","event":"run_started","total_runs":1,"ts":"2026-08-19T11:28:20Z"}
	07:29:06 {"run_id":"20260819-112820","event":"run_started","total_runs":1,"ts":"2026-08-19T11:28:20Z"}

That run also emitted 78 query_completed events for 22 distinct queries
across 1 run, so duplicates are not rare enough to ignore by hope.

A consumer that only prints the stream (watch-events.sh) is unaffected.
Any consumer that counts events to track progress, accumulates timings,
or treats an event as an edge (e.g. run_started resetting state) MUST
dedupe on (RunID, Event, Query) before acting — Try additionally
distinguishes ClickBench's per-query retries. If a consumer needs
ordering as well as exactly-once delivery, use a FIFO queue with
content-based deduplication instead of a dedupe key.

# Fire-and-forget

Emission never affects the benchmark. Sends happen outside every timed
region, carry a short timeout (default 2s), take no retries beyond the
SDK default, and log a warning on failure. After a few consecutive
failures the notifier disables itself so an unreachable queue cannot add
wall time to a long suite. A nil *Notifier is a valid disabled notifier:
every method is a no-op, which is what an unset --notify-sqs-url yields.
