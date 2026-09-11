# Worker poison task delivery defense

Source: internal/worker/poison.go — var poisonDefenseDisabled = os.Getenv("WADJET_POISON_DEFENSE") == "0", moved 2026-09-11 (#1026)

Poison-task defense (#318).

A task that OOM-kills its worker is redelivered by JetStream on restart
and kills the worker again: MaxDeliver/AckWait bound *transient* failures
but cannot tell "the worker died" from "the task killed the worker" —
a deterministic OOM spends the whole retry budget crashing the process,
and because the crash also prevents the terminal ack, a fast-enough kill
can loop beyond it (the two consecutive OOM'd server lifetimes in #318,
same task IDs in both).

The distinction needs a delivery-attempt record persisted OUTSIDE the
dying process. JetStream metadata already carries the delivery number;
what it cannot say is whether the PRIOR delivery began executing and
never finished. The worker therefore writes a breadcrumb to NATS KV
(same store that survives the restart and replays the task in the first
place) immediately before executing, and clears it on any graceful
completion — success or a properly published failure. A crash leaves the
record behind, so:

	delivery 1                        → normal execution (record written)
	delivery ≥2, no record            → prior delivery was lost before
	                                    execution; normal (not poison)
	delivery ≥2, record, not degraded → the prior attempt died mid-
	                                    execution: retry under a REDUCED
	                                    memory budget so the ADR-0006
	                                    machinery (spill early, bounded
	                                    state) engages instead of the heap
	                                    profile that killed the worker
	delivery ≥2, record, degraded     → the degraded attempt died too:
	                                    QUARANTINE — fail the query with an
	                                    error naming the task and its
	                                    memory demand, and Term the message
	                                    so redelivery stops

The degraded rung is preferred over quarantining at first suspicion
because most "poison" tasks are memory-shaped, and a reduced budget
turns the retry into exactly the graceful degradation ADR-0006 promises;
a task that then fails its degraded attempt GRACEFULLY (e.g. the #325
non-convergence error) produces an actionable query error on its own.
Known imprecision, accepted: a healthy-but-slow task redelivered at
AckWait expiry while its first worker is still running also matches
"record present" — it executes degraded, which is wasteful but safe
(task outputs are TaskID-idempotent, and degradation changes scheduling
and spill, never results).

WADJET_POISON_DEFENSE=0 disables the whole mechanism.
