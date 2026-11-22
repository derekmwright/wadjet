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

## JetStream intake: the lost wake-up stall

Source: internal/worker/worker.go — taskLoop, the single `consumer.Messages(jetstream.PullMaxMessages(1))` call. Issue #857.

This section lives here because this is the only internals document about how
JetStream delivers tasks to the worker; there is no separate intake document.
It records a KNOWN LIMITATION that is not fixed. Measured 2026-11-22; versions
and upstream status are as of that date.

**What happens.** A worker on JetStream task intake (the default when the gRPC
data plane is off) can sit idle for about 31 s on one lane while tasks are
queued for it. It then logs a WARN `task intake: iterator error` with the
error `nats: no heartbeat received`, and continues delivering. The stall
itself loses, drops or redelivers nothing: the tasks stay in the stream and
are delivered after it.

**Why.** A lost wake-up in the nats.go JetStream pull iterator
(`jetstream/pull.go`, verified in the v1.51.0 source). `pullMessages` sets
`fetchInProgress` to 1, publishes the pull request, then sets it back to 0;
`Next` sends a new pull (in `checkPending`) only when the client's pending
count is below the threshold AND `fetchInProgress` is 0, and only at the top
of each pass. With `PullMaxMessages(1)` every pull opens this window: if the
server's reply is consumed and `Next` is called again before the publishing
goroutine stores the 0, no new pull is sent and `Next` blocks with no pull
request outstanding at the server. Nothing then arrives until the iterator's
missing-heartbeat timer fires (2 × the 15 s pull heartbeat), after which
taskLoop backs off 1 s and the next `Next` sends the pull: 31.0 s measured.

- Wadjet selects nats.go **v1.51.0** (go.mod) with nats-server v2.12.15.
- The latest nats.go release at the time of writing, **v1.56.0**
  (2026-11-17), has the same code: `jetstream/pull.go` is byte-identical at
  v1.56.0 and at the upstream pull request's base.
- Upstream: issue [nats-io/nats.go#2240](https://github.com/nats-io/nats.go/issues/2240)
  (open; filed 2026-11-13 against v1.54.0) and pull request
  [nats-io/nats.go#2241](https://github.com/nats-io/nats.go/pull/2241)
  (open, not merged, in no release). The pull request adds the missing
  wake-up signal. Against a forced reproduction (a 50 ms delay inserted
  before the store of 0, the real worker, 20 runs per arm) v1.51.0 stalled
  20 of 20 runs and v1.51.0 with the pull request's change backported
  stalled 0 of 20.

**How an operator recognises it.** While it lasts, the lane's durable
consumer (`tasks`, `pritasks-leaf` or `pritasks-deep`, with the cluster id
inserted when one is configured) reports `num_pending > 0`,
`num_ack_pending = 0` and `num_waiting = 0` (no pull request waiting at the
server), while the worker is alive and idle. About 30 s after its last
delivery the worker logs the WARN above. The same three counters also
describe a lane whose worker is not running at all (measured with the worker
stopped), so the counters identify the stall only while the worker is up;
the WARN line is the worker's side of it. The durable is shared by every
worker on the lane, so `num_waiting = 0` means no worker has a pull
outstanding; when another worker on the lane is idle it takes the queued
tasks, and one worker's stall does not show in these counters (inferred from
the consumer configuration, not measured).
In the tests, the drain-failure message of `TestTaskIntakeDeliversAllWithoutLoss`,
`TestTaskIntakeDeliversTasksPublishedBeforeItBinds` and
`TestPriorityLaneDrains` prints these counters on one line:
`INTAKE-SIGNATURE lane=<lane> pending=<n> ack_pending=<n> redelivered=<n> num_waiting=<n> lost_wakeup_signature=<yes|no>`.

**How often.** No production rate is known. Measured: about 1 run in 40 to
80 under in-process CPU and GC contention in one experiment, 1 in 960 in
another on the same host the same day, and none in several hundred runs of
the intake tests alone; on the gate runner it failed an intake test in two
of three gate runs on 2026-11-22 (the race-detector HOOK lane).

**Consequences beyond the wait.**

- A query whose deadline is shorter than about 31 s can expire while its
  tasks sit queued.
- All three lanes (main, pri-leaf, pri-deep) use the same intake code, so
  priority tasks (dynamic-filter emitters) can be delayed the same way, which
  can delay the work waiting on them.
- About 31 s is the recovery of one isolated occurrence; it is not a bound on
  a query's added latency, since a lane can hit the window again after it
  recovers.
- Coordinator end-to-end tests that start a worker on JetStream intake are
  exposed in the same way.

**Mitigation considered and NOT adopted: a shorter pull heartbeat.** A
shorter `PullHeartbeat(h)` bounds each stall to 2h + 1 s (measured: 3.0 s at
h = 1 s, 5.0 s at 2 s, 11.0 s at 5 s) without removing the lost wake-up. It
was rejected (maintainer ruling, 2026-11-22) because the missing-heartbeat
error is also raised by a connection that goes silent without breaking, and
after two such trips a dispatch burst can drop a task client-side that the
server counts as delivered, so it stays unacked until the 10-minute AckWait
redelivers it. A shorter heartbeat lowers the silence that triggers that drop
from 30 s to 2h; measured with a stalling proxy, drops occurred at h = 1, 2
and 5 s and at the 15 s default. The heartbeat therefore stays at the
library default (15 s). That recovery-path defect is a separate open line on
issue #857 and is not described further here.

**Permanent correction and removal of this limitation.** A released nats.go
version that contains the fix (#2241 or an equivalent), adopted through a
dependency update and validated by the forced-window reproduction (and the
intake and mTLS tests). Until such a release exists the limitation stands; a
pinned backport would need its own justification. When the validated fix is
adopted, this section and the race-lane exception in
[docs/testing/RELEASE.md](../testing/RELEASE.md#race-lane-exception-for-the-natsgo-lost-wake-up)
are removed.
