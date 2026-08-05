# Background-upload QoS: drains yield to foreground queries

Status: v2 landed 2026-08-05. Kill switch: `WADJET_UPLOAD_QOS=0` (pins
the idle width — pre-QoS behavior).

## v2 (same day): bounded yield + demand release + breaker 404s

The v1 flat busy-width validated the acute fix spectacularly (SF100
arm 20260805-122925: Q06 18.6→2.2s, Q03 36.4→16.3s, Q18 steady
60.2→24.3s, Q21 cold 33.1→28.9s) but created CHRONIC upload
starvation: foreground is almost always active during a suite, so the
queue ran 2-wide for the whole run — upload_yield_ms ≈ 23–25 THOUSAND
seconds cumulative per worker, durability lagged minutes, consumers'
S3 fallbacks 404ed, the circuit breaker opened on those 404s, Q06/Q08
steady FAILED, and 28 uploads/worker were abandoned. Three additions:

1. **Bounded yield** (`uploadYieldMaxMs` = 10s): each job yields at
   most 10s before escalating to the idle width. Keeps the acute win
   (short queries complete inside the window) while capping durability
   lag.
2. **Demand release**: `awaitDurableObject` (a consumer whose S3
   fallback missed an upload-pending key) broadcasts
   `SubjectUploadRelease` for the key's root; the producing worker
   marks that root URGENT and its jobs bypass the gate. Reuses the
   lazy-policy release plumbing; idempotent.
3. **Breaker classification**: `ErrNotFound` no longer counts as an S3
   failure in `CircuitStore.onFailure` — a 404 is a definitive healthy
   answer, and under upload lag it is an expected race, not an
   availability signal. (Same family as the 2026-07-12
   context.Canceled exemption.)

## Problem: the previous query's drain starves the next query

Under streaming exchange + eager durability (ADR-0004/0007), a heavy
query's result returns while its workers are still flushing partition
sinks and background-uploading its shuffle files (~8 GB/worker for SF100
Q05). The upload manager ran a FIXED 8-wide semaphore, and each job is
not just a PUT: it re-reads the file from NVMe, s2-compresses it (a core
per job), then uploads. Eight such streams plus residual sink flushes
collide with the NEXT query's scans on the same cores, NVMe, and NIC.

Measured signature (2026-08-04/05 SF100 arms): Q06 — a trivial
lineitem scan+agg — ran **2.0s when the predecessor's drain finished
before it started (arm 3) and 17–20s when the drain overlapped (every
other window)**, with its scan tasks wall-blocked in the source
(`src_ms ≈ elapsed`, fragment phases showed the predecessor's tasks
completing 20s into Q06's window with 30–40s cumulative sink_ms).
Whether a short query lands clean is a RACE against the predecessor's
tail. This is the dominant cold-variance signature for short queries
(±30–50% same-plan swings, window-variance memo) and a plausible
contributor to "steady slower than cold" oddities and the 2026-08-04
S3 circuit-breaker episode (read-backs competing with a saturated
upload path).

## Fix

Adaptive upload width: `uploadSlotsIdle` (8) while the worker has no
task inside `Executor.Execute`, `uploadSlotsBusy` (2) while it does.
The gate is polled (50 ms) per job acquisition, covers compression and
PUT alike, and guarantees progress (busy width ≥ 1) so durability is
delayed, never lost. `Flush` (worker drain) runs with no foreground
tasks by construction → full width. Cumulative yield time is exported
as `upload_yield_ms` on the periodic `shuffle io stats` line — the A/B
engagement marker.

Semantics unchanged: uploads remain eager-policy; only their resource
share while a query is actively executing shrinks. The worst case adds
(bytes/2-slot-throughput) latency to durability of a just-finished
query — the coordinator's ErrInputLost classification and retry paths
are unaffected.

## Rejected alternatives

- `--shuffle-durability=lazy` as the fix: changes durability semantics
  suite-wide and shifts cost onto consumer-retry paths; the knob exists
  for workloads that want it, but the QoS keeps eager's guarantees.
- Benchmark-harness barrier (wait for drain between queries): masks the
  interference in benchmarks while production keeps paying it — the
  numbers would stop describing the system.
- OS-level I/O priorities: Go offers no per-goroutine scheduling class,
  and the contention is cross-resource (CPU + NVMe + NIC), which the
  slot width already models.

## Validation

- Unit: gate honors busy/idle widths and transitions, cancellation
  aborts waiters, yield time recorded (upload_qos_test.go, -race).
- SF100: A/B pair — expect Q06-class cold stabilization (short queries
  following heavy shufflers), `upload_yield_ms > 0` in treatment wlogs,
  suite-wide cold variance narrowing. Durability check: upload_failed
  stays 0, upload_done_bytes unchanged.
