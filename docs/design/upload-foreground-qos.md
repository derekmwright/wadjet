# Background-upload QoS: drains yield to foreground queries

Status: v4 landed 2026-08-08. Kill switch: `WADJET_UPLOAD_QOS=0` (pins
the idle width and disables in-flight pausing — pre-QoS behavior).

## v4: in-flight streams honor the window

v3's gate applied only at slot ACQUISITION — up to 8 jobs admitted
during idle kept compressing (a core each) and PUTting full-speed
straight through the next query's protection window. That residual was
the cold coin-flip class: Q06 10.7s midpoint in the v3 validation arm,
and same-plan cold swings (Q04 +192%, Q05 11→32s) in every window
since.

v4 threads the gate INTO running jobs: both the s2 compression source
and the PUT body read through `governedReader`, which re-consults
`chunkGate` every `uploadChunkBytes` (1 MB). While `busy && window
open`, non-urgent in-flight streams freeze — compression stops (the s2
writer advances only as fast as its reads) and the PUT body stalls (the
connection idles safely under the 30-minute ResponseHeaderTimeout).
Escapes mirror the admission gate: urgent (demand-released) roots never
pause, and a JOB-TOTAL yield budget (`uploadHardCapMs`, shared between
admission waits and chunk pauses) guarantees progress under any query
arrival pattern. Synchronous fallback uploads (the query is blocked on
them) run ungoverned by construction (nil yield budget). Engagement
marker: `upload_pause_ms` on the shuffle io stats line, distinct from
the admission gate's `upload_yield_ms`.

### v4 SF100 validation (2026-08-08): correct, and inert in sequential suites

Two SF100 arms (results/20260808-{022440,025007}, the second with the
pause budget split from the admission budget after the shared budget
let backlogged jobs enter their streams already past the escape):
44/44 identical rows/vsigs, `upload_failed=0`, walls at parity — and
`upload_pause_ms=0` on every worker. The zero is STRUCTURAL, not a bug:
in a sequential suite, `CancelQuery` kills a query's remaining uploads
at its completion broadcast (the boundary-crossing tail — 365–780
files/worker in the ledger), and uploads in flight during a query's
protection window belong to that query itself (10,773 completed
during their own query's run) — correctly exempt. There is nothing
for the gate to pause unless queries OVERLAP.

Consequences: (1) v4 stays default-on as defense for concurrent /
multi-tenant workloads, where another root's drain genuinely crosses a
query's window — the exact case the gate governs. (2) **The
"in-flight-upload cold coin flip" attribution for Q04/Q06-class cold
variance in benchmark suites is REFUTED** — those uploads are either
same-root (exempt by design: v3 already re-widens after the 10s
window) or cancelled at the boundary. The cold-disturbance cause is
open again; remaining suspects are intra-query upload/scan contention
at the post-window full width (a v3 design choice), S3 GET contention,
and page-cache effects.

## v3 (same day): foreground-epoch yield clock

The v2 pair delivered suite −12.4% cold / −12.6% steady with zero
failures — but the per-job wait cap referenced ENQUEUE time, and a
heavy query's jobs burn that budget while their own query still runs,
so protection for the NEXT query was a coin flip (Q06 cold: control
2.6s, treatment 20.6s — the residual lottery). v3 references the
FOREGROUND EPOCH instead: the first task of each NEW root query opens a
`uploadProtectMs` (10s) protection window; the busy width applies only
inside an open window. Every query gets the same head start, long
queries release the drain after 10s, repeat tasks of one query do not
re-open the window, and the per-job `uploadHardCapMs` (60s) backstop
guarantees progress under any query arrival pattern. Urgency (demand
release) still bypasses everything.

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
