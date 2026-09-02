# ADR-0027: A spill gate proves it spilled, and a clone's spill artifacts belong to the primary

Status: Accepted (2026-09-01, the spill-correctness arc: #782, #779, #632, #790; residuals #788, #791)

## Context

Every pipeline breaker spills (ADR-0006), and the spilled path is the one
path no shape-based corpus exercises on purpose: a spill is a CONDITION,
not a query shape, so a correct plan over correct data goes wrong only when
a drain lands on a particular batch. #782 was found by a four-arm census
whose spilled arm answered a plain `GROUP BY` above a join with 15 rows
for PostgreSQL's 8, at a split point that moved from run to run. The arc
that closed it found four more defects in the same seam, every one silent,
every one pre-existing, and every one invisible to the gates that existed —
including two spill suites that compared two in-memory answers because the
budget they set never made the operator write a file.

Mechanisms, each with its instrumented evidence in the commit body:

- A grace-partitioned join's spilled probe rows were replayed AFTER the
  partitioned aggregate had adopted its clones, straight into the primary,
  so every key in the flush lived in two sinks and was emitted twice (#782).
- The partial-run header's encoding flags were read from the flat
  accumulator arrays, which the NULL-key migration clears; a drain on that
  batch wrote DECIMAL and FLOAT sums down the integer arm and read back
  zero — and a MIN that reads back 0 WINS the merge (#782's twin, V15/V12).
- An ungrouped aggregate took the raw-row buffer under pressure and
  materialized a shape-only column (#779).
- The raw-row spill writer rendered a BYTES value as the text `fmt` prints
  (#632).
- A morsel-parallel clone drains into FOUR artifacts — `drainedRuns`,
  `partialSpillFiles` (via `SpillSome` on a peer's behalf), `spillFiles`,
  `spillBuffer` — and the merge transferred one of them; the clone's `Close`
  dropped the rest: 5000 rows in, 1100 out, no error (#790, and its raw-row
  half).

## Decision

1. **Every spill artifact a clone owns is transferred to the primary at
   `MergeSink`, before any merge branch is chosen.** Ownership of a run is
   not a property of the branch that merges the in-memory state; the four
   lists move first, then the state merges however it merges. The
   partitioned-adoption branch keeps the clone and finalizes it, which is
   the one place a clone's artifacts stay with the clone.

2. **A late producer of rows routes to the sink that owns the key, before
   adoption.** Under partitioned aggregation a key lives in exactly one
   sink until the primary adopts; the join's deferred flush, the outer
   join's unmatched-build flush, and any producer added later go through
   `partitionAndConsumeOwned` before the merge, never into the primary
   after it. An unroutable batch sets `routeFallback`, and the demotion to
   a keyed merge runs AFTER every producer has run.

3. **A run header's encodings derive from the aggregate's resolved input
   type, latched when it is resolved — never from an array a path migration
   can clear.** `aggEncodings` is monotone; `buildPartialAggSpecs` reads it.

4. **An ungrouped aggregate never buffers its input.** Its state is one row
   of accumulators plus extra state; the buffer bought nothing and read
   columns the planner had shipped shape-only. Grouped shapes keep the
   buffer, and the grouped half of the shape-only problem is #791, pinned.

5. **A spill gate proves it spilled.** The type-matrix spill sweep
   (`wadjet.TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget`) asserts
   per-family engagement — aggregate drains, sort runs, window runs,
   raw-row files, join evictions — and a family that spilled nothing FAILS.
   Cells that cannot spill are named with their reason. A budget that does
   not bite is the documented anti-pattern (`container_group_key_test.go`)
   and it was repeated once more in this arc's own first draft.

6. **Condition-triggered defects are gated with test-only forcing knobs,
   with the reference taken DISARMED.** `exec.ForceAggDrainEvery(N)` puts a
   drain on a chosen batch (bypassing the #325 productivity gate, which is
   a performance guard); `exec.ForceSmallSpillRuns` lowers the sort, window
   and raw-row run floors a 1.2 MB fixture could never cross. Budget-driven
   runs stay in the sweep, replicated (five per cell), because the knob
   proves the mechanism and the budget proves the seam. Arming the knob on
   BOTH sides of a comparison cancels the defect — #790 was nearly missed
   that way.

## Alternatives rejected

- Demoting to the keyed merge whenever a flush happened (#782 candidate b):
  lands the query on the O(state) in-memory merge for a condition unrelated
  to disjointness, and on the encoding path that decision 3 had to fix.
- Teaching the raw-row buffer about shape-only columns for the ungrouped
  case: the buffer has no reason to exist there (decision 4).
- Narrowing BYTES `SetValue` against a string carrier: a shared coercion
  the ingest and expression paths rely on; the producer was fixed instead,
  and no producer of a rendered BYTES form remains (the review searched).

## Consequences and boundaries

- The stage DAG carries the class: with the knob armed, a budgeted worker
  on the base commit lost the DECIMAL and FLOAT sums (census
  `dag+budgeted-workers`). The shared fix covers it, and the census gate
  now arms the knob around its DAG arms. No fixture exists that pressures
  a worker WITHOUT the knob; that boundary is recorded in the gate.
- #788 is NOT fixed: a DATE group key has two identities inside one
  aggregate (the int-keyed drain writes `"14610"`, the boxed remainder
  writes `FormatDate` text), so the k-way merge emits each date twice with
  the totals intact. Patching one producer does not close it; it is
  ADR-0026's territory (one identity per key). The sweep cell is pinned
  with a ratchet scoped to the two switches the bug needs.
- #791 (grouped raw-row path reads a shape-only column) is loud, pinned as
  a known error, and fails the pin if it starts answering.
- The typematrix fixture has no negative numbers, so the sweep sees only
  the lost-MIN half of decision 3's defect; the exec-level gate carries
  negated twins.
- The forcing knobs are one relaxed atomic load per `Consume` and two
  package vars; `ForceSmallSpillRuns` is not parallel-safe and says so.
