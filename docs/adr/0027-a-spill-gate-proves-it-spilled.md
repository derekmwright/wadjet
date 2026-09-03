# ADR-0027: A spill gate proves it spilled, and a clone's spill artifacts belong to the primary

Status: Accepted (2026-09-01, the spill-correctness arc: #782, #779, #632,
#790; residuals #788, #791). Amended 2026-09-03 (operational-lifecycle arc):
#791's third route, and why its plan-time fix is refused.

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

7. **A TOLERANCE in a spill gate ratchets, exactly like a pin.** `knownBug`
   and `knownError` already fail their cell when the pinned state stops
   reproducing — deleting the pin is the fix's proof. `budgetMayRefuse`, the
   sweep's third tolerance, did not: a cell could answer on every
   replication for release after release, and the tolerance — with the
   larger budget that came with it — would quietly outlive the defect that
   justified them. It now fails a cell that answers on every run, naming
   what to delete (#824). The threshold is the pins' own
   (`spillMxRatchetMinRuns`), because what is tolerated is itself
   nondeterministic: "no run refused" is evidence at full replication and a
   coin toss at one.

   The eighteen `join_group_by_*` cells that carried it were tolerating
   #789's moving floor. All eighteen answer on every run at
   `spillMxJoinBudget`, so none of them carries the tolerance any more.

   **The BUDGET RAISE those cells came with is the other half of the same
   tolerance, and it ratchets too.** A cell that runs at a larger budget than
   the rest of the sweep is tolerating something, and a raise nothing checks is
   indistinguishable from a raise nothing needs. So the sweep runs every
   `joinBudget` cell at `spillMxBudget` as well and FAILS the family when all
   of them answer on every run there, naming what to delete. It is a FAMILY
   ratchet, not a per-cell one, for two reasons: the raise is one decision for
   one family, and the condition cannot be judged per cell — three of these
   shapes reach both dispositions at `spillMxBudget` on a minority of runs
   (#789, open), so "this cell answered five times" is the split this decision
   forbids, while "all eighteen answered every time" is not. Any error counts
   as "did not answer", which is the correct direction for a ratchet that fires
   only on unanimous success: an unrelated breakage keeps it quiet rather than
   turning it red, and that breakage is caught by the cell's own budgeted run.

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
- #791 (grouped raw-row path reads a shape-only column) is loud, pinned,
  and fails the pin if it starts answering.

  **Amended 2026-09-03 (the operational-lifecycle arc): #791 has a THIRD
  route, and it is why the plan-time fix is REFUSED rather than deferred
  for want of effort.** The filing names two ways onto the raw-row path
  beside a shape-only column — a non-simple aggregate, and GROUPING
  SETS/ROLLUP. The third is a nullable GROUP BY key that actually CONTAINS
  a NULL: `migrateToGenericMap` clears `useIntGroupKey` and sets no
  replacement flag, so `canUseExternalMerge()` is false from that batch on
  and the aggregate takes the raw-row buffer with ONE SIMPLE aggregate and
  nothing else. Measured at 512 KiB in the sweep's own arm, twelve runs
  each: `SELECT g, COUNT(c_str) … GROUP BY g` (nullable, has NULLs) fails
  7 of 12 with the drain knob disarmed and 12 of 12 with
  `ForceAggDrainEvery(1)`; the identical shape on `GROUP BY id`
  (non-nullable) answers 12 of 12 either way. Both are now cells in the
  sweep, and the pair is the fixture: they differ only in the key's
  nullability, so a fix that closes one and not the other is visible.

  The filing prefers a plan-time decline — "have the planner decline the
  shape-only decode when the plan contains an aggregate that can reach the
  raw-row path". That cannot be written correctly. `simpleAggs` and the
  key-mode flags are latched from the FIRST BATCH'S VECTOR TYPES inside
  `resolveIndices`, not from the logical plan, and the third route depends
  on DATA — whether a nullable key contains a NULL. No plan-time fact
  answers that. A plan-time decline can therefore only be conservative to
  any GROUP BY on a nullable key, or any non-simple aggregate, or any
  GROUPING SET, beside a shape-only column — which disables the shape-only
  optimization for most `GROUP BY … COUNT(col)` shapes, including the
  ClickBench Q28 family the optimization was built for. That is a product
  trade, not a bug fix, so it is refused here and recorded.

  The other direction — teaching the raw-row buffer to carry lengths and
  refuse values — is NOT foreclosed by decision 4 above. Decision 4
  rejected buffer-teaching for the UNGROUPED case, on the grounds that the
  buffer bought nothing there; that argument does not transfer to grouped
  shapes, whose state grows with the key set. If #791 is taken, that is the
  direction, and it needs its own amendment.

  Both halves of the pair arm `ForceAggDrainEvery(1)`, which is decision 6
  applied to this defect: whether the aggregate reaches the raw-row buffer
  follows tracker timing, so the un-forced shape fails 7 runs in 12 and the
  forced one fails 12 in 12, while the non-nullable twin answers 12 in 12
  either way. The first draft instead pinned "at least one of five runs
  failed" — a TOLERANCE, which demands a particular outcome mix from an
  uncontrolled coin. It could not pass under `-short` (one run satisfies
  neither edge) and flaked 5 times in 50 at five runs. A pin whose trigger
  is a CONDITION bounds the condition; it never tolerates a split.
- The typematrix fixture has no negative numbers, so the sweep sees only
  the lost-MIN half of decision 3's defect; the exec-level gate carries
  negated twins.
- The forcing knobs are one relaxed atomic load per `Consume` and two
  package vars; `ForceSmallSpillRuns` is not parallel-safe and says so.
