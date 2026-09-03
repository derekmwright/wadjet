# ADR-0027: A spill gate proves it spilled, and a clone's spill artifacts belong to the primary

Status: Accepted (2026-09-01, the spill-correctness arc: #782, #779, #632,
#790; residual #788). Amended 2026-09-03 twice: the operational-lifecycle arc
added #791's third route and why its plan-time fix is refused, and the
spilled-arm arc added decision 8, which FIXES #791 in the direction that
amendment left open.

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
   columns the planner had shipped shape-only. Grouped shapes KEEP the
   buffer — their state grows with the key set, so moving input to disk is a
   real trade — and what the buffer had to learn instead is decision 8: a
   shape-only column stays shape-only across it.

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
   #789's moving floor. All eighteen answer on every run, so none of them
   carries the tolerance any more.

   **The BUDGET RAISE those cells came with was the other half of the same
   tolerance, and it ratcheted too** — the sweep ran every `joinBudget` cell at
   `spillMxBudget` as well and FAILED the family when all of them answered
   there, naming what to delete. **That ratchet fired and has been answered
   (2026-09-03).** A scan now holds a parquet file one row group at a time
   rather than whole (#789's file half, ADR-0006's 2026-09-03 amendment), all
   eighteen cells answer 20 of 20 runs at `spillMxBudget` on both the free and
   the `GOMAXPROCS=1` arms, and `joinBudget`, `spillMxJoinBudget` and the
   family ratchet are deleted. The sweep has one budget again.

   The RULE the ratchet encoded stands and is what to re-apply if a family ever
   needs a raise again: a cell that runs at a larger budget than the rest of the
   sweep is tolerating something, a raise nothing checks is indistinguishable
   from a raise nothing needs, and the ratchet belongs on the FAMILY rather than
   the cell — the raise is one decision for one family, and a per-cell judgement
   over a nondeterministic refusal is the split decision 6 forbids. Any error
   counts as "did not answer", which is the correct direction for a ratchet that
   fires only on unanimous success.

8. **A shape-only column stays shape-only across the raw-row buffer.**
   (Added 2026-09-03 with the #791 fix; it is the amendment the 2026-09-03
   operational-lifecycle amendment below said this direction would need.)

   The scan decodes a byte-array column as lengths-and-no-bytes when the
   planner proves every use of it reads its SHAPE — `COUNT(col)`,
   `LENGTH(col)`, `IS NULL`, the empty-string comparisons. The vector paths
   carried that faithfully; `copyShapeRange` propagates the mark rather than
   moving bytes that do not exist. The ROW paths could not. A grouped
   aggregate under pressure buffers its input through `RecordBatch.ToRows`,
   whose per-row box comes from `Vector.GetValue`, and the only answer
   GetValue had for such a row was the panic that says a value was read. So
   four correct queries answered only while they had memory to spare (#791).

   The box is `batch.ShapeOnlyLen`: **the length, and a refusal of the
   value.** Written back through `SetValue` it reconstructs a shape-only
   column with the same per-row lengths, so what comes out of the detour is
   what went in, and a consumer that then wants the bytes raises the same
   guard at the same place. `memory.SpillManager.SpillRows` carries it under
   a tag of its own (`spillTagShapeOnly`), for the reason `spillTagBytes`
   exists: down any value arm it would come back as bytes the file never
   held, which is #632's class one step worse.

   It is a TYPE of its own and not an `int`, because an int is
   indistinguishable from a value at every `switch v := x.(type)` in the
   tree — item 6's rule, that neither encoder may write bytes its own reader
   refuses, applied to a box.

   Three routes reach the raw-row buffer beside a shape-only column and all
   three are gated (a NON-SIMPLE aggregate, GROUPING SETS/ROLLUP, and a
   NULLABLE key that contains a NULL), together with a `LENGTH` shape that
   reads the carried length rather than only the null mask, and with the
   NON-NULLABLE twin that must keep never reaching the buffer.

   **The boundary is checked, not asserted.** GetValue no longer panicking
   means a shape-only column that DID reach a client would come back as an
   integer where a string belongs — loud turned silent, which a census
   forbids. The planner's analysis already makes that impossible (it refuses
   to run unless the plan's output comes from a Project or an Aggregate,
   whose output schema is a list in which a column is a VALUE use), so
   `CollectSink.Consume` now REFUSES a result batch carrying one, naming the
   column. `exec.TestACollectSinkRefusesAShapeOnlyResultColumn` is the
   fixture that attempts the impossibility.

   **The confusion is refused in BOTH directions.** A shape-only length
   landing on a column that holds values was refused from the start
   (`copyShapeRange`'s guard, and `setShapeOnlyLen`'s). The other order —
   value bytes written into a column already marked shape-only, through
   `Set`, `SetString`, `BulkSet`, `BulkCopy` or `SetFrom` — was SILENT, and
   produced a wrong LENGTH rather than an error: the offsets advance by the
   appended bytes while the earlier rows' offsets describe bytes that were
   never written, the pair goes descending, and `LengthAt`'s defence against a
   malformed pair answers 0. It was loud by accident before this decision,
   because GetValue panicked on the shape rows, and boxing the length took
   that accident away. `refuseValueIntoShapeOnly` is the mirror guard; zero
   bytes is not a value write, because that is how `WriteNullAt` advances a
   shape-only column's offsets.

   **The ClickBench Q28 family keeps the optimization.** Nothing in the
   planner changed: the plan-time decline this ADR refused below is still
   refused, and the corpus engagement check — `ShapeOnlyColumnsPlanned` must
   move somewhere in the corpus, `benchmarks/tpch/oracle_test.go:96`, inside
   `TestTPCHOptimizationInvariance` — still fires.

## Alternatives rejected

- Demoting to the keyed merge whenever a flush happened (#782 candidate b):
  lands the query on the O(state) in-memory merge for a condition unrelated
  to disjointness, and on the encoding path that decision 3 had to fix.
- Teaching the raw-row buffer about shape-only columns for the UNGROUPED
  case: the buffer has no reason to exist there (decision 4). The grouped
  case is decision 8, and the two dispositions are not in tension: the
  ungrouped buffer was deleted because it bought nothing, and the grouped
  one was taught because it buys the trade it exists for.
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
- #791 (grouped raw-row path reads a shape-only column) is FIXED by decision
  8 above, and the two `knownError` cells that pinned it are deleted — a
  knownError cell fails the moment its shape answers, so their deletion is
  the fix's proof. The analysis of the third route below stands as written
  and is why the fix took the buffer-teaching direction rather than the
  plan-time one.

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
  shapes, whose state grows with the key set. **That is the direction #791
  was taken in, on 2026-09-03: decision 8 is its amendment.**

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
