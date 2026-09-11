# Batch poison release ownership

Source: internal/engine/batch/poison.go — var poisonOnRelease atomic.Bool, moved 2026-09-11 (#1026)

Poison-on-release: the adversarial half of the batch-reuse contract.

A pooled batch's storage is UNDEFINED the moment Release() hands it back.
Every operator that keeps a value past the call it arrived on is required
to own it — Detach() (which severs the pool link and claims every column)
or a deep copy. The contract is stated at (*RecordBatch).Detach and repeated
at each `b.Detach() // prevent pool recycle` site in package exec.

Nothing enforced it. Whether a violation produces a wrong answer depended on
whether the next batch happened to write over the same bytes with something
different — so the failure was data-dependent, invisible at small scale, and
invisible to every corpus gate. (*Vector).GetValue's TypeBytes arm returns a
slice ALIASING the column arena; MIN_BY over a BYTES column retains it; no
gate could see it because no corpus has a top-level BYTES column.

Poison mode makes the undefined behaviour DEFINED and LOUD: on Release, every
unclaimed value arena in the batch is overwritten with a recognisable pattern
before the batch reaches the pool. A retained alias then reads 0xA5 bytes and
-1 scalars instead of the values it thought it held, and the query answers
differently from the same query with poison off. Comparing the two runs is
the gate; see TestTypeMatrixBatchReuse in package wadjet.

Fairness. Poison writes exactly where a real recycle is free to write:

  - Only when b.pool != nil. Detach() and DetachPool() both nil the pool, so
    a batch whose shell anybody claimed is never poisoned.
  - Never through a view (Base != nil). A view owns no storage; writing
    through it would hit a base that may not be pooled at all.
  - Only the VALUE arenas. Offsets, null bitmaps and nested shape metadata
    are rewritten wholesale by Reset/resetVectorForReuse, so scribbling them
    would model a recycle that cannot happen.
  - Never a batch carrying a claim. Until #897 a per-VECTOR claim exempted
    nothing, because nothing in the pool honoured one: resetVectorForReuse
    cleared claimed and wrote over the arena on the next Get, on the stated
    assumption that "a batch only reaches a pool when nobody claimed it" —
    which the derived-batch cases (ColumnPrune, the set-op emitter,
    partitioned aggregation's selView) broke. Now retainsClaimedStorage
    vetoes such a batch at Release, so it is neither pooled nor poisoned,
    and poison still writes exactly where a real recycle may write.

Cost when off: one relaxed atomic load per batch — per 2048 rows, not per
row — and no writes. It is a correctness instrument, not a debug build:
keeping it in the normal build is what lets a gate flip it on around a query
and off again, which is the only way to compare the two runs in one process.
