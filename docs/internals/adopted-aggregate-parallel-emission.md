# Adopted aggregate parallel emission

Source: internal/engine/exec/aggregate_parallel_emit.go — parallelEmitToggle, moved 2026-09-11 (#1026)

Parallel emit of adopted partitions (G4 in
docs/benchmarks/high-card-aggregation-gap-2026-08-17.md).

Partitioned parallel aggregation (partitioned_agg.go) already gives each
worker ownership of a disjoint hash partition of the group-key space, and
MergeSink ADOPTS those partitions wholesale instead of re-inserting their
groups. But the emission threw the parallelism away: Next() streamed the
primary's own state and then each adopted partition ONE AT A TIME, on the
single goroutine of a serial downstream pipeline. At ~46 ns/group that
tail measured 16-35% of ClickBench Q33/Q34/Q35 wall clock — 100M groups
is >=4.6s of irreducibly serial work.

The partitions are disjoint by construction and each one's emission touches
only its own state, so the drain fans out cleanly: one goroutine per unit
(the primary's own state plus every adopted partition), each running the
SAME per-unit emission loop as before, handing finished batches to the
consumer over a bounded channel. The expensive half — key decoding,
writeAccToColumn, output batch construction — runs k-wide; the consumer
keeps doing the cheap half (downstream projection, Top-N insert), which
profiles at <2%.

What this does NOT change: the values in each row, or which groups appear.
It DOES change the ORDER batches leave the aggregate in (partitions
interleave instead of concatenating). That was never a contract — group
emission order already depends on morsel arrival order under parallel
aggregation — and any user-visible ordering comes from a downstream ORDER
BY, which is unaffected.

Spilled aggregates keep the serial path (see parallelEmitEligible): their
emission runs through the streaming partial-state merger, which owns file
cursors and retired off-heap registries with their own lifecycle.
