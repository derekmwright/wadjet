# Batch shared vector claim pool boundary

Source: internal/engine/batch/pool.go — func retainsClaimedStorage(b *RecordBatch) bool {, moved 2026-09-11 (#1026)

retainsClaimedStorage reports whether returning b to a pool would hand out
storage somebody still reads.

The claim rides the VECTOR, not the batch shell, because the batch a
retaining consumer holds is not always the batch the producer emitted:
ColumnPrune, the set-op emitter and partitioned aggregation's per-partition
views all mint a DERIVED RecordBatch over the same *Vector pointers. Detach
on the derived batch severs only that shell's pool link and claims the
shared vectors; the ORIGINAL pooled batch still points at them and is still
released by whoever produced it (ChainDriver.ReleaseInputs, the shared-batch
refcount). Until #897 the pool ignored the claims: Get reset those same
vectors, cleared claimed, and overwrote storage the consumer was holding —
a retained [11 22] read back as [22 22].

So the pool is the boundary the claim has to hold at, and it holds WHOLE:
one claimed column vetoes the batch, exactly as the scan's BackingPool
already does (internal/engine/scan/backing_pool.go, "the backing is
surrendered whole or not at all"). Partial admission would mean minting
replacement vectors for the claimed columns, which is an allocation on the
release path to save an allocation on the next Get.

It is O(1) on every batch NewRecordBatch minted. Walking the columns —
let alone their bases and nested children — is per-Release work on a path
that does nothing else but a mutex and a slice append, and it MEASURED:
the first shape of this check cost 14-21% of a flat Get/Put cycle and
15-31% of a nested one (round-1 review P4). So the claim is recorded where
both ends can reach it instead: every vector under a batch carries that
batch's claimState, Claim sets its flag, and this reads the flag.

The walk survives for a batch NewRecordBatch did not mint — a derived shell
(ColumnPrune, the set-op emitter) has no claimState of its own. Those are
never pooled, so the walk is off the hot path; keeping it is what makes the
predicate total rather than conditional on how a batch was built.

The walk descends into nested children and view bases: Claim propagates
DOWN (Base, Child, Children), so a view over a ROW column's child claims
that child while the top-level column stays unclaimed.
