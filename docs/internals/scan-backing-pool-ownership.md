# Scan backing pool ownership

Source: internal/engine/scan/backing_pool.go — type BackingPool struct {, moved 2026-09-11 (#1026)

BackingPool is one scan source's free list of decoded row-group backings.

# Ownership rule

A backing may be handed to a later decode only when BOTH hold:

 1. RELEASED — the consumer side said it is finished. That is the morsel
    dispenser's retire callback for the parent batch (fired once every
    zero-copy view minted over it has retired, which is after the whole op
    chain AND the sink consume), or the serial fragment path's return from
    driver.push. This is what a claim check alone cannot supply: the
    decode-ahead ring (ADR-0015) decodes group N+1 while group N is being
    consumed, k morsel consumers read one parent concurrently, and
    HashJoinProbe.emitViewOutput's view columns read our columns through
    Vector.Base after Execute returned — none of those readers claim.

 2. UNCLAIMED — nobody kept it: neither RecordBatch.Detach on the batch we
    emitted nor Vector.Claim on any column, including a claim that arrived
    transitively through a derived batch (ColumnPrune, set-op emit, selView)
    or through Vector.Base from a view minted downstream over one of our
    columns. This is what a release signal alone cannot supply: Sort,
    Window, the hash-join build, SortMergeJoin and the spillable collector
    all retain past retire, and all of them Detach (ADR-0016).

The release is the liveness signal; the claim is the retention veto. The
backing is surrendered whole or not at all — one claimed column surrenders
the batch, permanently, because every column is reachable from the batch a
consumer kept.

# Identity, without a reference

The pool only ever takes back a batch it minted, at the generation it minted
it: a WSHF shuffle chunk, a row-based fallback batch, a batch from another
pool or a batch already recycled is ignored, so no second owner can be
created for storage someone else recycles.

That identity is a batch.MintStamp the pool writes ON the batch, never a
registry of outstanding pointers. A registry is a strong reference: the
batches this pool mints are whole decoded row groups (~280 MB each at SF100
lineitem widths), and most of the sources that own a pool have consumers
with NO release edge at all (the shuffle task's plain Next loop, the
hash-join build source, planner.StreamingSources) — every one of those would
pin its entire live set for the source's lifetime, invisibly to the memory
ledger, and any cap on the registry's size would silently turn reuse off
rather than bound the damage. The stamp inverts the direction: the pool
holds only its own idle list, a dropped batch (a bloom-filtered-to-nothing
row group, a ring discard at a cross-file boundary) is plain garbage the
moment the pipeline lets go of it, and a source whose consumer never
releases costs exactly one stamp per decode.

A pool is created only where a RELEASE EDGE exists — see the caller side in
internal/worker (batchRecyclerOf arms it). A pool without one could never
take a backing back, so it would be pure overhead.

See docs/design/scan-output-backing-reuse.md for the full statement and the
preconditions it rests on.
