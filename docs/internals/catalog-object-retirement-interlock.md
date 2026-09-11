# Catalog object retirement interlock

Source: internal/storage/catalog/retire.go — func (c *Catalog) RetireObjects(ctx context.Context, reqs []RetireRequest) map[string]RetireOutcome {, moved 2026-09-11 (#1026)

RetireObjects physically deletes objects that NO live manifest in this
catalog references, and preserves the bytes of every object it cannot prove
that about.

It is the one place an object is allowed to leave the bucket on a
compaction schedule, and it exists because "this table stopped referencing
the file" is not the same claim as "nothing references the file". #896 is
the difference: compaction removed a source from `events`'s manifest and
queued its bytes; a still-live `archive` registered the very same object
through AddFiles during the grace; the queue's only guard was the object's
LastModified, which registering unchanged bytes does not move. The queue
deleted a file a live table's manifest still names.

Three things stand between a queued path and the Delete call, and the order
they run in is the point:

 1. **The retirement mark, taken first.** Every candidate path is marked
    before anything is read. A registration naming a marked path is refused
    with ErrPathRetiring until the mark is released. A path with a
    registration already IN FLIGHT is not marked at all — it comes back
    RetireUnproven, and the caller tries again once that registration has
    landed and can be observed.
 2. **The live-manifest reference check**, over EVERY table in the catalog
    (`liveCatalogState`, shared with DROP reclaim). A path any current
    manifest names is RetireReferenced and is never deleted. Because the
    mark is already held, a registration that could invalidate this read
    cannot be running: it either finished before the mark (and this read
    sees it) or is refused.
 3. **The recreated-object guard**: an object written since the retirement
    was scheduled is not the object that was scheduled.

A catalog read that fails yields RetireUnproven for every path rather than
a delete against an incomplete picture. Doubt preserves bytes.

The residual, stated plainly: the mark is IN-PROCESS. It excludes a
registration through this same *Catalog — which is what #896 reproduced,
and what an embedder running a BackgroundCompactor beside its own AddFiles
calls reaches — and it does not exclude a DIFFERENT process registering the
path into a shared catalog. Closing that needs a catalog-side lease, which
the deferred-delete queue could not use anyway: the queue itself is
process-local, so another process's compactor never sees these paths.
