# Catalog dropped table reclaim

Source: internal/storage/catalog/catalog.go — func (c *Catalog) FlushDroppedTableFiles(ctx context.Context, grace time.Duration) int {, moved 2026-09-11 (#1026)

FlushDroppedTableFiles physically deletes the data files of tables
DropTable removed at least grace ago (zero or negative flushes
everything pending, for tests). Three independent safety layers stand
between a pending path and the Delete call below; the first alone bounds
the blast radius to bytes wadjet wrote, and either of the next two alone
blocks the #494 review's reproduced data loss:

 0. Ownership (DropTable, upstream of this list at all): a path is only
    ever in pendingDrops if its FileEntry was EngineWritten — stamped by
    AddNewFiles and SwapFileForGC, never by the AddFiles registration
    path. Nothing an operator staged and merely registered can reach
    this function, whatever shape its path takes.
 1. The live-manifest guard, RE-OBSERVED per pending entry immediately
    before that entry's deletes (liveCatalogState, and only when
    something is actually DUE): a path referenced by
    ANY current table's manifest is never deleted, no matter how long
    its OLD incarnation has been gone. This is the load-bearing layer —
    it is what makes drop-then-re-register-the-same-files (#278's
    workflow) and Iceberg's RefreshTable (drop+recreate over the same
    warehouse files, every refresh) safe. Building the set ONCE up front
    and deleting against it was the review's second reproduced data
    loss: a re-registration landing after the set was built and before
    the Delete fired was invisible to it. Re-observation narrows that
    window from "the whole flush" to "one entry's delete batch"; it does
    not close it (see the residual note below).
 2. Defense in depth: a path is only ever a delete candidate if it
    falls under its OWN table's partition.TablePrefix(name) —
    "tables/<name>/..." — and only via this catalog's own configured
    store and bucket. This is a CONVENTION, not an impossibility:
    iceberg/reader.go's resolvePath strips the scheme AND the bucket
    off an absolute data-file URI, so a warehouse at
    s3://somebucket/tables/events/... resolves into exactly the
    guarded shape. It is a cheap second opinion on paths that are
    already owned, not the thing standing between an Iceberg warehouse
    and a delete — layer 0 is (everything Iceberg registers goes
    through AddFiles, so none of it is ever marked).

On top of those, this mirrors compaction.Compactor's own
deleteFromStore/FlushDeferredDeletes recreated-object guard: a path
whose object was modified after the drop was recorded is skipped,
since something has legitimately written there since.

RESIDUAL, stated plainly: pendingDrops is in-process, and the
re-observation is a read; nothing serializes it against a write. dropMu
guards only pendingDrops itself, not the Head/Delete calls below, so
this is NOT scoped to a DIFFERENT *Catalog instance — a second
goroutine calling AddFiles on THIS SAME *Catalog while the delete loop
is mid-entry is just as invisible, and was reproduced directly against
one instance. The window is one pending entry's WHOLE delete batch
(every Head+Delete pair over that entry's paths), not a single call.
cmd/wadjet's standalone mode has no in-process AddFiles caller sharing
a *Catalog with its BackgroundCompactor (its pgwire server opens a
separate wadjet.DB), so this is unreachable through that binary today;
an embedder calling db.Catalog().AddFiles beside its own
BackgroundCompactor reaches it. Layer 0 — ownership — is the layer that
does not depend on timing at all, which is why it, not this one, is
what bounds the blast radius. See
docs/adr/0020-drop-table-reclaim-is-opt-in.md.

Not called from within this package on any timer, and — unlike
compaction's own deferred-delete flush — not called unconditionally by
the production background sweep either: see
compaction.BackgroundConfig.ReclaimDroppedTables (opt-in, default off).
Not every process that can DROP a table runs that sweep against the
same *Catalog (an embedded wadjet.DB and a standalone pgwire DB each
hold their own), so leaving this off by default means "not reclaimed
yet" rather than "reclaimed here but not there" is the honest default
everywhere; a leaked object is an ops cleanup problem, where an
incorrectly deleted one is data loss. Like the compactor's own
pendingDeletes, this list is process-local — a crash before the grace
elapses leaves the files in place rather than losing track of them
destructively, the same trade compaction already makes. Returns the
number of files deleted.
