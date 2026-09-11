# Catalog dml atomic publication

Source: internal/storage/catalog/dml_commit.go — func (c *Catalog) CommitDML(_ context.Context, tableName string, newFiles []PendingFile, markers []DeleteMarker) error {, moved 2026-09-11 (#1026)

CommitDML commits one DML statement's whole manifest change in a SINGLE CAS:
the files it wrote and the delete markers that remove the rows they replace,
or neither.

Two properties, and both are load-bearing:

 1. **Validation, over files and over rows.** Every marker names a file the
    manifest STILL HOLDS at commit time (`ErrDMLTargetMoved`), and no marker
    names a (file, row) the manifest ALREADY MARKS (`ErrDMLRowSuperseded`).
    The first is the check `AddDeleteMarkers` never had — it decodes the
    manifest and never looks at `Partitions`. The second is the one #691
    left open, and it rests on an invariant the DML door keeps: a statement
    filters its scan through `DeletedRowsByFile` before it matches anything
    (`deleteOnce`, `updateOnce` and `readMergeTarget` all do, which is
    #674's rule), so it NEVER mints a marker for a row the manifest it read
    already marked. An incoming (file, row) that is marked here was
    therefore marked by another statement SINCE this one read, and that is
    exactly the conflict.

    Both predicates are exactly right rather than merely conservative. A
    concurrent write that did not touch this statement's files leaves its
    markers valid; two statements over DIFFERENT rows of the same file both
    commit, because their marker sets are disjoint. A blunt "the revision
    moved" test would be wrong in both directions — it fails on any
    unrelated write, and an UPDATE's own ingest moves the revision.

 2. **Atomicity.** An UPDATE or MERGE used to commit twice — the ingester's
    AddNewFiles per flushed file, then AddDeleteMarkers — so a refusal at
    the second commit left the replacement rows beside the originals they
    were supposed to replace. Here the replacement files ride in the same
    CAS as the markers, so a refused statement has written nothing to the
    manifest and can simply be retried.

What remains outside it: the parquet objects a refused attempt already wrote
stay in the object store, unreferenced, until the orphan sweep reclaims
them. That is a leak of bytes on a rare retry, never a wrong row — the
manifest is the only thing that decides which rows exist.
