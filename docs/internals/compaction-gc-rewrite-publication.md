# Compaction gc rewrite publication

Source: internal/storage/compaction/compactor.go — func (c *Compactor) ForceCompactFile(ctx context.Context, tableName string, filePath string, gcIndices map[int64]bool) error {, moved 2026-09-11 (#1026)

ForceCompactFile rewrites a single data file, applying the delete markers
the manifest holds for it. Used by delete-marker GC to physically purge
deleted rows from files whose markers have aged out.

Safety invariants:
  - Write-before-delete: the new file is written to the object store before
    the old file leaves the manifest. On partial failure the new file may
    become an orphan in S3, but data is never lost.
  - ALL of the file's markers or none. The rewrite applies exactly the
    marker set the manifest held when it was read, and the publication
    (catalog.CommitCompaction, via SwapFileForGC) refuses if that set has
    moved since. The old contract — apply the GC-scanned indices, leave any
    that arrived since — was #894: a surviving marker names a row in a file
    that no longer exists, so no reader can apply it, the next sweep drops
    it as an orphan, and the replacement carries the deleted row for good.
    Removing a marker cannot remove a row from a file that already has it.
  - One conditional publication: the old file's removal, the replacement's
    addition, and the marker cleanup are a single validated CAS.
  - Per-file lock: prevents a double GC rewrite when two sweeps of THIS
    compactor overlap. It cannot exclude an independent compactor — that is
    what the commit-time input check is for (#895).

gcIndices is the GC scan's trigger, not the authority: it says this file has
aged markers worth rewriting. What actually gets applied is the manifest's
current marker set for the file, which is a superset when a DELETE landed
since the scan — and applying that newer delete is the right answer, not a
TOCTOU hazard.

A conflict is not an error to the caller: another writer got to this file
first, or a DELETE committed while the rewrite was being written. The
output is discarded and the rewrite is retried against the newer snapshot;
past maxGCRewriteAttempts it is left for the next GC sweep, which re-scans
from scratch. Compactor.PublicationConflicts counts those.
