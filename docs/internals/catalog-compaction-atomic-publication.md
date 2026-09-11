# Catalog compaction atomic publication

Source: internal/storage/catalog/compaction_commit.go — func (c *Catalog) CommitCompaction(_ context.Context, cc CompactionCommit) error {, moved 2026-09-11 (#1026)

CommitCompaction publishes a compaction replacement in ONE conditional
manifest transaction: the inputs leave the partition, their delete markers
leave with them, and the replacement arrives — or none of it does.

Before #893 this was two CAS writes, `RemoveFiles` then `AddNewFiles`. Each
was atomic and the PAIR was not, which cost three distinct properties:

 1. A failure between them left the table with the inputs gone and the
    replacement unpublished — zero visible rows, unrecoverable by retry
    because the compactor selects its inputs from the manifest it just
    emptied (#893).
 2. Even when both succeeded, a reader landing between them saw the
    intermediate manifest and answered from it.
 3. Neither call validated anything: `RemoveFiles` accepts inputs that are
    already gone (#895) and strips markers it never applied (#894).

The validation is the other half of the fix and does not follow from
atomicity: a single atomic write of a stale plan is still wrong. Two
predicates, both exact rather than conservative:

  - **Input identity.** Every path in Inputs is still in PartPath's file
    list. A losing compactor whose originals another compactor already
    consumed is refused with ErrCompactionInputMoved instead of adding a
    second copy of the same rows beside the winner's.
  - **The delete-marker snapshot.** The manifest's marker set for each
    input equals the set the output applied. A DELETE that committed while
    the output was being written moves the set, and the commit is refused
    with ErrCompactionDeletesAdvanced rather than republishing the row it
    removed.

Neither predicate fires on a write that did not touch this partition's
files, so unrelated ingest, DML on other files, and compaction of other
partitions all commit alongside it.
