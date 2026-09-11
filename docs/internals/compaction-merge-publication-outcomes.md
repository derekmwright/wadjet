# Compaction merge publication outcomes

Source: internal/storage/compaction/compactor.go — func (c *Compactor) mergeGroup(ctx context.Context, tableName string, schema parquet.Schema,, moved 2026-09-11 (#1026)

mergeGroup merges one group of a partition's files into a single new file
and publishes the replacement, folding the outcome into result.

The publication is ONE conditional manifest transaction
(catalog.CommitCompaction): the inputs leave, their delete markers leave
with them, and the replacement arrives — or none of it does, and the
snapshot the table already had is exactly the one it keeps. It used to be
RemoveFiles followed by AddNewFiles, two CAS writes whose PAIR was not
atomic: a failure between them emptied the table irrecoverably (#893), and
neither call checked that the inputs were still the table's or that the
delete markers were still the ones the output applied (#894, #895).

The caller classifies the error rather than the position:

  - *mergeError is the read-and-rewrite step failing on THIS partition's
    bytes, with its inputs untouched.
  - catalog.ErrCompactionConflict is another writer having moved this
    partition's files or its delete markers since the manifest was read.
    Nothing was written; the output object is deleted here, because a
    conflict is decided BEFORE the CAS and so is proof that no publication
    happened. The caller replans from the manifest that replaced ours.
  - anything else is the manifest or the object store — not a per-partition
    condition, and the output object is KEPT, because a publication error
    says nothing about whether the write landed and deleting the bytes on a
    maybe is the one mistake that is not recoverable.
