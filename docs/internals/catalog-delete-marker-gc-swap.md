# Catalog delete marker gc swap

Source: internal/storage/catalog/catalog.go — func (c *Catalog) SwapFileForGC(ctx context.Context, tableName string, oldPath string, newFile *FileEntry, partValues map[string]string, partPath string, appliedIndices map[int64]bool) error {, moved 2026-09-11 (#1026)

SwapFileForGC publishes a delete-marker GC rewrite: the old file leaves the
partition, the rewritten replacement arrives, and the markers the rewrite
applied go away — all in the one conditional transaction CommitCompaction
runs, validated against the same two preconditions every compaction
publication is (input identity, and the delete-marker snapshot the output
was cut from).

It used to be its own CAS loop with its own rule, and the rule was wrong in
two directions #894 and #895 reproduced:

  - It appended the rewrite output without requiring that oldPath was still
    a member of the partition, so two GC sweeps over the same file each
    published a rewrite and the surviving rows appeared twice.
  - It removed only the row indices the rewrite APPLIED and left any that
    had arrived since, under the OLD file's path — where no reader can
    apply them, because that file is gone. The comment here used to say
    those rows stayed visible "for at most one GC cycle". They did not:
    the next sweep removes the dangling marker as an orphan, and the
    replacement carries the row forever. Removing a marker cannot remove a
    row from a file that already contains it.

So a rewrite now applies ALL of a file's current markers or none of them:
if the marker set moved between the manifest read the rewrite was cut from
and this commit, the swap is refused with ErrCompactionDeletesAdvanced and
the caller re-reads and rewrites against the newer set. appliedIndices is
therefore a PRECONDITION, not just a cleanup list — it says which markers
the output reflects, and the commit checks it.

If newFile is nil, the old file is simply removed (all rows were deleted).
