# Compaction one pass format rewrite

Source: internal/storage/compaction/compactor.go — func (c *Compactor) RewriteTable(ctx context.Context, tableName string) (*Result, error) {, moved 2026-09-11 (#1026)
Superseded: The rewrite input list is captured once, but CommitCompaction rereads the manifest to validate publication; conflicts can leave captured files for a later rerun.

RewriteTable rewrites EVERY file of every partition of a table exactly once,
through the current writer, and replaces the originals.

This is the format-migration mode, and it is deliberately not compaction.
shouldCompact's floors — two files, MinFiles, an average size under
MaxFileSizeBytes — all answer "is this partition worth merging", which is
the right question for a background sweep and the wrong one for a
migration: a partition holding ONE 512 MB file is exactly the file that has
to be rewritten, and it is the one shape compaction will never touch. So a
rewrite is exempt from the floors and admits a 1 -> 1 pass.

It terminates structurally rather than by CompactTable's progress rule. The
file list is read from the manifest ONCE, split into memory-bounded groups,
and each group is written once; nothing re-reads the manifest, so no output
of this call can become an input to it. "1 removed, 1 created" is progress
here, which is precisely why the progress rule cannot apply.

Its use is ADR-0018's DECIMAL(p > 18) migration: files written before #429
annotate a wide DECIMAL over an INT64 leaf, and no reader outside wadjet can
open them. One rewrite through the current writer produces a FLBA(16) leaf
with byte-identical unscaled values. Every other type round-trips unchanged
(that is the compaction gate's property), so running it over a table that
needs nothing costs the rewrite and changes no value.

Like CompactTable, a partition whose merge fails does not stop the others;
the aggregate is *CompactionFailed.
