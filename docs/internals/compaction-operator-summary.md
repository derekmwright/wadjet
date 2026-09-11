# Compaction operator summary

Source: internal/storage/compaction/compactor.go — func (r *Result) Summary() []string {, moved 2026-09-11 (#1026)

Summary renders the lines a caller reports to an operator, in order. It is
the one place that decides what a compaction run SAYS about itself, so the
CLI cannot print a subset of it by omission.

It exists because PublicationConflicts was unreportable without it. A
`wadjet compact --rewrite` whose only group lost a publication race returns
a nil error, an empty Failed, PassLimitReached false and
PartitionsCompacted zero — so the CLI printed

	table events: 0 merges, 0 files removed, 0 created, 0 rows, 0 -> 0 bytes

which is character for character what an already-migrated table prints. The
operator concludes the format migration is done. It is not: RewriteTable
reads its file list exactly once, by construction, so a skipped group is not
retried inside the call and only a re-run picks it up. That is the same
reason PassLimitReached earns a line — a counter nobody prints cannot tell
an operator anything, which is precisely the argument ADR-0020's amendment
makes for having the counter at all.

The "run again" half is conditioned on this run having published NOTHING,
rather than on the conflict count alone: CompactTable replans after a
refusal, so a run that conflicted once and then compacted the partition has
finished its work and must not be reported as unfinished.

Failed is deliberately not here. Those go to stderr, one per partition, and
mixing streams in one list would decide that for the caller.
