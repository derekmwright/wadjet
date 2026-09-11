# Scan ordered decode ahead

Source: internal/engine/scan/decode_ahead.go — type DecodeAheadIter struct {, moved 2026-09-11 (#1026)

DecodeAheadIter is the parallel sibling of RowGroupIter
(docs/design/scan-decode-pipelining.md): k decode workers pull
row-group indices in file order, each runs ReadRowGroupNative for its
group, and results deliver to the consumer strictly in source order.
The consumer-facing contract is identical to RowGroupIter — same
batches in the same order, same error surfaced at the same position,
same prune behavior — only the decode of group N+1..N+w overlaps the
consumption of group N instead of waiting for it.

Memory is bounded by WindowBytes of decoded-but-undelivered batches,
estimated per group from the projected columns' TotalUncompressedSize
metadata (estimation error is bounded by one group per worker). The
group at the delivery cursor is always admitted regardless of the
window so a single oversized row group cannot deadlock the pipeline.

Concurrency safety rests on ReadRowGroupNative's documented contract:
FileReader is read-only after construction and every ColumnPages call
allocates a fresh ColumnPageReader, so concurrent decodes of distinct
row groups never share mutable state (columnar_native.go:124-126).

Lifecycle: workers start on the FIRST Next() call, not at Open —
filters attach after Open on the worker scan path (and may keep
arriving mid-scan; assignments re-read them per group). Close()
stops assignment and JOINS in-flight decodes before returning: the
caller munmaps the file bytes right after Close, so no decode may
touch the underlying slice once Close returns.
