# Scan row group iterator lifecycle

Source: internal/engine/scan/rowgroup_iter.go — type RowGroupIter struct {, moved 2026-09-11 (#1026)

RowGroupIter yields one RecordBatch per row group on demand, without
pre-decoding the rest of the file. Use this in long-running scan
pipelines (worker fragment runners) where holding every row group of
a file in memory would blow the per-task working set.

At SF100 each lineitem file has ~10 row groups × ~28 MB decoded each,
so eager decode (ReadFileBatchesShard) costs ~280 MB live per file,
times 2–4 files prefetched, times 3–4 concurrent tasks = multi-GB
transient that the GC can't reclaim until the consumer (HashAggregate)
drains. Streaming one row group at a time bounds the scan-side live
memory to one decoded RG per scan source plus whatever is in flight
downstream — typically <300 MB instead of multi-GB.

Lifecycle:

	it, err := OpenRowGroupIter(reader, schema, selectedCols, shardIdx, shardCount)
	if err != nil { ... }
	defer it.Close()
	for {
	    b, err := it.Next(ctx)
	    if err != nil || b == nil { break }
	    // consume b, then b.Release() when done
	}

The iterator does NOT support schemas containing Array/Map types — the
existing row-based fallback in readFileBatchesViaRows decodes the whole
file in one shot and predates row-group sharding. Callers must check
HasUnsupportedColumnarTypes(schema) and use ReadFileBatchesShard for
those types. The slice path stays available; this iterator is a parallel
fast lane for the common case.
