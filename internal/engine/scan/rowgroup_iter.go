package scan

import (
	"fmt"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// RowGroupIter yields one RecordBatch per row group on demand, without
// pre-decoding the rest of the file. Use this in long-running scan
// pipelines (worker fragment runners) where holding every row group of
// a file in memory would blow the per-task working set.
//
// At SF100 each lineitem file has ~10 row groups × ~28 MB decoded each,
// so eager decode (ReadFileBatchesShard) costs ~280 MB live per file,
// times 2–4 files prefetched, times 3–4 concurrent tasks = multi-GB
// transient that the GC can't reclaim until the consumer (HashAggregate)
// drains. Streaming one row group at a time bounds the scan-side live
// memory to one decoded RG per scan source plus whatever is in flight
// downstream — typically <300 MB instead of multi-GB.
//
// Lifecycle:
//
//	it, err := OpenRowGroupIter(reader, schema, selectedCols, shardIdx, shardCount)
//	if err != nil { ... }
//	defer it.Close()
//	for {
//	    b, err := it.Next(ctx)
//	    if err != nil || b == nil { break }
//	    // consume b, then b.Release() when done
//	}
//
// The iterator does NOT support schemas containing Array/Map types — the
// existing row-based fallback in readFileBatchesViaRows decodes the whole
// file in one shot and predates row-group sharding. Callers must check
// HasUnsupportedColumnarTypes(schema) and use ReadFileBatchesShard for
// those types. The slice path stays available; this iterator is a parallel
// fast lane for the common case.
type RowGroupIter struct {
	fr           *pqt.FileReader
	readSchema   []pqt.Column
	cur, end     int
	closed       bool

	// Empty-shard sentinel: shardIdx out of range or row-fallback shard >0
	// returns no batches without erroring. matches ReadFileBatchesShard.
	empty bool

	// Dynamic filters consulted before reading each row group. Both are
	// optional; a row group is skipped if EITHER prunes it. Wired by
	// callers (e.g., distributed worker scan source) via SetDynamicFilters.
	dynamicRanges []exec.DynamicRange
	bloomFilters  []*exec.BloomScanFilter

	// Per-iterator counters for diagnostics. Reset on each Open.
	rgPrunedBloom int
	rgPrunedRange int
	rgRead        int
}

// SetDynamicFilters attaches dynamic-filter pushdowns to the iterator. Must
// be called before the first Next(). Empty slices clear any existing
// filters. Multiple calls overwrite prior state.
func (it *RowGroupIter) SetDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) {
	if it == nil {
		return
	}
	it.dynamicRanges = ranges
	it.bloomFilters = blooms
}

// PruneStats returns counters for diagnostic logging: row groups skipped
// via bloom, via range, and actually read. Snapshot at any point.
func (it *RowGroupIter) PruneStats() (bloom, rangeP, read int) {
	if it == nil {
		return 0, 0, 0
	}
	return it.rgPrunedBloom, it.rgPrunedRange, it.rgRead
}

// OpenRowGroupIter constructs a streaming iterator over the row-group
// range assigned to (shardIdx, shardCount) of the given reader. With
// shardCount=1 the iterator covers the whole file. Returns ErrUnsupportedColumnar
// for schemas with Array/Map types (callers must use ReadFileBatchesShard).
func OpenRowGroupIter(reader *pqt.Reader, schema []pqt.Column, selectedCols []string, shardIdx, shardCount int) (*RowGroupIter, error) {
	if reader == nil {
		return nil, fmt.Errorf("OpenRowGroupIter: nil reader")
	}
	if shardCount < 1 {
		shardCount = 1
	}

	readSchema := schema
	if len(selectedCols) > 0 {
		readSchema = projectSchema(schema, selectedCols)
	}

	if HasUnsupportedColumnarTypes(readSchema) {
		return nil, fmt.Errorf("RowGroupIter does not support Array/Map types; caller must fall back to ReadFileBatchesShard")
	}

	if shardIdx < 0 || shardIdx >= shardCount {
		// Empty shard: out-of-range index. Return iterator that yields nothing.
		return &RowGroupIter{empty: true}, nil
	}

	fr := reader.FileReader()
	if fr == nil {
		return nil, fmt.Errorf("OpenRowGroupIter: reader has no FileReader")
	}

	total := fr.NumRowGroups()
	startRg, endRg := rowGroupRangeForShard(total, shardIdx, shardCount)
	return &RowGroupIter{
		fr:         fr,
		readSchema: readSchema,
		cur:        startRg,
		end:        endRg,
	}, nil
}

// Next returns the next decoded row group as a RecordBatch, or (nil, nil)
// when exhausted. The returned batch's lifetime is the caller's; Release()
// to a pool when done. Subsequent calls after exhaustion or Close return
// (nil, nil).
func (it *RowGroupIter) Next() (*batch.RecordBatch, error) {
	if it == nil || it.closed || it.empty {
		return nil, nil
	}
	for it.cur < it.end {
		rgIdx := it.cur
		it.cur++

		// Dynamic-filter row-group pruning. Stats read is cheap (already
		// available in file metadata); the read+decode that follows is
		// the expensive part we're avoiding when we can prune.
		if len(it.dynamicRanges) > 0 || len(it.bloomFilters) > 0 {
			stats := it.fr.RowGroupStats(rgIdx)
			pruned := false
			if !pruned && len(it.dynamicRanges) > 0 && CanRangePruneRowGroup(it.dynamicRanges, stats) {
				it.rgPrunedRange++
				pruned = true
			}
			if !pruned && len(it.bloomFilters) > 0 {
				for _, bf := range it.bloomFilters {
					if CanBloomPruneRowGroup(bf, stats) {
						it.rgPrunedBloom++
						pruned = true
						break
					}
				}
			}
			if pruned {
				continue
			}
		}

		b, err := ReadRowGroupNative(it.fr, rgIdx, it.readSchema, nil)
		if err != nil {
			return nil, fmt.Errorf("reading row group %d: %w", rgIdx, err)
		}
		if b == nil {
			// Empty row group (zero rows) — skip and try the next.
			continue
		}
		it.rgRead++
		return b, nil
	}
	return nil, nil
}

// Close marks the iterator as exhausted. Idempotent. No file handles
// are owned by the iterator (the caller's *pqt.Reader owns them), so
// Close is mostly a cancellation signal — subsequent Next calls return
// (nil, nil).
func (it *RowGroupIter) Close() error {
	if it == nil {
		return nil
	}
	it.closed = true
	return nil
}
