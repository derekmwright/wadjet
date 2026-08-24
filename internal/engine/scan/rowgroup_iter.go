package scan

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
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
	fr         *pqt.FileReader
	readSchema []pqt.Column
	cur, end   int
	closed     bool

	// rowOffsets[i] is the FILE-ABSOLUTE index of row group i's first row
	// (the prefix sum of RowGroupNumRows over the whole file, not over this
	// iterator's shard). lastOffset is that value for the group Next most
	// recently returned. Merge-on-read delete markers name file-absolute
	// rows, so a consumer applying them has to be told where in the file
	// the batch it just took delivery of begins — and a sharded read
	// starts at a group that is not group 0. See scan.DeleteSet.
	rowOffsets []int64
	lastOffset int64

	// Empty-shard sentinel: shardIdx out of range or row-fallback shard >0
	// returns no batches without erroring. matches ReadFileBatchesShard.
	empty bool

	// Dynamic filters consulted before reading each row group. Both are
	// optional; a row group is skipped if EITHER prunes it. Wired by
	// callers (e.g., distributed worker scan source) via SetDynamicFilters.
	dynamicRanges []exec.DynamicRange
	bloomFilters  []*exec.BloomScanFilter

	// cache, when set via SetDecodedCache, consults/feeds the worker's
	// decoded-chunk cache (docs/design/decoded-rowgroup-cache.md). Inert
	// unless the reader carries a CacheIdentity.
	cache *DecodedChunkCache

	// backing, when set via SetBackingPool, is the SCAN SOURCE's row-group
	// backing pool: a decode writes into storage a previous group used once
	// the consumer released it and nobody claimed it (BackingPool's ownership
	// rule, docs/design/scan-output-backing-reuse.md). Owned by the source,
	// so it outlives this iterator.
	backing *BackingPool

	// Per-iterator counters for diagnostics. Reset on each Open.
	rgPrunedBloom int
	rgPrunedRange int
	rgRead        int
}

// SetDynamicFilters attaches dynamic-filter pushdowns to the iterator.
// Safe mid-scan from the goroutine calling Next: filters are consulted
// per group, so a set attached after some groups were read prunes every
// remaining group (attach-on-arrival delivery; drop-only semantics).
// Empty slices clear any existing filters. Multiple calls overwrite
// prior state — callers pass the accumulated union.
func (it *RowGroupIter) SetDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) {
	if it == nil {
		return
	}
	it.dynamicRanges = ranges
	it.bloomFilters = blooms
}

// SetDecodedCache attaches the worker's decoded-chunk cache. Call before
// the first Next (the field is read without synchronization). nil = uncached.
func (it *RowGroupIter) SetDecodedCache(c *DecodedChunkCache) {
	if it == nil {
		return
	}
	it.cache = c
}

// SetBackingPool attaches the scan source's row-group backing pool. Call
// before the first Next (the field is read without synchronization). nil =
// every group allocates fresh. See BackingPool's ownership rule.
func (it *RowGroupIter) SetBackingPool(p *BackingPool) {
	if it == nil {
		return
	}
	it.backing = p
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
		rowOffsets: rowGroupRowOffsets(fr),
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

		b, err := ReadRowGroupNativeBacked(it.fr, rgIdx, it.readSchema, it.cache, it.backing)
		if err != nil {
			return nil, fmt.Errorf("reading row group %d: %w", rgIdx, err)
		}
		if b == nil {
			// Empty row group (zero rows) — skip and try the next.
			continue
		}
		it.rgRead++
		it.lastOffset = rowOffsetAt(it.rowOffsets, rgIdx)
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

// RowOffset is the FILE-ABSOLUTE index of the first row of the batch Next
// most recently returned. Valid only immediately after a non-nil Next; 0
// before the first delivery and for the empty-shard sentinel. Consumers use
// it to place a batch within its file, which is the frame merge-on-read
// delete markers are expressed in.
func (it *RowGroupIter) RowOffset() int64 {
	if it == nil {
		return 0
	}
	return it.lastOffset
}

// rowGroupRowOffsets is the prefix sum of a file's row-group row counts:
// entry i is the file-absolute index of row group i's first row. Computed
// once per open — NumRowGroups is tens, not millions — and never narrowed
// to a shard, because the frame delete markers use is the whole file.
func rowGroupRowOffsets(fr *pqt.FileReader) []int64 {
	if fr == nil {
		return nil
	}
	n := fr.NumRowGroups()
	if n <= 0 {
		return nil
	}
	offsets := make([]int64, n)
	var acc int64
	for i := 0; i < n; i++ {
		offsets[i] = acc
		acc += fr.RowGroupNumRows(i)
	}
	return offsets
}

func rowOffsetAt(offsets []int64, idx int) int64 {
	if idx < 0 || idx >= len(offsets) {
		return 0
	}
	return offsets[idx]
}
