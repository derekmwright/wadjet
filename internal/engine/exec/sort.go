package exec

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SortOrder specifies sort direction.
type SortOrder int

const (
	Ascending SortOrder = iota
	Descending
)

// SortKey defines a column and direction for sorting.
type SortKey struct {
	Column    string
	Order     SortOrder
	NullsLast bool
}

// Sort is a Sink that accumulates all batches columnar and sorts them
// using typed comparisons on an index array (no row-oriented conversion).
// When a SpillManager is set, Sort will spill to disk when memory pressure is high.
// When Limit > 0, only the top Limit rows are materialized (Top-K optimization).
type Sort struct {
	Keys   []SortKey
	Limit  int // 0 = no limit, >0 = only materialize top N rows
	schema []parquet.Column
	Spill  *memory.SpillManager // optional: enables spill-to-disk

	mu         sync.Mutex
	batches    []*batch.RecordBatch // columnar storage
	totalRows  int
	trackedMem int64                // memory reserved from shared tracker by this operator
	runFiles   []string             // sorted columnar runs (external-merge path)
	sorted     []*batch.RecordBatch // materialized sorted results
	pos        int
	// External-merge stream state (set by finalizeExternalMerge; drained by Next).
	merger    *runMerger
	mergeRuns []string // run files to delete once the merge drains
	mergeEmit int      // rows emitted so far (Limit enforcement)
	mergeDone bool
	// AccountedOperator (Phase 2) state — see HashAggregate for the contract.
	accInstanceID       uint64
	accState            atomic.Int32
	unregisterAccounted func()
	// finalized is set once finalize begins; after
	// that the input batches are gone and there is nothing left to spill.
	finalized bool
}

func NewSort(keys []SortKey) *Sort {
	return &Sort{Keys: keys}
}

func (s *Sort) Init(_ context.Context) error {
	s.batches = nil
	s.totalRows = 0
	s.sorted = nil
	s.pos = 0
	s.merger = nil
	s.mergeRuns = nil
	s.mergeEmit = 0
	s.mergeDone = false
	return nil
}

func (s *Sort) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.schema == nil {
		// A key that resolves to no column is a planner bug, and skipping it
		// is how that bug stays invisible: the sort "succeeds" and the rows
		// come back in arbitrary order (#313/#314/#316/#386 all wore this
		// face). Fail the first batch instead — the same loud contract the
		// sort-merge join adopted. Checked once per input, here, because
		// every downstream resolution (top-K compaction, spill runs, the
		// final columnar sort) shares this schema.
		for _, k := range s.Keys {
			if columnIndexFallback(b, k.Column) < 0 {
				return fmt.Errorf("sort: key column %q does not exist in the input schema", k.Column)
			}
		}
		s.schema = b.Schema
		// Register on first Consume when a SpillManager is configured. We
		// don't register in Init because (1) Init runs before any state
		// exists (footprint=0), and (2) some pipelines reuse a Sort across
		// queries with Init resets.
		if s.Spill != nil && s.unregisterAccounted == nil {
			s.accInstanceID = memory.NextInstanceID()
			s.accState.Store(int32(memory.OpActive))
			s.unregisterAccounted = s.Spill.RegisterAccounted(s)
		}
	}
	FlattenForConsumer(b, nil) // retained past the batch cycle: views must not survive
	b.Detach()                 // prevent pool recycle — pipeline calls Release() after Consume()
	// Snapshot the selection vector — Filter operators reuse their sel
	// buffer across calls (see CollectSink.Consume); a stored batch would
	// otherwise see its Sel silently rewritten by the NEXT batch's filter
	// pass, yielding out-of-range physical indices at finalize (ClickBench
	// Q24: SELECT * + LIKE filter straight into Sort — panic in
	// sortCompareInt64NoNulls) or silently wrong sorted output.
	if b.Sel != nil {
		selCopy := make([]uint32, len(b.Sel))
		copy(selCopy, b.Sel)
		b.Sel = selCopy
	}
	s.batches = append(s.batches, b)
	s.totalRows += b.ActiveLen()

	// Track memory usage for spill pressure detection
	if s.Spill != nil {
		cost := b.MemBytes()
		s.Spill.TrackBatch(cost)
		s.trackedMem += cost
		if s.accInstanceID != 0 {
			s.Spill.Tracker().PublishOwned(s.accInstanceID, s.trackedMem)
		}
	}

	// Streaming Top-K: with a known limit, periodically compact the buffer
	// down to the current top Limit rows. Without this, the top-K heap only
	// ran at FINALIZE, so ORDER BY ... LIMIT n buffered its entire input —
	// on a filter-into-sort shape (ClickBench Q24, SELECT * + LIKE) that
	// pinned every scanned wide batch and OOM-killed the process. Top-K of
	// a union equals top-K of (top-K(A) ∪ B), so compacting the running
	// buffer is exact. The threshold keeps compaction amortized: each pass
	// costs O(buffer · log limit) and fires once per ~threshold rows.
	// Trigger on ACTIVE rows, buffered BYTES, or batch count — active rows
	// alone is blind to selectivity: under a 2% filter over wide scan
	// batches, 8k active rows means ~24 pinned full-width 500k-row batches
	// (~6GB) per clone, which still OOM-killed the c6a on Q24. The byte
	// trigger uses the exact tracked cost when a spill manager is wired;
	// the batch-count trigger covers the untracked case.
	if s.Limit > 0 && (s.totalRows > s.Limit*2+4*batch.DefaultBatchSize ||
		s.trackedMem > 96<<20 || len(s.batches) >= 4) {
		s.compactTopKLocked()
	}

	// Spill to disk if memory pressure is high. The columnar run path
	// accumulates at least minSortRunBytes before flushing so runs stay
	// merge-friendly. Peer relief (SpillSome) bypasses the floor.
	if s.Spill != nil && s.Spill.ShouldSpillFor(memory.SpillCheap) && len(s.batches) > 0 {
		if s.trackedMem >= minSortRunBytes {
			if _, err := s.flushSpillLocked(); err != nil {
				return err
			}
		}
	}
	return nil
}

// flushSpillLocked drains all buffered batches to disk and releases their
// tracking, returning the bytes freed. Every schema writes a SORTED columnar
// run (the external-merge unit) — nested ARRAY/MAP/ROW columns round-trip
// the run format since the typed nested copy primitives landed. Caller
// holds s.mu.
func (s *Sort) flushSpillLocked() (int64, error) {
	if len(s.batches) == 0 || s.trackedMem == 0 {
		return 0, nil
	}
	path, err := sortBatchesToRun(s.Spill.SpillDir(), s.schema, s.batches, s.totalRows, s.Keys, s.Limit)
	if err != nil {
		return 0, err
	}
	if path != "" {
		s.runFiles = append(s.runFiles, path)
	}
	s.batches = s.batches[:0]
	s.totalRows = 0
	freed := s.trackedMem
	s.Spill.ReleaseTracking(freed)
	s.trackedMem = 0
	if s.accInstanceID != 0 {
		s.Spill.Tracker().PublishOwned(s.accInstanceID, 0)
	}
	return freed, nil
}

// compactTopKLocked replaces the buffered batches with a single dense
// batch holding only the current top Limit rows. Caller must hold s.mu,
// s.Limit must be > 0, and s.schema must be set.
func (s *Sort) compactTopKLocked() {
	if len(s.batches) == 0 {
		return
	}
	entries := buildSortEntries(s.batches, s.totalRows)
	resolved := resolveSortKeysForBatches(s.Keys, s.batches)
	entries = selectSortedEntries(entries, sortEntriesLessFunc(resolved, s.batches), s.Limit)
	compacted := gatherEntriesBatch(s.schema, s.batches, entries)
	if s.Spill != nil && s.trackedMem > 0 {
		s.Spill.ReleaseTracking(s.trackedMem)
		s.trackedMem = 0
	}
	s.batches = s.batches[:0]
	s.batches = append(s.batches, compacted)
	s.totalRows = compacted.ActiveLen()
	if s.Spill != nil {
		cost := compacted.MemBytes()
		s.Spill.TrackBatch(cost)
		s.trackedMem = cost
		if s.accInstanceID != 0 {
			s.Spill.Tracker().PublishOwned(s.accInstanceID, s.trackedMem)
		}
	}
}

func (s *Sort) Finalize(_ context.Context) error {
	// Once Finalize starts, the input batches will be consumed by the sort
	// itself; we can't usefully respond to a peer's RequestRelief any more.
	// Deregister now so the registry stops considering us. Close still
	// deregisters as a backstop.
	s.mu.Lock()
	s.finalized = true
	s.mu.Unlock()
	if s.unregisterAccounted != nil {
		s.accState.Store(int32(memory.OpClosed))
		s.unregisterAccounted()
		s.unregisterAccounted = nil
	}
	if len(s.runFiles) > 0 {
		return s.finalizeExternalMerge()
	}
	return s.finalizeColumnar()
}

// finalizeExternalMerge sets up the streaming k-way merge over sorted runs
// plus the in-memory remainder. Nothing is materialized here — Next() pulls
// one merged batch at a time, so finalize-phase memory is bounded by one
// buffered batch per run regardless of input size.
func (s *Sort) finalizeExternalMerge() error {
	runs := s.runFiles
	s.runFiles = nil

	// Multi-level pre-merge keeps the final fan-in (and its one-batch-per-run
	// buffer cost) bounded. Reserve one slot for the in-memory cursor.
	runs, err := preMergeRuns(s.Spill.SpillDir(), s.schema, s.Keys, runs, maxMergeFanIn-1, s.Limit)
	if err != nil {
		return err
	}

	// Error contract below: s.runFiles was already nil'd, so any failure
	// before s.mergeRuns is assigned must delete the run files here —
	// Close's backstops would see nothing and the runs would outlive the
	// query in the worker-lifetime spill dir.
	cursors := make([]*runCursor, 0, len(runs)+1)
	for ord, p := range runs {
		c, err := newFileRunCursor(p)
		if err != nil {
			for _, prev := range cursors {
				prev.close()
			}
			removeRunFiles(runs)
			return err
		}
		c.ord = ord
		cursors = append(cursors, c)
	}

	// In-memory remainder participates as the last cursor: sorted entries
	// gathered lazily, no run write needed. It arrived after every spilled
	// run, so it ties last — consistent with the stable in-memory sort.
	if len(s.batches) > 0 {
		entries := buildSortEntries(s.batches, s.totalRows)
		resolved := resolveSortKeysForBatches(s.Keys, s.batches)
		entries = selectSortedEntries(entries, sortEntriesLessFunc(resolved, s.batches), s.Limit)
		c, err := newMemRunCursor(s.schema, s.batches, entries)
		if err != nil {
			for _, prev := range cursors {
				prev.close()
			}
			removeRunFiles(runs)
			return err
		}
		c.ord = len(runs)
		cursors = append(cursors, c)
	}

	s.merger = newRunMerger(s.schema, s.Keys, cursors)
	s.mergeRuns = runs
	return nil
}

// finishMergeLocked tears down the external merge: closes cursors, deletes
// run files, releases the reservation still held for the in-memory remainder,
// and drops batch references.
func (s *Sort) finishMergeLocked() {
	if s.merger != nil {
		s.merger.close()
		s.merger = nil
	}
	removeRunFiles(s.mergeRuns)
	s.mergeRuns = nil
	s.batches = nil
	s.totalRows = 0
	if s.Spill != nil && s.trackedMem > 0 {
		s.Spill.ReleaseTracking(s.trackedMem)
		s.trackedMem = 0
	}
	s.mergeDone = true
}

// sortEntry identifies a row within the accumulated batches by batch and row index.
type sortEntry struct {
	batchIdx uint32
	rowIdx   uint32
}

// finalizeColumnar sorts using typed column comparisons on an index array.
// The entry-building / key-resolution / top-K machinery is shared with the
// external-merge run writer (sort_external.go).
func (s *Sort) finalizeColumnar() error {
	if len(s.batches) == 0 {
		return nil
	}

	entries := buildSortEntries(s.batches, s.totalRows)
	resolved := resolveSortKeysForBatches(s.Keys, s.batches)
	entries = selectSortedEntries(entries, sortEntriesLessFunc(resolved, s.batches), s.Limit)

	// Materialize sorted batches using typed column copies.
	for pos := 0; pos < len(entries); {
		end := pos + batch.DefaultBatchSize
		if end > len(entries) {
			end = len(entries)
		}
		s.sorted = append(s.sorted, gatherEntriesBatch(s.schema, s.batches, entries[pos:end]))
		pos = end
	}

	s.batches = nil // release input batches
	return nil
}

// Truncate keeps only the first n rows of sorted output (Top-K).
func (s *Sort) Truncate(n int) {
	// External-merge path: nothing is materialized — cap the stream instead.
	// Callers (fragment PostFinalize, sortSourceAdapter) invoke Truncate once
	// between Finalize and the first Next, before any rows are emitted.
	if s.merger != nil || s.mergeDone {
		if s.Limit == 0 || n < s.Limit {
			s.Limit = n
		}
		return
	}
	remaining := n
	for i, b := range s.sorted {
		if remaining <= 0 {
			s.sorted = s.sorted[:i]
			return
		}
		active := b.ActiveLen()
		if active <= remaining {
			remaining -= active
			continue
		}
		// Truncate this batch
		sel := make([]uint32, remaining)
		if b.Sel != nil {
			copy(sel, b.Sel[:remaining])
		} else {
			for j := range sel {
				sel[j] = uint32(j)
			}
		}
		b.Sel = sel
		s.sorted = s.sorted[:i+1]
		return
	}
}

// CloneSink returns a new Sort with the same configuration but fresh state.
// Used by parallel pipeline execution: each worker gets its own cloned sink,
// eliminating mutex contention during the parallel Consume phase.
func (s *Sort) CloneSink() SinkSource {
	return &Sort{
		Keys:   s.Keys,
		Limit:  s.Limit,
		schema: s.schema,
	}
}

// MergeSink merges another Sort's accumulated batches into this one.
// Called after all parallel workers finish to combine partial batch lists
// before the single-threaded Finalize sort.
//
// Memory accounting transfers with the batches: when the clone tracked its
// buffered rows (morsel-parallel clones charge a tracking-only SpillManager
// view against the shared pool), the reservation must follow the state or
// the merged bytes become invisible to the primary's spill trigger. Charge
// the primary FIRST, then release the clone, so the shared tracker never
// under-reports in between. No-op when neither side tracks (the
// single-process planner path).
func (s *Sort) MergeSink(other SinkSource) {
	o := other.(*Sort)
	// A primary that never consumed a batch itself (warmup filtered out,
	// every source batch claimed by a clone worker — a scheduling race) has
	// no schema, and finalize would gather the merged rows into ZERO output
	// columns: right row count, rows with no columns at all, varying run to
	// run with goroutine scheduling (#378). Inherit the clone's schema the
	// way HashAggregate.mergeSinkState inherits resolved column state.
	if s.schema == nil {
		s.schema = o.schema
	}
	s.batches = append(s.batches, o.batches...)
	s.totalRows += o.totalRows
	o.batches = nil
	if o.trackedMem > 0 {
		if s.Spill != nil {
			s.Spill.TrackBatch(o.trackedMem)
			s.trackedMem += o.trackedMem
			if s.accInstanceID != 0 {
				s.Spill.Tracker().PublishOwned(s.accInstanceID, s.trackedMem)
			}
		}
		o.Spill.ReleaseTracking(o.trackedMem)
		o.trackedMem = 0
		if o.accInstanceID != 0 {
			o.Spill.Tracker().PublishOwned(o.accInstanceID, 0)
		}
	}
}

// Close releases any tracker reservation Sort still holds for buffered rows
// that never crossed the spill threshold, and drops references so the GC
// can reclaim immediately. Without this, a non-spilling Sort accumulates a
// phantom reservation in the shared tracker for the lifetime of the
// process; see HashJoin.Close for the full background.
func (s *Sort) Close() error {
	if s.unregisterAccounted != nil {
		s.accState.Store(int32(memory.OpClosed))
		s.unregisterAccounted()
		s.unregisterAccounted = nil
	}
	s.mu.Lock()
	if s.merger != nil || len(s.mergeRuns) > 0 {
		// Early cancel mid-merge: close cursors and delete run scratch.
		s.finishMergeLocked()
	}
	removeRunFiles(s.runFiles)
	s.runFiles = nil
	if s.Spill != nil && s.trackedMem > 0 {
		s.Spill.ReleaseTracking(s.trackedMem)
		s.trackedMem = 0
	}
	s.batches = nil
	s.totalRows = 0
	s.mu.Unlock()
	return nil
}

// Inspect implements memory.AccountedOperator.
func (s *Sort) Inspect() memory.OperatorFootprint {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := memory.OpState(s.accState.Load())
	if st == memory.OpClosed {
		return memory.OperatorFootprint{State: memory.OpClosed, InstanceID: s.accInstanceID, Name: "Sort"}
	}
	return memory.OperatorFootprint{
		OwnedBytes:     s.trackedMem,
		RetainedBytes:  s.trackedMem,
		SpillableBytes: s.spillableBytesLocked(),
		State:          st,
		InstanceID:     s.accInstanceID,
		Name:           "Sort",
	}
}

// spillableBytesLocked: Sort has no incremental spill mechanism — SpillSome
// drains ALL buffered batches in one shot. Before finalize the whole tracked
// footprint is reclaimable; once finalize begins, s.batches is gone and the
// sorted output stream is not re-spillable raw rows, so it reports 0. Caller
// holds s.mu.
func (s *Sort) spillableBytesLocked() int64 {
	if s.Spill == nil || s.finalized || s.trackedMem == 0 || len(s.batches) == 0 {
		return 0
	}
	return s.trackedMem
}

// EstimateRelief implements memory.AccountedOperator. Sort frees all-or-nothing,
// so it reports the full spillable footprint whenever target > 0.
func (s *Sort) EstimateRelief(target int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if target <= 0 {
		return 0
	}
	return s.spillableBytesLocked()
}

// SpillSome drains accumulated input batches to disk and releases the freed
// bytes — a sorted columnar run on the external-merge path, a raw-row file on
// the nested-type fallback. All-or-nothing either way: input batches are
// pre-sort, so there's no partial state to keep.
//
// Implements memory.Spillable and memory.AccountedOperator.
func (s *Sort) SpillSome(_ int64) (int64, error) {
	s.accState.Store(int32(memory.OpSpilling))
	defer s.accState.CompareAndSwap(int32(memory.OpSpilling), int32(memory.OpActive))
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Spill == nil || s.finalized {
		return 0, nil
	}
	return s.flushSpillLocked()
}

// Next returns sorted results in batches. On the external-merge path it
// streams from the k-way run merger; otherwise it drains the materialized
// s.sorted list.
func (s *Sort) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.merger != nil || s.mergeDone {
		return s.nextMerged()
	}
	if s.pos >= len(s.sorted) {
		return nil, nil
	}
	b := s.sorted[s.pos]
	s.pos++
	return b, nil
}

// nextMerged pulls the next batch from the external merge, enforcing Limit
// and tearing the merge down (cursors, run files, reservations) at EOF.
func (s *Sort) nextMerged() (*batch.RecordBatch, error) {
	if s.mergeDone {
		return nil, nil
	}
	b, err := s.merger.Next()
	if err != nil {
		return nil, err
	}
	if b == nil {
		s.mu.Lock()
		s.finishMergeLocked()
		s.mu.Unlock()
		return nil, nil
	}
	if s.Limit > 0 {
		remaining := s.Limit - s.mergeEmit
		if remaining <= 0 {
			s.mu.Lock()
			s.finishMergeLocked()
			s.mu.Unlock()
			return nil, nil
		}
		if b.Len > remaining {
			b.Len = remaining
		}
	}
	s.mergeEmit += b.Len
	return b, nil
}

// copyVectorValue copies a single value between vectors using typed access (no boxing).
// For bytes-based types, values must be copied in sequential order for dst (i = 0, 1, 2, ...).
func copyVectorValue(dst *batch.Vector, di int, src *batch.Vector, si int) {
	switch dst.Type {
	case batch.TypeArray, batch.TypeMap, batch.TypeRow:
		// Nested writes share the sequential-dst contract; CopyValueFrom
		// advances offsets/children for null rows itself.
		dst.CopyValueFrom(di, src, si)
		return
	}
	if src.Nulls.IsNull(si) {
		dst.Nulls.SetNull(di)
		switch dst.Type {
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
			dst.BytesData.Set(di, nil)
		}
		return
	}
	dst.Nulls.SetValid(di)
	switch dst.Type {
	case batch.TypeBool:
		dst.BoolData[di] = src.BoolData[si]
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		dst.Int32Data[di] = src.Int32Data[si]
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		dst.Int64Data[di] = src.Int64Data[si]
	case batch.TypeFloat32:
		dst.Float32Data[di] = src.Float32Data[si]
	case batch.TypeFloat64:
		dst.Float64Data[di] = src.Float64Data[si]
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.SetFrom(di, &src.BytesData, si)
	case batch.TypeDecimal:
		dst.DecimalData.Data[di] = src.DecimalData.Data[si]
	case batch.TypeVector:
		dim := src.VectorDim
		if dim > 0 {
			copy(dst.Float32Data[di*dim:(di+1)*dim], src.Float32Data[si*dim:(si+1)*dim])
		}
	}
}

// gatherVector copies scattered source rows from a single vector into contiguous
// destination positions. srcRows[i] gives the source row index for destination row i.
// Hoists the type switch outside the loop, eliminating per-row function call overhead.
func gatherVector(dst, src *batch.Vector, srcRows []int) {
	hasNulls := src.Nulls.HasNulls()
	switch dst.Type {
	case batch.TypeBool:
		if !hasNulls {
			for di, si := range srcRows {
				dst.BoolData[di] = src.BoolData[si]
			}
		} else {
			for di, si := range srcRows {
				if src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.BoolData[di] = src.BoolData[si]
				}
			}
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		if !hasNulls {
			for di, si := range srcRows {
				dst.Int32Data[di] = src.Int32Data[si]
			}
		} else {
			for di, si := range srcRows {
				if src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Int32Data[di] = src.Int32Data[si]
				}
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		if !hasNulls {
			for di, si := range srcRows {
				dst.Int64Data[di] = src.Int64Data[si]
			}
		} else {
			for di, si := range srcRows {
				if src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Int64Data[di] = src.Int64Data[si]
				}
			}
		}
	case batch.TypeFloat32:
		if !hasNulls {
			for di, si := range srcRows {
				dst.Float32Data[di] = src.Float32Data[si]
			}
		} else {
			for di, si := range srcRows {
				if src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Float32Data[di] = src.Float32Data[si]
				}
			}
		}
	case batch.TypeFloat64:
		if !hasNulls {
			for di, si := range srcRows {
				dst.Float64Data[di] = src.Float64Data[si]
			}
		} else {
			for di, si := range srcRows {
				if src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Float64Data[di] = src.Float64Data[si]
				}
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		// Pre-calculate total byte size to avoid growslice in Set's append.
		totalBytes := 0
		srcOff := src.BytesData.Offsets
		for _, si := range srcRows {
			totalBytes += int(srcOff[si+1] - srcOff[si])
		}
		dst.BytesData.PreAllocBytes(totalBytes)
		if !hasNulls {
			for di, si := range srcRows {
				dst.BytesData.SetFrom(di, &src.BytesData, si)
			}
		} else {
			for di, si := range srcRows {
				if src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
					dst.BytesData.Set(di, nil)
				} else {
					dst.BytesData.SetFrom(di, &src.BytesData, si)
				}
			}
		}
	case batch.TypeDecimal:
		if !hasNulls {
			for di, si := range srcRows {
				dst.DecimalData.Data[di] = src.DecimalData.Data[si]
			}
		} else {
			for di, si := range srcRows {
				if src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.DecimalData.Data[di] = src.DecimalData.Data[si]
				}
			}
		}
	case batch.TypeVector:
		dim := src.VectorDim
		if dim > 0 {
			if !hasNulls {
				for di, si := range srcRows {
					copy(dst.Float32Data[di*dim:(di+1)*dim], src.Float32Data[si*dim:(si+1)*dim])
				}
			} else {
				for di, si := range srcRows {
					if src.Nulls.IsNullFast(si) {
						dst.Nulls.SetNull(di)
					} else {
						copy(dst.Float32Data[di*dim:(di+1)*dim], src.Float32Data[si*dim:(si+1)*dim])
					}
				}
			}
		}
	default:
		// Nested ARRAY/MAP/ROW: typed per-value copy. Without this default
		// the switch fell through writing NOTHING — join probe-side nested
		// columns emitted whatever the pooled destination batch held, rows
		// still marked valid (sibling gatherSortVector always had it).
		for di, si := range srcRows {
			copyVectorValue(dst, di, src, si)
		}
	}
}

// gatherSortVector copies scattered rows from multiple source batches into a
// contiguous destination vector. Uses the prevBatch caching pattern to avoid
// redundant batch lookups when consecutive entries reference the same batch.
// Hoists the type switch outside the loop, eliminating per-row overhead.
func gatherSortVector(dst *batch.Vector, colIdx int, entries []sortEntry, batches []*batch.RecordBatch) {
	switch dst.Type {
	case batch.TypeBool:
		var src *batch.Vector
		prevBatch := uint32(0xFFFFFFFF)
		srcHasNulls := true
		for di, e := range entries {
			if e.batchIdx != prevBatch {
				src = batches[e.batchIdx].Columns[colIdx]
				prevBatch = e.batchIdx
				srcHasNulls = src.Nulls.HasNulls()
			}
			si := int(e.rowIdx)
			if srcHasNulls && src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.BoolData[di] = src.BoolData[si]
			}
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		var src *batch.Vector
		prevBatch := uint32(0xFFFFFFFF)
		srcHasNulls := true
		for di, e := range entries {
			if e.batchIdx != prevBatch {
				src = batches[e.batchIdx].Columns[colIdx]
				prevBatch = e.batchIdx
				srcHasNulls = src.Nulls.HasNulls()
			}
			si := int(e.rowIdx)
			if srcHasNulls && src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Int32Data[di] = src.Int32Data[si]
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		var src *batch.Vector
		prevBatch := uint32(0xFFFFFFFF)
		srcHasNulls := true
		for di, e := range entries {
			if e.batchIdx != prevBatch {
				src = batches[e.batchIdx].Columns[colIdx]
				prevBatch = e.batchIdx
				srcHasNulls = src.Nulls.HasNulls()
			}
			si := int(e.rowIdx)
			if srcHasNulls && src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Int64Data[di] = src.Int64Data[si]
			}
		}
	case batch.TypeFloat32:
		var src *batch.Vector
		prevBatch := uint32(0xFFFFFFFF)
		srcHasNulls := true
		for di, e := range entries {
			if e.batchIdx != prevBatch {
				src = batches[e.batchIdx].Columns[colIdx]
				prevBatch = e.batchIdx
				srcHasNulls = src.Nulls.HasNulls()
			}
			si := int(e.rowIdx)
			if srcHasNulls && src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Float32Data[di] = src.Float32Data[si]
			}
		}
	case batch.TypeFloat64:
		var src *batch.Vector
		prevBatch := uint32(0xFFFFFFFF)
		srcHasNulls := true
		for di, e := range entries {
			if e.batchIdx != prevBatch {
				src = batches[e.batchIdx].Columns[colIdx]
				prevBatch = e.batchIdx
				srcHasNulls = src.Nulls.HasNulls()
			}
			si := int(e.rowIdx)
			if srcHasNulls && src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.Float64Data[di] = src.Float64Data[si]
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		var src *batch.Vector
		prevBatch := uint32(0xFFFFFFFF)
		srcHasNulls := true
		for di, e := range entries {
			if e.batchIdx != prevBatch {
				src = batches[e.batchIdx].Columns[colIdx]
				prevBatch = e.batchIdx
				srcHasNulls = src.Nulls.HasNulls()
			}
			si := int(e.rowIdx)
			if srcHasNulls && src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
				dst.BytesData.Set(di, nil)
			} else {
				dst.BytesData.SetFrom(di, &src.BytesData, si)
			}
		}
	case batch.TypeDecimal:
		var src *batch.Vector
		prevBatch := uint32(0xFFFFFFFF)
		srcHasNulls := true
		for di, e := range entries {
			if e.batchIdx != prevBatch {
				src = batches[e.batchIdx].Columns[colIdx]
				prevBatch = e.batchIdx
				srcHasNulls = src.Nulls.HasNulls()
			}
			si := int(e.rowIdx)
			if srcHasNulls && src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else {
				dst.DecimalData.Data[di] = src.DecimalData.Data[si]
			}
		}
	case batch.TypeVector:
		dim := dst.VectorDim
		var src *batch.Vector
		prevBatch := uint32(0xFFFFFFFF)
		srcHasNulls := true
		for di, e := range entries {
			if e.batchIdx != prevBatch {
				src = batches[e.batchIdx].Columns[colIdx]
				prevBatch = e.batchIdx
				srcHasNulls = src.Nulls.HasNulls()
				if src.VectorDim > 0 {
					dim = src.VectorDim
				}
			}
			si := int(e.rowIdx)
			if srcHasNulls && src.Nulls.IsNullFast(si) {
				dst.Nulls.SetNull(di)
			} else if dim > 0 {
				copy(dst.Float32Data[di*dim:(di+1)*dim], src.Float32Data[si*dim:(si+1)*dim])
			}
		}
	default:
		for di, e := range entries {
			copyVectorValue(dst, di, batches[e.batchIdx].Columns[colIdx], int(e.rowIdx))
		}
	}
}

// compareAny compares two interface{} values. Used by the spill fallback path.
func compareAny(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1 // nulls first
	}
	if b == nil {
		return 1
	}

	switch av := a.(type) {
	case int64:
		bv := toInt64(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int32:
		return compareAny(int64(av), b)
	case float64:
		bv := toFloat64(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float32:
		return compareAny(float64(av), b)
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0
		}
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// topNHeap implements container/heap.Interface for bounded TopN selection.
// The root is the WORST entry (sorts LAST among the heap), so new entries
// that are better can replace it. "Worse" = inverse of the sort order.
type topNHeap struct {
	entries []sortEntry
	less    func(a, b sortEntry) bool // sort-order comparator
}

func (h *topNHeap) Len() int { return len(h.entries) }

// Less returns true if entries[i] is WORSE than entries[j] (should be evicted first).
// This is the inverse of the sort comparator: a max-heap where root = worst.
func (h *topNHeap) Less(i, j int) bool {
	return h.less(h.entries[j], h.entries[i]) // inverted
}
func (h *topNHeap) Swap(i, j int) { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *topNHeap) Push(x any)    { h.entries = append(h.entries, x.(sortEntry)) }
func (h *topNHeap) Pop() any {
	old := h.entries
	n := len(old)
	x := old[n-1]
	h.entries = old[:n-1]
	return x
}

// appendKeyValue writes a value to a byte buffer without fmt.Sprint overhead.
func appendKeyValue(buf []byte, v any) []byte {
	if v == nil {
		return append(buf, "<null>"...)
	}
	switch tv := v.(type) {
	case int64:
		return strconv.AppendInt(buf, tv, 10)
	case int32:
		return strconv.AppendInt(buf, int64(tv), 10)
	case float64:
		return strconv.AppendFloat(buf, tv, 'g', -1, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(tv), 'g', -1, 32)
	case string:
		return append(buf, tv...)
	case bool:
		return strconv.AppendBool(buf, tv)
	default:
		return append(buf, "<unknown>"...)
	}
}
