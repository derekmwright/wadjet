package exec

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/sqlerr"
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
	// SlotPos is the 1-based position of the input column this key sorts on,
	// or 0 to resolve Column by name.
	//
	// A name is an address only while it is unique. `SELECT n_name AS u,
	// n_regionkey AS u FROM nation ORDER BY 2, 1` sorted by column ONE,
	// because every by-name resolution binds the FIRST column carrying `u` —
	// the right VALUES in the wrong SEQUENCE, which no unordered comparison
	// can see (#557). The planner knows the position exactly and hands it
	// over here.
	SlotPos int
}

// index resolves the key against an input schema: by POSITION when the
// planner supplied one and it is in range, by NAME otherwise.
func (k SortKey) index(b *batch.RecordBatch) int {
	if k.SlotPos > 0 && k.SlotPos <= len(b.Schema) {
		return k.SlotPos - 1
	}
	return columnIndexFallback(b, k.Column)
}

// Sort is a Sink that accumulates all batches columnar and sorts them
// using typed comparisons on an index array (no row-oriented conversion).
// When a SpillManager is set, Sort will spill to disk when memory pressure is high.
// When Limit >= 0, only the top Limit rows are materialized (Top-K
// optimization) — Limit is a real row count, so LIMIT 0 correctly
// materializes zero rows.
type Sort struct {
	Keys []SortKey
	// Limit is the Top-K row bound, or NoLimit (-1, the same sentinel
	// exec.Limit.Max and logical.NoLimit use) when the sort is unbounded.
	// Every reader below tests `>= 0` (never `> 0`) so a real zero is never
	// mistaken for "unbounded" — that collision was #481:
	// `ORDER BY ... LIMIT 0` returned every row because 0 doubled as the
	// "no limit" sentinel.
	Limit  int
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
	// consumesSinceSpill counts Consumes for the TEST-ONLY spill forcing knob
	// (sort_force_spill.go). Untouched when the knob is disarmed.
	consumesSinceSpill int64
}

func NewSort(keys []SortKey) *Sort {
	return &Sort{Keys: keys, Limit: -1}
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
			if k.index(b) < 0 {
				return unresolvedSortKey(k.Column)
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
	if s.Limit >= 0 && (s.totalRows > s.Limit*2+4*batch.DefaultBatchSize ||
		s.trackedMem > 96<<20 || len(s.batches) >= 4) {
		if err := s.compactTopKLocked(); err != nil {
			return err
		}
	}

	// Spill to disk if memory pressure is high. The columnar run path
	// accumulates at least minSortRunBytes before flushing so runs stay
	// merge-friendly. Peer relief (SpillSome) bypasses the floor.
	if s.Spill != nil && len(s.batches) > 0 {
		// The TEST-ONLY knob takes the same branch pressure would have taken,
		// and skips the minSortRunBytes floor: that floor is a merge-economy
		// heuristic sized against real pressure, and there is none here. See
		// sort_force_spill.go for why the pressure trigger alone cannot be
		// relied on to fire.
		forced := s.forcedSpillDue()
		if forced || s.Spill.ShouldSpillFor(memory.SpillCheap) {
			if forced || s.trackedMem >= minSortRunBytes {
				freed, err := s.flushSpillLocked()
				if err != nil {
					return err
				}
				if forced && freed > 0 {
					ForcedSortSpills.Add(1)
				}
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
		SortRunsWritten.Add(1)
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
// s.Limit must be >= 0 (a real bound, possibly zero), and s.schema must be
// set.
func (s *Sort) compactTopKLocked() error {
	if len(s.batches) == 0 {
		return nil
	}
	entries := buildSortEntries(s.batches, s.totalRows)
	resolved, err := resolveSortKeysForBatches(s.Keys, s.batches)
	if err != nil {
		return err
	}
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
	return nil
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
		resolved, err := resolveSortKeysForBatches(s.Keys, s.batches)
		if err != nil {
			for _, prev := range cursors {
				prev.close()
			}
			removeRunFiles(runs)
			return err
		}
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

	merger, err := newRunMerger(s.schema, s.Keys, cursors)
	if err != nil {
		for _, prev := range cursors {
			prev.close()
		}
		removeRunFiles(runs)
		return err
	}
	s.merger = merger
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
	resolved, err := resolveSortKeysForBatches(s.Keys, s.batches)
	if err != nil {
		return err
	}
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
		if s.Limit < 0 || n < s.Limit {
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
	// EVERY sorted run the clone wrote belongs to the primary now, and the
	// transfer comes FIRST — ADR-0027 decision 1, which HashAggregate.
	// mergeSinkState already applies to its four artifact lists. A Sort clone
	// has one list, `runFiles`, and it was not transferred at all: the
	// clone's Close then deleted the files (`removeRunFiles(s.runFiles)`) and
	// every row in them left the answer with no error anywhere — 1,100 /
	// 2,800 / 3,300 rows of 5,000, each a whole number of source batches
	// (#864, #790's shape on the Sort).
	//
	// `finalizeExternalMerge` merges the runs and the in-memory remainder in
	// key order, so which sink wrote a run does not matter; that all of them
	// are present does.
	if len(o.runFiles) > 0 {
		s.runFiles = append(s.runFiles, o.runFiles...)
		o.runFiles = nil
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
	if s.Limit >= 0 {
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
		// kernel's float order, so the boxed path and the columnar one agree
		// on a NaN: greatest, and equal to itself (#446).
		return kernel.CompareFloat64(av, toFloat64(b))
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
	case []byte:
		bv, ok := b.([]byte)
		if !ok {
			return 0
		}
		return bytes.Compare(av, bv)
	case []any:
		// ARRAY, and a MAP boxed as its list of entry maps. Element-wise
		// then by length — kernel.CompareValuesAt's rule for the columnar
		// form of the same value (#415).
		bv, ok := b.([]any)
		if !ok {
			return 0
		}
		n := min(len(av), len(bv))
		for i := 0; i < n; i++ {
			if c := compareAnyElem(av[i], bv[i]); c != 0 {
				return c
			}
		}
		return cmpInt(len(av), len(bv))
	case map[string]any:
		// ROW, and a MAP entry. Compared in FIELD-NAME order, because that
		// is all a boxed row carries with no declaration to consult:
		// Vector.GetValue renders a ROW as a Go map, which has no
		// declaration order. The columnar comparator compares fields
		// POSITIONALLY, as PostgreSQL's record_cmp does — so a caller that
		// HAS the declaration must resolve newBoxedCompare (compare_boxed.go)
		// instead of calling this, and every production caller now does
		// (#444). What is left here is the genuinely undeclared case, where
		// the box's own name order is the only order there is.
		bv, ok := b.(map[string]any)
		if !ok {
			return 0
		}
		return compareAnyFields(av, bv)
	case []float32:
		// VECTOR. Element-wise in kernel's float order — a bare `<`/`>` pair
		// ties a NaN against whatever sits opposite it, which does not
		// compose into a transitive whole-vector relation (#446).
		bv, ok := b.([]float32)
		if !ok {
			return 0
		}
		n := min(len(av), len(bv))
		for i := 0; i < n; i++ {
			if c := kernel.CompareFloat32(av[i], bv[i]); c != 0 {
				return c
			}
		}
		return cmpInt(len(av), len(bv))
	default:
		return 0
	}
}

// compareAnyElem compares one element INSIDE a boxed container, where a NULL
// sorts AFTER a non-NULL — PostgreSQL's array_cmp/record_cmp rule, and what
// kernel's columnar comparators apply. compareAny's own nil handling is the
// COLUMN-level one (nulls first), which is a different question.
func compareAnyElem(a, b any) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	}
	return compareAny(a, b)
}

// compareAnyFields compares two boxed ROW/MAP-entry values in field-name
// order: names first, so {"a":1} and {"b":1} are ordered rather than tied,
// then values.
func compareAnyFields(a, b map[string]any) int {
	an := sortedKeys(a)
	bn := sortedKeys(b)
	n := min(len(an), len(bn))
	for i := 0; i < n; i++ {
		if an[i] != bn[i] {
			if an[i] < bn[i] {
				return -1
			}
			return 1
		}
		if c := compareAnyElem(a[an[i]], b[bn[i]]); c != 0 {
			return c
		}
	}
	return cmpInt(len(an), len(bn))
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func cmpInt(x, y int) int {
	if x < y {
		return -1
	}
	if x > y {
		return 1
	}
	return 0
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
//
// This is the k-way MERGE key for drained partial aggregate runs
// (appendSerializedKey, aggregate_partial_drain_cursor.go), so a type that
// falls through here does not merely sort oddly — every group of that type
// merges into ONE. The default used to be the constant string "<unknown>",
// which did exactly that to a BYTES group key: distinct in memory, collapsed
// into a single group the moment memory pressure forced a drain, so the same
// query answered differently depending on how much memory it had.
//
// Boxed forms reaching here come from Vector.GetValue: bool, int32, int64,
// float32, float64, string (STRING and every type that renders as text —
// IPV6, CIDR, UUID, IPV4, MAC, DATE, DECIMAL), []byte (BYTES), []any (ARRAY),
// map[string]any (ROW, MAP) and []float32 (VECTOR).
//
// The encoding must be INJECTIVE, not merely deterministic. Two group keys
// that share bytes are one group after a drain, and the query answers
// differently depending on how much memory it had — the same failure the
// "<unknown>" constant caused, reached by a subtler route. `%v` is not
// injective for any container (ARRAY["a b"] and ARRAY["a","b"] both print
// `[a b]`; ROW{a:"b c:d"} and ROW{a:"b",c:"d"} both print `map[a:b c:d]`),
// and a raw byte run is not injective against serializeKey's single 0x00
// separator (BYTES "a\x00" ‖ "b" and BYTES "a" ‖ "\x00b" are the same five
// bytes). So every variable-width form is length-prefixed and every
// container walks its elements, mirroring appendColumnValue's framing.
//
// The fixed-width text forms — the integers, floats and bools — are
// unchanged: they contain no 0x00, so the separator still delimits them, and
// appendTypedIntKey (aggregate_partial_drain_cursor.go) writes the same bytes
// for an int-mode key without boxing it.
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
		// canonicalFloat64: -0.0 must render as "0" like +0.0 does, or the
		// two would key differently despite comparing equal (kernel/
		// float_order.go). NaN already renders as the literal string "NaN"
		// regardless of payload, so no fold is needed for that case here.
		return strconv.AppendFloat(buf, canonicalFloat64(tv), 'g', -1, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(canonicalFloat32(tv)), 'g', -1, 32)
	case string:
		// A length prefix, not the raw run: without it a NUL inside the
		// value is indistinguishable from the column separator — and a
		// literal "<null>" is indistinguishable from a NULL key.
		return appendKeyRaw(buf, tv)
	case []byte:
		return appendKeyRaw(buf, string(tv))
	case bool:
		return strconv.AppendBool(buf, tv)
	case []any:
		// ARRAY (and a MAP boxed as a list of entries).
		return appendKeyElems(buf, tv)
	case map[string]any:
		// ROW and MAP. Field names are sorted, as fmt sorted them, and are
		// part of the key: {"a":"x"} and {"b":"x"} are different values.
		return appendKeyFields(buf, tv)
	case []float32:
		// VECTOR.
		return appendKeyFloats(buf, tv)
	default:
		// A boxed shape Vector.GetValue does not produce. Rendered rather
		// than dropped, and length-prefixed like every other variable-width
		// case so it cannot swallow a separator.
		return appendKeyRaw(buf, fmt.Sprint(tv))
	}
}

// appendKeyValueWithMeta is appendKeyValue with the value's DECLARED column
// type available, so a CIDR value re-keys through kernel.CidrOrderKey even
// nested inside an ARRAY, MAP or ROW — where appendKeyValue's plain `any`
// switch has no type tag to tell a CIDR string from an ordinary one, the same
// gap appendSerializedKey already closes for a bare top-level CIDR column.
//
// Without this arm, GROUP BY arr_cidr agreed with the un-spilled columnar key
// (appendColumnValue → appendListKey → appendNestedElem, which walks the real
// *batch.Vector tree and re-keys every CIDR leaf already) only until a
// cross-batch, cross-worker or spill-boundary MERGE went through this boxed
// path instead: '10.0.0.1' and '10.0.0.1/32' inside the array serialized to
// two different byte strings, so a k-way merge of otherwise-identical groups
// answered two groups where the un-spilled path already answers one.
//
// Every leaf type this does not name keeps appendKeyValue's existing
// encoding exactly — this only intercepts CIDR and recurses into a
// container's own element/field metadata to find one.
//
// The recursion goes through appendKeyElemWithMeta, NOT back through this
// function. A container's elements are framed (a kind tag, then a fixed-width
// or length-prefixed payload) precisely because there is no separator down
// there; recursing here instead wrote each element in the TOP-LEVEL encoding,
// which for an int64 is bare decimal digits, and ARRAY[1,23] and ARRAY[12,3]
// became the same key.
func appendKeyValueWithMeta(buf []byte, v any, meta *parquet.Column) []byte {
	if meta == nil || v == nil {
		return appendKeyValue(buf, v)
	}
	switch meta.Type {
	case parquet.TypeCIDR:
		if s, ok := v.(string); ok {
			return appendKeyValue(buf, kernel.CidrOrderKey(s))
		}
	case parquet.TypeArray, parquet.TypeMap:
		if vals, ok := v.([]any); ok {
			return appendKeyElemsWithMeta(buf, vals, meta.ElementType)
		}
	case parquet.TypeRow:
		if m, ok := v.(map[string]any); ok {
			return appendKeyFieldsWithMeta(buf, m, meta)
		}
	}
	return appendKeyValue(buf, v)
}

// AppendBoxedGroupKey is appendKeyValueWithMeta under an exported name, for
// the ONE consumer outside this package that builds the same key from the
// same boxed value: the coordinator's cross-worker GROUP BY re-aggregation
// (reAggregatePartials' keyEncoders).
//
// That layer keys a container column by `fmt.Appendf("%v", ...)`, which is
// not injective for any of the four — ARRAY['a b'] and ARRAY['a','b'] both
// render `[a b]`, ROW{a:'b c:d'} and ROW{a:'b',c:'d'} both render
// `map[a:b c:d]` — so two distinct groups merged into one at the coordinator
// while every worker, and the single-process engine, kept them apart. It was
// unreachable while a container GROUP BY failed outright (#566/#576); making
// those queries answer is what exposes it, so the two fixes belong together.
//
// Exporting THIS rather than a fresh encoder is the point: the bytes have to
// be the ones the engine's own boxed merge key produces, or the coordinator
// re-splits what a worker merged. That includes the float fold (a NaN payload
// and a -0.0 are not part of a value's identity, kernel/float_order.go) and
// the CIDR re-key (#520), both of which a value-preserving encoding —
// appendContainerKeyValue, which the drained partial's VALUE uses — must not
// apply and this one must.
func AppendBoxedGroupKey(dst []byte, v any, col *parquet.Column) []byte {
	if col == nil {
		return appendKeyValue(dst, v)
	}
	// appendGroupKeyColumn, not appendKeyValueWithMeta: the declared type is
	// right here, and dispatching on it is what gives a DATE / IPv4 / MAC key
	// the same bytes the engine's own int-keyed drain writes (#788). Routing
	// the one EXPORTED producer through anything else would be a second
	// definition of equality again, one package over.
	return appendGroupKeyColumn(dst, v, col.Type, col)
}

// appendKeyElemsWithMeta is appendKeyElems with the elements' DECLARED type
// available. appendKeyFieldsWithMeta is appendKeyFields' counterpart.
//
// They exist because the meta path used to recurse into
// appendKeyValueWithMeta, which falls back to appendKeyValue for an ordinary
// leaf — the TOP-LEVEL encoding, where an int64 is bare decimal digits with no
// kind tag and no length. Inside a container there is no separator to delimit
// those, which is the whole reason appendKeyElem exists and exactly what its
// own const block warns about: ARRAY[1,23] and ARRAY[12,3] both serialized to
// 02 31 32 33 through here, so two different GROUP BY keys merged into one
// past a spill boundary and stayed two before it. Recursing through
// appendKeyElemWithMeta restores the framing and keeps the CIDR re-key.
func appendKeyElemsWithMeta(buf []byte, vals []any, elem *parquet.Column) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(vals)))
	for _, e := range vals {
		buf = appendKeyElemWithMeta(buf, e, elem)
	}
	return buf
}

func appendKeyFieldsWithMeta(buf []byte, m map[string]any, meta *parquet.Column) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(m)))
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	fieldMeta := make(map[string]*parquet.Column, len(meta.Fields))
	for i := range meta.Fields {
		fieldMeta[meta.Fields[i].Name] = &meta.Fields[i]
	}
	for _, name := range names {
		buf = appendKeyRaw(buf, name)
		buf = appendKeyElemWithMeta(buf, m[name], fieldMeta[name])
	}
	return buf
}

// appendKeyElemWithMeta is appendKeyElem with the element's DECLARED type
// available: it writes the same kind tag and payload for every type, and
// intercepts only the two the declaration is needed for — a CIDR leaf, which
// re-keys into PostgreSQL's inet order (#520), and a nested container, whose
// own element/field metadata has to travel one level further down.
//
// Every other type falls through to appendKeyElem, so a CIDR-free value
// serializes BYTE-IDENTICALLY whether or not its column was declared with
// element metadata. That equality is the property
// TestSerializedKeyMetaMatchesPlainEncoding asserts, and it is what makes the
// meta path safe to take for a container column whose leaves happen to hold no
// CIDR at all.
func appendKeyElemWithMeta(buf []byte, v any, meta *parquet.Column) []byte {
	if meta == nil || v == nil {
		return appendKeyElem(buf, v)
	}
	switch meta.Type {
	case parquet.TypeCIDR:
		if s, ok := v.(string); ok {
			// The same tag a plain nested string gets: the re-key changes the
			// BYTES a CIDR value contributes, never its framing.
			return appendKeyRaw(append(buf, keyElemString), kernel.CidrOrderKey(s))
		}
	case parquet.TypeArray, parquet.TypeMap:
		if vals, ok := v.([]any); ok {
			return appendKeyElemsWithMeta(append(buf, keyElemList), vals, meta.ElementType)
		}
	case parquet.TypeRow:
		if m, ok := v.(map[string]any); ok {
			return appendKeyFieldsWithMeta(append(buf, keyElemFields), m, meta)
		}
	}
	return appendKeyElem(buf, v)
}

// appendKeyRaw writes a uvarint length then the bytes. Uvarint rather than a
// fixed 4 bytes because the drain cursor's key arena holds one of these per
// string group-key column per group: a short value pays one byte.
func appendKeyRaw(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// appendKeyElems writes an ARRAY: its element count, then each element.
// The count is what makes it injective — without it [["a"],["b"]] and
// [["a","b"]] would produce the same bytes (appendListKey, aggregate.go,
// makes the same argument for the columnar form).
func appendKeyElems(buf []byte, vals []any) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(vals)))
	for _, e := range vals {
		buf = appendKeyElem(buf, e)
	}
	return buf
}

// appendKeyFields writes a ROW or MAP: its field count, then each
// name/value pair in name order.
func appendKeyFields(buf []byte, m map[string]any) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(m)))
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		buf = appendKeyRaw(buf, name)
		buf = appendKeyElem(buf, m[name])
	}
	return buf
}

// appendKeyFloats writes a VECTOR: its dimension, then every element's
// IEEE-754 bits. The dimension is part of the key because two vectors of
// different width are different values even when one is the other's prefix.
func appendKeyFloats(buf []byte, vals []float32) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(vals)))
	for _, f := range vals {
		buf = binary.LittleEndian.AppendUint32(buf, keyFloat32bits(f))
	}
	return buf
}

// canonicalFloat32 / canonicalFloat64 fold a value onto the one bit pattern
// PostgreSQL's float order treats as canonical for it: every NaN payload onto
// one NaN, and -0.0 onto +0.0 (kernel/float_order.go's CompareFloat32/64 say
// both pairs compare EQUAL, so a value that reaches here through GetValue's
// float64/float32 box and a value that reaches here through a vector's raw
// bits must key alike). Used by keyFloat32bits/keyFloat64bits (below) and by
// appendKeyValue's float arms, so the same fold applies whether the caller
// keys by bit pattern or by formatting the float64/float32 the box holds.
// The rule itself lives with the comparator it has to agree with
// (kernel.CanonicalFloat32/64, kernel/float_order.go) — the group key, the
// join key, the shuffle's partition router and this merge key all state it
// once, from there.
func canonicalFloat32(f float32) float32 { return kernel.CanonicalFloat32(f) }

func canonicalFloat64(f float64) float64 { return kernel.CanonicalFloat64(f) }

// keyFloat32bits / keyFloat64bits are Float32bits/Float64bits with every NaN
// folded onto one payload and -0.0 folded onto +0.0.
//
// The comparators order NaN as a single value — greatest, and equal to itself
// (kernel/float_order.go, #446) — and -0.0 as equal to +0.0, and the
// container-order contract is that "compares equal" and "serializes alike"
// name the same relation. Raw IEEE bits break both: two NaNs of different
// payload, or -0.0 and +0.0, compare EQUAL and would serialize DIFFERENTLY,
// so a drained partial aggregate would split one group in two and answer
// differently depending on how much memory it had (and a GROUP BY through
// this path would disagree with `rank() OVER (ORDER BY f)`, which the
// comparator already puts in one peer group). A payload, and a zero's sign,
// are not part of a DECIMAL-free SQL value's identity, so folding them is the
// side to give.
func keyFloat32bits(f float32) uint32 { return kernel.KeyFloat32Bits(f) }

func keyFloat64bits(f float64) uint64 { return kernel.KeyFloat64Bits(f) }

// Nested-element kind tags. Every nested element is SELF-DELIMITING: a tag,
// then a fixed-width or length-prefixed payload. The top level's text forms
// cannot be reused inside a container — text carries no length and there is
// no separator down here, so two adjacent elements would run together
// (ARRAY[1,23] and ARRAY[12,3] are the same digits).
const (
	keyElemNull byte = iota
	keyElemFalse
	keyElemTrue
	keyElemInt32
	keyElemInt64
	keyElemFloat32
	keyElemFloat64
	keyElemString
	keyElemBytes
	keyElemList
	keyElemFields
	keyElemFloats
	keyElemOther
)

// appendKeyElem writes one nested element: its kind tag, then its payload.
// The tag also keeps a nested NULL distinct from a zero or an empty string,
// the same job appendNestedElem's flag byte does for the columnar form.
func appendKeyElem(buf []byte, v any) []byte {
	switch tv := v.(type) {
	case nil:
		return append(buf, keyElemNull)
	case bool:
		if tv {
			return append(buf, keyElemTrue)
		}
		return append(buf, keyElemFalse)
	case int32:
		return binary.LittleEndian.AppendUint32(append(buf, keyElemInt32), uint32(tv))
	case int64:
		return binary.LittleEndian.AppendUint64(append(buf, keyElemInt64), uint64(tv))
	case float32:
		return binary.LittleEndian.AppendUint32(append(buf, keyElemFloat32), keyFloat32bits(tv))
	case float64:
		return binary.LittleEndian.AppendUint64(append(buf, keyElemFloat64), keyFloat64bits(tv))
	case string:
		return appendKeyRaw(append(buf, keyElemString), tv)
	case []byte:
		return appendKeyRaw(append(buf, keyElemBytes), string(tv))
	case []any:
		return appendKeyElems(append(buf, keyElemList), tv)
	case map[string]any:
		return appendKeyFields(append(buf, keyElemFields), tv)
	case []float32:
		return appendKeyFloats(append(buf, keyElemFloats), tv)
	default:
		return appendKeyRaw(append(buf, keyElemOther), fmt.Sprint(tv))
	}
}

// A key that names no column of its input is 0A000 (feature_not_supported),
// not the blanket class.
//
// It reaches a CLIENT — `SELECT x.w FROM (SELECT g*3 AS w FROM t ORDER BY w
// LIMIT 5) x ORDER BY x.w` is a query PostgreSQL ANSWERS, and on the stage DAG
// it arrives here (#807/#658) — so "every failure a client sees carries its
// SQLSTATE" (#649) covers it. It carried none: `ERR[]`, while the census's own
// 22003 and 22012 cells carried theirs.
//
// 0A000 and not 42703 or XX000 for the reason commit 7 gave the five
// order_by_keys.go refusals it classified: PostgreSQL answers the query, so the
// class a client is owed is "this engine does not implement it", not "your SQL
// is wrong" and not "tell someone". The MESSAGE keeps naming the planner bug —
// this is still a backstop against a wrong answer, not a supported refusal —
// and the class is what a client branches on.
func unresolvedSortKey(col string) error {
	return sqlerr.New("0A000",
		"sort: key column %q does not exist in the input schema", col)
}
