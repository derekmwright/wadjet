package exec

import (
	"container/heap"
	"context"
	"sort"
	"strconv"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec/kernel"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// SortOrder specifies sort direction.
type SortOrder int

const (
	Ascending SortOrder = iota
	Descending
)

// SortKey defines a column and direction for sorting.
type SortKey struct {
	Column   string
	Order    SortOrder
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
	spillFiles []string
	sorted     []*batch.RecordBatch // materialized sorted results
	pos        int
}

func NewSort(keys []SortKey) *Sort {
	return &Sort{Keys: keys}
}

func (s *Sort) Init(_ context.Context) error {
	s.batches = nil
	s.totalRows = 0
	s.sorted = nil
	s.pos = 0
	return nil
}

func (s *Sort) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.schema == nil {
		s.schema = b.Schema
	}
	s.batches = append(s.batches, b)
	s.totalRows += b.ActiveLen()

	// Spill to disk if memory pressure is high
	if s.Spill != nil && s.Spill.ShouldSpill() && len(s.batches) > 0 {
		var rows []map[string]any
		for _, sb := range s.batches {
			rows = append(rows, sb.ToRows()...)
		}
		path, err := s.Spill.SpillRows(rows)
		if err != nil {
			return err
		}
		s.spillFiles = append(s.spillFiles, path)
		s.batches = s.batches[:0]
		s.totalRows = 0
	}
	return nil
}

func (s *Sort) Finalize(_ context.Context) error {
	if len(s.spillFiles) > 0 {
		return s.finalizeWithSpill()
	}
	return s.finalizeColumnar()
}

// finalizeWithSpill falls back to row-oriented sort when spill files exist.
func (s *Sort) finalizeWithSpill() error {
	// Convert stored batches to rows
	var rows []map[string]any
	for _, b := range s.batches {
		rows = append(rows, b.ToRows()...)
	}
	s.batches = nil

	// Merge spilled data
	for _, spillFile := range s.spillFiles {
		spilled, err := memory.ReadSpilledRows(spillFile)
		if err != nil {
			return err
		}
		rows = append(rows, spilled...)
	}
	s.spillFiles = nil

	sort.SliceStable(rows, func(i, j int) bool {
		for _, key := range s.Keys {
			vi := rows[i][key.Column]
			vj := rows[j][key.Column]
			viNil := vi == nil
			vjNil := vj == nil
			if viNil && vjNil {
				continue
			}
			if viNil || vjNil {
				if key.NullsLast {
					return !viNil // nil sorts last: non-nil < nil
				}
				return viNil // nil sorts first: nil < non-nil
			}
			cmp := compareAny(vi, vj)
			if cmp == 0 {
				continue
			}
			if key.Order == Descending {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	// Materialize into sorted batches.
	// When Limit > 0, only materialize the top Limit rows (Top-K).
	materializeN := len(rows)
	if s.Limit > 0 && s.Limit < materializeN {
		materializeN = s.Limit
	}
	for pos := 0; pos < materializeN; {
		end := pos + batch.DefaultBatchSize
		if end > materializeN {
			end = materializeN
		}
		s.sorted = append(s.sorted, batch.FromRows(s.schema, rows[pos:end]))
		pos = end
	}
	return nil
}

// sortEntry identifies a row within the accumulated batches by batch and row index.
type sortEntry struct {
	batchIdx uint32
	rowIdx   uint16
}

// finalizeColumnar sorts using typed column comparisons on an index array.
func (s *Sort) finalizeColumnar() error {
	if len(s.batches) == 0 {
		return nil
	}

	// Build index entries for all active rows
	entries := make([]sortEntry, 0, s.totalRows)
	for bi, b := range s.batches {
		if b.Sel != nil {
			for _, idx := range b.Sel {
				entries = append(entries, sortEntry{batchIdx: uint32(bi), rowIdx: idx})
			}
		} else {
			for i := 0; i < b.Len; i++ {
				entries = append(entries, sortEntry{batchIdx: uint32(bi), rowIdx: uint16(i)})
			}
		}
	}

	// Resolve sort key column indices and pre-resolve typed comparison kernels
	type resolvedKey struct {
		colIdx    int
		order     SortOrder
		nullsLast bool
		compare   kernel.SortCompareKernel
	}
	firstBatch := s.batches[0]
	resolved := make([]resolvedKey, len(s.Keys))
	for i, key := range s.Keys {
		idx := firstBatch.ColumnIndex(key.Column)
		var cmp kernel.SortCompareKernel
		if idx >= 0 {
			cmp = kernel.ResolveSortCompare(firstBatch.Columns[idx].Type)
		}
		resolved[i] = resolvedKey{colIdx: idx, order: key.Order, nullsLast: key.NullsLast, compare: cmp}
	}

	// Sort comparison function used by both full sort and TopN heap.
	batches := s.batches
	lessFunc := func(ei, ej sortEntry) bool {
		bi, bj := batches[ei.batchIdx], batches[ej.batchIdx]
		ri, rj := int(ei.rowIdx), int(ej.rowIdx)
		for _, key := range resolved {
			if key.colIdx < 0 {
				continue
			}
			vi := bi.Columns[key.colIdx]
			vj := bj.Columns[key.colIdx]
			cmp := key.compare(vi, ri, vj, rj)
			if cmp != 0 && key.nullsLast {
				aiNull := vi.Nulls.IsNull(ri)
				bjNull := vj.Nulls.IsNull(rj)
				if aiNull || bjNull {
					cmp = -cmp // flip null ordering
				}
			}
			if cmp == 0 {
				continue
			}
			if key.order == Descending {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	}

	// When Limit is small relative to total rows, use a bounded heap
	// to find the top Limit entries in O(n log k) instead of O(n log n).
	if s.Limit > 0 && s.Limit < len(entries)/2 {
		k := s.Limit
		// Build a max-heap of size k where the root is the WORST entry
		// (the one we'd evict first). "Worse" = sorts LATER = inverse of lessFunc.
		h := &topNHeap{
			entries: make([]sortEntry, k),
			less:    lessFunc,
		}
		copy(h.entries, entries[:k])
		heap.Init(h)

		for _, e := range entries[k:] {
			// If this entry is better than the worst in the heap, replace
			if lessFunc(e, h.entries[0]) {
				h.entries[0] = e
				heap.Fix(h, 0)
			}
		}

		// Sort the heap entries to get final order
		entries = h.entries
		sort.SliceStable(entries, func(i, j int) bool {
			return lessFunc(entries[i], entries[j])
		})
	} else {
		// Full sort for unlimited or large-limit queries
		sort.SliceStable(entries, func(i, j int) bool {
			return lessFunc(entries[i], entries[j])
		})
	}

	// Materialize sorted batches using typed column copies.
	// When Limit > 0, only materialize the top Limit rows (Top-K).
	materializeN := len(entries)
	if s.Limit > 0 && s.Limit < materializeN {
		materializeN = s.Limit
	}
	for pos := 0; pos < materializeN; {
		end := pos + batch.DefaultBatchSize
		if end > materializeN {
			end = materializeN
		}
		chunk := entries[pos:end]
		out := batch.NewRecordBatch(s.schema, len(chunk))
		// Column-first iteration for better cache locality on destination arrays.
		for j := range s.schema {
			dst := out.Columns[j]
			for i, e := range chunk {
				copyVectorValue(dst, i, batches[e.batchIdx].Columns[j], int(e.rowIdx))
			}
		}
		s.sorted = append(s.sorted, out)
		pos = end
	}

	s.batches = nil // release input batches
	return nil
}

// Truncate keeps only the first n rows of sorted output (Top-K).
func (s *Sort) Truncate(n int) {
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
		sel := make([]uint16, remaining)
		if b.Sel != nil {
			copy(sel, b.Sel[:remaining])
		} else {
			for j := range sel {
				sel[j] = uint16(j)
			}
		}
		b.Sel = sel
		s.sorted = s.sorted[:i+1]
		return
	}
}

func (s *Sort) Close() error { return nil }

// Next returns sorted results in batches.
func (s *Sort) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.pos >= len(s.sorted) {
		return nil, nil
	}
	b := s.sorted[s.pos]
	s.pos++
	return b, nil
}

// copyVectorValue copies a single value between vectors using typed access (no boxing).
// For bytes-based types, values must be copied in sequential order for dst (i = 0, 1, 2, ...).
func copyVectorValue(dst *batch.Vector, di int, src *batch.Vector, si int) {
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
		dst.BytesData.Set(di, src.BytesData.Value(si))
	case batch.TypeDecimal:
		dst.DecimalData.Data[di] = src.DecimalData.Data[si]
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
		if !hasNulls {
			for di, si := range srcRows {
				dst.BytesData.Set(di, src.BytesData.Value(si))
			}
		} else {
			for di, si := range srcRows {
				if src.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
					dst.BytesData.Set(di, nil)
				} else {
					dst.BytesData.Set(di, src.BytesData.Value(si))
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

