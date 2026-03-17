package exec

import (
	"context"
	"sort"
	"sync"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/memory"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// WindowFunc identifies a window function type.
type WindowFunc int

const (
	WinRowNumber WindowFunc = iota
	WinRank
	WinDenseRank
	WinSum
	WinCount
	WinAvg
	WinMin
	WinMax
	WinLag
	WinLead
	WinFirstValue
	WinLastValue
	WinNtile
	WinPercentRank
	WinCumeDist
	WinNthValue
)

// WindowFrameSpec describes a window frame specification for execution.
type WindowFrameSpec struct {
	Mode  string // "rows" or "range"
	Start WindowBound
	End   WindowBound
}

// WindowBound describes one end of a window frame.
type WindowBound struct {
	Type   string // "unbounded_preceding", "preceding", "current_row", "following", "unbounded_following"
	Offset int
}

// WindowColumn defines a window function computation.
type WindowColumn struct {
	Func           WindowFunc
	InputCol       string // for aggregate window functions (empty for ranking funcs)
	OutputCol      string
	OutputType     parquet.TypeID
	PartitionBy    []string
	OrderBy        []SortKey
	Frame          *WindowFrameSpec // optional frame specification
	LagLeadOffset  int              // offset for LAG/LEAD (default 1)
	LagLeadDefault any              // default value for LAG/LEAD (default NULL)
	NtileBuckets   int              // number of buckets for NTILE
	NthValueN      int              // N for NTH_VALUE (1-based)
}

// Window is a SinkSource that collects all rows, partitions and sorts them,
// computes window function values, and emits the original rows with computed
// window columns appended. Operates directly on column vectors to avoid
// map[string]any materialization overhead.
// When a SpillManager is set, Window will spill input batches to disk under
// memory pressure and read them back during Finalize.
type Window struct {
	Columns []WindowColumn
	Spill   *memory.SpillManager // optional: enables spill-to-disk

	mu         sync.Mutex
	batches    []*batch.RecordBatch
	totalRows  int
	schema     []parquet.Column
	spillFiles []string

	result  []*batch.RecordBatch
	pos     int
	emitted bool
}

// NewWindow creates a new window operator.
func NewWindow(cols []WindowColumn) *Window {
	return &Window{Columns: cols}
}

func (w *Window) Init(_ context.Context) error {
	w.batches = nil
	w.totalRows = 0
	w.result = nil
	w.pos = 0
	w.emitted = false
	return nil
}

func (w *Window) Consume(_ context.Context, b *batch.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.schema == nil {
		w.schema = b.Schema
	}
	w.batches = append(w.batches, b)
	w.totalRows += b.ActiveLen()

	// Spill to disk if memory pressure is high
	if w.Spill != nil && w.Spill.ShouldSpill() && len(w.batches) > 0 {
		var rows []map[string]any
		for _, sb := range w.batches {
			rows = append(rows, sb.ToRows()...)
		}
		path, err := w.Spill.SpillRows(rows)
		if err != nil {
			return err
		}
		w.spillFiles = append(w.spillFiles, path)
		w.batches = w.batches[:0]
		w.totalRows = 0
	}
	return nil
}

func (w *Window) Finalize(_ context.Context) error {
	// Collect all data as batches
	allBatches := w.batches
	w.batches = nil

	// Read spill files back as batches
	for _, f := range w.spillFiles {
		spilled, err := memory.ReadSpilledRows(f)
		if err != nil {
			return err
		}
		if len(spilled) > 0 {
			allBatches = append(allBatches, batch.FromRows(w.schema, spilled))
		}
	}
	w.spillFiles = nil

	// Concatenate into single combined batch
	combined := windowConcatBatches(allBatches, w.schema)
	if combined == nil || combined.Len == 0 {
		return nil
	}

	// Build output schema: original columns + window columns
	outSchema := make([]parquet.Column, len(w.schema))
	copy(outSchema, w.schema)
	for _, wc := range w.Columns {
		outSchema = append(outSchema, parquet.Column{
			Name:     wc.OutputCol,
			Type:     wc.OutputType,
			Nullable: true,
		})
	}

	// Add window output vectors to combined batch (initialized as all-null)
	for _, wc := range w.Columns {
		vec := batch.NewVector(wc.OutputType, combined.Len)
		vec.Nulls = batch.NewBitmapAllNull(combined.Len)
		combined.Columns = append(combined.Columns, vec)
		combined.Schema = append(combined.Schema, parquet.Column{
			Name:     wc.OutputCol,
			Type:     wc.OutputType,
			Nullable: true,
		})
	}

	// Compute each window function directly on column vectors
	numOrigCols := len(w.schema)
	for i, wc := range w.Columns {
		computeWindowColumnar(combined, numOrigCols+i, wc)
	}

	// Slice combined batch into output batches
	for pos := 0; pos < combined.Len; {
		end := pos + batch.DefaultBatchSize
		if end > combined.Len {
			end = combined.Len
		}
		batchLen := end - pos
		out := batch.NewRecordBatch(outSchema, batchLen)
		for j := range outSchema {
			windowCopyVectorRange(out.Columns[j], combined.Columns[j], 0, pos, batchLen)
		}
		w.result = append(w.result, out)
		pos = end
	}
	return nil
}

func (w *Window) Close() error { return nil }

// Next returns windowed results in batches.
func (w *Window) Next(_ context.Context) (*batch.RecordBatch, error) {
	if w.pos >= len(w.result) {
		return nil, nil
	}
	b := w.result[w.pos]
	w.pos++
	return b, nil
}

// --- Columnar helpers ---

// windowConcatBatches combines multiple RecordBatches into a single batch.
// Selection vectors are applied before concatenation.
func windowConcatBatches(batches []*batch.RecordBatch, schema []parquet.Column) *batch.RecordBatch {
	totalRows := 0
	for _, b := range batches {
		totalRows += b.ActiveLen()
	}
	if totalRows == 0 {
		return nil
	}
	combined := batch.NewRecordBatch(schema, totalRows)
	pos := 0
	for _, b := range batches {
		cb := b.Compact()
		n := cb.Len
		for j := range schema {
			windowCopyVectorRange(combined.Columns[j], cb.Columns[j], pos, 0, n)
		}
		pos += n
	}
	return combined
}

// windowCopyVectorRange copies count values from src[srcOff..] to dst[dstOff..].
// Uses native slice copy for fixed-width types.
func windowCopyVectorRange(dst, src *batch.Vector, dstOff, srcOff, count int) {
	// Copy null bitmap
	for i := 0; i < count; i++ {
		if src.Nulls.IsNullFast(srcOff + i) {
			dst.Nulls.SetNull(dstOff + i)
		}
	}
	switch dst.Type {
	case batch.TypeBool:
		copy(dst.BoolData[dstOff:dstOff+count], src.BoolData[srcOff:srcOff+count])
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		copy(dst.Int32Data[dstOff:dstOff+count], src.Int32Data[srcOff:srcOff+count])
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		copy(dst.Int64Data[dstOff:dstOff+count], src.Int64Data[srcOff:srcOff+count])
	case batch.TypeFloat32:
		copy(dst.Float32Data[dstOff:dstOff+count], src.Float32Data[srcOff:srcOff+count])
	case batch.TypeFloat64:
		copy(dst.Float64Data[dstOff:dstOff+count], src.Float64Data[srcOff:srcOff+count])
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		for i := 0; i < count; i++ {
			if !src.Nulls.IsNullFast(srcOff + i) {
				dst.BytesData.Set(dstOff+i, src.BytesData.Value(srcOff+i))
			}
		}
	case batch.TypeDecimal:
		copy(dst.DecimalData.Data[dstOff:dstOff+count], src.DecimalData.Data[srcOff:srcOff+count])
	default:
		for i := 0; i < count; i++ {
			if !src.Nulls.IsNullFast(srcOff + i) {
				dst.SetValue(dstOff+i, src.GetValue(srcOff+i))
			}
		}
	}
}

// windowGatherBatch creates a new batch by gathering rows according to permutation.
func windowGatherBatch(src *batch.RecordBatch, perm []int) *batch.RecordBatch {
	n := len(perm)
	dst := batch.NewRecordBatch(src.Schema, n)
	for j := range src.Schema {
		windowGatherVector(dst.Columns[j], src.Columns[j], perm)
	}
	return dst
}

// windowGatherVector reorders a vector according to a permutation array.
func windowGatherVector(dst, src *batch.Vector, perm []int) {
	switch dst.Type {
	case batch.TypeBool:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.BoolData[i] = src.BoolData[p]
			}
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.Int32Data[i] = src.Int32Data[p]
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.Int64Data[i] = src.Int64Data[p]
			}
		}
	case batch.TypeFloat32:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.Float32Data[i] = src.Float32Data[p]
			}
		}
	case batch.TypeFloat64:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.Float64Data[i] = src.Float64Data[p]
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
				dst.BytesData.Set(i, nil)
			} else {
				dst.BytesData.Set(i, src.BytesData.Value(p))
			}
		}
	case batch.TypeDecimal:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.DecimalData.Data[i] = src.DecimalData.Data[p]
			}
		}
	default:
		for i, p := range perm {
			dst.SetValue(i, src.GetValue(p))
		}
	}
}

// compareVectorValues compares two values in a vector without boxing.
// Returns -1, 0, or 1.
func compareVectorValues(col *batch.Vector, a, b int) int {
	switch col.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		va, vb := col.Int64Data[a], col.Int64Data[b]
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeFloat64:
		va, vb := col.Float64Data[a], col.Float64Data[b]
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		va, vb := col.Int32Data[a], col.Int32Data[b]
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeFloat32:
		va, vb := col.Float32Data[a], col.Float32Data[b]
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeBool:
		va, vb := col.BoolData[a], col.BoolData[b]
		if !va && vb {
			return -1
		}
		if va && !vb {
			return 1
		}
		return 0
	case batch.TypeString:
		va := col.BytesData.StringValue(a)
		vb := col.BytesData.StringValue(b)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	default:
		return compareAny(col.GetValue(a), col.GetValue(b))
	}
}

// vecFloat64 reads a float64 value from any numeric vector without boxing.
func vecFloat64(v *batch.Vector, i int) float64 {
	if v == nil || v.Nulls.IsNullFast(i) {
		return 0
	}
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[i]
	case batch.TypeFloat32:
		return float64(v.Float32Data[i])
	case batch.TypeInt64, batch.TypeTimestamp:
		return float64(v.Int64Data[i])
	case batch.TypeInt32:
		return float64(v.Int32Data[i])
	default:
		return 0
	}
}

// sameColumnar returns true if two rows have equal values for the given column indices.
func sameColumnar(combined *batch.RecordBatch, a, b int, colIdxs []int) bool {
	for _, idx := range colIdxs {
		if idx < 0 {
			continue
		}
		col := combined.Columns[idx]
		aNil := col.Nulls.IsNullFast(a)
		bNil := col.Nulls.IsNullFast(b)
		if aNil != bNil {
			return false
		}
		if aNil {
			continue // both null, considered equal
		}
		if compareVectorValues(col, a, b) != 0 {
			return false
		}
	}
	return true
}

// --- Columnar window computation ---

// computeWindowColumnar computes a single window function over columnar data.
// It sorts the combined batch in-place by the window's partition/order keys,
// then walks partitions and computes values directly on column vectors.
func computeWindowColumnar(combined *batch.RecordBatch, winVecIdx int, wc WindowColumn) {
	n := combined.Len
	if n == 0 {
		return
	}

	// Build sort keys: partition columns first, then order columns
	var sortKeys []SortKey
	for _, pc := range wc.PartitionBy {
		sortKeys = append(sortKeys, SortKey{Column: pc, Order: Ascending})
	}
	sortKeys = append(sortKeys, wc.OrderBy...)

	if len(sortKeys) > 0 {
		// Resolve column indices for sort
		sortKeyIdxs := make([]int, len(sortKeys))
		for i, key := range sortKeys {
			sortKeyIdxs[i] = combined.ColumnIndex(key.Column)
		}

		// Build and sort permutation
		perm := make([]int, n)
		for i := range perm {
			perm[i] = i
		}
		sort.SliceStable(perm, func(a, b int) bool {
			for ki, key := range sortKeys {
				idx := sortKeyIdxs[ki]
				if idx < 0 {
					continue
				}
				col := combined.Columns[idx]
				aNil := col.Nulls.IsNullFast(perm[a])
				bNil := col.Nulls.IsNullFast(perm[b])
				if aNil && bNil {
					continue
				}
				if aNil || bNil {
					if key.NullsLast {
						return !aNil
					}
					return aNil
				}
				cmp := compareVectorValues(col, perm[a], perm[b])
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

		// Gather combined batch by permutation (sort in-place)
		gathered := windowGatherBatch(combined, perm)
		for j := range combined.Columns {
			combined.Columns[j] = gathered.Columns[j]
		}
	}

	// Re-acquire winVec after potential gather
	winVec := combined.Columns[winVecIdx]

	// Resolve column indices
	inputIdx := -1
	if wc.InputCol != "" {
		inputIdx = combined.ColumnIndex(wc.InputCol)
	}
	partIdxs := make([]int, len(wc.PartitionBy))
	for i, col := range wc.PartitionBy {
		partIdxs[i] = combined.ColumnIndex(col)
	}
	orderIdxs := make([]int, len(wc.OrderBy))
	for i, key := range wc.OrderBy {
		orderIdxs[i] = combined.ColumnIndex(key.Column)
	}

	// Walk partitions on sorted data
	i := 0
	for i < n {
		partEnd := i + 1
		for partEnd < n && sameColumnar(combined, i, partEnd, partIdxs) {
			partEnd++
		}
		computePartitionColumnar(combined, winVec, i, partEnd, wc, inputIdx, orderIdxs)
		i = partEnd
	}
}

// computePartitionColumnar computes the window function for a single partition.
// Operates directly on column vectors rather than row maps.
func computePartitionColumnar(combined *batch.RecordBatch, winVec *batch.Vector, start, end int, wc WindowColumn, inputIdx int, orderIdxs []int) {
	n := end - start
	var inputVec *batch.Vector
	if inputIdx >= 0 {
		inputVec = combined.Columns[inputIdx]
	}

	switch wc.Func {
	case WinRowNumber:
		for i := 0; i < n; i++ {
			winVec.Int64Data[start+i] = int64(i + 1)
			winVec.Nulls.SetValid(start + i)
		}

	case WinRank:
		rank := int64(1)
		for i := 0; i < n; i++ {
			if i > 0 && !sameColumnar(combined, start+i-1, start+i, orderIdxs) {
				rank = int64(i + 1)
			}
			winVec.Int64Data[start+i] = rank
			winVec.Nulls.SetValid(start + i)
		}

	case WinDenseRank:
		rank := int64(1)
		for i := 0; i < n; i++ {
			if i > 0 && !sameColumnar(combined, start+i-1, start+i, orderIdxs) {
				rank++
			}
			winVec.Int64Data[start+i] = rank
			winVec.Nulls.SetValid(start + i)
		}

	case WinSum:
		if len(orderIdxs) > 0 {
			var sum float64
			for i := 0; i < n; i++ {
				sum += vecFloat64(inputVec, start+i)
				winVec.Float64Data[start+i] = sum
				winVec.Nulls.SetValid(start + i)
			}
		} else {
			var sum float64
			for i := 0; i < n; i++ {
				sum += vecFloat64(inputVec, start+i)
			}
			for i := 0; i < n; i++ {
				winVec.Float64Data[start+i] = sum
				winVec.Nulls.SetValid(start + i)
			}
		}

	case WinCount:
		if len(orderIdxs) > 0 {
			var count int64
			for i := 0; i < n; i++ {
				count++
				winVec.Int64Data[start+i] = count
				winVec.Nulls.SetValid(start + i)
			}
		} else {
			count := int64(n)
			for i := 0; i < n; i++ {
				winVec.Int64Data[start+i] = count
				winVec.Nulls.SetValid(start + i)
			}
		}

	case WinAvg:
		if len(orderIdxs) > 0 {
			var sum float64
			for i := 0; i < n; i++ {
				sum += vecFloat64(inputVec, start+i)
				winVec.Float64Data[start+i] = sum / float64(i+1)
				winVec.Nulls.SetValid(start + i)
			}
		} else {
			var sum float64
			for i := 0; i < n; i++ {
				sum += vecFloat64(inputVec, start+i)
			}
			avg := sum / float64(n)
			for i := 0; i < n; i++ {
				winVec.Float64Data[start+i] = avg
				winVec.Nulls.SetValid(start + i)
			}
		}

	case WinMin:
		if len(orderIdxs) > 0 {
			var minVal any
			for i := 0; i < n; i++ {
				v := inputVec.GetValue(start + i)
				if minVal == nil || (v != nil && compareAny(v, minVal) < 0) {
					minVal = v
				}
				winVec.SetValue(start+i, minVal)
			}
		} else {
			var minVal any
			for i := 0; i < n; i++ {
				v := inputVec.GetValue(start + i)
				if minVal == nil || (v != nil && compareAny(v, minVal) < 0) {
					minVal = v
				}
			}
			for i := 0; i < n; i++ {
				winVec.SetValue(start+i, minVal)
			}
		}

	case WinMax:
		if len(orderIdxs) > 0 {
			var maxVal any
			for i := 0; i < n; i++ {
				v := inputVec.GetValue(start + i)
				if maxVal == nil || (v != nil && compareAny(v, maxVal) > 0) {
					maxVal = v
				}
				winVec.SetValue(start+i, maxVal)
			}
		} else {
			var maxVal any
			for i := 0; i < n; i++ {
				v := inputVec.GetValue(start + i)
				if maxVal == nil || (v != nil && compareAny(v, maxVal) > 0) {
					maxVal = v
				}
			}
			for i := 0; i < n; i++ {
				winVec.SetValue(start+i, maxVal)
			}
		}

	case WinLag:
		offset := wc.LagLeadOffset
		if offset <= 0 {
			offset = 1
		}
		for i := 0; i < n; i++ {
			if i-offset >= 0 {
				winVec.SetValue(start+i, inputVec.GetValue(start+i-offset))
			} else if wc.LagLeadDefault != nil {
				winVec.SetValue(start+i, wc.LagLeadDefault)
			} else {
				winVec.Nulls.SetNull(start + i)
			}
		}

	case WinLead:
		offset := wc.LagLeadOffset
		if offset <= 0 {
			offset = 1
		}
		for i := 0; i < n; i++ {
			if i+offset < n {
				winVec.SetValue(start+i, inputVec.GetValue(start+i+offset))
			} else if wc.LagLeadDefault != nil {
				winVec.SetValue(start+i, wc.LagLeadDefault)
			} else {
				winVec.Nulls.SetNull(start + i)
			}
		}

	case WinFirstValue:
		if n > 0 {
			first := inputVec.GetValue(start)
			for i := 0; i < n; i++ {
				if first != nil {
					winVec.SetValue(start+i, first)
				} else {
					winVec.Nulls.SetNull(start + i)
				}
			}
		}

	case WinLastValue:
		if len(orderIdxs) > 0 {
			// With ORDER BY: running last value (current row's value)
			for i := 0; i < n; i++ {
				winVec.SetValue(start+i, inputVec.GetValue(start+i))
			}
		} else {
			if n > 0 {
				last := inputVec.GetValue(start + n - 1)
				for i := 0; i < n; i++ {
					if last != nil {
						winVec.SetValue(start+i, last)
					} else {
						winVec.Nulls.SetNull(start + i)
					}
				}
			}
		}

	case WinNtile:
		buckets := wc.NtileBuckets
		if buckets <= 0 {
			buckets = 1
		}
		base := n / buckets
		remainder := n % buckets
		bucket := int64(1)
		count := 0
		limit := base
		if remainder > 0 {
			limit++
		}
		for i := 0; i < n; i++ {
			winVec.Int64Data[start+i] = bucket
			winVec.Nulls.SetValid(start + i)
			count++
			if count >= limit && int(bucket) < buckets {
				bucket++
				count = 0
				if int(bucket) <= remainder {
					limit = base + 1
				} else {
					limit = base
				}
			}
		}

	case WinPercentRank:
		if n <= 1 {
			for i := 0; i < n; i++ {
				winVec.Float64Data[start+i] = 0
				winVec.Nulls.SetValid(start + i)
			}
		} else {
			rank := int64(1)
			for i := 0; i < n; i++ {
				if i > 0 && !sameColumnar(combined, start+i-1, start+i, orderIdxs) {
					rank = int64(i + 1)
				}
				winVec.Float64Data[start+i] = float64(rank-1) / float64(n-1)
				winVec.Nulls.SetValid(start + i)
			}
		}

	case WinCumeDist:
		for i := 0; i < n; {
			j := i + 1
			for j < n && sameColumnar(combined, start+i, start+j, orderIdxs) {
				j++
			}
			cd := float64(j) / float64(n)
			for k := i; k < j; k++ {
				winVec.Float64Data[start+k] = cd
				winVec.Nulls.SetValid(start + k)
			}
			i = j
		}

	case WinNthValue:
		nth := wc.NthValueN
		if nth <= 0 {
			nth = 1
		}
		if nth <= n {
			val := inputVec.GetValue(start + nth - 1)
			for i := 0; i < n; i++ {
				if val != nil {
					winVec.SetValue(start+i, val)
				} else {
					winVec.Nulls.SetNull(start + i)
				}
			}
		} else {
			for i := 0; i < n; i++ {
				winVec.Nulls.SetNull(start + i)
			}
		}
	}
}
