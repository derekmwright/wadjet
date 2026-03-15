package exec

import (
	"context"
	"sync"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec/kernel"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// AggFunc identifies an aggregate function.
type AggFunc int

const (
	AggSum AggFunc = iota
	AggCount
	AggMin
	AggMax
	AggAvg
)

// AggColumn defines an aggregation to perform.
type AggColumn struct {
	Func       AggFunc
	InputCol   string // input column name (empty for COUNT(*))
	OutputCol  string // output column name
	OutputType parquet.TypeID
}

// HashAggregate is a Sink that performs grouped aggregation with a hash map.
// Uses kernel-resolved typed updaters and cached column indices.
type HashAggregate struct {
	GroupByCols []string
	Aggs        []AggColumn

	mu            sync.Mutex
	groups        map[string]*groupState
	keys          [][]any
	groupColIdx   []int
	aggColIdx     []int
	groupColTypes []batch.TypeID
	aggUpdaters   []kernel.RowAggUpdater // resolved typed updaters
	resolved      bool
	keyBuf        []byte
}

type groupState struct {
	keyValues []any
	accs      []kernel.Accumulator
}

func NewHashAggregate(groupByCols []string, aggs []AggColumn) *HashAggregate {
	return &HashAggregate{
		GroupByCols: groupByCols,
		Aggs:        aggs,
	}
}

func (h *HashAggregate) Init(_ context.Context) error {
	h.groups = make(map[string]*groupState)
	h.keys = nil
	h.resolved = false
	h.keyBuf = make([]byte, 0, 128)
	return nil
}

func (h *HashAggregate) Consume(_ context.Context, b *batch.RecordBatch) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Resolve column indices and typed updaters once
	if !h.resolved {
		h.groupColIdx = make([]int, len(h.GroupByCols))
		h.groupColTypes = make([]batch.TypeID, len(h.GroupByCols))
		for i, col := range h.GroupByCols {
			idx := b.ColumnIndex(col)
			h.groupColIdx[i] = idx
			if idx >= 0 {
				h.groupColTypes[i] = b.Columns[idx].Type
			}
		}
		h.aggColIdx = make([]int, len(h.Aggs))
		h.aggUpdaters = make([]kernel.RowAggUpdater, len(h.Aggs))
		for i, agg := range h.Aggs {
			if agg.InputCol != "" {
				idx := b.ColumnIndex(agg.InputCol)
				h.aggColIdx[i] = idx
				if idx >= 0 {
					h.aggUpdaters[i] = resolveAggUpdater(agg.Func, b.Columns[idx].Type)
				}
			} else {
				h.aggColIdx[i] = -1
				if agg.Func == AggCount {
					h.aggUpdaters[i] = kernel.ResolveRowCount(true) // COUNT(*)
				}
			}
		}
		h.resolved = true
	}

	// Iterate rows
	if b.Sel != nil {
		for _, idx := range b.Sel {
			h.processRow(b, int(idx))
		}
	} else {
		for i := 0; i < b.Len; i++ {
			h.processRow(b, i)
		}
	}
	return nil
}

func (h *HashAggregate) processRow(b *batch.RecordBatch, row int) {
	// Serialize group key directly from typed columns
	h.keyBuf = h.keyBuf[:0]
	for i, idx := range h.groupColIdx {
		if i > 0 {
			h.keyBuf = append(h.keyBuf, 0)
		}
		if idx < 0 {
			h.keyBuf = append(h.keyBuf, "<null>"...)
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			h.keyBuf = append(h.keyBuf, "<null>"...)
			continue
		}
		h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[i])
	}
	key := string(h.keyBuf)

	gs, ok := h.groups[key]
	if !ok {
		keyVals := make([]any, len(h.GroupByCols))
		for i, idx := range h.groupColIdx {
			if idx >= 0 {
				keyVals[i] = b.Columns[idx].GetValue(row)
			}
		}
		gs = &groupState{
			keyValues: keyVals,
			accs:      make([]kernel.Accumulator, len(h.Aggs)),
		}
		h.groups[key] = gs
		h.keys = append(h.keys, keyVals)
	}

	// Update accumulators via pre-resolved typed kernels
	for i := range h.Aggs {
		updater := h.aggUpdaters[i]
		if updater == nil {
			continue
		}
		idx := h.aggColIdx[i]
		if idx >= 0 {
			updater(&gs.accs[i], b.Columns[idx], row)
		} else {
			// COUNT(*) — pass nil vec
			updater(&gs.accs[i], nil, row)
		}
	}
}

func (h *HashAggregate) Finalize(_ context.Context) error { return nil }

func (h *HashAggregate) Close() error { return nil }

// Next returns the aggregated results as batches.
func (h *HashAggregate) Next(_ context.Context) (*batch.RecordBatch, error) {
	if len(h.keys) == 0 {
		return nil, nil
	}

	schema := h.outputSchema()
	numRows := len(h.keys)
	out := batch.NewRecordBatch(schema, numRows)

	for i, keyVals := range h.keys {
		key := serializeKey(h.keyBuf, keyVals)
		gs := h.groups[key]

		// Set group-by columns
		for j, val := range gs.keyValues {
			out.Columns[j].SetValue(i, val)
		}

		// Set aggregate columns
		for j, agg := range h.Aggs {
			colIdx := len(h.GroupByCols) + j
			result := finalizeKernelAcc(&gs.accs[j], agg.Func)
			out.Columns[colIdx].SetValue(i, result)
		}
	}

	h.keys = nil
	return out, nil
}

func (h *HashAggregate) outputSchema() []parquet.Column {
	cols := make([]parquet.Column, 0, len(h.GroupByCols)+len(h.Aggs))
	for _, name := range h.GroupByCols {
		cols = append(cols, parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true})
	}
	for _, agg := range h.Aggs {
		cols = append(cols, parquet.Column{Name: agg.OutputCol, Type: agg.OutputType, Nullable: true})
	}
	return cols
}

// resolveAggUpdater maps an AggFunc + TypeID to a kernel RowAggUpdater.
func resolveAggUpdater(fn AggFunc, typ batch.TypeID) kernel.RowAggUpdater {
	switch fn {
	case AggSum, AggAvg:
		return kernel.ResolveRowSum(typ)
	case AggCount:
		return kernel.ResolveRowCount(false)
	case AggMin:
		return kernel.ResolveRowMin(typ)
	case AggMax:
		return kernel.ResolveRowMax(typ)
	default:
		return nil
	}
}

// finalizeKernelAcc converts a kernel.Accumulator to the final result value.
func finalizeKernelAcc(acc *kernel.Accumulator, fn AggFunc) any {
	switch fn {
	case AggCount:
		return acc.Count
	case AggSum:
		return acc.FinalSum()
	case AggAvg:
		return acc.FinalAvg()
	case AggMin:
		return acc.FinalMin()
	case AggMax:
		return acc.FinalMax()
	default:
		return nil
	}
}

// serializeKey serializes group key values using the reusable buffer.
func serializeKey(buf []byte, vals []any) string {
	buf = buf[:0]
	for i, v := range vals {
		if i > 0 {
			buf = append(buf, 0)
		}
		buf = appendKeyValue(buf, v)
	}
	return string(buf)
}

// appendColumnValue writes a typed column value to the buffer without boxing.
func appendColumnValue(buf []byte, v *batch.Vector, row int, typ batch.TypeID) []byte {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC:
		return appendInt64(buf, v.Int64Data[row])
	case batch.TypeInt32:
		return appendInt64(buf, int64(v.Int32Data[row]))
	case batch.TypeFloat64:
		return appendFloat64(buf, v.Float64Data[row])
	case batch.TypeFloat32:
		return appendFloat64(buf, float64(v.Float32Data[row]))
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR:
		return append(buf, v.BytesData.Value(row)...)
	case batch.TypeBool:
		if v.BoolData[row] {
			return append(buf, "true"...)
		}
		return append(buf, "false"...)
	default:
		return append(buf, '?')
	}
}

func batchIterator(b *batch.RecordBatch) []int {
	if b.Sel != nil {
		rows := make([]int, len(b.Sel))
		for i, idx := range b.Sel {
			rows[i] = int(idx)
		}
		return rows
	}
	rows := make([]int, b.Len)
	for i := range rows {
		rows[i] = i
	}
	return rows
}
