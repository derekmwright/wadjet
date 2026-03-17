package exec

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec/kernel"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// AggFunc identifies an aggregate function.
type AggFunc int

const (
	AggSum AggFunc = iota
	AggCount
	AggMin
	AggMax
	AggAvg
	AggCountDistinct
	AggStringAgg
	AggBoolAnd
	AggBoolOr
	AggStddev
	AggVariance
	AggStddevPop
	AggVarPop
	AggApproxDistinct
	AggCorr
	AggCovarSamp
	AggCovarPop
	AggPercentileCont
	AggPercentileDisc
	AggMode
	AggMinBy
	AggMaxBy
	AggMedian
)

// AggColumn defines an aggregation to perform.
type AggColumn struct {
	Func       AggFunc
	InputCol   string // input column name (empty for COUNT(*))
	OutputCol  string // output column name
	OutputType parquet.TypeID
	Separator  string  // separator for STRING_AGG (default ',')
	InputCol2  string  // second input column (corr, covar, min_by, max_by)
	Percentile float64 // percentile value for percentile_cont/percentile_disc
}

// HashAggregate is a Sink that performs grouped aggregation with a hash map.
// Uses kernel-resolved typed updaters and cached column indices.
// When a SpillManager is set, input batches are spilled to disk under memory
// pressure and re-processed during Finalize.
type HashAggregate struct {
	GroupByCols   []string
	Aggs          []AggColumn
	Spill         *memory.SpillManager // optional: enables spill-to-disk
	NullGroupCols []string             // GROUPING SETS: columns to output as NULL

	mu            sync.Mutex
	groups        map[string]*groupState
	keys          [][]any
	serializedKeys []string // pre-serialized keys matching h.keys order
	groupColIdx   []int
	aggColIdx     []int
	aggColIdx2    []int                  // second column indices for two-column aggregates
	groupColTypes  []batch.TypeID
	aggUpdaters    []kernel.RowAggUpdater  // resolved typed updaters
	batchAggKernels []kernel.BatchAggKernel // batch-level kernels (scalar aggregate fast path)
	scalarAccs     []kernel.Accumulator    // accumulators for scalar aggregate fast path
	isScalarAgg    bool                    // true when len(GroupByCols)==0 and all aggs are batch-able
	resolved       bool
	keyBuf         []byte
	inputSchema   []parquet.Column // schema from first input batch (for spill recovery)
	spillFiles    []string
	outputPos     int              // position in keys for batched Next() output
}

type groupState struct {
	keyValues    []any
	accs         []kernel.Accumulator
	distinctSets []map[string]struct{} // per-agg distinct value sets (nil if not COUNT(DISTINCT))
	extraState   []any                 // per-agg custom state (string_agg builder, variance state, etc.)
}

// stringAggState accumulates strings with a separator.
type stringAggState struct {
	parts []string
	sep   string
}

// varianceState tracks running variance using Welford's online algorithm.
type varianceState struct {
	count int64
	mean  float64
	m2    float64
}

func (v *varianceState) update(x float64) {
	v.count++
	delta := x - v.mean
	v.mean += delta / float64(v.count)
	delta2 := x - v.mean
	v.m2 += delta * delta2
}

func (v *varianceState) variancePop() float64 {
	if v.count == 0 {
		return 0
	}
	return v.m2 / float64(v.count)
}

func (v *varianceState) varianceSamp() float64 {
	if v.count < 2 {
		return 0
	}
	return v.m2 / float64(v.count-1)
}

// covarianceState tracks running covariance using an online algorithm.
type covarianceState struct {
	count int64
	meanX float64
	meanY float64
	c     float64 // co-moment: sum of (xi - meanX_old)(yi - meanY_new)
	m2x   float64 // sum of (xi - meanX)^2
	m2y   float64 // sum of (yi - meanY)^2
}

func (s *covarianceState) update(x, y float64) {
	s.count++
	n := float64(s.count)
	dx := x - s.meanX
	s.meanX += dx / n
	dy := y - s.meanY
	s.meanY += dy / n
	s.c += dx * (y - s.meanY)
	s.m2x += dx * (x - s.meanX)
	s.m2y += dy * (y - s.meanY)
}

func (s *covarianceState) covarPop() float64 {
	if s.count == 0 {
		return 0
	}
	return s.c / float64(s.count)
}

func (s *covarianceState) covarSamp() float64 {
	if s.count < 2 {
		return 0
	}
	return s.c / float64(s.count-1)
}

func (s *covarianceState) correlation() float64 {
	if s.count < 2 || s.m2x == 0 || s.m2y == 0 {
		return 0
	}
	return s.c / math.Sqrt(s.m2x*s.m2y)
}

// collectState accumulates raw float64 values for percentile/mode/median.
type collectState struct {
	values []float64
}

// minMaxByState tracks the row where a comparison column is min/max.
type minMaxByState struct {
	hasValue bool
	bestCmp  float64 // the comparison column value (min or max)
	bestVal  any     // the return column value at that row
	isMin    bool
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
	h.serializedKeys = nil
	h.resolved = false
	h.keyBuf = make([]byte, 0, 128)
	h.outputPos = 0
	return nil
}

func (h *HashAggregate) Consume(_ context.Context, b *batch.RecordBatch) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Save schema from first batch for spill recovery
	if h.inputSchema == nil {
		h.inputSchema = b.Schema
	}

	// Resolve column indices and typed updaters once
	if !h.resolved {
		h.resolveIndices(b)
	}

	// Spill input batch to disk if memory pressure is high.
	// The spilled rows are re-processed during Finalize.
	if h.Spill != nil && h.Spill.ShouldSpill() {
		rows := b.ToRows()
		path, err := h.Spill.SpillRows(rows)
		if err != nil {
			return err
		}
		h.spillFiles = append(h.spillFiles, path)
		return nil
	}

	// Iterate rows
	h.consumeBatch(b)
	return nil
}

// columnIndexFallback resolves a column name with fallback for table-qualified names.
// Tries the name as-is first, then strips a "table." prefix and retries.
func columnIndexFallback(b *batch.RecordBatch, name string) int {
	idx := b.ColumnIndex(name)
	if idx < 0 {
		if dotIdx := strings.Index(name, "."); dotIdx >= 0 {
			idx = b.ColumnIndex(name[dotIdx+1:])
		}
	}
	return idx
}

func (h *HashAggregate) resolveIndices(b *batch.RecordBatch) {
	h.groupColIdx = make([]int, len(h.GroupByCols))
	h.groupColTypes = make([]batch.TypeID, len(h.GroupByCols))
	for i, col := range h.GroupByCols {
		idx := columnIndexFallback(b, col)
		h.groupColIdx[i] = idx
		if idx >= 0 {
			h.groupColTypes[i] = b.Columns[idx].Type
		}
	}
	h.aggColIdx = make([]int, len(h.Aggs))
	h.aggColIdx2 = make([]int, len(h.Aggs))
	h.aggUpdaters = make([]kernel.RowAggUpdater, len(h.Aggs))
	for i, agg := range h.Aggs {
		h.aggColIdx2[i] = -1 // default: no second column
		if agg.Func == AggCountDistinct || agg.Func == AggApproxDistinct {
			if agg.InputCol != "" {
				h.aggColIdx[i] = b.ColumnIndex(agg.InputCol)
			} else {
				h.aggColIdx[i] = -1
			}
			continue
		}
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
		// Resolve second column index for two-column aggregates
		if agg.InputCol2 != "" {
			h.aggColIdx2[i] = b.ColumnIndex(agg.InputCol2)
		}
	}
	h.resolved = true

	// Resolve batch-level kernels for scalar aggregate fast path
	if len(h.GroupByCols) == 0 {
		h.batchAggKernels = make([]kernel.BatchAggKernel, len(h.Aggs))
		allBatchable := true
		for i, agg := range h.Aggs {
			h.batchAggKernels[i] = resolveBatchAggKernel(agg.Func, h.aggColIdx[i], b)
			if h.batchAggKernels[i] == nil {
				allBatchable = false
			}
		}
		if allBatchable {
			h.isScalarAgg = true
			h.scalarAccs = make([]kernel.Accumulator, len(h.Aggs))
		}
	}
}

func (h *HashAggregate) consumeBatch(b *batch.RecordBatch) {
	// Scalar aggregate fast path: use batch-level kernels (no per-row dispatch)
	if h.isScalarAgg {
		for i := range h.Aggs {
			idx := h.aggColIdx[i]
			var vec *batch.Vector
			if idx >= 0 {
				vec = b.Columns[idx]
			}
			h.batchAggKernels[i](&h.scalarAccs[i], vec, b.Sel, b.Len)
		}
		return
	}

	if b.Sel != nil {
		for _, idx := range b.Sel {
			h.processRow(b, int(idx))
		}
	} else {
		for i := 0; i < b.Len; i++ {
			h.processRow(b, i)
		}
	}
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

	// Fast path: Go avoids allocating a string for map lookups of string([]byte).
	// Only allocate the key string when creating a new group.
	if gs, ok := h.groups[string(h.keyBuf)]; ok {
		h.updateGroup(gs, b, row)
		return
	}

	// Slow path: new group — allocate key string only here
	key := string(h.keyBuf)
	keyVals := make([]any, len(h.GroupByCols))
	for i, idx := range h.groupColIdx {
		if idx >= 0 {
			keyVals[i] = b.Columns[idx].GetValue(row)
		}
	}
	gs := &groupState{
		keyValues:    keyVals,
		accs:         make([]kernel.Accumulator, len(h.Aggs)),
		distinctSets: make([]map[string]struct{}, len(h.Aggs)),
		extraState:   make([]any, len(h.Aggs)),
	}
	for i, agg := range h.Aggs {
		switch agg.Func {
		case AggCountDistinct:
			gs.distinctSets[i] = make(map[string]struct{})
		case AggStringAgg:
			sep := agg.Separator
			if sep == "" {
				sep = ","
			}
			gs.extraState[i] = &stringAggState{sep: sep}
		case AggStddev, AggVariance, AggStddevPop, AggVarPop:
			gs.extraState[i] = &varianceState{}
		case AggBoolAnd:
			gs.extraState[i] = true
		case AggBoolOr:
			gs.extraState[i] = false
		case AggApproxDistinct:
			gs.distinctSets[i] = make(map[string]struct{})
		case AggCorr, AggCovarSamp, AggCovarPop:
			gs.extraState[i] = &covarianceState{}
		case AggPercentileCont, AggPercentileDisc, AggMode, AggMedian:
			gs.extraState[i] = &collectState{}
		case AggMinBy:
			gs.extraState[i] = &minMaxByState{isMin: true}
		case AggMaxBy:
			gs.extraState[i] = &minMaxByState{isMin: false}
		}
	}
	h.groups[key] = gs
	h.keys = append(h.keys, keyVals)
	h.serializedKeys = append(h.serializedKeys, key)

	h.updateGroup(gs, b, row)
}

// updateGroup updates a group's accumulators with values from a single row.
func (h *HashAggregate) updateGroup(gs *groupState, b *batch.RecordBatch, row int) {
	for i, agg := range h.Aggs {
		switch agg.Func {
		case AggCountDistinct:
			// COUNT(DISTINCT): hash the value, add to set
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			h.keyBuf = h.keyBuf[:0]
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, v.Type)
			valKey := string(h.keyBuf)
			gs.distinctSets[i][valKey] = struct{}{}

		case AggStringAgg:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			state := gs.extraState[i].(*stringAggState)
			state.parts = append(state.parts, fmt.Sprint(v.GetValue(row)))

		case AggBoolAnd:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			val := v.GetValue(row)
			boolVal := false
			switch tv := val.(type) {
			case bool:
				boolVal = tv
			case int64:
				boolVal = tv != 0
			case float64:
				boolVal = tv != 0
			}
			current := gs.extraState[i].(bool)
			gs.extraState[i] = current && boolVal

		case AggBoolOr:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			val := v.GetValue(row)
			boolVal := false
			switch tv := val.(type) {
			case bool:
				boolVal = tv
			case int64:
				boolVal = tv != 0
			case float64:
				boolVal = tv != 0
			}
			current := gs.extraState[i].(bool)
			gs.extraState[i] = current || boolVal

		case AggStddev, AggVariance, AggStddevPop, AggVarPop:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			state := gs.extraState[i].(*varianceState)
			var fval float64
			switch v.Type {
			case batch.TypeInt64, batch.TypeTimestamp:
				fval = float64(v.Int64Data[row])
			case batch.TypeInt32:
				fval = float64(v.Int32Data[row])
			case batch.TypeFloat64:
				fval = v.Float64Data[row]
			case batch.TypeFloat32:
				fval = float64(v.Float32Data[row])
			default:
				continue
			}
			state.update(fval)

		case AggApproxDistinct:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			h.keyBuf = h.keyBuf[:0]
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, v.Type)
			valKey := string(h.keyBuf)
			gs.distinctSets[i][valKey] = struct{}{}

		case AggCorr, AggCovarSamp, AggCovarPop:
			idx1 := h.aggColIdx[i]
			idx2 := h.aggColIdx2[i]
			if idx1 < 0 || idx2 < 0 {
				continue
			}
			v1 := b.Columns[idx1]
			v2 := b.Columns[idx2]
			if v1.Nulls.IsNullFast(row) || v2.Nulls.IsNullFast(row) {
				continue
			}
			state := gs.extraState[i].(*covarianceState)
			state.update(vecToFloat64(v1, row), vecToFloat64(v2, row))

		case AggPercentileCont, AggPercentileDisc, AggMedian:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			state := gs.extraState[i].(*collectState)
			state.values = append(state.values, vecToFloat64(v, row))

		case AggMode:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			state := gs.extraState[i].(*collectState)
			state.values = append(state.values, vecToFloat64(v, row))

		case AggMinBy, AggMaxBy:
			idx1 := h.aggColIdx[i]
			idx2 := h.aggColIdx2[i]
			if idx1 < 0 || idx2 < 0 {
				continue
			}
			v1 := b.Columns[idx1] // return column
			v2 := b.Columns[idx2] // comparison column
			if v1.Nulls.IsNullFast(row) || v2.Nulls.IsNullFast(row) {
				continue
			}
			state := gs.extraState[i].(*minMaxByState)
			cmpVal := vecToFloat64(v2, row)
			if !state.hasValue ||
				(state.isMin && cmpVal < state.bestCmp) ||
				(!state.isMin && cmpVal > state.bestCmp) {
				state.hasValue = true
				state.bestCmp = cmpVal
				state.bestVal = v1.GetValue(row)
			}

		default:
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
}

func (h *HashAggregate) Finalize(_ context.Context) error {
	if len(h.spillFiles) == 0 {
		return nil
	}

	// Re-process spilled input rows through the same aggregate logic.
	// This is correct for all aggregate functions because we're processing
	// raw input, not merging partial results.
	for _, f := range h.spillFiles {
		rows, err := memory.ReadSpilledRows(f)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			continue
		}
		b := batch.FromRows(h.inputSchema, rows)
		// Re-resolve indices for the reconstructed batch
		// (column order may differ from original)
		h.resolved = false
		h.resolveIndices(b)
		h.consumeBatch(b)
	}
	h.spillFiles = nil
	return nil
}

func (h *HashAggregate) Close() error { return nil }

// Next returns the aggregated results in batches of DefaultBatchSize rows.
func (h *HashAggregate) Next(_ context.Context) (*batch.RecordBatch, error) {
	// Scalar aggregate fast path: single row output from batch accumulators
	if h.isScalarAgg {
		if h.outputPos > 0 {
			return nil, nil
		}
		h.outputPos = 1
		schema := h.outputSchema()
		out := batch.NewRecordBatch(schema, 1)
		for j, agg := range h.Aggs {
			result := finalizeKernelAcc(&h.scalarAccs[j], agg.Func)
			out.Columns[j].SetValue(0, result)
		}
		return out, nil
	}

	if h.outputPos >= len(h.keys) {
		return nil, nil
	}

	schema := h.outputSchema()
	start := h.outputPos
	end := start + batch.DefaultBatchSize
	if end > len(h.keys) {
		end = len(h.keys)
	}
	numRows := end - start
	h.outputPos = end

	out := batch.NewRecordBatch(schema, numRows)

	// Build a set of null group columns for fast lookup
	nullSet := make(map[string]bool, len(h.NullGroupCols))
	for _, c := range h.NullGroupCols {
		nullSet[c] = true
	}

	for i := 0; i < numRows; i++ {
		gs := h.groups[h.serializedKeys[start+i]]

		// Set group-by columns
		for j, val := range gs.keyValues {
			out.Columns[j].SetValue(i, val)
		}

		// Set aggregate columns
		for j, agg := range h.Aggs {
			colIdx := len(h.GroupByCols) + j
			switch agg.Func {
			case AggCountDistinct:
				out.Columns[colIdx].SetValue(i, int64(len(gs.distinctSets[j])))
			case AggStringAgg:
				state := gs.extraState[j].(*stringAggState)
				if len(state.parts) == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, strings.Join(state.parts, state.sep))
				}
			case AggBoolAnd, AggBoolOr:
				out.Columns[colIdx].SetValue(i, gs.extraState[j].(bool))
			case AggStddev:
				state := gs.extraState[j].(*varianceState)
				if state.count < 2 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, math.Sqrt(state.varianceSamp()))
				}
			case AggVariance:
				state := gs.extraState[j].(*varianceState)
				if state.count < 2 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.varianceSamp())
				}
			case AggStddevPop:
				state := gs.extraState[j].(*varianceState)
				if state.count == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, math.Sqrt(state.variancePop()))
				}
			case AggVarPop:
				state := gs.extraState[j].(*varianceState)
				if state.count == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.variancePop())
				}
			case AggApproxDistinct:
				out.Columns[colIdx].SetValue(i, int64(len(gs.distinctSets[j])))
			case AggCorr:
				state := gs.extraState[j].(*covarianceState)
				if state.count < 2 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.correlation())
				}
			case AggCovarSamp:
				state := gs.extraState[j].(*covarianceState)
				if state.count < 2 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.covarSamp())
				}
			case AggCovarPop:
				state := gs.extraState[j].(*covarianceState)
				if state.count == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.covarPop())
				}
			case AggPercentileCont:
				state := gs.extraState[j].(*collectState)
				out.Columns[colIdx].SetValue(i, computePercentileCont(state.values, agg.Percentile))
			case AggPercentileDisc:
				state := gs.extraState[j].(*collectState)
				out.Columns[colIdx].SetValue(i, computePercentileDisc(state.values, agg.Percentile))
			case AggMedian:
				state := gs.extraState[j].(*collectState)
				out.Columns[colIdx].SetValue(i, computePercentileCont(state.values, 0.5))
			case AggMode:
				state := gs.extraState[j].(*collectState)
				out.Columns[colIdx].SetValue(i, computeMode(state.values))
			case AggMinBy, AggMaxBy:
				state := gs.extraState[j].(*minMaxByState)
				if !state.hasValue {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.bestVal)
				}
			default:
				result := finalizeKernelAcc(&gs.accs[j], agg.Func)
				out.Columns[colIdx].SetValue(i, result)
			}
		}

		// NULL out columns that are part of GROUPING SETS exclusion
		nullColIdx := len(h.GroupByCols) + len(h.Aggs)
		for k := 0; k < len(h.NullGroupCols); k++ {
			if nullColIdx+k < len(out.Columns) {
				out.Columns[nullColIdx+k].SetValue(i, nil)
			}
		}
	}

	// Release memory when all groups have been emitted
	if h.outputPos >= len(h.keys) {
		h.keys = nil
		h.serializedKeys = nil
		h.groups = nil
	}
	return out, nil
}

func (h *HashAggregate) outputSchema() []parquet.Column {
	cols := make([]parquet.Column, 0, len(h.GroupByCols)+len(h.Aggs)+len(h.NullGroupCols))
	for i, name := range h.GroupByCols {
		typ := parquet.TypeString // default fallback
		if i < len(h.groupColTypes) && h.groupColTypes[i] != 0 {
			typ = parquet.TypeID(h.groupColTypes[i])
		}
		cols = append(cols, parquet.Column{Name: name, Type: typ, Nullable: true})
	}
	for _, agg := range h.Aggs {
		cols = append(cols, parquet.Column{Name: agg.OutputCol, Type: agg.OutputType, Nullable: true})
	}
	// GROUPING SETS null columns (appear in other sets but not this one)
	for _, name := range h.NullGroupCols {
		cols = append(cols, parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true})
	}
	return cols
}

// resolveBatchAggKernel returns a batch-level aggregate kernel for scalar aggregates.
// Returns nil if the aggregate function is not batch-able (e.g., COUNT(DISTINCT), STRING_AGG).
func resolveBatchAggKernel(fn AggFunc, colIdx int, b *batch.RecordBatch) kernel.BatchAggKernel {
	switch fn {
	case AggSum, AggAvg:
		if colIdx < 0 {
			return nil
		}
		return kernel.ResolveBatchSum(b.Columns[colIdx].Type)
	case AggCount:
		if colIdx < 0 {
			// COUNT(*) — counts all rows
			return func(acc *kernel.Accumulator, _ *batch.Vector, sel []uint16, vecLen int) {
				if sel != nil {
					acc.Count += int64(len(sel))
				} else {
					acc.Count += int64(vecLen)
				}
			}
		}
		return kernel.ResolveBatchCount()
	case AggMin:
		if colIdx < 0 {
			return nil
		}
		return kernel.ResolveBatchMin(b.Columns[colIdx].Type)
	case AggMax:
		if colIdx < 0 {
			return nil
		}
		return kernel.ResolveBatchMax(b.Columns[colIdx].Type)
	default:
		return nil
	}
}

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
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return appendInt64(buf, v.Int64Data[row])
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return appendInt64(buf, int64(v.Int32Data[row]))
	case batch.TypeFloat64:
		return appendFloat64(buf, v.Float64Data[row])
	case batch.TypeFloat32:
		return appendFloat64(buf, float64(v.Float32Data[row]))
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		return append(buf, v.BytesData.Value(row)...)
	case batch.TypeBool:
		if v.BoolData[row] {
			return append(buf, "true"...)
		}
		return append(buf, "false"...)
	case batch.TypeDecimal:
		return appendFloat64(buf, v.DecimalData.Data[row].ToFloat64(v.DecimalData.Scale))
	default:
		return append(buf, '?')
	}
}

// computePercentileCont returns the interpolated percentile value (continuous).
func computePercentileCont(values []float64, p float64) any {
	if len(values) == 0 {
		return nil
	}
	sort.Float64s(values)
	n := float64(len(values))
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}
	idx := p * (n - 1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return values[lo]
	}
	frac := idx - float64(lo)
	return values[lo]*(1-frac) + values[hi]*frac
}

// computePercentileDisc returns the discrete percentile value (nearest rank).
func computePercentileDisc(values []float64, p float64) any {
	if len(values) == 0 {
		return nil
	}
	sort.Float64s(values)
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

// computeMode returns the most frequent value. Ties broken by smallest value.
func computeMode(values []float64) any {
	if len(values) == 0 {
		return nil
	}
	counts := make(map[float64]int)
	for _, v := range values {
		counts[v]++
	}
	var bestVal float64
	bestCount := 0
	for v, c := range counts {
		if c > bestCount || (c == bestCount && v < bestVal) {
			bestVal = v
			bestCount = c
		}
	}
	return bestVal
}

// vecToFloat64 extracts a float64 value from a vector at the given row.
func vecToFloat64(v *batch.Vector, row int) float64 {
	switch v.Type {
	case batch.TypeInt64, batch.TypeTimestamp:
		return float64(v.Int64Data[row])
	case batch.TypeInt32:
		return float64(v.Int32Data[row])
	case batch.TypeFloat64:
		return v.Float64Data[row]
	case batch.TypeFloat32:
		return float64(v.Float32Data[row])
	case batch.TypeDecimal:
		return v.DecimalData.Data[row].ToFloat64(v.DecimalData.Scale)
	default:
		return 0
	}
}

