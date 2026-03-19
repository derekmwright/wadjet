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

	// Single-column integer GROUP BY fast path: uses intHashTable
	// instead of serializing keys to strings and using map[string].
	useIntGroupKey bool
	intGroupIndex  *intHashTable
	intGroupStates []*groupState
	intGroupKeyCol int // column index for the integer group-by key

	// Multi-column compact GROUP BY fast path: binary-encoded key packed into int64.
	// Uses intHashTable for lookup. Falls back to generic path if key exceeds 8 bytes.
	useCompactGroupKey bool
	compactKeys        []string // serialized binary keys for fallback migration

	// String hash table for generic GROUP BY: open-addressing with arena-stored keys.
	// Replaces map[string]*groupState to eliminate GC scanning overhead.
	strGroupIndex  *strHashTable
	strGroupStates []*groupState

	resolved       bool
	needsDistinct  bool // true if any agg uses distinctSets
	needsExtra     bool // true if any agg uses extraState
	keyBuf         []byte
	inputSchema   []parquet.Column // schema from first input batch (for spill recovery)
	spillFiles    []string
	outputPos     int              // position in keys for batched Next() output
}

type groupState struct {
	keyValues    []any
	intKey       int64 // single int64 key for int-keyed groups (avoids []any boxing)
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
	h.strGroupIndex = newStrHashTable(4096)
	h.strGroupStates = nil
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

	// Check if all aggregates use simple kernel updaters (no COUNT(DISTINCT),
	// STRING_AGG, etc. which need the generic processRow path).
	allSimpleAggs := true
	for i, agg := range h.Aggs {
		switch agg.Func {
		case AggCountDistinct, AggApproxDistinct:
			allSimpleAggs = false
			h.needsDistinct = true
		case AggStringAgg, AggStddev, AggVariance, AggStddevPop, AggVarPop,
			AggBoolAnd, AggBoolOr, AggCorr, AggCovarSamp, AggCovarPop,
			AggPercentileCont, AggPercentileDisc, AggMedian,
			AggMinBy, AggMaxBy:
			allSimpleAggs = false
			h.needsExtra = true
		case AggMode:
			allSimpleAggs = false
			h.needsExtra = true
		default:
			if h.aggUpdaters[i] == nil && agg.InputCol != "" {
				allSimpleAggs = false
			}
		}
	}

	// Single-column integer GROUP BY fast path:
	// Use intHashTable when grouping by one integer-typed column.
	if len(h.GroupByCols) == 1 && h.groupColIdx[0] >= 0 {
		typ := h.groupColTypes[0]
		isIntType := typ == batch.TypeInt64 || typ == batch.TypeTimestamp ||
			typ == batch.TypeIPv4 || typ == batch.TypeMAC || typ == batch.TypeDuration ||
			typ == batch.TypeInt32 || typ == batch.TypePort || typ == batch.TypeProtocol || typ == batch.TypeDate
		if isIntType && allSimpleAggs {
			h.useIntGroupKey = true
			h.intGroupIndex = newIntHashTable(4096)
			h.intGroupKeyCol = h.groupColIdx[0]
		}
	}

	// Multi-column compact GROUP BY fast path:
	// When the binary-encoded GROUP BY key fits in 8 bytes, pack it into int64
	// and use intHashTable instead of map[string]. Avoids string hashing and
	// Go map overhead. Falls back to generic path if a key exceeds 8 bytes.
	if !h.useIntGroupKey && !h.isScalarAgg && len(h.GroupByCols) >= 2 && allSimpleAggs {
		estimatedWidth := 0
		canCompact := true
		for _, typ := range h.groupColTypes {
			estimatedWidth++ // null flag byte
			switch typ {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate, batch.TypeFloat32:
				estimatedWidth += 4
			case batch.TypeBool:
				estimatedWidth += 1
			case batch.TypeString, batch.TypeBytes:
				estimatedWidth += 3 // 2-byte length prefix + 1 byte min data
			default:
				canCompact = false
			}
		}
		if canCompact && estimatedWidth <= 8 {
			h.useCompactGroupKey = true
			h.intGroupIndex = newIntHashTable(4096)
		}
	}

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

	// Single-column integer GROUP BY fast path
	if h.useIntGroupKey {
		h.consumeBatchIntGroup(b)
		return
	}

	// Multi-column compact GROUP BY fast path
	if h.useCompactGroupKey {
		h.consumeBatchCompactGroup(b)
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

// consumeBatchIntGroup is the fast path for single-column integer GROUP BY.
// Uses intHashTable for group lookup — no key serialization, no string allocation.
// The loop body is fully inlined (no closure) to avoid per-call heap allocation.
func (h *HashAggregate) consumeBatchIntGroup(b *batch.RecordBatch) {
	gkVec := b.Columns[h.intGroupKeyCol]
	isInt32 := h.groupColTypes[0] == batch.TypeInt32 ||
		h.groupColTypes[0] == batch.TypePort ||
		h.groupColTypes[0] == batch.TypeProtocol ||
		h.groupColTypes[0] == batch.TypeDate
	hasNulls := gkVec.Nulls.HasNulls()

	// Local copies to avoid repeated field loads in the inner loop
	intIdx := h.intGroupIndex
	updaters := h.aggUpdaters
	colIdx := h.aggColIdx
	nAggs := len(h.Aggs)

	if b.Sel != nil {
		for _, si := range b.Sel {
			row := int(si)
			if hasNulls && gkVec.Nulls.IsNullFast(row) {
				h.processRow(b, row)
				continue
			}
			var key int64
			if isInt32 {
				key = int64(gkVec.Int32Data[row])
			} else {
				key = gkVec.Int64Data[row]
			}
			gsIdx, ok := intIdx.Get(key)
			if ok {
				gs := h.intGroupStates[gsIdx]
				for i := 0; i < nAggs; i++ {
					u := updaters[i]
					if u == nil {
						continue
					}
					ci := colIdx[i]
					if ci >= 0 {
						u(&gs.accs[i], b.Columns[ci], row)
					} else {
						u(&gs.accs[i], nil, row)
					}
				}
				continue
			}
			// New group — store intKey directly, defer []any boxing to output.
			gs := &groupState{
				intKey: key,
				accs:   make([]kernel.Accumulator, nAggs),
			}
			newIdx := int32(len(h.intGroupStates))
			intIdx.Put(key, newIdx)
			h.intGroupStates = append(h.intGroupStates, gs)
			for i := 0; i < nAggs; i++ {
				u := updaters[i]
				if u == nil {
					continue
				}
				ci := colIdx[i]
				if ci >= 0 {
					u(&gs.accs[i], b.Columns[ci], row)
				} else {
					u(&gs.accs[i], nil, row)
				}
			}
		}
	} else {
		for row := 0; row < b.Len; row++ {
			if hasNulls && gkVec.Nulls.IsNullFast(row) {
				h.processRow(b, row)
				continue
			}
			var key int64
			if isInt32 {
				key = int64(gkVec.Int32Data[row])
			} else {
				key = gkVec.Int64Data[row]
			}
			gsIdx, ok := intIdx.Get(key)
			if ok {
				gs := h.intGroupStates[gsIdx]
				for i := 0; i < nAggs; i++ {
					u := updaters[i]
					if u == nil {
						continue
					}
					ci := colIdx[i]
					if ci >= 0 {
						u(&gs.accs[i], b.Columns[ci], row)
					} else {
						u(&gs.accs[i], nil, row)
					}
				}
				continue
			}
			// New group — store intKey directly, defer []any boxing to output.
			gs := &groupState{
				intKey: key,
				accs:   make([]kernel.Accumulator, nAggs),
			}
			newIdx := int32(len(h.intGroupStates))
			intIdx.Put(key, newIdx)
			h.intGroupStates = append(h.intGroupStates, gs)
			for i := 0; i < nAggs; i++ {
				u := updaters[i]
				if u == nil {
					continue
				}
				ci := colIdx[i]
				if ci >= 0 {
					u(&gs.accs[i], b.Columns[ci], row)
				} else {
					u(&gs.accs[i], nil, row)
				}
			}
		}
	}
}

// consumeBatchCompactGroup is the fast path for multi-column GROUP BY where
// the binary-encoded key fits in int64. Uses intHashTable for group lookup.
// Falls back to generic path if any key exceeds 8 bytes.
func (h *HashAggregate) consumeBatchCompactGroup(b *batch.RecordBatch) {
	processRow := func(row int) bool {
		h.keyBuf = h.keyBuf[:0]
		for i, idx := range h.groupColIdx {
			if idx < 0 {
				h.keyBuf = append(h.keyBuf, 1) // null flag
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				h.keyBuf = append(h.keyBuf, 1) // null flag
				continue
			}
			h.keyBuf = append(h.keyBuf, 0) // not-null flag
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[i])
		}

		if len(h.keyBuf) > 8 {
			return false // key too long, need fallback
		}

		key := packKeyInt64(h.keyBuf)
		if gsIdx, ok := h.intGroupIndex.Get(key); ok {
			gs := h.intGroupStates[gsIdx]
			for i := range h.Aggs {
				updater := h.aggUpdaters[i]
				if updater == nil {
					continue
				}
				idx := h.aggColIdx[i]
				if idx >= 0 {
					updater(&gs.accs[i], b.Columns[idx], row)
				} else {
					updater(&gs.accs[i], nil, row) // COUNT(*)
				}
			}
			return true
		}

		// New group
		keyVals := make([]any, len(h.GroupByCols))
		for i, idx := range h.groupColIdx {
			if idx >= 0 {
				keyVals[i] = b.Columns[idx].GetValue(row)
			}
		}
		// useCompactGroupKey requires allSimpleAggs — skip distinctSets/extraState.
		gs := &groupState{
			keyValues: keyVals,
			accs:      make([]kernel.Accumulator, len(h.Aggs)),
		}

		newIdx := int32(len(h.intGroupStates))
		h.intGroupIndex.Put(key, newIdx)
		h.intGroupStates = append(h.intGroupStates, gs)
		h.compactKeys = append(h.compactKeys, string(h.keyBuf))
		h.keys = append(h.keys, keyVals)

		for i := range h.Aggs {
			updater := h.aggUpdaters[i]
			if updater == nil {
				continue
			}
			idx := h.aggColIdx[i]
			if idx >= 0 {
				updater(&gs.accs[i], b.Columns[idx], row)
			} else {
				updater(&gs.accs[i], nil, row) // COUNT(*)
			}
		}
		return true
	}

	if b.Sel != nil {
		for i, idx := range b.Sel {
			if !processRow(int(idx)) {
				// Key exceeded 8 bytes — migrate to generic path
				h.migrateCompactToGeneric()
				for j := i; j < len(b.Sel); j++ {
					h.processRow(b, int(b.Sel[j]))
				}
				return
			}
		}
	} else {
		for i := 0; i < b.Len; i++ {
			if !processRow(i) {
				h.migrateCompactToGeneric()
				for j := i; j < b.Len; j++ {
					h.processRow(b, j)
				}
				return
			}
		}
	}
}

// migrateCompactToGeneric moves all groups from intHashTable to the string map
// when compact mode cannot handle a key that exceeds 8 bytes.
func (h *HashAggregate) migrateCompactToGeneric() {
	h.useCompactGroupKey = false
	h.strGroupIndex = newStrHashTable(len(h.intGroupStates))
	h.strGroupStates = make([]*groupState, 0, len(h.intGroupStates))
	for i, gs := range h.intGroupStates {
		key := h.compactKeys[i]
		h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, key)
	}
	h.intGroupStates = nil
	h.intGroupIndex = nil
	h.compactKeys = nil
}

// packKeyInt64 interprets up to 8 bytes as a little-endian int64.
func packKeyInt64(b []byte) int64 {
	var v int64
	for i := 0; i < len(b); i++ {
		v |= int64(b[i]) << uint(i*8)
	}
	return v
}

func (h *HashAggregate) processRow(b *batch.RecordBatch, row int) {
	// Serialize group key using binary encoding (fixed-width for numeric types).
	// Each column is prefixed by a 1-byte null flag (0=value, 1=null).
	h.keyBuf = h.keyBuf[:0]
	for i, idx := range h.groupColIdx {
		if idx < 0 {
			h.keyBuf = append(h.keyBuf, 1) // null flag
			continue
		}
		v := b.Columns[idx]
		if v.Nulls.IsNullFast(row) {
			h.keyBuf = append(h.keyBuf, 1) // null flag
			continue
		}
		h.keyBuf = append(h.keyBuf, 0) // not-null flag
		h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[i])
	}

	// Use open-addressing string hash table to avoid GC overhead of map[string].
	groupIdx, found := h.strGroupIndex.GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
	if found {
		h.updateGroup(h.strGroupStates[groupIdx], b, row)
		return
	}

	// New group
	keyVals := make([]any, len(h.GroupByCols))
	for i, idx := range h.groupColIdx {
		if idx >= 0 {
			keyVals[i] = b.Columns[idx].GetValue(row)
		}
	}
	gs := &groupState{
		keyValues: keyVals,
		accs:      make([]kernel.Accumulator, len(h.Aggs)),
	}
	if h.needsDistinct {
		gs.distinctSets = make([]map[string]struct{}, len(h.Aggs))
	}
	if h.needsExtra {
		gs.extraState = make([]any, len(h.Aggs))
	}
	for i, agg := range h.Aggs {
		switch agg.Func {
		case AggCountDistinct:
			if gs.distinctSets != nil {
				gs.distinctSets[i] = make(map[string]struct{})
			}
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
			if gs.distinctSets != nil {
				gs.distinctSets[i] = make(map[string]struct{})
			}
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
	h.strGroupStates = append(h.strGroupStates, gs)
	h.keys = append(h.keys, keyVals)
	h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))

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

	totalGroups := len(h.keys)
	if h.useIntGroupKey {
		totalGroups = len(h.intGroupStates)
	}
	if h.outputPos >= totalGroups {
		return nil, nil
	}

	schema := h.outputSchema()
	start := h.outputPos
	end := start + batch.DefaultBatchSize
	if end > totalGroups {
		end = totalGroups
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
		var gs *groupState
		if h.useIntGroupKey || h.useCompactGroupKey {
			gs = h.intGroupStates[start+i]
		} else {
			gs = h.strGroupStates[start+i]
		}

		// Set group-by columns: use intKey directly for int-keyed groups
		// to avoid deferred []any boxing. For other paths, use keyValues.
		if h.useIntGroupKey && gs.keyValues == nil {
			out.Columns[0].SetValue(i, gs.intKey)
		} else {
			for j, val := range gs.keyValues {
				out.Columns[j].SetValue(i, val)
			}
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
	if h.outputPos >= totalGroups {
		h.keys = nil
		h.serializedKeys = nil
		h.strGroupStates = nil
		h.strGroupIndex = nil
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

// CloneSink returns a new HashAggregate with the same configuration but fresh state.
// Used by parallel pipeline execution: each worker gets its own cloned sink.
func (h *HashAggregate) CloneSink() SinkSource {
	clone := &HashAggregate{
		GroupByCols:   h.GroupByCols,
		Aggs:          h.Aggs,
		NullGroupCols: h.NullGroupCols,
		// No spill manager — partial aggregates are small enough
	}
	return clone
}

// MergeSink merges another HashAggregate's partial state into this one.
// Called after all parallel workers finish to combine partial aggregates.
func (h *HashAggregate) MergeSink(other SinkSource) {
	o := other.(*HashAggregate)

	// Scalar aggregate fast path: merge batch accumulators directly
	if h.isScalarAgg && o.isScalarAgg {
		for i := range h.scalarAccs {
			h.scalarAccs[i].Merge(&o.scalarAccs[i])
		}
		return
	}

	// Normalize both sides to the generic map path so merge is uniform.
	// This runs once at the end, so O(N_groups) overhead is negligible.
	h.migrateToGenericMap()
	o.migrateToGenericMap()

	for i := range o.keys {
		key := o.serializedKeys[i]
		oGS := o.strGroupStates[i]

		gsIdx, found := h.strGroupIndex.Get([]byte(key))
		if found {
			gs := h.strGroupStates[gsIdx]
			for j := range gs.accs {
				gs.accs[j].Merge(&oGS.accs[j])
			}
		} else {
			newIdx := int32(len(h.strGroupStates))
			h.strGroupIndex.Put([]byte(key), newIdx)
			h.strGroupStates = append(h.strGroupStates, oGS)
			h.keys = append(h.keys, oGS.keyValues)
			h.serializedKeys = append(h.serializedKeys, key)
		}
	}
}

// migrateToGenericMap converts int/compact group key state to the generic
// map[string]*groupState path. No-op if already using the generic path.
func (h *HashAggregate) migrateToGenericMap() {
	if h.useCompactGroupKey {
		h.migrateCompactToGeneric()
		return
	}
	if !h.useIntGroupKey {
		return
	}
	// Migrate int group key → generic path
	h.strGroupIndex = newStrHashTable(len(h.intGroupStates))
	h.strGroupStates = make([]*groupState, 0, len(h.intGroupStates))
	h.serializedKeys = make([]string, 0, len(h.intGroupStates))
	h.keys = make([][]any, 0, len(h.intGroupStates))
	for _, gs := range h.intGroupStates {
		// Lazily construct keyValues for groups that deferred boxing
		if gs.keyValues == nil {
			gs.keyValues = []any{gs.intKey}
		}
		h.keyBuf = h.keyBuf[:0]
		key := serializeKey(h.keyBuf, gs.keyValues)
		h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, key)
		h.keys = append(h.keys, gs.keyValues)
	}
	h.useIntGroupKey = false
	h.intGroupStates = nil
	h.intGroupIndex = nil
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

// appendColumnValue appends a binary-encoded column value to buf for GROUP BY
// key construction. Uses fixed-width binary encoding for numeric types (no
// strconv text conversion), eliminating expensive int→decimal and float→string
// conversions in the hot path.
func appendColumnValue(buf []byte, v *batch.Vector, row int, typ batch.TypeID) []byte {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		val := v.Int64Data[row]
		return append(buf,
			byte(val), byte(val>>8), byte(val>>16), byte(val>>24),
			byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		val := v.Int32Data[row]
		return append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24))
	case batch.TypeFloat64:
		val := math.Float64bits(v.Float64Data[row])
		return append(buf,
			byte(val), byte(val>>8), byte(val>>16), byte(val>>24),
			byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
	case batch.TypeFloat32:
		val := math.Float32bits(v.Float32Data[row])
		return append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24))
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		data := v.BytesData.Value(row)
		l := uint16(len(data))
		buf = append(buf, byte(l), byte(l>>8))
		return append(buf, data...)
	case batch.TypeBool:
		if v.BoolData[row] {
			return append(buf, 1)
		}
		return append(buf, 0)
	case batch.TypeDecimal:
		val := math.Float64bits(v.DecimalData.Data[row].ToFloat64(v.DecimalData.Scale))
		return append(buf,
			byte(val), byte(val>>8), byte(val>>16), byte(val>>24),
			byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
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

