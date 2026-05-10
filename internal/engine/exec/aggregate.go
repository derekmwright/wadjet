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

// isAggIntType returns true if the type can be used as an integer group-by key.
func isAggIntType(t batch.TypeID) bool {
	switch t {
	case batch.TypeInt32, batch.TypeInt64, batch.TypePort, batch.TypeProtocol,
		batch.TypeDate, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC,
		batch.TypeDuration:
		return true
	}
	return false
}

// HashAggregate is a Sink that performs grouped aggregation with a hash map.
// Uses kernel-resolved typed updaters and cached column indices.
// When a SpillManager is set, input batches are spilled to disk under memory
// pressure and re-processed during Finalize.
type HashAggregate struct {
	GroupByCols   []string
	Aggs          []AggColumn
	Spill         *memory.SpillManager // optional: enables spill-to-disk
	NullGroupCols []string             // GROUPING SETS: columns to output as NULL (legacy per-node)
	GroupingSets  [][]int             // single-pass grouping sets: column indices within GroupByCols per set
	InputRowHint  int64                // estimated input rows for pre-sizing hash table

	mu            sync.Mutex
	keys          [][]any
	serializedKeys []string // pre-serialized keys matching h.keys order
	groupColIdx   []int
	aggColIdx     []int
	aggColIdx2    []int                  // second column indices for two-column aggregates
	groupColTypes  []batch.TypeID
	aggUpdaters       []kernel.RowAggUpdater // resolved typed updaters
	aggUpdatersNoNull []kernel.RowAggUpdater // no-null-check variants
	batchUpdaters     []kernel.RowAggUpdater // per-batch updater selection (reusable)
	batchAggKernels []kernel.BatchAggKernel // batch-level kernels (scalar aggregate fast path)
	scalarAccs     []kernel.Accumulator    // accumulators for scalar aggregate fast path
	isScalarAgg    bool                    // true when len(GroupByCols)==0 and all aggs are batch-able
	aggF64Extract  []float64Extractor      // pre-resolved float64 extractors per agg column (variance, corr, etc.)
	aggF64Extract2 []float64Extractor      // pre-resolved float64 extractors for second column (corr, covar)

	// Single-column integer GROUP BY fast path: uses intHashTable
	// instead of serializing keys to strings and using map[string].
	useIntGroupKey bool
	intGroupIndex  *intHashTable
	intGroupStates []*groupState
	intGroupKeyCol int // column index for the integer group-by key

	// SoA (Struct of Arrays) accumulators for intGroupKey fast path.
	// Stores accumulator fields in contiguous arrays instead of per-group
	// heap objects, reducing working set from ~192MB to ~32MB for 2M groups.
	intFlatAccs   []flatAccumArrays // one per aggregate (nil = use AoS path)
	groupIndexBuf []int32           // reused per-batch for two-phase scatter

	// Dual-integer GROUP BY fast path: two integer columns hashed via dualIntHash
	// into intHashTable, with chain verification for collision handling.
	// Uses SoA scatter like the single-int path.
	useDualIntGroupKey  bool
	dualIntGroupKeyCols [2]int     // column indices for the two GROUP BY columns
	dualIntKeysA        []int64    // first key per group
	dualIntKeysB        []int64    // second key per group
	dualIntNextGroup    []int32    // chain for hash collisions (-1 = end)

	// Multi-column compact GROUP BY fast path: binary-encoded key packed into int64.
	// Uses intHashTable for lookup. Falls back to generic path if key exceeds 8 bytes.
	useCompactGroupKey bool
	compactKeys        []string // serialized binary keys for fallback migration

	// Single-column string GROUP BY fast path: uses strHashTable with SoA scatter.
	// Two-phase approach like consumeBatchIntGroup but with string key hashing.
	useStrGroupKey bool
	strGroupKeyCol int // column index for the string group-by key

	// Multi-column generic GROUP BY SoA fast path: binary key serialization
	// with strHashTable lookup and SoA flat accumulator scatter.
	// Used when GROUP BY has multiple columns that don't fit compact/int/str paths
	// but all aggregates are simple (SUM, COUNT, MIN, MAX, AVG).
	useGenericSoA bool

	// String hash table for generic GROUP BY: open-addressing with arena-stored keys.
	// Replaces map[string]*groupState to eliminate GC scanning overhead.
	strGroupIndex  *strHashTable
	strGroupStates []*groupState

	resolved       bool
	needsDistinct  bool // true if any agg uses distinctSets
	needsExtra     bool // true if any agg uses extraState
	simpleAggs     bool // true when every Agg fits the kernel.Accumulator shape
	                   // (SUM, COUNT, MIN, MAX, AVG only — no distinct/extra state).
	                   // Drives external-merge partial-state spill eligibility.
	keyBuf         []byte
	inputSchema   []parquet.Column // schema from first input batch (for spill recovery)
	spillFiles    []string
	// partialSpillFiles holds external-merge spill files (sorted partial group
	// state). These are produced when simpleAggs && one of the SoA paths is in
	// use; Finalize k-way merges them. Distinct from spillFiles because the
	// merge logic differs (re-aggregate raw rows vs. merge partial accs).
	partialSpillFiles []string
	// unregisterSpillable, when non-nil, deregisters this aggregate from the
	// SpillManager's cooperative-spill registry. Set on first Consume when
	// canUseExternalMerge is true; cleared in Finalize once the merge has
	// materialized the final result (no more reclaimable state) or in Close
	// as a backstop.
	unregisterSpillable func()
	// partialMerger, when non-nil, drives streaming Next() from the
	// k-way merge of partial-spill runs. Set by finalizeViaPartialMerge
	// (streaming variant); each Next() pulls one batch's worth of merged
	// groups from it. This avoids materializing the full merged result in
	// memory — the bound is "one output batch + heap entries", which is
	// what makes the SF1000+ "output >> memory" case tractable. Cleared in
	// Close (and naturally drains to nil-return when exhausted).
	partialMerger *kWayMerger
	// partialMergerSchema caches the output schema computed at finalize
	// time so streaming Next() doesn't recompute it per call.
	partialMergerSchema []parquet.Column
	// spillBuffer holds rows from Consume calls in the spill branch that
	// have not yet been flushed to disk. Rows are accumulated here across
	// many Consume calls so that each physical spill file contains a
	// non-trivial amount of data. Without this batching the old code wrote
	// one file per Consume, producing millions of ~4 KB files at SF100 and
	// making Finalize unable to complete in reasonable time.
	spillBuffer      []map[string]any
	spillBufferBytes int64 // tracker bytes attributable to rows in spillBuffer
	trackedGroupMem int64 // bytes charged to Spill tracker for group state growth
	outputPos     int              // position in keys for batched Next() output
	gsPool        groupStatePool   // chunk allocator for groupState (reduces GC pressure)
}

// spillFileTargetBytes is the approximate size at which the spill buffer is
// flushed to a new file. Sized to amortize per-file open/close and header
// overhead across many rows. Exposed as a var so regression tests can
// override it to exercise the flush path deterministically.
var spillFileTargetBytes int64 = 64 * 1024 * 1024

// groupState holds per-group hash table state. The slim 24-byte base lives
// inline in the groupStatePool chunks so the SoA simple-aggs hot path
// (Q17/Q01/most TPC-H) — which never needs keyValues/accs/distinctSets/
// extraState — pays only 8 bytes for the extras pointer (nil) instead of
// 96 bytes of slice headers. Complex-agg paths (COUNT(DISTINCT), STRING_AGG,
// variance, percentile, GROUPING SETS, str-group, generic SoA) call
// ensureExtras() to lazily allocate the heavy fields.
//
// Sizing: 8 (intKey) + 4 (setID) + 4 (pad) + 8 (extras) = 24 bytes.
// The 88-byte savings vs the inline layout is 1.76 GB at SF100 scan-5
// (20M groups) and brings worker peak heap under GOMEMLIMIT at
// max_concurrent=4.
type groupState struct {
	intKey int64 // single int64 key for int-keyed groups (avoids []any boxing)
	setID  int32 // grouping set index (-1 = not a grouping set); 4 B of tail padding follows
	extras *groupStateExtras
}

// groupStateExtras holds the heavy per-group fields that are only needed by
// complex-agg paths or by the generic str-keyed path. Allocated lazily by
// (*groupState).ensureExtras() and pointed at from the slim base. The SoA
// simple-aggs path (consumeBatchIntGroup, consumeBatchDualIntGroup) never
// touches this allocation, so the typical Q17/Q01 pattern leaves it nil.
type groupStateExtras struct {
	keyValues    []any
	accs         []kernel.Accumulator
	distinctSets []map[string]struct{} // per-agg distinct value sets (nil if not COUNT(DISTINCT))
	extraState   []any                 // per-agg custom state (string_agg builder, variance state, etc.)
}

// ensureExtras lazily allocates the extras struct on first complex-path
// access. Callers that do multiple writes should bind the result to a local
// (`ext := gs.ensureExtras(); ext.keyValues = ...`) instead of calling
// ensureExtras repeatedly.
func (gs *groupState) ensureExtras() *groupStateExtras {
	if gs.extras == nil {
		gs.extras = &groupStateExtras{}
	}
	return gs.extras
}

// groupStatePool allocates groupState objects in contiguous chunks to reduce
// heap allocations and GC pressure. With per-object allocation, 1.5M groups
// at SF1 create 1.5M heap objects; with chunk allocation, they create ~366.
// Each chunk is a single contiguous array; pointers into it remain valid
// because new chunks don't move old ones.
type groupStatePool struct {
	chunks [][]groupState
	pos    int // position within current chunk
}

const groupStateChunkSize = 4096

func (p *groupStatePool) alloc() *groupState {
	if len(p.chunks) == 0 || p.pos >= len(p.chunks[len(p.chunks)-1]) {
		p.chunks = append(p.chunks, make([]groupState, groupStateChunkSize))
		p.pos = 0
	}
	gs := &p.chunks[len(p.chunks)-1][p.pos]
	p.pos++
	return gs
}

// preAlloc pre-allocates a single large initial chunk to avoid repeated
// chunk allocations during the hot consume loop. Only useful before any
// alloc() calls (when the pool is empty).
func (p *groupStatePool) preAlloc(n int) {
	if len(p.chunks) == 0 && n > groupStateChunkSize {
		p.chunks = append(p.chunks, make([]groupState, n))
		p.pos = 0
	}
}

// groupMemoryUsage returns the estimated heap bytes consumed by the aggregate's
// group state: hash tables, flat accumulator arrays, group state pool, and key
// arrays. This does NOT include input batch data (tracked separately by
// SpillManager.TrackBatch).
func (h *HashAggregate) groupMemoryUsage() int64 {
	var size int64
	if h.intGroupIndex != nil {
		size += h.intGroupIndex.MemoryUsage()
	}
	if h.strGroupIndex != nil {
		size += h.strGroupIndex.MemoryUsage()
	}
	// Group state pool: each chunk is a contiguous array of slim groupState
	// structs (24 bytes each: intKey + setID + extras pointer). Complex-agg
	// paths additionally allocate a groupStateExtras (~96 bytes per group)
	// behind the pointer, but the dominant SF100 hot path leaves extras nil
	// so this estimate is exact for SoA simple-aggs and a slight under-count
	// (~96 B/group) for the rarer complex paths. The slice contents (keyVals,
	// accs, distinctSets, extraState data) are not tracked here in either
	// regime — same accuracy budget as before the slim-struct refactor.
	for _, chunk := range h.gsPool.chunks {
		size += int64(cap(chunk)) * 24
	}
	// SoA flat accumulator arrays: 8 bytes per element for int64/float64 fields.
	for _, fa := range h.intFlatAccs {
		size += int64(cap(fa.count)) * 8
		size += int64(cap(fa.sumI64)) * 8
		size += int64(cap(fa.sumF64)) * 8
		size += int64(cap(fa.sumDec)) * 16 // Int128
		size += int64(cap(fa.minI64)) * 8
		size += int64(cap(fa.maxI64)) * 8
		size += int64(cap(fa.minF64)) * 8
		size += int64(cap(fa.maxF64)) * 8
		size += int64(cap(fa.minDec)) * 16
		size += int64(cap(fa.maxDec)) * 16
		size += int64(cap(fa.hasMin))
		size += int64(cap(fa.hasMax))
	}
	// Dual-int key arrays
	size += int64(cap(h.dualIntKeysA)) * 8
	size += int64(cap(h.dualIntKeysB)) * 8
	size += int64(cap(h.dualIntNextGroup)) * 4
	// Group state pointer slices
	size += int64(cap(h.intGroupStates)) * 8
	size += int64(cap(h.strGroupStates)) * 8
	return size
}

// reconcileGroupMemory tracks group state growth in the spill manager so that
// ShouldSpill() triggers at the correct threshold. Without this, spill only
// sees input batch cost while group states grow unbounded.
func (h *HashAggregate) reconcileGroupMemory() {
	if h.Spill == nil {
		return
	}
	actual := h.groupMemoryUsage()
	if actual > h.trackedGroupMem {
		delta := actual - h.trackedGroupMem
		h.Spill.TrackBatch(delta)
		h.trackedGroupMem = actual
	}
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

// float64Extractor reads a float64 value from a vector at a given row index.
// Pre-resolved during Init to eliminate per-row type switches in updateGroup.
type float64Extractor func(v *batch.Vector, row int) float64

// resolveFloat64Extractor returns a typed float64 extractor for the given column type.
// Returns nil if the type cannot be converted to float64.
func resolveFloat64Extractor(typ batch.TypeID) float64Extractor {
	switch typ {
	case batch.TypeInt64, batch.TypeTimestamp:
		return func(v *batch.Vector, row int) float64 { return float64(v.Int64Data[row]) }
	case batch.TypeInt32:
		return func(v *batch.Vector, row int) float64 { return float64(v.Int32Data[row]) }
	case batch.TypeFloat64:
		return func(v *batch.Vector, row int) float64 { return v.Float64Data[row] }
	case batch.TypeFloat32:
		return func(v *batch.Vector, row int) float64 { return float64(v.Float32Data[row]) }
	case batch.TypeDecimal:
		return func(v *batch.Vector, row int) float64 { return v.DecimalData.Data[row].ToFloat64(v.DecimalData.Scale) }
	default:
		return nil
	}
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
		// Cooperative-spill registration. resolveIndices populates the path
		// flags (useIntGroupKey etc.) that canUseExternalMerge inspects, so
		// register only after they've been set. The Spill manager skips us
		// until SpillFootprint > 0, so registering on a brand-new aggregate
		// before any rows arrive doesn't disturb peer operators' relief
		// targeting.
		if h.Spill != nil && h.unregisterSpillable == nil && h.canUseExternalMerge() {
			h.unregisterSpillable = h.Spill.RegisterSpillable(h)
		}
	}

	// Spill decision is based on current group state size, not input
	// throughput. Input batches are transient — they're GC'd after Consume
	// returns. Only the hash table + accumulator arrays persist, and
	// reconcileGroupMemory (below) is what drives that tracking. An earlier
	// design also called TrackBatch(batchBytes) on every input batch, which
	// monotonically accumulated cumulative throughput into the tracker and
	// forced spill-every-batch once inputBytes crossed the budget — even
	// when the actual group state was a handful of rows (e.g. Q12 with 7
	// shipmode groups). That inflated 250ms Q12 work to 37s of spill I/O.
	if h.Spill != nil && h.Spill.ShouldSpillFor(memory.SpillCheap) {
		// External-merge path: when the aggregate uses simple kernel
		// accumulators on an SoA fast path, drain the current hash table
		// to a sorted partial-state file and reset state. Finalize will
		// k-way merge across runs. This avoids the legacy raw-row spill's
		// pathological re-scan in Finalize at SF100+.
		if h.canUseExternalMerge() {
			// Consume this batch FIRST so its rows enter the hash table
			// before we drain. This keeps the per-spill file dense (one
			// drain per pressure event) instead of a write per Consume.
			h.consumeBatch(b)
			h.reconcileGroupMemory()
			return h.spillPartialState()
		}
		// Legacy raw-row spill (extra-state aggs, grouping sets, scalar):
		// buffer rows on disk and re-aggregate in Finalize.
		rows := b.ToRows()
		h.spillBuffer = append(h.spillBuffer, rows...)
		h.spillBufferBytes += EstimateBatchBytes(b)
		if h.spillBufferBytes >= spillFileTargetBytes {
			if err := h.flushSpillBuffer(); err != nil {
				return err
			}
		}
		return nil
	}

	// Iterate rows
	h.consumeBatch(b)

	// Track group state memory growth so spill triggers at the right time.
	// consumeBatch grows hash tables and accumulator arrays; this is the
	// sole signal for HashAggregate spill pressure.
	h.reconcileGroupMemory()

	return nil
}

// ColumnIndexFallback is the exported alias for columnIndexFallback so other
// packages (worker shuffle sinks) can resolve column names with the same
// bidirectional table-qualifier fallback.
func ColumnIndexFallback(b *batch.RecordBatch, name string) int {
	return columnIndexFallback(b, name)
}

// columnIndexFallback resolves a column name with bidirectional fallback for
// table-qualified names. Lookup order:
//  1. Exact match.
//  2. If the name is qualified ("table.col"), try the bare column ("col").
//  3. If the name is unqualified ("col"), try every schema column whose
//     suffix matches ".col". Returns -1 when there are 2+ matches (ambiguous,
//     refuses to guess).
//
// Step 3 is required after the self-join planner pass (Q07's
// QualifyAllBuildCols) leaves the schema with "n1.n_name" but
// parseJoinKeys feeds the worker an unqualified "n_name" key — without
// the unqualified→qualified fallback the join would never find the column.
func columnIndexFallback(b *batch.RecordBatch, name string) int {
	if idx := b.ColumnIndex(name); idx >= 0 {
		return idx
	}
	if dotIdx := strings.Index(name, "."); dotIdx >= 0 {
		return b.ColumnIndex(name[dotIdx+1:])
	}
	// Unqualified name with no exact match: scan for a single qualified
	// match ".name". Reject ambiguity (>1 match) so we never silently
	// pick the wrong column.
	suffix := "." + name
	match := -1
	for i, c := range b.Schema {
		if strings.HasSuffix(c.Name, suffix) {
			if match >= 0 {
				return -1 // ambiguous
			}
			match = i
		}
	}
	return match
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
	h.aggUpdatersNoNull = make([]kernel.RowAggUpdater, len(h.Aggs))
	h.batchUpdaters = make([]kernel.RowAggUpdater, len(h.Aggs))
	for i, agg := range h.Aggs {
		h.aggColIdx2[i] = -1 // default: no second column
		if agg.Func == AggCountDistinct || agg.Func == AggApproxDistinct {
			if agg.InputCol != "" {
				h.aggColIdx[i] = columnIndexFallback(b, agg.InputCol)
			} else {
				h.aggColIdx[i] = -1
			}
			continue
		}
		if agg.InputCol != "" {
			// Use bidirectional fallback so qualified columns from a self-
			// join chain ("lineitem.l_quantity") still resolve to the bare
			// AggSpec.InputCol ("l_quantity") and vice-versa. Without this,
			// Q18's outer SUM(l_quantity) returned NULL because the join
			// chain's QualifyAllBuildCols renamed the column.
			idx := columnIndexFallback(b, agg.InputCol)
			h.aggColIdx[i] = idx
			if idx >= 0 {
				h.aggUpdaters[i] = resolveAggUpdater(agg.Func, b.Columns[idx].Type)
				h.aggUpdatersNoNull[i] = resolveAggUpdaterNoNull(agg.Func, b.Columns[idx].Type)
			}
		} else {
			h.aggColIdx[i] = -1
			if agg.Func == AggCount {
				h.aggUpdaters[i] = kernel.ResolveRowCount(true) // COUNT(*)
				h.aggUpdatersNoNull[i] = kernel.ResolveRowCount(true)
			}
		}
		// Resolve second column index for two-column aggregates
		if agg.InputCol2 != "" {
			h.aggColIdx2[i] = columnIndexFallback(b, agg.InputCol2)
		}
	}

	// Pre-resolve float64 extractors for aggregates that need per-row numeric conversion
	// (variance, stddev, corr, covar, percentile, mode, median, min_by, max_by).
	// This eliminates the per-row type switch in updateGroup.
	h.aggF64Extract = make([]float64Extractor, len(h.Aggs))
	h.aggF64Extract2 = make([]float64Extractor, len(h.Aggs))
	for i, agg := range h.Aggs {
		switch agg.Func {
		case AggStddev, AggVariance, AggStddevPop, AggVarPop,
			AggPercentileCont, AggPercentileDisc, AggMedian, AggMode:
			if idx := h.aggColIdx[i]; idx >= 0 {
				h.aggF64Extract[i] = resolveFloat64Extractor(b.Columns[idx].Type)
			}
		case AggCorr, AggCovarSamp, AggCovarPop:
			if idx := h.aggColIdx[i]; idx >= 0 {
				h.aggF64Extract[i] = resolveFloat64Extractor(b.Columns[idx].Type)
			}
			if idx := h.aggColIdx2[i]; idx >= 0 {
				h.aggF64Extract2[i] = resolveFloat64Extractor(b.Columns[idx].Type)
			}
		case AggMinBy, AggMaxBy:
			// Second column is the comparison column (needs float64 conversion)
			if idx := h.aggColIdx2[i]; idx >= 0 {
				h.aggF64Extract2[i] = resolveFloat64Extractor(b.Columns[idx].Type)
			}
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
	h.simpleAggs = allSimpleAggs

	// Pre-sizing hint: initial hash-table capacity. InputRowHint reflects the
	// aggregate's INPUT row count (derived from scan estimates), which is a
	// poor proxy for GROUP CARDINALITY — the actual thing the hash table
	// needs to hold. Q12 has 7 groups but its InputRowHint is 50M+ rows
	// from the orders+lineitem scans; sizing the hash to inputRows/8 would
	// preAlloc ~750MB of groupState for 7 slots (confirmed on SF1-sample
	// distributed: Q12 group pool = 750MB).
	//
	// Until the planner has NDV stats, cap the initial allocation at 64K.
	// Organic doubling handles high-cardinality cases (Q17 ~20M groups) with
	// ~2x amortized memcopy overhead — acceptable compared to over-allocating
	// 300–1900 MB for low-cardinality queries.
	const htInitCap = 64 * 1024
	htInitSize := 4096
	if h.InputRowHint > int64(htInitSize)*8 {
		est := int(h.InputRowHint / 8)
		if est > htInitCap {
			est = htInitCap
		}
		htInitSize = est
	}

	// Grouping sets force the generic path — keys are prefixed with set ID
	// and only subset columns are serialized per set.
	if len(h.GroupingSets) > 0 {
		allSimpleAggs = false // prevent SoA fast paths
		if h.strGroupIndex == nil {
			h.strGroupIndex = newStrHashTable(htInitSize)
		}
		if h.strGroupStates == nil {
			h.strGroupStates = make([]*groupState, 0, htInitSize)
			h.keys = make([][]any, 0, htInitSize)
			h.serializedKeys = make([]string, 0, htInitSize)
			h.gsPool.preAlloc(htInitSize)
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
			h.intGroupIndex = newIntHashTable(htInitSize)
			h.intGroupKeyCol = h.groupColIdx[0]
			if h.intGroupStates == nil {
				h.intGroupStates = make([]*groupState, 0, htInitSize)
				h.gsPool.preAlloc(htInitSize)
			}
			if h.intFlatAccs == nil {
				if len(h.intGroupStates) > 0 {
					h.rebuildFlatAccums(b)
				} else {
					h.initFlatAccums(b)
				}
			}
		}
	}

	// Dual-integer GROUP BY fast path:
	// When grouping by exactly 2 integer columns, use composite dualIntHash
	// with chain verification. Gets SoA scatter like the single-int path.
	if !h.useIntGroupKey && !h.isScalarAgg && len(h.GroupByCols) == 2 && allSimpleAggs {
		idx0, idx1 := h.groupColIdx[0], h.groupColIdx[1]
		if idx0 >= 0 && idx1 >= 0 {
			typ0, typ1 := h.groupColTypes[0], h.groupColTypes[1]
			if isAggIntType(typ0) && isAggIntType(typ1) {
				h.useDualIntGroupKey = true
				h.dualIntGroupKeyCols = [2]int{idx0, idx1}
				h.intGroupIndex = newIntHashTable(htInitSize)
				if h.intGroupStates == nil {
					h.intGroupStates = make([]*groupState, 0, htInitSize)
					h.gsPool.preAlloc(htInitSize)
				}
				if h.intFlatAccs == nil {
					if len(h.intGroupStates) > 0 {
						h.rebuildFlatAccums(b)
					} else {
						h.initFlatAccums(b)
					}
				}
			}
		}
	}

	// Multi-column compact GROUP BY fast path:
	// When the binary-encoded GROUP BY key fits in 8 bytes, pack it into int64
	// and use intHashTable instead of map[string]. Avoids string hashing and
	// Go map overhead. Falls back to generic path if a key exceeds 8 bytes.
	if !h.useIntGroupKey && !h.useDualIntGroupKey && !h.isScalarAgg && len(h.GroupByCols) >= 2 && allSimpleAggs {
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
			h.intGroupIndex = newIntHashTable(htInitSize)
			if h.intGroupStates == nil {
				h.intGroupStates = make([]*groupState, 0, htInitSize)
				h.gsPool.preAlloc(htInitSize)
			}
			if h.intFlatAccs == nil {
				if len(h.intGroupStates) > 0 {
					h.rebuildFlatAccums(b)
				} else {
					h.initFlatAccums(b)
				}
			}
		}
	}

	// Single-column string GROUP BY fast path:
	// When grouping by one string/bytes column with simple aggregates,
	// use two-phase SoA scatter like consumeBatchIntGroup.
	if !h.useIntGroupKey && !h.useDualIntGroupKey && !h.useCompactGroupKey &&
		!h.isScalarAgg && len(h.GroupByCols) == 1 && allSimpleAggs {
		idx := h.groupColIdx[0]
		if idx >= 0 {
			typ := h.groupColTypes[0]
			if typ == batch.TypeString || typ == batch.TypeBytes {
				h.useStrGroupKey = true
				h.strGroupKeyCol = idx
				if h.strGroupIndex == nil {
					h.strGroupIndex = newStrHashTable(htInitSize)
				}
				if h.strGroupStates == nil {
					h.strGroupStates = make([]*groupState, 0, htInitSize)
					h.keys = make([][]any, 0, htInitSize)
					h.serializedKeys = make([]string, 0, htInitSize)
					h.gsPool.preAlloc(htInitSize)
				}
				if h.intFlatAccs == nil {
					if len(h.strGroupStates) > 0 {
						h.rebuildFlatAccums(b)
					} else {
						h.initFlatAccums(b)
					}
				}
			}
		}
	}

	// Multi-column generic GROUP BY SoA fast path:
	// When all other fast paths are exhausted but aggregates are simple,
	// use strHashTable with binary key serialization and SoA scatter.
	// Benefits Q7, Q9, Q10, Q18 at SF10 (multi-column GROUP BY with strings).
	if !h.useIntGroupKey && !h.useDualIntGroupKey && !h.useCompactGroupKey &&
		!h.useStrGroupKey && !h.isScalarAgg && len(h.GroupByCols) > 0 && allSimpleAggs {
		h.useGenericSoA = true
		if h.strGroupIndex == nil {
			h.strGroupIndex = newStrHashTable(htInitSize)
		}
		if h.strGroupStates == nil {
			h.strGroupStates = make([]*groupState, 0, htInitSize)
			h.keys = make([][]any, 0, htInitSize)
			h.serializedKeys = make([]string, 0, htInitSize)
			h.gsPool.preAlloc(htInitSize)
		}
		if h.intFlatAccs == nil {
			if len(h.strGroupStates) > 0 {
				h.rebuildFlatAccums(b)
			} else {
				h.initFlatAccums(b)
			}
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

	// Select no-null-check updaters for columns without nulls in this batch.
	// Applies to all grouped paths: int, compact, and generic.
	for i := 0; i < len(h.Aggs); i++ {
		ci := h.aggColIdx[i]
		if ci >= 0 && h.aggUpdatersNoNull[i] != nil && !b.Columns[ci].Nulls.HasNulls() {
			h.batchUpdaters[i] = h.aggUpdatersNoNull[i]
		} else {
			h.batchUpdaters[i] = h.aggUpdaters[i]
		}
	}

	// Rebuild flat accumulators if they were cleared by materializeFlatAccums
	// (e.g. after parallel merge). This happens when flushSpilledOps replays
	// spilled batches through Consume after the merge phase.
	if h.intFlatAccs == nil && (h.useIntGroupKey || h.useDualIntGroupKey || h.useCompactGroupKey || h.useStrGroupKey || h.useGenericSoA) {
		h.rebuildFlatAccums(b)
	}

	// Single-column integer GROUP BY fast path
	if h.useIntGroupKey {
		h.consumeBatchIntGroup(b)
		return
	}

	// Dual-integer GROUP BY fast path
	if h.useDualIntGroupKey {
		h.consumeBatchDualIntGroup(b)
		return
	}

	// Multi-column compact GROUP BY fast path
	if h.useCompactGroupKey {
		h.consumeBatchCompactGroup(b)
		return
	}

	// Single-column string GROUP BY fast path
	if h.useStrGroupKey {
		h.consumeBatchStrGroup(b)
		return
	}

	// Multi-column generic GROUP BY SoA fast path
	if h.useGenericSoA {
		h.consumeBatchGenericSoA(b)
		return
	}

	// Grouping sets single-pass: insert each row once per set
	if len(h.GroupingSets) > 0 {
		if b.Sel != nil {
			for _, idx := range b.Sel {
				h.processRowGroupingSets(b, int(idx))
			}
		} else {
			for i := 0; i < b.Len; i++ {
				h.processRowGroupingSets(b, i)
			}
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

// consumeBatchIntGroup is the fast path for single-column integer GROUP BY.
// Uses intHashTable for group lookup — no key serialization, no string allocation.
//
// Two-phase SoA (Struct of Arrays) approach:
//   Phase 1: Hash lookup — compute group indices for all rows in the batch.
//   Phase 2: Per-aggregate typed scatter update using flat accumulator arrays.
//
// This eliminates per-row function pointer overhead (indirect calls can't inline),
// removes the inner nAggs loop per row, and stores accumulators in contiguous arrays
// instead of scattered per-group heap objects (~16MB vs ~192MB working set for 2M groups).
func (h *HashAggregate) consumeBatchIntGroup(b *batch.RecordBatch) {
	gkVec := b.Columns[h.intGroupKeyCol]
	isInt32 := h.groupColTypes[0] == batch.TypeInt32 ||
		h.groupColTypes[0] == batch.TypePort ||
		h.groupColTypes[0] == batch.TypeProtocol ||
		h.groupColTypes[0] == batch.TypeDate
	hasNulls := gkVec.Nulls.HasNulls()

	intIdx := h.intGroupIndex
	colIdx := h.aggColIdx
	nAggs := len(h.Aggs)

	// Pre-reserve accumulator capacity so per-group append() calls in the
	// hash lookup loop don't trigger growslice reallocations. The batch size
	// is an upper bound on new groups this batch can create.
	batchRows := b.ActiveLen()
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].ensureCapacity(batchRows)
	}

	// Phase 1: Hash lookup — build group index array.
	// gi[i] maps iteration index i to its group state index, or -1 for null keys.
	var gi []int32
	var sel []uint32
	var iterLen int
	hasNullKeys := false

	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			if hasNulls && gkVec.Nulls.IsNullFast(row) {
				gi[si] = -1
				hasNullKeys = true
				continue
			}
			var key int64
			if isInt32 {
				key = int64(gkVec.Int32Data[row])
			} else {
				key = gkVec.Int64Data[row]
			}
			newIdx := int32(len(h.intGroupStates))
			gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
			if ok {
				gi[si] = gsIdx
			} else {
				intIdx.CheckGrow()
				gs := h.gsPool.alloc()
				gs.intKey = key
				h.intGroupStates = append(h.intGroupStates, gs)
				for ai := range h.intFlatAccs {
					h.intFlatAccs[ai].appendGroup()
				}
				gi[si] = newIdx
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		for row := 0; row < iterLen; row++ {
			if hasNulls && gkVec.Nulls.IsNullFast(row) {
				gi[row] = -1
				hasNullKeys = true
				continue
			}
			var key int64
			if isInt32 {
				key = int64(gkVec.Int32Data[row])
			} else {
				key = gkVec.Int64Data[row]
			}
			newIdx := int32(len(h.intGroupStates))
			gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
			if ok {
				gi[row] = gsIdx
			} else {
				intIdx.CheckGrow()
				gs := h.gsPool.alloc()
				gs.intKey = key
				h.intGroupStates = append(h.intGroupStates, gs)
				for ai := range h.intFlatAccs {
					h.intFlatAccs[ai].appendGroup()
				}
				gi[row] = newIdx
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	// One pass per aggregate with inlined typed arithmetic (no function pointers).
	for i := 0; i < nAggs; i++ {
		fa := &h.intFlatAccs[i]
		ci := colIdx[i]
		if ci >= 0 {
			scatterFlatAggUpdate(fa, gi, h.Aggs[i].Func, b.Columns[ci], sel, iterLen)
		} else if h.Aggs[i].Func == AggCount {
			scatterCountStar(fa.count, gi, iterLen)
		}
	}

	// Handle null-key rows via generic path (rare: only when GROUP BY key is nullable).
	if hasNullKeys {
		if sel != nil {
			for si, selIdx := range sel {
				if gi[si] < 0 {
					h.processRow(b, int(selIdx))
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if gi[row] < 0 {
					h.processRow(b, row)
				}
			}
		}
	}
}

// consumeBatchDualIntGroup is the fast path for two-column integer GROUP BY.
// Uses composite dualIntHash in intHashTable with chain verification for collisions.
// Two-phase SoA approach like consumeBatchIntGroup.
func (h *HashAggregate) consumeBatchDualIntGroup(b *batch.RecordBatch) {
	col0 := b.Columns[h.dualIntGroupKeyCols[0]]
	col1 := b.Columns[h.dualIntGroupKeyCols[1]]
	hasNulls := col0.Nulls.HasNulls() || col1.Nulls.HasNulls()

	// Pre-extract typed arrays for the two GROUP BY columns.
	var d0i32 []int32
	var d0i64 []int64
	var d1i32 []int32
	var d1i64 []int64
	switch col0.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		d0i32 = col0.Int32Data
	default:
		d0i64 = col0.Int64Data
	}
	switch col1.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		d1i32 = col1.Int32Data
	default:
		d1i64 = col1.Int64Data
	}

	intIdx := h.intGroupIndex
	colIdx := h.aggColIdx
	nAggs := len(h.Aggs)

	// Pre-reserve accumulator capacity for this batch.
	batchRows := b.ActiveLen()
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].ensureCapacity(batchRows)
	}

	// Phase 1: Hash lookup — build group index array with chain verification.
	var gi []int32
	var sel []uint32
	var iterLen int
	hasNullKeys := false

	lookupOrInsert := func(a, b int64) int32 {
		ck := dualIntHash(a, b)
		head, ok := intIdx.Get(ck)
		if ok {
			// Walk chain, verify both keys
			for gi := head; gi >= 0; gi = h.dualIntNextGroup[gi] {
				if h.dualIntKeysA[gi] == a && h.dualIntKeysB[gi] == b {
					return gi
				}
			}
			// Collision: new group, chain to existing head
			newIdx := int32(len(h.intGroupStates))
			h.intGroupStates = append(h.intGroupStates, h.gsPool.alloc())
			h.dualIntKeysA = append(h.dualIntKeysA, a)
			h.dualIntKeysB = append(h.dualIntKeysB, b)
			h.dualIntNextGroup = append(h.dualIntNextGroup, head)
			intIdx.Put(ck, newIdx)
			for ai := range h.intFlatAccs {
				h.intFlatAccs[ai].appendGroup()
			}
			return newIdx
		}
		// New composite key
		newIdx := int32(len(h.intGroupStates))
		h.intGroupStates = append(h.intGroupStates, h.gsPool.alloc())
		h.dualIntKeysA = append(h.dualIntKeysA, a)
		h.dualIntKeysB = append(h.dualIntKeysB, b)
		h.dualIntNextGroup = append(h.dualIntNextGroup, -1)
		intIdx.Put(ck, newIdx)
		for ai := range h.intFlatAccs {
			h.intFlatAccs[ai].appendGroup()
		}
		return newIdx
	}

	extractKeys := func(row int) (int64, int64) {
		var a, b int64
		if d0i32 != nil {
			a = int64(d0i32[row])
		} else {
			a = d0i64[row]
		}
		if d1i32 != nil {
			b = int64(d1i32[row])
		} else {
			b = d1i64[row]
		}
		return a, b
	}

	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			if hasNulls && (col0.Nulls.IsNullFast(row) || col1.Nulls.IsNullFast(row)) {
				gi[si] = -1
				hasNullKeys = true
				continue
			}
			a, bv := extractKeys(row)
			gi[si] = lookupOrInsert(a, bv)
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		for row := 0; row < iterLen; row++ {
			if hasNulls && (col0.Nulls.IsNullFast(row) || col1.Nulls.IsNullFast(row)) {
				gi[row] = -1
				hasNullKeys = true
				continue
			}
			a, bv := extractKeys(row)
			gi[row] = lookupOrInsert(a, bv)
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	for i := 0; i < nAggs; i++ {
		fa := &h.intFlatAccs[i]
		ci := colIdx[i]
		if ci >= 0 {
			scatterFlatAggUpdate(fa, gi, h.Aggs[i].Func, b.Columns[ci], sel, iterLen)
		} else if h.Aggs[i].Func == AggCount {
			scatterCountStar(fa.count, gi, iterLen)
		}
	}

	// Handle null-key rows via generic path (rare).
	if hasNullKeys {
		if sel != nil {
			for si, selIdx := range sel {
				if gi[si] < 0 {
					h.processRow(b, int(selIdx))
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if gi[row] < 0 {
					h.processRow(b, row)
				}
			}
		}
	}
}

// ensureGroupIndexBuf returns a []int32 of at least length n, reusing the buffer.
func (h *HashAggregate) ensureGroupIndexBuf(n int) []int32 {
	if cap(h.groupIndexBuf) < n {
		h.groupIndexBuf = make([]int32, n)
	}
	return h.groupIndexBuf[:n]
}

// consumeBatchCompactGroup is the fast path for multi-column GROUP BY where
// the binary-encoded key fits in int64. Uses intHashTable for group lookup
// with SoA flat accumulator scatter (two-phase like consumeBatchIntGroup).
// Phase 1: Hash lookup builds group index array.
// Phase 2: Per-aggregate typed scatter update (one pass per agg, no per-row dispatch).
// Falls back to generic path if any key exceeds 8 bytes.
func (h *HashAggregate) consumeBatchCompactGroup(b *batch.RecordBatch) {
	intIdx := h.intGroupIndex
	colIdx := h.aggColIdx
	nAggs := len(h.Aggs)

	// Pre-reserve accumulator capacity.
	batchRows := b.ActiveLen()
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].ensureCapacity(batchRows)
	}

	// Phase 1: Encode keys, hash lookup, build group index array.
	var gi []int32
	var sel []uint32
	var iterLen int

	encodeKey := func(row int) bool {
		h.keyBuf = h.keyBuf[:0]
		for ci, idx := range h.groupColIdx {
			if idx < 0 {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			h.keyBuf = append(h.keyBuf, 0)
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[ci])
		}
		return len(h.keyBuf) <= 8
	}

	newGroup := func(row int, key int64) {
		intIdx.CheckGrow()
		keyVals := make([]any, len(h.GroupByCols))
		for ki, idx := range h.groupColIdx {
			if idx >= 0 {
				keyVals[ki] = b.Columns[idx].GetValue(row)
			}
		}
		gs := h.gsPool.alloc()
		gs.ensureExtras().keyValues = keyVals
		h.intGroupStates = append(h.intGroupStates, gs)
		h.compactKeys = append(h.compactKeys, string(h.keyBuf))
		h.keys = append(h.keys, keyVals)
		for ai := range h.intFlatAccs {
			h.intFlatAccs[ai].appendGroup()
		}
	}

	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			if !encodeKey(row) {
				h.compactFallback(b, gi, sel, si, iterLen)
				return
			}
			key := packKeyInt64(h.keyBuf)
			newIdx := int32(len(h.intGroupStates))
			gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
			if ok {
				gi[si] = gsIdx
			} else {
				newGroup(row, key)
				gi[si] = newIdx
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		for row := 0; row < iterLen; row++ {
			if !encodeKey(row) {
				h.compactFallback(b, gi, nil, row, iterLen)
				return
			}
			key := packKeyInt64(h.keyBuf)
			newIdx := int32(len(h.intGroupStates))
			gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
			if ok {
				gi[row] = gsIdx
			} else {
				newGroup(row, key)
				gi[row] = newIdx
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	for i := 0; i < nAggs; i++ {
		fa := &h.intFlatAccs[i]
		ci := colIdx[i]
		if ci >= 0 {
			scatterFlatAggUpdate(fa, gi, h.Aggs[i].Func, b.Columns[ci], sel, iterLen)
		} else if h.Aggs[i].Func == AggCount {
			scatterCountStar(fa.count, gi, iterLen)
		}
	}
}

// compactFallback handles the case where a compact key exceeds 8 bytes.
// Scatters the already-indexed rows via Phase 2, materializes SoA accumulators,
// migrates to generic path, and processes remaining rows per-row.
func (h *HashAggregate) compactFallback(b *batch.RecordBatch, gi []int32, sel []uint32, fallbackAt int, totalRows int) {
	// Phase 2 for rows already indexed.
	if fallbackAt > 0 {
		colIdx := h.aggColIdx
		truncSel := sel
		if truncSel != nil {
			truncSel = truncSel[:fallbackAt]
		}
		for i := 0; i < len(h.Aggs); i++ {
			fa := &h.intFlatAccs[i]
			ci := colIdx[i]
			if ci >= 0 {
				scatterFlatAggUpdate(fa, gi, h.Aggs[i].Func, b.Columns[ci], truncSel, fallbackAt)
			} else if h.Aggs[i].Func == AggCount {
				scatterCountStar(fa.count, gi[:fallbackAt], fallbackAt)
			}
		}
	}

	// Materialize SoA → AoS and migrate to generic.
	h.materializeFlatAccums()
	h.migrateCompactToGeneric()

	// Process remaining rows (including the fallback row) generically.
	if sel != nil {
		for j := fallbackAt; j < totalRows; j++ {
			h.processRow(b, int(sel[j]))
		}
	} else {
		for j := fallbackAt; j < totalRows; j++ {
			h.processRow(b, j)
		}
	}
}

// consumeBatchStrGroup is the fast path for single-column string GROUP BY.
// Uses strHashTable for group lookup with SoA flat accumulator scatter.
// Two-phase approach matches consumeBatchIntGroup: hash lookup then typed scatter.
func (h *HashAggregate) consumeBatchStrGroup(b *batch.RecordBatch) {
	gkVec := b.Columns[h.strGroupKeyCol]
	hasNulls := gkVec.Nulls.HasNulls()
	strIdx := h.strGroupIndex
	colIdx := h.aggColIdx
	nAggs := len(h.Aggs)

	// Pre-reserve accumulator capacity (same rationale as consumeBatchIntGroup).
	batchRows := b.ActiveLen()
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].ensureCapacity(batchRows)
	}

	// Phase 1: Hash lookup — build group index array.
	var gi []int32
	var sel []uint32
	var iterLen int
	hasNullKeys := false

	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			if hasNulls && gkVec.Nulls.IsNullFast(row) {
				gi[si] = -1
				hasNullKeys = true
				continue
			}
			key := gkVec.BytesData.Value(row)
			gsIdx, found := strIdx.GetOrInsert(key, int32(len(h.strGroupStates)))
			if found {
				gi[si] = gsIdx
			} else {
				keyStr := string(key)
				gs := h.gsPool.alloc()
				gs.ensureExtras().keyValues = []any{keyStr}
				h.strGroupStates = append(h.strGroupStates, gs)
				h.keys = append(h.keys, []any{keyStr})
				h.serializedKeys = append(h.serializedKeys, keyStr)
				for ai := range h.intFlatAccs {
					h.intFlatAccs[ai].appendGroup()
				}
				gi[si] = gsIdx
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		for row := 0; row < iterLen; row++ {
			if hasNulls && gkVec.Nulls.IsNullFast(row) {
				gi[row] = -1
				hasNullKeys = true
				continue
			}
			key := gkVec.BytesData.Value(row)
			gsIdx, found := strIdx.GetOrInsert(key, int32(len(h.strGroupStates)))
			if found {
				gi[row] = gsIdx
			} else {
				keyStr := string(key)
				gs := h.gsPool.alloc()
				gs.ensureExtras().keyValues = []any{keyStr}
				h.strGroupStates = append(h.strGroupStates, gs)
				h.keys = append(h.keys, []any{keyStr})
				h.serializedKeys = append(h.serializedKeys, keyStr)
				for ai := range h.intFlatAccs {
					h.intFlatAccs[ai].appendGroup()
				}
				gi[row] = gsIdx
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	for i := 0; i < nAggs; i++ {
		fa := &h.intFlatAccs[i]
		ci := colIdx[i]
		if ci >= 0 {
			scatterFlatAggUpdate(fa, gi, h.Aggs[i].Func, b.Columns[ci], sel, iterLen)
		} else if h.Aggs[i].Func == AggCount {
			scatterCountStar(fa.count, gi, iterLen)
		}
	}

	// Handle null-key rows via generic path.
	if hasNullKeys {
		if sel != nil {
			for si, selIdx := range sel {
				if gi[si] < 0 {
					h.processRow(b, int(selIdx))
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if gi[row] < 0 {
					h.processRow(b, row)
				}
			}
		}
	}
}

// consumeBatchGenericSoA is the SoA fast path for multi-column GROUP BY
// that doesn't fit int/dual-int/compact/single-string paths.
// Uses binary key serialization into strHashTable with two-phase scatter.
// Phase 1: Serialize keys, hash lookup, build group index array.
// Phase 2: Per-aggregate typed scatter update (one pass per agg, no per-row dispatch).
func (h *HashAggregate) consumeBatchGenericSoA(b *batch.RecordBatch) {
	strIdx := h.strGroupIndex
	colIdx := h.aggColIdx
	nAggs := len(h.Aggs)

	batchRows := b.ActiveLen()
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].ensureCapacity(batchRows)
	}

	// Phase 1: Serialize keys, hash lookup, build group index array.
	var gi []int32
	var sel []uint32
	var iterLen int

	serializeKey := func(row int) {
		h.keyBuf = h.keyBuf[:0]
		for ci, idx := range h.groupColIdx {
			if idx < 0 {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			h.keyBuf = append(h.keyBuf, 0)
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[ci])
		}
	}

	newGroup := func(row int) {
		keyVals := make([]any, len(h.GroupByCols))
		for ki, idx := range h.groupColIdx {
			if idx >= 0 {
				keyVals[ki] = b.Columns[idx].GetValue(row)
			}
		}
		gs := h.gsPool.alloc()
		gs.ensureExtras().keyValues = keyVals
		h.strGroupStates = append(h.strGroupStates, gs)
		h.keys = append(h.keys, keyVals)
		h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
		for ai := range h.intFlatAccs {
			h.intFlatAccs[ai].appendGroup()
		}
	}

	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			serializeKey(row)
			gsIdx, found := strIdx.GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
			if found {
				gi[si] = gsIdx
			} else {
				newGroup(row)
				gi[si] = gsIdx
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		for row := 0; row < iterLen; row++ {
			serializeKey(row)
			gsIdx, found := strIdx.GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
			if found {
				gi[row] = gsIdx
			} else {
				newGroup(row)
				gi[row] = gsIdx
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	for i := 0; i < nAggs; i++ {
		fa := &h.intFlatAccs[i]
		ci := colIdx[i]
		if ci >= 0 {
			scatterFlatAggUpdate(fa, gi, h.Aggs[i].Func, b.Columns[ci], sel, iterLen)
		} else if h.Aggs[i].Func == AggCount {
			scatterCountStar(fa.count, gi, iterLen)
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
	gs := h.gsPool.alloc()
	ext := gs.ensureExtras()
	ext.keyValues = keyVals
	ext.accs = make([]kernel.Accumulator, len(h.Aggs))
	if h.needsDistinct {
		ext.distinctSets = make([]map[string]struct{}, len(h.Aggs))
	}
	if h.needsExtra {
		ext.extraState = make([]any, len(h.Aggs))
	}
	for i, agg := range h.Aggs {
		switch agg.Func {
		case AggCountDistinct:
			if ext.distinctSets != nil {
				ext.distinctSets[i] = make(map[string]struct{})
			}
		case AggStringAgg:
			sep := agg.Separator
			if sep == "" {
				sep = ","
			}
			ext.extraState[i] = &stringAggState{sep: sep}
		case AggStddev, AggVariance, AggStddevPop, AggVarPop:
			ext.extraState[i] = &varianceState{}
		case AggBoolAnd:
			ext.extraState[i] = true
		case AggBoolOr:
			ext.extraState[i] = false
		case AggApproxDistinct:
			if ext.distinctSets != nil {
				ext.distinctSets[i] = make(map[string]struct{})
			}
		case AggCorr, AggCovarSamp, AggCovarPop:
			ext.extraState[i] = &covarianceState{}
		case AggPercentileCont, AggPercentileDisc, AggMode, AggMedian:
			ext.extraState[i] = &collectState{}
		case AggMinBy:
			ext.extraState[i] = &minMaxByState{isMin: true}
		case AggMaxBy:
			ext.extraState[i] = &minMaxByState{isMin: false}
		}
	}
	h.strGroupStates = append(h.strGroupStates, gs)
	h.keys = append(h.keys, keyVals)
	h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))

	// Keep the SoA flat accumulator length in sync with strGroupStates.
	// processRow is called from the null-key branch of consumeBatchStrGroup
	// (and other str-group paths) AFTER the SoA fast paths are configured;
	// without this appendGroup, the hash table maps a new key to gsIdx N
	// but fa.count is still len N, so the next batch's scatterCountStar
	// indexes out of bounds. Manifested as Q21 SF1's mysterious panic.
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].appendGroup()
	}

	h.updateGroup(gs, b, row)
}

// processRowGroupingSets inserts a row into each grouping set. The key is
// prefixed with the set index byte, and only columns in the set are serialized.
// Columns not in the set are stored as nil in keyValues so the output path
// can emit NULLs for excluded columns.
func (h *HashAggregate) processRowGroupingSets(b *batch.RecordBatch, row int) {
	for setIdx, colIndices := range h.GroupingSets {
		h.keyBuf = h.keyBuf[:0]
		h.keyBuf = append(h.keyBuf, byte(setIdx)) // set prefix

		// Serialize only the columns in this set
		for _, ci := range colIndices {
			idx := h.groupColIdx[ci]
			if idx < 0 {
				h.keyBuf = append(h.keyBuf, 1) // null flag
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				h.keyBuf = append(h.keyBuf, 1)
				continue
			}
			h.keyBuf = append(h.keyBuf, 0)
			h.keyBuf = appendColumnValue(h.keyBuf, v, row, h.groupColTypes[ci])
		}

		groupIdx, found := h.strGroupIndex.GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
		if found {
			h.updateGroup(h.strGroupStates[groupIdx], b, row)
			continue
		}

		// New group — store all GroupByCols values, NULLing excluded columns
		inSet := make(map[int]bool, len(colIndices))
		for _, ci := range colIndices {
			inSet[ci] = true
		}
		keyVals := make([]any, len(h.GroupByCols))
		for i, idx := range h.groupColIdx {
			if inSet[i] && idx >= 0 {
				keyVals[i] = b.Columns[idx].GetValue(row)
			}
			// columns not in set stay nil
		}
		gs := h.gsPool.alloc()
		gs.setID = int32(setIdx)
		ext := gs.ensureExtras()
		ext.keyValues = keyVals
		ext.accs = make([]kernel.Accumulator, len(h.Aggs))
		if h.needsDistinct {
			ext.distinctSets = make([]map[string]struct{}, len(h.Aggs))
		}
		if h.needsExtra {
			ext.extraState = make([]any, len(h.Aggs))
		}
		for i, agg := range h.Aggs {
			switch agg.Func {
			case AggCountDistinct, AggApproxDistinct:
				if ext.distinctSets != nil {
					ext.distinctSets[i] = make(map[string]struct{})
				}
			case AggStringAgg:
				sep := agg.Separator
				if sep == "" {
					sep = ","
				}
				ext.extraState[i] = &stringAggState{sep: sep}
			case AggStddev, AggVariance, AggStddevPop, AggVarPop:
				ext.extraState[i] = &varianceState{}
			case AggBoolAnd:
				ext.extraState[i] = true
			case AggBoolOr:
				ext.extraState[i] = false
			case AggCorr, AggCovarSamp, AggCovarPop:
				ext.extraState[i] = &covarianceState{}
			case AggPercentileCont, AggPercentileDisc, AggMode, AggMedian:
				ext.extraState[i] = &collectState{}
			case AggMinBy:
				ext.extraState[i] = &minMaxByState{isMin: true}
			case AggMaxBy:
				ext.extraState[i] = &minMaxByState{isMin: false}
			}
		}
		h.strGroupStates = append(h.strGroupStates, gs)
		h.keys = append(h.keys, keyVals)
		h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
		h.updateGroup(gs, b, row)
	}
}

// updateGroup updates a group's accumulators with values from a single row.
// updateGroup is only ever called on groups whose extras was allocated by
// processRow / processRowGroupingSets, so gs.extras is non-nil here and we
// bind it to a local for compactness.
func (h *HashAggregate) updateGroup(gs *groupState, b *batch.RecordBatch, row int) {
	ext := gs.extras
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
			ext.distinctSets[i][valKey] = struct{}{}

		case AggStringAgg:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			state := ext.extraState[i].(*stringAggState)
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
			current := ext.extraState[i].(bool)
			ext.extraState[i] = current && boolVal

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
			current := ext.extraState[i].(bool)
			ext.extraState[i] = current || boolVal

		case AggStddev, AggVariance, AggStddevPop, AggVarPop:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			extract := h.aggF64Extract[i]
			if extract == nil {
				continue
			}
			ext.extraState[i].(*varianceState).update(extract(v, row))

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
			ext.distinctSets[i][valKey] = struct{}{}

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
			e1, e2 := h.aggF64Extract[i], h.aggF64Extract2[i]
			if e1 == nil || e2 == nil {
				continue
			}
			ext.extraState[i].(*covarianceState).update(e1(v1, row), e2(v2, row))

		case AggPercentileCont, AggPercentileDisc, AggMedian:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			extract := h.aggF64Extract[i]
			if extract == nil {
				continue
			}
			ext.extraState[i].(*collectState).values = append(ext.extraState[i].(*collectState).values, extract(v, row))

		case AggMode:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			extract := h.aggF64Extract[i]
			if extract == nil {
				continue
			}
			ext.extraState[i].(*collectState).values = append(ext.extraState[i].(*collectState).values, extract(v, row))

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
			extract2 := h.aggF64Extract2[i]
			if extract2 == nil {
				continue
			}
			state := ext.extraState[i].(*minMaxByState)
			cmpVal := extract2(v2, row)
			if !state.hasValue ||
				(state.isMin && cmpVal < state.bestCmp) ||
				(!state.isMin && cmpVal > state.bestCmp) {
				state.hasValue = true
				state.bestCmp = cmpVal
				state.bestVal = v1.GetValue(row)
			}

		default:
			updater := h.batchUpdaters[i]
			if updater == nil {
				continue
			}
			idx := h.aggColIdx[i]
			if idx >= 0 {
				updater(&ext.accs[i], b.Columns[idx], row)
			} else {
				// COUNT(*) — pass nil vec
				updater(&ext.accs[i], nil, row)
			}
		}
	}
}

// flushSpillBuffer writes h.spillBuffer to a new spill file and releases the
// tracker bytes for those rows. Caller must hold h.mu.
func (h *HashAggregate) flushSpillBuffer() error {
	if len(h.spillBuffer) == 0 {
		return nil
	}
	path, err := h.Spill.SpillRows(h.spillBuffer)
	if err != nil {
		return err
	}
	h.spillFiles = append(h.spillFiles, path)
	// The rows are now on disk — release the tracker bytes charged for them
	// so ShouldSpill can flip back to false and subsequent batches can be
	// consumed directly into the hash table again.
	h.Spill.ReleaseTracking(h.spillBufferBytes)
	h.spillBuffer = nil
	h.spillBufferBytes = 0
	return nil
}

func (h *HashAggregate) Finalize(_ context.Context) error {
	// Once Finalize starts, the aggregate is no longer a viable cooperative
	// spill target — it's about to drain its in-memory state into the merger.
	// Deregister here so peer operators don't waste a RequestRelief call on
	// us mid-finalize. Close still calls unregister as a backstop.
	if h.unregisterSpillable != nil {
		h.unregisterSpillable()
		h.unregisterSpillable = nil
	}

	// External-merge path: when partial-state files exist, k-way merge them
	// (plus any in-memory remainder) instead of re-aggregating raw rows. This
	// is the SF100+ unblock: the legacy raw-row Finalize re-reads ALL spilled
	// input back into the hash table and re-runs Consume on it, blowing the
	// budget when input >> output. Partial-state merge processes each group
	// once and is bounded by the merged result size.
	if len(h.partialSpillFiles) > 0 {
		return h.finalizeViaPartialMerge()
	}

	if len(h.spillFiles) == 0 && len(h.spillBuffer) == 0 {
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
		// Resolve indices only if not already resolved. Do NOT force
		// re-resolution (h.resolved = false) — after a parallel merge
		// (MergeSink), the aggregate has been migrated from compact/int
		// key mode to generic string mode. Re-resolving would switch
		// back to compact mode with a fresh intGroupStates, losing all
		// merged groups and causing index-out-of-bounds in Next().
		// The spilled batch uses h.inputSchema (same column order as
		// the original), so re-resolution is unnecessary.
		if !h.resolved {
			h.resolveIndices(b)
		}
		h.consumeBatch(b)
	}
	h.spillFiles = nil

	// Drain any rows that were buffered but never crossed the flush
	// threshold — they're still in memory and can be consumed directly
	// without a round-trip through disk.
	if len(h.spillBuffer) > 0 {
		b := batch.FromRows(h.inputSchema, h.spillBuffer)
		if !h.resolved {
			h.resolveIndices(b)
		}
		h.consumeBatch(b)
		if h.Spill != nil {
			h.Spill.ReleaseTracking(h.spillBufferBytes)
		}
		h.spillBuffer = nil
		h.spillBufferBytes = 0
	}

	return nil
}

// Close releases any tracker reservation HashAggregate still holds for
// group-state memory and buffered-but-unspilled rows. Without this, a
// non-spilling HashAggregate accumulates a phantom reservation in the
// shared tracker for the lifetime of the process; see HashJoin.Close for
// the full background.
func (h *HashAggregate) Close() error {
	if h.unregisterSpillable != nil {
		h.unregisterSpillable()
		h.unregisterSpillable = nil
	}
	if h.Spill != nil {
		if h.trackedGroupMem > 0 {
			h.Spill.ReleaseTracking(h.trackedGroupMem)
			h.trackedGroupMem = 0
		}
		if h.spillBufferBytes > 0 {
			h.Spill.ReleaseTracking(h.spillBufferBytes)
			h.spillBufferBytes = 0
		}
	}
	// Close the streaming merger (if any) and remove its spill files. This
	// is the backstop for early-termination paths (cancellation, error
	// before exhaustion); the normal drain in Next() also calls
	// closePartialMerger when the merger returns nil.
	h.closePartialMerger()
	h.spillBuffer = nil
	return nil
}

// SpillFootprint reports the bytes of in-memory hash-table state this
// aggregate currently holds and can release via SpillSome. Returns 0 when
// the aggregate is in a configuration that doesn't support partial-state
// spill (extra-state aggs, grouping sets, scalar) — those cases must use
// the legacy raw-row spill path which can't be triggered cooperatively.
//
// Implements memory.Spillable.
func (h *HashAggregate) SpillFootprint() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.Spill == nil {
		return 0
	}
	if !h.canUseExternalMerge() {
		return 0
	}
	// trackedGroupMem reflects what we've reported to the SpillManager's
	// tracker. Use it directly so RequestRelief's "largest contributor"
	// arithmetic matches the tracker's view rather than our internal
	// estimate.
	return h.trackedGroupMem
}

// SpillSome drains the current SoA hash state to a partial-state spill file
// and releases the freed bytes back to the tracker, returning the number of
// bytes released. Called by SpillManager.RequestRelief on behalf of a peer
// operator under memory pressure.
//
// The target hint is advisory — partial-state spill is whole-hash-table at
// this granularity, so we either free everything reclaimable or nothing.
//
// Implements memory.Spillable.
func (h *HashAggregate) SpillSome(target int64) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.Spill == nil || !h.canUseExternalMerge() {
		return 0, nil
	}
	before := h.trackedGroupMem
	if before == 0 {
		return 0, nil
	}
	if err := h.spillPartialState(); err != nil {
		return 0, err
	}
	// trackedGroupMem is reset to 0 by resetGroupStateAfterSpill (called
	// from spillPartialState). before - 0 = before bytes freed.
	return before - h.trackedGroupMem, nil
}

// Next returns the aggregated results in batches of DefaultBatchSize rows.
func (h *HashAggregate) Next(_ context.Context) (*batch.RecordBatch, error) {
	// Streaming partial-merge path: when finalizeViaPartialMerge stored a
	// merger, drain it incrementally instead of walking strGroupStates.
	// This bounds peak Next() memory to ~one batch + merger heap, which is
	// what makes "output groups >> memory" tractable.
	if h.partialMerger != nil {
		return h.nextFromPartialMerger()
	}

	// SoA-direct read: when the SoA fast path is still active (intFlatAccs
	// non-nil), Next() reads accumulators straight from the flat arrays via
	// loadAccFromFlat + writeAccToColumn instead of calling
	// materializeFlatAccums to copy them per-group. Skipping that materialize
	// saves 96 B (extras) + 24 B (accs slice header) + nAggs × ~80 B
	// (Accumulator) of fresh heap per group, which on Q17 SF100 (20M groups
	// × 3 aggs) is the difference between ~3 GB and ~0.5 GB of group-state
	// heap inside Next.
	//
	// materializeFlatAccums is still called on the migration paths
	// (compact→generic, dual-int→generic, MergeSink generic merge) where
	// per-group accs are required to run kernel.Accumulator.Merge. After
	// those run, h.intFlatAccs == nil and the per-group ext.accs branch
	// below picks up the materialized values. The spill/finalize-via-merge
	// paths now drain SoA-direct via partialGroupCursor, so they do not
	// trigger materialize.

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

	// Scalar aggregate with no input: Consume was never called so isScalarAgg
	// was never set, but we still need to emit a single row with identity values.
	// Standard SQL: COUNT over empty → 0; SUM/AVG/MIN/MAX over empty → NULL.
	// This happens when all input batches were filtered out before reaching the
	// aggregate.
	if len(h.GroupByCols) == 0 && len(h.Aggs) > 0 && h.outputPos == 0 && len(h.keys) == 0 &&
		!h.useIntGroupKey && !h.useDualIntGroupKey && !h.useCompactGroupKey {
		h.outputPos = 1
		schema := h.outputSchema()
		out := batch.NewRecordBatch(schema, 1)
		for j, agg := range h.Aggs {
			if agg.Func == AggCount {
				out.Columns[j].SetValue(0, int64(0))
			} else {
				out.Columns[j].Nulls.SetNull(0)
			}
		}
		return out, nil
	}

	totalGroups := len(h.keys)
	if h.useIntGroupKey || h.useDualIntGroupKey || h.useCompactGroupKey {
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
		if h.useIntGroupKey || h.useDualIntGroupKey || h.useCompactGroupKey {
			gs = h.intGroupStates[start+i]
		} else {
			gs = h.strGroupStates[start+i]
		}
		// gs.extras is nil on the SoA hot path (consumeBatchIntGroup /
		// consumeBatchDualIntGroup defer boxing) — those reads go through
		// the SoA-direct branch below. Complex-agg / compact / generic
		// paths populate extras during consume or processRow.
		ext := gs.extras

		// Set group-by columns: use intKey directly for int-keyed groups
		// to avoid deferred []any boxing. For other paths, use keyValues.
		deferredBoxing := ext == nil || ext.keyValues == nil
		if h.useIntGroupKey && deferredBoxing {
			out.Columns[0].SetValue(i, gs.intKey)
		} else if h.useDualIntGroupKey && deferredBoxing {
			idx := start + i
			out.Columns[0].SetValue(i, h.dualIntKeysA[idx])
			out.Columns[1].SetValue(i, h.dualIntKeysB[idx])
		} else {
			for j, val := range ext.keyValues {
				out.Columns[j].SetValue(i, val)
			}
		}

		// Set aggregate columns
		for j, agg := range h.Aggs {
			colIdx := len(h.GroupByCols) + j
			switch agg.Func {
			case AggCountDistinct:
				out.Columns[colIdx].SetValue(i, int64(len(ext.distinctSets[j])))
			case AggStringAgg:
				state := ext.extraState[j].(*stringAggState)
				if len(state.parts) == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, strings.Join(state.parts, state.sep))
				}
			case AggBoolAnd, AggBoolOr:
				out.Columns[colIdx].SetValue(i, ext.extraState[j].(bool))
			case AggStddev:
				state := ext.extraState[j].(*varianceState)
				if state.count < 2 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, math.Sqrt(state.varianceSamp()))
				}
			case AggVariance:
				state := ext.extraState[j].(*varianceState)
				if state.count < 2 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.varianceSamp())
				}
			case AggStddevPop:
				state := ext.extraState[j].(*varianceState)
				if state.count == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, math.Sqrt(state.variancePop()))
				}
			case AggVarPop:
				state := ext.extraState[j].(*varianceState)
				if state.count == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.variancePop())
				}
			case AggApproxDistinct:
				out.Columns[colIdx].SetValue(i, int64(len(ext.distinctSets[j])))
			case AggCorr:
				state := ext.extraState[j].(*covarianceState)
				if state.count < 2 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.correlation())
				}
			case AggCovarSamp:
				state := ext.extraState[j].(*covarianceState)
				if state.count < 2 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.covarSamp())
				}
			case AggCovarPop:
				state := ext.extraState[j].(*covarianceState)
				if state.count == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.covarPop())
				}
			case AggPercentileCont:
				state := ext.extraState[j].(*collectState)
				out.Columns[colIdx].SetValue(i, computePercentileCont(state.values, agg.Percentile))
			case AggPercentileDisc:
				state := ext.extraState[j].(*collectState)
				out.Columns[colIdx].SetValue(i, computePercentileDisc(state.values, agg.Percentile))
			case AggMedian:
				state := ext.extraState[j].(*collectState)
				out.Columns[colIdx].SetValue(i, computePercentileCont(state.values, 0.5))
			case AggMode:
				state := ext.extraState[j].(*collectState)
				out.Columns[colIdx].SetValue(i, computeMode(state.values))
			case AggMinBy, AggMaxBy:
				state := ext.extraState[j].(*minMaxByState)
				if !state.hasValue {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, state.bestVal)
				}
			default:
				// Two paths: the SoA fast path reads directly from
				// intFlatAccs (no per-group accs allocation); the
				// post-materialize / post-merge generic path reads from
				// ext.accs which materializeFlatAccums has populated. We
				// pick by looking at gs.extras and h.intFlatAccs together so
				// that null-key fallback groups (which processRow filled in
				// ext.accs while their intFlatAccs slot stayed at zero) are
				// served from ext.accs even when SoA is otherwise active.
				//
				// writeAccToColumn dispatches the finalized value into the
				// typed Vector slot directly, skipping the kernel's `any`
				// return type and Vector.SetValue's type switch — saves one
				// box per (row × simple-agg-col) at SF100 scale.
				if ext != nil && ext.accs != nil {
					writeAccToColumn(out.Columns[colIdx], i, &ext.accs[j], agg.Func)
				} else {
					// SoA flat-arrays path: synthesize a stack-only Accumulator
					// from intFlatAccs at index start+i, then dispatch. The
					// Accumulator value never escapes (writeAccToColumn doesn't
					// retain it), so this is alloc-free.
					var acc kernel.Accumulator
					loadAccFromFlat(&h.intFlatAccs[j], start+i, &acc)
					writeAccToColumn(out.Columns[colIdx], i, &acc, agg.Func)
				}
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
		// Drop SoA arrays now that the SoA-direct path has finished reading
		// from them. materializeFlatAccums used to do this implicitly (it
		// nil'd intFlatAccs on the way through); since Next() no longer calls
		// materialize on the SoA hot path, we must release these explicitly.
		h.intFlatAccs = nil
		h.intGroupStates = nil
		h.intGroupIndex = nil
		h.dualIntKeysA = nil
		h.dualIntKeysB = nil
		h.dualIntNextGroup = nil
	}
	return out, nil
}

func (h *HashAggregate) outputSchema() []parquet.Column {
	cols := make([]parquet.Column, 0, len(h.GroupByCols)+len(h.Aggs)+len(h.NullGroupCols))

	// Pre-compute output names: strip table qualifiers unless stripping would
	// create duplicate column names (e.g., GROUP BY n1.n_name, n2.n_name must
	// keep qualifiers so downstream projections can distinguish them).
	outNames := make([]string, len(h.GroupByCols))
	baseCounts := make(map[string]int, len(h.GroupByCols))
	for i, name := range h.GroupByCols {
		base := name
		if dot := strings.IndexByte(name, '.'); dot >= 0 {
			base = name[dot+1:]
		}
		outNames[i] = base
		baseCounts[base]++
	}
	for i, name := range h.GroupByCols {
		if baseCounts[outNames[i]] > 1 {
			outNames[i] = name // keep qualified to avoid ambiguity
		}
	}

	for i, name := range outNames {
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
		GroupingSets:  h.GroupingSets,
		// No spill manager — partial aggregates are small enough
	}
	return clone
}

// MergeSink merges another HashAggregate's partial state into this one.
// Called after all parallel workers finish to combine partial aggregates.
func (h *HashAggregate) MergeSink(other SinkSource) {
	o := other.(*HashAggregate)

	// When the parent (h) was never fed a batch — runParallel's warmup batch
	// gets consumed when present, but if the warmup row group is fully
	// filtered out (e.g. shipdate range outside the data window) then
	// resolveIndices was never called on h. That leaves h.groupColTypes
	// empty, which forces outputSchema to fall back to TypeString for every
	// GROUP BY column. The string-typed output column then stores group
	// keys (int64 from gs.keyValues) as their decimal string form, and the
	// downstream HAVING / projection comparison against an int literal
	// silently produces zero matches even though the merged accumulator
	// had the right value. Inherit the worker's resolved schema metadata
	// on the first non-empty merge so the parent's output schema matches
	// the workers'.
	if len(h.groupColTypes) == 0 && len(o.groupColTypes) > 0 {
		h.groupColTypes = o.groupColTypes
		if len(h.groupColIdx) == 0 {
			h.groupColIdx = o.groupColIdx
		}
		if len(h.aggColIdx) == 0 {
			h.aggColIdx = o.aggColIdx
		}
		if len(h.aggColIdx2) == 0 {
			h.aggColIdx2 = o.aggColIdx2
		}
	}

	// Scalar aggregate fast path: merge batch accumulators directly.
	// The parent (h) is created by CloneSink and never consumes a batch
	// itself, so isScalarAgg / scalarAccs / batchAggKernels stay zero on h
	// even when the workers (o) all resolved as scalar. Adopt the worker's
	// scalar wiring on the first scalar merge so Next() takes the scalar
	// finalization path instead of falling through to the empty-input
	// "emit zeros" fallback.
	if o.isScalarAgg {
		if !h.isScalarAgg {
			h.isScalarAgg = true
			h.scalarAccs = make([]kernel.Accumulator, len(o.scalarAccs))
			h.batchAggKernels = o.batchAggKernels
			h.aggColIdx = o.aggColIdx
		}
		for i := range h.scalarAccs {
			h.scalarAccs[i].Merge(&o.scalarAccs[i])
		}
		return
	}

	// Int-keyed SoA fast path: merge flat accumulators directly without
	// materializing per-group Accumulator structs or migrating to generic map.
	if h.useIntGroupKey && o.useIntGroupKey && h.intFlatAccs != nil && o.intFlatAccs != nil {
		h.mergeIntGroupSoA(o)
		return
	}

	// Dual-int-keyed SoA fast path: merge with chain verification.
	if h.useDualIntGroupKey && o.useDualIntGroupKey && h.intFlatAccs != nil && o.intFlatAccs != nil {
		h.mergeDualIntGroupSoA(o)
		return
	}

	// Normalize both sides to the generic map path so merge is uniform.
	h.migrateToGenericMap()
	o.migrateToGenericMap()

	// migrateToGenericMap → materializeFlatAccums on both sides, so every
	// group's extras is allocated and extras.accs is populated. Bind locals.
	for i := range o.keys {
		key := o.serializedKeys[i]
		oGS := o.strGroupStates[i]
		oExt := oGS.extras

		newIdx := int32(len(h.strGroupStates))
		gsIdx, found := h.strGroupIndex.GetOrInsert([]byte(key), newIdx)
		if found {
			gs := h.strGroupStates[gsIdx]
			ext := gs.extras
			for j := range ext.accs {
				ext.accs[j].Merge(&oExt.accs[j])
			}
			// COUNT(DISTINCT) state lives in distinctSets, not accs. Without
			// this merge, parallel workers' partial distinct sets aren't
			// combined and COUNT(DISTINCT) under-counts whenever a group is
			// split across workers (test: Q16 missing the cnt=6 row at
			// position 2 because half the suppliers were on a different
			// worker than the other half).
			for j := range ext.distinctSets {
				if oExt.distinctSets == nil || j >= len(oExt.distinctSets) || oExt.distinctSets[j] == nil {
					continue
				}
				if ext.distinctSets[j] == nil {
					ext.distinctSets[j] = make(map[string]struct{}, len(oExt.distinctSets[j]))
				}
				for k := range oExt.distinctSets[j] {
					ext.distinctSets[j][k] = struct{}{}
				}
			}
		} else {
			h.strGroupStates = append(h.strGroupStates, oGS)
			h.keys = append(h.keys, oExt.keyValues)
			h.serializedKeys = append(h.serializedKeys, key)
		}
	}
}

// mergeIntGroupSoA merges another int-keyed SoA aggregate directly, avoiding
// materializeFlatAccums + migrateToGenericMap + per-group Accumulator.Merge.
// Operates on flat arrays (count, sumI64, sumF64, min, max) with int hash lookup.
func (h *HashAggregate) mergeIntGroupSoA(o *HashAggregate) {
	for i, oGS := range o.intGroupStates {
		newIdx := int32(len(h.intGroupStates))
		gsIdx, found := h.intGroupIndex.GetOrInsert(oGS.intKey, newIdx)
		if found {
			// Existing group: merge flat accumulators in-place
			for ai := range h.intFlatAccs {
				hfa := &h.intFlatAccs[ai]
				ofa := &o.intFlatAccs[ai]
				idx := int(gsIdx)
				hfa.count[idx] += ofa.count[i]
				if hfa.sumI64 != nil {
					hfa.sumI64[idx] += ofa.sumI64[i]
				}
				if hfa.sumF64 != nil {
					hfa.sumF64[idx] += ofa.sumF64[i]
				}
				if hfa.sumDec != nil {
					hfa.sumDec[idx] = hfa.sumDec[idx].Add(ofa.sumDec[i])
				}
				if ofa.hasMin != nil && ofa.hasMin[i] {
					if hfa.hasMin[idx] {
						if hfa.isFloat {
							if ofa.minF64[i] < hfa.minF64[idx] {
								hfa.minF64[idx] = ofa.minF64[i]
							}
						} else if hfa.isDecimal {
							if ofa.minDec[i].Less(hfa.minDec[idx]) {
								hfa.minDec[idx] = ofa.minDec[i]
							}
						} else {
							if ofa.minI64[i] < hfa.minI64[idx] {
								hfa.minI64[idx] = ofa.minI64[i]
							}
						}
					} else {
						hfa.hasMin[idx] = true
						if hfa.minI64 != nil {
							hfa.minI64[idx] = ofa.minI64[i]
						}
						if hfa.minF64 != nil {
							hfa.minF64[idx] = ofa.minF64[i]
						}
						if hfa.minDec != nil {
							hfa.minDec[idx] = ofa.minDec[i]
						}
					}
				}
				if ofa.hasMax != nil && ofa.hasMax[i] {
					if hfa.hasMax[idx] {
						if hfa.isFloat {
							if ofa.maxF64[i] > hfa.maxF64[idx] {
								hfa.maxF64[idx] = ofa.maxF64[i]
							}
						} else if hfa.isDecimal {
							if ofa.maxDec[i].Less(hfa.maxDec[idx]) {
								// other is less, keep ours
							} else {
								hfa.maxDec[idx] = ofa.maxDec[i]
							}
						} else {
							if ofa.maxI64[i] > hfa.maxI64[idx] {
								hfa.maxI64[idx] = ofa.maxI64[i]
							}
						}
					} else {
						hfa.hasMax[idx] = true
						if hfa.maxI64 != nil {
							hfa.maxI64[idx] = ofa.maxI64[i]
						}
						if hfa.maxF64 != nil {
							hfa.maxF64[idx] = ofa.maxF64[i]
						}
						if hfa.maxDec != nil {
							hfa.maxDec[idx] = ofa.maxDec[i]
						}
					}
				}
			}
		} else {
			// New group: append to all flat arrays (already inserted by GetOrInsert)
			h.intGroupStates = append(h.intGroupStates, oGS)
			for ai := range h.intFlatAccs {
				hfa := &h.intFlatAccs[ai]
				ofa := &o.intFlatAccs[ai]
				hfa.count = append(hfa.count, ofa.count[i])
				if hfa.sumI64 != nil {
					hfa.sumI64 = append(hfa.sumI64, ofa.sumI64[i])
				}
				if hfa.sumF64 != nil {
					hfa.sumF64 = append(hfa.sumF64, ofa.sumF64[i])
				}
				if hfa.sumDec != nil {
					hfa.sumDec = append(hfa.sumDec, ofa.sumDec[i])
				}
				if hfa.minI64 != nil {
					hfa.minI64 = append(hfa.minI64, ofa.minI64[i])
				}
				if hfa.maxI64 != nil {
					hfa.maxI64 = append(hfa.maxI64, ofa.maxI64[i])
				}
				if hfa.minF64 != nil {
					hfa.minF64 = append(hfa.minF64, ofa.minF64[i])
				}
				if hfa.maxF64 != nil {
					hfa.maxF64 = append(hfa.maxF64, ofa.maxF64[i])
				}
				if hfa.minDec != nil {
					hfa.minDec = append(hfa.minDec, ofa.minDec[i])
				}
				if hfa.maxDec != nil {
					hfa.maxDec = append(hfa.maxDec, ofa.maxDec[i])
				}
				if hfa.hasMin != nil {
					hfa.hasMin = append(hfa.hasMin, ofa.hasMin[i])
				}
				if hfa.hasMax != nil {
					hfa.hasMax = append(hfa.hasMax, ofa.hasMax[i])
				}
			}
		}
	}
}

// mergeDualIntGroupSoA merges another dual-int-keyed SoA aggregate directly,
// using composite hash lookup with chain verification for collision handling.
func (h *HashAggregate) mergeDualIntGroupSoA(o *HashAggregate) {
	for i := range o.intGroupStates {
		a := o.dualIntKeysA[i]
		b := o.dualIntKeysB[i]

		// Look up in h using chain verification
		var gsIdx int32 = -1
		ck := dualIntHash(a, b)
		if head, ok := h.intGroupIndex.Get(ck); ok {
			for gi := head; gi >= 0; gi = h.dualIntNextGroup[gi] {
				if h.dualIntKeysA[gi] == a && h.dualIntKeysB[gi] == b {
					gsIdx = gi
					break
				}
			}
		}

		if gsIdx >= 0 {
			// Existing group: merge flat accumulators
			for ai := range h.intFlatAccs {
				hfa := &h.intFlatAccs[ai]
				ofa := &o.intFlatAccs[ai]
				idx := int(gsIdx)
				hfa.count[idx] += ofa.count[i]
				if hfa.sumI64 != nil {
					hfa.sumI64[idx] += ofa.sumI64[i]
				}
				if hfa.sumF64 != nil {
					hfa.sumF64[idx] += ofa.sumF64[i]
				}
				if hfa.sumDec != nil {
					hfa.sumDec[idx] = hfa.sumDec[idx].Add(ofa.sumDec[i])
				}
				if ofa.hasMin != nil && ofa.hasMin[i] {
					if hfa.hasMin[idx] {
						if hfa.isFloat {
							if ofa.minF64[i] < hfa.minF64[idx] {
								hfa.minF64[idx] = ofa.minF64[i]
							}
						} else if hfa.isDecimal {
							if ofa.minDec[i].Less(hfa.minDec[idx]) {
								hfa.minDec[idx] = ofa.minDec[i]
							}
						} else {
							if ofa.minI64[i] < hfa.minI64[idx] {
								hfa.minI64[idx] = ofa.minI64[i]
							}
						}
					} else {
						hfa.hasMin[idx] = true
						if hfa.minI64 != nil {
							hfa.minI64[idx] = ofa.minI64[i]
						}
						if hfa.minF64 != nil {
							hfa.minF64[idx] = ofa.minF64[i]
						}
						if hfa.minDec != nil {
							hfa.minDec[idx] = ofa.minDec[i]
						}
					}
				}
				if ofa.hasMax != nil && ofa.hasMax[i] {
					if hfa.hasMax[idx] {
						if hfa.isFloat {
							if ofa.maxF64[i] > hfa.maxF64[idx] {
								hfa.maxF64[idx] = ofa.maxF64[i]
							}
						} else if hfa.isDecimal {
							if !ofa.maxDec[i].Less(hfa.maxDec[idx]) {
								hfa.maxDec[idx] = ofa.maxDec[i]
							}
						} else {
							if ofa.maxI64[i] > hfa.maxI64[idx] {
								hfa.maxI64[idx] = ofa.maxI64[i]
							}
						}
					} else {
						hfa.hasMax[idx] = true
						if hfa.maxI64 != nil {
							hfa.maxI64[idx] = ofa.maxI64[i]
						}
						if hfa.maxF64 != nil {
							hfa.maxF64[idx] = ofa.maxF64[i]
						}
						if hfa.maxDec != nil {
							hfa.maxDec[idx] = ofa.maxDec[i]
						}
					}
				}
			}
		} else {
			// New group
			newIdx := int32(len(h.intGroupStates))
			h.intGroupStates = append(h.intGroupStates, o.intGroupStates[i])
			h.dualIntKeysA = append(h.dualIntKeysA, a)
			h.dualIntKeysB = append(h.dualIntKeysB, b)
			// Chain to existing head if composite hash exists
			if head, ok := h.intGroupIndex.Get(ck); ok {
				h.dualIntNextGroup = append(h.dualIntNextGroup, head)
			} else {
				h.dualIntNextGroup = append(h.dualIntNextGroup, -1)
			}
			h.intGroupIndex.Put(ck, newIdx)
			for ai := range h.intFlatAccs {
				hfa := &h.intFlatAccs[ai]
				ofa := &o.intFlatAccs[ai]
				hfa.count = append(hfa.count, ofa.count[i])
				if hfa.sumI64 != nil {
					hfa.sumI64 = append(hfa.sumI64, ofa.sumI64[i])
				}
				if hfa.sumF64 != nil {
					hfa.sumF64 = append(hfa.sumF64, ofa.sumF64[i])
				}
				if hfa.sumDec != nil {
					hfa.sumDec = append(hfa.sumDec, ofa.sumDec[i])
				}
				if hfa.minI64 != nil {
					hfa.minI64 = append(hfa.minI64, ofa.minI64[i])
				}
				if hfa.maxI64 != nil {
					hfa.maxI64 = append(hfa.maxI64, ofa.maxI64[i])
				}
				if hfa.minF64 != nil {
					hfa.minF64 = append(hfa.minF64, ofa.minF64[i])
				}
				if hfa.maxF64 != nil {
					hfa.maxF64 = append(hfa.maxF64, ofa.maxF64[i])
				}
				if hfa.minDec != nil {
					hfa.minDec = append(hfa.minDec, ofa.minDec[i])
				}
				if hfa.maxDec != nil {
					hfa.maxDec = append(hfa.maxDec, ofa.maxDec[i])
				}
				if hfa.hasMin != nil {
					hfa.hasMin = append(hfa.hasMin, ofa.hasMin[i])
				}
				if hfa.hasMax != nil {
					hfa.hasMax = append(hfa.hasMax, ofa.hasMax[i])
				}
			}
		}
	}
}

// migrateToGenericMap converts int/compact group key state to the generic
// map[string]*groupState path. No-op if already using the generic path.
func (h *HashAggregate) migrateToGenericMap() {
	// Materialize SoA accumulators before migration needs gs.accs
	h.materializeFlatAccums()
	if h.useCompactGroupKey {
		h.migrateCompactToGeneric()
		return
	}
	if h.useDualIntGroupKey {
		// Migrate dual-int group key → generic path
		h.strGroupIndex = newStrHashTable(len(h.intGroupStates))
		h.strGroupStates = make([]*groupState, 0, len(h.intGroupStates))
		h.serializedKeys = make([]string, 0, len(h.intGroupStates))
		h.keys = make([][]any, 0, len(h.intGroupStates))
		for i, gs := range h.intGroupStates {
			ext := gs.ensureExtras()
			if ext.keyValues == nil {
				ext.keyValues = []any{h.dualIntKeysA[i], h.dualIntKeysB[i]}
			}
			h.keyBuf = h.keyBuf[:0]
			key := serializeKey(h.keyBuf, ext.keyValues)
			h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
			h.strGroupStates = append(h.strGroupStates, gs)
			h.serializedKeys = append(h.serializedKeys, key)
			h.keys = append(h.keys, ext.keyValues)
		}
		h.useDualIntGroupKey = false
		h.intGroupStates = nil
		h.intGroupIndex = nil
		h.dualIntKeysA = nil
		h.dualIntKeysB = nil
		h.dualIntNextGroup = nil
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
		ext := gs.ensureExtras()
		if ext.keyValues == nil {
			ext.keyValues = []any{gs.intKey}
		}
		h.keyBuf = h.keyBuf[:0]
		key := serializeKey(h.keyBuf, ext.keyValues)
		h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, key)
		h.keys = append(h.keys, ext.keyValues)
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
			return func(acc *kernel.Accumulator, _ *batch.Vector, sel []uint32, vecLen int) {
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

// resolveAggUpdaterNoNull returns a row-level updater that skips null checks.
// Used when the aggregate column's vector has no nulls in the current batch.
func resolveAggUpdaterNoNull(fn AggFunc, typ batch.TypeID) kernel.RowAggUpdater {
	switch fn {
	case AggSum, AggAvg:
		return kernel.ResolveRowSumNoNulls(typ)
	case AggCount:
		return kernel.ResolveRowCount(true) // no nulls → every row counts
	case AggMin:
		return kernel.ResolveRowMinNoNulls(typ)
	case AggMax:
		return kernel.ResolveRowMaxNoNulls(typ)
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

// initFlatAccums initializes SoA accumulator arrays for the intGroupKey fast path.
// Called once from resolveIndices when useIntGroupKey is true.
func (h *HashAggregate) initFlatAccums(b *batch.RecordBatch) {
	// Flat accumulator arrays: same sizing principle as the group-state pool
	// above. InputRowHint overshoots for low-cardinality GROUP BY (Q12: 7
	// groups but 50M-row InputRowHint would preAlloc 2M × 8B × nAggs here).
	// Cap at 64K slots; ensureCapacity doubles organically for high-cardinality
	// aggregates.
	nAggs := len(h.Aggs)
	h.intFlatAccs = make([]flatAccumArrays, nAggs)
	const flatInitCap = 64 * 1024
	initCap := 4096
	if h.InputRowHint > int64(initCap)*8 {
		est := int(h.InputRowHint / 8)
		if est > flatInitCap {
			est = flatInitCap
		}
		initCap = est
	}

	for i, agg := range h.Aggs {
		fa := &h.intFlatAccs[i]
		fa.count = make([]int64, 0, initCap)

		ci := h.aggColIdx[i]
		if ci < 0 {
			continue // COUNT(*) only needs count
		}
		typ := b.Columns[ci].Type

		switch agg.Func {
		case AggSum, AggAvg:
			switch typ {
			case batch.TypeFloat64, batch.TypeFloat32:
				fa.sumF64 = make([]float64, 0, initCap)
				fa.isFloat = true
			case batch.TypeDecimal:
				fa.sumDec = make([]batch.Int128, 0, initCap)
				fa.isDecimal = true
				fa.decScale = b.Columns[ci].DecimalData.Scale
			default: // int types
				fa.sumI64 = make([]int64, 0, initCap)
			}
		case AggCount:
			// count[] is all we need
		case AggMin:
			switch typ {
			case batch.TypeFloat64, batch.TypeFloat32:
				fa.minF64 = make([]float64, 0, initCap)
				fa.isFloat = true
			case batch.TypeDecimal:
				fa.minDec = make([]batch.Int128, 0, initCap)
				fa.isDecimal = true
			default:
				fa.minI64 = make([]int64, 0, initCap)
			}
			fa.hasMin = make([]bool, 0, initCap)
		case AggMax:
			switch typ {
			case batch.TypeFloat64, batch.TypeFloat32:
				fa.maxF64 = make([]float64, 0, initCap)
				fa.isFloat = true
			case batch.TypeDecimal:
				fa.maxDec = make([]batch.Int128, 0, initCap)
				fa.isDecimal = true
			default:
				fa.maxI64 = make([]int64, 0, initCap)
			}
			fa.hasMax = make([]bool, 0, initCap)
		}
	}

	h.groupIndexBuf = make([]int32, batch.DefaultBatchSize)
}

// loadAccFromFlat fills dst from a flatAccumArrays row at index gi. The
// SoA-direct counterpart to materializing per-group accs into extras.accs:
// callers build a stack-local Accumulator (which the compiler sees value-
// typed and proves doesn't escape) and pass it to a finalizer that doesn't
// retain it. At SF100 Q17 scale (20M groups) this saves a multi-GB heap
// pass through materializeFlatAccums.
func loadAccFromFlat(fa *flatAccumArrays, gi int, dst *kernel.Accumulator) {
	dst.Count = fa.count[gi]
	dst.IsFloat = fa.isFloat
	dst.IsDecimal = fa.isDecimal
	dst.DecScale = fa.decScale
	if fa.sumI64 != nil {
		dst.SumI64 = fa.sumI64[gi]
	}
	if fa.sumF64 != nil {
		dst.SumF64 = fa.sumF64[gi]
	}
	if fa.sumDec != nil {
		dst.SumDec = fa.sumDec[gi]
	}
	if fa.minI64 != nil {
		dst.MinI64 = fa.minI64[gi]
		dst.HasMin = fa.hasMin[gi]
	}
	if fa.maxI64 != nil {
		dst.MaxI64 = fa.maxI64[gi]
		dst.HasMax = fa.hasMax[gi]
	}
	if fa.minF64 != nil {
		dst.MinF64 = fa.minF64[gi]
		dst.HasMin = fa.hasMin[gi]
	}
	if fa.maxF64 != nil {
		dst.MaxF64 = fa.maxF64[gi]
		dst.HasMax = fa.hasMax[gi]
	}
	if fa.minDec != nil {
		dst.MinDec = fa.minDec[gi]
		dst.HasMin = fa.hasMin[gi]
	}
	if fa.maxDec != nil {
		dst.MaxDec = fa.maxDec[gi]
		dst.HasMax = fa.hasMax[gi]
	}
}

// materializeFlatAccums converts SoA flat arrays back to per-group Accumulator
// structs for output (Next) and merge (MergeSink). Called once after all input
// is consumed. O(groups) — negligible compared to the O(rows) hot loop.
func (h *HashAggregate) materializeFlatAccums() {
	if h.intFlatAccs == nil {
		return
	}
	nAggs := len(h.Aggs)
	// String GROUP BY and generic SoA use strGroupStates with SoA flat accumulators.
	if h.useStrGroupKey || h.useGenericSoA {
		for gi, gs := range h.strGroupStates {
			ext := gs.ensureExtras()
			if ext.accs == nil {
				ext.accs = make([]kernel.Accumulator, nAggs)
			}
			for ai := range h.intFlatAccs {
				fa := &h.intFlatAccs[ai]
				// Defensive: a gi that wasn't appended to the SoA arrays
				// (can happen when compact-to-generic migration runs with
				// no rows consumed, leaving intFlatAccs cap=0 while
				// strGroupStates carries migrated entries) would otherwise
				// index a zero-length array and panic. Leave the
				// accumulator at its zero value so downstream kernels
				// emit identity output rather than crashing the worker.
				if gi >= len(fa.count) {
					continue
				}
				acc := &ext.accs[ai]
				acc.Count = fa.count[gi]
				acc.IsFloat = fa.isFloat
				acc.IsDecimal = fa.isDecimal
				acc.DecScale = fa.decScale
				if fa.sumI64 != nil {
					acc.SumI64 = fa.sumI64[gi]
				}
				if fa.sumF64 != nil {
					acc.SumF64 = fa.sumF64[gi]
				}
				if fa.sumDec != nil {
					acc.SumDec = fa.sumDec[gi]
				}
				if fa.minI64 != nil {
					acc.MinI64 = fa.minI64[gi]
					acc.HasMin = fa.hasMin[gi]
				}
				if fa.maxI64 != nil {
					acc.MaxI64 = fa.maxI64[gi]
					acc.HasMax = fa.hasMax[gi]
				}
				if fa.minF64 != nil {
					acc.MinF64 = fa.minF64[gi]
					acc.HasMin = fa.hasMin[gi]
				}
				if fa.maxF64 != nil {
					acc.MaxF64 = fa.maxF64[gi]
					acc.HasMax = fa.hasMax[gi]
				}
				if fa.minDec != nil {
					acc.MinDec = fa.minDec[gi]
					acc.HasMin = fa.hasMin[gi]
				}
				if fa.maxDec != nil {
					acc.MaxDec = fa.maxDec[gi]
					acc.HasMax = fa.hasMax[gi]
				}
			}
		}
		h.intFlatAccs = nil
		h.groupIndexBuf = nil
		return
	}
	for gi, gs := range h.intGroupStates {
		ext := gs.ensureExtras()
		if ext.accs == nil {
			ext.accs = make([]kernel.Accumulator, nAggs)
		}
		for ai := range h.intFlatAccs {
			fa := &h.intFlatAccs[ai]
			acc := &ext.accs[ai]
			acc.Count = fa.count[gi]
			acc.IsFloat = fa.isFloat
			acc.IsDecimal = fa.isDecimal
			acc.DecScale = fa.decScale
			if fa.sumI64 != nil {
				acc.SumI64 = fa.sumI64[gi]
			}
			if fa.sumF64 != nil {
				acc.SumF64 = fa.sumF64[gi]
			}
			if fa.sumDec != nil {
				acc.SumDec = fa.sumDec[gi]
			}
			if fa.minI64 != nil {
				acc.MinI64 = fa.minI64[gi]
				acc.HasMin = fa.hasMin[gi]
			}
			if fa.maxI64 != nil {
				acc.MaxI64 = fa.maxI64[gi]
				acc.HasMax = fa.hasMax[gi]
			}
			if fa.minF64 != nil {
				acc.MinF64 = fa.minF64[gi]
				acc.HasMin = fa.hasMin[gi]
			}
			if fa.maxF64 != nil {
				acc.MaxF64 = fa.maxF64[gi]
				acc.HasMax = fa.hasMax[gi]
			}
			if fa.minDec != nil {
				acc.MinDec = fa.minDec[gi]
				acc.HasMin = fa.hasMin[gi]
			}
			if fa.maxDec != nil {
				acc.MaxDec = fa.maxDec[gi]
				acc.HasMax = fa.hasMax[gi]
			}
		}
	}
	// Free flat arrays — no longer needed after materialization
	h.intFlatAccs = nil
	h.groupIndexBuf = nil
}

// rebuildFlatAccums re-creates SoA flat accumulator arrays from materialized
// per-group Accumulator structs. Called when intFlatAccs was cleared by
// materializeFlatAccums (during parallel merge) but the fast path is
// re-enabled for processing spilled rows in Finalize.
func (h *HashAggregate) rebuildFlatAccums(b *batch.RecordBatch) {
	h.initFlatAccums(b)

	var groups []*groupState
	if h.useStrGroupKey || h.useGenericSoA {
		groups = h.strGroupStates
	} else {
		groups = h.intGroupStates
	}

	for gi, gs := range groups {
		for ai := range h.intFlatAccs {
			fa := &h.intFlatAccs[ai]
			fa.appendGroup()
			// extras may be nil if rebuild runs before any materialize/Group
			// path allocated them; treat those as the "no accumulators yet"
			// case the original `gs.accs == nil` branch handled.
			if gs.extras == nil || gs.extras.accs == nil || ai >= len(gs.extras.accs) {
				continue
			}
			acc := &gs.extras.accs[ai]
			fa.count[gi] = acc.Count
			if fa.sumI64 != nil {
				fa.sumI64[gi] = acc.SumI64
			}
			if fa.sumF64 != nil {
				fa.sumF64[gi] = acc.SumF64
			}
			if fa.sumDec != nil {
				fa.sumDec[gi] = acc.SumDec
			}
			if fa.minI64 != nil {
				fa.minI64[gi] = acc.MinI64
				fa.hasMin[gi] = acc.HasMin
			}
			if fa.maxI64 != nil {
				fa.maxI64[gi] = acc.MaxI64
				fa.hasMax[gi] = acc.HasMax
			}
			if fa.minF64 != nil {
				fa.minF64[gi] = acc.MinF64
				fa.hasMin[gi] = acc.HasMin
			}
			if fa.maxF64 != nil {
				fa.maxF64[gi] = acc.MaxF64
				fa.hasMax[gi] = acc.HasMax
			}
			if fa.minDec != nil {
				fa.minDec[gi] = acc.MinDec
				fa.hasMin[gi] = acc.HasMin
			}
			if fa.maxDec != nil {
				fa.maxDec[gi] = acc.MaxDec
				fa.hasMax[gi] = acc.HasMax
			}
		}
	}
}

