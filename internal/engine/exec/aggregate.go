package exec

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
	// AggVarState and AggVarStateMerge carry the variance family across a
	// partial/final aggregate split. A finished STDDEV/VARIANCE cannot be
	// re-aggregated — combining partials needs the (count, mean, M2) triple,
	// not the scalar each partial reports — so a distributed plan replaces
	// STDDEV(c) AS x with VAR_STATE(c) AS __var_state#<kind>#x on the partial
	// stage (emits the encoded triple, see varianceState.encode) and
	// VAR_STATE_MERGE on every merge stage above it (pairwise-combines the
	// encoded triples and re-emits one). The final stage's fold
	// (worker/var_fold.go) decodes the triple into the value <kind> asks for.
	AggVarState
	AggVarStateMerge
	// AggCovarState and AggCovarStateMerge do the same for CORR, COVAR_SAMP
	// and COVAR_POP, whose state is the (count, meanX, meanY, C, M2x, M2y)
	// sextuple and which combine by the same pairwise rule
	// (covarianceState.merge). Before #353 the DAG had no case for these
	// function names at all, so they fell to the worker's `default: AggSum`
	// and CORR(o_totalprice, o_custkey) answered 2.127e9 — the sum of its
	// first argument.
	AggCovarState
	AggCovarStateMerge
)

// AggColumn defines an aggregation to perform.
type AggColumn struct {
	Func       AggFunc
	InputCol   string // input column name (empty for COUNT(*))
	OutputCol  string // output column name
	OutputType parquet.TypeID
	// OutputPrecision/OutputScale carry a DECIMAL OutputType's (p,s) — the
	// piece a bare TypeID cannot hold, and the half of a DECIMAL's VALUE that
	// the .wshf header carries (ADR-0010). outputSchema fills them in from the
	// input VECTOR whenever one was observed; these are what it declares when
	// one never was, which is exactly the ungrouped identity row a partial
	// task emits after a selective filter matched none of its rows. Without
	// them that row shipped a file declaring DECIMAL(0,0), and the aggregate
	// merging it read a scaled Int128 as unscaled — 10^scale too large (#685).
	//
	// Zero means "the planner declared no (p,s)", which is every non-DECIMAL
	// aggregate and a DECIMAL one whose input is not a bare column reference.
	OutputPrecision int
	OutputScale     int
	Separator       string  // separator for STRING_AGG (default ',')
	InputCol2       string  // second input column (corr, covar, min_by, max_by)
	Percentile      float64 // percentile value for percentile_cont/percentile_disc
	// InputColIdx pins the input to a physical column POSITION, bypassing
	// name resolution, when InputColIdxSet is true. A distributed merge over
	// two aggregates sharing one alias (#575) reads two partial columns of
	// the SAME name; the name path resolves both to the first, collapsing
	// them, so the worker addresses each by its ordinal partial slot instead.
	InputColIdx    int
	InputColIdxSet bool
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
	GroupByCols []string
	// GroupByOutNames, when set, is the name each group key is PUBLISHED
	// under, parallel to GroupByCols. GroupByCols stays the name the key is
	// RESOLVED by against the input batch, and the two differ for a key the
	// planner had to materialize: that value is computed into a hidden slot
	// (`__gb_expr_N`) so it cannot be shadowed by — or shadow — an input
	// column the query happens to spell the same way, and the slot is renamed
	// to the key's own canonical text on the way out.
	//
	// Naming the slot after the key's text instead is what made
	// `SELECT g + 1 … GROUP BY g + 1` over a table that also has a column
	// called "g + 1" group by the COLUMN: the pre-aggregate projection
	// APPENDS, and batch.RecordBatch.ColumnIndex answers with the first exact
	// match (ADR-0026). Empty means "publish under GroupByCols", which is
	// every bare-column key and every caller that predates the slot.
	GroupByOutNames []string
	// GroupByAll makes the aggregate group by every input column, resolved
	// from the first batch's schema (GroupByCols must be empty). This is how
	// DISTINCT is planned: a keys-only hash aggregate inherits the spill
	// machinery, where the dedicated Distinct operator's seen-set grew
	// without bound or tracking. With no Aggs the output is exactly the
	// distinct key tuples, in input column order.
	GroupByAll bool
	Aggs       []AggColumn
	Spill      *memory.SpillManager // optional: enables spill-to-disk
	// PartialDrainBytes bounds this aggregate's in-memory state when it is a
	// morsel-parallel CLONE partial: past the threshold, Consume drains the
	// whole state to canonical partial-state run files (drainedRuns) that
	// MergeSink hands to the primary for Finalize's k-way merge. Clones run
	// on a tracking-only SpillManager view whose ShouldSpillFor is
	// unconditionally false, so without this bound a high-cardinality GROUP
	// BY multiplies serial state by k with no pressure valve — the SF100 Q17
	// worker deaths (morsel-agg-partials-v2.md §3.A). 0 = disabled (primary
	// aggregates keep their ShouldSpillFor-driven spill machinery).
	PartialDrainBytes int64
	// PartitionedDisjoint marks a sink participating in partitioned
	// parallel aggregation (partitioned_agg.go): every group key lives in
	// exactly one sink. MergeSink then ADOPTS clone partitions instead of
	// re-inserting their groups, and Next() streams each adopted
	// partition's state after its own.
	PartitionedDisjoint bool
	adoptedPartitions   []*HashAggregate
	// routeFallback records that at least one batch could NOT be
	// hash-routed and was consumed whole by whichever worker pulled it
	// (partitionAndDeliver's fallback). That breaks the disjointness
	// PartitionedDisjoint asserts — the same key then lives in several
	// sinks — so the pipeline demotes the merge back to a real key merge
	// before adopting anything. Lives on the shared router instance (the
	// primary aggregate), written from every worker goroutine.
	routeFallback atomic.Bool
	NullGroupCols []string // GROUPING SETS: columns to output as NULL (legacy per-node)
	GroupingSets  [][]int  // single-pass grouping sets: column indices within GroupByCols per set
	InputRowHint  int64    // estimated input rows for pre-sizing hash table
	// GroupNDVHint is the planner's HLL-based estimate of GROUP-KEY
	// cardinality (catalog merged sketches; ~2% error) — the quantity the
	// hash table actually holds, unlike InputRowHint's input-row proxy.
	// 0 = unknown. cloneNDVDivisor spreads the hint across partitioned
	// clones (each owns a disjoint 1/k of the key space).
	GroupNDVHint    int64
	cloneNDVDivisor int

	mu             sync.Mutex
	keys           [][]any
	serializedKeys []string // pre-serialized keys matching h.keys order
	// deferGenericKeyBoxing: on the generic SoA path, skip per-group
	// extras/keyValues/h.keys boxing at consume and reconstruct keys from
	// serializedKeys (a lossless binary encoding) at output/spill time.
	// Set in resolveIndices when every group column's type round-trips
	// through the serialization exactly.
	deferGenericKeyBoxing bool
	// Typed generic lookup (rides the same gate): rows hash from typed
	// column storage and verify against stored serializedKeys via a
	// chained int hash table — serialization happens once per NEW group,
	// not once per row. genKeyIdx maps combined key hash → chain head in
	// genKeyNext (chained-hash pattern). strGroupIndex is NOT maintained on
	// this path; ensureStrGroupIndexForMerge rebuilds it for the slow
	// merge fallback.
	genKeyIdx        *intHashTable
	genKeyNext       []int32
	keySerCols       []keySerCol // per-batch resolved key accessors (scratch)
	groupColIdx      []int
	aggColIdx        []int
	aggColIdx2       []int // second column indices for two-column aggregates
	groupColTypes    []batch.TypeID
	groupColMeta     []parquet.Column // full input column metadata per group col (Decimal Scale/Precision survive into outputSchema)
	aggInputTypes    []batch.TypeID   // observed input column type per aggregate (0 = unresolved)
	aggInputMeta     []parquet.Column // full input column metadata per aggregate (MIN/MAX over a container and MIN_BY/MAX_BY re-emit their input's Scale/Fields/ElementType/Dimension)
	aggInputDecScale []int            // DECIMAL input scale per aggregate, read from the VECTOR (not the schema column) so the declared output scale is the one the Int128 accumulator actually counts in (#455)
	// decScaleConflict is the operator-wide twin of
	// kernel.Accumulator.DecScaleConflict, for the GROUPED paths whose state
	// lives in the flat SoA arrays: those hold one scale per aggregate and no
	// per-group accumulator to carry a flag on, so the latch is here and
	// decAggErr reads it for every group. See Consume, where it is set.
	decScaleConflict  bool
	hasDecAggInput    bool                    // any aggregate reads a DECIMAL column — gates the per-batch scale reconciliation in Consume
	aggBoxedMinMax    []bool                  // per aggregate: MIN/MAX over ARRAY/ROW/MAP/VECTOR, which retains a value instead of filling an Accumulator slot (#426)
	hasBoxedMinMax    bool                    // any aggBoxedMinMax entry is set — one bool so the row and finalize loops pay a single load when none is
	aggUpdaters       []kernel.RowAggUpdater  // resolved typed updaters
	aggUpdatersNoNull []kernel.RowAggUpdater  // no-null-check variants
	batchUpdaters     []kernel.RowAggUpdater  // per-batch updater selection (reusable)
	batchAggKernels   []kernel.BatchAggKernel // batch-level kernels (scalar aggregate fast path)
	scalarAccs        []kernel.Accumulator    // accumulators for scalar aggregate fast path
	isScalarAgg       bool                    // true when len(GroupByCols)==0 and all aggs are batch-able
	aggF64Extract     []float64Extractor      // pre-resolved float64 extractors per agg column (variance, corr, etc.)
	aggF64Extract2    []float64Extractor      // pre-resolved float64 extractors for second column (corr, covar)

	// Single-column integer GROUP BY fast path: uses intHashTable
	// instead of serializing keys to strings and using map[string].
	useIntGroupKey bool
	intGroupIndex  *intHashTable
	// intGroupStates carries per-group AoS state for the int-keyed modes
	// that need it (compact keys, and any mode after materializeFlatAccums
	// reifies). The single-int and packed-key fast paths defer state entirely
	// to the SoA arrays and leave this EMPTY — numIntGroups, not len(), is
	// the group count on every int-keyed path. A []*groupState of pure nils
	// cost 8 GC-scanned bytes per group for nothing (ClickBench Q33: 100M
	// groups).
	intGroupStates []*groupState
	// numIntGroups is the authoritative slot count for the int-keyed modes
	// (single-int, packed, compact). Slots recycled through freeGroupIDs
	// do not bump it. Invariant: len(intGroupStates) is either 0 (deferred)
	// or == numIntGroups (materialized / compact).
	numIntGroups   int
	intGroupKeyCol int // column index for the integer group-by key
	// offheap owns the mmap reservations backing the pointer-free SoA
	// state arrays (flat accumulators, int key SoAs) when the offheap-agg
	// switch is on — see memory/offheap_linux.go for the growth-transient
	// rationale. Created lazily by offheapReg; adopted across merges;
	// unmapped at Close and on whole-state resets.
	offheap *memory.OffheapRegistry
	// retiredOffheap holds registries detached by whole-state resets whose
	// arrays a drain cursor / partial merger may still be reading; unmapped
	// via closeRetiredOffheap once those readers close.
	retiredOffheap []*memory.OffheapRegistry

	// intKeys is the per-group key SoA for the single-int path. With
	// simple aggregates the groupState carries NOTHING this array and the
	// flat accumulators don't (the 24B struct held only the key), so
	// intGroupStates entries stay nil — the same deferral the composite-key
	// path got in 6806c83 (ClickBench Q33: per-group structs were the
	// margin between fitting under GOMEMLIMIT and thrashing).
	// materializeFlatAccums reifies on the migration/merge cold paths.
	intKeys []int64

	// SoA (Struct of Arrays) accumulators for intGroupKey fast path.
	// Stores accumulator fields in contiguous arrays instead of per-group
	// heap objects, reducing working set from ~192MB to ~32MB for 2M groups.
	intFlatAccs []flatAccumArrays // one per aggregate (nil = use AoS path)
	// aggEncodings latches each aggregate's spill-format encoding (IsFloat /
	// IsDecimal / DecScale) so a drain can still write the right one after
	// intFlatAccs has been cleared. See latchAggEncodings.
	aggEncodings  []partialAggSpec
	groupIndexBuf []int32 // reused per-batch for two-phase scatter

	// Packed composite-key GROUP BY fast path (packed_hash.go): 2-4 fixed-width
	// int-class columns whose widths sum to <= 16 bytes are packed into one
	// 128-bit key stored INLINE in the hash entry — one probe, one compare, no
	// chain. Replaces the dual-int path, whose key lived across three SoA
	// arrays (three dependent misses per verify) and whose inserts probed
	// twice. Uses SoA scatter like the single-int path.
	usePackedGroupKey bool
	packedIdx         *packedHashTable
	packedLayout      []packedField  // per group column: word/shift/width in the key
	packedKeys        []packedKey    // composite key per group (16 B, ONE array)
	packedCols        []packedKeyCol // per-batch resolved column accessors (scratch)

	// Multi-column compact GROUP BY fast path: binary-encoded key packed into int64.
	// Uses intHashTable for lookup. Falls back to generic path if key exceeds 8 bytes.
	useCompactGroupKey bool
	compactKeys        []string // serialized binary keys for fallback migration

	// Single-column string GROUP BY fast path: uses strHashTable with SoA scatter.
	// Two-phase approach like consumeBatchIntGroup but with string key hashing.
	useStrGroupKey bool
	strGroupKeyCol int // column index for the string group-by key
	// strNullGroupIdx is the NULL-key group's slot in strGroupStates (-1 =
	// none yet). The NULL group is created inline WITH a flat-accumulator
	// slot: the previous shape diverted null-key rows to processRow, whose
	// groups skip appendGroup — strGroupStates and intFlatAccs went out of
	// alignment and the NULL group emitted with zeroed aggregates
	// (COUNT(*)=0 over 700 rows; issue #144 suite finding). Kept out of
	// strGroupIndex entirely so it can never collide with a real 1-byte
	// string key.
	strNullGroupIdx int32

	// Multi-column generic GROUP BY SoA fast path: binary key serialization
	// with strHashTable lookup and SoA flat accumulator scatter.
	// Used when GROUP BY has multiple columns that don't fit compact/int/str paths
	// but all aggregates are simple (SUM, COUNT, MIN, MAX, AVG).
	useGenericSoA bool

	// String hash table for generic GROUP BY: open-addressing with arena-stored keys.
	// Replaces map[string]*groupState to eliminate GC scanning overhead.
	strGroupIndex  *strHashTable
	strGroupStates []*groupState

	resolved      bool
	needsDistinct bool // true if any agg uses distinctSets
	needsExtra    bool // true if any agg uses extraState
	simpleAggs    bool // true when every Agg fits the kernel.Accumulator shape
	// (SUM, COUNT, MIN, MAX, AVG only — no distinct/extra state).
	// Drives external-merge partial-state spill eligibility.
	keyBuf      []byte
	inputSchema []parquet.Column // schema from first input batch (for spill recovery)
	spillFiles  []string
	// drainedRuns holds partial-state run files written by the clone-partial
	// bound (PartialDrainBytes) that have not yet been handed to a primary
	// via MergeSink. Close deletes any leftovers (error paths where the
	// barrier merge never ran).
	drainedRuns []string
	// partialSpillFiles holds external-merge spill files (sorted partial group
	// state). These are produced when simpleAggs && one of the SoA paths is in
	// use; Finalize k-way merges them. Distinct from spillFiles because the
	// merge logic differs (re-aggregate raw rows vs. merge partial accs).
	partialSpillFiles []string
	// freeGroupIDs holds intGroupStates indices that were drained by partial
	// spill and are available for reuse on the next int-keyed Consume. Each
	// entry pre-points at a groupState chunk allocation in h.gsPool and at a
	// pre-existing slot in every h.intFlatAccs[*] array (zeroed during drain).
	// Reusing these slots avoids the O(survivors) compaction cost a "rebuild
	// every drain" path would impose at SF100+ scale.
	freeGroupIDs []int32
	// drainK is the partition count fixed on the first partial SpillSome
	// call. Stays constant for the aggregate's lifetime so partition
	// assignments (fibHash(intKey) & (drainK-1)) are stable across drains.
	// Zero means "not yet initialized". Adaptive: scales as
	// nextPow2(max(numGroups/1M, 8)) capped at 512, sized so each partition
	// is ~100 MB–1 GB at SF100–SF1000 footprints.
	drainK uint32
	// nextDrainPartition is the round-robin cursor over partition indices in
	// [0, drainK). Each partial SpillSome advances by the number of
	// partitions drained so successive calls chip away at different slices
	// instead of churning the same one and re-rebuilding the same groups.
	nextDrainPartition uint32
	// Drain-productivity accounting (#325). A drain is only worth its I/O
	// when this operator actually owns the bytes under pressure; see
	// aggregate_drain_gate.go for the gate and the non-convergence check
	// these feed.
	//
	// lastDrainFootprint is the MEASURED group-state size (groupMemoryUsage,
	// not the tracked value — see noteDrain) immediately after the previous
	// drain, so the gate can require a floor of NEW state before spending
	// another drain. drainCount/drainNanos/drainFreedBytes are the counters
	// reported in the non-convergence error; firstDrainAt anchors the
	// observation window.
	lastDrainFootprint int64
	drainCount         int
	drainNanos         int64
	drainFreedBytes    int64
	firstDrainAt       time.Time
	// forcedDrainSeq counts Consume calls for the TEST-ONLY deterministic
	// drain knob (aggregate_force_drain.go). Zero and unread on every
	// production path.
	forcedDrainSeq int64
	// AccountedOperator (Phase 2) state. accInstanceID is the process-unique
	// id; accState is the lifecycle (memory.OpState) read/written atomically so
	// Inspect (called off the pipeline goroutine) sees a consistent state.
	// unregisterAccounted deregisters from the new relief registry.
	accInstanceID       uint64
	accState            atomic.Int32
	unregisterAccounted func()
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
	spillBufferBytes int64          // tracker bytes attributable to rows in spillBuffer
	trackedGroupMem  int64          // bytes charged to Spill tracker for group state growth
	outputPos        int            // position in keys for batched Next() output
	gsPool           groupStatePool // chunk allocator for groupState (reduces GC pressure)
	// emitSchema caches outputSchema() for the emission phase. It used to be
	// rebuilt for EVERY output batch (name de-qualification map, per-column
	// metadata copies) — pure waste at 2048 rows per call, and it has to be
	// read-only anyway once the parallel drain has several units emitting at
	// once. emitSchemaSet distinguishes "not computed" from the legitimately
	// empty schema of a zero-column aggregate.
	emitSchema    []parquet.Column
	emitSchemaSet bool
	// emit, when non-nil, is the parallel emit drain that owns the adopted
	// partitions and streams every unit's output concurrently — see
	// aggregate_parallel_emit.go.
	emit *emitDrain

	// Incremental byte counters for per-group state that groupMemoryUsage
	// cannot enumerate in O(1) from caps: string bytes behind serializedKeys
	// and compactKeys, COUNT(DISTINCT) set contents, extraState objects, and
	// per-group accumulator counts. Bumped at the allocation/append sites,
	// zeroed by resetStateByteCounters wherever the backing state is dropped
	// wholesale. These existed as acknowledged under-counts ("accuracy
	// budget", see groupMemoryUsage) until SF100 Q17 (2026-07-03) showed the
	// gap reaching 41-100% of live heap on high-cardinality GROUP BYs — the
	// tracker never crossed the spill threshold while the process died at
	// GOMEMLIMIT (docs/design/morsel-agg-partials-v2.md §1-2).
	// TestGroupMemoryUsageTruth guards these against rot.
	// serializedKeyBytes counts string bytes appended to serializedKeys (+
	// shared by keyValues boxes). The single-string fast path is excluded on
	// purpose: its entries alias the hash table's key arena, whose bytes
	// MemoryUsage() already charges.
	serializedKeyBytes int64
	compactKeyBytes    int64 // string bytes appended to compactKeys
	distinctBytes      int64 // COUNT(DISTINCT) map contents + entry overhead
	extraStateBytes    int64 // extraState objects (alloc-time estimates)
	extrasAccsCount    int64 // total kernel.Accumulator elements behind extras.accs

	// Hash-once routing state (partitioned_agg.go). routeOnce/routePlanV are
	// the ROUTER's: read concurrently by every worker off the shared primary
	// aggregate, so the plan is frozen behind a sync.Once and never re-derived
	// (a mid-query hash change would split a live group across two owners).
	// provHashes/provPlan are the SINK's: the current Consume's pre-computed
	// per-row key hashes, set by ConsumeHashed. Each partitioned sink is
	// touched by exactly one goroutine at a time, which is what lets them be
	// plain fields.
	//
	// Deliberately LAST in the struct: inserting them mid-struct shifts the
	// offset of every hot consume-loop field after them, which measured on its
	// own as a regression on BenchmarkHashAggregatePackedNearUnique with the
	// loop bodies byte-identical to baseline.
	routeOnce  sync.Once
	routePlanV *routePlan
	provHashes []uint64
	provPlan   *routePlan

	// Two-level group index (two_level_hash.go). EXACTLY ONE of
	// intGroupIndex/intTwoLevel is non-nil on the single-int path, and one of
	// packedIdx/packedTwoLevel on the packed path: the sink starts flat and
	// converts once past twoLevelConvertAt, or constructs bucketed straight
	// away when the NDV hint already exceeds it. Both are read ONCE per batch
	// (hoisted out of the row loop), never per row.
	//
	// Appended at the very END of the struct for the same reason the routing
	// fields were: adding a field mid-struct shifts every later hot
	// consume-loop field's offset, which measured on its own.
	intTwoLevel    *intTwoLevelTable
	packedTwoLevel *packedTwoLevelTable

	// Bounded-sink layout decision (two_level_hash.go,
	// twoLevelBoundedMinGroups). Also at the END of the struct, for the
	// reason above.
	//
	// epochByteCap > 0 marks this a BOUNDED sink: its owner finalizes it and
	// builds a fresh one whenever StateBytes() crosses the cap
	// (worker.cappedPartialAgg's 128 MB epochs). The bound is what lets
	// resolveIndices decide the group-index LAYOUT once, at construction,
	// instead of betting on a runtime conversion an epoch-scoped table can
	// never amortize. Set with SetEpochByteCap BEFORE Init.
	epochByteCap int64
	// inputRowBound > 0 is an EXACT upper bound on the rows this aggregate
	// will consume, declared by an owner that knows it — the coordinator's
	// per-partition row accounting for a DAG aggregate task. An index that
	// will see fewer than twoLevelMinAmortizeRows() rows in total has
	// nothing to repay a conversion with (two_level_hash.go,
	// twoLevelAmortizeMultiple). Set with SetInputRowBound BEFORE Init.
	// 0 = unknown, which leaves the adaptive path untouched.
	inputRowBound int64
	// indexBornFlat is that decision, taken in resolveIndices and read by
	// the NDV-hint direct-build branches and every conversion gate.
	// groupCeiling is the Gmax the epoch cap allowed (0 = no cap), kept for
	// the worker's log line, and indexFlatWhy names which bound pinned the
	// layout. indexFlatWhy is a byte, not a string: it packs beside
	// indexBornFlat instead of adding 16 bytes to a struct whose tail
	// offsets the hot consume loop is measurably sensitive to.
	indexBornFlat bool
	indexFlatWhy  indexFlatReason
	groupCeiling  int64
	// indexConversions counts THIS aggregate's flat→bucketed conversions.
	// The package-level TwoLevelConversions is worker-wide and cannot be
	// attributed to one operator when tasks run concurrently.
	indexConversions int
}

// SetEpochByteCap declares that this aggregate's owner will finalize it and
// build a fresh one whenever StateBytes() crosses cap — a BOUNDED sink. Call
// it BEFORE Init: the layout of the group index is decided from this bound
// (two_level_hash.go, twoLevelBoundedMinGroups) and a sink that learns its
// cap after the first batch has already chosen.
func (h *HashAggregate) SetEpochByteCap(cap int64) { h.epochByteCap = cap }

// SetInputRowBound declares an EXACT upper bound on the number of rows this
// aggregate will consume. Call it BEFORE Init, for the same reason
// SetEpochByteCap must be: the group-index layout is decided from it once,
// and a sink that learns its bound after the first batch has already chosen.
//
// "Exact" is load-bearing. The bound may over-state (a clone reads a subset
// of its parent's rows and inherits the parent's bound), because over-stating
// only keeps the adaptive path. It must never under-state: a bound below the
// truth pins a genuinely high-cardinality index flat. That is why estimates —
// InputRowHint, GroupNDVHint — are not routed here.
func (h *HashAggregate) SetInputRowBound(rows int64) { h.inputRowBound = rows }

// InputRowBound reports the declared bound (0 = none).
func (h *HashAggregate) InputRowBound() int64 { return h.inputRowBound }

// IndexFlatReason names the bound that pinned this aggregate's group index
// flat at construction: "epoch-cap", "row-bound", or "" when nothing did.
func (h *HashAggregate) IndexFlatReason() string { return h.indexFlatWhy.String() }

// IndexConversions reports how many flat→bucketed group-index conversions
// this aggregate paid; IndexBornFlat reports whether its layout was pinned
// flat at construction, and GroupCeiling the Gmax that pinned it (0 =
// unbounded sink). Read by the worker for its per-task log lines.
func (h *HashAggregate) IndexConversions() int { return h.indexConversions }

// IndexBornFlat reports whether the bounded-sink rule pinned this
// aggregate's group index flat — see SetEpochByteCap.
func (h *HashAggregate) IndexBornFlat() bool { return h.indexBornFlat }

// GroupCeiling reports the maximum group count this sink's epoch cap allows
// (0 when the sink is unbounded).
func (h *HashAggregate) GroupCeiling() int64 { return h.groupCeiling }

// kernelAccumulatorBytes is the per-element cost of extras.accs slices,
// resolved once so groupMemoryUsage stays arithmetic-only.
var kernelAccumulatorBytes = int64(unsafe.Sizeof(kernel.Accumulator{}))

// resetStateByteCounters zeroes the incremental per-group byte counters.
// Call wherever the backing state (keys/serializedKeys/compactKeys/extras)
// is dropped wholesale — spillFullState, migrate rebuilds, Close.
func (h *HashAggregate) resetStateByteCounters() {
	h.serializedKeyBytes = 0
	h.compactKeyBytes = 0
	h.distinctBytes = 0
	h.extraStateBytes = 0
	h.extrasAccsCount = 0
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
	// Pointer first so the GC pointer-scan region is 8 B, not 24 B. This struct
	// is one-per-group, allocated in contiguous []groupState chunks (millions of
	// groups at SF100), so shrinking the scanned prefix cuts GC mark work per
	// cycle. Size is unchanged at 24 B (extras 8 + intKey 8 + setID 4 + 4 tail pad).
	extras *groupStateExtras
	intKey int64 // single int64 key for int-keyed groups (avoids []any boxing)
	setID  int32 // grouping set index (-1 = not a grouping set)
}

// groupStateExtras holds the heavy per-group fields that are only needed by
// complex-agg paths or by the generic str-keyed path. Allocated lazily by
// (*groupState).ensureExtras() and pointed at from the slim base. The SoA
// simple-aggs path (consumeBatchIntGroup, consumeBatchPackedGroup) never
// touches this allocation, so the typical Q17/Q01 pattern leaves it nil.
type groupStateExtras struct {
	keyValues    []any
	accs         []kernel.Accumulator
	distinctSets []*distinctSet // per-agg distinct value sets (nil if not COUNT(DISTINCT)); typed int set for int-class columns
	extraState   []any          // per-agg custom state (string_agg builder, variance state, etc.)
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
	if h.intTwoLevel != nil {
		size += h.intTwoLevel.MemoryUsage()
	}
	if h.packedIdx != nil {
		size += h.packedIdx.MemoryUsage()
	}
	if h.packedTwoLevel != nil {
		size += h.packedTwoLevel.MemoryUsage()
	}
	if h.strGroupIndex != nil {
		size += h.strGroupIndex.MemoryUsage()
	}
	if h.genKeyIdx != nil {
		size += h.genKeyIdx.MemoryUsage()
	}
	size += int64(cap(h.genKeyNext)) * 4
	// Group state pool: each chunk is a contiguous array of slim groupState
	// structs (24 bytes each: intKey + setID + extras pointer). The heavy
	// per-group state behind the extras pointer is accounted below via the
	// incremental counters + the per-generic-group constant — the former
	// "accuracy budget" (slice contents untracked) let the tracker
	// under-report by 41-100% on high-cardinality non-int-SoA GROUP BYs,
	// which is how SF100 Q17 died at GOMEMLIMIT with the spill threshold
	// never crossed (2026-07-03 postmortem).
	for _, chunk := range h.gsPool.chunks {
		size += int64(cap(chunk)) * 24
	}
	// Generic/str-path per-group state. Every group on those paths appends
	// to h.keys, so len(h.keys) counts them: each carries a groupStateExtras
	// (96 B of slice headers) plus a keyValues []any backing array (16 B
	// interface slot + ~8 B boxed scalar per group column; boxed strings
	// share the bytes counted in serializedKeyBytes).
	if n := int64(len(h.keys)); n > 0 {
		size += n * (96 + int64(len(h.GroupByCols))*24)
	}
	// Slice backing arrays for the key mirrors, plus the string bytes and
	// per-category contents tracked incrementally at their append sites.
	size += int64(cap(h.keys)) * 24 // [][]any: 24 B slice header per element
	size += int64(cap(h.serializedKeys)) * 16
	size += int64(cap(h.compactKeys)) * 16
	size += h.serializedKeyBytes
	size += h.compactKeyBytes
	size += h.distinctBytes
	size += h.extraStateBytes
	size += h.extrasAccsCount * kernelAccumulatorBytes
	// SoA flat accumulator arrays: 8 bytes per element for int64/float64
	// fields. Off-heap arrays carry a huge virtual cap, so account them by
	// len (the committed/used prefix — appends only ever touch that);
	// heap-backed arrays keep cap so allocated-but-unused doubling slack
	// stays charged.
	dim := func(c, l int) int64 {
		if h.offheap != nil {
			return int64(l)
		}
		return int64(c)
	}
	for _, fa := range h.intFlatAccs {
		size += dim(cap(fa.count), len(fa.count)) * 8
		size += dim(cap(fa.sumI64), len(fa.sumI64)) * 8
		size += dim(cap(fa.sumF64), len(fa.sumF64)) * 8
		size += dim(cap(fa.sumDec), len(fa.sumDec)) * 16 // Int128
		size += dim(cap(fa.minI64), len(fa.minI64)) * 8
		size += dim(cap(fa.maxI64), len(fa.maxI64)) * 8
		size += dim(cap(fa.minF64), len(fa.minF64)) * 8
		size += dim(cap(fa.maxF64), len(fa.maxF64)) * 8
		size += dim(cap(fa.minDec), len(fa.minDec)) * 16
		size += dim(cap(fa.maxDec), len(fa.maxDec)) * 16
		size += dim(cap(fa.hasMin), len(fa.hasMin))
		size += dim(cap(fa.hasMax), len(fa.hasMax))
	}
	// Int-key SoAs
	size += dim(cap(h.packedKeys), len(h.packedKeys)) * 16
	size += dim(cap(h.intKeys), len(h.intKeys)) * 8
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
		// Publish the true owned footprint so the SpillManager drift-backstop
		// (OwnedTotal vs tracker.Used) has a real number; without this the
		// published total stays 0 and the backstop misreads drift as 100%.
		if h.accInstanceID != 0 {
			h.Spill.Tracker().PublishOwned(h.accInstanceID, h.trackedGroupMem)
		}
	}
}

// stringAggState accumulates strings with a separator.
type stringAggState struct {
	// sep first keeps the two pointer prefixes adjacent (scan region 24 B, not 32).
	sep   string
	parts []string
}

// varianceState tracks running variance using Welford's online algorithm:
// the running (count, mean, M2) triple, updated one value at a time and
// combined pairwise by merge. Welford accumulates the deviations directly
// instead of subtracting E[x]² from E[x²], so no digits are lost to
// cancellation when the mean dwarfs the spread — the case that breaks a
// sum-of-squares accumulator (o_totalprice: mean 2.5e5, spread 1.4e5, so
// the two terms agree to five digits before the subtraction).
//
// The triple is the whole state, which is what makes it mergeable: partial
// aggregates computed over disjoint row sets combine exactly (see merge),
// so the same accumulator serves the single-process pipeline, the
// morsel-parallel clone merge, and the distributed partial/final split.
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

// merge folds another partial's state into this one — Chan, Golub and
// LeVeque's pairwise combination:
//
//	M2 = M2_a + M2_b + delta² · n_a·n_b / n
//
// The delta² term is the between-partial contribution. Dropping it (summing
// M2 alone) leaves only the within-partial variance, and re-aggregating the
// partials' finished STDDEV values instead is not an approximation at all —
// it is the standard deviation of a handful of nearly identical numbers.
// Both were live before #339.
//
// mean is combined by the same weighting rather than recomputed from the
// two means' midpoint, so a small partial merged into a large one moves the
// mean by its own weight.
func (v *varianceState) merge(o *varianceState) {
	if o == nil || o.count == 0 {
		return
	}
	if v.count == 0 {
		*v = *o
		return
	}
	na, nb := float64(v.count), float64(o.count)
	n := na + nb
	delta := o.mean - v.mean
	v.mean += delta * nb / n
	v.m2 += o.m2 + delta*delta*na*nb/n
	v.count += o.count
}

// varianceStateWidth is the encoded width of a variance partial state:
// count (int64) + mean (float64) + M2 (float64) = 24 bytes, hex-encoded to
// 48 ASCII characters. Hex rather than raw bytes because the encoded state
// travels as a string column through parquet, the .wshf shuffle format and
// the NATS gather — every one of which is happier with text than with
// arbitrary bytes — and float64 bits round-trip exactly through it.
const varianceStateWidth = 48

// encode renders the state as a fixed-width hex string for a partial
// aggregate's output column. Exact: the float64s go over as their IEEE-754
// bit patterns, so a merge stage reads back the identical triple.
func (v *varianceState) encode() string {
	var buf [24]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(v.count))
	binary.BigEndian.PutUint64(buf[8:16], math.Float64bits(v.mean))
	binary.BigEndian.PutUint64(buf[16:24], math.Float64bits(v.m2))
	return hex.EncodeToString(buf[:])
}

// decodeVarianceState parses encode's output. Reports ok=false for anything
// else, which the caller treats as "no rows to merge" rather than as a
// silent zero — a state column that lost its encoding must not read as a
// valid empty partial.
func decodeVarianceState(s string) (varianceState, bool) {
	if len(s) != varianceStateWidth {
		return varianceState{}, false
	}
	var full [24]byte
	if _, err := hex.Decode(full[:], []byte(s)); err != nil {
		return varianceState{}, false
	}
	return varianceState{
		count: int64(binary.BigEndian.Uint64(full[0:8])),
		mean:  math.Float64frombits(binary.BigEndian.Uint64(full[8:16])),
		m2:    math.Float64frombits(binary.BigEndian.Uint64(full[16:24])),
	}, true
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

// merge folds another partial's co-moments into this one. Same pairwise
// combination as varianceState.merge, extended to the cross term:
//
//	C = C_a + C_b + dx·dy · n_a·n_b / n
//
// CORR/COVAR share varianceState's exposure to a dropped merge (they ride
// the same extraState slot), so they are combined here rather than left for
// the next report of the same defect.
func (s *covarianceState) merge(o *covarianceState) {
	if o == nil || o.count == 0 {
		return
	}
	if s.count == 0 {
		*s = *o
		return
	}
	na, nb := float64(s.count), float64(o.count)
	n := na + nb
	dx := o.meanX - s.meanX
	dy := o.meanY - s.meanY
	s.c += o.c + dx*dy*na*nb/n
	s.m2x += o.m2x + dx*dx*na*nb/n
	s.m2y += o.m2y + dy*dy*na*nb/n
	s.meanX += dx * nb / n
	s.meanY += dy * nb / n
	s.count += o.count
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

// covarianceStateWidth is the encoded width of a covariance partial state:
// count (int64) + meanX + meanY + C + M2x + M2y (five float64s) = 48 bytes,
// hex-encoded to 96 ASCII characters. Same reasoning as
// varianceStateWidth — the state travels as a string column through
// parquet, .wshf and NATS, and float64 bits round-trip exactly through hex.
const covarianceStateWidth = 96

func (s *covarianceState) encode() string {
	var buf [48]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(s.count))
	binary.BigEndian.PutUint64(buf[8:16], math.Float64bits(s.meanX))
	binary.BigEndian.PutUint64(buf[16:24], math.Float64bits(s.meanY))
	binary.BigEndian.PutUint64(buf[24:32], math.Float64bits(s.c))
	binary.BigEndian.PutUint64(buf[32:40], math.Float64bits(s.m2x))
	binary.BigEndian.PutUint64(buf[40:48], math.Float64bits(s.m2y))
	return hex.EncodeToString(buf[:])
}

// decodeCovarianceState parses encode's output. ok=false for anything else,
// which the caller treats as "no rows to merge" rather than as a valid empty
// partial.
func decodeCovarianceState(s string) (covarianceState, bool) {
	if len(s) != covarianceStateWidth {
		return covarianceState{}, false
	}
	var full [48]byte
	if _, err := hex.Decode(full[:], []byte(s)); err != nil {
		return covarianceState{}, false
	}
	return covarianceState{
		count: int64(binary.BigEndian.Uint64(full[0:8])),
		meanX: math.Float64frombits(binary.BigEndian.Uint64(full[8:16])),
		meanY: math.Float64frombits(binary.BigEndian.Uint64(full[16:24])),
		c:     math.Float64frombits(binary.BigEndian.Uint64(full[24:32])),
		m2x:   math.Float64frombits(binary.BigEndian.Uint64(full[32:40])),
		m2y:   math.Float64frombits(binary.BigEndian.Uint64(full[40:48])),
	}, true
}

// Covariance-family kinds, carried in the synthetic column's name exactly
// as the variance kinds are: one state serves all three, and which function
// finishes it is decided once, after the last merge.
const (
	CovarKindCorr      = "corr"
	CovarKindCovarSamp = "covar_samp"
	CovarKindCovarPop  = "covar_pop"
)

// FinalizeCovarianceState decodes a merged partial state and finishes it as
// the named kind. ok=false means SQL NULL, on the same thresholds the
// single-process finalization applies: fewer than two rows for CORR and
// COVAR_SAMP, no rows at all for COVAR_POP.
//
// Used by the distributed final-aggregate fold (worker/var_fold.go).
func FinalizeCovarianceState(encoded, kind string) (float64, bool) {
	st, ok := decodeCovarianceState(encoded)
	if !ok {
		return 0, false
	}
	switch kind {
	case CovarKindCorr:
		if st.count < 2 {
			return 0, false
		}
		return st.correlation(), true
	case CovarKindCovarSamp:
		if st.count < 2 {
			return 0, false
		}
		return st.covarSamp(), true
	case CovarKindCovarPop:
		if st.count == 0 {
			return 0, false
		}
		return st.covarPop(), true
	}
	return 0, false
}

// Variance-family kinds. A decomposed partial carries the same (count,
// mean, M2) triple whatever the caller asked for, so the kind travels
// beside it (in the synthetic column's name) and is applied once, by
// FinalizeVarianceState, after the last merge.
//
// STDDEV and VARIANCE without a suffix are the SAMPLE forms, matching
// DuckDB and PostgreSQL.
const (
	VarKindStddevSamp = "stddev_samp"
	VarKindVarSamp    = "var_samp"
	VarKindStddevPop  = "stddev_pop"
	VarKindVarPop     = "var_pop"
)

// FinalizeVarianceState decodes a merged partial state and finishes it as
// the named kind. ok=false means the result is SQL NULL: an unparseable or
// absent state, fewer than two rows for a sample form, or no rows at all
// for a population form — the same thresholds the single-process
// finalization applies.
//
// Used by the distributed final-aggregate fold (worker/var_fold.go), which
// is the only consumer outside this package.
func FinalizeVarianceState(encoded, kind string) (float64, bool) {
	st, ok := decodeVarianceState(encoded)
	if !ok {
		return 0, false
	}
	switch kind {
	case VarKindStddevSamp:
		if st.count < 2 {
			return 0, false
		}
		return math.Sqrt(st.varianceSamp()), true
	case VarKindVarSamp:
		if st.count < 2 {
			return 0, false
		}
		return st.varianceSamp(), true
	case VarKindStddevPop:
		if st.count == 0 {
			return 0, false
		}
		return math.Sqrt(st.variancePop()), true
	case VarKindVarPop:
		if st.count == 0 {
			return 0, false
		}
		return st.variancePop(), true
	}
	return 0, false
}

// collectState accumulates raw float64 values for percentile/mode/median.
type collectState struct {
	values []float64
}

// minMaxByState tracks the row where a comparison column is min/max.
type minMaxByState struct {
	// Field order packs this to 32 B (was 40) and puts the pointer prefix first:
	// one per group for MIN_BY/MAX_BY.
	bestVal  any     // the return column value at that row
	bestCmp  float64 // the comparison column value (min or max)
	hasValue bool
	isMin    bool
}

// boxContainerMemBytes estimates the retained heap footprint of a value
// Vector.GetValue boxed for a container column (ARRAY/MAP as []any, ROW as
// map[string]any, VECTOR as []float32) — MIN_BY/MAX_BY's bestVal shares the
// same declared-output-type rule as container MIN/MAX (aggSpecOutputType:
// "MIN_BY/MAX_BY hand the output vector the box GetValue produced"), so
// bestVal can retain an arbitrarily large nested structure the same way
// containerMinMaxState.best can. Anything else returns 0: the flat per-agg
// charge (extraStateBytes += len(h.Aggs) * 80) already tracks close to
// measured for a scalar bestVal — a STRING return column measured 487 B/
// group against 484 reported — so this must not double-charge what the
// flat estimate already covers.
func boxContainerMemBytes(v any) int64 {
	switch v.(type) {
	case []any, map[string]any, []float32:
		return boxMemBytesDeep(v)
	default:
		return 0
	}
}

// boxMemBytesDeep walks the exact shapes Vector.GetValue produces (its
// TypeArray/TypeMap/TypeRow/TypeVector/TypeString/TypeBytes arms) and sums
// a rough heap estimate. Not exhaustive over every Go type — GetValue's box
// set is closed, so it does not need to be; an unrecognized type gets a
// small fixed estimate rather than 0, so a future box shape undercounts
// rather than vanishing entirely.
func boxMemBytesDeep(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case []any:
		n := int64(24) // slice header
		for _, e := range x {
			n += 16 + boxMemBytesDeep(e) // interface header + element
		}
		return n
	case map[string]any:
		n := int64(48) // map header estimate
		for k, e := range x {
			n += int64(len(k)) + 16 + boxMemBytesDeep(e)
		}
		return n
	case []float32:
		return 24 + int64(len(x))*4
	case string:
		return 16 + int64(len(x))
	case []byte:
		return 24 + int64(len(x))
	case int64, float64:
		return 8
	case int32, float32:
		return 4
	case bool:
		return 1
	default:
		return 16
	}
}

// float64Extractor reads a float64 value from a vector at a given row index.
// Pre-resolved during Init to eliminate per-row type switches in updateGroup.
type float64Extractor func(v *batch.Vector, row int) float64

// resolveFloat64Extractor returns a typed float64 extractor for the given
// column type, or nil if the type has no numeric reading — updateGroup reads
// nil as "skip every row", so the aggregate answers NULL.
//
// The type list lives in numeric_promote.go, shared with the window operator's
// vecFloat64 and matched to kernel.ResolveRowSum's, because three lists that
// disagree is three different answers to one query. PORT, PROTOCOL and
// DURATION were absent and made STDDEV/VARIANCE/MEDIAN/PERCENTILE/MODE over
// them answer NULL; TypeDate's absence was the same defect fixed for one type
// only (#353).
func resolveFloat64Extractor(typ batch.TypeID) float64Extractor {
	if !numericPromotable(typ) {
		return nil
	}
	return func(v *batch.Vector, row int) float64 {
		f, _ := numericFloat64(v, row)
		return f
	}
}

// resolveOrderKeyExtractor is resolveFloat64Extractor for MIN_BY/MAX_BY's
// ORDERING column, which only has to order — it never becomes the answer. That
// admits IPV4, MAC and BOOL, whose stored integer form orders like the value.
func resolveOrderKeyExtractor(typ batch.TypeID) float64Extractor {
	if !orderKeyPromotable(typ) {
		return nil
	}
	return func(v *batch.Vector, row int) float64 {
		f, _ := orderKeyFloat64(v, row)
		return f
	}
}

func NewHashAggregate(groupByCols []string, aggs []AggColumn) *HashAggregate {
	return &HashAggregate{
		GroupByCols: groupByCols,
		Aggs:        aggs,
	}
}

func (h *HashAggregate) Init(_ context.Context) error {
	// Re-Init on an instance whose previous emission is still fanned out
	// would strand the drain's goroutines on state we are about to reset.
	if h.emit != nil {
		h.emit.shutdown()
		h.emit = nil
	}
	// The string group index is built by resolveIndices, which sizes it from
	// the planner's NDV hint. Constructing it here left every one of those
	// pre-size branches (all guarded on `strGroupIndex == nil`) unreachable,
	// so every string GROUP BY started at 4096 slots and doubled — rehashing
	// the whole table on the way up to Q34's ~18M keys. The generic per-row
	// paths, which resolveIndices doesn't cover, create it via
	// strIndexForRow.
	h.strGroupIndex = nil
	h.strGroupStates = nil
	h.strNullGroupIdx = -1
	h.keys = nil
	h.serializedKeys = nil
	h.resetStateByteCounters()
	h.resolved = false
	h.keyBuf = make([]byte, 0, 128)
	h.outputPos = 0
	h.emitSchema = nil
	h.emitSchemaSet = false
	return nil
}

// ConsumeHashed is Consume with the group-key hash of every active row
// already computed by the partition router (hash once — see the bit-budget
// note in partitioned_agg.go). hashes[i] belongs to b's i'th active row.
//
// The hashes are advisory: each consume path checks that the plan names the
// hash ITS table uses before consuming them, and recomputes otherwise. A sink
// that migrated to the generic path mid-query (a NULL key arrived) therefore
// keeps working with a stale plan on the wire.
func (h *HashAggregate) ConsumeHashed(ctx context.Context, b *batch.RecordBatch, hashes []uint64, plan *routePlan) error {
	h.provHashes = hashes
	h.provPlan = plan
	err := h.Consume(ctx, b)
	h.provHashes = nil
	h.provPlan = nil
	return err
}

func (h *HashAggregate) Consume(_ context.Context, b *batch.RecordBatch) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Zero-column batches (#277, the Q18 fused-chain breaker panic —
	// SF100 stacks show duration=0s tasks dying on their FIRST batch, a
	// 0-row 0-column one, in consumeBatch's updater-selection loop which
	// indexes b.Columns regardless of row count):
	//   - EMPTY (no active rows): a no-op by definition — nothing to key,
	//     nothing to accumulate, nothing to learn. Skip. The
	//     flushSpilledOps drain path feeds Consume without an ActiveLen
	//     gate, so empties DO arrive here.
	//   - rows WITHOUT columns: the claimed rows have no key or input
	//     values, so neither consuming (index panic) nor skipping (silent
	//     row loss) is sound — fail with a structured error unless the
	//     aggregate provably needs no columns (COUNT(*)-only ungrouped).
	//     "Needs columns" must consult resolved state too: CloneSink
	//     copies resolution, so a clone can hold live column indices
	//     while its spec fields alone look column-free.
	if len(b.Columns) == 0 {
		if b.ActiveLen() == 0 {
			return nil
		}
		needsColumns := len(h.GroupByCols) > 0 || h.GroupByAll ||
			h.useIntGroupKey || h.usePackedGroupKey || h.useCompactGroupKey ||
			h.useStrGroupKey || h.useGenericSoA
		for _, a := range h.Aggs {
			if a.InputCol != "" {
				needsColumns = true
				break
			}
		}
		for _, ci := range h.aggColIdx {
			if ci >= 0 {
				needsColumns = true
				break
			}
		}
		if needsColumns {
			return fmt.Errorf("hash aggregate: batch with %d active rows and zero columns (sel=%v len=%d) — upstream emitted a schemaless batch (#277)",
				b.ActiveLen(), b.Sel != nil, b.Len)
		}
	}
	// Reads typed column storage directly and is fed outside the pipeline
	// loops by the worker's multi-breaker runner — flatten at the boundary.
	FlattenForConsumer(b, nil)

	// Save schema from first batch for spill recovery
	if h.inputSchema == nil {
		h.inputSchema = b.Schema
	}

	// Resolve column indices and typed updaters once
	if !h.resolved {
		if err := h.resolveIndices(b); err != nil {
			return err
		}
		// Cooperative-spill registration. resolveIndices populates the path
		// flags (useIntGroupKey etc.) that canUseExternalMerge inspects, so
		// register only after they've been set. Inspect reports SpillableBytes
		// == 0 until rows arrive, so registering on a brand-new aggregate
		// before any rows arrive doesn't disturb peer operators' relief
		// targeting.
		if h.Spill != nil && h.unregisterAccounted == nil && h.canUseExternalMerge() {
			h.accInstanceID = memory.NextInstanceID()
			h.accState.Store(int32(memory.OpActive))
			h.unregisterAccounted = h.Spill.RegisterAccounted(h)
		}
	}

	// A DECIMAL aggregate's SCALE is learned from the input vector, and one
	// kind of input batch carries a scale that is not its column's: the
	// identity row an ungrouped aggregate emits when it consumed no rows at
	// all. That row has no input schema to read a scale from, so it ships
	// zero — and a downstream merge that adopted it would render every later
	// value 10^scale too large. Preferring the first NONZERO observation
	// costs a genuinely scale-0 column nothing (it reports 0 in every batch)
	// and is the only reachable case where two batches of one aggregate
	// disagree (#455).
	if h.hasDecAggInput {
		for i, ci := range h.aggColIdx {
			if ci < 0 || ci >= len(b.Columns) || i >= len(h.aggInputDecScale) {
				continue
			}
			c := b.Columns[ci]
			if c.Type != batch.TypeDecimal {
				continue
			}
			// The GROUPED half of kernel.Accumulator.DecScaleConflict. The
			// flat SoA arrays hold ONE scale per aggregate (fa.decScale, set
			// from the first batch's column) and the scatter adds raw
			// unscaled integers into them, so a second batch at a DIFFERENT
			// nonzero scale is summed as if the two were counted the same:
			// 12.75 (1275 at scale 2) plus 0.1275 (1275 at scale 4) came back
			// 25.50, silently, where the UNGROUPED form raises 22003. Latched
			// here rather than in the scatter because this is the one place
			// that sees the batch's vector beside the aggregate's established
			// scale, and it is off the per-row path (#685 review, item A).
			if got := c.DecimalData.Scale; got != 0 && h.aggInputDecScale[i] != 0 && got != h.aggInputDecScale[i] {
				h.decScaleConflict = true
			}
			if h.aggInputDecScale[i] != 0 || c.DecimalData.Scale == 0 {
				continue
			}
			h.aggInputDecScale[i] = c.DecimalData.Scale
			if i < len(h.intFlatAccs) && h.intFlatAccs[i].isDecimal && h.intFlatAccs[i].decScale == 0 {
				h.intFlatAccs[i].decScale = c.DecimalData.Scale
				// A scale learned late is still a scale the spill format has
				// to carry; re-latch so a later drain writes it.
				h.latchAggEncodings()
			}
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
	// forcedDrain is the TEST-ONLY deterministic drain knob
	// (aggregate_force_drain.go): it puts a drain on a CHOSEN batch, which is
	// what makes a spill gate reproduce a condition-triggered defect every run
	// instead of some runs.
	forcedDrain := h.forcedDrainDue()
	if h.Spill != nil && (forcedDrain || h.Spill.ShouldSpillFor(memory.SpillCheap)) {
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
			// Drain-productivity gate (#325): ShouldSpillFor answers for the
			// whole tracker, so an aggregate holding almost none of the
			// pressured bytes sees it on every batch. Draining then writes a
			// run file, frees nothing, and leaves the signal set — the
			// livelock in #325. Require a floor of new state of our own
			// before spending another drain; see aggregate_drain_gate.go.
			// The productivity gate measures relief against real pressure;
			// a forced drain has none to measure, so it is exempt.
			if !forcedDrain && !h.drainIsProductive() {
				return nil
			}
			if forcedDrain {
				ForcedAggDrains.Add(1)
			}
			// Self-triggered spill: drain enough to recover headroom below the
			// SpillCheap threshold (60% of budget), leaving a 5% hysteresis
			// margin so we don't immediately re-trigger. The partial-drain
			// dispatcher routes int-keyed cases through spillPartialPartitions
			// when target < footprint, breaking the drain-rebuild loop that
			// whole-table self-spill created on heavy GROUP BY workloads (Q18
			// SF100 mc=3 — 150M orderkey groups would otherwise drain fully on
			// every pressure event and rebuild from the next probe burst).
			// Large targets (target >= footprint) and non-int paths fall
			// through to spillFullState — semantically identical to before.
			return h.drainAndAccount(h.selfSpillReliefTarget())
		}
		// An UNGROUPED aggregate never buffers input rows (#779). Its state
		// is one row of accumulators — plus, for the non-simple aggregates,
		// whatever extra state they keep — and buffering the input builds
		// exactly that same state one Finalize later, out of rows that had to
		// be written to disk and read back first. There is no memory the
		// detour saves, and it is wrong twice over here:
		//
		//   - ToRows reads every column's VALUES, and the planner ships a
		//     column whose every use is a SHAPE use (COUNT(col), LENGTH(col),
		//     IS NULL) with its lengths and no bytes. `SELECT COUNT(c_str)`
		//     under a budget therefore failed with the shape-only guard's
		//     "some consumer of this column is not a shape consumer" — a
		//     correct query that answers only while it has memory to spare.
		//   - Rows that go through memory.SpillManager.SpillRows are rendered
		//     by a value tagger, which is a second, narrower encoder than the
		//     columnar one (#632 is its BYTES arm).
		//
		// Grouped shapes still take the row buffer: their state grows with the
		// key set, so moving input to disk is a real trade. GROUPING SETS and
		// GroupByAll (DISTINCT) are grouped shapes.
		if len(h.GroupByCols) == 0 && !h.GroupByAll && len(h.GroupingSets) == 0 {
			h.consumeBatch(b)
			h.reconcileGroupMemory()
			return nil
		}
		// Legacy raw-row spill (extra-state aggs, grouping sets):
		// buffer rows on disk and re-aggregate in Finalize.
		rows := b.ToRows()
		h.spillBuffer = append(h.spillBuffer, rows...)
		h.spillBufferBytes += b.MemBytes()
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

	// Clone-partial bound: drain the whole state to run files once it
	// crosses PartialDrainBytes (see the field doc). trackedGroupMem is
	// fresh from reconcileGroupMemory above.
	if h.PartialDrainBytes > 0 && h.trackedGroupMem > h.PartialDrainBytes &&
		h.canUseExternalMerge() && h.Spill != nil && h.Spill.SpillDir() != "" {
		paths, err := h.drainStateToRuns(h.Spill.SpillDir())
		if err != nil {
			return fmt.Errorf("clone partial drain: %w", err)
		}
		h.drainedRuns = append(h.drainedRuns, paths...)
	}

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
//  3. Try every schema column whose suffix matches ".col" — for a bare name
//     and for a qualified one whose own qualifier missed. Returns -1 when
//     there are 2+ matches (ambiguous, refuses to guess).
//
// Step 3 is required after the self-join planner pass (Q07's
// QualifyAllBuildCols) leaves the schema with "n1.n_name" but
// parseJoinKeys feeds the worker an unqualified "n_name" key — without
// the unqualified→qualified fallback the join would never find the column.
//
// That step reaches the QUALIFIED spelling too, and it did not until #762.
// `QualifyAllBuildCols` renames EVERY build column to the build's TABLE
// alias, so a CTE or derived table on the build side of a self-join publishes
// `decpair.dv` while every consumer above the join spells it `c.dv` — and the
// resolver returned -1 at step 2 without ever trying step 3. Each consumer
// then failed its own way: a filter answered UNKNOWN on every row (a silent
// zero), a projection and a sort key failed loudly, an aggregate refused its
// input. The BARE spelling of the same predicate already answered, through
// step 3, which is what says the qualifier is the whole of it.
//
// physical.columnResolves has compared a reference against a qualified column
// by its bare part since #656, so the planner's checker believed in this
// resolution and waved the plan through; implementing it removes a
// disagreement rather than adding a special case. Ambiguity still declines —
// two arms that both spell `.w` keep the loud failure instead of the engine
// guessing an arm (#742).
func columnIndexFallback(b *batch.RecordBatch, name string) int {
	if idx := b.ColumnIndex(name); idx >= 0 {
		return idx
	}
	bare := name
	if dotIdx := strings.Index(name, "."); dotIdx >= 0 {
		bare = name[dotIdx+1:]
		if idx := b.ColumnIndex(bare); idx >= 0 {
			return idx
		}
	}
	// No exact match under this spelling: scan for a single qualified
	// match ".bare". Reject ambiguity (>1 match) so we never silently
	// pick the wrong column.
	suffix := "." + bare
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

// unresolvedAggColumn is the error a name the aggregate reads but the input
// batch does not carry. Answering it with NULL is what #355 was: `SELECT
// MAX(n) FROM (SELECT o_custkey AS n FROM orders)` came back NULL on the
// stage DAG, because the subquery's rename emits no stage there and the
// aggregate asked for a column nothing produced. The same miss on a GROUP BY
// key is louder still — an unresolvable key serializes as a NULL key, so every
// row collapses into one group.
//
// The planner is where a rename is meant to be resolved (physical.
// resolveAggInputName). This is the backstop that stops the next one being a
// wrong answer instead of a failure.
func unresolvedAggColumn(role, col string, b *batch.RecordBatch) error {
	have := make([]string, len(b.Schema))
	for i, c := range b.Schema {
		have[i] = c.Name
	}
	return fmt.Errorf("hash aggregate: %s %q is not a column of its input (input has: %s)",
		role, col, strings.Join(have, ", "))
}

// readsSecondColumn reports whether an aggregate function reads InputCol2 per
// row. The *StateMerge pair does not: it consumes the encoded state its
// partial emitted, and carries InputCol2 only because the spec is copied
// whole.
func readsSecondColumn(fn AggFunc) bool {
	switch fn {
	case AggCorr, AggCovarSamp, AggCovarPop, AggCovarState, AggMinBy, AggMaxBy:
		return true
	}
	return false
}

func (h *HashAggregate) resolveIndices(b *batch.RecordBatch) error {
	// GroupByAll resolves the key set from the live schema, so every
	// downstream decision (fast-path selection, output schema, spill merge)
	// sees a concrete column list exactly as if the planner had named them.
	if h.GroupByAll && len(h.GroupByCols) == 0 {
		names := make([]string, len(b.Schema))
		for i, c := range b.Schema {
			names[i] = c.Name
		}
		h.GroupByCols = names
	}
	h.groupColIdx = make([]int, len(h.GroupByCols))
	h.groupColTypes = make([]batch.TypeID, len(h.GroupByCols))
	h.groupColMeta = make([]parquet.Column, len(h.GroupByCols))
	for i, col := range h.GroupByCols {
		idx := columnIndexFallback(b, col)
		h.groupColIdx[i] = idx
		if idx < 0 {
			return unresolvedAggColumn("GROUP BY key", col, b)
		}
		h.groupColTypes[i] = b.Columns[idx].Type
		if idx < len(b.Schema) {
			h.groupColMeta[i] = b.Schema[idx]
		}
	}
	h.aggColIdx = make([]int, len(h.Aggs))
	h.aggColIdx2 = make([]int, len(h.Aggs))
	h.aggInputTypes = make([]batch.TypeID, len(h.Aggs))
	h.aggInputMeta = make([]parquet.Column, len(h.Aggs))
	h.aggInputDecScale = make([]int, len(h.Aggs))
	h.aggUpdaters = make([]kernel.RowAggUpdater, len(h.Aggs))
	h.aggUpdatersNoNull = make([]kernel.RowAggUpdater, len(h.Aggs))
	h.batchUpdaters = make([]kernel.RowAggUpdater, len(h.Aggs))
	h.aggBoxedMinMax = make([]bool, len(h.Aggs))
	h.hasBoxedMinMax = false
	for i, agg := range h.Aggs {
		h.aggColIdx2[i] = -1 // default: no second column
		if agg.Func == AggCountDistinct || agg.Func == AggApproxDistinct {
			if agg.InputCol != "" {
				h.aggColIdx[i] = columnIndexFallback(b, agg.InputCol)
				if h.aggColIdx[i] < 0 {
					return unresolvedAggColumn("aggregate input", agg.InputCol, b)
				}
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
			//
			// A pinned position wins outright: it is the only way to tell two
			// same-named partial columns apart in a duplicate-alias merge
			// (#575), which the name lookup cannot.
			idx := -1
			if agg.InputColIdxSet && agg.InputColIdx >= 0 && agg.InputColIdx < len(b.Columns) {
				idx = agg.InputColIdx
			} else {
				idx = columnIndexFallback(b, agg.InputCol)
			}
			h.aggColIdx[i] = idx
			if idx < 0 {
				return unresolvedAggColumn("aggregate input", agg.InputCol, b)
			}
			h.aggInputTypes[i] = b.Columns[idx].Type
			if idx < len(b.Schema) {
				h.aggInputMeta[i] = b.Schema[idx]
			}
			if b.Columns[idx].Type == batch.TypeDecimal {
				h.hasDecAggInput = true
				// The VECTOR's scale, not the schema column's: the
				// accumulators count in the vector's scale (initFlatAccums,
				// kernel.sumRowDecimal), so declaring anything else would
				// emit an Int128 that means 10^(difference) times the
				// answer. The two agree everywhere they are both set; this
				// reads the one that is load-bearing.
				h.aggInputDecScale[i] = b.Columns[idx].DecimalData.Scale
			}
			h.aggUpdaters[i] = resolveAggUpdater(agg.Func, b.Columns[idx].Type)
			h.aggUpdatersNoNull[i] = resolveAggUpdaterNoNull(agg.Func, b.Columns[idx].Type)
			if isContainerMinMax(agg.Func, b.Columns[idx].Type) {
				h.aggBoxedMinMax[i] = true
				h.hasBoxedMinMax = true
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
			// Loud only for the functions that READ it. A partial/final
			// split leaves InputCol2 naming the original column on the
			// FINAL, whose input is the partial's encoded state column and
			// carries neither operand — stale metadata, not a lookup.
			if h.aggColIdx2[i] < 0 && readsSecondColumn(agg.Func) {
				return unresolvedAggColumn("aggregate input", agg.InputCol2, b)
			}
		}
	}

	// Pre-resolve float64 extractors for aggregates that need per-row numeric conversion
	// (variance, stddev, corr, covar, percentile, mode, median, min_by, max_by).
	// This eliminates the per-row type switch in updateGroup.
	h.aggF64Extract = make([]float64Extractor, len(h.Aggs))
	h.aggF64Extract2 = make([]float64Extractor, len(h.Aggs))
	for i, agg := range h.Aggs {
		switch agg.Func {
		case AggStddev, AggVariance, AggStddevPop, AggVarPop, AggVarState,
			AggPercentileCont, AggPercentileDisc, AggMedian, AggMode:
			// AggVarStateMerge is deliberately absent: its input is an
			// encoded state string, not a number to convert.
			if idx := h.aggColIdx[i]; idx >= 0 {
				h.aggF64Extract[i] = resolveFloat64Extractor(b.Columns[idx].Type)
			}
		case AggCorr, AggCovarSamp, AggCovarPop, AggCovarState:
			// AggCovarStateMerge is absent for the same reason
			// AggVarStateMerge is: its input is an encoded state string.
			if idx := h.aggColIdx[i]; idx >= 0 {
				h.aggF64Extract[i] = resolveFloat64Extractor(b.Columns[idx].Type)
			}
			if idx := h.aggColIdx2[i]; idx >= 0 {
				h.aggF64Extract2[i] = resolveFloat64Extractor(b.Columns[idx].Type)
			}
		case AggMinBy, AggMaxBy:
			// Second column is the ORDERING column: it picks the row and
			// never becomes the answer, so it takes the wider table.
			if idx := h.aggColIdx2[i]; idx >= 0 {
				h.aggF64Extract2[i] = resolveOrderKeyExtractor(b.Columns[idx].Type)
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
			AggVarState, AggVarStateMerge,
			AggBoolAnd, AggBoolOr, AggCorr, AggCovarSamp, AggCovarPop,
			AggCovarState, AggCovarStateMerge,
			AggPercentileCont, AggPercentileDisc, AggMedian,
			AggMinBy, AggMaxBy:
			allSimpleAggs = false
			h.needsExtra = true
		case AggMode:
			allSimpleAggs = false
			h.needsExtra = true
		case AggMin, AggMax:
			if idx := h.aggColIdx[i]; idx >= 0 {
				switch b.Columns[idx].Type {
				case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR,
					batch.TypeUUID, batch.TypeBool:
					// The flat SoA arrays have no byte-backed or bool
					// min/max storage; route through the generic
					// kernel.Accumulator path. The list must stay in step
					// with the byte-backed arms of kernel.ResolveRowMin: a
					// type with a working updater that is NOT named here
					// stays on the SoA path, whose scatter has no arm for
					// it, and answers NULL again — which is #417 reached by
					// its other door.
					allSimpleAggs = false
				}
			}
			if h.aggBoxedMinMax[i] {
				// A container's extreme is a retained VALUE, not an
				// Accumulator slot — the same shape MIN_BY needs, so it
				// takes the same per-group extraState (#426).
				allSimpleAggs = false
				h.needsExtra = true
			}
			if h.aggUpdaters[i] == nil && agg.InputCol != "" {
				allSimpleAggs = false
			}
		default:
			if h.aggUpdaters[i] == nil && agg.InputCol != "" {
				allSimpleAggs = false
			}
		}
	}
	// Kill switch (see agg_fast_path_toggle.go): treat every aggregate as
	// non-simple so all typed fast paths below stay dormant and grouping
	// runs on the generic AoS accumulator path.
	if !aggFastPaths.On() {
		allSimpleAggs = false
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
	// NDV presize: when the planner supplies a group-key cardinality
	// estimate (merged-HLL, ~2% error), size the table ONCE instead of
	// doubling up through the cardinality (each doubling rehashes
	// everything; the top grows of a 100M-group query rehash tens of
	// millions of entries with old+new tables both live). +12.5% slack
	// absorbs HLL underestimation. Two caps bound overestimate damage by
	// per-slot cost: the generic/string paths pre-allocate arena + group
	// pool per slot (the Q12 lesson: never over-provision heavy slots),
	// so they cap at 4M; the int-key paths pay only the 16B entry — and
	// off-heap when the registry is live — so they presize up to a 1GB
	// table (intHTInitSize, applied at the int-table sites).
	intHTInitSize := 0
	if h.GroupNDVHint > 0 {
		est := h.GroupNDVHint + h.GroupNDVHint/8
		if d := int64(h.cloneNDVDivisor); d > 1 {
			est = est/d + est/(8*d) // partition skew slack
		}
		const intNDVCap = 1 << 26
		const genericNDVCap = 1 << 22
		intEst := est
		if intEst > intNDVCap {
			intEst = intNDVCap
		}
		intHTInitSize = int(intEst)
		if est > genericNDVCap {
			est = genericNDVCap
		}
		if int(est) > htInitSize {
			htInitSize = int(est)
		}
	}
	if intHTInitSize < htInitSize {
		intHTInitSize = htInitSize
	}

	// Group-index LAYOUT, decided here and only here, from the bounds the
	// sink's owner declared before the first row: an epoch byte cap
	// (twoLevelBoundedMinGroups) or an exact input-row bound
	// (twoLevelAmortizeMultiple). Either one pins the index flat for life
	// when a flat→bucketed conversion could not be repaid. A sink with
	// neither bound leaves this false and keeps the adaptive path unchanged.
	bornFlat, ceiling, flatWhy := h.indexLayoutStaysFlat(b)
	h.groupCeiling = ceiling
	if bornFlat && !h.indexBornFlat {
		TwoLevelBornFlat.Add(1)
	}
	h.indexBornFlat = bornFlat
	h.indexFlatWhy = flatWhy

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
			// NDV hint already past the conversion threshold: build bucketed
			// straight away and never pay a conversion (two_level_hash.go).
			// Gated on a real hint — the hint-free default presize (4096, or
			// up to 64K from InputRowHint) is a guess, and guessing bucketed
			// would give a small aggregate 256 sub-tables for nothing.
			if twoLevelToggle.On() && !h.indexBornFlat &&
				h.GroupNDVHint > 0 && intHTInitSize >= twoLevelConvertAt {
				h.intTwoLevel = newIntTwoLevelTable(intHTInitSize, h.offheapReg())
				h.intGroupIndex = nil
				TwoLevelDirectBuilds.Add(1)
			} else {
				h.intGroupIndex = newIntHashTableReg(intHTInitSize, h.offheapReg())
				h.intTwoLevel = nil
			}
			h.intGroupKeyCol = h.groupColIdx[0]
			if h.intKeys == nil {
				h.intKeys = memory.Offheap[int64](h.offheapReg(), htInitSize)
				// No gsPool.preAlloc and no []*groupState: state is deferred
				// entirely to the SoA arrays on this path, and a hint-sized
				// chunk would be dead weight at exactly the group counts
				// where memory is the constraint.
			}
			if h.intFlatAccs == nil {
				if h.numIntGroups > 0 {
					h.rebuildFlatAccums(b)
				} else {
					h.initFlatAccums(b)
				}
			}
		}
	}

	// Packed composite-key GROUP BY fast path:
	// 2-4 fixed-width int-class columns whose widths sum to <= 16 bytes pack
	// into one 128-bit key held inline in packedHashTable's entries — one
	// probe per row, no chain, one key array. Two-column shapes are exactly
	// what the dual-int path used to take (two int columns are at most
	// 8+8 = 16 bytes, so the coverage is a superset); the widened 3-4 column
	// shapes are gated on the kill switch, which restores their previous
	// compact/generic routing when off.
	if !h.useIntGroupKey && !h.isScalarAgg && len(h.GroupByCols) >= 2 && allSimpleAggs {
		layout := buildPackedLayout(h.groupColTypes)
		colsResolved := true
		for _, idx := range h.groupColIdx {
			if idx < 0 {
				colsResolved = false
				break
			}
		}
		if layout != nil && colsResolved &&
			(len(h.GroupByCols) == 2 || packedKeysToggle.On()) {
			h.usePackedGroupKey = true
			h.packedLayout = layout
			if twoLevelToggle.On() && !h.indexBornFlat &&
				h.GroupNDVHint > 0 && intHTInitSize >= twoLevelConvertAt {
				h.packedTwoLevel = newPackedTwoLevelTable(intHTInitSize, h.offheapReg())
				h.packedIdx = nil
				TwoLevelDirectBuilds.Add(1)
			} else {
				h.packedIdx = newPackedHashTableReg(intHTInitSize, h.offheapReg())
				h.packedTwoLevel = nil
			}
			if h.packedKeys == nil {
				// Key SoA off-heap (no per-group state at all on this path —
				// 6806c83); no preAlloc for the same dead-weight reason as the
				// single-int branch.
				h.packedKeys = memory.Offheap[packedKey](h.offheapReg(), htInitSize)
			}
			if h.intFlatAccs == nil {
				if h.numIntGroups > 0 {
					h.rebuildFlatAccums(b)
				} else {
					h.initFlatAccums(b)
				}
			}
		}
	}

	// Multi-column compact GROUP BY fast path:
	// When the binary-encoded GROUP BY key fits in 8 bytes, pack it into int64
	// and use intHashTable instead of map[string]. Avoids string hashing and
	// Go map overhead. Falls back to generic path if a key exceeds 8 bytes.
	if !h.useIntGroupKey && !h.usePackedGroupKey && !h.isScalarAgg && len(h.GroupByCols) >= 2 && allSimpleAggs {
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
			h.intGroupIndex = newIntHashTableReg(intHTInitSize, h.offheapReg())
			if h.intGroupStates == nil {
				h.intGroupStates = make([]*groupState, 0, htInitSize)
				h.gsPool.preAlloc(htInitSize)
			}
			if h.intFlatAccs == nil {
				if h.numIntGroups > 0 {
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
	if !h.useIntGroupKey && !h.usePackedGroupKey && !h.useCompactGroupKey &&
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
					h.serializedKeys = make([]string, 0, htInitSize)
					// No gsPool.preAlloc and no h.keys pre-size: this path
					// defers group state entirely (nil entries in
					// strGroupStates, keys carried by serializedKeys), so
					// only the NULL group ever allocates either. At the NDV
					// pre-size cap that would be 4M × 24 B of groupState plus
					// 4M × 24 B of slice header for one used slot — the same
					// dead weight the int paths dropped in 6806c83.
					h.keys = nil
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
	if !h.useIntGroupKey && !h.usePackedGroupKey && !h.useCompactGroupKey &&
		!h.useStrGroupKey && !h.isScalarAgg && len(h.GroupByCols) > 0 && allSimpleAggs {
		h.useGenericSoA = true
		// Defer key boxing when every group column both round-trips through
		// the binary key encoding losslessly AND boxes to a primitive whose
		// reconstruction is trivial (GetValue parity: int64/int32/float/
		// bool/string/[]byte). Network types, Date, and UUID box as
		// FORMATTED STRINGS in GetValue; Decimal boxes as one too
		// (FormatDecimal at the column's scale, vector.go) but its storage
		// is Int128 plus that scale, not a flat typed slice buildKeySerCols/
		// serializeGroupKey's typed reconstruction below handles, and its
		// KEY is the canonical unscaled digits at the column's scale
		// (AppendDecimalKey, #474), not a float64 as it was before —
		// Decimal keeps eager boxing for the storage-shape reason, not the
		// old key-encoding one. Kills the per-new-group []any +
		// GetValue-box + extras allocations that were 29% (mallocgc cum) of
		// ClickBench Q19's profile.
		h.deferGenericKeyBoxing = true
		for _, t := range h.groupColTypes {
			switch t {
			case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration,
				batch.TypeInt32, batch.TypePort, batch.TypeProtocol,
				batch.TypeFloat64, batch.TypeFloat32, batch.TypeBool,
				batch.TypeString, batch.TypeBytes:
			default:
				h.deferGenericKeyBoxing = false
			}
		}
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
			// scalarAccs may already hold merged clone partials: mergeSinkState's
			// scalar adoption does not set h.resolved, so a post-merge Consume
			// (collapse serial continuation, spill replay) re-enters resolution
			// here — remaking the accumulators would silently discard the merged
			// state (#279 sibling: wrong results instead of a panic).
			if h.scalarAccs == nil {
				h.scalarAccs = make([]kernel.Accumulator, len(h.Aggs))
			}
		}
	}
	return nil
}

func (h *HashAggregate) consumeBatch(b *batch.RecordBatch) {
	// Terminal defense for the #277 panic family: a zero-column batch can
	// reach here through paths the Consume-entry guard cannot cover (the
	// flushSpilledOps drain feeds Consume without gates). Pure
	// COUNT(*)-style aggregates (every aggColIdx < 0) legitimately consume
	// schemaless batches and proceed. Otherwise skipping is no worse than
	// the panic it replaces. NOTE: the 2026-08-02 SF100 Q18 join-8 stacks
	// that were first read as "Columns emptied between this guard and the
	// updater loop" were actually #279 — a nil batchUpdaters scratch after
	// adoptStateFrom, one line below the b.Columns index on the same
	// source line. The batch was never mutated; no such race exists.
	if len(b.Columns) == 0 {
		for _, ci := range h.aggColIdx {
			if ci >= 0 {
				return
			}
		}
		if !h.isScalarAgg {
			return // grouped: keys unavailable without columns
		}
	}
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
	if h.intFlatAccs == nil && (h.useIntGroupKey || h.usePackedGroupKey || h.useCompactGroupKey || h.useStrGroupKey || h.useGenericSoA) {
		h.rebuildFlatAccums(b)
	}

	// Single-column integer GROUP BY fast path
	// NULL group keys cannot live in the int hash tables. The int paths
	// used to divert null-key rows into strGroupStates via processRow — a
	// second store that Next(), the SoA merges, and the migrations all
	// ignored, so the NULL group silently vanished from results (GROUP BY
	// over a nullable int column dropped its NULL row; DISTINCT would drop
	// NULLs). On the first batch whose key column actually contains nulls,
	// migrate to the generic path once and stay there — null keys are rare,
	// and a single store is the only shape every reader gets right.
	if h.useIntGroupKey && b.Columns[h.intGroupKeyCol].Nulls.HasNulls() {
		h.migrateToGenericMap()
	} else if h.usePackedGroupKey {
		// Same rule for every packed key column: a NULL in ANY of them
		// migrates the whole aggregate to the generic path BEFORE this batch
		// is consumed. This is what makes an in-table NULL impossible, which
		// in turn is what lets the packing be total — no bit pattern has to
		// be reserved to mean NULL, so no real value can be mistaken for one.
		for _, ci := range h.groupColIdx {
			if b.Columns[ci].Nulls.HasNulls() {
				h.migrateToGenericMap()
				break
			}
		}
	}

	if h.useIntGroupKey {
		h.consumeBatchIntGroup(b)
		return
	}

	// Packed composite-key GROUP BY fast path
	if h.usePackedGroupKey {
		h.consumeBatchPackedGroup(b)
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
//
//	Phase 1: Hash lookup — compute group indices for all rows in the batch.
//	Phase 2: Per-aggregate typed scatter update using flat accumulator arrays.
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
	// Live-entry count before this batch: the conversion decision at the
	// bottom needs the batch's new-group count, and the free-list path
	// recycles slots without moving numIntGroups.
	liveBefore := 0
	if intIdx != nil {
		liveBefore = intIdx.Len()
	}

	// Hash once: the partition router already computed fibHash over this
	// column for every routed row. Accept its array only when the plan names
	// the same key extraction this loop performs. The routed and self-hashing
	// loops below are written out separately so this test never enters the
	// per-row path — see the note in consumeBatchPackedGroup for the cost.
	var ph []uint64
	if p := h.provPlan; p != nil && p.kind == hashKindInt && p.isI32 == isInt32 &&
		len(h.provHashes) == b.ActiveLen() {
		ph = h.provHashes
		HashOnceRoutedRows.Add(int64(len(ph)))
	}

	// Pre-reserve key capacity so per-group appends in the hash lookup loop
	// don't trigger growslice reallocations. The batch size is an upper
	// bound on new groups this batch can create. The flat accumulators are
	// NOT touched here — they are grown once, after the loop, to the final
	// group count (scatterBatchAggs below).
	batchRows := b.ActiveLen()
	h.intKeys = ensureAppendCap(h.intKeys, batchRows)

	// Phase 1: Hash lookup — build group index array.
	// gi[i] maps iteration index i to its group state index, or -1 for null keys.
	var gi []int32
	var sel []uint32
	var iterLen int
	hasNullKeys := false

	if tl := h.intTwoLevel; tl != nil {
		gi, sel, iterLen, hasNullKeys = h.intGroupPhase1TwoLevel(b, tl, gkVec, isInt32, hasNulls, ph)
	} else if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen] // provable bound: si ranges over b.Sel
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
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := intIdx.GetOrInsertNoGrowAt(key, ph[si], newIdx)
				if ok {
					gi[si] = gsIdx
				} else {
					intIdx.CheckGrow()
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[si] = newIdx
				}
			}
		} else {
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
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
				if ok {
					gi[si] = gsIdx
				} else {
					intIdx.CheckGrow()
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						// No per-group state at all: the key lives in the intKeys
						// SoA and the accumulators in the flat arrays, which are
						// grown to numIntGroups once this loop finishes.
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[si] = newIdx
				}
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen]
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
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := intIdx.GetOrInsertNoGrowAt(key, ph[row], newIdx)
				if ok {
					gi[row] = gsIdx
				} else {
					intIdx.CheckGrow()
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[row] = newIdx
				}
			}
		} else {
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
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := intIdx.GetOrInsertNoGrow(key, newIdx)
				if ok {
					gi[row] = gsIdx
				} else {
					intIdx.CheckGrow()
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						// No per-group state at all: the key lives in the intKeys
						// SoA and the accumulators in the flat arrays, which are
						// grown to numIntGroups once this loop finishes.
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[row] = newIdx
				}
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	// One pass per aggregate with inlined typed arithmetic (no function pointers).
	h.scatterBatchAggs(h.numIntGroups, b, gi, sel, iterLen)

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

	// Bucketed conversion is decided ONCE per batch, here at the end: the
	// loops above hoisted their table pointer, and the decision needs this
	// batch's new-group count (two_level_hash.go).
	if intIdx != nil {
		h.maybeConvertIntIndex(intIdx.Len() - liveBefore)
	}
}

// intGroupPhase1TwoLevel is consumeBatchIntGroup's phase 1 against the
// BUCKETED index (two_level_hash.go). Everything else about the batch —
// key extraction, free-list reuse, the intKeys SoA append, phase 2's
// scatter, the null-key fallback — is identical; only the table the probe
// lands in differs, and group ids stay dense globals.
//
// Written out rather than branching inside the flat loops for the reason
// recorded in consumeBatchPackedGroup: a per-row test on a hoistable
// condition measured +4% on this class of loop. The routed and self-hashing
// variants are likewise separate; the two-level probe needs the hash as a
// value (it selects the bucket), so the self-hashing variant computes
// fibHash once and hands it to the same entry point.
func (h *HashAggregate) intGroupPhase1TwoLevel(b *batch.RecordBatch, tl *intTwoLevelTable,
	gkVec *batch.Vector, isInt32, hasNulls bool, ph []uint64) (gi []int32, sel []uint32, iterLen int, hasNullKeys bool) {
	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen] // provable bound: si ranges over b.Sel
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
				var newIdx int32
				fromFree := false
				if nf := len(h.freeGroupIDs); nf > 0 {
					newIdx = h.freeGroupIDs[nf-1]
					fromFree = true
				} else {
					newIdx = int32(h.numIntGroups)
				}
				gsIdx, ok := tl.GetOrInsertAt(key, ph[si], newIdx)
				if ok {
					gi[si] = gsIdx
				} else {
					if fromFree {
						h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
						h.intKeys[newIdx] = key
					} else {
						h.numIntGroups++
						h.intKeys = append(h.intKeys, key)
					}
					gi[si] = newIdx
				}
			}
			return gi, sel, iterLen, hasNullKeys
		}
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
			var newIdx int32
			fromFree := false
			if nf := len(h.freeGroupIDs); nf > 0 {
				newIdx = h.freeGroupIDs[nf-1]
				fromFree = true
			} else {
				newIdx = int32(h.numIntGroups)
			}
			gsIdx, ok := tl.GetOrInsertAt(key, fibHash(key), newIdx)
			if ok {
				gi[si] = gsIdx
			} else {
				if fromFree {
					h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
					h.intKeys[newIdx] = key
				} else {
					h.numIntGroups++
					h.intKeys = append(h.intKeys, key)
				}
				gi[si] = newIdx
			}
		}
		return gi, sel, iterLen, hasNullKeys
	}

	iterLen = b.Len
	gi = h.ensureGroupIndexBuf(iterLen)
	if ph != nil {
		ph = ph[:iterLen]
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
			var newIdx int32
			fromFree := false
			if nf := len(h.freeGroupIDs); nf > 0 {
				newIdx = h.freeGroupIDs[nf-1]
				fromFree = true
			} else {
				newIdx = int32(h.numIntGroups)
			}
			gsIdx, ok := tl.GetOrInsertAt(key, ph[row], newIdx)
			if ok {
				gi[row] = gsIdx
			} else {
				if fromFree {
					h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
					h.intKeys[newIdx] = key
				} else {
					h.numIntGroups++
					h.intKeys = append(h.intKeys, key)
				}
				gi[row] = newIdx
			}
		}
		return gi, sel, iterLen, hasNullKeys
	}
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
		var newIdx int32
		fromFree := false
		if nf := len(h.freeGroupIDs); nf > 0 {
			newIdx = h.freeGroupIDs[nf-1]
			fromFree = true
		} else {
			newIdx = int32(h.numIntGroups)
		}
		gsIdx, ok := tl.GetOrInsertAt(key, fibHash(key), newIdx)
		if ok {
			gi[row] = gsIdx
		} else {
			if fromFree {
				h.freeGroupIDs = h.freeGroupIDs[:len(h.freeGroupIDs)-1]
				h.intKeys[newIdx] = key
			} else {
				h.numIntGroups++
				h.intKeys = append(h.intKeys, key)
			}
			gi[row] = newIdx
		}
	}
	return gi, sel, iterLen, hasNullKeys
}

// consumeBatchPackedGroup is the fast path for multi-column int-class GROUP
// BY whose packed key fits in 128 bits. Two-phase SoA approach like
// consumeBatchIntGroup, with the composite key held inline in the hash
// entry: ONE probe per row resolves the group (or mints it), against the
// dual-int predecessor's Get-then-Put plus a chain walk across three
// separate per-group arrays.
func (h *HashAggregate) consumeBatchPackedGroup(b *batch.RecordBatch) {
	// Resolve each key column's typed slice and its fixed slot in the key
	// once per batch.
	cols := h.packedCols[:0]
	hasNulls := false
	for ci, colIdx := range h.groupColIdx {
		v := b.Columns[colIdx]
		f := h.packedLayout[ci]
		pc := packedKeyCol{nulls: &v.Nulls, word: f.word, shift: f.shift}
		if f.i32 {
			pc.i32 = v.Int32Data
		} else {
			pc.i64 = v.Int64Data
		}
		cols = append(cols, pc)
		hasNulls = hasNulls || v.Nulls.HasNulls()
	}
	h.packedCols = cols

	idx := h.packedIdx
	liveBefore := 0
	if idx != nil {
		liveBefore = idx.Len()
	}

	// Hash once: the router already folded this row's 128-bit key through
	// packedHash to choose the owner. Accept its array only when the plan's
	// key layout is identical to this aggregate's, so the lo/hi words the
	// router packed are the ones this loop packs.
	var ph []uint64
	if p := h.provPlan; p != nil && p.kind == hashKindPacked &&
		samePackedLayout(p.layout, h.packedLayout) && len(h.provHashes) == b.ActiveLen() {
		ph = h.provHashes
		HashOnceRoutedRows.Add(int64(len(ph)))
	}

	// Pre-reserve key capacity for this batch. The flat accumulators grow
	// once, after the lookup loop, to the final group count.
	batchRows := b.ActiveLen()
	h.packedKeys = ensureAppendCap(h.packedKeys, batchRows)

	// Phase 1: hash lookup — one probe per row.
	var gi []int32
	var sel []uint32
	var iterLen int
	hasNullKeys := false

	// hasNulls is defensively false in practice: consumeBatch migrates the
	// whole aggregate to the generic path before a batch with a NULL key
	// column reaches here. Keeping the branch costs one predictable test per
	// row and keeps null-key rows accounted if that ever changes.
	// The routed and self-hashing loops are written out separately rather than
	// branching on ph per row. That branch is perfectly predicted and still
	// measured +4% on this path's own benchmark (two-int64 2.22 -> 2.32 ms,
	// same-window A/B) — at ~4 ns/row a single extra compare is real money, and
	// hoisting it is why the self-hashing loops below are byte-identical to
	// what they were before hash-once landed.
	if tl := h.packedTwoLevel; tl != nil {
		gi, sel, iterLen, hasNullKeys = h.packedPhase1TwoLevel(b, tl, cols, hasNulls, ph)
	} else if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen] // provable bound: si ranges over b.Sel
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && packedRowHasNull(cols, row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				newIdx := int32(h.numIntGroups)
				gsIdx := idx.GetOrInsertNoGrowAt(lo, hi, ph[si], newIdx)
				gi[si] = gsIdx
				if gsIdx == newIdx {
					idx.CheckGrow()
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
		} else {
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && packedRowHasNull(cols, row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				// newIdx is the id this row would claim; the table hands back
				// exactly it when the key was new (see GetOrInsertNoGrow).
				newIdx := int32(h.numIntGroups)
				gsIdx := idx.GetOrInsertNoGrow(lo, hi, newIdx)
				gi[si] = gsIdx
				if gsIdx == newIdx {
					idx.CheckGrow()
					// No per-group state object: this path is gated to simple
					// aggregates, whose state lives entirely in the flat SoA
					// arrays + packedKeys. materializeFlatAccums reifies on
					// demand for the migration/merge cold paths.
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen]
			for row := 0; row < iterLen; row++ {
				if hasNulls && packedRowHasNull(cols, row) {
					gi[row] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				newIdx := int32(h.numIntGroups)
				gsIdx := idx.GetOrInsertNoGrowAt(lo, hi, ph[row], newIdx)
				gi[row] = gsIdx
				if gsIdx == newIdx {
					idx.CheckGrow()
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if hasNulls && packedRowHasNull(cols, row) {
					gi[row] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				newIdx := int32(h.numIntGroups)
				gsIdx := idx.GetOrInsertNoGrow(lo, hi, newIdx)
				gi[row] = gsIdx
				if gsIdx == newIdx {
					idx.CheckGrow()
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
		}
	}

	// Phase 2: per-aggregate typed scatter update using flat arrays.
	h.scatterBatchAggs(h.numIntGroups, b, gi, sel, iterLen)

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

	// Bucketed conversion, decided once per batch at the end — see
	// consumeBatchIntGroup.
	if idx != nil {
		h.maybeConvertPackedIndex(idx.Len() - liveBefore)
	}
}

// packedPhase1TwoLevel is consumeBatchPackedGroup's phase 1 against the
// BUCKETED index (two_level_hash.go) — same key packing, same dense group
// ids, same packedKeys SoA append; only the probe's table differs.
func (h *HashAggregate) packedPhase1TwoLevel(b *batch.RecordBatch, tl *packedTwoLevelTable,
	cols []packedKeyCol, hasNulls bool, ph []uint64) (gi []int32, sel []uint32, iterLen int, hasNullKeys bool) {
	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen]
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && packedRowHasNull(cols, row) {
					gi[si] = -1
					hasNullKeys = true
					continue
				}
				lo, hi := packedKeyAt(cols, row)
				newIdx := int32(h.numIntGroups)
				gsIdx := tl.GetOrInsertAt(lo, hi, ph[si], newIdx)
				gi[si] = gsIdx
				if gsIdx == newIdx {
					h.numIntGroups++
					h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
				}
			}
			return gi, sel, iterLen, hasNullKeys
		}
		for si, selIdx := range b.Sel {
			row := int(selIdx)
			if hasNulls && packedRowHasNull(cols, row) {
				gi[si] = -1
				hasNullKeys = true
				continue
			}
			lo, hi := packedKeyAt(cols, row)
			newIdx := int32(h.numIntGroups)
			gsIdx := tl.GetOrInsertAt(lo, hi, packedHash(lo, hi), newIdx)
			gi[si] = gsIdx
			if gsIdx == newIdx {
				h.numIntGroups++
				h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
			}
		}
		return gi, sel, iterLen, hasNullKeys
	}

	iterLen = b.Len
	gi = h.ensureGroupIndexBuf(iterLen)
	if ph != nil {
		ph = ph[:iterLen]
		for row := 0; row < iterLen; row++ {
			if hasNulls && packedRowHasNull(cols, row) {
				gi[row] = -1
				hasNullKeys = true
				continue
			}
			lo, hi := packedKeyAt(cols, row)
			newIdx := int32(h.numIntGroups)
			gsIdx := tl.GetOrInsertAt(lo, hi, ph[row], newIdx)
			gi[row] = gsIdx
			if gsIdx == newIdx {
				h.numIntGroups++
				h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
			}
		}
		return gi, sel, iterLen, hasNullKeys
	}
	for row := 0; row < iterLen; row++ {
		if hasNulls && packedRowHasNull(cols, row) {
			gi[row] = -1
			hasNullKeys = true
			continue
		}
		lo, hi := packedKeyAt(cols, row)
		newIdx := int32(h.numIntGroups)
		gsIdx := tl.GetOrInsertAt(lo, hi, packedHash(lo, hi), newIdx)
		gi[row] = gsIdx
		if gsIdx == newIdx {
			h.numIntGroups++
			h.packedKeys = append(h.packedKeys, packedKey{lo: lo, hi: hi})
		}
	}
	return gi, sel, iterLen, hasNullKeys
}

// packedRowHasNull reports whether any key column is NULL at row.
func packedRowHasNull(cols []packedKeyCol, row int) bool {
	for i := range cols {
		if cols[i].nulls.IsNullFast(row) {
			return true
		}
	}
	return false
}

// scatterBatchAggs is Phase 2 for every SoA consume path: extend the flat
// accumulators to the batch's final group count in ONE step per array, then
// run one typed scatter pass per aggregate.
//
// The growth used to happen one group at a time inside the hash-probe loop
// (flatAccumArrays.appendGroup, 11 nil-checks + up to 3 appends per NEW group
// PER AGGREGATE). In the near-unique-key regime that is ~33 branch
// evaluations per row for three aggregates, all of it redundant: the arrays
// are append-in-place off-heap slices, so the extension itself is a length
// bump plus a memclr.
//
// Aggregates whose entire accumulator is a SHARED count array (a duplicate
// COUNT over an identical non-null predicate) are skipped — the owner's pass
// already wrote the array they read. SUM/AVG sharers still run, through the
// count-free kernels.
func (h *HashAggregate) scatterBatchAggs(nGroups int, b *batch.RecordBatch, gi []int32, sel []uint32, iterLen int) {
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].growTo(nGroups)
	}
	for i := range h.Aggs {
		fa := &h.intFlatAccs[i]
		ci := h.aggColIdx[i]
		if h.Aggs[i].Func == AggCount && fa.count == nil {
			continue // shared count IS this aggregate's whole state
		}
		if ci >= 0 {
			scatterFlatAggUpdate(fa, gi, h.Aggs[i].Func, b.Columns[ci], sel, iterLen)
		} else if h.Aggs[i].Func == AggCount {
			scatterCountStar(fa.count, gi, iterLen)
		}
	}
}

// appendFlatAccumSlot extends every flat accumulator by exactly one
// zero-initialized slot. Used by the two paths that mint a group outside the
// batch-oriented growth above: processRow (generic per-row insert) and
// strNullGroupSlot (created mid-loop, before the batch's group count is
// known).
func (h *HashAggregate) appendFlatAccumSlot() {
	for ai := range h.intFlatAccs {
		fa := &h.intFlatAccs[ai]
		fa.growTo(flatAccumLen(fa) + 1)
	}
}

// flatAccumLen reports the group-slot length of a flat accumulator. count is
// the natural probe, but MIN/MAX aggregates have none and count-sharing
// aggregates borrow one, so fall back to the first live array.
func flatAccumLen(fa *flatAccumArrays) int {
	switch {
	case fa.count != nil:
		return len(fa.count)
	case fa.sumI64 != nil:
		return len(fa.sumI64)
	case fa.sumF64 != nil:
		return len(fa.sumF64)
	case fa.sumDec != nil:
		return len(fa.sumDec)
	case fa.hasMin != nil:
		return len(fa.hasMin)
	case fa.hasMax != nil:
		return len(fa.hasMax)
	}
	return 0
}

// --- two-level group index plumbing (two_level_hash.go) ------------------
//
// The int and packed key modes hold EITHER a flat table or a bucketed one.
// These helpers are the seam: hot paths hoist the choice out of the row loop
// (see consumeBatchIntGroup), cold paths — merge, spill, accounting — call
// through here so the mode is decided in exactly one place per operation.

// convertsToTwoLevel decides, at the END of a batch, whether a flat index
// should become bucketed. Two conditions:
//
//   - size: live >= twoLevelConvertAt — the measured structural crossover,
//     below which a flat rehash is still a cache-resident scatter and
//     bucketing is overhead (see two_level_hash.go for the curve).
//   - imminent rehash: live + incoming crosses the flat table's 70% load
//     factor. That is the only moment the conversion is free: it rehashes
//     the entries grow() was about to rehash, into the capacity grow() was
//     about to allocate, and the flat doubling then never happens. Anywhere
//     else in the fill the conversion REPLACES NOTHING and the table still
//     owes its doubling — the ≈10:1 overhead-to-benefit ratio the SF100
//     profile found, and the near-unique-key regression it produced (Q18,
//     +87%).
//
// incoming is what the caller is about to insert: the consume path passes
// the new-group count of the batch just finished, as the estimate of the
// next batch's; the merge path passes the incoming aggregate's group count,
// which is exact.
//
// The growth-rate test this replaces (newGroups*4 >= rows, "still filling")
// could not veto the losing bet it was written for: a near-unique key mints
// a group on every row, so it passed unconditionally, on every batch, for
// exactly the shape that pays the most. The load-factor test subsumes its
// real intent — a saturated table adds no groups, so it can never cross,
// so it can never convert.
//
// Both are per-BATCH tests on numbers the consume loop already has; nothing
// here runs per row.
func convertsToTwoLevel(live, slots, incoming int) bool {
	if !twoLevelToggle.On() || live < twoLevelConvertAt {
		return false
	}
	// WADJET_TWO_LEVEL_AT (and the test helper that stands in for it) drops
	// the lookahead so CI corpora, whose tables never reach a doubling
	// either, still exercise the bucketed path — see two_level_hash.go.
	return twoLevelConvertEager || (live+incoming)*10 > slots*7
}

// indexConverts is convertsToTwoLevel with this sink's construction-time
// layout decision in front of it: a bounded sink was born flat and stays
// flat for its whole (single-epoch) life. Every conversion site — consume,
// both SoA merges — goes through here so the rule lives in one place.
func (h *HashAggregate) indexConverts(live, slots, incoming int) bool {
	return !h.indexBornFlat && convertsToTwoLevel(live, slots, incoming)
}

// indexLayoutStaysFlat decides the group-index layout from what the sink
// knows before its first row. Two independent bounds, either of which pins
// the index flat for life; see two_level_hash.go for both rules, their
// derivations and their measurements:
//
//   - the epoch byte cap and the per-group state size give a group ceiling
//     Gmax = C/s, compared against twoLevelBoundedMinGroups (ADR-0014);
//   - the declared input-row bound is compared against
//     twoLevelMinAmortizeRows() — an index that will see fewer rows than
//     that in total has nothing left to repay a conversion with.
//
// Returns whether the index must be flat, the epoch-cap group ceiling
// (0 = no cap — kept for the worker's log line), and which bound decided it.
func (h *HashAggregate) indexLayoutStaysFlat(b *batch.RecordBatch) (flat bool, ceiling int64, why indexFlatReason) {
	if h.epochByteCap > 0 && bornFlatToggle.On() {
		s := h.perGroupStateBytes(b)
		if s <= 0 {
			// No usable estimate. A bounded sink is flat unconditionally
			// then — an index rebuilt every epoch can never amortize a
			// conversion.
			return true, 0, flatReasonEpochCap
		}
		ceiling = h.epochByteCap / int64(s)
		if ceiling < twoLevelBoundedMinGroups {
			return true, ceiling, flatReasonEpochCap
		}
	}
	if h.inputRowBound > 0 && rowBoundToggle.On() &&
		h.inputRowBound < twoLevelMinAmortizeRows() {
		return true, ceiling, flatReasonRowBound
	}
	return false, ceiling, flatNotPinned
}

// indexFlatReason is which of the two construction-time bounds pinned a
// group index flat, as it surfaces through the exported IndexFlatReason
// accessor and, from there, the worker's task logs: the fragment aggregate's
// "fragment task phases" line carries it as agg_layout/agg_layout_reason
// (executor_fragment.go's aggregateLayoutReporter); the shuffle partial
// agg's "shuffle partial agg" line carries only the born_flat bool, since
// that path never sets an input-row bound and so is always epoch-cap when
// pinned (executor.go).
type indexFlatReason uint8

const (
	flatNotPinned indexFlatReason = iota
	flatReasonEpochCap
	flatReasonRowBound
)

func (r indexFlatReason) String() string {
	switch r {
	case flatReasonEpochCap:
		return "epoch-cap"
	case flatReasonRowBound:
		return "row-bound"
	}
	return ""
}

// perGroupStateBytes is a LOWER bound on the bytes one group adds to
// groupMemoryUsage on the int/packed SoA fast paths — the only paths that
// can hold a two-level index. A lower bound gives an UPPER bound on the
// group ceiling, which is the safe direction: the layout is pinned flat only
// when even the most optimistic group count stays under G*.
//
// The terms mirror what resolveIndices and initFlatAccums actually allocate:
// one hash-table entry per group at the load-factor ceiling, the key SoA,
// and one flat accumulator array element per aggregate. Anything charged
// beyond these (probe slack below 70 % load, the group-state pool, generic
// key mirrors) only makes the real per-group cost larger, i.e. the real
// ceiling smaller.
func (h *HashAggregate) perGroupStateBytes(b *batch.RecordBatch) int {
	// Index: 16-byte entries (intHashEntry; packedHashEntry is 32, charged
	// as 16 to keep this a lower bound for both) at the 70 % load factor.
	// The bucketed form splits the same slot count 256 ways, so this term is
	// identical for either layout.
	n := 16 * 10 / 7
	// Key SoA: one int64 for the single-int path, one 128-bit packed key for
	// the composite path.
	if len(h.GroupByCols) >= 2 {
		n += 16
	} else {
		n += 8
	}
	needsCount := false
	for i, agg := range h.Aggs {
		if aggNeedsCount(agg.Func) {
			needsCount = true
		}
		ci := -1
		if i < len(h.aggColIdx) {
			ci = h.aggColIdx[i]
		}
		if ci < 0 || ci >= len(b.Columns) {
			continue // COUNT(*): the shared count array below is all it takes
		}
		switch agg.Func {
		case AggSum, AggAvg, AggMin, AggMax:
			if b.Columns[ci].Type == batch.TypeDecimal {
				n += 16 // Int128 sum/min/max
			} else {
				n += 8 // int64 / float64
			}
			if agg.Func == AggMin || agg.Func == AggMax {
				n++ // hasMin / hasMax
			}
		}
	}
	if needsCount {
		n += 8 // one shared count[] array (planCountArrays folds the rest)
	}
	return n
}

// maybeConvertIntIndex converts the flat single-int index to the bucketed
// form. Called at the end of a batch, so the consume loop's hoisted table
// pointer stays valid for the whole batch and the conversion applies from
// the next one.
func (h *HashAggregate) maybeConvertIntIndex(newGroups int) {
	idx := h.intGroupIndex
	if idx == nil || !h.indexConverts(idx.Len(), idx.Slots(), newGroups) {
		return
	}
	h.intTwoLevel = convertIntHashTableToTwoLevel(idx, h.offheapReg())
	h.intGroupIndex = nil
	h.indexConversions++
	TwoLevelConversions.Add(1)
}

// maybeConvertPackedIndex is maybeConvertIntIndex for the packed composite
// key mode.
func (h *HashAggregate) maybeConvertPackedIndex(newGroups int) {
	idx := h.packedIdx
	if idx == nil || !h.indexConverts(idx.Len(), idx.Slots(), newGroups) {
		return
	}
	h.packedTwoLevel = convertPackedHashTableToTwoLevel(idx, h.offheapReg())
	h.packedIdx = nil
	h.indexConversions++
	TwoLevelConversions.Add(1)
}

// intIndexLen reports the live entry count of whichever int index is active.
func (h *HashAggregate) intIndexLen() int {
	if h.intTwoLevel != nil {
		return h.intTwoLevel.Len()
	}
	if h.intGroupIndex != nil {
		return h.intGroupIndex.Len()
	}
	return 0
}

// intIndexPresent reports whether this aggregate holds a single-int index.
func (h *HashAggregate) intIndexPresent() bool {
	return h.intGroupIndex != nil || h.intTwoLevel != nil
}

// intIndexForEach iterates the active int index's live entries.
func (h *HashAggregate) intIndexForEach(fn func(key int64, val int32)) {
	if h.intTwoLevel != nil {
		h.intTwoLevel.ForEach(fn)
		return
	}
	if h.intGroupIndex != nil {
		h.intGroupIndex.ForEach(fn)
	}
}

// intIndexDelete removes a key from the active int index (partial-drain slot
// reclaim).
func (h *HashAggregate) intIndexDelete(key int64) {
	if h.intTwoLevel != nil {
		h.intTwoLevel.Delete(key)
		return
	}
	if h.intGroupIndex != nil {
		h.intGroupIndex.Delete(key)
	}
}

// intIndexGetOrInsert is the cold-path (merge) insert into the active int
// index.
func (h *HashAggregate) intIndexGetOrInsert(key int64, val int32) (int32, bool) {
	if h.intTwoLevel != nil {
		return h.intTwoLevel.GetOrInsert(key, val)
	}
	return h.intGroupIndex.GetOrInsert(key, val)
}

// packedIndexGetOrInsert is the cold-path (merge) insert into the active
// packed index.
func (h *HashAggregate) packedIndexGetOrInsert(lo, hi uint64, val int32) (int32, bool) {
	if h.packedTwoLevel != nil {
		return h.packedTwoLevel.GetOrInsert(lo, hi, val)
	}
	return h.packedIdx.GetOrInsert(lo, hi, val)
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

	// Pre-reserve key/state capacity. The flat accumulators grow once, after
	// the lookup loop, to the final group count.
	batchRows := b.ActiveLen()
	h.intGroupStates = ensureAppendCap(h.intGroupStates, batchRows)
	h.compactKeys = ensureAppendCap(h.compactKeys, batchRows)
	h.keys = ensureAppendCap(h.keys, batchRows)

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
		h.numIntGroups++
		h.compactKeys = append(h.compactKeys, string(h.keyBuf))
		h.compactKeyBytes += int64(len(h.keyBuf))
		h.keys = append(h.keys, keyVals)
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
			newIdx := int32(h.numIntGroups)
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
			newIdx := int32(h.numIntGroups)
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
	h.scatterBatchAggs(h.numIntGroups, b, gi, sel, iterLen)
}

// compactFallback handles the case where a compact key exceeds 8 bytes.
// Scatters the already-indexed rows via Phase 2, materializes SoA accumulators,
// migrates to generic path, and processes remaining rows per-row.
func (h *HashAggregate) compactFallback(b *batch.RecordBatch, gi []int32, sel []uint32, fallbackAt int, totalRows int) {
	// Phase 2 for rows already indexed.
	if fallbackAt > 0 {
		truncSel := sel
		if truncSel != nil {
			truncSel = truncSel[:fallbackAt]
		}
		h.scatterBatchAggs(h.numIntGroups, b, gi[:fallbackAt], truncSel, fallbackAt)
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

// arenaString builds a string header over hash-table arena bytes without
// copying them. Sound ONLY for bytes handed out by strHashTable's chunked key
// arena: those chunks are append-only, are never reallocated, and are never
// written again once a key lands in them (see str_hash.go), so the bytes
// behind the header can never change. The GC keeps the chunk alive through the
// header's interior pointer, so the string also outlives the table itself —
// which is what the emit, spill, drain-cursor and merge readers of
// serializedKeys rely on after Next() drops strGroupIndex.
func arenaString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// consumeBatchStrGroup is the fast path for single-column string GROUP BY.
// Uses strHashTable for group lookup with SoA flat accumulator scatter.
// Two-phase approach matches consumeBatchIntGroup: hash lookup then typed scatter.
//
// Group keys are stored ONCE, in the hash table's key arena: serializedKeys
// entries alias those bytes (arenaString) instead of holding a private copy.
// Their bytes are therefore accounted by strGroupIndex.MemoryUsage(), not by
// h.serializedKeyBytes — bumping the counter here would double-charge them.
func (h *HashAggregate) consumeBatchStrGroup(b *batch.RecordBatch) {
	gkVec := b.Columns[h.strGroupKeyCol]
	hasNulls := gkVec.Nulls.HasNulls()
	strIdx := h.strGroupIndex

	// Hash once: the router already ran strHash over these bytes to pick the
	// owner — the expensive half of the probe on ClickBench Q34's ~88-byte
	// URLs. Reusing it also supplies strEntry.hashTag, which is its low half.
	var ph []uint64
	if p := h.provPlan; p != nil && p.kind == hashKindStr && len(h.provHashes) == b.ActiveLen() {
		ph = h.provHashes
		HashOnceRoutedRows.Add(int64(len(ph)))
	}

	// Pre-reserve key/state capacity (same rationale as consumeBatchIntGroup);
	// the flat accumulators grow once after the lookup loop.
	batchRows := b.ActiveLen()
	h.strGroupStates = ensureAppendCap(h.strGroupStates, batchRows)
	h.serializedKeys = ensureAppendCap(h.serializedKeys, batchRows)

	// Phase 1: Hash lookup — build group index array. NULL keys get their
	// own first-class group slot (strNullGroupIdx) so the typed scatter in
	// Phase 2 updates its flat accumulators like any other group. The
	// previous shape diverted null-key rows to processRow, which appends a
	// groupState WITHOUT a flat-accumulator slot — strGroupStates and
	// intFlatAccs went out of alignment and the NULL group emitted zeroed
	// aggregates (and a 1-byte "\x01" string key could collide with the
	// binary null sentinel in the shared hash table).
	var gi []int32
	var sel []uint32
	var iterLen int

	if b.Sel != nil {
		iterLen = len(b.Sel)
		sel = b.Sel
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen] // provable bound: si ranges over b.Sel
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[si] = h.strNullGroupSlot()
					continue
				}
				key := gkVec.BytesData.Value(row)
				gsIdx, found, stored := strIdx.GetOrInsertRefAt(key, ph[si], int32(len(h.strGroupStates)))
				if found {
					gi[si] = gsIdx
				} else {
					h.strGroupStates = append(h.strGroupStates, nil)
					h.serializedKeys = append(h.serializedKeys, arenaString(stored))
					gi[si] = gsIdx
				}
			}
		} else {
			for si, selIdx := range b.Sel {
				row := int(selIdx)
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[si] = h.strNullGroupSlot()
					continue
				}
				key := gkVec.BytesData.Value(row)
				gsIdx, found, stored := strIdx.GetOrInsertRef(key, int32(len(h.strGroupStates)))
				if found {
					gi[si] = gsIdx
				} else {
					// Deferred state: the serializedKeys entry IS the key, and
					// it aliases the table's arena copy rather than making a
					// second one (arenaString). Simple aggs live in the flat
					// SoA arrays. The per-group groupState + []any box +
					// h.keys slot were ~100B of overhead per group (Q34's 18M
					// URL groups).
					h.strGroupStates = append(h.strGroupStates, nil)
					h.serializedKeys = append(h.serializedKeys, arenaString(stored))
					gi[si] = gsIdx
				}
			}
		}
	} else {
		iterLen = b.Len
		gi = h.ensureGroupIndexBuf(iterLen)
		if ph != nil {
			ph = ph[:iterLen]
			for row := 0; row < iterLen; row++ {
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[row] = h.strNullGroupSlot()
					continue
				}
				key := gkVec.BytesData.Value(row)
				gsIdx, found, stored := strIdx.GetOrInsertRefAt(key, ph[row], int32(len(h.strGroupStates)))
				if found {
					gi[row] = gsIdx
				} else {
					h.strGroupStates = append(h.strGroupStates, nil)
					h.serializedKeys = append(h.serializedKeys, arenaString(stored))
					gi[row] = gsIdx
				}
			}
		} else {
			for row := 0; row < iterLen; row++ {
				if hasNulls && gkVec.Nulls.IsNullFast(row) {
					gi[row] = h.strNullGroupSlot()
					continue
				}
				key := gkVec.BytesData.Value(row)
				gsIdx, found, stored := strIdx.GetOrInsertRef(key, int32(len(h.strGroupStates)))
				if found {
					gi[row] = gsIdx
				} else {
					// Deferred state: the serializedKeys entry IS the key, and
					// it aliases the table's arena copy rather than making a
					// second one (arenaString). Simple aggs live in the flat
					// SoA arrays. The per-group groupState + []any box +
					// h.keys slot were ~100B of overhead per group (Q34's 18M
					// URL groups).
					h.strGroupStates = append(h.strGroupStates, nil)
					h.serializedKeys = append(h.serializedKeys, arenaString(stored))
					gi[row] = gsIdx
				}
			}
		}
	}

	// Phase 2: Per-aggregate typed scatter update using flat arrays.
	h.scatterBatchAggs(len(h.strGroupStates), b, gi, sel, iterLen)

}

// strNullGroupSlot returns the NULL-key group's flat-accumulator slot for
// the single-string fast path, creating it on first use. The group lives in
// strGroupStates with an aligned slot in every flat accumulator, but is
// deliberately NOT inserted into strGroupIndex — a raw 1-byte string key
// could otherwise collide with any in-band null sentinel. serializedKeys
// gets the generic binary form (single 0x01 null flag) so spill/merge
// round-trips distinguish the NULL group from every real string.
func (h *HashAggregate) strNullGroupSlot() int32 {
	if h.strNullGroupIdx >= 0 {
		return h.strNullGroupIdx
	}
	gs := h.gsPool.alloc()
	gs.ensureExtras().keyValues = []any{nil}
	h.strNullGroupIdx = int32(len(h.strGroupStates))
	h.strGroupStates = append(h.strGroupStates, gs)
	h.keys = append(h.keys, []any{nil})
	h.serializedKeys = append(h.serializedKeys, "\x01")
	h.serializedKeyBytes++
	h.appendFlatAccumSlot()
	return h.strNullGroupIdx
}

// mergeNullGroupSlot resolves the destination slot for another sink's
// NULL-key group during the in-memory merge, returning (slot, found).
//
// The single-string fast path keeps its NULL group OUT of strGroupIndex on
// purpose (strNullGroupSlot: that table stores raw string keys, so a real
// one-byte "\x01" value would land on the sentinel), which means the merge
// loop's table probe can never match it. Every merge therefore appended one
// more NULL group and GROUP BY over a nullable string reported the NULL row
// once per parallel partial instead of once — the split-group half of #338.
// GROUP BY treats all NULLs as one group, so the match has to run through
// strNullGroupIdx here.
func (h *HashAggregate) mergeNullGroupSlot(key string, newIdx int32) (int32, bool) {
	if h.strNullGroupIdx >= 0 {
		return h.strNullGroupIdx, true
	}
	// A generic-path destination carries its NULL key in the table like any
	// other key (the binary encoding of a single NULL column IS "\x01"), so
	// there the ordinary probe is the correct match.
	if !h.useStrGroupKey {
		return h.strGroupIndex.GetOrInsert([]byte(key), newIdx)
	}
	// String-path destination with no NULL group yet: the incoming one
	// becomes it, so the next merge matches instead of appending again.
	h.strNullGroupIdx = newIdx
	return newIdx, false
}

// consumeBatchGenericSoA is the SoA fast path for multi-column GROUP BY
// that doesn't fit int/packed/compact/single-string paths.
// Uses binary key serialization into strHashTable with two-phase scatter.
// Phase 1: Serialize keys, hash lookup, build group index array.
// Phase 2: Per-aggregate typed scatter update (one pass per agg, no per-row dispatch).
func (h *HashAggregate) consumeBatchGenericSoA(b *batch.RecordBatch) {
	strIdx := h.strGroupIndex

	batchRows := b.ActiveLen()
	h.strGroupStates = ensureAppendCap(h.strGroupStates, batchRows)
	h.serializedKeys = ensureAppendCap(h.serializedKeys, batchRows)
	if !h.deferGenericKeyBoxing {
		h.keys = ensureAppendCap(h.keys, batchRows)
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
		gs := h.gsPool.alloc()
		if !h.deferGenericKeyBoxing {
			keyVals := make([]any, len(h.GroupByCols))
			for ki, idx := range h.groupColIdx {
				if idx >= 0 {
					keyVals[ki] = b.Columns[idx].GetValue(row)
				}
			}
			gs.ensureExtras().keyValues = keyVals
			h.keys = append(h.keys, keyVals)
		}
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
		h.serializedKeyBytes += int64(len(h.keyBuf))
	}

	if h.deferGenericKeyBoxing {
		// Typed lookup: hash and chain-verify straight off the typed
		// column storage; serialize only on insert. strGroupIndex is not
		// maintained here — serializedKeys + genKeyIdx/genKeyNext carry
		// the identity (see ensureStrGroupIndexForMerge for the one
		// consumer that still needs the string table).
		h.keySerCols = buildKeySerCols(h.keySerCols, b, h.groupColIdx, h.groupColTypes)
		kcols := h.keySerCols
		if h.genKeyIdx == nil {
			h.genKeyIdx = newIntHashTable(4096)
		}
		h.genKeyNext = ensureAppendCap(h.genKeyNext, batchRows)

		newTypedGroup := func(row int, chainHead int32) int32 {
			newIdx := int32(len(h.strGroupStates))
			// nil state — simple aggs live entirely in the flat SoA
			// arrays + serializedKeys; materializeFlatAccums reifies on
			// the migration/merge cold paths (same deferral as packed keys:
			// 32B x groups of pure overhead otherwise).
			h.strGroupStates = append(h.strGroupStates, nil)
			h.keyBuf = serializeGroupKey(h.keyBuf[:0], kcols, row)
			h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
			h.serializedKeyBytes += int64(len(h.keyBuf))
			h.genKeyNext = append(h.genKeyNext, chainHead)
			return newIdx
		}
		lookup := func(row int) int32 {
			ck := int64(typedRowHash(kcols, row))
			if head, ok := h.genKeyIdx.Get(ck); ok {
				for g := head; g >= 0; g = h.genKeyNext[g] {
					if serializedKeyMatchesRow(h.serializedKeys[g], kcols, row) {
						return g
					}
				}
				newIdx := newTypedGroup(row, head)
				h.genKeyIdx.Put(ck, newIdx)
				return newIdx
			}
			newIdx := newTypedGroup(row, -1)
			h.genKeyIdx.Put(ck, newIdx)
			return newIdx
		}

		if b.Sel != nil {
			iterLen = len(b.Sel)
			sel = b.Sel
			gi = h.ensureGroupIndexBuf(iterLen)
			for si, selIdx := range b.Sel {
				gi[si] = lookup(int(selIdx))
			}
		} else {
			iterLen = b.Len
			gi = h.ensureGroupIndexBuf(iterLen)
			for row := 0; row < iterLen; row++ {
				gi[row] = lookup(row)
			}
		}
	} else if b.Sel != nil {
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
	h.scatterBatchAggs(len(h.strGroupStates), b, gi, sel, iterLen)
}

// ensureStrGroupIndexForMerge rebuilds the string hash table from
// serializedKeys when the typed generic path (which doesn't maintain it)
// produced the groups. Cold path: only the slow in-memory merge fallback
// needs the string table.
func (h *HashAggregate) ensureStrGroupIndexForMerge() {
	if h.useGenericSoA && h.deferGenericKeyBoxing {
		idx := newStrHashTable(len(h.strGroupStates) + 16)
		for i, k := range h.serializedKeys {
			idx.Put([]byte(k), int32(i))
		}
		h.strGroupIndex = idx
		return
	}
	// A sink that never resolved a key path has no table at all (Init no
	// longer pre-builds one so resolveIndices' NDV pre-size can take
	// effect); the dedup loop that follows probes it unconditionally.
	if h.strGroupIndex == nil {
		h.strGroupIndex = newStrHashTable(len(h.strGroupStates) + 16)
	}
}

// migrateCompactToGeneric moves all groups from intHashTable to the string map
// when compact mode cannot handle a key that exceeds 8 bytes.
func (h *HashAggregate) migrateCompactToGeneric() {
	h.useCompactGroupKey = false
	h.strGroupIndex = newStrHashTable(h.numIntGroups)
	h.strGroupStates = make([]*groupState, 0, h.numIntGroups)
	for i, gs := range h.intGroupStates {
		key := h.compactKeys[i]
		h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, key)
		h.serializedKeyBytes += int64(len(key))
	}
	h.intGroupStates = nil
	h.numIntGroups = 0
	h.intGroupIndex = nil
	h.compactKeys = nil
	h.compactKeyBytes = 0
}

// packKeyInt64 interprets up to 8 bytes as a little-endian int64.
func packKeyInt64(b []byte) int64 {
	var v int64
	for i := 0; i < len(b); i++ {
		v |= int64(b[i]) << uint(i*8)
	}
	return v
}

// strIndexForRow returns the string group index used by the generic per-row
// paths, creating it on first use. Those paths are reached without going
// through resolveIndices' pre-sized branches (spill replay, grouping sets on
// a sink that never resolved a fast path), so they own the fallback
// construction now that Init no longer hands out a fixed 4096-slot table.
func (h *HashAggregate) strIndexForRow() *strHashTable {
	if h.strGroupIndex == nil {
		h.strGroupIndex = newStrHashTable(4096)
	}
	return h.strGroupIndex
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
	groupIdx, found := h.strIndexForRow().GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
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
	h.extrasAccsCount += int64(len(h.Aggs))
	if h.needsDistinct {
		ext.distinctSets = make([]*distinctSet, len(h.Aggs))
	}
	if h.needsExtra {
		ext.extraState = make([]any, len(h.Aggs))
		h.extraStateBytes += int64(len(h.Aggs)) * 80
	}
	h.initGroupState(ext, b)
	h.strGroupStates = append(h.strGroupStates, gs)
	h.keys = append(h.keys, keyVals)
	h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
	h.serializedKeyBytes += int64(len(h.keyBuf))

	// Keep the SoA flat accumulator length in sync with strGroupStates.
	// processRow is called from the null-key branch of consumeBatchStrGroup
	// (and other str-group paths) AFTER the SoA fast paths are configured;
	// without this extra slot, the hash table maps a new key to gsIdx N but
	// fa.count is still len N, so the next batch's scatterCountStar indexes
	// out of bounds. Manifested as Q21 SF1's mysterious panic.
	h.appendFlatAccumSlot()

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

		groupIdx, found := h.strIndexForRow().GetOrInsert(h.keyBuf, int32(len(h.strGroupStates)))
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
		h.extrasAccsCount += int64(len(h.Aggs))
		if h.needsDistinct {
			ext.distinctSets = make([]*distinctSet, len(h.Aggs))
		}
		if h.needsExtra {
			ext.extraState = make([]any, len(h.Aggs))
			h.extraStateBytes += int64(len(h.Aggs)) * 80
		}
		h.initGroupState(ext, b)
		h.strGroupStates = append(h.strGroupStates, gs)
		h.keys = append(h.keys, keyVals)
		h.serializedKeys = append(h.serializedKeys, string(h.keyBuf))
		h.serializedKeyBytes += int64(len(h.keyBuf))
		h.updateGroup(gs, b, row)
	}
}

// initGroupState fills a freshly allocated group's distinct sets and
// extraState, one entry per aggregate. Shared by processRow and
// processRowGroupingSets: they used to carry a copy each, and a copy that
// falls behind hands updateGroup a nil state to type-assert (the panic
// AggVarState hit on its first distributed run).
//
// Callers must have allocated ext.distinctSets / ext.extraState already —
// whether either is needed is decided once, at resolution, by
// h.needsDistinct / h.needsExtra.
func (h *HashAggregate) initGroupState(ext *groupStateExtras, b *batch.RecordBatch) {
	for i, agg := range h.Aggs {
		switch agg.Func {
		case AggCountDistinct, AggApproxDistinct:
			if ext.distinctSets != nil {
				ext.distinctSets[i] = newDistinctSetFor(h.distinctColType(b, i))
				h.distinctBytes += 48
			}
		case AggStringAgg:
			sep := agg.Separator
			if sep == "" {
				sep = ","
			}
			ext.extraState[i] = &stringAggState{sep: sep}
		case AggStddev, AggVariance, AggStddevPop, AggVarPop,
			AggVarState, AggVarStateMerge:
			ext.extraState[i] = &varianceState{}
		case AggBoolAnd, AggBoolOr:
			// Deliberately left nil: the state is seeded by the first
			// non-NULL input (updateGroup), so a group that never sees one
			// finalizes to NULL per SQL — an eager identity value (true for
			// AND) answered `true` over an all-NULL group.
		case AggCorr, AggCovarSamp, AggCovarPop,
			AggCovarState, AggCovarStateMerge:
			ext.extraState[i] = &covarianceState{}
		case AggPercentileCont, AggPercentileDisc, AggMode, AggMedian:
			ext.extraState[i] = &collectState{}
		case AggMinBy:
			ext.extraState[i] = &minMaxByState{isMin: true}
		case AggMaxBy:
			ext.extraState[i] = &minMaxByState{isMin: false}
		case AggMin, AggMax:
			// Only the container form takes an extraState; scalar MIN/MAX
			// stay on the Accumulator, whose slot is already allocated.
			if i < len(h.aggBoxedMinMax) && h.aggBoxedMinMax[i] {
				ext.extraState[i] = &containerMinMaxState{isMin: agg.Func == AggMin}
			}
		}
	}
}

// updateGroup updates a group's accumulators with values from a single row.
// updateGroup is only ever called on groups whose extras was allocated by
// processRow / processRowGroupingSets, so gs.extras is non-nil here and we
// bind it to a local for compactness.
func (h *HashAggregate) updateGroup(gs *groupState, b *batch.RecordBatch, row int) {
	ext := gs.extras
	for i, agg := range h.Aggs {
		// MIN/MAX over a container is the one aggregate whose FUNC does not
		// decide its path — the input type does. Kept ahead of the switch,
		// behind one bool, so the ordinary MIN/MAX arm below stays exactly
		// what it was for every scalar type (#426).
		if h.hasBoxedMinMax && i < len(h.aggBoxedMinMax) && h.aggBoxedMinMax[i] {
			idx := h.aggColIdx[i]
			// extraState can be short on a state adopted from a clone that
			// resolved differently; a missing slot means no value observed,
			// which finalizes NULL — never an index panic on the emit path.
			if idx < 0 || i >= len(ext.extraState) {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			if st, ok := ext.extraState[i].(*containerMinMaxState); ok {
				before := st.memBytes()
				st.observe(v, row)
				h.extraStateBytes += st.memBytes() - before
			}
			continue
		}
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
			if ds := ext.distinctSets[i]; ds.ints != nil {
				if ds.addInt(intColValue(v, row)) {
					h.distinctBytes += 16
				}
			} else {
				h.keyBuf = appendColumnValue(h.keyBuf[:0], v, row, v.Type)
				// addStr probes zero-copy and copies into the set's arena
				// only on insert — no per-row string allocation.
				if ds.addStr(h.keyBuf) {
					h.distinctBytes += int64(len(h.keyBuf)) + 48
				}
			}

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

		case AggBoolAnd, AggBoolOr:
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
			// The first non-NULL input seeds the state; nil means "no
			// input yet" so an all-NULL group finalizes to NULL (SQL's
			// rule). mergeSinkState and the finalize write share the
			// convention.
			if current, ok := ext.extraState[i].(bool); ok {
				if agg.Func == AggBoolAnd {
					ext.extraState[i] = current && boolVal
				} else {
					ext.extraState[i] = current || boolVal
				}
			} else {
				ext.extraState[i] = boolVal
			}

		case AggStddev, AggVariance, AggStddevPop, AggVarPop, AggVarState:
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

		case AggVarStateMerge:
			// One input row is one upstream partial's encoded (count, mean,
			// M2) triple. Combining them pairwise is the whole point of the
			// column: re-aggregating finished STDDEV values here would be
			// the standard deviation of a handful of near-identical numbers.
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			s, ok := v.GetValue(row).(string)
			if !ok {
				continue
			}
			if partial, ok := decodeVarianceState(s); ok {
				ext.extraState[i].(*varianceState).merge(&partial)
			}

		case AggApproxDistinct:
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			if ds := ext.distinctSets[i]; ds.ints != nil {
				if ds.addInt(intColValue(v, row)) {
					h.distinctBytes += 16
				}
			} else {
				h.keyBuf = appendColumnValue(h.keyBuf[:0], v, row, v.Type)
				// addStr probes zero-copy and copies into the set's arena
				// only on insert — no per-row string allocation.
				if ds.addStr(h.keyBuf) {
					h.distinctBytes += int64(len(h.keyBuf)) + 48
				}
			}

		case AggCovarStateMerge:
			// One input row is one upstream partial's encoded sextuple —
			// combined pairwise, never re-correlated. A CORR of per-task
			// CORR values is the correlation of a handful of numbers that
			// have nothing to do with the question.
			idx := h.aggColIdx[i]
			if idx < 0 {
				continue
			}
			v := b.Columns[idx]
			if v.Nulls.IsNullFast(row) {
				continue
			}
			s, ok := v.GetValue(row).(string)
			if !ok {
				continue
			}
			if partial, ok := decodeCovarianceState(s); ok {
				ext.extraState[i].(*covarianceState).merge(&partial)
			}

		case AggCorr, AggCovarSamp, AggCovarPop, AggCovarState:
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
				(state.isMin && kernel.CompareFloat64(cmpVal, state.bestCmp) < 0) ||
				(!state.isMin && kernel.CompareFloat64(cmpVal, state.bestCmp) > 0) {
				before := boxContainerMemBytes(state.bestVal)
				state.hasValue = true
				state.bestCmp = cmpVal
				state.bestVal = v1.GetValue(row)
				h.extraStateBytes += boxContainerMemBytes(state.bestVal) - before
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
	// A container group key (or a container agg input) boxes as []any /
	// map[string]any / []float32, which SpillRows can only render to display
	// text — a string batch.FromRows then refuses to write back into a
	// container vector (#611). Encode those boxes to the lossless ADR-0023
	// container codec first; Finalize's read loop decodes them symmetrically.
	// The buffer is discarded right after this flush, so the in-place rewrite
	// is safe.
	encodeContainerColsForSpill(h.spillBuffer, h.inputSchema)
	path, err := h.Spill.SpillRows(h.spillBuffer)
	if err != nil {
		return err
	}
	h.spillFiles = append(h.spillFiles, path)
	RawRowSpillFiles.Add(1)
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
	if h.unregisterAccounted != nil {
		h.accState.Store(int32(memory.OpClosed))
		h.unregisterAccounted()
		h.unregisterAccounted = nil
	}

	// Legacy raw-row spill re-aggregation. This MUST run before the
	// partial-merge dispatch below: an aggregate that changes group-key
	// paths mid-stream AFTER partial-state runs already exist (post-spill
	// MergeSink migration to the generic map, null-group-key demotion)
	// loses canUseExternalMerge, so later pressure consumes buffer raw rows
	// into spillFiles/spillBuffer while partialSpillFiles still holds the
	// earlier runs. Re-consuming the raw rows into the in-memory table here
	// folds them into the remainder that finalizeViaPartialMerge merges.
	// The previous ordering returned early on partialSpillFiles and
	// silently orphaned the legacy files (rows dropped from the output).
	if len(h.spillFiles) > 0 || len(h.spillBuffer) > 0 {
		// Re-process spilled input rows through the same aggregate logic.
		// This is correct for all aggregate functions because we're
		// processing raw input, not merging partial results.
		for _, f := range h.spillFiles {
			rows, err := memory.ReadSpilledRows(f)
			if err != nil {
				return err
			}
			// A consumed file is done for good — unlink it here rather than
			// leaving it for SpillManager.Cleanup, which the shared
			// (worker-injected) manager path never calls (#324). Files not
			// yet reached when an error aborts this loop stay in
			// h.spillFiles for Close's backstop removal.
			h.Spill.RemoveSpilled(f)
			if len(rows) == 0 {
				continue
			}
			// Reverse flushSpillBuffer's container encoding: rebuild the box
			// GetValue produced before batch.FromRows writes it (#611).
			if err := decodeContainerColsFromSpill(rows, h.inputSchema); err != nil {
				return err
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
				if err := h.resolveIndices(b); err != nil {
					return err
				}
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
				if err := h.resolveIndices(b); err != nil {
					return err
				}
			}
			h.consumeBatch(b)
			if h.Spill != nil {
				h.Spill.ReleaseTracking(h.spillBufferBytes)
			}
			h.spillBuffer = nil
			h.spillBufferBytes = 0
		}
	}

	// External-merge path: when partial-state files exist, k-way merge them
	// (plus any in-memory remainder, which now includes any re-aggregated
	// legacy spill from above) instead of re-aggregating raw rows. This
	// is the SF100+ unblock: the legacy raw-row Finalize re-reads ALL spilled
	// input back into the hash table and re-runs Consume on it, blowing the
	// budget when input >> output. Partial-state merge processes each group
	// once and is bounded by the merged result size.
	if len(h.partialSpillFiles) > 0 {
		if err := h.finalizeViaPartialMerge(); err != nil {
			return err
		}
	}
	// Partitioned-disjoint adoption: each adopted partition finalizes its
	// own state (including any drained runs it produced).
	for _, ap := range h.adoptedPartitions {
		if err := ap.Finalize(context.Background()); err != nil {
			return fmt.Errorf("finalizing adopted partition: %w", err)
		}
	}
	return nil
}

// Close releases any tracker reservation HashAggregate still holds for
// group-state memory and buffered-but-unspilled rows. Without this, a
// non-spilling HashAggregate accumulates a phantom reservation in the
// shared tracker for the lifetime of the process; see HashJoin.Close for
// the full background.
func (h *HashAggregate) Close() error {
	// Stop the parallel drain FIRST and wait for it: its goroutines own the
	// adopted partitions (each closes its own unit, and with it that unit's
	// off-heap registry) and read this aggregate's own SoA arrays. Nothing
	// below may run while a drain goroutine is still live.
	if h.emit != nil {
		h.emit.shutdown()
		h.emit = nil
	}
	for _, ap := range h.adoptedPartitions {
		ap.Close()
	}
	h.adoptedPartitions = nil
	if h.unregisterAccounted != nil {
		h.accState.Store(int32(memory.OpClosed))
		h.unregisterAccounted()
		h.unregisterAccounted = nil
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
		// Legacy raw-row spill files never consumed by Finalize (error or
		// early-cancel path). On the shared-manager path nothing else
		// removes them (#324); on the owned path this merely beats
		// SpillManager.Cleanup to the unlink.
		for _, f := range h.spillFiles {
			h.Spill.RemoveSpilled(f)
		}
		h.spillFiles = nil
	}
	// Close the streaming merger (if any) and remove its spill files. This
	// is the backstop for early-termination paths (cancellation, error
	// before exhaustion); the normal drain in Next() also calls
	// closePartialMerger when the merger returns nil.
	h.closePartialMerger()
	// Clone-partial runs never handed to a primary (error path where the
	// barrier merge did not run) would otherwise leak on the spill volume.
	for _, path := range h.drainedRuns {
		os.Remove(path)
	}
	h.drainedRuns = nil
	h.spillBuffer = nil
	// Unmap off-heap state LAST: every slice referencing the reservations
	// (flat accs, key SoAs — including any adopted from merged clones) is
	// dropped above or dead with this instance. Drop the slice headers
	// explicitly so a use-after-Close is a nil-index panic rather than a
	// fault into unmapped memory.
	if h.offheap != nil {
		h.intFlatAccs = nil
		h.packedKeys = nil
		h.intKeys = nil
		h.offheap.Close()
		h.offheap = nil
	}
	h.closeRetiredOffheap()
	return nil
}

// Inspect implements memory.AccountedOperator. Wait-free w.r.t. the registry
// but takes h.mu to read group-state fields consistently.
func (h *HashAggregate) Inspect() memory.OperatorFootprint {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := memory.OpState(h.accState.Load())
	if st == memory.OpClosed || h.Spill == nil || !h.canUseExternalMerge() {
		return memory.OperatorFootprint{
			State: memory.OpClosed, InstanceID: h.accInstanceID, Name: h.accName(),
		}
	}
	owned := h.trackedGroupMem
	return memory.OperatorFootprint{
		OwnedBytes:     owned,
		RetainedBytes:  owned, // all group state is detained
		SpillableBytes: h.spillableBytesLocked(),
		SpillReadBytes: h.spillReadBytesLocked(),
		State:          st,
		InstanceID:     h.accInstanceID,
		Name:           h.accName(),
	}
}

// StateBytes reports the current in-memory group-state size. Exposed for
// callers that bound memory without a SpillManager (the shuffle sender's
// capped partial aggregate flushes an epoch when this crosses its cap);
// Inspect() reports zero footprint when no Spill is attached, so it can't
// serve that role.
func (h *HashAggregate) StateBytes() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.groupMemoryUsage()
}

// accName is the stable identifier for this aggregate instance.
func (h *HashAggregate) accName() string {
	if len(h.GroupByCols) == 0 {
		return "HashAggregate"
	}
	return "HashAggregate/group_by=" + strings.Join(h.GroupByCols, ",")
}

// spillableBytesLocked reports the bytes RequestRelief may target without
// forming a drain-rebuild loop or re-reading about-to-be-merged state (TODO-1).
//
// Derivation: before Finalize installs partialMerger the merge has not begun
// and every in-memory byte is reclaimable; the only cap is that a single
// cooperative drain must leave one survivor partition (pickPartitionsToDrain
// caps at K-1), so the int-keyed path reports perPartition*(K-1). Once
// partialMerger != nil the merger owns the remaining bytes and we report 0 so
// the SpillManager never targets state that is being merged back in. Caller
// holds h.mu.
func (h *HashAggregate) spillableBytesLocked() int64 {
	if h.Spill == nil || !h.canUseExternalMerge() {
		return 0
	}
	if h.partialMerger != nil {
		return 0 // finalize started: merger owns the bytes
	}
	if !h.useIntGroupKey {
		return h.trackedGroupMem // non-int paths whole-drain; all reclaimable
	}
	n := h.intIndexLen()
	if n == 0 {
		return 0
	}
	K := h.drainK
	if K == 0 {
		K = computeAdaptiveK(n)
	}
	if K <= 1 {
		return h.trackedGroupMem
	}
	perPartition := h.trackedGroupMem / int64(K)
	return perPartition * int64(K-1) // never offer the last survivor partition
}

// spillReadBytesLocked reports bytes currently being read back by the merger
// during finalize (about to re-enter the heap; never reclaimable). Caller
// holds h.mu.
func (h *HashAggregate) spillReadBytesLocked() int64 {
	if h.partialMerger != nil {
		return h.trackedGroupMem
	}
	return 0
}

// EstimateRelief implements memory.AccountedOperator: a pure read of the
// rebuild-safe spillable bytes, capped at target.
func (h *HashAggregate) EstimateRelief(target int64) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.spillableBytesLocked()
	if target > 0 && target < s {
		return target
	}
	return s
}

// selfSpillReliefTarget computes the bytes to release in response to our
// own Consume-time pressure detection. Targets bringing tracker.Used()
// below 55% of budget — 5% hysteresis below the 60% SpillCheap trigger so
// the next batch doesn't immediately re-trip the threshold.
//
// Returns 0 when the budget is unknown (tests without a tracker, ad-hoc
// environments) so the dispatcher falls through to spillFullState — the
// pre-partial-drain behavior.
//
// Caller is the self-spill Consume path; cooperative SpillSome callers
// supply their own target via the Spillable interface.
func (h *HashAggregate) selfSpillReliefTarget() int64 {
	if h.Spill == nil {
		return 0
	}
	t := h.Spill.Tracker()
	if t == nil || h.Spill.SpillBudget() <= 0 {
		return 0
	}
	// SpillBudget: honors a #318 degraded-retry view's reduced cap.
	threshold := h.Spill.SpillBudget() * 55 / 100
	relief := t.Used() - threshold
	if relief <= 0 {
		return 0
	}
	return relief
}

// SpillSome drains a portion of the SoA hash state to a partial-state spill
// file and releases the freed bytes back to the tracker, returning the
// number of bytes released. Called by SpillManager.RequestRelief on behalf
// of a peer operator under memory pressure.
//
// On the int-keyed path, spillPartialState drains a hash-partition slice
// sized roughly to `target` bytes, leaving surviving groups in place. This
// breaks the drain-rebuild loop that whole-table draining created at SF100
// scale (PR #88 → drain-rebuild loop → heartbeat starvation): future
// Consume rows whose keys hash to a surviving partition continue to hit
// existing in-memory groups, paying no rebuild cost.
//
// On other paths (packed, compact, string, generic) and when target
// covers the full footprint, falls through to the whole-drain path —
// semantically identical to the pre-partial-drain behavior.
//
// Implements memory.Spillable and memory.AccountedOperator. The OpSpilling
// state is published for the duration so a concurrent RequestRelief snapshot
// skips this instance rather than double-dispatching.
func (h *HashAggregate) SpillSome(target int64) (int64, error) {
	h.accState.Store(int32(memory.OpSpilling))
	defer h.accState.CompareAndSwap(int32(memory.OpSpilling), int32(memory.OpActive))
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.Spill == nil || !h.canUseExternalMerge() {
		return 0, nil
	}
	before := h.trackedGroupMem
	if before == 0 {
		return 0, nil
	}
	start := time.Now()
	if err := h.spillPartialState(target); err != nil {
		return 0, err
	}
	// Rebase the #325 drain gate on the post-drain footprint: a cooperative
	// drain reclaims just as a self-triggered one does, and leaving the
	// baseline stale would make the next self-drain wait for regrowth past a
	// footprint we no longer hold.
	h.noteDrain(before, start)
	freed := before - h.trackedGroupMem
	if h.accInstanceID != 0 {
		h.Spill.Tracker().PublishOwned(h.accInstanceID, h.trackedGroupMem)
	}
	return freed, nil
}

// Next returns the aggregated results in batches of DefaultBatchSize rows.
//
// With adopted disjoint partitions (partitioned parallel aggregation) the
// emission is fanned across one goroutine per partition when eligible — see
// aggregate_parallel_emit.go. The serial fallback below streams this
// aggregate's own state and then each adopted partition in turn.
func (h *HashAggregate) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if h.emit != nil {
		return h.emit.next()
	}
	if h.parallelEmitEligible() {
		h.startParallelEmit(ctx)
		return h.emit.next()
	}
	b, err := h.nextOwn(ctx)
	if err != nil || b != nil {
		return b, err
	}
	// Own state exhausted — stream adopted disjoint partitions in order.
	for len(h.adoptedPartitions) > 0 {
		b, err := h.adoptedPartitions[0].Next(ctx)
		if err != nil || b != nil {
			return b, err
		}
		h.adoptedPartitions[0].Close()
		h.adoptedPartitions = h.adoptedPartitions[1:]
	}
	return nil, nil
}

func (h *HashAggregate) nextOwn(_ context.Context) (*batch.RecordBatch, error) {
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
	// (compact→generic, packed→generic, MergeSink generic merge) where
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
		out := batch.NewRecordBatch(h.emitOutputSchema(), 1)
		for j, agg := range h.Aggs {
			if err := h.decAggErr(&h.scalarAccs[j], j, agg.Func); err != nil {
				return nil, err
			}
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
		!h.useIntGroupKey && !h.usePackedGroupKey && !h.useCompactGroupKey {
		h.outputPos = 1
		out := batch.NewRecordBatch(h.emitOutputSchema(), 1)
		for j, agg := range h.Aggs {
			if agg.Func == AggCount {
				out.Columns[j].SetValue(0, int64(0))
			} else {
				out.Columns[j].Nulls.SetNull(0)
			}
		}
		return out, nil
	}

	// strGroupStates is 1:1 with groups on every str-side path; h.keys is
	// not — the generic SoA path defers key boxing and leaves it empty.
	totalGroups := len(h.strGroupStates)
	if h.useIntGroupKey || h.usePackedGroupKey || h.useCompactGroupKey {
		totalGroups = h.numIntGroups
	}
	if h.outputPos >= totalGroups {
		return nil, nil
	}

	start := h.outputPos
	end := start + batch.DefaultBatchSize
	if end > totalGroups {
		end = totalGroups
	}
	numRows := end - start
	h.outputPos = end

	out := batch.NewRecordBatch(h.emitOutputSchema(), numRows)

	for i := 0; i < numRows; i++ {
		var gs *groupState
		if h.useIntGroupKey || h.usePackedGroupKey || h.useCompactGroupKey {
			// Empty on the deferred single-int / packed paths: numIntGroups is
			// the count and every group's state lives in the SoA arrays.
			if start+i < len(h.intGroupStates) {
				gs = h.intGroupStates[start+i]
			}
		} else {
			gs = h.strGroupStates[start+i]
		}
		// gs is NIL for packed-key groups (state fully deferred to the SoA
		// arrays); gs.extras is nil on the other SoA hot paths — both go
		// through the SoA-direct branches below. Complex-agg / compact /
		// generic paths populate extras during consume or processRow.
		var ext *groupStateExtras
		if gs != nil {
			ext = gs.extras
		}

		// Set group-by columns. Int and packed paths take the typed-direct
		// route (writeIntKeyToColumn) so the per-row int64 → `any` box that
		// the prior SetValue path forced is avoided. For SF100 Q17 scale
		// (20M emitted groups × 1 int key) this is 20M boxes eliminated per
		// drain. The generic path reads pre-boxed values from extras.keyValues
		// and hands them to SetValue without re-boxing.
		deferredBoxing := ext == nil || ext.keyValues == nil
		if h.useIntGroupKey && deferredBoxing {
			writeIntKeyToColumn(out.Columns[0], i, h.intKeys[start+i], h.groupColTypes[0])
		} else if h.usePackedGroupKey && deferredBoxing {
			k := h.packedKeys[start+i]
			for j, f := range h.packedLayout {
				writeIntKeyToColumn(out.Columns[j], i, f.get(k), h.groupColTypes[j])
			}
		} else if h.useStrGroupKey && deferredBoxing {
			// Single-string path: serializedKeys holds the RAW key string
			// (no binary framing) — write it straight into the key column.
			out.Columns[0].BytesData.Set(i, []byte(h.serializedKeys[start+i]))
		} else if deferredBoxing {
			// Generic-path deferred boxing: keys were never boxed at
			// consume; decode the group's binary serialized key straight
			// into the typed output columns.
			decodeSerializedKeyIntoColumns(h.serializedKeys[start+i], h.groupColTypes, out.Columns, i)
		} else {
			for j, val := range ext.keyValues {
				out.Columns[j].SetValue(i, val)
			}
		}

		// Set aggregate columns
		for j, agg := range h.Aggs {
			colIdx := len(h.GroupByCols) + j
			// Mirror of updateGroup's pre-switch arm: a container MIN/MAX
			// finalizes from its retained value, not from an Accumulator.
			if h.hasBoxedMinMax && j < len(h.aggBoxedMinMax) && h.aggBoxedMinMax[j] {
				var st *containerMinMaxState
				if ext != nil && j < len(ext.extraState) {
					st, _ = ext.extraState[j].(*containerMinMaxState)
				}
				out.Columns[colIdx].SetValue(i, st.value())
				continue
			}
			switch agg.Func {
			case AggCountDistinct:
				out.Columns[colIdx].SetValue(i, int64(ext.distinctSets[j].count()))
			case AggStringAgg:
				state := ext.extraState[j].(*stringAggState)
				if len(state.parts) == 0 {
					out.Columns[colIdx].SetValue(i, nil)
				} else {
					out.Columns[colIdx].SetValue(i, strings.Join(state.parts, state.sep))
				}
			case AggBoolAnd, AggBoolOr:
				// nil state = the group never saw a non-NULL input → NULL.
				if v, ok := ext.extraState[j].(bool); ok {
					out.Columns[colIdx].SetValue(i, v)
				} else {
					out.Columns[colIdx].SetValue(i, nil)
				}
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
			case AggVarState, AggVarStateMerge:
				// Partial output: the state itself, for the merge stage
				// above (or the final stage's fold) to combine. Never a
				// finished STDDEV — that is not re-aggregatable.
				state := ext.extraState[j].(*varianceState)
				out.Columns[colIdx].SetValue(i, state.encode())
			case AggApproxDistinct:
				out.Columns[colIdx].SetValue(i, int64(ext.distinctSets[j].count()))
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
			case AggCovarState, AggCovarStateMerge:
				// Partial output: the state itself, for the merge stage
				// above (or the final stage's fold) to combine.
				state := ext.extraState[j].(*covarianceState)
				out.Columns[colIdx].SetValue(i, state.encode())
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
					if err := h.decAggErr(&ext.accs[j], j, agg.Func); err != nil {
						return nil, err
					}
					writeAccToColumn(out.Columns[colIdx], i, &ext.accs[j], agg.Func)
				} else {
					// SoA flat-arrays path: synthesize a stack-only Accumulator
					// from intFlatAccs at index start+i, then dispatch. The
					// Accumulator value never escapes (writeAccToColumn doesn't
					// retain it), so this is alloc-free.
					var acc kernel.Accumulator
					loadAccFromFlat(&h.intFlatAccs[j], countArrayOf(h.intFlatAccs, j), start+i, &acc)
					if err := h.decAggErr(&acc, j, agg.Func); err != nil {
						return nil, err
					}
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
		h.resetStateByteCounters()
		h.strGroupStates = nil
		h.strGroupIndex = nil
		h.genKeyIdx = nil
		h.genKeyNext = nil
		h.strNullGroupIdx = -1
		// Drop SoA arrays now that the SoA-direct path has finished reading
		// from them. materializeFlatAccums used to do this implicitly (it
		// nil'd intFlatAccs on the way through); since Next() no longer calls
		// materialize on the SoA hot path, we must release these explicitly.
		h.intFlatAccs = nil
		h.intGroupStates = nil
		h.numIntGroups = 0
		h.intGroupIndex = nil
		h.intTwoLevel = nil
		h.intKeys = nil
		h.packedIdx = nil
		h.packedTwoLevel = nil
		h.packedKeys = nil
		// Off-heap state returns to the OS the moment emission finishes —
		// multi-GB reservations shouldn't wait for operator Close while
		// downstream operators (sort, limit) still run.
		if h.offheap != nil {
			h.offheap.Close()
			h.offheap = nil
		}
	}
	return out, nil
}

// emitOutputSchema returns the emission-phase output schema, computed once.
// Everything it derives from (group column types/metadata, aggregate output
// types) is frozen by Finalize, so the per-batch rebuild was pure overhead —
// and a single read-only slice is what lets several parallel drain units
// share the schema safely.
func (h *HashAggregate) emitOutputSchema() []parquet.Column {
	if !h.emitSchemaSet {
		h.emitSchema = h.outputSchema()
		h.emitSchemaSet = true
	}
	return h.emitSchema
}

func (h *HashAggregate) outputSchema() []parquet.Column {
	cols := make([]parquet.Column, 0, len(h.GroupByCols)+len(h.Aggs)+len(h.NullGroupCols))

	// Pre-compute output names: strip table qualifiers unless stripping would
	// create duplicate column names (e.g., GROUP BY n1.n_name, n2.n_name must
	// keep qualifiers so downstream projections can distinguish them).
	// GroupByAll (DISTINCT) passes the input schema through verbatim — the
	// operator must be name-transparent to downstream column references.
	outNames := make([]string, len(h.GroupByCols))
	if h.GroupByAll {
		copy(outNames, h.GroupByCols)
	} else {
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
	}
	// A key the planner NAMED overrides whatever the rule above produced: it
	// resolves by a hidden slot and publishes under its own canonical text,
	// and neither the qualifier strip nor the ambiguity rule applies to a
	// name the planner already decided (ADR-0026).
	for i := range outNames {
		if i < len(h.GroupByOutNames) && h.GroupByOutNames[i] != "" {
			outNames[i] = h.GroupByOutNames[i]
		}
	}

	for i, name := range outNames {
		typ := parquet.TypeString // default fallback
		// Resolution is groupColIdx >= 0, NOT a non-zero type: TypeBool IS
		// zero, so a BOOL group key read as "unresolved" and got a String
		// output column — while the key decoder, reading groupColTypes,
		// wrote BoolData into it and killed the process. `GROUP BY
		// bool_col` panicked on its own; declared function return types
		// (#310) also route `GROUP BY starts_with(c, 'x')` through here.
		if i < len(h.groupColTypes) && i < len(h.groupColIdx) && h.groupColIdx[i] >= 0 {
			typ = parquet.TypeID(h.groupColTypes[i])
		}
		out := parquet.Column{Name: name, Type: typ, Nullable: true}
		// Decimal group keys need the source Scale/Precision: the output
		// vector parses keyValues with its OWN scale, so a scale-0 column
		// stored 0.25 as 0 — every fractional decimal key truncated
		// (issue #144 suite finding). Nested key columns likewise need
		// their Fields/ElementType to reconstruct children.
		if i < len(h.groupColMeta) && h.groupColMeta[i].Type == typ {
			meta := h.groupColMeta[i]
			out.Precision = meta.Precision
			out.Scale = meta.Scale
			out.Fields = meta.Fields
			out.ElementType = meta.ElementType
			out.Dimension = meta.Dimension
		}
		cols = append(cols, out)
	}
	for i, agg := range h.Aggs {
		out := parquet.Column{Name: agg.OutputCol, Type: agg.OutputType, Nullable: true}
		// A DECIMAL's (p,s) is half its value on the wire (ADR-0010), so the
		// column declares the plan-time pair BEFORE the observed-input arms
		// below get a chance to refine it. Those arms need an input vector,
		// and the one output that has none is the identity row an ungrouped
		// aggregate emits when it consumed no rows — the shape a selective
		// filter produces on a partial task whose files matched nothing.
		// Declaring (0,0) there wrote a .wshf header that said the unscaled
		// integers in every OTHER partial meant 10^scale more than they do
		// (#685).
		//
		// Keyed on PRECISION, which a DECIMAL declaration always has (1..38,
		// ADR-0024's DDL bound): a scale of 0 is a real declaration for
		// DECIMAL(p,0), so keying on the scale would leave exactly that
		// column's precision at 0.
		if out.Type == parquet.TypeDecimal && agg.OutputPrecision > 0 {
			out.Precision, out.Scale = agg.OutputPrecision, agg.OutputScale
		}
		resolved := i < len(h.aggColIdx) && h.aggColIdx[i] >= 0 && i < len(h.aggInputTypes)
		switch agg.Func {
		case AggMinBy, AggMaxBy:
			// MIN_BY/MAX_BY emit a value taken VERBATIM from their first
			// argument — finalize writes back the very box GetValue
			// produced (minMaxByState.bestVal), so the output column IS the
			// input column, type and metadata alike. Any other declaration
			// hands SetValue a value the vector cannot hold: a FLOAT64
			// declaration over BOOL/IPV6/CIDR/MAC/UUID/DECIMAL/ARRAY/ROW
			// raised the #361 guard on the parallel-emit goroutine, where no
			// caller's recover can reach it, and took the process down
			// (#392). PORT/PROTOCOL/DURATION were the quiet half of the same
			// defect: they box as int32/int64, which a FLOAT64 vector
			// accepts, so `MIN_BY(port_col, id)` simply answered as a float.
			// #353 was the STRING corner of the same declaration:
			// MIN_BY(o_orderpriority, o_totalprice) wrote its string into a
			// Float64 vector and came back as 0 on every row.
			//
			// The metadata travels with the type because it is what makes
			// the box round-trip: DECIMAL re-parses its formatted string
			// against the OUTPUT vector's scale, and ARRAY/ROW/MAP/VECTOR
			// need Child/Children/VectorDim to exist before SetValue can
			// write anything at all.
			if resolved {
				out.Type = parquet.TypeID(h.aggInputTypes[i])
				if i < len(h.aggInputMeta) && h.aggInputMeta[i].Type == out.Type {
					meta := h.aggInputMeta[i]
					out.Precision, out.Scale = meta.Precision, meta.Scale
					out.Fields, out.ElementType, out.Dimension = meta.Fields, meta.ElementType, meta.Dimension
				}
			}
		case AggMin, AggMax:
			// MIN/MAX preserve their input's type. The planner declares
			// float64 for the types whose ACCUMULATOR used to finalize as a
			// float; override from the type observed at Consume so MIN(url)
			// emits a string, MIN(date) stays a date instead of surfacing
			// raw epoch days, and MIN(dec) stays a DECIMAL instead of the
			// float64 that dropped every digit past the 16th (#455).
			if resolved {
				if t, ok := minMaxOutputType(h.aggInputTypes[i]); ok {
					out.Type = t
				}
				// A container answer is the input value itself, so it needs
				// the input's SHAPE for the same reason MIN_BY does:
				// Child/Children/VectorDim must exist before SetValue can
				// write anything at all (#426).
				if batch.IsContainerType(h.aggInputTypes[i]) &&
					i < len(h.aggInputMeta) && h.aggInputMeta[i].Type == out.Type {
					meta := h.aggInputMeta[i]
					out.Fields, out.ElementType, out.Dimension = meta.Fields, meta.ElementType, meta.Dimension
				}
				// MIN/MAX of a DECIMAL is a value the column HOLDS, so it
				// keeps the column's own precision and scale — the same
				// rule PostgreSQL's min(numeric)/max(numeric) follow.
				if h.aggInputTypes[i] == batch.TypeDecimal {
					out.Precision, out.Scale = h.decOutputParams(i, h.aggInputDecScale[i])
				}
			}
		case AggSum, AggAvg:
			// SUM and AVG over a DECIMAL answer in DECIMAL, exactly
			// (#455). The accumulator was already an Int128 at the column's
			// scale; only the declaration said float64, which is where the
			// digits went.
			//
			// SUM keeps the input's scale and declares the carrier's full
			// precision, because a sum genuinely can exceed its column's:
			// declaring the input's precision would make the parquet writer
			// pick a leaf too narrow for the value it is handed. AVG widens
			// the scale by batch.AvgScaleIncrement — see that constant for
			// the contract and how it differs from PostgreSQL's rule.
			if resolved && h.aggInputTypes[i] == batch.TypeDecimal {
				out.Type = parquet.TypeDecimal
				scale := h.aggInputDecScale[i]
				if agg.Func == AggAvg {
					scale = batch.AvgScale(scale)
				}
				out.Precision, out.Scale = batch.MaxDecimalPrecision, scale
			}
		}
		cols = append(cols, out)
	}
	// GROUPING SETS null columns (appear in other sets but not this one)
	for _, name := range h.NullGroupCols {
		cols = append(cols, parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true})
	}
	return cols
}

// decOutputParams returns the (precision, scale) a DECIMAL aggregate output
// declares for aggregate i. The scale is the caller's — MIN/MAX keep the
// input's, AVG widens it — and the precision is the input column's when the
// schema recorded one, else the carrier's full width. A zero precision on a
// DECIMAL column is a schema that never declared one; declaring 0 downstream
// would describe a column that can hold no digits at all.
func (h *HashAggregate) decOutputParams(i, scale int) (precision, outScale int) {
	precision = batch.MaxDecimalPrecision
	if i < len(h.aggInputMeta) && h.aggInputMeta[i].Type == parquet.TypeDecimal &&
		h.aggInputMeta[i].Precision > 0 {
		precision = h.aggInputMeta[i].Precision
	}
	if scale > precision {
		precision = batch.MaxDecimalPrecision
	}
	return precision, scale
}

// minMaxOutputType maps a MIN/MAX input column type to its output type.
// ok=false means "keep the planner-declared type" — reached only by a type
// with no MIN/MAX kernel at all. It is a second return rather than a zero TypeID
// because parquet.TypeBool IS zero, and a caller reading a real BOOL
// declaration as "undeclared" is how #354 and #371 went wrong.
//
// MIN/MAX return a value taken from the input column, so the rule is "the
// input's own type", the same rule #392 settled for MIN_BY/MAX_BY. The
// switch used to name seven types and return 0 for the rest, which left the
// planner's FLOAT64 declaration standing: `MIN(mac_col)` answered
// 1.877e+14 instead of an address, `MIN(port_col)` answered a float, and
// IPV6/CIDR/UUID/BOOL — whose kernels answered NULL anyway until #417 —
// would have landed a string or a bool in a FLOAT64 vector and raised the
// #361 guard on the parallel-emit goroutine.
//
// DECIMAL is in the list since #455: the accumulator was always an exact
// Int128 at the column's scale, and outputSchema copies that scale onto the
// declaration, so MIN/MAX of a DECIMAL answers with a value the column
// actually holds instead of a float64 that cannot hold it.
//
// ARRAY/ROW/MAP/VECTOR are NOT in that list: containers answer a real
// value now (#426, containerMinMaxState), boxed by GetValue and written
// back with SetValue, so the only declaration that can hold it is the
// input's own — the same rule #392 settled for MIN_BY/MAX_BY.
//
// physical.minMaxDeclaredType mirrors this function and must change with it.
func minMaxOutputType(in batch.TypeID) (parquet.TypeID, bool) {
	switch in {
	case batch.TypeString:
		return parquet.TypeString, true
	case batch.TypeBytes:
		return parquet.TypeBytes, true
	case batch.TypeDate:
		return parquet.TypeDate, true
	case batch.TypeTimestamp:
		return parquet.TypeTimestamp, true
	case batch.TypeIPv4:
		return parquet.TypeIPv4, true
	case batch.TypeIPv6:
		return parquet.TypeIPv6, true
	case batch.TypeCIDR:
		return parquet.TypeCIDR, true
	case batch.TypeUUID:
		return parquet.TypeUUID, true
	case batch.TypeMAC:
		return parquet.TypeMAC, true
	case batch.TypePort:
		return parquet.TypePort, true
	case batch.TypeProtocol:
		return parquet.TypeProtocol, true
	case batch.TypeDuration:
		return parquet.TypeDuration, true
	case batch.TypeBool:
		return parquet.TypeBool, true
	case batch.TypeInt64:
		return parquet.TypeInt64, true
	case batch.TypeInt32:
		return parquet.TypeInt64, true
	case batch.TypeFloat64, batch.TypeFloat32:
		return parquet.TypeFloat64, true
	case batch.TypeDecimal:
		return parquet.TypeDecimal, true
	case batch.TypeArray, batch.TypeRow, batch.TypeMap, batch.TypeVector:
		// The answer IS an input value, boxed by GetValue and written back
		// with SetValue, so the only declaration that can hold it is the
		// input's own — the MIN_BY rule (#392), now reached by MIN/MAX too
		// because containers answer (#426). outputSchema copies the shape
		// metadata alongside.
		return parquet.TypeID(in), true
	}
	return 0, false
}

// CloneSink returns a new HashAggregate with the same configuration but fresh state.
// Used by parallel pipeline execution: each worker gets its own cloned sink.
func (h *HashAggregate) CloneSink() SinkSource {
	clone := &HashAggregate{
		GroupByCols: h.GroupByCols,
		// A clone EMITS: the morsel-parallel path finalizes each one and the
		// primary merges their state, so a clone that did not carry the
		// published names would publish its keys under their hidden slots
		// and every consumer above would read NULL.
		GroupByOutNames: h.GroupByOutNames,
		GroupByAll:      h.GroupByAll, // clones must resolve the same key set, not fall into the scalar path
		Aggs:            h.Aggs,
		NullGroupCols:   h.NullGroupCols,
		GroupingSets:    h.GroupingSets,
		// No spill manager — partial aggregates are small enough
		strNullGroupIdx: -1, // defensive: Init sets it, but the zero value is a VALID slot
	}
	// Partitioned clones own disjoint 1/k slices of the key space, so the
	// NDV presize divides cleanly across them (the pipeline sets the
	// divisor before cloning). Non-partitioned clones can each see the
	// full key set — presizing every clone to full NDV would k× the
	// table memory, so they keep organic growth.
	if h.cloneNDVDivisor > 1 {
		clone.GroupNDVHint = h.GroupNDVHint
		clone.cloneNDVDivisor = h.cloneNDVDivisor
	}
	// A clone of a bounded sink is bounded: the owner tears the whole
	// aggregate down on the cap, clones included. Passing the cap unchanged
	// over-states what one clone can hold (clones own disjoint 1/k slices),
	// which keeps the ceiling an upper bound — the safe direction.
	clone.epochByteCap = h.epochByteCap
	// Same for the row bound: a clone consumes a SUBSET of the parent's
	// input, so the parent's bound bounds it too. Deliberately not divided
	// by the clone count — morsel clones take dynamic row slices, and a
	// bound that over-states only ever keeps the adaptive path.
	clone.inputRowBound = h.inputRowBound
	return clone
}

// MergeSink merges another HashAggregate's partial state into this one.
// Called after all parallel workers finish to combine partial aggregates.
//
// After the state merge, h's group footprint has grown by the clone's
// state; reconcileGroupMemory recharges h so the shared-tracker reservation
// follows the state (morsel-parallel clones charge a tracking-only
// SpillManager view; their own charge is released at clone Close, AFTER
// this recharge, so the tracker never under-reports in between). No-op when
// h.Spill is nil — the single-process planner path.
func (h *HashAggregate) MergeSink(other SinkSource) {
	o := other.(*HashAggregate)
	// Partitioned-disjoint adoption: keys never overlap across sinks, so
	// re-inserting the clone's groups into the primary's table (a serial
	// O(total groups) rehash) is pure waste — keep the clone's state and
	// stream it during Next().
	if h.PartitionedDisjoint && o.PartitionedDisjoint {
		// The clone keeps its state AND its drained runs: normally
		// mergeSinkState hands drainedRuns to the primary, but an adopted
		// partition finalizes its own runs itself — without this transfer
		// they were orphaned and every drained group silently vanished.
		o.partialSpillFiles = append(o.partialSpillFiles, o.drainedRuns...)
		o.drainedRuns = nil
		h.adoptedPartitions = append(h.adoptedPartitions, o)
		return
	}
	h.mergeSinkState(o)
	h.reconcileGroupMemory()
}

func (h *HashAggregate) mergeSinkState(o *HashAggregate) {
	// EVERY run the clone wrote belongs to the primary now, whatever merge
	// path the remaining in-memory state takes — and there are two lists,
	// because there are two ways a clone drains.
	//
	// drainedRuns is the PartialDrainBytes bound, the one a clone is designed
	// to take. partialSpillFiles is what spillPartialState appends to, and a
	// clone reaches THAT through SpillSome: a clone registers as an accounted
	// operator like any other aggregate (Consume does it whenever Spill is
	// non-nil and canUseExternalMerge holds — a tracking-only view is still
	// non-nil), so SpillManager.RequestRelief can ask it for bytes on a peer's
	// behalf and it drains a whole run in answer.
	//
	// Only the first list was transferred. The clone's own Close then dropped
	// the second, and every group in those runs vanished from the answer with
	// no error anywhere — 5000 rows in, 1100 out, on a shape whose totals a
	// COUNT(*) makes obvious and a SUM does not (#790). The partitioned
	// adoption branch above does not have the bug: it keeps the clone and
	// finalizes it, which is why it moves drainedRuns INTO partialSpillFiles
	// rather than away from them.
	if len(o.drainedRuns) > 0 {
		h.partialSpillFiles = append(h.partialSpillFiles, o.drainedRuns...)
		o.drainedRuns = nil
	}
	if len(o.partialSpillFiles) > 0 {
		h.partialSpillFiles = append(h.partialSpillFiles, o.partialSpillFiles...)
		o.partialSpillFiles = nil
	}

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
		h.groupColMeta = o.groupColMeta
		if len(h.groupColIdx) == 0 {
			h.groupColIdx = o.groupColIdx
		}
		if len(h.aggColIdx) == 0 {
			h.aggColIdx = o.aggColIdx
		}
		if len(h.aggColIdx2) == 0 {
			h.aggColIdx2 = o.aggColIdx2
		}
		// GroupByAll (DISTINCT) parents have no plan-time column list at
		// all — the clones resolved it from the first batch's schema.
		// Without inheriting it, outputSchema emits zero group columns
		// while the merged states hold full key tuples (index panic).
		if len(h.GroupByCols) == 0 && len(o.GroupByCols) > 0 {
			h.GroupByCols = o.GroupByCols
		}
	}
	// Same inheritance for the aggregate inputs, and for the same reason:
	// MIN/MAX/MIN_BY/MAX_BY re-declare their output from the type (and, for
	// DECIMAL and the containers, the full column metadata) they observed at
	// Consume. A parent that consumed nothing observed nothing, so without
	// this it emits the value under whatever the planner guessed. The
	// GROUP BY block above cannot carry it: a SCALAR aggregate has no group
	// columns at all, and MIN_BY's ungrouped form is exactly the shape the
	// parallel merge produces.
	if len(h.aggInputTypes) == 0 && len(o.aggInputTypes) > 0 {
		h.aggInputTypes = o.aggInputTypes
		h.aggInputMeta = o.aggInputMeta
		h.aggInputDecScale = append([]int(nil), o.aggInputDecScale...)
		h.aggBoxedMinMax = o.aggBoxedMinMax
		h.hasBoxedMinMax = o.hasBoxedMinMax
		if len(h.aggColIdx) == 0 {
			h.aggColIdx = o.aggColIdx
		}
	}
	// A clone that latched a cross-scale batch hands the finding to the
	// primary: the flag is a property of the INPUT the operator saw, and the
	// primary saw none of it.
	if o.decScaleConflict {
		h.decScaleConflict = true
	}
	// The DECIMAL scale upgrades on EVERY merge, not only the first one.
	//
	// It is the one piece of that metadata a clone can report WRONG rather
	// than not at all: a clone whose morsel held no non-NULL value observed a
	// vector but contributed nothing, so it reports scale 0 — and inheriting
	// from that clone alone declares the output at scale 0 while the merged
	// accumulator counts in the column's, which renders 4.00 as "4". This is
	// the "prefer the first NONZERO observation" rule Consume already applies
	// ACROSS BATCHES (#455) applied ACROSS CLONES: it costs a genuinely
	// scale-0 column nothing, because every clone of one then reports 0.
	//
	// The inherit above copies rather than aliases so this loop cannot write
	// through into the clone it inherited from.
	//
	// And the clone seam is where the GROUPED cross-scale pair is DETECTED, not
	// only where the flag travels. Consume's latch compares a batch against the
	// scale its own operator established, so two clones each fed one scale each
	// see one scale and neither latches anything; without the second arm below
	// this loop then took the primary's and DISCARDED the disagreement, and
	// `p.Consume@2 + c.Consume@4 + MergeSink` answered 25.50 where the
	// ungrouped form raises 22003 (#685 review, item A, second pass).
	//
	// A known inconsistency, recorded rather than fixed because no producer in
	// the tree reaches it: a genuinely scale-0 batch followed by a scale-2 one
	// is an UPGRADE on this path (5 at scale 0 summed with 1.00 gives 1.05)
	// where the ungrouped adoptDecScale calls the same pair a conflict. The
	// "first NONZERO" rule is what #455 needs for the identity row, and it
	// cannot tell that row's absent scale from a real DECIMAL(p,0).
	for i := range h.aggInputDecScale {
		if i >= len(o.aggInputDecScale) {
			continue
		}
		switch {
		case h.aggInputDecScale[i] == 0:
			h.aggInputDecScale[i] = o.aggInputDecScale[i]
		case o.aggInputDecScale[i] != 0 && o.aggInputDecScale[i] != h.aggInputDecScale[i]:
			h.decScaleConflict = true
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

	// Packed-key SoA fast path: merge via one probe per source group.
	if h.usePackedGroupKey && o.usePackedGroupKey && h.intFlatAccs != nil && o.intFlatAccs != nil {
		h.mergePackedGroupSoA(o)
		return
	}

	// Empty-primary adoption. A primary that just whole-state-drained
	// (spillFullState → resetGroupStateAfterSpill) has intFlatAccs == nil
	// until its next Consume lazily rebuilds — a barrier merge landing in
	// that window used to fall through to the migrate path below and
	// materialize BOTH sides into the generic map. On SF100 Q17 (GROUP BY
	// l_partkey, ~20M keys, 8 clone partials) that fallback allocated a
	// second full copy of every partial (migrateToGenericMap 14.2 GB +
	// materializeFlatAccums 9.6 GB cum in the worker heap profiles) at the
	// moment memory was already critical, and one such merge poisoned all
	// later ones by nil'ing the SoA arrays (2026-07-03 postmortem,
	// morsel-agg-partials-v2.md §3.C). When the primary is empty, adopting
	// the clone's state wholesale is O(1) and mode-preserving.
	if h.groupCount() == 0 && o.groupCount() > 0 {
		h.adoptStateFrom(o)
		return
	}

	// Drain-to-runs fallback for simple aggregates: instead of migrating an
	// SoA-capable side into the generic map, write its state as canonical
	// partial-state runs (sorted by binary sortKey — the format is
	// instance-independent) and let Finalize's existing k-way merge combine
	// them with the primary's in-memory state. O(state) disk I/O instead of
	// O(state) heap at the barrier. Falls through to the legacy in-memory
	// merge on any write error — correctness never depends on disk.
	if h.simpleAggs && len(h.GroupingSets) == 0 && o.canUseExternalMerge() &&
		h.Spill != nil && h.Spill.SpillDir() != "" {
		if paths, err := o.drainStateToRuns(h.Spill.SpillDir()); err == nil {
			h.partialSpillFiles = append(h.partialSpillFiles, paths...)
			return
		}
	}

	// Normalize both sides to the generic map path so merge is uniform.
	h.migrateToGenericMap()
	o.migrateToGenericMap()

	// Typed-generic sinks don't maintain strGroupIndex during consume;
	// the dedup loop below probes it, so rebuild from serializedKeys.
	h.ensureStrGroupIndexForMerge()

	// migrateToGenericMap → materializeFlatAccums on both sides, so every
	// group's extras is allocated and extras.accs is populated. Iterate
	// strGroupStates, not o.keys — deferred-boxing generic sinks never
	// populate o.keys (serializedKeys carries the identity).
	for i := range o.strGroupStates {
		key := o.serializedKeys[i]
		oGS := o.strGroupStates[i]
		oExt := oGS.extras

		newIdx := int32(len(h.strGroupStates))
		var gsIdx int32
		var found bool
		if int32(i) == o.strNullGroupIdx {
			gsIdx, found = h.mergeNullGroupSlot(key, newIdx)
		} else {
			gsIdx, found = h.strGroupIndex.GetOrInsert([]byte(key), newIdx)
		}
		if found {
			gs := h.strGroupStates[gsIdx]
			ext := gs.extras
			for j := range ext.accs {
				ext.accs[j].Merge(&oExt.accs[j])
			}
			// STDDEV/VARIANCE/CORR/COVAR/STRING_AGG/MEDIAN/PERCENTILE/
			// MODE/MIN_BY/MAX_BY/BOOL_AND/BOOL_OR keep their state in
			// extraState, not in accs. Without this merge a group split
			// across morsel-parallel clones kept only the FIRST clone's
			// partial — the primary starts empty, adopts clone 1 wholesale
			// (adoptStateFrom), and every later clone's state was dropped
			// here. STDDEV(o_totalprice) over 15000 rows answered from
			// 3750 of them (#339): a plausible-looking number, wrong in the
			// fourth digit, that no row count or NULL check can catch.
			h.mergeExtraState(ext, oExt)
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
					ext.distinctSets[j] = oExt.distinctSets[j]
					h.distinctBytes += oExt.distinctSets[j].memBytes()
					continue
				}
				before := ext.distinctSets[j].memBytes()
				ext.distinctSets[j].mergeFrom(oExt.distinctSets[j])
				h.distinctBytes += ext.distinctSets[j].memBytes() - before
			}
		} else {
			h.strGroupStates = append(h.strGroupStates, oGS)
			if oExt.accs != nil {
				h.extrasAccsCount += int64(len(oExt.accs))
			}
			if oExt.extraState != nil {
				h.extraStateBytes += int64(len(oExt.extraState)) * 80
				// The flat charge above is sized for a scalar box; a
				// container MIN/MAX's retained value (or MIN_BY/MAX_BY's
				// bestVal, which shares the same container-shaped output
				// per aggSpecOutputType) is a whole copied structure and
				// needs its own accounting on top, the same as observe/
				// merge do for a group already in h.
				for _, st := range oExt.extraState {
					switch st := st.(type) {
					case *containerMinMaxState:
						h.extraStateBytes += st.memBytes()
					case *minMaxByState:
						h.extraStateBytes += boxContainerMemBytes(st.bestVal)
					}
				}
			}
			for _, ds := range oExt.distinctSets {
				h.distinctBytes += ds.memBytes()
			}
			h.keys = append(h.keys, oExt.keyValues)
			h.serializedKeys = append(h.serializedKeys, key)
			h.serializedKeyBytes += int64(len(key))
		}
	}
}

// mergeExtraState folds one group's extraState from a clone (src) into the
// primary's (dst). Every kind is combined by its own algebra: the variance
// and covariance families pairwise (see varianceState.merge), the
// value-collecting kinds by concatenation, MIN_BY/MAX_BY by keeping the
// better comparison value, and the boolean kinds by their operator.
//
// Missing or short slices are tolerated rather than indexed blindly: a
// clone that never consumed a row for this group has no state to give.
func (h *HashAggregate) mergeExtraState(dst, src *groupStateExtras) {
	if dst == nil || src == nil || src.extraState == nil || dst.extraState == nil {
		return
	}
	for j := range dst.extraState {
		if j >= len(src.extraState) || src.extraState[j] == nil || j >= len(h.Aggs) {
			continue
		}
		switch s := src.extraState[j].(type) {
		case *varianceState:
			if d, ok := dst.extraState[j].(*varianceState); ok {
				d.merge(s)
			} else {
				dst.extraState[j] = s
			}
		case *covarianceState:
			if d, ok := dst.extraState[j].(*covarianceState); ok {
				d.merge(s)
			} else {
				dst.extraState[j] = s
			}
		case *stringAggState:
			// Concatenation order across parallel clones is the order the
			// clones merge in, which is as defined as STRING_AGG without an
			// ORDER BY ever is.
			if d, ok := dst.extraState[j].(*stringAggState); ok {
				d.parts = append(d.parts, s.parts...)
			} else {
				dst.extraState[j] = s
			}
		case *collectState:
			// PERCENTILE/MEDIAN/MODE sort or tally the pooled values at
			// finalize, so appending is exact.
			if d, ok := dst.extraState[j].(*collectState); ok {
				d.values = append(d.values, s.values...)
			} else {
				dst.extraState[j] = s
			}
		case *minMaxByState:
			d, ok := dst.extraState[j].(*minMaxByState)
			if !ok {
				dst.extraState[j] = s
				h.extraStateBytes += boxContainerMemBytes(s.bestVal)
				continue
			}
			if !s.hasValue {
				continue
			}
			if !d.hasValue || (d.isMin && kernel.CompareFloat64(s.bestCmp, d.bestCmp) < 0) || (!d.isMin && kernel.CompareFloat64(s.bestCmp, d.bestCmp) > 0) {
				before := boxContainerMemBytes(d.bestVal)
				d.bestVal, d.bestCmp, d.hasValue = s.bestVal, s.bestCmp, true
				h.extraStateBytes += boxContainerMemBytes(d.bestVal) - before
			}
		case *containerMinMaxState:
			d, ok := dst.extraState[j].(*containerMinMaxState)
			if !ok {
				dst.extraState[j] = s
				h.extraStateBytes += s.memBytes()
				continue
			}
			before := d.memBytes()
			d.merge(s)
			h.extraStateBytes += d.memBytes() - before
		case bool:
			d, ok := dst.extraState[j].(bool)
			if !ok {
				dst.extraState[j] = s
				continue
			}
			if h.Aggs[j].Func == AggBoolAnd {
				dst.extraState[j] = d && s
			} else {
				dst.extraState[j] = d || s
			}
		}
	}
}

// distinctColType resolves the input column type for aggregate i from the
// live batch, for choosing the distinct-set representation. Unresolvable
// columns fall back to the string representation (safe for any type).
func (h *HashAggregate) distinctColType(b *batch.RecordBatch, i int) batch.TypeID {
	if i < len(h.aggColIdx) {
		if idx := h.aggColIdx[i]; idx >= 0 && idx < len(b.Columns) {
			return b.Columns[idx].Type
		}
	}
	return batch.TypeString
}

// intColValue reads an int-class column value widened to int64.
func intColValue(v *batch.Vector, row int) int64 {
	switch v.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(v.Int32Data[row])
	default:
		return v.Int64Data[row]
	}
}

// groupCount returns the number of group slots across the key modes. Used
// as the emptiness gate for state adoption — a partially-drained aggregate
// (freed slots still occupy the slices) reports non-zero and is not adopted
// into.
func (h *HashAggregate) groupCount() int {
	return h.numIntGroups + len(h.strGroupStates)
}

// groupStateAt returns the AoS state for int-keyed slot i, or nil when the
// path deferred it (single-int / packed, where intGroupStates is empty).
func (h *HashAggregate) groupStateAt(i int) *groupState {
	if i < len(h.intGroupStates) {
		return h.intGroupStates[i]
	}
	return nil
}

// appendIntGroupSlot claims the next int-keyed group slot and returns its
// index. A nil gs on a path that never materialized state leaves
// intGroupStates empty (numIntGroups alone tracks the count); anything else
// stores gs, padding the slice so slot indices stay aligned.
func (h *HashAggregate) appendIntGroupSlot(gs *groupState) int32 {
	idx := int32(h.numIntGroups)
	h.numIntGroups++
	if gs == nil && len(h.intGroupStates) == 0 {
		return idx
	}
	for len(h.intGroupStates) < int(idx) {
		h.intGroupStates = append(h.intGroupStates, nil)
	}
	h.intGroupStates = append(h.intGroupStates, gs)
	return idx
}

// adoptStateFrom moves o's entire group state (and the schema-resolution
// and key-mode fields it depends on — deterministic given the shared config
// and input schema, so overwriting is safe even when h already resolved)
// into an EMPTY h. O(1): slice/pointer moves, no per-group work. o's state
// is zeroed so its Close releases only its (still-intact) tracking charge
// and emits nothing.
func (h *HashAggregate) adoptStateFrom(o *HashAggregate) {
	// Resolution state.
	h.groupColIdx = o.groupColIdx
	h.aggColIdx = o.aggColIdx
	h.aggColIdx2 = o.aggColIdx2
	h.aggInputTypes = o.aggInputTypes
	h.aggInputMeta = o.aggInputMeta
	// COPIED, not aliased, for mergeSinkState's reason: the scale upgrade
	// there writes into h.aggInputDecScale on every later merge, and sharing
	// the slice with the clone it was adopted from would write back into a
	// state o still owns until its Close.
	//
	// The comparison happens BEFORE the overwrite, and for mergeSinkState's
	// reason one seam over: an empty primary reaches the adopt path instead of
	// the upgrade loop, so without this the same two clones at two scales came
	// back 25.50 through here.
	for i, have := range h.aggInputDecScale {
		if i < len(o.aggInputDecScale) && have != 0 &&
			o.aggInputDecScale[i] != 0 && have != o.aggInputDecScale[i] {
			h.decScaleConflict = true
		}
	}
	h.aggInputDecScale = append([]int(nil), o.aggInputDecScale...)
	h.aggBoxedMinMax = o.aggBoxedMinMax
	h.hasBoxedMinMax = o.hasBoxedMinMax
	// The operator-wide cross-scale latch travels with the state it describes.
	if o.decScaleConflict {
		h.decScaleConflict = true
	}
	h.groupColTypes = o.groupColTypes
	h.groupColMeta = o.groupColMeta
	h.aggUpdaters = o.aggUpdaters
	h.aggUpdatersNoNull = o.aggUpdatersNoNull
	// Per-batch updater-selection scratch, normally sized by resolveIndices.
	// h skipped resolveIndices when its warmup batch was fully filtered, and
	// adopting o.resolved below suppresses it forever — yet the post-merge
	// consume paths (pressure-collapse serial continuation, spilled-partition
	// replay) index this scratch unconditionally (#279: SF100 Q18 join-8,
	// index-out-of-range at the first post-adoption consumeBatch).
	h.batchUpdaters = make([]kernel.RowAggUpdater, len(h.Aggs))
	h.batchAggKernels = o.batchAggKernels
	h.aggF64Extract = o.aggF64Extract
	h.aggF64Extract2 = o.aggF64Extract2
	h.resolved = o.resolved
	h.needsDistinct = o.needsDistinct
	h.needsExtra = o.needsExtra
	h.simpleAggs = o.simpleAggs
	h.inputSchema = o.inputSchema
	// Key mode + state.
	h.useIntGroupKey = o.useIntGroupKey
	h.intGroupIndex = o.intGroupIndex
	h.intTwoLevel = o.intTwoLevel
	h.intGroupStates = o.intGroupStates
	h.numIntGroups = o.numIntGroups
	h.intGroupKeyCol = o.intGroupKeyCol
	h.intKeys = o.intKeys
	h.intFlatAccs = o.intFlatAccs
	// The adopted slices live in o's off-heap reservations: take ownership
	// so o.Close (the pipeline closes merged clones) doesn't unmap them
	// out from under h.
	if o.offheap != nil {
		if h.offheap == nil {
			h.offheap = o.offheap
		} else {
			h.offheap.AdoptFrom(o.offheap)
		}
		o.offheap = nil
	}
	h.usePackedGroupKey = o.usePackedGroupKey
	h.packedIdx = o.packedIdx
	h.packedTwoLevel = o.packedTwoLevel
	h.packedLayout = o.packedLayout
	h.packedKeys = o.packedKeys
	h.useCompactGroupKey = o.useCompactGroupKey
	h.compactKeys = o.compactKeys
	h.useStrGroupKey = o.useStrGroupKey
	h.strGroupKeyCol = o.strGroupKeyCol
	h.strNullGroupIdx = o.strNullGroupIdx
	h.useGenericSoA = o.useGenericSoA
	h.strGroupIndex = o.strGroupIndex
	h.strGroupStates = o.strGroupStates
	h.deferGenericKeyBoxing = o.deferGenericKeyBoxing
	h.genKeyIdx = o.genKeyIdx
	h.genKeyNext = o.genKeyNext
	h.keys = o.keys
	h.serializedKeys = o.serializedKeys
	h.gsPool = o.gsPool
	h.freeGroupIDs = o.freeGroupIDs
	h.drainK = o.drainK
	h.nextDrainPartition = o.nextDrainPartition
	// State byte counters travel with the state.
	h.serializedKeyBytes = o.serializedKeyBytes
	h.compactKeyBytes = o.compactKeyBytes
	h.distinctBytes = o.distinctBytes
	h.extraStateBytes = o.extraStateBytes
	h.extrasAccsCount = o.extrasAccsCount

	// Zero o's moved state (o keeps its trackedGroupMem so Close releases
	// the charge it made while accumulating).
	o.intGroupIndex = nil
	o.intTwoLevel = nil
	o.intGroupStates = nil
	o.numIntGroups = 0
	o.intKeys = nil
	o.intFlatAccs = nil
	o.packedIdx = nil
	o.packedTwoLevel = nil
	o.packedKeys = nil
	o.compactKeys = nil
	o.strGroupIndex = nil
	o.strGroupStates = nil
	o.strNullGroupIdx = -1
	o.genKeyIdx = nil
	o.genKeyNext = nil
	o.keys = nil
	o.serializedKeys = nil
	o.gsPool = groupStatePool{}
	o.freeGroupIDs = nil
	o.resetStateByteCounters()
}

// mergeIntGroupSoA merges another int-keyed SoA aggregate directly, avoiding
// materializeFlatAccums + migrateToGenericMap + per-group Accumulator.Merge.
// Operates on flat arrays (count, sumI64, sumF64, min, max) with int hash lookup.
func (h *HashAggregate) mergeIntGroupSoA(o *HashAggregate) {
	// A merge that will drive the destination past its load factor converts
	// FIRST, so the inserted groups land in per-bucket tables instead of
	// driving whole-table rehashes. Same rule as the consume path: the
	// incoming group count is the lookahead, and a merge that fits under the
	// existing capacity stays flat because there is no rehash to displace
	// (two_level_hash.go).
	if idx := h.intGroupIndex; idx != nil &&
		h.indexConverts(idx.Len(), idx.Slots(), o.numIntGroups) {
		h.intTwoLevel = convertIntHashTableToTwoLevel(idx, h.offheapReg())
		h.intGroupIndex = nil
		h.indexConversions++
		TwoLevelConversions.Add(1)
	}
	for i := 0; i < o.numIntGroups; i++ {
		newIdx := int32(h.numIntGroups)
		gsIdx, found := h.intIndexGetOrInsert(o.intKeys[i], newIdx)
		if found {
			mergeFlatAccumRow(h.intFlatAccs, o.intFlatAccs, int(gsIdx), i)
		} else {
			// New group: claim the slot, then copy o's row into it.
			h.appendIntGroupSlot(o.groupStateAt(i))
			h.intKeys = append(h.intKeys, o.intKeys[i])
			copyFlatAccumRow(h.intFlatAccs, o.intFlatAccs, int(newIdx), i)
		}
	}
}

// mergeFlatAccumRow combines src's row srcIdx into dst's existing row dstIdx
// for every aggregate: sums add, MIN/MAX take the extreme, and an unseeded
// destination adopts the source's value. The single definition serves every
// SoA merge path (int-keyed and packed-keyed); it used to be copy-pasted per
// key mode.
func mergeFlatAccumRow(dst, src []flatAccumArrays, dstIdx, srcIdx int) {
	for ai := range dst {
		hfa := &dst[ai]
		ofa := &src[ai]
		if hfa.count != nil {
			hfa.count[dstIdx] += ofa.count[srcIdx]
		}
		if hfa.sumI64 != nil {
			hfa.sumI64[dstIdx] += ofa.sumI64[srcIdx]
		}
		if hfa.sumF64 != nil {
			hfa.sumF64[dstIdx] += ofa.sumF64[srcIdx]
		}
		if hfa.sumDec != nil {
			sum, ok := hfa.sumDec[dstIdx].AddChecked(ofa.sumDec[srcIdx])
			hfa.sumDec[dstIdx] = sum
			if !ok {
				hfa.sumDecOverflow = true
			}
		}
		if ofa.sumDecOverflow {
			hfa.sumDecOverflow = true
		}
		if ofa.hasMin != nil && ofa.hasMin[srcIdx] {
			if hfa.hasMin[dstIdx] {
				if hfa.isFloat {
					if kernel.CompareFloat64(ofa.minF64[srcIdx], hfa.minF64[dstIdx]) < 0 {
						hfa.minF64[dstIdx] = ofa.minF64[srcIdx]
					}
				} else if hfa.isDecimal {
					if ofa.minDec[srcIdx].Less(hfa.minDec[dstIdx]) {
						hfa.minDec[dstIdx] = ofa.minDec[srcIdx]
					}
				} else {
					if ofa.minI64[srcIdx] < hfa.minI64[dstIdx] {
						hfa.minI64[dstIdx] = ofa.minI64[srcIdx]
					}
				}
			} else {
				hfa.hasMin[dstIdx] = true
				if hfa.minI64 != nil {
					hfa.minI64[dstIdx] = ofa.minI64[srcIdx]
				}
				if hfa.minF64 != nil {
					hfa.minF64[dstIdx] = ofa.minF64[srcIdx]
				}
				if hfa.minDec != nil {
					hfa.minDec[dstIdx] = ofa.minDec[srcIdx]
				}
			}
		}
		if ofa.hasMax != nil && ofa.hasMax[srcIdx] {
			if hfa.hasMax[dstIdx] {
				if hfa.isFloat {
					if kernel.CompareFloat64(ofa.maxF64[srcIdx], hfa.maxF64[dstIdx]) > 0 {
						hfa.maxF64[dstIdx] = ofa.maxF64[srcIdx]
					}
				} else if hfa.isDecimal {
					if !ofa.maxDec[srcIdx].Less(hfa.maxDec[dstIdx]) {
						hfa.maxDec[dstIdx] = ofa.maxDec[srcIdx]
					}
				} else {
					if ofa.maxI64[srcIdx] > hfa.maxI64[dstIdx] {
						hfa.maxI64[dstIdx] = ofa.maxI64[srcIdx]
					}
				}
			} else {
				hfa.hasMax[dstIdx] = true
				if hfa.maxI64 != nil {
					hfa.maxI64[dstIdx] = ofa.maxI64[srcIdx]
				}
				if hfa.maxF64 != nil {
					hfa.maxF64[dstIdx] = ofa.maxF64[srcIdx]
				}
				if hfa.maxDec != nil {
					hfa.maxDec[dstIdx] = ofa.maxDec[srcIdx]
				}
			}
		}
	}
}

// copyFlatAccumRow grows dst to cover slot dstIdx and copies every live field
// of src's row srcIdx into it. Growth is idempotent per array, so a count
// array shared by several aggregates is extended (and copied) exactly once —
// which is why the merge paths grow-then-assign instead of appending per
// field.
func copyFlatAccumRow(dst, src []flatAccumArrays, dstIdx, srcIdx int) {
	for ai := range dst {
		dfa := &dst[ai]
		sfa := &src[ai]
		dfa.growTo(dstIdx + 1)
		if dfa.count != nil {
			dfa.count[dstIdx] = sfa.count[srcIdx]
		}
		if dfa.sumI64 != nil {
			dfa.sumI64[dstIdx] = sfa.sumI64[srcIdx]
		}
		if dfa.sumF64 != nil {
			dfa.sumF64[dstIdx] = sfa.sumF64[srcIdx]
		}
		if dfa.sumDec != nil {
			dfa.sumDec[dstIdx] = sfa.sumDec[srcIdx]
		}
		if sfa.sumDecOverflow {
			dfa.sumDecOverflow = true
		}
		if dfa.minI64 != nil {
			dfa.minI64[dstIdx] = sfa.minI64[srcIdx]
		}
		if dfa.maxI64 != nil {
			dfa.maxI64[dstIdx] = sfa.maxI64[srcIdx]
		}
		if dfa.minF64 != nil {
			dfa.minF64[dstIdx] = sfa.minF64[srcIdx]
		}
		if dfa.maxF64 != nil {
			dfa.maxF64[dstIdx] = sfa.maxF64[srcIdx]
		}
		if dfa.minDec != nil {
			dfa.minDec[dstIdx] = sfa.minDec[srcIdx]
		}
		if dfa.maxDec != nil {
			dfa.maxDec[dstIdx] = sfa.maxDec[srcIdx]
		}
		if dfa.hasMin != nil {
			dfa.hasMin[dstIdx] = sfa.hasMin[srcIdx]
		}
		if dfa.hasMax != nil {
			dfa.hasMax[dstIdx] = sfa.hasMax[srcIdx]
		}
	}
}

// mergePackedGroupSoA merges another packed-key SoA aggregate directly. One
// probe per source group against the composite key held inline in the entry
// — the dual-int predecessor did a Get, a chain walk over three arrays, and
// then a second Get before its Put.
func (h *HashAggregate) mergePackedGroupSoA(o *HashAggregate) {
	if idx := h.packedIdx; idx != nil &&
		h.indexConverts(idx.Len(), idx.Slots(), o.numIntGroups) {
		h.packedTwoLevel = convertPackedHashTableToTwoLevel(idx, h.offheapReg())
		h.packedIdx = nil
		h.indexConversions++
		TwoLevelConversions.Add(1)
	}
	for i := 0; i < o.numIntGroups; i++ {
		k := o.packedKeys[i]
		newIdx := int32(h.numIntGroups)
		gsIdx, found := h.packedIndexGetOrInsert(k.lo, k.hi, newIdx)
		if found {
			mergeFlatAccumRow(h.intFlatAccs, o.intFlatAccs, int(gsIdx), i)
			continue
		}
		h.appendIntGroupSlot(o.groupStateAt(i))
		h.packedKeys = append(h.packedKeys, k)
		copyFlatAccumRow(h.intFlatAccs, o.intFlatAccs, int(newIdx), i)
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
	if h.usePackedGroupKey {
		// Migrate packed composite group key → generic path. Keys are
		// re-encoded in processRow's binary format ([null-flag]
		// [appendColumnValue bytes] per column) — NOT serializeKey's text
		// format — because the generic path keeps inserting after this
		// migration runs (a null group key triggers it mid-consume, and
		// MergeSink can pair a migrated side with a natively-generic side).
		// A text-format index entry would never match the binary key of the
		// same logical group, silently duplicating groups.
		h.strGroupIndex = newStrHashTable(h.numIntGroups)
		h.strGroupStates = make([]*groupState, 0, h.numIntGroups)
		h.serializedKeys = make([]string, 0, h.numIntGroups)
		h.serializedKeyBytes = 0
		h.keys = make([][]any, 0, h.numIntGroups)
		for i, gs := range h.intGroupStates {
			k := h.packedKeys[i]
			ext := gs.ensureExtras()
			if ext.keyValues == nil {
				vals := make([]any, len(h.packedLayout))
				for j, f := range h.packedLayout {
					vals[j] = f.get(k)
				}
				ext.keyValues = vals
			}
			h.keyBuf = h.keyBuf[:0]
			for j, f := range h.packedLayout {
				h.keyBuf = appendIntKeyRowFormat(h.keyBuf, f.get(k), h.groupColTypes[j])
			}
			key := string(h.keyBuf)
			h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
			h.strGroupStates = append(h.strGroupStates, gs)
			h.serializedKeys = append(h.serializedKeys, key)
			h.serializedKeyBytes += int64(len(key))
			h.keys = append(h.keys, ext.keyValues)
		}
		h.usePackedGroupKey = false
		h.intGroupStates = nil
		h.numIntGroups = 0
		h.packedIdx = nil
		h.packedTwoLevel = nil
		h.packedKeys = nil
		return
	}
	if !h.useIntGroupKey {
		return
	}
	// Migrate int group key → generic path (binary key format — see the
	// packed-key branch comment).
	h.strGroupIndex = newStrHashTable(h.numIntGroups)
	h.strGroupStates = make([]*groupState, 0, h.numIntGroups)
	h.serializedKeys = make([]string, 0, h.numIntGroups)
	h.serializedKeyBytes = 0
	h.keys = make([][]any, 0, h.numIntGroups)
	for gi, gs := range h.intGroupStates {
		intKey := h.intKeys[gi]
		if gs == nil {
			// Deferred single-int state: reify for the generic path.
			gs = h.gsPool.alloc()
			gs.intKey = intKey
		}
		// Lazily construct keyValues for groups that deferred boxing
		ext := gs.ensureExtras()
		if ext.keyValues == nil {
			ext.keyValues = []any{intKey}
		}
		key := string(appendIntKeyRowFormat(h.keyBuf[:0], intKey, h.groupColTypes[0]))
		h.strGroupIndex.Put([]byte(key), int32(len(h.strGroupStates)))
		h.strGroupStates = append(h.strGroupStates, gs)
		h.serializedKeys = append(h.serializedKeys, key)
		h.serializedKeyBytes += int64(len(key))
		h.keys = append(h.keys, ext.keyValues)
	}
	h.useIntGroupKey = false
	h.intGroupStates = nil
	h.numIntGroups = 0
	h.intGroupIndex = nil
	h.intTwoLevel = nil
	h.intKeys = nil
}

// keySerCol resolves one group-key column's typed accessors once per
// batch, for the generic path's typed lookup loop: per-row hashing and
// key verification run straight off the typed slices instead of
// serializing every row's key (ClickBench Q19: serialization +
// string-hash-table probing was ~30% of the profile).
type keySerCol struct {
	kind  byte // 0=missing 1=i64-class 2=i32-class 3=f64 4=f32 5=bytes-class 6=bool
	i64   []int64
	i32   []int32
	f64   []float64
	f32   []float32
	bools []bool
	bytes *batch.BytesColumn
	nulls *batch.Bitmap
}

// buildKeySerCols resolves the group-key columns of one batch. dst is
// reused across batches (scratch on the sink). Callers gate on
// deferGenericKeyBoxing, whose type set exactly matches the kinds here.
func buildKeySerCols(dst []keySerCol, b *batch.RecordBatch, colIdx []int, colTypes []batch.TypeID) []keySerCol {
	dst = dst[:0]
	for ci, idx := range colIdx {
		if idx < 0 {
			dst = append(dst, keySerCol{kind: 0})
			continue
		}
		v := b.Columns[idx]
		c := keySerCol{nulls: &v.Nulls}
		switch colTypes[ci] {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			c.kind, c.i64 = 1, v.Int64Data
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
			c.kind, c.i32 = 2, v.Int32Data
		case batch.TypeFloat64:
			c.kind, c.f64 = 3, v.Float64Data
		case batch.TypeFloat32:
			c.kind, c.f32 = 4, v.Float32Data
		case batch.TypeString, batch.TypeBytes:
			c.kind, c.bytes = 5, &v.BytesData
		case batch.TypeBool:
			c.kind, c.bools = 6, v.BoolData
		default:
			// Unreachable under the deferGenericKeyBoxing gate; treated as
			// always-null so a gate/kind drift fails loudly in tests
			// (groups collapse) rather than corrupting memory.
			c.kind = 0
		}
		dst = append(dst, c)
	}
	return dst
}

// serializeGroupKey writes one row's group key into buf, byte-identical
// to the appendColumnValue-based serializeKey loop (null flag byte, then
// the typed payload). Only called once per NEW group on the typed path.
func serializeGroupKey(buf []byte, cols []keySerCol, row int) []byte {
	for i := range cols {
		c := &cols[i]
		if c.kind == 0 || c.nulls.IsNullFast(row) {
			buf = append(buf, 1)
			continue
		}
		buf = append(buf, 0)
		switch c.kind {
		case 1:
			v := uint64(c.i64[row])
			buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
				byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
		case 2:
			v := uint32(c.i32[row])
			buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
		case 3:
			v := keyFloat64bits(c.f64[row])
			buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
				byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
		case 4:
			v := keyFloat32bits(c.f32[row])
			buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
		case 5:
			data := c.bytes.Value(row)
			l := uint16(len(data))
			buf = append(buf, byte(l), byte(l>>8))
			buf = append(buf, data...)
		case 6:
			if c.bools[row] {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	}
	return buf
}

// typedRowHash combines per-column hashes of one row's group key. Values
// hash from their typed storage (no serialization); NULL and missing
// columns contribute a fixed marker so (NULL, x) and (x, NULL) differ.
func typedRowHash(cols []keySerCol, row int) uint64 {
	h := uint64(0x9e3779b97f4a7c15)
	for i := range cols {
		c := &cols[i]
		var ch uint64
		if c.kind == 0 || c.nulls.IsNullFast(row) {
			ch = 0xdeadbeefdeadbeef
		} else {
			switch c.kind {
			case 1:
				ch = mix64(uint64(c.i64[row]))
			case 2:
				ch = mix64(uint64(uint32(c.i32[row])))
			case 3:
				ch = mix64(keyFloat64bits(c.f64[row]))
			case 4:
				ch = mix64(uint64(keyFloat32bits(c.f32[row])))
			case 5:
				ch = strHash([]byte(c.bytes.UnsafeStringValue(row)))
			case 6:
				if c.bools[row] {
					ch = 0xb001b001b001b001
				} else {
					ch = 0x0b000b000b000b00
				}
			}
		}
		h = mix64(h ^ ch)
	}
	return h
}

// serializedKeyMatchesRow reports whether a stored binary group key equals
// the key formed by this row — the typed path's chain-verification,
// equivalent to serializing the row and comparing bytes, without the
// serialization.
func serializedKeyMatchesRow(key string, cols []keySerCol, row int) bool {
	for i := range cols {
		c := &cols[i]
		if len(key) == 0 {
			return false
		}
		flag := key[0]
		key = key[1:]
		isNull := c.kind == 0 || c.nulls.IsNullFast(row)
		if isNull != (flag == 1) {
			return false
		}
		if isNull {
			continue
		}
		switch c.kind {
		case 1:
			if len(key) < 8 || int64(binary.LittleEndian.Uint64([]byte(key[:8]))) != c.i64[row] {
				return false
			}
			key = key[8:]
		case 2:
			if len(key) < 4 || int32(binary.LittleEndian.Uint32([]byte(key[:4]))) != c.i32[row] {
				return false
			}
			key = key[4:]
		case 3:
			if len(key) < 8 || binary.LittleEndian.Uint64([]byte(key[:8])) != keyFloat64bits(c.f64[row]) {
				return false
			}
			key = key[8:]
		case 4:
			if len(key) < 4 || binary.LittleEndian.Uint32([]byte(key[:4])) != keyFloat32bits(c.f32[row]) {
				return false
			}
			key = key[4:]
		case 5:
			// Compare the row's own wrapped-uint16 prefix + full data —
			// exact byte-equality with the serialized form, correct even
			// for >64KB values where the stored prefix wraps.
			data := c.bytes.UnsafeStringValue(row)
			l := uint16(len(data))
			if len(key) < 2 || key[0] != byte(l) || key[1] != byte(l>>8) {
				return false
			}
			key = key[2:]
			if len(key) < len(data) || key[:len(data)] != data {
				return false
			}
			key = key[len(data):]
		case 6:
			want := byte(0)
			if c.bools[row] {
				want = 1
			}
			if key[0] != want {
				return false
			}
			key = key[1:]
		}
	}
	return len(key) == 0
}

// decodeSerializedKeyIntoColumns parses a group's binary serialized key
// ([null-flag][typed payload] per column — serializeKey/processRow's
// format) and writes each column's value into the typed output vector at
// row. The hot-output counterpart of decodeSerializedKey: no `any` boxes.
func decodeSerializedKeyIntoColumns(key string, types []batch.TypeID, cols []*batch.Vector, row int) {
	for j, t := range types {
		if len(key) == 0 {
			// WriteNullAt, not bare SetNull: Bytes-class vectors are
			// offset-append storage — a skipped write desyncs every later
			// row's offsets in that column.
			cols[j].WriteNullAt(row)
			continue
		}
		flag := key[0]
		key = key[1:]
		if flag == 1 {
			cols[j].WriteNullAt(row)
			continue
		}
		switch t {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			v := int64(binary.LittleEndian.Uint64([]byte(key[:8])))
			key = key[8:]
			writeIntKeyToColumn(cols[j], row, v, t)
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
			v := int64(int32(binary.LittleEndian.Uint32([]byte(key[:4]))))
			key = key[4:]
			writeIntKeyToColumn(cols[j], row, v, t)
		case batch.TypeFloat64:
			cols[j].Float64Data[row] = math.Float64frombits(binary.LittleEndian.Uint64([]byte(key[:8])))
			key = key[8:]
		case batch.TypeFloat32:
			cols[j].Float32Data[row] = math.Float32frombits(binary.LittleEndian.Uint32([]byte(key[:4])))
			key = key[4:]
		case batch.TypeBool:
			cols[j].BoolData[row] = key[0] == 1
			key = key[1:]
		case batch.TypeString, batch.TypeBytes:
			l := int(uint16(key[0]) | uint16(key[1])<<8)
			key = key[2:]
			cols[j].BytesData.Set(row, []byte(key[:l]))
			key = key[l:]
		default:
			// deferGenericKeyBoxing excludes every other type at resolve;
			// reaching here means the gate and the decoder disagree.
			cols[j].Nulls.SetNull(row)
		}
	}
}

// decodeSerializedKey is the boxed-value variant for cold paths (spill
// drain cursor): parses the binary key into a fresh []any matching what
// consume-time boxing would have produced.
func decodeSerializedKey(key string, types []batch.TypeID) []any {
	vals := make([]any, len(types))
	for j, t := range types {
		if len(key) == 0 {
			continue
		}
		flag := key[0]
		key = key[1:]
		if flag == 1 {
			continue // nil
		}
		// Box types mirror Vector.GetValue exactly (int32 stays int32,
		// float32 stays float32, TypeBytes gives []byte) so the spill
		// cursor's tag dispatch sees the same shapes as eager boxing.
		switch t {
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
			vals[j] = int64(binary.LittleEndian.Uint64([]byte(key[:8])))
			key = key[8:]
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
			vals[j] = int32(binary.LittleEndian.Uint32([]byte(key[:4])))
			key = key[4:]
		case batch.TypeFloat64:
			vals[j] = math.Float64frombits(binary.LittleEndian.Uint64([]byte(key[:8])))
			key = key[8:]
		case batch.TypeFloat32:
			vals[j] = math.Float32frombits(binary.LittleEndian.Uint32([]byte(key[:4])))
			key = key[4:]
		case batch.TypeBool:
			vals[j] = key[0] == 1
			key = key[1:]
		case batch.TypeString:
			l := int(uint16(key[0]) | uint16(key[1])<<8)
			key = key[2:]
			vals[j] = key[:l]
			key = key[l:]
		case batch.TypeBytes:
			l := int(uint16(key[0]) | uint16(key[1])<<8)
			key = key[2:]
			vals[j] = []byte(key[:l])
			key = key[l:]
		}
	}
	return vals
}

// appendIntKeyRowFormat encodes one int-fast-path group key column exactly
// as processRow / consumeBatchGenericSoA encode it from a live batch row:
// a 0x00 not-null flag followed by appendColumnValue's fixed-width little-
// endian bytes for the column type. Int-mode keys are never null (a null
// key migrates the aggregate to the generic path before consumption).
func appendIntKeyRowFormat(buf []byte, key int64, typ batch.TypeID) []byte {
	buf = append(buf, 0)
	switch typ {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		v := int32(key)
		return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	default:
		// Int64 and every type stored in Int64Data (timestamp, ipv4, mac,
		// duration) — 8 bytes, matching appendColumnValue's int64 case.
		return append(buf,
			byte(key), byte(key>>8), byte(key>>16), byte(key>>24),
			byte(key>>32), byte(key>>40), byte(key>>48), byte(key>>56))
	}
}

// resolveBatchAggKernel returns a batch-level aggregate kernel for scalar aggregates.
// Returns nil if the aggregate function is not batch-able (e.g., COUNT(DISTINCT), STRING_AGG).
func resolveBatchAggKernel(fn AggFunc, colIdx int, b *batch.RecordBatch) kernel.BatchAggKernel {
	switch fn {
	case AggSum:
		if colIdx < 0 {
			return nil
		}
		return kernel.ResolveBatchSum(b.Columns[colIdx].Type)
	case AggAvg:
		if colIdx < 0 {
			return nil
		}
		// AVG accumulates float64 for int64-class inputs (overflow-safe).
		return kernel.ResolveBatchAvg(b.Columns[colIdx].Type)
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
	case AggSum:
		return kernel.ResolveRowSum(typ)
	case AggAvg:
		return kernel.ResolveRowAvg(typ)
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
	case AggSum:
		return kernel.ResolveRowSumNoNulls(typ)
	case AggAvg:
		return kernel.ResolveRowAvgNoNulls(typ)
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

// decimalSumOverflow reports a DECIMAL SUM that left the 128-bit range.
//
// PostgreSQL's sum(numeric) is unbounded; wadjet's carrier is an Int128, which
// holds every DECIMAL(38) value but not every SUM of them — two rows near
// 10^38 are enough. The wrapped total is a different number wearing the right
// type, so the query fails here instead of answering it (ADR-0012 item 9). The
// accumulator carries the flag from wherever the add happened (row kernel,
// batch kernel, SoA scatter, merge, spill) to this one emit-time check.
// The SQLSTATE is PostgreSQL's 22003 (numeric_value_out_of_range), which is
// what it raises for an out-of-range numeric result and what ADR-0024 item 4
// requires at every value-producing site. As a bare fmt.Errorf this reached
// clients as the internal-error class, so nothing on the wire distinguished
// "your total is too big" from "the server broke".
func decimalSumOverflow(col string) error {
	if col == "" {
		col = "sum"
	}
	return sqlerr.New("22003", "SUM over a DECIMAL column overflowed the 128-bit exact accumulator (%s): "+
		"the running total is outside the range DECIMAL(38) can represent", col)
}

// decimalScaleConflict reports an accumulator handed DECIMAL values at two
// different scales — see kernel.Accumulator.DecScaleConflict. The unscaled
// integers were added as if they were counted in one scale and they were not,
// so the running total is a different number under either reading. It is the
// same class of refusal as decimalSumOverflow: a value the carrier cannot hold
// correctly is an error, never a plausible wrong number (ADR-0024 item 4).
//
// Every path that could deliver such a pair is closed upstream — the planner
// reconciles set-operation arms, the shuffle writer refuses a cross-scale
// chunk, the shuffle reader refuses a cross-scale stage input — so this is the
// backstop, not the gate. It exists because those three cover the producers
// that exist today and this covers the accumulator itself.
func decimalScaleConflict(col string) error {
	if col == "" {
		col = "the aggregate"
	}
	return sqlerr.New("22003", "a DECIMAL aggregate (%s) was handed values at two different scales: "+
		"the accumulator carries unscaled integers counted in one scale, so the running total "+
		"means a different number under each of them", col)
}

// decimalAvgUnrepresentable reports a DECIMAL AVG whose exact quotient has no
// Int128. It is the SAME refusal decimalSumOverflow makes, one multiplication
// later: AVG scales the sum by 10^AvgScaleIncrement before it divides, so a
// sum well inside the carrier can still have no representable average.
//
// It must be an ERROR and not a NULL. A NULL here is indistinguishable from
// "no rows contributed", so the client cannot tell a missing group from a
// number the engine declined to print — and the DAG's own fold raises on this
// condition (worker.writeDecimalAvgColumn), so answering NULL on the
// single-process path would make the two paths disagree about the same query
// (ADR-0018 §3, the two-path contract).
// It carries the same SQLSTATE as decimalSumOverflow, 22003, for the same
// reason: it is the same refusal one multiplication later (ADR-0024 item 4).
func decimalAvgUnrepresentable(col string) error {
	if col == "" {
		col = "avg"
	}
	return sqlerr.New("22003", "AVG over a DECIMAL column has no exact 128-bit value (%s): "+
		"the sum scaled to the output's scale is outside the range DECIMAL(38) can represent", col)
}

// decAggErr returns the error for aggregate j when its accumulator cannot
// answer EXACTLY: a DECIMAL SUM that wrapped, or a DECIMAL AVG whose exact
// quotient has no Int128. nil otherwise.
//
// The AVG arm divides here and again in the writer. That is deliberate: the
// alternative is an error return threaded through writeAccToColumn's typed
// fast path, and a DECIMAL AVG is not on any hot path (TPC-H's prices are
// FLOAT64), so the second division costs nothing that shows.
func (h *HashAggregate) decAggErr(acc *kernel.Accumulator, j int, fn AggFunc) error {
	name := func() string {
		if j < len(h.Aggs) {
			return h.Aggs[j].OutputCol
		}
		return ""
	}
	if acc.DecOverflow {
		return decimalSumOverflow(name())
	}
	if acc.DecScaleConflict || h.decScaleConflict {
		return decimalScaleConflict(name())
	}
	if fn == AggAvg && acc.IsDecimal && acc.Count > 0 {
		if _, ok := acc.DecimalAvg(); !ok {
			return decimalAvgUnrepresentable(name())
		}
	}
	return nil
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
		// keyFloat64bits, not Float64bits: every NaN payload folds onto one
		// NaN and -0.0 onto +0.0, because the comparator calls those pairs
		// EQUAL (kernel/float_order.go) and a key that split them would make
		// GROUP BY, DISTINCT and a hash join disagree with ORDER BY, with
		// `rank()`, and with the spilled merge key that already folds them
		// (#459).
		val := keyFloat64bits(v.Float64Data[row])
		return append(buf,
			byte(val), byte(val>>8), byte(val>>16), byte(val>>24),
			byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
	case batch.TypeFloat32:
		val := keyFloat32bits(v.Float32Data[row])
		return append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24))
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		data := v.BytesData.Value(row)
		l := uint16(len(data))
		buf = append(buf, byte(l), byte(l>>8))
		return append(buf, data...)
	case batch.TypeCIDR:
		// PostgreSQL's inet equality, not byte equality of the stored TEXT:
		// '10.0.0.1' and '10.0.0.1/32' are the SAME value there (#492), so
		// they must be the SAME key here too or GROUP BY, DISTINCT,
		// COUNT(DISTINCT) and a hash join all split one value into two
		// (#520) — kernel.CidrOrderKey is the canonical re-key every other
		// CIDR comparison site (filter, sort, MIN/MAX) already uses.
		key := kernel.CidrOrderKey(v.BytesData.UnsafeStringValue(row))
		l := uint16(len(key))
		buf = append(buf, byte(l), byte(l>>8))
		return append(buf, key...)
	case batch.TypeBool:
		if v.BoolData[row] {
			return append(buf, 1)
		}
		return append(buf, 0)
	case batch.TypeDecimal:
		// The value's canonical (unscaled, minimal scale) digits, exactly —
		// NOT its float64. A float64 holds ~16 significant digits and a
		// DECIMAL(38,10) holds 38, so keying on the double merged every pair
		// of values that agreed to 16 digits into one group / one distinct
		// value / a join match (#474). Scale-normalized so that 12.75 keys
		// alike whether the column declares scale 2 or scale 4, which is what
		// the comparator says and therefore what a cross-scale join needs.
		return batch.AppendDecimalKey(buf, v.DecimalData.Data[row], v.DecimalData.Scale)
	case batch.TypeVector:
		return appendVectorKey(buf, v, row)
	case batch.TypeArray, batch.TypeMap:
		return appendListKey(buf, v, row)
	case batch.TypeRow:
		return appendRowKey(buf, v, row)
	default:
		// Unreachable for a column type: every one of the 22 has an arm
		// above. Kept as a loud marker rather than a silent constant — the
		// constant '?' that used to live here collapsed every ARRAY, ROW, MAP
		// and VECTOR value into ONE key, so GROUP BY, DISTINCT, the set
		// operations, COUNT(DISTINCT), APPROX_DISTINCT and hash-join keys all
		// answered one group / one distinct value / a cross join.
		return append(buf, "\x00unsupported-key-type"...)
	}
}

// appendVectorKey encodes a VECTOR cell: its dimension, then every element's
// IEEE-754 bits. The dimension is part of the key because two vectors of
// different width are different values even when one is the other's prefix.
func appendVectorKey(buf []byte, v *batch.Vector, row int) []byte {
	dim := v.VectorDim
	buf = appendKeyUint32(buf, uint32(dim))
	if dim <= 0 {
		// A genuinely zero-width VECTOR column: every value IS the empty
		// vector, so one group is the right answer, not a lost one.
		return buf
	}
	base := row * dim
	if base+dim > len(v.Float32Data) {
		panic(malformedKeyColumn("VECTOR", "declares dimension %d but row %d needs %d floats and the column stores %d",
			dim, row, base+dim, len(v.Float32Data)))
	}
	for _, f := range v.Float32Data[base : base+dim] {
		// keyFloat32bits: the VECTOR element comparator folds NaN payloads
		// and -0.0 (kernel/float_order.go), and the boxed VECTOR key
		// (appendKeyFloats, sort.go) already folds them — this arm was the
		// last one keying raw IEEE bits.
		buf = appendKeyUint32(buf, keyFloat32bits(f))
	}
	return buf
}

// appendListKey encodes an ARRAY or MAP cell: the element count, then each
// element. MAP is stored as ARRAY(ROW(key,value)), so it rides the same code —
// and, like every other engine path over MAP, it treats entry ORDER as part of
// the value rather than canonicalising it.
//
// The length prefix is what makes the encoding injective: without it
// [["a"],["b"]] and [["a","b"]] would produce the same bytes.
func appendListKey(buf []byte, v *batch.Vector, row int) []byte {
	if row+1 >= len(v.Offsets) {
		panic(malformedKeyColumn(v.Type.String(), "row %d needs offsets[%d] but the column carries %d",
			row, row+1, len(v.Offsets)))
	}
	start, end := int(v.Offsets[row]), int(v.Offsets[row+1])
	if end < start {
		end = start
	}
	buf = appendKeyUint32(buf, uint32(end-start))
	if v.Child == nil {
		if end > start {
			panic(malformedKeyColumn(v.Type.String(), "row %d spans elements [%d,%d) but the column has no child vector",
				row, start, end))
		}
		return buf
	}
	for i := start; i < end; i++ {
		buf = appendNestedElem(buf, v.Child, i)
	}
	return buf
}

// malformedKeyColumn is what a container column whose STORAGE contradicts its
// own shape raises. The guards used to fall back to a constant key, which is
// the #408 defect wearing a different hat: a constant makes every row of the
// column one group, so GROUP BY, DISTINCT, the set operations and the hash
// joins all answer from a single key and the query returns a plausible wrong
// answer instead of failing. A FatalEvalPanic is the one way to be loud from
// here — appendColumnValue has no error return, and giving it one would put a
// check in the innermost key loop of every grouped query.
func malformedKeyColumn(typ, format string, args ...any) fatalEvalError {
	return fatalEvalError{sqlerr.New("XX000", "malformed %s group key column: "+format, append([]any{typ}, args...)...)}
}

// appendRowKey encodes a ROW cell: the field count, then each field at this
// row. Field NAMES are not encoded — every row of one column carries the same
// schema, so they add bytes and no discrimination.
func appendRowKey(buf []byte, v *batch.Vector, row int) []byte {
	buf = appendKeyUint32(buf, uint32(len(v.Children)))
	for _, ch := range v.Children {
		buf = appendNestedElem(buf, ch, row)
	}
	return buf
}

// appendNestedElem writes one nested element: a null flag, then the value.
// The flag is what keeps a NULL element distinct from a zero one, the same way
// the top-level key loops flag a NULL column value before calling
// appendColumnValue.
func appendNestedElem(buf []byte, child *batch.Vector, i int) []byte {
	if child == nil || i >= child.Len || child.Nulls.IsNullFast(i) {
		return append(buf, 1)
	}
	buf = append(buf, 0)
	return appendColumnValue(buf, child, i, child.Type)
}

func appendKeyUint32(buf []byte, val uint32) []byte {
	return append(buf, byte(val), byte(val>>8), byte(val>>16), byte(val>>24))
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
// offheapReg returns the aggregate's off-heap registry, creating it on
// first use when the platform path is available. nil (heap fallback in
// every memory.Offheap constructor) otherwise.
func (h *HashAggregate) offheapReg() *memory.OffheapRegistry {
	if h.offheap == nil && memory.OffheapAvailable() {
		h.offheap = memory.NewOffheapRegistry()
	}
	return h.offheap
}

// aggNeedsCount reports whether an aggregate's flat state includes a row
// count. MIN/MAX do not: no scatter kernel writes their count[], and every
// consumer (finalizeKernelAcc, writeAccToColumn, the partial-spill
// emitAcc/readAcc format) drives MIN/MAX purely off HasMin/HasMax. The array
// was 8 bytes per group per aggregate of permanent zeroes.
func aggNeedsCount(fn AggFunc) bool {
	return fn == AggSum || fn == AggAvg || fn == AggCount
}

// planCountArrays decides, per aggregate, which aggregate's count[] it reads.
// Result[i] == i means "owns its own"; result[i] == j < i means "shares j's".
//
// Two aggregates may share a count array only when every row increments both
// counts or neither — i.e. their count kernels run over an identical
// predicate. That holds exactly when:
//
//   - both are COUNT(*) (no input column: every row with a live group index
//     counts), or
//   - both read the SAME input column AND both count kernels are guaranteed
//     to fire for that column's type.
//
// The type guard matters: scatterFlatAggUpdate's SUM/AVG dispatch has no case
// for e.g. Bool or String, so SUM over such a column silently increments
// nothing while COUNT over it increments every non-null row. Restricting
// sharing to the numeric set the SUM/AVG switches actually handle keeps the
// two predicates identical.
//
// MIN/MAX never participate — they have no count at all (aggNeedsCount).
//
// NOT shared: aggregates over different columns, even when the data happens
// to have no nulls in either. Null-ness is a per-batch property, so that
// equality isn't provable at plan time. ClickBench Q33's three aggregates
// (COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)) fall in exactly that
// bucket and each keep their own count.
func (h *HashAggregate) planCountArrays(b *batch.RecordBatch) []int32 {
	plan := make([]int32, len(h.Aggs))
	for i := range plan {
		plan[i] = int32(i)
	}
	// classOf returns a shareable class key, or ok=false for "never shares".
	classOf := func(i int) (int, bool) {
		agg := h.Aggs[i]
		if !aggNeedsCount(agg.Func) {
			return 0, false
		}
		ci := h.aggColIdx[i]
		if ci < 0 {
			// COUNT(*): counts every row of every group.
			return -1, agg.Func == AggCount
		}
		if agg.Func != AggCount && !isFlatSumType(b.Columns[ci].Type) {
			return 0, false
		}
		return ci, true
	}
	owners := make(map[int]int32, len(h.Aggs))
	for i := range h.Aggs {
		key, ok := classOf(i)
		if !ok {
			continue
		}
		if owner, seen := owners[key]; seen {
			plan[i] = owner
			continue
		}
		owners[key] = int32(i)
	}
	return plan
}

// isFlatSumType reports whether scatterFlatAggUpdate's SUM/AVG dispatch has a
// case for this column type (and therefore increments count for it).
func isFlatSumType(t batch.TypeID) bool {
	switch t {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration,
		batch.TypeInt32, batch.TypePort, batch.TypeDate,
		batch.TypeFloat64, batch.TypeFloat32, batch.TypeDecimal:
		return true
	}
	return false
}

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
	reg := h.offheapReg()
	if h.InputRowHint > int64(initCap)*8 {
		est := int(h.InputRowHint / 8)
		if est > flatInitCap {
			est = flatInitCap
		}
		initCap = est
	}

	// Count-array plan: which aggregates need a count[] of their own, and
	// which can read another aggregate's. See planCountArrays for the
	// safety argument.
	countPlan := h.planCountArrays(b)

	for i, agg := range h.Aggs {
		fa := &h.intFlatAccs[i]
		fa.countFrom = countPlan[i]
		if fa.countFrom == int32(i) && aggNeedsCount(agg.Func) {
			fa.count = memory.Offheap[int64](reg, initCap)
		}

		ci := h.aggColIdx[i]
		if ci < 0 {
			continue // COUNT(*) only needs count
		}
		typ := b.Columns[ci].Type

		switch agg.Func {
		case AggSum, AggAvg:
			switch typ {
			case batch.TypeFloat64, batch.TypeFloat32:
				fa.sumF64 = memory.Offheap[float64](reg, initCap)
				fa.isFloat = true
			case batch.TypeDecimal:
				fa.sumDec = memory.Offheap[batch.Int128](reg, initCap)
				fa.isDecimal = true
				fa.decScale = b.Columns[ci].DecimalData.Scale
			case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
				if agg.Func == AggAvg {
					// Overflow-safe AVG: float64 accumulation, matching
					// scatterFlatAggUpdate's AggAvg case and the row kernels.
					fa.sumF64 = memory.Offheap[float64](reg, initCap)
					fa.isFloat = true
				} else {
					fa.sumI64 = memory.Offheap[int64](reg, initCap)
				}
			default: // int32-class: exact int64 sum, cannot overflow
				fa.sumI64 = memory.Offheap[int64](reg, initCap)
			}
		case AggCount:
			// count[] is all we need
		case AggMin:
			switch typ {
			case batch.TypeFloat64, batch.TypeFloat32:
				fa.minF64 = memory.Offheap[float64](reg, initCap)
				fa.isFloat = true
			case batch.TypeDecimal:
				fa.minDec = memory.Offheap[batch.Int128](reg, initCap)
				fa.isDecimal = true
				// The SUM/AVG arm above always set this; the min/max arms
				// did not, and loadAccFromFlat finalizes MinDec through
				// ToFloat64(DecScale) — so a scale-4 column would have
				// answered 10000x too large the moment the scatter started
				// writing anything at all.
				fa.decScale = b.Columns[ci].DecimalData.Scale
			default:
				fa.minI64 = memory.Offheap[int64](reg, initCap)
			}
			fa.hasMin = memory.Offheap[bool](reg, initCap)
		case AggMax:
			switch typ {
			case batch.TypeFloat64, batch.TypeFloat32:
				fa.maxF64 = memory.Offheap[float64](reg, initCap)
				fa.isFloat = true
			case batch.TypeDecimal:
				fa.maxDec = memory.Offheap[batch.Int128](reg, initCap)
				fa.isDecimal = true
				fa.decScale = b.Columns[ci].DecimalData.Scale
			default:
				fa.maxI64 = memory.Offheap[int64](reg, initCap)
			}
			fa.hasMax = memory.Offheap[bool](reg, initCap)
		}
	}

	h.groupIndexBuf = make([]int32, batch.DefaultBatchSize)

	// The flags were just resolved from this batch's column types; latch them
	// now, because the drain that needs them may run after the arrays holding
	// them have been cleared (latchAggEncodings).
	h.latchAggEncodings()
}

// loadAccFromFlat fills dst from a flatAccumArrays row at index gi. countArr
// is the aggregate's resolved count array (countArrayOf) — nil for MIN/MAX,
// or the owner's array when this aggregate shares one. The
// SoA-direct counterpart to materializing per-group accs into extras.accs:
// callers build a stack-local Accumulator (which the compiler sees value-
// typed and proves doesn't escape) and pass it to a finalizer that doesn't
// retain it. At SF100 Q17 scale (20M groups) this saves a multi-GB heap
// pass through materializeFlatAccums.
func loadAccFromFlat(fa *flatAccumArrays, countArr []int64, gi int, dst *kernel.Accumulator) {
	dst.Count = 0
	if countArr != nil {
		dst.Count = countArr[gi]
	}
	dst.IsFloat = fa.isFloat
	dst.IsDecimal = fa.isDecimal
	dst.DecScale = fa.decScale
	dst.DecOverflow = fa.sumDecOverflow
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
	// This is the one funnel that CLEARS the flat arrays, so it is where the
	// spill format's encoding flags have to be preserved: a drain landing
	// after it (the migrating batch's, MergeSink's normalization) reads the
	// latch instead of the nil arrays.
	h.latchAggEncodings()
	nAggs := len(h.Aggs)
	// String GROUP BY and generic SoA use strGroupStates with SoA flat accumulators.
	if h.useStrGroupKey || h.useGenericSoA {
		for gi, gs := range h.strGroupStates {
			if gs == nil {
				// Deferred state (typed-generic / str paths): reify for
				// the migration/merge cold path.
				gs = h.gsPool.alloc()
				h.strGroupStates[gi] = gs
			}
			ext := gs.ensureExtras()
			if ext.accs == nil {
				ext.accs = make([]kernel.Accumulator, nAggs)
				h.extrasAccsCount += int64(nAggs)
			}
			for ai := range h.intFlatAccs {
				fa := &h.intFlatAccs[ai]
				// Resolve the SHARED count array before the bound check.
				// flatAccumLen probes an aggregate's OWN arrays, and a
				// count-sharing COUNT owns none of them (count nil by
				// definition, no sum, no min/max) — so it measured 0 for
				// every group, the guard below skipped it unconditionally,
				// and its accumulator stayed at the zero value. Sharing is
				// by COLUMN CLASS (planCountArrays), so that is every COUNT
				// that is not the FIRST count-needing aggregate over its
				// column: `SUM(v), COUNT(v)`, `AVG(v), COUNT(v)`, a second
				// COUNT(*), the second and third of three COUNT(v). Each
				// emitted 0 while the aggregate it shares with emitted the
				// right number, on whichever groups happened to reach this
				// materialize (#402).
				ca := countArrayOf(h.intFlatAccs, ai)
				// Defensive: a gi that wasn't appended to the SoA arrays
				// (can happen when compact-to-generic migration runs with
				// no rows consumed, leaving intFlatAccs cap=0 while
				// strGroupStates carries migrated entries) would otherwise
				// index a zero-length array and panic. Leave the
				// accumulator at its zero value so downstream kernels
				// emit identity output rather than crashing the worker.
				// Both bounds, because the two arrays are the same length
				// whenever both are live: growTo extends every array the
				// aggregate owns to the same n each batch, and the shared
				// count array is grown by its OWNER on the same schedule.
				// The only aggregate whose own length can lag is a sharer
				// that owns no arrays at all, and `gi >= len(ca)` is what
				// keeps it in.
				if gi >= flatAccumLen(fa) && gi >= len(ca) {
					continue
				}
				acc := &ext.accs[ai]
				acc.Count = 0
				if gi < len(ca) {
					acc.Count = ca[gi]
				}
				acc.IsFloat = fa.isFloat
				acc.IsDecimal = fa.isDecimal
				acc.DecScale = fa.decScale
				acc.DecOverflow = fa.sumDecOverflow
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
	// Single-int and packed keys defer per-group state entirely: numIntGroups is
	// the count and intGroupStates is empty. Reify the slice here — the
	// migration/merge cold paths this feeds all read intGroupStates.
	if len(h.intGroupStates) < h.numIntGroups {
		reified := make([]*groupState, h.numIntGroups)
		copy(reified, h.intGroupStates)
		h.intGroupStates = reified
	}
	for gi, gs := range h.intGroupStates {
		if gs == nil {
			// Deferred state (single-int and packed-key paths): reify for
			// the migration/merge cold path that needs boxed extras. The
			// single-int key rides along so post-reify readers of
			// gs.intKey stay correct; composite keys stay in their SoA.
			gs = h.gsPool.alloc()
			if h.useIntGroupKey && gi < len(h.intKeys) {
				gs.intKey = h.intKeys[gi]
			}
			h.intGroupStates[gi] = gs
		}
		ext := gs.ensureExtras()
		if ext.accs == nil {
			ext.accs = make([]kernel.Accumulator, nAggs)
			h.extrasAccsCount += int64(nAggs)
		}
		for ai := range h.intFlatAccs {
			fa := &h.intFlatAccs[ai]
			acc := &ext.accs[ai]
			acc.Count = 0
			if ca := countArrayOf(h.intFlatAccs, ai); ca != nil {
				acc.Count = ca[gi]
			}
			acc.IsFloat = fa.isFloat
			acc.IsDecimal = fa.isDecimal
			acc.DecScale = fa.decScale
			acc.DecOverflow = fa.sumDecOverflow
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
	nGroups := h.numIntGroups
	if h.useStrGroupKey || h.useGenericSoA {
		groups = h.strGroupStates
		nGroups = len(groups)
	} else {
		groups = h.intGroupStates
	}

	// Size every array to the group count first, so slots survive even for
	// group slices that were left deferred (empty) by the SoA paths.
	for ai := range h.intFlatAccs {
		h.intFlatAccs[ai].growTo(nGroups)
	}
	for gi, gs := range groups {
		if gs == nil {
			continue
		}
		for ai := range h.intFlatAccs {
			fa := &h.intFlatAccs[ai]
			// extras may be nil if rebuild runs before any materialize/Group
			// path allocated them; treat those as the "no accumulators yet"
			// case the original `gs.accs == nil` branch handled.
			if gs.extras == nil || gs.extras.accs == nil || ai >= len(gs.extras.accs) {
				continue
			}
			acc := &gs.extras.accs[ai]
			if fa.count != nil {
				fa.count[gi] = acc.Count
			}
			if fa.sumI64 != nil {
				fa.sumI64[gi] = acc.SumI64
			}
			if fa.sumF64 != nil {
				fa.sumF64[gi] = acc.SumF64
			}
			if fa.sumDec != nil {
				fa.sumDec[gi] = acc.SumDec
			}
			if acc.DecOverflow {
				fa.sumDecOverflow = true
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
