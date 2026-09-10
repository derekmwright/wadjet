package exec

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
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
	// AggOhlcv is the bar (#965, ADR-0035): ohlcv(ts, price, volume) folded
	// into ONE mergeable state and finished as a ROW. AggOhlcvState and
	// AggOhlcvStateMerge carry that state across a partial/final split, the
	// way the variance and covariance pairs above do — a finished bar cannot
	// be re-aggregated (a MAX of two bars is not a bar), but the state can,
	// because its merge is associative and commutative.
	AggOhlcv
	AggOhlcvState
	AggOhlcvStateMerge
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
	Separator       string // separator for STRING_AGG (default ',')
	InputCol2       string // second input column (corr, covar, min_by, max_by)
	InputCol3       string // third input column (ohlcv's volume, #965)
	// OutputFields declares a ROW-valued aggregate's fields — ohlcv's bar.
	// The output vector is built from them, so a missing list is a bar that
	// cannot be written at all rather than one written wrong.
	OutputFields []parquet.Column
	Percentile   float64 // percentile value for percentile_cont/percentile_disc
	// Distinct is SQL's `AGG(DISTINCT x)` for every aggregate but COUNT,
	// which spells it as its own AggFunc (AggCountDistinct) because its whole
	// state IS the set.
	//
	// It used to be dropped for all the others: the planner mapped the flag
	// onto AggCountDistinct and threw it away otherwise, so over decpair
	// `SUM(DISTINCT a)` answered 52.99 for PostgreSQL's 14.74 and
	// `AVG(DISTINCT a)` 7.570000 for 3.685 — the plain aggregate wearing the
	// DISTINCT spelling, silently (#703). MIN/MAX were right by construction
	// and stay so; every other aggregate now dedupes its input.
	//
	// The set is the same distinctSet COUNT(DISTINCT) uses and it is keyed on
	// the value at its EXACT type (ADR-0023's encoding through
	// appendColumnValue), so a DECIMAL dedupes at its own scale rather than
	// through a float.
	Distinct bool
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
	// GroupingCalls / GroupingCallNames implement SQL's GROUPING(a[, b, ...])
	// over those sets: one entry per call, holding the GroupByCols positions
	// its arguments name IN ARGUMENT ORDER, and the output column the bitmask
	// is published under. Argument order is the answer — the leftmost
	// argument is the most significant bit — so these are positions, not a
	// set (#804).
	//
	// Only the operator that assigned a row its grouping set can answer this:
	// a key that is NULL because its set excluded it is indistinguishable, in
	// the output, from a key that is NULL in the data. That is the whole
	// reason SQL has the function.
	GroupingCalls     [][]int
	GroupingCallNames []string
	InputRowHint      int64 // estimated input rows for pre-sizing hash table
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
	genKeyIdx   *intHashTable
	genKeyNext  []int32
	keySerCols  []keySerCol // per-batch resolved key accessors (scratch)
	groupColIdx []int
	aggColIdx   []int
	aggColIdx2  []int // second column indices for two-column aggregates
	aggColIdx3  []int // third column indices (ohlcv's volume, #965)
	aggOhlcvDom []ohlcvDomain
	// aggVolMeta is the DECLARATION of ohlcv's third input column, kept for
	// the same reason aggInputMeta keeps the first's: the bar's `volume`
	// field is SUM(volume)'s type, which a bare TypeID cannot carry for a
	// DECIMAL.
	aggVolMeta       []parquet.Column
	groupColTypes    []batch.TypeID
	groupColMeta     []parquet.Column // full input column metadata per group col (Decimal Scale/Precision survive into outputSchema)
	aggInputTypes    []batch.TypeID   // observed input column type per aggregate (0 = unresolved)
	aggInputMeta     []parquet.Column // full input column metadata per aggregate (MIN/MAX over a container and MIN_BY/MAX_BY re-emit their input's Scale/Fields/ElementType/Dimension)
	aggInputDecScale []int            // DECIMAL input scale per aggregate, read from the VECTOR (not the schema column) so the declared output scale is the one the Int128 accumulator actually counts in (#455)
	// decScaleConflict is the operator-wide twin of
	// kernel.Accumulator.DecScaleConflict, for the GROUPED paths whose state
	// lives in the flat SoA arrays: those hold one scale per aggregate and no
	// per-group accumulator to carry a flag on, so the latch is here and
	// aggEmitErr reads it for every group. See Consume, where it is set.
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
