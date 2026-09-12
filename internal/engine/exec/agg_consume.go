// This file holds aggregate initialization, input binding, and batch consumption.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// NewHashAggregate builds a hash aggregate over groupByCols and aggs.
//
// strNullGroupIdx is set here as well as in Init and CloneSink because its
// zero value is the VALID slot 0: an aggregate that consumes before Init would
// otherwise bind every NULL key of a single STRING or BYTES GROUP BY to
// whichever group it minted first, silently (#1058). Init is still the
// contract — the worker's morsel-parallel branch skipping it is what made that
// reachable — and this is the representation refusing to hold the state that
// made the omission a wrong ANSWER rather than a loud failure.
func NewHashAggregate(groupByCols []string, aggs []AggColumn) *HashAggregate {
	return &HashAggregate{
		GroupByCols:     groupByCols,
		Aggs:            aggs,
		strNullGroupIdx: -1,
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
		// CHARGE what this buffer now holds. The rows are ours: `ToRows`
		// copies them out of the batch, which the pipeline releases as soon
		// as Consume returns, so from here the buffer is memory the tracker
		// cannot otherwise see.
		//
		// Every release site — flushSpillBuffer, Finalize's drain of the
		// unflushed tail, and Close — already released `spillBufferBytes`
		// ("release the tracker bytes charged for them", flushSpillBuffer),
		// and nothing ever charged them. So the ledger lost this figure on
		// every query that took the raw-row branch: measured at 931,840 bytes
		// released against zero charged on the filing's own shape, which is
		// what drove the query tracker to -165,652 (#862). A negative ledger
		// is not cosmetic — from there every admission is measured against a
		// floor below the memory that exists.
		//
		// The estimate is the batch's own MemBytes, deliberately the SAME
		// figure the release sites use, so the pair is exact. It understates
		// the boxed rows the buffer actually holds; making it truthful is a
		// separate change, and one that must not arrive as an unpaired half.
		bufBytes := b.MemBytes()
		h.spillBufferBytes += bufBytes
		if h.Spill != nil && bufBytes > 0 {
			h.Spill.TrackBatch(bufBytes)
			if h.accInstanceID != 0 {
				h.Spill.Tracker().PublishOwned(h.accInstanceID, h.trackedGroupMem+h.spillBufferBytes)
			}
		}
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

// columnIndexFallback tries exact, then bare lookup for a qualified name, then
// one schema column ending in ".col" for either bare or missed qualified names.
// Two or more suffix matches return -1; never guess an arm (#762, #656, #742).
// ResolveColumnIndex supplies folded-identifier matching (#731), including catalog
// CamelCase; delimited references do not fold. See internal/engine/batch/schema.go.
// ROW field paths cannot resolve to a column index; the planner must materialize them.
func columnIndexFallback(b *batch.RecordBatch, name string) int {
	if idx := b.ResolveColumnIndex(name); idx >= 0 {
		return idx
	}
	// A ROW FIELD PATH is not a column and this resolver cannot serve one:
	// its callers want an INDEX, and a field has none — the container's index
	// would hand them the whole ROW. ADR-0022 rule 2 says such a reference is
	// MATERIALIZED by the planner, so reaching here with one means the plan
	// did not, and -1 is the honest answer: the caller's own miss error names
	// the column and the class, where stripping the qualifier bound the
	// reference to whatever OTHER relation in the stream publishes a column of
	// the FIELD's name.
	//
	// `GROUP BY c_row.b` beside a join arm publishing `b` is the shape: the
	// key took that arm's DECIMAL while the DECLARATION said INT64, and #361's
	// silent-write guard — a guard about something else entirely — was all
	// that stood between it and a wrong number (#769 round 1). This is the
	// SEVENTH resolver of the six the field-path reorder reached, and the one
	// the group keys, the aggregate inputs, the sort keys and the join keys
	// all come through.
	if _, _, ok := b.RowFieldPath(name); ok {
		return -1
	}
	bare := name
	if dotIdx := strings.Index(name, "."); dotIdx >= 0 {
		bare = name[dotIdx+1:]
		if idx := b.ResolveColumnIndex(bare); idx >= 0 {
			return idx
		}
	}
	// No exact match under this spelling: scan for a single qualified
	// match ".bare". Reject ambiguity (>1 match) so we never silently
	// pick the wrong column.
	suffix := "." + bare
	match := -1
	folded := batch.IsFoldedIdent(bare)
	for i, c := range b.Schema {
		if strings.HasSuffix(c.Name, suffix) ||
			(folded && len(c.Name) >= len(suffix) &&
				batch.EqualFoldIdent(c.Name[len(c.Name)-len(suffix):], suffix)) {
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
	// 0A000, the same class its sort and window siblings carry: this is a
	// planner bound reaching a client on a query PostgreSQL answers, and the
	// message keeps naming the bug while the class says what a client can do
	// about it (#649's invariant, #776's shape).
	return sqlerr.New("0A000",
		"hash aggregate: %s %q is not a column of its input (input has: %s)",
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
	case AggOhlcv, AggOhlcvState:
		// ohlcv's InputCol2 is its ORDERING key — the instant. AggOhlcvState
		// is here beside AggOhlcv because a PARTIAL bar reads raw rows too;
		// only the MERGE form reads an encoded state and nothing else.
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
	h.aggColIdx3 = make([]int, len(h.Aggs))
	h.aggOhlcvDom = make([]ohlcvDomain, len(h.Aggs))
	h.aggVolMeta = make([]parquet.Column, len(h.Aggs))
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
		h.aggColIdx3[i] = -1 // default: no third column
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
			h.aggUpdaters[i] = resolveAggUpdater(agg, b.Columns[idx].Type)
			h.aggUpdatersNoNull[i] = resolveAggUpdaterNoNull(agg, b.Columns[idx].Type)
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
		if agg.InputCol3 != "" {
			h.aggColIdx3[i] = columnIndexFallback(b, agg.InputCol3)
			// Loud for the one function that READS it, stale metadata on the
			// merge stage above it — readsSecondColumn's rule.
			// AggOhlcvState is here beside AggOhlcv because a PARTIAL bar
			// reads the raw columns too; only the MERGE form consumes an
			// encoded state and carries InputCol3 as stale metadata. Without
			// it, `ohlcv(ts, price, volume*2)` failed loud in process and
			// answered a NULL bar on the DAG (#965, #713's boundary).
			if h.aggColIdx3[i] < 0 && (agg.Func == AggOhlcv || agg.Func == AggOhlcvState) {
				return unresolvedAggColumn("aggregate input", agg.InputCol3, b)
			}
			if idx := h.aggColIdx3[i]; idx >= 0 && idx < len(b.Schema) {
				h.aggVolMeta[i] = b.Schema[idx]
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
		case AggOhlcv, AggOhlcvState:
			// The bar's CARRIER, resolved from the vectors the operator will
			// actually read — through exec.OhlcvDomainFor, the same function
			// the planner asked to DECLARE the ROW. One question, one answer,
			// so the bar cannot be declared exact and computed as a float.
			//
			// An argument type that has no bar is REFUSED here rather than
			// answered. The refusal is at the OPERATOR, which both the
			// single-process pipeline and every worker fragment run, so one
			// sentence reaches a client whichever path planned the query. A
			// silent NULL was the alternative and it is the worst of the
			// three: `ohlcv(text_col, price, volume)` folded no rows and
			// answered an empty bar (#965).
			px, tsi, vol := h.aggColIdx[i], h.aggColIdx2[i], h.aggColIdx3[i]
			if px >= 0 && tsi >= 0 && vol >= 0 {
				pv, tv, vv := b.Columns[px], b.Columns[tsi], b.Columns[vol]
				dom, err := ohlcvResolveDomain(tv, pv, vv)
				if err != nil {
					return err
				}
				h.aggOhlcvDom[i] = dom
			}
		}
	}

	h.resolved = true

	// Check if all aggregates use simple kernel updaters (no COUNT(DISTINCT),
	// STRING_AGG, etc. which need the generic processRow path).
	allSimpleAggs := true
	for i, agg := range h.Aggs {
		// A DISTINCT aggregate reads its own value set before it accumulates,
		// which the flat scatter kernels have no room for: they update an
		// array slot from a vector and never look at what came before. So a
		// DISTINCT aggregate takes the generic processRow path, exactly as
		// COUNT(DISTINCT) already does (#703).
		if agg.Distinct {
			allSimpleAggs = false
			h.needsDistinct = true
		}
		switch agg.Func {
		case AggCountDistinct, AggApproxDistinct:
			allSimpleAggs = false
			h.needsDistinct = true
		case AggStringAgg, AggStddev, AggVariance, AggStddevPop, AggVarPop,
			AggVarState, AggVarStateMerge,
			AggBoolAnd, AggBoolOr, AggCorr, AggCovarSamp, AggCovarPop,
			AggCovarState, AggCovarStateMerge,
			AggPercentileCont, AggPercentileDisc, AggMedian,
			AggMinBy, AggMaxBy,
			AggOhlcv, AggOhlcvState, AggOhlcvStateMerge:
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
			if !genericKeyBoxingDeferrable(t) {
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
			h.batchAggKernels[i] = resolveBatchAggKernel(agg, h.aggColIdx[i], b)
			if h.batchAggKernels[i] == nil {
				allBatchable = false
			}
			// A DISTINCT aggregate has no batch kernel, for the same reason
			// it has no flat scatter one: a batch kernel folds a whole vector
			// into an accumulator and never asks what the group has already
			// seen. resolveBatchAggKernel answers by FUNC, so it declines
			// COUNT(DISTINCT) — which is its own AggFunc — and happily
			// returned SUM's for `SUM(DISTINCT a)`. UNGROUPED was therefore
			// the one shape #703's fix missed: `SELECT SUM(DISTINCT a) FROM
			// decpair` stayed 52.99 while the grouped form and the same query
			// with a COUNT(DISTINCT) beside it (which declines the fast path
			// for its own reason) both answered PostgreSQL's 14.74.
			if agg.Distinct {
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
