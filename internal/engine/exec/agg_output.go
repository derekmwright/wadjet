// This file holds aggregate result emission and declared output schemas.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"context"
	"math"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

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
			if err := h.aggEmitErr(&h.scalarAccs[j], j, agg.Func); err != nil {
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

	// One mask per (grouping set, GROUPING call); nil when the query has no
	// GROUPING(...). Depends on nothing per-row, so it is computed once here
	// rather than per group (#804).
	gMasks := h.groupingMasks()

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
					out.Columns[colIdx].SetValue(i, state.render())
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
			case AggOhlcvState, AggOhlcvStateMerge:
				// Partial output: the STATE, for the merge stage above (or
				// the final fold) to combine. Never a finished bar — a bar
				// is not re-aggregatable, which is the whole reason the
				// state exists (ADR-0035).
				state := ext.extraState[j].(*ohlcvState)
				out.Columns[colIdx].SetValue(i, state.encode())
			case AggOhlcv:
				state := ext.extraState[j].(*ohlcvState)
				v, err := state.value(state.declaredFields(h, j))
				if err != nil {
					return nil, err
				}
				out.Columns[colIdx].SetValue(i, v)
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
					if err := h.aggEmitErr(&ext.accs[j], j, agg.Func); err != nil {
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
					if err := h.aggEmitErr(&acc, j, agg.Func); err != nil {
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

		// GROUPING(...) bitmasks. gs.setID is the only record of WHICH set
		// produced this row, and it is the only thing that can answer the
		// question: ext.keyValues[j] == nil is not a proxy, because a key
		// grouped in this set can be NULL in the data (#804).
		if len(h.GroupingCalls) > 0 {
			base := nullColIdx + len(h.NullGroupCols)
			switch {
			case len(gMasks) == 0:
				// No grouping sets: every key is grouped in every row, so
				// every GROUPING call answers 0. Reached by a plain GROUP BY,
				// where gs is nil on the SoA key paths and is not needed.
				for k := range h.GroupingCalls {
					if base+k < len(out.Columns) {
						out.Columns[base+k].SetValue(i, int32(0))
					}
				}
			case gs != nil && int(gs.setID) >= 0 && int(gs.setID) < len(gMasks):
				for k, mask := range gMasks[gs.setID] {
					if base+k < len(out.Columns) {
						out.Columns[base+k].SetValue(i, mask)
					}
				}
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

// PublishedGroupKeyNames is the name each GROUP BY key's value is emitted
// under, given the names the aggregate RESOLVES its keys by (groupByCols) and
// the planner's published overrides (groupByOutNames, empty entries meaning
// "apply the rule below").
//
// The rule: strip a table qualifier, unless stripping would make two keys
// share one output name — `GROUP BY n1.n_name, n2.n_name` keeps both
// qualifiers so a projection above can tell them apart. `GroupByAll`
// (DISTINCT) passes its input schema through verbatim, because the operator
// must be name-transparent to every downstream column reference.
//
// It is exported because BOTH engines have to answer this question with one
// rule. The single-process planner feeds this operator directly; the stage DAG
// ships the key list to a worker that builds its own HashAggregate, and the
// worker computes the same answer from the same inputs so the two aggregate
// output schemas are identical (ADR-0026 §2b). A copy of the rule in the
// planner is exactly how the two would drift.
func PublishedGroupKeyNames(groupByCols, groupByOutNames []string, groupByAll bool) []string {
	outNames := make([]string, len(groupByCols))
	if groupByAll {
		copy(outNames, groupByCols)
	} else {
		baseCounts := make(map[string]int, len(groupByCols))
		for i, name := range groupByCols {
			base := name
			if dot := strings.IndexByte(name, '.'); dot >= 0 {
				base = name[dot+1:]
			}
			outNames[i] = base
			baseCounts[base]++
		}
		for i, name := range groupByCols {
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
		if i < len(groupByOutNames) && groupByOutNames[i] != "" {
			outNames[i] = groupByOutNames[i]
		}
	}
	return outNames
}

func (h *HashAggregate) outputSchema() []parquet.Column {
	cols := make([]parquet.Column, 0, len(h.GroupByCols)+len(h.Aggs)+len(h.NullGroupCols))

	outNames := PublishedGroupKeyNames(h.GroupByCols, h.GroupByOutNames, h.GroupByAll)

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
		case AggOhlcv:
			// The bar is a ROW, and a ROW vector with no FieldNames cannot be
			// written at all — SetValue has nothing to address. The fields
			// come from the planner (AggColumn.OutputFields), derived through
			// exec.OhlcvOutputFields, and the operator re-derives them from
			// the vectors it actually reads when the planner could not
			// resolve the input columns. One function either way.
			out.Type = parquet.TypeRow
			out.Precision, out.Scale = 0, 0
			out.Fields = h.ohlcvFields(i)
		case AggOhlcvState, AggOhlcvStateMerge:
			// The encoded partial travels as text.
			out.Type = parquet.TypeString
			out.Precision, out.Scale = 0, 0
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
			// PostgreSQL's types for an INTEGER input (#784), from
			// `pg_typeof` on the live server: SUM(int2/int4) is bigint,
			// SUM(int8) and AVG(int*) are numeric. The accumulator for the
			// numeric ones is already an Int128 at scale 0 (aggIntExact); the
			// bigint one was always an exact int64 array and only the
			// declaration said float64, which is where the digits went past
			// 2^53.
			if resolved {
				switch {
				case aggIntExact(agg, h.aggInputTypes[i]):
					out.Type = parquet.TypeDecimal
					out.Precision = batch.MaxDecimalPrecision
					out.Scale = 0
					if agg.Func == AggAvg {
						out.Scale = batch.AvgScale(0)
					}
				case agg.Func == AggSum && h.aggInputTypes[i] == batch.TypeInt32:
					out.Type = parquet.TypeInt64
				}
			}
		}
		cols = append(cols, out)
	}
	// GROUPING SETS null columns (appear in other sets but not this one)
	for _, name := range h.NullGroupCols {
		cols = append(cols, parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true})
	}
	// GROUPING(...) bitmasks. Int32, because PostgreSQL's GROUPING returns
	// `integer` — checked with \gdesc against PostgreSQL 17 — and the wire
	// has to declare the OID a client expects.
	for _, name := range h.GroupingCallNames {
		cols = append(cols, parquet.Column{Name: name, Type: parquet.TypeInt32})
	}
	return cols
}

// groupingMasks precomputes, for every grouping set, the bitmask each
// GROUPING(...) call answers on a row from that set: bit (n-1-i) is set when
// argument i's key position is NOT in the set. PostgreSQL orders the bits
// with the LEFTMOST argument most significant, so GROUPING(g, h) and
// GROUPING(h, g) differ (2 vs 1 on a row grouping h but not g).
//
// Indexed [setIdx][callIdx]. Computed once per operator: the answer depends
// only on the set and the call, never on the row.
func (h *HashAggregate) groupingMasks() [][]int32 {
	if len(h.GroupingCalls) == 0 || len(h.GroupingSets) == 0 {
		return nil
	}
	masks := make([][]int32, len(h.GroupingSets))
	for s, set := range h.GroupingSets {
		inSet := make(map[int]bool, len(set))
		for _, ci := range set {
			inSet[ci] = true
		}
		row := make([]int32, len(h.GroupingCalls))
		for c, args := range h.GroupingCalls {
			var mask int32
			n := len(args)
			for i, keyPos := range args {
				if !inSet[keyPos] {
					mask |= 1 << uint(n-1-i)
				}
			}
			row[c] = mask
		}
		masks[s] = row
	}
	return masks
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

// minMaxOutputType maps MIN/MAX inputs to output types; physical.minMaxDeclaredType
// mirrors it and must change with it. ok=false keeps the planner declaration;
// zero TypeID is BOOL, not an undeclared sentinel (#354, #371).
// MIN/MAX retain input values and their declarations, subject to the scalar mapping
// below (#392); unsupported kernels must not fall into FLOAT64 (#417, #361).
// DECIMAL keeps its exact Int128 and input scale via outputSchema (#455).
// Containers use GetValue/SetValue and require the input's type and shape metadata
// (#426), the same declaration rule as MIN_BY/MAX_BY (#392).
// See docs/internals/min-max-output-declarations.md for the design.
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
	case batch.TypeFloat64:
		return parquet.TypeFloat64, true
	case batch.TypeFloat32:
		// REAL in, REAL out — `pg_typeof(min(real))` is real (#760). The
		// value was always exact (a float32 widened to a float64 is), so this
		// is the declaration catching up with it, and the planner's
		// minMaxDeclaredType carries the same rule so the two paths agree.
		return parquet.TypeFloat32, true
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
