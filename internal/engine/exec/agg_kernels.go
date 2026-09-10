// This file holds aggregate kernel selection and exact-result refusal helpers.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// resolveBatchAggKernel returns a batch-level aggregate kernel for scalar aggregates.
// Returns nil if the aggregate function is not batch-able (e.g., COUNT(DISTINCT), STRING_AGG).
func resolveBatchAggKernel(agg AggColumn, colIdx int, b *batch.RecordBatch) kernel.BatchAggKernel {
	fn := agg.Func
	if colIdx >= 0 && aggIntExact(agg, b.Columns[colIdx].Type) {
		// See resolveAggUpdater (#784): the ungrouped scalar path needs the
		// same carrier the grouped ones use, or the same query answers two
		// different numbers depending on whether it has a GROUP BY.
		if k := kernel.ResolveBatchSumIntExact(b.Columns[colIdx].Type); k != nil {
			return k
		}
	}
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

func resolveAggUpdater(agg AggColumn, typ batch.TypeID) kernel.RowAggUpdater {
	fn := agg.Func
	// The Int128 arm first, for both SUM and AVG (#784): the carrier is
	// aggIntExact's answer and the by-type resolvers below cannot see the
	// function, so an INT64 SUM would otherwise land in SumI64 while the flat
	// path put it in SumDec — the two-carrier disagreement ADR-0027 decision 3
	// records.
	if aggIntExact(agg, typ) {
		if u := kernel.ResolveRowSumIntExact(typ, false); u != nil {
			return u
		}
	}
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
func resolveAggUpdaterNoNull(agg AggColumn, typ batch.TypeID) kernel.RowAggUpdater {
	fn := agg.Func
	if aggIntExact(agg, typ) { // see resolveAggUpdater (#784)
		if u := kernel.ResolveRowSumIntExact(typ, true); u != nil {
			return u
		}
	}
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

// integerSumOverflow reports a SUM whose INT64 carrier wrapped. It is
// decimalSumOverflow's sibling and says which carrier so the reading is not
// ambiguous: the wrapped total is a different number wearing the right type,
// and ADR-0012 item 9 makes it an error rather than an answer.
//
// The carrier is not always the exact one. A BARE integer column sums into
// Int128 and is exact (#784); a COMPUTED integer argument is declared bigint
// on purpose, so that `SUM(CASE WHEN … THEN 1 ELSE 0 END)` keeps PostgreSQL's
// int8 OID (physical.aggOutputFromInputDecl), and that is the arm this error
// belongs to. PostgreSQL would answer the exact numeric; wadjet refuses.
func integerSumOverflow(col string) error {
	if col == "" {
		col = "sum"
	}
	return sqlerr.New("22003", "SUM over an integer expression overflowed the 64-bit accumulator (%s): "+
		"the running total is outside the range BIGINT can represent", col)
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

// aggEmitErr returns the error for aggregate j when its accumulator cannot
// answer EXACTLY: a SUM that wrapped — on either carrier — or a DECIMAL AVG
// whose exact quotient has no Int128. nil otherwise.
//
// It was decAggErr, for the DECIMAL carrier alone, and the name was the whole
// gap: an integer SUM has a carrier too, it wraps at 2^63 instead of at 38
// digits, and nothing looked. `SUM(-b)` over a column whose total is exactly
// 2^64 answered 0 while `SUM(b)` answered 18446744073709551616 (review round 2
// F4). Both carriers now report through one door.
//
// The AVG arm divides here and again in the writer. That is deliberate: the
// alternative is an error return threaded through writeAccToColumn's typed
// fast path, and a DECIMAL AVG is not on any hot path (TPC-H's prices are
// FLOAT64), so the second division costs nothing that shows.
func (h *HashAggregate) aggEmitErr(acc *kernel.Accumulator, j int, fn AggFunc) error {
	name := func() string {
		if j < len(h.Aggs) {
			return h.Aggs[j].OutputCol
		}
		return ""
	}
	if acc.DecOverflow {
		return decimalSumOverflow(name())
	}
	// The integer carrier's wrap. Read only for the functions that ACCUMULATE
	// into SumI64 — SUM, and AVG over the int32 class — because MIN/MAX and
	// COUNT share the accumulator type and never write that field.
	if acc.IntOverflow && (fn == AggSum || fn == AggAvg) {
		return integerSumOverflow(name())
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
