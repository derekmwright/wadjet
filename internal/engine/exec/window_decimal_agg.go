package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Windowed SUM/AVG over a DECIMAL answer what the GROUPED SUM/AVG answer
// (#586, #475, ADR-0024 item 2).
//
// `SUM(d) GROUP BY g` and `SUM(d) OVER (PARTITION BY g)` are the same question
// written twice, and a BI tool flips between the two spellings freely. Until
// this file existed they disagreed about the TYPE of the answer and about its
// DIGITS: the grouped form kept an exact Int128 accumulator and declared
// DECIMAL(38,s) (#455, ADR-0012 item 9), while the window accumulated in
// float64 through vecFloat64 and declared FLOAT64, so everything past ~16
// significant digits was gone before any consumer saw it.
//
// The rules here are ADR-0012 item 9's, unchanged:
//
//	SUM(DECIMAL(p,s)) -> DECIMAL(38, s)
//	AVG(DECIMAL(p,s)) -> DECIMAL(38, min(s+4, 38)), exact Int128 division
//	                     rounded half away from zero
//	overflow          -> SQLSTATE 22003, never a wrapped total
//
// The declared precision is the carrier's full width rather than the input's,
// because a sum genuinely exceeds its column's precision and a narrower
// declaration would hand the parquet writer a leaf too small for the value.

// WindowDecimalAggMeta is the (precision, scale) a windowed SUM or AVG
// declares over a DECIMAL input of scale inScale. It is the window's copy of
// aggSpecOutputDecimal's SUM/AVG arms — deliberately the same two lines, so a
// change to one is a visible divergence from the other.
//
// Exported for the same reason WindowMinMaxType is: the physical planner
// declares this column from the catalog and the operator declares it from the
// vector it reads, and two implementations of one rule are two chances to
// disagree about the type of one answer.
func WindowDecimalAggMeta(fn WindowFunc, inScale int) (precision, scale int) {
	if fn == WinAvg {
		return batch.MaxDecimalPrecision, batch.AvgScale(inScale)
	}
	return batch.MaxDecimalPrecision, inScale
}

// windowAccumulates reports whether f builds its answer by ADDING its input's
// values rather than copying one of them. The distinction is the whole of
// #586: MIN/MAX and the value functions return a value the column holds, so
// they keep the input's own (p,s) (#569); SUM and AVG return a value it does
// not, so they declare the accumulator's.
func windowAccumulates(f WindowFunc) bool {
	return f == WinSum || f == WinAvg
}

// windowDecimalSumOverflow reports a windowed DECIMAL SUM that left the
// 128-bit range. It is aggregate.go's decimalSumOverflow one operator over,
// with the same SQLSTATE (22003, PostgreSQL's numeric_value_out_of_range) and
// the same position: a wrapped total is a different number wearing the right
// type, so the query fails instead of answering it (ADR-0012 item 9,
// ADR-0024 item 4).
//
// The refusal is STICKY across a partition's slide, exactly as item 9's
// grouped one is sticky across a group: a frame whose running total leaves the
// range and comes back — the sliding frame adds the arriving row before it
// subtracts the departing one — fails rather than reporting a total it did
// carry exactly. That is the conservative side of a limit whose other side is
// a wrapped number nobody can see is wrong.
func windowDecimalSumOverflow(col string) error {
	if col == "" {
		col = "sum"
	}
	return sqlerr.New("22003", "SUM over a DECIMAL column overflowed the 128-bit exact accumulator "+
		"in a window frame (%s): the running total is outside the range DECIMAL(38) can represent", col)
}

// windowDecimalAvgUnrepresentable reports a windowed DECIMAL AVG whose exact
// quotient has no Int128 — the same refusal one multiplication later, since
// AVG scales the frame's sum by 10^AvgScaleIncrement before it divides.
func windowDecimalAvgUnrepresentable(col string) error {
	if col == "" {
		col = "avg"
	}
	return sqlerr.New("22003", "AVG over a DECIMAL column has no exact 128-bit value "+
		"in a window frame (%s): the frame's sum scaled to the output's scale is outside "+
		"the range DECIMAL(38) can represent", col)
}

// decimalFrameAcc is the exact running state of one window frame: the Int128
// sum of the frame's non-NULL rows and how many there were.
//
// Both halves matter. The sum is Int128 because that is the carrier the value
// lives in and the only one that can hold it exactly; the COUNT is separate
// from the frame WIDTH because SQL excludes NULLs from an aggregate's input,
// so AVG divides by the rows that contributed and a frame holding only NULLs
// answers NULL rather than zero.
//
// lo/hi are partition-relative and only ever move FORWARD — every frame bound
// is a non-decreasing function of the row index — so each row is added once
// and removed once and the whole partition costs O(n) regardless of frame
// width.
type decimalFrameAcc struct {
	sum      batch.Int128
	count    int64
	lo, hi   int
	overflow bool
}

// slide advances the accumulator from its current frame to [lo, hi).
//
// Rows are added before any are removed, so the window never inverts on an
// empty frame (hi < lo), where the rows added to reach lo are immediately
// subtracted again and the sum correctly lands back on zero. Both directions
// are CHECKED: the retract is a subtraction of a value the accumulator already
// holds, and an unchecked one would let a wrapped intermediate become a
// plausible-looking total.
func (a *decimalFrameAcc) slide(in *batch.Vector, start, lo, hi int) {
	if hi < lo {
		hi = lo
	}
	data := in.DecimalData.Data
	for a.hi < hi {
		r := start + a.hi
		if !in.Nulls.IsNullFast(r) {
			s, ok := a.sum.AddChecked(data[r])
			a.sum = s
			a.overflow = a.overflow || !ok
			a.count++
		}
		a.hi++
	}
	for a.lo < lo {
		r := start + a.lo
		if !in.Nulls.IsNullFast(r) {
			s, ok := a.sum.SubChecked(data[r])
			a.sum = s
			a.overflow = a.overflow || !ok
			a.count--
		}
		a.lo++
	}
}

// windowDecimalFrames computes SUM or AVG over every frame of one partition
// into an exact DECIMAL output vector.
//
// winVec's scale is the DECLARED output scale — the input's own for SUM,
// AvgScale(input) for AVG — so the division's added digits come from the
// difference between the two rather than from a constant this function would
// have to keep in step with WindowDecimalAggMeta.
func windowDecimalFrames(winVec, inputVec *batch.Vector, fr resolvedFrame, start, n int, wc WindowColumn) error {
	addScale := winVec.DecimalData.Scale - inputVec.DecimalData.Scale
	if addScale < 0 {
		// Unreachable through windowOutputColumn, whose SUM scale IS the
		// input's and whose AVG scale is never below it. A spec that reached
		// here with a narrower output would round digits away silently, so
		// it is refused instead.
		return windowDecimalAvgUnrepresentable(wc.OutputCol)
	}
	var acc decimalFrameAcc
	out := winVec.DecimalData.Data
	avg := wc.Func == WinAvg
	// The division memo. A frame whose ends did not move has the same (sum,
	// count) as the previous row's and therefore the same quotient — which is
	// EVERY row of a whole-partition window, the commonest shape there is.
	// batch.DecimalAvg falls to a big.Int division whenever the sum scaled by
	// 10^addScale leaves int64, so without this a partition of n rows paid n
	// allocating divisions for one answer.
	var memoSum batch.Int128
	var memoCount int64
	var memoQ batch.Int128
	for i := 0; i < n; i++ {
		lo, hi := fr.bounds(i)
		acc.slide(inputVec, start, lo, hi)
		if acc.overflow {
			if avg {
				return windowDecimalAvgUnrepresentable(wc.OutputCol)
			}
			return windowDecimalSumOverflow(wc.OutputCol)
		}
		if hi <= lo || acc.count == 0 {
			// An empty frame, or one holding only NULLs: SQL says NULL for
			// both SUM and AVG, and winVec starts all-null.
			continue
		}
		if avg {
			if acc.count != memoCount || !acc.sum.Equal(memoSum) {
				q, ok := batch.DecimalAvg(acc.sum, acc.count, addScale)
				if !ok {
					return windowDecimalAvgUnrepresentable(wc.OutputCol)
				}
				memoSum, memoCount, memoQ = acc.sum, acc.count, q
			}
			out[start+i] = memoQ
		} else {
			out[start+i] = acc.sum
		}
		winVec.Nulls.SetValid(start + i)
	}
	return nil
}

// windowFloat64Frames is windowDecimalFrames' inexact twin: SUM/AVG over
// every frame of one partition into a FLOAT64 output vector, which is what
// every non-DECIMAL numeric input still answers.
//
// It carries the same non-NULL COUNT the exact path does, for the same reason:
// AVG divides by the rows that contributed, not by the frame's width, and a
// frame holding only NULLs answers NULL. Those two were the float path's own
// defects — `AVG(x) OVER (...)` over a frame with a NULL in it answered a
// number PostgreSQL does not, and a frame of only NULLs answered 0 where
// PostgreSQL answers NULL.
func windowFloat64Frames(winVec, inputVec *batch.Vector, fr resolvedFrame, start, n int, fn WindowFunc) {
	var sum float64
	var cnt int64
	curLo, curHi := 0, 0
	out := winVec.Float64Data
	avg := fn == WinAvg
	for i := 0; i < n; i++ {
		lo, hi := fr.bounds(i)
		curLo, curHi = slideFrameSum(inputVec, start, lo, hi, curLo, curHi, &sum, &cnt)
		if hi <= lo || cnt == 0 {
			continue
		}
		if avg {
			out[start+i] = sum / float64(cnt)
		} else {
			out[start+i] = sum
		}
		winVec.Nulls.SetValid(start + i)
	}
}

// windowExactDecimal reports whether this window column's SUM/AVG runs on the
// exact path: an accumulating function whose OUTPUT vector and INPUT vector
// are both DECIMAL.
//
// Both halves are asked because they are declared in different places. The
// output type comes from the planner (windowSpecOutputType) or the stage spec,
// corrected at runtime by Window.retypeValueColumns; the input type is
// whatever vector arrives. They agree in every plan the planner builds — but a
// spec whose declaration the planner had to decline keeps FLOAT64 and takes
// the inexact path, which is the pre-#586 answer rather than a wrong write
// into a vector of the other type.
func windowExactDecimal(winVec, inputVec *batch.Vector) bool {
	return winVec != nil && inputVec != nil &&
		winVec.Type == batch.TypeDecimal && inputVec.Type == batch.TypeDecimal
}

// windowAccOutputType is Window.retypeValueColumns' rule for SUM and AVG: a
// DECIMAL input makes the output DECIMAL, and every other numeric input keeps
// FLOAT64.
//
// The second clause is not decoration. A stage spec built before this change,
// or a planner declaration resolved against a different scan, can declare
// DECIMAL over an input that is not one; writing float sums into a DECIMAL
// vector's Int128 array would produce values off by a power of ten with
// nothing to report it, so the declaration is corrected DOWN as well as up.
//
// INT32/INT64 keep FLOAT64 and are a recorded residual: PostgreSQL's
// sum(int4) is bigint, sum(int8) and avg(int) are numeric. Fixing that is a
// separate change to the GROUPED aggregate first — it answers float64 for an
// integer column too — because the two spellings have to keep agreeing.
func windowAccOutputType(in parquet.TypeID) parquet.TypeID {
	if in == parquet.TypeDecimal {
		return parquet.TypeDecimal
	}
	return parquet.TypeFloat64
}
