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
// The refusal is scoped to ONE FRAME, and inside that frame it is item 9's
// rule verbatim: the frame's own rows are added in order, and a running total
// that leaves the range fails even if later rows would bring it back. That is
// the same answer `SUM(d) ... GROUP BY` gives for the same set of rows, which
// is the whole contract this file exists to keep.
//
// It is NOT sticky across the SLIDE, and an earlier draft of this comment
// claiming it was described a defect rather than a rule. A sliding
// accumulator carries state between frames, and a transient it holds while
// moving from one frame to the next belongs to NEITHER of them: adding the
// arriving row before subtracting the departing one made
// `SUM(d) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW)` over three
// 9x10^37 values hold 1.8x10^38 between two frames that each hold 9x10^37,
// and refuse a query PostgreSQL and the grouped spelling both answer.
// exactFrameAcc.slide retracts before it adds and resets outright between
// disjoint frames; windowExactFrames RECOMPUTES any frame whose incremental
// state flagged overflow, and refuses only if the frame's own rows overflow
// on their own.
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

// windowExactCells is one input column read as EXACT Int128 cells, resolved
// ONCE per partition (ADR-0002's typed-kernel rule: resolve the type once,
// then dispatch to a typed reader, never per row).
//
// Three carriers reach it and they are the three the engine stores an exactly
// summable number in: a DECIMAL's Int128 array, an int64 array, and an int32
// array (INT32 and — since #953 — the int4-domain PORT and PROTOCOL). The
// DECIMAL arm hands back the stored cell untouched, which is what keeps the
// #586 path byte-identical; the integer arms widen at scale 0, which is
// exactly what kernel.sumRowInt64Decimal does for the GROUPED spelling
// (#784).
type windowExactCells struct {
	dec   []batch.Int128
	i64   []int64
	i32   []int32
	scale int // the INPUT's scale: its own for a DECIMAL, 0 for an integer
}

func (c windowExactCells) at(r int) batch.Int128 {
	if c.dec != nil {
		return c.dec[r]
	}
	if c.i64 != nil {
		return batch.Int128From(c.i64[r])
	}
	return batch.Int128From(int64(c.i32[r]))
}

// exactFrameAcc is the exact running state of one window frame: the Int128
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
type exactFrameAcc struct {
	sum      batch.Int128
	count    int64
	lo, hi   int
	overflow bool
}

// slide advances the accumulator from its current frame to [lo, hi).
//
// The ORDER is the correctness-relevant part, and it is retract-then-add.
// Adding first means the accumulator transiently holds
// sum(previous frame + arriving rows) — a value that belongs to NEITHER
// frame — and for an exact carrier that transient can leave the range and
// refuse a query both spellings answer: three DECIMAL(38,0) rows of 9x10^37
// under `ROWS BETWEEN CURRENT ROW AND CURRENT ROW` held 1.8x10^38 between two
// frames that each hold 9x10^37. Retracting first bounds every intermediate
// by a PREFIX of the target frame, so the only overflow left is one the
// frame's own rows produce — which is exactly what the grouped SUM over those
// rows reports (ADR-0012 item 9).
//
// DISJOINT frames reset instead of retracting to empty. When lo has passed
// the last row this accumulator added, nothing carries over, and walking the
// subtraction chain down to zero would re-introduce intermediates unrelated to
// either frame (removing a large negative row from a total near the ceiling
// overflows on the way out). Resetting is exact, cheaper, and — because every
// frame bound is non-decreasing in the row index — costs O(sum of frame
// widths) over the partition, which is bounded by the partition's own length:
// a frame disjoint from its predecessor advances lo by at least its own width.
//
// Both directions stay CHECKED even so. A retract is a subtraction of a value
// the accumulator already holds, and an unchecked one would let a wrapped
// intermediate become a plausible-looking total; the flag it raises is not
// final, since windowExactFrames recomputes the frame before refusing.
func (a *exactFrameAcc) slide(in *batch.Vector, cells windowExactCells, start, lo, hi int) {
	if hi < lo {
		hi = lo
	}
	if lo >= a.hi {
		a.reset(lo)
	}
	for a.lo < lo {
		r := start + a.lo
		if !in.Nulls.IsNullFast(r) {
			s, ok := a.sum.SubChecked(cells.at(r))
			a.sum = s
			a.overflow = a.overflow || !ok
			a.count--
		}
		a.lo++
	}
	for a.hi < hi {
		r := start + a.hi
		if !in.Nulls.IsNullFast(r) {
			s, ok := a.sum.AddChecked(cells.at(r))
			a.sum = s
			a.overflow = a.overflow || !ok
			a.count++
		}
		a.hi++
	}
}

// reset empties the accumulator and positions it at an empty frame starting
// at pos.
func (a *exactFrameAcc) reset(pos int) {
	a.sum, a.count, a.overflow = batch.Int128{}, 0, false
	a.lo, a.hi = pos, pos
}

// recompute rebuilds the accumulator from a CLEAN state over [lo, hi) alone.
//
// It is the answer to "did this frame really overflow, or was that the
// incremental state?": summing the frame's own rows in order is precisely
// what the grouped SUM over those rows does, so whatever this reports is the
// answer the other spelling of the query gives. Called only when the
// incremental slide raised the flag, so its O(frame width) cost is paid on
// the rare overflowing frame and never on the common path.
func (a *exactFrameAcc) recompute(in *batch.Vector, cells windowExactCells, start, lo, hi int) {
	if hi < lo {
		hi = lo
	}
	a.reset(lo)
	for a.hi < hi {
		r := start + a.hi
		if !in.Nulls.IsNullFast(r) {
			s, ok := a.sum.AddChecked(cells.at(r))
			a.sum = s
			a.overflow = a.overflow || !ok
			a.count++
		}
		a.hi++
	}
}

// windowExactFrames computes SUM or AVG over every frame of one partition
// into an EXACT output vector — a DECIMAL one, or the INT64 one PostgreSQL's
// `sum(int4)` declares.
//
// For a DECIMAL input, winVec's scale is the DECLARED output scale — the
// input's own for SUM, AvgScale(input) for AVG — so the division's added
// digits come from the difference between the two rather than from a constant
// this function would have to keep in step with WindowDecimalAggMeta. For an
// INTEGER input the input's scale is 0 and the same subtraction gives AVG its
// four digits.
func windowExactFrames(winVec, inputVec *batch.Vector, cells windowExactCells,
	fr resolvedFrame, start, n int, wc WindowColumn) error {
	avg := wc.Func == WinAvg
	// The BIGINT arm: `SUM(int4) OVER (…)` declares bigint, exactly as the
	// grouped spelling does, and accumulates in the same Int128 the numeric
	// arms use so that no INTERMEDIATE can wrap. Only the frame's own total
	// has to fit, and one that does not is 22003 — PostgreSQL's own answer
	// there is `22003: bigint out of range`, measured live. AVG never
	// declares an integer output, so this arm is SUM's alone.
	if winVec.Type == batch.TypeInt64 {
		if avg {
			return windowDecimalAvgUnrepresentable(wc.OutputCol)
		}
		return windowExactIntFrames(winVec, inputVec, cells, fr, start, n, wc)
	}
	addScale := winVec.DecimalData.Scale - cells.scale
	if addScale < 0 {
		// Unreachable through windowOutputColumn, whose SUM scale IS the
		// input's and whose AVG scale is never below it. A spec that reached
		// here with a narrower output would round digits away silently, so
		// it is refused instead.
		return windowDecimalAvgUnrepresentable(wc.OutputCol)
	}
	var acc exactFrameAcc
	out := winVec.DecimalData.Data
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
		acc.slide(inputVec, cells, start, lo, hi)
		if acc.overflow {
			// The incremental state left the range. That is not yet an
			// answer: a slide carries state between frames, so the flag may
			// belong to a transient rather than to THIS frame's rows. Sum
			// them on their own — the grouped SUM's own arithmetic — and
			// refuse only if that overflows too.
			acc.recompute(inputVec, cells, start, lo, hi)
			if acc.overflow {
				if avg {
					return windowDecimalAvgUnrepresentable(wc.OutputCol)
				}
				return windowDecimalSumOverflow(wc.OutputCol)
			}
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

// windowExactIntFrames is windowExactFrames' BIGINT arm: an exact Int128
// running total written back as an int64, which is what `SUM(int4) OVER (…)`
// declares (exec.IntegerAccOutputType, PostgreSQL's `sum(int4) -> bigint`).
//
// The accumulator is the wide one on purpose. PostgreSQL's own int4 sum
// accumulates in int8 and the frame's total is the only thing that has to fit
// it; accumulating in int64 here would additionally make a PREFIX of the
// frame able to wrap, so a frame whose own total is in range could fail. The
// Int128 carrier removes that difference — over an int4 column no
// intermediate can leave it at all — and the fit test is on the value the
// query actually returns.
func windowExactIntFrames(winVec, inputVec *batch.Vector, cells windowExactCells,
	fr resolvedFrame, start, n int, wc WindowColumn) error {
	var acc exactFrameAcc
	out := winVec.Int64Data
	for i := 0; i < n; i++ {
		lo, hi := fr.bounds(i)
		acc.slide(inputVec, cells, start, lo, hi)
		if acc.overflow {
			acc.recompute(inputVec, cells, start, lo, hi)
			if acc.overflow {
				return integerSumOverflow(wc.OutputCol)
			}
		}
		if hi <= lo || acc.count == 0 {
			continue
		}
		if !acc.sum.FitsInt64() {
			return integerSumOverflow(wc.OutputCol)
		}
		out[start+i] = acc.sum.ToInt64()
		winVec.Nulls.SetValid(start + i)
	}
	return nil
}

// windowFloat64Frames is windowExactFrames' inexact twin: SUM/AVG over
// every frame of one partition into a FLOAT64 output vector, which is what
// every non-DECIMAL numeric input still answers.
//
// It carries the same non-NULL COUNT the exact path does, for the same reason:
// AVG divides by the rows that contributed, not by the frame's width, and a
// frame holding only NULLs answers NULL. Those two were the float path's own
// defects — `AVG(x) OVER (...)` over a frame with a NULL in it answered a
// number PostgreSQL does not, and a frame of only NULLs answered 0 where
// PostgreSQL answers NULL.
func windowFloat64Frames(winVec, inputVec *batch.Vector, rd windowNumericReader,
	fr resolvedFrame, start, n int, fn WindowFunc) {
	var acc float64FrameAcc
	out := winVec.Float64Data
	avg := fn == WinAvg
	for i := 0; i < n; i++ {
		lo, hi := fr.bounds(i)
		acc.slide(inputVec, rd, start, lo, hi)
		if hi <= lo || acc.count == 0 {
			continue
		}
		if avg {
			out[start+i] = acc.sum / float64(acc.count)
		} else {
			out[start+i] = acc.sum
		}
		winVec.Nulls.SetValid(start + i)
	}
}

// resolveWindowExactCells reports whether this window column's SUM/AVG runs
// on the EXACT path, and hands back the cell reader it runs with.
//
// Two questions are asked because they are declared in different places. The
// output type comes from the planner (windowSpecOutputType) or the stage spec,
// corrected at runtime by Window.retypeValueColumns; the input type is
// whatever vector arrives. They agree in every plan the planner builds — but a
// spec whose declaration the planner had to decline keeps FLOAT64 and takes
// the inexact path, which is the pre-#586 answer rather than a wrong write
// into a vector of the other type.
//
// A DECIMAL input needs a DECIMAL output. An INTEGER input — the set
// IntegerAccOutputType names — needs the DECIMAL that PostgreSQL's
// sum(int8)/avg(int) declare, or the INT64 its sum(int4) declares; either is
// a carrier that holds the total exactly, which is the whole point (#987).
func resolveWindowExactCells(winVec, inputVec *batch.Vector) (windowExactCells, bool) {
	if winVec == nil || inputVec == nil {
		return windowExactCells{}, false
	}
	if inputVec.Type == batch.TypeDecimal {
		if winVec.Type != batch.TypeDecimal {
			return windowExactCells{}, false
		}
		return windowExactCells{dec: inputVec.DecimalData.Data, scale: inputVec.DecimalData.Scale}, true
	}
	if !integerAccInput(inputVec.Type) {
		return windowExactCells{}, false
	}
	if winVec.Type != batch.TypeDecimal && winVec.Type != batch.TypeInt64 {
		return windowExactCells{}, false
	}
	if inputVec.Type == batch.TypeInt64 {
		return windowExactCells{i64: inputVec.Int64Data}, true
	}
	// INT32 and the int4-domain PORT/PROTOCOL, all Int32Data-backed.
	return windowExactCells{i32: inputVec.Int32Data}, true
}

// windowAccOutputType is Window.retypeValueColumns' rule for SUM and AVG: the
// output type is the ACCUMULATOR's, not the input's. A DECIMAL input makes it
// DECIMAL; an INTEGER input takes PostgreSQL's own result type, which is
// exec.IntegerAccOutputType — bigint for sum(int4), numeric for sum(int8) and
// for avg of either; everything else keeps FLOAT64.
//
// The last clause is not decoration. A stage spec built before this change, or
// a planner declaration resolved against a different scan, can declare DECIMAL
// over an input that is not one; writing float sums into a DECIMAL vector's
// Int128 array would produce values off by a power of ten with nothing to
// report it, so the declaration is corrected DOWN as well as up.
//
// The INTEGER arm is #987 and #813: until it existed, `SUM(int8) OVER ()`
// accumulated in float64, so past 2^53 the total depended on the ORDER the
// rows arrived in — the same query answered 9007201419001868 or
// 9007201419001864 on the same data — while `SUM(int8) GROUP BY` next to it
// answered exactly. One question, two spellings, two numbers. It asks
// IntegerAccOutputType rather than repeating the rule so that the grouped
// declaration, this one and the planner's window declaration cannot drift.
//
// `declared` is the spec's own type, and it decides ONE case this correction
// must not touch: a SUM whose plan says bigint over an int64-carried input.
// Every integer expression in this engine computes in int64 (ADR-0024's
// widening), so the input VECTOR of `SUM(CASE WHEN … THEN 1 ELSE 0 END)
// OVER ()` is indistinguishable from `SUM(int8_col + 0) OVER ()`'s — while the
// PLAN, which still has the argument's syntax, can tell them apart and says
// bigint for the first (physical.windowArgIsNarrowInteger, #987 review B1).
// Widening it back to numeric here would undo that and put the window's OID
// at 1700 where its grouped twin's is 20. Both arms accumulate in the same
// Int128 and the bigint arm refuses a total that does not fit rather than
// wrapping, so keeping the narrower declaration costs no exactness.
func windowAccOutputType(fn WindowFunc, declared, in parquet.TypeID) parquet.TypeID {
	if in == parquet.TypeDecimal {
		return parquet.TypeDecimal
	}
	if fn == WinSum && declared == parquet.TypeInt64 && in == parquet.TypeInt64 {
		return parquet.TypeInt64
	}
	if out, _, _, ok := IntegerAccOutputType(fn == WinAvg, in); ok {
		return out
	}
	return parquet.TypeFloat64
}
