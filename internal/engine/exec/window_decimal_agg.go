package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Windowed DECIMAL SUM/AVG must match grouped values and declarations
// (#586, #475, ADR-0024 item 2; #455, ADR-0012 item 9).
// SUM(DECIMAL(p,s)) returns DECIMAL(38,s); AVG returns DECIMAL(38,min(s+4,38)),
// using exact Int128 division rounded half away from zero.
// Overflow raises 22003, never a wrapped total (ADR-0012 item 9).
// Declare the carrier's full precision, not the input's: accumulated values may
// exceed the input precision and must fit the writer's declared leaf.
// See docs/internals/window-decimal-sum-avg-contract.md for the design.

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

// windowDecimalSumOverflow refuses exact frame overflow with SQLSTATE 22003
// (ADR-0012 item 9, ADR-0024 item 4), never a wrapped total.
// Within one frame, add its rows in order: a running total outside the range fails
// even if later rows would bring it back, matching grouped SUM over those rows.
// Overflow must not stick across slides: transition intermediates belong to neither frame.
// exactFrameAcc.slide retracts before adding and resets between disjoint frames.
// windowExactFrames recomputes flagged frames, refusing only when the frame's
// own ordered accumulation overflows.
// See docs/internals/window-frame-overflow-boundary.md for the design.
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

// windowExactCells resolves one column to exact Int128 cells once per partition
// under ADR-0002's typed-kernel rule, never resolving the type per row.
// DECIMAL returns stored cells unchanged at its own scale (#586).
// Int64 and int32 storage widen at scale zero, including INT32 and int4-domain
// PORT/PROTOCOL (#953), matching grouped kernel.sumRowInt64Decimal (#784).
// This shared reader serves DECIMAL and integer accumulators; measured overhead
// and the performance follow-up are recorded in the design (#987).
// See docs/internals/window-exact-cell-reader.md for the design.
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

// slide advances to [lo,hi), retracting before adding and resetting disjoint frames.
// Do not transiently sum the previous frame plus arriving rows: that can overflow
// when both frames fit. Disjoint frames must reset instead of subtracting to empty,
// which can also create unrelated overflowing intermediates (ADR-0012 item 9).
// Frame bounds move only forward, so disjoint reset work is bounded by partition length.
// Check both subtraction and addition: neither may silently wrap.
// An overflow flag is provisional; windowExactFrames recomputes the target frame
// before deciding whether its own ordered sum must refuse.
// See docs/internals/exact-window-frame-slide.md for the design.
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

// windowAccOutputType declares SUM/AVG's accumulator: DECIMAL for DECIMAL input,
// IntegerAccOutputType for integers (SUM(int4) bigint, SUM(int8)/AVG numeric),
// FLOAT64 otherwise. Correct erroneous declarations down as well as up.
// Share integer typing with grouped aggregates and both planners (#987, #813).
// Preserve SUM declared bigint over int64 storage: widened integer expressions
// share that vector type, but the plan retains their syntactic width (ADR-0024;
// physical.windowComputedArgDecl / physical.integerAccArgWidth).
// Both integer arms accumulate in Int128; bigint refuses totals outside int64.
// See docs/internals/window-accumulator-type-correction.md for the design.
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
