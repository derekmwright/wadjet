package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// IntegerAccOutputType is THE result type of an ACCUMULATING aggregate — SUM
// or AVG — over an INTEGER input, for the grouped spelling and the windowed
// one alike. PostgreSQL's rules, taken from the live server (#784, #813,
// #953):
//
//	pg_typeof(sum(int4)) -> bigint     pg_typeof(sum(int8)) -> numeric
//	pg_typeof(avg(int4)) -> numeric    pg_typeof(avg(int8)) -> numeric
//
// The two SUM rules differ because int4's sum has a wider integer type to
// grow into and int8's does not; a SUM(int8) in an int64 WRAPS past 2^63,
// which ADR-0024 item 4 makes a 22003 rather than an answer. AVG's scale is
// batch.AvgScale(0) = 4 — the same +4 rule a DECIMAL input takes, chosen over
// PostgreSQL's magnitude-dependent numeric division scale because that would
// change a column's declared scale when the same query saw more rows
// (ADR-0024 item 2).
//
// ONE function, asked by four sites, which is the point of it existing:
//
//	physical.aggIntegerOutputType / aggIntegerOutputDecimal — the GROUPED
//	   aggregate's plan-time declaration;
//	physical.windowSpecOutputType — the WINDOW's plan-time declaration;
//	exec.windowAccOutputType — the window operator's runtime correction of
//	   that declaration from the vector it actually reads;
//	exec.aggIntExact — which CARRIER the grouped accumulator uses.
//
// `SUM(x) GROUP BY g` and `SUM(x) OVER (PARTITION BY g)` are one question
// written twice and a client reads both in one result set; before this
// function they were answered from two tables and disagreed about the type
// AND the digits of the same total (#813, ADR-0012's divergence list).
//
// PORT and PROTOCOL are here beside INT32 (#953). They are int4-domain values
// — PORT is 0..65535 and PROTOCOL 0..255 — and since #834 both DECLARE int4
// on the wire, so `sum(port)` is a sum over an int4 column as far as any
// client can see and PostgreSQL's answer for that is bigint. Before this they
// were worse than mis-typed: `SUM(protocol)` had no arm in the grouped
// dispatch at all and answered NULL where the WINDOWED spelling answered the
// total.
//
// DATE, TIMESTAMP and DURATION are deliberately NOT here: `date` and
// `timestamp` are their own wire types with no PostgreSQL `sum`, and an
// interval's sum is an interval rather than a number. They keep float64 in
// both spellings, which is what they already agreed on.
//
// ok=false for every other input type, and the caller then keeps whatever it
// declared before — which is what makes this additive rather than a second
// dispatch nothing keeps in step with the first.
func IntegerAccOutputType(avg bool, in parquet.TypeID) (out parquet.TypeID, precision, scale int, ok bool) {
	switch in {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		if avg {
			return parquet.TypeDecimal, batch.MaxDecimalPrecision, batch.AvgScale(0), true
		}
		return parquet.TypeInt64, 0, 0, true
	case parquet.TypeInt64:
		if avg {
			return parquet.TypeDecimal, batch.MaxDecimalPrecision, batch.AvgScale(0), true
		}
		return parquet.TypeDecimal, batch.MaxDecimalPrecision, 0, true
	}
	return 0, 0, 0, false
}

// integerAccInput reports whether a vector of this type is summed EXACTLY by
// the rules above — the input side of IntegerAccOutputType, asked where only
// the input is in hand.
func integerAccInput(in parquet.TypeID) bool {
	_, _, _, ok := IntegerAccOutputType(false, in)
	return ok
}
