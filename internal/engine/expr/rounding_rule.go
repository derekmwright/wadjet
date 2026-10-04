// SPDX-License-Identifier: MIT

package expr

import "github.com/derekmwright/wadjet/internal/engine/batch"

// THE ROUNDING RULE a value gets is decided by its DECLARED TYPE, in this one
// place, for every site that rounds a float64-carried value to an integral one
// (#381, #1542): ROUND(x), CAST(x AS SMALLINT / INTEGER / BIGINT) and `::`,
// and each element of an array cast to an integer array.
//
// PostgreSQL 17.11 rounds a double precision or real value half TO EVEN —
// round(float8) and the float8 → integer cast are both rint() — and a numeric
// value half AWAY from zero (round(numeric), numeric → integer):
//
//	round(2.5::float8) = 2   round(2.5) = 3   CAST(2.5::float8 AS int) = 2
//	CAST(2.5 AS int)   = 3   ARRAY[2.5::float8, 1.5::float8]::bigint[] = {2,2}
//
// This engine carries both types in one float64, so the box cannot say which
// rule applies: the operand's PostgreSQL CATEGORY does (the planner's
// category walk, operandDecl.category — a column's declaration, an
// expression's operand rules, a scalar subquery's column). Before this
// function the decision was made three ways: ROUND looked for a CAST to a
// float type spelled directly in its argument, so `round(f)` over a DOUBLE
// PRECISION column rounded half away; the CAST read the category; an array
// element was cast as a bare literal and so took the numeric constant's rule.
//
// An integer value has no fraction, so either rule answers it, and an
// integer category keeps the carrier's reading like a category this layer
// cannot name (no resolver, no batch):
// a numeric LITERAL — PostgreSQL types every fractional literal numeric — is
// numeric, and every other float64 is a double.
//
// The assignment cast of INSERT / UPDATE / MERGE decides the same question in
// wadjet.assignIntegerValue from the source's declaration (dmlSourceIsFloat),
// which is the same category read through the declared-type walk.
func roundsHalfEven(cat PGCategory, operand Expr) bool {
	switch cat {
	case PGCatFloat8:
		return true
	case PGCatNumeric:
		return false
	}
	return !isConstNumericOperand(operand)
}

// roundRule is ROUND's operand-dependent choice of kernel: away is fnRound
// (half away from zero, numeric), even fnRoundHalfEven (half to even,
// float8). Both share the call's arguments; the choice is made once, against
// the first batch, by roundsHalfEven.
type roundRule struct {
	operand Expr
	decl    *operandDecl
	away    *FuncCall
	even    *FuncCall
}

func newRoundRule(fc *FuncCall) *roundRule {
	if len(fc.Args) < 1 || len(fc.Args) > 2 {
		return nil
	}
	r := &roundRule{
		operand: fc.Args[0],
		away:    &FuncCall{Name: "round", Args: fc.Args},
		even:    &FuncCall{Name: "round_half_even", Args: fc.Args},
	}
	if len(fc.argDecls) > 0 {
		r.decl = fc.argDecls[0]
	}
	return r
}

func (r *roundRule) pick(b *batch.RecordBatch) *FuncCall {
	if roundsHalfEven(r.decl.category(b), r.operand) {
		return r.even
	}
	return r.away
}
