// SPDX-License-Identifier: MIT

// This file holds PostgreSQL's NUMERIC CATEGORY of a float64-carried
// expression: the half of a declaration that says whether the value the
// engine computes in a double is, to PostgreSQL, a `numeric` or a `double
// precision`. Governed by ADR-0024 item 2 (its 2026-09-28 amendment).
package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// pgCategory is PostgreSQL's numeric type category of an operand, as far as
// the rules below need it: an integer, a numeric, a float, or something this
// layer cannot name (a quoted literal, a NULL, an undecided expression, a
// non-number). Unknown never produces a numeric answer: the assignment then
// keeps the carrier's reading, which is what it had.
type pgCategory int

const (
	pgCatUnknown pgCategory = iota
	pgCatInteger
	pgCatNumeric
	pgCatFloat
)

// pgCategoryOfDecl maps one declaration onto PostgreSQL's category.
//
// A FLOAT64 is a float8 unless something says otherwise: PGNumeric (a value
// PostgreSQL computes in numeric) or a numeric LITERAL too wide for the
// DECIMAL carrier, which keeps the FLOAT64 declaration (DeclNumericLit) while
// PostgreSQL types every fractional or out-of-bigint literal numeric.
func pgCategoryOfDecl(d expr.DeclType, c expr.Confidence) pgCategory {
	if c != expr.Decided || d.Untyped || d.Quoted {
		return pgCatUnknown
	}
	switch d.ID {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypePort, parquet.TypeProtocol:
		// PORT ⊕ int is int4 arithmetic (ADR-0024 item 2).
		return pgCatInteger
	case parquet.TypeDecimal:
		return pgCatNumeric
	case parquet.TypeFloat32:
		return pgCatFloat
	case parquet.TypeFloat64:
		if d.PGNumeric || d.Lit {
			return pgCatNumeric
		}
		return pgCatFloat
	}
	return pgCatUnknown
}

// pgCategoryOf is PostgreSQL's category of an expression, resolved
// STRUCTURALLY by PostgreSQL's own rules rather than read back off this
// engine's declaration of every subtree: the declared-type walk already
// re-resolves children at every level, and asking it again from each FLOAT64
// arm made a 20-term sum take a second. This walk visits each node once.
//
// The engine's declaration still decides WHERE the answer is read — only a
// FLOAT64-declared node carries PGNumeric (withPGNumeric) — so a category
// that disagrees with an exact DECIMAL or INTEGER declaration changes
// nothing.
func pgCategoryOf(n plansql.Node, decls ColDecls) pgCategory {
	switch x := n.(type) {
	case *plansql.ParenNode:
		return pgCategoryOf(x.Inner, decls)
	case *plansql.Lit:
		if x.Kind != plansql.LitNumber {
			return pgCatUnknown
		}
		// PostgreSQL's literal rule: digits that fit a bigint are an
		// integer, anything else (a fraction, an exponent, more digits) is
		// numeric.
		if _, err := strconv.ParseInt(x.Value, 10, 64); err == nil {
			return pgCatInteger
		}
		return pgCatNumeric
	case *plansql.ColRef:
		d, c := colRefDeclaredType(x, decls)
		return pgCategoryOfDecl(withPGNumeric(d, decls.colPGNumeric(x)), c)
	case *plansql.UnaryOp:
		if x.Op == "-" || x.Op == "+" {
			return pgCategoryOf(x.Inner, decls)
		}
	case *plansql.BinaryOp:
		switch x.Op {
		case "+", "-", "*", "/", "%":
			return pgArith(pgCategoryOf(x.Left, decls), pgCategoryOf(x.Right, decls))
		}
	case *plansql.CastNode:
		if _, _, _, ok := expr.DecimalCastDest(x.TypeName); ok {
			return pgCatNumeric
		}
		switch inferCastType(x.TypeName) {
		case parquet.TypeFloat32, parquet.TypeFloat64:
			return pgCatFloat
		case parquet.TypeInt32, parquet.TypeInt64:
			return pgCatInteger
		}
	case *plansql.CaseNode:
		arms := make([]plansql.Node, 0, len(x.Whens)+1)
		for _, w := range x.Whens {
			arms = append(arms, w.Result)
		}
		return pgFold(append(arms, x.Else), decls)
	case *plansql.FuncCallNode:
		return funcPGCategory(x, decls)
	case *plansql.SubqueryNode:
		if decls.subqueryDecl != nil && !x.Array {
			if col, ok := decls.subqueryDecl(x.SQL); ok {
				return pgCategoryOfDecl(expr.Decl(col.Type), expr.Decided)
			}
		}
	}
	return pgCatUnknown
}

// pgArith is the binary operators' resolution: float8 over everything,
// numeric over the integers, integer only between integers.
func pgArith(a, b pgCategory) pgCategory {
	switch {
	case a == pgCatUnknown || b == pgCatUnknown:
		return pgCatUnknown
	case a == pgCatFloat || b == pgCatFloat:
		return pgCatFloat
	case a == pgCatNumeric || b == pgCatNumeric:
		return pgCatNumeric
	}
	return pgCatInteger
}

// pgFold is select_common_type over the value arms of a CASE-family
// construct. A NULL arm (or a missing ELSE) produces no value and takes no
// part; an arm this walk cannot name makes the answer unknown.
func pgFold(arms []plansql.Node, decls ColDecls) pgCategory {
	out := pgCatUnknown
	for _, a := range arms {
		if a == nil {
			continue
		}
		if l, ok := unwrapParens(a).(*plansql.Lit); ok && l.Kind == plansql.LitNull {
			continue
		}
		c := pgCategoryOf(a, decls)
		if c == pgCatUnknown {
			return pgCatUnknown
		}
		if out == pgCatUnknown {
			out = c
			continue
		}
		out = pgArith(out, c)
	}
	return out
}

// funcPGCategory is PostgreSQL's result category of a call, from its own
// overloads of the functions the registry declares double precision
// (measured on 17.11 over pg_proc and pg_typeof):
//
//	abs ceil ceiling floor round trunc sign sqrt exp ln log log10
//	    (x)                  numeric(x) → numeric, float8(x) → float8;
//	                         an integer resolves to the float8 overload
//	                         (abs(integer) is integer)
//	round trunc (x, n)       numeric, integer → numeric
//	log (b, x)               numeric, numeric → numeric
//	power pow (x, y)         float8 over a float8 or two integers, else numeric
//	mod (x, y)               integer over two integers, else numeric
//	EXTRACT(field FROM x)    numeric, always (date_part stays float8)
//	coalesce nullif greatest least — and ifnull / if, this engine's
//	spellings of COALESCE and CASE — fold like CASE
//
// Every other function answers the category of the type the registry
// declares for it: the trigonometric family, cbrt, degrees, radians, pi and
// random are float8 in PostgreSQL too, and a function PostgreSQL has no
// spelling of is what this engine declares.
func funcPGCategory(n *plansql.FuncCallNode, decls ColDecls) pgCategory {
	if n.OutputLabel == "extract" {
		return pgCatNumeric
	}
	arg := func(i int) pgCategory {
		if i >= len(n.Args) {
			return pgCatUnknown
		}
		return pgCategoryOf(n.Args[i], decls)
	}
	name := strings.ToLower(n.Name)
	switch name {
	case "abs", "ceil", "ceiling", "floor", "round", "trunc", "sign", "sqrt", "exp", "ln", "log", "log10":
		switch len(n.Args) {
		case 1:
			switch arg(0) {
			case pgCatNumeric:
				return pgCatNumeric
			case pgCatFloat:
				return pgCatFloat
			case pgCatInteger:
				if name == "abs" {
					return pgCatInteger
				}
				return pgCatFloat
			}
			return pgCatUnknown
		case 2:
			switch name {
			case "round", "trunc":
				if c := arg(0); c == pgCatNumeric || c == pgCatInteger {
					return pgCatNumeric
				}
			case "log":
				a, b := arg(0), arg(1)
				if (a == pgCatNumeric || a == pgCatInteger) && (b == pgCatNumeric || b == pgCatInteger) {
					return pgCatNumeric
				}
			}
		}
		return pgCatUnknown
	case "power", "pow":
		if len(n.Args) != 2 {
			return pgCatUnknown
		}
		c := pgArith(arg(0), arg(1))
		if c == pgCatInteger {
			return pgCatFloat
		}
		return c
	case "mod":
		if len(n.Args) != 2 {
			return pgCatUnknown
		}
		return pgArith(arg(0), arg(1))
	case "coalesce", "ifnull", "greatest", "least", "nullif":
		return pgFold(n.Args, decls)
	case "if":
		if len(n.Args) == 3 {
			return pgFold(n.Args[1:], decls)
		}
		return pgCatUnknown
	}
	ret := expr.DefaultRegistry.ReturnType(name)
	if t, ok := ret.FixedType(); ok {
		return pgCategoryOfDecl(expr.Decl(t), expr.Decided)
	}
	return pgCatUnknown
}

// withPGNumeric stamps the category onto a FLOAT64 declaration and clears it
// on every other one.
func withPGNumeric(d expr.DeclType, numeric bool) expr.DeclType {
	d.PGNumeric = d.ID == parquet.TypeFloat64 && numeric
	return d
}

// colPGNumeric resolves a column reference to the category its declaration
// carries, in the order colIntWidth resolves the width.
func (d ColDecls) colPGNumeric(n *plansql.ColRef) bool {
	if n == nil || len(d.pgNumeric) == 0 {
		return false
	}
	if n.Table != "" {
		if v, ok := d.pgNumeric[strings.ToLower(n.Table+"."+n.Column)]; ok {
			return v
		}
	}
	if d.isFieldPath(n) {
		return false
	}
	return d.pgNumeric[strings.ToLower(n.Column)]
}

// pgNumericOverNoFloat is select_common_type's answer over categories already
// resolved (a set operation's arms): numeric when at least one is numeric,
// none is float8 and every one is named.
func pgNumericOverNoFloat(cats ...pgCategory) bool {
	if len(cats) == 0 {
		return false
	}
	out := cats[0]
	for _, c := range cats[1:] {
		out = pgArith(out, c)
	}
	return out == pgCatNumeric
}
