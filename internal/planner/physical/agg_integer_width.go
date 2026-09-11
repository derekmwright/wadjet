package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// intWidth is PostgreSQL's INTEGER WIDTH for an expression: the property that
// decides what an accumulating aggregate over it declares — `sum(int4)` is
// bigint, `sum(int8)` is numeric (exec.IntegerAccOutputType).
//
// It is NOT the carrier. Every integer in this engine computes and is stored
// in an int64 (ADR-0024's recorded widening), so the carrier answers int8 for
// `id & 3` where PostgreSQL answers int4, and declaring an INT32 vector for it
// instead would put every such value in front of the #361 store guard. The
// width is METADATA BESIDE the carrier, exactly as a DECIMAL's (p,s) is — and
// like the (p,s) it has to RIDE the declaration through every Project, derived
// table, CTE, set-operation arm, window slot and stage boundary, or the same
// expression means two different things either side of a materialization
// (#1018 round 5, B1).
//
// Three values, not two: intWidthUnknown is "this declaration says nothing",
// and its reader falls back to the carrier — which for a base column IS the
// catalog's storage width and so is the right answer there.
type intWidth uint8

const (
	intWidthUnknown intWidth = iota
	intWidth4
	intWidth8
)

// widerIntWidth is PostgreSQL's common-type resolution for two integer
// operands, which is simply the wider one: `int4 + int8` is int8 and its SUM
// is numeric. An unknown operand contributes nothing rather than narrowing.
func widerIntWidth(a, b intWidth) intWidth {
	if a == intWidth8 || b == intWidth8 {
		return intWidth8
	}
	if a == intWidth4 || b == intWidth4 {
		return intWidth4
	}
	return intWidthUnknown
}

// catalogIntWidth is the width a STORED column's type declares, and it is the
// table exec.IntegerAccOutputType answers about: INT32/PORT/PROTOCOL are the
// int4 side (their SUM is bigint), INT64 is the int8 side (its SUM is
// numeric). DURATION rides with INT64 because its wire declaration is int8
// nanoseconds (#834); no other type has a width question, because no other
// type reaches the integer accumulator at all.
func catalogIntWidth(t parquet.TypeID) intWidth {
	switch t {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		return intWidth4
	case parquet.TypeInt64, parquet.TypeDuration:
		return intWidth8
	}
	return intWidthUnknown
}

// carriesIntWidth reports whether a declared TypeID is one the width is
// metadata FOR. A width is recorded beside an integer carrier and nowhere
// else: a STRING or a BOOL column has no PostgreSQL integer width, and
// recording one for it would be a fact about a column that does not have it.
func carriesIntWidth(t parquet.TypeID) bool {
	return catalogIntWidth(t) != intWidthUnknown
}

// aggInputIsWideInteger answers wide only for a provable int8-domain operand.
// It is the BOOLEAN FACE of declaredIntWidth, so the grouped aggregate
// (aggComputedInputDecl), the window (windowComputedArgDecl) and the column
// declaration a derived table publishes (emittedColIntWidth) cannot disagree:
// there is one walk and one rule.
// Expression declarations alone cannot recover width (ADR-0024).
// PORT/PROTOCOL arithmetic is deliberately excluded: expr.operandIsInt and
// intArithAllInt keep it on the FLOAT path; bare columns use int4's table.
// See docs/internals/computed-integer-aggregate-width.md for the design.
func aggInputIsWideInteger(node plansql.Node, decls colDecls) bool {
	return declaredIntWidth(node, decls) == intWidth8
}

// declaredIntWidth is PostgreSQL's integer width for an expression, read off
// the AST, the column declarations and PostgreSQL's own result width for a
// function (expr.PGIntegerResultWidth, #966).
//
// It is the SAME walk in both of its uses. Asked about an aggregate's
// argument it says which accumulator the aggregate declares; asked about a
// projection's expression it says what that projection's OUTPUT COLUMN
// declares, so the next query block reads the width instead of guessing it
// from the carrier.
func declaredIntWidth(node plansql.Node, decls colDecls) intWidth {
	switch n := node.(type) {
	case *plansql.SubqueryNode:
		// A SCALAR SUBQUERY's width is a CATALOG fact, stamped on the plan by
		// annotateSubqueryColumnDecls and installed beside subqueryDecl. The
		// carrier it comes back in cannot say it: `(SELECT c & 3 FROM u)` is
		// an int4-domain value in an int64 box, and SUM over it is bigint
		// where SUM over an int8 one is numeric (#1018 round 5 review, P2).
		if decls.subqueryIntWidth == nil {
			return intWidthUnknown
		}
		if w, ok := decls.subqueryIntWidth(n.SQL); ok {
			return w
		}
		return intWidthUnknown
	case *plansql.ParenNode:
		return declaredIntWidth(n.Inner, decls)
	case *plansql.UnaryOp:
		return declaredIntWidth(n.Inner, decls)
	case *plansql.BinaryOp:
		return widerIntWidth(declaredIntWidth(n.Left, decls), declaredIntWidth(n.Right, decls))
	case *plansql.CaseNode:
		// A choice is as wide as its widest arm, which is what PostgreSQL's
		// own common-type resolution says for a CASE over int4 and int8.
		w := intWidthUnknown
		for _, whenArm := range n.Whens {
			w = widerIntWidth(w, declaredIntWidth(whenArm.Result, decls))
		}
		if n.Else != nil {
			w = widerIntWidth(w, declaredIntWidth(n.Else, decls))
		}
		return w
	case *plansql.FuncCallNode:
		// ABS and MOD answer in their ARGUMENT's own numeric domain — that is
		// expr.NumericDomainScalarFn's set, measured against PostgreSQL for
		// every width by #768, and it is the same predicate that makes a
		// materialized `ABS(int8_col)` column declare INT64 through
		// scalarFnDeclaredNumericDomain. So they are as wide as their
		// arguments, exactly as a polymorphic choice is.
		//
		// This is the half the WINDOW spelling already had and the grouped one
		// did not (#987 review, P2). `SUM(ABS(w_i64)) OVER ()` reads the
		// materialized argument's INT64 declaration and answers numeric, which
		// is PostgreSQL's type; the GROUPED spelling declined here, read the
		// expression as int4 and declared bigint. Same digits, two boxes — the
		// exact class #813 was, with the spellings' roles swapped. Both ask
		// this one predicate now.
		if _, domain := expr.NumericDomainScalarFn(n.Name); domain {
			return widestArgIntWidth(n.Args, nil, decls)
		}
		// A function's own result width is PostgreSQL's, from the ONE table
		// in expr.PGIntegerResultWidth — never the Ret declaration, which is
		// the CARRIER this engine stores the result in.
		//
		// The two are not the same fact and reading one for the other is
		// wrong in both directions. Almost every integer-returning function
		// here declares RetInt64 because every integer in this engine
		// computes in an int64 (ADR-0024's widening), so `regexp_count`,
		// whose PostgreSQL result is `integer`, declares it exactly as
		// `bit_count`, whose PostgreSQL result is `bigint`, does. Round 3 of
		// #966 read that declaration and made `SUM(regexp_count(…))` numeric
		// where PostgreSQL declares bigint — twelve wire cells, grouped and
		// windowed, text and binary.
		//
		// Measured on PostgreSQL 17.11:
		//
		//   sum(regexp_count('abab','a'))   bigint    (result integer)
		//   sum(masklen(cidr))              bigint    (result integer)
		//   sum(octet_length(text))         bigint    (result integer)
		//   sum(bit_count(bytea))           numeric   (result bigint)
		//   sum(txid_current())             numeric   (result bigint)
		//
		// A name the table does not know is not an integer-result function at
		// all, and answers unknown: its own declaration leaves the integer
		// accumulator table anyway.
		if w, known := expr.PGIntegerResultWidth(n.Name); known {
			switch w.Width {
			case expr.PGIntWidth8:
				return intWidth8
			case expr.PGIntWidth4:
				return intWidth4
			}
			// PGIntWidthOperands: the bitwise family, which is arithmetic for
			// this purpose. `f8 & 18` is bigint in PostgreSQL and its SUM is
			// numeric; `f4 & 18` is integer and its SUM is bigint. A shift
			// names argument 0 only, because the count is a separate int4
			// there and does not widen the result.
			return widestArgIntWidth(n.Args, &w, decls)
		}
		// COALESCE / GREATEST / LEAST / NULLIF / IF choose between their
		// arguments and are as wide as the widest. Every other function
		// declines: it is not an integer-result function, so its declaration
		// leaves the integer accumulator table on its own.
		if _, poly := expr.DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args)); !poly {
			return intWidthUnknown
		}
		return widestArgIntWidth(n.Args, nil, decls)
	case *plansql.CastNode:
		// A CAST's TARGET NAME decides its domain, independently of the operand.
		// Do not use nodeDeclaredType: inferCastType declares every integer cast
		// INT64 and cannot distinguish ::int4 from ::bigint (ADR-0012 item 12).
		// The rule applies to grouped and window SUM (#987, #841); PostgreSQL
		// answers must not become refusals (ADR-0012).
		return castTargetIntWidth(n.TypeName)
	case *plansql.ColRef:
		// The DECLARED width first, the carrier only where the declaration is
		// silent. For a base column the carrier IS the catalog's storage
		// width and the two agree; for a MATERIALIZED column — a derived
		// table's, a CTE's, a set-operation arm's, a window slot's — the
		// carrier is the INT64 every integer computes in and says nothing,
		// which is the whole of #1018's round-5 finding.
		if decls.isFieldPath(n) {
			f, ok := decls.field(n)
			if !ok {
				return intWidthUnknown
			}
			return catalogIntWidth(f.Type)
		}
		if w, ok := decls.colIntWidth(n); ok {
			return w
		}
		c, ok := decls.colDecl(n)
		if !ok {
			return intWidthUnknown
		}
		return catalogIntWidth(c.Type)
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return intWidthUnknown
		}
		// PostgreSQL's own literal rule: an integer literal is int4 unless it
		// does not fit, and then it is int8 — so the arithmetic around a wide
		// literal is int8 arithmetic and its SUM is numeric.
		v, err := strconv.ParseInt(strings.TrimSpace(n.Value), 10, 64)
		if err != nil {
			return intWidthUnknown
		}
		if v > 2147483647 || v < -2147483648 {
			return intWidth8
		}
		return intWidth4
	}
	return intWidthUnknown
}

// widestArgIntWidth is the width of the widest argument that CONTRIBUTES one.
// w == nil means every argument contributes, which is what a polymorphic
// choice and the numeric-domain functions want; a PGIntWidthOperands row names
// the positions it takes its width from.
func widestArgIntWidth(args []plansql.Node, w *expr.PGIntegerResult, decls colDecls) intWidth {
	out := intWidthUnknown
	for i, a := range args {
		if w != nil && !pgWidthArg(*w, i) {
			continue
		}
		out = widerIntWidth(out, declaredIntWidth(a, decls))
	}
	return out
}

// castTargetIntWidth is the width a CAST's target names, for declaredIntWidth's
// CastNode arm.
//
// The spellings are inferCastType's own integer list, split by WIDTH — which
// that function deliberately does not do, because the engine carries every
// integer as an int64 and the declaration only has to be wide enough. The
// aggregate's RESULT TYPE is the one question where the width is the whole
// answer: `sum(int4)` is bigint and `sum(int8)` is numeric.
//
// SIGNED has no PostgreSQL meaning (it is a MySQL spelling this engine
// accepts) and is read as int8: numeric is the reading that cannot lose
// digits, and an unknown target answers unknown for the same reason it is safe
// to — a non-integer target's declaration leaves the integer table anyway.
func castTargetIntWidth(typeName string) intWidth {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "BIGINT", "INT8", "INT64", "SIGNED":
		return intWidth8
	case "INTEGER", "INT", "INT4", "INT32", "SMALLINT", "INT2", "PORT", "PROTOCOL":
		return intWidth4
	}
	return intWidthUnknown
}

// pgWidthArg reports whether argument i contributes its width to a
// PGIntWidthOperands result. Nil WidthArgs means every argument, which is what
// `&`, `|`, `#` and `~` want; the shifts name argument 0.
func pgWidthArg(w expr.PGIntegerResult, i int) bool {
	if len(w.WidthArgs) == 0 {
		return true
	}
	for _, a := range w.WidthArgs {
		if a == i {
			return true
		}
	}
	return false
}
