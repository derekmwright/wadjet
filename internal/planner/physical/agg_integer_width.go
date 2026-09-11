package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// aggInputIsWideInteger answers wide only for a provable int8-domain operand
// from the AST, the column declarations and PostgreSQL's own result width for
// a function (expr.PGIntegerResultWidth, #966); other shapes keep the int4
// reading.
// Grouped aggComputedInputDecl and windowComputedArgDecl share this walk:
// SUM(int2|int4-domain) is bigint; SUM(int8-domain) is numeric.
// Expression declarations alone cannot recover width (ADR-0024).
// PORT/PROTOCOL arithmetic is deliberately excluded: expr.operandIsInt and
// intArithAllInt keep it on the FLOAT path; bare columns use int4's table.
// See docs/internals/computed-integer-aggregate-width.md for the design.
func aggInputIsWideInteger(node plansql.Node, decls colDecls) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return aggInputIsWideInteger(n.Inner, decls)
	case *plansql.UnaryOp:
		return aggInputIsWideInteger(n.Inner, decls)
	case *plansql.BinaryOp:
		return aggInputIsWideInteger(n.Left, decls) || aggInputIsWideInteger(n.Right, decls)
	case *plansql.CaseNode:
		// A choice is as wide as its widest arm, which is what PostgreSQL's
		// own common-type resolution says for a CASE over int4 and int8.
		for _, w := range n.Whens {
			if aggInputIsWideInteger(w.Result, decls) {
				return true
			}
		}
		return n.Else != nil && aggInputIsWideInteger(n.Else, decls)
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
			for _, a := range n.Args {
				if aggInputIsWideInteger(a, decls) {
					return true
				}
			}
			return false
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
		// all, and answers false: its own declaration leaves the integer
		// accumulator table anyway.
		if w, known := expr.PGIntegerResultWidth(n.Name); known {
			switch w.Width {
			case expr.PGIntWidth8:
				return true
			case expr.PGIntWidth4:
				return false
			}
			// PGIntWidthOperands: the bitwise family, which is arithmetic for
			// this purpose. `f8 & 18` is bigint in PostgreSQL and its SUM is
			// numeric; `f4 & 18` is integer and its SUM is bigint. A shift
			// names argument 0 only, because the count is a separate int4
			// there and does not widen the result.
			for i, a := range n.Args {
				if !pgWidthArg(w, i) {
					continue
				}
				if aggInputIsWideInteger(a, decls) {
					return true
				}
			}
			return false
		}
		// COALESCE / GREATEST / LEAST / NULLIF / IF choose between their
		// arguments and are as wide as the widest. Every other function
		// declines: it is not an integer-result function, so its declaration
		// leaves the integer accumulator table on its own.
		if _, poly := expr.DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args)); !poly {
			return false
		}
		for _, a := range n.Args {
			if aggInputIsWideInteger(a, decls) {
				return true
			}
		}
		return false
	case *plansql.CastNode:
		// A CAST's TARGET NAME decides its domain, independently of the operand.
		// Only int8 is wide; int4 is not, and non-integer targets leave the integer
		// accumulator table for their float or DECIMAL declaration.
		// Do not use nodeDeclaredType: inferCastType declares every integer cast
		// INT64 and cannot distinguish ::int4 from ::bigint (ADR-0012 item 12).
		// The rule applies to grouped and window SUM (#987, #841); PostgreSQL
		// answers must not become refusals (ADR-0012).
		return castTargetIsWideInteger(n.TypeName)
	case *plansql.ColRef:
		if decls.isFieldPath(n) {
			f, ok := decls.field(n)
			return ok && f.Type == parquet.TypeInt64
		}
		c, ok := decls.colDecl(n)
		return ok && c.Type == parquet.TypeInt64
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return false
		}
		// An integer literal outside int4's range is int8 in PostgreSQL, so
		// the arithmetic around it is int8 arithmetic and its SUM is numeric.
		v, err := strconv.ParseInt(strings.TrimSpace(n.Value), 10, 64)
		return err == nil && (v > 2147483647 || v < -2147483648)
	}
	return false
}

// castTargetIsWideInteger reports whether a CAST's target names the int8
// domain, for aggInputIsWideInteger's CastNode arm.
//
// The spellings are inferCastType's own integer list, split by WIDTH — which
// that function deliberately does not do, because the engine carries every
// integer as an int64 and the declaration only has to be wide enough. The
// aggregate's RESULT TYPE is the one question where the width is the whole
// answer: `sum(int4)` is bigint and `sum(int8)` is numeric.
//
// SIGNED has no PostgreSQL meaning (it is a MySQL spelling this engine
// accepts) and is read as int8: numeric is the reading that cannot lose
// digits, and an unknown target answers false for the same reason it is safe
// to — a non-integer target's declaration leaves the integer table anyway.
func castTargetIsWideInteger(typeName string) bool {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "BIGINT", "INT8", "INT64", "SIGNED":
		return true
	}
	return false
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
