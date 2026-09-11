package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// aggInputIsWideInteger answers wide only for a provable int8-domain operand
// from the AST and the column and FUNCTION declarations (#966); other shapes
// keep the int4 reading.
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
		// COALESCE / GREATEST / LEAST / NULLIF / IF choose between their
		// arguments and are as wide as the widest.
		if _, poly := expr.DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args)); !poly {
			// Everything else is as wide as it DECLARES. A function with a
			// fixed RetInt64 is an int8-domain operand — it is exactly what
			// `f8 & 18` is on PostgreSQL, whose type is bigint and whose SUM
			// is therefore numeric — and a fixed RetInt32 is an int4-domain
			// one, whose SUM is bigint (`SUM(length(s))`, PostgreSQL's own).
			// Reading the declaration is what the CastNode arm already does
			// with a target name, and what the ColRef arm does with a column.
			//
			// Not reading it was #966 round 2 B1: BITWISE_AND had just been
			// declared int8, the walk did not follow an ordinary function, so
			// SUM over it took the BIGINT accumulator and
			// `SUM(BITWISE_AND(f8, 4611686018427387904))` over two rows of
			// 2^62 answered 22003 where PostgreSQL answers
			// 9223372036854775808 — a right value turned into a refusal, on
			// all four arms and both the grouped and the windowed spelling,
			// which share this walk.
			//
			// A declaration that is not a fixed integer (float, text, a
			// container, DYNAMIC) answers false, which is the conservative
			// side: a non-integer operand leaves the integer accumulator
			// table for its own declaration anyway.
			return expr.FuncDeclaresInt64(n.Name)
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
