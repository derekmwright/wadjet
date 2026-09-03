package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The WIDTH of a COMPUTED integer aggregate argument (#841's second half).
//
// PostgreSQL's SUM rule is by INPUT WIDTH: `sum(int2|int4)` is bigint, because
// there is a wider integer to grow into, and `sum(int8)` is NUMERIC, because
// there is not. A BARE column already gets that rule here
// (aggIntegerOutputType). A COMPUTED argument did not: every integer
// expression declares INT64 in this engine (ADR-0024's recorded widening), so
// aggOutputFromInputDecl could not tell `SUM(CASE WHEN … THEN 1 ELSE 0 END)`
// — TPC-H Q12's shape, int4 in PostgreSQL and bigint under SUM — from
// `SUM(bigint_col + 0)`, which is numeric there. It read them all as int4,
// keeping Q12's OID and leaving the int8 case as a residual: the total sums
// into an int64 carrier and a query PostgreSQL answers becomes 22003.
//
// That residual was invisible while `rewriteConstArithAggs` was lifting the
// constant out — `SUM(b + 0)` ran as `SUM(b) + 0*COUNT(b)` over a BARE column,
// which takes the exact path — and it surfaced the moment the lift stopped
// moving refusals. It is the same question #841 asks: one expression, one
// disposition, whichever position it is written in.
//
// The width is recoverable from the AST plus the column declarations, which is
// what this walk does. It answers "wide" ONLY for an expression that provably
// carries an int8-domain operand, and everything else keeps the int4 reading
// it had — so the change is confined to shapes that can be pointed at, and no
// declaration moves on a shape this walk cannot see through.
//
//	SUM(CASE WHEN … THEN 1 ELSE 0 END)   not wide → bigint   (PostgreSQL: bigint)
//	SUM(int32_col * 2)                   not wide → bigint   (PostgreSQL: bigint)
//	SUM(int64_col + 0)                   WIDE     → numeric  (PostgreSQL: numeric)
//	SUM(row_number_slot * 2)             WIDE     → numeric  (PostgreSQL: numeric)
//	SUM(9223372036854775807 * x)         WIDE     → numeric  (the literal is int8)
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
		// COALESCE / GREATEST / LEAST / NULLIF / IF choose between their
		// arguments and are as wide as the widest. Every other function
		// declines: an unrecognized return width keeps today's reading, which
		// is the conservative side — it leaves the declaration where it is.
		if _, poly := expr.DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args)); !poly {
			return false
		}
		for _, a := range n.Args {
			if aggInputIsWideInteger(a, decls) {
				return true
			}
		}
		return false
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
