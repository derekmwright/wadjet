package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// checkLiteralTypes refuses a constant that names no value of the type its
// context demands, from the column's DECLARATION, before any row exists.
//
// Today that is one rule: a quoted string that is not a number, compared
// against a DECIMAL column. PostgreSQL resolves an unknown-typed literal's
// type from the column it meets and refuses at parse/bind time — `SELECT
// count(*) FROM t WHERE d = 'abc'` is 22P02 there whether or not the table
// holds a row.
//
// Wadjet already raised the same SQLSTATE, but from inside the COMPARISON, so
// it depended on a row reaching it and on which operand won (#517):
//
//   - PER ROW. An empty table, or a conjunct no row survives to — `k > 100000
//     AND d IS DISTINCT FROM 'abc'` — answered zero rows instead of erroring.
//   - PAIRWISE, so the DATA decided. GREATEST/LEAST compare (best-so-far,
//     candidate) pairs and a pair refuses only when a DECIMAL column is on one
//     side and the bad literal on the other, so the SAME three arguments
//     refused under GREATEST and answered under LEAST:
//     `GREATEST(k, 'abc', d_2)` raised and `LEAST(k, 'abc', d_2)` returned a
//     row. A refusal that depends on which operand won a comparison is not a
//     type rule at all.
//
// Both close here, because a declared type is not a property of a row. The
// runtime refusals stay: they cover the shapes this binder cannot see — an
// expression it does not parse, an open scope, a column whose source is a
// derived table or a CTE — and, being the same predicate
// (`expr.IsNumericLiteralText`), they cannot disagree with this one about
// which strings are numbers.
//
// It is as conservative as the rest of the binder (validate.go's contract): it
// refuses only when the column PROVABLY resolves to a declared DECIMAL in a
// closed scope. A false positive breaks a working query; a false negative
// merely leaves the refusal where it already was.
func checkLiteralTypes(node plansql.Node, scope *colScope) error {
	if node == nil || scope == nil || scope.open {
		return nil
	}
	switch n := node.(type) {
	case *plansql.CmpExpr:
		// Every comparison operator, and IS [NOT] DISTINCT FROM, which the
		// parser lowers to a CmpExpr with its own Op rather than to a node of
		// its own — so the boxed site gets this for free.
		if err := refuseLiteralPair(scope, n.Left, n.Right); err != nil {
			return err
		}
		return checkLiteralChildren(n.Left, n.Right, scope)
	case *plansql.InExpr:
		for _, v := range n.Values {
			if err := refuseLiteralPair(scope, n.Left, v); err != nil {
				return err
			}
		}
		if err := checkLiteralTypes(n.Left, scope); err != nil {
			return err
		}
		return checkLiteralList(n.Values, scope)
	case *plansql.BetweenExpr:
		if err := refuseLiteralPair(scope, n.Left, n.Low); err != nil {
			return err
		}
		if err := refuseLiteralPair(scope, n.Left, n.High); err != nil {
			return err
		}
		return checkLiteralList([]plansql.Node{n.Left, n.Low, n.High}, scope)
	case *plansql.CaseNode:
		// A SIMPLE CASE compares its subject against each WHEN value; a
		// searched CASE's WHENs are conditions and are covered by the
		// recursion below.
		if n.Subject != nil {
			for _, w := range n.Whens {
				if err := refuseLiteralPair(scope, n.Subject, w.Cond); err != nil {
					return err
				}
			}
		}
		if err := checkLiteralTypes(n.Subject, scope); err != nil {
			return err
		}
		for _, w := range n.Whens {
			if err := checkLiteralChildren(w.Cond, w.Result, scope); err != nil {
				return err
			}
		}
		return checkLiteralTypes(n.Else, scope)
	case *plansql.FuncCallNode:
		// GREATEST/LEAST compare their arguments PAIRWISE at runtime, and
		// which pairs get compared depends on the values. Here every argument
		// is in hand at once, so the question is the type rule it should have
		// been: does this call put a DECIMAL column and a non-numeric literal
		// in the same comparison? Both functions answer it the same way, on
		// the same arguments, whatever the data.
		switch strings.ToLower(n.Name) {
		case "greatest", "least":
			if err := refuseLiteralAmong(scope, n.Args); err != nil {
				return err
			}
		}
		return checkLiteralList(n.Args, scope)
	case *plansql.BinaryOp:
		return checkLiteralChildren(n.Left, n.Right, scope)
	case *plansql.UnaryOp:
		return checkLiteralTypes(n.Inner, scope)
	case *plansql.AndNode:
		return checkLiteralChildren(n.Left, n.Right, scope)
	case *plansql.OrNode:
		return checkLiteralChildren(n.Left, n.Right, scope)
	case *plansql.NotNode:
		return checkLiteralTypes(n.Inner, scope)
	case *plansql.ParenNode:
		return checkLiteralTypes(n.Inner, scope)
	case *plansql.CastNode:
		return checkLiteralTypes(n.Inner, scope)
	case *plansql.LikeExpr:
		return checkLiteralChildren(n.Left, n.Pattern, scope)
	case *plansql.IsExpr:
		return checkLiteralTypes(n.Left, scope)
	case *plansql.ArrayLitNode:
		return checkLiteralList(n.Elements, scope)
	case *plansql.TupleNode:
		return checkLiteralList(n.Elements, scope)
	case *plansql.AnyAllExpr:
		return checkLiteralTypes(n.Left, scope)
	}
	return nil
}

func checkLiteralChildren(a, b plansql.Node, scope *colScope) error {
	if err := checkLiteralTypes(a, scope); err != nil {
		return err
	}
	return checkLiteralTypes(b, scope)
}

func checkLiteralList(nodes []plansql.Node, scope *colScope) error {
	for _, n := range nodes {
		if err := checkLiteralTypes(n, scope); err != nil {
			return err
		}
	}
	return nil
}

// refuseLiteralPair refuses one comparison's two operands, in either order.
func refuseLiteralPair(scope *colScope, a, b plansql.Node) error {
	if err := refuseLiteralAgainstColumn(scope, a, b); err != nil {
		return err
	}
	return refuseLiteralAgainstColumn(scope, b, a)
}

// refuseLiteralAmong refuses a call whose arguments put a DECIMAL column and a
// non-numeric literal in the same comparison, whichever pair the values end up
// selecting.
func refuseLiteralAmong(scope *colScope, args []plansql.Node) error {
	for _, a := range args {
		for _, b := range args {
			if err := refuseLiteralAgainstColumn(scope, a, b); err != nil {
				return err
			}
		}
	}
	return nil
}

// refuseLiteralAgainstColumn raises when colSide is a DECIMAL column this
// scope can prove and litSide is a quoted string that names no number.
//
// A NUMERIC literal is never refused, however large: `1e400` IS a number and
// is read as one at the column's scale, saturating past the carrier
// (ADR-0012 item 6). A NULL or a boolean is a different rule and is left to
// the comparison. The numeric test is `expr.IsNumericLiteralText`, the SAME
// predicate the runtime refusal uses, so the two cannot disagree about which
// strings are numbers.
func refuseLiteralAgainstColumn(scope *colScope, colSide, litSide plansql.Node) error {
	ref, ok := colSide.(*plansql.ColRef)
	if !ok || !scope.isDecimalRef(ref) {
		return nil
	}
	lit, ok := litSide.(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return nil
	}
	if expr.IsNumericLiteralText(lit.Value) {
		return nil
	}
	return &expr.InvalidLiteralError{Input: lit.Value, DestType: "numeric"}
}
