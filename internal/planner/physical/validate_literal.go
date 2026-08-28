package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// checkLiteralTypes refuses a constant that names no value of the type its
// context demands, from the column's DECLARATION, before any row exists.
//
// The column's declared parquet.TypeID reaches here through colScope
// (validate.go), and refuseLiteralForType holds the rule per type that has a
// "this string names no value of me" test. Today that is DECIMAL alone: a
// quoted string that is not a number against a DECIMAL column. PostgreSQL
// resolves an unknown-typed literal's type from the column it meets and
// refuses at parse/bind time — `SELECT count(*) FROM t WHERE d = 'abc'` is
// 22P02 there whether or not the table holds a row.
//
// #579 widened colScope from a bare `isDecimal bool` to the full TypeID so the
// network types (CIDR/IPv4/IPv6/MAC/UUID) can join this rule, but wiring their
// refusal waits on #627 — wadjet's network parsers are stricter than
// PostgreSQL's grammar, so refusing on them here would reject PG-valid input
// (see refuseLiteralForType).
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

// refuseLiteralAmong refuses a call whose arguments put a column with a
// literal rule (see refuseLiteralForType — today DECIMAL) and a literal that
// names no value of its type in the same comparison, whichever pair the
// values end up selecting.
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

// refuseLiteralAgainstColumn raises when colSide is a column this scope can
// prove has a type with a "this string names no value of me" rule (today
// DECIMAL — see refuseLiteralForType) and litSide is a quoted string the
// type's own test rejects.
//
// A NUMERIC literal is never refused against a DECIMAL, however large: `1e400`
// IS a number and is read as one at the column's scale, saturating past the
// carrier (ADR-0012 item 6). A NULL or a boolean is a different rule and is
// left to the comparison. The DECIMAL text test is `expr.IsNumericLiteralText`,
// the SAME predicate the runtime refusal uses, so the two paths cannot
// disagree about which strings name a value.
func refuseLiteralAgainstColumn(scope *colScope, colSide, litSide plansql.Node) error {
	ref, ok := unwrapParens(colSide).(*plansql.ColRef)
	if !ok {
		return nil
	}
	typ, ok := scope.provableColType(ref)
	if !ok {
		return nil
	}
	lit, ok := unwrapParens(litSide).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return nil
	}
	return refuseLiteralForType(typ, lit.Value)
}

// refuseLiteralForType raises when text names no value of a column type that
// has a plan-time literal rule.
//
// Today that is DECIMAL alone → expr.InvalidLiteralError (22P02, "type
// numeric"), the site #517 closed, kept byte-for-byte.
//
// The switch is deliberately extensible per TypeID because #579 widened
// colScope to carry the column's parquet.TypeID (not a bare bool), but the
// NETWORK types (CIDR/IPv4/IPv6/MAC/UUID) are NOT wired here yet: wadjet's
// network literal parsers (net.ParseCIDR, net.ParseMAC, the brace-unaware
// UUID parser) are STRICTER than PostgreSQL's input grammar — they reject
// abbreviated cidr/inet ('192.168', '10/8'), several macaddr notations
// ('08002b:010203', '0800-2b01-0203') and the brace/no-dash/uppercase UUID
// forms that PostgreSQL ACCEPTS. Refusing on those parsers here would raise
// 22P02 for input PostgreSQL answers — a PG-superset regression the binder
// must never make (ADR-0012 item 1: never refuse what PostgreSQL accepts),
// net-new at the boxed sites (GREATEST/LEAST, simple CASE, IN, IS DISTINCT
// FROM) that had no refusal before. That over-strictness is a latent RUNTIME
// bug too (exec.networkConstError refuses the same PG-valid forms
// data-dependently), and both halves are deferred to #627: widen the parsers
// to a SUPERSET of PostgreSQL's grammar first, then a network arm can be
// added here using that same predicate without ever refusing a PG-valid
// literal.
//
// Types with no rule (every other TypeID, PORT/PROTOCOL included — they have
// no PostgreSQL analog) return nil: a legal comparison must still work, and
// the binder refuses only what PostgreSQL refuses.
func refuseLiteralForType(typ parquet.TypeID, text string) error {
	switch typ {
	case parquet.TypeDecimal:
		if !expr.IsNumericLiteralText(text) {
			return &expr.InvalidLiteralError{Input: text, DestType: "numeric"}
		}
	}
	return nil
}

// unwrapParens strips redundant parentheses from an operand.
//
// `WHERE (d) = 'abc'` is the same query as `WHERE d = 'abc'` and PostgreSQL
// refuses both, but the check matched a bare *ColRef only, so a pair of
// parentheses put the refusal back where #517 found it: per row, and skipped
// entirely over an empty table (#504 review, non-blocker b). Parentheses carry
// no meaning past grouping, so a rule that reads operand SHAPES has to see
// through them.
func unwrapParens(n plansql.Node) plansql.Node {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok || p.Inner == nil {
			return n
		}
		n = p.Inner
	}
}
