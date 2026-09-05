package physical

import (
	"math"
	"strconv"
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
// "this string names no value of me" test: the whole numeric family — the
// integer types, the FLOAT types and DECIMAL — each read with its OWN
// PostgreSQL input grammar. PostgreSQL resolves an unknown-typed literal's
// type from the column it meets and refuses at parse/bind time — `SELECT
// count(*) FROM t WHERE d = 'abc'` is 22P02 there whether or not the table
// holds a row.
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
// (`expr.RefuseNumericLiteral` over `kernel.QuotedLitStatus`), they cannot
// disagree with this one about which strings name a value of which type.
//
// It is as conservative as the rest of the binder (validate.go's contract): it
// refuses only when the column PROVABLY resolves to a declared type with a
// rule, in a closed scope. A false positive breaks a working query; a false
// negative merely leaves the refusal where it already was.
func checkLiteralTypes(node plansql.Node, scope *colScope) error {
	if node == nil || scope == nil || scope.open {
		return nil
	}
	switch n := node.(type) {
	case *plansql.CmpExpr:
		// Every comparison operator, and IS [NOT] DISTINCT FROM, which the
		// parser lowers to a CmpExpr with its own Op rather than to a node of
		// its own — so the boxed site gets this for free.
		//
		// CHILDREN FIRST, here and at every node below that can hold a nested
		// call. PostgreSQL analyses inside-out and reports the innermost
		// coercion that fails, so `GREATEST('3.1','12.75',bigint) = '12.75'`
		// names '3.1' there; refusing this node's own pair first named
		// '12.75'. Same SQLSTATE and same type either way, but the message is
		// part of the answer (ADR-0012 item 1).
		if err := checkLiteralChildren(n.Left, n.Right, scope); err != nil {
			return err
		}
		return refuseLiteralPair(scope, n.Left, n.Right)
	case *plansql.InExpr:
		if err := checkLiteralTypes(n.Left, scope); err != nil {
			return err
		}
		if err := checkLiteralList(n.Values, scope); err != nil {
			return err
		}
		for _, v := range n.Values {
			if err := refuseLiteralPair(scope, n.Left, v); err != nil {
				return err
			}
		}
		return nil
	case *plansql.BetweenExpr:
		if err := checkLiteralList([]plansql.Node{n.Left, n.Low, n.High}, scope); err != nil {
			return err
		}
		if err := refuseLiteralPair(scope, n.Left, n.Low); err != nil {
			return err
		}
		return refuseLiteralPair(scope, n.Left, n.High)
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
		// been: does this call put a numeric column and a quoted literal its
		// type cannot read in the same comparison? Both functions answer it
		// the same way, on the same arguments, whatever the data.
		if err := checkLiteralList(n.Args, scope); err != nil {
			return err
		}
		switch strings.ToLower(n.Name) {
		case "greatest", "least", "coalesce", "ifnull":
			// COALESCE folds its arguments to ONE type at parse analysis, the
			// same select_common_type GREATEST/LEAST fold through, so a quoted
			// literal beside a column whose type cannot read it is 22P02 on
			// the server before any row: `COALESCE(cidr_col, 'zzz')`,
			// `COALESCE(numeric_col, 'abc')` and the macaddr/uuid/inet forms
			// all raise there (measured 17.11). It ANSWERED here for all five
			// network types on every arm — the last site outside the "one
			// classification, every site" claim (round-2 review P-7).
			if err := refuseLiteralAmong(scope, n.Args); err != nil {
				return err
			}
		case "nullif":
			// NULLIF(x, y) is an EQUALITY test between its two arguments, so
			// the pair takes the same rule `=` does: PostgreSQL plans
			// `NULLIF(r, '3.1')` as `NULLIF(r, '3.1'::real)` and raises 22P02
			// for `NULLIF(int_col, 'abc')` (both verified live). It compares
			// exactly one pair, unlike GREATEST/LEAST, so it names that pair
			// rather than every ordered pair of arguments.
			if len(n.Args) == 2 {
				if err := refuseLiteralPair(scope, n.Args[0], n.Args[1]); err != nil {
					return err
				}
			}
		}
		return nil
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

// refuseLiteralAmong refuses a GREATEST/LEAST whose arguments put a quoted
// literal in the same call as numeric operands that cannot read it.
//
// It folds ONE common type over EVERY argument first, because that is what
// PostgreSQL does: select_common_type resolves the call's type and the
// unknown-typed literal is coerced to THAT, not to whichever argument it
// happens to be compared against. Verified live on postgres:17-alpine —
// `GREATEST(bigint, 'abc')` names bigint, `GREATEST(bigint, 'abc', numeric)`
// names numeric, `GREATEST(real, 'abc', bigint)` names real. Refusing
// pairwise named the first column instead, which is a different type in the
// message for the same query, and for a literal only SOME types refuse
// ('3.1' is a numeric and not a bigint) it would refuse a query PostgreSQL
// ANSWERS: `GREATEST(k, '3.1', d)` folds to numeric there.
//
// Any argument whose type this scope cannot prove takes the whole refusal
// away, for that reason — the fold would be a lower bound, and a lower bound
// is exactly what produces a false positive. That is the binder's standing
// contract (validate.go): a false positive breaks a working query, a false
// negative leaves the refusal where the runtime already has it.
func refuseLiteralAmong(scope *colScope, args []plansql.Node) error {
	common, kind := foldArgTypes(scope, args)
	if kind != argTyped {
		return nil
	}
	for _, b := range args {
		lit, ok := unwrapParens(b).(*plansql.Lit)
		if !ok || lit.Kind != plansql.LitString {
			continue
		}
		if err := refuseLiteralForType(common, lit.Value); err != nil {
			return err
		}
	}
	return nil
}

// foldArgTypes folds a composite's arms to the one type select_common_type
// resolves for it, and reports argUntyped when any arm's type this scope
// cannot prove — a partial fold is a LOWER BOUND on PostgreSQL's, and a lower
// bound is what refuses a literal PostgreSQL reads.
func foldArgTypes(scope *colScope, args []plansql.Node) (parquet.TypeID, argKind) {
	var common parquet.TypeID
	have := false
	for _, a := range args {
		typ, kind := argDeclaredType(scope, a)
		switch kind {
		case argUntyped:
			return 0, argUntyped
		case argUnknownLiteral:
			continue
		}
		if !have {
			common, have = typ, true
			continue
		}
		w, ok := setOpWiden(common, typ)
		if !ok {
			return 0, argUntyped
		}
		common = w
	}
	if !have {
		return 0, argUntyped
	}
	return common, argTyped
}

// argKind classifies one call argument for the common-type fold.
type argKind int

const (
	// argTyped: the type came back and is usable.
	argTyped argKind = iota
	// argUnknownLiteral: a QUOTED string literal, which PostgreSQL types FROM
	// the fold rather than contributing to it — and a NULL, which is
	// unknown-typed the same way.
	argUnknownLiteral
	// argUntyped: this scope cannot say what the argument is.
	argUntyped
)

// argDeclaredType is one argument's contribution to select_common_type: a
// provable column's declared type, a NUMERIC constant's own type, or nothing.
//
// PostgreSQL types an unsuffixed constant `numeric` once it carries a decimal
// point or an exponent, and otherwise as the narrowest integer type that holds
// it — ADR-0012 item 12's rule for a set-operation literal arm, which is the
// same select_common_type this folds through.
func argDeclaredType(scope *colScope, n plansql.Node) (parquet.TypeID, argKind) {
	switch v := unwrapParens(n).(type) {
	case *plansql.FuncCallNode:
		// A COMPOSITE is typed by the fold over its arms, which is what the
		// literal beside it is coerced to. Without this the refusal needed a
		// BARE COLUMN on one side, so `COALESCE(numeric, float8) = 'abc'` had
		// no plan-time refusal at all and fell to the runtime — where it
		// depended on which arm each row supplied, and on there BEING a row:
		// `WHERE id > 100 AND COALESCE(a, f) = 'abc'` answered zero rows where
		// PostgreSQL raises 22P02, because the coercion happens at parse
		// analysis and does not wait for data.
		switch strings.ToLower(v.Name) {
		case "greatest", "least", "coalesce", "ifnull":
			return foldArgTypes(scope, v.Args)
		case "nullif":
			// NULLIF's result mirrors argument 0, and its comparison resolves
			// over both — the same fold either way for a two-argument call.
			return foldArgTypes(scope, v.Args)
		}
		return 0, argUntyped
	case *plansql.CaseNode:
		// A CASE answers with one of its RESULTS; the WHEN conditions and a
		// simple CASE's subject only steer.
		arms := make([]plansql.Node, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return foldArgTypes(scope, arms)
	case *plansql.ColRef:
		typ, ok := scope.provableColType(v)
		if !ok {
			return 0, argUntyped
		}
		return typ, argTyped
	case *plansql.Lit:
		switch v.Kind {
		case plansql.LitString, plansql.LitNull:
			return 0, argUnknownLiteral
		case plansql.LitNumber:
			if strings.ContainsAny(v.Value, ".eE") {
				return parquet.TypeDecimal, argTyped
			}
			if i, err := strconv.ParseInt(strings.TrimSpace(v.Value), 10, 64); err == nil {
				if i >= math.MinInt32 && i <= math.MaxInt32 {
					return parquet.TypeInt32, argTyped
				}
				return parquet.TypeInt64, argTyped
			}
			return parquet.TypeDecimal, argTyped
		}
	}
	return 0, argUntyped
}

// refuseLiteralAgainstColumn raises when colSide is a column this scope can
// prove has a type with a "this string names no value of me" rule (the
// numeric family — see refuseLiteralForType) and litSide is a quoted string
// that type's own input grammar rejects.
//
// Only a QUOTED literal is examined. An UNQUOTED numeric constant is not
// unknown-typed — PostgreSQL resolves `real = 1e40` as a double comparison
// that answers no rows, and `d = 1e400` as a numeric read at the column's
// scale, saturating past the carrier (ADR-0012 item 6) — so neither is ever
// refused here. A NULL or a boolean is a different rule and is left to the
// comparison. The text test is `expr.RefuseNumericLiteral`, the SAME predicate
// the runtime refusals use, so the two paths cannot disagree about which
// strings name a value of which type.
func refuseLiteralAgainstColumn(scope *colScope, colSide, litSide plansql.Node) error {
	lit, ok := unwrapParens(litSide).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return nil
	}
	typ, kind := argDeclaredType(scope, colSide)
	if kind != argTyped {
		return nil
	}
	return refuseLiteralForType(typ, lit.Value)
}

// refuseLiteralForType raises when text names no value of a column type that
// has a plan-time literal rule.
//
// It is the WHOLE numeric family now — DECIMAL (#517), the integer types
// (#536) and the FLOAT types (#646) — through the one predicate
// expr.RefuseNumericLiteral, which is kernel.QuotedLitStatus, which is what
// the vectorized kernel, the row-at-a-time evaluator and the boxed sites all
// read. The plan-time refusal and the runtime one CANNOT disagree about which
// strings name a value, because they are the same function; that identity is
// the property, not the coverage.
//
// The rule is per type because PostgreSQL's input functions are:
//
//	'3.1'    bigint 22P02   real 3.1     numeric 3.1
//	'1_000'  bigint 1000    real 22P02   numeric 1000 (wadjet: 22P02, #634)
//	'0x1p3'  bigint 22P02   real 8       numeric 16   (wadjet: 22P02, #634)
//	'NaN'    bigint 22P02   real NaN     numeric NaN-as-a-bound (ADR-0024 item 6)
//	'1e400'  bigint 22P02   real 22003   numeric a very large number
//
// all verified live on postgres:17-alpine. A range failure is 22003, a
// different SQLSTATE with different wording, so the error type carries the
// distinction rather than collapsing it.
//
// The NETWORK types (CIDR/IPv4/IPv6/MAC/UUID) are NOT wired here yet:
// wadjet's network literal parsers (net.ParseCIDR, net.ParseMAC, the
// brace-unaware UUID parser) are STRICTER than PostgreSQL's input grammar —
// they reject abbreviated cidr/inet ('192.168', '10/8'), several macaddr
// notations ('08002b:010203', '0800-2b01-0203') and the brace/no-dash/
// uppercase UUID forms that PostgreSQL ACCEPTS. Refusing on those parsers
// here would raise 22P02 for input PostgreSQL answers — a PG-superset
// regression the binder must never make (ADR-0012 item 1: never refuse what
// PostgreSQL accepts), net-new at the boxed sites (GREATEST/LEAST, simple
// CASE, IN, IS DISTINCT FROM) that had no refusal before. That
// over-strictness is a latent RUNTIME bug too (exec.networkConstError refuses
// the same PG-valid forms data-dependently), and both halves are deferred to
// #627: widen the parsers to a SUPERSET of PostgreSQL's grammar first, then a
// network arm can be added here using that same predicate without ever
// refusing a PG-valid literal.
//
// Types with no rule return nil: a legal comparison must still work, and the
// binder refuses only what PostgreSQL refuses.
func refuseLiteralForType(typ parquet.TypeID, text string) error {
	// A network PREFIX met by a bare-address column is 0A000 and not a syntax
	// error, so it is asked first: `'10/8'` IS valid inet text, and calling it
	// invalid input would be the wrong claim about PostgreSQL's own grammar
	// (#627 round 2, B1).
	if err := expr.RefuseNetworkPrefixLiteral(typ, text); err != nil {
		return err
	}
	return expr.RefuseNumericLiteral(typ, text)
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
