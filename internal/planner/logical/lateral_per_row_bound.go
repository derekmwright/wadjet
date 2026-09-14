package logical

import (
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A CORRELATED LATERAL'S BOUND IS PER OUTER ROW, AND A PER-KEY TOP-N IS HOW
// THE DECORRELATION KEEPS IT — #1019, ADR-0021 §1h's next layer.
//
// PostgreSQL evaluates a LATERAL body once per outer row, so its `ORDER BY …
// LIMIT n` bounds EACH evaluation:
//
//	SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.product, i.amount
//	  FROM lat_item i WHERE i.order_id = o.id
//	  ORDER BY i.amount DESC LIMIT 1) s ON true
//	-- PostgreSQL 17.11: one row per order — 1,Gadget,100 and 2,Doohickey,125
//
// The decorrelation promotes `i.order_id = o.id` into the JOIN condition,
// which makes the body ONE relation joined once — and the bound then applies
// to the whole of it, so the statement answered a single row, silently, on
// every arm and in every spelling of the consumer. `LIMIT n`, `OFFSET`, both
// together and the grouped body are the same defect; `lateralBoundIsNotPerOuterRow`
// recorded it as a mark rather than a refusal because whether a bound BINDS is
// a property of the data.
//
// **The bound travels with the correlation key.** `ROW_NUMBER() OVER
// (PARTITION BY <the inner column the correlation keys on> ORDER BY <the body's
// own ORDER BY>)` numbers each outer key's rows in the body's order, and a
// QUALIFY over that number is the bound — which is what the clause is for, and
// the reason #1076 is this arc's first commit rather than an unrelated one.
// The join then matches each outer row to its own key's surviving rows.
//
//	OFFSET m LIMIT n  ->  QUALIFY rn > m AND rn <= m+n
//	OFFSET m          ->  QUALIFY rn > m
//	LIMIT n           ->  QUALIFY rn <= n
//
// The body's `ORDER BY` is CONSUMED by the window: a FROM item's row order is
// not preserved by SQL, so the clause's only effect was to decide which rows
// the bound keeps, and that is exactly what the window's ORDER BY now decides.
// With no ORDER BY the surviving row is arbitrary, which is what PostgreSQL
// answers for an unordered `LIMIT 1` too (ADR-0013's nondeterminism classes).
//
// **What it DECLINES, and why each decline is a different rule and not a
// missing case.** In each of these the plan keeps the disposition it had —
// `Node.LateralBoundNotPerRow` and its pinned row count:
//
//   - an UNCORRELATED body. There is no decorrelation, the body is evaluated
//     once, and its own bound means exactly what it says.
//   - a bound that cannot change any answer — no LIMIT and no OFFSET,
//     `LIMIT ALL`, `OFFSET 0` — because the two forms then agree already.
//   - a bound this pass cannot read as a non-negative integer constant. A
//     rewrite needs `m+n`, and guessing at an expression would put a number in
//     the plan the query does not contain.
//   - a correlation whose inner column an equality does not name. The
//     partition IS the correlation key; without one there is no key to
//     partition by, and partitioning by something else is a different query.
//   - a body that is a SET OPERATION, or one carrying DISTINCT. The bound
//     applies to what those operators PRODUCE, and the QUALIFY filter runs
//     below both.
//
// **The partition is spelled against what the window's INPUT carries**, which
// is the rule `respellKeyRefsToSlot` states for HAVING and the body's own
// ORDER BY. Over a plain body the window sits above the scan and the inner
// column is there under its own name; over an AGGREGATED one it sits above the
// aggregate, which publishes the correlation key under the name the join keys
// on — a hidden `__key_N` where the lowering minted one. Written as the source
// column there, the operator refused: `PARTITION BY "i.order_id" is not a
// column of its input (input has: product, __key_0, m)`.
func lateralBoundPerOuterRow(info *plansql.SelectInfo, correlatedParts []string,
	leftAliases map[string]bool, aggregates bool, keyRename map[string]string) bool {
	if info == nil || len(correlatedParts) == 0 || info.Union != nil || info.Distinct {
		return false
	}
	limit, hasLimit, okL := lateralConstBound(info.Limit, true)
	offset, hasOffset, okO := lateralConstBound(info.Offset, false)
	if !okL || !okO || (!hasLimit && !hasOffset) {
		return false
	}
	if !hasLimit && offset == 0 {
		return false
	}
	parts := make([]plansql.Node, 0, len(correlatedParts))
	for _, cp := range correlatedParts {
		innerCol := extractInnerColumn(cp, leftAliases)
		if innerCol == "" {
			return false
		}
		term := strings.TrimSpace(innerCol)
		if aggregates {
			pub, ok := keyRename[strings.ToLower(term)]
			if !ok || pub == "" {
				return false
			}
			term = pub
		}
		ref, err := plansql.ParseExpression(term)
		if err != nil || ref == nil {
			return false
		}
		parts = append(parts, ref)
	}
	rank := &plansql.WindowFuncNode{
		Func:        &plansql.FuncCallNode{Name: "row_number"},
		PartitionBy: parts,
		OrderBy:     lateralWindowOrder(info.OrderBy),
	}
	var pred plansql.Node
	if hasOffset && offset > 0 {
		pred = &plansql.CmpExpr{Left: rank, Op: ">", Right: &plansql.Lit{Value: strconv.FormatInt(offset, 10), Kind: plansql.LitNumber}}
	}
	if hasLimit {
		upper := &plansql.CmpExpr{Left: rank, Op: "<=",
			Right: &plansql.Lit{Value: strconv.FormatInt(offset+limit, 10), Kind: plansql.LitNumber}}
		if pred == nil {
			pred = upper
		} else {
			pred = &plansql.AndNode{Left: pred, Right: upper}
		}
	}
	if info.QualifyExpr != nil {
		pred = &plansql.AndNode{Left: info.QualifyExpr, Right: pred}
	}
	info.QualifyExpr = pred
	info.Qualify = pred.String()
	info.Limit, info.Offset, info.OrderBy = "", "", nil
	return true
}

// lateralWindowOrder carries the body's own ORDER BY into the window's.
//
// The parsed term is preferred over its text for the reason #320 gives: a
// sort term that is not a plain column reference can only be honoured by
// evaluating it, and the tree is what evaluates.
func lateralWindowOrder(items []plansql.OrderByItem) []plansql.WindowOrderBy {
	var out []plansql.WindowOrderBy
	for _, ob := range items {
		term := ob.Expr
		if term == nil {
			parsed, err := plansql.ParseExpression(strings.TrimSpace(ob.Column))
			if err != nil || parsed == nil {
				continue
			}
			term = parsed
		}
		out = append(out, plansql.WindowOrderBy{Expr: term, Desc: ob.Desc, NullsFirst: ob.NullsFirst})
	}
	return out
}

// lateralConstBound reads a LIMIT or OFFSET as a non-negative integer.
//
// It reports (value, present, readable). `LIMIT ALL` and an absent clause are
// absent; anything this cannot read as a non-negative integer constant is
// UNREADABLE, which is a decline and not a zero — `limitCanBind` assumes such
// a bound binds, and the two have to agree about which spellings they can see.
func lateralConstBound(text string, isLimit bool) (int64, bool, bool) {
	t := strings.TrimSpace(text)
	if t == "" {
		return 0, false, true
	}
	if isLimit && strings.EqualFold(t, "all") {
		return 0, false, true
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 {
		return 0, false, false
	}
	return n, true, true
}
