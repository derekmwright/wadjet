// SPDX-License-Identifier: MIT

package logical

import (
	"math"
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A CORRELATED LATERAL'S BOUND IS PER OUTER ROW, AND A PER-KEY TOP-N IS HOW
// THE DECORRELATION KEEPS IT — #1019, ADR-0021 §1s.
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
// which makes the body ONE relation joined once — and the bound then applied
// to the whole of it, so the statement answered a single row, silently, on
// every arm and in every spelling of the consumer.
//
// THE RULE IS KEY-PARTITIONABILITY (ADR-0021 §1s). The join is exact when the
// body's result restricted to one outer key equals the body evaluated for that
// key, and a bound is a pipeline breaker that commutes with that restriction
// only when it is a PER-KEY bound. So when every correlated predicate is an
// equality naming an inner column — the keys K the join partitions by — the
// bound travels with K: `ROW_NUMBER() OVER (PARTITION BY K ORDER BY <the
// body's own ORDER BY>)` numbers each key's rows in the body's order and a
// QUALIFY over that number is the bound. The join then matches each outer row
// to its own key's surviving rows.
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
// An ORDINAL term names the select item at that position.
//
// **The partition is spelled against what the window's INPUT carries**, which
// is the rule `respellKeyRefsToSlot` states for HAVING and the body's own
// ORDER BY. Over a plain body the window sits above the scan and the inner
// column is there under its own name; over an AGGREGATED one it sits above the
// aggregate, which publishes the correlation key under the name the join keys
// on — a hidden `__key_N` where the lowering minted one, the list's own alias
// where it published one, the source column otherwise.
//
// WHAT IS REFUSED, AND WHY EACH IS THE SAME RULE. Where there is no K the
// bound cannot be made per key, and there is no per-row runner for a
// relation-valued body — so the shape is refused, 0A000, on every arm, in
// place of the plausible wrong row set it used to answer (39 cells of the arc
// LT seam table, `lateralBoundRefusal`):
//
//   - a correlated predicate that is not an equality on an inner column —
//     `i.v > o.total`, or `i.k = o.k AND i.v > o.total`: the bound is over
//     the rows the OUTER value selects, which differ per outer row;
//   - a bound this pass cannot read as a non-negative integer constant: a
//     rewrite needs `m+n`, and guessing at an expression would put a number
//     in the plan the query does not contain;
//   - a DISTINCT body, or a set operation: the bound applies to what those
//     operators PRODUCE, and the QUALIFY filter runs below both.
//
// An UNCORRELATED body is untouched: there is no decorrelation, the body is
// evaluated once, and its own bound means exactly what it says. So is a bound
// that cannot change any answer (`OFFSET 0`, no LIMIT).
//
// The structural closure of the refused shapes is a DEPENDENT JOIN — the body
// re-run per outer row with the outer values substituted, the way the scalar
// rerun does — recorded with its mechanism in arc LT's notes.
func lateralBoundPerOuterRow(info *plansql.SelectInfo, correlatedParts []string,
	leftAliases map[string]bool, aggregates bool, keyRename map[string]string, injectedLead int) error {
	if info == nil || len(correlatedParts) == 0 {
		return nil
	}
	limit, hasLimit, okL := lateralConstBound(info.Limit, true)
	offset, hasOffset, okO := lateralConstBound(info.Offset, false)
	if !hasLimit && !hasOffset && okL && okO {
		return nil
	}
	if !hasLimit && hasOffset && offset == 0 && okL {
		return nil
	}
	if !okL || !okO {
		return lateralBoundRefusal(info, "its LIMIT/OFFSET is not a non-negative integer constant")
	}
	// `LIMIT 0` is empty for every outer row and for the whole relation
	// alike, so the body keeps it as written (round-2 review, B5: the
	// inequality-correlated and the DISTINCT `LIMIT 0` bodies were right on
	// five arms before the rewrite refused them).
	if hasLimit && limit == 0 {
		return nil
	}
	if info.Union != nil {
		return lateralBoundRefusal(info, "the body is a set operation")
	}
	// The body's OWN QUALIFY runs in the same window layer as the rank this
	// rewrite mints, so the rank would number the rows BEFORE the QUALIFY
	// removed any — `QUALIFY rn > 1 … LIMIT 1` became `rn > 1 AND rn <= 1`,
	// zero rows for PostgreSQL's equivalent three (round-2 review, B1).
	// Numbering the rows the QUALIFY leaves needs a second window layer
	// above the first, which a single block cannot express; refused.
	if info.QualifyExpr != nil || strings.TrimSpace(info.Qualify) != "" {
		return lateralBoundRefusal(info, "the body carries its own QUALIFY, and the bound would number the rows before that clause removes any")
	}
	// THE KEY RULE, checked on the parsed predicate and not on its text
	// (round-2 review, B6): every correlated part must be `<inner column> =
	// <outer column>` — one side a bare column of the body's own relations,
	// the other a bare column of the enclosing query. An opposite side that
	// mixes inner and outer references (`i.k = o.k + i.id - 3`) is not a
	// restriction on an inner column at all, and the partition it would name
	// is not the one PostgreSQL evaluates per outer row; an outer EXPRESSION
	// (`o.k + 0`) is a join key this engine does not yet bind (it answered
	// zero rows with or without the bound; recorded), so it is refused here
	// rather than partitioned on a guess.
	parts := make([]plansql.Node, 0, len(correlatedParts))
	for _, cp := range correlatedParts {
		innerCol, ok := lateralEqualityKey(cp, leftAliases)
		if !ok {
			return lateralBoundRefusal(info, "its correlated predicate "+
				sqlerr.Quote(strings.TrimSpace(cp))+" is not `<inner expression> = <outer column>`, so there is no key to partition the bound by")
		}
		term := strings.TrimSpace(innerCol)
		if aggregates {
			if pub, ok := keyRename[strings.ToLower(term)]; ok && pub != "" {
				term = pub
			}
		}
		ref, err := plansql.ParseExpression(term)
		if err != nil || ref == nil {
			return lateralBoundRefusal(info, "its correlation key "+sqlerr.Quote(term)+" cannot be read as a column")
		}
		parts = append(parts, ref)
	}
	if info.Distinct {
		// A DISTINCT over EXACTLY the correlation key yields at most one row
		// per key, so `LIMIT n` (n >= 1, no OFFSET) is the identity and the
		// bound is dropped; any other bound over a DISTINCT body is refused,
		// because the QUALIFY filter runs below the DISTINCT (round-2 review,
		// B5/B6).
		if lateralDistinctOverKeyOnly(info, parts) && hasLimit && limit >= 1 && (!hasOffset || offset == 0) {
			info.Limit, info.Offset, info.OrderBy = "", "", nil
			return nil
		}
		return lateralBoundRefusal(info, "the body carries DISTINCT")
	}
	order, err := lateralWindowOrder(info, injectedLead)
	if err != nil {
		return err
	}
	rank := &plansql.WindowFuncNode{
		Func:        &plansql.FuncCallNode{Name: "row_number"},
		PartitionBy: parts,
		OrderBy:     order,
	}
	var pred plansql.Node
	if hasOffset && offset > 0 {
		pred = &plansql.CmpExpr{Left: rank, Op: ">", Right: &plansql.Lit{Value: strconv.FormatInt(offset, 10), Kind: plansql.LitNumber}}
	}
	if hasLimit {
		// SATURATED: `LIMIT 9223372036854775807 OFFSET 1` is a valid bound
		// whose sum wraps below zero (round-2 review, B3).
		upper := offset + limit
		if upper < offset {
			upper = math.MaxInt64
		}
		up := &plansql.CmpExpr{Left: rank, Op: "<=",
			Right: &plansql.Lit{Value: strconv.FormatInt(upper, 10), Kind: plansql.LitNumber}}
		if pred == nil {
			pred = up
		} else {
			pred = &plansql.AndNode{Left: pred, Right: up}
		}
	}
	info.QualifyExpr = pred
	info.Qualify = pred.String()
	info.Limit, info.Offset, info.OrderBy = "", "", nil
	return nil
}

// lateralEqualityKey reads a correlated part as `<inner expression> = <outer
// column>` and returns the inner side's text. The OUTER side must be a bare
// column of the enclosing query; the INNER side may be any expression over the
// body's own relations alone (`i.k + 0`, `i.k % 2` partition correctly on
// every arm) — a side that mixes inner and outer references, an inequality,
// or an outer EXPRESSION is not a key this rewrite may partition by.
func lateralEqualityKey(cp string, leftAliases map[string]bool) (string, bool) {
	node, err := plansql.ParseExpression(cp)
	if err != nil || node == nil {
		return "", false
	}
	cmp, ok := node.(*plansql.CmpExpr)
	if !ok || cmp.Op != "=" {
		return "", false
	}
	outerCol := func(n plansql.Node) bool {
		ref, ok := n.(*plansql.ColRef)
		return ok && ref.Table != "" && leftAliases[strings.ToLower(ref.Table)]
	}
	innerOnly := func(n plansql.Node) bool {
		refs, err := plansql.ColumnRefs(n)
		if err != nil || len(refs) == 0 {
			return false
		}
		for _, r := range refs {
			if r.Table != "" && leftAliases[strings.ToLower(r.Table)] {
				return false
			}
		}
		return true
	}
	switch {
	case outerCol(cmp.Left) && innerOnly(cmp.Right):
		return cmp.Right.String(), true
	case outerCol(cmp.Right) && innerOnly(cmp.Left):
		return cmp.Left.String(), true
	}
	return "", false
}

// lateralDistinctOverKeyOnly reports whether a DISTINCT body's list is
// exactly the correlation key(s) — one row per key at most.
func lateralDistinctOverKeyOnly(info *plansql.SelectInfo, keys []plansql.Node) bool {
	want := map[string]bool{}
	for _, k := range keys {
		if ref, ok := k.(*plansql.ColRef); ok {
			want[strings.ToLower(ref.Column)] = true
		}
	}
	if len(want) == 0 {
		return false
	}
	for _, c := range info.Columns {
		if c.Star || c.IsAgg || c.IsWindow || c.ASTExpr == nil {
			return false
		}
		ref, ok := c.ASTExpr.(*plansql.ColRef)
		if !ok || !want[strings.ToLower(ref.Column)] {
			return false
		}
	}
	return true
}

// lateralBoundRefusal is the one sentence every refused bound carries.
func lateralBoundRefusal(info *plansql.SelectInfo, why string) error {
	bound := "LIMIT " + strings.TrimSpace(info.Limit)
	if strings.TrimSpace(info.Limit) == "" {
		bound = "OFFSET " + strings.TrimSpace(info.Offset)
	} else if strings.TrimSpace(info.Offset) != "" {
		bound += " OFFSET " + strings.TrimSpace(info.Offset)
	}
	return sqlerr.New("0A000",
		"a correlated LATERAL body bounded by %s is evaluated per outer row by PostgreSQL, "+
			"and this engine cannot apply that bound per outer row here: %s. The correlation "+
			"is evaluated as a join and the bound travels with an EQUALITY key only — "+
			"correlate on an equality with an inner column, or move the bound outside the LATERAL",
		bound, why)
}

// lateralWindowOrder carries the body's own ORDER BY into the window's.
//
// The parsed term is preferred over its text for the reason #320 gives: a
// sort term that is not a plain column reference can only be honoured by
// evaluating it, and the tree is what evaluates. An ORDINAL names the select
// item at that position IN THE LIST THE QUERY WROTE: the lowering has already
// injected `injectedLead` correlation slots at the front of info.Columns, so
// the position is read past them (measured: `ORDER BY 1 DESC` numbered by the
// injected key and kept each key's SMALLEST value).
func lateralWindowOrder(info *plansql.SelectInfo, injectedLead int) ([]plansql.WindowOrderBy, error) {
	var out []plansql.WindowOrderBy
	user := info.Columns
	if injectedLead > 0 && injectedLead <= len(user) {
		user = user[injectedLead:]
	}
	for _, ob := range info.OrderBy {
		term := ob.Expr
		if term == nil {
			parsed, err := plansql.ParseExpression(strings.TrimSpace(ob.Column))
			if err != nil || parsed == nil {
				return nil, lateralBoundRefusal(info, "its ORDER BY term "+sqlerr.Quote(ob.Column)+" cannot be read")
			}
			term = parsed
		}
		ordinal := ob.Ordinal
		if ordinal == 0 {
			// The parser records a position only where it can resolve it
			// against a list it has already expanded; a body's `ORDER BY 1`
			// arrives here as the literal, which a window would read as a
			// CONSTANT key and order nothing by (measured: `ORDER BY 1 DESC
			// LIMIT 1` kept each key's SMALLEST value).
			if lit, ok := term.(*plansql.Lit); ok && lit.Kind == plansql.LitNumber {
				if n, err := strconv.Atoi(strings.TrimSpace(lit.Value)); err == nil && n > 0 {
					ordinal = n
				}
			}
		}
		// A SELECT-list ALIAS (`… i.v * -1 AS v … ORDER BY v`) names the item's
		// EXPRESSION, which PostgreSQL resolves before any input column of
		// that name; the window reads its input, where the alias does not
		// exist yet, so it read the source `v` instead (round-2 review, B2).
		if ref, ok := term.(*plansql.ColRef); ok && ref.Table == "" && ordinal == 0 {
			for _, col := range user {
				if col.Alias != "" && strings.EqualFold(col.Alias, ref.Column) {
					if col.Star || col.IsAgg && !info.Distinct && false {
						break
					}
					if col.ASTExpr != nil {
						term = col.ASTExpr
					}
					break
				}
			}
		}
		if ordinal > 0 {
			if ordinal > len(user) {
				return nil, sqlerr.New("42P10", "ORDER BY position %d is not in select list", ordinal)
			}
			col := user[ordinal-1]
			switch {
			case col.Star:
				return nil, lateralBoundRefusal(info, "its ORDER BY names a star item by position")
			case col.ASTExpr != nil:
				term = col.ASTExpr
			default:
				parsed, err := plansql.ParseExpression(col.Expr)
				if err != nil || parsed == nil {
					return nil, lateralBoundRefusal(info, "its ORDER BY position "+strconv.Itoa(ordinal)+" cannot be read")
				}
				term = parsed
			}
		}
		out = append(out, plansql.WindowOrderBy{Expr: term, Desc: ob.Desc, NullsFirst: ob.NullsFirst})
	}
	return out, nil
}

// lateralConstBound reads a LIMIT or OFFSET as a non-negative integer.
//
// It reports (value, present, readable). An absent clause is absent; anything
// this cannot read as a non-negative integer constant is UNREADABLE, which is
// a refusal above and never a zero. (`LIMIT ALL` does not parse at all.)
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

// lateralBoundRemovesTheOneRow reports whether a body's bound leaves NO row
// of a one-row body: `LIMIT 0`, or an OFFSET of one or more.
func lateralBoundRemovesTheOneRow(info *plansql.SelectInfo) bool {
	limit, hasLimit, okL := lateralConstBound(info.Limit, true)
	offset, hasOffset, okO := lateralConstBound(info.Offset, false)
	if !okL || !okO {
		return false
	}
	return (hasLimit && limit == 0) || (hasOffset && offset >= 1)
}
