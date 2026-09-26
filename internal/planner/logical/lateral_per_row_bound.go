// SPDX-License-Identifier: MIT

package logical

import (
	"math"
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// lateralBoundPerOuterRow preserves a correlated bound with a per-key
// ROW_NUMBER and QUALIFY: rn > offset and rn <= offset+limit, saturating
// the upper sum. Keys require an inner-only expression equal to an
// outer-only expression. ORDER BY aliases and ordinals resolve to the selected items.
// An uncorrelated body and a non-removing bound need no rewrite; LIMIT 0
// stays empty. DISTINCT over exactly the keys needs no positive limit.
// Other DISTINCT bodies, set operations, QUALIFY and unsupported correlations
// refuse 0A000. See ADR-0021 §1s and
// docs/internals/lateral-per-outer-row-bound.md.
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
	// (round-2 review, B6): every correlated part must be `<inner expression>
	// = <outer expression>` — one side over the body's own relations alone,
	// the other over the enclosing query's alone. An opposite side that mixes
	// inner and outer references (`i.k = o.k + i.id - 3`) is not a
	// restriction on an inner column at all, and the partition it would name
	// is not the one PostgreSQL evaluates per outer row. An outer EXPRESSION
	// (`o.k + 0`) is: the rows one outer row may match are the inner rows
	// whose key equals ONE value, so the per-key bound over the inner side is
	// the per-row bound (ADR-0021 §1s; #1302 closed the join it rides).
	parts := make([]plansql.Node, 0, len(correlatedParts))
	for _, cp := range correlatedParts {
		innerCol, ok := lateralEqualityKey(cp, leftAliases)
		if !ok {
			return lateralBoundRefusal(info, "its correlated predicate "+
				sqlerr.Quote(strings.TrimSpace(cp))+" is not `<inner expression> = <outer expression>`, so there is no key to partition the bound by")
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
// expression>` and returns the inner side's text (lateralCorrelatedEquality).
func lateralEqualityKey(cp string, leftAliases map[string]bool) (string, bool) {
	inner, _, ok := lateralCorrelatedEquality(cp, leftAliases)
	if !ok {
		return "", false
	}
	return inner.String(), true
}

// lateralCorrelatedEquality splits a correlated part into its INNER side — any
// expression over the body's own relations alone (`i.k`, `i.k + 0`, `i.k % 2`;
// a bare reference is the body's, which is how SQL scopes it) — and its OUTER
// side, an expression every reference of which names the enclosing query
// (`o.k`, `o.k - 0`, `CAST(o.k AS text)`, `o.a + o.b`). A side that mixes the
// two, or an inequality, is not one: it restricts no inner column to a value
// fixed by the outer row.
//
// THE ONE RULE for one predicate shape (#1302): the bounded rewrite partitions
// by the inner side, and the unbounded lowering keys the join on it — where
// the outer side is a bare column the pair is a hash key, and where it is an
// expression the equality is evaluated over the join's output, exactly as an
// ordinary `JOIN … ON i.k = o.k - 0` is (buildLateralSubquery emits the key
// slot for that read).
func lateralCorrelatedEquality(cp string, leftAliases map[string]bool) (inner, outer plansql.Node, ok bool) {
	node, err := plansql.ParseExpression(cp)
	if err != nil || node == nil {
		return nil, nil, false
	}
	for {
		p, isParen := node.(*plansql.ParenNode)
		if !isParen {
			break
		}
		node = p.Inner
	}
	cmp, isCmp := node.(*plansql.CmpExpr)
	if !isCmp || cmp.Op != "=" {
		return nil, nil, false
	}
	side := func(n plansql.Node, wantOuter bool) bool {
		refs, err := plansql.ColumnRefs(n)
		if err != nil || len(refs) == 0 {
			return false
		}
		for _, r := range refs {
			isOuter := r.Table != "" && leftAliases[strings.ToLower(r.Table)]
			if isOuter != wantOuter {
				return false
			}
		}
		return true
	}
	switch {
	case side(cmp.Left, true) && side(cmp.Right, false):
		return cmp.Right, cmp.Left, true
	case side(cmp.Right, true) && side(cmp.Left, false):
		return cmp.Left, cmp.Right, true
	}
	return nil, nil, false
}

// lateralOuterSideIsColumn reports whether a correlated equality's OUTER side
// is a bare column — the only shape the join can key on by name. Anything else
// (an outer expression, or a part lateralCorrelatedEquality does not read) is
// evaluated over the join's output.
func lateralOuterSideIsColumn(cp string, leftAliases map[string]bool) bool {
	_, outer, ok := lateralCorrelatedEquality(cp, leftAliases)
	if !ok {
		return false
	}
	for {
		p, isParen := outer.(*plansql.ParenNode)
		if !isParen {
			break
		}
		outer = p.Inner
	}
	_, isRef := outer.(*plansql.ColRef)
	return isRef
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

// lateralEnclosingBareStar reports whether the enclosing SELECT list writes an
// UNQUALIFIED star — the one spelling that can publish a LATERAL join's
// stream whole (where the arms' lists cannot expand it; see
// RefuseStarPublishingLiftedSlot).
func lateralEnclosingBareStar(outer *plansql.SelectInfo) bool {
	if outer == nil {
		return false
	}
	for _, c := range outer.Columns {
		if c.Star && c.TableRef == "" {
			return true
		}
	}
	return false
}

// lateralStarHint is the qualifier the refusal suggests a star under.
func lateralStarHint(alias string) string {
	if alias == "" {
		return "<lateral alias>"
	}
	return alias
}
