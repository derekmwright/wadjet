package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// This file holds the two analyses that decide where a join's predicates may
// legally live: which of a join's inputs can be NULL-padded (so a WHERE
// predicate must not be pushed below it unexamined), and which ON-clause
// conjuncts the physical planner's key parser is able to represent at all.
//
// Both existed only implicitly before, and both were wrong:
//
//	#335 — pushFilterThroughJoin pushed a WHERE predicate to whichever join
//	       child owned its columns, without looking at the join type. Over a
//	       LEFT JOIN that pushed the predicate below the NULL-padding, so
//	       `... LEFT JOIN region r ON ... WHERE r.r_regionkey = 2` filtered
//	       region to one row and then padded every unmatched nation back in:
//	       25 rows out of a 5-row answer, and 25 again for the IS NULL
//	       anti-join idiom whose answer is 0.
//
//	#336 — an ON conjunct comparing two COLUMNS across the join
//	       (`a.s_suppkey < b.s_suppkey`) stayed in JoinCond, where
//	       parseJoinKeys keeps only the parts containing "=" and drops the
//	       rest without a word. The same conjunct in WHERE is honoured, so
//	       ON residuals and WHERE residuals were two paths with two answers.

// joinKind maps the many spellings a join type reaches the logical plan under
// ("", "join", "left join", "LEFT OUTER JOIN", "full outer join") onto the
// canonical short form the rules below switch on. It mirrors
// physical.mapJoinType; logical cannot import physical (the dependency runs
// the other way), and the two must agree.
func joinKind(jt string) string {
	lower := strings.ToLower(strings.TrimSpace(jt))
	switch {
	case strings.Contains(lower, "cross"):
		return "cross"
	case strings.Contains(lower, "full"):
		return "full"
	case strings.Contains(lower, "right"):
		return "right"
	case strings.Contains(lower, "semi"):
		return "semi"
	case strings.Contains(lower, "anti"):
		return "anti"
	case strings.Contains(lower, "left"):
		return "left"
	default:
		return "inner"
	}
}

// nullSupplyingSides reports, for a canonical join kind, which input's columns
// the join can emit as NULL for a row that found no partner. A WHERE predicate
// over a null-supplying side's columns sees those manufactured NULLs, so it
// may only be pushed below the join once it is proven to reject them.
//
// semi/anti are reported as neither: their output carries the probe side's
// columns alone, so no WHERE predicate above them can reference the build
// side, and the probe side is never padded.
func nullSupplyingSides(kind string) (left, right bool) {
	switch kind {
	case "left":
		return false, true
	case "right":
		return true, false
	case "full":
		return true, true
	}
	return false, false
}

// demoteJoinKind returns the join kind that remains once a null-rejecting
// WHERE predicate has eliminated every NULL-padded row on the named side(s).
// This is the outer-join simplification that makes the pushdown legal in the
// first place: a LEFT JOIN whose WHERE rejects the padded NULLs on its right
// input can only ever emit matched rows, which is an inner join, and only
// then may the predicate move below it.
func demoteJoinKind(kind string, demoteLeft, demoteRight bool) string {
	switch kind {
	case "left":
		if demoteRight {
			return "inner"
		}
	case "right":
		if demoteLeft {
			return "inner"
		}
	case "full":
		switch {
		case demoteLeft && demoteRight:
			return "inner"
		case demoteLeft:
			// Right-only rows (left columns NULL) are gone; every left row
			// survives, the right stays optional.
			return "left"
		case demoteRight:
			return "right"
		}
	}
	return kind
}

// rejectsNulls reports whether pred is guaranteed NOT to hold for a row whose
// referenced columns are all NULL — the "null-rejecting" property that lets a
// WHERE predicate collapse an outer join to an inner one.
//
// Callers use it only on predicates whose every column reference resolves to
// ONE side of the join, so "its referenced columns" and "that side's columns"
// are the same set.
//
// It is deliberately one-sided: an expression it cannot prove rejecting is
// treated as tolerant, which costs a pushdown and never a row. `IS NULL` is
// the case that must come out tolerant — it is how an anti-join is spelled,
// and a fix that pushed it down (or demoted the join) would delete the
// unmatched rows the query exists to find.
func rejectsNulls(pred Predicate) bool {
	expr := pred.ASTExpr
	if expr == nil {
		expr = tryParseExpr(pred.Raw)
	}
	return exprRejectsNulls(expr)
}

func exprRejectsNulls(expr plansql.Node) bool {
	switch e := expr.(type) {
	case *plansql.ParenNode:
		return exprRejectsNulls(e.Inner)
	case *plansql.AndNode:
		// One rejecting conjunct is enough: the AND cannot be true.
		return exprRejectsNulls(e.Left) || exprRejectsNulls(e.Right)
	case *plansql.OrNode:
		// Both arms must reject, or the OR may still be true.
		return exprRejectsNulls(e.Left) && exprRejectsNulls(e.Right)
	case *plansql.NotNode:
		// NOT of a NULL is NULL, so a strict operand makes NOT rejecting.
		// (NOT (x IS NULL) is handled here too: IsExpr is not strict, so
		// this falls through to false and the IsExpr case never sees it —
		// conservative, which is the safe direction.)
		return isStrictNull(e.Inner)
	case *plansql.CmpExpr:
		return isStrictNull(e.Left) || isStrictNull(e.Right)
	case *plansql.LikeExpr:
		return isStrictNull(e.Left) || isStrictNull(e.Pattern)
	case *plansql.BetweenExpr:
		return isStrictNull(e.Left) || isStrictNull(e.Low) || isStrictNull(e.High)
	case *plansql.InExpr:
		// NULL IN (...) and NULL NOT IN (...) are both NULL.
		return isStrictNull(e.Left)
	case *plansql.IsExpr:
		if !isStrictNull(e.Left) {
			return false
		}
		switch strings.ToLower(e.Check) {
		case "null":
			// IS NULL holds for the padded row (tolerant, and the whole
			// point of the anti-join idiom); IS NOT NULL rejects it.
			return e.Not
		case "true", "false":
			// NULL IS TRUE and NULL IS FALSE are both false; the negations
			// are both true.
			return !e.Not
		}
		return false
	}
	return false
}

// isStrictNull reports whether expr evaluates to NULL whenever the columns it
// references are NULL — SQL's strictness, which is what makes the comparison
// wrapped around it reject rather than hold.
//
// Function calls are NOT assumed strict: COALESCE(r.x, 0) = 0 is exactly the
// shape that holds for a padded row, and there is no way to tell it from a
// strict call without a per-function property the expression package does not
// expose. Treating them as tolerant loses a pushdown, never a row.
func isStrictNull(expr plansql.Node) bool {
	switch e := expr.(type) {
	case *plansql.ColRef:
		return true
	case *plansql.ParenNode:
		return isStrictNull(e.Inner)
	case *plansql.UnaryOp:
		return isStrictNull(e.Inner)
	case *plansql.BinaryOp:
		// Arithmetic and concatenation propagate NULL through either side.
		return isStrictNull(e.Left) || isStrictNull(e.Right)
	case *plansql.CastNode:
		return isStrictNull(e.Inner)
	}
	return false
}

// liftInnerJoinOnResiduals moves every ON-clause conjunct that is not a
// cross-side equality out of an inner or cross join's condition and into a
// filter above it (#336).
//
// The physical planner represents a join condition as key column pairs:
// parseJoinKeys splits JoinCond on AND, keeps the parts containing "=", and
// discards the rest in silence. `a.s_nationkey = b.s_nationkey AND
// a.s_suppkey < b.s_suppkey` therefore joined on the first conjunct and
// answered as if the second had never been written — 494 rows for a 197-row
// query, which is how self-join deduplication and band joins are spelled.
//
// For an inner join ON and WHERE are interchangeable, so the residual is
// exact above the join, and it lands on the path that already carries WHERE
// residuals correctly. pushdownPredicates, which runs next, then pushes back
// down whatever is single-sided.
//
// Outer joins are left alone: their ON clause is evaluated BEFORE the
// NULL-padding, so a residual moved above the join would delete rows the join
// is required to preserve. There is no equivalent placement for it in the
// current plan vocabulary — the executor has no residual predicate on an
// outer join's probe — so that shape stays broken and is tracked separately.
func liftInnerJoinOnResiduals(n *Node) *Node {
	if n == nil {
		return nil
	}
	// The filter-over-join pair is handled BEFORE recursing, so the join's
	// residual merges into the WHERE above it rather than stacking a second
	// filter node: pushdownPredicates only looks one level down, and a filter
	// sandwiched between the WHERE and the join would strand the WHERE's own
	// conjuncts above it.
	if n.Type == NodeFilter && len(n.Children) == 1 && n.Children[0].Type == NodeJoin {
		if res := takeJoinCondResiduals(n.Children[0]); len(res) > 0 {
			n.Predicates = append(n.Predicates, res...)
		}
	}

	for i, child := range n.Children {
		n.Children[i] = liftInnerJoinOnResiduals(child)
	}

	if n.Type == NodeJoin {
		if res := takeJoinCondResiduals(n); len(res) > 0 {
			return NewFilter(n, res)
		}
	}
	return n
}

// takeJoinCondResiduals strips the non-key conjuncts from an inner/cross
// join's ON clause and returns them as predicates for the caller to place
// above the join. The join keeps only the conjuncts parseJoinKeys can turn
// into a key pair; if that leaves nothing, it becomes an explicit cross join
// and the residuals do the whole of the work.
func takeJoinCondResiduals(join *Node) []Predicate {
	if len(join.Children) != 2 || strings.TrimSpace(join.JoinCond) == "" {
		return nil
	}
	switch joinKind(join.JoinType) {
	case "inner", "cross":
	default:
		return nil
	}

	parts := splitOnAnd(join.JoinCond, strings.ToUpper(join.JoinCond))
	if len(parts) == 0 {
		return nil
	}

	var keyParts []string
	var residuals []Predicate
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		expr := tryParseExpr(part)
		if expr == nil || isEqualityConjunct(expr) {
			// An equality is what parseJoinKeys turns into a key pair, and
			// an unparseable fragment is not something the filter path could
			// evaluate either. Both stay where they are.
			keyParts = append(keyParts, part)
			continue
		}
		residuals = append(residuals, Predicate{Raw: part, ASTExpr: expr})
	}
	if len(residuals) == 0 {
		return nil
	}

	if len(keyParts) > 0 {
		join.JoinCond = strings.Join(keyParts, " AND ")
	} else {
		join.JoinCond = ""
		join.JoinType = "cross"
	}
	return residuals
}

// isEqualityConjunct reports whether a top-level ON conjunct is an equality —
// the only shape parseJoinKeys turns into a key pair, and so the only shape
// that survives being left in JoinCond.
//
// It asks nothing about the operands: which side each column lives on is
// decided later (physical.parseJoinKeys, then FixKeyAssignment), and an
// equality against a literal is already handled — extractJoinCondPredicates
// pushes it to the child that owns it. The point here is only to separate
// "the join can represent this" from "the join will drop this".
func isEqualityConjunct(expr plansql.Node) bool {
	if p, ok := expr.(*plansql.ParenNode); ok {
		return isEqualityConjunct(p.Inner)
	}
	cmp, ok := expr.(*plansql.CmpExpr)
	return ok && cmp.Op == "="
}
