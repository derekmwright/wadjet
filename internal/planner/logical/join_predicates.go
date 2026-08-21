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
//
//	#351 — the same analysis, one level finer. An ON conjunct can BE an
//	       equality and still have no key representation, because the join
//	       executor matches on column NAMES: `n.n_regionkey = r.r_regionkey
//	       + 3` reached it with "r.r_regionkey + 3" as a key column, which
//	       resolves to nothing and matches nothing — 0 rows for a 10-row
//	       query. The residual test is therefore on the OPERANDS, not on
//	       the operator alone.

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
		if expr == nil || isJoinKeyEquality(expr) {
			// An equality between two bare columns is what parseJoinKeys
			// turns into a key pair, and an unparseable fragment is not
			// something the filter path could evaluate either. Both stay
			// where they are.
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

// routeOuterJoinOnResiduals moves every ON-clause conjunct of a LEFT, RIGHT
// or FULL OUTER join that is not a bare-column equality out of JoinCond and
// into JoinFilter — the join's residual predicate, evaluated by the executor
// on the combined (probe row + candidate build row) BEFORE a key match is
// accepted (#358).
//
// This is the placement liftInnerJoinOnResiduals explicitly could not use:
// an outer join's ON runs before the NULL-padding, so a residual lifted above
// the join deletes the very rows the join preserves, and one pushed into a
// preserved side's scan deletes the rows owed back unmatched (the
// FullJoinOnConjunctBuildSide shape). On the probe it is exact for every
// disposition: a probe row whose candidates all fail the residual is simply
// unmatched — LEFT/FULL still emit it NULL-padded — and a build row counts as
// matched only when some probe row passed key AND residual, which is what the
// RIGHT/FULL unmatched flush consults.
//
// Runs AFTER pushdownPredicates so extractJoinCondPredicates has already
// pushed the conjuncts that have a strictly better home (a LEFT join's
// build-side conjunct filters that scan directly; same for a RIGHT join's
// probe side). What remains is exactly what had no legal home before:
// cross-side non-equalities, expression-operand equalities, and any single-
// sided conjunct of a FULL join. Conjuncts that fail to parse stay in
// JoinCond, where the physical planner's key parser still refuses them
// loudly.
//
// When no conjunct survives as a key pair the join becomes keyless: JoinCond
// empties and the executor degenerates to one all-rows candidate chain, with
// the residual doing the whole of the work (`LEFT JOIN r ON n.x = r.y + 3`).
func routeOuterJoinOnResiduals(n *Node) *Node {
	if n == nil {
		return nil
	}
	for i, child := range n.Children {
		n.Children[i] = routeOuterJoinOnResiduals(child)
	}
	if n.Type != NodeJoin || len(n.Children) != 2 || strings.TrimSpace(n.JoinCond) == "" {
		return n
	}
	switch joinKind(n.JoinType) {
	case "left", "right", "full":
	default:
		return n
	}

	parts := splitOnAnd(n.JoinCond, strings.ToUpper(n.JoinCond))
	var keyParts, residuals []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		expr := tryParseExpr(part)
		if expr == nil || isJoinKeyEquality(expr) {
			keyParts = append(keyParts, part)
			continue
		}
		residuals = append(residuals, part)
	}
	if len(residuals) == 0 {
		return n
	}

	n.JoinCond = strings.Join(keyParts, " AND ")
	res := strings.Join(residuals, " AND ")
	if n.JoinFilter != "" {
		n.JoinFilter = n.JoinFilter + " AND " + res
	} else {
		n.JoinFilter = res
	}
	return n
}

// isJoinKeyEquality reports whether a top-level ON conjunct is an equality
// between two BARE COLUMN REFERENCES — the only shape parseJoinKeys turns
// into a key pair, and so the only shape that survives being left in
// JoinCond.
//
// The operands are what distinguishes this from "is an equality". The join
// executor matches on column NAMES, so an operand that is not a column has no
// representation there: `n.n_regionkey = r.r_regionkey + 3` used to reach the
// executor with "r.r_regionkey + 3" as a key column, which resolves to
// nothing and matches nothing — 0 rows for a 10-row query, on both execution
// paths (#351). Lifting it into the filter above the join is exact for an
// inner join and is the same treatment #336 gave the non-equality conjuncts.
//
// An equality against a LITERAL takes the same route. extractJoinCondPredicates
// would otherwise push it to the child that owns it, which lands it in the
// same place; running it through the residual path means the single-conjunct
// case (`ON n.n_regionkey = 1`, which that pass declines because it has
// nothing to split) is covered too — it was another 0-row answer.
//
// Which SIDE each column lives on is still decided later
// (physical.parseJoinKeys, then FixKeyAssignment). The point here is only to
// separate "the join can represent this" from "the join will mis-execute this".
func isJoinKeyEquality(expr plansql.Node) bool {
	if p, ok := expr.(*plansql.ParenNode); ok {
		return isJoinKeyEquality(p.Inner)
	}
	cmp, ok := expr.(*plansql.CmpExpr)
	if !ok || cmp.Op != "=" {
		return false
	}
	return isBareColRef(cmp.Left) && isBareColRef(cmp.Right)
}

// isBareColRef reports whether expr is a plain column reference, qualified or
// not — the only operand physical.parseJoinKeys can turn into a key name.
func isBareColRef(expr plansql.Node) bool {
	if p, ok := expr.(*plansql.ParenNode); ok {
		return isBareColRef(p.Inner)
	}
	_, ok := expr.(*plansql.ColRef)
	return ok
}
