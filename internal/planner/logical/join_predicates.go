package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Join predicate placement must account for which inputs can be NULL-padded:
// never push WHERE below padding without checking semantics (#335).
// ON conjuncts need a representation the physical key parser can preserve (#336).
// Test OPERANDS, not just equality: computed operands are not column-name keys
// and must be treated as residuals (#351).
// See docs/internals/join-predicate-placement-contract.md for the design.

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

// liftInnerJoinOnResiduals moves ON conjuncts other than cross-side equalities
// from INNER/CROSS joins to a filter above them (#336); the key parser cannot keep them.
// ON and WHERE are interchangeable there; subsequent pushdownPredicates returns
// single-sided conjuncts to their inputs.
// Leave OUTER joins alone: ON runs BEFORE NULL-padding, and moving residuals above
// would delete preserved rows. This pass does not supply outer-join residual execution.
// See docs/internals/inner-join-on-residual-placement.md for the design.
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
	rowFields := subtreeRowFields(join)

	var keyParts []string
	var residuals []Predicate
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		expr := tryParseExpr(part)
		if expr == nil || isJoinKeyEquality(expr, rowFields) {
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

// routeOuterJoinOnResiduals moves LEFT/RIGHT/FULL ON non-key conjuncts into JoinFilter,
// evaluated on probe + candidate build row BEFORE accepting a key match (#358).
// Never move them above NULL-padding or into a preserved-side scan: unmatched rows are owed.
// A probe whose candidates all fail stays unmatched; LEFT/FULL emit it padded.
// A build row matches only if key AND residual pass; RIGHT/FULL flush uses that fact.
// Run AFTER pushdownPredicates moves legal single-side conjuncts (LEFT build, RIGHT probe).
// Route cross-side non-equalities, computed equalities and FULL single-side conjuncts;
// parse failures stay in JoinCond for loud physical refusal. Without key pairs,
// JoinCond empties and one all-rows candidate chain is tested wholly by the residual.
// See docs/internals/outer-join-residual-match-contract.md for the design.
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
	rowFields := subtreeRowFields(n)
	var keyParts, residuals []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		expr := tryParseExpr(part)
		if expr == nil || isJoinKeyEquality(expr, rowFields) {
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

// isJoinKeyEquality accepts only top-level equality of two BARE COLUMN REFERENCES:
// only these operands have a key-pair representation in parseJoinKeys (#351).
// Computed operands and literals take the residual route, including a single ON
// literal equality that extractJoinCondPredicates cannot split (#336).
// This checks representability only; physical.parseJoinKeys and FixKeyAssignment
// later decide which SIDE each column belongs to.
// See docs/internals/join-key-equality-operand-boundary.md for the design.
func isJoinKeyEquality(expr plansql.Node, rowFields map[string][]parquet.Column) bool {
	if p, ok := expr.(*plansql.ParenNode); ok {
		return isJoinKeyEquality(p.Inner, rowFields)
	}
	cmp, ok := expr.(*plansql.CmpExpr)
	if !ok || cmp.Op != "=" {
		return false
	}
	return isBareColRef(cmp.Left, rowFields) && isBareColRef(cmp.Right, rowFields)
}

// isBareColRef accepts plain qualified or unqualified column references, but NOT
// ROW FIELD PATHS (#769): a field must be materialized like a computed expression,
// never passed as a key name (ADR-0022 rule 1; #351).
// rowFields comes from subtreeRowFields and depends on scan annotation. Without it,
// the empty map recognizes no field paths and retains pre-#769 routing.
// TestRowFieldPathPushdownFollowsTheAnnotation pins that conservative boundary.
// See docs/internals/row-field-join-key-materialization.md for the design.
func isBareColRef(expr plansql.Node, rowFields map[string][]parquet.Column) bool {
	if p, ok := expr.(*plansql.ParenNode); ok {
		return isBareColRef(p.Inner, rowFields)
	}
	ref, ok := expr.(*plansql.ColRef)
	if !ok {
		return false
	}
	return !isRowFieldPath(ref, rowFields)
}

// isRowFieldPath reports whether ref's QUALIFIER names a ROW container that
// DECLARES the referenced field — ADR-0022 rule 1's test, asked with the
// declarations a subtree carries. The container must declare the field, so an
// ordinary qualified reference whose qualifier happens to name a container is
// untouched (#604).
func isRowFieldPath(ref *plansql.ColRef, rowFields map[string][]parquet.Column) bool {
	if ref == nil || ref.Table == "" || len(rowFields) == 0 {
		return false
	}
	fields, ok := rowFields[strings.ToLower(ref.Table)]
	if !ok {
		return false
	}
	for _, f := range fields {
		if strings.EqualFold(f.Name, ref.Column) {
			return true
		}
	}
	return false
}
