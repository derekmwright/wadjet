package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// liftWhereEquiPredsIntoJoins moves WHERE equality predicates into the
// join conditions of comma-FROM join chains.
//
// Explicit JOIN ... ON syntax carries its conditions on the join nodes,
// which is what reorderJoins' edge extraction and the physical planner's
// key assignment consume. Comma-separated FROM lists instead parse into
// bare relations (the builder folds them as condition-less cross joins,
// issue #281) with the equalities left in the WHERE filter. Without this
// pass those chains would execute as cross products with a post-filter —
// correct, but catastrophic at scale.
//
// Shape handled: Filter → (any number of semi/anti levels, descending
// their probe side — semi/anti commute with conjunctive probe-side
// filters, see pushOneSemi) → left-deep inner-join chain. A filter
// predicate is lifted when it is a bare column equality whose two sides
// are owned by two DIFFERENT relations of the chain; it attaches to the
// first chain join whose subtree covers both. Activation is gated on the
// chain containing at least one condition-less inner/cross join, so
// explicit-JOIN queries are untouched.
func liftWhereEquiPredsIntoJoins(n *Node) *Node {
	if n == nil {
		return nil
	}
	for i, child := range n.Children {
		n.Children[i] = liftWhereEquiPredsIntoJoins(child)
	}
	if n.Type != NodeFilter || len(n.Children) == 0 {
		return n
	}

	// Descend through semi/anti probe sides to find the inner-join chain
	// (semi/anti commute with conjunctive probe-side filters).
	chain := n.Children[0]
	for chain != nil && chain.Type == NodeJoin && isSemiOrAnti(chain) {
		chain = chain.Children[0]
	}
	if chain == nil || chain.Type != NodeJoin || !isInnerJoin(chain) {
		return n
	}

	// Collect the left-deep spine (root first) and its leaf relations
	// (bottom-up: rel[0] is the deepest left child, rel[k] is the right
	// child of the (k-1)-th spine join from the bottom).
	var spine []*Node
	cur := chain
	for cur.Type == NodeJoin && isInnerJoin(cur) {
		spine = append(spine, cur)
		cur = cur.Children[0]
	}
	condless := false
	for _, j := range spine {
		if j.JoinCond == "" {
			condless = true
			break
		}
	}
	if !condless {
		return n // explicit-join chain — nothing to repair
	}
	rels := make([]*Node, 0, len(spine)+1)
	rels = append(rels, cur) // deepest left child
	for i := len(spine) - 1; i >= 0; i-- {
		rels = append(rels, spine[i].Children[1])
	}
	relCols := make([]map[string]bool, len(rels))
	relAliases := make([]map[string]bool, len(rels))
	for i, r := range rels {
		relCols[i] = liftExposedColumns(r)
		relAliases[i] = liftRelationAliases(r)
	}

	// owners returns the relation indexes a reference can name. A QUALIFIED
	// reference is resolved by its qualifier first: with the same table in
	// the FROM list twice the bare column name is owned by both, and picking
	// by name alone attached both of `s_nationkey = n1.n_nationkey` and
	// `c_nationkey = n2.n_nationkey` to whichever alias came first. The other
	// alias was then left dangling under a bare cross join carrying a
	// condition that names it from a subtree that does not contain it — a key
	// resolving to nothing, so TPC-H Q7 and Q8 in their official comma-join
	// spelling answered ZERO ROWS (#593, #594).
	owners := func(qual, col string) []int {
		if qual != "" {
			var byAlias []int
			for i := range rels {
				if relAliases[i][qual] {
					byAlias = append(byAlias, i)
				}
			}
			// Trust the qualifier only when it names relations of this chain;
			// otherwise fall through to the name-based rule, which is what a
			// reference to an outer scope or an un-annotated scan needs.
			if len(byAlias) > 0 {
				return byAlias
			}
		}
		var out []int
		for i := range rels {
			if relCols[i][col] {
				out = append(out, i)
			}
		}
		return out
	}

	var kept []Predicate
	for _, pred := range n.Predicates {
		expr := pred.ASTExpr
		if expr == nil {
			expr = tryParseExpr(pred.Raw)
		}
		cmp, ok := expr.(*plansql.CmpExpr)
		if !ok || cmp.Op != "=" {
			kept = append(kept, pred)
			continue
		}
		lqual, lcol := colRefParts(cmp.Left)
		rqual, rcol := colRefParts(cmp.Right)
		if lcol == "" || rcol == "" {
			kept = append(kept, pred)
			continue
		}
		lown, rown := owners(lqual, lcol), owners(rqual, rcol)
		li, ri := disjointOwnerPair(lown, rown)
		if li < 0 {
			kept = append(kept, pred)
			continue
		}
		// Attach to the spine join that introduced the higher-indexed
		// relation: spine is root-first, rel[k] joined by spine[len-k].
		hi := li
		if ri > hi {
			hi = ri
		}
		target := spine[len(spine)-hi]
		cond := strings.TrimSpace(pred.Raw)
		if cond == "" {
			cond = cmp.String()
		}
		if target.JoinCond == "" {
			target.JoinCond = cond
			target.JoinType = "join" // no longer a bare cross join
		} else {
			target.JoinCond = target.JoinCond + " AND " + cond
		}
	}
	if len(kept) == len(n.Predicates) {
		return n
	}
	if len(kept) == 0 {
		// Every predicate lifted — the filter node dissolves.
		return n.Children[0]
	}
	n.Predicates = kept
	return n
}

// disjointOwnerPair picks owner indexes (a, b) with a != b, a from lown
// and b from rown. Returns (-1, -1) when no disjoint assignment exists.
// Preference: unambiguous single owners first; otherwise the pair
// minimizing the higher index (attaches the condition as deep in the
// chain as possible).
func disjointOwnerPair(lown, rown []int) (int, int) {
	best, bestHi := [2]int{-1, -1}, 1<<31-1
	for _, a := range lown {
		for _, b := range rown {
			if a == b {
				continue
			}
			hi := a
			if b > hi {
				hi = b
			}
			if hi < bestHi {
				bestHi = hi
				best = [2]int{a, b}
			}
		}
	}
	return best[0], best[1]
}

// liftExposedColumns returns the column names a chain relation EXPOSES to the
// join above it. For a scan (or a filter over one) that is the scan's own
// columns, which is what collectSubtreeColumns answers. For a DERIVED TABLE
// in the comma list it is not: `(SELECT p_partkey AS pk FROM part) d` reads
// p_partkey and emits pk, so resolving ownership through the scan looks for
// the wrong name, finds nothing, and the WHERE equality is left as a filter
// above a real cross product.
func liftExposedColumns(n *Node) map[string]bool {
	if n == nil {
		return map[string]bool{}
	}
	switch n.Type {
	case NodeProject:
		out := make(map[string]bool, len(n.Projections))
		for _, p := range n.Projections {
			if p.Hidden {
				continue
			}
			name := p.Alias
			if name == "" {
				name = p.Column
			}
			if name != "" {
				out[strings.ToLower(stripQualifier(name))] = true
			}
		}
		if len(out) > 0 {
			return out
		}
	case NodeAggregate:
		out := make(map[string]bool, len(n.GroupBy)+len(n.AggExprs))
		for _, g := range n.GroupBy {
			out[strings.ToLower(stripQualifier(g))] = true
		}
		for _, a := range n.AggExprs {
			if a.OutputCol != "" {
				out[strings.ToLower(stripQualifier(a.OutputCol))] = true
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return collectSubtreeColumns(n)
}

// colRefParts splits a column reference into its relation qualifier and its
// column name, or ("", "") when the node is not a bare column reference. The
// qualifier has to come off the AST node: colRefName returns ColRef.Column
// alone and DROPS ColRef.Table, which is exactly the information that tells
// `n1.n_nationkey` from `n2.n_nationkey`.
//
// The COLUMN folds and the QUALIFIER does not — the identity rule, here as
// everywhere else. The lexer folded an unquoted qualifier already, so `t` and
// `"T"` reach this point as two names and must stay two: folding them made
// two relations one, and ownership then assigned both sides of
// `t.k = "T".k` to the same relation.
func colRefParts(node plansql.Node) (qual, col string) {
	ref, ok := node.(*plansql.ColRef)
	if !ok {
		return "", ""
	}
	return ref.Table, strings.ToLower(ref.Column)
}

// liftRelationAliases returns the names a chain relation may be QUALIFIED by:
// each scan's alias (or, unaliased, its table name) and every derived-table
// scope it sits inside. It is what lets ownership tell `nation n1` from
// `nation n2`, which share every bare column name.
func liftRelationAliases(n *Node) map[string]bool {
	out := make(map[string]bool, 2)
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Type == NodeScan {
			// Byte-exact, matching colRefParts' qualifier. A qualifier that
			// names no relation of the chain falls through to the
			// column-name rule in `owners`, which is the existing path for
			// an outer-scope or un-annotated reference — so holding the
			// relation exactly can lose an optimization, never an answer.
			if n.TableAlias != "" {
				out[n.TableAlias] = true
			} else if n.TableName != "" {
				out[n.TableName] = true
			}
			for _, d := range n.DerivedAliases {
				out[d] = true
			}
		}
		// A semi/anti join contributes only its probe side's names, matching
		// liftExposedColumns' view of what the subtree emits.
		if n.Type == NodeJoin && len(n.Children) == 2 && isSemiOrAnti(n) {
			walk(n.Children[0])
			return
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(n)
	return out
}
