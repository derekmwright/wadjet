package logical

import (
	"strings"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
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
	for i, r := range rels {
		relCols[i] = collectSubtreeColumns(r)
	}

	// owners returns the relation indexes that expose col.
	owners := func(col string) []int {
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
		lcol := stripQualifier(colRefName(cmp.Left))
		rcol := stripQualifier(colRefName(cmp.Right))
		if lcol == "" || rcol == "" {
			kept = append(kept, pred)
			continue
		}
		lown, rown := owners(lcol), owners(rcol)
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
