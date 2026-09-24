// SPDX-License-Identifier: MIT

package logical

import (
	"fmt"
	"sort"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A JOIN ARM IS THE RELATION ITS QUALIFIER NAMES (#1299, ADR-0026 §8k).
//
// The reorderer takes an inner-join chain apart into relations and edges and
// puts it back together in the cheapest order, hanging every ON conjunct on
// the first join whose subtree holds the relations the conjunct's edge names.
// So an edge's relations have to be the ones the conjunct READS. They were
// resolved by bare COLUMN NAME: the endpoint was a relation owning `order_id`
// or `id`. Where one table is joined more than once every copy owns every
// such name, and the self-join arms were told apart by elimination — which
// fails as soon as three copies meet: `o JOIN item s ON s.order_id = o.id
// JOIN item t ON t.order_id = o.id` recorded t's edge as s–t, the DP joined s
// and t first on `t.order_id = o.id`, and the physical planner resolved the
// absent `o.id` against the copy that was there. The answer paired one
// order's s with another order's t, with no error.
//
// The rule: a conjunct's edge is the set of relations its column references
// name — a qualified reference belongs to the one relation that publishes its
// qualifier, a bare one to the one relation that carries the column — and it
// may hang only on a join whose subtree holds EVERY one of them. A conjunct
// that names three relations is an edge over three (joinEdge.extra), never a
// pair that leaves the third to chance. Where a reference cannot be resolved
// to exactly one relation, the previous column-name endpoints stand, widened
// by every relation that WAS resolved: a wider edge only delays a conjunct to
// a join that certainly holds what it reads.

// relationMembership is what each relation of one flattened chain answers to:
// the qualifiers it publishes (byte-exact, as collectScanInfoRec records
// them) and the bare columns it carries.
type relationMembership struct {
	names []map[string]bool
	cols  []map[string]bool
}

func membershipOf(rels []*Node) relationMembership {
	names, _ := joinSidesScanInfo(rels...)
	cols := make([]map[string]bool, len(rels))
	for i, r := range rels {
		cols[i] = subtreePublishedColumns(r)
	}
	return relationMembership{names: names, cols: cols}
}

// conjunctRelations returns the relations (indexes into the membership's
// relations, ascending) the conjunct's column references name, and whether
// EVERY reference resolved to exactly one relation. An expression shape the
// walk does not know is incomplete: its references are not all seen.
func (m relationMembership) conjunctRelations(expr plansql.Node) ([]int, bool) {
	set := make(map[int]bool, 4)
	complete := true
	var walk func(plansql.Node)
	owner := func(c *plansql.ColRef) {
		found := -1
		for i := range m.names {
			var has bool
			if c.Table != "" {
				has = m.names[i][c.Table]
			} else {
				has = m.cols[i][strings.ToLower(c.Column)]
			}
			if !has {
				continue
			}
			if found >= 0 {
				complete = false
				return
			}
			found = i
		}
		if found < 0 {
			complete = false
			return
		}
		set[found] = true
	}
	walk = func(node plansql.Node) {
		switch n := node.(type) {
		case nil:
		case *plansql.Lit:
		case *plansql.ColRef:
			owner(n)
		case *plansql.ParenNode:
			walk(n.Inner)
		case *plansql.NotNode:
			walk(n.Inner)
		case *plansql.UnaryOp:
			walk(n.Inner)
		case *plansql.AndNode:
			walk(n.Left)
			walk(n.Right)
		case *plansql.OrNode:
			walk(n.Left)
			walk(n.Right)
		case *plansql.BinaryOp:
			walk(n.Left)
			walk(n.Right)
		case *plansql.CmpExpr:
			walk(n.Left)
			walk(n.Right)
		case *plansql.IsExpr:
			walk(n.Left)
		case *plansql.LikeExpr:
			walk(n.Left)
			walk(n.Pattern)
		case *plansql.BetweenExpr:
			walk(n.Left)
			walk(n.Low)
			walk(n.High)
		case *plansql.InExpr:
			walk(n.Left)
			for _, v := range n.Values {
				walk(v)
			}
		case *plansql.CastNode:
			walk(n.Inner)
		case *plansql.FuncCallNode:
			for _, a := range n.Args {
				walk(a)
			}
		case *plansql.CaseNode:
			walk(n.Subject)
			for _, w := range n.Whens {
				walk(w.Cond)
				walk(w.Result)
			}
			walk(n.Else)
		default:
			complete = false
		}
	}
	walk(expr)
	out := make([]int, 0, len(set))
	for i := range set {
		out = append(out, i)
	}
	sort.Ints(out)
	return out, complete
}

// members lists every relation the edge names.
func (e joinEdge) members() []int {
	out := make([]int, 0, 2+len(e.extra))
	out = append(out, e.leftIdx)
	if e.rightIdx != e.leftIdx {
		out = append(out, e.rightIdx)
	}
	return append(out, e.extra...)
}

// mask is members as a bitmask (the DP's relation sets; n ≤ 16 there).
func (e joinEdge) mask() int {
	m := 0
	for _, i := range e.members() {
		m |= 1 << i
	}
	return m
}

// joinsInto reports whether the edge becomes applicable at the join that
// adds relation j to the relation set `have`: it names j, and every other
// relation it names is already in `have`.
func (e joinEdge) joinsInto(have, j int) bool {
	em := e.mask()
	rest := em &^ (1 << j)
	return em&(1<<j) != 0 && rest != 0 && rest&^have == 0
}

// edgeKey groups a join's conjuncts by the relation set they name.
func (e joinEdge) edgeKey() string {
	ms := e.members()
	sort.Ints(ms)
	return fmt.Sprint(ms)
}
