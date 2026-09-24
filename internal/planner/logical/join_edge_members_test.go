// SPDX-License-Identifier: MIT

package logical

import (
	"sort"
	"testing"
)

// A JOIN CONJUNCT'S EDGE IS THE SET OF RELATIONS ITS QUALIFIERS NAME (#1299).
// Three copies of one table keyed on an outer column whose NAME every copy
// also carries: each conjunct's edge must be (o, <its own copy>) — the
// column-name endpoints recorded (s, t) and (t, u), and the reorderer hung
// `t.order_id = o.id` on a join of two item copies.
func TestAJoinConjunctsEdgeIsTheRelationsItsQualifiersName(t *testing.T) {
	o := armScan("lat_ord", "o", "id", "customer")
	s := armScan("lat_item", "s", "id", "order_id")
	tt := armScan("lat_item", "t", "id", "order_id")
	u := armScan("lat_item", "u", "id", "order_id")
	chain := NewJoin(NewJoin(NewJoin(o, s, "inner", "s.order_id = o.id"),
		tt, "inner", "t.order_id = o.id"), u, "inner", "u.order_id = o.id AND u.id = t.id")

	var rels []*Node
	var edges []joinEdge
	flattenJoinChain(chain, &rels, &edges)
	if len(rels) != 4 {
		t.Fatalf("flattened %d relations, want 4", len(rels))
	}
	got := map[string][]int{}
	for _, e := range edges {
		m := e.members()
		sort.Ints(m)
		got[e.joinCond] = m
	}
	want := map[string][]int{
		"s.order_id = o.id": {0, 1},
		"t.order_id = o.id": {0, 2},
		"u.order_id = o.id": {0, 3},
		"u.id = t.id":       {2, 3},
	}
	for cond, w := range want {
		g, ok := got[cond]
		if !ok {
			t.Errorf("no edge for %q (edges: %v)", cond, got)
			continue
		}
		if len(g) != len(w) || g[0] != w[0] || g[1] != w[1] {
			t.Errorf("edge %q names relations %v, want %v", cond, g, w)
		}
	}
}

// A conjunct over THREE relations is one edge over three: it applies only at
// the join that holds all of them, never on a pair that leaves the third out.
func TestAConjunctOverThreeRelationsWaitsForAllThree(t *testing.T) {
	a := armScan("ta", "a", "x")
	b := armScan("tb", "b", "y")
	c := armScan("tc", "c", "z")
	chain := NewJoin(NewJoin(a, b, "inner", "b.y = a.x"), c, "inner", "c.z = a.x + b.y")
	var rels []*Node
	var edges []joinEdge
	flattenJoinChain(chain, &rels, &edges)
	var three *joinEdge
	for i := range edges {
		if edges[i].joinCond == "c.z = a.x + b.y" {
			three = &edges[i]
		}
	}
	if three == nil {
		t.Fatalf("no edge for the three-relation conjunct: %+v", edges)
	}
	if n := len(three.members()); n != 3 {
		t.Fatalf("the three-relation conjunct names %d relations (%v), want 3", n, three.members())
	}
	// Adding c to {a} must not apply it; adding c to {a, b} must.
	if three.joinsInto(1<<0, 2) {
		t.Errorf("the edge applied at a join holding a and c only")
	}
	if !three.joinsInto(1<<0|1<<1, 2) {
		t.Errorf("the edge did not apply at the join holding a, b and c")
	}
}
