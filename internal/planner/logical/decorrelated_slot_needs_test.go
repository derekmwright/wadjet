// SPDX-License-Identifier: MIT

package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestStackedScalarJoinsKeepLowerSlots pins that two or three correlated
// scalar subqueries decorrelated in one Filter keep every `__scalar_N` slot up
// to the Filter that reads them (#1701). Each becomes a LEFT join stacked over
// the last; the column-needs partition at an upper join used to send a lower
// join's slot to neither side, so the lower join's NeededColumns (its
// single-process probe OutputFilter) lacked it and the Filter read NULL in its
// place. A decorrelated join must need its own slot and every slot of the
// decorrelated joins below it.
func TestStackedScalarJoinsKeepLowerSlots(t *testing.T) {
	ann := func(n *Node) {
		var walk func(*Node)
		walk = func(n *Node) {
			if n == nil {
				return
			}
			if n.Type == NodeScan && len(n.ScanColumns) == 0 {
				if n.TableName == "sn_i" || n.TableName == "sn_j" {
					n.ScanColumns = []string{"k", "c"}
				} else {
					n.ScanColumns = []string{"k", "p"}
				}
			}
			for _, c := range n.Children {
				walk(c)
			}
		}
		walk(n)
	}
	cases := []struct {
		where string
		joins int
	}{
		{"o.p = (SELECT max(x.c) FROM sn_i x WHERE x.k = o.k) AND 0 <= (SELECT max(x.c) FROM sn_i x WHERE x.k = o.k)", 2},
		{"o.p = (SELECT count(*) FROM sn_i x WHERE x.k = o.k) AND o.p < (SELECT sum(x.c) FROM sn_j x WHERE x.k = o.k)", 2},
		{"o.p = (SELECT max(x.c) FROM sn_i x WHERE x.k = o.k) AND 1 <= (SELECT min(x.c) FROM sn_i x WHERE x.k = o.k)" +
			" AND 0 <= (SELECT count(*) FROM sn_j x WHERE x.k = o.k GROUP BY x.k)", 3},
	}
	for _, c := range cases {
		q := "SELECT o.k, o.p FROM sn_o o WHERE " + c.where
		parsed, err := plansql.Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		info, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		plan, err := BuildFromSelect(info)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		ann(plan)
		opt := Optimize(plan, ann)

		// The decorrelated joins, bottom first.
		var joins []*Node
		var walk func(*Node)
		walk = func(n *Node) {
			if n == nil {
				return
			}
			for _, ch := range n.Children {
				walk(ch)
			}
			if n.Type == NodeJoin && n.ScalarDecorrelated {
				joins = append(joins, n)
			}
		}
		walk(opt)
		if len(joins) != c.joins {
			t.Errorf("%s: %d decorrelated joins, want %d", c.where, len(joins), c.joins)
			continue
		}
		var below []string
		for i, j := range joins {
			need := map[string]bool{}
			for _, col := range j.NeededColumns {
				need[strings.ToLower(col)] = true
			}
			own := strings.ToLower(j.Children[1].AggExprs[0].OutputCol)
			for _, slot := range append(append([]string(nil), below...), own) {
				if !need[slot] {
					t.Errorf("%s: decorrelated join %d of %d needs %v, missing %s", c.where, i+1, len(joins), j.NeededColumns, slot)
				}
			}
			below = append(below, own)
		}
	}
}
