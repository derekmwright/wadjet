// SPDX-License-Identifier: MIT

package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestScalarDecorrelationKeepsOrDeclinesBodyClauses pins which correlated
// scalar subquery bodies the WHERE-comparison rewrite takes (#1701). A body
// whose clause the LEFT join + per-key aggregate cannot carry declines and
// stays a per-row subquery; one ungrouped aggregate still decorrelates, and
// a COUNT reads its slot through COALESCE(slot, 0). The answers are pinned
// against PostgreSQL 17.11 by internal/coordinator's
// TestWhereCompareScalarEveryArm.
func TestScalarDecorrelationKeepsOrDeclinesBodyClauses(t *testing.T) {
	ann := func(n *Node) {
		var walk func(*Node)
		walk = func(n *Node) {
			if n == nil {
				return
			}
			if n.Type == NodeScan && len(n.ScanColumns) == 0 {
				if n.TableName == "sb_i" {
					n.ScanColumns = []string{"k", "a", "c"}
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
	const from = " FROM sb_i x WHERE x.k = o.k"
	cases := []struct {
		body       string
		decorrel   bool
		wantFilter string // a substring of the rewritten predicate, when decorrelated
	}{
		{"SELECT max(x.c)" + from, true, "o.p = __scalar_0"},
		{"SELECT sum(x.c)" + from, true, "o.p = __scalar_0"},
		{"SELECT count(*)" + from, true, "o.p = coalesce(__scalar_0, 0)"},
		{"SELECT count(x.c) + 1" + from, true, "coalesce(__scalar_0, 0) + 1"},
		{"SELECT max(x.c)" + from + " ORDER BY 1", true, "o.p = __scalar_0"},
		{"SELECT DISTINCT max(x.c)" + from, true, "o.p = __scalar_0"},
		{"SELECT max(x.c)" + from + " LIMIT 1", true, "o.p = __scalar_0"},
		{"SELECT count(*)" + from + " GROUP BY x.a", false, ""},
		{"SELECT count(*)" + from + " GROUP BY x.a HAVING count(*) > 1", false, ""},
		{"SELECT count(*)" + from + " HAVING count(*) > 1", false, ""},
		{"SELECT count(*)" + from + " GROUP BY x.a ORDER BY count(*) DESC LIMIT 1", false, ""},
		{"SELECT max(x.c)" + from + " LIMIT 0", false, ""},
		{"SELECT max(x.c)" + from + " OFFSET 1", false, ""},
		{"SELECT DISTINCT count(*)" + from + " GROUP BY x.c", false, ""},
		{"SELECT count(*)" + from + " GROUP BY ROLLUP (x.a) ORDER BY count(*) DESC LIMIT 1", false, ""},
		{"SELECT count(*)" + from + " GROUP BY GROUPING SETS (())", false, ""},
		{"SELECT approx_distinct(x.c)" + from, false, ""},
	}
	for _, c := range cases {
		q := "SELECT o.k, o.p FROM sb_o o WHERE o.p = (" + c.body + ")"
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
		got := findNodeMatch(opt, func(n *Node) bool { return n.ScalarDecorrelated })
		if got != c.decorrel {
			t.Errorf("%s: decorrelated=%v, want %v", c.body, got, c.decorrel)
			continue
		}
		if !c.decorrel {
			continue
		}
		var preds []string
		var walk func(*Node)
		walk = func(n *Node) {
			if n == nil {
				return
			}
			if n.Type == NodeFilter {
				for _, p := range n.Predicates {
					preds = append(preds, p.Raw)
				}
			}
			for _, ch := range n.Children {
				walk(ch)
			}
		}
		walk(opt)
		if !strings.Contains(strings.Join(preds, " AND "), c.wantFilter) {
			t.Errorf("%s: filter %q, want it to contain %q", c.body, preds, c.wantFilter)
		}
	}
}
