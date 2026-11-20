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
// stays a per-row subquery; one aggregate, ungrouped or grouped on exactly
// the correlation key, still decorrelates, and an ungrouped COUNT or
// APPROX_DISTINCT reads its slot through COALESCE(slot, 0). Every body of
// internal/coordinator's TestWhereCompareScalarEveryArm matrix, which pins
// the answers against PostgreSQL 17.11, has a case here, so a body that
// silently leaves (or joins) the decorrelated path fails this test.
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
					n.ScanColumns = []string{"k", "a", "p"}
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
		// APPROX_DISTINCT answers as COUNT(DISTINCT x) does over an empty
		// match: 0 (#1691).
		{"SELECT approx_distinct(x.c)" + from, true, "o.p = coalesce(__scalar_0, 0)"},
		{"SELECT count(DISTINCT x.c)" + from, true, "o.p = coalesce(__scalar_0, 0)"},
		// GROUP BY exactly the correlation key: one group per matched key,
		// none (NULL, no COALESCE) for an unmatched one.
		{"SELECT count(*)" + from + " GROUP BY x.k", true, "o.p = __scalar_0"},
		{"SELECT sum(x.c)" + from + " GROUP BY x.k", true, "o.p = __scalar_0"},
		{"SELECT max(x.c)" + from + " GROUP BY x.k", true, "o.p = __scalar_0"},
		{"SELECT count(*)" + from + " GROUP BY x.k ORDER BY count(*) DESC LIMIT 1", true, "o.p = __scalar_0"},
		{"SELECT max(x.c)" + from + " GROUP BY x.k ORDER BY 1 LIMIT 1", true, "o.p = __scalar_0"},
		{"SELECT count(*)" + from + " GROUP BY (x.k)", true, "o.p = __scalar_0"},
		{"SELECT count(*)" + from + " GROUP BY x.k, x.k", true, "o.p = __scalar_0"},
		{"SELECT count(*)" + from + " GROUP BY k", true, "o.p = __scalar_0"},
		{"SELECT count(*) FROM sb_i WHERE sb_i.k = o.k GROUP BY k", true, "o.p = __scalar_0"},
		{"SELECT count(*) FROM sb_i WHERE k = o.k GROUP BY sb_i.k", true, "o.p = __scalar_0"},
		{"SELECT count(*)" + from + " AND x.a = o.a GROUP BY x.k, x.a", true, "o.p = __scalar_0"},
		// ...and anything else grouped declines.
		{"SELECT count(*)" + from + " AND x.a = o.a GROUP BY x.k", false, ""},
		{"SELECT count(*)" + from + " GROUP BY x.k, x.a", false, ""},
		{"SELECT count(*)" + from + " GROUP BY x.k + 1", false, ""},
		{"SELECT count(*)" + from + " GROUP BY x.k, 1 + 0", false, ""},
		{"SELECT count(*)" + from + " GROUP BY o.k", false, ""},
		{"SELECT count(*)" + from + " GROUP BY x.k HAVING count(*) > 1", false, ""},
		{"SELECT count(*) + x.k" + from + " GROUP BY x.k", true, "o.p = __scalar_0 + x.k"},
		// The plan computes one aggregate: a second one, or one under a CASE
		// that ReplaceAggregate does not reach, declines; one aggregate
		// under a function or arithmetic still decorrelates.
		{"SELECT max(x.c) - min(x.c)" + from, false, ""},
		{"SELECT CASE WHEN count(*) = 0 THEN 2 ELSE 1 END" + from, false, ""},
		{"SELECT abs(max(x.c))" + from, true, "o.p = abs(__scalar_0)"},
		{"SELECT coalesce(count(*), 5)" + from, true, "coalesce(coalesce(__scalar_0, 0), 5)"},
		{"SELECT sum(x.c) / sum(x.c)" + from, true, "o.p = __scalar_0 / __scalar_0"},
		// Bodies the WHERE-comparison rule never decorrelated.
		{"SELECT x.c" + from, false, ""},
		{"SELECT x.c" + from + " ORDER BY x.c DESC LIMIT 1", false, ""},
		{"SELECT x.c" + from + " ORDER BY x.c DESC LIMIT 1 OFFSET 1", false, ""},
		{"SELECT count(*)" + from + " GROUP BY x.a ORDER BY count(*) DESC LIMIT 1 OFFSET 1", false, ""},
		{"SELECT count(*) FROM sb_i x WHERE x.k = o.k GROUP BY x.k LIMIT 1", true, "o.p = __scalar_0"},
		{"SELECT sum(x.c)" + from + " GROUP BY x.k ORDER BY 1 LIMIT 1", true, "o.p = __scalar_0"},
		{"SELECT max(x.c)" + from + " AND x.a = o.a GROUP BY x.a, x.k", true, "o.p = __scalar_0"},
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
