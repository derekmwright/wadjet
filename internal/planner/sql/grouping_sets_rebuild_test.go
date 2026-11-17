// SPDX-License-Identifier: MIT

package sql

import (
	"fmt"
	"strings"
	"testing"
)

// THE REBUILT TEXT GROUPS BY THE SETS IT WAS GIVEN (#1700). Every grouping
// form the parser accepts — GROUPING SETS with parenthesized, bare and empty
// sets, ROLLUP, CUBE, a repeated term, a qualified column, an expression, an
// ordinal, an alias — is rendered by RebuildSQL and by RebuildSQLForRerun and
// parsed back: the sets must index the same keys in the same order, and the
// text must never be a plain GROUP BY of the keys' union, which is what the
// rebuild wrote before (a ROLLUP body re-run as GROUP BY a lost its subtotal
// and grand-total rows). The plain GROUP BY and the ungrouped block are the
// controls: their text is unchanged by the fix.
func TestRebuildGroupingSetsRoundTrip(t *testing.T) {
	cases := []struct {
		sql  string
		sets string // the rebuilt GROUP BY's expanded sets, by key position
		text string // the GROUP BY clause RebuildSQL writes
	}{
		{"SELECT a, count(*) FROM t x WHERE x.k = o.k GROUP BY ROLLUP (a)",
			"[[0] []]", "GROUP BY GROUPING SETS ((a), ())"},
		{"SELECT a, b, count(*) FROM t x WHERE x.k = o.k GROUP BY ROLLUP (a, b)",
			"[[0 1] [0] []]", "GROUP BY GROUPING SETS ((a, b), (a), ())"},
		{"SELECT a, b, count(*) FROM t x WHERE x.k = o.k GROUP BY CUBE (a, b)",
			"[[0 1] [1] [0] []]", "GROUP BY GROUPING SETS ((a, b), (b), (a), ())"},
		{"SELECT a, b, count(*) FROM t x WHERE x.k = o.k GROUP BY GROUPING SETS ((a), (b), ())",
			"[[0] [1] []]", "GROUP BY GROUPING SETS ((a), (b), ())"},
		{"SELECT a, b, count(*) FROM t x WHERE x.k = o.k GROUP BY GROUPING SETS (a, (a, b))",
			"[[0] [0 1]]", "GROUP BY GROUPING SETS ((a), (a, b))"},
		{"SELECT count(*) FROM t x WHERE x.k = o.k GROUP BY GROUPING SETS (())",
			"[[]]", "GROUP BY GROUPING SETS (())"},
		{"SELECT count(*) FROM t x WHERE x.k = o.k GROUP BY GROUPING SETS ((), ())",
			"[[] []]", "GROUP BY GROUPING SETS ((), ())"},
		{"SELECT a, count(*) FROM t x WHERE x.k = o.k GROUP BY ROLLUP (a, a)",
			"[[0 0] [0] []]", "GROUP BY GROUPING SETS ((a, a), (a), ())"},
		{"SELECT x.a, count(*) FROM t x WHERE x.k = o.k GROUP BY ROLLUP (x.a)",
			"[[0] []]", "GROUP BY GROUPING SETS ((x.a), ())"},
		{"SELECT a + 1, count(*) FROM t x WHERE x.k = o.k GROUP BY CUBE (a + 1)",
			"[[0] []]", "GROUP BY GROUPING SETS ((a + 1), ())"},
		{"SELECT a, count(*) FROM t x WHERE x.k = o.k GROUP BY ROLLUP (1)",
			"[[0] []]", "GROUP BY GROUPING SETS ((a), ())"},
		{"SELECT a AS aa, count(*) FROM t x WHERE x.k = o.k GROUP BY ROLLUP (aa)",
			"[[0] []]", "GROUP BY GROUPING SETS ((a), ())"},
		{"SELECT GROUPING(a) AS g, count(*) FROM t x WHERE x.k = o.k GROUP BY ROLLUP (a) HAVING count(*) > 0",
			"[[0] []]", "GROUP BY GROUPING SETS ((a), ())"},
		// A key that reads the outer row: the re-run substitutes it.
		{"SELECT count(*) FROM t x WHERE x.k = o.k GROUP BY ROLLUP (a, o.k)",
			"[[0 1] [0] []]", "GROUP BY GROUPING SETS ((a, o.k), (a), ())"},
		// Controls.
		{"SELECT a, count(*) FROM t x WHERE x.k = o.k GROUP BY a", "[]", "GROUP BY a"},
		{"SELECT count(*) FROM t x WHERE x.k = o.k", "[]", ""},
	}
	outer := map[string]bool{"o": true}
	vals := map[string]any{"o.k": int64(42)}
	rewrite := func(n Node) Node { return RewriteOuterRefs(n, outer, vals) }
	extract := func(t *testing.T, sql string) *SelectInfo {
		t.Helper()
		pq, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		info, err := ExtractSelect(pq)
		if err != nil {
			t.Fatalf("extract %q: %v", sql, err)
		}
		return info
	}
	keysOf := func(info *SelectInfo) string {
		if len(info.GroupingSets) == 0 {
			return "[]"
		}
		return fmt.Sprint(groupingSetKeyPositions(info))
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			info := extract(t, c.sql)
			if got := keysOf(info); got != c.sets {
				t.Fatalf("parsed sets %s, want %s", got, c.sets)
			}
			plain := RebuildSQL(info, info.WhereExpr)
			rerun, changed := RebuildSQLForRerun(info, rewrite)
			if !changed {
				t.Fatalf("the re-run substituted nothing: %s", rerun)
			}
			gotText := ""
			if i := strings.Index(plain, " GROUP BY "); i >= 0 {
				gotText = strings.TrimSpace(plain[i:])
				if j := strings.Index(gotText, " HAVING "); j >= 0 {
					gotText = gotText[:j]
				}
			}
			if gotText != c.text {
				t.Errorf("RebuildSQL GROUP BY %q, want %q\n  in %s", gotText, c.text, plain)
			}
			for _, rebuilt := range []string{plain, rerun} {
				back := extract(t, rebuilt)
				if got := keysOf(back); got != c.sets {
					t.Errorf("re-parsed sets %s, want %s\n  rebuilt %s", got, c.sets, rebuilt)
				}
				if len(back.GroupBy) != len(info.GroupBy) {
					t.Errorf("re-parsed key list %v, want %d keys (%v)\n  rebuilt %s",
						back.GroupBy, len(info.GroupBy), info.GroupBy, rebuilt)
				}
			}
		})
	}
}
