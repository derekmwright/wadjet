package sql

import (
	"strings"
	"testing"
)

// A grouping-set TERM is a GROUP BY term, and the parser has to record it the
// same way: the canonical name AND the parsed form, aligned.
//
// It recorded only the name. `buildAggregate` reads `GroupByExprs` to decide
// whether a key is DERIVED — a value one of the two engines has to materialize
// into a hidden slot — so with the list empty every grouping-set key looked
// like a column of the input, and `GROUP BY ROLLUP (g + 1)` was refused outright
// with `GROUP BY key "g + 1" is not a column of its input` while the stage DAG
// answered it as a plain GROUP BY (#778).
func TestGroupingConstructsCarryTheirParsedTerms(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"grouping-sets", "SELECT g + 1, h, COUNT(*) FROM t " +
			"GROUP BY GROUPING SETS ((g + 1), (h))", []string{"g + 1", "h"}},
		{"rollup", "SELECT g + 1, COUNT(*) FROM t GROUP BY ROLLUP (g + 1)",
			[]string{"g + 1"}},
		{"cube", "SELECT g + 1, h * 2, COUNT(*) FROM t GROUP BY CUBE (g + 1, h * 2)",
			[]string{"g + 1", "h * 2"}},
		{"plain-columns", "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP (a, b)",
			[]string{"a", "b"}},
		{"a-delimited-identifier-is-a-NAME", `SELECT "g + 1", COUNT(*) FROM t ` +
			`GROUP BY ROLLUP ("g + 1")`, []string{"g + 1"}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			info := groupingInfo(t, tc.sql)
			if got := strings.Join(info.GroupBy, ","); got != strings.Join(tc.want, ",") {
				t.Fatalf("GroupBy = %q, want %q", got, strings.Join(tc.want, ","))
			}
			if len(info.GroupByExprs) != len(info.GroupBy) {
				t.Fatalf("GroupByExprs has %d entries for %d keys — buildAggregate reads them "+
					"pairwise and treats a short list as 'no ASTs at all', which is what made a "+
					"derived grouping-set key unmaterializable (#778)",
					len(info.GroupByExprs), len(info.GroupBy))
			}
			for i, e := range info.GroupByExprs {
				if e == nil {
					t.Errorf("GroupByExprs[%d] is nil for key %q", i, info.GroupBy[i])
					continue
				}
				// The AST and the recorded name are two renderings of ONE term.
				if got := GroupKeyName(e); got != info.GroupBy[i] {
					t.Errorf("GroupByExprs[%d] renders as %q, key %d is recorded as %q",
						i, got, i, info.GroupBy[i])
				}
			}
		})
	}

	// The delimited spelling binds as a NAME, not as arithmetic — ADR-0026 §2c,
	// asserted here because a grouping-set term goes through a different parse
	// path from a simple GROUP BY term and could have grown its own rule.
	t.Run("a-delimited-term-is-a-column-reference", func(t *testing.T) {
		info := groupingInfo(t, `SELECT "g + 1", COUNT(*) FROM t GROUP BY ROLLUP ("g + 1")`)
		if _, isRef := info.GroupByExprs[0].(*ColRef); !isRef {
			t.Fatalf("ROLLUP (\"g + 1\") parsed to %T; a delimited identifier is a NAME and "+
				"re-reading it as structure is the defect ADR-0026 §2c settles", info.GroupByExprs[0])
		}
	})
}

// The key POSITIONS every grouping set indexes into come from `info.GroupBy`,
// so its order has to be a function of the QUERY.
//
// The union was collected in a Go map, whose iteration order is randomised, so
// the same query planned a different key order on different runs. Nothing
// caught it because the values are read back by NAME — which is why this
// asserts the parse and not an answer.
func TestGroupingSetsTermOrderIsDeterministic(t *testing.T) {
	const sql = "SELECT d, c, b, a, COUNT(*) FROM t " +
		"GROUP BY GROUPING SETS ((d, c), (b), (a, d))"
	var first string
	for i := 0; i < 64; i++ {
		got := strings.Join(groupingInfo(t, sql).GroupBy, ",")
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("run %d recorded GROUP BY %q where run 0 recorded %q — the key order a "+
				"grouping set indexes into is not a function of the query", i, got, first)
		}
	}
	// FIRST APPEARANCE, which is the only order a reader can predict from the
	// text, and the one that keeps `(d, c)` indexing 0,1.
	if first != "d,c,b,a" {
		t.Errorf("GROUP BY recorded as %q, want first-appearance order \"d,c,b,a\"", first)
	}
}

func groupingInfo(t *testing.T, sql string) *SelectInfo {
	t.Helper()
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract %q: %v", sql, err)
	}
	return info
}
