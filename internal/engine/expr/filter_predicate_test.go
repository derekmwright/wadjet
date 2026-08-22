package expr

import (
	"fmt"
	"testing"
)

// TestEvalBoolIsCollapse holds evalBoolIsCollapse's list to its definition:
// for every operator on it, EvalBool must be exactly `val && !null` of
// EvalBoolNull, because FilterPredicate exchanges one for the other. An
// operator that grows its own two-valued EvalBool — a different
// short-circuit, a different NULL rule — has to leave the list, and this
// test is what says so.
func TestEvalBoolIsCollapse(t *testing.T) {
	b := nullBearingBatch()
	cases := []struct {
		sql  string
		node any // the concrete node the expression must compile to
	}{
		{"d >= '1980-01-01'", &CmpTemporalLit{}},
		{"d < '1980-01-01'", &CmpTemporalLit{}},
		{"n IN (5, 50, 500)", &In{}},
		{"n NOT IN (5, 50)", &In{}},
		{"n BETWEEN 10 AND 1000", &Between{}},
		{"n NOT BETWEEN 10 AND 1000", &Between{}},
		{"s LIKE 'abc%'", &Like{}},
		{"s NOT LIKE 'abc%'", &Like{}},
		{"NOT (n IN (5, 50))", &Not{}},
		{"n IS DISTINCT FROM 5", &IsDistinctFrom{}},
		{"n IS NOT DISTINCT FROM 5", &IsDistinctFrom{}},
		{"s = ''", &ColEmptyStr{}},
		{"s <> ''", &ColEmptyStr{}},
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			e := compileExprSQL(t, tc.sql)
			nodeType := fmt.Sprintf("%T", e)
			if want := fmt.Sprintf("%T", tc.node); nodeType != want {
				t.Fatalf("compiled to %s, want %s", nodeType, want)
			}
			if !evalBoolIsCollapse(e) {
				t.Fatalf("%s is not listed in evalBoolIsCollapse", nodeType)
			}
			seen[nodeType] = true
			be, ok := e.(BoolExpr)
			if !ok {
				t.Fatalf("%s does not implement BoolExpr", nodeType)
			}
			ne, ok := e.(BoolNullExpr)
			if !ok {
				t.Fatalf("%s does not implement BoolNullExpr", nodeType)
			}
			pred := FilterPredicate(e)
			for row := 0; row < b.Len; row++ {
				val, null := ne.EvalBoolNull(b, row)
				collapsed := val && !null
				if got := be.EvalBool(b, row); got != collapsed {
					t.Fatalf("row %d: EvalBool=%v, collapse of EvalBoolNull(%v,%v)=%v",
						row, got, val, null, collapsed)
				}
				if got := pred(b, row); got != collapsed {
					t.Fatalf("row %d: FilterPredicate=%v, want %v", row, got, collapsed)
				}
			}
		})
	}
	// Every listed operator must have an instance above, or the list is
	// asserting something nothing checks.
	for _, want := range []string{
		"*expr.CmpTemporalLit", "*expr.In", "*expr.Between", "*expr.Like",
		"*expr.Not", "*expr.IsDistinctFrom", "*expr.ColEmptyStr",
	} {
		if !seen[want] {
			t.Errorf("evalBoolIsCollapse lists %s but no case exercises it", want)
		}
	}
}

// TestFilterPredicateMatchesEvalBool covers the operators FilterPredicate
// keeps on EvalBool — the connectives, the NULL tests, the typed compares —
// where the two protocols are separate implementations rather than a wrapper
// and its wrapped. The predicate must still answer what the filter loop
// answered before it existed.
func TestFilterPredicateMatchesEvalBool(t *testing.T) {
	b := nullBearingBatch()
	for _, sql := range []string{
		"n > 10 AND s LIKE 'abc%'",
		"n > 10 OR s LIKE 'abc%'",
		"n IS NULL",
		"n IS NOT NULL",
		"n = 50",
		"f > 100.0",
		"s = 'abcdef'",
		"NOT (n > 10 AND s LIKE 'abc%')",
		"s IS NULL",
	} {
		t.Run(sql, func(t *testing.T) {
			e := compileExprSQL(t, sql)
			pred := FilterPredicate(e)
			for row := 0; row < b.Len; row++ {
				var want bool
				if be, ok := e.(BoolExpr); ok {
					want = be.EvalBool(b, row)
				} else {
					v := e.Eval(b, row)
					bv, isBool := v.(bool)
					want = isBool && bv
				}
				if got := pred(b, row); got != want {
					t.Fatalf("row %d: FilterPredicate=%v, want %v", row, got, want)
				}
			}
		})
	}
}
