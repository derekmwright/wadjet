package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// parseSelectInfo parses one SELECT into the shape BuildFromSelect takes.
func parseSelectInfo(sql string) (*plansql.SelectInfo, error) {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, err
	}
	return plansql.ExtractSelect(parsed)
}

// aggOf parses one SELECT and returns its single aggregate expression.
func aggOf(t *testing.T, sql string) AggExpr {
	t.Helper()
	info, err := parseSelectInfo(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	plan, err := BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build %q: %v", sql, err)
	}
	var found *AggExpr
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Type == NodeAggregate && len(n.AggExprs) > 0 && found == nil {
			found = &n.AggExprs[0]
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(plan)
	if found == nil {
		t.Fatalf("no aggregate in the plan for %q", sql)
	}
	return *found
}

// TestAggExtraArgs_ReachThePlan is the parser half of #353: every argument
// after the first used to be dropped, so CORR lost its second column,
// STRING_AGG its separator and PERCENTILE_CONT its fraction — each of them
// then answering with something plausible instead of failing.
func TestAggExtraArgs_ReachThePlan(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		wantCol  string
		wantCol2 string
		wantSep  string
		wantPct  float64
	}{
		{name: "corr", sql: "SELECT CORR(a, b) AS c FROM t", wantCol: "a", wantCol2: "b"},
		{name: "covar_samp", sql: "SELECT COVAR_SAMP(a, b) AS c FROM t", wantCol: "a", wantCol2: "b"},
		{name: "covar_pop", sql: "SELECT COVAR_POP(a, b) AS c FROM t", wantCol: "a", wantCol2: "b"},
		{name: "min_by", sql: "SELECT MIN_BY(label, k) AS m FROM t", wantCol: "label", wantCol2: "k"},
		{name: "max_by", sql: "SELECT MAX_BY(label, k) AS m FROM t", wantCol: "label", wantCol2: "k"},
		{name: "string_agg", sql: "SELECT STRING_AGG(p, '::') AS s FROM t", wantCol: "p", wantSep: "::"},
		// One argument is legal and means the default separator, which the
		// operator supplies rather than the planner.
		{name: "string_agg default sep", sql: "SELECT STRING_AGG(p) AS s FROM t", wantCol: "p"},
		// The fraction comes FIRST in this spelling, so the aggregated
		// column is the second argument and InputCol has to move to it.
		{name: "percentile_cont", sql: "SELECT PERCENTILE_CONT(0.9, v) AS p FROM t", wantCol: "v", wantPct: 0.9},
		{name: "percentile_disc", sql: "SELECT PERCENTILE_DISC(0.25, v) AS p FROM t", wantCol: "v", wantPct: 0.25},
		// DuckDB's spelling, arguments the other way round, same function.
		{name: "quantile_cont", sql: "SELECT quantile_cont(v, 0.9) AS p FROM t", wantCol: "v", wantPct: 0.9},
		{name: "quantile_disc", sql: "SELECT quantile_disc(v, 0.25) AS p FROM t", wantCol: "v", wantPct: 0.25},
		// A HAVING-only aggregate goes through a different construction
		// site in the builder, and a site that falls behind is a silent
		// wrong answer rather than a compile error.
		{name: "having", sql: "SELECT g FROM t GROUP BY g HAVING CORR(a, b) > 0", wantCol: "a", wantCol2: "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := aggOf(t, tc.sql)
			if got.InputCol != tc.wantCol {
				t.Errorf("InputCol = %q, want %q", got.InputCol, tc.wantCol)
			}
			if got.InputCol2 != tc.wantCol2 {
				t.Errorf("InputCol2 = %q, want %q", got.InputCol2, tc.wantCol2)
			}
			if got.Separator != tc.wantSep {
				t.Errorf("Separator = %q, want %q", got.Separator, tc.wantSep)
			}
			if got.Percentile != tc.wantPct {
				t.Errorf("Percentile = %v, want %v", got.Percentile, tc.wantPct)
			}
		})
	}
}

// TestAggExtraArgs_ArityIsChecked: the arity check is what makes the fields
// above trustworthy downstream — a spec for one of these functions either
// carries its extra argument or the query does not plan. Silently accepting
// a one-argument CORR is how it came to answer NULL.
func TestAggExtraArgs_ArityIsChecked(t *testing.T) {
	for _, tc := range []struct{ name, sql, wantErr string }{
		{"corr one arg", "SELECT CORR(a) AS c FROM t", "corr takes 2 arguments"},
		{"corr three args", "SELECT CORR(a, b, c) AS c FROM t", "corr takes 2 arguments"},
		{"min_by one arg", "SELECT MIN_BY(a) AS m FROM t", "min_by takes 2 arguments"},
		{"percentile one arg", "SELECT PERCENTILE_CONT(v) AS p FROM t", "percentile_cont takes 2 arguments"},
		{"percentile non-literal", "SELECT PERCENTILE_CONT(v, w) AS p FROM t", "must be a numeric fraction literal"},
		{"percentile out of range", "SELECT PERCENTILE_CONT(1.5, v) AS p FROM t", "outside [0, 1]"},
		{"string_agg non-literal sep", "SELECT STRING_AGG(p, q) AS s FROM t", "separator must be a string literal"},
		{"string_agg three args", "SELECT STRING_AGG(p, ',', 'x') AS s FROM t", "string_agg takes 1 or 2 arguments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info, err := parseSelectInfo(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			_, err = BuildFromSelect(info)
			if err == nil {
				t.Fatalf("%q planned without error", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}
