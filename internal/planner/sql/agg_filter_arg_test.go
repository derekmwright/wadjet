// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestAggFilterArgCoversRegistry holds every registered aggregate to one side
// of the FILTER ruling (#1690): it declares the argument a FILTER nulls
// (aggFilterArg), or FILTER is refused on it (aggFilterRefused). A new
// aggregate added to knownAggregates without either fails here, instead of
// inheriting a guess.
func TestAggFilterArgCoversRegistry(t *testing.T) {
	for name := range knownAggregates {
		_, declared := aggFilterArg[name]
		if declared == aggFilterRefused[name] {
			t.Errorf("aggregate %s: declared=%v refused=%v; it must be exactly one", name, declared, aggFilterRefused[name])
		}
	}
	for name := range aggFilterArg {
		if !knownAggregates[name] {
			t.Errorf("aggFilterArg names %s, which is not a registered aggregate", name)
		}
	}
}

// TestAggFilterWrapsTheDeclaredArgument pins which argument the rewrite nulls
// for each shape the table decided, and that every other argument passes
// through as written.
func TestAggFilterWrapsTheDeclaredArgument(t *testing.T) {
	for _, c := range []struct {
		sql     string
		wrapped int    // the argument that becomes CASE WHEN p THEN a END
		args    string // every argument as the planner sees it
	}{
		{"SELECT sum(x) FILTER (WHERE p) FROM t", 0, "case when p then x end"},
		{"SELECT string_agg(s, '-') FILTER (WHERE p) FROM t", 0, "case when p then s end|'-'"},
		{"SELECT corr(x, y) FILTER (WHERE p) FROM t", 0, "case when p then x end|y"},
		{"SELECT min_by(x, y) FILTER (WHERE p) FROM t", 0, "case when p then x end|y"},
		{"SELECT quantile_cont(x, 0.5) FILTER (WHERE p) FROM t", 0, "case when p then x end|0.5"},
		// The fraction comes first: the per-row value is the second.
		{"SELECT percentile_cont(0.5, x) FILTER (WHERE p) FROM t", 1, "0.5|case when p then x end"},
		{"SELECT percentile_disc(0.5, x) FILTER (WHERE p) FROM t", 1, "0.5|case when p then x end"},
		// ohlcv(ts, price, volume): the price.
		{"SELECT ohlcv(ts, px, vol) FILTER (WHERE p) FROM t", 1, "ts|case when p then px end|vol"},
		{"SELECT count(*) FILTER (WHERE p) FROM t", 0, "case when p then 1 end"},
	} {
		parsed, err := Parse(c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		info, err := ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		var args []string
		for _, a := range info.Columns[0].AggArgs {
			args = append(args, strings.ToLower(a.String()))
		}
		if got := strings.Join(args, "|"); got != c.args {
			t.Errorf("%s\n  arguments %q\n  want      %q", c.sql, got, c.args)
			continue
		}
		if _, ok := info.Columns[0].AggArgs[c.wrapped].(*CaseNode); !ok {
			t.Errorf("%s: argument %d is not the CASE", c.sql, c.wrapped)
		}
	}
}

// TestAggFilterRefusals pins the refusals by SQLSTATE and by the name the
// message carries. ResolveFunction is unset in this package's tests (the
// expression compiler installs it), so a scalar name reaches PostgreSQL's
// 42809 here; the 42883 for an unknown name is pinned end to end in
// wadjet.TestAggregateFilterEveryAggregate.
func TestAggFilterRefusals(t *testing.T) {
	for _, c := range []struct{ sql, state, word string }{
		{"SELECT g, grouping(g) FILTER (WHERE p) FROM t GROUP BY ROLLUP(g)", "0A000", "grouping"},
		{"SELECT lag(x) FILTER (WHERE p) OVER (ORDER BY id) FROM t", "0A000", "non-aggregate window functions"},
		{"SELECT row_number() FILTER (WHERE p) OVER (ORDER BY id) FROM t", "0A000", "non-aggregate window functions"},
		{"SELECT abs(x) FILTER (WHERE p) FROM t", "42809", "abs"},
		{"SELECT coalesce(x, 0) FILTER (WHERE p) FROM t", "42809", "coalesce"},
		{"SELECT sum(*) FILTER (WHERE p) FROM t", "0A000", "sum"},
		{"SELECT count() FILTER (WHERE p) FROM t", "0A000", "count"},
	} {
		_, err := Parse(c.sql)
		if err == nil {
			t.Errorf("%s: parsed, want %s naming %q", c.sql, c.state, c.word)
			continue
		}
		if sqlerr.StateOf(err) != c.state || !strings.Contains(err.Error(), c.word) || !strings.Contains(err.Error(), "FILTER") {
			t.Errorf("%s\n  refused %s %q\n  want    %s naming FILTER and %q", c.sql, sqlerr.StateOf(err), err.Error(), c.state, c.word)
		}
	}
}
