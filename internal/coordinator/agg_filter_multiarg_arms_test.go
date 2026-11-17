// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// THE AGGREGATES THAT ANSWER FILTER NOW, ON ALL FIVE ARMS (#1690).
//
// At base 197385ed the FILTER rewrite kept only the first argument, so every
// multi-argument aggregate written with FILTER was refused by the arity check
// (`corr takes 2 arguments, got 1`). The rewrite now nulls the argument the
// parser's aggFilterArg declares and keeps the rest, so these answer, and the
// table pins that they answer the same aggregate over the rows the predicate
// keeps on every arm:
//
//   - MIN_BY, MAX_BY, CORR, COVAR_SAMP, COVAR_POP and the QUANTILE spelling
//     null their first argument;
//   - PERCENTILE_CONT / PERCENTILE_DISC null their SECOND (the first is the
//     fraction literal);
//   - OHLCV(ts, price, volume) nulls the price;
//   - STRING_AGG keeps its separator (it answered the default ',' at base;
//     length() makes the cell independent of the arms' row order).
//
// The window `id BETWEEN 48 AND 56` keeps rows whose arguments are themselves
// NULL (typematrix: m1 is NULL at id 50, c_ts at 52, m2 at 56), so a kept row
// the aggregate must skip is in every "some" cell. Each want is PostgreSQL
// 17.11's over the same rows (ids 0..99 of typematrix loaded there; MIN_BY,
// MAX_BY and OHLCV spelled out with array_agg/min/max/sum, since PostgreSQL
// has neither), floats rendered to ten significant digits. Each "some" cell
// is also compared on its own arm with the same aggregate over the
// pre-filtered subquery.
func TestAggFilterMultiArgFiveArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the typemx table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := brArms(t, ctx)

	const win = "id BETWEEN 48 AND 56"
	type class struct {
		call                 string // the aggregate call, without FILTER
		some, grouped        string // PostgreSQL 17.11
		zero, none, rollZero string
	}
	classes := []class{
		{"min_by(m1, m2)", "rows=1 51.5",
			"rows=8 0,14.5 | 1,15.5 | 2,30.5 | 3,17.5 | 4,NULL | 5,26.5 | 6,27.5 | NULL,25.5", "", "", ""},
		{"max_by(m1, m2)", "rows=1 49.5",
			"rows=8 0,21.5 | 1,22.5 | 2,23.5 | 3,24.5 | 4,NULL | 5,19.5 | 6,20.5 | NULL,25.5", "", "", ""},
		{"corr(m1, m2)", "rows=1 -0.6516656642",
			"rows=8 0,0.1725248422 | 1,0.1725248422 | 2,-1 | 3,1 | 4,NULL | 5,-1 | 6,-1 | NULL,NULL", "", "", ""},
		{"covar_samp(m1, m2)", "rows=1 -49.35714286",
			"rows=8 0,31.5 | 1,31.5 | 2,-140 | 3,171.5 | 4,NULL | 5,-140 | 6,-140 | NULL,NULL", "", "", ""},
		{"covar_pop(m1, m2)", "rows=1 -42.30612245",
			"rows=8 0,21 | 1,21 | 2,-70 | 3,85.75 | 4,NULL | 5,-70 | 6,-70 | NULL,0", "", "", ""},
		{"quantile_cont(m1, 0.5)", "rows=1 53",
			"rows=8 0,21.5 | 1,22.5 | 2,27 | 3,21 | 4,18.5 | 5,23 | 6,24 | NULL,25.5", "", "", ""},
		{"quantile_disc(m1, 0.5)", "rows=1 52.5",
			"rows=8 0,21.5 | 1,22.5 | 2,23.5 | 3,17.5 | 4,18.5 | 5,19.5 | 6,20.5 | NULL,25.5", "", "", ""},
		{"percentile_cont(0.5, m1)", "rows=1 53",
			"rows=8 0,21.5 | 1,22.5 | 2,27 | 3,21 | 4,18.5 | 5,23 | 6,24 | NULL,25.5", "", "", ""},
		{"percentile_disc(0.5, m1)", "rows=1 52.5",
			"rows=8 0,21.5 | 1,22.5 | 2,23.5 | 3,17.5 | 4,18.5 | 5,19.5 | 6,20.5 | NULL,25.5", "", "", ""},
		// ohlcv: open/high/low/close/volume/vwap over the kept rows whose ts,
		// price and volume are all non-NULL (48, 49, 51, 53, 54, 55):
		// PostgreSQL's spelled-out answer is open 48.5, low 48.5, high 55.5,
		// close 55.5, volume 192.5, vwap 50.66883117.
		{"ohlcv(c_ts, m1, m2)", "rows=1 " + ohlcvBar(48.5, 55.5, 48.5, 55.5, 192.5, 50.66883117),
			"rows=8 " + strings.Join([]string{
				"0," + ohlcvBar(14.5, 28.5, 14.5, 28.5, 75.25, 22.337209302325583),
				"1," + ohlcvBar(15.5, 29.5, 15.5, 29.5, 96.25, 23.154545454545456),
				"2," + ohlcvBar(23.5, 30.5, 23.5, 30.5, 97.5, 25.564102564102566),
				"3," + ohlcvBar(17.5, 24.5, 17.5, 24.5, 102.5, 22.673170731707316),
				"4,NULL",
				"5," + ohlcvBar(19.5, 26.5, 19.5, 26.5, 41.5, 19.626506024096386),
				"6," + ohlcvBar(20.5, 27.5, 20.5, 27.5, 55.5, 21.47747747747748),
				"NULL," + ohlcvBar(25.5, 25.5, 25.5, 25.5, 82.75, 25.5),
			}, " | "), "", "", ""},
		{"length(string_agg(CAST(id AS TEXT), ' / '))", "rows=1 42",
			"rows=8 0,12 | 1,12 | 2,12 | 3,7 | 4,2 | 5,7 | 6,7 | NULL,2", "", "", ""},
	}

	type cell struct{ name, sql, want, control string }
	var cells []cell
	for _, c := range classes {
		f := func(p string) string {
			if strings.HasPrefix(c.call, "length(") {
				inner := strings.TrimSuffix(strings.TrimPrefix(c.call, "length("), ")")
				return "length(" + inner + " FILTER (WHERE " + p + "))"
			}
			return c.call + " FILTER (WHERE " + p + ")"
		}
		key := strings.SplitN(c.call, "(", 2)[0]
		if key == "length" {
			key = "string_agg_sep"
		}
		cells = append(cells,
			cell{key + "/some", "SELECT " + f(win) + " AS v FROM typemx", c.some,
				"SELECT " + c.call + " AS v FROM typemx WHERE " + win},
			cell{key + "/zero", "SELECT " + f(win) + " AS v FROM typemx WHERE id < 0", "rows=1 NULL", ""},
			cell{key + "/filterNone", "SELECT " + f("id < 0") + " AS v FROM typemx", "rows=1 NULL", ""},
			cell{key + "/zeroRollup", "SELECT g, " + f(win) + " AS v FROM typemx WHERE id < 0 GROUP BY ROLLUP(g)", "rows=1 NULL,NULL", ""},
			cell{key + "/zeroGrouped", "SELECT g, " + f(win) + " AS v FROM typemx WHERE id < 0 GROUP BY g", "rows=0 ", ""},
		)
		if c.grouped != "" {
			cells = append(cells, cell{key + "/grouped",
				"SELECT g, " + f("id BETWEEN 14 AND 30") + " AS v FROM typemx WHERE id < 40 GROUP BY g", c.grouped, ""})
		}
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v", tc.sql, arm.name, err)
					continue
				}
				got := afRender(res)
				t.Logf("%-14s %s", arm.name, got)
				if got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, tc.want)
				}
				if tc.control == "" {
					continue
				}
				cres, err := arm.run(tc.control)
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  control refused: %v", tc.control, arm.name, err)
					continue
				}
				if cg := afRender(cres); cg != got {
					t.Errorf("arm %s: FILTER answers %s, the pre-filtered subquery %s\n  %s\n  %s", arm.name, got, cg, tc.sql, tc.control)
				}
			}
		})
	}
}

// ohlcvBar renders one bar the way afRender prints a ROW value.
func ohlcvBar(open, high, low, close, volume, vwap float64) string {
	return fmt.Sprintf("{close:%s high:%s low:%s open:%s volume:%s vwap:%s}",
		afFloat(close), afFloat(high), afFloat(low), afFloat(open), afFloat(volume), afFloat(vwap))
}

func afFloat(f float64) string { return strconv.FormatFloat(f, 'g', 10, 64) }

// afRender is brRender with every float (and every float field of a ROW)
// printed to ten significant digits, so a merge order's last-ulp difference
// does not read as a wrong answer.
func afRender(res *oracle.Result) string {
	var out []string
	for _, r := range brCells(res) {
		parts := make([]string, len(r))
		for i, v := range r {
			parts[i] = afCell(v)
		}
		out = append(out, strings.Join(parts, ","))
	}
	sort.Strings(out)
	return fmt.Sprintf("rows=%d %s", len(out), strings.Join(out, " | "))
}

func afCell(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case float64:
		return afFloat(x)
	case float32:
		return afFloat(float64(x))
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var kv []string
		for _, k := range keys {
			s := afCell(x[k])
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				s = afFloat(f)
			}
			kv = append(kv, k+":"+s)
		}
		return "{" + strings.Join(kv, " ") + "}"
	}
	return fmt.Sprint(v)
}
