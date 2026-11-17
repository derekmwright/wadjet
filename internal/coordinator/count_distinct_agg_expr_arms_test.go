// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// AN AGGREGATE INSIDE AN EXPRESSION BESIDE A DISTINCT ONE, ON EVERY ARM
// (#1692, round 2: the expression-over-AVG class).
//
// At b0e0f116 `SELECT AVG(x) + COUNT(*) AS a, COUNT(DISTINCT x) AS b FROM
// cz_t` answered NULL,4 on the three stage-DAG arms over a NON-empty table
// (29.333333,4 on the single-process arms). The two-level DISTINCT rewrite's
// finalizing Project held BOTH `__tl_avgsum_0 / __tl_avgcnt_0 AS __agg_0`
// (AVG's division, under the synthetic name the SELECT expression reads) and
// `coalesce(__agg_1, 0) AS __agg_1` (COUNT's zero); the stage planner's
// absorbAggregateOutputProjection declined the coalesce and with it the whole
// projection, so the final aggregate stage emitted only the partials
// (__tl_avgsum_0, __tl_avgcnt_0, __agg_1, b). The gather computes
// `__agg_0 + __agg_1` itself — an expression rename the reachability check
// does not inspect — over a batch with no __agg_0, and the sum read NULL. An
// AVG under its own SELECT name (not inside an expression) was instead refused
// onto the single-process path by that check, and an expression over AVG with
// no COUNT beside it carried no coalesce, so neither answered wrong.
//
// The boundary table: {AVG, SUM, MIN, MAX, COUNT, STDDEV}(x) × {bare, + 1,
// abs(), CASE, CAST AS bigint, + COUNT(*)} × {alone, beside COUNT(*) AS c} ×
// {COUNT(DISTINCT x), COUNT(DISTINCT x) and COUNT(DISTINCT y), SUM(DISTINCT x)}
// × {ungrouped, GROUP BY g} × {every row, one row, zero rows}, plus the
// zero-row consumers of a COUNT beside a COUNT(DISTINCT) (a coalesce, a CASE,
// a derived table filtered on the count, a CROSS JOIN with a derived count, a
// scalar subquery under HAVING COUNT(DISTINCT x) >= 0).
// testdata/count_distinct_agg_expr_pg17.tsv is PostgreSQL 17.11's answer per
// cell; testdata/count_distinct_agg_expr_kept.tsv lists the cells an arm
// answers otherwise (residuals, wrong at the base too, with the single arm's
// answer as the control). Float digits that differ by summation order compare
// within a few ulps (czClose), as nxFloatSumClose does.

func ceCells() []cwCell {
	aggs := []struct{ name, f string }{
		{"avg", "AVG"}, {"sum", "SUM"}, {"min", "MIN"}, {"max", "MAX"}, {"count", "COUNT"}, {"stddev", "STDDEV"},
	}
	wraps := []struct{ name, f string }{
		{"bare", "%s"},
		{"arith", "%s + 1"},
		{"func", "abs(%s)"},
		{"case", "CASE WHEN %s > 15 THEN 'hi' ELSE 'lo' END"},
		{"cast", "CAST(%s AS bigint)"},
		{"plusCount", "%s + COUNT(*)"},
	}
	companions := []struct{ name, list string }{
		{"alone", ""},
		{"besideCount", ", COUNT(*) AS c"},
	}
	distincts := []struct{ name, list string }{
		{"countDistinct", "COUNT(DISTINCT x) AS b"},
		{"twoCountDistinct", "COUNT(DISTINCT x) AS b, COUNT(DISTINCT y) AS d"},
		{"sumDistinct", "SUM(DISTINCT x) AS b"},
	}
	inputs := []struct{ name, where string }{
		{"all", ""}, {"one", " WHERE id = 1"}, {"zero", " WHERE id < 0"},
	}
	var out []cwCell
	for _, in := range inputs {
		for _, a := range aggs {
			for _, w := range wraps {
				for _, c := range companions {
					for _, d := range distincts {
						item := fmt.Sprintf(w.f, a.f+"(x)") + " AS a" + c.list
						key := strings.Join([]string{in.name, a.name, w.name, c.name, d.name}, "/") + "/"
						from := "FROM cz_t" + in.where
						out = append(out,
							cwCell{key + "ungrouped", fmt.Sprintf("SELECT %s, %s %s", item, d.list, from)},
							cwCell{key + "grouped", fmt.Sprintf("SELECT g, %s, %s %s GROUP BY g", item, d.list, from)},
						)
					}
				}
			}
		}
	}
	const z = "FROM cz_t WHERE id < 0"
	out = append(out,
		cwCell{"consumer/coalesce", "SELECT coalesce(COUNT(*), 5) AS a, COUNT(DISTINCT x) AS b " + z},
		cwCell{"consumer/case", "SELECT CASE WHEN COUNT(*) = 0 THEN 'empty' ELSE 'rows' END AS a, COUNT(DISTINCT x) AS b " + z},
		cwCell{"consumer/derivedWhere", "SELECT d.a, d.b FROM (SELECT COUNT(*) AS a, COUNT(DISTINCT x) AS b " + z + ") d WHERE d.a = 0"},
		cwCell{"consumer/crossJoin", "SELECT t.id, d.a FROM cz_t t CROSS JOIN (SELECT COUNT(*) AS a, COUNT(DISTINCT x) AS b " + z + ") d WHERE t.id <= 2"},
		cwCell{"consumer/scalarHaving", "SELECT (SELECT COUNT(*) " + z + " HAVING COUNT(DISTINCT x) >= 0) = 0 AS a"},
		cwCell{"consumer/scalarHavingFilter", "SELECT id FROM cz_t WHERE id < (SELECT COUNT(*) " + z + " HAVING COUNT(DISTINCT x) >= 0) + 3"},
	)
	return out
}

const (
	ceAnswersPath = "testdata/count_distinct_agg_expr_pg17.tsv"
	ceKeptPath    = "testdata/count_distinct_agg_expr_kept.tsv"
)

// czClose reports whether two renderings are the same answer: the same
// header and row count, every field equal or both doubles within four ulps
// (summation order differs by arm).
func czClose(got, want string) bool {
	if got == want {
		return true
	}
	gh, gr, gok := strings.Cut(got, " | ")
	wh, wr, wok := strings.Cut(want, " | ")
	if !gok || !wok || gh != wh {
		return false
	}
	// Only a double column is compared within ulps: a numeric's digits are
	// its scale, and 10.000000 is not PostgreSQL's 10.0000000000000000.
	cols := strings.Split(gh, ",")
	isDouble := make([]bool, len(cols))
	for i, c := range cols {
		_, typ, _ := strings.Cut(c, ":")
		isDouble[i] = typ == "double precision" || typ == "real"
	}
	grows, wrows := strings.Split(gr, "; "), strings.Split(wr, "; ")
	if len(grows) != len(wrows) {
		return false
	}
	for i := range grows {
		gf, wf := strings.Split(grows[i], ","), strings.Split(wrows[i], ",")
		if len(gf) != len(wf) || len(gf) != len(cols) {
			return false
		}
		for k := range gf {
			if gf[k] == wf[k] {
				continue
			}
			if !isDouble[k] {
				return false
			}
			a, aerr := strconv.ParseFloat(gf[k], 64)
			b, berr := strconv.ParseFloat(wf[k], 64)
			if aerr != nil || berr != nil || math.Signbit(a) != math.Signbit(b) {
				return false
			}
			ua, ub := math.Float64bits(math.Abs(a)), math.Float64bits(math.Abs(b))
			if ua > ub+4 || ub > ua+4 {
				return false
			}
		}
	}
	return true
}

func TestCZCloseIsNarrow(t *testing.T) {
	want := "a:double precision,b:bigint | 11.69045194450012,4"
	for _, tc := range []struct {
		got  string
		want bool
	}{
		{"a:double precision,b:bigint | 11.690451944500122,4", true},
		{"a:double precision,b:bigint | 11.69045194450012,4", true},
		{"a:double precision,b:bigint | 11.6904519445,4", false},
		{"a:double precision,b:bigint | 11.69045194450012,5", false},
		{"a:numeric,b:bigint | 11.69045194450012,4", false},
		{"a:double precision,b:bigint | 11.69045194450012,4.0000000000000001", false},
		{"a:double precision,b:bigint | NULL,4", false},
		{"(0 rows)", false},
	} {
		if got := czClose(tc.got, want); got != tc.want {
			t.Errorf("czClose(%q) = %v, want %v", tc.got, got, tc.want)
		}
	}
}

// TestCountDistinctAggExprMeasure runs every cell against PostgreSQL
// (CZ_PG_DSN) and rewrites the answer file; skipped otherwise.
func TestCountDistinctAggExprMeasure(t *testing.T) {
	dsn := os.Getenv("CZ_PG_DSN")
	if dsn == "" {
		t.Skip("CZ_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s';"+czPGFixture).ReadAll(); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 answers for count_distinct_agg_expr_arms_test.go (TestCountDistinctAggExprMeasure).\n")
	for _, c := range ceCells() {
		r := conn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
		var ans string
		if r.Err != nil {
			pe, ok := r.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("%s: %v", c.name, r.Err)
			}
			ans = "ERR " + pe.Code
		} else {
			header := make([]string, len(r.FieldDescriptions))
			for i, fd := range r.FieldDescriptions {
				header[i] = fd.Name + ":" + ftPGTypeName(fd.DataTypeOID)
			}
			rows := make([]string, len(r.Rows))
			for i, row := range r.Rows {
				f := make([]string, len(row))
				for j, v := range row {
					if v == nil {
						f[j] = "NULL"
					} else {
						f[j] = string(v)
					}
				}
				rows[i] = strings.Join(f, ",")
			}
			ans = czRender(ftRender(header, rows))
		}
		b.WriteString(c.name + "\t" + ans + "\n")
	}
	if err := os.WriteFile(ceAnswersPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestCountDistinctAggExprDump writes every arm's answer for every cell
// (CE_DUMP=<path>): the base / tip measurement the landing note compares.
func TestCountDistinctAggExprDump(t *testing.T) {
	path := os.Getenv("CE_DUMP")
	if path == "" {
		t.Skip("CE_DUMP unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := czArms(t, ctx)
	cells := ceCells()
	got := cwRunAll(t, arms, cells)
	var b strings.Builder
	for _, c := range cells {
		for i, arm := range arms {
			b.WriteString(c.name + "\t" + arm.name + "\t" + got[c.name][i] + "\n")
		}
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestCountDistinctAggExprEveryArm holds every cell on every arm to
// PostgreSQL's answer, or to its kept line (the same reader and families as
// TestCountDistinctZeroRowsEveryArm).
func TestCountDistinctAggExprEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the aggregate-expression-beside-distinct table")
	}
	answers := cwReadTSV(t, ceAnswersPath)
	kept := czReadKeptFrom(t, ceKeptPath)
	cells := ceCells()
	known := map[string]bool{}
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		known[c.name] = true
	}
	for k := range kept {
		cell, _, _ := strings.Cut(k, "@")
		if !known[cell] {
			t.Fatalf("kept line %s names no generated cell", k)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := czArms(t, ctx)
	got := cwRunAll(t, arms, cells)
	agree, keptN := 0, 0
	for _, c := range cells {
		pg := answers[c.name]
		for i, arm := range arms {
			ans := got[c.name][i]
			if k, ok := czKeptFor(kept, c.name, arm.name); ok {
				keptN++
				if czClose(ans, pg) {
					t.Errorf("%s [%s]: the kept line now agrees with PostgreSQL (%s): delete it\n  sql %s", c.name, arm.name, k.why, c.sql)
				} else if !czClose(ans, k.want) {
					t.Errorf("%s [%s]\n  sql  %s\n  got  %s\n  kept %s (%s)", c.name, arm.name, c.sql, ans, k.want, k.why)
				}
				continue
			}
			if !czClose(ans, pg) {
				t.Errorf("%s [%s]\n  sql  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, ans, pg)
				continue
			}
			agree++
		}
	}
	if len(cells) < 1000 || agree < 2000 {
		t.Fatalf("%d cells, %d (cell, arm) agreeing, %d kept: the table must discriminate", len(cells), agree, keptN)
	}
	t.Logf("%d cells × %d arms: %d agree with PostgreSQL 17.11, %d kept", len(cells), len(arms), agree, keptN)
}
