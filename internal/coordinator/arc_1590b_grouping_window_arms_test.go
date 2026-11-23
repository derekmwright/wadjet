// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1590 (the P1 line): GROUPING(...) written inside a WINDOW's spec over a
// grouping-set query — `count(*) OVER (PARTITION BY GROUPING(k))` — answered
// wrong rows on every arm, for a plain column key and a computed one alike.
// The builder's window-spec aggregate hoist took GROUPING for an aggregate
// (it is in the parser's aggregate list so it can be found inside an
// expression) and published it as one; the physical planner maps an
// aggregate name it does not know to COUNT, so the window partitioned on
// count(k) per group, not on which grouping set produced the row. The SELECT
// list, HAVING and ORDER BY already gave GROUPING its own slot.
//
// The matrix crosses the GROUPING argument (a plain column, COALESCE, ||,
// lower(), integer arithmetic, a CAST, an alias, an ordinal, a respaced
// repetition, two arguments, a key missing from a set), the grouping construct
// (ROLLUP of one, two and three keys, CUBE, GROUPING SETS with and without the
// empty set, plain GROUP BY), the position (window PARTITION BY, ORDER BY,
// both, the window's argument, beside the window in the SELECT list, HAVING,
// the query's ORDER BY, the CASE total-row idiom, a derived table) and the
// data (a real NULL group, duplicates, one group, no rows), with the window
// replaced by a plain projection as controls. testdata/
// arc_1590b_grouping_window_pg17.tsv holds PostgreSQL 17.11's answer for every
// cell (postgres:17-alpine, initdb --locale=C --encoding=UTF8).

func u1590bTables() []tmdTable {
	t := tmdTable{name: "gw_t", schema: parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "t", Type: parquet.TypeString, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}}
	type r struct {
		id   int64
		s, t any
		n    any
	}
	for _, x := range []r{
		{1, "hi", "x", int64(10)}, {2, "hx", "y", int64(20)}, {3, "A", "x", int64(30)},
		{4, "ab", "y", int64(40)}, {5, nil, "x", nil}, {6, "z", nil, int64(60)}, {7, "hi", "y", int64(70)},
	} {
		t.rows = append(t.rows, map[string]any{"id": x.id, "s": x.s, "t": x.t, "n": x.n})
	}
	return []tmdTable{t}
}

// u1590bPGFixture is u1590bTables as PostgreSQL DDL.
const u1590bPGFixture = `CREATE TABLE gw_t (id bigint, s text, t text, n bigint);
INSERT INTO gw_t VALUES (1,'hi','x',10),(2,'hx','y',20),(3,'A','x',30),(4,'ab','y',40),(5,NULL,'x',NULL),(6,'z',NULL,60),(7,'hi','y',70);`

// u1590bKeys are the GROUPING arguments: {K} is written in GROUP BY, {G} in
// GROUPING() — the same expression, except where the cell is about a
// different spelling of it.
var u1590bKeys = []struct{ name, k, g string }{
	{"col", "s", "s"},
	{"coalesce", "COALESCE(s,'hi')", "COALESCE(s,'hi')"},
	{"respaced", "COALESCE(s,'hi')", "COALESCE( s , 'hi' )"},
	{"concat", "s || 'a'", "s || 'a'"},
	{"lower", "lower(s)", "lower(s)"},
	{"arith", "n + 1", "n + 1"},
	{"cast", "CAST(n AS TEXT)", "CAST(n AS TEXT)"},
}

// u1590bKeyTemplates run per key.
var u1590bKeyTemplates = []struct{ name, q string }{
	// the failing position, with and without GROUPING in the SELECT list
	{"w/part", `SELECT GROUPING({G}) AS g, count(*) AS n, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{"w/part-only", `SELECT count(*) AS n, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{"w/part-key", `SELECT {K} AS k, count(*) AS n, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{"w/order", `SELECT {K} AS k, row_number() OVER (ORDER BY GROUPING({G}), {K}) AS r FROM gw_t GROUP BY ROLLUP ({K})`},
	{"w/sum-count", `SELECT {K} AS k, sum(count(*)) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{"w/arg-sum", `SELECT {K} AS k, sum(GROUPING({G})) OVER () AS c FROM gw_t GROUP BY ROLLUP ({K})`},
	{"w/cube", `SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY CUBE ({K})`},
	{"w/sets-empty", `SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY GROUPING SETS (({K}), ())`},
	{"w/plain", `SELECT {K} AS k, count(*) OVER (PARTITION BY GROUPING({G})) AS c FROM gw_t GROUP BY {K}`},
	// controls: GROUPING() projected, no window
	{"p/proj", `SELECT {K} AS k, GROUPING({G}) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP ({K})`},
	{"p/having", `SELECT {K} AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP ({K}) HAVING GROUPING({G}) = 0`},
	{"p/order", `SELECT GROUPING({G}) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP ({K}) ORDER BY GROUPING({G}) DESC, count(*) DESC LIMIT 2`},
}

// u1590bFixed are the cells outside the per-key grid.
var u1590bFixed = []struct{ name, sql string }{
	// two and three keys
	{"x/rollup2-part-s", `SELECT s, t, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s, t)`},
	{"x/rollup2-part-t", `SELECT COALESCE(s,'hi') AS k, t, count(*) OVER (PARTITION BY GROUPING(t)) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'), t)`},
	{"x/rollup2-part-two-args", `SELECT GROUPING(s, t) AS g, count(*) OVER (PARTITION BY GROUPING(s, t)) AS c FROM gw_t GROUP BY ROLLUP (s, t)`},
	{"x/rollup2-part-two-args-rev", `SELECT GROUPING(t, s) AS g, count(*) OVER (PARTITION BY GROUPING(t, s)) AS c FROM gw_t GROUP BY ROLLUP (s, t)`},
	{"x/rollup2-part-two-computed", `SELECT count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'), lower(t))) AS c, count(*) AS n FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'), lower(t))`},
	{"x/rollup3-part", `SELECT t, id % 2 AS m, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY ROLLUP (t, COALESCE(s,'hi'), id % 2)`},
	{"x/rollup3-part-proj", `SELECT t, id % 2 AS m, count(*) AS n, GROUPING(COALESCE(s,'hi')) AS g FROM gw_t GROUP BY ROLLUP (t, COALESCE(s,'hi'), id % 2)`},
	{"x/sets-missing-key", `SELECT s, t, count(*) OVER (PARTITION BY GROUPING(t)) AS c FROM gw_t GROUP BY GROUPING SETS ((s), (t))`},
	{"x/sets-missing-key-proj", `SELECT s, t, GROUPING(t) AS g FROM gw_t GROUP BY GROUPING SETS ((s), (t))`},
	{"x/sets-no-empty", `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY GROUPING SETS ((COALESCE(s,'hi')))`},
	{"x/cube2-part", `SELECT s, t, count(*) OVER (PARTITION BY GROUPING(s, t)) AS c FROM gw_t GROUP BY CUBE (s, t)`},
	// alias, ordinal
	{"x/alias-group", `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY ROLLUP (k)`},
	{"x/alias-grouping", `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(k)) AS c FROM gw_t GROUP BY ROLLUP (k)`},
	{"x/ordinal-group", `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY ROLLUP (1)`},
	{"x/ordinal-group-proj", `SELECT COALESCE(s,'hi') AS k, GROUPING(COALESCE(s,'hi')) AS g FROM gw_t GROUP BY ROLLUP (1)`},
	// both positions, rank, first_value, a frame, the argument
	{"x/both-rank", `SELECT COALESCE(s,'hi') AS k, rank() OVER (PARTITION BY GROUPING(COALESCE(s,'hi')) ORDER BY count(*) DESC) AS r FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))`},
	{"x/both-row-number", `SELECT s, row_number() OVER (PARTITION BY GROUPING(s) ORDER BY GROUPING(s), s) AS r FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/first-value", `SELECT s, first_value(count(*)) OVER (PARTITION BY GROUPING(s) ORDER BY count(*) DESC, s) AS f FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/first-value-arg", `SELECT s, first_value(GROUPING(s)) OVER (PARTITION BY s ORDER BY count(*)) AS f FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/frame", `SELECT s, sum(count(*)) OVER (PARTITION BY GROUPING(s) ORDER BY s ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/max-arg-part", `SELECT s, max(GROUPING(s)) OVER (PARTITION BY s) AS m FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/arg-plus", `SELECT s, sum(GROUPING(s) + 1) OVER () AS m FROM gw_t GROUP BY ROLLUP (s)`},
	// GROUPING inside an expression in the window spec, and the CASE idiom
	{"x/case-part", `SELECT s, count(*) OVER (PARTITION BY CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE 'one' END) AS c FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/expr-part", `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi')) + 1) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))`},
	{"x/case-label", `SELECT CASE WHEN GROUPING(COALESCE(s,'hi')) = 1 THEN 'ALL' ELSE COALESCE(s,'hi') END AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))`},
	{"x/case-label-col", `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) AS n FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/case-label-window", `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) AS n, sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS tot FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/case-label-share", `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) * 100 / sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS pct FROM gw_t GROUP BY ROLLUP (s)`},
	{"x/case-label-share-int", `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) * 100 / CAST(sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS BIGINT) AS pct FROM gw_t GROUP BY ROLLUP (s)`},
	// a derived table over the grouped rows
	{"x/derived-agg", `SELECT g, count(*) AS rows_, sum(n) AS n FROM (SELECT GROUPING(COALESCE(s,'hi')) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))) q GROUP BY g`},
	{"x/derived-window", `SELECT g, n, count(*) OVER (PARTITION BY g) AS c FROM (SELECT GROUPING(s) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP (s)) q`},
	// data: one group, a NULL-only group, no rows
	{"d/one-group", `SELECT s, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id IN (1, 7) GROUP BY ROLLUP (s)`},
	{"d/null-group", `SELECT s, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id = 5 GROUP BY ROLLUP (s)`},
	{"d/null-group-proj", `SELECT s, count(*) AS n, GROUPING(s) AS g FROM gw_t WHERE id = 5 GROUP BY ROLLUP (s)`},
	{"d/empty", `SELECT s, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id > 100 GROUP BY ROLLUP (s)`},
	{"d/empty-proj", `SELECT s, count(*) AS n, GROUPING(s) AS g FROM gw_t WHERE id > 100 GROUP BY ROLLUP (s)`},
	{"d/empty-plain", `SELECT s, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id > 100 GROUP BY s`},
	// the issue's statement, over TEXT
	{"i/issue-text", `SELECT GROUPING(COALESCE(s,'\x6869')) AS g, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'\x6869'))) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,'\x6869'))`},
	{"i/issue-cast", `SELECT GROUPING(COALESCE(s,CAST('hi' AS TEXT))) AS g, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,CAST('hi' AS TEXT)))) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,CAST('hi' AS TEXT)))`},
}

func u1590bCells() []struct{ name, sql string } {
	var out []struct{ name, sql string }
	for _, k := range u1590bKeys {
		for _, tp := range u1590bKeyTemplates {
			q := strings.NewReplacer("{K}", k.k, "{G}", k.g).Replace(tp.q)
			out = append(out, struct{ name, sql string }{k.name + "/" + tp.name, q})
		}
	}
	return append(out, u1590bFixed...)
}

func u1590bPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_1590b_grouping_window_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	return out
}

// u1590bResidual are cells still wrong at this fix, each with its cause; the
// test holds them WRONG so a later fix has to delete the line.
// Neither is a GROUPING defect and both answer the same at the base.
var u1590bResidual = map[string]string{
	// bigint / numeric keeps ADR-0024's fixed division scale (21 fraction
	// digits here), where PostgreSQL's select_div_scale keeps 16: the
	// partitions and values agree (x/case-label-share-int is right).
	"x/case-label-share": "fixed numeric division scale (ADR-0024), not PostgreSQL's select_div_scale",
	// GROUPING(<output alias>) is refused 42703 on both sides; the message
	// reads `unknown column "k"` here, `column "k" does not exist` there.
	"x/alias-grouping": "42703 on both sides; message wording differs",
}

// TestArc1590bGroupingInWindowEveryArm holds every cell to PostgreSQL 17.11's
// answer on all five arms. U1590B_DUMP=<path> writes every (cell, arm)
// verdict instead of failing (the base-against-tip comparison);
// U1590B_LIST=<path> writes the cells for measuring PostgreSQL.
func TestArc1590bGroupingInWindowEveryArm(t *testing.T) {
	cells := u1590bCells()
	if p := os.Getenv("U1590B_LIST"); p != "" {
		var b strings.Builder
		for _, c := range cells {
			fmt.Fprintf(&b, "%s\t%s\n", c.name, c.sql)
		}
		if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if testing.Short() {
		t.Skip("-short: five arms over the #1590 GROUPING-in-window matrix")
	}
	answers := u1590bPGAnswers(t)
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	for name := range u1590bResidual {
		if _, ok := answers[name]; !ok {
			t.Fatalf("residual %s names no cell", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := byArmsOver(t, ctx, u1590bTables())
	dump := os.Getenv("U1590B_DUMP")
	var dumpLines []string
	compared := 0
	for _, c := range cells {
		want := answers[c.name]
		gots := make([]string, len(arms))
		oks := make([]bool, len(arms))
		var wg sync.WaitGroup
		for i, arm := range arms {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := arm.run(c.sql)
				gots[i], oks[i] = u1590Judge(want, res, err)
			}()
		}
		wg.Wait()
		for i, arm := range arms {
			if dump != "" {
				verdict := "right"
				if !oks[i] {
					verdict = "wrong"
					if strings.HasPrefix(gots[i], "ERR ") {
						verdict = "refused"
					}
				}
				dumpLines = append(dumpLines, fmt.Sprintf("%s\t%s\t%s\t%s", c.name, arm.name, verdict, gots[i]))
				continue
			}
			_, residual := u1590bResidual[c.name]
			switch {
			case oks[i] && residual:
				t.Errorf("%s [%s] now AGREES with PostgreSQL: delete its u1590bResidual line", c.name, arm.name)
			case !oks[i] && !residual:
				t.Errorf("%s\n  sql  %s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, arm.name, gots[i], want)
			}
			compared++
		}
	}
	if dump != "" {
		sort.Strings(dumpLines)
		if err := os.WriteFile(dump, []byte(strings.Join(dumpLines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if want := len(arms) * len(cells); compared != want {
		t.Errorf("%d (cell, arm) comparisons, want %d", compared, want)
	}
}
