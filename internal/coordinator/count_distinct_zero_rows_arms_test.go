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

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A PLAIN AGGREGATE BESIDE A DISTINCT ONE, OVER ZERO ROWS AND OTHERWISE, ON
// EVERY ARM (#1692, the stage-DAG twin of #1584).
//
// The coverage table is generated: a plain aggregate {COUNT(*), COUNT(x),
// SUM(x), MIN(x), AVG(x), AVG(x) and COUNT(*) together} beside a distinct one
// {COUNT(DISTINCT x), COUNT(DISTINCT x % 3), two COUNT(DISTINCT)s on different
// columns, SUM(DISTINCT x)} × the input {zero rows, one row, every row, a
// column with NULLs} × the shape {ungrouped, GROUP BY g, GROUP BY ROLLUP(g),
// GROUPING SETS ((), ()), an ungrouped HAVING, a FILTER on the plain
// aggregate, under a scalar subquery, under UNION ALL}.
// testdata/count_distinct_zero_rows_pg17.tsv is PostgreSQL 17.11's answer for
// every cell, value and declared type (TestCountDistinctZeroRowsMeasure
// regenerates it); testdata/count_distinct_zero_rows_kept.tsv lists the cells
// an arm answers otherwise, each a residual with its reason.
//
// At b0e0f116 the three DAG arms answered NULL for an ungrouped COUNT beside a
// lone COUNT(DISTINCT) over zero rows, and no row at all under an ungrouped
// HAVING, where the single-process arms and PostgreSQL answer 0. The two-level
// DISTINCT rewrite re-sums COUNT partials and restores the 0 in its finalizing
// projection, `coalesce(<count>, 0) AS <count>`; the stage planner's
// absorbAggregateOutputProjection declines every function call, and because
// the rewrite publishes the coalesce under the very name of the SUM it wraps,
// nothing downstream noticed the projection was gone (#1692).
//
// cz_t's x has a duplicate (10, 30) so a distinct count differs from a count;
// xn has NULLs; one row's group key is NULL.

func czTable() tmdTable {
	i32 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt32, Nullable: true} }
	tbl := tmdTable{name: "cz_t", schema: parquet.Schema{Columns: []parquet.Column{
		i32("id"), i32("g"), i32("x"), i32("xn"), i32("y")}}}
	v := func(p *int32) any {
		if p == nil {
			return nil
		}
		return *p
	}
	n := func(i int32) *int32 { return &i }
	for _, r := range [][5]*int32{
		{n(1), n(1), n(10), n(10), n(100)},
		{n(2), n(1), n(10), nil, n(100)},
		{n(3), n(2), n(20), n(20), n(200)},
		{n(4), n(2), n(30), nil, n(300)},
		{n(5), n(3), n(30), n(30), n(300)},
		{n(6), nil, n(40), n(40), n(400)},
	} {
		tbl.rows = append(tbl.rows, map[string]any{"id": v(r[0]), "g": v(r[1]), "x": v(r[2]), "xn": v(r[3]), "y": v(r[4])})
	}
	return tbl
}

const czPGFixture = `DROP TABLE IF EXISTS cz_t;
CREATE TABLE cz_t (id integer, g integer, x integer, xn integer, y integer);
INSERT INTO cz_t VALUES (1,1,10,10,100),(2,1,10,NULL,100),(3,2,20,20,200),(4,2,30,NULL,300),(5,3,30,30,300),(6,NULL,40,40,400);`

func czCells() []cwCell {
	plains := []struct{ name, agg string }{
		{"countStar", "COUNT(*)"}, {"countX", "COUNT(X)"}, {"sumX", "SUM(X)"}, {"minX", "MIN(X)"}, {"avgX", "AVG(X)"},
		// AVG's division and COUNT's zero in ONE finalizing projection.
		{"avgXAndCountStar", "AVG(X) AS v, COUNT(*)"},
	}
	// Each distinct list names its outputs b (and c).
	distincts := []struct{ name, list string }{
		{"countDistinct", "COUNT(DISTINCT X) AS b"},
		{"countDistinctExpr", "COUNT(DISTINCT X % 3) AS b"},
		{"twoCountDistinct", "COUNT(DISTINCT X) AS b, COUNT(DISTINCT y) AS c"},
		{"sumDistinct", "SUM(DISTINCT X) AS b"},
	}
	inputs := []struct{ name, col, where string }{
		{"zero", "x", " WHERE id < 0"},
		{"one", "x", " WHERE id = 1"},
		{"all", "x", ""},
		{"nulls", "xn", ""},
	}
	var out []cwCell
	for _, in := range inputs {
		for _, p := range plains {
			for _, d := range distincts {
				pa := strings.ReplaceAll(p.agg, "X", in.col)
				da := strings.ReplaceAll(d.list, "X", in.col)
				// The FILTER keeps the odd ids: on the one-row input it keeps
				// the row, on the zero-row input there is nothing to keep.
				pf := pa + " FILTER (WHERE id % 2 = 1)"
				from := "FROM cz_t" + in.where
				key := in.name + "/" + p.name + "/" + d.name + "/"
				out = append(out,
					cwCell{key + "ungrouped", fmt.Sprintf("SELECT %s AS a, %s %s", pa, da, from)},
					cwCell{key + "grouped", fmt.Sprintf("SELECT g, %s AS a, %s %s GROUP BY g", pa, da, from)},
					cwCell{key + "rollup", fmt.Sprintf("SELECT g, %s AS a, %s %s GROUP BY ROLLUP(g)", pa, da, from)},
					cwCell{key + "emptySets", fmt.Sprintf("SELECT %s AS a, %s %s GROUP BY GROUPING SETS ((), ())", pa, da, from)},
					cwCell{key + "having", fmt.Sprintf("SELECT %s AS a, %s %s HAVING COUNT(*) >= 0", pa, da, from)},
					cwCell{key + "filter", fmt.Sprintf("SELECT %s AS a, %s %s", pf, da, from)},
					cwCell{key + "scalarSubquery", fmt.Sprintf(
						"SELECT (SELECT s.a FROM (SELECT %s AS a, %s %s) s WHERE s.b IS NOT NULL OR s.b IS NULL) AS a", pa, da, from)},
					cwCell{key + "unionAll", fmt.Sprintf("SELECT %s AS a, %s %s UNION ALL SELECT %s AS a, %s FROM cz_t WHERE id < 0",
						pa, da, from, pa, da)},
				)
			}
		}
	}
	return out
}

// czRender sorts the rows of ftEngineRender's form: grouped shapes return
// their groups in no defined order.
func czRender(s string) string {
	head, rows, ok := strings.Cut(s, " | ")
	if !ok {
		return s
	}
	r := strings.Split(rows, "; ")
	sort.Strings(r)
	return head + " | " + strings.Join(r, "; ")
}

const czAnswersPath = "testdata/count_distinct_zero_rows_pg17.tsv"

// TestCountDistinctZeroRowsMeasure runs every cell against PostgreSQL
// (CZ_PG_DSN, e.g. postgres://wadjet:wadjet@127.0.0.1:55472/wadjet_oracle) and
// rewrites the answer file; skipped otherwise.
func TestCountDistinctZeroRowsMeasure(t *testing.T) {
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
	b.WriteString("# PostgreSQL 17.11 answers for count_distinct_zero_rows_arms_test.go (TestCountDistinctZeroRowsMeasure).\n")
	for _, c := range czCells() {
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
	if err := os.WriteFile(czAnswersPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func czStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	tbl := czTable()
	if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
		t.Fatalf("create %s: %v", tbl.name, err)
	}
	ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2})
	if err := ing.Ingest(ctx, tbl.rows); err != nil {
		t.Fatalf("ingest %s: %v", tbl.name, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", tbl.name, err)
	}
	return db
}

// czArms is cwArms over cz_t: the five arms, every answer row-sorted.
func czArms(t *testing.T, ctx context.Context) []cwArm {
	t.Helper()
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, []tmdTable{czTable()})
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single, spilled := czStandalone(t, ctx, 0), czStandalone(t, ctx, 512*1024)
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	var spillMu sync.Mutex
	return []cwArm{
		{"single", func(sql string) string { return czRender(cwRunSingle(ctx, single, sql)) }},
		{"spilled512k", func(sql string) string {
			spillMu.Lock()
			defer spillMu.Unlock()
			restoreDrain := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(2)
			defer func() { restoreRuns(); exec.ForceAggDrainEvery(restoreDrain) }()
			return czRender(cwRunSingle(ctx, spilled, sql))
		}},
		{"dag", func(sql string) string { return czRender(cwRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) string { return czRender(cwRunDAG(ctx, coordB, sql)) }},
		{"dag-morsel4", func(sql string) string { return czRender(cwRunDAG(ctx, coordM, sql)) }},
	}
}

// TestCountDistinctZeroRowsDump writes every arm's answer for every cell
// (CZ_DUMP=<path>): the base / tip measurement the landing note compares.
func TestCountDistinctZeroRowsDump(t *testing.T) {
	path := os.Getenv("CZ_DUMP")
	if path == "" {
		t.Skip("CZ_DUMP unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := czArms(t, ctx)
	cells := czCells()
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

const czKeptPath = "testdata/count_distinct_zero_rows_kept.tsv"

type czKeptLine struct{ want, why string }

// czReadKept reads the kept file: name, arms (all | dag | single), answer,
// why-code; keyed "<name>@<arms>".
func czReadKept(t *testing.T) map[string]czKeptLine {
	t.Helper()
	f, err := os.Open(czKeptPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]czKeptLine{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fl := strings.Split(line, "\t")
		if len(fl) != 4 || (fl[1] != "all" && fl[1] != "dag" && fl[1] != "single") {
			t.Fatalf("malformed kept line %q", line)
		}
		key := fl[0] + "@" + fl[1]
		if _, dup := out[key]; dup {
			t.Fatalf("kept line %s listed twice", key)
		}
		out[key] = czKeptLine{want: fl[2], why: fl[3]}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// czKeptFor is the kept line for one cell on one arm: an "all" line, or the
// line for the arm's family (the three stage-DAG arms, the two single-process
// arms).
func czKeptFor(kept map[string]czKeptLine, cell, arm string) (czKeptLine, bool) {
	if k, ok := kept[cell+"@all"]; ok {
		return k, true
	}
	family := "single"
	if strings.HasPrefix(arm, "dag") {
		family = "dag"
	}
	k, ok := kept[cell+"@"+family]
	return k, ok
}

func TestCountDistinctZeroRowsEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the plain-beside-distinct table")
	}
	answers := cwReadTSV(t, czAnswersPath)
	kept := czReadKept(t)
	cells := czCells()
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
				if ans == pg {
					t.Errorf("%s [%s]: the kept line now agrees with PostgreSQL (%s): delete it\n  sql %s", c.name, arm.name, k.why, c.sql)
				} else if ans != k.want {
					t.Errorf("%s [%s]\n  sql  %s\n  got  %s\n  kept %s (%s)", c.name, arm.name, c.sql, ans, k.want, k.why)
				}
				continue
			}
			if ans != pg {
				t.Errorf("%s [%s]\n  sql  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, ans, pg)
				continue
			}
			agree++
		}
	}
	if len(cells) < 700 || agree < 2000 {
		t.Fatalf("%d cells, %d (cell, arm) agreeing, %d kept: the table must discriminate", len(cells), agree, keptN)
	}
	t.Logf("%d cells × %d arms: %d agree with PostgreSQL 17.11, %d kept", len(cells), len(arms), agree, keptN)
}
