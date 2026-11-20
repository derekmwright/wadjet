// SPDX-License-Identifier: MIT

package wadjet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A GROUPING-SET AGGREGATE ANSWERS THE SAME DRAINED OR SPILLED AS IN MEMORY
// (#1700). Grouping sets always take the aggregate's raw-row spill branch: a
// drain boxes the input rows by column name and Finalize rebuilds them into a
// batch under the schema the aggregate recorded from its first input batch.
// Under the morsel pipeline a clone can consume every row while the primary
// consumes none (the warmup batch was empty — a pruned or fully filtered row
// group), and the barrier merge moved the clone's buffered rows to the primary
// without the schema they are read back under. The rows came back as a batch
// with no columns: `SELECT count(*) FROM gs_i x WHERE x.k = 2 GROUP BY
// GROUPING SETS (())` answered 0 and `... GROUP BY ROLLUP (x.a)` refused with
// 0A000 "GROUP BY key "x.a" is not a column of its input".
//
// The table (testdata/grouping_set_drain_pg17.tsv) is the enumeration: two
// fixtures (gsd_i, four rows in two-row row groups, so a filter can empty the
// warmup row group; gsd_b, 6000 rows, big enough to spill under the budget) x
// grouping construct {GROUPING SETS (()), ((),()), ((a),()), ((a),(b)),
// ROLLUP(a), ROLLUP(a,b), CUBE(a,b)} x WHERE {none, some rows, one group, zero
// rows} x aggregate {count(*), sum(b), count(DISTINCT b), min_by(b, a*1000+b),
// count(*) with GROUPING(key)} x key spelling {bare, qualified, expression,
// ordinal}. Every want is PostgreSQL 17.11's (min_by measured as array_agg's
// first element in the same order); a fourth field is a RESIDUAL pinned at
// this engine's answer. Each cell runs plain, with every aggregate forced to
// drain, and (gsd_b) under a budget small enough that the raw-row buffer
// really reaches disk.
//
// Regenerate: GSD_PG_DSN=postgres://... go test -run TestGroupingSetDrainGenerate ./wadjet/

const gsdFixturePG = `DROP TABLE IF EXISTS gsd_i; DROP TABLE IF EXISTS gsd_b;
CREATE TABLE gsd_i (k integer, a integer, b integer);
INSERT INTO gsd_i VALUES (1,10,100),(1,10,200),(1,20,100),(2,30,300);
CREATE TABLE gsd_b (k integer, a integer, b integer);
INSERT INTO gsd_b SELECT i % 3 + 1, (i % 5 + 1) * 10, (i % 7 + 1) * 100 FROM generate_series(0, 5999) i`

type gsdCell struct{ name, sql, pg, pin string }

// gsdCellSQL enumerates the cells; the PostgreSQL spelling differs only in
// min_by, which PostgreSQL 17 does not have.
func gsdCellSQL() (names, ours, theirs []string) {
	type spell struct{ name, sel1, sel, a, b, gfn string }
	spells := []spell{
		{"bare", "a", "a, b", "a", "b", "GROUPING(a)"},
		{"qualified", "x.a", "x.a, x.b", "x.a", "x.b", "GROUPING(x.a)"},
		{"expr", "a + 1", "a + 1, b + 1", "a + 1", "b + 1", "GROUPING(a + 1)"},
		{"ordinal", "a", "a, b", "1", "2", "GROUPING(a)"},
	}
	forms := []struct {
		name string
		keys int // how many keys the select list carries
		grp  func(s spell) string
	}{
		{"setsE", 0, func(spell) string { return "GROUPING SETS (())" }},
		{"setsEE", 0, func(spell) string { return "GROUPING SETS ((), ())" }},
		{"setsAE", 1, func(s spell) string { return "GROUPING SETS ((" + s.a + "), ())" }},
		{"setsAB", 2, func(s spell) string { return "GROUPING SETS ((" + s.a + "), (" + s.b + "))" }},
		{"rollupA", 1, func(s spell) string { return "ROLLUP (" + s.a + ")" }},
		{"rollupAB", 2, func(s spell) string { return "ROLLUP (" + s.a + ", " + s.b + ")" }},
		{"cubeAB", 2, func(s spell) string { return "CUBE (" + s.a + ", " + s.b + ")" }},
	}
	wheres := []struct{ name, cond string }{
		{"all", ""}, {"some", " WHERE x.b <> 200"}, {"onegroup", " WHERE x.a = 30"}, {"zero", " WHERE x.k = 99"},
	}
	aggs := []struct{ name, ours, theirs string }{
		{"count", "count(*)", "count(*)"},
		{"sum", "sum(x.b)", "sum(x.b)"},
		{"countDistinct", "count(DISTINCT x.b)", "count(DISTINCT x.b)"},
		{"minBy", "min_by(x.b, x.a * 1000 + x.b)", "(array_agg(x.b ORDER BY x.a * 1000 + x.b))[1]"},
		{"grouping", "count(*), {G}", "count(*), {G}"},
	}
	for _, tbl := range []string{"gsd_i", "gsd_b"} {
		for _, f := range forms {
			for _, w := range wheres {
				for _, ag := range aggs {
					for _, sp := range spells {
						if f.keys == 0 && sp.name != "bare" {
							continue
						}
						sel := ""
						switch f.keys {
						case 1:
							sel = sp.sel1 + ", "
						case 2:
							sel = sp.sel + ", "
						}
						mk := func(agg string) string {
							agg = strings.ReplaceAll(agg, "{G}", sp.gfn)
							return "SELECT " + sel + agg + " FROM " + tbl + " x" + w.cond + " GROUP BY " + f.grp(sp)
						}
						name := tbl + "/" + f.name + "/" + w.name + "/" + ag.name
						if f.keys > 0 {
							name += "/" + sp.name
						}
						names = append(names, name)
						ours = append(ours, mk(ag.ours))
						theirs = append(theirs, mk(ag.theirs))
					}
				}
			}
		}
	}
	return names, ours, theirs
}

// gsdRender is an answer as sorted rows joined by " | ", NULL spelled out,
// (none) for no rows.
func gsdRender(rows [][]any) string {
	if len(rows) == 0 {
		return "(none)"
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		parts := make([]string, len(r))
		for j, v := range r {
			if v == nil {
				parts[j] = "NULL"
				continue
			}
			parts[j] = fmt.Sprint(v)
		}
		out[i] = strings.Join(parts, ",")
	}
	sort.Strings(out)
	return strings.Join(out, " | ")
}

func TestGroupingSetDrainGenerate(t *testing.T) {
	dsn := os.Getenv("GSD_PG_DSN")
	if dsn == "" {
		t.Skip("GSD_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(gsdFixturePG, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// Keep the pins already recorded: a regeneration re-measures PostgreSQL,
	// not this engine.
	pins := map[string]string{}
	if old, err := gsdLoad(); err == nil {
		for _, c := range old {
			pins[c.name] = c.pin
		}
	}
	names, ours, theirs := gsdCellSQL()
	var b strings.Builder
	b.WriteString("# name\tSQL\tPostgreSQL 17.11 answer\tresidual pin (empty: none)\n")
	for i := range names {
		ans := func() string {
			rows, err := conn.Query(ctx, theirs[i])
			if err != nil {
				return gsdPGErr(err)
			}
			defer rows.Close()
			var all [][]any
			for rows.Next() {
				v, err := rows.Values()
				if err != nil {
					return gsdPGErr(err)
				}
				all = append(all, v)
			}
			if err := rows.Err(); err != nil {
				return gsdPGErr(err)
			}
			return gsdRender(all)
		}()
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\n", names[i], ours[i], ans, pins[names[i]])
	}
	if err := os.WriteFile("testdata/grouping_set_drain_pg17.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func gsdPGErr(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}

func gsdLoad() ([]gsdCell, error) {
	f, err := os.Open("testdata/grouping_set_drain_pg17.tsv")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []gsdCell
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.Split(line, "\t")
		if len(p) != 4 {
			return nil, fmt.Errorf("malformed line %q", line)
		}
		out = append(out, gsdCell{p[0], p[1], p[2], p[3]})
	}
	return out, sc.Err()
}

// gsdOpen is an embedded DB over both fixtures. gsd_i is one file of two-row
// row groups, so `WHERE x.k = 2` leaves the first (warmup) row group empty.
func gsdOpen(t *testing.T, ctx context.Context, budget int64) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: budget, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32, Nullable: true},
		{Name: "a", Type: parquet.TypeInt32, Nullable: true},
		{Name: "b", Type: parquet.TypeInt32, Nullable: true},
	}}
	var tiny, big []map[string]any
	for _, r := range [][3]int32{{1, 10, 100}, {1, 10, 200}, {1, 20, 100}, {2, 30, 300}} {
		tiny = append(tiny, map[string]any{"k": r[0], "a": r[1], "b": r[2]})
	}
	for i := int32(0); i < 6000; i++ {
		big = append(big, map[string]any{"k": i%3 + 1, "a": (i%5 + 1) * 10, "b": (i%7 + 1) * 100})
	}
	for _, tb := range []struct {
		name string
		rows []map[string]any
		rg   int
	}{{"gsd_i", tiny, 2}, {"gsd_b", big, 512}} {
		if err := db.CreateTable(ctx, tb.name, sch, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(tb.name, sch, nil, ingest.Config{MaxBufferRows: len(tb.rows) + 1, RowGroupSize: tb.rg})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func gsdAnswer(ctx context.Context, db *DB, sql string) string {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return "ERR " + sqlerr.StateOf(err)
	}
	rows := make([][]any, len(res.Rows))
	for i := range res.Rows {
		rows[i] = res.Cells(i)
	}
	return gsdRender(rows)
}

// TestGroupingSetDrain runs the table plain, force-drained and (gsd_b) under a
// spilling budget. GSD_MEASURE=1 logs every cell instead of asserting.
func TestGroupingSetDrain(t *testing.T) {
	ctx := context.Background()
	cells, err := gsdLoad()
	if err != nil {
		t.Fatal(err)
	}
	measure := os.Getenv("GSD_MEASURE") != ""
	check := func(t *testing.T, arm string, c gsdCell, got string) {
		if measure {
			status := "RIGHT"
			if got != c.pg {
				status = "DIFF"
			}
			t.Logf("CELL\t%s\t%s\t%s\tgot=%s\tpg=%s", status, c.name, arm, got, c.pg)
			return
		}
		switch {
		case c.pin == "" && got != c.pg:
			t.Errorf("%s [%s]: %s\n got  %s\n want %s (PostgreSQL 17.11)", c.name, arm, c.sql, got, c.pg)
		case c.pin != "" && got != c.pin:
			t.Errorf("%s [%s]: %s\n got    %s\n pinned %s (a residual; PostgreSQL 17.11 answers %s)", c.name, arm, c.sql, got, c.pin, c.pg)
		}
	}
	t.Run("plain", func(t *testing.T) {
		db := gsdOpen(t, ctx, 0)
		for _, c := range cells {
			check(t, "plain", c, gsdAnswer(ctx, db, c.sql))
		}
	})
	t.Run("forced", func(t *testing.T) {
		db := gsdOpen(t, ctx, 1<<20)
		defer exec.ForceAggDrainEvery(exec.ForceAggDrainEvery(1))
		before := exec.ForcedRawRowDrains.Load()
		for _, c := range cells {
			check(t, "forced", c, gsdAnswer(ctx, db, c.sql))
		}
		if exec.ForcedRawRowDrains.Load() == before {
			t.Error("no raw-row drain was forced: the arm did not exercise the drain")
		}
	})
	t.Run("budget", func(t *testing.T) {
		// A budget small enough that the raw-row buffer reaches disk on most
		// gsd_b cells (which ones depends on the pressure each query meets:
		// clones never spill, so only the cells whose rows reach the primary
		// can). ForceSmallSpillRuns lowers the raw-row flush target below its
		// 64 MiB floor. The arm asserts that cells spilled and how many; the
		// forced arm above is the deterministic one. At this budget nearly
		// every row-group load waits out the file-load admission wait (2s)
		// before it is forced; across 440 cells that alone ran past CI's 8m
		// package timeout, so the arm forces at once instead.
		db := gsdOpen(t, ctx, 1<<20)
		defer exec.ForceSmallSpillRuns(4096)()
		defer physical.ForceFileLoadReserveWait(0)()
		spilled, ran := 0, 0
		for _, c := range cells {
			if !strings.HasPrefix(c.name, "gsd_b/") {
				continue
			}
			before := exec.RawRowSpillFiles.Load()
			got := gsdAnswer(ctx, db, c.sql)
			ran++
			if exec.RawRowSpillFiles.Load() > before {
				spilled++
			}
			check(t, "budget", c, got)
		}
		t.Logf("budget arm: %d of %d cells spilled a raw-row buffer to disk", spilled, ran)
		if spilled == 0 {
			t.Error("no raw-row buffer reached disk: the budget arm did not spill")
		}
	})
}
