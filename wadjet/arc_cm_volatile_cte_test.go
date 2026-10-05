// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A VOLATILE CTE READ MORE THAN ONCE IS EVALUATED ONCE (#1531) — the embedded
// engine's gates: the spill cell, the stored row count of INSERT … WITH …
// UNION ALL, EXPLAIN VERBOSE, the planning-depth bound, and the deterministic
// CTE's plan shapes, which this rule must not move.
//
// PostgreSQL 17.11 answers every cell here (tooling/arcs/cm_cte_materialize,
// pg_raw.txt / pg2_raw.txt); a cell asserts the SHAPE of its answer — two
// references agree, so a difference is 0 and a count is equal — never a
// random value.

func cmOpen(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	load := func(name string, n int) {
		sch := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeFloat64}}}
		if err := db.CreateTable(ctx, name, sch, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		rows := make([]map[string]any, n)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(i + 1), "v": float64(i+1) * 1.5}
		}
		ing := db.NewIngester(name, sch, nil, ingest.Config{MaxBufferRows: n + 1, RowGroupSize: 4096})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", name, err)
		}
	}
	load("cm_p", 3)
	load("cm_big", 20000)
	if err := db.CreateTable(ctx, "cm_t", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64}, {Name: "r", Type: parquet.TypeString, Nullable: true}}}, nil); err != nil {
		t.Fatalf("create cm_t: %v", err)
	}
	return db
}

func cmAnswer(t *testing.T, db *DB, sql string) string {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		return "ERR " + err.Error()
	}
	var rows []string
	for i := range res.Rows {
		rows = append(rows, fmt.Sprint(res.Cells(i)...))
	}
	return strings.Join(rows, "; ")
}

// Two million rows of a volatile body, read twice under a 512 KiB budget: the
// one evaluation spills (engagement: physical.TestOnceCTEMaterializationSpills
// AndBothReferencesAgree) and the two references agree. At c67ebf5b each
// scalar subquery evaluated the body itself and the sums differed (3).
func TestArcCMVolatileCTEOverTheBudgetIsEvaluatedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: two million rows under a 512 KiB budget")
	}
	db := cmOpen(t, 512<<10)
	cells := []struct{ sql, want string }{
		{`WITH s AS (SELECT g AS id, random() AS r FROM generate_series(1, 2000000) g) ` +
			`SELECT count(*) FROM cm_p WHERE (SELECT sum(r) FROM s) <> (SELECT sum(r) FROM s) ` +
			`OR (SELECT count(*) FROM s) <> 2000000`, "0"},
		// The same body on a nested block, read by both sides of a join.
		{`SELECT count(*) FROM (WITH s AS (SELECT g AS id, random() AS r FROM generate_series(1, 200000) g) ` +
			`SELECT a.id FROM s a JOIN s b ON a.id = b.id WHERE a.r <> b.r) x`, "0"},
		// A stored relation sampled at 50 %, read by two scalar subqueries.
		{`WITH s AS (SELECT id, v FROM cm_big TABLESAMPLE BERNOULLI (50)) ` +
			`SELECT count(*) FROM cm_p WHERE (SELECT count(*) FROM s) <> (SELECT count(*) FROM s)`, "0"},
	}
	for _, c := range cells {
		for rep := 0; rep < 8; rep++ {
			if got := cmAnswer(t, db, c.sql); got != c.want {
				t.Fatalf("rep %d: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", rep, c.sql, got, c.want)
			}
		}
	}
}

// INSERT … WITH … SELECT … UNION ALL over a volatile CTE STORES one
// evaluation: every id is stored twice with one r. PostgreSQL 17.11 stores
// 100 rows and the read-back finds no id with two values (0); at c67ebf5b the
// embedded engine stored two evaluations and every one of the 50 ids
// carried two values (50).
func TestArcCMInsertWithUnionAllStoresOneEvaluation(t *testing.T) {
	db := cmOpen(t, 0)
	ctx := context.Background()
	bodies := []struct{ name, body, want string }{
		{"random", "SELECT id, CAST(random() AS TEXT) AS r FROM cm_big WHERE id <= 50", "100 0"},
		{"uuid", "SELECT id, CAST(uuid() AS TEXT) AS r FROM cm_big WHERE id <= 50", "100 0"},
		{"sum_random", "SELECT 1 AS id, CAST(sum(random()) AS TEXT) AS r FROM cm_big", "2 0"},
		{"tablesample", "SELECT id, CAST(v AS TEXT) AS r FROM cm_big TABLESAMPLE BERNOULLI (100)", "40000 0"},
	}
	for _, b := range bodies {
		for rep := 0; rep < 8; rep++ {
			if _, err := db.Execute(ctx, "DELETE FROM cm_t"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			stmt := "INSERT INTO cm_t WITH s AS (" + b.body + ") SELECT id, r FROM s UNION ALL SELECT id, r FROM s"
			if _, err := db.Execute(ctx, stmt); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
			got := cmAnswer(t, db, "SELECT (SELECT count(*) FROM cm_t), "+
				"(SELECT count(*) FROM (SELECT id FROM cm_t GROUP BY id HAVING count(DISTINCT r) <> 1) g)")
			if got != b.want {
				t.Fatalf("%s rep %d: stored rows / ids with two values = %s, want %s (PostgreSQL 17.11)",
					b.name, rep, got, b.want)
			}
		}
	}
}

// EXPLAIN VERBOSE names the volatile CTE the pipeline evaluates once, and
// names no deterministic one.
func TestArcCMExplainVerboseNamesTheOnceEvaluatedCTE(t *testing.T) {
	db := cmOpen(t, 0)
	got := cmAnswer(t, db, `EXPLAIN VERBOSE WITH s AS (SELECT id, random() AS r FROM cm_big), `+
		`d AS (SELECT id FROM cm_p) SELECT count(*) FROM s a JOIN s b ON a.id = b.id JOIN d ON d.id = a.id`)
	if !strings.Contains(got, "CTE s: volatile, evaluated once; every reference reads that result") {
		t.Errorf("EXPLAIN VERBOSE does not name the once-evaluated CTE s:\n%s", got)
	}
	if strings.Contains(got, "CTE d:") {
		t.Errorf("EXPLAIN VERBOSE names the deterministic CTE d:\n%s", got)
	}
	got = cmAnswer(t, db, `EXPLAIN VERBOSE SELECT count(*) FROM (WITH s AS (SELECT id FROM cm_big TABLESAMPLE BERNOULLI (5)) `+
		`SELECT a.id FROM s a JOIN s b ON a.id = b.id) x`)
	if !strings.Contains(got, "CTE s: volatile, evaluated once") {
		t.Errorf("EXPLAIN VERBOSE does not name a nested block's sampled CTE:\n%s", got)
	}
}

// PLANNING DEPTH (COMMON: a planner arc has a timing gate). Sixteen volatile
// CTEs each read twice from scalar subqueries, and a chain of sixteen whose
// last is read twice, plan and answer in milliseconds; the answer is
// PostgreSQL's 0 (every pair of references agrees).
func TestArcCMSixteenVolatileCTEsEachReadTwicePlanQuickly(t *testing.T) {
	db := cmOpen(t, 0)
	var with, conds []string
	for i := 1; i <= 16; i++ {
		with = append(with, fmt.Sprintf("c%d AS (SELECT sum(random()) AS r FROM cm_p)", i))
		conds = append(conds, fmt.Sprintf("(SELECT r FROM c%d) <> (SELECT r FROM c%d)", i, i))
	}
	flat := "WITH " + strings.Join(with, ", ") + " SELECT count(*) FROM cm_p WHERE " + strings.Join(conds, " OR ")
	chain := []string{"c1 AS (SELECT id, random() AS r FROM cm_p)"}
	for i := 2; i <= 16; i++ {
		chain = append(chain, fmt.Sprintf("c%d AS (SELECT id, r + 1 AS r FROM c%d)", i, i-1))
	}
	chained := "WITH " + strings.Join(chain, ", ") +
		" SELECT count(*) FROM c16 a JOIN c16 b ON a.id = b.id WHERE a.r <> b.r"
	for _, q := range []string{flat, chained} {
		start := time.Now()
		got := cmAnswer(t, db, q)
		elapsed := time.Since(start)
		if got != "0" {
			t.Errorf("%s\n  got %s, want 0 (PostgreSQL 17.11)", q, got)
		}
		t.Logf("elapsed %s", elapsed)
		if elapsed > 2*time.Second {
			t.Errorf("sixteen CTEs read twice took %s, want < 2s", elapsed)
		}
	}
}

// A DETERMINISTIC CTE KEEPS ITS PLAN: inlined at each reference, with the
// enclosing predicate pushed into it. The six shapes' EXPLAIN text is the
// text c67ebf5b printed (measured on both).
func TestArcCMDeterministicCTEPlanShapesAreUnchanged(t *testing.T) {
	db := cmOpen(t, 0)
	for _, c := range cmDeterministicShapes {
		if got := cmAnswer(t, db, "EXPLAIN "+c.sql); got != c.plan {
			t.Errorf("%s\n  got  %q\n  want %q (the plan at c67ebf5b)", c.sql, got, c.plan)
		}
	}
}

// cmDeterministicShapes: the plan c67ebf5b printed for each (measured on
// base and tip, identical; tooling/arcs/cm_cte_materialize/cm_author/r2).
var cmDeterministicShapes = []struct{ sql, plan string }{
	{"WITH s AS (SELECT id, v FROM cm_big) SELECT count(*) FROM s WHERE id < 10",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Filter: [id < 10];       Project: [id v];         Scan: cm_big"},
	{"WITH s AS (SELECT id, v FROM cm_big) SELECT count(*) FROM s a JOIN s b ON a.id = b.id WHERE a.id < 10",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Join: join ON a.id = b.id;       Filter: [a.id < 10];         Project: [id v];           Scan: cm_big;       Project: [id v];         Scan: cm_big"},
	{"WITH s AS (SELECT id, v FROM cm_big) SELECT count(*) FROM cm_p WHERE (SELECT max(v) FROM s WHERE id < 5) > 0",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Filter: [(SELECT max(v) FROM s WHERE id < 5) > 0];       Scan: cm_p"},
	{"SELECT count(*) FROM (WITH s AS (SELECT id, v FROM cm_big) SELECT a.id FROM s a JOIN s b ON a.id = b.id WHERE a.id < 10) x",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Project: [a.id];       Join: join ON a.id = b.id;         Filter: [a.id < 10];           Project: [id v];             Scan: cm_big;         Project: [id v];           Scan: cm_big"},
	{"WITH s AS (SELECT id, v FROM cm_big) SELECT id FROM s WHERE id < 3 UNION ALL SELECT id FROM s WHERE id > 19998",
		"UNION ALL;   Project: [id];     Filter: [id < 3];       Project: [id v];         Scan: cm_big;   Project: [id];     Filter: [id > 19998];       Project: [id v];         Scan: cm_big"},
	{"WITH s AS (SELECT id, v FROM cm_big), t AS (SELECT id FROM s WHERE v > 10) SELECT count(*) FROM t a JOIN t b ON a.id = b.id",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Join: join ON a.id = b.id;       Project: [id];         Filter: [v > 10];           Project: [id v];             Scan: cm_big;       Project: [id];         Filter: [v > 10];           Project: [id v];             Scan: cm_big"},
	{"WITH r AS (SELECT id % 7 AS k, sum(v) AS total FROM cm_big GROUP BY id % 7) SELECT k FROM r WHERE total = (SELECT max(total) FROM r)",
		"Project: [k];   Filter: [total = (SELECT max(total) FROM r)];     Project: [k total];       Aggregate: group_by=[mod(id, 7)] aggs=[sum(v) AS total];         Scan: cm_big"},
}
