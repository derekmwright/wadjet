// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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

// A VOLATILE CTE READ ONCE IS PLANNED EXACTLY AS AT c67ebf5b (#1531 round 3):
// one reader, nothing to share, so its body is inlined (and pushed into) as
// any other block's. EXPLAIN VERBOSE prints the text c67ebf5b printed for
// eight single-reference shapes — FROM, LIMIT, a body raising past the rows
// read, TABLESAMPLE, uuid(), a scalar subquery, a correlated EXISTS, a nested
// block — on both arms. At d56767c1 each printed a "CTE s: volatile,
// evaluated once" line and the third raised 22012.
func TestArcCMSingleReferenceVolatileCTEPlansAsAtBase(t *testing.T) {
	for _, budget := range []int64{0, 512 << 10} {
		db := cmOpen(t, budget)
		for _, c := range cmSingleReferenceShapes {
			want := c.plan
			if budget > 0 && c.spilled != "" {
				want = c.spilled
			}
			got := cmAnswer(t, db, "EXPLAIN VERBOSE "+c.sql)
			if got != want && !(strings.HasPrefix(want, "ERR") && strings.HasPrefix(got, want)) {
				t.Errorf("budget %d: %s\n  got  %q\n  want %q (the plan at c67ebf5b)", budget, c.sql, got, want)
			}
		}
	}
}

// cmSingleReferenceShapes: EXPLAIN VERBOSE at c67ebf5b (cm_author/r3/explain_tree_c67ebf5b.tsv);
// spilled is what c67ebf5b printed under 512 KiB where that differs (the
// uuid() shape's hash join build is refused there: filing candidate F6).
var cmSingleReferenceShapes = []struct{ sql, plan, spilled string }{
	{"WITH s AS (SELECT id, random() AS r FROM cm_big) SELECT count(*) FROM s WHERE id < 10",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Filter: [id < 10];       Project: [id r];         Scan: cm_big; ; -- Physical Plan --; Single-stage local execution", ""},
	{"WITH s AS (SELECT id, random() AS r FROM cm_big) SELECT id, r FROM s LIMIT 1",
		"Limit: 1 offset: 0;   Project: [id r];     Project: [id r];       Scan: cm_big; ; -- Physical Plan --; Single-stage local execution", ""},
	{"WITH s AS (SELECT g, random() AS r, 1/(g-150000) AS z FROM generate_series(1, 200000) g) SELECT g FROM s LIMIT 1",
		"Limit: 1 offset: 0;   Project: [g];     Project: [g r z];       Scan: generate_series AS g; ; -- Physical Plan --; Single-stage local execution", ""},
	{"WITH s AS (SELECT id FROM cm_big TABLESAMPLE BERNOULLI (50)) SELECT count(*) FROM s WHERE id > 5",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Filter: [id > 5];       Project: [id];         Scan: cm_big TABLESAMPLE BERNOULLI (50); ; -- Physical Plan --; Single-stage local execution", ""},
	{"WITH s AS (SELECT id, uuid() AS u FROM cm_big) SELECT count(*) FROM s a JOIN cm_p b ON a.id = b.id",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Join: join ON a.id = b.id;       Project: [id u];         Scan: cm_big;       Scan: cm_p AS b; ; -- Physical Plan --; Single-stage local execution",
		"ERR building physical plan: building hash table: hash join build: query: memory budget exceeded"},
	{"WITH s AS (SELECT sum(random()) AS r FROM cm_big) SELECT count(*) FROM cm_p WHERE (SELECT r FROM s) > 0",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Filter: [(SELECT r FROM s) > 0];       Scan: cm_p; ; -- Physical Plan --; Single-stage local execution", ""},
	{"WITH s AS (SELECT id, random() AS r FROM cm_big) SELECT count(*) FROM cm_p WHERE EXISTS (SELECT 1 FROM s WHERE s.id = cm_p.id)",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Filter: [exists (SELECT 1 FROM s WHERE s.id = cm_p.id)];       Scan: cm_p; ; -- Physical Plan --; Single-stage local execution", ""},
	{"SELECT count(*) FROM (WITH s AS (SELECT id, random() AS r FROM cm_big) SELECT id FROM s WHERE id < 5) x",
		"Project: [count(*)];   Aggregate: group_by=[] aggs=[count() AS count(*)];     Project: [id];       Filter: [id < 5];         Project: [id r];           Scan: cm_big AS x; ; -- Physical Plan --; Single-stage local execution", ""},
}

// cmRaisingBody raises 22012 at its 150 000th row; cmHalfBody does not raise.
const (
	cmRaisingBody = "WITH s AS (SELECT g, random() AS r, 1/(g-150000) AS z FROM generate_series(1, 200000) g) "
	cmHalfBody    = "WITH s AS (SELECT g, random() AS r FROM generate_series(1, 100000) g) "
)

// A VOLATILE CTE READ MORE THAN ONCE IS FILLED ON DEMAND (#1531 round 3): its
// one evaluation advances only when a reader asks for a row it does not hold
// yet, so a reader that stops early never forces rows nobody reads — nor the
// error on one — and every reader, fast or slow, in any order, reads the same
// rows. PostgreSQL 17.11 answers each cell (cm_author/r3/pg_new.tsv,
// pg_rev.tsv); ×8 per arm. At d56767c1 the body ran whole when a reference was
// built, and the E1b / F1 / F5 / G1b / G1c / G2 / G3 cells raised 22012 or
// were refused (gate_lazy_at_d56767c1_FAILS.log).
func TestArcCMSharedVolatileCTEIsFilledOnDemand(t *testing.T) {
	cells := []struct{ name, sql, want string }{
		{"E1b_two_limit_readers", cmRaisingBody + "SELECT (SELECT g FROM s LIMIT 1) + (SELECT g FROM s LIMIT 1)", "2"},
		{"F1b_two_exists_readers", cmRaisingBody + "SELECT EXISTS (SELECT 1 FROM s) AND EXISTS (SELECT 1 FROM s LIMIT 1)", "true"},
		{"F5b_uuid_two_limit_readers", "WITH s AS (SELECT g, uuid() AS u, 1/(g-150000) AS z FROM generate_series(1, 200000) g) " +
			"SELECT (SELECT count(*) FROM (SELECT g FROM s LIMIT 5) x) + (SELECT count(*) FROM (SELECT u FROM s LIMIT 3) y)", "8"},
		{"G1b_limit_reader_then_raising_full_reader", cmRaisingBody + "SELECT (SELECT g FROM s LIMIT 1), (SELECT count(*) FROM s)",
			"ERR division by zero"},
		{"G1_two_speeds", cmHalfBody + "SELECT (SELECT g FROM s ORDER BY g LIMIT 1), (SELECT count(*) FROM s), " +
			"(SELECT sum(r) FROM s) - (SELECT sum(r) FROM s)", "1 100000 0"},
		{"G1c_limit_then_full", cmHalfBody + "SELECT (SELECT g FROM s LIMIT 1), (SELECT count(*) FROM s)", "1 100000"},
		{"G1d_full_then_limit", cmHalfBody + "SELECT (SELECT count(*) FROM s), (SELECT g FROM s LIMIT 1)", "100000 1"},
		{"G2_selfjoin", cmHalfBody + "SELECT count(*) FROM s a JOIN s b ON a.g = b.g WHERE a.r <> b.r", "0"},
		{"G2b_selfjoin_count", cmHalfBody + "SELECT count(*) FROM s a JOIN s b ON a.g = b.g", "100000"},
		{"G3_union_all_arms", cmHalfBody + "SELECT count(DISTINCT r) FROM (SELECT g, r FROM s UNION ALL SELECT g, r FROM s) x",
			"100000"},
		{"G3b_except_all_arms", cmHalfBody + "SELECT count(*) FROM (SELECT g, r FROM s EXCEPT ALL SELECT g, r FROM s) x", "0"},
		{"G4_correlated_reader", "WITH s AS (SELECT random() AS r) SELECT count(*) FROM cm_big t " +
			"WHERE t.id <= 50 AND (SELECT r + t.id*0 FROM s) <> (SELECT r FROM s)", "0"},
	}
	for _, budget := range []int64{0, 512 << 10} {
		db := cmOpen(t, budget)
		for _, c := range cells {
			for rep := 0; rep < 8; rep++ {
				got := cmAnswer(t, db, c.sql)
				if got != c.want && !(strings.HasPrefix(c.want, "ERR") && strings.HasPrefix(got, c.want)) {
					t.Errorf("budget %d %s rep %d: got %s, want %s (PostgreSQL 17.11)", budget, c.name, rep, got, c.want)
				}
			}
		}
	}
}

// THE CLASSIFIER ASKS THE FUNCTION REGISTRY (#1531 round 3, review B2): a
// function CREATE FUNCTION defined is volatile when its BODY is, followed
// through the functions the body calls. The issue's cell through
// `CREATE FUNCTION f_r() AS random()` — and through f_r2() calling f_r() —
// answers PostgreSQL's 0 ×8 per arm (3 at d56767c1 and c67ebf5b:
// gate_udf_at_d56767c1_FAILS.log); a deterministic function's body keeps
// its CTE inlined (EXPLAIN VERBOSE names no shared CTE).
func TestArcCMVolatileUserFunctionMakesTheCTEShared(t *testing.T) {
	// The function store is the process's: create once, drop at the end.
	ddlDB := cmOpen(t, 0)
	for _, ddl := range []string{"CREATE FUNCTION cm_f_r() AS random()", "CREATE FUNCTION cm_f_r2() AS cm_f_r() + 1",
		"CREATE FUNCTION cm_f_d(x) AS x * 2.0"} {
		if got := cmAnswer(t, ddlDB, ddl); strings.HasPrefix(got, "ERR") {
			t.Fatalf("%s: %s", ddl, got)
		}
	}
	t.Cleanup(func() {
		for _, name := range []string{"cm_f_r2", "cm_f_r", "cm_f_d"} {
			cmAnswer(t, ddlDB, "DROP FUNCTION "+name)
		}
	})
	for _, budget := range []int64{0, 512 << 10} {
		db := cmOpen(t, budget)
		for _, fn := range []string{"cm_f_r()", "cm_f_r2()", "cm_f_d(id)"} {
			q := "WITH s AS (SELECT sum(" + fn + ") AS r FROM cm_big) SELECT count(*) FROM cm_p WHERE (SELECT r FROM s) <> (SELECT r FROM s)"
			for rep := 0; rep < 8; rep++ {
				if got := cmAnswer(t, db, q); got != "0" {
					t.Errorf("budget %d: %s rep %d: got %s, want 0 (PostgreSQL 17.11)", budget, q, rep, got)
				}
			}
			plan := cmAnswer(t, db, "EXPLAIN VERBOSE "+q)
			shared := strings.Contains(plan, "CTE s: volatile")
			if want := fn != "cm_f_d(id)"; shared != want {
				t.Errorf("budget %d: %s: EXPLAIN VERBOSE names a shared CTE = %v, want %v:\n%s", budget, fn, shared, want, plan)
			}
		}
	}
}

// THE SHARED EVALUATION LIVES AS LONG AS ITS STATEMENT (#1531 round 3): under
// a 512 KiB budget, a spool that spilled, a body that raised mid-spool, and a
// statement cancelled mid-spool each leave no run file in the spill directory
// and no body goroutine behind; concurrent statements each get their own
// evaluation (every pair of references agrees, the sums differ).
func TestArcCMSharedSpoolEndsWithItsStatement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: 512 << 10, SpillDir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	files := func() int {
		n := 0
		_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				n++
			}
			return nil
		})
		return n
	}
	bodies := func() int {
		buf := make([]byte, 1<<22)
		return strings.Count(string(buf[:runtime.Stack(buf, true)]), "(*SharedSpool).run")
	}
	settled := func(what string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for (files() != 0 || bodies() != 0) && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if n, b := files(), bodies(); n != 0 || b != 0 {
			t.Errorf("%s: %d spill files and %d body goroutines left after the statement", what, n, b)
		}
	}
	big := "WITH s AS (SELECT g, random() AS r FROM generate_series(1, 2000000) g) "
	if got := cmAnswer(t, db, big+"SELECT (SELECT sum(r) FROM s) - (SELECT sum(r) FROM s)"); got != "0" {
		t.Errorf("2M rows read twice: got %s, want 0", got)
	}
	settled("spilled spool")
	if got := cmAnswer(t, db, cmRaisingBody+"SELECT (SELECT g FROM s LIMIT 1), (SELECT count(*) FROM s)"); !strings.HasPrefix(got, "ERR division by zero") {
		t.Errorf("raising body: got %s, want 22012", got)
	}
	settled("raising body")
	if got := cmAnswer(t, db, cmRaisingBody+"SELECT (SELECT g FROM s LIMIT 1) + (SELECT g FROM s LIMIT 1)"); got != "2" {
		t.Errorf("two early readers: got %s, want 2", got)
	}
	settled("early readers")
	for _, d := range []time.Duration{20 * time.Millisecond, 100 * time.Millisecond, 400 * time.Millisecond} {
		cctx, cancel := context.WithTimeout(ctx, d)
		_, err := db.Query(cctx, big+"SELECT (SELECT sum(r) FROM s) - (SELECT sum(r) FROM s)")
		cancel()
		if err == nil {
			t.Logf("cancel after %s: the statement finished first", d)
		}
		settled(fmt.Sprintf("cancelled after %s", d))
	}
	var wg sync.WaitGroup
	diffs, sums := make([]string, 6), make([]string, 6)
	for i := range diffs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := "WITH s AS (SELECT g, random() AS r FROM generate_series(1, 50000) g) "
			diffs[i] = cmAnswer(t, db, q+"SELECT (SELECT sum(r) FROM s) - (SELECT sum(r) FROM s)")
			sums[i] = cmAnswer(t, db, q+"SELECT (SELECT sum(r) FROM s) + 0 * (SELECT count(*) FROM s)")
		}(i)
	}
	wg.Wait()
	seen := map[string]bool{}
	for i := range diffs {
		if diffs[i] != "0" {
			t.Errorf("session %d: two references disagree: %s", i, diffs[i])
		}
		if seen[sums[i]] {
			t.Errorf("session %d: sum %s repeats another statement's evaluation", i, sums[i])
		}
		seen[sums[i]] = true
	}
	settled("concurrent sessions")
}

// A SET OPERATION AT THE STATEMENT ROOT READS THE STATEMENT'S WITH LIST FROM
// ITS ARMS' EXPRESSION SUBQUERIES (#1531 round 3, review P2). The root of a
// set operation carries no WITH list — each arm's root does — and a scalar
// subquery in an arm found no `c`: NULL where PostgreSQL 17.11 answers the
// value, for a deterministic body too. At c67ebf5b and d56767c1 every cell
// below answered NULL or no row (gate_setop_at_d56767c1_FAILS.log).
func TestArcCMSetOperationArmsReadTheStatementWith(t *testing.T) {
	const c = "WITH c AS (SELECT id FROM cm_p) "
	cells := []struct{ sql, want string }{
		{c + "SELECT (SELECT max(id) FROM c) AS id UNION ALL SELECT (SELECT min(id) FROM c)", "3; 1"},
		{"WITH c AS (SELECT id, random() AS r FROM cm_p) SELECT (SELECT max(id) FROM c) AS id UNION ALL SELECT (SELECT min(id) FROM c)", "3; 1"},
		{c + "SELECT (SELECT max(id) FROM c) AS id FROM cm_p WHERE id = 1 UNION ALL SELECT (SELECT min(id) FROM c) FROM cm_p WHERE id = 1", "3; 1"},
		{c + "SELECT (SELECT max(id) FROM c) AS id UNION ALL SELECT (SELECT min(id) FROM c) ORDER BY 1", "1; 3"},
		{c + "SELECT (SELECT max(id) FROM c) AS id UNION ALL SELECT (SELECT min(id) FROM c) UNION ALL SELECT (SELECT count(*) FROM c)", "3; 1; 3"},
		{c + "SELECT id FROM cm_p WHERE id = (SELECT max(id) FROM c) EXCEPT SELECT id FROM cm_p WHERE id = (SELECT min(id) FROM c)", "3"},
	}
	for _, budget := range []int64{0, 512 << 10} {
		db := cmOpen(t, budget)
		for _, cell := range cells {
			if got := cmAnswer(t, db, cell.sql); got != cell.want {
				t.Errorf("budget %d: %s\n  got %s, want %s (PostgreSQL 17.11)", budget, cell.sql, got, cell.want)
			}
		}
	}
}

// PINS (catalog other#r27): a volatile CTE read ONCE is inlined (ADR-0021
// §2d), and a correlated subquery re-runs it per outer row; a WITH declared
// inside the correlated subquery is re-parsed per execution. PostgreSQL 17.11
// answers 1 for both (one evaluation, kept across rescans); this engine 50 at
// c67ebf5b and here. A pin that starts answering 1 FAILS: delete it with the
// catalog row.
func TestArcCMCorrelatedReaderIsEvaluatedPerOuterRow(t *testing.T) {
	db := cmOpen(t, 0)
	for _, q := range []string{
		"WITH s AS (SELECT random() AS r) SELECT count(DISTINCT (SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50",
		"SELECT count(DISTINCT (WITH s AS (SELECT random() AS r) SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50",
	} {
		if got := cmAnswer(t, db, q); got != "50" {
			t.Errorf("%s\n  got %s, want the pinned 50 (PostgreSQL 17.11: 1) — if it agrees now, delete this pin and other#r27", q, got)
		}
	}
}
