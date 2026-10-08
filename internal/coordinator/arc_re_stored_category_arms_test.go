// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A STORED COLUMN ROUNDS BY THE CATEGORY IT WAS CREATED WITH (#381), on five
// arms.
//
// CREATE TABLE AS of `5 / 2.0 + id * 0`, `sqrt(6.25 + id * 0)`, `power(2.5 +
// id * 0, 1)`, `sqrt(n * n)` or `ln(exp(2.5 + id * 0))` creates a numeric
// column on PostgreSQL 17.11, and this engine a DOUBLE PRECISION one
// (ADR-0024 §2c). At 1f580f7d a reader took that stored FLOAT64 for a float8
// and `round(b)` over 2.5 answered 2 where PostgreSQL — and 89cea148, which
// rounded every operand not spelled as a float cast half away — answer 3.
// The created column now records the PostgreSQL category the plan gave it
// (parquet.Column.PGNumeric, set by physical.Planner.CreatedColumns), and a
// scan of it reads that category exactly as a plan column's.
//
// The writes are the embedded engine's (a CREATE TABLE AS, WITH NO DATA then
// INSERT … SELECT, INSERT … SELECT and a literal INSERT into the created
// table, UPDATE, a copy of a marked column, a star, a CTE, two set
// operations, aggregates, a star join, a star grouping, a declared table, an
// integer target); the DAG arms read the catalog record and the files the
// embedded engine wrote into THEIR catalog and store (wadjet.Config.MetaKV /
// Store), which is a cluster reading an embedded-written table. Every want
// is PostgreSQL 17.11's answer to the same statements
// (testdata/arc_re_stored_category_pg17.tsv).

var rsFixture = []string{
	"CREATE TABLE rs_a (id BIGINT, f DOUBLE PRECISION, n NUMERIC(38,16), i INTEGER)",
	"INSERT INTO rs_a VALUES (1, 0.5, 0.5, 1), (2, 1.5, 1.5, 3), (3, 2.5, 2.5, 5), (4, 3.5, 3.5, 7), (5, -0.5, -0.5, -1), (6, -1.5, -1.5, -3), (7, -2.5, -2.5, -5), (8, NULL, NULL, NULL)",
}

var rsWrites = []string{
	"CREATE TABLE rs_h AS SELECT id, 5 / 2.0 + id * 0 AS a, sqrt(6.25 + id * 0) AS b, power(2.5 + id * 0, 1) AS c, i / 2.0 AS d, sqrt(n * n) AS e, ln(exp(2.5 + id * 0)) AS g FROM rs_a WHERE id IN (1,3)",
	"INSERT INTO rs_h SELECT id + 10, 5 / 2.0, sqrt(6.25), power(2.5, 1), i / 2.0, sqrt(n * n), 2.5 FROM rs_a WHERE id = 3",
	"INSERT INTO rs_h VALUES (20, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5)",
	"UPDATE rs_h SET b = 0.5 WHERE id = 1",
	"CREATE TABLE rs_n AS SELECT id, sqrt(6.25 + id * 0) AS b FROM rs_a WITH NO DATA",
	"INSERT INTO rs_n SELECT id, sqrt(6.25 + id * 0) FROM rs_a WHERE id IN (1,3)",
	"CREATE TABLE rs_k AS SELECT id, b, b + 0 AS bn, b + CAST(0 AS DOUBLE PRECISION) AS bf FROM rs_h WHERE id IN (3, 13)",
	"CREATE TABLE rs_e (id BIGINT, x DOUBLE PRECISION, y NUMERIC)",
	"INSERT INTO rs_e SELECT id, sqrt(6.25 + id * 0), sqrt(6.25 + id * 0) FROM rs_a WHERE id IN (1,3)",
	"CREATE TABLE rs_i (id BIGINT, v INTEGER)",
	"INSERT INTO rs_i SELECT id, b FROM rs_h WHERE id = 3",
	"CREATE TABLE rs_c AS SELECT id, i / 2.0 AS x, sqrt(i * i / 4.0) AS s, f AS g FROM rs_a WHERE id IN (1,3,5,7)",
	"CREATE TABLE rs_s AS SELECT * FROM rs_h",
	"CREATE TABLE rs_t AS WITH w AS (SELECT id, sqrt(6.25 + id * 0) AS b FROM rs_a WHERE id IN (1,3)) SELECT * FROM w",
	"CREATE TABLE rs_u AS SELECT id, sqrt(6.25 + id * 0) AS b FROM rs_a WHERE id = 1 UNION ALL SELECT id, 2.5 FROM rs_a WHERE id = 3",
	"CREATE TABLE rs_w AS SELECT id, sqrt(6.25 + id * 0) AS b FROM rs_a WHERE id = 1 UNION ALL SELECT id, f FROM rs_a WHERE id = 3",
	"CREATE TABLE rs_g AS SELECT max(sqrt(6.25 + id * 0)) AS m, avg(f) AS af, sum(5 / 2.0 + id * 0) AS s FROM rs_a WHERE id IN (1,3)",
	"CREATE TABLE rs_sj AS SELECT * FROM (SELECT id AS hid, b FROM rs_h) h JOIN (SELECT id, f FROM rs_a) r ON r.id = h.hid WHERE h.hid = 3",
	"CREATE TABLE rs_sg AS SELECT * FROM rs_h GROUP BY id, a, b, c, d, e, g",
}

type rsCell struct{ name, sql string }

var rsCells = []rsCell{
	{"h_round", "SELECT id, round(a), round(b), round(c), round(d), round(e), round(g) FROM rs_h ORDER BY 1"},
	{"h_cast", "SELECT id, CAST(a AS INTEGER), CAST(b AS INTEGER), CAST(c AS INTEGER), CAST(e AS INTEGER) FROM rs_h ORDER BY 1"},
	{"h_colon_arr", "SELECT id, b::bigint, CAST(ARRAY[b] AS BIGINT[]), round(b, 0) FROM rs_h ORDER BY 1"},
	{"h_where", "SELECT id FROM rs_h WHERE round(b) = 3 ORDER BY 1"},
	{"h_group", "SELECT round(b), count(*) FROM rs_h GROUP BY round(b) ORDER BY 1"},
	{"h_groupkey", "SELECT round(b), count(*) FROM rs_h WHERE id IN (3, 13) GROUP BY b"},
	{"h_agg", "SELECT round(max(b)), round(avg(b)), round(min(b) + 0) FROM rs_h WHERE id IN (3, 13)"},
	{"h_window", "SELECT id, round(max(b) OVER ()) FROM rs_h WHERE id IN (3, 13) ORDER BY 1"},
	{"h_derived", "SELECT id, round(y) FROM (SELECT id, b AS y FROM rs_h) s WHERE id = 3"},
	{"h_cte", "WITH w AS (SELECT id, b FROM rs_h) SELECT id, round(b) FROM w WHERE id = 3"},
	{"h_subquery", "SELECT round((SELECT b FROM rs_h WHERE id = 3))"},
	{"h_union_float", "SELECT round(x) FROM (SELECT b AS x FROM rs_h WHERE id = 3 UNION ALL SELECT f FROM rs_a WHERE id = 3) s"},
	{"h_union_numeric", "SELECT round(x) FROM (SELECT b AS x FROM rs_h WHERE id = 3 UNION ALL SELECT n FROM rs_a WHERE id = 3) s"},
	{"h_join", "SELECT round(h.b), round(r.f) FROM rs_h h JOIN rs_a r ON r.id = h.id WHERE h.id = 3"},
	{"h_join_same_name", "SELECT round(h.b), round(w.b) FROM rs_h h JOIN rs_w w ON w.id = h.id WHERE h.id = 3"},
	{"n_nodata", "SELECT id, round(b), CAST(b AS INTEGER) FROM rs_n ORDER BY 1"},
	{"k_copy", "SELECT id, round(b), round(bn), round(bf) FROM rs_k ORDER BY 1"},
	{"e_declared", "SELECT id, round(x), round(y) FROM rs_e ORDER BY 1"},
	{"i_integer_target", "SELECT id, v FROM rs_i"},
	{"c_g02", "SELECT id, round(x), CAST(x AS INTEGER), round(s), round(g) FROM rs_c ORDER BY 1"},
	{"s_star", "SELECT id, round(a), round(b) FROM rs_s ORDER BY 1"},
	{"t_cte", "SELECT id, round(b) FROM rs_t ORDER BY 1"},
	{"u_union_numeric", "SELECT id, round(b) FROM rs_u ORDER BY 1"},
	{"w_union_float", "SELECT id, round(b) FROM rs_w ORDER BY 1"},
	{"g_aggregates", "SELECT round(m), round(af), round(s / 2) FROM rs_g"},
	{"sj_star_join", "SELECT round(b), round(f) FROM rs_sj ORDER BY 1, 2"},
	{"sg_star_group", "SELECT round(b) FROM rs_sg ORDER BY 1"},
	// A name the stored column shares with another relation's float8
	// column: the stage DAG's map cannot key it, and the plan runs on the
	// coordinator-local pipeline (errStoredCategoryByName).
	{"x_union_two_tables", "SELECT round(b) FROM rs_h WHERE id = 3 UNION ALL SELECT round(b) FROM rs_w WHERE id = 3"},
	{"x_union_distinct", "SELECT round(b) FROM rs_h WHERE id = 3 UNION SELECT round(b) FROM rs_w WHERE id = 3 ORDER BY 1"},
	{"x_union_derived", "SELECT x FROM (SELECT round(b) AS x FROM rs_h WHERE id = 3) s UNION ALL SELECT round(b) FROM rs_w WHERE id = 3"},
	{"x_scalar_other_table", "SELECT id, round(b) FROM rs_h WHERE id = 3 AND b < (SELECT max(b) + 10 FROM rs_w)"},
	{"x_semi_in", "SELECT round(b) FROM rs_h WHERE id IN (SELECT id FROM rs_w) ORDER BY 1"},
	{"x_semi_exists", "SELECT round(b) FROM rs_h WHERE EXISTS (SELECT 1 FROM rs_w WHERE rs_w.id = rs_h.id) ORDER BY 1"},
	{"x_anti_not_in", "SELECT round(b) FROM rs_h WHERE id NOT IN (SELECT id FROM rs_w) ORDER BY 1"},
	{"x_semi_derived", "SELECT round(b) FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM rs_a) s WHERE id IN (SELECT id FROM rs_w) ORDER BY 1"},
	// A scalar subquery's category (no stored column): the declaration a
	// ROUND or a CAST over the subquery reads (SubqueryOutputColumn).
	{"q_subquery_sqrt", "SELECT round((SELECT sqrt(6.25 + id * 0) FROM rs_a WHERE id = 3))"},
	{"q_subquery_div", "SELECT round((SELECT 5 / 2.0 + id * 0 FROM rs_a WHERE id = 3))"},
	{"q_subquery_cast", "SELECT CAST((SELECT sqrt(6.25 + id * 0) FROM rs_a WHERE id = 3) AS INTEGER)"},
	{"q_subquery_float", "SELECT round((SELECT f FROM rs_a WHERE id = 3))"},
	{"q_subquery_where", "SELECT id FROM rs_a WHERE id = round((SELECT 5 / 2.0 + id * 0 FROM rs_a WHERE id = 3))"},
	{"q_subquery_numeric", "SELECT round((SELECT n FROM rs_a WHERE id = 3))"},
}

// rsLoad creates rs_a on db and runs the writes.
func rsLoad(t *testing.T, ctx context.Context, db *wadjet.DB, fixture bool) {
	t.Helper()
	stmts := rsWrites
	if fixture {
		stmts = append(append([]string(nil), rsFixture...), rsWrites...)
	}
	for _, q := range stmts {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// rsMarked is the declared marks of the created tables, by table.column.
func rsMarked(t *testing.T, ctx context.Context, db *wadjet.DB) string {
	t.Helper()
	tables, err := db.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var marks []string
	for _, name := range tables {
		tb, err := db.Catalog().GetTable(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range tb.Schema.Columns {
			if c.PGNumeric {
				marks = append(marks, name+"."+c.Name)
			}
		}
	}
	sort.Strings(marks)
	return strings.Join(marks, " ")
}

// rsWantMarks are the columns PostgreSQL creates numeric that this engine
// creates FLOAT64: every other created column is unmarked (rs_k.bf and
// rs_w.b are float8 there; rs_e.y and rs_h.d are DECIMAL here).
const rsWantMarks = "rs_c.s rs_g.m rs_g.s rs_h.a rs_h.b rs_h.c rs_h.e rs_h.g rs_k.b rs_k.bn rs_n.b rs_s.a rs_s.b rs_s.c rs_s.e rs_s.g rs_sg.a rs_sg.b rs_sg.c rs_sg.e rs_sg.g rs_sj.b rs_t.b rs_u.b"

func rsPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_re_stored_category_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
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

func TestArcREStoredColumnRoundsByItsCreatedCategoryOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	if os.Getenv("RS_GEN") != "" {
		// The PostgreSQL script: the fixture, the writes, then each cell
		// as name<TAB>statement.
		var b strings.Builder
		for _, q := range append(append([]string(nil), rsFixture...), rsWrites...) {
			fmt.Fprintf(&b, "-\t%s\n", q)
		}
		for _, c := range rsCells {
			fmt.Fprintf(&b, "%s\t%s\n", c.name, c.sql)
		}
		if err := os.WriteFile(os.Getenv("RS_GEN"), []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	answers := rsPGAnswers(t)
	for _, c := range rsCells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure (RS_GEN)", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	embedded := func(budget int64) *wadjet.DB {
		cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
		if budget > 0 {
			cfg.MemoryBudget = budget
			cfg.SpillDir = t.TempDir()
		}
		db, err := wadjet.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		rsLoad(t, ctx, db, true)
		if got := rsMarked(t, ctx, db); got != rsWantMarks {
			t.Fatalf("the created columns are marked\n  %s\n  want\n  %s", got, rsWantMarks)
		}
		return db
	}
	single := embedded(0)
	spilled := embedded(512 * 1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		// The cluster's catalog and store, written by the embedded engine.
		w, err := wadjet.Open(ctx, wadjet.Config{Store: infra.store, Bucket: "test", MetaKV: infra.kv})
		if err != nil {
			t.Fatal(err)
		}
		rsLoad(t, ctx, w, true)
		if got := rsMarked(t, ctx, w); got != rsWantMarks {
			t.Fatalf("the cluster's created columns are marked\n  %s\n  want\n  %s", got, rsWantMarks)
		}
		w.Close()
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	arms := []struct {
		name string
		run  func(string) string
	}{
		{"single", func(s string) string { return reRunSingle(ctx, single, s) }},
		{"spilled512k", func(s string) string { return reRunSingle(ctx, spilled, s) }},
		{"dag", func(s string) string { return reRunDAG(ctx, coord, s) }},
		{"dag-shuffled", func(s string) string { return reRunDAG(ctx, coordB, s) }},
		{"dag-morsel4", func(s string) string { return reRunDAG(ctx, coordM, s) }},
	}
	ties := 0
	for _, c := range rsCells {
		want := answers[c.name]
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				if got := arm.run(c.sql); got != want {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", arm.name, c.sql, got, want)
				}
			}
		})
		if strings.Contains(want, "3") {
			ties++
		}
	}
	if len(rsCells) < 41 || ties < 32 {
		t.Fatalf("the table shrank: %d cells, %d answering a half-way value away from zero", len(rsCells), ties)
	}
}
