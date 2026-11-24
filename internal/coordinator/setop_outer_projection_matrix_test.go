// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// An outer projection over a deduplicated set operation (#1613, epic #1678).
//
// The stage DAG lowers a bare UNION to a union stage and a keys-only
// GroupByAll dedup stage above it. The root SELECT-list pass gives a SELECT
// list over a COLLAPSING producer a StageProject of its own — the mechanism
// that answers the UNION ALL form — but its column models read the dedup as
// emitting nothing, so the pass declined, the gather-reachability check called
// the plan unmodelled, and the client got the union's raw columns under their
// raw names: `SELECT r + 100 AS z FROM (… UNION …) s` answered `r` unchanged,
// and `nosuchfn(r)` was never compiled anywhere, so it was not refused.
// The same held when the bare UNION carried its OWN ORDER BY inside the
// derived table: the sort fused onto the dedup (or, under an outer WHERE, sat
// as a standalone sort above it) and the pass declined to hide an ordering no
// consumer reads.
//
// This is the boundary matrix: outer projection kinds over inner shapes, on
// the embedded engine (single, spilled512k), the stage DAG (dag, dag-shuffled,
// dag-morsel4) and the coordinator's small-query fast path (fastpath), all over
// the wire, each compared with PostgreSQL 17.11's answer (names, declared
// types, value text, order where the statement orders it, SQLSTATE).
//
// PostgreSQL's answers are testdata/setop_outer_projection_pg17.tsv, measured
// by TestSetOpOuterProjectionMeasurePostgres from the same fixture. Writer
// switch SETOP1613_PG_WRITE=1 (with WADJET_PG_DSN) writes a NEW file in a fresh
// directory under os.TempDir() and logs its path: copy it by hand after review.

// m1613Tables is the fixture: duplicates within and across arms, NULL keys and
// NULL values, and values the outer projections visibly change.
func m1613Tables() []tmdTable {
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
	}}
	row := func(id, v any) map[string]any { return map[string]any{"id": id, "v": v} }
	i := func(n int) any { return int64(n) }
	return []tmdTable{
		{"sp_p", sch, []map[string]any{row(i(1), 2.0), row(i(1), 2.0), row(i(2), 7.0), row(i(2), nil), row(i(3), 11.0), row(nil, 17.0), row(i(5), 19.5)}},
		{"sp_q", sch, []map[string]any{row(i(1), 2.0), row(i(2), 7.0), row(i(2), 7.0), row(i(3), nil), row(i(6), 37.0), row(nil, 17.0)}},
		{"sp_r", sch, []map[string]any{row(i(2), 7.0), row(i(6), 37.0), row(i(9), 41.0)}},
	}
}

type m1613Cell struct {
	name, mode, sql string
}

// m1613Cells is outer × inner. Every inner shape publishes (id, v).
func m1613Cells() []m1613Cell {
	inners := []struct{ name, body string }{
		{"table", "SELECT id, v FROM sp_p"},
		{"union_all", "SELECT id, v FROM sp_p UNION ALL SELECT id, v FROM sp_q"},
		{"union", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q"},
		{"intersect", "SELECT id, v FROM sp_p INTERSECT SELECT id, v FROM sp_q"},
		{"intersect_all", "SELECT id, v FROM sp_p INTERSECT ALL SELECT id, v FROM sp_q"},
		{"except", "SELECT id, v FROM sp_p EXCEPT SELECT id, v FROM sp_q"},
		{"except_all", "SELECT id, v FROM sp_p EXCEPT ALL SELECT id, v FROM sp_q"},
		{"union3", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q UNION SELECT id, v FROM sp_r"},
		{"union_then_all", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q UNION ALL SELECT id, v FROM sp_r"},
		{"union_except", "SELECT id, v FROM (SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q) w EXCEPT SELECT id, v FROM sp_r"},
		{"union_in_all", "SELECT id, v FROM (SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q) w UNION ALL SELECT id, v FROM sp_r"},
		{"union_derived", "SELECT id, v FROM (SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q) w"},
		{"distinct", "SELECT DISTINCT id, v FROM sp_p"},
		{"group_by", "SELECT id, max(v) AS v FROM sp_p GROUP BY id"},
		// The derived table's OWN ORDER BY (a bare UNION's, fused onto its
		// dedup) under the outer SELECT list, with and without LIMIT, and
		// the siblings that carry the same inner ORDER BY ... LIMIT (#1613
		// round 2). Every key list is a total order over the distinct rows,
		// so each LIMIT is deterministic.
		{"union_ord_limit", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q ORDER BY v NULLS LAST, id NULLS LAST LIMIT 3"},
		{"union_ord", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q ORDER BY v"},
		{"union_ord_pos_limit", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q ORDER BY 2 DESC, 1 LIMIT 4"},
		{"union3_ord_limit", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q UNION SELECT id, v FROM sp_r ORDER BY v NULLS LAST, id NULLS LAST LIMIT 4"},
		{"union_all_ord_limit", "SELECT id, v FROM sp_p UNION ALL SELECT id, v FROM sp_q ORDER BY v NULLS LAST, id NULLS LAST LIMIT 3"},
		{"distinct_ord_limit", "SELECT DISTINCT id, v FROM sp_p ORDER BY v NULLS LAST, id NULLS LAST LIMIT 3"},
		{"intersect_ord_limit", "SELECT id, v FROM sp_p INTERSECT SELECT id, v FROM sp_q ORDER BY v NULLS LAST, id NULLS LAST LIMIT 3"},
		{"union_offset0", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q OFFSET 0"},
		{"union_limit_all", "SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q LIMIT 100"},
	}
	outers := []struct{ name, mode, sel, tail string }{
		{"bare", "bag", "SELECT v FROM", ""},
		{"alias", "bag", "SELECT v AS w FROM", ""},
		{"arith", "bag", "SELECT v + 100 AS z FROM", ""},
		{"func", "bag", "SELECT abs(v - 10) AS z FROM", ""},
		{"cast", "bag", "SELECT CAST(id AS TEXT) AS t FROM", ""},
		{"concat", "bag", "SELECT CAST(v AS TEXT) || '!' AS t FROM", ""},
		{"case", "bag", "SELECT CASE WHEN v > 10 THEN 'big' WHEN v IS NULL THEN 'none' ELSE 'small' END AS c FROM", ""},
		{"const", "bag", "SELECT 7 AS k, v FROM", ""},
		{"nosuchfn", "bag", "SELECT nosuchfn(v) AS z FROM", ""},
		{"subquery", "bag", "SELECT v, (SELECT max(id) FROM sp_r) AS m FROM", ""},
		{"tworefs", "bag", "SELECT v, v * 2 AS d FROM", ""},
		{"reorder", "bag", "SELECT v, id FROM", ""},
		{"drop", "bag", "SELECT id FROM", ""},
		{"count", "bag", "SELECT count(*) AS n FROM", ""},
		{"aggs", "bag", "SELECT sum(v + 1) AS sv, max(id) AS mi FROM", ""},
		{"order_limit", "ordered", "SELECT v + 100 AS z FROM", " ORDER BY z DESC NULLS LAST LIMIT 3"},
		{"order_expr", "ordered", "SELECT CAST(id AS TEXT) AS t FROM", " ORDER BY CAST(id AS TEXT) NULLS LAST, t"},
		{"where", "bag", "SELECT v + 100 AS z FROM", " WHERE s.id > 1"},
		{"join", "bag", "SELECT s.v + 100 AS z, r.id AS rid FROM", " JOIN sp_r r ON s.id = r.id"},
	}
	// Adjacent shapes: statements that read a bare UNION's dedup through a
	// pass the fix also changes (a union ARM over it, a window, a CTE, a
	// further projection or aggregate, a sort on a dropped column), and the
	// unaliased and star forms.
	const pq = "(SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q)"
	out := []m1613Cell{
		{"adj/arm_over_union", "bag", "SELECT v + 1 AS v FROM " + pq + " w UNION ALL SELECT v FROM sp_r"},
		{"adj/arm_over_union_dedup", "bag", "SELECT v + 1 AS v FROM " + pq + " w UNION SELECT v FROM sp_r"},
		{"adj/window", "bag", "SELECT v * 2 AS d, row_number() OVER (ORDER BY v NULLS LAST, id NULLS LAST) AS rn FROM " + pq + " s"},
		{"adj/cte", "bag", "WITH u AS (SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q) SELECT v * 2 AS d FROM u"},
		{"adj/cte_twice", "bag", "WITH u AS (SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q) SELECT a.v * 2 AS d, b.id FROM u a JOIN u b ON a.id = b.id"},
		{"adj/agg_over_project", "bag", "SELECT count(*) AS n, sum(z) AS sz FROM (SELECT v + 1 AS z FROM " + pq + " s) t"},
		{"adj/distinct_over", "bag", "SELECT DISTINCT v + 1 AS z FROM " + pq + " s"},
		{"adj/group_over", "bag", "SELECT id, max(v) + 1 AS m FROM " + pq + " s GROUP BY id"},
		{"adj/project_of_project", "bag", "SELECT z FROM (SELECT v + 100 AS z FROM " + pq + " s) t WHERE z > 110"},
		{"adj/order_dropped", "ordered", "SELECT v + 1 AS z FROM " + pq + " s ORDER BY id NULLS LAST, v NULLS LAST LIMIT 4"},
		{"adj/star", "bag", "SELECT * FROM " + pq + " s"},
		{"adj/arms_computed", "bag", "SELECT v + 1 AS z FROM sp_p UNION SELECT v FROM sp_q"},
		{"adj/unaliased", "bag", "SELECT v + 1 FROM " + pq + " s"},
		{"adj/arm_rename", "bag", "SELECT x FROM (SELECT v + 1 AS x FROM sp_p UNION SELECT v FROM sp_q) s"},
		{"adj/arm_rename_outer", "bag", "SELECT x * 10 AS y FROM (SELECT v + 1 AS x FROM sp_p UNION SELECT v FROM sp_q) s"},
		{"adj/where_expr", "bag", "SELECT v FROM " + pq + " s WHERE v + 0 > 10"},
		{"adj/one_col", "bag", "SELECT x + 1 AS y FROM (SELECT v AS x FROM sp_p UNION SELECT v FROM sp_q) s"},
		{"adj/shadow", "bag", "SELECT v + 1 AS v FROM (SELECT id, v FROM sp_p UNION SELECT id, v FROM sp_q) s"},
		{"adj/qualified", "bag", "SELECT s.v + 1 AS z, s.id FROM " + pq + " s"},
		{"adj/union_of_unions", "bag", "SELECT v + 1 AS z FROM (" + pq[1:len(pq)-1] + " UNION SELECT id, v FROM sp_r) s"},
		{"adj/intersect_over_union", "bag", "SELECT v + 1 AS z FROM (SELECT id, v FROM " + pq + " w INTERSECT SELECT id, v FROM sp_q) s"},
		// A bare UNION's own ORDER BY ... LIMIT under the SELECT list, with
		// an outer LIMIT (counted: which rows survive it is unspecified), an
		// outer DISTINCT, and an outer ORDER BY on a key the list drops.
		{"adj/inner_ord_outer_limit", "bag", "SELECT count(*) AS n FROM (SELECT v + 1 AS z FROM (" + pq[1:len(pq)-1] + " ORDER BY v NULLS LAST, id NULLS LAST LIMIT 3) s LIMIT 2) t"},
		{"adj/inner_ord_outer_distinct", "bag", "SELECT DISTINCT v + 1 AS z FROM (" + pq[1:len(pq)-1] + " ORDER BY v NULLS LAST, id NULLS LAST LIMIT 3) s"},
		{"adj/inner_ord_one_col", "bag", "SELECT v + 1 AS j FROM (SELECT v FROM sp_p UNION SELECT v FROM sp_q ORDER BY v NULLS LAST LIMIT 2) u"},
		{"adj/inner_ord_shadow_neg", "bag", "SELECT -v AS v FROM (SELECT v FROM sp_p UNION SELECT v FROM sp_q ORDER BY v NULLS LAST LIMIT 3) u"},
		{"adj/inner_ord_shadow_outer_order", "ordered", "SELECT -v AS v FROM (SELECT v FROM sp_p UNION SELECT v FROM sp_q ORDER BY v NULLS LAST LIMIT 3) u ORDER BY v"},
		{"adj/inner_ord_outer_order_dropped", "ordered", "SELECT v + 1 AS z FROM (" + pq[1:len(pq)-1] + " ORDER BY v NULLS LAST, id NULLS LAST LIMIT 3) s ORDER BY id DESC"},
	}
	for _, in := range inners {
		for _, o := range outers {
			out = append(out, m1613Cell{
				name: o.name + "/" + in.name,
				mode: o.mode,
				sql:  o.sel + " (" + in.body + ") s" + o.tail,
			})
		}
	}
	return out
}

const m1613PGPath = "testdata/setop_outer_projection_pg17.tsv"

func m1613PGDDL() []string {
	out := []string{"DROP SCHEMA IF EXISTS sp1613 CASCADE", "CREATE SCHEMA sp1613", "SET search_path = sp1613"}
	lit := func(x any) string {
		switch v := x.(type) {
		case nil:
			return "NULL"
		case int64:
			return fmt.Sprint(v)
		case float64:
			return fmt.Sprint(v)
		}
		panic(fmt.Sprintf("literal %T", x))
	}
	for _, tb := range m1613Tables() {
		out = append(out, fmt.Sprintf("CREATE TABLE %s (id bigint, v double precision)", tb.name))
		var vals []string
		for _, r := range tb.rows {
			vals = append(vals, "("+lit(r["id"])+","+lit(r["v"])+")")
		}
		out = append(out, "INSERT INTO "+tb.name+" VALUES "+strings.Join(vals, ","))
	}
	return append(out, "ANALYZE")
}

func m1613ReadPG(t *testing.T) map[string]r0Result {
	t.Helper()
	f, err := os.Open(m1613PGPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]r0Result{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 2)
		var r r0Result
		if err := json.Unmarshal([]byte(p[1]), &r); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		if _, dup := out[p[0]]; dup {
			t.Fatalf("duplicate PostgreSQL answer for %s", p[0])
		}
		out[p[0]] = r
	}
	return out
}

// TestSetOpOuterProjectionMeasurePostgres measures PostgreSQL's answer for
// every cell. Runs only with WADJET_PG_DSN and SETOP1613_PG_WRITE=1.
func TestSetOpOuterProjectionMeasurePostgres(t *testing.T) {
	on, err := r0WriterOn("SETOP1613_PG_WRITE")
	if err != nil {
		t.Fatal(err)
	}
	dsn := os.Getenv("WADJET_PG_DSN")
	if !on || dsn == "" {
		t.Skip("set WADJET_PG_DSN and SETOP1613_PG_WRITE=1 to measure PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	for _, s := range m1613PGDDL() {
		if r := r0Query(ctx, conn, s); r.Err != "" {
			t.Fatalf("%s: %s %s", s, r.Err, r.Msg)
		}
	}
	ver := r0Query(ctx, conn, "SELECT version() || ' / ' || datcollate || ' / ' || datctype || ' / ' || pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()")
	var b strings.Builder
	fmt.Fprintf(&b, "# PostgreSQL answers for the #1613 outer-projection matrix, measured by TestSetOpOuterProjectionMeasurePostgres.\n")
	if len(ver.Rows) == 1 && ver.Rows[0][0] != nil {
		fmt.Fprintf(&b, "# %s\n", *ver.Rows[0][0])
	}
	b.WriteString("# name<TAB>json\n")
	for _, c := range m1613Cells() {
		r := r0Query(ctx, conn, c.sql)
		js, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "%s\t%s\n", c.name, js)
	}
	r0WriteArtifact(t, "SETOP1613_PG_WRITE", "setop_outer_projection_pg17.tsv", b.String())
}

func m1613Embedded(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
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
	for _, tb := range m1613Tables() {
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(tb.name, tb.schema, nil, ingest.Config{MaxBufferRows: len(tb.rows) + 1, RowGroupSize: 4096})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func m1613Cluster(t *testing.T, ctx context.Context, wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
	t.Helper()
	infra := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infra, nil, m1613Tables())
	return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
}

var m1613Arms = []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "fastpath"}

// m1613Measure runs every cell on every arm and returns name -> arm -> verdict.
func m1613Measure(t *testing.T) (map[string]map[string]string, map[string]map[string]r0Result, map[string]r0Result) {
	t.Helper()
	pg := m1613ReadPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	serve := func(db *wadjet.DB, r *r0Router) *pgconn.PgConn {
		srv := pgwire.NewServer(db, pgwire.Config{}, nil)
		if r != nil {
			srv.SetRouter(r)
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Shutdown() })
		conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/test?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(context.Background()) })
		return conn
	}
	single := m1613Embedded(t, ctx, 0)
	spilled := m1613Embedded(t, ctx, 512*1024)
	routers := map[string]*r0Router{
		"dag":          {c: m1613Cluster(t, ctx, nil)},
		"dag-shuffled": {c: m1613Cluster(t, ctx, nil, func(c *Config) { c.BroadcastBytesOverride = 1 })},
		"dag-morsel4":  {c: m1613Cluster(t, ctx, func(w *worker.Config) { w.MorselWorkers = 4 })},
		"fastpath":     {c: m1613Cluster(t, ctx, nil, func(c *Config) { c.LocalFastPathBytes = 64 << 20 })},
	}
	conns := map[string]*pgconn.PgConn{"single": serve(single, nil), "spilled512k": serve(spilled, nil)}
	for a, r := range routers {
		conns[a] = serve(single, r)
	}
	verdicts := map[string]map[string]string{}
	got := map[string]map[string]r0Result{}
	for _, c := range m1613Cells() {
		want, ok := pg[c.name]
		if !ok {
			t.Fatalf("%s has no PostgreSQL answer: re-measure (TestSetOpOuterProjectionMeasurePostgres)", c.name)
		}
		verdicts[c.name] = map[string]string{}
		got[c.name] = map[string]r0Result{}
		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, a := range m1613Arms {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := r0Query(ctx, conns[a], c.sql)
				routed := false
				if rt := routers[a]; rt != nil {
					e := rt.take()
					if e.calls != 1 {
						t.Errorf("%s on %s: the router saw %d calls, want 1", c.name, a, e.calls)
					}
					routed = e.routed
				}
				v := r0Verdict(want, r, c.mode, routed)
				mu.Lock()
				verdicts[c.name][a] = v
				got[c.name][a] = r
				mu.Unlock()
			}()
		}
		wg.Wait()
	}
	return verdicts, got, pg
}

// m1613Outcome folds a verdict to right / wrong / refused for the report; a
// route that answers PostgreSQL's answer is right on its arm (the client sees
// the answer), and the fast path always routes by construction.
func m1613Outcome(v string) string {
	switch {
	case v == "right" || v == "routed-right":
		return "right"
	case strings.HasPrefix(v, "refused:"):
		return "refused"
	}
	return "wrong"
}

// TestSetOpOuterProjectionMatrix is the kept gate: on every arm, every cell
// answers PostgreSQL's answer (directly or through a route), except the cells
// listed in m1613KnownDefects with exactly the verdict they are listed with.
func TestSetOpOuterProjectionMatrix(t *testing.T) {
	r0RequireWriterUsage(t, "SETOP1613_REPORT")
	if testing.Short() {
		t.Skip("-short: six arms over the #1613 matrix")
	}
	verdicts, got, pg := m1613Measure(t)
	var report []string
	for _, c := range m1613Cells() {
		var row []string
		for _, a := range m1613Arms {
			v := verdicts[c.name][a]
			row = append(row, a+"="+v)
			want, known := m1613KnownDefects[c.name+"|"+a]
			switch {
			case known && v == want:
			case known:
				t.Errorf("%s on %s: %s, listed as the known defect %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, a, v, want,
					r0Canon(got[c.name][a], c.mode), r0Canon(pg[c.name], c.mode))
			case m1613Outcome(v) != "right":
				t.Errorf("%s on %s: %s\n  sql  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, a, v, c.sql,
					r0Canon(got[c.name][a], c.mode), r0Canon(pg[c.name], c.mode))
			}
		}
		report = append(report, c.name+"\t"+strings.Join(row, "\t"))
	}
	sort.Strings(report)
	r0WriteArtifact(t, "SETOP1613_REPORT", "setop_outer_projection_report.tsv", strings.Join(report, "\n")+"\n")
}

// m1613KnownDefects lists cells that are not PostgreSQL's answer for a reason
// other than #1613, each with its exact verdict (a listed cell that moves, in
// either direction, fails).
//
// The ORDER BY forms over a bare UNION are still the base's loud refusal on
// the three stage-DAG arms (#1613's ORDER BY half, unrepaired): the planner
// fuses the sort onto the dedup, so a projection the sort keys on cannot be
// placed above it, and the worker's sort refuses `key column ... does not
// exist` (0A000). Repairing it moves the sort's placement in stage emission
// (ADR-0051's territory); refusing it inside PlanDistributed instead would
// turn the asynchronous door's right answer into a refusal.
var m1613KnownDefects = func() map[string]string {
	out := map[string]string{}
	for _, name := range []string{
		"order_limit/union", "order_expr/union", "order_limit/union3", "order_expr/union3",
		"order_limit/union_derived", "order_expr/union_derived", "adj/order_dropped",
		// OFFSET 0 inside the derived table plans like the bare form: the
		// outer sort fuses onto the dedup the same way (refused at base too).
		"order_limit/union_offset0", "order_expr/union_offset0",
	} {
		for _, a := range []string{"dag", "dag-shuffled", "dag-morsel4"} {
			out[name+"|"+a] = "refused:0A000"
		}
	}
	return out
}()
