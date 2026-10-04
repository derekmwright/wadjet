// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"strings"
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

// The statement clock (#1566): now() / CURRENT_TIMESTAMP / LOCALTIMESTAMP /
// CURRENT_DATE are ONE value per statement in PostgreSQL 17.11 — the
// transaction's start — on every row, in every clause and (here) on every
// worker. The cells compare clock functions with each other inside one
// statement, never with a wall-clock literal, so the answers are fixed.

const scRows = 4096

func scSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp},
	}}
}

func scBase() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }

func scRowsData() []map[string]any {
	rows := make([]map[string]any, scRows)
	for i := range rows {
		id := int64(i + 1)
		rows[i] = map[string]any{"id": id, "v": id % 7, "ts": scBase().Add(time.Duration(id) * time.Second).UnixMilli()}
	}
	return rows
}

func scTables() []tmdTable {
	return []tmdTable{
		{name: "sc_r", schema: scSchema(), rows: scRowsData()},
		{name: "sc_k", schema: tcKSchema(), rows: tcKRows()},
	}
}

func scPGFixture() []string {
	return []string{
		"CREATE TABLE sc_r (id bigint, v bigint, ts timestamp)",
		fmt.Sprintf("INSERT INTO sc_r SELECT g, g %% 7, TIMESTAMP '2024-01-01' + g * INTERVAL '1 second' FROM generate_series(1, %d) g", scRows),
		"CREATE TABLE sc_k (k bigint)",
		"INSERT INTO sc_k VALUES (1),(2),(3),(4),(5),(6),(7),(8)",
	}
}

// scStandalone is one embedded engine over the fixture, 256 rows per row
// group so the 4096 rows cross many row groups and two batches.
func scStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range scTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: 1024, RowGroupSize: 256})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

type scCell struct{ name, sql, pg string }

// scUnboundClockGuard, when set (arc_sc_statement_clock_seam_test.go),
// fails a test that compiled a clock function with no statement clock bound.
var scUnboundClockGuard func(t *testing.T)

// scReps is how many times each multi-row cell runs per door (ADR-0027): a
// per-evaluation clock read disagrees on a few rows of a run, not every run.
const scReps = 8

// scCells is the coverage table: function × position, every answer
// PostgreSQL 17.11's (re-measured when WADJET_PG_DSN names a server).
func scCells() []scCell {
	multi := []scCell{
		// The issue's two cells.
		{"issue/cross_now_eq_now", "SELECT count(*) FROM sc_k a, sc_k b, sc_k c, sc_k e WHERE CAST(now() AS TIMESTAMP) = CAST(now() AS TIMESTAMP)", "rows=[4096]"},
		{"issue/distinct_now_text", "SELECT count(DISTINCT CAST(now() AS TEXT)) FROM sc_r", "rows=[1]"},
		// Function: one value over 4096 rows.
		{"fn/now", "SELECT count(DISTINCT now()) FROM sc_r", "rows=[1]"},
		{"fn/current_timestamp", "SELECT count(DISTINCT CURRENT_TIMESTAMP) FROM sc_r", "rows=[1]"},
		{"fn/localtimestamp", "SELECT count(DISTINCT LOCALTIMESTAMP) FROM sc_r", "rows=[1]"},
		{"fn/current_date", "SELECT count(DISTINCT CURRENT_DATE) FROM sc_r", "rows=[1]"},
		{"fn/extract_epoch", "SELECT count(DISTINCT extract(epoch FROM now())) FROM sc_r", "rows=[1]"},
		// Function against function, one statement.
		{"fn/now_eq_current_timestamp", "SELECT count(*) FROM sc_r WHERE now() = CURRENT_TIMESTAMP", "rows=[4096]"},
		{"fn/localtimestamp_eq_now", "SELECT count(*) FROM sc_r WHERE LOCALTIMESTAMP = CAST(now() AS TIMESTAMP)", "rows=[4096]"},
		{"fn/current_date_eq_now_date", "SELECT count(*) FROM sc_r WHERE CURRENT_DATE = CAST(now() AS DATE)", "rows=[4096]"},
		// Position.
		{"pos/where_eq", "SELECT count(*) FROM sc_r WHERE now() = now()", "rows=[4096]"},
		{"pos/where_pushed", "SELECT count(*) FROM sc_r WHERE id > 0 AND now() = now()", "rows=[4096]"},
		{"pos/case", "SELECT count(*) FROM sc_r WHERE CASE WHEN id > 0 THEN now() END = now()", "rows=[4096]"},
		{"pos/max_eq_min", "SELECT max(now()) = min(now()) FROM sc_r", "rows=[t]"},
		{"pos/join_on", "SELECT count(*) FROM sc_r a JOIN sc_r b ON a.id = b.id AND now() = now()", "rows=[4096]"},
		{"pos/join_sides", "SELECT count(*) FROM (SELECT id, now() t FROM sc_r) a JOIN (SELECT id, now() t FROM sc_r) b ON a.id = b.id AND a.t = b.t", "rows=[4096]"},
		{"pos/join_select", "SELECT count(DISTINCT now()) FROM sc_r a JOIN sc_r b ON a.id = b.id", "rows=[1]"},
		{"pos/group_by_now", "SELECT count(*) FROM (SELECT now() t, count(*) n FROM sc_r GROUP BY now()) s", "rows=[1]"},
		{"pos/group_by_agg", "SELECT count(DISTINCT t) FROM (SELECT v, max(now()) t FROM sc_r GROUP BY v) s", "rows=[1]"},
		{"pos/having", "SELECT count(*) FROM (SELECT v FROM sc_r GROUP BY v HAVING max(now()) = min(now())) s", "rows=[7]"},
		{"pos/distinct", "SELECT count(*) FROM (SELECT DISTINCT now() FROM sc_r) s", "rows=[1]"},
		{"pos/order_by", "SELECT count(DISTINCT t) FROM (SELECT now() t FROM sc_r ORDER BY now(), id) s", "rows=[1]"},
		{"pos/scalar_subquery", "SELECT count(*) FROM sc_r WHERE now() = (SELECT now())", "rows=[4096]"},
		{"pos/in_subquery", "SELECT count(*) FROM sc_r WHERE now() IN (SELECT now() FROM sc_k)", "rows=[4096]"},
		{"pos/cte_twice", "WITH c AS (SELECT now() t FROM sc_r) SELECT count(DISTINCT x.t) FROM (SELECT t FROM c UNION ALL SELECT t FROM c) x", "rows=[1]"},
		{"pos/window", "SELECT count(DISTINCT w) FROM (SELECT max(now()) OVER (PARTITION BY v) w FROM sc_r) s", "rows=[1]"},
		{"pos/union_all", "SELECT count(DISTINCT t) FROM (SELECT now() t FROM sc_r UNION ALL SELECT now() FROM sc_r) s", "rows=[1]"},
		{"pos/interval_arith", "SELECT count(DISTINCT now() + id * INTERVAL '0 second') FROM sc_r", "rows=[1]"},
		{"pos/correlated_subquery", "SELECT count(*) FROM sc_k k WHERE (SELECT now() FROM sc_r r WHERE r.id = k.k) = now()", "rows=[8]"},
		{"pos/lateral_dual", "SELECT count(DISTINCT x.t) FROM sc_r, LATERAL (SELECT now() t) x", "rows=[1]"},
		{"pos/lateral_left", "SELECT count(DISTINCT t) FROM sc_k LEFT JOIN LATERAL (SELECT now() t FROM sc_r WHERE sc_r.id = sc_k.k * 1000) x ON true", "rows=[1]"},
		{"pos/values", "SELECT count(DISTINCT t) FROM (VALUES (now()), (now()), (now())) v(t)", "rows=[1]"},
		{"pos/recursive_cte", "WITH RECURSIVE r(n, t) AS (SELECT 1, now() UNION ALL SELECT n + 1, now() FROM r WHERE n < 500) SELECT count(DISTINCT t) FROM r", "rows=[1]"},
	}
	var cells []scCell
	for _, c := range multi {
		for i := 1; i <= scReps; i++ {
			cells = append(cells, scCell{fmt.Sprintf("%s/%d", c.name, i), c.sql, c.pg})
		}
	}
	// The clock functions PostgreSQL has and this engine does not, and the
	// one that must stay per evaluation.
	cells = append(cells,
		scCell{"other/current_timestamp_p", "SELECT count(DISTINCT CURRENT_TIMESTAMP(2)) FROM sc_r", "rows=[1]"},
		scCell{"other/transaction_timestamp", "SELECT count(DISTINCT transaction_timestamp()) FROM sc_r", "rows=[1]"},
		scCell{"other/statement_timestamp", "SELECT count(DISTINCT statement_timestamp()) FROM sc_r", "rows=[1]"},
		scCell{"other/clock_timestamp", "SELECT count(DISTINCT clock_timestamp()) > 1 FROM sc_r", "rows=[t]"},
		scCell{"other/timeofday", "SELECT count(DISTINCT timeofday()) > 1 FROM sc_r", "rows=[t]"},
		scCell{"other/current_time", "SELECT count(DISTINCT CURRENT_TIME) FROM sc_r", "rows=[1]"},
		scCell{"other/localtime", "SELECT count(DISTINCT LOCALTIME) FROM sc_r", "rows=[1]"},
		scCell{"other/age_one_arg", "SELECT count(*) FROM sc_r WHERE age(ts) = age(CAST(CURRENT_DATE AS TIMESTAMP), ts)", "rows=[4096]"},
	)
	return cells
}

func scRender(rr *pgconn.ResultReader) string {
	res := rr.Read()
	if res.Err != nil {
		return pwDoorErr(res.Err)
	}
	var rows []string
	for _, r := range res.Rows {
		var f []string
		for _, v := range r {
			if v == nil {
				f = append(f, "NULL")
			} else {
				f = append(f, string(v))
			}
		}
		rows = append(rows, strings.Join(f, "|"))
	}
	return "rows=[" + strings.Join(rows, " ; ") + "]"
}

func scRun(ctx context.Context, conn *pgconn.PgConn, sql string) string {
	mrr := conn.Exec(ctx, sql)
	defer mrr.Close()
	out := "rows=[]"
	for mrr.NextResult() {
		out = scRender(mrr.ResultReader())
	}
	if err := mrr.Close(); err != nil {
		return pwDoorErr(err)
	}
	return out
}

// scKept is an answer this engine keeps for a reason outside the statement
// clock: the function is not implemented here (CURRENT_TIME is ADR-0012
// temporal r23; the rest are not catalogued — a new function is outside
// this arc). A pin that starts agreeing with PostgreSQL FAILS.
var scKept = map[string]string{
	"other/current_timestamp_p":   "ERR 42883",
	"other/transaction_timestamp": "ERR 42883",
	"other/statement_timestamp":   "ERR 42883",
	"other/clock_timestamp":       "ERR 42883",
	"other/timeofday":             "ERR 42883",
	"other/current_time":          "ERR 42883",
	"other/localtime":             "ERR 42703",
	"other/age_one_arg":           "ERR 42883",
}

// scDoorPin is an answer one door gives for a reason that is not the clock,
// identical at base 8e681724 with a literal in now()'s place: on the stage
// DAG a computed item in a LEFT JOIN LATERAL body answers 42000 (column
// "__key_0" does not exist), and a recursive CTE over no table has no scan
// stage (42000, the #1190 family). A pin that starts agreeing FAILS.
func scDoorPin(door, cell string) (string, bool) {
	if !strings.HasPrefix(door, "dag") {
		return "", false
	}
	switch cell {
	case "pos/lateral_left", "pos/recursive_cte":
		return "ERR 42000", true
	}
	return "", false
}

// TestArcSCStatementClockEveryArm: every SQL clock function this engine has
// reads ONE value per statement — on every row, in every clause, and on the
// DAG on every worker — on five doors over pgwire (the embedded engine, the
// embedded engine under a 512 KiB budget, the coordinator's router over the
// DAG, the shuffled DAG and the DAG with four morsel workers).
func TestArcSCStatementClockEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five doors over pgwire")
	}
	if scUnboundClockGuard != nil {
		scUnboundClockGuard(t)
	}
	ctx := context.Background()
	cells := scCells()
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		pg, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pg.Close(ctx)
		pre := append([]string{"SET statement_timeout = '30s'", "SET TimeZone = 'UTC'",
			"DROP SCHEMA IF EXISTS scarc CASCADE", "CREATE SCHEMA scarc", "SET search_path = scarc"}, scPGFixture()...)
		for _, s := range pre {
			if _, err := pg.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		for _, c := range cells {
			if got := scRun(ctx, pg, c.sql); got != c.pg {
				t.Errorf("PostgreSQL %s answered %s, pinned %s", c.name, got, c.pg)
			}
		}
	}
	serve := func(db *wadjet.DB, c *Coordinator) string {
		srv := pgwire.NewServer(db, pgwire.Config{}, nil)
		if c != nil {
			srv.SetRouter(NewQueryRouter(c))
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Shutdown() })
		return srv.Addr()
	}
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, scTables())
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	type door struct{ name, addr string }
	doors := []door{
		{"single", serve(scStandalone(t, ctx, 0), nil)},
		{"spilled512k", serve(scStandalone(t, ctx, 512*1024), nil)},
		{"dag", serve(scStandalone(t, ctx, 0), stand(nil))},
		{"dag-shuffled", serve(scStandalone(t, ctx, 0), stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 }))},
		{"dag-morsel4", serve(scStandalone(t, ctx, 0), stand(func(w *worker.Config) { w.MorselWorkers = 4 }))},
	}
	dump := os.Getenv("SC_ENGINE_DUMP")
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+d.addr+"/test?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			var lines []string
			for _, c := range cells {
				got := scRun(ctx, conn, c.sql)
				lines = append(lines, c.name+"\t"+got)
				if dump != "" {
					continue
				}
				want, why := c.pg, ""
				parts := strings.SplitN(c.name, "/", 3)
				base := parts[0] + "/" + parts[1]
				if k, ok := scKept[base]; ok {
					want, why = k, " (kept: no such function)"
				}
				if k, ok := scDoorPin(d.name, base); ok {
					want, why = k, " (door pin: not the clock)"
				}
				if got != want {
					t.Errorf("%s on %s: %s\n  got  %s\n  want %s (PostgreSQL 17.11 %s%s)", c.name, d.name, c.sql, got, want, c.pg, why)
				}
			}
			if dump != "" {
				_ = os.WriteFile(dump+"."+d.name+".tsv", []byte(strings.Join(lines, "\n")+"\n"), 0o644)
			}
		})
	}
}
