// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The statement clock under an ADVANCING clock (#1566, round 2): the one
// accessor's clock is replaced by one that moves on one second at every
// read, so a statement that reads the clock anywhere but its own stamp —
// a plan-time fold, a worker, a second door — answers a different instant
// deterministically, and the read count says so. Every cell must read the
// clock EXACTLY ONCE and answer PostgreSQL 17.11's value, on the five doors
// and the coordinator's small-query fast path.

// scK is an integer 1..7 computed from the clock: the offset / bucket count
// a window function, a LIMIT or a TABLESAMPLE reads.
const scK = "(CAST(CAST(extract(epoch FROM now()) AS BIGINT) % 7 AS INT) + 1)"

// scMs is the statement's instant in epoch milliseconds.
const scMs = "CAST(extract(epoch FROM now()) * 1000 AS BIGINT)"

func scPSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "p", Type: parquet.TypeInt64},
	}}
}

func scPRows() []map[string]any {
	rows := make([]map[string]any, 0, 300)
	for i := 0; i < 300; i++ {
		rows = append(rows, map[string]any{"id": int64(i + 1), "p": int64(i % 3)})
	}
	return rows
}

// scAdvCells is the advancing-clock table: round 1's positions and the
// round-1 review's (window integer arguments, TABLESAMPLE, table-function
// arguments, LIMIT / OFFSET, an IN list, a CAST argument, a partition
// predicate, a CREATE FUNCTION body). "DDL" cells run before the others.
func scAdvCells() []scCell {
	return []scCell{
		{"issue/cross_now_eq_now", "SELECT count(*) FROM sc_k a, sc_k b, sc_k c, sc_k e WHERE CAST(now() AS TIMESTAMP) = CAST(now() AS TIMESTAMP)", "rows=[4096]"},
		{"issue/distinct_now_text", "SELECT count(DISTINCT CAST(now() AS TEXT)) FROM sc_r", "rows=[1]"},
		{"fn/all_equal", "SELECT count(*) FROM sc_r WHERE now() = CURRENT_TIMESTAMP AND LOCALTIMESTAMP = CAST(now() AS TIMESTAMP) AND CURRENT_DATE = CAST(now() AS DATE)", "rows=[4096]"},
		{"pos/where_pushed", "SELECT count(*) FROM sc_r WHERE id > 0 AND now() = now()", "rows=[4096]"},
		{"pos/join_sides", "SELECT count(*) FROM (SELECT id, now() t FROM sc_r) a JOIN (SELECT id, now() t FROM sc_r) b ON a.id = b.id AND a.t = b.t", "rows=[4096]"},
		{"pos/group_by_now", "SELECT count(*) FROM (SELECT now() t, count(*) n FROM sc_r GROUP BY now()) s", "rows=[1]"},
		{"pos/having", "SELECT count(*) FROM (SELECT v FROM sc_r GROUP BY v HAVING max(now()) = min(now())) s", "rows=[7]"},
		{"pos/scalar_subquery", "SELECT count(*) FROM sc_r WHERE now() = (SELECT now())", "rows=[4096]"},
		{"pos/in_subquery", "SELECT count(*) FROM sc_r WHERE now() IN (SELECT now() FROM sc_k)", "rows=[4096]"},
		{"pos/correlated_subquery", "SELECT count(*) FROM sc_k k WHERE (SELECT now() FROM sc_r r WHERE r.id = k.k) = now()", "rows=[8]"},
		{"pos/cte_twice", "WITH c AS (SELECT now() t FROM sc_r) SELECT count(DISTINCT x.t) FROM (SELECT t FROM c UNION ALL SELECT t FROM c) x", "rows=[1]"},
		{"pos/window", "SELECT count(DISTINCT w) FROM (SELECT max(now()) OVER (PARTITION BY v) w FROM sc_r) s", "rows=[1]"},
		{"pos/union_all", "SELECT count(DISTINCT t) FROM (SELECT now() t FROM sc_r UNION ALL SELECT now() FROM sc_r) s", "rows=[1]"},
		{"pos/lateral_dual", "SELECT count(DISTINCT x.t) FROM sc_r, LATERAL (SELECT now() t) x", "rows=[1]"},
		{"pos/values", "SELECT count(DISTINCT t) FROM (VALUES (now()), (now()), (now())) v(t)", "rows=[1]"},
		{"pos/in_list", "SELECT count(*) FROM sc_r WHERE CAST(now() AS DATE) IN (CURRENT_DATE, DATE '2000-01-01')", "rows=[4096]"},
		{"pos/cast_arg", "SELECT count(DISTINCT CAST(CAST(now() AS TEXT) AS TIMESTAMP)) FROM sc_r", "rows=[1]"},
		{"pos/case", "SELECT count(*) FROM sc_r WHERE CASE WHEN id > 0 THEN now() END = now()", "rows=[4096]"},
		{"pos/join_on", "SELECT count(*) FROM sc_r a JOIN sc_r b ON a.id = b.id AND now() = now()", "rows=[4096]"},
		{"pos/distinct", "SELECT count(*) FROM (SELECT DISTINCT now() FROM sc_r) s", "rows=[1]"},
		{"pos/order_by", "SELECT count(DISTINCT t) FROM (SELECT now() t FROM sc_r ORDER BY now(), id) s", "rows=[1]"},
		{"pos/case_const", "SELECT count(*) FROM sc_r WHERE CASE WHEN now() = now() THEN true ELSE false END", "rows=[4096]"},
		{"fold/lag", "SELECT count(*) = 4096 - " + scK + " FROM (SELECT id, lag(id, " + scK + ") OVER (ORDER BY id) l FROM sc_r) s WHERE l = id - " + scK, "rows=[t]"},
		{"fold/lead", "SELECT count(*) = 4096 - " + scK + " FROM (SELECT id, lead(id, " + scK + ") OVER (ORDER BY id) l FROM sc_r) s WHERE l = id + " + scK, "rows=[t]"},
		{"fold/ntile", "SELECT max(n) = " + scK + " FROM (SELECT ntile(" + scK + ") OVER (ORDER BY id) n FROM sc_r) s", "rows=[t]"},
		{"fold/nth_value", "SELECT count(*) = 4096 FROM (SELECT nth_value(id, " + scK + ") OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) n FROM sc_r) s WHERE n = " + scK, "rows=[t]"},
		{"fold/tablesample", "SELECT count(*) = 4096 * (" + scK + " % 2) FROM sc_r TABLESAMPLE BERNOULLI (100 * (" + scK + " % 2))", "rows=[t]"},
		{"fold/generate_series", "SELECT count(*) FROM generate_series(" + scMs + ", " + scMs + " + 5) g WHERE g = " + scMs, "rows=[1]"},
		{"fold/generate_series_join", "SELECT count(*) = " + scK + " FROM generate_series(1, " + scK + ") g JOIN sc_k ON sc_k.k = g WHERE g <= " + scK, "rows=[t]"},
		{"fold/limit", "SELECT count(*) = " + scK + " FROM (SELECT id FROM sc_r ORDER BY id LIMIT " + scK + ") s", "rows=[t]"},
		{"fold/offset", "SELECT min(id) = " + scK + " + 1 FROM (SELECT id FROM sc_r ORDER BY id OFFSET " + scK + ") s", "rows=[t]"},
		{"fold/partition", "SELECT count(*) = 100 FROM sc_p WHERE p = CAST(extract(epoch FROM now()) AS BIGINT) % 3", "rows=[t]"},
		{"fold/udf", "SELECT count(*) FROM sc_r WHERE sc_adv_u(id) = now()", "rows=[4096]"},
		{"fold/udf_nested", "SELECT count(DISTINCT sc_adv_u2(id)) FROM sc_r", "rows=[1]"},
		{"fold/lag_default", "SELECT count(*) FROM (SELECT lag(now(), 1, now()) OVER (ORDER BY id) l FROM sc_r) s WHERE l = now()", "rows=[4096]"},
		{"ddl/default", "CREATE TABLE sc_def (id BIGINT, ts TIMESTAMP DEFAULT now())", "rows=[]"},
	}
}

// scAdvKept is a cell this engine answers differently from PostgreSQL for a
// reason that is not the clock, base-identical; a pin that starts agreeing
// FAILS. Filled from the measurement.
var scAdvKept = map[string]string{
	// LIMIT / OFFSET take a number literal only: any expression there is a
	// syntax error (42601), a clock or not — base-identical, ADR-0012
	// temporal r27.
	"fold/limit":  "ERR 42601",
	"fold/offset": "ERR 42601",
	// A column DEFAULT is not in this engine's CREATE TABLE grammar (42601),
	// whatever the default — ADR-0012 temporal r27.
	"ddl/default": "ERR 42601",
}

func TestArcSCAdvancingClockOneReadEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six doors over pgwire")
	}
	ctx := context.Background()
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		pg, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pg.Close(ctx)
		pre := append([]string{"SET statement_timeout = '30s'", "SET TimeZone = 'UTC'",
			"DROP SCHEMA IF EXISTS scadv CASCADE", "CREATE SCHEMA scadv", "SET search_path = scadv"}, scPGFixture()...)
		pre = append(pre,
			"CREATE TABLE sc_p (id bigint, p bigint)",
			"INSERT INTO sc_p SELECT g, (g - 1) % 3 FROM generate_series(1, 300) g",
			"CREATE FUNCTION sc_adv_u(bigint) RETURNS timestamptz LANGUAGE sql AS 'SELECT now()'",
			"CREATE FUNCTION sc_adv_u2(bigint) RETURNS timestamptz LANGUAGE sql AS 'SELECT sc_adv_u($1)'")
		for _, s := range pre {
			if _, err := pg.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		for _, c := range scAdvCells() {
			if got := scRun(ctx, pg, c.sql); got != c.pg {
				t.Errorf("PostgreSQL %s answered %s, pinned %s", c.name, got, c.pg)
			}
		}
	}

	standalone := func(budget int64) *wadjet.DB {
		db := scStandalone(t, ctx, budget)
		if err := db.CreateTable(ctx, "sc_p", scPSchema(), []string{"p"}); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester("sc_p", scPSchema(), []string{"p"}, ingest.Config{MaxBufferRows: 1000, RowGroupSize: 64})
		if err := ing.Ingest(ctx, scPRows()); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
		return db
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
		if err := infra.cat.CreateTable(ctx, "sc_p", scPSchema(), []string{"p"}); err != nil {
			t.Fatal(err)
		}
		ing := ingest.New(infra.cat, "sc_p", scPSchema(), []string{"p"}, ingest.Config{MaxBufferRows: 1000, RowGroupSize: 64})
		if err := ing.Ingest(ctx, scPRows()); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	type door struct{ name, addr string }
	doors := []door{
		{"single", serve(standalone(0), nil)},
		{"spilled512k", serve(standalone(512*1024), nil)},
		{"dag", serve(standalone(0), stand(nil))},
		{"dag-shuffled", serve(standalone(0), stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 }))},
		{"dag-morsel4", serve(standalone(0), stand(func(w *worker.Config) { w.MorselWorkers = 4 }))},
		{"fastpath", serve(standalone(0), stand(nil, func(c *Config) { c.LocalFastPathBytes = 64 << 20 }))},
	}

	// The advancing clock, installed through the one accessor: every read
	// moves it on one second.
	var reads atomic.Int64
	base := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	restore := expr.SetClockForTest(func() time.Time {
		n := reads.Add(1)
		return base.Add(time.Duration(n) * time.Second)
	})
	defer restore()

	dump := os.Getenv("SC_ENGINE_DUMP")
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+d.addr+"/test?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			for _, f := range []string{"CREATE OR REPLACE FUNCTION sc_adv_u(x) AS now()", "CREATE OR REPLACE FUNCTION sc_adv_u2(x) AS sc_adv_u(x)"} {
				if got := scRun(ctx, conn, f); strings.HasPrefix(got, "ERR") {
					t.Fatalf("%s: %s", f, got)
				}
			}
			var lines []string
			for _, c := range scAdvCells() {
				before := reads.Load()
				got := scRun(ctx, conn, c.sql)
				n := reads.Load() - before
				lines = append(lines, fmt.Sprintf("%s\t%s\treads=%d", c.name, got, n))
				if dump != "" {
					continue
				}
				want, why := c.pg, ""
				if k, ok := scAdvKept[c.name]; ok {
					want, why = k, " (kept)"
				}
				if got != want {
					t.Errorf("%s on %s: %s\n  got  %s\n  want %s (PostgreSQL 17.11 %s%s)", c.name, d.name, c.sql, got, want, c.pg, why)
				}
				if n != 1 {
					t.Errorf("%s on %s read the clock %d times; a statement reads it once", c.name, d.name, n)
				}
			}
			if dump != "" {
				_ = os.WriteFile(dump+"."+d.name+".tsv", []byte(strings.Join(lines, "\n")+"\n"), 0o644)
			}
		})
	}
}
