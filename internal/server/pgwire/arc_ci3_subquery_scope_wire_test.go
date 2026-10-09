// SPDX-License-Identifier: MIT

package pgwire

// AN EXPRESSION SUBQUERY'S BODY IS PLANNED IN THE WITH CHAIN WHERE IT IS
// WRITTEN, ON THE WIRE (ADR-0047 stage 3; #1602, #1603, #1606, #1599): the
// RowDescription's OIDs and the DataRow bytes over the simple protocol, the
// extended protocol (text) and the extended protocol with binary results, for
// the four issues' statements, the shadowing and per-run controls, and what
// CREATE TABLE … AS stores from a subquery over a WITH item. At 542b4f37 a
// subquery reading a nested block's WITH item answered NULL (#1602, declared
// text for the scalar), the declaration of `sum((SELECT g FROM s …))` was
// planned without the WITH list and the sum was NULL (#1603), a nested WITH
// reusing the statement's name read the statement's item (#1606), and a
// volatile WITH item read from a correlated subquery or a recursive term was
// evaluated per run (#1599).
//
// PostgreSQL 17.11's answers (testdata/arc_ci3_subquery_scope_wire_pg17.tsv:
// name, mode, ordered, sql, answer) are measured by this test with
// CI3W_PG_DSN naming a scratch database: the same cells, the same client, the
// same fixture as PostgreSQL DDL. A cell's statements are joined by " ;; " and
// the answer is the last one's; kept rows
// (testdata/arc_ci3_subquery_scope_wire_kept.tsv) record catalogued
// differences, and a kept row that starts agreeing FAILS.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

func ci3WireCells() [][3]string {
	return [][3]string{
		{"1602/issue", "true", "WITH s AS (SELECT sum(random()) r FROM cm_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT (SELECT r FROM t) - (SELECT r FROM t) AS d) x"},
		{"1602/L12", "true", "SELECT * FROM (WITH s AS (SELECT sum(random()) AS r FROM cm_big) SELECT (SELECT r FROM s) - (SELECT r FROM s) AS d) x ORDER BY 1"},
		{"1602/detVal", "true", "WITH s AS (SELECT sum(v) r FROM cm_big) SELECT x.d FROM (WITH t AS (SELECT r FROM s) SELECT (SELECT r FROM t) AS d) x"},
		{"1603/Q4", "true", "WITH s AS (SELECT g FROM generate_series(1, 100) g) SELECT sum((SELECT g FROM s WHERE g >= t.id LIMIT 1)) FROM cm_p t"},
		{"1603/S6d", "true", "WITH s AS (SELECT g, random() AS r FROM generate_series(1, 100000) g) SELECT sum((SELECT g FROM s WHERE g >= t.id LIMIT 1)) FROM cm_p t WHERE (SELECT count(*) FROM s) = 100000"},
		{"1603/tblCte", "true", "WITH s AS (SELECT id AS g FROM cm_big) SELECT sum((SELECT min(g) FROM s WHERE g >= t.id)) FROM cm_p t"},
		{"1606/N05", "true", "WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id, (SELECT max(id) FROM c) AS m FROM c) d WHERE id = (SELECT max(id) FROM c) + 1 ORDER BY 1"},
		{"1606/N05b", "true", "WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d WHERE id > (SELECT max(id) FROM c) LIMIT 3"},
		{"1606/N05c", "true", "WITH c AS (SELECT 1 AS id) SELECT * FROM (WITH c AS (SELECT 2 AS id) SELECT id FROM c) d UNION ALL SELECT max(id) FROM c UNION ALL SELECT (SELECT max(id) FROM c)"},
		{"1606/N05d", "true", "WITH c AS (SELECT sum(random()) AS r FROM cm_big) SELECT * FROM (WITH c AS (SELECT 5.0 AS r) SELECT r FROM c) d WHERE r <> (SELECT r FROM c) + (SELECT r FROM c) - (SELECT r FROM c) ORDER BY 1"},
		{"1606/L06", "true", "WITH c AS (SELECT id FROM cm_p WHERE id = 1) SELECT * FROM (WITH c AS (SELECT id FROM cm_p WHERE id = 2) SELECT id FROM c) d WHERE id > (SELECT max(id) FROM c) ORDER BY 1"},
		{"1606/subWithCorr", "true", "WITH c AS (SELECT 1 AS id) SELECT t.id, (WITH c AS (SELECT 2 AS id) SELECT max(id) + t.id FROM c) AS m FROM cm_p t ORDER BY 1"},
		{"1599/issue", "true", "WITH s AS (SELECT random() r) SELECT count(DISTINCT (SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50"},
		{"1599/inner", "true", "SELECT count(DISTINCT (WITH s AS (SELECT random() r) SELECT r + t.id*0 FROM s)) FROM cm_big t WHERE t.id <= 50"},
		{"1599/rec", "true", "WITH RECURSIVE s AS (SELECT random() AS v), r(n, v) AS (SELECT 1, 0.0::float8 UNION ALL SELECT n + 1, (SELECT v FROM s) FROM r WHERE n < 5) SELECT count(DISTINCT v) FROM r WHERE n > 1"},
		{"store/ctas1603", "true", "DROP TABLE IF EXISTS ci3_c ;; CREATE TABLE ci3_c AS WITH s AS (SELECT id AS g FROM cm_big) " +
			"SELECT t.id, (SELECT min(g) FROM s WHERE g > t.id) AS m FROM cm_p t ;; SELECT id, m FROM ci3_c ORDER BY id"},
		{"store/ctasNested", "true", "DROP TABLE IF EXISTS ci3_n ;; CREATE TABLE ci3_n AS SELECT x.d FROM (WITH t AS (SELECT 7 AS r) SELECT (SELECT r FROM t) AS d) x ;; SELECT d FROM ci3_n"},
	}
}

// ci3WirePGFixture is setupCI3WireDB's fixture as PostgreSQL DDL.
const ci3WirePGFixture = `DROP TABLE IF EXISTS cm_p; DROP TABLE IF EXISTS cm_big;
CREATE TABLE cm_p (id BIGINT, v DOUBLE PRECISION);
INSERT INTO cm_p SELECT g, g * 1.5 FROM generate_series(1, 3) g;
CREATE TABLE cm_big (id BIGINT, v DOUBLE PRECISION);
INSERT INTO cm_big SELECT g, g * 1.5 FROM generate_series(1, 20000) g;`

func setupCI3WireDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE cm_p (id BIGINT, v DOUBLE)",
		"INSERT INTO cm_p SELECT g, g * 1.5 FROM generate_series(1, 3) g",
		"CREATE TABLE cm_big (id BIGINT, v DOUBLE)",
		"INSERT INTO cm_big SELECT g, g * 1.5 FROM generate_series(1, 20000) g",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func TestArcCI3SubqueryScopeOnTheWire(t *testing.T) {
	ctx := context.Background()
	if dsn := os.Getenv("CI3W_PG_DSN"); dsn != "" {
		ci3WireMeasure(t, ctx, dsn)
		return
	}
	srv := setupCI3WireDB(t)
	conns := map[string]*pgx.Conn{}
	for m, qm := range ci2WireModes {
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable&default_query_exec_mode=%s", srv.Addr(), qm))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(ctx) })
		conns[m] = conn
	}
	kept := map[string][2]string{}
	for _, k := range ssAuditWireTSV(t, "testdata/arc_ci3_subquery_scope_wire_kept.tsv", 4) {
		kept[k[0]+"/"+k[1]] = [2]string{k[2], k[3]}
	}
	cells := ssAuditWireTSV(t, "testdata/arc_ci3_subquery_scope_wire_pg17.tsv", 5)
	if want := 3 * len(ci3WireCells()); len(cells) != want {
		t.Fatalf("%d measured cells, want %d (three modes per cell): re-measure", len(cells), want)
	}
	for _, c := range cells {
		name, mode, ordered, sql, pg := c[0], c[1], c[2] == "true", c[3], c[4]
		t.Run(name+"/"+mode, func(t *testing.T) {
			got := ci2WireCell(ctx, conns[mode], sql, ordered, mode == "b")
			want, why := pg, "PostgreSQL 17.11 sends"
			if k, ok := kept[name+"/"+mode]; ok {
				if got == pg {
					t.Fatalf("%s now sends PostgreSQL's %s: delete its kept row (%s)", sql, pg, k[1])
				}
				want, why = k[0], "kept: "+k[1]
			}
			if got != want {
				t.Errorf("%s\n  got  %s\n  want %s (%s)", sql, got, want, why)
			}
		})
	}
}

// ci3WireMeasure writes PostgreSQL's answers for every cell and mode.
func ci3WireMeasure(t *testing.T, ctx context.Context, dsn string) {
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 (postgres:17-alpine), measured by TestArcCI3SubqueryScopeOnTheWire with CI3W_PG_DSN: name, mode, ordered, sql, answer.\n")
	for _, mode := range []string{"s", "e", "b"} {
		conn, err := pgx.Connect(ctx, dsn+"&default_query_exec_mode="+ci2WireModes[mode])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, ci3WirePGFixture); err != nil {
			t.Fatal(err)
		}
		for _, c := range ci3WireCells() {
			got := ci2WireCell(ctx, conn, c[2], c[1] == "true", mode == "b")
			fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\n", c[0], mode, c[1], c[2], got)
		}
		conn.Close(ctx)
	}
	if err := os.WriteFile("testdata/arc_ci3_subquery_scope_wire_pg17.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
