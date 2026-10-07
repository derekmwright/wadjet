// SPDX-License-Identifier: MIT

package pgwire

// A NODE'S DECLARED OUTPUT IS READ BY IDENTITY, ON THE WIRE (#1393, ADR-0047
// stage 2): the RowDescription's OIDs and the DataRow bytes, over the simple
// protocol, the extended protocol (text) and the extended protocol with binary
// results, for #1393's statements — an expression whose text ends in a
// qualified column of another type, over a join with a derived table — and
// for the same expression read through a GROUP BY key: zero rows, SUM and
// arithmetic over the grouped derived table, and what INSERT … SELECT *, CREATE
// TABLE … AS and MERGE store. At a0f0c322 the key's declaration was the column
// its text named after the last dot: `x + a.i` declared int8 where PostgreSQL
// declares float8, and SUM over `f + a.i` answered 98 for 101.375; and an
// aggregate's argument `sum(q.i)` over `(SELECT f AS i FROM ss_t) q` was
// typed from the scan's integer `i`: bigint 99 for 99.375.
//
// PostgreSQL 17.11's answers (testdata/arc_ci2_declared_output_wire_pg17.tsv:
// name, mode, ordered, sql, answer) are measured by this test with
// CI2W_PG_DSN naming a scratch database: the same cells, the same client, the
// same fixture as PostgreSQL DDL. A cell's statements are joined by " ;; "
// and the answer is the last one's; kept rows record catalogued differences.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func ci2WireCells() [][3]string {
	join := "FROM ss_t a JOIN (SELECT id, f AS x FROM ss_t) b ON a.id = b.id"
	bid := "FROM ss_t a JOIN (SELECT id AS bid FROM ss_t) b ON a.id = b.bid"
	return [][3]string{
		{"issue/panic", "true", "SELECT x + a.i AS v " + join + " WHERE a.id = 1"},
		{"issue/noAlias", "true", "SELECT x + a.i " + join + " WHERE a.id = 1"},
		{"issue/zeroRow", "true", "SELECT x + a.i AS v " + join + " WHERE a.id = 99"},
		{"issue/numericStar", "true", "SELECT * FROM (SELECT a.id, CAST(f AS NUMERIC) + a.i AS v " + bid + ") s2 ORDER BY id"},
		{"issue/numericTwo", "true", "SELECT a.id, CAST(f AS NUMERIC) + a.i AS c, n + a.i AS e " + bid + " ORDER BY 1"},
		{"issue/floatStar", "true", "SELECT * FROM (SELECT a.id, f + a.i AS v " + bid + ") s2 ORDER BY id"},
		{"grp/zeroKey", "true", "SELECT x + a.i AS v, count(*) AS c " + join + " WHERE a.id = 99 GROUP BY x + a.i"},
		{"grp/key", "true", "SELECT x + a.i AS v, count(*) AS c " + join + " GROUP BY x + a.i ORDER BY 1"},
		{"grp/sumOverKey", "true", "SELECT sum(q.v) AS s FROM (SELECT f + a.i AS v FROM ss_t a GROUP BY f + a.i) q"},
		{"grp/mulOverKey", "true", "SELECT q.v * 2 AS w FROM (SELECT f + a.i AS v FROM ss_t a GROUP BY f + a.i) q ORDER BY 1"},
		{"grp/distinctSum", "true", "SELECT sum(q.v) AS s FROM (SELECT DISTINCT f + a.i AS v FROM ss_t a) q"},
		{"grp/numKeyJoin", "true", "SELECT u.b * t.n AS v, count(*) AS c FROM ss_t t JOIN (SELECT id, n AS b FROM ss_t) u ON t.id = u.id GROUP BY u.b * t.n ORDER BY 1"},
		{"grp/wideSum", "true", "SELECT sum(q.v) AS s FROM (SELECT a.b + a.i AS v FROM ss_t a GROUP BY a.b + a.i) q"},
		{"f1649/derivedKey", "true", "SELECT d.x + 1 AS k, count(*) FROM ss_t t LEFT JOIN (SELECT id, v AS x FROM ss_i) d ON d.id = t.id GROUP BY d.x + 1 ORDER BY 1"},
		{"store/insertStarNumeric", "true", "DROP TABLE IF EXISTS ci2_n ;; CREATE TABLE ci2_n (id BIGINT, v NUMERIC) ;; " +
			"INSERT INTO ci2_n SELECT * FROM (SELECT a.id, CAST(f AS NUMERIC) + a.i AS v " + bid + ") s2 ;; SELECT id, v FROM ci2_n ORDER BY id"},
		{"store/insertStarInt", "true", "DROP TABLE IF EXISTS ci2_i ;; CREATE TABLE ci2_i (id BIGINT, v INT) ;; " +
			"INSERT INTO ci2_i SELECT * FROM (SELECT a.id, CAST(f AS NUMERIC) + a.i AS v " + bid + ") s2 ;; SELECT id, v FROM ci2_i ORDER BY id"},
		{"store/insertGrouped", "true", "DROP TABLE IF EXISTS ci2_f ;; CREATE TABLE ci2_f (v DOUBLE PRECISION) ;; " +
			"INSERT INTO ci2_f SELECT * FROM (SELECT f + a.i AS v FROM ss_t a GROUP BY f + a.i) q ;; SELECT v FROM ci2_f ORDER BY v"},
		{"store/ctasGrouped", "true", "DROP TABLE IF EXISTS ci2_c ;; CREATE TABLE ci2_c AS SELECT f + a.i AS v FROM ss_t a GROUP BY f + a.i ;; SELECT v FROM ci2_c ORDER BY v"},
		{"store/ctasSumGrouped", "true", "DROP TABLE IF EXISTS ci2_cs ;; CREATE TABLE ci2_cs AS SELECT sum(q.v) AS s FROM (SELECT f + a.i AS v FROM ss_t a GROUP BY f + a.i) q ;; SELECT s FROM ci2_cs"},
		{"store/merge", "true", "DROP TABLE IF EXISTS ci2_m ;; CREATE TABLE ci2_m (id BIGINT, v DOUBLE PRECISION) ;; " +
			"MERGE INTO ci2_m USING (SELECT a.id, f + a.i AS v " + bid + ") s2 ON ci2_m.id = s2.id WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s2.id, s2.v) ;; " +
			"SELECT id, v FROM ci2_m ORDER BY id"},
		{"store/mergeGrouped", "true", "DROP TABLE IF EXISTS ci2_mg ;; CREATE TABLE ci2_mg (k DOUBLE PRECISION, c BIGINT) ;; " +
			"MERGE INTO ci2_mg USING (SELECT f + a.i AS v, count(*) AS c FROM ss_t a GROUP BY f + a.i) s2 ON ci2_mg.k = s2.v WHEN NOT MATCHED THEN INSERT (k, c) VALUES (s2.v, s2.c) ;; " +
			"SELECT k, c FROM ci2_mg ORDER BY k"},
		// An aggregate's argument over a derived column that shadows a scan
		// column of another type (the closure review's B1: D7a–g, r04, r12,
		// a05, v1, v2, v12).
		{"agg/D7a", "true", "SELECT sum(q.i) AS s FROM (SELECT f AS i FROM ss_t) q"},
		{"agg/D7b", "true", "SELECT sum(q.n) AS s FROM (SELECT f AS n FROM ss_t) q"},
		{"agg/D7c", "true", "SELECT sum(u.i) AS s FROM ss_t t JOIN (SELECT id, f AS i FROM ss_t) u ON t.id = u.id"},
		{"agg/D7d", "true", "SELECT avg(q.i) AS a FROM (SELECT f AS i FROM ss_t) q"},
		{"agg/D7e", "true", "SELECT q.o, sum(q.i) AS s FROM (SELECT o, f AS i FROM ss_t) q GROUP BY q.o ORDER BY 1"},
		{"agg/D7f", "true", "SELECT sum(q.v) AS s FROM (SELECT g AS v FROM ss_i) q"},
		{"agg/D7g", "true", "SELECT max(q.i) + 0.5 AS m FROM (SELECT f AS i FROM ss_t) q"},
		{"agg/r04", "true", "SELECT sum(r.i) AS si, sum(r.f) AS sf FROM (SELECT q.id, q.f AS i, q.i AS f FROM (SELECT p.id, p.f AS i, p.i AS f FROM (SELECT id, i AS f, f AS i FROM ss_t) p) q) r"},
		{"agg/r12", "true", "SELECT sum(q.i) AS s FROM (SELECT DISTINCT p.f AS i FROM (SELECT f FROM ss_t) p) q"},
		{"agg/a05", "true", "SELECT sum(d.i) AS si, sum(d.f) AS sf FROM (SELECT f, i FROM ss_t) d(i, f)"},
		{"agg/v1", "true", "SELECT sum(q.i) AS s FROM (SELECT f AS i FROM ss_t) q"},
		{"agg/v2", "true", "SELECT sum(q.i) AS s FROM (SELECT p.f AS i FROM (SELECT f FROM ss_t) p) q"},
		{"agg/v12", "true", "SELECT sum(q.i) AS s FROM (SELECT f AS i FROM ss_t) q GROUP BY q.i IS NULL ORDER BY 1"},
	}
}

// ci2WirePGFixture is setupSSAuditWireDB's fixture as PostgreSQL DDL.
const ci2WirePGFixture = `DROP TABLE IF EXISTS ss_t; DROP TABLE IF EXISTS ss_i;
CREATE TABLE ss_t (id bigint, i integer, b bigint, f double precision, n numeric(10,2), s varchar, o boolean, d date, ts timestamp, u uuid, a integer[]);
INSERT INTO ss_t VALUES
 (1, 3, 30, 1.5, 2.25, 'abc', true, '2024-03-04', '2024-03-04 12:00:00', '00000000-0000-4000-8000-000000000001', '{1,2}'),
 (2, -7, -70, -2.5, -3.5, 'Hello', false, '1970-01-01', '1970-01-01 00:00:00', '00000000-0000-4000-8000-000000000002', '{3}'),
 (3, 5, 9000000000, 0.25, 10.00, 'zz', true, '9999-12-31', '9999-12-31 23:59:59.999', '00000000-0000-4000-8000-000000000003', '{4,5,6}'),
 (4, 0, 0, 0.0, 0.00, '', false, '1000-01-01', '1000-01-01 00:00:00', '00000000-0000-4000-8000-000000000004', '{7}'),
 (5, 1, 1, 100.125, 0.01, 'x', true, '1969-12-31', '1969-12-31 23:59:59.999', '00000000-0000-4000-8000-000000000005', '{8}'),
 (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
CREATE TABLE ss_i (id bigint, v integer, g double precision, m numeric(10,2));
INSERT INTO ss_i VALUES (1, 5, 0.5, 1.25), (2, 6, 0.25, NULL);`

var ci2WireModes = map[string]string{"s": "simple_protocol", "e": "exec", "b": "describe_exec"}

func ci2WireCell(ctx context.Context, conn *pgx.Conn, sql string, ordered, binary bool) string {
	var got, firstErr string
	for _, st := range strings.Split(sql, " ;; ") {
		r := ssAuditWireRun(ctx, conn, st, ordered, binary)
		if strings.HasPrefix(r, "ERR") && firstErr == "" && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(st)), "DROP") {
			firstErr = r
		}
		got = r
	}
	if firstErr != "" {
		return firstErr
	}
	return got
}

func TestArcCI2DeclaredOutputOnTheWire(t *testing.T) {
	ctx := context.Background()
	if dsn := os.Getenv("CI2W_PG_DSN"); dsn != "" {
		ci2WireMeasure(t, ctx, dsn)
		return
	}
	srv := setupSSAuditWireDB(t)
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
	for _, k := range ssAuditWireTSV(t, "testdata/arc_ci2_declared_output_wire_kept.tsv", 4) {
		kept[k[0]+"/"+k[1]] = [2]string{k[2], k[3]}
	}
	cells := ssAuditWireTSV(t, "testdata/arc_ci2_declared_output_wire_pg17.tsv", 5)
	if want := 3 * len(ci2WireCells()); len(cells) != want {
		t.Fatalf("%d measured cells, want %d (three modes per cell): re-measure", len(cells), want)
	}
	var dump strings.Builder
	if p := os.Getenv("CI2W_DUMP"); p != "" {
		t.Cleanup(func() { _ = os.WriteFile(p, []byte(dump.String()), 0o644) })
	}
	for _, c := range cells {
		name, mode, ordered, sql, pg := c[0], c[1], c[2] == "true", c[3], c[4]
		t.Run(name+"/"+mode, func(t *testing.T) {
			got := ci2WireCell(ctx, conns[mode], sql, ordered, mode == "b")
			fmt.Fprintf(&dump, "%s\t%s\t%s\n", name, mode, got)
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

// ci2WireMeasure writes PostgreSQL's answers for every cell and mode.
func ci2WireMeasure(t *testing.T, ctx context.Context, dsn string) {
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 (postgres:17-alpine), measured by TestArcCI2DeclaredOutputOnTheWire with CI2W_PG_DSN: name, mode, ordered, sql, answer.\n")
	for _, mode := range []string{"s", "e", "b"} {
		conn, err := pgx.Connect(ctx, dsn+"&default_query_exec_mode="+ci2WireModes[mode])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, ci2WirePGFixture); err != nil {
			t.Fatal(err)
		}
		for _, c := range ci2WireCells() {
			got := ci2WireCell(ctx, conn, c[2], c[1] == "true", mode == "b")
			fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\n", c[0], mode, c[1], c[2], got)
		}
		conn.Close(ctx)
	}
	if err := os.WriteFile("testdata/arc_ci2_declared_output_wire_pg17.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
