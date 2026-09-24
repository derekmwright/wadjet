// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/oracle/cwfixture"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc CW's wire gate: a container value DECLARES its type and RENDERS as
// PostgreSQL 17.11 does (#1250 #1017 #1133 #1303 #1268 #1021).
//
// Every cell is one statement with the RowDescription's OIDs and the text
// rows the simple protocol (psql's) must carry. The expectations are
// PostgreSQL's own answers, measured on a live 17.11 over the same fixture
// (cwfixture.PGDDL); where this engine declares differently for a reason
// ADR-0012 records — a nested array or a ROW element keeps text, a network
// element is text[], a ROW is text, an integer literal list is int4 but a
// fractional one is float8 (ADR-0024's literal deferral) — pgOIDs / pgText
// hold PostgreSQL's answer beside ours, and with WADJET_PG_DSN set the gate
// re-measures PostgreSQL and fails if the pinned answer drifted. A cell with
// pg "-" has no PostgreSQL spelling (MAP, tcp_flags, semver_parse), and its
// rendering is this engine's recorded rule (docs/postgres-differences.md).
//
// At base 83cd4a93 every constructor, function, derived-table, VALUES,
// UNION and zero-row cell failed: the value went out as text in Go's
// rendering (`[1 2 3]`, `[SYN ACK]`, `[1718454645500]`) under OID 25.
type cwWireCell struct {
	name   string
	sql    string
	pg     string // PostgreSQL's spelling when it differs; "-" when it has none
	oids   string
	text   string
	pgOIDs string // PostgreSQL's OIDs when they differ ("*" = a composite's catalog OID)
	pgText string // PostgreSQL's text when it differs
}

var cwWireCells = []cwWireCell{
	// --- the ARRAY constructor (#1250) ---
	{name: "K1/int", sql: `SELECT ARRAY[1,2,3] AS v`, oids: "1007", text: "{1,2,3}"},
	{name: "K2/text-quoting", sql: `SELECT ARRAY['a b','c','','NULL',NULL,'x"y','a,b','{}'] AS v`,
		oids: "1009", text: `{"a b",c,"","NULL",NULL,"x\"y","a,b","{}"}`},
	{name: "K3/timestamp", sql: `SELECT ARRAY[TIMESTAMP '2024-06-15 12:30:45.5', NULL] AS v`,
		oids: "1115", text: `{"2024-06-15 12:30:45.5",NULL}`},
	{name: "K4/date", sql: `SELECT ARRAY[DATE '2024-01-02', NULL] AS v`, oids: "1182", text: "{2024-01-02,NULL}"},
	{name: "K5/bool", sql: `SELECT ARRAY[true, false, NULL] AS v`, oids: "1000", text: "{t,f,NULL}"},
	{name: "K6/uuid", sql: `SELECT ARRAY[CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)] AS v`,
		oids: "2951", text: "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"},
	{name: "K7/null-element", sql: `SELECT ARRAY[1, NULL, 3] AS v`, oids: "1007", text: "{1,NULL,3}"},
	// `int[]` is the array of what `CAST(x AS INT)` declares, which is bigint
	// here (ADR-0012 item 12's recorded integer-width divergence).
	{name: "K8/empty", sql: `SELECT ARRAY[]::int[] AS v`, oids: "1016", text: "{}", pgOIDs: "1007"},
	{name: "K9/ipv4", sql: `SELECT ARRAY[CAST('1.2.3.4' AS IPV4)] AS v`, pg: `SELECT ARRAY['1.2.3.4'::inet] AS v`,
		oids: "1009", text: "{1.2.3.4}", pgOIDs: "1041"},
	{name: "K10/timestamp-element", sql: `SELECT (ARRAY[TIMESTAMP '2024-06-15 12:30:45.5'])[1] AS e`,
		oids: "1114", text: "2024-06-15 12:30:45.5"},
	{name: "K11/nested", sql: `SELECT ARRAY[ARRAY[1,2],ARRAY[3,4]] AS v`,
		oids: "25", text: "{{1,2},{3,4}}", pgOIDs: "1007"},
	{name: "K12/fractional-literals", sql: `SELECT ARRAY[1.5, 2.25] AS v`,
		oids: "1022", text: "{1.5,2.25}", pgOIDs: "1231"},
	{name: "K13/two-columns", sql: `SELECT ARRAY['x'] AS v, ARRAY[TIMESTAMP '2024-01-01 00:00:00'] AS w`,
		oids: "1009,1115", text: `{x}|{"2024-01-01 00:00:00"}`},

	// --- stored columns ---
	{name: "S1/stored", sql: `SELECT ai, at, ats, ad, ab, au FROM cw ORDER BY id`,
		oids: "1007,1009,1115,1182,1000,2951",
		text: `{1,2,3}|{"a b",c,"","NULL",NULL,"x\"y","a,b","{}"}|{"2024-06-15 12:30:45.5",NULL}|{2024-01-02}|{t,f,NULL}|{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}` +
			` || {}|NULL|{}|NULL|{}|{}` +
			` || {NULL,5}|{z}|{"1999-12-31 23:59:59"}|{1970-01-01,NULL}|{t}|NULL`},
	{name: "S2/stored-ipv4", sql: `SELECT aip FROM cw ORDER BY id`,
		oids: "1009", text: "{1.2.3.4,10.0.0.1} || NULL || {}", pgOIDs: "1041"},
	{name: "S3/stored-row-with-timestamp", sql: `SELECT r FROM cw ORDER BY id`,
		oids: "25", text: `(7,"x y","2024-06-15 12:30:45") || NULL || (,"",)`, pgOIDs: "*"},
	{name: "S4/stored-array-of-row", sql: `SELECT ar FROM cw ORDER BY id`,
		oids: "25", text: `{"(1,a)","(2,\"b c\")"} || {} || NULL`, pgOIDs: "*"},
	{name: "S5/stored-map", sql: `SELECT m FROM cw ORDER BY id`, pg: "-",
		oids: "25", text: "{a: 1, b: 2} || {} || NULL"},

	// --- container-returning functions (#1017) ---
	{name: "F1/tcp_flags", sql: `SELECT tcp_flags(18) AS v`, pg: "-", oids: "1009", text: "{SYN,ACK}"},
	{name: "F2/map-functions", sql: `SELECT map_keys(m) AS k, map_values(m) AS vv, map_entries(m) AS e FROM cw ORDER BY id`,
		pg: "-", oids: "1009,1016,25",
		text: `{a,b}|{1,2}|{"(a,1)","(b,2)"} || {}|{}|{} || NULL|NULL|NULL`},
	{name: "F3/current_schemas", sql: `SELECT current_schemas(true) AS v`,
		oids: "1009", text: "{pg_catalog,public}", pgOIDs: "1003"},
	{name: "F4/tcp_flags-over-a-derived-table", sql: `SELECT f, f[1] AS e FROM (SELECT tcp_flags(18) AS f) s`,
		pg: "-", oids: "1009,25", text: "{SYN,ACK}|SYN"},

	// --- a derived table / CTE / VALUES / UNION (#1303) ---
	{name: "D1/derived-subscript-and-any", sql: `SELECT v, v[1] AS e, 2 = ANY(v) AS m FROM (SELECT ARRAY[1,2] AS v) s`,
		oids: "1007,23,16", text: "{1,2}|1|t"},
	{name: "D2/derived-stored-timestamps", sql: `SELECT ats, ats[1] AS e FROM (SELECT ats FROM cw WHERE id = 1) s`,
		oids: "1115,1114", text: `{"2024-06-15 12:30:45.5",NULL}|2024-06-15 12:30:45.5`},
	{name: "D3/cte", sql: `WITH c AS (SELECT ARRAY['x','y'] AS v) SELECT v, v[2] AS e FROM c`,
		oids: "1009,25", text: "{x,y}|y"},
	{name: "D4/derived-row-field", sql: `SELECT r, (r).t AS t FROM (SELECT r FROM cw WHERE id = 1) s`,
		oids: "25,1114", text: `(7,"x y","2024-06-15 12:30:45")|2024-06-15 12:30:45`, pgOIDs: "*,1114"},
	{name: "D5/derived-rename-ordered", sql: `SELECT a FROM (SELECT ai AS a FROM cw) s ORDER BY a`,
		oids: "1007", text: "{} || {1,2,3} || {NULL,5}"},
	{name: "V1/values", sql: `SELECT v FROM (VALUES (ARRAY[1,2]), (ARRAY[3])) t(v) ORDER BY v`,
		oids: "1007", text: "{1,2} || {3}"},
	{name: "V2/values-timestamp-element", sql: `SELECT v[1] AS e FROM (VALUES (ARRAY[TIMESTAMP '2024-06-15 12:30:45.5'])) t(v)`,
		oids: "1114", text: "2024-06-15 12:30:45.5"},
	{name: "U1/union", sql: `SELECT ARRAY[2,3] AS v UNION ALL SELECT ARRAY[10] ORDER BY v`,
		oids: "1007", text: "{2,3} || {10}"},
	{name: "U2/union-subscript", sql: `SELECT v[2] AS e FROM (SELECT ARRAY[1,2] AS v UNION ALL SELECT ARRAY[3,4]) u ORDER BY e`,
		oids: "23", text: "2 || 4"},
	{name: "U3/union-stored-and-constructed", sql: `SELECT ai FROM cw WHERE id = 1 UNION ALL SELECT ARRAY[9] ORDER BY 1`,
		oids: "1007", text: "{1,2,3} || {9}"},

	// --- the zero-row declaration (#1133) ---
	{name: "Z1/zero-row-constructor", sql: `SELECT ARRAY[1,2] AS v WHERE false`, oids: "1007", text: ""},
	{name: "Z2/zero-row-stored", sql: `SELECT ai, ats, ad, au, ab, at FROM cw WHERE false`,
		oids: "1007,1115,1182,2951,1000,1009", text: ""},
	{name: "Z3/zero-row-tcp_flags", sql: `SELECT tcp_flags(18) AS v WHERE false`, pg: "-", oids: "1009", text: ""},
	{name: "Z4/zero-row-map_keys", sql: `SELECT map_keys(m) AS k FROM cw WHERE false`, pg: "-", oids: "1009", text: ""},
	{name: "Z5/zero-row-derived", sql: `SELECT v FROM (SELECT ARRAY[DATE '2024-01-02'] AS v) s WHERE false`,
		oids: "1182", text: ""},

	// --- an element read back out (#1268) ---
	{name: "E1/elements", sql: `SELECT ats[1] AS e, ad[1] AS d, (r).t AS t FROM cw WHERE id = 1`,
		oids: "1114,1082,1114", text: "2024-06-15 12:30:45.5|2024-01-02|2024-06-15 12:30:45"},
	{name: "T1/cast-to-text", sql: `SELECT CAST(ai AS TEXT) AS v, CAST(ARRAY[1,2] AS TEXT) AS w, CAST(ats AS TEXT) AS x FROM cw WHERE id = 1`,
		oids: "25,25,25", text: `{1,2,3}|{1,2}|{"2024-06-15 12:30:45.5",NULL}`},

	// --- ordering (#1021): element-wise, an empty array first, a NULL element last ---
	// (O1/O2 fold `::int[]`'s bigint[] with the int4[] constructors, so they
	// are bigint[] here where PostgreSQL's `int` is int4 — ADR-0012 item 12.)
	{name: "O1/order-int", sql: `SELECT v FROM (VALUES (ARRAY[100]), (ARRAY[50]), (ARRAY[50,1]), (ARRAY[NULL::int]), (ARRAY[]::int[]), (ARRAY[50,NULL]), (ARRAY[-1])) t(v) ORDER BY v`,
		oids: "1016", text: "{} || {-1} || {50} || {50,1} || {50,NULL} || {100} || {NULL}", pgOIDs: "1007"},
	{name: "O2/min-max-int", sql: `SELECT min(v) AS lo, max(v) AS hi FROM (VALUES (ARRAY[100]), (ARRAY[50]), (ARRAY[50,1]), (ARRAY[NULL::int]), (ARRAY[]::int[]), (ARRAY[-1])) t(v)`,
		oids: "1016,1016", text: "{}|{NULL}", pgOIDs: "1007,1007"},
	{name: "O3/order-text", sql: `SELECT v FROM (VALUES (ARRAY['b']), (ARRAY['ab']), (ARRAY['a','z']), (ARRAY['B']), (ARRAY[NULL::text])) t(v) ORDER BY v`,
		oids: "1009", text: "{B} || {a,z} || {ab} || {b} || {NULL}"},
	{name: "O4/order-timestamp-desc", sql: `SELECT v FROM (VALUES (ARRAY[TIMESTAMP '2024-01-02 00:00:00']), (ARRAY[TIMESTAMP '1999-12-31 00:00:00']), (ARRAY[TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '1970-01-01 00:00:00'])) t(v) ORDER BY v DESC`,
		oids: "1115", text: `{"2024-01-02 00:00:00"} || {"2024-01-01 00:00:00","1970-01-01 00:00:00"} || {"1999-12-31 00:00:00"}`},
	{name: "O5/order-date", sql: `SELECT v FROM (VALUES (ARRAY[DATE '2024-01-10']), (ARRAY[DATE '2024-01-09']), (ARRAY[DATE '2023-12-31'])) t(v) ORDER BY v`,
		oids: "1182", text: "{2023-12-31} || {2024-01-09} || {2024-01-10}"},
	{name: "O6/order-bool", sql: `SELECT v FROM (VALUES (ARRAY[true]), (ARRAY[false]), (ARRAY[false, true])) t(v) ORDER BY v`,
		oids: "1000", text: "{f} || {f,t} || {t}"},
	{name: "O7/order-uuid", sql: `SELECT v FROM (VALUES (ARRAY[CAST('b0000000-0000-0000-0000-000000000000' AS UUID)]), (ARRAY[CAST('a0000000-0000-0000-0000-000000000000' AS UUID)])) t(v) ORDER BY v`,
		oids: "2951", text: "{a0000000-0000-0000-0000-000000000000} || {b0000000-0000-0000-0000-000000000000}"},
	{name: "O8/min-max-stored", sql: `SELECT min(ats) AS lo, max(ats) AS hi, min(ai) AS ilo, max(ai) AS ihi FROM cw`,
		oids: "1115,1115,1007,1007", text: `{}|{"2024-06-15 12:30:45.5",NULL}|{}|{NULL,5}`},
	{name: "O9/order-stored-desc", sql: `SELECT ai FROM cw ORDER BY ai DESC`,
		oids: "1007", text: "{NULL,5} || {1,2,3} || {}"},
	{name: "O10/distinct", sql: `SELECT DISTINCT v FROM (VALUES (ARRAY[2]), (ARRAY[10]), (ARRAY[2])) t(v) ORDER BY v`,
		oids: "1007", text: "{2} || {10}"},
	{name: "O11/order-fractional", sql: `SELECT v FROM (VALUES (ARRAY[1.5]), (ARRAY[10.25]), (ARRAY[2.0])) t(v) ORDER BY v`,
		oids: "1022", text: "{1.5} || {2} || {10.25}", pgOIDs: "1231", pgText: "{1.5} || {2.0} || {10.25}"},
	{name: "O12/order-ipv4", sql: `SELECT v FROM (VALUES (ARRAY[CAST('10.0.0.1' AS IPV4)]), (ARRAY[CAST('9.0.0.1' AS IPV4)])) t(v) ORDER BY v`,
		pg:   `SELECT v FROM (VALUES (ARRAY['10.0.0.1'::inet]), (ARRAY['9.0.0.1'::inet])) t(v) ORDER BY v`,
		oids: "1009", text: "{9.0.0.1} || {10.0.0.1}", pgOIDs: "1041"},
}

// cwDB is the embedded engine over cwfixture.
func cwDB(t *testing.T) *wadjet.DB {
	t.Helper()
	db := pcCorpusDB(t)
	ctx := context.Background()
	sch := cwfixture.Schema()
	if err := db.CreateTable(ctx, cwfixture.Table, sch, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester(cwfixture.Table, sch, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, cwfixture.Rows()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// cwSimple runs sql over the SIMPLE protocol (text format, psql's) and
// answers the RowDescription's OIDs and the rows, `|` between fields and
// ` || ` between rows.
func cwSimple(ctx context.Context, conn *pgx.Conn, sql string) (string, string, error) {
	mrr := conn.PgConn().Exec(ctx, sql)
	var oids, rows []string
	for mrr.NextResult() {
		rr := mrr.ResultReader()
		fds := rr.FieldDescriptions()
		if len(fds) == 0 {
			rr.Close()
			continue
		}
		oids, rows = nil, nil
		for _, fd := range fds {
			oids = append(oids, fmt.Sprint(fd.DataTypeOID))
		}
		for rr.NextRow() {
			var vals []string
			for _, v := range rr.Values() {
				if v == nil {
					vals = append(vals, "NULL")
					continue
				}
				vals = append(vals, string(v))
			}
			rows = append(rows, strings.Join(vals, "|"))
		}
		if _, err := rr.Close(); err != nil {
			return "", "", err
		}
	}
	if err := mrr.Close(); err != nil {
		return "", "", err
	}
	return strings.Join(oids, ","), strings.Join(rows, " || "), nil
}

// cwDecoded runs sql over the EXTENDED protocol with every result column in
// the given format and decodes each value by its declared OID with pgx's own
// codecs — what a typed client (pgJDBC's getArray, pgx's scanning) does.
func cwDecoded(ctx context.Context, conn *pgx.Conn, sql string, format int16) ([][]any, error) {
	rr := conn.PgConn().ExecParams(ctx, sql, nil, nil, nil, []int16{format})
	fds := rr.FieldDescriptions()
	m := pgtype.NewMap()
	var out [][]any
	for rr.NextRow() {
		var row []any
		for i, raw := range rr.Values() {
			var v any
			if raw != nil {
				if err := m.Scan(fds[i].DataTypeOID, format, raw, &v); err != nil {
					return nil, fmt.Errorf("column %d (OID %d, format %d): %w", i, fds[i].DataTypeOID, format, err)
				}
			}
			row = append(row, v)
		}
		out = append(out, row)
	}
	if _, err := rr.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func TestArcCWContainersDeclareAndRenderOnTheWire(t *testing.T) {
	db := cwDB(t)
	srv := startTestServer(t, db)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })

	for _, c := range cwWireCells {
		t.Run(c.name, func(t *testing.T) {
			oids, text, err := cwSimple(ctx, conn, c.sql)
			if err != nil {
				t.Fatalf("%s\n  refused: %v", c.sql, err)
			}
			if oids != c.oids || text != c.text {
				t.Errorf("%s\n  got  OIDs %s  %s\n  want OIDs %s  %s", c.sql, oids, text, c.oids, c.text)
			}
			// BINARY where the declaration has a binary form: the value a
			// typed client decodes from the binary bytes is the value it
			// decodes from the text — the promise the OID makes.
			tv, terr := cwDecoded(ctx, conn, c.sql, 0)
			bv, berr := cwDecoded(ctx, conn, c.sql, 1)
			if terr != nil || berr != nil {
				t.Fatalf("%s\n  decode: text %v, binary %v", c.sql, terr, berr)
			}
			if !reflect.DeepEqual(tv, bv) {
				t.Errorf("%s\n  binary decodes to %#v\n  text   decodes to %#v", c.sql, bv, tv)
			}
		})
	}
}

// TestArcCWTypedClientsReadArrays holds the client-library cells the brief
// names: a pgJDBC-style getArray read (pgx scanning a declared array into a
// typed slice — it refuses a text OID), and psql's `\d` statement, which is
// also what SQLAlchemy's column reflection reads (format_type per attribute).
func TestArcCWTypedClientsReadArrays(t *testing.T) {
	db := cwDB(t)
	srv := startTestServer(t, db)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })

	var ints []int32
	if err := conn.QueryRow(ctx, `SELECT ARRAY[1,2,3] AS v`).Scan(&ints); err != nil || !reflect.DeepEqual(ints, []int32{1, 2, 3}) {
		t.Errorf("getArray of ARRAY[1,2,3]: %v %v", ints, err)
	}
	var flags []string
	if err := conn.QueryRow(ctx, `SELECT tcp_flags(18) AS v`).Scan(&flags); err != nil || !reflect.DeepEqual(flags, []string{"SYN", "ACK"}) {
		t.Errorf("getArray of tcp_flags(18): %v %v", flags, err)
	}
	var keys []string
	if err := conn.QueryRow(ctx, `SELECT map_keys(m) FROM cw WHERE id = 1`).Scan(&keys); err != nil || !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Errorf("getArray of map_keys(m): %v %v", keys, err)
	}
	var ts []*time.Time
	if err := conn.QueryRow(ctx, `SELECT v FROM (SELECT ats AS v FROM cw WHERE id = 1) s`).Scan(&ts); err != nil ||
		len(ts) != 2 || ts[0] == nil || !ts[0].Equal(time.UnixMilli(cwfixture.TS1).UTC()) || ts[1] != nil {
		t.Errorf("getArray of a derived timestamp[]: %v %v", ts, err)
	}
	var one int32
	if err := conn.QueryRow(ctx, `SELECT a[1] FROM (VALUES (ARRAY[7,8])) t(a)`).Scan(&one); err != nil || one != 7 {
		t.Errorf("an element of a VALUES array: %v %v", one, err)
	}

	// psql's `\d cw` (its third statement, captured with psql -E) and
	// SQLAlchemy's get_columns both read format_type per attribute.
	_, text, err := cwSimple(ctx, conn, `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) `+
		`FROM pg_catalog.pg_attribute a WHERE a.attrelid = 'cw'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`)
	if err != nil {
		t.Fatal(err)
	}
	const want = "id|bigint || ai|integer[] || at|text[] || ats|timestamp without time zone[] || ad|date[] || " +
		"ab|boolean[] || aip|text[] || au|uuid[] || r|text || m|text || ar|text"
	if text != want {
		t.Errorf("psql \\d cw / SQLAlchemy get_columns:\n  got  %s\n  want %s", text, want)
	}
}

// TestArcCWPinnedPostgresAnswersStillHold re-measures every cell with a
// PostgreSQL spelling on a live 17.11 when WADJET_PG_DSN names one
// (`task pg-oracle:up`), so a pinned expectation cannot drift from the oracle
// it claims to be.
func TestArcCWPinnedPostgresAnswersStillHold(t *testing.T) {
	dsn := os.Getenv("WADJET_PG_DSN")
	if dsn == "" || testing.Short() {
		t.Skip("WADJET_PG_DSN unset: the pins stand as measured")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, cwfixture.PGDDL); err != nil {
		t.Fatal(err)
	}
	for _, c := range cwWireCells {
		if c.pg == "-" {
			continue
		}
		sql := c.sql
		if c.pg != "" {
			sql = c.pg
		}
		oids, text, err := cwSimple(ctx, conn, sql)
		if err != nil {
			t.Errorf("%s: PostgreSQL refused %s: %v", c.name, sql, err)
			continue
		}
		wantOIDs, wantText := c.oids, c.text
		if c.pgOIDs != "" {
			wantOIDs = c.pgOIDs
		}
		if c.pgText != "" {
			wantText = c.pgText
		}
		if !cwOIDsMatch(oids, wantOIDs) || text != wantText {
			t.Errorf("%s: PostgreSQL answers OIDs %s  %s\n  pinned OIDs %s  %s", c.name, oids, text, wantOIDs, wantText)
		}
	}
}

// cwOIDsMatch compares OID lists where "*" stands for a composite's catalog
// OID, which PostgreSQL assigns per database.
func cwOIDsMatch(got, want string) bool {
	g, w := strings.Split(got, ","), strings.Split(want, ",")
	if len(g) != len(w) {
		return false
	}
	for i := range g {
		if w[i] != "*" && g[i] != w[i] {
			return false
		}
	}
	return true
}
