package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// #848: the HTTP query door answered 500 with no `sqlstate` for `SELECT
// nosuchcol FROM t` and `SELECT * FROM nosuchtable`, where the embedded and
// pgwire doors raise 42703 and 42P01.
//
// The parse path had been fixed on its own (06c25cc1) and its comment claimed
// it "was the one refusal class on this door that dropped it" — one path too
// wide: name resolution, physical planning and execution all still called
// writeError, which has no SQLSTATE argument at all. So a client could branch
// on `sqlstate` for bad syntax and not for a bad column, and every runtime
// data error (22012 division by zero, 22003 overflow, 22P02 a bad cast) came
// back as 500 Internal Server Error — the server blaming itself for the
// client's statement.
//
// The gate is the census below: one entry per SQLSTATE class this engine can
// raise through a SELECT, asserting on this door the STATUS, the CODE, and
// that the error is the same one pgwire reports for the same statement.

func hdSetup(t *testing.T) (httpURL string, pgConn *pgconn.PgConn) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2},
	}}
	if err := db.CreateTable(ctx, "hd", sch, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("hd", sch, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "s": "a", "d": "1.25"}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	srv := New(Config{Addr: ":0", Catalog: db.Catalog()}, nil)
	hs := httptest.NewServer(srv.Mux())
	t.Cleanup(hs.Close)

	pg := pgwire.NewServer(db, pgwire.Config{}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)
	conn, err := pgconn.Connect(ctx,
		fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", pg.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return hs.URL, conn
}

// hdPost runs one statement through the HTTP door and returns its status, its
// `sqlstate` ("" when the body carries none) and its `error` message.
func hdPost(t *testing.T, base, sql string) (int, string, string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(base+"/v1/queries", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %q: %v", sql, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var e struct {
		Error    string `json:"error"`
		SQLState string `json:"sqlstate"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatalf("POST %q returned %d with unparseable body %q", sql, resp.StatusCode, raw)
	}
	return resp.StatusCode, e.SQLState, e.Error
}

// TestHTTPDoorCarriesEverySQLStateClass is the census.
//
// The classes are the ones a SELECT can reach on this engine. 23 (integrity
// constraint violation) has no entry because nothing here can raise it —
// wadjet declares no UNIQUE, PRIMARY KEY or FOREIGN KEY constraints, so 23505
// is unreachable through any door; the class's PLACEMENT is asserted directly
// in TestSQLStateClientFaultTable instead, so the table is complete even where
// the corpus cannot be.
func TestHTTPDoorCarriesEverySQLStateClass(t *testing.T) {
	base, conn := hdSetup(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name, sql, state string
		status           int
	}{
		{"42703_unknown_column", "SELECT nosuchcol FROM hd", "42703", http.StatusBadRequest},
		{"42703_in_subquery", "SELECT * FROM hd WHERE id IN (SELECT nosuchcol FROM hd)",
			"42703", http.StatusBadRequest},
		{"42703_insert_column", "INSERT INTO hd (nosuchcol) VALUES (1)", "42703", http.StatusBadRequest},
		{"42P01_unknown_table", "SELECT * FROM nosuchtable", "42P01", http.StatusBadRequest},
		{"42601_syntax", "SELECT FROM", "42601", http.StatusBadRequest},
		{"42601_limit_not_a_number", "SELECT id FROM hd LIMIT 'x'", "42601", http.StatusBadRequest},
		{"42883_unknown_function", "SELECT nosuchfn(id) FROM hd", "42883", http.StatusBadRequest},
		{"22012_division_by_zero", "SELECT 1/0 FROM hd", "22012", http.StatusBadRequest},
		{"22012_modulo_by_zero", "SELECT id % 0 FROM hd", "22012", http.StatusBadRequest},
		{"22P02_bad_cast", "SELECT CAST(s AS BIGINT) FROM hd", "22P02", http.StatusBadRequest},
		{"22003_decimal_overflow",
			"SELECT CAST('99999999999999999999999999999999999999999' AS DECIMAL(9,2)) FROM hd",
			"22003", http.StatusBadRequest},
		{"22003_decimal_arithmetic",
			"SELECT d * 1000000000000000000000000000000000000 FROM hd", "22003", http.StatusBadRequest},
		{"0A000_unresolvable_sort_key", "SELECT * FROM hd ORDER BY nosuchcol", "0A000", http.StatusBadRequest},
		{"0A000_unresolvable_group_key", "SELECT * FROM hd GROUP BY nosuchcol", "0A000", http.StatusBadRequest},
		// EXPLAIN is a SEPARATE handler on the same door and it dropped the
		// class after the first pass claimed "every refusal on this door now
		// comes through here" — one handler too narrow, which is round-1 B2.
		// Its own three refusal sites are here so the claim is gated rather
		// than asserted.
		{"explain_42703", "EXPLAIN SELECT nosuchcol FROM hd", "42703", http.StatusBadRequest},
		{"explain_verbose_42703", "EXPLAIN VERBOSE SELECT nosuchcol FROM hd", "42703", http.StatusBadRequest},
		{"explain_42P01", "EXPLAIN SELECT * FROM nosuchtable", "42P01", http.StatusBadRequest},
		{"explain_42601", "EXPLAIN SELECT FROM", "42601", http.StatusBadRequest},
		// The DDL sub-handlers are more separate handlers on the same door,
		// reached from the same POST by statement type. This one put its
		// class in the MESSAGE and nowhere a client could branch on it.
		{"create_table_22023", "CREATE TABLE hd_bad (d DECIMAL(50,2))", "22023", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, state, msg := hdPost(t, base, tc.sql)
			if state == "" {
				t.Fatalf("HTTP door answered %d with NO sqlstate for %q (%q).\n"+
					"Every refusal the planner or the engine raises must reach the client with "+
					"its class, the way the pgwire and embedded doors do (#848).",
					status, tc.sql, msg)
			}
			if state != tc.state {
				t.Errorf("HTTP door reported sqlstate %s for %q, want %s (%q)", state, tc.sql, tc.state, msg)
			}
			if status != tc.status {
				t.Errorf("HTTP door answered %d for %q, want %d — a statement refused for what it "+
					"CONTAINS is the client's error, not the server's", status, tc.sql, tc.status)
			}

			// The same statement through pgwire: same class, same error.
			res := conn.ExecParams(ctx, tc.sql, nil, nil, nil, nil).Read()
			if res.Err == nil {
				t.Fatalf("pgwire ANSWERED %q that the HTTP door refused with %s", tc.sql, state)
			}
			pe, ok := res.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("pgwire error for %q is not a PgError: %v", tc.sql, res.Err)
			}
			if pe.Code != state {
				t.Errorf("the two doors report DIFFERENT classes for %q: HTTP %s, pgwire %s",
					tc.sql, state, pe.Code)
			}
			// The HTTP door reports the engine's message verbatim, which is
			// what PostgreSQL puts in its own ErrorResponse. pgwire reaches
			// the engine through wadjet.DB.Query, which prefixes the STAGE
			// ("executing query: ", "parsing SQL: ") — one recorded
			// difference, and the reason this is a suffix and not an equality.
			// Anything else means the two doors are reporting different
			// errors, which is what this half of the census is for.
			if !strings.HasSuffix(pe.Message, msg) {
				t.Errorf("the two doors report different MESSAGES for %q:\n HTTP   %q\n pgwire %q",
					tc.sql, msg, pe.Message)
			}
		})
	}
}

// TestSQLStateClientFaultTable is writeSQLError's class → status table, asserted
// from both sides (rule 11). The classes that are the CLIENT's fault answer
// 400; every other class — including the ones this corpus cannot reach —
// keeps the server's status, so a genuine server failure is never reported as
// a bad request.
func TestSQLStateClientFaultTable(t *testing.T) {
	for _, state := range []string{
		"0A000",                                     // feature not supported
		"22003", "22012", "22P02", "2201E", "22023", // data exception
		"23502", "23505", // integrity constraint violation
		"42601", "42703", "42P01", "42883", "42702", "42804", // syntax / access rule
	} {
		if !sqlStateIsClientFault(state) {
			t.Errorf("%s is not treated as a client fault; it must answer 400", state)
		}
	}
	for _, state := range []string{
		"XX000", // internal error
		"58030", // io_error
		"53300", // too_many_connections
		"57014", // query_canceled
		"08P01", // protocol violation
		"40001", // serialization failure
	} {
		if sqlStateIsClientFault(state) {
			t.Errorf("%s is treated as a client fault; a server-side or transport failure "+
				"must not be reported to the client as a bad request", state)
		}
	}
}
