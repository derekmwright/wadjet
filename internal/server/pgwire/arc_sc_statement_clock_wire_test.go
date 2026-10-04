// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The statement clock on the wire (#1566): the connection starts each
// statement's clock (queryContext), so a statement reads one now() over
// every row on the simple and the extended protocol; the clock is the
// EXECUTE's — a prepared statement executed twice reads two values, and
// neither is the Parse's. PostgreSQL 17.11's answers, measured.

func scWireDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := db.CreateTable(ctx, "sc_r", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 4096)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i + 1)}
	}
	ing := db.NewIngester("sc_r", schema, nil, ingest.Config{MaxBufferRows: 1024, RowGroupSize: 256})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func scWireRows(rr *pgconn.ResultReader) string {
	res := rr.Read()
	if res.Err != nil {
		return "ERR " + res.Err.Error()
	}
	var out []string
	for _, r := range res.Rows {
		var f []string
		for _, v := range r {
			f = append(f, string(v))
		}
		out = append(out, strings.Join(f, "|"))
	}
	return strings.Join(out, " ; ")
}

func scWireSimple(t *testing.T, ctx context.Context, conn *pgconn.PgConn, sql string) []string {
	t.Helper()
	res, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	var out []string
	for _, r := range res {
		var rows []string
		for _, row := range r.Rows {
			var f []string
			for _, v := range row {
				f = append(f, string(v))
			}
			rows = append(rows, strings.Join(f, "|"))
		}
		out = append(out, strings.Join(rows, " ; "))
	}
	return out
}

func TestArcSCStatementClockOnTheWire(t *testing.T) {
	ctx := context.Background()
	srv := startTestServer(t, scWireDB(t))
	conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	const one = "SELECT count(DISTINCT now()), count(*) FROM sc_r WHERE now() = CURRENT_TIMESTAMP AND id > %s"
	for i := 1; i <= 8; i++ {
		// Simple protocol.
		if got := scWireSimple(t, ctx, conn, fmt.Sprintf(one, "0")); len(got) != 1 || got[0] != "1|4096" {
			t.Errorf("simple %d: %v; PostgreSQL 17.11: 1|4096", i, got)
		}
		// Extended protocol, unnamed statement, a bound parameter.
		if got := scWireRows(conn.ExecParams(ctx, fmt.Sprintf(one, "$1"), [][]byte{[]byte("0")}, nil, nil, nil)); got != "1|4096" {
			t.Errorf("extended %d: %s; PostgreSQL 17.11: 1|4096", i, got)
		}
	}

	// A prepared statement: one value over its rows per EXECUTE, a later
	// value at the second EXECUTE, and the Parse's instant at neither.
	if _, err := conn.Prepare(ctx, "sc_ps", "SELECT count(DISTINCT CAST(now() AS TEXT)), min(CAST(now() AS TEXT)), now() > CAST($1 AS TIMESTAMP) FROM sc_r", nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	before := scWireSimple(t, ctx, conn, "SELECT CAST(now() AS TEXT)")[0]
	time.Sleep(5 * time.Millisecond)
	first := scWireRows(conn.ExecPrepared(ctx, "sc_ps", [][]byte{[]byte(before)}, nil, nil))
	time.Sleep(5 * time.Millisecond)
	second := scWireRows(conn.ExecPrepared(ctx, "sc_ps", [][]byte{[]byte(before)}, nil, nil))
	f, s := strings.Split(first, "|"), strings.Split(second, "|")
	if len(f) != 3 || len(s) != 3 {
		t.Fatalf("prepared: %q, %q", first, second)
	}
	if f[0] != "1" || s[0] != "1" {
		t.Errorf("prepared statement over 4096 rows: %s and %s distinct values; PostgreSQL 17.11: 1 and 1", f[0], s[0])
	}
	if f[2] != "t" || s[2] != "t" {
		t.Errorf("prepared statement's now() after a statement run after its Parse: %s, %s; PostgreSQL 17.11: t, t (the clock is the EXECUTE's)", f[2], s[2])
	}
	if f[1] == s[1] {
		t.Errorf("a prepared statement executed twice 5 ms apart read one value %s; PostgreSQL 17.11: two", f[1])
	}
}

// TestArcSCStatementClockIsTheStatements: this engine has no transactions —
// BEGIN / COMMIT are accepted and ignored and a multi-statement string is a
// sequence (ADR-0012 parameters-pgwire E11) — so its clock is the
// STATEMENT's: two statements inside BEGIN … COMMIT, or in one simple-query
// string, read two values where PostgreSQL 17.11 reads the transaction's one
// (ADR-0012 temporal r26, kept). A pin that starts agreeing FAILS.
func TestArcSCStatementClockIsTheStatements(t *testing.T) {
	ctx := context.Background()
	srv := startTestServer(t, scWireDB(t))
	conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	scWireSimple(t, ctx, conn, "BEGIN")
	a := scWireSimple(t, ctx, conn, "SELECT CAST(now() AS TEXT)")[0]
	time.Sleep(5 * time.Millisecond)
	b := scWireSimple(t, ctx, conn, "SELECT CAST(now() AS TEXT)")[0]
	scWireSimple(t, ctx, conn, "COMMIT")
	if a == b {
		t.Errorf("BEGIN; now(); now(); COMMIT read one value %s — PostgreSQL 17.11's transaction clock; the kept row (temporal r26) says two, one per statement", a)
	}

	// One simple-query string, three statements; the middle one takes several
	// milliseconds (a 511 × 511 cross product; no pg_sleep here). On
	// PostgreSQL 17.11 the string is one implicit transaction and both now()
	// read one value.
	got := scWireSimple(t, ctx, conn, "SELECT CAST(now() AS TEXT); SELECT count(*) FROM sc_r a, sc_r b WHERE a.id < 512 AND b.id < 512; SELECT CAST(now() AS TEXT)")
	if len(got) != 3 {
		t.Fatalf("multi-statement string: %v", got)
	}
	if got[1] != "261121" {
		t.Errorf("the middle statement: %s; PostgreSQL 17.11: 261121", got[1])
	}
	if got[0] == got[2] {
		t.Errorf("a multi-statement string read one value %s — PostgreSQL 17.11's implicit transaction; the kept row (temporal r26) says one per statement", got[0])
	}
}
