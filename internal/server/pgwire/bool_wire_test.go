package pgwire

// Regression tests for #364: a boolean-valued EXPRESSION column must declare
// OID 16 (bool, size 1), render 't'/'f' under a text format code, and one
// byte under a binary one. Before the fix the planner typed a predicate-
// shaped projection String, so the wire declared OID 25 and spelled the value
// Go-style "false"/"true" — which even a correctly-typed client could not
// read, since pgx's text bool decoder accepts only 't'/'f'.

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func connectPgconn(t *testing.T, addr string) *pgconn.PgConn {
	t.Helper()
	conn, err := pgconn.Connect(context.Background(),
		fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", addr))
	if err != nil {
		t.Fatalf("pgconn.Connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func TestBooleanExpressionDeclaresBoolOIDAndRendersTF(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	res := conn.ExecParams(context.Background(),
		`SELECT (id = 1) AS flag FROM users ORDER BY id`, nil, nil, nil, []int16{0}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	f := res.FieldDescriptions[0]
	if f.DataTypeOID != 16 {
		t.Errorf("boolean expression declared OID %d, want 16 (bool)", f.DataTypeOID)
	}
	if f.DataTypeSize != 1 {
		t.Errorf("boolean expression declared size %d, want 1", f.DataTypeSize)
	}
	want := []string{"t", "f", "f"}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
	}
	for i, w := range want {
		if got := string(res.Rows[i][0]); got != w {
			t.Errorf("row %d rendered %q, want %q (PostgreSQL boolean output is t/f)", i, got, w)
		}
	}
}

// TestBooleanExpressionBinaryFormat pins the binary half: one byte, 0 or 1.
func TestBooleanExpressionBinaryFormat(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	res := conn.ExecParams(context.Background(),
		`SELECT (id = 1) AS flag FROM users ORDER BY id`, nil, nil, nil, []int16{1}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(res.Rows))
	}
	wantBytes := [][]byte{{1}, {0}, {0}}
	for i, w := range wantBytes {
		got := res.Rows[i][0]
		if len(got) != 1 || got[0] != w[0] {
			t.Errorf("row %d binary bool = %v, want %v", i, got, w)
		}
	}
}
