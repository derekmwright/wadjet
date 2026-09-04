package pgwire

// An integer expression with no room in its declared type reaches a client as
// SQLSTATE 22003 (numeric_value_out_of_range), the code and the wording
// PostgreSQL raises for the same rows.
//
// The shape is `ABS(<column>)` at the type's floor. |min| has no value in a
// two's-complement type, so PostgreSQL 17.11 raises `integer out of range` for
// int4 and `bigint out of range` for int8 — measured live. wadjet answered
// -2147483648 for the int4 column: the kernel computed the correct int64
// 2147483648 (ColRef.Eval widens an INT32 column on purpose) and the store
// narrowed it back and wrapped. A different number wearing the right type.
//
// It runs through a REAL pgx connection for the reason
// decimal_overflow_sqlstate_test.go gives: the question is what the WIRE
// carries. batch.IntegerRangeError raises inside a vector store, and the
// panic has to survive the pipeline's recover, the planner's wrapping and the
// pgwire error path before its SQLSTATE becomes an ErrorResponse field. A unit
// test at the raise site cannot see a code dropped anywhere on that route.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestIntegerOutOfRangeReaches22003OnTheWire(t *testing.T) {
	ctx := context.Background()
	db, srv := setupRealDB(t)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "i64", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "intmin", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("intmin", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "i32": int32(-2147483648), "i64": int64(-9223372036854775808)},
		{"id": int64(2), "i32": int32(-2147483647), "i64": int64(-9223372036854775807)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	for _, tc := range []struct{ name, sql, wantMsg string }{
		{"int32_column_at_its_floor",
			`SELECT ABS(i32) AS a FROM intmin WHERE id = 1`, "integer out of range"},
		{"int64_column_at_its_floor",
			`SELECT ABS(i64) AS a FROM intmin WHERE id = 1`, "bigint out of range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, qErr := conn.Query(ctx, tc.sql, pgx.QueryExecModeSimpleProtocol)
			if qErr == nil {
				_, qErr = pgx.CollectRows(rows, pgx.RowToMap)
			}
			if qErr == nil {
				t.Fatalf("%s answered; |min| has no value in its own integer type", tc.sql)
			}
			var pgErr *pgconn.PgError
			if !errors.As(qErr, &pgErr) {
				t.Fatalf("query error = %v, want a *pgconn.PgError", qErr)
			}
			if pgErr.Code != "22003" {
				t.Errorf("SQLSTATE = %s, want 22003 (numeric_value_out_of_range); message = %s",
					pgErr.Code, pgErr.Message)
			}
			if !strings.Contains(pgErr.Message, tc.wantMsg) {
				t.Errorf("message should be PostgreSQL's %q; got %q", tc.wantMsg, pgErr.Message)
			}
		})
	}

	// The row just inside the floor still answers, on the same connection: the
	// rule is about the VALUE, and the session survives a refusal.
	var a int32
	if err := conn.QueryRow(ctx, `SELECT ABS(i32) AS a FROM intmin WHERE id = 2`,
		pgx.QueryExecModeSimpleProtocol).Scan(&a); err != nil {
		t.Fatalf("the row inside the floor must answer: %v", err)
	}
	if a != 2147483647 {
		t.Errorf("ABS(-2147483647) = %d, want 2147483647", a)
	}
}
