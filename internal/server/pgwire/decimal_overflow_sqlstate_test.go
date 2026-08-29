package pgwire

// The engine's DECIMAL overflow refusals reach a client as SQLSTATE 22003
// (numeric_value_out_of_range), the code PostgreSQL raises for the same
// conditions.
//
// The four overflow sites — exec.coerceDecimalVector, the single-process set
// operation's arm coercion, and the SUM/AVG accumulators — were bare
// fmt.Errorf, so sqlerr.StateOf found nothing on them and pgwire fell back to
// its generic class. A client branching on SQLSTATE could not tell "your total
// is too big for the type" from "the server broke", which is the whole point
// of the code. ADR-0024 item 4 makes 22003 mandatory at every value-producing
// site.
//
// This runs through a REAL pgx connection rather than asserting sqlerr.StateOf
// in the engine, because the question is what the WIRE carries: the engine's
// error travels through the pipeline, the planner's wrapping and the pgwire
// error path before it becomes an ErrorResponse field, and a code lost
// anywhere on that route is invisible to a unit test at the source.

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setupDecimalOverflowDB builds the two fixtures the two refusals need.
//
//   - sums: DECIMAL(38,0) holding two values near 10^38. Each fits the type;
//     their SUM does not fit the Int128 accumulator, which is exactly the
//     condition ADR-0012 item 9 settled as an error rather than a wrapped
//     total.
//   - unions: DECIMAL(38,0) holding 10^30 beside DECIMAL(11,10). The union's
//     own type is DECIMAL(38,10), and 10^30 at scale 10 needs 10^40 — a value
//     both columns hold before the set operation and neither can hold after
//     it (#552's shape, #553's value).
func setupDecimalOverflowDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, srv := setupRealDB(t)

	near, ok := new(big.Int).SetString("90000000000000000000000000000000000000", 10) // 9e37
	if !ok {
		t.Fatal("9e37 must parse")
	}
	sumSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "sums", sumSchema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("sums", sumSchema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int32(1), "d": bigDecimal128(near)},
		{"id": int32(2), "d": bigDecimal128(near)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	e30, _ := new(big.Int).SetString("1000000000000000000000000000000", 10)
	unionSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "d380", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
		{Name: "d1110", Type: parquet.TypeDecimal, Precision: 11, Scale: 10, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "unions", unionSchema, nil); err != nil {
		t.Fatal(err)
	}
	ing = db.NewIngester("unions", unionSchema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int32(1), "d380": bigDecimal128(e30), "d1110": parquet.Decimal128{Lo: 10000000001}},
		{"id": int32(2), "d380": parquet.Decimal128{Lo: 7}, "d1110": parquet.Decimal128{Lo: 20000000000}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return srv
}

// bigDecimal128 renders a non-negative big.Int as the two words a DECIMAL
// column's box carries.
func bigDecimal128(v *big.Int) parquet.Decimal128 {
	lo := new(big.Int).And(v, new(big.Int).SetUint64(^uint64(0)))
	hi := new(big.Int).Rsh(v, 64)
	return parquet.Decimal128{Hi: hi.Int64(), Lo: lo.Uint64()}
}

func TestDecimalOverflowReaches22003OnTheWire(t *testing.T) {
	srv := setupDecimalOverflowDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	for _, tc := range []struct {
		name    string
		sql     string
		wantMsg string
	}{
		// exec.decimalSumOverflow: 9e37 + 9e37 = 1.8e38, past 2^127-1.
		{"sum_overflow", `SELECT SUM(d) AS s FROM sums`, "overflowed the 128-bit exact accumulator"},
		// The set operation's arm coercion: 10^30 has no DECIMAL(38,10).
		{"setop_coercion_overflow",
			`SELECT d380 AS v FROM unions UNION ALL SELECT d1110 FROM unions`,
			"numeric field overflow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, qErr := conn.Query(ctx, tc.sql, pgx.QueryExecModeSimpleProtocol)
			if qErr == nil {
				_, qErr = pgx.CollectRows(rows, pgx.RowToMap)
			}
			if qErr == nil {
				t.Fatalf("%s answered; a DECIMAL value with no exact carrier must fail the query", tc.sql)
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
				t.Errorf("message should mention %q; got %q", tc.wantMsg, pgErr.Message)
			}
		})
	}

	// The connection is still usable: an error is an error, not a desync.
	var n int64
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) AS c FROM sums`,
		pgx.QueryExecModeSimpleProtocol).Scan(&n); err != nil {
		t.Fatalf("the session must survive the refusals: %v", err)
	}
	if n != 2 {
		t.Errorf("COUNT(*) = %d, want 2", n)
	}
}
