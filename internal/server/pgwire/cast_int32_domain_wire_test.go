package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// The WIRE half of #901: an int4-domain cast declares an int4-family OID and
// refuses with PostgreSQL's own SQLSTATE.
//
// A value oracle cannot see this. Before the fix `SELECT 3000000000::INT32`
// answered the number under OID 25 (text) — right-looking in psql, a Go string
// in pgx, and a value PostgreSQL refuses outright — and `SELECT 443::PORT` did
// the same where a PORT COLUMN has declared integer (OID 23) since #834. One
// type, two OIDs, decided by whether the value came from a column or a cast.
func TestAnInt32DomainCastOnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()

	for _, c := range []struct {
		sql  string
		oid  uint32
		text string
	}{
		{`SELECT 2147483647::INT32 AS v`, 20, "2147483647"}, // int8: every integer cast lands on INT64
		{`SELECT 443::PORT AS v`, 23, "443"},                // int4, the same OID a PORT column declares
		{`SELECT 6::PROTOCOL AS v`, 23, "6"},                // int4
		{`SELECT CAST(1.5 AS FLOAT32) AS v`, 700, "1.5"},    // float4
		{`SELECT CAST(3000000000 AS INTEGER) AS v`, 20, ""}, // the control: refuses, no row
	} {
		t.Run(c.sql, func(t *testing.T) {
			res := conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}).Read()
			if c.text == "" {
				if res.Err == nil {
					t.Fatalf("answered %v where PostgreSQL raises integer out of range", res.Rows)
				}
				return
			}
			if res.Err != nil {
				t.Fatalf("ExecParams: %v", res.Err)
			}
			if got := res.FieldDescriptions[0].DataTypeOID; got != c.oid {
				t.Errorf("declared OID %d, want %d — a number under OID 25 reaches a "+
					"driver as a string", got, c.oid)
			}
			if len(res.Rows) != 1 || string(res.Rows[0][0]) != c.text {
				t.Errorf("rendered %q, want %q", res.Rows, c.text)
			}
		})
	}

	// The refusals carry PostgreSQL's class, not XX000 ("the server broke").
	for _, sql := range []string{
		`SELECT 3000000000::INT32 AS v`,
		`SELECT 3000000000::PORT AS v`,
		`SELECT 3000000000::PROTOCOL AS v`,
		`SELECT CAST(1e40 AS FLOAT32) AS v`,
	} {
		t.Run("refusal/"+sql, func(t *testing.T) {
			res := conn.ExecParams(ctx, sql, nil, nil, nil, []int16{0}).Read()
			if res.Err == nil {
				t.Fatalf("answered %v; the value has no place in the destination", res.Rows)
			}
			pge, ok := res.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("refusal is %T, not a PgError: %v", res.Err, res.Err)
			}
			if pge.Code != "22003" {
				t.Errorf("SQLSTATE %q, want 22003 numeric_value_out_of_range (%s)",
					pge.Code, pge.Message)
			}
		})
	}
}
