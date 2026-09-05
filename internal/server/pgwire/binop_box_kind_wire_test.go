package pgwire

// The generic arithmetic node on the WIRE (#849 round-3 residual, #555).
//
// `COALESCE(d, 0) + 1` is `numeric` on PostgreSQL, and the value under that
// OID has to be the number PostgreSQL computes — but the wire's other half is
// what a PREDICATE over the same expression SELECTS, and that is what this
// engine got wrong: the node boxes its exact result as a DECIMAL column's
// rendered text, `expr.classifyOperand` had no arm for it, and the comparison
// above it ordered `"1.00"` against `"1"` by BYTES.
//
// PostgreSQL 17.11 over the same four rows (d numeric(9,2) = 12.75, -0.01,
// 0.00, NULL), measured live:
//
//	SELECT COALESCE(d,0)+1 FROM …               --> numeric: 13.75, 0.99, 1.00, 1.00
//	SELECT count(*) … WHERE COALESCE(d,0)+1 > 1 --> 1   (was 3)
//	SELECT count(*) … WHERE ABS(d)+1 = 1        --> 1   (was 0)

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// setupBoxKindDB is a DECIMAL(9,2) fixture with the three values a byte order
// and a numeric order disagree about — one above the literal, one just below
// it, and one exactly ON it, which is the row `"1.00" > "1"` decided wrongly —
// plus a NULL so the choosing construct has something to choose.
func setupBoxKindDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "boxdec", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": int64(1), "d": parquet.Decimal128{Lo: 1275}},               // 12.75
		{"id": int64(2), "d": parquet.Decimal128{Hi: -1, Lo: ^uint64(0)}}, // -0.01
		{"id": int64(3), "d": parquet.Decimal128{}},                       // 0.00
		{"id": int64(4)}, // NULL
	}
	ing := db.NewIngester("boxdec", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

// TestArithmeticOverAChoiceIsNumericOnTheWire is the DECLARATION: OID 1700
// with no modifier, which is what PostgreSQL sends for every one of these.
func TestArithmeticOverAChoiceIsNumericOnTheWire(t *testing.T) {
	srv := setupBoxKindDB(t)
	for _, c := range []struct {
		name, sql string
		oid       uint32
		typmod    int32
		value     string
	}{
		{"coalesce", `SELECT COALESCE(d, 0) + 1 AS v FROM boxdec WHERE id = 1`, 1700, -1, "13.75"},
		{"case", `SELECT (CASE WHEN id > 0 THEN d ELSE 0 END) + 1 AS v FROM boxdec WHERE id = 1`,
			1700, -1, "13.75"},
		{"greatest", `SELECT GREATEST(d, 0) + 1 AS v FROM boxdec WHERE id = 1`, 1700, -1, "13.75"},
		{"unary_minus", `SELECT -d + 1 AS v FROM boxdec WHERE id = 1`, 1700, -1, "-11.75"},
		{"cast", `SELECT CAST(d AS DECIMAL(9,2)) + 1 AS v FROM boxdec WHERE id = 1`, 1700, -1, "13.75"},
		{"scalar_fn", `SELECT ABS(d) + 1 AS v FROM boxdec WHERE id = 1`, 1700, -1, "13.75"},
		// The row the byte order decided wrongly, so the value that reaches
		// the client is the one the predicate below is asked about.
		{"lands_on_the_literal", `SELECT COALESCE(d, 0) + 1 AS v FROM boxdec WHERE id = 3`,
			1700, -1, "1.00"},
		// The NULL row takes the literal arm, and PostgreSQL prints `1` here
		// where this engine prints `1.00`: numeric carries each VALUE's own
		// dscale and a single-scale vector renders every row at the fold's
		// (ADR-0024's recorded per-value scale, #764). The DIGITS agree, which
		// is what the cell is for.
		{"null_arm_lands_on_the_literal", `SELECT COALESCE(d, 0) + 1 AS v FROM boxdec WHERE id = 4`,
			1700, -1, "1.00"},
		// The BOUNDARY: an all-integer choice keeps bigint, which is what
		// #849 settled and what a widening fix would break.
		{"ctl_integer_choice", `SELECT COALESCE(id, 0) + 1 AS v FROM boxdec WHERE id = 1`,
			20, -1, "2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, c.sql)
			}
			f := res.FieldDescriptions[0]
			if f.DataTypeOID != c.oid {
				t.Errorf("%s\n  OID %d, want %d (PostgreSQL 17.11)", c.sql, f.DataTypeOID, c.oid)
			}
			if f.TypeModifier != c.typmod {
				t.Errorf("%s\n  typmod %d, want %d (PostgreSQL 17.11)",
					c.sql, f.TypeModifier, c.typmod)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%s\n  %d rows, want 1", c.sql, len(res.Rows))
			}
			if got := string(res.Rows[0][0]); got != c.value {
				t.Errorf("%s\n  wire sent %q, PostgreSQL 17.11 sends %q", c.sql, got, c.value)
			}
		})
	}
}

// TestPredicateOverArithmeticOverAChoiceSelectsPostgresRowsOnTheWire is the
// half a declaration check cannot see: the rows the SAME expression selects.
// A right value under a right OID says nothing about which rows a WHERE above it
// admitted.
func TestPredicateOverArithmeticOverAChoiceSelectsPostgresRowsOnTheWire(t *testing.T) {
	srv := setupBoxKindDB(t)
	for _, c := range []struct {
		name, sql string
		want      string
	}{
		// COALESCE(d,0)+1 = 13.75, 0.99, 1.00, 1.00 — one row above 1.
		{"gt", `SELECT COUNT(*) AS n FROM boxdec WHERE (COALESCE(d, 0) + 1) > 1`, "1"},
		{"ge", `SELECT COUNT(*) AS n FROM boxdec WHERE (COALESCE(d, 0) + 1) >= 1`, "3"},
		{"eq", `SELECT COUNT(*) AS n FROM boxdec WHERE (COALESCE(d, 0) + 1) = 1`, "2"},
		{"between", `SELECT COUNT(*) AS n FROM boxdec WHERE (COALESCE(d, 0) + 1) BETWEEN 1 AND 2`, "2"},
		{"quoted_literal", `SELECT COUNT(*) AS n FROM boxdec WHERE (COALESCE(d, 0) + 1) > '1'`, "1"},
		// ABS(d)+1 = 13.75, 1.01, 1.00, NULL.
		{"scalar_fn_eq", `SELECT COUNT(*) AS n FROM boxdec WHERE (ABS(d) + 1) = 1`, "1"},
		// -d+1 = -11.75, 1.01, 1.00, NULL.
		{"unary_minus_gt", `SELECT COUNT(*) AS n FROM boxdec WHERE (-d + 1) > 1`, "1"},
		// The BOUNDARY again: the integer choice's rows are unchanged.
		{"ctl_integer_choice", `SELECT COUNT(*) AS n FROM boxdec WHERE (COALESCE(id, 0) + 1) > 1`, "4"},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, c.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%s\n  %d rows, want 1", c.sql, len(res.Rows))
			}
			if got := string(res.Rows[0][0]); got != c.want {
				t.Errorf("%s\n  wire sent %q, PostgreSQL 17.11 sends %q", c.sql, got, c.want)
			}
		})
	}
}
