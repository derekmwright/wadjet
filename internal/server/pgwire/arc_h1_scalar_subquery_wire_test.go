package pgwire

// A SELECT-LIST SCALAR SUBQUERY DECLARES ITS OWN TYPE ON THE WIRE — #874.
//
// `SELECT id, (SELECT MAX(c_i64) FROM t) AS mx FROM t` sent the right digits
// under OID 25 (text): `expr.ScalarSubquery` carried no declaration into the
// projection, `nodeDeclaredType` had no `*plansql.SubqueryNode` arm, and the
// output vector fell to its STRING fallback. PostgreSQL 17 declares bigint,
// and the value came back as a Go `string` through the embedded API on every
// arm as well.
//
// It is the wire arm that this gate exists for: a value oracle cannot see a
// right value under a wrong OID (ADR-0012). The `+ 1` cell is #714's third
// box, closed by the same declaration — an Undecided operand made the
// const-arith fold take the FLOAT rung, so `4999014997 + 1` came back as
// 4.999014998e+09.
//
// Every OID and value below is PostgreSQL 17, measured live over the same
// rows (this arc's ROUND0 fixtures).

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupH1ScalarDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "c_str", Type: parquet.TypeString, Nullable: true},
		{Name: "c_dec", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "h1scal", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": int64(1), "c_i64": int64(4999014997), "c_i32": int32(14997),
			"c_f64": 1666.5, "c_str": "s-000001", "c_dec": parquet.Decimal128{Lo: 1275}},
		{"id": int64(2), "c_i64": int64(7), "c_i32": int32(3),
			"c_f64": 0.5, "c_str": "s-000002", "c_dec": parquet.Decimal128{Lo: 100}},
	}
	ing := db.NewIngester("h1scal", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
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

func TestArcH1AScalarSubqueryDeclaresItsOwnTypeOnTheWire(t *testing.T) {
	srv := setupH1ScalarDB(t)
	for _, c := range []struct {
		name, sql string
		oid       uint32
		value     string
	}{
		{"bigint", `SELECT (SELECT MAX(c_i64) FROM h1scal) AS v FROM h1scal WHERE id = 1`,
			20, "4999014997"},
		{"bigint_plus_one", `SELECT (SELECT MAX(c_i64) FROM h1scal) + 1 AS v FROM h1scal WHERE id = 1`,
			20, "4999014998"},
		{"double", `SELECT (SELECT MAX(c_f64) FROM h1scal) AS v FROM h1scal WHERE id = 1`,
			701, "1666.5"},
		{"text", `SELECT (SELECT MAX(c_str) FROM h1scal) AS v FROM h1scal WHERE id = 1`,
			25, "s-000002"},
		{"bare_column_subquery", `SELECT (SELECT c_i64 FROM h1scal WHERE id = 2) AS v ` +
			`FROM h1scal WHERE id = 1`, 20, "7"},

		// THE CONTROLS: the same value spelled WITHOUT a subquery must not
		// move, and neither must the wire's own reading of it.
		{"ctl_plain_aggregate", `SELECT MAX(c_i64) AS v FROM h1scal`, 20, "4999014997"},
		{"ctl_plain_column", `SELECT c_i64 AS v FROM h1scal WHERE id = 2`, 20, "7"},
		{"ctl_plain_double", `SELECT MAX(c_f64) AS v FROM h1scal`, 701, "1666.5"},
		{"ctl_plain_text", `SELECT MAX(c_str) AS v FROM h1scal`, 25, "s-000002"},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, c.sql)
			}
			f := res.FieldDescriptions[0]
			if f.DataTypeOID != c.oid {
				t.Errorf("%s\n  OID %d, want %d (PostgreSQL 17)", c.sql, f.DataTypeOID, c.oid)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%s\n  %d rows, want 1", c.sql, len(res.Rows))
			}
			if got := string(res.Rows[0][0]); got != c.value {
				t.Errorf("%s\n  wire sent %q, PostgreSQL 17 sends %q", c.sql, got, c.value)
			}
		})
	}

	// RECORDED, NOT CLOSED: the INT32 and DECIMAL spellings.
	//
	// `(SELECT MAX(c_i32) …)` is `integer` on PostgreSQL and bigint here —
	// but so is the PLAIN `SELECT MAX(c_i32)`, because MAX over an INT32
	// column declares bigint in this engine (ADR-0024's aggregate result
	// type, not this arc's). The subquery agreeing with the plain spelling is
	// the property #874 is about, and it holds; the width question belongs to
	// the aggregate.
	//
	// `(SELECT MAX(c_dec) …)` stays TEXT because the subquery's plan declares
	// a DECIMAL with NO (p,s) — `declaredProjectionDecl`'s honest fallback for
	// a COMPUTED decimal (#458) — and a DECIMAL declared without its scale
	// builds an output vector that reads every value at the wrong power of
	// ten. Declining is the rule ADR-0024 item 2 states, and the plain
	// `MAX(c_dec)` spelling reaches the wire the same way. Asserted in both
	// directions so the pair cannot drift apart unnoticed.
	t.Run("recorded-int32-and-decimal-agree-with-their-plain-spelling", func(t *testing.T) {
		for _, c := range []struct{ name, sub, plain string }{
			{"int32", `SELECT (SELECT MAX(c_i32) FROM h1scal) AS v FROM h1scal WHERE id = 1`,
				`SELECT MAX(c_i32) AS v FROM h1scal`},
			{"decimal", `SELECT (SELECT MAX(c_dec) FROM h1scal) AS v FROM h1scal WHERE id = 1`,
				`SELECT MAX(c_dec) AS v FROM h1scal`},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				conn := connectPgconn(t, srv.Addr())
				sub := conn.ExecParams(context.Background(), c.sub, nil, nil, nil, []int16{0}).Read()
				if sub.Err != nil {
					t.Fatalf("%v\n  SQL: %s", sub.Err, c.sub)
				}
				plain := conn.ExecParams(context.Background(), c.plain, nil, nil, nil, []int16{0}).Read()
				if plain.Err != nil {
					t.Fatalf("%v\n  SQL: %s", plain.Err, c.plain)
				}
				if got, want := sub.FieldDescriptions[0].DataTypeOID,
					plain.FieldDescriptions[0].DataTypeOID; got != want {
					t.Errorf("the subquery spelling declares OID %d and the plain one %d — a "+
						"scalar subquery answers what its own SELECT list answers\n  %s\n  %s",
						got, want, c.sub, c.plain)
				}
				if got, want := string(sub.Rows[0][0]), string(plain.Rows[0][0]); got != want {
					t.Errorf("the subquery spelling sent %q and the plain one %q\n  %s\n  %s",
						got, want, c.sub, c.plain)
				}
			})
		}
	})
}
