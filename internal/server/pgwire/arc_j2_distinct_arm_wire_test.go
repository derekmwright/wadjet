package pgwire

// A DISTINCT ARM'S COMPUTED COLUMN DECLARES numeric ON THE WIRE — #949.
//
// The issue filed a VALUE defect and a DECLARATION defect. The value half is
// gated on four arms in `coordinator.TestJ2ADistinctArmComputedColumnKeepsItsType`;
// this is the declaration half, and it is the arm no value oracle can provide
// (ADR-0012): a right value under a wrong OID is invisible to one.
//
// The DISTINCT lowering makes every SELECT item a GROUP BY key, so
// `SELECT DISTINCT a * 2 AS v` publishes `a * 2` as a COLUMN and emits no `a`
// at all. `declaredProjectionDecl` read the projection above it as ARITHMETIC,
// looked for `a`, found nothing and fell to the float rule — so `v` was
// DECIMAL(9+2,2) on the producer and FLOAT64 to every consumer, and
// `SUM(v*2)` went out as float8 / OID 701 where PostgreSQL 17 sends numeric /
// OID 1700 with typmod -1 (what it sends for any unconstrained numeric).
//
// Every OID and value below is PostgreSQL 17's, measured live over these rows.

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupJ2DistinctArmDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "j2wire", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": int64(1), "a": parquet.Decimal128{Lo: 1275}},
		{"id": int64(2), "a": parquet.Decimal128{Lo: 1275}},
		{"id": int64(3), "a": parquet.Decimal128{Lo: 200}},
		{"id": int64(4)},
	}
	ing := db.NewIngester("j2wire", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
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

func TestArcJ2ADistinctArmComputedColumnDeclaresNumericOnTheWire(t *testing.T) {
	srv := setupJ2DistinctArmDB(t)
	const arm = `(SELECT DISTINCT a * 2 AS v FROM j2wire) x`
	for _, c := range []struct {
		name, sql string
		oid       uint32
		typmod    int32
		value     string
	}{
		// #949's exact shape. float8 / OID 701 before this arc.
		{"sum_over_the_arms_computed_column",
			`SELECT SUM(v*2) AS v FROM ` + arm, 1700, -1, "59.00"},
		{"sum_of_the_bare_alias",
			`SELECT SUM(v) AS v FROM ` + arm, 1700, -1, "29.50"},
		// MAX copies its input's declaration, so this cell says the (p,s)
		// crossing the stage is the ARM's own and not SUM's widening.
		{"max_of_the_bare_alias",
			`SELECT MAX(v) AS v FROM ` + arm, 1700, -1, "25.50"},
		// The arm's column read straight through, with no aggregate above it.
		{"the_arms_column_itself",
			`SELECT v FROM ` + arm + ` WHERE v > 10`, 1700, -1, "25.50"},

		// CONTROLS: the same values spelled WITHOUT the DISTINCT arm. They
		// were already numeric before this arc and must not move.
		{"ctl_sum_without_the_distinct",
			`SELECT SUM(a*2*2) AS v FROM j2wire`, 1700, -1, "110.00"},
		{"ctl_plain_column",
			`SELECT a AS v FROM j2wire WHERE id = 3`, 1700, 589830, "2.00"},
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
			if f.TypeModifier != c.typmod {
				t.Errorf("%s\n  typmod %d, want %d (PostgreSQL 17)", c.sql, f.TypeModifier, c.typmod)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%s\n  %d rows, want 1", c.sql, len(res.Rows))
			}
			if got := string(res.Rows[0][0]); got != c.value {
				t.Errorf("%s\n  wire sent %q, PostgreSQL 17 sends %q", c.sql, got, c.value)
			}
		})
	}
}
