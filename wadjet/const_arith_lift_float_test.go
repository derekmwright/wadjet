package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The const-arith aggregate lift is for EXACT arithmetic, and this is the
// fixture that says so (round-1 review, B1).
//
// The first cut of the typed lift took a DOUBLE PRECISION column on the
// grounds that "float arithmetic never refuses". That is true of this engine
// and beside the point: the lift is an identity over VALUES or it is not
// applied, and over a float it is not. IEEE addition is not associative, so
// `SUM(f + k)` and `SUM(f) + k*COUNT(f)` are different numbers as soon as the
// summands span enough magnitude to cancel.
//
// The values below are chosen to SEPARATE the two forms, which is the whole
// content of the test. The corpus that missed this used `c_f64 = i/3` over the
// type matrix — magnitudes 0…333, no cancellation — so the two arms were bit
// identical whatever the rewrite did, and the cell could not fail.
//
// Every expectation is live PostgreSQL 17.11 over the same rows:
//
//	CREATE TABLE t (f double precision);
//	INSERT INTO t VALUES (1e16),(1),(1),(1),(1);
//	SELECT sum(f+1), sum(f*3) FROM t;
//	  -->  1.0000000000000008e+16 | 3.0000000000000016e+16
func caFloatDB(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64},
		{Name: "r", Type: parquet.TypeFloat32},
	}}
	if err := db.CreateTable(ctx, "caflt", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("caflt", schema, nil, ingest.Config{MaxBufferRows: 16, RowGroupSize: 16})
	// 1e16 is the first double whose successor is 2 away, so adding 1 to the
	// SUM and adding 1 to each summand land on different values.
	rows := []map[string]any{
		{"id": int64(1), "f": 1e16, "r": float32(1e7)},
		{"id": int64(2), "f": 1.0, "r": float32(1)},
		{"id": int64(3), "f": 1.0, "r": float32(1)},
		{"id": int64(4), "f": 1.0, "r": float32(1)},
		{"id": int64(5), "f": 1.0, "r": float32(1)},
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}

func TestConstArithLiftIsNotAppliedToFloatColumns(t *testing.T) {
	ctx := context.Background()
	db := caFloatDB(t)
	for _, c := range []struct {
		name, sql string
		want      float64
	}{
		// PostgreSQL 17.11, measured on the same five rows.
		{"sum_plus_one", `SELECT SUM(f + 1) AS v FROM caflt`, 1.0000000000000008e+16},
		{"sum_times_three", `SELECT SUM(f * 3) AS v FROM caflt`, 3.0000000000000016e+16},
		// `-` at these magnitudes lands back on 1e16 for both forms, so these
		// two are CONTROLS rather than separators: they say the decline did
		// not move a cell that already agreed.
		{"sum_minus_one", `SELECT SUM(f - 1) AS v FROM caflt`, 1e16},
		{"sum_one_minus", `SELECT SUM(1 - f) AS v FROM caflt`, -1e16},
		// A NON-INTEGER literal, which the deleted syntactic pass lifted on
		// every tree before this one. It is the same non-identity and the same
		// answer is owed: `SUM(f + 1.0)` was …004e+16 at base and on the tip
		// before B1, where PostgreSQL says …008e+16.
		{"sum_plus_a_fractional_literal", `SELECT SUM(f + 1.0) AS v FROM caflt`,
			1.0000000000000008e+16},
		// `* 2.0` is exact at every magnitude here, so this one is the control
		// for the pair above it: the fractional-literal lift moved `+ 1.0` and
		// could not move this.
		{"sum_times_a_fractional_literal", `SELECT SUM(f * 2.0) AS v FROM caflt`, 2e16},
		// AVG over the same column: the division is the same either way, so a
		// lift here would move the value for the same reason.
		{"avg_plus_one", `SELECT AVG(f + 1) AS v FROM caflt`, 2000000000000001.5},
		// A REAL column, where the divergence is a different ACCUMULATOR
		// rather than a reordering.
		{"real_sum_times_two", `SELECT SUM(r * 2) AS v FROM caflt`, 20000008},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1", len(res.Rows))
			}
			got, ok := res.Rows[0]["v"].(float64)
			if !ok {
				t.Fatalf("= %T(%v), want a float64", res.Rows[0]["v"], res.Rows[0]["v"])
			}
			if got != c.want {
				t.Errorf("= %.17g, want %.17g (live PostgreSQL 17.11). The const-arith lift "+
					"is an identity over VALUES or it is not applied, and over a float column "+
					"it is not — IEEE addition is not associative (#850, round-1 B1)"+
					"\n  SQL: %s", got, c.want, c.sql)
			}
			// And the same answer with the lift disabled, which is what
			// "not applied" means: the two arms must be BIT identical here,
			// unlike the fixture this replaces.
			tog := caaToggle(t)
			prev := tog.Set(false)
			off, err := db.Query(ctx, c.sql)
			tog.Set(prev)
			if err != nil {
				t.Fatalf("per-row: %v", err)
			}
			if off.Rows[0]["v"] != res.Rows[0]["v"] {
				t.Errorf("the kill switch changes the answer: %v on, %v off — nothing over a "+
					"float column may be lifted", res.Rows[0]["v"], off.Rows[0]["v"])
			}
		})
	}
}
