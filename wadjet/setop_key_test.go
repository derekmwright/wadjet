package wadjet

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #499: UNION, INTERSECT and EXCEPT decide membership by EQUALITY, so their
// dedup key has to agree with the comparator — two values `=` calls equal must
// produce ONE key. The single-process path keyed on `fmt.Sprintf("%v", ...)`
// of the boxed value, and a DECIMAL boxes as its RENDERED TEXT, so "12.75"
// from a DECIMAL(9,2) and "12.7500" from a DECIMAL(18,4) were two keys for one
// number.
//
// The tell in the filing: `GROUP BY` over the same `UNION ALL` was already
// correct, because it keys through the columnar encoding (#474). One engine,
// two answers to "are these the same value".
//
// A FLOAT column carries the same defect one type over: the comparator calls
// -0.0 and +0.0 equal (ADR-0012 item 8), and `%v` renders them "-0" and "0".
//
// Every expectation is live postgres:17-alpine's on the identical fixture.
func setopOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	negZero := math.Copysign(0, -1)
	for _, tbl := range []struct {
		name  string
		scale int
		prec  int
		vals  []int64 // unscaled, at this table's scale
		fs    []float64
	}{
		{name: "dk2", prec: 9, scale: 2, vals: []int64{1275, 300}, fs: []float64{0, 1}},
		{name: "dk4", prec: 18, scale: 4, vals: []int64{127500, 30000}, fs: []float64{negZero, 1}},
	} {
		schema := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "d", Type: parquet.TypeDecimal, Precision: tbl.prec, Scale: tbl.scale},
			{Name: "f", Type: parquet.TypeFloat64},
		}}
		if err := db.CreateTable(ctx, tbl.name, schema, nil); err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, len(tbl.vals))
		for i, v := range tbl.vals {
			rows[i] = map[string]any{
				"id": int64(i + 1),
				"d":  parquet.Decimal128{Lo: uint64(v)},
				"f":  tbl.fs[i],
			}
		}
		ing := db.NewIngester(tbl.name, schema, nil, ingest.Config{MaxBufferRows: 16})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestSetOpDedupsAcrossDecimalScales(t *testing.T) {
	ctx := context.Background()
	db := setopOpen(t)

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		// The reported matrix. 12.75 == 12.7500 and 3.00 == 3.0000, so the
		// two arms hold the same two values at two scales.
		{"union", "SELECT d FROM dk2 UNION SELECT d FROM dk4", 2},
		{"intersect", "SELECT d FROM dk2 INTERSECT SELECT d FROM dk4", 2},
		{"except", "SELECT d FROM dk2 EXCEPT SELECT d FROM dk4", 0},
		{"intersect all", "SELECT d FROM dk2 INTERSECT ALL SELECT d FROM dk4", 2},
		{"except all", "SELECT d FROM dk2 EXCEPT ALL SELECT d FROM dk4", 0},
		// UNION ALL does not dedup at all — the control that says the fix is
		// in the KEY and not in the concatenation.
		{"union all", "SELECT d FROM dk2 UNION ALL SELECT d FROM dk4", 4},
		// One arm against itself: the shape that was already right, so it
		// pins that the key did not change for a single scale.
		{"self union", "SELECT d FROM dk2 UNION SELECT d FROM dk2", 2},
		// The filing's tell: GROUP BY over the same concatenation keys
		// through the columnar encoding and was already correct. It has to
		// stay correct, and it has to AGREE with the set operation above.
		{"group by control",
			"SELECT u.d FROM (SELECT d FROM dk2 UNION ALL SELECT d FROM dk4) u GROUP BY u.d", 2},
		{"distinct control",
			"SELECT DISTINCT u.d FROM (SELECT d FROM dk2 UNION ALL SELECT d FROM dk4) u", 2},
		// The same defect one type over: -0.0 and +0.0 are one value to the
		// comparator and two renderings to %v.
		{"float union", "SELECT f FROM dk2 UNION SELECT f FROM dk4", 2},
		{"float intersect", "SELECT f FROM dk2 INTERSECT SELECT f FROM dk4", 2},
		{"float except", "SELECT f FROM dk2 EXCEPT SELECT f FROM dk4", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tmRun(ctx, db, tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if got := int64(len(res.Rows)); got != tc.want {
				t.Errorf("%s\n  got %d rows, want %d (live PostgreSQL 17)\n  rows: %v",
					tc.sql, got, tc.want, res.Rows)
			}
		})
	}
}
