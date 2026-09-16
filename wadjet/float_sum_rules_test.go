// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The float SUM's two rules, end to end (#950, #1082). Every expectation was
// measured on live PostgreSQL 17.11 over these same rows; the census that
// walks them on five execution arms is
// coordinator.TestNumericValuesMatchPostgres.

const fsrReal = "fsrreal"
const fsrWide = "fsrwide"

func fsrOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	load := func(name string, sch parquet.Schema, rows []map[string]any) {
		t.Helper()
		if err := db.CreateTable(ctx, name, sch, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(name, sch, nil, ingest.Config{MaxBufferRows: len(rows) + 1})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// 2^24 is where a real stops counting by ones, so 16777216 followed by
	// -20 is the shape that separates a float4 accumulation from a float8 one
	// narrowed at the end: the server answers 1.677721e+07 and a float8 total
	// answers 1.6777211e+07.
	load(fsrReal, parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "r", Type: parquet.TypeFloat32, Nullable: true},
	}}, []map[string]any{
		{"id": int64(1), "g": int32(1), "r": float32(2)},
		{"id": int64(2), "g": int32(1), "r": float32(0.1)},
		{"id": int64(3), "g": int32(1), "r": float32(12.75)},
		{"id": int64(4), "g": int32(1), "r": float32(16777216)},
		{"id": int64(5), "g": int32(1), "r": float32(-20)},
		{"id": int64(6), "g": int32(2), "r": float32(0)},
		{"id": int64(7), "g": int32(2), "r": float32(12.75)},
		{"id": int64(8), "g": int32(2), "r": float32(1.5)},
		{"id": int64(9), "g": int32(2), "r": nil},
	})
	load(fsrWide, parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
	}}, []map[string]any{
		{"id": int64(1), "g": int32(1), "f": 1e308},
		{"id": int64(2), "g": int32(1), "f": 1e308},
		{"id": int64(3), "g": int32(2), "f": 2.5},
		{"id": int64(4), "g": int32(2), "f": 1.5},
	})
	return db
}

func fsrOne(t *testing.T, db *DB, sql string) any {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%s returned %d rows, want 1", sql, len(res.Rows))
	}
	return res.Rows[0][res.Columns[0]]
}

// TestSumOverARealAccumulatesAtRealWidth is #950: three spellings of one
// question answered three numbers, because the grouped accumulator totalled in
// float64 and the window one did too while the declaration narrowed once at
// the end.
func TestSumOverARealAccumulatesAtRealWidth(t *testing.T) {
	db := fsrOpen(t)
	for _, c := range []struct {
		name, sql string
		want      float32
	}{
		{"ungrouped", "SELECT SUM(r) AS v FROM " + fsrReal, 1.6777224e+07},
		{"grouped", "SELECT SUM(r) AS v FROM " + fsrReal + " WHERE g = 1", 1.677721e+07},
		{"distinct", "SELECT SUM(DISTINCT r) AS v FROM " + fsrReal, 1.6777212e+07},
		{"through_a_derived_table",
			"SELECT SUM(v) AS v FROM (SELECT r AS v FROM " + fsrReal + " WHERE g = 1) x", 1.677721e+07},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := fsrOne(t, db, c.sql)
			f, ok := got.(float32)
			if !ok || f != c.want {
				t.Errorf("%s = %T(%v), want float32(%v) — PostgreSQL 17.11 accumulates "+
					"sum(real) at float4's width (#950)", c.sql, got, got, c.want)
			}
		})
	}
}

// TestTheWindowedRealSumAnswersWhatTheGroupedOneAnswers is the other half of
// #950: the window spelling was a THIRD number. Since #1118 the column
// DECLARES real as well, so the box is a float32 — the same box the grouped
// spelling produces, which is the point of the pairing.
func TestTheWindowedRealSumAnswersWhatTheGroupedOneAnswers(t *testing.T) {
	db := fsrOpen(t)
	res, err := db.Query(context.Background(),
		"SELECT id, SUM(r) OVER (ORDER BY id) AS v FROM "+fsrReal+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	// psql, PostgreSQL 17.11, same rows: 2, 2.1, 14.85, 1.677723e+07,
	// 1.677721e+07, 1.677721e+07, 1.6777222e+07, 1.6777224e+07, 1.6777224e+07.
	want := []float32{2, 2.1, 14.85, 1.677723e+07, 1.677721e+07,
		1.677721e+07, 1.6777222e+07, 1.6777224e+07, 1.6777224e+07}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
	}
	for i, row := range res.Rows {
		f, ok := row["v"].(float32)
		if !ok {
			t.Fatalf("row %d: %T, want float32 — `sum(real) over ()` declares real (#1118)",
				i, row["v"])
		}
		if f != want[i] {
			t.Errorf("row %d = %v, want %v (the real total)", i, f, want[i])
		}
	}
}

// TestAMovingRealFrameRecomputesRatherThanRetracting is the frame half: with
// no inverse transition for a float sum, PostgreSQL recomputes a frame whose
// lower end advanced, and subtracting a float4 total instead accumulates
// rounding the server never has.
func TestAMovingRealFrameRecomputesRatherThanRetracting(t *testing.T) {
	db := fsrOpen(t)
	res, err := db.Query(context.Background(),
		"SELECT id, SUM(r) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM "+
			fsrReal+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	// psql, PostgreSQL 17.11: 2, 2.1, 12.85, 1.6777228e+07, 1.6777196e+07,
	// -20, 12.75, 14.25, 1.5.
	want := []float32{2, 2.1, 12.85, 1.6777228e+07, 1.6777196e+07, -20, 12.75, 14.25, 1.5}
	for i, row := range res.Rows {
		f, ok := row["v"].(float32)
		if !ok {
			t.Fatalf("row %d: %T, want float32 — `sum(real) over ()` declares real (#1118)",
				i, row["v"])
		}
		if f != want[i] {
			t.Errorf("row %d = %v, want %v", i, f, want[i])
		}
	}
}

// TestAverageOverARealStaysDoublePrecision is the rule AVG does NOT share:
// PostgreSQL's avg(real) is double precision and totals each value at that
// width (#760). Narrowing it here would absorb the 0.1 the server keeps.
func TestAverageOverARealStaysDoublePrecision(t *testing.T) {
	db := fsrOpen(t)
	got := fsrOne(t, db, "SELECT AVG(r) AS v FROM "+fsrReal)
	f, ok := got.(float64)
	// psql: 2097153.1375
	if !ok || f != 2097153.1375 {
		t.Errorf("AVG(r) = %T(%v), want float64(2097153.1375) — avg(real) is double "+
			"precision on PostgreSQL 17.11 (#760)", got, got)
	}
}

// TestAFloatSumOutsideTheTypeIsRefused is #1082's aggregate half: the running
// total left float8's range and this engine answered +Infinity, which a CTAS
// then stored.
func TestAFloatSumOutsideTheTypeIsRefused(t *testing.T) {
	db := fsrOpen(t)
	for _, c := range []struct{ name, sql string }{
		{"ungrouped_sum", "SELECT SUM(f) AS v FROM " + fsrWide + " WHERE g = 1"},
		{"ungrouped_avg", "SELECT AVG(f) AS v FROM " + fsrWide + " WHERE g = 1"},
		{"grouped_sum", "SELECT g, SUM(f) AS v FROM " + fsrWide + " GROUP BY g"},
		{"windowed_sum", "SELECT SUM(f) OVER () AS v FROM " + fsrWide + " WHERE g = 1"},
		{"windowed_avg", "SELECT AVG(f) OVER () AS v FROM " + fsrWide + " WHERE g = 1"},
		{"aggregate_over_a_computed_argument", "SELECT MAX(f * 10) AS v FROM " + fsrWide},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := db.Query(context.Background(), c.sql)
			if err == nil {
				t.Fatalf("%s answered; PostgreSQL 17.11 raises 22003 for it (#1082)", c.sql)
			}
			if state := sqlerr.StateOf(err); state != "22003" {
				t.Errorf("%s raised SQLSTATE %s, want 22003: %v", c.sql, state, err)
			}
		})
	}
	t.Run("a_group_that_does_not_overflow_still_answers", func(t *testing.T) {
		got := fsrOne(t, db, "SELECT SUM(f) AS v FROM "+fsrWide+" WHERE g = 2")
		if f, ok := got.(float64); !ok || f != 4 {
			t.Errorf("SUM over the finite group = %T(%v), want float64(4)", got, got)
		}
	})
}

// TestAnInfiniteInputSumsToInfinity is the operand exemption at the aggregate:
// PostgreSQL's float8pl exempts an infinite input, so a column that HOLDS an
// infinity totals to one on both engines rather than raising.
func TestAnInfiniteInputSumsToInfinity(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "fsrinf", sch, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("fsrinf", sch, nil, ingest.Config{MaxBufferRows: 8})
	rows := []map[string]any{{"id": int64(1), "f": math.Inf(1)}, {"id": int64(2), "f": 1.0}}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	got := fsrOne(t, db, "SELECT SUM(f) AS v FROM fsrinf")
	if f, ok := got.(float64); !ok || !math.IsInf(f, 1) {
		t.Errorf("SUM = %T(%v), want +Inf — an infinity that ARRIVES is a value on "+
			"both engines, and only a total that leaves the type from finite inputs "+
			"is 22003", got, got)
	}
}
