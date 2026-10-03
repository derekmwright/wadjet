// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupSampleDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "value", Type: parquet.TypeFloat64},
		},
	}
	if err := db.CreateTable(ctx, "data", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Insert 1000 rows
	rows := make([]map[string]any, 1000)
	for i := range rows {
		rows[i] = map[string]any{
			"id":    int64(i + 1),
			"value": float64(i) * 1.5,
		}
	}
	ing := db.NewIngester("data", schema, nil, ingest.Config{MaxBufferRows: 2000, RowGroupSize: 2000})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestTablesample_Bernoulli(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// BERNOULLI(50) should return ~50% of rows (with statistical tolerance)
	r, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM data TABLESAMPLE BERNOULLI(50)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	cnt, _ := r.Rows[0]["cnt"].(int64)
	// Expect ~500 rows ± 100 (generous tolerance for randomness)
	if cnt < 300 || cnt > 700 {
		t.Errorf("BERNOULLI(50) returned %d rows, expected ~500 ± 200", cnt)
	}
}

func TestTablesample_BernoulliSmall(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// BERNOULLI(10) should return ~10% of rows
	r, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM data TABLESAMPLE BERNOULLI(10)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	cnt, _ := r.Rows[0]["cnt"].(int64)
	// Expect ~100 rows ± 60
	if cnt < 30 || cnt > 200 {
		t.Errorf("BERNOULLI(10) returned %d rows, expected ~100 ± 100", cnt)
	}
}

func TestTablesample_System(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// SYSTEM sampling: block-level. With 1 batch, result is either 0 or 1000.
	// Run multiple times to verify it works (at least one should include data)
	var gotData bool
	for i := 0; i < 20; i++ {
		r, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM data TABLESAMPLE SYSTEM(50)")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		cnt, _ := r.Rows[0]["cnt"].(int64)
		if cnt > 0 {
			gotData = true
			// SYSTEM returns entire batches, so should be all rows
			if cnt != 1000 {
				t.Errorf("SYSTEM(50) returned %d rows, expected 0 or 1000", cnt)
			}
		}
	}
	if !gotData {
		t.Error("SYSTEM(50) returned 0 rows in 20 attempts (extremely unlikely)")
	}
}

func TestTablesample_BernoulliWithFilter(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// Sample + filter should work together
	r, err := db.Query(ctx, "SELECT AVG(value) as avg_val FROM data TABLESAMPLE BERNOULLI(100)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// BERNOULLI(100) = include all rows. Average of 0*1.5, 1*1.5, ..., 999*1.5 = 499.5*1.5 = 749.25
	avg, _ := r.Rows[0]["avg_val"].(float64)
	if math.Abs(avg-749.25) > 0.01 {
		t.Errorf("BERNOULLI(100) avg=%f, want 749.25", avg)
	}
}

func TestTablesample_Bernoulli100Percent(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()

	// 100% should return all rows
	r, err := db.Query(ctx, "SELECT COUNT(*) as cnt FROM data TABLESAMPLE BERNOULLI(100)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	cnt, _ := r.Rows[0]["cnt"].(int64)
	if cnt != 1000 {
		t.Errorf("BERNOULLI(100) returned %d rows, want 1000", cnt)
	}
}

// The embedded engine reads a TABLESAMPLE argument as PostgreSQL 17.11 does
// (#1411): any constant expression coerced to real when the statement is
// planned, the range checked when the scan begins. `data` has 1000 rows and
// `none` none. Each want is PostgreSQL's over the same rows; a sampled count
// is held to a range. At 6184761c BERNOULLI (0) and SYSTEM (0) answered all
// 1000 rows, 101 / 1e20 / CAST('1e400' AS DOUBLE PRECISION) / a bare 1e400
// answered all 1000, and -1, NULL, '50', 25 * 2 and CAST(50 AS NUMERIC) were
// 42601.
func TestTablesampleArgumentIsPostgresReal(t *testing.T) {
	db := setupSampleDB(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := db.CreateTable(ctx, "none", schema, nil); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		sql    string
		lo, hi int64  // the count's range
		state  string // or the SQLSTATE PostgreSQL raises
	}{
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (0)", 0, 0, ""},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (-0.0)", 0, 0, ""},
		{"SELECT COUNT(*) FROM data TABLESAMPLE SYSTEM (0)", 0, 0, ""},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (100.000001)", 1000, 1000, ""},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI ('50')", 350, 650, ""},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (25 * 2)", 350, 650, ""},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (CAST(50 AS NUMERIC))", 350, 650, ""},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (101)", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (-1)", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (1e20)", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (99999999999999999999)", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (CAST('1e20' AS DOUBLE PRECISION))", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (CAST('NaN' AS DOUBLE PRECISION))", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (NULL)", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE SYSTEM (101)", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM none TABLESAMPLE BERNOULLI (101)", 0, 0, "2202H"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (1e400)", 0, 0, "22003"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (CAST('1e400' AS DOUBLE PRECISION))", 0, 0, "22003"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (1e-46)", 0, 0, "22003"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (CAST('50' AS TEXT))", 0, 0, "42804"},
		{"SELECT COUNT(*) FROM data TABLESAMPLE BERNOULLI (id)", 0, 0, "42703"},
	} {
		r, err := db.Query(ctx, c.sql)
		if c.state != "" {
			if got := sqlerr.StateOf(err); got != c.state {
				t.Errorf("%s: %v (%s), want %s", c.sql, err, got, c.state)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		cnt, _ := r.Rows[0]["count"].(int64)
		if cnt < c.lo || cnt > c.hi {
			t.Errorf("%s: %d rows, want %d..%d", c.sql, cnt, c.lo, c.hi)
		}
	}
}
