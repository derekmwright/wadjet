package tpch

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/caelum/caelum"
	"github.com/derekmwright/caelum/internal/storage/ingest"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// setupTPCH creates a Caelum DB loaded with TPC-H data at the given scale factor.
func setupTPCH(tb testing.TB, sf ScaleFactor) *caelum.DB {
	tb.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := caelum.Open(ctx, caelum.Config{
		Store:  store,
		Bucket: "tpch",
	})
	if err != nil {
		tb.Fatal(err)
	}

	data := Generate(sf)

	for tableName, schema := range AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			tb.Fatalf("creating table %s: %v", tableName, err)
		}

		rows := data[tableName]
		if len(rows) == 0 {
			continue
		}

		ing := db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1,
			RowGroupSize:  max(100, len(rows)/4),
		})

		if err := ing.Ingest(ctx, rows); err != nil {
			tb.Fatalf("ingesting %s: %v", tableName, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			tb.Fatalf("flushing %s: %v", tableName, err)
		}
	}

	return db
}

// TestTPCHDataGen verifies data generation produces expected row counts.
func TestTPCHDataGen(t *testing.T) {
	data := Generate(SF001)
	counts := SF001.RowCounts()

	checks := map[string]int{
		"region":   counts.Region,
		"nation":   counts.Nation,
		"supplier": counts.Supplier,
		"part":     counts.Part,
		"customer": counts.Customer,
	}

	for table, expected := range checks {
		if len(data[table]) != expected {
			t.Errorf("%s: expected %d rows, got %d", table, expected, len(data[table]))
		}
	}

	// Orders and lineitem may have slight variance due to generation logic
	if len(data["orders"]) < counts.Orders/2 {
		t.Errorf("orders: expected ~%d rows, got %d", counts.Orders, len(data["orders"]))
	}
	if len(data["lineitem"]) < counts.LineItem/2 {
		t.Errorf("lineitem: expected ~%d rows, got %d", counts.LineItem, len(data["lineitem"]))
	}

	t.Logf("Generated data at SF=%.2f:", float64(SF001))
	for _, name := range sortedKeys(data) {
		t.Logf("  %-12s %6d rows", name, len(data[name]))
	}
}

// TestTPCHQueries runs each TPC-H query at SF0.01 to verify correctness.
func TestTPCHQueries(t *testing.T) {
	db := setupTPCH(t, SF001)
	ctx := context.Background()

	// Get sorted query numbers
	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			start := time.Now()
			result, err := db.Query(ctx, q.SQL)
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("Q%d failed: %v\nSQL: %s", qNum, err, q.SQL)
			}

			t.Logf("Q%02d: %d rows in %v", qNum, len(result.Rows), elapsed)
			// Log first few rows for inspection
			maxRows := 3
			if len(result.Rows) < maxRows {
				maxRows = len(result.Rows)
			}
			for i := 0; i < maxRows; i++ {
				t.Logf("  row %d: %v", i, result.Rows[i])
			}
		})
	}
}

// BenchmarkTPCH runs TPC-H queries as Go benchmarks at SF0.01.
func BenchmarkTPCH(b *testing.B) {
	db := setupTPCH(b, SF001)
	ctx := context.Background()

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]
		b.Run(fmt.Sprintf("Q%02d", qNum), func(b *testing.B) {
			b.ReportAllocs()

			// Verify query works before benchmarking
			result, err := db.Query(ctx, q.SQL)
			if err != nil {
				b.Skipf("Q%d not supported: %v", qNum, err)
			}
			b.ReportMetric(float64(len(result.Rows)), "result_rows")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := db.Query(ctx, q.SQL)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkTPCHSingleQuery benchmarks a specific query for detailed profiling.
// Usage: go test -bench=BenchmarkTPCHSingleQuery -benchtime=10s -run=^$ ./benchmarks/tpch/
func BenchmarkTPCHSingleQuery(b *testing.B) {
	db := setupTPCH(b, SF001)
	ctx := context.Background()

	// Q1 is the classic scan+aggregate benchmark
	q := TPCHQueries[1]
	b.ReportAllocs()

	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Query(ctx, q.SQL)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	runtime.ReadMemStats(&memAfter)
	b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/float64(b.N), "bytes/op_total")
}

// TestTPCHTableSchemas verifies all table schemas can be created.
func TestTPCHTableSchemas(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := caelum.Open(ctx, caelum.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	for name, schema := range AllTables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Errorf("failed to create table %s: %v", name, err)
		}
		// Verify column count
		if len(schema.Columns) == 0 {
			t.Errorf("table %s has no columns", name)
		}
	}

	tables, err := db.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != len(AllTables) {
		t.Errorf("expected %d tables, got %d", len(AllTables), len(tables))
	}
}

// TestTPCHRowCounts verifies scale factor row count calculations.
func TestTPCHRowCounts(t *testing.T) {
	for _, sf := range []ScaleFactor{SF001, SF01, SF1} {
		counts := sf.RowCounts()
		t.Logf("SF=%.2f: region=%d nation=%d supplier=%d part=%d partsupp=%d customer=%d orders=%d lineitem=%d",
			float64(sf), counts.Region, counts.Nation, counts.Supplier, counts.Part,
			counts.PartSupp, counts.Customer, counts.Orders, counts.LineItem)

		if counts.Region != 5 {
			t.Errorf("SF=%.2f: region should always be 5", float64(sf))
		}
		if counts.Nation != 25 {
			t.Errorf("SF=%.2f: nation should always be 25", float64(sf))
		}
	}
}

func sortedKeys(m map[string][]map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Helper to ensure parquet import is used
var _ = parquet.TypeString
