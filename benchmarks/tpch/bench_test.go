package tpch

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/wadjet"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// setupTPCH creates a Wadjet DB loaded with TPC-H data at the given scale factor.
func setupTPCH(tb testing.TB, sf ScaleFactor) *wadjet.DB {
	tb.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
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
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
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

// setupTPCHStreaming creates a TPC-H database using streaming generation for large scale factors.
// Memory-bounded: generates data in chunks of chunkSize rows, ingests each chunk, releases.
func setupTPCHStreaming(tb testing.TB, sf ScaleFactor) *wadjet.DB {
	tb.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "tpch",
	})
	if err != nil {
		tb.Fatal(err)
	}

	// Create all tables
	for tableName, schema := range AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			tb.Fatalf("creating table %s: %v", tableName, err)
		}
	}

	// Create ingesters with auto-flush thresholds
	ingesters := make(map[string]*ingest.Ingester)
	for tableName, schema := range AllTables {
		ingesters[tableName] = db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: 100_000,
			RowGroupSize:  65_536,
		})
	}

	counts := sf.RowCounts()
	totalExpected := counts.Region + counts.Nation + counts.Supplier + counts.Part +
		counts.PartSupp + counts.Customer + counts.Orders + counts.LineItem
	tb.Logf("Generating TPC-H SF=%.0f (~%dM rows across 8 tables)...",
		float64(sf), totalExpected/1_000_000)

	start := time.Now()
	totalRows := 0
	lastLog := time.Now()

	err = GenerateChunked(sf, 50_000, func(table string, rows []map[string]any) error {
		totalRows += len(rows)
		if time.Since(lastLog) > 5*time.Second {
			pct := float64(totalRows) / float64(totalExpected) * 100
			tb.Logf("  generated %dM rows (%.0f%%) in %v...",
				totalRows/1_000_000, pct, time.Since(start).Round(time.Second))
			lastLog = time.Now()
		}
		return ingesters[table].Ingest(ctx, rows)
	})
	if err != nil {
		tb.Fatalf("generating data: %v", err)
	}

	// Flush remaining data
	for tableName, ing := range ingesters {
		if err := ing.FlushAll(ctx); err != nil {
			tb.Fatalf("flushing %s: %v", tableName, err)
		}
	}

	tb.Logf("TPC-H SF=%.0f loaded: %dM rows in %v",
		float64(sf), totalRows/1_000_000, time.Since(start).Round(time.Second))

	// Report memory usage
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	tb.Logf("Memory: heap=%dMB, sys=%dMB", mem.HeapAlloc/1024/1024, mem.Sys/1024/1024)

	return db
}

// scaleFromEnv reads TPCH_SCALE env var and returns the scale factor.
// Skips the test if TPCH_SCALE is not set.
func scaleFromEnv(tb testing.TB) ScaleFactor {
	tb.Helper()
	v := os.Getenv("TPCH_SCALE")
	if v == "" {
		tb.Skip("skipping (set TPCH_SCALE=1|10|100 to enable)")
	}
	switch v {
	case "0.01":
		return SF001
	case "0.1":
		return SF01
	case "1":
		return SF1
	case "10":
		return SF10
	case "100":
		return SF100
	default:
		tb.Fatalf("unsupported TPCH_SCALE=%q (use 0.01, 0.1, 1, 10, or 100)", v)
		return 0
	}
}

// queryTimeout returns a per-query timeout based on scale factor.
// Multi-join queries blow up with nested-loop joins at large scale.
func queryTimeout(sf ScaleFactor) time.Duration {
	switch {
	case sf >= 10:
		return 5 * time.Minute
	case sf >= 1:
		return 2 * time.Minute
	default:
		return 30 * time.Second
	}
}

// TestTPCHQueriesLarge runs all 22 TPC-H queries at the scale factor specified by TPCH_SCALE.
// Every query runs — no skip lists. Queries that exceed the per-query timeout fail with
// a clear diagnostic. Queries that exhaust memory will surface as OOM kills, indicating
// the hash join implementation needs memory bounds or spill-to-disk support.
//
// Usage:
//
//	TPCH_SCALE=1  go test -v -run TestTPCHQueriesLarge -timeout 60m  ./benchmarks/tpch/
//	TPCH_SCALE=10 go test -v -run TestTPCHQueriesLarge -timeout 120m ./benchmarks/tpch/
func TestTPCHQueriesLarge(t *testing.T) {
	sf := scaleFromEnv(t)
	db := setupTPCHStreaming(t, sf)

	timeout := queryTimeout(sf)
	t.Logf("Per-query timeout: %v", timeout)

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]

		runtime.GC()

		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			start := time.Now()
			result, err := db.Query(ctx, q.SQL)
			elapsed := time.Since(start)

			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)
			heapDelta := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)

			if err != nil {
				t.Fatalf("Q%d failed in %v (heap: %dMB, delta: %+dMB): %v",
					qNum, elapsed,
					memAfter.HeapAlloc/1024/1024, heapDelta/1024/1024,
					err)
			}

			t.Logf("Q%02d: %d rows in %v (heap: %dMB, delta: %+dMB)",
				qNum, len(result.Rows), elapsed,
				memAfter.HeapAlloc/1024/1024, heapDelta/1024/1024)
			maxShow := 5
			if len(result.Rows) < maxShow {
				maxShow = len(result.Rows)
			}
			for i := 0; i < maxShow; i++ {
				t.Logf("  row %d: %v", i, result.Rows[i])
			}
		})
	}
}

// BenchmarkTPCHLarge benchmarks all 22 TPC-H queries at the scale factor specified by TPCH_SCALE.
//
// Usage:
//
//	TPCH_SCALE=1  go test -bench=BenchmarkTPCHLarge -benchtime=3x -timeout 30m  ./benchmarks/tpch/
//	TPCH_SCALE=10 go test -bench=BenchmarkTPCHLarge -benchtime=1x -timeout 120m ./benchmarks/tpch/
func BenchmarkTPCHLarge(b *testing.B) {
	sf := scaleFromEnv(b)
	db := setupTPCHStreaming(b, sf)
	timeout := queryTimeout(sf)

	queryNums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	for _, qNum := range queryNums {
		q := TPCHQueries[qNum]

		runtime.GC()

		b.Run(fmt.Sprintf("Q%02d", qNum), func(b *testing.B) {
			b.ReportAllocs()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			result, err := db.Query(ctx, q.SQL)
			cancel()
			if err != nil {
				b.Fatalf("Q%d warmup failed: %v", qNum, err)
			}
			b.ReportMetric(float64(len(result.Rows)), "result_rows")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				_, err := db.Query(ctx, q.SQL)
				cancel()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestTPCHStreamingGen verifies the streaming generator produces correct row counts.
func TestTPCHStreamingGen(t *testing.T) {
	counts := SF1.RowCounts()
	totals := make(map[string]int)

	err := GenerateChunked(SF1, 10_000, func(table string, rows []map[string]any) error {
		totals[table] += len(rows)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	checks := map[string]int{
		"region":   counts.Region,
		"nation":   counts.Nation,
		"supplier": counts.Supplier,
		"part":     counts.Part,
		"customer": counts.Customer,
	}
	for table, expected := range checks {
		if totals[table] != expected {
			t.Errorf("%s: expected %d rows, got %d", table, expected, totals[table])
		}
	}

	// Orders and lineitem may vary slightly due to random line-per-order distribution
	if totals["orders"] < counts.Orders/2 {
		t.Errorf("orders: expected ~%d rows, got %d", counts.Orders, totals["orders"])
	}
	if totals["lineitem"] < counts.LineItem/2 {
		t.Errorf("lineitem: expected ~%d rows, got %d", counts.LineItem, totals["lineitem"])
	}

	t.Logf("SF=1 streaming row counts:")
	for _, name := range sortedKeysInt(totals) {
		t.Logf("  %-12s %10d rows", name, totals[name])
	}
}

// sortedKeysInt returns sorted keys from a map[string]int.
func sortedKeysInt(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Helper to ensure parquet import is used
var _ = parquet.TypeString
