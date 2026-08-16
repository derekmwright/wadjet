package security

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// setupSecurity creates a Wadjet DB loaded with security event data.
func setupSecurity(tb testing.TB, sf ScaleFactor) *wadjet.DB {
	tb.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "security",
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

// setupSecurityStreaming uses chunked generation for large scale factors.
func setupSecurityStreaming(tb testing.TB, sf ScaleFactor) *wadjet.DB {
	tb.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "security",
	})
	if err != nil {
		tb.Fatal(err)
	}

	for tableName, schema := range AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			tb.Fatalf("creating table %s: %v", tableName, err)
		}
	}

	ingesters := make(map[string]*ingest.Ingester)
	for tableName, schema := range AllTables {
		ingesters[tableName] = db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: 100_000,
			RowGroupSize:  65_536,
		})
	}

	counts := sf.RowCounts()
	tb.Logf("Generating security data SF=%.0f (~%dM rows across 5 tables)...",
		float64(sf), counts.Total()/1_000_000)

	start := time.Now()
	totalRows := 0
	lastLog := time.Now()

	err = GenerateChunked(sf, 50_000, func(table string, rows []map[string]any) error {
		totalRows += len(rows)
		if time.Since(lastLog) > 5*time.Second {
			pct := float64(totalRows) / float64(counts.Total()) * 100
			tb.Logf("  generated %dM rows (%.0f%%) in %v...",
				totalRows/1_000_000, pct, time.Since(start).Round(time.Second))
			lastLog = time.Now()
		}
		return ingesters[table].Ingest(ctx, rows)
	})
	if err != nil {
		tb.Fatalf("generating data: %v", err)
	}

	for tableName, ing := range ingesters {
		if err := ing.FlushAll(ctx); err != nil {
			tb.Fatalf("flushing %s: %v", tableName, err)
		}
	}

	tb.Logf("Security data SF=%.0f loaded: %dM rows in %v",
		float64(sf), totalRows/1_000_000, time.Since(start).Round(time.Second))

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	tb.Logf("Memory: heap=%dMB, sys=%dMB", mem.HeapAlloc/1024/1024, mem.Sys/1024/1024)

	return db
}

// TestSecurityDataGen verifies data generation produces expected row counts.
func TestSecurityDataGen(t *testing.T) {
	data := Generate(SF001)
	counts := SF001.RowCounts()

	checks := map[string]int{
		"netflow":  counts.Netflow,
		"firewall": counts.Firewall,
		"dns":      counts.DNS,
		"auth":     counts.Auth,
		"alerts":   counts.Alerts,
	}

	for table, expected := range checks {
		if len(data[table]) != expected {
			t.Errorf("%s: expected %d rows, got %d", table, expected, len(data[table]))
		}
	}

	t.Logf("Generated data at SF=%.2f:", float64(SF001))
	for _, name := range sortedTableNames(data) {
		t.Logf("  %-12s %6d rows", name, len(data[name]))
	}
}

// expectedRowsSF001 defines expected row counts for each query at SF0.01.
// Zero means "any count is OK" (used for queries where count depends on data distribution).
var expectedRowsSF001 = map[int]int{
	2:  3, // 3 protocols (TCP, UDP, ICMP)
	15: 0, // varies
}

// TestSecurityQueries runs all security queries at SF0.01 for correctness.
func TestSecurityQueries(t *testing.T) {
	db := setupSecurity(t, SF001)
	ctx := context.Background()

	queryNums := sortedQueryNums()

	for _, qNum := range queryNums {
		q := SecurityQueries[qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			start := time.Now()
			result, err := db.Query(ctx, q.SQL)
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("Q%d failed: %v\nSQL: %s", qNum, err, q.SQL)
			}

			t.Logf("Q%02d: %d rows in %v", qNum, len(result.Rows), elapsed)

			// Validate against known row counts
			if expected, ok := expectedRowsSF001[qNum]; ok && expected > 0 {
				if len(result.Rows) != expected {
					t.Errorf("Q%02d row count: got %d, want %d", qNum, len(result.Rows), expected)
				}
			}

			// All queries should return at least some rows at SF0.01
			// (except very selective ones)
			if len(result.Rows) == 0 && qNum != 5 && qNum != 13 {
				t.Logf("Q%02d WARNING: 0 rows returned", qNum)
			}

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

// TestSecurityTableSchemas verifies all table schemas can be created.
func TestSecurityTableSchemas(t *testing.T) {
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
	}

	tables, err := db.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != len(AllTables) {
		t.Errorf("expected %d tables, got %d", len(AllTables), len(tables))
	}
}

// BenchmarkSecurity runs security queries as Go benchmarks at SF0.01.
func BenchmarkSecurity(b *testing.B) {
	db := setupSecurity(b, SF001)
	ctx := context.Background()

	for _, qNum := range sortedQueryNums() {
		q := SecurityQueries[qNum]
		b.Run(fmt.Sprintf("Q%02d", qNum), func(b *testing.B) {
			b.ReportAllocs()

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

func scaleFromEnv(tb testing.TB) ScaleFactor {
	tb.Helper()
	v := os.Getenv("SECURITY_SCALE")
	if v == "" {
		tb.Skip("skipping (set SECURITY_SCALE=1|10|100 to enable)")
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
		tb.Fatalf("unsupported SECURITY_SCALE=%q", v)
		return 0
	}
}

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

// TestSecurityQueriesLarge runs all queries at the scale factor from SECURITY_SCALE.
func TestSecurityQueriesLarge(t *testing.T) {
	sf := scaleFromEnv(t)
	db := setupSecurityStreaming(t, sf)

	timeout := queryTimeout(sf)
	t.Logf("Per-query timeout: %v", timeout)

	for _, qNum := range sortedQueryNums() {
		q := SecurityQueries[qNum]

		runtime.GC()

		t.Run(fmt.Sprintf("Q%02d_%s", qNum, q.Name), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			start := time.Now()
			result, err := db.Query(ctx, q.SQL)
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("Q%02d failed in %v: %v", qNum, elapsed, err)
			}

			t.Logf("Q%02d: %d rows in %v", qNum, len(result.Rows), elapsed)

			if len(result.Rows) == 0 {
				t.Errorf("Q%02d returned 0 rows — likely a bug at this scale", qNum)
			}

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

func sortedQueryNums() []int {
	nums := make([]int, 0, len(SecurityQueries))
	for n := range SecurityQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	return nums
}

func sortedTableNames(m map[string][]map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
