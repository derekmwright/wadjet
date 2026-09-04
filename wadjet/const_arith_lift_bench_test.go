package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ClickBench Q30 SHAPE, which is what #850 is measured on: ninety
// `SUM(col + k)` over one integer column.
//
// There is no hits part on the machine this was written on, so the fixture is
// generated with Q30's shape and column profile rather than its data — an
// int32 column of realistic magnitude, one million rows, the aggregate count
// the published query has. What the benchmark measures is the difference
// between ninety per-row expression passes with ninety accumulators and one
// SUM plus one COUNT, which is the whole content of the regression.
const caaBenchRows = 1_000_000

func caaBenchDB(tb testing.TB) *DB {
	tb.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32},
		{Name: "w", Type: parquet.TypeInt32},
		{Name: "b", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64},
	}}
	if err := db.CreateTable(ctx, "q30", schema, nil); err != nil {
		tb.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("q30", schema, nil, ingest.Config{
		MaxBufferRows: caaBenchRows + 1, RowGroupSize: 100_000,
	})
	rows := make([]map[string]any, 0, caaBenchRows)
	for i := 0; i < caaBenchRows; i++ {
		rows = append(rows, map[string]any{
			"k": int32(i % 1000),
			"w": int32(i%1600 + 320),
			"b": int64(i) * 1000,
			"f": float64(i) / 7,
		})
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		tb.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		tb.Fatalf("flush: %v", err)
	}
	return db
}

// caaQ30SQL is the published Q30 shape: N aggregates over one column, each
// with its own constant.
func caaQ30SQL(col string, n int) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "SUM(%s + %d) AS s%d", col, i, i)
	}
	sb.WriteString(" FROM q30")
	return sb.String()
}

func BenchmarkConstArithLiftQ30Shape(b *testing.B) {
	db := caaBenchDB(b)
	ctx := context.Background()
	for _, c := range []struct{ name, sql string }{
		// The lifted shapes: an int32 column bounds itself, so no ANALYZE is
		// needed; a float column can never refuse.
		{"int32_x90", caaQ30SQL("w", 90)},
		{"float64_x90", caaQ30SQL("f", 90)},
		// An INT64 column, which needs a min/max to be bounded — and gets one
		// from the MANIFEST's footer statistics without any ANALYZE, which is
		// measured here rather than assumed: this arm lifts too. The shape
		// that genuinely declines (no statistics at all) is
		// logical.TestConstArithLiftDecidesFromTheColumnType's
		// int64_without_stats cell, which needs a hand-built plan to reach.
		{"int64_x90", caaQ30SQL("b", 90)},
		// One aggregate, so the per-aggregate cost is visible apart from the
		// ninety-fold one.
		{"int32_x1", caaQ30SQL("w", 1)},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Query(ctx, c.sql); err != nil {
					b.Fatalf("%v", err)
				}
			}
		})
	}
}
