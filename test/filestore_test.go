package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestFileStoreEndToEnd verifies the full query pipeline using local
// filesystem storage instead of S3, simulating an edge deployment.
func TestFileStoreEndToEnd(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store, err := objstore.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "edge-data",
	})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "sensor_id", Type: parquet.TypeString},
			{Name: "reading", Type: parquet.TypeFloat64},
			{Name: "base", Type: parquet.TypeString},
		},
	}

	if err := db.CreateTable(ctx, "sensor_data", schema, []string{"base"}); err != nil {
		t.Fatal(err)
	}

	// Generate sensor readings
	rng := rand.New(rand.NewSource(42))
	bases := []string{"afb-east", "afb-west"}
	rows := make([]map[string]any, 200)
	for i := range rows {
		rows[i] = map[string]any{
			"sensor_id": fmt.Sprintf("sensor-%03d", rng.Intn(20)),
			"reading":   rng.Float64() * 100,
			"base":      bases[rng.Intn(len(bases))],
		}
	}

	ing := db.NewIngester("sensor_data", schema, []string{"base"}, ingest.Config{
		MaxBufferRows: 500,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// SELECT *
	result, err := db.Query(ctx, "SELECT * FROM sensor_data LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result.Rows))
	}

	// Aggregate
	result, err = db.Query(ctx, "SELECT base, AVG(reading) as avg_reading, COUNT(reading) as cnt FROM sensor_data GROUP BY base")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result.Rows))
	}

	var totalCnt int64
	for _, row := range result.Rows {
		cnt := row["cnt"].(int64)
		avg := row["avg_reading"].(float64)
		t.Logf("  base=%s avg=%.2f cnt=%d", row["base"], avg, cnt)
		totalCnt += cnt
	}
	if totalCnt != 200 {
		t.Fatalf("expected total count 200, got %d", totalCnt)
	}

	// Window function
	result, err = db.Query(ctx, "SELECT sensor_id, reading, ROW_NUMBER() OVER (PARTITION BY base ORDER BY reading DESC) as rn FROM sensor_data")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 200 {
		t.Fatalf("expected 200 rows, got %d", len(result.Rows))
	}

	t.Logf("FileStore end-to-end: all queries passed on local filesystem at %s", dir)
}
