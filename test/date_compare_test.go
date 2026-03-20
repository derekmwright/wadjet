package test

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
)

// TestDateColumnComparison is a regression test for GitHub issue #3:
// DATE column equality and >= comparisons fail on exact match.
// The root cause was toInt64() converting date strings to milliseconds-since-epoch
// instead of days-since-epoch, causing int32 overflow in the comparison kernel.
func TestDateColumnComparison(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "event_date", Type: parquet.TypeDate},
			{Name: "label", Type: parquet.TypeString},
		},
	}

	if err := db.CreateTable(ctx, "threat_events", schema, nil); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"id": int64(1), "event_date": "2026-03-19", "label": "a"},
		{"id": int64(2), "event_date": "2026-03-19", "label": "b"},
		{"id": int64(3), "event_date": "2026-03-19", "label": "c"},
		{"id": int64(4), "event_date": "2026-03-20", "label": "d"},
		{"id": int64(5), "event_date": "2026-03-18", "label": "e"},
	}

	ing := db.NewIngester("threat_events", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		query     string
		wantCount int64
	}{
		{"eq exact date", "SELECT COUNT(*) AS cnt FROM threat_events WHERE event_date = '2026-03-19'", 3},
		{"gte exact date", "SELECT COUNT(*) AS cnt FROM threat_events WHERE event_date >= '2026-03-19'", 4},
		{"gt previous date", "SELECT COUNT(*) AS cnt FROM threat_events WHERE event_date > '2026-03-18'", 4},
		{"lt date", "SELECT COUNT(*) AS cnt FROM threat_events WHERE event_date < '2026-03-19'", 1},
		{"lte date", "SELECT COUNT(*) AS cnt FROM threat_events WHERE event_date <= '2026-03-19'", 4},
		{"between dates", "SELECT COUNT(*) AS cnt FROM threat_events WHERE event_date BETWEEN '2026-03-18' AND '2026-03-19'", 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := db.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(r.Rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(r.Rows))
			}
			cnt, ok := r.Rows[0]["cnt"].(int64)
			if !ok {
				t.Fatalf("cnt is not int64: %T %v", r.Rows[0]["cnt"], r.Rows[0]["cnt"])
			}
			if cnt != tt.wantCount {
				t.Fatalf("got count=%d, want %d", cnt, tt.wantCount)
			}
		})
	}
}
