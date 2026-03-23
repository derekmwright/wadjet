package test

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/wadjet"
)

func TestGenerateSeriesE2E(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		query string
		rows  int
		first int64
		last  int64
	}{
		{
			name:  "basic ascending",
			query: "SELECT * FROM generate_series(1, 5)",
			rows:  5, first: 1, last: 5,
		},
		{
			name:  "with step",
			query: "SELECT * FROM generate_series(0, 10, 3)",
			rows:  4, first: 0, last: 9,
		},
		{
			name:  "descending auto-step",
			query: "SELECT * FROM generate_series(5, 1)",
			rows:  5, first: 5, last: 1,
		},
		{
			name:  "single value",
			query: "SELECT * FROM generate_series(42, 42)",
			rows:  1, first: 42, last: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := db.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(r.Rows) != tt.rows {
				t.Fatalf("got %d rows, want %d", len(r.Rows), tt.rows)
			}
			firstVal, ok := r.Rows[0]["generate_series"].(int64)
			if !ok {
				t.Fatalf("first row: expected int64, got %T (%v)", r.Rows[0]["generate_series"], r.Rows[0]["generate_series"])
			}
			if firstVal != tt.first {
				t.Errorf("first value: got %d, want %d", firstVal, tt.first)
			}
			lastVal := r.Rows[len(r.Rows)-1]["generate_series"].(int64)
			if lastVal != tt.last {
				t.Errorf("last value: got %d, want %d", lastVal, tt.last)
			}
		})
	}
}

func TestGenerateSeriesWithExpressions(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test using generate_series with expressions (SUM, WHERE, etc.)
	r, err := db.Query(ctx, "SELECT SUM(generate_series) AS total FROM generate_series(1, 100)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(r.Rows))
	}
	// SUM(1..100) = 5050
	var total float64
	switch v := r.Rows[0]["total"].(type) {
	case int64:
		total = float64(v)
	case float64:
		total = v
	default:
		t.Fatalf("total: unexpected type %T (%v)", r.Rows[0]["total"], r.Rows[0]["total"])
	}
	if total != 5050 {
		t.Errorf("SUM(1..100) = %v, want 5050", total)
	}
}
