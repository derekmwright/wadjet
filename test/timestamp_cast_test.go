package test

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestTimestampStringComparison verifies implicit string-to-timestamp casting
// for all comparison operators. Regression test for GitHub issue #2.
func TestTimestampStringComparison(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "event_time", Type: parquet.TypeTimestamp},
		},
	}
	if err := db.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"id": int64(1), "event_time": time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)},
		{"id": int64(2), "event_time": time.Date(2026, 3, 19, 15, 0, 0, 0, time.UTC)},
		{"id": int64(3), "event_time": time.Date(2026, 3, 19, 20, 0, 0, 0, time.UTC)},
	}

	ing := db.NewIngester("events", schema, nil, ingest.Config{MaxBufferRows: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{"ge_midnight", "SELECT COUNT(*) as cnt FROM events WHERE event_time >= '2026-03-19T00:00:00Z'", 3},
		{"le_2300", "SELECT COUNT(*) as cnt FROM events WHERE event_time <= '2026-03-19T23:00:00Z'", 3},
		{"lt_2000", "SELECT COUNT(*) as cnt FROM events WHERE event_time < '2026-03-19T20:00:00Z'", 2},
		{"gt_1500", "SELECT COUNT(*) as cnt FROM events WHERE event_time > '2026-03-19T15:00:00Z'", 1},
		{"eq_1500", "SELECT COUNT(*) as cnt FROM events WHERE event_time = '2026-03-19T15:00:00Z'", 1},
		{"ge_sql_format", "SELECT COUNT(*) as cnt FROM events WHERE event_time >= '2026-03-19 00:00:00'", 3},
		{"le_date_only", "SELECT COUNT(*) as cnt FROM events WHERE event_time <= '2026-03-20'", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := db.Query(ctx, tt.query)
			if err != nil {
				t.Fatal(err)
			}
			cnt, ok := res.Rows[0]["cnt"].(int64)
			if !ok || cnt != tt.expected {
				t.Errorf("expected %d rows, got %v", tt.expected, res.Rows[0]["cnt"])
			}
		})
	}
}

// TestTimestampStringCompoundWhere reproduces issue #2 — compound WHERE with
// string column AND timestamp column compared against string literal.
func TestTimestampStringCompoundWhere(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "tenant_id", Type: parquet.TypeString},
			{Name: "event_time", Type: parquet.TypeTimestamp},
			{Name: "severity", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "threat_events", schema, nil); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"tenant_id": "tenant-a", "event_time": time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC), "severity": "high"},
		{"tenant_id": "tenant-a", "event_time": time.Date(2026, 3, 19, 15, 0, 0, 0, time.UTC), "severity": "medium"},
		{"tenant_id": "tenant-a", "event_time": time.Date(2026, 3, 19, 20, 0, 0, 0, time.UTC), "severity": "low"},
		{"tenant_id": "tenant-b", "event_time": time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC), "severity": "high"},
	}

	ing := db.NewIngester("threat_events", schema, nil, ingest.Config{MaxBufferRows: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{
			"compound_ge_midnight",
			"SELECT COUNT(*) as cnt FROM threat_events WHERE tenant_id = 'tenant-a' AND event_time >= '2026-03-19T00:00:00Z'",
			3,
		},
		{
			"compound_le_2300",
			"SELECT COUNT(*) as cnt FROM threat_events WHERE tenant_id = 'tenant-a' AND event_time <= '2026-03-19T23:00:00Z'",
			3,
		},
		{
			"compound_range",
			"SELECT COUNT(*) as cnt FROM threat_events WHERE tenant_id = 'tenant-a' AND event_time >= '2026-03-19T10:00:00Z' AND event_time <= '2026-03-19T15:00:00Z'",
			2,
		},
		{
			"tenant_b_only",
			"SELECT COUNT(*) as cnt FROM threat_events WHERE tenant_id = 'tenant-b' AND event_time >= '2026-03-19T00:00:00Z'",
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := db.Query(ctx, tt.query)
			if err != nil {
				t.Fatal(err)
			}
			cnt, ok := res.Rows[0]["cnt"].(int64)
			if !ok || cnt != tt.expected {
				t.Errorf("expected %d rows, got %v", tt.expected, res.Rows[0]["cnt"])
			}
		})
	}
}
