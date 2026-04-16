package alerts

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

func TestEnsureHistoryTableIdempotent(t *testing.T) {
	ctx := context.Background()
	kv := catalog.NewMemKV()
	store := objstore.NewMemStore()
	cat := catalog.NewWithCluster(kv, store, "test-bucket", "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := EnsureHistoryTable(ctx, cat); err != nil {
		t.Fatal(err)
	}
	if err := EnsureHistoryTable(ctx, cat); err != nil {
		t.Fatalf("second call should be a no-op, got %v", err)
	}
	tm, err := cat.GetTable(ctx, HistoryTableName)
	if err != nil {
		t.Fatalf("alert_history not found: %v", err)
	}
	wantCols := []string{"fired_at", "alert_name", "row_count", "delivery_status"}
	have := make(map[string]bool)
	for _, c := range tm.Schema.Columns {
		have[c.Name] = true
	}
	for _, w := range wantCols {
		if !have[w] {
			t.Errorf("schema missing column %q", w)
		}
	}
}
