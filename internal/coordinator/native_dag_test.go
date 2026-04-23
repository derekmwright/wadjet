package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestNativeDAG_SimpleAggregate runs a small query end-to-end through the
// Phase 3 native-DAG executor (UseNativeDAG=true) and verifies correctness
// against the same query on the legacy path. Smoke test for Commit 4 wiring:
// EnsureDistribution emits Exchange stages, executeStageDAG dispatches them
// via the new TaskTypeStage path, gather receives results.
func TestNativeDAG_SimpleAggregate(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"k": int64(1), "v": int64(10)},
		{"k": int64(1), "v": int64(20)},
		{"k": int64(2), "v": int64(30)},
		{"k": int64(2), "v": int64(40)},
		{"k": int64(3), "v": int64(50)},
	}
	ingestTestData(t, ctx, store, cat, "simple", schema, rows)

	sql := "SELECT k, SUM(v) AS total FROM simple GROUP BY k"

	// Baseline: legacy path.
	legacyRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("legacy ExecuteSQL: %v", err)
	}
	if legacyRes.TotalRows != 3 {
		t.Fatalf("legacy: expected 3 groups, got %d", legacyRes.TotalRows)
	}

	// Native DAG.
	coord.UseNativeDAG = true
	defer func() { coord.UseNativeDAG = false }()
	natRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("native DAG ExecuteSQL: %v", err)
	}
	if natRes.TotalRows != legacyRes.TotalRows {
		t.Fatalf("native DAG rows %d != legacy %d", natRes.TotalRows, legacyRes.TotalRows)
	}
}

// TestNativeDAG_Join exercises a hash_join compute stage end-to-end by
// running a two-table INNER JOIN through the native DAG executor.
func TestNativeDAG_Join(t *testing.T) {
	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	usersSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	users := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
		{"id": int64(3), "name": "carol"},
	}
	ingestTestData(t, ctx, store, cat, "users", usersSchema, users)

	ordersSchema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}
	orders := []map[string]any{
		{"user_id": int64(1), "amount": int64(100)},
		{"user_id": int64(2), "amount": int64(200)},
		{"user_id": int64(1), "amount": int64(50)},
	}
	ingestTestData(t, ctx, store, cat, "orders", ordersSchema, orders)

	sql := "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"

	legacyRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("legacy ExecuteSQL: %v", err)
	}

	coord.UseNativeDAG = true
	defer func() { coord.UseNativeDAG = false }()
	natRes, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("native DAG ExecuteSQL: %v", err)
	}
	if natRes.TotalRows != legacyRes.TotalRows {
		t.Fatalf("native DAG rows %d != legacy %d", natRes.TotalRows, legacyRes.TotalRows)
	}
	if natRes.TotalRows != 3 {
		t.Fatalf("expected 3 joined rows, got %d", natRes.TotalRows)
	}
}
