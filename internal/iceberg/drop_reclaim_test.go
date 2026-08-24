package iceberg

import (
	"bytes"
	"context"
	"testing"
)

// TestReviewRepro_IcebergRefreshDeletesLiveWarehouseFiles is a permanent
// #494 regression, promoted from the adversarial review's reproducer:
// RefreshTable drops and re-creates the catalog table over the SAME
// Iceberg warehouse data files on every metadata refresh. Before the
// live-manifest guard in catalog.Catalog.FlushDroppedTableFiles, DropTable
// scheduling the dropped manifest's exact paths for physical deletion
// meant a background sweep run after the drop grace elapsed deleted the
// files the LIVE, freshly-refreshed table still references.
func TestReviewRepro_IcebergRefreshDeletesLiveWarehouseFiles(t *testing.T) {
	cat, store := setupCatalogTest(t)
	ctx := context.Background()

	seedIcebergTable(t, store, "test-bucket")

	// The actual warehouse data objects the Iceberg manifest points at.
	dataObjects := []string{
		"warehouse/events/data/year=2024/part-00000.parquet",
		"warehouse/events/data/year=2024/part-00001.parquet",
		"warehouse/events/data/year=2025/part-00000.parquet",
	}
	for _, p := range dataObjects {
		if _, err := store.Put(ctx, "test-bucket", p, bytes.NewReader([]byte("PAR1-real-user-data")), 19, "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
	}

	ci := NewCatalogIntegration(cat)
	if _, err := ci.RegisterTable(ctx, "events", "warehouse/events/metadata/v1.metadata.json"); err != nil {
		t.Fatal(err)
	}
	// A routine metadata refresh: drop + re-create + re-register the SAME files.
	if _, err := ci.RefreshTable(ctx, "events", "warehouse/events/metadata/v1.metadata.json"); err != nil {
		t.Fatal(err)
	}

	// The table is live and its manifest references all three files.
	man, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	live := 0
	for _, p := range man.Partitions {
		live += len(p.Files)
	}
	if live != 3 {
		t.Fatalf("live manifest should reference 3 files, got %d", live)
	}

	// The background sweep runs after the grace elapses.
	n := cat.FlushDroppedTableFiles(ctx, 0)
	t.Logf("FlushDroppedTableFiles deleted %d objects", n)

	for _, p := range dataObjects {
		if _, _, err := store.Get(ctx, "test-bucket", p); err != nil {
			t.Errorf("DATA LOSS: live table's Iceberg warehouse file %q was deleted: %v", p, err)
		}
	}
}
