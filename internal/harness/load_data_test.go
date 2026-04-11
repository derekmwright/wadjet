package harness

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// TestLoadSampleDataPopulatesCatalog verifies that loadSampleData writes
// parquet files to the FileStore and registers all 8 TPC-H tables in the
// NATS KV catalog.
func TestLoadSampleDataPopulatesCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("requires embedded NATS, skip with -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Start embedded NATS.
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	defer embedded.Shutdown()

	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}

	// Create a fake cluster with just the embedded NATS (no processes).
	dataDir := filepath.Join(t.TempDir(), "data")
	cluster := &Cluster{
		embeddedNATS: embedded,
		natsURL:      embedded.ClientURL(),
	}

	sliceCfg := SliceConfigs[SliceSmall]
	if err := loadSampleData(ctx, cluster, dataDir, sliceCfg, logger); err != nil {
		t.Fatalf("loadSampleData: %v", err)
	}

	// Verify: reconnect and check the catalog has all 8 tables.
	nc2, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		t.Fatalf("reconnecting: %v", err)
	}
	defer nc2.Close()

	js2, err := distributed.NewJetStream(nc2)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	kv, err := catalog.NewNATSKV(js2)
	if err != nil {
		t.Fatalf("NATS KV: %v", err)
	}
	store, err := objstore.NewFileStore(dataDir)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}
	cat := catalog.New(kv, store, "wadjet")

	tables, err := cat.ListTables(ctx)
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}

	expected := map[string]bool{
		"region": true, "nation": true, "supplier": true, "part": true,
		"partsupp": true, "customer": true, "orders": true, "lineitem": true,
	}
	if len(tables) != len(expected) {
		t.Errorf("want %d tables, got %d: %v", len(expected), len(tables), tables)
	}
	for _, name := range tables {
		if !expected[name] {
			t.Errorf("unexpected table %q", name)
		}
	}

	// Verify parquet files exist on disk.
	matches, err := filepath.Glob(filepath.Join(dataDir, "wadjet", "tables", "*", "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Error("no parquet files found in data dir")
	}
	t.Logf("loaded %d tables, %d parquet files", len(tables), len(matches))
}
