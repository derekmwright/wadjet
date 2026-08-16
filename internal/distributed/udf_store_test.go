package distributed

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/expr"
)

func setupUDFStore(t *testing.T) (context.Context, *NATSUDFStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("ConnectInProcess: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	localStore := expr.NewUDFStore()
	udfStore, err := NewNATSUDFStore(ctx, js, localStore, logger)
	if err != nil {
		t.Fatalf("NewNATSUDFStore: %v", err)
	}

	return ctx, udfStore
}

func TestNewNATSUDFStore(t *testing.T) {
	ctx, store := setupUDFStore(t)
	_ = ctx
	if store == nil {
		t.Fatal("expected non-nil UDF store")
	}
}

func TestNewNATSUDFStoreNilLogger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	localStore := expr.NewUDFStore()
	store, err := NewNATSUDFStore(ctx, js, localStore, nil)
	if err != nil {
		t.Fatalf("NewNATSUDFStore with nil logger: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestUDFStoreCreateAndList(t *testing.T) {
	ctx, store := setupUDFStore(t)

	def := expr.UDFDef{
		Name:   "double",
		Params: []string{"x"},
		Body:   "x * 2",
		Owner:  "admin",
	}
	err := store.Create(ctx, def, true)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	udfs := store.List()
	if len(udfs) == 0 {
		t.Fatal("expected at least 1 UDF after create")
	}

	found := false
	for _, u := range udfs {
		if u.Name == "double" {
			found = true
			if u.Owner != "admin" {
				t.Errorf("Owner: got %q, want %q", u.Owner, "admin")
			}
		}
	}
	if !found {
		t.Error("expected to find 'double' UDF in list")
	}
}

func TestUDFStoreCreateAndDelete(t *testing.T) {
	ctx, store := setupUDFStore(t)

	def := expr.UDFDef{
		Name:   "triple",
		Params: []string{"x"},
		Body:   "x * 3",
		Owner:  "admin",
	}
	if err := store.Create(ctx, def, true); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Verify it exists
	udfs := store.List()
	found := false
	for _, u := range udfs {
		if u.Name == "triple" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected to find 'triple' UDF")
	}

	// Delete it
	if err := store.Delete(ctx, "triple", "admin", true); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify it's gone from local store
	udfs = store.List()
	for _, u := range udfs {
		if u.Name == "triple" {
			t.Error("expected 'triple' UDF to be deleted")
		}
	}
}

func TestUDFStoreLoadAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	// Create store #1 and add a UDF
	localStore1 := expr.NewUDFStore()
	store1, err := NewNATSUDFStore(ctx, js, localStore1, logger)
	if err != nil {
		t.Fatal(err)
	}

	def := expr.UDFDef{
		Name:   "add_one",
		Params: []string{"x"},
		Body:   "x + 1",
		Owner:  "user1",
	}
	if err := store1.Create(ctx, def, true); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Create store #2 with a fresh local store and load all
	localStore2 := expr.NewUDFStore()
	store2, err := NewNATSUDFStore(ctx, js, localStore2, logger)
	if err != nil {
		t.Fatal(err)
	}

	if err := store2.LoadAll(ctx); err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	// store2 should now have the UDF
	udfs := store2.List()
	found := false
	for _, u := range udfs {
		if u.Name == "add_one" {
			found = true
		}
	}
	if !found {
		t.Error("expected LoadAll to load 'add_one' UDF")
	}
}

func TestUDFStoreLoadAllEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	localStore := expr.NewUDFStore()
	store, err := NewNATSUDFStore(ctx, js, localStore, nil)
	if err != nil {
		t.Fatal(err)
	}

	// LoadAll on empty bucket should not error
	if err := store.LoadAll(ctx); err != nil {
		t.Fatalf("LoadAll on empty: %v", err)
	}
}

func TestUDFStoreWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	// Create two stores sharing the same KV bucket
	localStore1 := expr.NewUDFStore()
	store1, err := NewNATSUDFStore(ctx, js, localStore1, logger)
	if err != nil {
		t.Fatal(err)
	}

	localStore2 := expr.NewUDFStore()
	store2, err := NewNATSUDFStore(ctx, js, localStore2, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Start watching on store2
	stopWatch, err := store2.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stopWatch()

	// Create a UDF via store1
	def := expr.UDFDef{
		Name:   "watched_func",
		Params: []string{"x"},
		Body:   "x + 10",
		Owner:  "user1",
	}
	if err := store1.Create(ctx, def, true); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Give the watcher time to propagate
	time.Sleep(500 * time.Millisecond)

	// store2 should have picked up the new UDF via the watch
	udfs := store2.List()
	found := false
	for _, u := range udfs {
		if u.Name == "watched_func" {
			found = true
		}
	}
	if !found {
		t.Error("expected watcher to propagate 'watched_func' to store2")
	}
}
