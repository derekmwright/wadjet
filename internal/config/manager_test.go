package config

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestManagerCurrent(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)

	got := m.Current()
	if got.Mode != "standalone" {
		t.Fatalf("expected mode=standalone, got %s", got.Mode)
	}
}

// A Manager only holds values something consumes: a key with no subscriber
// is preserved on Apply, because the manager is what GET /v1/admin/config
// reports and the process is not running on a value nothing re-reads (#828).
// Every test below that expects a worker.* or parquet.* write to land
// therefore declares the subscriber that would apply it.
func consumeWorkerAndParquet(m *Manager) {
	m.SubscribeKeys([]string{"worker", "parquet"}, func(ChangeEvent) {})
}

func TestManagerApply(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)
	consumeWorkerAndParquet(m)

	newCfg := DefaultConfig()
	newCfg.Worker.MaxConcurrent = 16
	newCfg.Parquet.Compression = "zstd"

	if err := m.Apply(&newCfg); err != nil {
		t.Fatal(err)
	}

	got := m.Current()
	if got.Worker.MaxConcurrent != 16 {
		t.Fatalf("expected max_concurrent=16, got %d", got.Worker.MaxConcurrent)
	}
	// parquet.* is a Rule 11 deferral: no runtime consumer exists to reach,
	// so no subscriber can make it hot-reloadable and Apply preserves it.
	// Reporting "zstd" here would be the manager claiming a codec the
	// writers are not using (#828).
	if got.Parquet.Compression != "snappy" {
		t.Fatalf("expected the deferred parquet.compression to be preserved (snappy), got %s",
			got.Parquet.Compression)
	}
}

func TestManagerApplyPreservesFrozenFields(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HTTP.Addr = ":9090"
	cfg.NATS.Port = 5222
	m := NewManager(&cfg, nil)
	consumeWorkerAndParquet(m)

	newCfg := DefaultConfig()
	newCfg.HTTP.Addr = ":1111" // should be ignored
	newCfg.NATS.Port = 9999    // should be ignored
	newCfg.Worker.MaxConcurrent = 8

	if err := m.Apply(&newCfg); err != nil {
		t.Fatal(err)
	}

	got := m.Current()
	if got.HTTP.Addr != ":9090" {
		t.Fatalf("expected frozen addr=:9090, got %s", got.HTTP.Addr)
	}
	if got.NATS.Port != 5222 {
		t.Fatalf("expected frozen nats port=5222, got %d", got.NATS.Port)
	}
	if got.Worker.MaxConcurrent != 8 {
		t.Fatalf("expected max_concurrent=8, got %d", got.Worker.MaxConcurrent)
	}
}

func TestManagerApplyValidation(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)
	consumeWorkerAndParquet(m)

	bad := DefaultConfig()
	bad.Worker.MaxConcurrent = 0
	if err := m.Apply(&bad); err == nil {
		t.Fatal("expected validation error for max_concurrent=0")
	}

	bad2 := DefaultConfig()
	bad2.Mode = "invalid"
	if err := m.Apply(&bad2); err != nil {
		// Mode is frozen from old config, so this should succeed
		// because Apply preserves Mode from current config
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManagerSubscribe(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)

	var called atomic.Int32
	var lastEvent ChangeEvent
	m.SubscribeKeys([]string{"worker"}, func(event ChangeEvent) {
		called.Add(1)
		lastEvent = event
	})

	newCfg := DefaultConfig()
	newCfg.Worker.MaxConcurrent = 12
	if err := m.Apply(&newCfg); err != nil {
		t.Fatal(err)
	}

	if called.Load() != 1 {
		t.Fatalf("expected subscriber called once, got %d", called.Load())
	}
	if lastEvent.Old.Worker.MaxConcurrent != 4 {
		t.Fatalf("expected old max_concurrent=4, got %d", lastEvent.Old.Worker.MaxConcurrent)
	}
	if lastEvent.New.Worker.MaxConcurrent != 12 {
		t.Fatalf("expected new max_concurrent=12, got %d", lastEvent.New.Worker.MaxConcurrent)
	}
}

func TestManagerReload(t *testing.T) {
	yaml := `
mode: standalone
worker:
  max_concurrent: 32
  cache_bytes: 1073741824
`
	dir := t.TempDir()
	path := filepath.Join(dir, "wadjet.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)
	consumeWorkerAndParquet(m)

	if err := m.Reload(path); err != nil {
		t.Fatal(err)
	}

	got := m.Current()
	if got.Worker.MaxConcurrent != 32 {
		t.Fatalf("expected max_concurrent=32, got %d", got.Worker.MaxConcurrent)
	}
}

func TestManagerMultipleSubscribers(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)

	var count1, count2 atomic.Int32
	m.SubscribeKeys([]string{"worker"}, func(_ ChangeEvent) { count1.Add(1) })
	m.SubscribeKeys([]string{"worker"}, func(_ ChangeEvent) { count2.Add(1) })

	newCfg := DefaultConfig()
	newCfg.Worker.MaxConcurrent = 7
	m.Apply(&newCfg)

	if count1.Load() != 1 || count2.Load() != 1 {
		t.Fatalf("expected both subscribers called once, got %d and %d", count1.Load(), count2.Load())
	}
}
