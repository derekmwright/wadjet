package config

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatcherDetectsChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wadjet.yaml")

	initial := `
mode: standalone
worker:
  max_concurrent: 4
`
	if err := os.WriteFile(path, []byte(initial), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)

	var reloaded atomic.Int32
	m.Subscribe(func(_ ChangeEvent) {
		reloaded.Add(1)
	})

	watcher := NewWatcher(WatcherConfig{
		Path:     path,
		Interval: 100 * time.Millisecond,
		Debounce: 50 * time.Millisecond,
	}, m, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watcher.Watch(ctx)

	// Wait for watcher to start
	time.Sleep(200 * time.Millisecond)

	// Write updated config
	updated := `
mode: standalone
worker:
  max_concurrent: 16
`
	if err := os.WriteFile(path, []byte(updated), 0644); err != nil {
		t.Fatal(err)
	}

	// Wait for reload
	deadline := time.After(3 * time.Second)
	for reloaded.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("watcher did not detect config change within timeout")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	got := m.Current()
	if got.Worker.MaxConcurrent != 16 {
		t.Fatalf("expected max_concurrent=16 after reload, got %d", got.Worker.MaxConcurrent)
	}
}

func TestWatcherStopsOnCancel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wadjet.yaml")
	os.WriteFile(path, []byte("mode: standalone\n"), 0644)

	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)

	watcher := NewWatcher(WatcherConfig{
		Path:     path,
		Interval: 50 * time.Millisecond,
	}, m, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		watcher.Watch(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// Watcher stopped
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not stop after context cancel")
	}
}

func TestWatcherIgnoresMissingFile(t *testing.T) {
	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)

	watcher := NewWatcher(WatcherConfig{
		Path:     "/nonexistent/path.yaml",
		Interval: 50 * time.Millisecond,
	}, m, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Should not panic
	watcher.Watch(ctx)
}
