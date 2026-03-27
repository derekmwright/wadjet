package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// failStore always returns an error on Get.
type failStore struct {
	*MemStore
	failCount int
	calls     int
}

func (f *failStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	f.calls++
	if f.calls <= f.failCount {
		return nil, ObjectInfo{}, errors.New("simulated S3 timeout")
	}
	return f.MemStore.Get(ctx, bucket, key)
}

func TestCircuitStore_ClosedNormal(t *testing.T) {
	mem := NewMemStore()
	ctx := context.Background()
	mem.MakeBucket(ctx, "b")
	mem.Put(ctx, "b", "k", bytes.NewReader([]byte("hello")), 5, "text/plain")

	cs := NewCircuitStore(mem, DefaultCircuitConfig(), nil)
	if cs.State() != CircuitClosed {
		t.Fatalf("expected closed, got %s", cs.State())
	}

	rc, _, err := cs.Get(ctx, "b", "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "hello" {
		t.Errorf("got %q, want %q", data, "hello")
	}
}

func TestCircuitStore_OpensAfterThreshold(t *testing.T) {
	fs := &failStore{MemStore: NewMemStore(), failCount: 100}
	ctx := context.Background()
	fs.MakeBucket(ctx, "b")

	cfg := DefaultCircuitConfig()
	cfg.FailureThreshold = 3
	cfg.RequestTimeout = time.Second
	cs := NewCircuitStore(fs, cfg, nil)

	// First 3 calls should fail normally
	for i := 0; i < 3; i++ {
		_, _, err := cs.Get(ctx, "b", "nonexistent")
		if err == nil {
			t.Fatalf("call %d: expected error", i)
		}
	}

	// Circuit should now be open
	if cs.State() != CircuitOpen {
		t.Fatalf("expected open after %d failures, got %s", cfg.FailureThreshold, cs.State())
	}

	// Next call should return ErrCircuitOpen immediately
	_, _, err := cs.Get(ctx, "b", "k")
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitStore_HalfOpenRecovery(t *testing.T) {
	fs := &failStore{MemStore: NewMemStore(), failCount: 3}
	ctx := context.Background()
	fs.MakeBucket(ctx, "b")
	fs.MemStore.Put(ctx, "b", "k", bytes.NewReader([]byte("data")), 4, "text/plain")

	cfg := DefaultCircuitConfig()
	cfg.FailureThreshold = 3
	cfg.ResetTimeout = 50 * time.Millisecond
	cfg.RequestTimeout = time.Second
	cs := NewCircuitStore(fs, cfg, nil)

	// Trigger circuit open
	for i := 0; i < 3; i++ {
		cs.Get(ctx, "b", "nonexistent")
	}
	if cs.State() != CircuitOpen {
		t.Fatalf("expected open, got %s", cs.State())
	}

	// Wait for reset timeout
	time.Sleep(60 * time.Millisecond)

	// Next call should be half-open probe — failStore now returns success
	rc, _, err := cs.Get(ctx, "b", "k")
	if err != nil {
		t.Fatalf("half-open probe: %v", err)
	}
	rc.Close()

	// Circuit should be closed again
	if cs.State() != CircuitClosed {
		t.Fatalf("expected closed after successful probe, got %s", cs.State())
	}
}

func TestCircuitStore_StateString(t *testing.T) {
	tests := map[CircuitState]string{
		CircuitClosed:   "closed",
		CircuitOpen:     "open",
		CircuitHalfOpen: "half-open",
	}
	for state, want := range tests {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", state, got, want)
		}
	}
}
