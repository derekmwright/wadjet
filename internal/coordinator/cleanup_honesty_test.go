package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// blockedDeleteStore fails every Delete with the error a slow S3 produces
// once the caller's deadline has expired, and counts how many reached it.
type blockedDeleteStore struct {
	*objstore.MemStore
	err     error
	deletes int
}

func (s *blockedDeleteStore) Delete(ctx context.Context, bucket, key string) error {
	s.deletes++
	if s.err != nil {
		return fmt.Errorf("deleting object: %w", s.err)
	}
	return s.MemStore.Delete(ctx, bucket, key)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestCleanQueryReportsWhatItFailedToReclaim is #820. The loop logged every
// failure and returned nil, so a caller was told a cleanup that deleted
// nothing had succeeded — and nothing retried, so the prefix leaked to the
// TTL sweep. The return value has to name what remains.
func TestCleanQueryReportsWhatItFailedToReclaim(t *testing.T) {
	inner := &blockedDeleteStore{MemStore: objstore.NewMemStore()}
	ctx := context.Background()
	if err := inner.MakeBucket(ctx, "results"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		putObject(t, inner.MemStore, "results", fmt.Sprintf("queries/q1/stage-0/part-%04d", i))
	}
	inner.err = context.DeadlineExceeded

	rc := NewResultCleaner(inner, "results", 0, quietLogger())
	deleted, err := rc.CleanQuery(ctx, "q1")
	if err == nil {
		t.Fatalf("CleanQuery deleted %d of 6 objects and returned nil: the caller is told a cleanup that reclaimed nothing succeeded", deleted)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0", deleted)
	}
	if !strings.Contains(err.Error(), "6 of 6 objects remain") {
		t.Fatalf("error does not name what remains: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error does not carry the cause: %v", err)
	}
}

// TestCleanQueryStopsAtTheFirstContextError is the other half of #820 and
// the producer half of #798: once the caller's deadline has expired, every
// remaining delete returns DeadlineExceeded instantly. Continuing the loop
// manufactures N-1 more consecutive breaker failures out of one slow
// cleanup — the burst that opened a process-wide breaker in a single call.
func TestCleanQueryStopsAtTheFirstContextError(t *testing.T) {
	inner := &blockedDeleteStore{MemStore: objstore.NewMemStore()}
	base := context.Background()
	if err := inner.MakeBucket(base, "results"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		putObject(t, inner.MemStore, "results", fmt.Sprintf("queries/q2/stage-0/part-%04d", i))
	}

	ctx, cancel := context.WithCancel(base)
	rc := NewResultCleaner(inner, "results", 0, quietLogger())
	// Cancel after the List, before any delete: the shape of a cleanup whose
	// 30 s deadline expires while the store is slow.
	cancel()

	deleted, err := rc.CleanQuery(ctx, "q2")
	if err == nil {
		t.Fatal("CleanQuery returned nil after the context was cancelled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0", deleted)
	}
	if inner.deletes != 0 {
		t.Fatalf("%d deletes were dispatched after the context expired; each one is a manufactured breaker failure", inner.deletes)
	}
}

// TestCleanQuerySucceedsQuietly keeps the happy path honest: a cleanup that
// reclaims everything still returns (n, nil).
func TestCleanQuerySucceedsQuietly(t *testing.T) {
	store := testStore(t, "results")
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		putObject(t, store, "results", fmt.Sprintf("queries/q3/stage-0/part-%d", i))
	}
	rc := NewResultCleaner(store, "results", 0, quietLogger())
	n, err := rc.CleanQuery(ctx, "q3")
	if err != nil {
		t.Fatalf("CleanQuery: %v", err)
	}
	if n != 3 {
		t.Fatalf("deleted = %d, want 3", n)
	}
	left, err := store.List(ctx, "results", objstore.ListOptions{Prefix: "queries/q3/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("%d objects survived a successful CleanQuery", len(left))
	}
}
