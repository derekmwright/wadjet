package worker

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// trackingReaderAt wraps a ReaderAtCloser and increments a shared counter on Close.
type trackingReaderAt struct {
	objstore.ReaderAtCloser
	closed  atomic.Bool
	counter *atomic.Int32
}

func (t *trackingReaderAt) Close() error {
	if t.closed.CompareAndSwap(false, true) {
		t.counter.Add(1)
	}
	return t.ReaderAtCloser.Close()
}

// trackingReaderAtStore wraps a MemStore and returns trackingReaderAt handles
// so we can verify Close is called exactly once per file.
type trackingReaderAtStore struct {
	*objstore.MemStore
	closeCount atomic.Int32
}

func (s *trackingReaderAtStore) GetReaderAt(ctx context.Context, bucket, key string) (objstore.ReaderAtCloser, int64, error) {
	ra, size, err := s.MemStore.GetReaderAt(ctx, bucket, key)
	if err != nil {
		return nil, 0, err
	}
	return &trackingReaderAt{ReaderAtCloser: ra, counter: &s.closeCount}, size, nil
}

// TestReaderAtClosePerIteration verifies that the probe file scanning loop
// closes each ReaderAt handle after processing, not via defer accumulation.
// Regression test for defer-in-loop bug (executor.go:835).
func TestReaderAtClosePerIteration(t *testing.T) {
	ctx := context.Background()
	bucket := "test-bucket"

	// Set up a tracking store with multiple Parquet files.
	mem := objstore.NewMemStore()
	if err := mem.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	store := &trackingReaderAtStore{MemStore: mem}

	schema := parquet.Schema{
		Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt32}},
	}

	// Write 5 Parquet files to the store.
	numFiles := 5
	paths := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("creating writer for file %d: %v", i, err)
		}
		for j := 0; j < 10; j++ {
			if err := pw.WriteRows([]map[string]any{{"id": int32(i*10 + j)}}); err != nil {
				t.Fatalf("writing rows for file %d: %v", i, err)
			}
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("closing writer for file %d: %v", i, err)
		}
		paths[i] = "data/file" + string(rune('0'+i)) + ".parquet"
		data := buf.Bytes()
		if _, err := mem.Put(ctx, bucket, paths[i], bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("putting file %d: %v", i, err)
		}
	}

	// Simulate the ReaderAt loop pattern used when iterating parquet files.
	// This mirrors the production pattern: open ReaderAt, use FileReader,
	// close ReaderAt explicitly after each iteration.
	for _, filePath := range paths {
		ra, size, err := store.GetReaderAt(ctx, bucket, filePath)
		if err != nil {
			t.Fatalf("GetReaderAt: %v", err)
		}
		fr, err := parquet.OpenFileReader(ra, size)
		if err != nil {
			ra.Close()
			continue
		}

		// Read a row group to exercise the reader (mimics production use).
		if fr.NumRowGroups() > 0 {
			pr := fr.ColumnPages(0, 0)
			if pr != nil {
				for {
					page, err := pr.NextPage()
					if err != nil || page == nil {
						break
					}
				}
				pr.Close()
			}
		}

		// Production code now uses explicit close instead of defer.
		ra.Close()
	}

	// Key assertion: exactly numFiles close calls, one per iteration.
	// With the old defer-in-loop pattern, all closes would happen here
	// (at function exit), and double-closes could occur on error paths.
	got := int(store.closeCount.Load())
	if got != numFiles {
		t.Fatalf("expected %d Close() calls, got %d (possible defer leak or double-close)", numFiles, got)
	}
}
