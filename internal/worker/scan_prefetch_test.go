package worker

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// writePrefetchFixture writes n small parquet files ("id" int64, "val"
// int64) to store and returns the keys plus the expected sum of "val"
// across all files.
func writePrefetchFixture(t *testing.T, store *objstore.MemStore, bucket string, n, rowsPerFile int) ([]string, int64) {
	t.Helper()
	if err := store.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, n)
	var wantSum int64
	rowID := 0
	for f := 0; f < n; f++ {
		rows := make([]map[string]any, 0, rowsPerFile)
		for r := 0; r < rowsPerFile; r++ {
			v := int64(rowID * 3)
			rows = append(rows, map[string]any{"id": int64(rowID), "val": v})
			wantSum += v
			rowID++
		}
		key := fmt.Sprintf("tables/t/part-%04d.parquet", f)
		writeParquetFile(t, store, bucket, key, rows)
		keys = append(keys, key)
	}
	return keys, wantSum
}

// drainValSum reads the source to exhaustion and sums the "val" column.
func drainValSum(t *testing.T, ctx context.Context, src *cachedFileStreamSource) (sum int64, rows int) {
	t.Helper()
	valIdx := -1
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			return sum, rows
		}
		if valIdx < 0 {
			for i, c := range b.Schema {
				if c.Name == "val" {
					valIdx = i
					break
				}
			}
			if valIdx < 0 {
				t.Fatalf("val column not in schema %v", b.Schema)
			}
		}
		for i := 0; i < b.Len; i++ {
			sum += b.Columns[valIdx].Int64Data[i]
			rows++
		}
	}
}

// assertNoPrefetchTemps fails if any scan-prefetch temp files remain in dir.
func assertNoPrefetchTemps(t *testing.T, dir string) {
	t.Helper()
	leftover, err := filepath.Glob(filepath.Join(dir, "scan-prefetch-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftover) > 0 {
		t.Fatalf("leaked prefetch temp files: %v", leftover)
	}
}

// TestScanPrefetchEndToEnd verifies the prefetched path yields identical
// data to the serial path over a multi-file parquet input.
func TestScanPrefetchEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	keys, wantSum := writePrefetchFixture(t, store, bucket, 9, 40)

	spill := t.TempDir()
	e := &Executor{store: store, spillDir: spill}
	src := newCachedFileStreamSource(e, "", bucket, keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	sum, rows := drainValSum(t, ctx, src)
	if src.prefetch == nil {
		t.Fatal("prefetcher did not engage on a multi-file parquet input")
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if sum != wantSum || rows != 9*40 {
		t.Fatalf("prefetched read: sum=%d rows=%d, want sum=%d rows=%d", sum, rows, wantSum, 9*40)
	}
	assertNoPrefetchTemps(t, spill)

	// Serial control: same input with prefetch disabled.
	e2 := &Executor{store: store, spillDir: t.TempDir()}
	src2 := newCachedFileStreamSource(e2, "", bucket, keys)
	src2.prefetchDisabled = true
	if err := src2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src2.Close()
	sum2, rows2 := drainValSum(t, ctx, src2)
	if src2.prefetch != nil {
		t.Fatal("prefetcher engaged despite prefetchDisabled")
	}
	if sum2 != sum || rows2 != rows {
		t.Fatalf("serial control disagrees: sum=%d rows=%d vs prefetched sum=%d rows=%d", sum2, rows2, sum, rows)
	}
}

// TestScanPrefetchMissingObjectFallsThrough verifies that a prefetch miss
// falls back to the tiered path, whose error (naming the file) is
// authoritative.
func TestScanPrefetchMissingObjectFallsThrough(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	keys, _ := writePrefetchFixture(t, store, bucket, 3, 10)
	keys = append(keys, "tables/t/part-9999.parquet") // never written

	e := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(e, "", bucket, keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for {
		b, err := src.Next(ctx)
		if err != nil {
			if !strings.Contains(err.Error(), "part-9999.parquet") {
				t.Fatalf("error does not name the missing file: %v", err)
			}
			return
		}
		if b == nil {
			t.Fatal("source exhausted without surfacing the missing-object error")
		}
	}
}

// TestScanPrefetchCloseReapsTemps verifies that closing a source
// mid-stream cancels the download workers and removes every temp file
// they produced.
func TestScanPrefetchCloseReapsTemps(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	keys, _ := writePrefetchFixture(t, store, bucket, 8, 50)

	spill := t.TempDir()
	e := &Executor{store: store, spillDir: spill}
	src := newCachedFileStreamSource(e, "", bucket, keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Read a single batch so the prefetcher is running with most files
	// still ahead, then abandon the source.
	if b, err := src.Next(ctx); err != nil || b == nil {
		t.Fatalf("first Next: b=%v err=%v", b, err)
	}
	if src.prefetch == nil {
		t.Fatal("prefetcher did not engage")
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	assertNoPrefetchTemps(t, spill)
}

// gateCountingStore wraps a Store and tracks the maximum number of
// concurrent Get calls.
type gateCountingStore struct {
	objstore.Store
	mu      sync.Mutex
	current int
	max     int
}

func (g *gateCountingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	g.mu.Lock()
	g.current++
	if g.current > g.max {
		g.max = g.current
	}
	g.mu.Unlock()
	rc, info, err := g.Store.Get(ctx, bucket, key)
	if err != nil {
		g.mu.Lock()
		g.current--
		g.mu.Unlock()
		return nil, info, err
	}
	return &gateClosingReader{ReadCloser: rc, g: g}, info, nil
}

type gateClosingReader struct {
	io.ReadCloser
	g    *gateCountingStore
	once sync.Once
}

func (r *gateClosingReader) Close() error {
	r.once.Do(func() {
		r.g.mu.Lock()
		r.g.current--
		r.g.mu.Unlock()
	})
	return r.ReadCloser.Close()
}

// TestScanPrefetchConcurrencyBound verifies concurrent GETs never exceed
// scanPrefetchConcurrency (plus the tiered path's own synchronous Get,
// which cannot overlap itself).
func TestScanPrefetchConcurrencyBound(t *testing.T) {
	ctx := context.Background()
	mem := objstore.NewMemStore()
	const bucket = "test"
	keys, wantSum := writePrefetchFixture(t, mem, bucket, 16, 30)
	store := &gateCountingStore{Store: mem}

	e := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(e, "", bucket, keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	sum, _ := drainValSum(t, ctx, src)
	if sum != wantSum {
		t.Fatalf("sum=%d want %d", sum, wantSum)
	}
	// +1: openNextFile's own fallback Get could in principle overlap the
	// workers, though with all prefetches succeeding it shouldn't fire.
	if store.max > scanPrefetchConcurrency+1 {
		t.Fatalf("max concurrent Gets = %d, want <= %d", store.max, scanPrefetchConcurrency+1)
	}
	if store.max < 2 {
		t.Fatalf("max concurrent Gets = %d — prefetch never overlapped downloads", store.max)
	}
}

// TestScanPrefetchTinyWindowNoDeadlock forces every admission through the
// lowest-unconsumed-index bypass (window smaller than any file) and
// verifies the pipeline still completes. Guards the out-of-order
// window-fill deadlock the bypass exists for.
func TestScanPrefetchTinyWindowNoDeadlock(t *testing.T) {
	oldWin := scanPrefetchWindowBytes
	scanPrefetchWindowBytes = 1
	defer func() { scanPrefetchWindowBytes = oldWin }()

	// A deadline on the drain context turns a deadlock into a test
	// failure instead of a suite hang: take()/acquireWindow both abort
	// on ctx cancellation. (Draining on a goroutine isn't an option —
	// drainValSum uses t.Fatalf, which must run on the test goroutine.)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := objstore.NewMemStore()
	const bucket = "test"
	keys, wantSum := writePrefetchFixture(t, store, bucket, 12, 25)

	e := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(e, "", bucket, keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	sum, _ := drainValSum(t, ctx, src)
	if sum != wantSum {
		t.Fatalf("sum=%d want %d", sum, wantSum)
	}
}
