package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// getCountingStore counts Get calls so the test can prove the second scan
// pass never reached S3.
type getCountingStore struct {
	objstore.Store
	gets atomic.Int64
}

func (s *getCountingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, bucket, key)
}

// TestBaseTableCacheTier_SecondPassServesWithoutGET: with the base-table
// cache enabled, the first scan pass populates the cache (via the miss
// tee) and the second pass must be served entirely from local disk — the
// prefetcher skips resident files (HasCachedPath probe) and openNextFile
// mmaps the cache files in place, which also means Close must NOT unlink
// them (the cache owns its files; docs/design/base-table-nvme-cache.md §7).
func TestBaseTableCacheTier_SecondPassServesWithoutGET(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}}
	inner := &getCountingStore{Store: objstore.NewMemStore()}
	ctx := context.Background()
	if err := inner.MakeBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	var files []string
	const rowsPerFile = 100
	for f := 0; f < 3; f++ {
		var buf bytes.Buffer
		w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(f*rowsPerFile + i)}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/t/chunk_%d.parquet", f)
		if _, err := inner.Put(ctx, "b", path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), ""); err != nil {
			t.Fatal(err)
		}
		files = append(files, path)
	}

	cacheDir := t.TempDir()
	btc, err := objstore.NewBaseTableCache(inner, cacheDir, 1<<30, nil)
	if err != nil {
		t.Fatal(err)
	}
	ex := &Executor{store: btc, spillDir: t.TempDir()}

	scanAll := func() int {
		t.Helper()
		src := newCachedFileStreamSource(ex, "", "b", files)
		if err := src.Init(ctx); err != nil {
			t.Fatal(err)
		}
		rows := 0
		for {
			b, err := src.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			rows += b.Len
		}
		if err := src.Close(); err != nil {
			t.Fatal(err)
		}
		if src.mmapData != nil || src.localPath != "" {
			t.Fatal("mmap/localPath still held after Close")
		}
		return rows
	}

	if rows := scanAll(); rows != 3*rowsPerFile {
		t.Fatalf("cold pass rows = %d, want %d", rows, 3*rowsPerFile)
	}
	coldGets := inner.gets.Load()
	if coldGets == 0 {
		t.Fatal("cold pass issued no inner Gets")
	}
	if s := btc.Stats(); s.Entries != 3 {
		t.Fatalf("cold pass cached %d entries, want 3 (stats %+v)", s.Entries, s)
	}

	if rows := scanAll(); rows != 3*rowsPerFile {
		t.Fatalf("warm pass rows = %d, want %d", rows, 3*rowsPerFile)
	}
	if got := inner.gets.Load(); got != coldGets {
		t.Fatalf("warm pass reached the inner store: gets %d -> %d", coldGets, got)
	}
	if s := btc.Stats(); s.Hits < 3 {
		t.Fatalf("warm pass hits = %d, want >= 3 (stats %+v)", s.Hits, s)
	}

	// The warm pass mmap'd the cache's files in place — they must survive
	// the source Close (only LRU eviction may unlink them).
	survivors := 0
	if err := filepath.Walk(filepath.Join(cacheDir, "b"), func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			survivors++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if survivors != 3 {
		t.Fatalf("%d cache files survive after warm pass, want 3", survivors)
	}
}
