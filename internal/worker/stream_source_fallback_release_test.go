package worker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestFallbackPath_ReleasesMmapAndTempPerFile: the Array/Map eager-decode
// fallback used to leak the file's mmap + spill temp on every multi-file
// advance (openNextFile overwrote mmapData/localPath without release) —
// N fallback files leaked N-1 mmaps and temp files in the worker-lifetime
// spill dir for the process lifetime.
func TestFallbackPath_ReleasesMmapAndTempPerFile(t *testing.T) {
	elem := parquet.Column{Name: "element", Type: parquet.TypeInt64}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "tags", Type: parquet.TypeArray, ElementType: &elem},
	}}

	store := objstore.NewMemStore()
	ctx := context.Background()
	if err := store.MakeBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	var files []string
	for f := 0; f < 3; f++ {
		var buf bytes.Buffer
		w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]map[string]any, 5)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(f*10 + i), "tags": []any{int64(i)}}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("q/file%d.parquet", f)
		if _, err := store.Put(ctx, "b", path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), ""); err != nil {
			t.Fatal(err)
		}
		files = append(files, path)
	}

	spillDir := t.TempDir()
	ex := &Executor{store: store, spillDir: spillDir}
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
		// The fallback path must never hold more than ONE file's temp at a
		// time — the leak left every prior file's temp behind. Download-ahead
		// temps (scan-prefetch-*) are excluded: the prefetcher holds a
		// bounded set by design, and the after-Close check below still
		// requires ALL of them gone.
		if n := countFilesExcluding(t, spillDir, "scan-prefetch-"); n > 1 {
			t.Fatalf("%d temp files held mid-stream, want <=1 (per-file release missing)", n)
		}
	}
	if rows != 15 {
		t.Fatalf("rows = %d, want 15", rows)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if src.mmapData != nil || src.localPath != "" {
		t.Fatal("mmap/localPath still held after Close")
	}
	if n := countFiles(t, spillDir); n != 0 {
		t.Fatalf("%d temp files leaked after Close, want 0", n)
	}
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	return countFilesExcluding(t, dir, "")
}

// countFilesExcluding counts regular files under dir, skipping names with
// the given prefix ("" excludes nothing).
func countFilesExcluding(t *testing.T, dir, excludePrefix string) int {
	t.Helper()
	n := 0
	if err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if excludePrefix != "" && strings.HasPrefix(info.Name(), excludePrefix) {
				return nil
			}
			n++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}
