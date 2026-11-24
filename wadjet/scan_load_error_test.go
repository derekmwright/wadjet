// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1710 through the embedded engine: a table's file that the store can no
// longer serve — removed or truncated on disk after the table was written and
// first read, or a Get the store fails — must fail a SELECT with an error
// naming the file. Before the fix, the whole-file load swallowed the error and
// the statement answered without that file's rows, as success.

// getFailStore fails Get (only Get) for one key once armed; GetReaderAt still
// serves the object, so the footer pass sees a good file and only the data
// load fails.
type getFailStore struct {
	objstore.Store
	mu     sync.Mutex
	target string
}

func (s *getFailStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	s.mu.Lock()
	fail := key == s.target
	s.mu.Unlock()
	if fail {
		return nil, objstore.ObjectInfo{}, fmt.Errorf("get %s: injected object-store failure", key)
	}
	return s.Store.Get(ctx, bucket, key)
}

func (s *getFailStore) GetReaderAt(ctx context.Context, bucket, key string) (objstore.ReaderAtCloser, int64, error) {
	return s.Store.(objstore.ReaderAtStore).GetReaderAt(ctx, bucket, key)
}

func (s *getFailStore) StoreID() string { return objstore.StoreID(s.Store) }

func setScanRGBuffers(t *testing.T, on bool) {
	t.Helper()
	for _, tg := range optswitch.All() {
		if tg.Name == "scan-rg-buffers" {
			prev := tg.Set(on)
			t.Cleanup(func() { tg.Set(prev) })
			return
		}
	}
	t.Fatal("no scan-rg-buffers switch")
}

func TestEmbeddedScanLoadErrorFailsTheStatement(t *testing.T) {
	queries := []struct{ sql, want string }{
		{"SELECT count(*), sum(id) FROM t", "[400 79800]"},
		{"SELECT count(*), sum(id) FROM t WHERE id % 2 = 0", "[200 39800]"},
	}
	for _, damage := range []string{"removed", "truncated", "get-error"} {
		// "analyzed" is the default configuration's route to the whole-file
		// load: ANALYZE persists the row-group metadata, so a scan in a fresh
		// process never decodes a footer, finds none cached, and cannot take
		// the row-group load.
		for _, arm := range []string{"rg-buffers", "whole-file", "analyzed"} {
			t.Run(damage+"/"+arm, func(t *testing.T) {
				setScanRGBuffers(t, arm != "whole-file")
				ctx := context.Background()
				dir := t.TempDir()
				fsStore, err := objstore.NewFileStore(dir)
				if err != nil {
					t.Fatal(err)
				}
				var store objstore.Store = fsStore
				// A FileStore serves scans through ReaderAt (staged pread), so
				// a Get failure is injected over a MemStore, whose scans Get.
				gfs := &getFailStore{Store: objstore.NewMemStore()}
				if damage == "get-error" {
					store = gfs
				}
				db, err := Open(ctx, Config{Store: store, Bucket: "test"})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if err := db.CreateTable(ctx, "t", parquet.Schema{Columns: []parquet.Column{
					{Name: "id", Type: parquet.TypeInt64}}}, nil); err != nil {
					t.Fatal(err)
				}
				// Four INSERTs, four files of 100 rows: id 0..399.
				for f := 0; f < 4; f++ {
					var vals []string
					for r := 0; r < 100; r++ {
						vals = append(vals, fmt.Sprintf("(%d)", f*100+r))
					}
					if _, err := db.Execute(ctx, "INSERT INTO t VALUES "+strings.Join(vals, ",")); err != nil {
						t.Fatal(err)
					}
				}
				m, err := db.Catalog().GetManifest(ctx, "t")
				if err != nil {
					t.Fatal(err)
				}
				var keys []string
				for _, p := range m.Partitions {
					for _, f := range p.Files {
						keys = append(keys, f.Path)
					}
				}
				if len(keys) < 2 {
					t.Fatalf("want several files, got %v", keys)
				}
				// The first reads answer in full, and leave every footer cached
				// as a real first query would.
				for _, q := range queries {
					res, err := db.Query(ctx, q.sql)
					if err != nil {
						t.Fatalf("control %s: %v", q.sql, err)
					}
					if got := fmt.Sprint(res.Cells(0)); got != q.want {
						t.Fatalf("control %s = %s, want %s", q.sql, got, q.want)
					}
				}

				if arm == "analyzed" {
					if _, err := db.Query(ctx, "ANALYZE TABLE t"); err != nil {
						t.Fatalf("ANALYZE: %v", err)
					}
					parquet.ResetFooterCache()
				}

				target := keys[len(keys)-1]
				onDisk := filepath.Join(dir, "test", filepath.FromSlash(target))
				switch damage {
				case "removed":
					if err := os.Remove(onDisk); err != nil {
						t.Fatal(err)
					}
				case "truncated":
					data, err := os.ReadFile(onDisk)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(onDisk, data[:len(data)/2], 0o644); err != nil {
						t.Fatal(err)
					}
				case "get-error":
					gfs.mu.Lock()
					gfs.target = target
					gfs.mu.Unlock()
				}

				for _, q := range queries {
					res, err := db.Query(ctx, q.sql)
					if err == nil {
						got := fmt.Sprint(res.Cells(0))
						if got == q.want {
							continue // the damage never reached a byte this read needs
						}
						t.Fatalf("%s answered %s as success (full answer %s): the %s file %s was dropped silently",
							q.sql, got, q.want, damage, target)
					}
					if errors.Is(err, context.Canceled) {
						t.Fatalf("%s: a load error surfaced as a cancellation: %v", q.sql, err)
					}
					if !strings.Contains(err.Error(), target) {
						t.Fatalf("%s: error %q does not name %s", q.sql, err, target)
					}
					t.Logf("%s: %v", q.sql, err)
				}
			})
		}
	}
}
