// SPDX-License-Identifier: MIT

package wadjet_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc EC (#1255): a process killed between the Parquet flush and the
// catalog write leaves EITHER the table with its rows OR the table with no
// rows and an orphan object — never a catalog entry naming a file that is
// not there.
//
// The ingester Puts the object BEFORE it commits the manifest by CAS, so the
// catalog can be behind the store but never ahead of it (ADR-0041 §5). A
// child is killed on both sides of that window and the parent reopens the
// directory through the public API: "before" (the object landed, no commit)
// finds the table, zero rows and exactly ONE orphan object the manifest does
// not name; "after" (FlushAll returned, no Close) finds every row. Both
// assert every manifest path exists on disk; the kernel drops the flock.

const ecCrashEnv = "WADJET_EC_CRASH_MODE"

// ecExitAfterParquetPut is a Store that exits the process the moment a
// Parquet object has been written — the instant BEFORE the manifest commit.
type ecExitAfterParquetPut struct{ objstore.Store }

func (s ecExitAfterParquetPut) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, ct string) (string, error) {
	etag, err := s.Store.Put(ctx, bucket, key, r, size, ct)
	if err == nil && strings.HasSuffix(key, ".parquet") {
		fmt.Fprintln(os.Stderr, "EC-CRASH: parquet landed, exiting before the manifest commit:", key)
		os.Exit(3)
	}
	return etag, err
}

func ecCrashSchema() wadjet.Schema {
	return wadjet.Schema{Columns: []wadjet.Column{
		{Name: "id", Type: wadjet.TypeInt64},
		{Name: "ip", Type: wadjet.TypeIPv4, Nullable: true},
		{Name: "amount", Type: wadjet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
}

// TestECCrashChild is the child. It is a no-op unless WADJET_EC_CRASH_MODE
// is set, which only the parent below does.
func TestECCrashChild(t *testing.T) {
	mode := os.Getenv(ecCrashEnv)
	if mode == "" {
		t.Skip("child only")
	}
	dir := os.Getenv("WADJET_EC_CRASH_DIR")
	ctx := context.Background()
	fs, err := wadjet.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var store objstore.Store = fs
	if mode == "before" {
		store = ecExitAfterParquetPut{fs}
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "wadjet", CatalogDir: filepath.Join(dir, "_catalog")})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "crash", ecCrashSchema(), nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("crash", ecCrashSchema(), nil, wadjet.IngestConfig{MaxBufferRows: 1 << 20, FlushInterval: time.Hour})
	ing.Start()
	rows := make([]map[string]any, 0, 50)
	for i := 0; i < 50; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "ip": fmt.Sprintf("10.0.0.%d", i), "amount": fmt.Sprintf("%d.25", i)})
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	// "after": the manifest is committed; die without Close, without Stop.
	fmt.Fprintln(os.Stderr, "EC-CRASH: manifest committed, exiting without Close")
	os.Exit(3)
}

func TestAKillBetweenFlushAndCatalogWriteNeverLeavesADanglingEntry(t *testing.T) {
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			child := exec.Command(os.Args[0], "-test.run=^TestECCrashChild$", "-test.count=1")
			child.Env = append(os.Environ(), ecCrashEnv+"="+mode, "WADJET_EC_CRASH_DIR="+dir)
			out, err := child.CombinedOutput()
			if err == nil {
				t.Fatalf("the child did not die:\n%s", out)
			}
			if !strings.Contains(string(out), "EC-CRASH:") {
				t.Fatalf("the child died somewhere else:\n%s", out)
			}

			// The survivor: the directory, through the public API.
			ctx := context.Background()
			fs, err := wadjet.NewFileStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			db, err := wadjet.Open(ctx, wadjet.Config{Store: fs, Bucket: "wadjet", CatalogDir: filepath.Join(dir, "_catalog")})
			if err != nil {
				t.Fatalf("reopening after the kill (the lock must have died with the child): %v", err)
			}
			defer db.Close()

			names, err := db.ListTables(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(names) != 1 || names[0] != "crash" {
				t.Fatalf("after the kill ListTables = %v, want [crash]: CreateTable committed before the flush", names)
			}

			// Every path the manifest names exists; the manifest is the set
			// of files the scan will read.
			manifest, err := db.Catalog().GetManifest(ctx, "crash")
			if err != nil {
				t.Fatal(err)
			}
			named := map[string]bool{}
			for _, p := range manifest.Partitions {
				for _, f := range p.Files {
					named[f.Path] = true
					if _, err := os.Stat(filepath.Join(dir, "wadjet", filepath.FromSlash(f.Path))); err != nil {
						t.Errorf("the manifest names %s, which is not on disk: %v", f.Path, err)
					}
				}
			}
			// Every object on disk under the table, minus the named ones.
			var orphans []string
			tableDir := filepath.Join(dir, "wadjet", "tables", "crash")
			filepath.WalkDir(tableDir, func(path string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
					return nil
				}
				rel, _ := filepath.Rel(filepath.Join(dir, "wadjet"), path)
				if !named[filepath.ToSlash(rel)] {
					orphans = append(orphans, rel)
				}
				return nil
			})

			res, err := db.Query(ctx, "SELECT count(*) AS n, count(ip) AS ips FROM crash")
			if err != nil {
				t.Fatalf("the survivor cannot read the table: %v", err)
			}
			got := fmt.Sprintf("%v/%v", res.Rows[0]["n"], res.Rows[0]["ips"])

			switch mode {
			case "before":
				if len(named) != 0 || got != "0/0" {
					t.Errorf("killed before the commit: manifest names %d files and the table answers %s; want 0 files, 0/0 rows", len(named), got)
				}
				if len(orphans) != 1 {
					t.Errorf("killed before the commit: %d orphan objects, want exactly 1 (the flushed file the manifest never learned of): %v", len(orphans), orphans)
				}
			case "after":
				if len(named) != 1 || got != "50/50" {
					t.Errorf("killed after the commit: manifest names %d files and the table answers %s; want 1 file, 50/50 rows", len(named), got)
				}
				if len(orphans) != 0 {
					t.Errorf("killed after the commit: %d orphan objects, want 0: %v", len(orphans), orphans)
				}
			}
		})
	}
}
