// SPDX-License-Identifier: MIT

package wadjet_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc EC, review round 1 (Codex, b44edc29). Each gate below fails at that
// tip; the logs are beside the review under ec_author/.

// B5: a relative DataDir is resolved ONCE, at Open. A program that changes
// its working directory after Open keeps writing under the directory it
// opened — a store that re-resolved a relative root at each Put sent the
// next flushed object under the new cwd while the catalog kept naming the
// old root, and the committed row vanished on reopen with no error.
//
// Runs in a child process: os.Chdir is process-wide and the rest of this
// package's tests read the working directory.
const ecRelativeEnv = "WADJET_EC_RELATIVE_CHILD"

func TestECRelativeChild(t *testing.T) {
	root := os.Getenv(ecRelativeEnv)
	if root == "" {
		t.Skip("child only")
	}
	ctx := context.Background()
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	for _, d := range []string{a, b} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chdir(a); err != nil {
		t.Fatal(err)
	}
	db, err := wadjet.Open(ctx, wadjet.Config{DataDir: "data"}) // relative
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "shifted", wadjet.Schema{Columns: []wadjet.Column{{Name: "id", Type: wadjet.TypeInt64}}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO shifted VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(b); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO shifted VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := os.Chdir(a); err != nil {
		t.Fatal(err)
	}
	db, err = wadjet.Open(ctx, wadjet.Config{DataDir: "data"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	res, err := db.Query(ctx, "SELECT id FROM shifted ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range res.Rows {
		ids = append(ids, fmt.Sprint(r["id"]))
	}
	fmt.Println("EC-RELATIVE rows:", strings.Join(ids, ","))
	if entries, _ := filepath.Glob(filepath.Join(b, "data", "wadjet", "tables", "shifted", "*.parquet")); len(entries) > 0 {
		fmt.Println("EC-RELATIVE strayed:", entries)
	}
}

func TestARelativeDataDirIsResolvedAtOpenNotAtEachFlush(t *testing.T) {
	root := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestECRelativeChild$", "-test.count=1")
	child.Env = append(os.Environ(), ecRelativeEnv+"="+root)
	out, err := child.CombinedOutput()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "EC-RELATIVE rows: 1,2") {
		t.Errorf("a row committed after a chdir is gone on reopen (review B5):\n%s", out)
	}
	if strings.Contains(string(out), "EC-RELATIVE strayed:") {
		t.Errorf("an object was written under the NEW working directory:\n%s", out)
	}
}

// B2 + N1: a refused Config refuses BEFORE the filesystem is touched — no
// directory is created for any refused combination — and an empty Config
// is a refusal, not a nil-pointer panic in Catalog.Init.
func TestARefusedConfigCreatesNothingAndAnEmptyConfigIsRefused(t *testing.T) {
	root := t.TempDir()
	memStore := wadjet.NewMemStore()
	dirs := func(n string) (string, string) {
		return filepath.Join(root, n, "data"), filepath.Join(root, n, "cat")
	}
	d1, _ := dirs("store")
	d2, _ := dirs("metakv")
	_, c3 := dirs("catkv")
	_, c4 := dirs("catonly")
	cases := []struct {
		name string
		cfg  wadjet.Config
		want string
		must []string // paths that must NOT exist afterwards
	}{
		{"DataDir+Store", wadjet.Config{DataDir: d1, Store: memStore}, "DataDir and Store", []string{d1}},
		{"DataDir+MetaKV", wadjet.Config{DataDir: d2, MetaKV: catalog.NewMemKV()}, "DataDir and MetaKV", []string{d2}},
		{"CatalogDir+MetaKV", wadjet.Config{CatalogDir: c3, Store: memStore, Bucket: "b", MetaKV: catalog.NewMemKV()}, "CatalogDir and MetaKV", []string{c3}},
		{"CatalogDir without Store", wadjet.Config{CatalogDir: c4}, "without a Store", []string{c4}},
		{"empty Config", wadjet.Config{}, "no Store and no DataDir", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var db *wadjet.DB
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("PANIC: %v", r)
					}
				}()
				db, err = wadjet.Open(context.Background(), c.cfg)
			}()
			if err == nil {
				db.Close()
				t.Fatalf("Open accepted %s", c.name)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("refusal %q does not say %q", err, c.want)
			}
			for _, p := range c.must {
				if _, statErr := os.Stat(p); statErr == nil {
					t.Errorf("a refused Open created %s (review B2)", p)
				}
			}
		})
	}
}

// P1: ErrCatalogHeld is a held LOCK and nothing else. A directory that
// cannot be created or a `_catalog` that is a regular file report their own
// cause — errors.Is(err, os.ErrPermission) answers — and are never mistaken
// for another process holding the catalog.
func TestAFileErrorIsNotReportedAsAHeldDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	ctx := context.Background()
	t.Run("read-only parent", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "ro")
		if err := os.Mkdir(parent, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(parent, 0o755) })
		db, err := wadjet.Open(ctx, wadjet.Config{DataDir: filepath.Join(parent, "data")})
		if err == nil {
			db.Close()
			t.Fatal("Open succeeded under a read-only parent")
		}
		if errors.Is(err, wadjet.ErrCatalogHeld) {
			t.Errorf("a permission error is reported as a held directory: %v", err)
		}
		if !errors.Is(err, os.ErrPermission) {
			t.Errorf("the permission cause is lost from the chain: %v", err)
		}
	})
	t.Run("_catalog is a regular file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "_catalog"), []byte("not a directory"), 0o644); err != nil {
			t.Fatal(err)
		}
		db, err := wadjet.Open(ctx, wadjet.Config{DataDir: dir})
		if err == nil {
			db.Close()
			t.Fatal("Open succeeded with a regular file at _catalog")
		}
		if errors.Is(err, wadjet.ErrCatalogHeld) {
			t.Errorf("a not-a-directory error is reported as a held directory: %v", err)
		}
	})
}
