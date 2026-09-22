// SPDX-License-Identifier: MIT

package wadjet_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc EC (#1255): an embedded program's catalog survives a restart.
//
// Everything below goes through the PUBLIC API the way an out-of-tree
// program does — wadjet.Open with Config.DataDir, CreateTable, NewIngester,
// Query, Close — and only the FIXTURE (the type matrix's schema and rows)
// is an internal import. The point is the seam a user crosses: a table
// created by one Open is there for the next one, with the same rows and
// the same declared types, for every type the engine has.
//
// At base wadjet.Config has no DataDir, so this file does not compile; the
// base probe beside it (ec_author/gate_restart_at_base_FAILS.log) shows the
// same program under the base API — Store: NewFileStore, no MetaKV — losing
// the table at the second Open while its Parquet files stay behind.

const ecRows = 300

// ecOpen opens a persistent database over dir through the public API.
func ecOpen(t *testing.T, dir string) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(context.Background(), wadjet.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("wadjet.Open(DataDir=%s): %v", dir, err)
	}
	return db
}

// ecIngest creates table with schema and ingests rows through it, flushed
// and stopped before it returns.
func ecIngest(t *testing.T, db *wadjet.DB, table string, schema wadjet.Schema, rows []map[string]any) {
	t.Helper()
	ctx := context.Background()
	if err := db.CreateTable(ctx, table, schema, nil); err != nil {
		t.Fatalf("CreateTable %s: %v", table, err)
	}
	ing := db.NewIngester(table, schema, nil, wadjet.IngestConfig{MaxBufferRows: 1 << 20, FlushInterval: time.Hour})
	ing.Start()
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("Ingest %s: %v", table, err)
	}
	if err := ing.Stop(ctx); err != nil {
		t.Fatalf("Stop %s: %v", table, err)
	}
}

// ecSnapshot reads a table back positionally with its declared types.
func ecSnapshot(t *testing.T, db *wadjet.DB, table string) (types []string, rows []string) {
	t.Helper()
	res, err := db.Query(context.Background(), "SELECT * FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT * FROM %s: %v", table, err)
	}
	for _, m := range res.ColumnMetas {
		types = append(types, m.Name+":"+m.TypeName)
	}
	for i := range res.Rows {
		cells := res.Cells(i)
		parts := make([]string, len(cells))
		for j, v := range cells {
			parts[j] = fmt.Sprintf("%v", v)
		}
		rows = append(rows, strings.Join(parts, "|"))
	}
	return types, rows
}

func TestAnEmbeddedCatalogSurvivesARestartForEveryType(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	tables := []struct {
		name   string
		schema wadjet.Schema
		rows   []map[string]any
	}{
		{"tm_flat", typematrix.Schema(), typematrix.Data(ecRows)},
		{"tm_nested", typematrix.NestedSchema(), typematrix.NestedData(ecRows)},
	}

	// First life: create, ingest, read, close.
	first := ecOpen(t, dir)
	before := map[string][2][]string{}
	for _, tb := range tables {
		ecIngest(t, first, tb.name, tb.schema, tb.rows)
		types, rows := ecSnapshot(t, first, tb.name)
		if len(rows) != ecRows {
			t.Fatalf("%s: %d rows before the restart, want %d", tb.name, len(rows), ecRows)
		}
		before[tb.name] = [2][]string{types, rows}
	}
	first.Close()

	// Second life: nothing but the directory.
	second := ecOpen(t, dir)
	defer second.Close()
	names, err := second.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tb := range tables {
		found := false
		for _, n := range names {
			found = found || n == tb.name
		}
		if !found {
			t.Fatalf("after the restart ListTables = %v; %s is gone (#1255)", names, tb.name)
		}
		types, rows := ecSnapshot(t, second, tb.name)
		if !reflect.DeepEqual(types, before[tb.name][0]) {
			t.Errorf("%s: declared types changed across the restart:\n before %v\n after  %v", tb.name, before[tb.name][0], types)
		}
		if !reflect.DeepEqual(rows, before[tb.name][1]) {
			n := len(rows)
			if len(before[tb.name][1]) < n {
				n = len(before[tb.name][1])
			}
			for i := 0; i < n; i++ {
				if rows[i] != before[tb.name][1][i] {
					t.Errorf("%s: row %d differs across the restart:\n before %s\n after  %s", tb.name, i, before[tb.name][1][i], rows[i])
					break
				}
			}
			if len(rows) != len(before[tb.name][1]) {
				t.Errorf("%s: %d rows after the restart, %d before", tb.name, len(rows), len(before[tb.name][1]))
			}
		}
	}

	// The second life can WRITE too: a DML statement commits against the
	// manifest the restart read back (ADR-0030), and a third life sees it.
	if _, err := second.Execute(ctx, "DELETE FROM tm_flat WHERE id < 10"); err != nil {
		t.Fatalf("DELETE after the restart: %v", err)
	}
	second.Close()
	third := ecOpen(t, dir)
	defer third.Close()
	_, rows := ecSnapshot(t, third, "tm_flat")
	if len(rows) != ecRows-10 {
		t.Fatalf("after a DELETE and a second restart: %d rows, want %d", len(rows), ecRows-10)
	}
}

// The layout is the CLI's: `wadjet --storage-type=file --data-dir=D` keeps
// the objects under D/<bucket>/ and the catalog under D/_catalog/, and so
// does Config.DataDir — that identity is what lets a `serve` over D see the
// program's tables (the e2e gate in internal/cli exercises the server half).
func TestADataDirHasTheCLIsLayout(t *testing.T) {
	dir := t.TempDir()
	db := ecOpen(t, dir)
	ecIngest(t, db, "layout", wadjet.Schema{Columns: []wadjet.Column{{Name: "id", Type: wadjet.TypeInt64}}},
		[]map[string]any{{"id": int64(1)}})
	db.Close()

	for _, want := range []string{
		filepath.Join(dir, "_catalog", "wadjet.lock"),
		filepath.Join(dir, "_catalog", "jetstream"),
		filepath.Join(dir, "wadjet", "tables", "layout"),
	} {
		if _, err := os.Stat(want); err != nil {
			t.Errorf("DataDir layout: %s missing: %v", want, err)
		}
	}
}

// A held directory is refused, loudly, naming the holder — never opened a
// second time beside the first. Both the same process (two DBs, one dir)
// and, since the lock is a flock the kernel scopes per open file
// description, any other process. After Close the directory is free again.
func TestASecondOpenOfAHeldDirectoryIsRefused(t *testing.T) {
	dir := t.TempDir()
	first := ecOpen(t, dir)
	defer first.Close()

	second, err := wadjet.Open(context.Background(), wadjet.Config{DataDir: dir})
	if err == nil {
		second.Close()
		t.Fatal("a second Open of a held DataDir succeeded: two processes would now write one catalog store")
	}
	if !errors.Is(err, wadjet.ErrCatalogHeld) {
		t.Fatalf("the refusal is not ErrCatalogHeld: %v", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("process %d", os.Getpid())) {
		t.Errorf("the refusal does not name the holder's pid %d: %v", os.Getpid(), err)
	}
	if !strings.Contains(err.Error(), filepath.Join(dir, "_catalog")) {
		t.Errorf("the refusal does not name the directory: %v", err)
	}

	first.Close()
	again := ecOpen(t, dir)
	again.Close()
	// Close is idempotent, and a closed DB holds nothing.
	again.Close()
	last := ecOpen(t, dir)
	last.Close()
}

// A contradictory Config is refused before anything is opened: two stores,
// two catalogs, or a catalog directory with nothing to store the data in.
func TestAContradictoryPersistenceConfigIsRefused(t *testing.T) {
	dir := t.TempDir()
	store, err := wadjet.NewFileStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		cfg  wadjet.Config
		want string
	}{
		{"DataDir and Store", wadjet.Config{DataDir: dir, Store: store}, "DataDir and Store"},
		{"CatalogDir without a Store", wadjet.Config{CatalogDir: filepath.Join(dir, "cat")}, "without a Store"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, err := wadjet.Open(context.Background(), c.cfg)
			if err == nil {
				db.Close()
				t.Fatalf("Open accepted %s", c.name)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("refusal %q does not say %q", err, c.want)
			}
			// Nothing was opened: the directory is not held.
			if _, statErr := os.Stat(filepath.Join(dir, "_catalog", "wadjet.lock")); statErr == nil {
				t.Errorf("a refused Open left a lock file behind")
			}
		})
	}
}

// CatalogDir alone: a persistent catalog beside a store the caller built
// (the S3 embedder's shape, exercised here over a file store), and the
// in-memory catalog — no directory named — still works and is still
// process-local, which is the documented contract, not a defect.
func TestCatalogDirBesideACallersStoreAndTheInMemoryDefault(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	schema := wadjet.Schema{Columns: []wadjet.Column{{Name: "id", Type: wadjet.TypeInt64}}}

	t.Run("CatalogDir beside a Store", func(t *testing.T) {
		open := func() *wadjet.DB {
			store, err := wadjet.NewFileStore(filepath.Join(dir, "objects"))
			if err != nil {
				t.Fatal(err)
			}
			db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "b", CatalogDir: filepath.Join(dir, "cat")})
			if err != nil {
				t.Fatal(err)
			}
			return db
		}
		db := open()
		ecIngest(t, db, "beside", schema, []map[string]any{{"id": int64(42)}})
		db.Close()
		db = open()
		defer db.Close()
		_, rows := ecSnapshot(t, db, "beside")
		if len(rows) != 1 || rows[0] != "42" {
			t.Fatalf("CatalogDir beside a Store: rows after the restart = %v, want [42]", rows)
		}
	})

	t.Run("in-memory catalog stays process-local", func(t *testing.T) {
		db, err := wadjet.Open(ctx, wadjet.Config{Store: wadjet.NewMemStore(), Bucket: "b"})
		if err != nil {
			t.Fatal(err)
		}
		ecIngest(t, db, "mem", schema, []map[string]any{{"id": int64(1)}})
		_, rows := ecSnapshot(t, db, "mem")
		if len(rows) != 1 {
			t.Fatalf("in-memory: %d rows, want 1", len(rows))
		}
		db.Close()
		db, err = wadjet.Open(ctx, wadjet.Config{Store: wadjet.NewMemStore(), Bucket: "b"})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		names, _ := db.ListTables(ctx)
		if len(names) != 0 {
			t.Fatalf("a fresh in-memory catalog lists %v", names)
		}
	})
}
