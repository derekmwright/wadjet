package compaction

// Object retirement: an object is physically retired only with proof that no
// live manifest in the catalog references it, and doubt preserves bytes
// (ADR-0020, "An object is retired only with proof"). #896.
//
// The shared fixture below is also what the publication-safety gates
// (publication_safety_test.go) drive.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// pubStore runs afterPut once, immediately after a compaction or rewrite
// OUTPUT has landed in the store and before Put returns to the compactor.
// That is the exact window every one of these defects lives in: the output
// exists, and nothing has been published yet.
type pubStore struct {
	objstore.Store
	afterPut func()
}

func (s *pubStore) Put(ctx context.Context, b, k string, r io.Reader, n int64, ct string) (string, error) {
	et, e := s.Store.Put(ctx, b, k, r, n, ct)
	if e == nil && s.afterPut != nil && (strings.Contains(k, "compacted_") || strings.Contains(k, "rewrite_")) {
		f := s.afterPut
		s.afterPut = nil
		f()
	}
	return et, e
}

// pubKV wraps the real MemKV. beforeUpdate sees every manifest payload before
// it is written (and may refuse it, which is the injected publication
// failure); beforeGet runs a scheduled operation at a manifest READ.
type pubKV struct {
	catalog.MetaKV
	beforeUpdate func(key string, payload []byte) error
	beforeGet    func(key string)
}

func (k *pubKV) Update(key string, b []byte, rev uint64) (uint64, error) {
	if k.beforeUpdate != nil {
		if e := k.beforeUpdate(key, b); e != nil {
			return 0, e
		}
	}
	return k.MetaKV.Update(key, b, rev)
}

func (k *pubKV) Get(key string) ([]byte, uint64, error) {
	if k.beforeGet != nil {
		k.beforeGet(key)
	}
	return k.MetaKV.Get(key)
}

type pubFixture struct {
	cat     *catalog.Catalog
	store   *pubStore
	kv      *pubKV
	paths   []string
	entries []catalog.FileEntry
	// grace overrides Config.DeleteGrace for compactors this fixture makes.
	// Zero keeps the production default (compacted-away bytes linger for the
	// grace); negative retires them inline, which is what lets a cell assert
	// exactly which objects are still in the store.
	grace time.Duration
}

// pubSetup builds a table of nfiles two-row parquet files holding the ids
// 1..2*nfiles, one row per id.
func pubSetup(t *testing.T, nfiles int) *pubFixture {
	t.Helper()
	ctx := context.Background()
	s := &pubStore{Store: objstore.NewMemStore()}
	k := &pubKV{MetaKV: catalog.NewMemKV()}
	c := catalog.New(k, s, "test-bucket")
	if err := c.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateTable(ctx, "events", pubSchema(), nil); err != nil {
		t.Fatal(err)
	}
	f := &pubFixture{cat: c, store: s, kv: k}
	for i := 0; i < nfiles; i++ {
		path := fmt.Sprintf("tables/events/source%d.parquet", i)
		size := pubWriteFile(t, s, path, []map[string]any{
			{"id": int64(i*2 + 1)}, {"id": int64(i*2 + 2)},
		})
		entry := catalog.FileEntry{Path: path, SizeBytes: size, NumRows: 2, CreatedAt: time.Now().UTC()}
		if err := c.AddNewFiles(ctx, "events", nil, "tables/events", []catalog.FileEntry{entry}); err != nil {
			t.Fatal(err)
		}
		f.paths = append(f.paths, path)
		f.entries = append(f.entries, entry)
	}
	return f
}

func pubSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
}

func pubWriteFile(t *testing.T, store objstore.Store, path string, rows []map[string]any) int64 {
	t.Helper()
	var b bytes.Buffer
	w, err := parquet.NewWriter(&b, pubSchema(), parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), "test-bucket", path,
		bytes.NewReader(b.Bytes()), int64(b.Len()), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	return int64(b.Len())
}

func (f *pubFixture) compactor() *Compactor {
	cfg := DefaultConfig()
	cfg.MinFiles = 2
	if f.grace != 0 {
		cfg.DeleteGrace = f.grace
	}
	return New(f.cat, nil, cfg)
}

// ids is the table's row set as a READER computes it: every file the manifest
// names, minus every row its delete markers mark.
func (f *pubFixture) ids(t *testing.T) []int64 {
	t.Helper()
	m, err := f.cat.GetManifest(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	return pubIDsOf(t, f.store, m)
}

// pubIDsOf is ids over an arbitrary manifest — including one decoded straight
// out of a KV payload that has not been written yet, which is how the
// commit-boundary cell observes what a reader at that revision would answer.
func pubIDsOf(t *testing.T, store objstore.Store, m *catalog.PartitionManifest) []int64 {
	t.Helper()
	deleted := catalog.DeletedRowsByFile(m.DeleteMarkers)
	out := []int64{}
	for _, p := range m.Partitions {
		for _, entry := range p.Files {
			rc, _, err := store.Get(context.Background(), "test-bucket", entry.Path)
			if err != nil {
				t.Fatalf("manifest names %q, which the store does not have: %v", entry.Path, err)
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			r, err := parquet.NewReaderFromBytes(data)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := r.ReadRows(nil)
			if err != nil {
				t.Fatal(err)
			}
			for i, row := range rows {
				if !deleted[entry.Path][int64(i)] {
					out = append(out, row["id"].(int64))
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (f *pubFixture) manifestPaths(t *testing.T) []string {
	t.Helper()
	m, err := f.cat.GetManifest(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, p := range m.Partitions {
		for _, e := range p.Files {
			out = append(out, e.Path)
		}
	}
	sort.Strings(out)
	return out
}

// engineWrittenObjects lists the compaction/rewrite OUTPUTS physically in the
// store. Every one of them must be named by the manifest: a refused
// publication deletes its own output, so a leftover here is the losing
// compactor leaking bytes nobody can reach.
func (f *pubFixture) engineWrittenObjects(t *testing.T) []string {
	t.Helper()
	objs, err := f.store.List(context.Background(), "test-bucket", objstore.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, o := range objs {
		if strings.Contains(o.Key, "compacted_") || strings.Contains(o.Key, "rewrite_") {
			out = append(out, o.Key)
		}
	}
	sort.Strings(out)
	return out
}

func pubWant(t *testing.T, got, want []int64, what string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: visible IDs=%v; want %v", what, got, want)
	}
}

// ---------------------------------------------------------------- #896

// TestADeferredRetirementNeverDeletesAnObjectALiveTableReferences is #896.
//
// Removing a file from ONE table's manifest is not proof that nothing
// references the object. The deferred-delete queue's only guard was the
// object's LastModified, and registering unchanged bytes into another table
// does not move it.
func TestADeferredRetirementNeverDeletesAnObjectALiveTableReferences(t *testing.T) {
	t.Run("reregistered_during_the_grace", func(t *testing.T) {
		f := pubSetup(t, 2)
		ctx := context.Background()
		c := f.compactor()
		if _, err := c.CompactTable(ctx, "events"); err != nil {
			t.Fatal(err)
		}
		// Re-register the original immutable object in a different table,
		// without overwriting it.
		if err := f.cat.CreateTable(ctx, "archive", pubSchema(), nil); err != nil {
			t.Fatal(err)
		}
		if err := f.cat.AddFiles(ctx, "archive", nil, "archive", []catalog.FileEntry{f.entries[0]}); err != nil {
			t.Fatal(err)
		}
		c.config.DeleteGrace = time.Nanosecond
		c.FlushDeferredDeletes(ctx)

		if _, err := f.store.Head(ctx, "test-bucket", f.paths[0]); err != nil {
			t.Errorf("archive's referenced source was deleted: %v", err)
		}
		// The unreferenced sibling is still collected: the guard is a
		// reference check, not a blanket refusal to ever delete anything.
		if _, err := f.store.Head(ctx, "test-bucket", f.paths[1]); err == nil {
			t.Error("the unreferenced original should have been retired")
		}
	})

	t.Run("immediate_grace", func(t *testing.T) {
		// A negative grace deletes inline rather than through the queue. That
		// path had no reference check either.
		f := pubSetup(t, 2)
		ctx := context.Background()
		if err := f.cat.CreateTable(ctx, "archive", pubSchema(), nil); err != nil {
			t.Fatal(err)
		}
		if err := f.cat.AddFiles(ctx, "archive", nil, "archive", []catalog.FileEntry{f.entries[0]}); err != nil {
			t.Fatal(err)
		}
		cfg := DefaultConfig()
		cfg.MinFiles = 2
		cfg.DeleteGrace = -time.Second
		if _, err := New(f.cat, nil, cfg).CompactTable(ctx, "events"); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.Head(ctx, "test-bucket", f.paths[0]); err != nil {
			t.Errorf("archive's referenced source was deleted by the immediate path: %v", err)
		}
	})

	t.Run("registration_racing_the_flush", func(t *testing.T) {
		// The half a reference check cannot cover on its own: the
		// registration lands AFTER the flush has read the catalog. The
		// retirement mark is taken before that read, so the registration is
		// refused rather than silently racing the delete.
		f := pubSetup(t, 2)
		ctx := context.Background()
		c := f.compactor()
		if _, err := c.CompactTable(ctx, "events"); err != nil {
			t.Fatal(err)
		}
		if err := f.cat.CreateTable(ctx, "archive", pubSchema(), nil); err != nil {
			t.Fatal(err)
		}

		var raceErr error
		raced := false
		f.kv.beforeGet = func(key string) {
			// Inside the flush's live-catalog read, after the mark is held.
			if raced || !strings.Contains(key, "manifest.events") {
				return
			}
			raced = true
			raceErr = f.cat.AddFiles(ctx, "archive", nil, "archive", []catalog.FileEntry{f.entries[0]})
		}
		c.config.DeleteGrace = time.Nanosecond
		c.FlushDeferredDeletes(ctx)
		f.kv.beforeGet = nil

		if !raced {
			t.Fatal("the scheduled registration did not run: the flush read no manifest")
		}
		if !errors.Is(raceErr, catalog.ErrPathRetiring) {
			t.Fatalf("a registration racing a retirement must be refused with ErrPathRetiring, got %v", raceErr)
		}
		// Refused means nothing was registered, so archive names nothing and
		// the object was free to go.
		m, err := f.cat.GetManifest(ctx, "archive")
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range m.Partitions {
			if len(p.Files) != 0 {
				t.Errorf("a refused registration must write nothing: %+v", p.Files)
			}
		}
	})

	t.Run("drop_reclaim_unchanged", func(t *testing.T) {
		// The other retirement schedule is untouched by all of this: a
		// dropped table's engine-written files are still reclaimed.
		f := pubSetup(t, 2)
		ctx := context.Background()
		f.cat.EnableDropReclaim()
		if err := f.cat.DropTable(ctx, "events"); err != nil {
			t.Fatal(err)
		}
		if n := f.cat.FlushDroppedTableFiles(ctx, -time.Second); n != len(f.paths) {
			t.Fatalf("drop reclaim deleted %d files, want %d", n, len(f.paths))
		}
		for _, p := range f.paths {
			if _, err := f.store.Head(ctx, "test-bucket", p); err == nil {
				t.Errorf("dropped table's file %q should have been reclaimed", p)
			}
		}
	})
}
