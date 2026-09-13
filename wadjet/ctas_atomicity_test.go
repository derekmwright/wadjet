package wadjet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// failAfterStore fails every Put of a DATA object after the first n, so a
// statement that writes more than one file fails in the middle of writing.
//
// It is a wrapper and not a flag on MemStore because the failure has to be
// selective: the catalog's own keys go through the same Store, and a store
// that refused those too would fail the statement before it wrote anything,
// which is the case this fixture is NOT about.
type failAfterStore struct {
	objstore.Store
	writes atomic.Int64
	after  int64
}

func (s *failAfterStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, ct string) (string, error) {
	if strings.HasPrefix(key, "tables/") && s.writes.Add(1) > s.after {
		return "", errors.New("simulated object-store failure")
	}
	return s.Store.Put(ctx, bucket, key, r, size, ct)
}

// A query-sourced write that fails leaves the catalog and the bucket as it
// found them (#1024).
//
// This is the atomicity claim, and it has two halves. The CATALOG half: a
// failed CTAS creates no table, because the table is created by the commit
// that publishes the files rather than before the query runs — so there is no
// window in which the name resolves to something empty and no half-written
// table to clean up. The BUCKET half: the parquet objects written before the
// failure are retired, because nothing references them.
//
// The bucket half is stricter than the residual ADR-0030 accepts for a refused
// DML retry ("bytes, never rows"), and it can be: a DML retry may legitimately
// re-run and needs its objects, while this statement is over.
func TestAFailedQuerySourcedWriteLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()

	t.Run("TheQueryFailsMidStream", func(t *testing.T) {
		db, store := ctasAtomicityDB(t, 0)
		before := ctasObjectKeys(t, ctx, store)

		// 22012, division_by_zero, raised on the row where g = 20. The rows
		// before it were already produced.
		_, err := db.Query(ctx, `CREATE TABLE half AS SELECT id, id / (g - 20) AS q FROM shp`)
		if err == nil {
			t.Fatal("the statement succeeded over a query that divides by zero")
		}
		ctasAssertNoTable(t, ctx, db, "half")
		ctasAssertNoNewObjects(t, ctx, store, before)
	})

	t.Run("TheWriteFails", func(t *testing.T) {
		// One data object may land — the fixture's own INSERT — and the next
		// one, which is this statement's, fails.
		db, store := ctasAtomicityDB(t, 1)
		before := ctasObjectKeys(t, ctx, store)

		_, err := db.Query(ctx, `CREATE TABLE broke AS SELECT id, s FROM shp`)
		if err == nil {
			t.Fatal("the statement succeeded over a store that refuses to write")
		}
		ctasAssertNoTable(t, ctx, db, "broke")
		ctasAssertNoNewObjects(t, ctx, store, before)
	})

	t.Run("TheStatementIsCancelled", func(t *testing.T) {
		db, store := ctasAtomicityDB(t, 0)
		before := ctasObjectKeys(t, ctx, store)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := db.Query(cancelled, `CREATE TABLE gone AS SELECT id, s FROM shp`)
		if err == nil {
			t.Fatal("a cancelled statement succeeded")
		}
		ctasAssertNoTable(t, ctx, db, "gone")
		ctasAssertNoNewObjects(t, ctx, store, before)
	})

	t.Run("AnAppendThatFailsAppendsNothing", func(t *testing.T) {
		db, store := ctasAtomicityDB(t, 1)
		if _, err := db.Query(ctx, `CREATE TABLE app (a INT64, b STRING)`); err != nil {
			t.Fatal(err)
		}
		before := ctasObjectKeys(t, ctx, store)

		_, err := db.Query(ctx, `INSERT INTO app SELECT id, s FROM shp`)
		if err == nil {
			t.Fatal("the append succeeded over a store that refuses to write")
		}
		if n := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM app`); n != 0 {
			t.Errorf("the target holds %d rows after a failed append", n)
		}
		ctasAssertNoNewObjects(t, ctx, store, before)
	})
}

// Two CTAS statements racing for one name: one wins whole, the other is
// 42P07, and the winner's table is complete — never a name that resolves to a
// table holding one statement's files and another's rows.
func TestTwoCreatesRacingForOneNameLeaveOneTable(t *testing.T) {
	ctx := context.Background()
	db, _ := ctasAtomicityDB(t, 0)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = db.Query(ctx, `CREATE TABLE raced AS SELECT id, s FROM shp`)
		}(i)
	}
	wg.Wait()

	ok := 0
	for i, err := range errs {
		if err == nil {
			ok++
			continue
		}
		if got := sqlerr.StateOf(err); got != "42P07" {
			t.Errorf("statement %d failed %s: %v; the loser of this race is 42P07", i, got, err)
		}
	}
	if ok == 0 {
		t.Fatalf("neither statement created the table: %v", errs)
	}
	if n := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM raced`); n != 4 {
		t.Errorf("the surviving table holds %d rows, want 4 — a race must not merge two statements' files", n)
	}
}

// A CTAS reads its source through the manifest it read, and an ingest flush
// landing on that source while the statement runs does not change the answer
// it already computed (ADR-0030, the read half — SELECT has statement-level
// snapshot isolation).
//
// The assertion is that the created table holds ONE of the two consistent
// answers — the four rows the statement's manifest named, or the six a
// statement that read the later manifest would name — and never a mixture, and
// that the statement commits rather than failing.
func TestACreateReadsOneManifestOfItsSource(t *testing.T) {
	ctx := context.Background()
	db, _ := ctasAtomicityDB(t, 0)

	// A concurrent flush into the SOURCE table, landing while the CTAS runs.
	done := make(chan error, 1)
	go func() {
		ing := db.NewIngester("shp", ctasShapeSchema(), nil, ingest.DefaultConfig())
		err := ing.Ingest(ctx, []map[string]any{
			{"id": int64(5), "g": int64(30), "s": "e", "d": nil, "ip": nil},
			{"id": int64(6), "g": int64(30), "s": "f", "d": nil, "ip": nil},
		})
		if err == nil {
			err = ing.FlushAll(ctx)
		}
		done <- err
	}()

	res, err := db.Query(ctx, `CREATE TABLE snap AS SELECT id, s FROM shp`)
	if err != nil {
		t.Fatalf("the CTAS failed under a concurrent ingest: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("the concurrent ingest failed: %v", err)
	}

	n := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM snap`)
	if n != 4 && n != 6 {
		t.Errorf("the created table holds %d rows; the only consistent answers are 4 "+
			"(the manifest the statement read) and 6 (the one the flush published)", n)
	}
	if got, want := res.Rows[0]["result"], fmt.Sprintf("SELECT %d", n); got != want {
		t.Errorf("command tag %v, but the table holds %d rows", got, n)
	}
}

func ctasShapeSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "g", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 12, Scale: 3, Nullable: true},
		{Name: "ip", Type: parquet.TypeIPv4, Nullable: true},
	}}
}

// ctasAtomicityDB is ctasShapeDB over a store that fails data writes after
// `after` of them. `after` = 0 means never fail.
func ctasAtomicityDB(t *testing.T, after int64) (*DB, objstore.Store) {
	t.Helper()
	ctx := context.Background()
	mem := objstore.NewMemStore()
	var store objstore.Store = mem
	var fail *failAfterStore
	if after > 0 {
		fail = &failAfterStore{Store: mem, after: after}
		store = fail
	}
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(ctx, "shp", ctasShapeSchema(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO shp (id, g, s, d, ip) VALUES `+
		`(1, 10, 'a', 1.5, '10.0.0.1'), (2, 10, 'b', 2.25, '10.0.0.2'), `+
		`(3, 20, 'c', 3.125, '10.0.0.3'), (4, 20, NULL, NULL, NULL)`); err != nil {
		t.Fatal(err)
	}
	return db, store
}

func ctasObjectKeys(t *testing.T, ctx context.Context, store objstore.Store) map[string]bool {
	t.Helper()
	// EVERY key, not just tables/: a flush uploads a statistics object beside
	// its data file and an orphan of that is the same leak one layer over.
	infos, err := store.List(ctx, "test", objstore.ListOptions{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	out := make(map[string]bool, len(infos))
	for _, info := range infos {
		out[info.Key] = true
	}
	return out
}

func ctasAssertNoNewObjects(t *testing.T, ctx context.Context, store objstore.Store, before map[string]bool) {
	t.Helper()
	for key := range ctasObjectKeys(t, ctx, store) {
		if !before[key] {
			t.Errorf("the failed statement left the object %q behind", key)
		}
	}
}

func ctasAssertNoTable(t *testing.T, ctx context.Context, db *DB, name string) {
	t.Helper()
	if _, err := db.catalog.GetTable(ctx, name); err == nil {
		t.Errorf("the failed statement created the table %q", name)
	}
	tables, err := db.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		if tbl == name {
			t.Errorf("the failed statement left %q in the catalog's table list", name)
		}
	}
}
