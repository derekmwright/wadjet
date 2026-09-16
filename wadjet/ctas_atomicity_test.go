// SPDX-License-Identifier: MIT

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

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
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

// failKeyKV fails every Put of a key whose suffix matches, so a gate can make
// the catalog's commit fail at a chosen step.
type failKeyKV struct {
	catalog.MetaKV
	suffix string
	armed  atomic.Bool
}

func (k *failKeyKV) Put(key string, value []byte) (uint64, error) {
	if k.armed.Load() && strings.HasSuffix(key, k.suffix) {
		return 0, errors.New("simulated catalog write failure")
	}
	return k.MetaKV.Put(key, value)
}

// A CTAS whose COMMIT fails leaves the name FREE (#1024 round-2 review B6).
//
// A table is three catalog records — `table.<name>`, `manifest.<name>` and the
// `meta` list — and readers do not agree about which one IS the table:
// `GetTable` and `tableExists` read the first, `ListTables` and `DropTable` go
// through the last. A partial write therefore does not leave a half-table, it
// leaves a WEDGED NAME: one that cannot be read (`manifest not found`), cannot
// be created (42P07 from the record that is there) and cannot be dropped (the
// list never got it).
//
// The trigger the review found needed no fault injection at all — a float
// column whose min/max is an infinity has no JSON form, and this arc is what
// put query-derived statistics into that manifest. That trigger is gone (a
// non-finite bound is dropped before the stats leave for the catalog, and the
// statement now SUCCEEDS, which is what PostgreSQL 17.11 does with an infinity
// in a double precision column — measured). So the gate injects the failure
// instead: at each of the three keys, in turn.
func TestACreateWhoseCommitFailsLeavesTheNameFree(t *testing.T) {
	ctx := context.Background()

	for _, step := range []struct{ name, suffix string }{
		{"AtTheManifest", "manifest.wedged"},
		{"AtTheTableRecord", "table.wedged"},
		{"AtTheCatalogList", ".meta"},
	} {
		t.Run(step.name, func(t *testing.T) {
			kv := &failKeyKV{MetaKV: catalog.NewMemKV(), suffix: step.suffix}
			store := objstore.NewMemStore()
			db, err := Open(ctx, Config{Store: store, Bucket: "test", MetaKV: kv})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			if err := db.CreateTable(ctx, "src", parquet.Schema{Columns: []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64, Nullable: true},
			}}, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Execute(ctx, "INSERT INTO src (id) VALUES (1),(2)"); err != nil {
				t.Fatal(err)
			}
			before := ctasObjectKeys(t, ctx, store)

			kv.armed.Store(true)
			if _, err := db.Query(ctx, "CREATE TABLE wedged AS SELECT id FROM src"); err == nil {
				t.Fatal("the statement succeeded over a catalog that refused its commit")
			}
			kv.armed.Store(false)

			// Nothing is left: not the record every existence test reads, not
			// the list, and not the objects.
			ctasAssertNoTable(t, ctx, db, "wedged")
			ctasAssertNoNewObjects(t, ctx, store, before)

			// And the NAME is reusable, which is the property the wedge took
			// away: the retry must create the table, not answer 42P07.
			if _, err := db.Query(ctx, "CREATE TABLE wedged AS SELECT id FROM src"); err != nil {
				t.Fatalf("the name is not reusable after a failed commit: %v", err)
			}
			if n := ctasScalar(t, ctx, db, "SELECT COUNT(*) AS c FROM wedged"); n != 2 {
				t.Errorf("the retry's table holds %d rows, want 2", n)
			}
			if _, err := db.Query(ctx, "DROP TABLE wedged"); err != nil {
				t.Errorf("the name cannot be dropped: %v", err)
			}
		})
	}
}

// A value the CATALOG cannot encode is not a value the statement may lose.
//
// PostgreSQL 17.11 stores an infinity in a double precision column (measured:
// `CREATE TABLE t AS SELECT 'Infinity'::float8` answers `Infinity`), and a
// manifest carrying one as a min/max bound cannot be marshalled at all — so
// for a CAST that NAMES the value the statement must succeed and the value
// must read back, and the bound is what gives way, not the row.
//
// The other half is #1082, and it is a refusal: `f * 10` over a float at the
// edge of the range is `22003 value out of range: overflow` on both engines
// now, so no infinity is computed and none reaches the writer. Those two
// spellings were entries 0 and 1 of the list below and they are the first
// subtest here.
func TestANonFiniteValueIsStoredAndItsBoundIsDropped(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(ctx, "fsrc", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO fsrc (id, f) VALUES (1, 1e308), (2, 2.0)"); err != nil {
		t.Fatal(err)
	}

	// An arithmetic result that leaves the type never reaches the writer at
	// all: PostgreSQL raises 22003 for it and so does this engine now, so a
	// CTAS over one fails instead of storing an infinity (#1082). `f * 10`
	// and `f * f` over 1e308 were this list's first two entries.
	for _, sql := range []string{
		`SELECT id, f * 10 AS x FROM fsrc`,
		`SELECT id, f * f AS x FROM fsrc`,
	} {
		t.Run("refused/"+sql, func(t *testing.T) {
			_, err := db.Query(ctx, "CREATE TABLE nonfiniteovf AS "+sql)
			if err == nil {
				t.Fatalf("%s stored an infinity; PostgreSQL 17.11 raises 22003 for it", sql)
			}
			if state := sqlerr.StateOf(err); state != "22003" {
				t.Errorf("%s raised SQLSTATE %s, want 22003: %v", sql, state, err)
			}
		})
	}

	for i, sql := range []string{
		`SELECT id, CAST('Infinity' AS FLOAT64) AS x FROM fsrc`,
		`SELECT id, CAST('-Infinity' AS FLOAT64) AS x FROM fsrc`,
		`SELECT id, CAST('NaN' AS FLOAT64) AS x FROM fsrc`,
		`SELECT id, CAST('Infinity' AS FLOAT32) AS x FROM fsrc`,
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			tbl := fmt.Sprintf("nonfinite%d", i)
			if _, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE %s AS %s", tbl, sql)); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			// The row is the query's row, infinity and all.
			ctasCompareQueries(t, ctx, db, sql+" ORDER BY id", "SELECT id, x FROM "+tbl+" ORDER BY id")
			// And the table is usable from here on: the name reads, drops and
			// is re-creatable.
			if _, err := db.Query(ctx, "DROP TABLE "+tbl); err != nil {
				t.Errorf("the table cannot be dropped: %v", err)
			}
		})
	}
}

// ctxHonouringStore refuses every call whose context is done, the way every
// real object store does. MemStore ignores context.Context entirely
// (`func (m *MemStore) Put(_ context.Context, …)`), which is why no fixture
// over a bare MemStore can see a cancel reach the store at all.
type ctxHonouringStore struct {
	objstore.Store
	puts     atomic.Int64
	cancelAt int64
	cancel   context.CancelFunc
}

func (s *ctxHonouringStore) Put(ctx context.Context, b, k string, r io.Reader, n int64, ct string) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	etag, err := s.Store.Put(ctx, b, k, r, n, ct)
	if err == nil && strings.HasPrefix(k, "tables/") && s.puts.Add(1) == s.cancelAt && s.cancel != nil {
		// The client goes away AFTER this file has landed.
		s.cancel()
	}
	return etag, err
}

func (s *ctxHonouringStore) Delete(ctx context.Context, b, k string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return s.Store.Delete(ctx, b, k)
}

func (s *ctxHonouringStore) List(ctx context.Context, b string, o objstore.ListOptions) ([]objstore.ObjectInfo, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return s.Store.List(ctx, b, o)
}

// A CANCEL that lands after a file has been uploaded still reclaims it
// (#1024 round-2 review B4).
//
// The reclaim used to run on the STATEMENT's own context — the one that was
// just cancelled — so `RetireObjects` could neither read the live catalog state
// nor issue its deletes, logged the retirement as deferred, and the bytes
// stayed forever. It runs on a fresh bounded context now.
//
// The store here honours the context, which is the whole of the fixture: the
// arc's own cancel cell cancels BEFORE the statement runs, so nothing was ever
// uploaded, and a MemStore would have accepted the upload either way.
func TestACancelAfterAFileLandsStillReclaimsIt(t *testing.T) {
	ctx := context.Background()
	mem := objstore.NewMemStore()
	st := &ctxHonouringStore{Store: mem, cancelAt: 1}
	db, err := Open(ctx, Config{Store: st, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "part", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "csrc", schema, nil); err != nil {
		t.Fatal(err)
	}
	// A PARTITIONED target, so the append writes one file per partition and
	// there is a file on the ground when the cancel lands.
	if err := db.CreateTable(ctx, "cdst", schema, []string{"part"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO csrc (id, part) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')`); err != nil {
		t.Fatal(err)
	}
	before := ctasObjectKeys(t, ctx, st)

	stmtCtx, cancel := context.WithCancel(ctx)
	st.puts.Store(0)
	st.cancel = cancel
	_, err = db.Query(stmtCtx, `INSERT INTO cdst SELECT id, part FROM csrc`)
	cancel()
	if err == nil {
		t.Fatal("the cancelled statement succeeded")
	}

	if n := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM cdst`); n != 0 {
		t.Errorf("the cancelled statement published %d rows", n)
	}
	ctasAssertNoNewObjects(t, ctx, st, before)
}

// A query-sourced write's GATHER is bounded, and past it the statement is a
// LOUD resource refusal (#1024 round-2 review B5).
//
// A statement whose source is a query reads the whole result before it writes.
// Without a bound that is the heap's business — measured at 860 MiB peak for a
// 99 MiB result, and no refusal, while the docs and ADR-0036 promised a 53400
// nobody could reach: `CollectSink.MaxBytes` was set only inside the
// coordinator, and the coordinator refuses these statements 0A000 before it
// gets there.
func TestAQuerySourcedWriteRefusesPastItsGatherBudget(t *testing.T) {
	ctx := context.Background()
	// A small budget so the fixture stays small; Config.MemoryBudget is the
	// documented way to move it.
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: 256 << 10, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if got := db.querySourcedWriteBudget(); got != 256<<10 {
		t.Fatalf("the budget is %d, want the configured 256 KiB", got)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "bsrc", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, 20000)
	for i := 0; i < 20000; i++ {
		rows = append(rows, map[string]any{"id": int64(i), "s": strings.Repeat("x", 64)})
	}
	ing := db.NewIngester("bsrc", schema, nil, ingest.Config{MaxBufferRows: len(rows) + 1})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "bdst", schema, nil); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ name, sql string }{
		{"CreateTableAsSelect", `CREATE TABLE btoobig AS SELECT id, s FROM bsrc`},
		{"InsertIntoSelect", `INSERT INTO bdst SELECT id, s FROM bsrc`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Query(ctx, tc.sql)
			if err == nil {
				t.Fatal("a result past the gather budget was accepted; the refusal the docs " +
					"and ADR-0036 promise must be reachable on a door that RUNS the statement")
			}
			if got := sqlerr.StateOf(err); got != physical.QueryLimitSQLState {
				t.Errorf("SQLSTATE %q, want %s: %v", got, physical.QueryLimitSQLState, err)
			}
			if !strings.Contains(err.Error(), "budget") {
				t.Errorf("the refusal does not name the bound: %v", err)
			}
			ctasAssertNoTable(t, ctx, db, "btoobig")
			if n := ctasScalar(t, ctx, db, `SELECT COUNT(*) AS c FROM bdst`); n != 0 {
				t.Errorf("the refused append wrote %d rows", n)
			}
		})
	}

	// The boundary: a result INSIDE the budget is written, not refused.
	if _, err := db.Query(ctx, `CREATE TABLE bsmall AS SELECT id, s FROM bsrc WHERE id < 100`); err != nil {
		t.Errorf("a result inside the budget was refused: %v", err)
	}
}
