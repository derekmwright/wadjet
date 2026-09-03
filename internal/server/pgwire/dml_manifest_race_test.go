package pgwire

// #691 — a DML statement that commits delete markers against a manifest that
// moved under it, on all three doors.
//
// The interleaving is ordinary and needs no goroutine, no sleep and no product
// seam: a DELETE/UPDATE/MERGE reads the manifest, scans the files it names,
// records WHICH ROW OF WHICH FILE it affected, and commits the markers at the
// end. Compaction between those two points rewrites exactly those files —
// RemoveFiles strips their markers, AddNewFiles publishes the merged output —
// so the statement's markers arrive naming files the table no longer has.
// AddDeleteMarkers accepted them (dm.FilePath was only ever a map key there),
// and the result was:
//
//	DELETE FROM t WHERE id = 1   →  "DELETE 1", and row 1 is STILL THERE
//	UPDATE t SET n = 99 …        →  "UPDATE 1", and the table holds 1:10 AND 1:99
//	MERGE … WHEN MATCHED …       →  "MERGE 1", and the row is duplicated
//
// every one of them reported as success.
//
// compactingKV makes it deterministic. Config.MetaKV is a public seam, so a
// decorating KV can run a full table rewrite inside the DML's own manifest
// read — after the stale value has been captured and before the statement
// scans a single file. That is the real interleaving, driven by the real
// compactor, inside a real db.Execute.

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// compactingKV runs `fire` once, inside the first Get whose key ENDS WITH
// `key` while it is armed, AFTER the stale value has been read. The suffix
// match is because a manifest key carries the catalog's cluster id as a
// prefix, which this test has no business knowing.
//
// The MetaKV interface is embedded rather than the concrete MemKV so that
// RevisionReader is deliberately NOT promoted: the catalog then reaches this
// Get instead of a revision probe, which is where the statement's read is.
type compactingKV struct {
	catalog.MetaKV
	key string

	mu     sync.Mutex
	armed  bool
	fired  bool
	always bool // fire on EVERY read, not only the first
	inFire bool // re-entrancy guard: the compactor reads this key too
	fire   func()
}

func (k *compactingKV) Get(key string) ([]byte, uint64, error) {
	val, rev, err := k.MetaKV.Get(key)
	k.mu.Lock()
	run := k.armed && !k.inFire && (k.always || !k.fired) && strings.HasSuffix(key, k.key)
	if run {
		// inFire, not just `fired`: the compactor reads this same key, so
		// without a re-entrancy guard an always-armed hook recurses until the
		// stack gives out. `fired` records that it happened at all.
		k.fired, k.inFire = true, true
	}
	f := k.fire
	k.mu.Unlock()
	if run && f != nil {
		f()
		k.mu.Lock()
		k.inFire = false
		k.mu.Unlock()
	}
	return val, rev, err
}

func (k *compactingKV) arm(fire func()) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.armed, k.fired, k.always, k.inFire, k.fire = true, false, false, false, fire
}

// armAlways fires on EVERY manifest read, so a statement can never commit and
// has to exhaust its retries. It is the fixture for the 40001 arm, which
// nothing reached while the hook fired once (review P2).
func (k *compactingKV) armAlways(fire func()) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.armed, k.fired, k.always, k.inFire, k.fire = true, false, true, false, fire
}

func (k *compactingKV) didFire() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.fired
}

// raceDB builds the arcb_pr fixture over a KV that can compact mid-statement.
func raceDB(t *testing.T) (*wadjet.DB, *compactingKV) {
	t.Helper()
	ctx := context.Background()
	mem := catalog.NewMemKV()
	hook := &compactingKV{MetaKV: mem, key: "manifest.arcb_pr"}
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  objstore.NewMemStore(),
		Bucket: "test",
		MetaKV: hook,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	for _, stmt := range censusFixture {
		if _, err := db.Query(ctx, stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}
	for _, stmt := range censusSeed {
		if _, err := db.Execute(ctx, stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}
	return db, hook
}

// armCompaction points the hook at a real full-table rewrite of arcb_pr.
func armCompaction(t *testing.T, db *wadjet.DB, hook *compactingKV) {
	t.Helper()
	ctx := context.Background()
	// DefaultConfig's DeleteGrace keeps the rewritten-away bytes in the store,
	// so the statement's scan of the OLD path still reads real rows — which is
	// what makes the wrong answer SILENT rather than an object-not-found.
	comp := compaction.New(db.Catalog(), slog.New(slog.DiscardHandler), compaction.DefaultConfig())
	hook.arm(func() {
		if _, err := comp.RewriteTable(ctx, "arcb_pr"); err != nil {
			t.Errorf("mid-statement compaction: %v", err)
		}
	})
}

// The key assertion for every case below: the statement's own report and the
// table it leaves behind agree with each other and with PostgreSQL, whatever
// the compactor did in the middle.
func TestDMLCommitsAgainstTheManifestItRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		tag   string
		table string
	}{
		{name: "DELETE", sql: "DELETE FROM arcb_pr WHERE id = 1",
			tag: "DELETE 1", table: "[2:20:b 3:30:c]"},
		{name: "DELETE many", sql: "DELETE FROM arcb_pr WHERE id > 1",
			tag: "DELETE 2", table: "[1:10:a]"},
		{name: "UPDATE", sql: "UPDATE arcb_pr SET n = 99 WHERE id = 1",
			tag: "UPDATE 1", table: "[1:99:a 2:20:b 3:30:c]"},
		{name: "UPDATE many", sql: "UPDATE arcb_pr SET n = 0 WHERE id > 0",
			tag: "UPDATE 3", table: "[1:0:a 2:0:b 3:0:c]"},
		{name: "MERGE update", tag: "MERGE 1", table: "[1:100:a 2:20:b 3:30:c]",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n"},
		{name: "MERGE delete", tag: "MERGE 1", table: "[2:20:b 3:30:c]",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN DELETE"},
		{name: "MERGE upsert", tag: "MERGE 2", table: "[1:100:a 2:20:b 3:30:c 4:400:y]",
			sql: "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
				"WHEN MATCHED THEN UPDATE SET n = s.n " +
				"WHEN NOT MATCHED THEN INSERT (id, n, name) VALUES (s.id, s.n, s.name)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := "tag=" + tc.tag + " table=" + tc.table

			t.Run("embedded", func(t *testing.T) {
				ctx := context.Background()
				db, hook := raceDB(t)
				armCompaction(t, db, hook)
				got := raceAnswer(t, ctx, db, tc.sql)
				assertRaceFired(t, hook)
				if got != want {
					t.Errorf("%s with a compaction inside it\n  answered %s\n  want     %s", tc.sql, got, want)
				}
			})

			for _, door := range []struct {
				name string
				mode pgx.QueryExecMode
			}{
				{"simple", pgx.QueryExecModeSimpleProtocol},
				{"extended", pgx.QueryExecModeExec},
			} {
				t.Run(door.name, func(t *testing.T) {
					ctx := context.Background()
					db, hook := raceDB(t)
					srv := NewServer(db, Config{}, nil)
					if err := srv.Start("127.0.0.1:0"); err != nil {
						t.Fatal(err)
					}
					defer srv.Shutdown()
					conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
					if err != nil {
						t.Fatalf("pgx connect: %v", err)
					}
					defer conn.Close(ctx)

					armCompaction(t, db, hook)
					tag, execErr := censusWireExec(ctx, conn, tc.sql, door.mode)
					if execErr != nil {
						t.Fatalf("%s: %v", tc.sql, execErr)
					}
					assertRaceFired(t, hook)

					// The tag on the extended protocol is #816's territory and
					// is pinned in the census; what this gate owns is the
					// COUNT and the table, so compare the count alone there.
					cols, rows, err := censusWireRows(ctx, conn, censusDigestSQL["pr"], door.mode)
					if err != nil {
						t.Fatalf("digest: %v", err)
					}
					if got := censusDigest(cols, rows); got != tc.table {
						t.Errorf("%s with a compaction inside it left %s, want %s", tc.sql, got, tc.table)
					}
					if door.mode == pgx.QueryExecModeSimpleProtocol && tag != tc.tag {
						t.Errorf("%s with a compaction inside it reported %q, want %q", tc.sql, tag, tc.tag)
					}
				})
			}
		})
	}
}

func raceAnswer(t *testing.T, ctx context.Context, db *wadjet.DB, sql string) string {
	t.Helper()
	res, err := db.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	after, err := db.Query(ctx, censusDigestSQL["pr"])
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	return "tag=" + commandTag(res.Command, res.RowsAffected) + " table=" + censusDigest(after.Columns, after.Rows)
}

// assertRaceFired is method 10: a "cannot happen" needs a fixture where it is
// attempted, and this gate is worthless if the compaction never ran. Without
// it a change to the manifest read path — a revision probe that skips Get, a
// cache hit — would silently turn every case here into an ordinary DML test
// that passes for the wrong reason.
func assertRaceFired(t *testing.T, hook *compactingKV) {
	t.Helper()
	if !hook.didFire() {
		t.Fatal("the mid-statement compaction never ran: this gate proved nothing")
	}
}

// The other half of #691: a marker for a file the manifest no longer holds is
// REFUSED, and it is refused by the catalog rather than by the DML layer, so
// no future writer can commit one by taking a different route.
func TestCommitDMLRefusesAMarkerForAFileTheManifestLost(t *testing.T) {
	ctx := context.Background()
	db, _ := raceDB(t)
	cat := db.Catalog()

	manifest, err := cat.GetManifest(ctx, "arcb_pr")
	if err != nil {
		t.Fatal(err)
	}
	var live string
	for _, p := range manifest.Partitions {
		for _, f := range p.Files {
			live = f.Path
		}
	}
	if live == "" {
		t.Fatal("fixture has no files")
	}

	// A marker for a file that IS there is accepted.
	if err := cat.CommitDML(ctx, "arcb_pr", nil, []catalog.DeleteMarker{
		{FilePath: live, RowIndices: []int64{0}},
	}); err != nil {
		t.Fatalf("committing a marker for a live file: %v", err)
	}

	// A marker for a file that is NOT is refused, and named.
	err = cat.CommitDML(ctx, "arcb_pr", nil, []catalog.DeleteMarker{
		{FilePath: "tables/arcb_pr/no_such_file.parquet", RowIndices: []int64{0}},
	})
	if err == nil {
		t.Fatal("a marker naming a file the manifest does not hold was accepted")
	}
	if !strings.Contains(err.Error(), "no_such_file.parquet") {
		t.Errorf("the refusal does not name the file: %v", err)
	}

	// And it did not half-commit: the manifest still holds exactly the one
	// marker the accepted call put there.
	after, err := cat.GetManifest(ctx, "arcb_pr")
	if err != nil {
		t.Fatal(err)
	}
	for _, dm := range after.DeleteMarkers {
		if dm.FilePath != live {
			t.Errorf("manifest carries a marker for %q after the refusal", dm.FilePath)
		}
	}
}
