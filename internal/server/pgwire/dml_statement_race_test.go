package pgwire

// #835 — two DML statements over the same row, on all three doors.
//
// #691 closed statement-vs-COMPACTION and ADR-0030 said in its own words what
// it left open: "Two writers racing each other … both succeed, and the second
// one's markers are valid because the files did not move … Closing it needs a
// conflict rule over ROWS, which this record does not decide." This is that
// rule's gate.
//
// The interleaving needs no goroutine and no sleep, for the reason the #691
// gate does not: `compactingKV` runs a hook inside a statement's own manifest
// read, AFTER the stale bytes are captured and BEFORE it scans a file. #691
// points that hook at a compaction; here it runs ANOTHER DML STATEMENT to
// completion, which is exactly the window — B commits, then A scans and
// commits against the manifest B replaced.
//
// Measured on v0.18.22, before the row-level conflict rule:
//
//	UPDATE n=111 WHERE id=1 ‖ UPDATE n=222 WHERE id=1 → 1:111 AND 1:222, both "UPDATE 1"
//	UPDATE n=111 WHERE id=1 ‖ DELETE      WHERE id=1 → the deleted row is BACK
//	DELETE      WHERE id=1 ‖ UPDATE n=222 WHERE id=1 → "DELETE 1", row still readable
//	UPDATE n=111 WHERE id=1 ‖ MERGE … UPDATE SET n=s.n → 1:100 AND 1:111
//
// Every one a success tag over a wrong table.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/wadjet"
)

// statementRace is one interleaving: `inner` runs to completion inside
// `outer`'s manifest read.
//
// `tables` is the set of table states PostgreSQL could produce for the two
// statements in EITHER serial order — one entry when both orders agree.
// `tags` is the corresponding set of outer-statement tags. The pair is what
// makes this a serializability assertion rather than a golden value: the
// engine may pick either order, and it may not pick a third thing.
type statementRace struct {
	name   string
	outer  string
	inner  string
	tables []string
	tags   []string
	// redone says whether the outer statement must have redone itself. Rows
	// alone cannot tell "committed first try because the rows were disjoint"
	// from "conflicted and redid": both leave the same table (item 11).
	redone bool
}

func statementRaces() []statementRace {
	return []statementRace{
		// The issue's own shape. Serially: 222-then-111 leaves 1:111, and
		// 111-then-222 leaves 1:222. The key is present ONCE either way.
		{name: "UPDATE || UPDATE same row",
			outer:  "UPDATE arcb_pr SET n = 111 WHERE id = 1",
			inner:  "UPDATE arcb_pr SET n = 222 WHERE id = 1",
			tables: []string{"[1:111:a 2:20:b 3:30:c]", "[1:222:a 2:20:b 3:30:c]"},
			tags:   []string{"UPDATE 1", "UPDATE 0"},
			redone: true},

		// Read-modify-write, which is the shape a lost update is usually
		// reported as. Serially the row ends at 12 in either order; a
		// statement that keeps the value it read leaves 11.
		{name: "UPDATE n=n+1 || UPDATE n=n+1 same row",
			outer:  "UPDATE arcb_pr SET n = n + 1 WHERE id = 1",
			inner:  "UPDATE arcb_pr SET n = n + 1 WHERE id = 1",
			tables: []string{"[1:12:a 2:20:b 3:30:c]"},
			tags:   []string{"UPDATE 1"},
			redone: true},

		// Either order ends with the row GONE. The tag depends on the order:
		// delete-then-update is UPDATE 0, update-then-delete is UPDATE 1.
		{name: "UPDATE || DELETE same row",
			outer:  "UPDATE arcb_pr SET n = 111 WHERE id = 1",
			inner:  "DELETE FROM arcb_pr WHERE id = 1",
			tables: []string{"[2:20:b 3:30:c]"},
			tags:   []string{"UPDATE 0", "UPDATE 1"},
			redone: true},

		{name: "DELETE || UPDATE same row",
			outer:  "DELETE FROM arcb_pr WHERE id = 1",
			inner:  "UPDATE arcb_pr SET n = 222 WHERE id = 1",
			tables: []string{"[2:20:b 3:30:c]"},
			tags:   []string{"DELETE 1"},
			redone: true},

		// PostgreSQL's answer for the second DELETE of a row already gone is
		// DELETE 0, not DELETE 1.
		{name: "DELETE || DELETE same row",
			outer:  "DELETE FROM arcb_pr WHERE id = 1",
			inner:  "DELETE FROM arcb_pr WHERE id = 1",
			tables: []string{"[2:20:b 3:30:c]"},
			tags:   []string{"DELETE 0"},
			redone: true},

		{name: "UPDATE || MERGE same row",
			outer:  "UPDATE arcb_pr SET n = 111 WHERE id = 1",
			inner:  "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n",
			tables: []string{"[1:111:a 2:20:b 3:30:c]", "[1:100:a 2:20:b 3:30:c]"},
			tags:   []string{"UPDATE 1"},
			redone: true},

		{name: "MERGE || UPDATE same row",
			outer:  "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.n",
			inner:  "UPDATE arcb_pr SET n = 222 WHERE id = 1",
			tables: []string{"[1:100:a 2:20:b 3:30:c]"},
			tags:   []string{"MERGE 1"},
			redone: true},

		// THE BOUNDARY, from the other side. Two statements over DIFFERENT
		// rows are not a conflict: both commit on their first attempt, and a
		// rule that failed them would be a lock rather than a conflict rule.
		{name: "UPDATE || UPDATE different rows",
			outer:  "UPDATE arcb_pr SET n = 111 WHERE id = 1",
			inner:  "UPDATE arcb_pr SET n = 222 WHERE id = 2",
			tables: []string{"[1:111:a 2:222:b 3:30:c]"},
			tags:   []string{"UPDATE 1"},
			redone: false},

		// Different rows of the same file, where the marker sets are disjoint
		// but the FILE is shared — the case a file-level rule would fail.
		{name: "DELETE || DELETE different rows",
			outer:  "DELETE FROM arcb_pr WHERE id = 1",
			inner:  "DELETE FROM arcb_pr WHERE id = 3",
			tables: []string{"[2:20:b]"},
			tags:   []string{"DELETE 1"},
			redone: false},

		// An outer statement whose redo finds NOTHING left to do.
		{name: "UPDATE || DELETE the whole table",
			outer:  "UPDATE arcb_pr SET n = 111 WHERE id > 0",
			inner:  "DELETE FROM arcb_pr WHERE id > 0",
			tables: []string{"[]"},
			tags:   []string{"UPDATE 0"},
			redone: true},

		// A statement whose predicate matches nothing conflicts with nothing:
		// it mints no marker at all.
		{name: "UPDATE no rows || UPDATE same row",
			outer:  "UPDATE arcb_pr SET n = 111 WHERE id = 99",
			inner:  "UPDATE arcb_pr SET n = 222 WHERE id = 1",
			tables: []string{"[1:222:a 2:20:b 3:30:c]"},
			tags:   []string{"UPDATE 0"},
			redone: false},
	}
}

// TestConcurrentDMLLeavesOneOfTwoSerialOrders is #835's gate.
func TestConcurrentDMLLeavesOneOfTwoSerialOrders(t *testing.T) {
	for _, tc := range statementRaces() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Run("embedded", func(t *testing.T) {
				ctx := context.Background()
				db, hook := raceDB(t)
				before := db.DMLRedos()
				armStatement(t, db, hook, tc.inner)

				res, err := db.Execute(ctx, tc.outer)
				if err != nil {
					t.Fatalf("%s: %v", tc.outer, err)
				}
				assertRaceFired(t, hook)
				assertRace(t, tc, commandTag(res.Command, res.RowsAffected),
					raceTable(t, ctx, db), db.DMLRedos()-before)
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

					// The interfering statement goes through the SAME door, on
					// its own connection: this is two clients, not a library
					// call dressed as one.
					inner, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
					if err != nil {
						t.Fatalf("pgx connect: %v", err)
					}
					defer inner.Close(ctx)

					before := db.DMLRedos()
					hook.arm(func() {
						if _, err := censusWireExec(ctx, inner, tc.inner, door.mode); err != nil {
							t.Errorf("interfering %s: %v", tc.inner, err)
						}
					})

					tag, execErr := censusWireExec(ctx, conn, tc.outer, door.mode)
					if execErr != nil {
						t.Fatalf("%s: %v", tc.outer, execErr)
					}
					assertRaceFired(t, hook)

					cols, rows, qErr := censusWireRows(ctx, conn, censusDigestSQL["pr"], door.mode)
					if qErr != nil {
						t.Fatalf("digest: %v", qErr)
					}
					assertRace(t, tc, tag, censusDigest(cols, rows), db.DMLRedos()-before)
				})
			}
		})
	}
}

// assertRace holds the outer statement to ONE OF the serial orders — never a
// third table, never a tag that names a different one — and to whether it had
// to redo itself.
func assertRace(t *testing.T, tc statementRace, tag, table string, redos uint64) {
	t.Helper()
	if !containsAnswer(tc.tables, table) {
		t.Errorf("%s\n  with %q inside it\n  left  %s\n  want one of %v",
			tc.outer, tc.inner, table, tc.tables)
	}
	if !containsAnswer(tc.tags, tag) {
		t.Errorf("%s\n  with %q inside it\n  reported %q\n  want one of %v",
			tc.outer, tc.inner, tag, tc.tags)
	}
	// The boundary, asserted rather than assumed. A conflict rule that fired
	// on every concurrent statement would leave the same rows here and be a
	// lock; one that never fired would leave the same rows on the disjoint
	// cases and be nothing at all.
	switch {
	case tc.redone && redos == 0:
		t.Errorf("%s\n  with %q inside it: the statement never redid itself, so it "+
			"cannot have observed the conflict — the rows are right by accident",
			tc.outer, tc.inner)
	case !tc.redone && redos != 0:
		t.Errorf("%s\n  with %q inside it: %d redo(s) over rows that do not "+
			"overlap — the conflict rule is a lock, not a conflict rule",
			tc.outer, tc.inner, redos)
	}
}

func containsAnswer(all []string, want string) bool {
	for _, s := range all {
		if s == want {
			return true
		}
	}
	return false
}

// armStatement points the manifest-read hook at another DML STATEMENT.
func armStatement(t *testing.T, db *wadjet.DB, hook *compactingKV, sql string) {
	t.Helper()
	ctx := context.Background()
	hook.arm(func() {
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Errorf("interfering %s: %v", sql, err)
		}
	})
}

func raceTable(t *testing.T, ctx context.Context, db *wadjet.DB) string {
	t.Helper()
	after, err := db.Query(ctx, censusDigestSQL["pr"])
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	return censusDigest(after.Columns, after.Rows)
}

// TestConcurrentDMLStormReports40001: a statement that loses the ROW race on
// every attempt reports the retryable class and writes nothing, exactly as one
// that loses the file race does (#691's P2 arm, over the new conflict).
func TestConcurrentDMLStormReports40001(t *testing.T) {
	ctx := context.Background()
	db, hook := raceDB(t)

	// A fresh interfering UPDATE on EVERY manifest read, so the outer
	// statement's markers always name a row somebody else just superseded.
	n := 0
	hook.armAlways(func() {
		n++
		if _, err := db.Execute(ctx, "UPDATE arcb_pr SET n = 1000 + "+strconv.Itoa(n)+" WHERE id = 1"); err != nil {
			t.Errorf("interfering update %d: %v", n, err)
		}
	})

	_, err := db.Execute(ctx, "UPDATE arcb_pr SET n = 111 WHERE id = 1")
	if err == nil {
		t.Fatal("the statement committed although every attempt lost the row race")
	}
	if got := sqlerr.StateOf(err); got != "40001" {
		t.Errorf("SQLSTATE %q, want 40001 serialization_failure (err: %v)", got, err)
	}
	// It wrote nothing of its own: the value is the interfering statement's.
	table := raceTable(t, ctx, db)
	if strings.Contains(table, "1:111:a") {
		t.Errorf("a refused statement published its rows: %s", table)
	}
	// And the key is present exactly once whatever the interference did.
	if got := keyCount(table, "1"); got != 1 {
		t.Errorf("key 1 appears %d times in %s, want 1", got, table)
	}
}

// TestCommitDMLRefusesAMarkerForARowAlreadySuperseded pins the catalog's half
// directly, the way #691's file half is pinned: the rule lives in the catalog
// so no future writer can commit past it by taking a different route.
func TestCommitDMLRefusesAMarkerForARowAlreadySuperseded(t *testing.T) {
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

	if err := cat.CommitDML(ctx, "arcb_pr", nil, []catalog.DeleteMarker{
		{FilePath: live, RowIndices: []int64{0}},
	}); err != nil {
		t.Fatalf("committing a marker for a live row: %v", err)
	}

	// The same row again is the conflict.
	err = cat.CommitDML(ctx, "arcb_pr", nil, []catalog.DeleteMarker{
		{FilePath: live, RowIndices: []int64{0}},
	})
	if !errors.Is(err, catalog.ErrDMLRowSuperseded) {
		t.Fatalf("a marker for an already-superseded row was accepted: %v", err)
	}
	if !strings.Contains(err.Error(), "row 0") {
		t.Errorf("the refusal does not name the row: %v", err)
	}

	// A DIFFERENT row of the SAME file is not: the rule is over rows, not
	// files, and unrelated concurrent DML must still commit.
	if err := cat.CommitDML(ctx, "arcb_pr", nil, []catalog.DeleteMarker{
		{FilePath: live, RowIndices: []int64{1}},
	}); err != nil {
		t.Fatalf("committing a marker for a different row of the same file: %v", err)
	}

	// A batch that overlaps in ONE row is refused WHOLE — the statement is
	// redone, so a partial commit would leave rows removed by a statement
	// that then reports a different count.
	err = cat.CommitDML(ctx, "arcb_pr", nil, []catalog.DeleteMarker{
		{FilePath: live, RowIndices: []int64{1, 2}},
	})
	if !errors.Is(err, catalog.ErrDMLRowSuperseded) {
		t.Fatalf("a partially overlapping marker set was accepted: %v", err)
	}
	after, err := cat.GetManifest(ctx, "arcb_pr")
	if err != nil {
		t.Fatal(err)
	}
	for _, dm := range after.DeleteMarkers {
		for _, idx := range dm.RowIndices {
			if idx == 2 {
				t.Error("the refused batch half-committed: row 2 is marked")
			}
		}
	}
}

// keyCount counts the rows of a census digest whose first column is key.
func keyCount(digest, key string) int {
	n := 0
	for _, row := range strings.Fields(strings.Trim(digest, "[]")) {
		if strings.HasPrefix(row, key+":") {
			n++
		}
	}
	return n
}
