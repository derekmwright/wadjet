package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestADMLSubquerysResultIsBounded is the gate for the one thing a subquery on
// a WRITE door can do that no census cell can see: read an unbounded relation
// into coordinator memory.
//
// The query path bounds an IN-set with `WADJET_IN_SET_MAX`, but that bound
// guards the PLANNER's inlining of the set into filter text
// (physical/in_subquery_set.go), and the DML door takes no such path — it
// hands `expr.InSubquery` a runner and the evaluator builds a membership map
// from whatever comes back. So `DELETE FROM t WHERE id IN (SELECT id FROM
// huge)` materialised every id with no budget and no cap, on a door that
// before this arc had no set at all.
//
// The bound is enforced in the runner now, and it REFUSES rather than
// truncating: a set short by one row deletes the wrong rows. The control is
// the half that says the bound is a bound and not a ban — one row under it
// still answers, and the statement writes.
//
// The env var is the same knob with the same meaning at both sites (how many
// rows a subquery result may become a set of), which is why it is one knob;
// what it protects differs — plan text there, a hash map here.
func TestADMLSubquerysResultIsBounded(t *testing.T) {
	t.Setenv("WADJET_IN_SET_MAX", "1")
	ctx := context.Background()
	db := newCensusDB(t)

	// arcb_src has TWO rows, so this is one past the bound.
	_, err := db.Execute(ctx, "DELETE FROM arcb_pr WHERE id IN (SELECT id FROM arcb_src)")
	if err == nil {
		t.Fatal("a subquery past the row bound was not refused; a write door's set is " +
			"otherwise unbounded (WADJET_IN_SET_MAX reaches only the planner's inlining)")
	}
	if got := sqlerr.StateOf(err); got != "54000" {
		t.Errorf("SQLSTATE %q, want 54000 (program_limit_exceeded): a documented refusal is "+
			"a promise about the CODE\n  %v", got, err)
	}
	if !strings.Contains(err.Error(), "WADJET_IN_SET_MAX") {
		t.Errorf("the refusal does not name the knob that decides it: %v", err)
	}
	assertArcbPr(t, ctx, db, "1 2 3",
		"the refused statement must write NOTHING — a set short by one row deletes "+
			"the wrong rows, so this refuses rather than truncating")

	// The control: one row is inside the bound and the statement writes.
	if _, err := db.Execute(ctx,
		"DELETE FROM arcb_pr WHERE id IN (SELECT id FROM arcb_src WHERE id = 1)"); err != nil {
		t.Fatalf("a one-row subquery is inside the bound and must answer: %v", err)
	}
	assertArcbPr(t, ctx, db, "2 3", "the control must have deleted row 1")
}

// TestTheDMLBoundIsOnTheSetAndNotOnEveryDMLSubquery is the other half of the
// bound: it is a bound on a SET, and only one of the four subquery constructs
// asks for one.
//
// EXISTS asks whether there is A row. A scalar subquery is an ERROR past ONE.
// Bounding those the way an IN-set is bounded charged them for rows neither
// would ever look at, and the charge was a WRONG ANSWER twice over:
//
//	DELETE ... WHERE EXISTS (SELECT 1 FROM big)   refused (54000) where
//	                                              PostgreSQL answers
//	DELETE ... WHERE n < (SELECT n FROM big)      reported 54000 where this
//	                                              engine's own rule is 21000
//
// The second is the worse one, because 54000 is a resource complaint and
// 21000 is a statement about the data: a client that reads the code learns
// the wrong thing about its own query. Two rows is all the cardinality rule
// needs, and it needs them whatever the bound says.
//
// The read is bounded at the construct now (plansql.AppendRowLimit), so
// neither reaches the runner's cap at all. IN keeps it — IN really does need
// the set — which the test above gates.
//
// WADJET_IN_SET_MAX=1 with a two-row arcb_src puts every cell here one row
// past the bound, which is how the whole file can run on the census fixture.
func TestTheDMLBoundIsOnTheSetAndNotOnEveryDMLSubquery(t *testing.T) {
	t.Setenv("WADJET_IN_SET_MAX", "1")
	ctx := context.Background()

	// An EXISTS past the bound ANSWERS, on each of the three doors.
	answers := []struct {
		name, sql, want, why string
	}{
		{"delete EXISTS", "DELETE FROM arcb_pr WHERE EXISTS (SELECT 1 FROM arcb_src)",
			"", "arcb_src has rows, so every arcb_pr row goes"},
		{"delete NOT EXISTS", "DELETE FROM arcb_pr WHERE NOT EXISTS (SELECT 1 FROM arcb_src)",
			"1 2 3", "arcb_src has rows, so NOT EXISTS matches nothing"},
		{"delete correlated EXISTS", "DELETE FROM arcb_pr WHERE EXISTS " +
			"(SELECT 1 FROM arcb_src s WHERE s.id <> arcb_pr.id)",
			"", "every outer row has a non-matching src row, and rows 2 and 3 have two"},
		{"update EXISTS", "UPDATE arcb_pr SET n = 0 WHERE EXISTS (SELECT 1 FROM arcb_src)",
			"1 2 3", "an UPDATE writes values, not rows — the ids stay"},
		{"merge EXISTS", "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
			"WHEN MATCHED AND EXISTS (SELECT 1 FROM arcb_src) THEN DELETE",
			"2 3", "arcb_src matches arcb_pr row 1 only"},
	}
	for _, c := range answers {
		t.Run(c.name, func(t *testing.T) {
			db := newCensusDB(t)
			if _, err := db.Execute(ctx, c.sql); err != nil {
				t.Fatalf("an EXISTS asks whether there is A row and must not be charged for "+
					"a set it never builds; this refused past WADJET_IN_SET_MAX: %v", err)
			}
			assertArcbPr(t, ctx, db, c.want, c.why)
		})
	}

	// A multi-row scalar subquery past the bound is 21000, not 54000 — on
	// each of the three doors, and correlated as well as not.
	cardinality := []struct{ name, sql string }{
		{"delete scalar", "DELETE FROM arcb_pr WHERE n < (SELECT n FROM arcb_src)"},
		{"delete correlated scalar", "DELETE FROM arcb_pr WHERE n < " +
			"(SELECT s.n FROM arcb_src s WHERE s.id <> arcb_pr.id)"},
		{"update scalar", "UPDATE arcb_pr SET n = 0 WHERE n < (SELECT n FROM arcb_src)"},
		{"merge scalar", "MERGE INTO arcb_pr AS t USING arcb_src AS s ON t.id = s.id " +
			"WHEN MATCHED AND t.n < (SELECT n FROM arcb_src) THEN DELETE"},
	}
	for _, c := range cardinality {
		t.Run(c.name, func(t *testing.T) {
			db := newCensusDB(t)
			_, err := db.Execute(ctx, c.sql)
			if err == nil {
				t.Fatal("a scalar subquery over two rows must raise, never take the first row")
			}
			if got := sqlerr.StateOf(err); got != "21000" {
				t.Errorf("SQLSTATE %q, want 21000 (cardinality_violation): the second row "+
					"decides this, and it decides it before any resource bound does — "+
					"54000 here tells a client its query is too big when what is wrong "+
					"is its meaning\n  %v", got, err)
			}
			assertArcbPr(t, ctx, db, "1 2 3", "the refused statement writes nothing")
		})
	}
}

// assertArcbPr reads arcb_pr's ids back and compares them to a
// space-separated list.
func assertArcbPr(t *testing.T, ctx context.Context, db *wadjet.DB, want, why string) {
	t.Helper()
	res, err := db.Query(ctx, "SELECT id FROM arcb_pr ORDER BY id")
	if err != nil {
		t.Fatalf("reading arcb_pr back: %v", err)
	}
	ids := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		ids = append(ids, fmt.Sprint(r["id"]))
	}
	if got := strings.Join(ids, " "); got != want {
		t.Errorf("arcb_pr is [%s], want [%s] — %s", got, want, why)
	}
}
