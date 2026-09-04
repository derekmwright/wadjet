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
