package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// E6-P — why `#852/correlated_scalar_over_a_derived_table` both answers and
// refuses at 512 KiB, and what part of that IS deterministic.
//
// E6 measured five replicates at 2 answered / 3 refused, could not explain it,
// and dropped the cell's budgeted arm with "pinning either outcome would be
// pinning a coin flip" (ADR-0027: a spill is a condition, not a shape). A
// census that cannot say what a shape does is a census with a hole in it, so
// this is the localization, written as the assertion it supports.
//
// THE MECHANISM, measured. The refusal is always the same figure —
// `used=535822, requested=72, budget=524288, of which forced=535822 by "spill
// tracking"`, with `build_rows=0, batches=0`, so the hash join has built
// nothing and the whole floor is somebody else's. A stack probe on
// memory.SpillManager.TrackBatch names that somebody: HashAggregate units on a
// TrackingOnlyView of the same tracker — the primary plus one morsel CLONE per
// partition of the key space Pipeline.runParallel hands out — each charging
// ~107 KB for a FORTY-ROW input, because groupMemoryUsage counts the hash
// table's presized CAPACITY and a clone presizes like the primary.
//
// The CHARGE PER UNIT is the constant; the NUMBER OF UNITS follows the budget,
// and the round-1 review measured all three (10 replicates each):
//
//	512 KiB  used=535822  = 5 x 107097   6 answered / 4 refused
//	384 KiB  used=428725  = 4 x 107097   1 answered / 9 refused
//	256 KiB  used=321628  = 3 x 107097   0 answered / 10 refused
//
// So "five units" is this budget's number and not the shape's property. What
// is the shape's property is that the floor is N x one hash table for an input
// of forty rows.
//
// Each unit releases its charge at HashAggregate.Close, which the parallel
// emit runs on that unit's OWN goroutine as it finishes draining —
// CONCURRENTLY with the downstream hash join's first arrival reservation. So
// the floor the join is admitted against is "how many units have finished
// draining by now", which is a scheduling fact and nothing else. Measured:
// with GOMAXPROCS=1 the query refuses on 8 of 8 replicates; with
// WADJET_PARALLEL_EMIT=0 it refuses on 7 of 8; with the fan-out itself off it
// answers on 8 of 8.
//
// That last one is the deterministic half and it is what this gate asserts:
// with the morsel fan-out off, the plan fits its budget every time. The
// day the per-clone charge stops multiplying — a clone sized to the key space
// it owns, or a charge released when a unit's state is merged rather than when
// its goroutine ends — the ON arm below stops refusing too, and the log line
// says so.
//
// Both switches are internal/engine/exec's; the deferral is in the arc's
// report.
func TestAMorselFannedAggregateIsWhatPutsTheCorrelatedScalarPastItsBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate runs a budgeted engine over the multikey fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	const sql = `SELECT COUNT(*) AS n FROM mk_outer a WHERE a.n = (` +
		`SELECT MAX(b.n) FROM (SELECT s, n FROM mk_inner) b WHERE b.n = a.n)`
	const want = "n=int64:40" // live PostgreSQL 17 over this fixture

	var partitioned *optswitch.Toggle
	for _, tog := range optswitch.All() {
		if tog.Name == "partitioned-agg" {
			partitioned = tog
		}
	}
	if partitioned == nil {
		t.Fatal("the partitioned-agg toggle is not registered: this gate's subject is gone")
	}

	// THE ASSERTION. With the aggregate's morsel fan-out off there is ONE
	// hash table instead of five, and the plan fits 512 KiB on every
	// replicate. Five, because one passing budgeted run proves nothing
	// (ADR-0027 §5).
	prev := partitioned.Set(false)
	// t.Cleanup, not a paired Set at the end: na2Standalone and na2Run both
	// t.Fatalf on their own errors, and every one of those paths used to leave
	// the fan-out OFF for the rest of the package binary (round-1 review P1).
	t.Cleanup(func() { partitioned.Set(prev) })
	db := na2Standalone(t, ctx, 512*1024)
	for i := 0; i < 5; i++ {
		got, err := na2Run(tmdRunSingle(ctx, db, sql))
		if err != nil {
			t.Fatalf("replicate %d refused with the fan-out OFF: %v\n"+
				"  the serial plan's floor was under 512 KiB when this was written, so either\n"+
				"  the floor moved or the fan-out is no longer what puts it over\n  SQL: %s",
				i, err, sql)
		}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("replicate %d: %v, want [%s] (live PostgreSQL 17)", i, got, want)
		}
	}
	partitioned.Set(prev)

	// THE MEASUREMENT, logged rather than asserted, because what it records is
	// a race and pinning either side of one is pinning a coin flip. What IS
	// asserted is the refusal's own CONTENT: if this shape refuses, it refuses
	// because of tracked aggregate state and not for some other reason.
	answered, refused := 0, 0
	dbOn := na2Standalone(t, ctx, 512*1024)
	for i := 0; i < 5; i++ {
		got, err := na2Run(tmdRunSingle(ctx, dbOn, sql))
		switch {
		case err == nil:
			answered++
			if len(got) != 1 || got[0] != want {
				t.Errorf("replicate %d answered %v, want [%s] — a WRONG answer under a budget "+
					"is not the same finding as a refusal", i, got, want)
			}
		case strings.Contains(err.Error(), `by "spill tracking"`):
			refused++
		default:
			t.Errorf("replicate %d refused for a reason this gate does not know: %v\n"+
				"  the measured refusal is the aggregate's tracked group state "+
				"(forced=… by \"spill tracking\"); a different one is a different defect",
				i, err)
		}
	}
	t.Logf("with the morsel fan-out ON at 512 KiB: %d answered, %d refused past the budget "+
		"(the race between the parallel emit's per-unit release and the join's first "+
		"reservation). With it OFF: 5 answered, 0 refused.", answered, refused)
	if refused == 0 {
		t.Logf("NOTE: the ON arm answered every replicate. If that holds, the per-clone " +
			"charge no longer multiplies the floor and the census cell's budgeted arm can " +
			"be re-armed (arcD5Cell.skipBudgetedArm on #852/correlated_scalar_over_a_" +
			"derived_table).")
	}
}
