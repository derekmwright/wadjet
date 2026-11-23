// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// E6-P — `#852/correlated_scalar_over_a_derived_table` at 512 KiB answers
// PostgreSQL's row with the aggregate's morsel fan-out OFF and ON.
//
// HISTORY. E6 measured five replicates at 2 answered / 3 refused, could not
// explain it, and dropped the cell's budgeted arm with "pinning either outcome
// would be pinning a coin flip" (ADR-0027: a spill is a condition, not a
// shape). The localization that followed found the mechanism: the refusal was
// always `used=535822, requested=72, budget=524288, of which forced=535822 by
// "spill tracking"` with `build_rows=0`, the forced floor being HashAggregate
// units on a TrackingOnlyView of the same tracker — the primary plus one
// morsel CLONE per partition of the key space Pipeline.runParallel hands out —
// each charging ~107 KB for a FORTY-ROW input (5 x 107,097 at 512 KiB, 4 at
// 384 KiB, 3 at 256 KiB), because groupMemoryUsage counts the presized
// CAPACITY and a clone presized like the primary. Each unit released at
// HashAggregate.Close on its own drain goroutine, concurrently with the
// downstream hash join's first arrival reservation, so the verdict was how
// many units had finished draining by then: a scheduling fact.
//
// That per-clone charge is gone. Under a budget the aggregate's presize is
// bounded by its input estimate (#1706, ADR-0006 2026-11-22) and so is its
// group-state pool's first chunk (2026-11-23): a clone of this statement now
// charges about 9.4 KB (gsPool 576, string index 8,448, for one group of a
// 24-row estimate), five of them under 50 KB, so the join's first reservation
// fits whichever releases land first. Measured at #1706 line 1706e: the ON
// arm answered every replicate in 30 fresh processes at each of GOMAXPROCS 1,
// 2, 4 and the default, and with the two bounds reverted it refuses again with
// the old figure. So both arms ASSERT the answer now; the census cell's
// budgeted arm is re-armed (arcD5Cell `#852/correlated_scalar_over_a_derived_table`).
//
// The OFF arm stays: it is the deterministic half the localization asserted
// (one hash table instead of five), and a refusal there would mean the serial
// plan's own floor moved.
func TestAMorselFannedAggregateNoLongerPutsTheCorrelatedScalarPastItsBudget(t *testing.T) {
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

	// Five replicates per arm, because one passing budgeted run proves nothing
	// (ADR-0027 §5).
	replicates := func(arm string) {
		t.Helper()
		db := na2Standalone(t, ctx, 512*1024)
		for i := 0; i < 5; i++ {
			got, err := na2Run(tmdRunSingle(ctx, db, sql))
			if err != nil {
				t.Fatalf("fan-out %s, replicate %d refused: %v\n"+
					"  a refusal forced by \"spill tracking\" here is the per-clone aggregate charge "+
					"of #1706 back (each clone charging its presized capacity for a forty-row input)\n  SQL: %s",
					arm, i, err, sql)
			}
			if len(got) != 1 || got[0] != want {
				t.Fatalf("fan-out %s, replicate %d: %v, want [%s] (live PostgreSQL 17)", arm, i, got, want)
			}
		}
	}

	// THE SERIAL ARM. t.Cleanup, not a paired Set at the end: na2Standalone
	// and na2Run both t.Fatalf on their own errors, and every one of those
	// paths used to leave the fan-out OFF for the rest of the package binary
	// (round-1 review P1).
	prev := partitioned.Set(false)
	t.Cleanup(func() { partitioned.Set(prev) })
	replicates("OFF")
	partitioned.Set(prev)

	// THE FANNED ARM, the configuration every budgeted query runs in.
	replicates("ON")
}
