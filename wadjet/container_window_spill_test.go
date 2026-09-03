package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// TestContainerWindowKeysAnswerTheSameAcrossAWindowSpill is #630's gate: an
// ARRAY, ROW, MAP or VECTOR column as a window PARTITION BY key or as the
// window's own ORDER BY key answers the same values across a window spill as
// it does with memory to spare.
//
// WHAT THE FILING DESCRIBED, AND WHY IT NO LONGER EXISTS. #630 was filed as
// the window-site sibling of #611: `window.go:534` serialized rows through
// memory.SpillManager.SpillRows, whose default arm rendered a container box
// with fmt's %v, and batch.FromRows then refused that text into a container
// vector (#361's silent-write guard), crashing the query. That site is now
// reachable only when Window.useColumnarRuns() is false, and that is
// `len(w.groups) == 0` — groups being one entry per distinct (PARTITION BY,
// ORDER BY) pair across w.Columns, rebuilt from w.Columns at Init and again at
// the first Consume. So it is false exactly when the operator has NO window
// functions, which no plan produces: both constructors (physical planner,
// worker fragment executor) build their column list from the node's window
// expressions. Every window with a spec — which is every window — spills
// through the columnar run format instead, and that format carries nested
// schemas. The four-arm census confirms it: single, spilled, DAG and
// DAG-shuffled all answer identically for all four container types, in both
// key positions.
//
// So this file is not a fix's regression test. It is the COVERAGE the filing
// asked for, in the place where it can be asserted: the shape reaches a real
// window spill and the answer is unchanged. Without it, "the columnar run
// format carries containers" is a claim about a code path with no container
// fixture running through it.
//
// THE BUDGET IS 256 KiB AND THAT IS LOAD-BEARING. At 512 KiB — the sweep's
// budget — c_row, c_rownest and c_vec write window runs and c_arr and c_map
// write NONE: their columns are smaller, so the tracker never reaches the
// SpillCheap threshold and those two cells would compare two in-memory runs,
// which is the vacuous shape ADR-0027 decision 5 exists to stop. Measured, one
// run per cell, window runs written:
//
//	budget    c_arr  c_map  c_row  c_vec
//	512 KiB     0      0      1      4
//	256 KiB     1      1      1      4
//	128 KiB     1      1      1      5
//
// Hence the per-CELL engagement assertion below rather than a per-family one:
// at this budget every cell engages, so anything less is a coverage hole
// rather than a shape whose state was too small.
func TestContainerWindowKeysAnswerTheSameAcrossAWindowSpill(t *testing.T) {
	ctx := context.Background()
	n := typematrix.Nested
	plain := cgkOpen(t, 0)
	budgeted := cgkOpen(t, containerWindowSpillBudget)
	// The window run floor is 64 MiB; a 1.2 MB fixture cannot cross it at any
	// budget, so without this seam the operator buffers and never writes a run
	// (ADR-0027 decision 6). The reference arm above was taken before it.
	defer exec.ForceSmallSpillRuns(4096)()

	for _, col := range []string{"c_arr", "c_row", "c_rownest", "c_map", "c_vec"} {
		for _, shape := range []struct{ name, sql string }{
			// PARTITION BY the container: the key that #630 names.
			{"partition_by", `SELECT id, ROW_NUMBER() OVER (PARTITION BY %[1]s ORDER BY id) AS r FROM %[2]s ORDER BY id`},
			// ORDER BY the container INSIDE the window: the run files are
			// sorted by this key, so a container that cannot be compared in
			// the run format fails here and not above.
			{"order_by", `SELECT id, ROW_NUMBER() OVER (ORDER BY %[1]s, id) AS r FROM %[2]s ORDER BY id`},
			// A frame-carrying function over the partition, so the value read
			// back out of the run is used and not merely counted.
			{"partition_agg", `SELECT id, COUNT(*) OVER (PARTITION BY %[1]s) AS c, ` +
				`MIN(id) OVER (PARTITION BY %[1]s) AS lo FROM %[2]s ORDER BY id`},
			// The container in the OUTPUT as well as the key: the run has to
			// carry the value back, not just compare it.
			{"partition_projected", `SELECT id, %[1]s AS k, ROW_NUMBER() OVER (PARTITION BY %[1]s ORDER BY id) AS r FROM %[2]s ORDER BY id`},
		} {
			t.Run(col+"_"+shape.name, func(t *testing.T) {
				sql := fmt.Sprintf(shape.sql, col, n)
				want, err := tmRun(ctx, plain, sql)
				if err != nil {
					t.Fatalf("unbudgeted: %v\n  SQL: %s", err, sql)
				}
				if len(want.Rows) == 0 {
					t.Fatalf("the unbudgeted run returned no rows — this cell would compare nothing\n  SQL: %s", sql)
				}
				w := spillMxRender(want.Columns, want.Rows, true)
				runsBefore := exec.WindowRunsWritten.Load()
				// A spill is a CONDITION, not a query shape: which batch
				// crosses the threshold moves between runs, so one passing
				// spilled run proves nothing (ADR-0027).
				for run := 0; run < spillMxRuns(); run++ {
					got, err := tmRun(ctx, budgeted, sql)
					if err != nil {
						t.Fatalf("under a %d KiB budget, run %d: %v\n  SQL: %s",
							containerWindowSpillBudget/1024, run, err, sql)
					}
					if diff := spillMxDiff(spillMxRender(got.Columns, got.Rows, true), w); diff != "" {
						t.Fatalf("under a %d KiB budget, run %d: %s\n  SQL: %s",
							containerWindowSpillBudget/1024, run, diff, sql)
					}
				}
				if runs := exec.WindowRunsWritten.Load() - runsBefore; runs == 0 {
					t.Errorf("no window run reached disk in %d runs — this cell compared two in-memory "+
						"runs and would pass with the window spill path deleted (ADR-0027 decision 5)\n  SQL: %s",
						spillMxRuns(), sql)
				}
			})
		}
	}
}

// containerWindowSpillBudget is where every container column's window
// engages. See the budget table in the test's doc comment: 512 KiB leaves
// c_arr and c_map comparing two in-memory runs.
const containerWindowSpillBudget int64 = 256 * 1024
