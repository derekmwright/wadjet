// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// A JOIN'S BUILD-KEY SCAN FILTERS NEVER PRUNE THROUGH A TYPE CHANGE (#1677
// line r, the second member of line q's class).
//
// Line q proved the probe key at the join is the scan's unmodified stored
// column before a build-key filter (the dynamic min/max range, the bloom scan
// filter) is installed on that scan. It did not prove the filter's VALUES are
// in the stored column's type. A join whose keys differ in type compares them
// at a resolved common type (exec.HashJoin.KeyTypes) — DATE against TIMESTAMP
// as the midnight's milliseconds, NUMERIC against an integer as a decimal —
// while the build recorded its range in its OWN type's units, and the scan
// compared that range with the stored column's statistics in theirs: day
// counts against milliseconds (both ways), the stored NUMERIC(10,2)'s unscaled
// 300 against the integer 3. Every row group was pruned and `IN` / `= ANY` /
// `EXISTS` answered 0 where PostgreSQL 17.11 answers 1.
//
// The grid (testdata/arc_1677r_cells.tsv, PostgreSQL 17.11's answers over
// testdata/arc_1677r_fixture.sql) is every cross-type key pair the engine
// accepts — DATE/TIMESTAMP, INTEGER/BIGINT, the integers, REAL, DOUBLE and
// NUMERIC against each other, NUMERIC at two scales — both directions, plus
// the same-type controls, × the probe {stored, pass-through derived table} ×
// {inner, left, IN, IN over a derived body, = ANY, EXISTS, NOT IN, NOT
// EXISTS}. The arms are line q's: unbudgeted, budgeted and evict-every-batch,
// each on the host's build routes and forced onto the one-CPU routes that
// record the build keys' min/max (exec.ForceSerialJoinBuildForTest).
func TestArc1677rScanFiltersNeverPruneThroughATypeChange(t *testing.T) {
	ctx := context.Background()
	cells := arc1677qCells(t, "testdata/arc_1677r_cells.tsv")
	arms := []struct {
		name   string
		budget int64
		forced bool
	}{{"unbudgeted", 0, false}, {"budget256m", 256 << 20, false}, {"evict-every-batch", 4 << 20, true}}
	for _, serial := range []bool{true, false} {
		for _, arm := range arms {
			name := arm.name
			if serial {
				name += "/serial-build"
			}
			t.Run(name, func(t *testing.T) {
				db := arc1677qOpen(t, ctx, arm.budget, "testdata/arc_1677r_fixture.sql")
				if serial {
					defer exec.ForceSerialJoinBuildForTest()()
				}
				if arm.forced {
					prev := exec.ForceJoinPartitionEvictEvery(1)
					defer exec.ForceJoinPartitionEvictEvery(prev)
				}
				for _, c := range cells {
					if got := arc1677qAnswer(ctx, db, c[2]); got != c[1] {
						t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c[0], c[2], got, c[1])
					}
				}
			})
		}
	}
}
