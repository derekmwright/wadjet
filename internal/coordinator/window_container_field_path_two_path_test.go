// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// A window over a ROW FIELD PATH of a CONTAINER type answers the same on every
// arm (#618).
//
// The materialized window key carries a bare TypeID plus (p,s)
// (`exec.ProjectColumn`), which has no room for a container's Fields /
// ElementType or a VECTOR's dimension. The pre-window operator built its output
// vector from that, so a container key came out with nil Children / nil Child
// and every value written into it was dropped:
//
//	MIN(c_rownest.s) OVER ()                 map[x:0]  ->  NULL
//	COUNT(*) OVER (PARTITION BY c_rownest.s)        1  ->  2   (one partition)
//
// It was TWO builders with the same gap, and they fail on different arms, so a
// gate on one path could not have seen the other: `windowKeyProjections` on the
// single-process path (fixed by carrying the field's `parquet.Column` as
// `meta`, the repair #568 made for the aggregate's pre-projection), and
// `buildWindowKeyProjection` on the DAG's worker, which has the stage spec's
// TEXT and no catalog and now names the SOURCE so the declaration is read off
// the parent ROW in the batch.
//
// The DECIMAL half of the filing is NOT reproduced here and needs no cell: a
// windowed MIN/MAX over `c_row.dc` already agreed with the flat column on
// every arm before this fix, because `windowKey` carries Precision/Scale. The
// `dec` cell below is the control that says so.
func TestAWindowedContainerFieldPathAgreesOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	arms := append(dajArms(t, ctx), dajArm{
		name: "spilled (" + e3ArmBudgetName + ")",
		run: func(q string) (*oracle.Result, error) {
			return tmdRunSingle(ctx, e3BudgetedStandalone(t, ctx), q)
		},
	})
	n := typematrix.Nested
	for _, tc := range []struct {
		name, sql, want string
		// refuse, when set, is the refusal every arm must raise instead.
		refuse string
	}{
		{
			// The ROW field of a ROW: `c_rownest.s` is ROW(x INT64). MIN over
			// a ROW is PostgreSQL's 42883 since arc BR (#1061) — the key's
			// materialization is still asserted, through PARTITION BY below.
			name: "row-field/min-over-an-empty-window",
			sql: "SELECT x.id AS id, MIN(c_rownest.s) OVER () AS v FROM " + n +
				" x WHERE x.id < 3 ORDER BY x.id",
			refuse: "function min(record) does not exist",
		},
		{
			// The ARRAY field of a ROW: `c_rownest.l` is ARRAY(STRING).
			name: "array-field/min-over-an-empty-window",
			sql: "SELECT x.id AS id, MIN(c_rownest.l) OVER () AS v FROM " + n +
				" x WHERE x.id < 3 ORDER BY x.id",
			want: "3 rows: 0|[];1|[];2|[];",
		},
		{
			// The PARTITION BY face, which fails as a wrong COUNT rather than
			// as a NULL: an empty key puts every row in one partition.
			name: "row-field/partition-by",
			sql: "SELECT x.id AS id, COUNT(*) OVER (PARTITION BY c_rownest.s) AS v FROM " + n +
				" x WHERE x.id < 3 ORDER BY x.id",
			want: "3 rows: 0|1;1|1;2|1;",
		},
		{
			name: "array-field/partition-by",
			sql: "SELECT x.id AS id, COUNT(*) OVER (PARTITION BY c_rownest.l) AS v FROM " + n +
				" x WHERE x.id < 3 ORDER BY x.id",
			want: "3 rows: 0|1;1|1;2|1;",
		},
		{
			// CONTROL: the parameterized (non-container) field, which agreed
			// on every arm before this fix. It is here so a fix that moved a
			// right answer shows up.
			name: "ctl-decimal-field/min-over-an-empty-window",
			sql: "SELECT x.id AS id, MIN(c_row.dc) OVER () AS v FROM " + n +
				" x WHERE x.id < 3 ORDER BY x.id",
			want: "3 rows: 0|0.0000;1|0.0000;2|0.0000;",
		},
		{
			// CONTROL: the whole container as a plain COLUMN, no field path.
			// Right on every arm at base, and the shape that says the defect
			// belongs to the field-path materialization and not to windows
			// over containers.
			name: "ctl-whole-container-column/min-over-an-empty-window",
			sql: "SELECT x.id AS id, MIN(c_rownest) OVER () AS v FROM " + n +
				" x WHERE x.id < 3 ORDER BY x.id",
			refuse: "function min(record) does not exist",
		},
		{
			// The whole-container control over a container MIN still takes.
			name: "ctl-whole-array-column/min-over-an-empty-window",
			sql: "SELECT x.id AS id, MIN(c_arr) OVER () AS v FROM " + n +
				" x WHERE x.id < 3 ORDER BY x.id",
			want: "3 rows: 0|[];1|[];2|[];",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if tc.refuse != "" {
					if err == nil || !strings.Contains(err.Error(), tc.refuse) {
						t.Errorf("the %s arm: %v, want the refusal %q\n  SQL: %s", arm.name, err, tc.refuse, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("the %s arm refused: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if got := dajDigest(res, []string{"id", "v"}); got != tc.want {
					t.Errorf("the %s arm answered\n  %s\nthis gate records\n  %s\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
