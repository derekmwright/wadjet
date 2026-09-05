package coordinator

import (
	"context"
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
	}{
		{
			// The ROW field of a ROW: `c_rownest.s` is ROW(x INT64).
			name: "row-field/min-over-an-empty-window",
			sql: "SELECT x.id AS id, MIN(c_rownest.s) OVER () AS v FROM " + n +
				" x WHERE x.id < 3 ORDER BY x.id",
			want: "3 rows: 0|map[x:0];1|map[x:0];2|map[x:0];",
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
			want: "3 rows: 0|map[l:[n00000] m:[map[key:k0 value:0]] s:map[x:0]];" +
				"1|map[l:[n00000] m:[map[key:k0 value:0]] s:map[x:0]];" +
				"2|map[l:[n00000] m:[map[key:k0 value:0]] s:map[x:0]];",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
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
