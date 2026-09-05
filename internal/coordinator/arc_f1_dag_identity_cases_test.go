package coordinator

import (
	"context"
	"testing"
	"time"
)

// spillRefusal is what the arc's 512 KiB spilled arm answers to every cross
// join over this corpus, and it is ADR-0006's routed-probe amendment (#832),
// not this arc's: a cross join's probe reads every build row, so its build
// cannot be grace-partitioned and must fit the budget. Asserted as itself
// rather than skipped.
const spillRefusal = "ERR building physical plan: building hash table: hash join build " +
	"(a cross join's probe reads every build row, so its build cannot be grace-partitioned " +
	"and cannot spill; the build must fit the budget)"

// TestF1AWindowKeyGuardReadsProvenanceNotSpelling is #745: `__winkey_N` is the
// name the planner MINTS for a materialized window key, and a table written
// before that namespace was reserved can STORE a column of that name. The
// guard read the name and refused a bound reference on both DAG arms for a
// query the single-process path answers.
//
// The three cells are the guard's whole claim: a stored column whose name is
// in the reserved family is BOUND and must answer; an ordinary column is the
// control; and a genuinely MATERIALIZED key (`PARTITION BY id % 2`), which the
// fragment computes into a real `__winkey_N` slot, must still answer — that is
// the shape #585's guard was added for, and a fix that let it through would
// trade one loud failure for a window over one partition.
func TestF1AWindowKeyGuardReadsProvenanceNotSpelling(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			name: "745 PARTITION BY a STORED __winkey_1",
			sql:  "SELECT id, SUM(id) OVER (PARTITION BY __winkey_1) AS w FROM oldtab ORDER BY id",
			want: "cols=[id:INT64 w:FLOAT64] rows=4 | 1,1 | 2,2 | 3,3 | 4,4",
		},
		{
			name: "745 control: PARTITION BY an ordinary stored column",
			sql:  "SELECT id, SUM(id) OVER (PARTITION BY plain) AS w FROM oldtab ORDER BY id",
			want: "cols=[id:INT64 w:FLOAT64] rows=4 | 1,1 | 2,2 | 3,3 | 4,4",
		},
		{
			name: "745 control: a key the FRAGMENT really materializes still answers",
			sql:  "SELECT id, SUM(id) OVER (PARTITION BY id % 2) AS w FROM oldtab ORDER BY id",
			want: "cols=[id:INT64 w:FLOAT64] rows=4 | 1,4 | 2,6 | 3,4 | 4,6",
		},
		{
			name: "745 control: the same stored column as a GROUP BY key",
			sql:  "SELECT __winkey_1 AS k, COUNT(*) AS n FROM oldtab GROUP BY __winkey_1 ORDER BY k",
			want: "cols=[k:INT64 n:INT64] rows=4 | 10,1 | 20,1 | 30,1 | 40,1",
		},
	})
}
