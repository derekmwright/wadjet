package coordinator

import (
	"context"
	"testing"
	"time"
)

// Two shapes whose VALUE is right on both execution paths and whose Go BOX —
// and therefore the wire OID a client reads — is not the same on both.
//
// They are recorded rather than fixed, and each is recorded with the mechanism
// that produces it, because each sits on a decision this arc took deliberately:
//
//  1. A SHADOWED RENAME. `(SELECT c_i32 AS c_dec, c_dec AS other FROM typemx)`
//     binds the name `c_dec` to an INT32 column while a scan below carries a
//     DECIMAL column of that name. PostgreSQL answers `sum(int4)` as BIGINT and
//     the DAG agrees; the single-process path boxes the DECIMAL. That is the
//     SCAN-FIRST order in physical.aggInputColumnType, which round 2 adopted
//     because preferring the emitted walk typed the DAG's re-spelled dispatch
//     name from the derived table and turned eight shapes into refused shuffle
//     reads (ADR-0010's type guard). The order is right for the DAG and wrong
//     for exactly this shadow; the comment on that function says so.
//
//  2. A WINDOW SLOT. `SUM(w)` over `ROW_NUMBER() OVER (ORDER BY id)` is
//     NUMERIC in PostgreSQL — row_number is bigint and sum(bigint) is numeric —
//     and the single-process path agrees. The DAG boxes float64, because the
//     worker rebuilds the aggregate's input from a declaration a window output
//     does not carry. That is the same mechanism as #775's pinned DAG half
//     (TestWindowFedAggregateArgumentIsPinnedOnTheDAG), one operator earlier:
//     there it is a hard failure, here it is a float box over a right number.
//
// So each path is the correct one on one shape. The test asserts BOTH boxes as
// they are and FAILS when either converges — at which point the shape belongs
// in the census with PostgreSQL's box and this entry goes away.
func TestKnownBoxDivergencesBetweenPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this pin stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)

	for _, tc := range []struct {
		name, sql             string
		wantSingle, wantDAG   string
		postgres, whoDiverges string
	}{
		{
			name: "shadowed_rename_boxes_decimal_on_the_single_path",
			sql: `SELECT SUM(c_dec) AS s FROM ` +
				`(SELECT c_i32 AS c_dec, c_dec AS other FROM typemx WHERE id < 8) x`,
			wantSingle: "s=84", wantDAG: "s=int64:84",
			postgres:    "bigint 84",
			whoDiverges: "the single-process path",
		},
		{
			name: "window_slot_boxes_float_on_the_dag",
			sql: `SELECT SUM(w*2) AS s FROM ` +
				`(SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS w FROM typemx WHERE id < 10) x`,
			wantSingle: "s=110", wantDAG: "s=float:110",
			postgres:    "numeric 110",
			whoDiverges: "the DAG",
		},
		{
			name: "window_slot_bare_argument_boxes_float_on_the_dag",
			sql: `SELECT SUM(w) AS s FROM ` +
				`(SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS w FROM typemx WHERE id < 10) x`,
			wantSingle: "s=55", wantDAG: "s=float:55",
			postgres:    "numeric 55",
			whoDiverges: "the DAG",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := na2Run(tmdRunSingle(ctx, single, tc.sql))
			if err != nil {
				t.Fatalf("single arm: %v\n  SQL: %s", err, tc.sql)
			}
			gotDAG, err := tmdRunDAG(ctx, coord, tc.sql)
			if err != nil {
				t.Fatalf("dag arm: %v\n  SQL: %s", err, tc.sql)
			}
			dag, err := na2Run(gotDAG, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || len(dag) != 1 {
				t.Fatalf("want one row per arm, got single=%v dag=%v", got, dag)
			}
			if got[0] == dag[0] {
				t.Errorf("the two paths now agree on the box (%s). PostgreSQL 17 says %s and "+
					"%s was the one diverging — put this shape in the census with "+
					"PostgreSQL's box and delete this entry.\n  SQL: %s",
					got[0], tc.postgres, tc.whoDiverges, tc.sql)
				return
			}
			if got[0] != tc.wantSingle {
				t.Errorf("single arm: got %s, recorded %s (PostgreSQL 17: %s)\n  SQL: %s",
					got[0], tc.wantSingle, tc.postgres, tc.sql)
			}
			if dag[0] != tc.wantDAG {
				t.Errorf("dag arm: got %s, recorded %s (PostgreSQL 17: %s)\n  SQL: %s",
					dag[0], tc.wantDAG, tc.postgres, tc.sql)
			}
		})
	}
}
