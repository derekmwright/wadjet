package coordinator

import (
	"context"
	"testing"
	"time"
)

// #775's DAG half, pinned with the shape that measures it.
//
// A COMPUTED aggregate argument over a DECIMAL WINDOW output — `SUM(w * 2)`
// and `SUM(w + 1)` over `SUM(a) OVER ()`, `AVG(a) OVER ()`, `MAX(a) OVER ()` —
// answers on the single-process path and dies on both DAG arms at the #361
// store guard: `cannot store string into FLOAT64 vector`. The exact DECIMAL
// the evaluator produces meets a vector the worker built from a FLOAT64
// declaration.
//
// Three CONTROLS make this a statement about the (p,s) rather than about
// windows or about multiplication, and they live in the census where they are
// asserted against PostgreSQL on every arm:
//
//	SUM(w)   over SUM(a) OVER ()   answers everywhere — a BARE argument
//	SUM(w*2) over SUM(f) OVER ()   answers everywhere — a FLOAT window output
//	SUM(a)*2, SUM(dw)*2            answer everywhere — no window at all
//
// So what is missing is the DECIMAL (p,s) of a window output when that output
// is an aggregate's INPUT. #728's walk gives the single-process path the
// declaration; what the worker rebuilds from `AggSpec.InputType` and the
// expression TEXT does not carry it.
//
// This is a PIN, not a gate: it asserts that the DAG still FAILS, and it FAILS
// ITSELF the day the DAG answers — at which point the shapes belong in the
// census beside their controls and this file goes away. Two candidate fixes
// were tried and reverted during the arc: routing `aggSpecInputDecimal`
// through the emitted walk, which changes no arm's answer on any constructible
// shape; and declaring the post-aggregate projection from `Stage.AggSpecs`,
// which IS a real improvement — it closed #784's computed-argument gap and is
// kept — but is not the site this fails at, because the failure is a
// POST-BREAKER op that the stage's ProjectExprs never reach.
func TestWindowFedAggregateArgumentIsPinnedOnTheDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this pin stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)

	for _, tc := range []struct {
		name, sql, want string
	}{
		{"sum_times_two_over_a_sum_window",
			`SELECT SUM(w*2) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x`, "953.82"},
		{"sum_plus_one_over_a_sum_window",
			`SELECT SUM(w+1) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x`, "485.91"},
		{"sum_times_two_over_an_avg_window",
			`SELECT SUM(w*2) AS s FROM (SELECT id, AVG(a) OVER () AS w FROM decpair) x`, "136.260000"},
		{"sum_times_two_over_a_max_window",
			`SELECT SUM(w*2) AS s FROM (SELECT id, MAX(a) OVER () AS w FROM decpair) x`, "229.50"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The single-process arm is GATED: it must keep answering
			// PostgreSQL's digits, so a "fix" that made both paths fail
			// equally could not pass this.
			res, err := tmdRunSingle(ctx, single, tc.sql)
			if err != nil {
				t.Fatalf("single-process arm: %v\n  SQL: %s", err, tc.sql)
			}
			rows, rerr := na2Run(res, nil)
			if rerr != nil || len(rows) != 1 || rows[0] != "s="+tc.want {
				t.Errorf("single-process arm: %v, want [s=%s] (live PostgreSQL 17)\n  SQL: %s",
					rows, tc.want, tc.sql)
			}
			// The DAG arm is PINNED: it must still fail, and this pin fails
			// when it stops failing.
			if _, err := tmdRunDAG(ctx, coord, tc.sql); err == nil {
				t.Errorf("the DAG arm now ANSWERS this, so #775's DAG half is FIXED for it:\n"+
					"  %s\nMove this shape into TestNumericArc2ShapesMatchPostgres beside its "+
					"controls and delete this pin.", tc.sql)
			}
		})
	}
}
