package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The set-operation shapes the single-process path ANSWERS and the stage DAG
// REFUSES, pinned with their mechanism (round-2 review of arc E4).
//
// All three are pre-existing — byte-identical at 2d4220c9 — and all three are
// the union stage's ARM PRODUCER walk rather than the arm TYPING this arc
// repaired: the DAG projects each arm's SELECT list over the arm's own
// materialized output, and these are the three shapes where that output has no
// spelling the projection can use. They are not deferrals of anything this arc
// changed, and none of them was pinned anywhere, which is why they are here:
// a two-path split with no gate is a split nobody will notice closing OR
// widening.
//
// The routing counters are asserted beside the refusal, because "the DAG
// refused the plan" and "the DAG refused and the coordinator-local pipeline
// answered" are different states — and these all reach the client.
//
// Each cell FAILS the day the DAG answers, which is how the pin is deleted.
func TestKnownSetOperationTwoPathSplits(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range []struct {
		name, why, sql string
		// singleRows is PostgreSQL 17.11's row count, which the
		// single-process path answers.
		singleRows int
		// dagErr is the substring of the refusal both DAG arms give.
		dagErr string
	}{
		{
			name: "an_aggregate_as_an_arms_select_item",
			why: "the arm's aggregate stage names its own output, so the union stage's " +
				"projection would have to guess that name; setOpArmProjection refuses rather " +
				"than guess. It is the arc's own position 1 — an arm is an OPAQUE producer — " +
				"reached through an aggregate, and closing it means reading the aggregate " +
				"stage's emitted names the way a derived table's Project is read.",
			sql:        `SELECT SUM(a) AS s FROM decpair UNION ALL SELECT a AS s FROM decpair`,
			singleRows: 10,
			dagErr:     "selects the aggregate",
		},
		{
			name: "a_star_over_a_join_as_an_arm",
			why: "`SELECT *` over a JOIN builds no Project, and the star's expansion is not " +
				"recorded on the join node, so setOpOutputNames has no column list to take " +
				"the result's names from. Over a single relation the scan's ScanColumns are " +
				"that list, which is why the star arm below answers.",
			sql: `SELECT * FROM (SELECT id FROM decpair) p JOIN (SELECT id AS id2 FROM decpair) q ` +
				`ON p.id = q.id2 UNION ALL SELECT id, id FROM decpair`,
			singleRows: 18,
			dagErr:     "no resolvable output column list",
		},
		{
			name: "duplicate_output_names_of_different_types",
			why: "the union stage's two arms write .wshf files whose column NAMES collide, and " +
				"the reader keys a column by name — so the second `n` is read under the " +
				"first's declared type and the shuffle read refuses. Slot identity by " +
				"POSITION (#556/#557) reaches the single-process adapter and the sort key; " +
				"it does not reach the shuffle file's own schema.",
			sql:        `SELECT id AS n, a AS n FROM decpair UNION ALL SELECT id, b FROM decpair`,
			singleRows: 18,
			dagErr:     "shuffle read",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tmdRunSingle(ctx, single, tc.sql)
			if err != nil {
				t.Fatalf("the single-process arm is the control and must answer: %v\n  SQL: %s",
					err, tc.sql)
			}
			if len(res.Rows) != tc.singleRows {
				t.Errorf("the single-process arm returned %d rows, want %d (PostgreSQL 17)\n  SQL: %s",
					len(res.Rows), tc.singleRows, tc.sql)
			}
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				before := a2ReadRoutes(arm.c)
				_, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), a2Routes{}, tc.sql)
				if derr == nil {
					t.Errorf("the %s arm now ANSWERS this shape, so the split is closed: delete "+
						"this pin and assert the rows on both arms.\n  mechanism: %s\n  SQL: %s",
						arm.name, tc.why, tc.sql)
					continue
				}
				if !strings.Contains(derr.Error(), tc.dagErr) {
					t.Errorf("the %s arm refused for a different reason than the pinned one (%q):"+
						" %v\n  SQL: %s", arm.name, tc.dagErr, derr, tc.sql)
				}
			}
		})
	}

	// A DECLARED-TYPE split rather than a refusal: `MAX` over a CTE inside a
	// LATERAL is scale 2 on the single-process path where the stage DAG and
	// PostgreSQL are scale 4. The same number under two declarations, and only
	// with a CTE inside the LATERAL — the same aggregate over a CTE without a
	// LATERAL, and over a LATERAL without a CTE, agrees on both paths. Newly
	// REACHABLE when a block's own WITH came into scope (#684); before that the
	// shape answered four NULL rows there.
	t.Run("max_over_a_cte_inside_a_lateral_declares_a_narrower_scale", func(t *testing.T) {
		sql := fmt.Sprintf(`SELECT t.dx AS v FROM %s a, LATERAL (WITH c AS (SELECT dx FROM %s) `+
			`SELECT MAX(dx) AS dx FROM c) t`, sodJoinA, sodJoinB)
		sres, err := tmdRunSingle(ctx, single, sql)
		if err != nil {
			t.Fatalf("single: %v\n  SQL: %s", err, sql)
		}
		dres, err := tmdRunDAG(ctx, coord, sql)
		if err != nil {
			t.Fatalf("dag: %v\n  SQL: %s", err, sql)
		}
		s := fmt.Sprintf("%v", sres.Rows[0]["v"])
		d := fmt.Sprintf("%v", dres.Rows[0]["v"])
		if s == d {
			t.Errorf("the two paths now declare one scale (%q), so this split is closed: delete "+
				"the pin and assert PostgreSQL's 12.7500 on both.\n  SQL: %s", s, sql)
			return
		}
		if s != "12.75" || d != "12.7500" {
			t.Errorf("the split moved: single %q, dag %q; it was single 12.75 (scale 2) against "+
				"dag 12.7500 (scale 4, which is PostgreSQL's)\n  SQL: %s", s, d, sql)
		}
	})
}
