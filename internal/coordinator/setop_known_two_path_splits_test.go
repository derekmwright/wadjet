// SPDX-License-Identifier: AGPL-3.0-only

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
		// dagErr is the substring of the refusal both DAG arms give, or
		// EMPTY where both arms answer — a cell whose split has closed stays
		// here, asserting the rows on every arm, so the shape keeps a gate
		// rather than losing one.
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
			// CLOSED 2026-09-13 by arc O1 (#997, #1012), in this cell's own
			// terms: "`SELECT *` over a JOIN builds no Project, and the
			// star's expansion is not recorded on the join node, so
			// setOpOutputNames has no column list to take the result's names
			// from". The star now BUILDS a projection, carrying the FROM
			// clause's arms in written order (ADR-0026 §9), so the arm has a
			// column list like any other and both DAG arms answer. dagErr is
			// empty, which is how this table says "both arms answer" — the
			// pin is deleted and the rows are asserted instead.
			name: "a_star_over_a_join_as_an_arm",
			why:  "CLOSED by arc O1: the star over a join is a projection now",
			sql: `SELECT * FROM (SELECT id FROM decpair) p JOIN (SELECT id AS id2 FROM decpair) q ` +
				`ON p.id = q.id2 UNION ALL SELECT id, id FROM decpair`,
			singleRows: 18,
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
				dres, derr := tmdRunDAG(ctx, arm.c, tc.sql)
				a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.c), a2Routes{}, tc.sql)
				if tc.dagErr == "" {
					if derr != nil {
						t.Errorf("the %s arm REFUSED a shape this table says both arms answer:"+
							" %v\n  %s\n  SQL: %s", arm.name, derr, tc.why, tc.sql)
						continue
					}
					if len(dres.Rows) != tc.singleRows {
						t.Errorf("the %s arm returned %d rows, want %d (PostgreSQL 17)\n  SQL: %s",
							arm.name, len(dres.Rows), tc.singleRows, tc.sql)
					}
					continue
				}
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

	// CLOSED at arc L1 (#1111), and asserted rather than pinned. `MAX` over a
	// CTE inside a LATERAL was scale 2 on the single-process path where the
	// stage DAG and PostgreSQL are scale 4 — the same number under two
	// declarations, and only with a CTE inside the LATERAL.
	//
	// The CTE was never the mechanism. The body published `dx`, a name the
	// ENCLOSING relation also carries, and the lateral was an arm with no
	// alias for the join to qualify its duplicate by — so the join DROPPED the
	// body's column and `t.dx` bound the outer row's own `dx`, which is
	// DECIMAL(9,2). The single-process path read the outer value; the DAG read
	// the lateral's. The lateral's subtree root carries the alias the query
	// wrote now, and both paths answer PostgreSQL's 12.7500.
	t.Run("max_over_a_cte_inside_a_lateral_declares_one_scale", func(t *testing.T) {
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
		for arm, got := range map[string]string{
			"single": fmt.Sprintf("%v", sres.Rows[0]["v"]),
			"dag":    fmt.Sprintf("%v", dres.Rows[0]["v"]),
		} {
			if got != "12.7500" {
				t.Errorf("the %s arm answered %q, want 12.7500 (PostgreSQL 17.11)\n  SQL: %s",
					arm, got, sql)
			}
		}
	})
}
