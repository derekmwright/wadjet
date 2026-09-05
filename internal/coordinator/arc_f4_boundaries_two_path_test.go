package coordinator

import (
	"context"
	"testing"
	"time"
)

// The two BOUNDARIES of arc F4's fixes, driven as fail-on-agree pins.
//
// Both were found by the round-1 adversarial review, both are pre-existing at
// `18f3660e` (the review reproduced them there), and both are spellings the
// arc's own text would otherwise be read as covering. A pin that starts
// agreeing FAILS, so deleting one is the next fix's proof.
//
//   - #785's collision under a SET-OP wrapper and under DISTINCT. The three
//     WRAPPED spellings this arc closed are gated in
//     TestArcE3NamesAndScopesTwoPath; these two are NOT closed, and ADR-0026
//     §3a says so.
//   - #732's published name over a SET OPERATION. Every non-set-op shape takes
//     PostgreSQL's name on four arms and both doors; a set operation does not,
//     and docs/sql-reference.md, ADR-0012 and #732's close text all say so.
//
// One mechanism explains both: `physical.findOutputProjectionNode` answers nil
// for any root that is not Project / Sort / Limit / Filter / Distinct, so a
// SET-OP root emits no gather `OutputRenames` at all — the class walk never
// runs, there is nothing for `pinProjectSpecSlots` to pin, and neither
// `CollectSink.OutputNames` nor `OutputRename.To` is ever set.
// `aggregateEmittedSlots` independently declines any producer carrying
// `UnionArms`. Closing them means the set operation publishing its LEFTMOST
// arm's names — PostgreSQL's rule — on BOTH engines at once, which is a change
// to what a union stage's output identity IS rather than a widening of this
// arc's two application sites.
func TestArcF4BoundariesArePinned(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	cases := []struct {
		name string
		sql  string
		// want is PostgreSQL 17's answer, rendered by e3Render.
		want string
		// pin is what an arm answers TODAY where that is not PostgreSQL's.
		pin map[string]string
	}{
		// --- #785 under a SET-OP wrapper and under DISTINCT ----------------
		// The DAG answers the GROUP BY KEY under both names, exactly as the
		// three closed spellings did.
		{
			name: "785/under-a-set-op-wrapper",
			sql: "SELECT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x FROM collslot " +
				"GROUP BY g HAVING COUNT(*) > 0) u " +
				"UNION ALL SELECT 99, 99 FROM collslot WHERE 1=0",
			want: "g,x | 80,0 | 80,1 | 80,2",
			pin: map[string]string{
				"dag":     "g,x | 0,0 | 1,1 | 2,2",
				"dagshuf": "g,x | 0,0 | 1,1 | 2,2",
			},
		},
		{
			name: "785/under-a-distinct",
			sql: "SELECT DISTINCT u.g, u.x FROM (SELECT COUNT(*) AS g, g AS x FROM collslot " +
				"GROUP BY g HAVING COUNT(*) > 0) u ORDER BY u.x",
			want: "g,x | 80,0 | 80,1 | 80,2",
			pin: map[string]string{
				"dag":     "g,x | 0,0 | 1,1 | 2,2",
				"dagshuf": "g,x | 0,0 | 1,1 | 2,2",
			},
		},

		// --- #732 over a SET OPERATION -------------------------------------
		// Every arm agrees with every other and none agrees with PostgreSQL,
		// which is what makes this an EXCEPTION to state rather than a split.
		{
			name: "732/set-op-arithmetic",
			sql: "SELECT g + 1 FROM typemx WHERE id < 2 " +
				"UNION ALL SELECT g + 2 FROM typemx WHERE id < 1",
			want: "?column? | 1 | 2 | 2",
			pin: map[string]string{
				"single":   "g + 1 | 1 | 2 | 2",
				spilledArm: "g + 1 | 1 | 2 | 2",
				"dag":      "g + 1 | 1 | 2 | 2",
				"dagshuf":  "g + 1 | 1 | 2 | 2",
			},
		},
		// A CAST rather than an aggregate, because an aggregate SELECTED
		// directly by a union arm is refused on the DAG for a reason of its
		// own (#346, pre-existing): the arm's aggregate stage names its own
		// output and the union stage cannot project a SELECT list over it.
		{
			name: "732/set-op-cast",
			sql: "SELECT CAST(g AS BIGINT) FROM typemx WHERE id < 2 " +
				"UNION ALL SELECT CAST(g AS BIGINT) FROM typemx WHERE id < 1",
			want: "g | 0 | 0 | 1",
			pin: map[string]string{
				"single":   "cast(g as bigint) | 0 | 0 | 1",
				spilledArm: "cast(g as bigint) | 0 | 0 | 1",
				"dag":      "cast(g as bigint) | 0 | 0 | 1",
				"dagshuf":  "cast(g as bigint) | 0 | 0 | 1",
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(c.sql)
				if err != nil {
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, c.sql)
				}
				got := e3SortLines(e3Render(cols, rows))
				want := e3SortLines(c.want)
				if pinned, ok := c.pin[arm.name]; ok {
					if got == want {
						t.Fatalf("the %s arm now answers PostgreSQL's result for a shape this "+
							"gate PINS as divergent. It is fixed: assert it, delete the pin, and "+
							"take the exception out of docs/sql-reference.md, ADR-0012 and "+
							"ADR-0026 §3a\n  %s\n  SQL: %s", arm.name, got, c.sql)
					}
					if got != e3SortLines(pinned) {
						t.Fatalf("%s arm: %s\n  pinned: %s\n  SQL: %s", arm.name, got, pinned, c.sql)
					}
					continue
				}
				if got != want {
					t.Fatalf("%s arm:\n  got  %s\n  want %s (PostgreSQL 17)\n  SQL: %s",
						arm.name, got, want, c.sql)
				}
			}
		})
	}
}
