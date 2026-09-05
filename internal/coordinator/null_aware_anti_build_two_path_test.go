package coordinator

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// #539 / #507 — a NOT IN whose build the broadcast decision would REFUSE is
// replicated anyway, and the counter is what says so.
//
// `x NOT IN (SELECT y FROM t)` is three-valued, and the third value is a fact
// about the WHOLE build: a NULL anywhere in the list makes the predicate
// UNKNOWN for every probe row it did not otherwise match, so the answer is no
// rows at all. `exec.HashJoin` reads that as one bit off its build side. A
// hash-partitioned build splits the bit — the task holding the NULL partition
// emits nothing, every other task emits its probe rows, and the query comes
// back with the row set a TWO-valued anti join (its `NOT EXISTS` twin) would
// give. Silently, because every task behaved correctly for the rows it held.
//
// walkStages therefore FORCES the build to replicate, past the size decision
// and past an explicit "broadcast disabled". That is a real trade — a build
// the threshold refused is now N× on the cluster — so it is counted rather
// than only logged, and this is the gate that reads the counter.
//
// `BroadcastBytesOverride = 1` is the arm that makes it visible: a one-byte
// threshold refuses every broadcast, so every join in the plan goes through a
// hash shuffle EXCEPT the null-aware anti join, which overrides it. Without
// the override the shape would be broadcast by size and the counter would
// never move — which is how a corpus can carry a dozen NOT IN entries and
// still not test the rule.
func TestANullAwareAntiJoinReplicatesItsBuildPastTheBroadcastDecision(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	// One byte: every build the planner sizes is too big to broadcast, so a
	// null-aware anti join is the only thing in the plan that replicates.
	coord := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })

	cells := []struct {
		name, sql string
		// wantForced is whether this shape must FORCE a replicated build past
		// the one-byte threshold.
		wantForced bool
	}{
		// The degenerate half: the list carries NULLs, so the predicate is
		// UNKNOWN for every row and PostgreSQL answers none. This is the cell
		// a partitioned build gets wrong — it would answer the probe rows the
		// non-NULL partitions did not match.
		{"notin_over_a_null_carrying_list",
			`SELECT COUNT(*) AS n FROM typemx a WHERE a.c_i64 NOT IN ` +
				`(SELECT b.c_i64 FROM typemx b WHERE b.id < 500)`, true},
		// The non-degenerate half: a clean list, so the anti join answers —
		// but the PROBE carries NULLs and a NULL key is UNKNOWN against every
		// value, matched or not.
		{"notin_over_a_clean_list_with_a_null_probe",
			`SELECT COUNT(*) AS n FROM typemx a WHERE a.c_i64 NOT IN ` +
				`(SELECT b.c_i64 FROM typemx b WHERE b.id < 500 AND b.c_i64 IS NOT NULL)`, true},
		// The CONTROL, and it is the pair that says the force is about NOT IN
		// and not about anti joins: `NOT EXISTS` asks the two-valued question,
		// needs no whole-build fact, and shuffles like any other join.
		{"control_not_exists_shuffles",
			`SELECT COUNT(*) AS n FROM typemx a WHERE NOT EXISTS ` +
				`(SELECT 1 FROM typemx b WHERE b.c_i64 = a.c_i64 AND b.id < 500)`, false},
		// The second control: an ordinary semi join over the same relations.
		{"control_in_shuffles",
			`SELECT COUNT(*) AS n FROM typemx a WHERE a.c_i64 IN ` +
				`(SELECT b.c_i64 FROM typemx b WHERE b.id < 500)`, false},
	}

	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			want, err := na2Run(tmdRunSingle(ctx, single, tc.sql))
			if err != nil {
				t.Fatalf("single arm: %v\n  SQL: %s", err, tc.sql)
			}
			sort.Strings(want)

			before := physical.NullAwareAntiForcedBroadcasts.Load()
			got, err := na2Run(tmdRunDAG(ctx, coord, tc.sql))
			if err != nil {
				t.Fatalf("dag arm: %v\n  SQL: %s", err, tc.sql)
			}
			sort.Strings(got)
			forced := physical.NullAwareAntiForcedBroadcasts.Load() - before

			if len(got) != len(want) {
				t.Fatalf("dag arm: %v, single arm: %v\n  SQL: %s", got, want, tc.sql)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("dag arm row %d: %s, single arm: %s\n  SQL: %s",
						i, got[i], want[i], tc.sql)
				}
			}
			// The counter beside the rows (protocol rule 11): rows alone
			// cannot tell "replicated because the rule says so" from
			// "broadcast because it happened to be small".
			if tc.wantForced && forced == 0 {
				t.Errorf("NullAwareAntiForcedBroadcasts did not move: this join's build was "+
					"NOT forced to replicate past the one-byte threshold, so it was either "+
					"shuffled (which splits NOT IN's three-valued fact, #507) or the shape "+
					"stopped being lowered to a null-aware anti join at all\n  SQL: %s", tc.sql)
			}
			if !tc.wantForced && forced != 0 {
				t.Errorf("NullAwareAntiForcedBroadcasts moved by %d for a shape that asks the "+
					"TWO-valued question and needs no whole-build fact\n  SQL: %s", forced, tc.sql)
			}
		})
	}
}
