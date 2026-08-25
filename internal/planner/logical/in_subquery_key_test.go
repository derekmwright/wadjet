package logical

import (
	"strings"
	"testing"
)

// #516 — `WHERE a.x IN (SELECT b.x FROM t b …)` returned ZERO rows.
//
// decorrelateInSubqueries lowers the IN to a semi join whose build side is the
// inner plan: Scan → [Join] → [Filter] → [Aggregate], and never a Project. So
// the build carries the inner relation's SOURCE column names — which is why
// the inner WHERE goes through stripTableQualifiers. The rewrite spelled the
// join's inner key from the SELECT list instead: `b.x` (a qualifier the Scan
// below it does not carry) or the item's ALIAS (which no node materializes).
//
// Nothing errored. The physical planner split the condition literally,
// exec.HashJoin.FixKeyAssignment found the OTHER side's bare name in the build
// schema — on a self-IN it always is — and swapped the pair, leaving the probe
// to resolve a name only the build has. The join matched nothing.
//
// These assert the KEY, not the answer: an inner key that is not a column of
// the inner subtree is the defect, whatever a particular fixture makes of it.
func TestInSubqueryNamesAnInnerKeyTheInnerPlanEmits(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		wantCond string
	}{
		{
			name:     "inner select item qualified by the subquery's own alias",
			sql:      `SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT b.o_custkey FROM orders b WHERE b.o_orderkey < 500)`,
			wantCond: "o_custkey = o_custkey",
		},
		{
			name:     "inner select item qualified by the table name",
			sql:      `SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT orders.o_custkey FROM orders WHERE orders.o_orderkey < 500)`,
			wantCond: "o_custkey = o_custkey",
		},
		{
			name:     "inner select item carries an alias no projection materializes",
			sql:      `SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT b.o_custkey AS bk FROM orders b WHERE b.o_orderkey < 500)`,
			wantCond: "o_custkey = o_custkey",
		},
		{
			name:     "NOT IN takes the same key through the anti join",
			sql:      `SELECT o_orderkey FROM orders a WHERE a.o_custkey NOT IN (SELECT b.o_custkey FROM orders b WHERE b.o_orderkey < 500)`,
			wantCond: "o_custkey = o_custkey",
		},
		{
			name:     "a qualified GROUP BY key is spelled the way its Scan emits it",
			sql:      `SELECT o_orderkey FROM orders a WHERE a.o_orderstatus IN (SELECT b.o_orderstatus FROM orders b GROUP BY b.o_orderstatus)`,
			wantCond: "o_orderstatus = o_orderstatus",
		},
		{
			name:     "a different relation still resolves to its own bare column",
			sql:      `SELECT o_orderkey FROM orders a WHERE a.o_custkey IN (SELECT c.c_custkey FROM customer c WHERE c.c_nationkey < 5)`,
			wantCond: "o_custkey = c_custkey",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := buildPlan(t, tc.sql)
			annotateScanColumnsForTest(plan)
			join := findNodeMatching(Optimize(plan, annotateScanColumnsForTest), func(n *Node) bool {
				return n.Type == NodeJoin && (n.JoinType == "semi" || n.JoinType == "anti")
			})
			if join == nil {
				t.Fatalf("no semi/anti join in the optimized plan for %q", tc.sql)
			}
			if got := strings.ToLower(strings.TrimSpace(join.JoinCond)); got != tc.wantCond {
				t.Errorf("join condition = %q, want %q", got, tc.wantCond)
			}
			// The invariant behind the literal above: the build subtree here
			// is a Scan of one relation, so the key it emits is BARE.
			// `o_custkey = b.o_custkey` reads fine and names nothing, and
			// `= bk` names an alias no node in the subtree materializes.
			key := strings.TrimSpace(strings.SplitN(join.JoinCond, "=", 2)[1])
			if strings.Contains(key, ".") {
				t.Errorf("inner join key %q carries a table qualifier the build subtree's Scan does not", key)
			}
		})
	}
}

// #526 — over a JOINED inner, the key is spelled from the join's REAL output,
// which only reorderJoins settles. A join emits its probe side's columns bare
// and qualifies a build column exactly where the bare name collides, so
// neither "keep what the user wrote" nor "strip the qualifier" is right on its
// own: each is right for one of the two cases and silently wrong for the
// other. repairDecorrelatedSpelling picks per join, after the reorder.
//
// These assert the KEY, because that is the defect — a key naming a column
// the build subtree does not carry lets exec.HashJoin's repair swap the pair
// and the join then matches nothing. The ANSWERS are gated against
// PostgreSQL in wadjet.TestInSubqueryOverAJoinedInnerAgreesWithPostgres.
func TestInSubqueryOverAJoinedInnerNamesTheKeyTheJoinEmits(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantKey string
	}{
		{
			// customer and nation share no bare column name, so whichever
			// side the estimator puts on the build, the join emits
			// n_nationkey BARE. The qualified spelling names nothing.
			name: "no collision: the join emits the column bare",
			sql: `SELECT o_orderkey FROM orders WHERE o_custkey IN
				(SELECT n.n_nationkey FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey)`,
			wantKey: "n_nationkey",
		},
		{
			// A self-join: both relations carry n_nationkey, so the build
			// side's copy IS qualified and the key must say so.
			name: "collision: the build side's copy is qualified",
			sql: `SELECT o_orderkey FROM orders WHERE o_custkey IN
				(SELECT b.n_nationkey FROM nation a JOIN nation b ON a.n_regionkey = b.n_regionkey)`,
			wantKey: "b.n_nationkey",
		},
		{
			// The other half of the same self-join: the probe side's copy is
			// the bare one.
			name: "collision: the probe side's copy is bare",
			sql: `SELECT o_orderkey FROM orders WHERE o_custkey IN
				(SELECT a.n_nationkey FROM nation a JOIN nation b ON a.n_regionkey = b.n_regionkey)`,
			wantKey: "n_nationkey",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := innerSemiJoinKeyFor(t, tc.sql)
			if !ok {
				t.Fatal("decorrelation declined a shape it used to accept")
			}
			if got != tc.wantKey {
				t.Errorf("inner key = %q, want %q", got, tc.wantKey)
			}
		})
	}
}

// #482 — the subquery's LIMIT/OFFSET was dropped on the floor: the semi join's
// build side IS the relation the subquery reads, so the membership set was the
// whole unbounded column and the predicate matched every row for any n.
// Declining leaves the IN a subquery filter, which is executed as written.
func TestInSubqueryWithALimitDeclinesDecorrelation(t *testing.T) {
	for _, sql := range []string{
		`SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT o_custkey FROM orders ORDER BY o_custkey LIMIT 3)`,
		`SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT o_custkey FROM orders LIMIT 3)`,
		`SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT o_custkey FROM orders ORDER BY o_custkey LIMIT 3 OFFSET 5)`,
		`SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT o_custkey FROM orders ORDER BY o_custkey LIMIT 0)`,
		`SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT o_custkey FROM orders ORDER BY o_custkey OFFSET 5)`,
		`SELECT o_orderkey FROM orders WHERE o_custkey NOT IN (SELECT o_custkey FROM orders ORDER BY o_custkey LIMIT 3)`,
	} {
		plan := buildPlan(t, sql)
		annotateScanColumnsForTest(plan)
		optimized := Optimize(plan, annotateScanColumnsForTest)
		if join := findNodeMatching(optimized, func(n *Node) bool {
			return n.Type == NodeJoin && (n.JoinType == "semi" || n.JoinType == "anti")
		}); join != nil {
			t.Errorf("a bounded IN-subquery was decorrelated into a %s join, which has nowhere to "+
				"put the bound\n  SQL: %s\n  ON: %s", join.JoinType, sql, join.JoinCond)
		}
		if !hasNodeType(optimized, NodeFilter) {
			t.Errorf("the IN predicate survived neither as a join nor as a filter\n  SQL: %s", sql)
		}
	}

	// The unbounded twin must still decorrelate, or the guard above is a
	// blanket disable of the rewrite rather than a bound on it.
	plan := buildPlan(t, `SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT o_custkey FROM orders WHERE o_orderkey < 500)`)
	annotateScanColumnsForTest(plan)
	if findNodeMatching(Optimize(plan, annotateScanColumnsForTest), func(n *Node) bool { return n.Type == NodeJoin && n.JoinType == "semi" }) == nil {
		t.Error("an unbounded IN-subquery no longer decorrelates into a semi join")
	}
}

// innerSemiJoinKeyFor returns the inner side of the semi/anti join sql
// decorrelates to, and ok=false when it did not decorrelate.
func innerSemiJoinKeyFor(t *testing.T, sql string) (string, bool) {
	t.Helper()
	plan := buildPlan(t, sql)
	annotateScanColumnsForTest(plan)
	join := findNodeMatching(Optimize(plan, annotateScanColumnsForTest), func(n *Node) bool {
		return n.Type == NodeJoin && (n.JoinType == "semi" || n.JoinType == "anti")
	})
	if join == nil {
		return "", false
	}
	parts := strings.SplitN(join.JoinCond, "=", 2)
	if len(parts) != 2 {
		return "", false
	}
	return strings.TrimSpace(parts[1]), true
}

// findNodeMatching returns the first node satisfying pred, or nil.
func findNodeMatching(n *Node, pred func(*Node) bool) *Node {
	if n == nil {
		return nil
	}
	if pred(n) {
		return n
	}
	for _, c := range n.Children {
		if f := findNodeMatching(c, pred); f != nil {
			return f
		}
	}
	return nil
}
