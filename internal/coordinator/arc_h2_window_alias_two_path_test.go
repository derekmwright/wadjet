package coordinator

import (
	"context"
	"testing"
	"time"
)

// A DERIVED TABLE'S ALIAS FOR A WINDOW'S OUTPUT is a name only the
// single-process pipeline has — #877 and #878, on FOUR arms.
//
// A window publishes its result under the hidden slot `__win_N`. The SELECT
// list that names it `w` is an ordinary Project, and walkStages emits no stage
// for one (ADR-0025), so on the DAG `w` is a name nothing publishes. An
// aggregate above reading `SUM(w * 2)` shipped that TEXT to the worker,
// `expr.ColRef.Eval` answered nil for `w` on every row, and the SUM came back
// NULL — 953.82 on PostgreSQL 17 and on the single-process path, NULL on both
// DAG arms, silently (#877 with a JOIN under the window, #878 with the derived
// table a CTE joined to a second reference of ITSELF).
//
// Every Want below is PostgreSQL 17.11's over rows identical to the `decpair`
// fixture. The DECLARED type is asserted beside the rows because the two paths
// have diverged about window output types before (#796), and the routing
// counter is asserted beside both because rows alone cannot tell a DAG that
// EXECUTED a query from one that refused it and answered on the coordinator's
// local pipeline — five of these shapes moved from routed to executed and that
// is a result this arc claims.
//
// The BOUNDARY is a claim and the controls are what measure it. The rewrite
// touches a reference only when it resolves into the window-output slot family,
// which is RESERVED (a user can neither store nor alias a column there), so:
//
//   - C1/C2 a derived COMPUTED alias and a derived RENAME with no window in
//     sight must answer exactly what they answered before;
//   - C4 arithmetic over an AGGREGATE's output — the shape that made an
//     earlier respell ship `__agg_0 * 2` and hard-fail — must still be
//     COMPUTED rather than respelled;
//   - C5 a JOIN ARM's alias, which really IS materialized under its own name,
//     must not be respelled to its source (that move took a correct 25.50 to
//     0.00 once already);
//   - B4 is the adversarial one: the window's alias SHADOWS the base column it
//     aggregates (`SUM(a) OVER () AS a`), so a rule that resolved by name to a
//     real column rather than to the slot answers 105.98 instead of 953.82 —
//     which is what both DAG arms did, silently, before this fix.
func TestH2AWindowAliasBindsTheSlotItsProducerPublishes(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			// #877's exact SQL. Both DAG arms answered NULL.
			name: "877 a window aggregate over a JOIN, under an outer SUM",
			sql: "SELECT SUM(w*2) AS s FROM (SELECT g.id AS id, SUM(g.a) OVER () AS w " +
				"FROM decpair g JOIN decpair h ON g.id = h.id) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 953.82",
		},
		{
			// #878's exact SQL: the same defect one qualifier deeper, where
			// `q.w` has to be resolved inside the CTE's scope and each
			// reference of the CTE gets its OWN slot (__win_0 / __win_1).
			name: "878 a CTE joined to itself, under an outer SUM",
			sql: "WITH q AS (SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a FROM decpair) t) " +
				"SELECT SUM(q.w*2) AS s FROM q JOIN q q2 ON q.id = q2.id",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 953.82",
		},
		{
			// The same family with a GROUP BY beside it: nine wrong numbers
			// instead of one, which is the only form of this defect a row
			// COUNT could ever have noticed.
			name: "877 family: GROUP BY beside the window alias",
			sql: "SELECT id, SUM(w*2) AS s FROM (SELECT g.id AS id, SUM(g.a) OVER () AS w " +
				"FROM decpair g JOIN decpair h ON g.id = h.id) x GROUP BY id ORDER BY id",
			want: "cols=[id:INT64 s:DECIMAL(38,2)] rows=9 | 1,105.98 | 2,105.98 | 3,105.98 | " +
				"4,105.98 | 5,105.98 | 6,105.98 | 7,105.98 | 8,105.98 | 9,105.98",
		},
		{
			// B4: the window's alias SHADOWS the base column. The DAG bound
			// the SCAN's `a` and answered a plausible 105.98 — the wrong
			// number that looks like a right one.
			name: "877 family: the window alias shadows the base column",
			sql:  "SELECT SUM(a*2) AS s FROM (SELECT id, SUM(a) OVER () AS a FROM decpair g) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 953.82",
		},
		{
			// A bare argument over the same alias already resolved
			// (resolveAggInputName's rename branch), and must keep doing so.
			name: "877 control: a BARE aggregate argument over the window alias",
			sql: "SELECT SUM(w) AS s FROM (SELECT g.id AS id, SUM(g.a) OVER () AS w " +
				"FROM decpair g JOIN decpair h ON g.id = h.id) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 476.91",
		},
		{
			// A nested alias whose DEFINITION is arithmetic over the window
			// alias. Both DAG arms answered this correctly by REFUSING the
			// plan and routing local; they now execute it as stages.
			name: "877 family: a nested alias defined over the window alias",
			sql: "SELECT SUM(z) AS s FROM (SELECT w*2 AS z FROM " +
				"(SELECT id, SUM(a) OVER () AS w FROM decpair) y) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 953.82",
		},
		{
			name: "877 family: two sibling windows in one derived table",
			sql: "SELECT SUM(w1*2 + w2) AS s FROM " +
				"(SELECT SUM(a) OVER () AS w1, MAX(a) OVER () AS w2 FROM decpair) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 1068.57",
		},
		{
			name: "877 family: a PARTITIONED window under the aggregate",
			sql:  "SELECT SUM(w*2) AS s FROM (SELECT id, SUM(a) OVER (PARTITION BY s) AS w FROM decpair) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 131.48",
		},
		{
			name: "877 family: a window under an ORDER BY / LIMIT",
			sql:  "SELECT SUM(w*2) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair ORDER BY id LIMIT 5) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 529.90",
		},
		{
			name: "877 control: COUNT over the window alias",
			sql:  "SELECT COUNT(w) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x",
			want: "cols=[s:INT64] rows=1 | 9",
		},
		{
			name: "877 control: MAX over the window alias",
			sql:  "SELECT MAX(w) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 52.99",
		},
		{
			name: "877 control: a derived COMPUTED alias with no window",
			sql:  "SELECT SUM(v*2) AS s FROM (SELECT id, a*2 AS v FROM decpair) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 211.96",
		},
		{
			name: "877 control: a derived RENAME with no window",
			sql:  "SELECT SUM(v*2) AS s FROM (SELECT id, a AS v FROM decpair) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 105.98",
		},
		{
			// Arithmetic over an AGGREGATE's output: the shape whose respell
			// shipped `__agg_0 * 2` and hard-failed on both arms. It must
			// still be COMPUTED, not respelled.
			name: "877 control: arithmetic over an aggregate's output",
			sql:  "SELECT SUM(v) AS s FROM (SELECT SUM(a)*2 AS v FROM decpair GROUP BY s) x",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 105.98",
		},
		{
			// A JOIN ARM's alias IS materialized under its own name, so
			// respelling it to the source column is the defect, not the fix:
			// this shape answered 25.50 and a respell took it to 0.00.
			name: "877 control: a join arm's alias stays the alias",
			sql: "SELECT SUM(CASE WHEN x.s = '1.50' THEN x.v ELSE 0 END) AS s FROM " +
				"(SELECT s, a*2 AS v FROM decpair) x JOIN decpair y ON x.s = y.s",
			want: "cols=[s:DECIMAL(38,2)] rows=1 | 25.50",
		},
	})
}
