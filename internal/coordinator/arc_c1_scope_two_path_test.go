package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/worker"
)

// ARC C1 — the two PLANNER SCOPE shapes, on FIVE arms, against PostgreSQL
// 17.11 measured live BEFORE the code changed.
//
// #1033  a LATERAL body with NO FROM clause is a PROJECTION OVER THE OUTER ROW.
//
//	The decorrelation reads the body's WHERE and promotes the equalities it
//	finds there into join keys; a body with no WHERE has no correlated part,
//	so `u.id` in its SELECT list was promoted nowhere. `BuildFromSelect`
//	built `Project(Dual, [u.id AS v])`, the Dual carries no `u`, and every
//	projected value came back NULL under the STRING default an unresolvable
//	reference falls to — three NULLs and a text declaration where PostgreSQL
//	answers 1, 2, 3 as bigint. COUNT(*) was right throughout: the ROWS were
//	produced and only the VALUES were lost.
//
// #1047  a recursive CTE is materialized WHERE ITS BLOCK IS PLANNED.
//
//	`materializeCTEs` fills the cache from `root.CTEs` alone, so a recursive
//	CTE declared in a derived table, in another CTE's body, in a LATERAL or
//	in a set-operation arm was materialized by nobody; the tagged scan the
//	builder leaves for a recursive reference missed the cache and fell
//	through to a scan of a RELATION THAT DOES NOT EXIST, which answers zero
//	rows instead of failing.
//
// THE ROUTING IS ASSERTED BESIDE THE ROWS. Both families reach the DAG as
// plans the stage planner refuses — a Dual has no distributed stage (#806) and
// a recursive CTE has no distributed form at all (#1042) — so every DAG cell
// here names the counter it moves. Rows alone cannot tell an executed query
// from one the coordinator answered in-process, and a right-to-routed move is
// invisible to a row assertion.

// c1Arm is one execution arm: a name, a runner and the coordinator whose
// local-routing counters this arm's disposition is read from (nil on the
// single-process arms).
type c1Arm struct {
	name  string
	run   func(sql string) (string, error)
	coord *Coordinator
}

// c1Case is one shape: the SQL, PostgreSQL 17.11's answer as f1RenderSingle
// writes it, the routing counters each DAG arm must move, and a per-arm pin
// with its mechanism.
type c1Case struct {
	name   string
	sql    string
	want   string
	pin    map[string]string
	why    string
	routed map[string]string
}

// c1Arms stands the five arms up over the shared corpus, which already holds
// `lat_ord` — three orders with ids 1, 2, 3 and customers Alice, Bob, Carol,
// the rows every PostgreSQL measurement in this file was taken over.
func c1Arms(t *testing.T, ctx context.Context) []c1Arm {
	t.Helper()
	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })
	return []c1Arm{
		{"single", func(s string) (string, error) { return f1RenderSingle(ctx, single, s) }, nil},
		{"spilled512k", func(s string) (string, error) { return f1RenderSingle(ctx, spilled, s) }, nil},
		{"dag", func(s string) (string, error) { return f1RenderDAG(ctx, coord, s) }, coord},
		{"dag-shuffled", func(s string) (string, error) { return f1RenderDAG(ctx, coordB, s) }, coordB},
		{"dag-morsel4", func(s string) (string, error) { return f1RenderDAG(ctx, coordM, s) }, coordM},
	}
}

// c1RouteDelta renders which local-routing counters this query moved, over
// EVERY counter the coordinator publishes and not the one a shape expects: a
// refusal that moves to a different counter is still a refusal, and the cell
// must say which one it took.
func c1RouteDelta(before, after a2fRoutes) string {
	var moved []string
	for i, name := range after.names {
		if d := after.values[i] - before.values[i]; d != 0 {
			moved = append(moved, fmt.Sprintf("%s +%d", name, d))
		}
	}
	return strings.Join(moved, ", ")
}

func c1Run(t *testing.T, arms []c1Arm, cases []c1Case) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := a2fReadRoutes(arm.coord)
				got, err := arm.run(tc.sql)
				if err != nil {
					got = "ERR " + err.Error()
				}
				want := tc.want
				if p, ok := tc.pin[arm.name]; ok {
					want = p
				}
				ok := got == want
				if !ok && strings.HasPrefix(want, "ERR ") {
					// A refusal cell claims WHICH refusal, not how the door
					// that raised it worded its wrapper: the embedded API
					// says "building logical plan: …" and the coordinator
					// says "logical plan: …" for the same sentence.
					ok = strings.Contains(got, strings.TrimPrefix(want, "ERR "))
				}
				if !ok {
					why := ""
					if tc.why != "" {
						why = "\n  pinned: " + tc.why
					}
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s%s",
						tc.sql, arm.name, got, want, why)
				}
				// AN EMPTY COLUMN LIST IS NEVER AN ANSWER (arc N1): a
				// statement that produces a result set declares its columns
				// or fails, and two empty lists compare equal.
				if strings.HasPrefix(got, "cols=[] ") {
					t.Errorf("%s\n  arm %s answered with NO COLUMNS", tc.sql, arm.name)
				}
				if arm.coord != nil {
					if d := c1RouteDelta(before, a2fReadRoutes(arm.coord)); d != tc.routed[arm.name] {
						t.Errorf("%s\n  arm %s disposition: got %q, want %q",
							tc.sql, arm.name, d, tc.routed[arm.name])
					}
				}
			}
		})
	}
}

// c1TableLess is the disposition every #1033 cell MEASURES on the three DAG
// arms: the plan carries a Dual, the stage planner has no distributed
// single-row source to dispatch it with (physical.ErrTableLessSelectDistributed,
// #806), and the coordinator answers in-process. That is why the lowering
// keeps the Dual in the tree — one engine answers all five arms — and it is
// recorded rather than wished away.
var c1TableLess = map[string]string{
	"dag":          "TableLess +1",
	"dag-shuffled": "TableLess +1",
	"dag-morsel4":  "TableLess +1",
}

// c1LateralDualLowered is #1033's census: a table-less LATERAL body is a
// projection over the outer row, so the four shapes the issue names answer
// PostgreSQL's values under PostgreSQL's declared type.
func TestC1ATableLessLateralIsAProjectionOverTheOuterRow(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	c1Run(t, arms, []c1Case{
		// ---- the four shapes #1033 was filed for.
		{
			name:   "1033 the bitwise body",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT BITWISE_AND(u.id,3) AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
		},
		{
			// THE HEADLINE. PostgreSQL declares bigint (OID 20); the engine
			// answered OID 25 over three NULLs.
			name:   "1033 the bare outer column",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
		},
		{
			name: "1033 SUM over the lateral column",
			sql:  "SELECT SUM(l.v) AS s FROM lat_ord u, LATERAL (SELECT u.id AS v) l",
			// PostgreSQL 17.11: numeric, 6. The VALUE is the whole of what
			// #1033 lost — it was NULL on every arm — and it is right here.
			//
			// THE DECLARATION IS PINNED, and it is NOT this lowering's: the
			// identical shape over a plain derived table,
			// `SELECT SUM(s.v) FROM (SELECT u.id AS v FROM lat_ord u) s`,
			// declares float8 at this arc's base with no LATERAL anywhere in
			// it. An accumulating aggregate reads its input's declared width
			// through `aggInputColumnType`, which stops at a JOIN because
			// `emittedColTypes` has no arm for one — the "a declaration has to
			// RIDE through every materialization" gap ADR-0024 and #1018 name.
			// The control below is the twin that proves it, and it FAILS if
			// this ever starts agreeing.
			want:   "cols=[s:FLOAT64] rows=1 | 6",
			why:    "SUM over a materialized block declares float8 where PostgreSQL declares numeric — pre-existing, reproduces with no LATERAL (see the derived-table control)",
			routed: c1TableLess,
		},
		{
			name:   "1033 COUNT over the lateral",
			sql:    "SELECT COUNT(*) AS n FROM lat_ord u, LATERAL (SELECT u.id AS v) l",
			want:   "cols=[n:INT64] rows=1 | 3",
			routed: c1TableLess,
		},

		// ---- the twins the issue names.
		{
			name:   "1033 the grouped twin",
			sql:    "SELECT SUM(l.v) AS s FROM lat_ord u, LATERAL (SELECT u.id * 2 AS v) l",
			want:   "cols=[s:FLOAT64] rows=1 | 12",
			why:    "the same pinned SUM declaration",
			routed: c1TableLess,
		},
		{
			// The grouped twin with a GROUP BY over it: the values are per
			// group, which is what makes a pruned outer column visible. It
			// answered three NULLs because `u.id` was pruned off the scan —
			// nothing asked the outer relation to read what the body names.
			name:   "1033 the grouped twin under GROUP BY",
			sql:    "SELECT u.customer AS c, SUM(l.v) AS s FROM lat_ord u, LATERAL (SELECT u.id * 2 AS v) l GROUP BY u.customer ORDER BY 1",
			want:   "cols=[c:STRING s:FLOAT64] rows=3 | Alice,2 | Bob,4 | Carol,6",
			why:    "the same pinned SUM declaration",
			routed: c1TableLess,
		},
		{
			name:   "1033 the WHERE twin",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v) l WHERE l.v > 1 ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 2 | 3",
			routed: c1TableLess,
		},

		// ---- the shapes the lowering has to keep right.
		{
			name:   "two items of two types",
			sql:    "SELECT l.a, l.b FROM lat_ord u, LATERAL (SELECT u.id AS a, u.customer AS b) l ORDER BY 1",
			want:   "cols=[a:INT64 b:STRING] rows=3 | 1,Alice | 2,Bob | 3,Carol",
			routed: c1TableLess,
		},
		{
			name:   "a FLOAT column keeps its declaration",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.total AS v) l ORDER BY 1",
			want:   "cols=[v:FLOAT64] rows=3 | 0 | 150 | 200",
			routed: c1TableLess,
		},
		{
			name:   "a function over an outer column",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT UPPER(u.customer) AS v) l ORDER BY 1",
			want:   "cols=[v:STRING] rows=3 | ALICE | BOB | CAROL",
			routed: c1TableLess,
		},
		{
			// A SECOND table-less lateral reading the FIRST one's column: the
			// chain is what proves the join publishes the item rather than
			// the operator merely computing it.
			name:   "a chained table-less lateral",
			sql:    "SELECT m.w FROM lat_ord u, LATERAL (SELECT u.id AS v) l, LATERAL (SELECT l.v AS w) m ORDER BY 1",
			want:   "cols=[w:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
		},
		{
			name:   "GROUP BY the lateral's own column",
			sql:    "SELECT l.v AS v, COUNT(*) AS n FROM lat_ord u, LATERAL (SELECT u.id % 2 AS v) l GROUP BY l.v ORDER BY 1",
			want:   "cols=[v:INT64 n:INT64] rows=2 | 0,1 | 1,2",
			routed: c1TableLess,
		},
		{
			name:   "the body's own WHERE filters the outer row",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v WHERE u.id > 1) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 2 | 3",
			routed: c1TableLess,
		},
		{
			name:   "the written ON filters the lateral's column",
			sql:    "SELECT l.v FROM lat_ord u JOIN LATERAL (SELECT u.id AS v) l ON l.v > 1 ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 2 | 3",
			routed: c1TableLess,
		},
		{
			// A table-less body always yields a row, so LEFT JOIN … ON true
			// pads nothing and is the inner join under another spelling. It
			// used to be the loud `could not extract join keys from: `.
			name:   "LEFT JOIN LATERAL ON true",
			sql:    "SELECT u.id, l.v FROM lat_ord u LEFT JOIN LATERAL (SELECT u.id AS v) l ON true ORDER BY 1",
			want:   "cols=[id:INT64 v:INT64] rows=3 | 1,1 | 2,2 | 3,3",
			routed: c1TableLess,
		},
		{
			// AN ITEM MAY COLLIDE WITH AN OUTER COLUMN, and those are TWO
			// columns. PostgreSQL answers `Alice, 1` under (text, bigint); a
			// lowering that wrote the item OVER the outer column answered
			// `1, 1` and lost `u.customer` for the rest of the query, which is
			// a right answer turned wrong. The item is emitted qualified by the
			// lateral's alias — the spelling the join already uses for a build
			// column that collides with a probe column — so both names resolve
			// to their own value.
			name: "an item colliding with an outer column",
			sql: "SELECT u.customer AS uc, l.customer AS lc FROM lat_ord u, " +
				"LATERAL (SELECT u.id AS customer) l ORDER BY 1",
			want:   "cols=[uc:STRING lc:INT64] rows=3 | Alice,1 | Bob,2 | Carol,3",
			routed: c1TableLess,
		},
		{
			// …and the OUTER column is still the outer column everywhere else.
			// This is the cell the replace-in-place lowering failed.
			name:   "the outer column survives a colliding item",
			sql:    "SELECT u.id, u.customer FROM lat_ord u, LATERAL (SELECT u.id AS customer) l ORDER BY 1",
			want:   "cols=[id:INT64 customer:STRING] rows=3 | 1,Alice | 2,Bob | 3,Carol",
			routed: c1TableLess,
		},
		{
			// A BARE reference to the colliding name is ambiguous, which is
			// PostgreSQL's 42702 and was already this engine's answer.
			name:   "a bare reference to a colliding name is 42702",
			sql:    "SELECT customer FROM lat_ord u, LATERAL (SELECT u.id AS customer) l",
			want:   "ERR column reference \"customer\" is ambiguous",
			routed: map[string]string{},
		},
		{
			// CONTROL: a body with no outer reference at all was RIGHT before
			// this lowering and has to stay right — it is the cell that says
			// the lowering did not break the shape it did not need to fix.
			name:   "control: a constant body",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT 7 AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// CONTROL: the body WITH a FROM clause takes the decorrelation
			// path and this lowering must never see it. Right before, right
			// after, and it EXECUTES on the DAG rather than routing.
			name: "control: a lateral body with a FROM clause",
			sql:  "SELECT l.v FROM lat_ord u, LATERAL (SELECT z.id AS v FROM lat_ord z WHERE z.id = u.id) l ORDER BY 1",
			want: "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			// It EXECUTES as stages on all three DAG arms — no counter moves —
			// which is the other half of the claim: the lowering did not push
			// a shape it does not own onto the local route.
			routed: map[string]string{},
		},
		{
			// THE OUTER SIDE NEED NOT BE A SCAN. `inputColDecls` stops at a
			// Project, so a derived-table, CTE, sorted or aggregated outer gave
			// every item the STRING default — the right VALUES under OID 25,
			// which is the half of #1033 the issue was filed for, one position
			// over (round-2 review, P1/B2i). The item is typed against what the
			// outer side PUBLISHES.
			name:   "an item over a DERIVED outer side declares the column's type",
			sql:    "SELECT l.v FROM (SELECT id FROM lat_ord) u, LATERAL (SELECT u.id AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
		},
		{
			name:   "an item over a CTE outer side declares the column's type",
			sql:    "WITH z AS (SELECT id FROM lat_ord) SELECT l.v FROM z u, LATERAL (SELECT u.id AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
		},
		{
			name:   "an item over a SORTED outer side declares the column's type",
			sql:    "SELECT l.v FROM (SELECT id FROM lat_ord ORDER BY id LIMIT 2) u, LATERAL (SELECT u.id AS v) l ORDER BY 1",
			want:   "cols=[v:INT64] rows=2 | 1 | 2",
			routed: c1TableLess,
		},
		{
			// THE FROM ITEM'S COLUMN-ALIAS LIST renames the body's items
			// positionally. The lowering published each item under its own
			// alias, so the list renamed a column nothing carried and `l.w`
			// answered NULL — #1033's headline shape under a second spelling
			// (round-2 review, P2/B2ii).
			name:   "a column-alias list renames the item",
			sql:    "SELECT l.w FROM lat_ord u, LATERAL (SELECT u.id AS v) l(w) ORDER BY 1",
			want:   "cols=[w:INT64] rows=3 | 1 | 2 | 3",
			routed: c1TableLess,
		},
		{
			name: "a column-alias list renames two items of two types",
			sql: "SELECT l.w, l.x FROM lat_ord u, LATERAL (SELECT u.id AS v, u.customer AS c) l(w,x) " +
				"ORDER BY 1",
			want:   "cols=[w:INT64 x:STRING] rows=3 | 1,Alice | 2,Bob | 3,Carol",
			routed: c1TableLess,
		},
		{
			// …and on the UNCORRELATED body too, which takes the base path.
			name:   "a column-alias list over an uncorrelated body",
			sql:    "SELECT l.w FROM lat_ord u, LATERAL (SELECT 7 AS v) l(w) ORDER BY 1",
			want:   "cols=[w:INT64] rows=3 | 7 | 7 | 7",
			routed: c1TableLess,
		},
		{
			// A list LONGER than the body is PostgreSQL's 42P10, with its own
			// sentence.
			name:   "a column-alias list longer than the body is 42P10",
			sql:    "SELECT l.w FROM lat_ord u, LATERAL (SELECT u.id AS v) l(w,x) ORDER BY 1",
			want:   "ERR table \"l\" has 1 columns available but 2 columns specified",
			routed: map[string]string{},
		},
		{
			// A COLUMN-ALIAS LIST OVER A `SELECT *` BODY is refused only when
			// the enclosing query READS a name the list introduces. PostgreSQL
			// applies a SHORT list to the first k columns of the expansion and
			// leaves the rest under their own names, so a query that never
			// mentions a renamed name is unaffected — refusing on the PRESENCE
			// of the list was ten cells right → refused (round-3 review, B2).
			name: "a star body whose alias list the query never reads",
			sql: "SELECT u.id FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want: "cols=[id:INT64] rows=4 | 1 | 1 | 2 | 2",
			// It EXECUTES as stages on all three DAG arms.
			routed: map[string]string{},
		},
		{
			name: "a star body, reading a column the list does NOT rename",
			sql: "SELECT l.amount FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want:   "cols=[amount:FLOAT64] rows=4 | 50 | 75 | 100 | 125",
			routed: map[string]string{},
		},
		{
			// READ, and therefore refused: the width the rename needs is not
			// knowable here, and a positional rename over an uncounted star can
			// take the column the decorrelation keys on. Wrong → loud.
			name: "a star body whose alias list the query READS",
			sql: "SELECT l.w FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want:   "ERR the query reads \"w\", a name the column-alias list on table \"l\" introduces",
			why:    "PostgreSQL answers 1,2,3,4; both bases answered four NULLs",
			routed: map[string]string{},
		},
		{
			name: "a star body whose alias list the query READS, two names",
			sql: "SELECT l.w, l.x FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w,x) ORDER BY 1",
			want:   "ERR the query reads",
			why:    "PostgreSQL answers 1|1, 2|1, 3|2, 4|2; both bases answered four NULL,NULL",
			routed: map[string]string{},
		},
		{
			// An over-long list is PostgreSQL's 42P10 whether or not it is read.
			name: "a column-alias list longer than a star body",
			sql: "SELECT l.w FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w,x,y,z,zz) ORDER BY 1",
			want:   "ERR table \"l\" has 4 columns available but 5 columns specified",
			routed: map[string]string{},
		},
		{
			// CONTROL: no list at all.
			name: "control: a star body with no alias list",
			sql: "SELECT l.amount FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l ORDER BY 1",
			want:   "cols=[amount:FLOAT64] rows=4 | 50 | 75 | 100 | 125",
			routed: map[string]string{},
		},
		{
			// THE READ IS FOUND BY THE ONE WALK (round-4 review, B2). The old
			// test was a second `RewriteExpr` scan over the enclosing block
			// only, and `RewriteExpr` enters neither an aggregate call nor a
			// window call — so four spellings of "the query reads w" were
			// invisible and the query answered plausible NULLs under a sentence
			// promising a refusal.
			name:   "a read inside a WINDOW call",
			sql:    "SELECT SUM(l.w) OVER () AS s FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want:   "ERR the query reads",
			why:    "PostgreSQL answers 10,10,10,10; RewriteExpr enters no window call, so this answered four NULLs",
			routed: map[string]string{},
		},
		{
			// THE HAVING POSITION, and ONLY it (round-5 review, P1). The cell
			// this replaces also held `MAX(l.w)` in its SELECT LIST, so the
			// SELECT-list aggregate found the read and the cell passed at the
			// round-4 tip too — it gated the aggregate, never the HAVING
			// clause. Here HAVING is the only place the name appears.
			name:   "a read ONLY inside an aggregate in HAVING",
			sql:    "SELECT u.id, COUNT(*) AS c FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w) GROUP BY u.id HAVING MAX(l.w) > 2 ORDER BY 1",
			want:   "ERR the query reads",
			why:    "PostgreSQL answers one row, 2,2; the round-4 tip answered ZERO rows under no refusal",
			routed: map[string]string{},
		},
		{
			name:   "a read in a SECOND lateral's body",
			sql:    "SELECT m.z FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w), LATERAL (SELECT l.w + 100 AS z) m ORDER BY 1",
			want:   "ERR the query reads",
			why:    "PostgreSQL answers 101..104; a later FROM item's body is not an expression of this block",
			routed: map[string]string{},
		},
		{
			// CONTROL, and the boundary of the cell above (round-5 review, B1):
			// a LATER lateral that publishes its OWN column called `w` and
			// reads it as `b.w` is reading ITS name, not this lateral's. Keyed
			// on the name alone the position fired here too.
			name: "control: a sibling lateral's own name equal to the list name",
			sql: "SELECT b.w, u.id FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w), LATERAL (SELECT w FROM (SELECT 2 AS w) y) b ORDER BY 2, 1",
			want: "cols=[w:INT64 id:INT64] rows=4 | 2,1 | 2,1 | 2,2 | 2,2",
			// The sibling's own body is table-less under its derived table, so
			// the DAG arms take #806's local route — the existing disposition
			// of a table-less item, not this cell's subject.
			routed: c1TableLess,
		},
		{
			// CONTROL: the same, with the sibling EARLIER in the FROM clause —
			// a position the walk reaches from the other side.
			name: "control: an earlier sibling lateral's own name equal to the list name",
			sql: "SELECT a.w, u.id FROM lat_ord u, LATERAL (SELECT w FROM (SELECT 1 AS w) x) a, " +
				"LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w) ORDER BY 2",
			want:   "cols=[w:INT64 id:INT64] rows=4 | 1,1 | 1,1 | 1,2 | 1,2",
			routed: c1TableLess,
		},
		{
			name:   "a read ONE BLOCK UP through a star",
			sql:    "SELECT x.w FROM (SELECT * FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w)) x ORDER BY 1",
			want:   "ERR the query reads",
			why:    "PostgreSQL answers 1,2,3,4; a star in the enclosing block republishes every name it holds",
			routed: map[string]string{},
		},
		{
			// CONTROL, and the boundary of the cell above (round-5 review, B1):
			// a star QUALIFIED with the outer relation republishes the OUTER
			// relation's names, not this lateral's. Keyed on the name alone the
			// position fired here and refused a query PostgreSQL, main and the
			// round-4 tip all answer.
			name:   "control: a star qualified with the OUTER relation is not a read",
			sql:    "SELECT u.* FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want:   "cols=[id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,Alice,150 | 1,Alice,150 | 2,Bob,200 | 2,Bob,200",
			routed: map[string]string{},
		},
		{
			// CONTROL: the same qualified star beside a named outer column, so
			// the block holds both a star and a plain column reference.
			name: "control: a qualified star beside a named outer column",
			sql:  "SELECT u.*, u.id AS again FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 again:INT64] rows=4 | 1,Alice,150,1 | 1,Alice,150,1 | 2,Bob,200,2 | 2,Bob,200,2",
			// The star expanded beside the same column under another name is a
			// DAG-unbuildable output, so the DAG arms answer it in-process —
			// again an existing disposition, and the VALUES are the same.
			routed: map[string]string{
				"dag": "UnreachableOutput +1", "dag-shuffled": "UnreachableOutput +1",
				"dag-morsel4": "UnreachableOutput +1",
			},
		},
		{
			name:   "a read inside COALESCE",
			sql:    "SELECT COALESCE(l.w, 0) AS c FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want:   "ERR the query reads",
			why:    "PostgreSQL answers 1,2,3,4",
			routed: map[string]string{},
		},
		{
			// A SORT TERM IS A READ (round-5 review, B2). Round 5 excluded the
			// enclosing ORDER BY on the reasoning that a sort term decides the
			// ORDER and never the values — but on the lowered path the rename
			// is dropped, so the sort term binds NOTHING and the order is
			// whatever the scan hands over. That is a wrong ORDER under no
			// refusal, which the two cells below make visible. The rename
			// cannot be applied here for the same reason it cannot be applied
			// in the SELECT list — the star's width is not known where the
			// rename must be made (round-3 measured the alternative: deferring
			// the list past the star answered ZERO ROWS, because the join keyed
			// on a renamed column) — so the disposition is the refusal.
			name: "a read only in the ORDER BY refuses",
			sql: "SELECT l.amount FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY l.w, l.amount",
			want:   "ERR the query reads",
			why:    "PostgreSQL sorts by l.w; the round-5 tip sorted by nothing and called it an answer",
			routed: map[string]string{},
		},
		{
			// THE CELL THAT MAKES THE WRONG ORDER VISIBLE: the same shape with
			// DESC. An unbound sort term coincides with PostgreSQL's answer
			// ascending and disagrees with it descending — which is why the
			// ascending spelling looked like an agreement for five rounds.
			name:   "a DESC sort term over an alias-list name refuses",
			sql:    "SELECT u.id FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w) ORDER BY l.w DESC",
			want:   "ERR the query reads",
			why:    "PostgreSQL answers 2,2,1,1; main and the round-5 tip answered 1,1,2,2",
			routed: map[string]string{},
		},
		{
			// A PERMUTED FULL LIST: every name is the list's, and the sort term
			// names the list's fourth column. Sorting on the body's own fourth
			// column instead is a different order over the same rows.
			name: "a permuted alias list read only by the sort term refuses",
			sql: "SELECT u.customer FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(order_id, id, amount, product) ORDER BY l.product",
			want:   "ERR the query reads",
			why:    "PostgreSQL answers Alice,Bob,Alice,Bob; main sorted by a different column",
			routed: map[string]string{},
		},
		{

			// CONTROL: the two FROM items the deferral IS wired into keep
			// applying the list, which is what makes this a lateral-only
			// disposition rather than a lost feature.
			name:   "control: a derived table's alias list over a star",
			sql:    "SELECT q.w FROM (SELECT * FROM lat_item) q(w) ORDER BY 1",
			want:   "cols=[w:INT64] rows=4 | 1 | 2 | 3 | 4",
			routed: map[string]string{},
		},
		{
			name:   "control: a CTE's alias list over a star",
			sql:    "WITH q(w) AS (SELECT * FROM lat_item) SELECT w FROM q ORDER BY 1",
			want:   "cols=[w:INT64] rows=4 | 1 | 2 | 3 | 4",
			routed: map[string]string{},
		},
		{
			// CONTROL: a NAMED list over a lateral is unaffected — it is the
			// round-2 repair and it still applies.
			name: "control: a named list over a lateral still renames",
			sql: "SELECT l.w FROM lat_ord u, LATERAL (SELECT i.id AS z FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want: "cols=[w:INT64] rows=4 | 1 | 2 | 3 | 4",
			// It EXECUTES as stages on all three DAG arms.
			routed: map[string]string{},
		},
		{
			// PINNED, and NOT the lateral's: an ARRAY literal declares STRING
			// wherever it is written. `SELECT ARRAY[id, id] AS v FROM lat_ord`
			// declares STRING too, with no LATERAL in the query. The VALUES are
			// right on both.
			name:   "an ARRAY item carries its values under a pinned declaration",
			sql:    "SELECT l.v FROM lat_ord u, LATERAL (SELECT ARRAY[u.id, u.id] AS v) l ORDER BY 1",
			want:   "cols=[v:STRING] rows=3 | [1 1] | [2 2] | [3 3]",
			why:    "PostgreSQL declares bigint[]; an ARRAY literal declares STRING engine-wide, with or without a LATERAL",
			routed: c1TableLess,
		},
		{
			// PINNED, the second of the two the declaration sentence names: a
			// table-less lateral inside a SCALAR SUBQUERY's own block declares
			// STRING with the right value, where the same MAX written at the
			// top level declares INT64. The sentence said "both are pinned" and
			// only one cell existed (round-3 review, P1); this is the other.
			name: "an item inside a scalar subquery's block is pinned",
			sql: "SELECT u.id, (SELECT MAX(l.v) FROM lat_ord z, " +
				"LATERAL (SELECT z.id AS v) l) AS m FROM lat_ord u ORDER BY 1",
			want: "cols=[id:INT64 m:STRING] rows=3 | 1,3 | 2,3 | 3,3",
			why:  "PostgreSQL declares bigint; the values are right on both",
			routed: map[string]string{
				"dag": "UnbuildableStage +1", "dag-shuffled": "UnbuildableStage +1",
				"dag-morsel4": "UnbuildableStage +1",
			},
		},
		{
			// PINNED: a derived outer side that is EMPTY declares STRING where
			// the same shape with rows declares INT64. A zero-row declaration
			// is described from the plan alone, and this one is not — the same
			// family as the SUM pin below.
			name:   "an item over an EMPTY derived outer side is pinned",
			sql:    "SELECT l.v FROM (SELECT id FROM lat_ord WHERE id > 99) u, LATERAL (SELECT u.id AS v) l ORDER BY 1",
			want:   "cols=[v:STRING] rows=0",
			why:    "PostgreSQL declares bigint on a zero-row result; the non-empty twin above declares INT64",
			routed: c1TableLess,
		},
		{
			// CONTROL + PIN: the SUM declaration divergence with no LATERAL in
			// the query. It is the proof that the pinned cells above are not
			// this lowering's, and it FAILS when it starts agreeing — at which
			// point those pins come out in the same commit.
			name: "control: SUM over a plain derived block declares the same thing",
			sql:  "SELECT SUM(s.v) AS s FROM (SELECT u.id AS v FROM lat_ord u) s",
			want: "cols=[s:FLOAT64] rows=1 | 6",
			// AND THE TWO ENGINES DISAGREE ABOUT IT. The DAG declares
			// DECIMAL(38,0) — PostgreSQL's numeric — for the same query the
			// single path declares float8 for. Both are recorded: a pin that
			// carried one expectation for both could not state what the
			// divergence IS (#993).
			pin: map[string]string{
				"dag":          "cols=[s:DECIMAL(38,0)] rows=1 | 6",
				"dag-shuffled": "cols=[s:DECIMAL(38,0)] rows=1 | 6",
				"dag-morsel4":  "cols=[s:DECIMAL(38,0)] rows=1 | 6",
			},
			why:    "PostgreSQL 17.11 declares numeric; the single path declares float8 and the DAG declares numeric — the pre-existing gap the two SUM cells above inherit",
			routed: map[string]string{},
		},
	})
}

// TestC1BTableLessLateralRefusesWhatItCannotProject is #1033's BOUNDARY, and
// the boundary is a claim: every body class the projection lowering does not
// express is attempted here, and each one is 0A000 naming the feature rather
// than the plausible NULLs it answered before.
func TestC1BTableLessLateralRefusesWhatItCannotProject(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	// The SENTENCE, without the door's own wrapper — "building logical plan: …"
	// through the embedded API and "logical plan: …" through the coordinator.
	const refusal = "ERR logical plan: a LATERAL subquery with no FROM clause is " +
		"computed as a projection over the outer row"
	noRoute := map[string]string{}
	c1Run(t, arms, []c1Case{
		{name: "an aggregate body", routed: noRoute,
			sql:  "SELECT l.v FROM lat_ord u, LATERAL (SELECT MAX(u.id) AS v) l",
			want: refusal},
		{name: "a GROUP BY body", routed: noRoute,
			sql:  "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v GROUP BY u.id) l",
			want: refusal},
		{name: "a DISTINCT body", routed: noRoute,
			sql:  "SELECT l.v FROM lat_ord u, LATERAL (SELECT DISTINCT u.id AS v) l",
			want: refusal},
		// An ORDER BY is NOT here: a table-less body yields at most one row, so
		// its sort is the identity and is dropped rather than refused (round-3
		// brief, B1). The lowered answer is a cell of TestC1EATheBodyClassTable.
		{name: "a LIMIT body", routed: noRoute,
			sql:  "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v LIMIT 1) l",
			want: refusal},
		{name: "a set-operation body", routed: noRoute,
			sql:  "SELECT l.v FROM lat_ord u, LATERAL (SELECT u.id AS v UNION ALL SELECT 9) l",
			want: refusal},
		{name: "a WITH body", routed: noRoute,
			sql:  "SELECT l.v FROM lat_ord u, LATERAL (WITH n AS (SELECT 1 AS x) SELECT u.id AS v) l",
			want: refusal},
		{name: "an outer join whose ON does not fold to true", routed: noRoute,
			sql:  "SELECT u.id, l.v FROM lat_ord u LEFT JOIN LATERAL (SELECT u.id AS v) l ON l.v > 1",
			want: refusal},
		{name: "an outer join over a body with a WHERE", routed: noRoute,
			sql:  "SELECT u.id, l.v FROM lat_ord u LEFT JOIN LATERAL (SELECT u.id AS v WHERE u.id > 1) l ON true",
			want: refusal},
	})
}
