package coordinator

import (
	"context"
	"testing"
	"time"
)

// #1047 — A RECURSIVE CTE IS MATERIALIZED WHERE ITS BLOCK IS PLANNED, on five
// arms, against PostgreSQL 17.11 measured live before the code changed.
//
// The builder does not expand a recursive CTE into the plan — that would
// re-enter its own body without bound — so it leaves a tagged Scan and the
// physical planner serves it from `cteCache`. `materializeCTEs` filled that
// cache from `root.CTEs` ALONE, so a recursive CTE declared in a derived
// table, in another CTE's body, in a LATERAL or in a set-operation arm was
// materialized by nobody: the lookup missed and the tagged scan fell through
// to a scan of a RELATION THAT DOES NOT EXIST, which answers zero rows instead
// of failing — the same "a missing cache entry is an empty CTE" door #1041
// names, one level up.
//
// The definition now rides on the REFERENCE (logical.Node.RecursiveCTE), the
// physical planner materializes a miss from it, and a miss it cannot serve is
// a refusal rather than an empty relation.
//
// TWO THINGS THE ROWS CANNOT SAY, both asserted:
//   - the DISPOSITION. A recursive CTE has no distributed form (#1042), so
//     every cell here routes to the coordinator's in-process pipeline and the
//     counter that moves is named per cell.
//   - the IDENTITY of the materialization. Two sibling blocks may each declare
//     `WITH RECURSIVE r`, and they are two relations: the statement-wide cache
//     is keyed by NAME, so the second materialization overwrote the first and
//     both references read the second one's rows. The census carries that pair.

// c1RecDAGRefusal is what EVERY recursive CTE does on the three DAG arms, this
// arc's shapes and the one at the STATEMENT ROOT alike: the tagged scan the
// builder leaves for a recursive reference becomes a stage with no scan files
// and no dependencies, and the dispatcher fails it. That is #1042 — the DAG
// cannot run any recursive CTE — it is LOUD, it is measured on the root
// control below, and it is not this arc's to fix.
//
// Matched as a SUBSTRING (see c1Run): the stage id in the message is the
// planner's own numbering and moves with the shape.
const c1RecDAGRefusal = "ERR has no dependencies and no ScanFiles"

// c1RecDAGPins is that refusal on the three distributed arms.
func c1RecDAGPins() map[string]string {
	return map[string]string{
		"dag": c1RecDAGRefusal, "dag-shuffled": c1RecDAGRefusal,
		"dag-morsel4": c1RecDAGRefusal,
	}
}

// c1RecRoutes is the disposition those arms record: the plan is DISPATCHED and
// fails there, so no local-routing counter moves.
var c1RecRoutes = map[string]string{}

func TestC1CARecursiveCTEIsMaterializedWhereItsBlockIsPlanned(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	const rec = "WITH RECURSIVE r AS (SELECT id AS v FROM lat_ord WHERE id=1 " +
		"UNION ALL SELECT v+1 FROM r WHERE v<3) SELECT v FROM r"

	c1Run(t, arms, []c1Case{
		{
			// CONTROL: the same CTE at the STATEMENT ROOT, which was right
			// before this fix. It is the bar every nested cell below is held
			// to — "nested answers what top-level answers" is the whole claim.
			name:   "control: the recursive CTE at the statement root",
			sql:    rec + " ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			name:   "1047 inside a derived table",
			sql:    "SELECT q.v FROM (" + rec + ") q ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			name:   "1047 inside another CTE's body",
			sql:    "WITH o AS (" + rec + ") SELECT v FROM o ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			// COUNT is the cell that shows the rows were LOST rather than
			// merely unreadable: it answered 0 where PostgreSQL answers 3.
			name:   "1047 counted through a derived table",
			sql:    "SELECT COUNT(*) AS n FROM (" + rec + ") q",
			want:   "cols=[n:INT64] rows=1 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			name: "1047 as a set-operation arm",
			sql:  "SELECT v FROM (SELECT 99 AS v) z UNION ALL SELECT q.v FROM (" + rec + ") q ORDER BY 1",
			want: "cols=[v:INT64] rows=4 | 1 | 2 | 3 | 99",
			// THE ONLY CELL HERE THE DAG ANSWERS, and it does so because the
			// other arm — `SELECT 99` — is TABLE-LESS: the plan carries a Dual,
			// the stage planner refuses it (#806) and the coordinator answers
			// the whole query in-process, recursive CTE included. The route is
			// what makes the rows right, so the route is asserted.
			routed: c1TableLess,
		},
		{
			name: "1047 inside a LATERAL",
			sql: "SELECT u.id, q.m FROM lat_ord u, LATERAL (WITH RECURSIVE r AS " +
				"(SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v<3) " +
				"SELECT MAX(v) AS m FROM r) q ORDER BY 1",
			want:   "cols=[id:INT64 m:INT64] rows=3 | 1,3 | 2,3 | 3,3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			// A RECURSIVE CTE inside a RECURSIVE CTE's own body: the inner one
			// is materialized while the outer one is iterating, so the name
			// binding the iteration seeds for ITS self-reference has to
			// survive the inner materialization.
			name: "1047 inside a recursive CTE's own body",
			sql: "WITH RECURSIVE o AS (WITH RECURSIVE i AS (SELECT 1 AS v UNION ALL " +
				"SELECT v+1 FROM i WHERE v<3) SELECT v FROM i UNION ALL " +
				"SELECT v+10 FROM o WHERE v<5) SELECT v FROM o ORDER BY 1",
			want:   "cols=[v:INT64] rows=6 | 1 | 2 | 3 | 11 | 12 | 13",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			name:   "1047 two derived-table levels deep",
			sql:    "SELECT z.v FROM (SELECT q.v FROM (" + rec + ") q) z ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			// TWO REFERENCES to ONE nested definition: both must read the same
			// materialization, which is what keying by the definition rather
			// than by the reference buys.
			name:   "1047 one nested definition referenced twice",
			sql:    "WITH o AS (" + rec + ") SELECT a.v FROM o a JOIN o b ON a.v=b.v ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			// TWO DEFINITIONS, ONE NAME. The statement-wide cache is keyed by
			// name; without a per-definition identity the second
			// materialization overwrote the first and BOTH blocks answered
			// 10, 11 — `20 | 21 | 21 | 22` where PostgreSQL answers
			// `11 | 12 | 12 | 13`.
			name: "1047 two sibling blocks declaring the same name",
			sql: "SELECT a.v + b.v AS s FROM " +
				"(WITH RECURSIVE r AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM r WHERE v<2) SELECT v FROM r) a, " +
				"(WITH RECURSIVE r AS (SELECT 10 AS v UNION ALL SELECT v+1 FROM r WHERE v<11) SELECT v FROM r) b " +
				"ORDER BY 1",
			// PostgreSQL 17.11 declares integer; every computed integer in
			// this engine is carried in an int64 and declared float8 through a
			// materialized block (ADR-0024's recorded widening, #1018) — the
			// VALUES are the claim here and they are PostgreSQL's.
			want: "cols=[s:FLOAT64] rows=4 | 11 | 12 | 12 | 13",
			pin:  c1RecDAGPins(),
			why: "#1042 on the DAG arms; the computed-integer declaration through a " +
				"materialized block is #1018's, not this one's",
			routed: c1RecRoutes,
		},
		{
			// A COLUMN ALIAS LIST on a nested recursive CTE: the positional
			// rename runs inside the materialization, so it has to work the
			// same way it does at the root (#957).
			name: "1047 a nested recursive CTE with a column alias list",
			sql: "SELECT q.a FROM (WITH RECURSIVE r(a) AS (SELECT 1 UNION ALL " +
				"SELECT a+1 FROM r WHERE a<3) SELECT a FROM r) q ORDER BY 1",
			want:   "cols=[a:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			// CONTROL: a NON-recursive nested CTE is INLINED by the builder and
			// never reaches the cache at all. Right before, right after.
			name:   "control: a plain CTE nested in a derived table",
			sql:    "SELECT q.v FROM (WITH n AS (SELECT id AS v FROM lat_ord WHERE id<=3) SELECT v FROM n) q ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			routed: map[string]string{},
		},
		{
			// CONTROL: `WITH RECURSIVE` with no self-reference. The keyword
			// alone routes it through the recursive path, and it answered ZERO
			// ROWS nested at this arc's base.
			name:   "control: WITH RECURSIVE with no self-reference, nested",
			sql:    "SELECT q.v FROM (WITH RECURSIVE n AS (SELECT id AS v FROM lat_ord WHERE id<=3) SELECT v FROM n) q ORDER BY 1",
			want:   "cols=[v:INT64] rows=3 | 1 | 2 | 3",
			pin:    c1RecDAGPins(),
			why:    "#1042: the DAG cannot run any recursive CTE, at the root or nested",
			routed: c1RecRoutes,
		},
		{
			// CONTROL: a name that is NOT a CTE is still 42P01 and never an
			// empty relation. The refusal this fix installs must not have made
			// a real missing table quieter.
			name:   "control: a missing relation is still 42P01",
			sql:    "SELECT v FROM c1_no_such_relation",
			want:   "ERR relation \"c1_no_such_relation\" does not exist",
			routed: map[string]string{},
		},
	})
}
