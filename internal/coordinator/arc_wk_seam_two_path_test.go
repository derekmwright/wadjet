// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// THE WINDOW-KEY / NAME-OWNERSHIP SEAM, ENUMERATED ONCE — arc WK.
//
// The rule that settles it is docs/design/window-key-ownership.md (ADR-0026
// §8j): a window key, a sort key, a lifted predicate column and a join-arm
// reference bind by IDENTITY — the OCCURRENCE that produced the column — and a
// NAME is derived from the identity for publication, never the reverse.
//
// The table crosses every CONSUMER with every PRODUCER an arm can be, on five
// arms, against live PostgreSQL 17.11:
//
//	{window PARTITION BY, window ORDER BY, window ARGUMENT, sort key,
//	 join-arm reference, star} × {base scan, derived block, LATERAL, set
//	 operation, grouped block, nested block}
//	  + {lifted predicate} × {LATERAL} × three spellings
//
// 50 cells × 5 arms = 250 results, 216 agreeing. Every producer publishes `id`,
// which `lat_ord o` also publishes, so ownership is LIVE in every cell. What
// remains is the LATERAL producer and an expression key naming two occurrences
// — DAG-only, `distributed`, pinned per arm (#1126). Excluded dimensions: the
// memo's own list. A pin that starts agreeing FAILS (arc JP round 2 deleted
// the sortkey/lateral and armref/lateral pins: the DAG now resolves `p.id` to
// the LATERAL body's source column, ADR-0026 §8l).
func TestWKASeamConsumerBindsItsOwnOccurrence(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the ownership seam's own table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	c1Run(t, c1Arms(t, ctx), wkSeamCells())
}

func wkSeamCells() []c1Case {
	return []c1Case{
		{
			name: "winpart/base",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY p.id) AS n FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
		},
		{
			name: "winorder/base",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (ORDER BY p.id) AS n FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,2 | 2,3,3 | 2,4,4",
		},
		{
			name: "winarg/base",
			sql:  "SELECT o.id AS a, p.id AS b, SUM(o.id) OVER () AS n FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,2,6 | 2,3,6 | 2,4,6",
		},
		{
			name: "sortkey/base",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY p.id DESC, a",
			want: "cols=[a:INT64 b:INT64] rows=4 | 2,4 | 2,3 | 1,2 | 1,1",
		},
		{
			name: "armref/base",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,2 | 2,3 | 2,4",
		},
		{
			name: "star/base",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY 1, 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "winpart/derived",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY p.id) AS n FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
		},
		{
			name: "winorder/derived",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (ORDER BY p.id) AS n FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,2 | 2,3,3 | 2,4,4",
		},
		{
			name: "winarg/derived",
			sql:  "SELECT o.id AS a, p.id AS b, SUM(o.id) OVER () AS n FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,2,6 | 2,3,6 | 2,4,6",
		},
		{
			name: "sortkey/derived",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item) p ON p.order_id = o.id ORDER BY p.id DESC, a",
			want: "cols=[a:INT64 b:INT64] rows=4 | 2,4 | 2,3 | 1,2 | 1,1",
		},
		{
			name: "armref/derived",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,2 | 2,3 | 2,4",
		},
		{
			name: "star/derived",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item) p ON p.order_id = o.id ORDER BY 1, 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,1,50 | 1,Alice,150,2,1,100 | 2,Bob,200,3,2,75 | 2,Bob,200,4,2,125",
		},
		{
			name: "winpart/lateral",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY p.id) AS n FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
			// ARC LT round 2: the DAG routes a window above a LATERAL join to
			// the single-process pipeline (dagplan.refuseWindowOverDependentJoin),
			// so the DAG-only pin that stood here agrees now and is deleted.
		},
		{
			name: "winorder/lateral",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (ORDER BY p.id) AS n FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,2 | 2,3,3 | 2,4,4",
			// ARC LT round 2: the DAG routes a window above a LATERAL join to
			// the single-process pipeline (dagplan.refuseWindowOverDependentJoin),
			// so the DAG-only pin that stood here agrees now and is deleted.
		},
		{
			name: "winarg/lateral",
			sql:  "SELECT o.id AS a, p.id AS b, SUM(o.id) OVER () AS n FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,2,6 | 2,3,6 | 2,4,6",
			// ARC LT round 2: the DAG routes a window above a LATERAL join to
			// the single-process pipeline (dagplan.refuseWindowOverDependentJoin),
			// so the DAG-only pin that stood here agrees now and is deleted.
		},
		{
			name: "sortkey/lateral",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY p.id DESC, a",
			want: "cols=[a:INT64 b:INT64] rows=4 | 2,4 | 2,3 | 1,2 | 1,1",
			// ARC JP round 2: the DAG resolves a LATERAL body's unaliased item
			// to its source column (ADR-0026 §8l), so the DAG-only pin that
			// stood here agrees now and is deleted.
		},
		{
			name: "armref/lateral",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,2 | 2,3 | 2,4",
			// ARC JP round 2: the DAG resolves a LATERAL body's unaliased item
			// to its source column (ADR-0026 §8l), so the DAG-only pin that
			// stood here agrees now and is deleted.
		},
		{
			name: "star/lateral",
			sql:  "SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY 1, 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,50 | 1,Alice,150,2,100 | 2,Bob,200,3,75 | 2,Bob,200,4,125",
			pin: map[string]string{
				"single":       "ERR ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
				"spilled512k":  "ERR ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
				"dag":          "ERR ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
				"dag-shuffled": "ERR ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
				"dag-morsel4":  "ERR ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
			},
			why: "REFUSED on all five arms, pre-existing: a star over a LATERAL arm is not expanded \u2014 the arm's list carries the correlation slot the join drops (ADR-0026 \u00a73c) \u2014 so `ORDER BY 1` has no position to count. The refusal states that; PostgreSQL answers the query. ADR-0026 \u00a79's decline list. Identical at aed447e3.",
		},
		{
			name: "winpart/setop",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY p.id) AS n FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item UNION ALL SELECT id, order_id, amount FROM lat_item WHERE 1=0) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
		},
		{
			name: "winorder/setop",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (ORDER BY p.id) AS n FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item UNION ALL SELECT id, order_id, amount FROM lat_item WHERE 1=0) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,2 | 2,3,3 | 2,4,4",
		},
		{
			name: "winarg/setop",
			sql:  "SELECT o.id AS a, p.id AS b, SUM(o.id) OVER () AS n FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item UNION ALL SELECT id, order_id, amount FROM lat_item WHERE 1=0) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,2,6 | 2,3,6 | 2,4,6",
		},
		{
			name: "sortkey/setop",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item UNION ALL SELECT id, order_id, amount FROM lat_item WHERE 1=0) p ON p.order_id = o.id ORDER BY p.id DESC, a",
			want: "cols=[a:INT64 b:INT64] rows=4 | 2,4 | 2,3 | 1,2 | 1,1",
		},
		{
			name: "armref/setop",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item UNION ALL SELECT id, order_id, amount FROM lat_item WHERE 1=0) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,2 | 2,3 | 2,4",
		},
		{
			name: "star/setop",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item UNION ALL SELECT id, order_id, amount FROM lat_item WHERE 1=0) p ON p.order_id = o.id ORDER BY 1, 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,1,50 | 1,Alice,150,2,1,100 | 2,Bob,200,3,2,75 | 2,Bob,200,4,2,125",
		},
		{
			name: "winpart/grouped",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY p.id) AS n FROM lat_ord o JOIN (SELECT MIN(id) AS id, order_id, SUM(amount) AS amount FROM lat_item GROUP BY order_id) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=2 | 1,1,1 | 2,3,1",
		},
		{
			name: "winorder/grouped",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (ORDER BY p.id) AS n FROM lat_ord o JOIN (SELECT MIN(id) AS id, order_id, SUM(amount) AS amount FROM lat_item GROUP BY order_id) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=2 | 1,1,1 | 2,3,2",
		},
		{
			name: "winarg/grouped",
			sql:  "SELECT o.id AS a, p.id AS b, SUM(o.id) OVER () AS n FROM lat_ord o JOIN (SELECT MIN(id) AS id, order_id, SUM(amount) AS amount FROM lat_item GROUP BY order_id) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=2 | 1,1,3 | 2,3,3",
		},
		{
			name: "sortkey/grouped",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN (SELECT MIN(id) AS id, order_id, SUM(amount) AS amount FROM lat_item GROUP BY order_id) p ON p.order_id = o.id ORDER BY p.id DESC, a",
			want: "cols=[a:INT64 b:INT64] rows=2 | 2,3 | 1,1",
		},
		{
			name: "armref/grouped",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN (SELECT MIN(id) AS id, order_id, SUM(amount) AS amount FROM lat_item GROUP BY order_id) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64] rows=2 | 1,1 | 2,3",
		},
		{
			name: "star/grouped",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT MIN(id) AS id, order_id, SUM(amount) AS amount FROM lat_item GROUP BY order_id) p ON p.order_id = o.id ORDER BY 1, 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 amount:FLOAT64] rows=2 | 1,Alice,150,1,1,150 | 2,Bob,200,3,2,200",
		},
		{
			name: "winpart/nested",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY p.id) AS n FROM lat_ord o JOIN (SELECT n.id, n.order_id, n.amount FROM (SELECT id, order_id, amount FROM lat_item) n) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,1 | 2,3,1 | 2,4,1",
		},
		{
			name: "winorder/nested",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (ORDER BY p.id) AS n FROM lat_ord o JOIN (SELECT n.id, n.order_id, n.amount FROM (SELECT id, order_id, amount FROM lat_item) n) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,2 | 2,3,3 | 2,4,4",
		},
		{
			name: "winarg/nested",
			sql:  "SELECT o.id AS a, p.id AS b, SUM(o.id) OVER () AS n FROM lat_ord o JOIN (SELECT n.id, n.order_id, n.amount FROM (SELECT id, order_id, amount FROM lat_item) n) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,2,6 | 2,3,6 | 2,4,6",
		},
		{
			name: "sortkey/nested",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN (SELECT n.id, n.order_id, n.amount FROM (SELECT id, order_id, amount FROM lat_item) n) p ON p.order_id = o.id ORDER BY p.id DESC, a",
			want: "cols=[a:INT64 b:INT64] rows=4 | 2,4 | 2,3 | 1,2 | 1,1",
		},
		{
			name: "armref/nested",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN (SELECT n.id, n.order_id, n.amount FROM (SELECT id, order_id, amount FROM lat_item) n) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,2 | 2,3 | 2,4",
		},
		{
			name: "star/nested",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT n.id, n.order_id, n.amount FROM (SELECT id, order_id, amount FROM lat_item) n) p ON p.order_id = o.id ORDER BY 1, 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,1,50 | 1,Alice,150,2,1,100 | 2,Bob,200,3,2,75 | 2,Bob,200,4,2,125",
		},
		{
			// THE LEAF CELL the memo names (§(a) M2): an EXPRESSION key
			// whose two LEAVES name two different occurrences. The mint gives
			// the RESULT a name nothing else owns; it says nothing about the
			// leaves, and each leaf is bound by the ordinary reference rules.
			// x.w is `amount` and y.w is `200 - amount`, so the correct sum is
			// 200 on every row — ONE partition of four — and a leaf bound to
			// the other occurrence gives four singletons.
			name: "winpart/exprTwoOccurrences",
			sql:  "SELECT x.id AS a, COUNT(*) OVER (PARTITION BY x.w + y.w) AS n FROM (SELECT id, amount AS w FROM lat_item) x JOIN (SELECT id, 200 - amount AS w FROM lat_item) y ON y.id = x.id ORDER BY a",
			want: "cols=[a:INT64 n:INT64] rows=4 | 1,4 | 2,4 | 3,4 | 4,4",
			pin: map[string]string{
				"dag":          "cols=[a:INT64 n:INT64] rows=4 | 1,1 | 2,1 | 3,1 | 4,1",
				"dag-shuffled": "cols=[a:INT64 n:INT64] rows=4 | 1,1 | 2,1 | 3,1 | 4,1",
				"dag-morsel4":  "cols=[a:INT64 n:INT64] rows=4 | 1,1 | 2,1 | 3,1 | 4,1",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: both leaves of the key expression bind ONE occurrence's `w` on the three DAG arms, so the computed slot holds the wrong value under a correctly-minted name. Corollary 1 reaches a key that IS a reference; a key that CONTAINS one is the same question one layer down, and the arm whose spelling the stage's stream carries decides it. Right on single and spilled512k. Identical at aed447e3.",
		},
		{
			// The same leaves through the window's ARGUMENT: the sum is 800
			// and a leaf bound to y on both sides gives 900.
			name: "winarg/exprTwoOccurrences",
			sql:  "SELECT SUM(x.w + y.w) OVER () AS s FROM (SELECT id, amount AS w FROM lat_item) x JOIN (SELECT id, 200 - amount AS w FROM lat_item) y ON y.id = x.id ORDER BY s",
			want: "cols=[s:FLOAT64] rows=4 | 800 | 800 | 800 | 800",
			pin: map[string]string{
				"dag":          "cols=[s:FLOAT64] rows=4 | 900 | 900 | 900 | 900",
				"dag-shuffled": "cols=[s:FLOAT64] rows=4 | 900 | 900 | 900 | 900",
				"dag-morsel4":  "cols=[s:FLOAT64] rows=4 | 900 | 900 | 900 | 900",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: 900 is y.w + y.w summed, so BOTH leaves bound y's occurrence. The same fact as winpart/exprTwoOccurrences through the ARGUMENT. Right on single and spilled512k. Identical at aed447e3.",
		},
		{
			// And the same leaves in a plain SELECT item, which is the
			// narrowest form: no window at all, so the mint is not in the
			// picture and only the leaf binding is.
			name: "armref/exprTwoOccurrences",
			sql:  "SELECT x.id AS a, x.w + y.w AS k FROM (SELECT id, amount AS w FROM lat_item) x JOIN (SELECT id, 200 - amount AS w FROM lat_item) y ON y.id = x.id ORDER BY a",
			want: "cols=[a:INT64 k:FLOAT64] rows=4 | 1,200 | 2,200 | 3,200 | 4,200",
			pin: map[string]string{
				"dag":          "cols=[a:INT64 k:FLOAT64] rows=4 | 1,100 | 2,200 | 3,150 | 4,250",
				"dag-shuffled": "cols=[a:INT64 k:FLOAT64] rows=4 | 1,100 | 2,200 | 3,150 | 4,250",
				"dag-morsel4":  "cols=[a:INT64 k:FLOAT64] rows=4 | 1,100 | 2,200 | 3,150 | 4,250",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: 2×amount, so both leaves bound x's occurrence here. No window is involved, which localises the leaf binding to the join-arm REFERENCE consumer rather than to the window. Right on single and spilled512k. Identical at aed447e3.",
		},
		// THE MIRROR SPELLING. Every cell above keys on `p.id`, the arm the
		// plan publishes BARE, and the whole table therefore answers
		// identically at aed447e3 — measured. These six key the same window
		// on the OUTER occurrence, which is #1028’s own spelling, so the
		// table moves for the defect it enumerates.
		{
			name: "winpartOuter/base",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
		},
		{
			// The window's OTHER TWO positions on the same mirror, so the
			// discriminating dimension is not one cell wide. Both fail at
			// `aed447e3` on all five arms.
			name: "winorderOuter/base",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (ORDER BY o.id) AS n FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,2,2 | 2,3,4 | 2,4,4",
		},
		{
			name: "winargOuter/base",
			sql:  "SELECT o.id AS a, p.id AS b, SUM(o.total) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN lat_item p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:FLOAT64] rows=4 | 1,1,300 | 1,2,300 | 2,3,400 | 2,4,400",
		},
		{
			name: "winpartOuter/derived",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
		},
		{
			name: "winpartOuter/lateral",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
			// ARC LT round 2: the DAG routes a window above a LATERAL join to
			// the single-process pipeline (dagplan.refuseWindowOverDependentJoin),
			// so the DAG-only pin that stood here agrees now and is deleted.
		},
		{
			name: "winpartOuter/setop",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN (SELECT id, order_id, amount FROM lat_item UNION ALL SELECT id, order_id, amount FROM lat_item WHERE 1=0) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
		},
		{
			name: "winpartOuter/grouped",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN (SELECT MIN(id) AS id, order_id, SUM(amount) AS amount FROM lat_item GROUP BY order_id) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=2 | 1,1,1 | 2,3,1",
		},
		{
			name: "winpartOuter/nested",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (PARTITION BY o.id) AS n FROM lat_ord o JOIN (SELECT n.id, n.order_id, n.amount FROM (SELECT id, order_id, amount FROM lat_item) n) p ON p.order_id = o.id ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,2,2 | 2,3,2 | 2,4,2",
		},
		{
			name: "lifted/lateral",
			sql:  "SELECT o.id AS a, p.m AS b FROM lat_ord o LEFT JOIN LATERAL (SELECT i.id AS m FROM lat_item i WHERE i.amount < o.total) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64] rows=9 | 1,1 | 1,2 | 1,3 | 1,4 | 2,1 | 2,2 | 2,3 | 2,4 | 3,NULL",
		},
		{
			name: "lifted/lateralRenamed",
			sql:  "SELECT o.id AS a, p.m AS b FROM lat_ord o LEFT JOIN LATERAL (SELECT i.product AS m FROM lat_item i WHERE i.amount < o.total) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:STRING] rows=9 | 1,Doohickey | 1,Gadget | 1,Widget | 1,Widget | 2,Doohickey | 2,Gadget | 2,Widget | 2,Widget | 3,NULL",
		},
		{
			name: "lifted/lateralContested",
			sql:  "SELECT o.id AS a, p.m AS b FROM lat_ord o LEFT JOIN LATERAL (SELECT i.product AS m FROM lat_item i WHERE i.id < o.id) p ON true ORDER BY a, b",
			// Refused on every arm from arc LT (#1130) until arc JP round 3:
			// the lifted predicate is spelled through the lateral's alias
			// (`p.id < o.id`), the single-process pipeline tells the two `id`
			// columns apart, and the stage DAG routes the plan there
			// (dagplan.ErrLateralIdentityDistributed). PostgreSQL 17.11's rows.
			want: "cols=[a:INT64 b:STRING] rows=4 | 1,NULL | 2,Widget | 3,Gadget | 3,Widget",
		}}
}
