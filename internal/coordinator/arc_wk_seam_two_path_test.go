// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// THE WINDOW-KEY / NAME-OWNERSHIP SEAM, ENUMERATED ONCE — arc WK.
//
// ADR-0026 §8j recorded this seam as NOT SETTLED after three bounded repairs
// were written and measured back out. The rule that settles it is in
// docs/design/window-key-ownership.md: **a window key, a sort key, a lifted
// predicate column and a join-arm reference bind by IDENTITY — the OCCURRENCE
// that produced the column, and the column within that occurrence — carried
// from binding through every rewrite; a NAME is derived from the identity for
// publication, never the reverse.**
//
// This table is the seam enumerated ONCE, by measurement, rather than one
// position per round. It crosses every CONSUMER of a column reference with
// every PRODUCER an arm can be, on five arms, against live PostgreSQL 17.11:
//
//	{window PARTITION BY, window ORDER BY, window ARGUMENT, sort key,
//	 join-arm reference, star}
//	  × {base scan, derived block, LATERAL, set operation, grouped block,
//	     nested block}
//	  + {lifted predicate column} × {LATERAL} × three spellings
//	  × {single, spilled512k, dag, dag-shuffled, dag-morsel4}
//
// 50 cells × 5 arms = 250 (cell, arm) results. Every producer publishes `id`,
// which `lat_ord o` also publishes, so the ownership question is LIVE in every
// cell: a consumer that erases the occurrence binds `o.id` and the cell says
// so. A corpus whose producer publishes a name nothing else spells answers
// correctly by luck — that is what arc R2 measured about its own
// name-collision dimension, and it is why this one is built the other way.
//
// The PostgreSQL answers were taken from a postgres:17-alpine container
// standing alone (`--locale=C`, text columns `COLLATE "C"`), loaded with this
// package's own lat_ord / lat_item rows; the command and every answer are in
// the arc's `pg_corpus.log`.
//
// WHAT MOVED. Before this arc, `PARTITION BY o.id`, `ORDER BY o.id` and
// `SUM(o.total) OVER (PARTITION BY o.id)` over two BASE-SCAN arms bound
// whichever arm the reorderer put bare — seven pinned cells of
// TestArcL1AWindowKeyBindsItsOwnJoinArm and one of
// TestArcL1QualifyAnswersDuckDBOnEveryArm, on all five arms, in silence. Those
// pins are DELETED; see that gate's header for the mechanism.
//
// WHAT REMAINS. One COLUMN of this table, the LATERAL producer, and one
// KEY SHAPE, an expression whose two leaves name two occurrences — the cell
// docs/design/window-key-ownership.md §(a) M2 names, added here with its
// measured mechanism. Both are DAG-only and `distributed`.
//
// The LATERAL producer first.
// Five consumers × three DAG arms bind the outer occurrence, because a
// decorrelated body's Project emits no stage and the DAG's join publishes the
// body's inner-scan spelling where the single-process join publishes the arm's
// own alias. Right on the engine's own arm, wrong only on the distributed
// ones: pinned per arm with that mechanism, labelled `distributed`, and NOT
// chased here (engine-first, Derek 2026-09-16). The star over a LATERAL is a
// refusal on all five arms and the contested lifted predicate is #1130 on the
// two single arms — each pinned with its own sentence.
//
// EXCLUDED DIMENSIONS, and why (the reviewer starts here):
//
//   - A lifted predicate column over a NON-LATERAL producer: there is no such
//     consumer. A lifted predicate exists only where the decorrelation lifts a
//     correlated body's non-equality out of it, so its producer is always a
//     LATERAL body. Three lateral spellings stand in its row instead.
//   - The window FRAME (`ROWS`/`RANGE`/`GROUPS` bounds): a frame names no
//     relation, so it has no occurrence to own. ADR-0026 §4b's territory.
//   - The WIRE declaration of a window output over a published slot: #1135,
//     `distributed`, a fact about a DECLARATION rather than a value.
//     `pgwire.TestWKTheWireDeclaresTheSeamsOwnColumns` carries this table's
//     wire half instead.
//   - The 22 data TYPES: the ownership question is about which column a name
//     IS, and every cell here would ask it identically under any type. The
//     type matrix is `wadjet.TestTypeMatrix*`'s and ADR-0024's.
//   - A SPILLED window's own re-read: `exec/window_external.go` resolves its
//     keys through the same `columnIndexFallback` as the in-memory path, so
//     `spilled512k` replicates `single`'s binding rather than being a sixth
//     mechanism — which every row of this table shows by answering identically
//     on the two.
//   - CROSS and OUTER join shapes over the same producers: the arm's
//     publication convention is the same and ADR-0026 §8i's own table already
//     crosses {LEFT, RIGHT, FULL} with the five producer classes.
//
// A pin that starts agreeing FAILS. Deleting it is the fix's proof.
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
			pin: map[string]string{
				"dag":          "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,2 | 2,2,2",
				"dag-shuffled": "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,2 | 2,2,2",
				"dag-morsel4":  "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,2 | 2,2,2",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: a decorrelated LATERAL body's Project emits no stage, so the DAG's join publishes the body's INNER SCAN spelling (`i.id`) where the single-process join publishes the arm's own alias (`l.id`). The reference `p.id` therefore misses exactly and `exec.ColumnIndexFallback`'s qualifier strip binds the OUTER `id` \u2014 corollary 2's precondition failing, not its lookup working (docs/design/window-key-ownership.md). Closing it is the rule's DAG half: translate `p.id` to the body's carrier inside the occurrence the qualifier names before the consumer binds it. Identical at aed447e3.",
		},
		{
			name: "winorder/lateral",
			sql:  "SELECT o.id AS a, p.id AS b, COUNT(*) OVER (ORDER BY p.id) AS n FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,1 | 1,2,2 | 2,3,3 | 2,4,4",
			pin: map[string]string{
				"dag":          "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,4 | 2,2,4",
				"dag-shuffled": "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,4 | 2,2,4",
				"dag-morsel4":  "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,4 | 2,2,4",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: a decorrelated LATERAL body's Project emits no stage, so the DAG's join publishes the body's INNER SCAN spelling (`i.id`) where the single-process join publishes the arm's own alias (`l.id`). The reference `p.id` therefore misses exactly and `exec.ColumnIndexFallback`'s qualifier strip binds the OUTER `id` \u2014 corollary 2's precondition failing, not its lookup working (docs/design/window-key-ownership.md). Closing it is the rule's DAG half: translate `p.id` to the body's carrier inside the occurrence the qualifier names before the consumer binds it. Identical at aed447e3.",
		},
		{
			name: "winarg/lateral",
			sql:  "SELECT o.id AS a, p.id AS b, SUM(o.id) OVER () AS n FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,2,6 | 2,3,6 | 2,4,6",
			pin: map[string]string{
				"dag":          "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,1,6 | 2,2,6 | 2,2,6",
				"dag-shuffled": "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,1,6 | 2,2,6 | 2,2,6",
				"dag-morsel4":  "cols=[a:INT64 b:INT64 n:DECIMAL(38,0)] rows=4 | 1,1,6 | 1,1,6 | 2,2,6 | 2,2,6",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: a decorrelated LATERAL body's Project emits no stage, so the DAG's join publishes the body's INNER SCAN spelling (`i.id`) where the single-process join publishes the arm's own alias (`l.id`). The reference `p.id` therefore misses exactly and `exec.ColumnIndexFallback`'s qualifier strip binds the OUTER `id` \u2014 corollary 2's precondition failing, not its lookup working (docs/design/window-key-ownership.md). Closing it is the rule's DAG half: translate `p.id` to the body's carrier inside the occurrence the qualifier names before the consumer binds it. Identical at aed447e3.",
		},
		{
			name: "sortkey/lateral",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY p.id DESC, a",
			want: "cols=[a:INT64 b:INT64] rows=4 | 2,4 | 2,3 | 1,2 | 1,1",
			pin: map[string]string{
				"dag":          "cols=[a:INT64 b:INT64] rows=4 | 2,2 | 2,2 | 1,1 | 1,1",
				"dag-shuffled": "cols=[a:INT64 b:INT64] rows=4 | 2,2 | 2,2 | 1,1 | 1,1",
				"dag-morsel4":  "cols=[a:INT64 b:INT64] rows=4 | 2,2 | 2,2 | 1,1 | 1,1",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: a decorrelated LATERAL body's Project emits no stage, so the DAG's join publishes the body's INNER SCAN spelling (`i.id`) where the single-process join publishes the arm's own alias (`l.id`). The reference `p.id` therefore misses exactly and `exec.ColumnIndexFallback`'s qualifier strip binds the OUTER `id` \u2014 corollary 2's precondition failing, not its lookup working (docs/design/window-key-ownership.md). Closing it is the rule's DAG half: translate `p.id` to the body's carrier inside the occurrence the qualifier names before the consumer binds it. Identical at aed447e3.",
		},
		{
			name: "armref/lateral",
			sql:  "SELECT o.id AS a, p.id AS b FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY a, b",
			want: "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,2 | 2,3 | 2,4",
			pin: map[string]string{
				"dag":          "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,1 | 2,2 | 2,2",
				"dag-shuffled": "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,1 | 2,2 | 2,2",
				"dag-morsel4":  "cols=[a:INT64 b:INT64] rows=4 | 1,1 | 1,1 | 2,2 | 2,2",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: a decorrelated LATERAL body's Project emits no stage, so the DAG's join publishes the body's INNER SCAN spelling (`i.id`) where the single-process join publishes the arm's own alias (`l.id`). The reference `p.id` therefore misses exactly and `exec.ColumnIndexFallback`'s qualifier strip binds the OUTER `id` \u2014 corollary 2's precondition failing, not its lookup working (docs/design/window-key-ownership.md). Closing it is the rule's DAG half: translate `p.id` to the body's carrier inside the occurrence the qualifier names before the consumer binds it. Identical at aed447e3.",
		},
		{
			name: "star/lateral",
			sql:  "SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) p ON true ORDER BY 1, 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,50 | 1,Alice,150,2,100 | 2,Bob,200,3,75 | 2,Bob,200,4,125",
			pin: map[string]string{
				"single":       "ERR building physical plan: ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
				"spilled512k":  "ERR building physical plan: ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
				"dag":          "ERR physical plan: ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
				"dag-shuffled": "ERR physical plan: ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
				"dag-morsel4":  "ERR physical plan: ORDER BY position 1: this `SELECT *` was not expanded into a column list, so there is no position to count. A star over a JOIN is expanded \u2014 every FROM arm's own list, in the clause's written order \u2014 and one is left alone only where an arm's list is not knowable here: a LATERAL, a table function, a relation whose own list a block does not state, or an arm publishing one name twice. Name the columns, or ORDER BY the column itself",
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
			pin: map[string]string{
				"dag":          "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,2 | 2,2,2",
				"dag-shuffled": "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,2 | 2,2,2",
				"dag-morsel4":  "cols=[a:INT64 b:INT64 n:INT64] rows=4 | 1,1,2 | 1,1,2 | 2,2,2 | 2,2,2",
			},
			why: "DAG-ONLY, pre-existing, `distributed`: the LATERAL column of this table, same mechanism as the `p.id` spelling beside it. Identical at aed447e3.",
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
			want: "cols=[a:INT64 b:STRING] rows=4 | 1,NULL | 2,Widget | 3,Gadget | 3,Widget",
			pin: map[string]string{
				"single":      "cols=[a:INT64 b:STRING] rows=3 | 1,NULL | 2,NULL | 3,NULL",
				"spilled512k": "cols=[a:INT64 b:STRING] rows=3 | 1,NULL | 2,NULL | 3,NULL",
			},
			why: "#1130, single-process only: the lifted non-equality predicate names an inner column the OUTER relation also carries, so the materialization DECLINES (ADR-0021 \u00a71q round 4) and the two single arms answer NULL pads where the three DAG arms \u2014 which read the column off a stream carrying the scan's own names \u2014 answer PostgreSQL's rows. Not a key-binding question: arc L1 measured both available routes out. Identical at aed447e3.",
		}}
}
