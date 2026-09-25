// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A STAR PUBLISHES ITS ARMS' OWN COLUMNS — over a JOIN, over a `USING` merge
// and over a BLOCK — on FIVE arms, against PostgreSQL 17.11 measured live over
// this fixture.
//
// Arc O1 settled the ORDER a bare star publishes (§9: the FROM clause's arms,
// left arm first) and arc O2 what a derived BLOCK publishes (§9: its visible
// list, under `StarColumn{Resolve, Publish}`). This table is the same rule's
// remaining faces, enumerated once:
//
//	{`*`, `t.*`, `*` beside an item, `t.*, u.*`} ×
//	{base×base, derived×derived, three-way, four-way, self-join, set-op arm,
//	 CTE arm, LATERAL arm, USING join} ×
//	{arms share 0 / 1 / 2 column names; unaliased expression items} ×
//	{value, declared name, declared (p,s), row order, zero-row declaration} ×
//	{single, spilled512k, dag, dag-shuffled, dag-morsel4}
//
// zzp/zzj is the DISCRIMINATING pair: its two arms share BOTH column names and
// declare `d92` at two different scales, so a reference bound to the wrong arm
// is a wrong VALUE and a wrong DECLARATION at once. psa/psb share exactly the
// join key and nothing else — the control that says a cell can fail.

// What this arc changed, and the cell that fails without it:
//
//   - #1177 / the USING merge's shared-tail-name decline. `usingJoinStarColumns`
//     refused (0A000) whenever the arms shared a name outside the USING list,
//     on the claim that a qualified reference "binds one of them wherever the
//     plan put it". Measured at 563aa517 the claim is FALSE: the `on/*` cells,
//     the same pair spelled with `ON`, answer PostgreSQL's values and both
//     DECIMAL declarations on every arm. Every `using/*` cell over zzp/zzj was
//     a refusal at base.
//   - #1079 / the set operation's published names. Its result columns are its
//     LEFTMOST arm's (ADR-0026 §8b) and the PUBLISHED half was applied by
//     nobody, because `findOutputProjectionNode` answers nil for a set-op root
//     — so the operation went out under the arm's RESOLUTION spelling on all
//     five arms and in RowDescription. The `naming/*-set-op-*` cells failed at
//     base; the derived-table and CTE spellings were already right, which is
//     how it survived.
//   - #1094 / a reference into a block publishing one name twice. The block is
//     a legal relation and a star over it answers both columns BY POSITION, but
//     a REFERENCE names neither and PostgreSQL raises 42702.
//     `dupname/a-reference-into-the-block` ANSWERED the first column at base.

// #1164, #1001, #1078, #1097 and #1124 are CLOSED BY MEASUREMENT: their cells
// agree at 563aa517 (arcs O1, O2 and R2 closed them without naming them) and
// are carried here so the agreement is gated rather than assumed.
//
// `pgwire.TestSRTheWireDeclaresAStarsOwnArms` is this rule on the wire and
// `server.TestArcSRAStarOverAPolicedArmNeverPublishesTheOtherArmsValue` is its
// masking half on all nine doors.
func TestSRAStarPublishesItsArmsOwnColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	c1Run(t, arms, srStarCases())
}

// srSetOpChain is a LEFT-DEEP chain of n `UNION ALL` arms over one row, whose
// second item is UNALIASED — so the operation's published name is the question
// and the depth is the variable.
//
// It exists because every `naming/*` cell above is TWO arms deep, and the walk
// that answers "whose names does the client read" used to carry a hop bound:
// one hop per arm, so from the ninth arm on it answered nil and the operation
// published the arm's RESOLUTION spelling again — the divergence #1079 closes,
// reappearing on ordinary SQL with nothing recorded that said so (round-1
// review, B4). The filter keeps one row per arm so the whole answer is legible.
func srSetOpChain(n int) string {
	arms := make([]string, 0, n)
	for i := 0; i < n; i++ {
		arms = append(arms, fmt.Sprintf("SELECT id, total + %d FROM lat_ord WHERE id = 1", i+1))
	}
	return strings.Join(arms, " UNION ALL ")
}

// srChainRows is the rendering all five arms and PostgreSQL 17.11 give for
// `srSetOpChain(n)`: `?column?` for the unaliased item, one row per arm.
func srChainRows(n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "cols=[id:INT64 ?column?:FLOAT64] rows=%d", n)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, " | 1,%d", 151+i)
	}
	return b.String()
}

// srUnreachable is the disposition a cell takes when the stage planner cannot
// state the query's output and the coordinator answers it in-process.
var srUnreachable = map[string]string{
	"dag":          "UnreachableOutput +1",
	"dag-shuffled": "UnreachableOutput +1",
	"dag-morsel4":  "UnreachableOutput +1",
}

func srStarCases() []c1Case {
	return []c1Case{
		{
			name: "on/base-arms-share-two-names",
			sql:  "SELECT * FROM zzp a JOIN zzj b ON a.id = b.id ORDER BY a.id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1,1.1111 | 2,0.00,2,12345678.1234 | 3,12.75,3,3.3333",
		},
		{
			name: "on/base-arms-share-two-names-fromrev",
			sql:  "SELECT * FROM zzj b JOIN zzp a ON a.id = b.id ORDER BY b.id",
			want: "cols=[id:INT64 d92:DECIMAL(18,4) id:INT64 d92:DECIMAL(9,2)] rows=3 | 1,1.1111,1,-3.50 | 2,12345678.1234,2,0.00 | 3,3.3333,3,12.75",
		},
		{
			name: "on/base-arms-share-two-names-filtered",
			sql:  "SELECT * FROM zzp a JOIN zzj b ON a.id = b.id WHERE a.d92 <> 99 ORDER BY a.id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1,1.1111 | 2,0.00,2,12345678.1234 | 3,12.75,3,3.3333",
		},
		{
			name: "on/left-arms-share-two-names",
			sql:  "SELECT * FROM zzp a LEFT JOIN zzj b ON a.id = b.id ORDER BY a.id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1,1.1111 | 2,0.00,2,12345678.1234 | 3,12.75,3,3.3333",
		},
		{
			name: "on/three-way-arms-share-two-names",
			sql:  "SELECT * FROM zzp a JOIN zzj b ON a.id = b.id JOIN zzp c ON c.id = a.id ORDER BY a.id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4) id:INT64 d92:DECIMAL(9,2)] rows=3 | 1,-3.50,1,1.1111,1,-3.50 | 2,0.00,2,12345678.1234,2,0.00 | 3,12.75,3,3.3333,3,12.75",
		},
		{
			name: "on/two-qualified-stars",
			sql:  "SELECT a.*, b.* FROM zzp a JOIN zzj b ON a.id = b.id ORDER BY a.id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1,1.1111 | 2,0.00,2,12345678.1234 | 3,12.75,3,3.3333",
		},
		{
			name: "on/star-beside-a-reference",
			sql:  "SELECT *, a.d92 FROM zzp a JOIN zzj b ON a.id = b.id ORDER BY a.id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4) d92:DECIMAL(9,2)] rows=3 | 1,-3.50,1,1.1111,-3.50 | 2,0.00,2,12345678.1234,0.00 | 3,12.75,3,3.3333,12.75",
		},
		// The star's own columns and the item's VALUE agree; the item's
		// DECLARATION does not. PostgreSQL declares `d92 + 0` unconstrained
		// numeric and this engine declares DECIMAL(10,2) — ADR-0024's result
		// type for an exact-numeric addition, on ADR-0012's divergence list,
		// and not a star question. The cell is here because it is the shape
		// #1164 names, and the star half of it is what is asserted.
		// PG: cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4) ?column?:DECIMAL] rows=3 1|-3.5|1|1.1111|-3.5 · 2|0|2|12345678.1234|0 · 3|12.75|3|3.3333|12.75
		{
			name: "on/star-beside-an-expression",
			sql:  "SELECT *, a.d92 + 0 FROM zzp a JOIN zzj b ON a.id = b.id ORDER BY a.id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4) ?column?:DECIMAL(10,2)] rows=3 | 1,-3.50,1,1.1111,-3.50 | 2,0.00,2,12345678.1234,0.00 | 3,12.75,3,3.3333,12.75",
			// ROUTED, not executed on the DAG: a star beside a COMPUTED item
			// over a join is an output the stage planner cannot state, so all
			// three distributed arms answer in-process. Recorded rather than
			// wished away — rows alone cannot tell an executed query from one
			// the coordinator answered locally.
			routed: srUnreachable,
		},
		{
			name: "on/star-beside-an-expression-1164",
			sql:  "SELECT *, o.id + 0 FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 ?column?:INT64] rows=4 | 1,Alice,150,1,1,Widget,50,1 | 1,Alice,150,2,1,Gadget,100,1 | 2,Bob,200,3,2,Widget,75,2 | 2,Bob,200,4,2,Doohickey,125,2",
			// ROUTED, not executed on the DAG: a star beside a COMPUTED item
			// over a join is an output the stage planner cannot state, so all
			// three distributed arms answer in-process. Recorded rather than
			// wished away — rows alone cannot tell an executed query from one
			// the coordinator answered locally.
			routed: srUnreachable,
		},
		{
			name: "using/inner",
			sql:  "SELECT * FROM zzp a JOIN zzj b USING (id) ORDER BY id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1.1111 | 2,0.00,12345678.1234 | 3,12.75,3.3333",
		},
		{
			name: "using/left",
			sql:  "SELECT * FROM zzp a LEFT JOIN zzj b USING (id) ORDER BY id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1.1111 | 2,0.00,12345678.1234 | 3,12.75,3.3333",
		},
		{
			name: "using/right",
			sql:  "SELECT * FROM zzp a RIGHT JOIN zzj b USING (id) ORDER BY id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1.1111 | 2,0.00,12345678.1234 | 3,12.75,3.3333",
		},
		{
			name: "using/inner-fromrev",
			sql:  "SELECT * FROM zzj b JOIN zzp a USING (id) ORDER BY id",
			want: "cols=[id:INT64 d92:DECIMAL(18,4) d92:DECIMAL(9,2)] rows=3 | 1,1.1111,-3.50 | 2,12345678.1234,0.00 | 3,3.3333,12.75",
		},
		{
			name: "using/derived-arms",
			sql:  "SELECT * FROM (SELECT id, d92 FROM zzp) a JOIN (SELECT id, d92 FROM zzj) b USING (id) ORDER BY id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1.1111 | 2,0.00,12345678.1234 | 3,12.75,3.3333",
		},
		{
			name: "using/star-beside-a-reference",
			sql:  "SELECT *, a.d92 FROM zzp a JOIN zzj b USING (id) ORDER BY id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4) d92:DECIMAL(9,2)] rows=3 | 1,-3.50,1.1111,-3.50 | 2,0.00,12345678.1234,0.00 | 3,12.75,3.3333,12.75",
		},
		{
			name: "using/qualified-star-left",
			sql:  "SELECT a.* FROM zzp a JOIN zzj b USING (id) ORDER BY a.id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2)] rows=3 | 1,-3.50 | 2,0.00 | 3,12.75",
		},
		{
			name: "using/qualified-star-right",
			sql:  "SELECT b.* FROM zzp a JOIN zzj b USING (id) ORDER BY b.id",
			want: "cols=[id:INT64 d92:DECIMAL(18,4)] rows=3 | 1,1.1111 | 2,12345678.1234 | 3,3.3333",
		},
		{
			name: "using/filtered",
			sql:  "SELECT * FROM zzp a JOIN zzj b USING (id) WHERE a.d92 <> 99 ORDER BY id",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1.1111 | 2,0.00,12345678.1234 | 3,12.75,3.3333",
		},
		{
			name: "using/ordinal-2",
			sql:  "SELECT * FROM zzp a JOIN zzj b USING (id) ORDER BY 2",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1.1111 | 2,0.00,12345678.1234 | 3,12.75,3.3333",
		},
		{
			name: "using/ordinal-3-desc",
			sql:  "SELECT * FROM zzp a JOIN zzj b USING (id) ORDER BY 3 DESC",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4)] rows=3 | 2,0.00,12345678.1234 | 3,12.75,3.3333 | 1,-3.50,1.1111",
		},
		{
			name: "using/zero-row",
			sql:  "SELECT * FROM zzp a JOIN zzj b USING (id) WHERE a.id < 0",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) d92:DECIMAL(18,4)] rows=0",
		},
		{
			name: "using/no-shared-tail-name",
			sql:  "SELECT * FROM psa a JOIN psb b USING (id) ORDER BY id",
			want: "cols=[id:INT64 a:INT64 b:INT64] rows=1 | 2,20,200",
		},
		// REFUSED on all five arms where PostgreSQL answers, and it is the
		// ONE bound this arc's USING work did not move. A FULL join's merged
		// key is `COALESCE(l.c, r.c)` — the one star item that is not a plain
		// reference (ADR-0026 §9) — and the term is MINTED by the projection
		// the star expands into, which sits ABOVE the Sort. Ordering by it
		// needs the expression materialized BELOW the sort, and this builder
		// materializes a computed sort key only beside a NAMED select list.
		// Loud, with the construct named, and the boundary is exactly there:
		// the same statement with the key named, or with `ON`, answers.
		// PG: cols=[id:INT64 a:INT64 b:INT64] rows=3 1|10|NULL · 2|20|200 · 3|NULL|300
		{
			name: "using/full-ordered-by-the-merged-key",
			sql:  "SELECT * FROM psa a FULL JOIN psb b USING (id) ORDER BY id",
			want: "ERR ORDER BY coalesce(a.id, b.id): a bare `SELECT *` over a FULL JOIN ... USING cannot be ORDERED BY the merged column: its value is COALESCE of the two sides, computed by the projection the star expands into, and this builder materializes a computed sort key beside a named SELECT list, which a star-only list is not. Name the columns, or write the join condition with ON",
			pin: map[string]string{
				"dag":          "ERR ORDER BY coalesce(a.id, b.id): a bare `SELECT *` over a FULL JOIN ... USING cannot be ORDERED BY the merged column: its value is COALESCE of the two sides, computed by the projection the star expands into, and this builder materializes a computed sort key beside a named SELECT list, which a star-only list is not. Name the columns, or write the join condition with ON",
				"dag-shuffled": "ERR ORDER BY coalesce(a.id, b.id): a bare `SELECT *` over a FULL JOIN ... USING cannot be ORDERED BY the merged column: its value is COALESCE of the two sides, computed by the projection the star expands into, and this builder materializes a computed sort key beside a named SELECT list, which a star-only list is not. Name the columns, or write the join condition with ON",
				"dag-morsel4":  "ERR ORDER BY coalesce(a.id, b.id): a bare `SELECT *` over a FULL JOIN ... USING cannot be ORDERED BY the merged column: its value is COALESCE of the two sides, computed by the projection the star expands into, and this builder materializes a computed sort key beside a named SELECT list, which a star-only list is not. Name the columns, or write the join condition with ON",
			},
		},
		{
			name: "using/duplicate-right-keys",
			sql:  "SELECT * FROM psa a JOIN psc b USING (id) ORDER BY id",
			want: "cols=[id:INT64 a:INT64 c:INT64] rows=0",
		},
		{
			name: "on/ordinal-2",
			sql:  "SELECT * FROM zzp a JOIN zzj b ON a.id = b.id ORDER BY 2",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4)] rows=3 | 1,-3.50,1,1.1111 | 2,0.00,2,12345678.1234 | 3,12.75,3,3.3333",
		},
		{
			name: "on/ordinal-4-desc",
			sql:  "SELECT * FROM zzp a JOIN zzj b ON a.id = b.id ORDER BY 4 DESC",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4)] rows=3 | 2,0.00,2,12345678.1234 | 3,12.75,3,3.3333 | 1,-3.50,1,1.1111",
		},
		{
			name: "on/ordinal-over-two-relations",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY 2, 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "on/ordinal-over-three-way",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id JOIN lat_ord p ON p.id = o.id ORDER BY 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50,1,Alice,150 | 1,Alice,150,2,1,Gadget,100,1,Alice,150 | 2,Bob,200,3,2,Widget,75,2,Bob,200 | 2,Bob,200,4,2,Doohickey,125,2,Bob,200",
		},
		{
			name: "zero-row/two-way",
			sql:  "SELECT * FROM zzp a JOIN zzj b ON a.id = b.id WHERE a.id < 0",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4)] rows=0",
		},
		{
			name: "zero-row/three-way",
			sql:  "SELECT * FROM zzp a JOIN zzj b ON a.id = b.id JOIN psa c ON c.id = a.id WHERE a.id < 0",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4) id:INT64 a:INT64] rows=0",
		},
		{
			name: "zero-row/four-way",
			sql:  "SELECT * FROM zzp a JOIN zzj b ON a.id = b.id JOIN psa c ON c.id = a.id JOIN psb d ON d.id = a.id WHERE a.id < 0",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4) id:INT64 a:INT64 id:INT64 b:INT64] rows=0",
		},
		{
			name: "zero-row/mixed-outer",
			sql:  "SELECT * FROM zzp a LEFT JOIN zzj b ON a.id = b.id JOIN psa c ON c.id = a.id WHERE a.id < 0",
			want: "cols=[id:INT64 d92:DECIMAL(9,2) id:INT64 d92:DECIMAL(18,4) id:INT64 a:INT64] rows=0",
		},
		{
			name: "zero-row/derived-side-three-way",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id, order_id FROM lat_item) i ON i.order_id = o.id JOIN lat_ord p ON p.id = o.id WHERE o.id < 0",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 id:INT64 customer:STRING total:FLOAT64] rows=0",
		},
		{
			name: "naming/join-of-two-derived-blocks",
			sql:  "SELECT * FROM (SELECT id, amount + 1 FROM lat_item) a JOIN (SELECT id, amount + 2 FROM lat_item) b ON b.id = a.id ORDER BY a.id",
			want: "cols=[id:INT64 ?column?:FLOAT64 id:INT64 ?column?:FLOAT64] rows=4 | 1,51,1,52 | 2,101,2,102 | 3,76,3,77 | 4,126,4,127",
		},
		{
			name: "naming/derived-block-under-a-join",
			sql:  "SELECT * FROM lat_ord t JOIN (SELECT id, total + 1 FROM lat_ord) s ON s.id = t.id ORDER BY t.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 ?column?:FLOAT64] rows=3 | 1,Alice,150,1,151 | 2,Bob,200,2,201 | 3,Carol,0,3,1",
		},
		{
			name: "naming/set-op-in-a-cte",
			sql:  "WITH c AS (SELECT id, total + 1 FROM lat_ord UNION ALL SELECT id, total + 2 FROM lat_ord) SELECT * FROM c ORDER BY id, 2",
			want: "cols=[id:INT64 ?column?:FLOAT64] rows=6 | 1,151 | 1,152 | 2,201 | 2,202 | 3,1 | 3,2",
		},
		{
			name: "naming/qualified-star-over-a-set-op-cte",
			sql:  "WITH c AS (SELECT id, total + 1 FROM lat_ord UNION ALL SELECT id, total + 2 FROM lat_ord) SELECT c.* FROM c ORDER BY id, 2",
			want: "cols=[id:INT64 ?column?:FLOAT64] rows=6 | 1,151 | 1,152 | 2,201 | 2,202 | 3,1 | 3,2",
		},
		{
			name: "naming/set-op-block-under-a-join",
			sql:  "SELECT * FROM (SELECT id, total + 1 FROM lat_ord UNION ALL SELECT id, total + 2 FROM lat_ord) a JOIN psa b ON a.id = b.id ORDER BY a.id, 2",
			want: "cols=[id:INT64 ?column?:FLOAT64 id:INT64 a:INT64] rows=4 | 1,151,1,10 | 1,152,1,10 | 2,201,2,20 | 2,202,2,20",
		},
		{
			name: "naming/top-level-set-op",
			sql:  "SELECT id, total + 1 FROM lat_ord UNION ALL SELECT id, total + 2 FROM lat_ord ORDER BY id, 2",
			want: "cols=[id:INT64 ?column?:FLOAT64] rows=6 | 1,151 | 1,152 | 2,201 | 2,202 | 3,1 | 3,2",
		},
		{
			name: "naming/count-in-a-cte",
			sql:  "WITH c AS (SELECT id, COUNT(*) FROM lat_item GROUP BY id) SELECT * FROM c ORDER BY id",
			want: "cols=[id:INT64 count:INT64] rows=4 | 1,1 | 2,1 | 3,1 | 4,1",
		},
		// The NAME is this arc's and it is right on every arm that answers:
		// the leftmost arm's `COUNT(*)` publishes `count`. The three DAG arms
		// REFUSE the statement for a reason that predates this arc and is not
		// a naming one — a set-operation arm that selects an aggregate has no
		// union stage that can project the SELECT list over the aggregate
		// stage's own output (#346). Right on the engine's arms, loud on the
		// distributed ones: `distributed` by the arm rule, pinned, not chased.
		// PG: cols=[id:INT64 count:INT64] rows=8 1|1 · 1|1 · 2|1 · 2|1 · 3|1 · 3|1 · 4|1 · 4|1
		{
			name: "naming/count-in-a-set-op-cte",
			sql:  "WITH c AS (SELECT id, COUNT(*) FROM lat_item GROUP BY id UNION ALL SELECT id, 1 FROM lat_item) SELECT * FROM c ORDER BY id, 2",
			want: "cols=[id:INT64 count:INT64] rows=8 | 1,1 | 1,1 | 2,1 | 2,1 | 3,1 | 3,1 | 4,1 | 4,1",
			pin: map[string]string{
				"dag":          "ERR UNION ALL: arm 1: selects the aggregate \"count(*)\", whose output the arm's aggregate stage names for itself — the union stage cannot project the SELECT list over it. See issue #346",
				"dag-shuffled": "ERR UNION ALL: arm 1: selects the aggregate \"count(*)\", whose output the arm's aggregate stage names for itself — the union stage cannot project the SELECT list over it. See issue #346",
				"dag-morsel4":  "ERR UNION ALL: arm 1: selects the aggregate \"count(*)\", whose output the arm's aggregate stage names for itself — the union stage cannot project the SELECT list over it. See issue #346",
			},
		},
		{
			name: "naming/cast-in-a-set-op-cte",
			sql:  "WITH c AS (SELECT id, CAST(total AS VARCHAR) FROM lat_ord UNION ALL SELECT id, customer FROM lat_ord) SELECT * FROM c ORDER BY id, 2",
			want: "cols=[id:INT64 total:STRING] rows=6 | 1,150 | 1,Alice | 2,200 | 2,Bob | 3,0 | 3,Carol",
		},
		{
			name: "dupname/a-reference-into-the-block",
			sql:  "SELECT x.id FROM (SELECT a.id, b.id FROM lat_item a JOIN lat_item b ON a.id = b.id) x ORDER BY 1",
			want: "ERR column reference \"id\" is ambiguous",
		},
		// REFUSED on all five arms where PostgreSQL answers both columns by
		// POSITION. This is arc O2's own residue (c8d94fe3) and it is still
		// open: every expanded star item is a QUALIFIED REFERENCE, and a
		// block publishing two `id` cannot be addressed by one — the second
		// item would carry the first's VALUES. Closing it means the block's
		// list travelling by POSITION (`ProjectExprSpec.SourceSlot` one
		// relation out), which is a slot-identity change, not a star one.
		// Loud beats plausible: the bare star below, which reads the relation
		// positionally, answers — and the REFERENCE beside it is now 42702,
		// which is this arc's #1094.
		// PG: cols=[id:INT64 id:INT64] rows=4 1|1 · 2|2 · 3|3 · 4|4
		{
			name: "dupname/a-qualified-star-over-it",
			sql:  "SELECT x.* FROM (SELECT a.id, b.id FROM lat_item a JOIN lat_item b ON a.id = b.id) x ORDER BY 1",
			want: "ERR column \"x.*\" does not exist in the input schema: a `x.*` expands only from a relation whose column list is known — a base table, or a derived table or CTE whose own SELECT list names its columns — and this one's is not; name the columns",
			pin: map[string]string{
				"dag":          "ERR column \"x.*\" does not exist in the input schema: a `x.*` expands only from a relation whose column list is known — a base table, or a derived table or CTE whose own SELECT list names its columns — and this one's is not; name the columns",
				"dag-shuffled": "ERR column \"x.*\" does not exist in the input schema: a `x.*` expands only from a relation whose column list is known — a base table, or a derived table or CTE whose own SELECT list names its columns — and this one's is not; name the columns",
				"dag-morsel4":  "ERR column \"x.*\" does not exist in the input schema: a `x.*` expands only from a relation whose column list is known — a base table, or a derived table or CTE whose own SELECT list names its columns — and this one's is not; name the columns",
			},
		},
		{
			name: "dupname/a-bare-star-over-it",
			sql:  "SELECT * FROM (SELECT a.id, b.id FROM lat_item a JOIN lat_item b ON a.id = b.id) x ORDER BY 1",
			want: "cols=[id:INT64 id:INT64] rows=4 | 1,1 | 2,2 | 3,3 | 4,4",
		},
		// A star over a LATERAL arm is expanded to the FROM arms' own lists
		// since arc JP round 4, the lateral's read as `l.*` reads it: every arm
		// publishes PostgreSQL's bare `id` (#1126's name half, which the arm
		// published `l.id` — `i.id` on the DAG — while the star read the join
		// operator's stream), and the pin that recorded it is gone.
		{
			name: "lateral/a-star-over-a-lateral-arm",
			sql:  "SELECT * FROM lat_ord o, LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) l ORDER BY o.id, l.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,50 | 1,Alice,150,2,100 | 2,Bob,200,3,75 | 2,Bob,200,4,125",
			// Runs single-process since arc JP round 3: the lateral arm carries a
			// name the outer relation also carries (dagplan.ErrLateralIdentityDistributed).
			routed: map[string]string{"dag": "LateralIdentity +1", "dag-morsel4": "LateralIdentity +1", "dag-shuffled": "LateralIdentity +1"},
		},
		// The SAME reference with no star in the statement: `l.id` was 1,1,2,2
		// on the three DAG arms (#1126's column, round-1 review B2) until arc JP
		// round 2 resolved a LATERAL body's unaliased item to its source column
		// on the DAG (ADR-0026 §8l); the pin agreed and is deleted.
		// PG: cols=[id:INT64 id:INT64 amount:FLOAT64] rows=4 1|1|50 · 1|2|100 · 2|3|75 · 2|4|125
		{
			name: "lateral/a-reference-into-a-lateral-arm",
			sql:  "SELECT o.id, l.id, l.amount FROM lat_ord o, LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) l ORDER BY o.id, l.id",
			want: "cols=[id:INT64 id:INT64 amount:FLOAT64] rows=4 | 1,1,50 | 1,2,100 | 2,3,75 | 2,4,125",
			// Runs single-process since arc JP round 3: the lateral arm carries a
			// name the outer relation also carries (dagplan.ErrLateralIdentityDistributed).
			routed: map[string]string{"dag": "LateralIdentity +1", "dag-morsel4": "LateralIdentity +1", "dag-shuffled": "LateralIdentity +1"},
		},
		{
			name: "ctl/arms-share-one-name",
			sql:  "SELECT * FROM psa a JOIN psb b ON a.id = b.id ORDER BY a.id",
			want: "cols=[id:INT64 a:INT64 id:INT64 b:INT64] rows=1 | 2,20,2,200",
		},
		{
			name: "ctl/arms-share-one-name-zero-row",
			sql:  "SELECT * FROM psa a JOIN psb b ON a.id = b.id WHERE a.id < 0",
			want: "cols=[id:INT64 a:INT64 id:INT64 b:INT64] rows=0",
		},
		// ---- THE DEPTH the walk used to stop at (round-1 review, B4) -------
		//
		// A set operation's published name is its LEFTMOST arm's, and the
		// walks that reach that arm descend one node per arm. Each of the four
		// carried an arbitrary hop bound and each turned into a different
		// wrong answer past it: the operation published `total + 1` again
		// (`publishedOutputProjectionNode`), a qualified star over the block
		// became 42703 and a bare star over it as a join ARM published the
		// PLAN's `b.id` (`blockOwnProjection`), and a positional ORDER BY over
		// the CTE spelling was refused 0A000 (`projectOutputNamesBelow`). The
		// cells below sit PAST every one of those bounds; the `naming/*` cells
		// above are all two arms deep, which is why the cliff shipped.
		{
			name: "naming/set-op-chain-9-arms",
			sql:  srSetOpChain(9) + " ORDER BY 1, 2",
			want: srChainRows(9),
		},
		{
			name: "naming/set-op-chain-16-arms",
			sql:  srSetOpChain(16) + " ORDER BY 1, 2",
			want: srChainRows(16),
		},
		{
			name: "naming/set-op-chain-9-arms-in-a-cte",
			sql:  "WITH c AS (" + srSetOpChain(9) + ") SELECT * FROM c ORDER BY 1, 2",
			want: srChainRows(9),
		},
		{
			// A written key BESIDE a positional one still needs the walk —
			// the ordinal is what `projectOutputNamesBelow` resolves, so this
			// cell is refused past the bound exactly as the all-positional
			// one is. It is here because a mixed key is the ordinary
			// spelling, not because it is a control.
			name: "naming/set-op-chain-9-arms-in-a-cte-written-key-beside-an-ordinal",
			sql:  "WITH c AS (" + srSetOpChain(9) + ") SELECT * FROM c ORDER BY id, 2",
			want: srChainRows(9),
		},
		{
			// THE CONTROL that localises the refusal to the BOUND and not to
			// the shape: the same nine-arm chain, the same CTE, the same
			// depth — with the second item ALIASED so both keys are written
			// names and no ordinal is resolved. It answers at every depth
			// with the bound in place and without it. A cell that fails only
			// when a POSITION must be counted says the walk, not the query,
			// was the refusal.
			name: "naming/set-op-chain-9-arms-in-a-cte-named-key",
			sql: "WITH c AS (" + strings.Replace(srSetOpChain(9), "total + 1 FROM", "total + 1 AS t FROM", 1) +
				") SELECT * FROM c ORDER BY id, t",
			want: strings.Replace(srChainRows(9), "?column?", "t", 1),
		},
		{
			// The ARM spelling: past the bound the star could not state this
			// arm at all, so the whole expansion fell back to the join
			// operator's stream and published `b.id`.
			name: "naming/9-arm-set-op-block-as-a-join-arm",
			sql:  "SELECT * FROM (" + srSetOpChain(9) + ") a JOIN psa b ON a.id = b.id ORDER BY 2",
			want: "cols=[id:INT64 ?column?:FLOAT64 id:INT64 a:INT64] rows=9 | 1,151,1,10 | 1,152,1,10 | " +
				"1,153,1,10 | 1,154,1,10 | 1,155,1,10 | 1,156,1,10 | 1,157,1,10 | 1,158,1,10 | 1,159,1,10",
		},
	}
}
