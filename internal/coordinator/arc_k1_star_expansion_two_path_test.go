package coordinator

import (
	"context"
	"testing"
	"time"
)

// A STAR IS ITS SOURCE, IN ITS POSITION — #979, #982, #976, #963 and #962, on
// FOUR arms against live PostgreSQL 17 over rows identical to `lat_ord`,
// `lat_item`, `decpair`, `typemx` and `typemx_dim`.
//
// One expansion rule, `logical.ExpandStarProjections`, reading the relation's
// own output list — arc J1's rule. What this arc changed is the three places
// that never reached it:
//
//   - a QUALIFIED star ALONE built no projection at all (`isStarOnly` read
//     `o.*` as the identity of its input), so nothing carried a star for the
//     expansion to rewrite and the query published the WHOLE join. #979.
//   - the binder answered "there is a star, ask somebody with a catalog" — and
//     nobody did, though the binder IS somebody with a catalog. A derived
//     block whose body held a star opened its scope, and an open scope
//     validates nothing. #976, and the missing column list behind #963.
//   - a positional ORDER BY over a star could not count a SET OPERATION's
//     columns, though a set operation publishes its leftmost arm's names.
//     #982, which is #810's residual.
//
// #962 is VERIFIED CLOSED AT BASE and carried here as a value: arc J1's
// expansion closed it and deleted arc I1's per-arm pin; nothing in this arc
// touches it, and these two cells say so the day something does.
func TestArcK1AStarIsItsSourceInItsPosition(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const lat = `FROM lat_ord o JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item ` +
		`WHERE order_id = o.id) s ON true`
	const ord3 = "cols=[id:INT64 customer:STRING total:FLOAT64] rows=3 | " +
		"1,Alice,150 | 2,Bob,200 | 3,Carol,0"

	f1Run(t, arms, []f1Case{
		// ---- #979 a qualified star ALONE names ONE relation ----------------
		{
			// The filing's shape. It published `id, customer, total, mx` —
			// four columns where PostgreSQL publishes o's three.
			name: "979 a qualified star alone over a LATERAL",
			sql:  `SELECT o.* ` + lat + ` ORDER BY o.id`,
			want: ord3,
		},
		{
			// WIDER than filed, and this is the cell that says so: over a
			// PLAIN join the same spelling published SEVEN columns — the whole
			// join, probe side first — with one of them literally named
			// `o.id`. The filing called it a lateral defect; it was never
			// about laterals.
			name: "979 a qualified star alone over a plain join",
			sql:  `SELECT o.* FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY o.id, i.id`,
			want: "cols=[id:INT64 customer:STRING total:FLOAT64] rows=4 | " +
				"1,Alice,150 | 1,Alice,150 | 2,Bob,200 | 2,Bob,200",
		},
		{
			name: "979 a qualified star alone over a derived table",
			sql: `SELECT x.* FROM (SELECT id, customer FROM lat_ord) x ` +
				`JOIN lat_item i ON i.order_id = x.id ORDER BY x.id, i.id`,
			want: "cols=[id:INT64 customer:STRING] rows=4 | 1,Alice | 1,Alice | 2,Bob | 2,Bob",
		},
		{
			name: "979 a qualified star alone over a CTE",
			sql: `WITH c AS (SELECT id, customer FROM lat_ord) SELECT c.* FROM c ` +
				`JOIN lat_item i ON i.order_id = c.id ORDER BY c.id, i.id`,
			want: "cols=[id:INT64 customer:STRING] rows=4 | 1,Alice | 1,Alice | 2,Bob | 2,Bob",
		},
		{
			// The BARE star alone, which IS the identity of its input and must
			// stay elided: the boundary attempted from the other side. Every
			// star-only `SELECT *` in the tree depends on this.
			name: "979 ctl a bare star alone is still the identity of its input",
			sql:  `SELECT * FROM lat_ord ORDER BY id`,
			want: ord3,
		},
		{
			// THE LATERAL'S OWN STAR, ALONE — a WRONG → LOUD move this arc
			// makes, and declares. At bb8635a4 `SELECT s.*` published the
			// whole join (`id, customer, total, mx`) where PostgreSQL
			// publishes `mx`, on four arms and on the wire; it is refused now,
			// because the expansion deliberately declines a lateral's own
			// output (ADR-0012, arc J1's reason: that output is a projection
			// this pass does not enumerate, and its scan carries the
			// correlation slot the join is about to drop).
			//
			// The refusal is the PLANNER's one sentence on every arm, which is
			// what `docs/sql-reference.md` and ADR-0012 promise. It reached the
			// executor's generic `42000 operator execute: column "s.*" …` for
			// as long as a star-only list built no projection for
			// `refuseUnexpandedStarBesideItems` to see.
			name: "979 the lateral's own star alone is refused, in one sentence",
			sql:  `SELECT s.* ` + lat,
			want: `ERR building physical plan: column "s.*" does not exist in the input ` +
				"schema: a `s.*` expands only from a relation whose column list is known",
			pin: map[string]string{
				"dag": `ERR physical plan: column "s.*" does not exist in the input ` +
					"schema: a `s.*` expands only from a relation whose column list is known",
				"dagshuf": `ERR physical plan: column "s.*" does not exist in the input ` +
					"schema: a `s.*` expands only from a relation whose column list is known",
			},
			why: "ONE refusal under the two engines' own error prefixes, not a divergence; " +
				"PostgreSQL publishes `mx` and answers",
		},
		{
			name: "979 ctl a qualified star BESIDE another item, right since J1",
			sql:  `SELECT o.*, s.mx ` + lat + ` ORDER BY o.id`,
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64] rows=3 | " +
				"1,Alice,150,100 | 2,Bob,200,125 | 3,Carol,0,NULL",
		},
		{
			// A qualified star ALONE under DISTINCT, which is the shape that
			// says the expansion's QUALIFIED references really resolve: the
			// DISTINCT lowers to a GROUP BY whose keys are then `o.id`,
			// `o.customer`, `o.total`, and on the shuffled arm the exchange
			// is keyed on them. `SELECT DISTINCT s.*` is what
			// `physical.TestDerivedStarDistinctEmitsADedupStage` checks as a
			// plan shape; this is the same thing as ROWS, on four arms.
			name: "979 DISTINCT over a qualified star alone, over a join",
			sql: `SELECT DISTINCT o.* FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ` +
				`ORDER BY 1`,
			want: "cols=[id:INT64 customer:STRING total:FLOAT64] rows=2 | " +
				"1,Alice,150 | 2,Bob,200",
		},
		{
			// The same over a relation big enough to really shuffle: 5000
			// distinct rows, so the exchange keyed on the qualified names is
			// exercised rather than elided.
			name: "979 DISTINCT over a qualified star alone, 5000 rows",
			sql:  `SELECT COUNT(*) AS c FROM (SELECT DISTINCT tx.* FROM typemx tx) u`,
			want: "cols=[c:INT64] rows=1 | 5000",
		},

		// ---- #982 a positional ORDER BY counts the star's columns ----------
		{
			// #810's residual. A set operation publishes its LEFTMOST arm's
			// names — PostgreSQL's rule, which `BlockOutputColumns` and
			// `applyColumnAliases` already read — so the position is
			// countable, and psql and every table preview send this shape.
			name: "982 a positional ORDER BY over a star of a UNION derived table",
			sql: `SELECT * FROM (SELECT id, customer FROM lat_ord WHERE id<3 ` +
				`UNION ALL SELECT id, customer FROM lat_ord WHERE id>=3) u ORDER BY 2`,
			want: "cols=[id:INT64 customer:STRING] rows=3 | 1,Alice | 2,Bob | 3,Carol",
		},
		{
			// The filing's CTE cell, which did NOT reproduce at bb8635a4 — it
			// answers there and here. Kept as a value so the claim is on
			// record rather than repeated.
			name: "982 ctl a positional ORDER BY over a star of a CTE",
			sql:  `WITH c AS (SELECT id, customer FROM lat_ord) SELECT * FROM c ORDER BY 1`,
			want: "cols=[id:INT64 customer:STRING] rows=3 | 1,Alice | 2,Bob | 3,Carol",
		},
		{
			// The filing's VALUES cell, with the reviewer's note followed: the
			// shape that parses ANSWERS, at base and here. What does not parse
			// is `(VALUES …) v(a,b)` with no `AS`, which is a parser gap and
			// not a star-expansion one — recorded in REPORT as a filing
			// candidate rather than folded in here.
			name: "982 ctl a positional ORDER BY over a VALUES derived table",
			sql:  `SELECT * FROM (VALUES (2,'b'),(1,'a')) AS v(a,b) ORDER BY 1`,
			want: "cols=[a:INT64 b:STRING] rows=2 | 1,a | 2,b",
		},
		{
			name: "982 ctl the base-table spelling #810 fixed",
			sql:  `SELECT * FROM lat_ord ORDER BY 1`,
			want: ord3,
		},

		// ---- #976 a derived block's column list validates its readers ------
		{
			// It answered NULL on every arm — a column that does not exist,
			// read as a value. The binder has a catalog and now expands the
			// star, so the reference is checked.
			name: "976 an unknown column through a derived star over a join",
			sql:  `SELECT x.nosuchcol AS leaked FROM (SELECT * ` + lat + `) x ORDER BY 1`,
			want: `ERR unknown column "x.nosuchcol" (available: customer, id, mx, total)`,
		},
		{
			// The sibling J1 pinned beside it: a planner-minted slot is as
			// unreachable as a column that does not exist, which is the
			// property — and it is asserted through the same refusal now.
			name: "976 the minted slot is unreachable through a derived star",
			sql:  `SELECT x.__key_0 AS leaked FROM (SELECT * ` + lat + `) x ORDER BY 1`,
			want: `ERR unknown column "x.__key_0" (available: customer, id, mx, total)`,
		},
		{
			name: "976 ctl the same reference through a plain derived table",
			sql:  `SELECT x.nosuchcol AS leaked FROM (SELECT id FROM lat_ord) x ORDER BY 1`,
			want: `ERR unknown column "x.nosuchcol" (available: id)`,
		},
		{
			name: "976 ctl a REAL column through a derived star over a join",
			sql:  `SELECT x.mx AS m FROM (SELECT * ` + lat + `) x ORDER BY 1`,
			want: "cols=[m:FLOAT64] rows=3 | 100 | 125 | NULL",
		},

		// ---- #963 a derived table whose SELECT list is a star over a JOIN --
		{
			// The filing's SQL. All four arms failed with `filter column "id"
			// does not exist in the input schema`, because the star-only
			// select list built no projection and the derived block published
			// the join rather than tx's columns.
			name: "963 a derived qualified star over a join, filtered",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT tx.* FROM typemx_dim dim ` +
				`JOIN typemx tx ON tx.g = dim.k) t WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: "cols=[n:INT64] rows=1 | 10",
		},
		{
			// The BARE-star twin, NOT closed and pinned with its mechanism:
			// `ExpandStarProjections` declines a bare star over a JOIN by
			// design, because guessing a join's column set would silently
			// change which columns a query returns (ADR-0012, #810). Lifting
			// it needs an ORDERED model of a join's emitted columns and the
			// three refusals #810 records lift together; that is its own arc.
			// LOUD at bb8635a4 and loud here, by the same sentence.
			name: "963 PINNED a derived BARE star over a join, filtered",
			sql: `SELECT (SELECT COUNT(*) FROM (SELECT * FROM typemx_dim dim ` +
				`JOIN typemx tx ON tx.g = dim.k) t WHERE id < 10) AS n FROM decpair WHERE id < 2`,
			want: "cols=[n:INT64] rows=1 | 10",
			pin: map[string]string{
				"single":   `ERR executing query: scalar subquery could not be executed`,
				spilledArm: `ERR executing query: scalar subquery could not be executed`,
				"dag":      `ERR SELECT list no stage computes local execution`,
				"dagshuf":  `ERR SELECT list no stage computes local execution`,
			},
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1",
			},
			why: "a BARE star over a JOIN is left unexpanded by design (ADR-0012, #810): " +
				"guessing a join's column set would silently change which columns a query " +
				"returns, and lifting it needs an ordered model of a join's emitted columns",
		},

		// ---- #962 VERIFIED CLOSED AT BASE ----------------------------------
		{
			name: "962 two qualified stars in one SELECT list, inside a CTE",
			sql: `WITH c AS (SELECT dim.*, tx.* FROM typemx_dim dim JOIN typemx tx ` +
				`ON tx.g = dim.k) SELECT (SELECT COUNT(*) FROM c WHERE id < 10) AS n ` +
				`FROM decpair WHERE id < 2`,
			want: "cols=[n:INT64] rows=1 | 10",
		},
		{
			// The TOP-LEVEL spelling, which no suite covered — and which the
			// round-1 review found had no STAR in it at all. It does now:
			// `o.*, i.*` publishes o's three columns then i's four, in
			// PostgreSQL's order (measured live).
			name: "962 two qualified stars at the top level publish both lists in order",
			sql: `SELECT o.*, i.* FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ` +
				`ORDER BY o.id, i.id`,
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 " +
				"product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | " +
				"1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | " +
				"2,Bob,200,4,2,Doohickey,125",
		},
	})
}
