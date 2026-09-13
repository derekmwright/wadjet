package coordinator

import (
	"context"
	"testing"
	"time"
)

// ARC C1 — THE READ-TEST SEAM, ENUMERATED ONCE.
//
// A column-alias list over a LATERAL body whose SELECT list holds a `*` cannot
// be applied: the width of the star is not known where the rename must be made,
// and the decorrelation joins on the column the body's correlated predicate
// names, so a positional rename could take that column and leave the join with
// no key (round 3 measured the alternative — deferring the list past the star
// answered ZERO ROWS). So the list is refused `0A000` WHEN the enclosing query
// READS a name it introduces, and answers otherwise.
//
// "Reads" is one question asked at many POSITIONS, and four review rounds moved
// one position at a time: too wide (every star, every bare name in a sibling),
// then too narrow (no sort term, only qualified sibling references), each swap
// trading a class of wrong answers for a class of wrong refusals. This table is
// the whole seam in one place — every position an enclosing query can name a
// lateral's column from, crossed with what the reference RESOLVES to, with the
// required disposition on all five arms. A new position is a row here, not a
// round.
//
// Every ANSWER cell carries PostgreSQL 17.11's rows, measured live over this
// fixture before the code was written; every LOUD cell names the refusal. The
// two exceptions are marked PINNED and say why.
func TestC1FTheReadTestSeam(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this seam stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	const lat = "FROM lat_ord u, LATERAL (SELECT * FROM lat_item i WHERE i.order_id = u.id) l(w) "
	reads := `ERR the query reads "w"`

	c1Run(t, arms, []c1Case{
		// ── THE BLOCK'S OWN EXPRESSIONS ─────────────────────────────────
		{
			name: "select item, qualified with the lateral's alias -> the list -> LOUD",
			sql:  "SELECT l.w " + lat + "ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers 1,2,3,4; main answered four NULLs",
		},
		{
			name: "select item, bare -> the list -> LOUD",
			sql:  "SELECT w " + lat + "ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers 1,2,3,4; a bare name resolves to the list here, and main answered four NULLs",
		},
		{
			name: "select item, a column of the same lateral the list does NOT introduce -> the body -> ANSWERS",
			sql:  "SELECT l.amount " + lat + "ORDER BY 1",
			want: "cols=[amount:FLOAT64] rows=4 | 50 | 75 | 100 | 125",
		},
		{
			name: "an aggregate's ARGUMENT -> the list -> LOUD",
			sql:  "SELECT SUM(l.w) AS s " + lat,
			want: reads,
			why:  "PostgreSQL answers 10; RewriteExpr enters no aggregate, so this once answered NULL",
		},
		{
			name: "a window call's ARGUMENT -> the list -> LOUD",
			sql:  "SELECT SUM(l.w) OVER () AS s " + lat,
			want: reads,
			why:  "PostgreSQL answers 10,10,10,10; main answered four NULLs",
		},
		{
			name: "a window's PARTITION BY -> the list -> LOUD",
			sql:  "SELECT COUNT(*) OVER (PARTITION BY l.w) AS c " + lat + "ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers 1,1,1,1; main failed at execution naming the missing column",
		},
		{
			name: "a window's ORDER BY -> the list -> LOUD",
			sql:  "SELECT COUNT(*) OVER (ORDER BY l.w) AS c " + lat + "ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers 1,2,3,4; main failed at execution naming the missing column",
		},
		{
			// PINNED, and not this seam's: a frame bound naming a COLUMN is
			// refused by both engines. PostgreSQL raises "argument of ROWS
			// must not contain variables"; this parser refuses the token.
			// Loud on both sides, so the position cannot answer a plausible
			// number whatever the read test decides.
			name: "a window's FRAME BOUND -> refused by the parser, as PostgreSQL refuses it",
			sql: "SELECT SUM(u.id) OVER (ORDER BY u.id ROWS BETWEEN l.w PRECEDING AND CURRENT ROW) AS s " +
				lat + "ORDER BY 1",
			want: "ERR unexpected frame bound token",
			why:  "PostgreSQL: argument of ROWS must not contain variables",
		},
		{
			name: "a WHERE predicate -> the list -> LOUD",
			sql:  "SELECT u.id " + lat + "WHERE l.w > 2 ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers 2,2; main failed in the physical plan naming the missing filter column",
		},
		{
			name: "a HAVING clause, the only read -> the list -> LOUD",
			sql:  "SELECT u.id, COUNT(*) AS c " + lat + "GROUP BY u.id HAVING MAX(l.w) > 2 ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers one row, 2,2; main answered ZERO rows",
		},
		{
			name: "a GROUP BY key -> the list -> LOUD",
			sql:  "SELECT l.w " + lat + "GROUP BY l.w ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers 1,2,3,4; main failed at execution naming the missing key",
		},
		{
			// QUALIFY is not PostgreSQL's — it cannot be the oracle here — but
			// the position is one the walk asks, and the disposition is the
			// same refusal the other filter positions take.
			name: "a QUALIFY clause -> the list -> LOUD",
			sql: "SELECT u.id, ROW_NUMBER() OVER (ORDER BY l.w) AS rn " + lat +
				"QUALIFY rn = 1",
			want: reads,
			why:  "PostgreSQL has no QUALIFY; main failed at execution naming the missing window key",
		},
		{
			name: "a written join ON condition -> the list -> LOUD",
			sql: "SELECT m.z " + lat + "JOIN LATERAL (SELECT 7 AS z) m ON m.z > l.w " +
				"ORDER BY 1",
			want:   reads,
			why:    "PostgreSQL answers 7,7,7,7; main answered ZERO rows",
			routed: map[string]string{},
		},

		// ── THE ENCLOSING ORDER BY ──────────────────────────────────────
		{
			name: "sort term, qualified with the lateral's alias -> the list -> LOUD",
			sql:  "SELECT u.id " + lat + "ORDER BY l.w DESC",
			want: reads,
			why:  "PostgreSQL answers 2,2,1,1; main sorted on nothing and answered 1,1,2,2",
		},
		{
			name: "sort term, bare, with no output column of that name -> the list -> LOUD",
			sql:  "SELECT u.id " + lat + "ORDER BY w DESC",
			want: reads,
			why:  "PostgreSQL answers 2,2,1,1; main sorted on nothing and answered 1,1,2,2",
		},
		{
			// PostgreSQL resolves an unqualified sort term against the SELECT
			// list's OUTPUT columns FIRST, so this one is `u.total` and the
			// alias list is never consulted (round-6 review, B3).
			name: "sort term, bare, SHADOWED by an output alias -> the output column -> ANSWERS",
			sql:  "SELECT u.total AS w " + lat + "ORDER BY w DESC",
			want: "cols=[w:FLOAT64] rows=4 | 200 | 200 | 150 | 150",
		},
		{
			// THE OTHER HALF OF PostgreSQL'S RULE (round-7 review, B1): "an
			// output column name has to stand alone, that is, it cannot be
			// used in an expression". Inside one the name is an INPUT column —
			// this alias list's — so the read test must see it. Skipping it
			// per NODE rather than per TERM dropped the rename and sorted on
			// nothing, which is the failure the whole refusal exists for.
			name: "sort term, the output alias INSIDE an expression -> the INPUT column -> LOUD",
			sql:  "SELECT u.total AS w " + lat + "ORDER BY w + 0 DESC",
			want: reads,
			why:  "PostgreSQL answers 200,200,150,150 — it binds the list's column here; the round-7 tip answered 150,150,200,200",
		},
		{
			name: "sort term, the output alias inside a NEGATION -> the INPUT column -> LOUD",
			sql:  "SELECT u.total AS w " + lat + "ORDER BY w * -1",
			want: reads,
			why:  "PostgreSQL answers 200,200,150,150; the round-7 tip answered 150,150,200,200",
		},
		{
			name: "sort term, the output alias inside a CASE -> the INPUT column -> LOUD",
			sql: "SELECT u.total AS w " + lat +
				"ORDER BY CASE WHEN w > 2 THEN 0 ELSE 1 END, u.id",
			want: reads,
			why:  "PostgreSQL answers 200,200,150,150; the round-7 tip answered 150,150,200,200",
		},
		{
			// PostgreSQL treats a parenthesised bare name as standing alone
			// and binds the OUTPUT column (measured: same rows as the bare
			// spelling). The AST does show the ParenNode — what forbids the
			// skip is what taking it produces: peeled, the sort binds nothing
			// and `SELECT u.total AS w … ORDER BY (w) DESC` answers
			// 150,150,200,200 for PostgreSQL's 200,200,150,150 (round-8
			// review, P2). Loud, not a sort that binds nothing.
			name: "sort term, the output alias PARENTHESISED -> LOUD rather than a sort binding nothing",
			sql:  "SELECT u.total AS w " + lat + "ORDER BY (w) DESC",
			want: reads,
			why:  "PostgreSQL answers 200,200,150,150 by binding the output column; refusing is the conservative half",
		},
		{
			// A MIXED SORT LIST, and the DOCTRINE'S COST recorded with
			// PostgreSQL's value beside it (round-8 review, P1). One term names
			// the alias list through an expression and the other is the
			// standalone output alias, so the first term is a read and the
			// query is refused — where PostgreSQL, and this branch before the
			// sort term became a read, answer 200,200,150,150. Its neighbour
			// with a QUALIFIED `l.w` second term has been refused since the
			// position was added. Answering either needs the rename the star's
			// width makes impossible; the day it does, this cell fails and is
			// rewritten to PostgreSQL's rows.
			name: "a MIXED sort list, one term over the list and one standalone alias -> LOUD",
			sql: "SELECT u.total AS w " + lat +
				"ORDER BY CASE WHEN w > 100 THEN 0 ELSE 1 END, w DESC",
			want: reads,
			why:  "PostgreSQL answers 200,200,150,150 — a right answer traded for a loud one, which is this refusal's cost",
		},
		{
			// THE DISCRIMINATING FIXTURE. `u.total` and `l.w` order these rows
			// the same way, so the corpus could not say which column a sort
			// term bound until the outer column was NEGATED: the output alias
			// descends -150,-200 and the list's column descends 4,3,2,1.
			name: "DISCRIMINATOR: a negated outer alias, the name inside an expression -> the INPUT column -> LOUD",
			sql: "SELECT -u.total AS w FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY w + 0 DESC",
			want: reads,
			why:  "PostgreSQL answers -200,-200,-150,-150 (the list's column); the round-7 tip answered -150,-150,-200,-200",
		},
		{
			name: "DISCRIMINATOR: a negated outer alias, the name STANDING ALONE -> the output column -> ANSWERS",
			sql: "SELECT -u.total AS w FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY w DESC",
			want: "cols=[w:FLOAT64] rows=4 | -150 | -150 | -200 | -200",
			why:  "PostgreSQL answers -150,-150,-200,-200 — the output column, which is the half this branch applies",
		},
		{
			name: "sort term, bare, an output alias the list does not introduce -> the output column -> ANSWERS",
			sql: "SELECT u.id AS w FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(x) ORDER BY w DESC",
			want: "cols=[w:INT64] rows=4 | 2 | 2 | 1 | 1",
		},
		{
			name: "sort term, an ORDINAL -> the output position -> ANSWERS",
			sql:  "SELECT u.id " + lat + "ORDER BY 1",
			want: "cols=[id:INT64] rows=4 | 1 | 1 | 2 | 2",
		},
		{
			name: "sort term, a column of the same lateral the list does not introduce -> the body -> ANSWERS",
			sql:  "SELECT u.id " + lat + "ORDER BY l.amount DESC",
			want: "cols=[id:INT64] rows=4 | 2 | 1 | 2 | 1",
		},
		{
			name: "sort term, the OUTER column -> the outer relation -> ANSWERS",
			sql:  "SELECT u.id " + lat + "ORDER BY u.id",
			want: "cols=[id:INT64] rows=4 | 1 | 1 | 2 | 2",
		},

		// ── A LATER FROM ITEM'S BODY ────────────────────────────────────
		{
			name: "a sibling body's reference, qualified -> the list -> LOUD",
			sql: "SELECT m.z " + lat + ", LATERAL (SELECT l.w + 100 AS z) m " +
				"ORDER BY 1",
			want:   reads,
			why:    "PostgreSQL answers 101..104; main answered four NULLs",
			routed: map[string]string{},
		},
		{
			// A table-less sibling has no FROM of its own, so a bare `w` there
			// can resolve to nothing but this lateral. Requiring qualification
			// dropped it and answered NULLs (round-6 review, B1).
			name: "a sibling body's reference, bare, the sibling is TABLE-LESS -> the list -> LOUD",
			sql: "SELECT m.z " + lat + ", LATERAL (SELECT w + 100 AS z) m " +
				"ORDER BY 1",
			want:   reads,
			why:    "PostgreSQL answers 101..104; the round-6 tip answered four NULLs",
			routed: map[string]string{},
		},
		{
			name: "a sibling body's bare reference inside a CASE -> the list -> LOUD",
			sql: "SELECT m.z " + lat + ", LATERAL (SELECT CASE WHEN w > 2 THEN 1 ELSE 0 END AS z) m " +
				"ORDER BY 1",
			want:   reads,
			why:    "PostgreSQL answers 0,0,1,1; the round-6 tip answered 0,0,0,0 — a wrong VALUE, not a NULL",
			routed: map[string]string{},
		},
		{
			name: "a sibling body's bare reference the SIBLING publishes -> the sibling's column -> ANSWERS",
			sql: "SELECT b.k, u.id " + lat + ", LATERAL (SELECT w + 5 AS k FROM (SELECT 2 AS w) y) b " +
				"ORDER BY 2, 1",
			want:   "cols=[k:INT64 id:INT64] rows=4 | 7,1 | 7,1 | 7,2 | 7,2",
			routed: c1TableLess,
		},
		{
			name: "a sibling that publishes the same name, projected -> the sibling's column -> ANSWERS",
			sql: "SELECT b.w, u.id " + lat + ", LATERAL (SELECT w FROM (SELECT 2 AS w) y) b " +
				"ORDER BY 2, 1",
			want:   "cols=[w:INT64 id:INT64] rows=4 | 2,1 | 2,1 | 2,2 | 2,2",
			routed: c1TableLess,
		},
		{
			// The sibling's OWN sort term is a different question from the
			// enclosing block's: a one-row body's sort is the identity
			// whatever it names, so it is not asked (round-6 review, B2).
			name: "a sibling body's OWN sort term, qualified -> a one-row sort, not asked -> ANSWERS",
			sql: "SELECT m.z, u.id " + lat + ", LATERAL (SELECT 7 AS z ORDER BY l.w) m " +
				"ORDER BY 2, 1",
			want:   "cols=[z:INT64 id:INT64] rows=4 | 7,1 | 7,1 | 7,2 | 7,2",
			routed: c1TableLess,
		},
		{
			name: "a sibling body's OWN sort term, bare -> a one-row sort, not asked -> ANSWERS",
			sql: "SELECT m.z, u.id " + lat + ", LATERAL (SELECT 7 AS z ORDER BY w) m " +
				"ORDER BY 2, 1",
			want:   "cols=[z:INT64 id:INT64] rows=4 | 7,1 | 7,1 | 7,2 | 7,2",
			routed: c1TableLess,
		},

		// ── A STAR IN THE BLOCK, AND ONE BLOCK UP ───────────────────────
		{
			name: "a BARE star -> republishes the list one block up -> LOUD",
			sql:  "SELECT * " + lat + "ORDER BY 1, 4",
			want: reads,
			why:  "PostgreSQL answers four seven-column rows; main refused for an unrelated reason",
		},
		{
			name: "a star qualified with THIS lateral -> republishes the list -> LOUD",
			sql:  "SELECT l.* " + lat + "ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers the body's four columns; main refused naming `l.*`",
		},
		{
			name: "a star qualified with the OUTER relation -> that relation's names -> ANSWERS",
			sql:  "SELECT u.* " + lat + "ORDER BY 1",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,Alice,150 | 1,Alice,150 | 2,Bob,200 | 2,Bob,200",
		},
		{
			name: "a star qualified with a THIRD relation -> that relation's names -> ANSWERS",
			sql: "SELECT v.customer FROM lat_ord v, lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want: "cols=[customer:STRING] rows=12 | Alice | Alice | Alice | Alice | Bob | Bob | Bob | Bob | " +
				"Carol | Carol | Carol | Carol",
		},
		{
			name: "ONE BLOCK UP through a bare star -> the list -> LOUD",
			sql: "SELECT x.w FROM (SELECT * FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w)) x ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers 1,2,3,4; main answered four NULLs",
		},
		{
			name: "ONE BLOCK UP through a QUALIFIED star -> the outer relation -> ANSWERS",
			sql: "SELECT x.id FROM (SELECT u.* FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w)) x ORDER BY 1",
			want: "cols=[id:INT64] rows=4 | 1 | 1 | 2 | 2",
		},

		// ── A NESTED SUBQUERY ───────────────────────────────────────────
		{
			name: "a subquery's LEFT OPERAND, in the block -> the list -> LOUD",
			sql: "SELECT u.id " + lat + "WHERE l.w IN (SELECT j.id FROM lat_item j) " +
				"ORDER BY 1",
			want: reads,
			why:  "PostgreSQL answers 1,1,2,2; main answered ZERO rows",
		},
		{
			name: "a subquery whose OWN FROM publishes the name -> that subquery's column -> ANSWERS",
			sql: "SELECT u.id, (SELECT q.w FROM (SELECT 9 AS w) q) AS z " + lat +
				"ORDER BY 1, 2",
			want: "cols=[id:INT64 z:INT64] rows=4 | 1,9 | 1,9 | 2,9 | 2,9",
			// An uncorrelated scalar subquery in the SELECT list has no
			// distributed stage of its own; the coordinator answers it
			// in-process. Pre-existing, and asserted so a right-to-routed move
			// stays visible.
			routed: map[string]string{
				"dag": "ScalarProjection +1", "dag-shuffled": "ScalarProjection +1",
				"dag-morsel4": "ScalarProjection +1",
			},
		},
		{
			// PINNED, and NOT this lane's: the walk stops at a subquery, whose
			// references are its own FROM's, so a correlated reference INSIDE
			// one is invisible to the read test and the rename is dropped
			// without a refusal. Measured IDENTICAL at main `98a90f9d`, which
			// is why it is pinned here rather than repaired in a closure
			// round: it is the correlation lane's territory (the scalar-
			// subquery spelling below already fails loudly there). Filed.
			name: "PINNED: a correlated reference INSIDE a nested EXISTS is not seen",
			sql:  "SELECT u.id " + lat + "WHERE EXISTS (SELECT 1 FROM lat_item j WHERE j.id = l.w) ORDER BY 1",
			want: "cols=[id:INT64] rows=0",
			why:  "PostgreSQL answers 1,1,2,2; main answers zero rows identically — a pre-existing gap, filed",
			// The shuffled arm does not even produce the wrong answer: the
			// dropped rename leaves the shuffle keying on a column no schema
			// carries. Loud, and pinned as measured.
			pin: map[string]string{
				"dag-shuffled": `ERR partitioned shuffle: key "w" not in schema`,
			},
			routed: map[string]string{},
		},
		{
			// Since arc C2 landed (ADR-0021 §1l), a FROM-less scalar subquery
			// IS its SELECT expression in the block that supplies the row — so
			// `l.w` is an expression of THIS block and the read test sees it,
			// where before it reached the correlation layer and failed at
			// execution. Loud → loud, earlier and better named, and no route
			// is taken because the refusal is now plan-time.
			name:   "a correlated reference inside a SCALAR subquery -> substituted into the block -> LOUD",
			sql:    "SELECT u.id, (SELECT l.w) AS z " + lat + "ORDER BY 1, 2",
			want:   reads,
			why:    "PostgreSQL answers 1,1 | 1,2 | 2,3 | 2,4; loud at main too, in the correlation layer's words",
			routed: map[string]string{},
		},

		// ── THE REFUSAL IS NOT ARMED WITHOUT BOTH HALVES ────────────────
		{
			name: "control: no alias list at all -> nothing to rename -> ANSWERS",
			sql: "SELECT l.amount FROM lat_ord u, LATERAL (SELECT * FROM lat_item i " +
				"WHERE i.order_id = u.id) l ORDER BY 1",
			want: "cols=[amount:FLOAT64] rows=4 | 50 | 75 | 100 | 125",
		},
		{
			name: "control: a list over a NAMED body -> the rename is applied -> ANSWERS",
			sql: "SELECT l.w FROM lat_ord u, LATERAL (SELECT i.amount AS a FROM lat_item i " +
				"WHERE i.order_id = u.id) l(w) ORDER BY 1",
			want: "cols=[w:FLOAT64] rows=4 | 50 | 75 | 100 | 125",
			why:  "PostgreSQL answers 50,75,100,125; main answered four NULLs",
		},
	})
}
