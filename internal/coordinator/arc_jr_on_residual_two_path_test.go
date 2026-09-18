// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"
)

// ARC JR — AN OUTER JOIN'S ON RESIDUAL, ENUMERATED ONCE, ON FIVE ARMS.
//
// #1153  an outer join whose ON holds a function call, a CAST or LIKE was
//
//	REFUSED. PostgreSQL evaluates any ON expression for any join kind. An
//	inner join lifts a non-equality conjunct into a filter above the join;
//	an outer join cannot (the padded rows would go with it), so the conjunct
//	is evaluated AT the join, per probe row against each build candidate,
//	and the padding is decided from its result. The evaluator that did that
//	was a second, smaller expression implementation — columns, literals,
//	arithmetic and comparisons — and everything else came back as a plan
//	refusal. It is the engine's own expression compiler now.
//
// #1178  a BETWEEN of any spelling inside ON was REFUSED, because the ON
//
//	clause was split into conjuncts by cutting its rendered text at every
//	" AND " and BETWEEN carries one. The split is on the AST now.
//
// THE TABLE. {LEFT, RIGHT, FULL, INNER control} × {ON = equality + residual,
// ON = residual only} × 18 residual spellings, plus a two-residual ON, an
// empty BUILD side and an empty PROBE side per join kind: 156 cells × 5 arms
// = 780 (cell, arm) results. The fixture (arc_jr_on_residual_fixture_test.go)
// puts every match disposition inside each cell — a probe row with no
// candidate, one whose chain is partially accepted, one whose chain is wholly
// rejected and is therefore PADDED rather than dropped, duplicate keys on both
// sides, NULL keys and NULL values on both sides, and a build row no probe row
// matches for the RIGHT/FULL unmatched flush.
//
// WHAT MOVED. At 563aa517, 95 of these 156 cells were a plan REFUSAL and 12
// answered a WRONG ROW SET in silence: `IS DISTINCT FROM` and `IS NOT DISTINCT
// FROM` parse to a comparison whose operator the old interpreter's operator
// switch did not carry, so it returned SQL UNKNOWN for every candidate pair,
// every candidate was rejected, and a LEFT JOIN answered its whole probe side
// NULL-padded where PostgreSQL matches. That was on all three outer kinds and
// on every arm — `engine` under the arm rule. The remaining 49 cells were
// right at the base and are unchanged here, which is the "no new refusal on a
// base-right shape" half of the definition of done.
//
// THE ORACLE. Every cell's answer is PostgreSQL 17.11's, measured live
// (`--locale=C`, `COLLATE "C"`). Fourteen cells are the one place PostgreSQL
// cannot be asked directly: it REFUSES `FULL JOIN ... ON <non-equi>` with
// "FULL JOIN is only supported with merge-joinable or hash-joinable join
// conditions". A full outer join is still DEFINED there, and definable —
// `(l LEFT JOIN r ON p) UNION ALL (r WHERE NOT EXISTS (l WHERE p))` — and all
// fourteen agree with that. PG-rejects-but-we-answer is the superset class
// ADR-0012 keeps; the cells are marked `PG refuses` below.
//
// EXCLUDED DIMENSIONS, and why (the reviewer starts there):
//
//   - NULL ORDERING. Every cell orders by COALESCE(id, -1) so that where a
//     padded NULL sorts is not one of this table's questions. ADR-0012's
//     divergence list owns that, and `wadjet` orders it identically anyway.
//   - THE 22 DATA TYPES. A residual is an EXPRESSION, and which types an
//     expression may be written over is the expression compiler's question,
//     not the join's — the combined row this evaluator builds is typed from
//     the source column's own declaration, so a type it could get wrong is a
//     type the compiler gets wrong everywhere. `wadjet.TestTypeMatrix*` and
//     ADR-0024 own that; the ROW-FIELD path through a residual, which IS
//     join-specific (#769), is pinned in physical's own gate instead.
//   - SEMI / ANTI joins. Their ON residual is SemiAntiFilter, a different
//     seam with different unmatched semantics, and neither issue names it.
//   - THE WIRE. The padded row's DECLARATION is pgwire's
//     TestJRThePaddedRowDeclaresItsBuildSideType, on both format codes.
//   - A SUBQUERY IN ON. Refused, loudly, and named as such by the refusal —
//     physical.TestBuildJoinResidualFilterRefusesWhatItCannotEvaluate pins
//     that boundary. Making it evaluable is a different capability (a
//     per-candidate subquery runner) and is recorded as a residue.
//
// A pin that starts agreeing FAILS. Deleting it is the fix's proof.
func TestJRAOuterJoinOnResidualsAgreeOnFiveArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the ON-residual table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	t.Cleanup(cancel)
	c1Run(t, c1Arms(t, ctx), jrCells())
}

func jrCells() []c1Case {
	return []c1Case{
		// ---- LEFT
		{
			name: "left/eqres/fn",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 1,102,15 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/cast",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND CAST(l.n AS VARCHAR) = CAST(r.n AS VARCHAR) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/like",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND r.s LIKE l.s || '%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/likeconst",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND r.s LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,101,10 | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/between",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND l.n BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 1,102,15 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61",
		},
		{
			name: "left/eqres/inlist",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND r.s IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,101,10 | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,104,61",
		},
		{
			name: "left/eqres/inexpr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND l.n IN (r.n, r.n + 10) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,101,10 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/case",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND CASE WHEN l.n > 15 THEN r.n > 5 ELSE r.n < 100 END ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=8 | 1,101,10 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61",
		},
		{
			name: "left/eqres/distinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND l.s IS DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61",
		},
		{
			name: "left/eqres/nullres",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND r.s > l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,NULL,NULL | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/arith",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND l.n < r.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,102,15 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,104,61",
		},
		{
			name: "left/eqres/notdistinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND l.s IS NOT DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/notlike",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND r.s NOT LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,102,15 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61",
		},
		{
			name: "left/eqres/notin",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND r.s NOT IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,102,15 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/notbetween",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND l.n NOT BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,NULL,NULL | 2,101,10 | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/coalesce",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND COALESCE(r.s, 'zz') = COALESCE(l.s, 'zz') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/substr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND SUBSTR(r.s, 1, 1) = SUBSTR(l.s, 1, 1) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/eqres/strand",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND r.s <> ' AND ' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=8 | 1,101,10 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61",
		},
		{
			name: "left/resonly/fn",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 1,102,15 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/resonly/cast",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON CAST(l.n AS VARCHAR) = CAST(r.n AS VARCHAR) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/resonly/like",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON r.s LIKE l.s || '%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/resonly/likeconst",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON r.s LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,101,10 | 3,101,10 | 4,101,10 | 5,101,10 | 6,101,10",
		},
		{
			name: "left/resonly/between",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.n BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 1,102,15 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61",
		},
		{
			name: "left/resonly/inlist",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON r.s IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=12 | 1,101,10 | 1,104,61 | 2,101,10 | 2,104,61 | 3,101,10 | 3,104,61 | 4,101,10 | 4,104,61 | 5,101,10 | 5,104,61 | 6,101,10 | 6,104,61",
		},
		{
			name: "left/resonly/inexpr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.n IN (r.n, r.n + 10) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,101,10 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/resonly/case",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON CASE WHEN l.n > 15 THEN r.n > 5 ELSE r.n < 100 END ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=30 | 1,101,10 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "left/resonly/distinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.s IS DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=34 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 1,106,NULL | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 2,106,NULL | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 3,106,NULL | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 4,106,NULL | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 5,106,NULL | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "left/resonly/nullres",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON r.s > l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=16 | 1,103,30 | 1,104,61 | 1,105,70 | 2,103,30 | 2,104,61 | 2,105,70 | 3,103,30 | 3,104,61 | 3,105,70 | 4,103,30 | 4,104,61 | 4,105,70 | 5,103,30 | 5,104,61 | 5,105,70 | 6,NULL,NULL",
		},
		{
			name: "left/resonly/arith",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.n < r.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=14 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,103,30 | 2,104,61 | 2,105,70 | 3,104,61 | 3,105,70 | 4,NULL,NULL | 5,104,61 | 5,105,70 | 6,104,61 | 6,105,70",
		},
		{
			name: "left/resonly/notdistinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.s IS NOT DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,106,NULL",
		},
		{
			name: "left/resonly/notlike",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON r.s NOT LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=24 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "left/resonly/notin",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON r.s NOT IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=18 | 1,102,15 | 1,103,30 | 1,105,70 | 2,102,15 | 2,103,30 | 2,105,70 | 3,102,15 | 3,103,30 | 3,105,70 | 4,102,15 | 4,103,30 | 4,105,70 | 5,102,15 | 5,103,30 | 5,105,70 | 6,102,15 | 6,103,30 | 6,105,70",
		},
		{
			name: "left/resonly/notbetween",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.n NOT BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=21 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,104,61 | 3,105,70 | 4,NULL,NULL | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,105,70",
		},
		{
			name: "left/resonly/coalesce",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON COALESCE(r.s, 'zz') = COALESCE(l.s, 'zz') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,106,NULL",
		},
		{
			name: "left/resonly/substr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON SUBSTR(r.s, 1, 1) = SUBSTR(l.s, 1, 1) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/resonly/strand",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON r.s <> ' AND ' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=30 | 1,101,10 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "left/tworesid",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s AND r.n >= l.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 1,102,15 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
		},
		{
			name: "left/emptybuild",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l LEFT JOIN jr_e r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,NULL,NULL | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
			pin:  jrEmptyRelationPin(1),
			why:  jrEmptyRelationWhy,
		},
		{
			name: "left/emptyprobe",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_e l LEFT JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=0",
			pin:  jrEmptyRelationPin(0),
			why:  jrEmptyRelationWhy,
		},
		// ---- RIGHT
		{
			name: "right/eqres/fn",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 1,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/cast",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND CAST(l.n AS VARCHAR) = CAST(r.n AS VARCHAR) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 3,103,30 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/like",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND r.s LIKE l.s || '%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 3,103,30 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/likeconst",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND r.s LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 2,101,10 | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/between",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND l.n BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 1,102,15 | 2,102,15 | 3,103,30 | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/inlist",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND r.s IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 2,101,10 | 6,104,61 | NULL,102,15 | NULL,103,30 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/inexpr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND l.n IN (r.n, r.n + 10) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 2,101,10 | 3,103,30 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/case",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND CASE WHEN l.n > 15 THEN r.n > 5 ELSE r.n < 100 END ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=8 | 1,101,10 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/distinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND l.s IS DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/nullres",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND r.s > l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 3,103,30 | NULL,101,10 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/arith",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND l.n < r.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,102,15 | 6,104,61 | NULL,101,10 | NULL,103,30 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/notdistinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND l.s IS NOT DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/notlike",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND r.s NOT LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,102,15 | 2,102,15 | 3,103,30 | 6,104,61 | NULL,101,10 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/notin",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND r.s NOT IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,102,15 | 2,102,15 | 3,103,30 | NULL,101,10 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/notbetween",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND l.n NOT BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 2,101,10 | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/coalesce",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND COALESCE(r.s, 'zz') = COALESCE(l.s, 'zz') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/substr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND SUBSTR(r.s, 1, 1) = SUBSTR(l.s, 1, 1) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 3,103,30 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/eqres/strand",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND r.s <> ' AND ' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=8 | 1,101,10 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/fn",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 1,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/cast",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON CAST(l.n AS VARCHAR) = CAST(r.n AS VARCHAR) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 3,103,30 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/like",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON r.s LIKE l.s || '%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 3,103,30 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/likeconst",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON r.s LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,101,10 | 2,101,10 | 3,101,10 | 4,101,10 | 5,101,10 | 6,101,10 | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/between",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.n BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 1,102,15 | 2,102,15 | 3,103,30 | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/inlist",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON r.s IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=16 | 1,101,10 | 1,104,61 | 2,101,10 | 2,104,61 | 3,101,10 | 3,104,61 | 4,101,10 | 4,104,61 | 5,101,10 | 5,104,61 | 6,101,10 | 6,104,61 | NULL,102,15 | NULL,103,30 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/inexpr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.n IN (r.n, r.n + 10) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=7 | 1,101,10 | 2,101,10 | 3,103,30 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/case",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON CASE WHEN l.n > 15 THEN r.n > 5 ELSE r.n < 100 END ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=31 | 1,101,10 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/distinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.s IS DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=34 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 1,106,NULL | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 2,106,NULL | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 3,106,NULL | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 4,106,NULL | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 5,106,NULL | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "right/resonly/nullres",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON r.s > l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=18 | 1,103,30 | 1,104,61 | 1,105,70 | 2,103,30 | 2,104,61 | 2,105,70 | 3,103,30 | 3,104,61 | 3,105,70 | 4,103,30 | 4,104,61 | 4,105,70 | 5,103,30 | 5,104,61 | 5,105,70 | NULL,101,10 | NULL,102,15 | NULL,106,NULL",
		},
		{
			name: "right/resonly/arith",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.n < r.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=15 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,103,30 | 2,104,61 | 2,105,70 | 3,104,61 | 3,105,70 | 5,104,61 | 5,105,70 | 6,104,61 | 6,105,70 | NULL,101,10 | NULL,106,NULL",
		},
		{
			name: "right/resonly/notdistinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.s IS NOT DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 6,106,NULL | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70",
		},
		{
			name: "right/resonly/notlike",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON r.s NOT LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=26 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70 | NULL,101,10 | NULL,106,NULL",
		},
		{
			name: "right/resonly/notin",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON r.s NOT IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=21 | 1,102,15 | 1,103,30 | 1,105,70 | 2,102,15 | 2,103,30 | 2,105,70 | 3,102,15 | 3,103,30 | 3,105,70 | 4,102,15 | 4,103,30 | 4,105,70 | 5,102,15 | 5,103,30 | 5,105,70 | 6,102,15 | 6,103,30 | 6,105,70 | NULL,101,10 | NULL,104,61 | NULL,106,NULL",
		},
		{
			name: "right/resonly/notbetween",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.n NOT BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=21 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,104,61 | 3,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/coalesce",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON COALESCE(r.s, 'zz') = COALESCE(l.s, 'zz') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 6,106,NULL | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70",
		},
		{
			name: "right/resonly/substr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON SUBSTR(r.s, 1, 1) = SUBSTR(l.s, 1, 1) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 3,103,30 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/resonly/strand",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON r.s <> ' AND ' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=31 | 1,101,10 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70 | NULL,106,NULL",
		},
		{
			name: "right/tworesid",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s AND r.n >= l.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 1,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "right/emptybuild",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l RIGHT JOIN jr_e r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=0",
			pin:  jrEmptyRelationPin(1),
			why:  jrEmptyRelationWhy,
		},
		{
			name: "right/emptyprobe",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_e l RIGHT JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | NULL,101,10 | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
			pin:  jrEmptyRelationPin(0),
			why:  jrEmptyRelationWhy,
		},
		// ---- FULL
		{
			name: "full/eqres/fn",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,101,10 | 1,102,15 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/cast",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND CAST(l.n AS VARCHAR) = CAST(r.n AS VARCHAR) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/like",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND r.s LIKE l.s || '%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/likeconst",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND r.s LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,101,10 | 2,101,10 | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/between",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND l.n BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=9 | 1,101,10 | 1,102,15 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/inlist",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND r.s IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,101,10 | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,104,61 | NULL,102,15 | NULL,103,30 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/inexpr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND l.n IN (r.n, r.n + 10) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,101,10 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/case",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND CASE WHEN l.n > 15 THEN r.n > 5 ELSE r.n < 100 END ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/distinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND l.s IS DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=9 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/nullres",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND r.s > l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,NULL,NULL | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,101,10 | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/arith",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND l.n < r.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,102,15 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,104,61 | NULL,101,10 | NULL,103,30 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/notdistinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND l.s IS NOT DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,101,10 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/notlike",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND r.s NOT LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=9 | 1,102,15 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61 | NULL,101,10 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/notin",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND r.s NOT IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,102,15 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,101,10 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/notbetween",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND l.n NOT BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,NULL,NULL | 2,101,10 | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/coalesce",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND COALESCE(r.s, 'zz') = COALESCE(l.s, 'zz') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,101,10 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/substr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND SUBSTR(r.s, 1, 1) = SUBSTR(l.s, 1, 1) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/eqres/strand",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND r.s <> ' AND ' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/resonly/fn",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,101,10 | 1,102,15 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/resonly/cast",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON CAST(l.n AS VARCHAR) = CAST(r.n AS VARCHAR) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/like",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON r.s LIKE l.s || '%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/likeconst",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON r.s LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,101,10 | 2,101,10 | 3,101,10 | 4,101,10 | 5,101,10 | 6,101,10 | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/between",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.n BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=9 | 1,101,10 | 1,102,15 | 2,102,15 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/inlist",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON r.s IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=16 | 1,101,10 | 1,104,61 | 2,101,10 | 2,104,61 | 3,101,10 | 3,104,61 | 4,101,10 | 4,104,61 | 5,101,10 | 5,104,61 | 6,101,10 | 6,104,61 | NULL,102,15 | NULL,103,30 | NULL,105,70 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/inexpr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.n IN (r.n, r.n + 10) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,101,10 | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/case",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON CASE WHEN l.n > 15 THEN r.n > 5 ELSE r.n < 100 END ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=31 | 1,101,10 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/distinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.s IS DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=34 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 1,106,NULL | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 2,106,NULL | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 3,106,NULL | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 4,106,NULL | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 5,106,NULL | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/nullres",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON r.s > l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=19 | 1,103,30 | 1,104,61 | 1,105,70 | 2,103,30 | 2,104,61 | 2,105,70 | 3,103,30 | 3,104,61 | 3,105,70 | 4,103,30 | 4,104,61 | 4,105,70 | 5,103,30 | 5,104,61 | 5,105,70 | 6,NULL,NULL | NULL,101,10 | NULL,102,15 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/arith",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.n < r.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=16 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,103,30 | 2,104,61 | 2,105,70 | 3,104,61 | 3,105,70 | 4,NULL,NULL | 5,104,61 | 5,105,70 | 6,104,61 | 6,105,70 | NULL,101,10 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/notdistinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.s IS NOT DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,106,NULL | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/notlike",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON r.s NOT LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=26 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70 | NULL,101,10 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/notin",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON r.s NOT IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=21 | 1,102,15 | 1,103,30 | 1,105,70 | 2,102,15 | 2,103,30 | 2,105,70 | 3,102,15 | 3,103,30 | 3,105,70 | 4,102,15 | 4,103,30 | 4,105,70 | 5,102,15 | 5,103,30 | 5,105,70 | 6,102,15 | 6,103,30 | 6,105,70 | NULL,101,10 | NULL,104,61 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/notbetween",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.n NOT BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=22 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,104,61 | 3,105,70 | 4,NULL,NULL | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,105,70 | NULL,106,NULL",
		},
		{
			name: "full/resonly/coalesce",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON COALESCE(r.s, 'zz') = COALESCE(l.s, 'zz') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,106,NULL | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70",
		},
		{
			name: "full/resonly/substr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON SUBSTR(r.s, 1, 1) = SUBSTR(l.s, 1, 1) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=10 | 1,101,10 | 2,NULL,NULL | 3,103,30 | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,102,15 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			// PG refuses to EXECUTE this shape ("FULL JOIN is only supported
			// with merge-joinable or hash-joinable join conditions"); the answer
			// is the one it DEFINES, measured through the LEFT JOIN plus the
			// build rows no probe row satisfies. Superset, ADR-0012.
			name: "full/resonly/strand",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON r.s <> ' AND ' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=31 | 1,101,10 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70 | NULL,106,NULL",
		},
		{
			name: "full/tworesid",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s AND r.n >= l.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=11 | 1,101,10 | 1,102,15 | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
		},
		{
			name: "full/emptybuild",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l FULL JOIN jr_e r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,NULL,NULL | 2,NULL,NULL | 3,NULL,NULL | 4,NULL,NULL | 5,NULL,NULL | 6,NULL,NULL",
			pin:  jrEmptyRelationPin(1),
			why:  jrEmptyRelationWhy,
		},
		{
			name: "full/emptyprobe",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_e l FULL JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | NULL,101,10 | NULL,102,15 | NULL,103,30 | NULL,104,61 | NULL,105,70 | NULL,106,NULL",
			pin:  jrEmptyRelationPin(0),
			why:  jrEmptyRelationWhy,
		},
		// ---- INNER
		{
			name: "inner/eqres/fn",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 1,102,15",
		},
		{
			name: "inner/eqres/cast",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND CAST(l.n AS VARCHAR) = CAST(r.n AS VARCHAR) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 3,103,30",
		},
		{
			name: "inner/eqres/like",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND r.s LIKE l.s || '%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 3,103,30",
		},
		{
			name: "inner/eqres/likeconst",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND r.s LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 2,101,10",
		},
		{
			name: "inner/eqres/between",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND l.n BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=5 | 1,101,10 | 1,102,15 | 2,102,15 | 3,103,30 | 6,104,61",
		},
		{
			name: "inner/eqres/inlist",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND r.s IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=3 | 1,101,10 | 2,101,10 | 6,104,61",
		},
		{
			name: "inner/eqres/inexpr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND l.n IN (r.n, r.n + 10) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=3 | 1,101,10 | 2,101,10 | 3,103,30",
		},
		{
			name: "inner/eqres/case",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND CASE WHEN l.n > 15 THEN r.n > 5 ELSE r.n < 100 END ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 6,104,61",
		},
		{
			name: "inner/eqres/distinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND l.s IS DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=5 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 6,104,61",
		},
		{
			name: "inner/eqres/nullres",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND r.s > l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=1 | 3,103,30",
		},
		{
			name: "inner/eqres/arith",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND l.n < r.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,102,15 | 6,104,61",
		},
		{
			name: "inner/eqres/notdistinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND l.s IS NOT DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=1 | 1,101,10",
		},
		{
			name: "inner/eqres/notlike",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND r.s NOT LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=4 | 1,102,15 | 2,102,15 | 3,103,30 | 6,104,61",
		},
		{
			name: "inner/eqres/notin",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND r.s NOT IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=3 | 1,102,15 | 2,102,15 | 3,103,30",
		},
		{
			name: "inner/eqres/notbetween",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND l.n NOT BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=1 | 2,101,10",
		},
		{
			name: "inner/eqres/coalesce",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND COALESCE(r.s, 'zz') = COALESCE(l.s, 'zz') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=1 | 1,101,10",
		},
		{
			name: "inner/eqres/substr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND SUBSTR(r.s, 1, 1) = SUBSTR(l.s, 1, 1) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 3,103,30",
		},
		{
			name: "inner/eqres/strand",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND r.s <> ' AND ' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 1,102,15 | 2,101,10 | 2,102,15 | 3,103,30 | 6,104,61",
		},
		{
			name: "inner/resonly/fn",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 1,102,15",
		},
		{
			name: "inner/resonly/cast",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON CAST(l.n AS VARCHAR) = CAST(r.n AS VARCHAR) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 3,103,30",
		},
		{
			name: "inner/resonly/like",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON r.s LIKE l.s || '%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 3,103,30",
		},
		{
			name: "inner/resonly/likeconst",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON r.s LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=6 | 1,101,10 | 2,101,10 | 3,101,10 | 4,101,10 | 5,101,10 | 6,101,10",
		},
		{
			name: "inner/resonly/between",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.n BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=5 | 1,101,10 | 1,102,15 | 2,102,15 | 3,103,30 | 6,104,61",
		},
		{
			name: "inner/resonly/inlist",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON r.s IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=12 | 1,101,10 | 1,104,61 | 2,101,10 | 2,104,61 | 3,101,10 | 3,104,61 | 4,101,10 | 4,104,61 | 5,101,10 | 5,104,61 | 6,101,10 | 6,104,61",
		},
		{
			name: "inner/resonly/inexpr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.n IN (r.n, r.n + 10) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=3 | 1,101,10 | 2,101,10 | 3,103,30",
		},
		{
			name: "inner/resonly/case",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON CASE WHEN l.n > 15 THEN r.n > 5 ELSE r.n < 100 END ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=30 | 1,101,10 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "inner/resonly/distinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.s IS DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=34 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 1,106,NULL | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 2,106,NULL | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 3,106,NULL | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 4,106,NULL | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 5,106,NULL | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "inner/resonly/nullres",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON r.s > l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,103,30 | 2,104,61 | 2,105,70 | 3,103,30 | 3,104,61 | 3,105,70 | 4,103,30 | 4,104,61 | 4,105,70 | 5,103,30 | 5,104,61 | 5,105,70",
		},
		{
			name: "inner/resonly/arith",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.n < r.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=13 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,103,30 | 2,104,61 | 2,105,70 | 3,104,61 | 3,105,70 | 5,104,61 | 5,105,70 | 6,104,61 | 6,105,70",
		},
		{
			name: "inner/resonly/notdistinct",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.s IS NOT DISTINCT FROM r.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 6,106,NULL",
		},
		{
			name: "inner/resonly/notlike",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON r.s NOT LIKE 'a%' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=24 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "inner/resonly/notin",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON r.s NOT IN ('alpha', 'zeta') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=18 | 1,102,15 | 1,103,30 | 1,105,70 | 2,102,15 | 2,103,30 | 2,105,70 | 3,102,15 | 3,103,30 | 3,105,70 | 4,102,15 | 4,103,30 | 4,105,70 | 5,102,15 | 5,103,30 | 5,105,70 | 6,102,15 | 6,103,30 | 6,105,70",
		},
		{
			name: "inner/resonly/notbetween",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.n NOT BETWEEN r.n - 5 AND r.n + 5 ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=20 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,104,61 | 3,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,105,70",
		},
		{
			name: "inner/resonly/coalesce",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON COALESCE(r.s, 'zz') = COALESCE(l.s, 'zz') ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 6,106,NULL",
		},
		{
			name: "inner/resonly/substr",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON SUBSTR(r.s, 1, 1) = SUBSTR(l.s, 1, 1) ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 3,103,30",
		},
		{
			name: "inner/resonly/strand",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON r.s <> ' AND ' ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=30 | 1,101,10 | 1,102,15 | 1,103,30 | 1,104,61 | 1,105,70 | 2,101,10 | 2,102,15 | 2,103,30 | 2,104,61 | 2,105,70 | 3,101,10 | 3,102,15 | 3,103,30 | 3,104,61 | 3,105,70 | 4,101,10 | 4,102,15 | 4,103,30 | 4,104,61 | 4,105,70 | 5,101,10 | 5,102,15 | 5,103,30 | 5,104,61 | 5,105,70 | 6,101,10 | 6,102,15 | 6,103,30 | 6,104,61 | 6,105,70",
		},
		{
			name: "inner/tworesid",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s AND r.n >= l.n ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=2 | 1,101,10 | 1,102,15",
		},
		{
			name: "inner/emptybuild",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_l l JOIN jr_e r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=0",
			pin:  jrEmptyRelationPin(0),
			why:  jrEmptyRelationWhy,
		},
		{
			name: "inner/emptyprobe",
			sql:  "SELECT l.id AS a, r.id AS b, r.n AS rn FROM jr_e l JOIN jr_r r ON l.k = r.k AND LOWER(r.s) = l.s ORDER BY 1, 2, 3",
			want: "cols=[a:INT64 b:INT64 rn:INT64] rows=0",
			pin:  jrEmptyRelationPin(0),
			why:  jrEmptyRelationWhy,
		},
	}
}

// jrEmptyRelationPin is the DAG arms' answer for every cell whose join has an
// EMPTY relation on one side, pinned per arm with its mechanism.
//
// It is NOT about the ON clause, and this is the measurement that says so: on
// the same three arms, `SELECT id FROM jr_e ORDER BY 1` and
// `SELECT COUNT(*) FROM jr_e` — no join, no ON, no residual — fail with the
// identical sentence. A relation whose catalog entry lists no FILES produces a
// scan stage with no ScanFiles and no dependencies, and the stage planner has
// nothing to dispatch it with; the two single-process arms answer the rows
// PostgreSQL answers. Pre-existing at 563aa517, `distributed` under the arm
// rule, and NOT chased here (engine-first, Derek 2026-09-16) — recorded as a
// filing candidate in the arc's landing notes instead.
//
// A pin that starts agreeing FAILS: when the empty relation gets a
// distributed scan, these eight cells are the proof, and deleting the pin is
// how it is claimed.
func jrEmptyRelationPin(stage int) map[string]string {
	want := "ERR stage scan-" + string(rune('0'+stage)) + " has no dependencies and no ScanFiles"
	return map[string]string{"dag": want, "dag-shuffled": want, "dag-morsel4": want}
}

const jrEmptyRelationWhy = "a relation with no files has no distributed scan stage; " +
	"`SELECT id FROM jr_e` fails identically on these three arms with no join in the query at all"
