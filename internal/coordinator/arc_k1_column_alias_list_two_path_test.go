package coordinator

import (
	"context"
	"testing"
	"time"
)

// A COLUMN-ALIAS LIST IS A POSITIONAL RENAME — #958, on FOUR arms against live
// PostgreSQL 17 over rows identical to `decpair` and `lat_ord`.
//
// PostgreSQL renames the LEADING columns and the rest keep their own names:
// `WITH c(kk) AS (SELECT id, s FROM decpair)` publishes `kk` AND `s`. Three of
// the six readers of such a list treated it as the WHOLE namespace, and the
// two that mattered most were the binder (`registerCTE` stored `cte.Columns`
// outright) and the correlation classifier (`plansql.CTEColumns` returned it
// outright). So `s` was not an inner name:
//
//   - at top level it was `42703 unknown column "s" (available: kk)`, for a
//     query PostgreSQL answers;
//   - inside a subquery it bound the ENCLOSING query, the subquery was
//     re-run per outer row with the value substituted, and the answer was
//     `9, 0` where PostgreSQL answers `1, 1`. Silent, on all four arms.
//
// The rule is now in ONE place — `plansql.OverlayColumnAliases`, which the
// derived-table path already implemented — and every reader takes it.
//
// The `SELECT *` body is the fourth reader and ADR-0012's recorded divergence.
// The list could not be applied there because the star's width is a catalog
// question the builder cannot ask; it is DEFERRED to the pass that answers it
// (`logical.ApplyDeferredColumnAliases`, immediately after
// `ExpandStarProjections`) rather than dropped, and the ARITY refusal moves
// with it. Where the expansion declines — a bare star over a join — the
// wrapper is removed and the relation keeps exactly the disposition it had.
func TestArcK1AColumnAliasListRenamesPositionally(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const three = "cols=[kk:INT64 s:STRING] rows=3 | 1,1.50 | 2,1.5 | 3,abc"

	f1Run(t, arms, []f1Case{
		{
			// The column the list did NOT rename is still a column.
			name: "958 a short CTE list keeps the columns it did not rename",
			sql:  `WITH c(kk) AS (SELECT id, s FROM decpair) SELECT kk, s FROM c ORDER BY kk LIMIT 3`,
			want: three,
		},
		{
			// Arc I1's pinned cell 29, deleted there and asserted here: the
			// reference is INNER, so the subquery is uncorrelated and answers
			// once per outer row with the same number.
			name: "958 the same name inside a subquery is the CTE's, not the outer query's",
			sql: `WITH c(kk) AS (SELECT id, s FROM decpair) ` +
				`SELECT (SELECT COUNT(*) FROM c WHERE s = '1.50') AS n FROM decpair d WHERE d.id < 3`,
			want: "cols=[n:INT64] rows=2 | 1 | 1",
		},
		{
			// A SET-OPERATION body publishes its LEFT arm's names, which is
			// where PostgreSQL applies the list too.
			name: "958 a short list over a set-operation body",
			sql: `WITH c(kk) AS (SELECT id, s FROM decpair WHERE id<3 ` +
				`UNION ALL SELECT id, s FROM decpair WHERE id>7) SELECT kk, s FROM c ORDER BY kk`,
			want: "cols=[kk:INT64 s:STRING] rows=4 | 1,1.50 | 2,1.5 | 8,-1 | 9,1.5",
		},
		{
			// ADR-0012's divergence, CLOSED: a list over a `SELECT *` body.
			// It was not merely published under the inner names — the entry
			// understated it — but LOUD, `sort: key column "kk" does not exist
			// in the input schema`, for the very name the list renames to.
			name: "958 a list over a SELECT * body",
			sql:  `WITH c(kk) AS (SELECT * FROM lat_ord) SELECT * FROM c ORDER BY kk`,
			want: "cols=[kk:INT64 customer:STRING total:FLOAT64] rows=3 | " +
				"1,Alice,150 | 2,Bob,200 | 3,Carol,0",
		},
		{
			// The DERIVED-TABLE spelling of the same, which had the same gap:
			// `applyColumnAliases` declines over a star exactly as the CTE
			// applier does.
			name: "958 the derived-table spelling of a list over a SELECT * body",
			sql:  `SELECT * FROM (SELECT * FROM lat_ord) t(kk) ORDER BY kk`,
			want: "cols=[kk:INT64 customer:STRING total:FLOAT64] rows=3 | " +
				"1,Alice,150 | 2,Bob,200 | 3,Carol,0",
		},
		{
			// The ARITY refusal moves with the rename: PostgreSQL 17 says
			// `WITH query "c" has 3 columns available but 4 columns specified`
			// over a star body too, and the width is only countable after the
			// expansion — so the refusal is raised there.
			name: "958 an overlong list over a SELECT * body is 42P10",
			sql:  `WITH c(p,q,r,s2) AS (SELECT * FROM lat_ord) SELECT * FROM c`,
			want: `ERR WITH query "c" has 3 columns available but 4 columns specified`,
		},
		{
			name: "958 ctl an overlong list over a named body is 42P10",
			sql:  `WITH c(kk,ss,tt) AS (SELECT id, s FROM decpair) SELECT * FROM c`,
			want: `ERR WITH query "c" has 2 columns available but 3 columns specified`,
		},
		{
			name: "958 ctl the derived-table prefix rule, right before this arc",
			sql:  `SELECT kk, s FROM (SELECT id, s FROM decpair) t(kk) ORDER BY kk LIMIT 3`,
			want: three,
		},
		{
			name: "958 ctl an overlong derived-table list is 42P10",
			sql:  `SELECT * FROM (SELECT id, s FROM decpair) t(kk,ss,tt)`,
			want: `ERR table "t" has 2 columns available but 3 columns specified`,
		},
		{
			// THE BOUNDARY, attempted from the side the deferral does NOT
			// reach: a bare star over a JOIN, whose column set
			// `ExpandStarProjections` refuses to guess (ADR-0012, #810). The
			// list cannot be applied truthfully, so it is REFUSED in one
			// sentence rather than dropped — at bb8635a4 it was dropped, and
			// every reference to a name it renames TO read NULL on all four
			// arms for a query PostgreSQL answers. Silent-wrong to loud.
			name: "958 boundary a list over a star the expansion declines",
			sql: `SELECT kk FROM (SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id) ` +
				`t(kk)`,
			want: "ERR building physical plan: table \"t\" renames columns of a `SELECT *`",
			pin: map[string]string{
				"dag":     "ERR physical plan: table \"t\" renames columns of a `SELECT *`",
				"dagshuf": "ERR physical plan: table \"t\" renames columns of a `SELECT *`",
			},
			why: "ONE refusal under the two engines' own error prefixes, not a divergence: " +
				"PostgreSQL publishes `kk` and the join's remaining six columns, the " +
				"expansion declines a bare star over a join (ADR-0012, #810), and the " +
				"list cannot be applied truthfully",
		},
	})
}
