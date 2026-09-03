package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// #710 on BOTH execution paths: `= ANY(…)`, `<> ALL(…)` and row-value
// comparison.
//
// Neither spelling compiled. `compileWithCtx`'s `default:` arm turned any
// node type it had no case for into `Lit{node.String()}` — the expression's
// own SQL text as a string constant — and it had no case for AnyAllExpr or
// TupleNode. Six spellings answered ZERO ROWS on the ordinary query path,
// silently, and every DML door inherited it.
//
// This is a QUERY-PATH defect, so ADR-0013's two-path rule applies: the fix
// is in the expression compiler, which both arms call, but the stage DAG
// SERIALIZES a filter expression and re-parses it on the worker, so a
// spelling that survives compilation in-process still has to survive that
// round trip. "Same file on both paths" is not evidence; a query result on
// both paths is.
//
// The comparison is against the single-process arm AND against a nonzero
// count, because #710's wrong answer was zero rows on both arms — two arms
// agreeing on nothing is exactly what this defect looked like.
func TestQuantifiedAndRowValueTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, c := range []struct {
		name string
		sql  string
		// nonEmpty says the answer must contain a row with a nonzero count —
		// the assertion the two-path comparison alone cannot make.
		nonEmpty bool
	}{
		{name: "= ANY array", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE id = ANY(ARRAY[1, 2, 3])`},
		{name: "= SOME array", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE id = SOME(ARRAY[1, 2, 3])`},
		{name: "<> ALL array", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE id <> ALL(ARRAY[1, 2, 3])`},
		{name: "> ANY array", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE id > ANY(ARRAY[1, 2, 3])`},
		{name: "< ALL array", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE id < ALL(ARRAY[4000, 4001])`},
		{name: "= ANY value list", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE id = ANY(1, 2, 3)`},
		{name: "= ANY subquery", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE g = ANY(SELECT k FROM typemx_dim)`},
		{name: "row equality", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE (id, id) = (1, 1)`},
		{name: "row inequality", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE (id, id) <> (1, 1)`},
		{name: "row ordering", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE (id, id) < (3, 3)`},
		{name: "row ordering, first field decides", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE (id, g) < (3, -999999)`},
		{name: "row IN list", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE (id, id) IN ((1, 1), (2, 2))`},
		{name: "row NOT IN list", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE (id, id) NOT IN ((1, 1), (2, 2))`},
		{name: "row comparison of two columns", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE (id, g) = (id, g)`},
		// Projected, not only filtered: a row comparison in the SELECT list
		// goes through the same compiler by a different route.
		{name: "row comparison projected", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE ((id, id) = (1, 1)) OR id = 2`},
		{name: "quantified under NOT", nonEmpty: true,
			sql: `SELECT count(*) AS c FROM typemx WHERE NOT (id = ANY(ARRAY[1, 2, 3]))`},
		{name: "quantified in a grouped query", nonEmpty: true,
			sql: `SELECT g, count(*) AS c FROM typemx WHERE id = ANY(ARRAY[1, 2, 3, 4, 5]) GROUP BY g ORDER BY g`},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			aRes, aErr := tmdRunSingle(ctx, single, c.sql)
			bRes, bErr := tmdRunDAG(ctx, coord, c.sql)
			if aErr != nil {
				t.Fatalf("the single-process engine refused this query: %v\n  SQL: %s", aErr, c.sql)
			}
			if bErr != nil {
				t.Fatalf("the stage DAG refused a query the single-process engine answered (%d rows): %v\n  SQL: %s",
					len(aRes.Rows), bErr, c.sql)
			}
			if diff := oracle.Compare(aRes, bRes, oracle.CompareSpec{Mode: oracle.CmpOrdered}); diff != "" {
				t.Errorf("TWO-PATH DIVERGENCE\n  SQL: %s\n  %s\n  single: %s\n  dag:    %s",
					c.sql, diff, tmdRender(aRes, 4), tmdRender(bRes, 4))
			}
			if c.nonEmpty && countIsZero(aRes) {
				t.Errorf("every count is zero, which is #710's own wrong answer: a predicate that "+
					"compiled to a STRING constant matched nothing.\n  SQL: %s\n  %s",
					c.sql, tmdRender(aRes, 4))
			}
		})
	}
}

// countIsZero reports whether every row's `c` column is zero (or there are no
// rows at all).
func countIsZero(res *oracle.Result) bool {
	if res == nil || len(res.Rows) == 0 {
		return true
	}
	for _, row := range res.Rows {
		switch v := row["c"].(type) {
		case int64:
			if v != 0 {
				return false
			}
		case int32:
			if v != 0 {
				return false
			}
		case float64:
			if v != 0 {
				return false
			}
		default:
			// Anything this helper cannot read is not evidence of zero.
			return false
		}
	}
	return true
}
