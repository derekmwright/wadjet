// SPDX-License-Identifier: MIT

package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A deferred subquery failure raises its SQLSTATE when a row EVALUATES it
// and never otherwise: under a WHEN no row satisfies, behind an AND whose
// left operand is false, behind an OR whose left is true, or behind a
// COALESCE argument that is not NULL, behind a filter's NULL AND. That is the whole of the rule the stage
// planner relies on (#1411 review r3 B1): PostgreSQL runs an uncorrelated
// sublink when it is first referenced.
func TestDeferredErrorRaisesOnlyWhereARowEvaluatesIt(t *testing.T) {
	const fail = "cast(__deferred_error('2202H', 'sample percentage must be between 0 and 100') as boolean)"
	b := batch.NewRecordBatch([]parquet.Column{{Name: "id", Type: parquet.TypeInt64}}, 7)
	for i := 0; i < 7; i++ {
		b.Columns[0].Int64Data[i] = int64(i + 1)
	}
	compile := func(src string) Expr {
		t.Helper()
		n, err := plansql.ParseExpressionComplete(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		c, err := Compile(n)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		return c
	}
	for _, src := range []string{
		"CASE WHEN id > 5 THEN " + fail + " ELSE true END",
		"CASE WHEN false THEN " + fail + " ELSE true END",
		"CASE WHEN true THEN true ELSE " + fail + " END",
		"id > 5 AND " + fail,
		"id < 6 OR " + fail,
		"COALESCE(id < 9, " + fail + ")",
		// A filter's AND and OR read UNKNOWN as false before they
		// short-circuit, as PostgreSQL's qual canonicalization drops a NULL
		// arm of a top-level WHERE: `id = 1 OR (NULL AND …)` is `id = 1`.
		"id = 1 OR (NULL AND " + fail + ")",
	} {
		pred := FilterPredicate(compile(src))
		for row := 0; row < 5; row++ { // ids 1–5: no row reaches the failure
			pred(b, row)
		}
	}
	for _, src := range []string{
		"CASE WHEN id > 5 THEN " + fail + " ELSE true END",
		"id > 5 AND " + fail,
		fail,
		"NOT " + fail,
	} {
		pred := FilterPredicate(compile(src))
		state, _ := recoverFatalEvalForTest(t, func() { pred(b, 5) }) // id 6 reaches it
		if state != "2202H" {
			t.Errorf("%s over id 6: %q, want 2202H", src, state)
		}
	}
}
