// SPDX-License-Identifier: MIT

package expr

import (
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ArraySubquery is PostgreSQL's ARRAY(subquery) constructor: the subquery's
// single column as an array, one element per row in the subquery's row order
// — its ORDER BY, when it has one — and the EMPTY array when it returns no
// rows (never NULL, which is what an aggregate over no rows would answer).
//
// It is a scalar subquery in every respect but the value it forms, so it is
// compiled from the same analysis: an uncorrelated one runs once and is
// cached, and a correlated one is re-run per outer row from the same rebuilt
// text a correlated scalar subquery is (Corr), with the same refusals for a
// body that rebuild cannot write.
type ArraySubquery struct {
	SQL    string
	Runner SubqueryRunner
	Cols   SubqueryColumnsFunc
	Scope  plansql.TableColumns
	// Corr is the correlated analysis, nil for an uncorrelated subquery.
	Corr *CorrelatedScalarSubquery

	once sync.Once
	val  []any
}

func (e *ArraySubquery) Eval(b *batch.RecordBatch, row int) any {
	if e.Corr == nil {
		e.once.Do(func() { e.val = e.run(e.SQL) })
		return e.val
	}
	sql, err := e.Corr.buildSQL(b, row)
	if err != nil {
		failEval(err)
	}
	return e.run(sql)
}

func (e *ArraySubquery) run(sql string) []any {
	if e.Corr == nil {
		refuseDanglingSubquery("ARRAY", sql, e.Scope)
	}
	refuseMultiColumnSubqueryByPlan(e.Cols, sql, false)
	rows, err := e.Runner(sql)
	if err != nil {
		failEval(subqueryRunFailed("ARRAY", sql, err))
	}
	refuseMultiColumnSubquery(sql, rows, false)
	out := make([]any, 0, len(rows))
	for _, r := range rows {
		v, verr := ScalarSubqueryValue(sql, []map[string]any{r})
		if verr != nil {
			failEval(verr)
		}
		out = append(out, v)
	}
	return out
}
