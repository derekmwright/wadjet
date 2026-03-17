package expr

import (
	"strings"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// CorrelatedScalarSubquery evaluates a correlated scalar subquery per-row.
// Unlike ScalarSubquery, it cannot cache the result because the inner query
// depends on values from the outer row.
type CorrelatedScalarSubquery struct {
	Runner         SubqueryRunner
	OuterRefs      []plansql.OuterRef // correlated column references
	OuterTables    map[string]bool    // outer table aliases
	ParsedInfo     *plansql.SelectInfo
	UnqualOuterCols map[string]string // unqualified column → table mapping for outer refs
}

func (e *CorrelatedScalarSubquery) Eval(b *batch.RecordBatch, row int) any {
	sql := e.buildSQL(b, row)
	rows, err := e.Runner(sql)
	if err != nil || len(rows) == 0 {
		return nil
	}
	for _, v := range rows[0] {
		return v
	}
	return nil
}

func (e *CorrelatedScalarSubquery) buildSQL(b *batch.RecordBatch, row int) string {
	vals := readOuterValues(b, row, e.OuterRefs)
	rewrittenWhere := plansql.RewriteOuterRefs(e.ParsedInfo.WhereExpr, e.OuterTables, vals)
	if len(e.UnqualOuterCols) > 0 {
		rewrittenWhere = plansql.RewriteUnqualifiedOuterRefs(rewrittenWhere, e.UnqualOuterCols, vals)
	}
	return plansql.RebuildSQL(e.ParsedInfo, rewrittenWhere)
}

// CorrelatedInSubquery checks if a value is in the result set of a correlated subquery.
type CorrelatedInSubquery struct {
	Expr            Expr
	Runner          SubqueryRunner
	Not             bool
	OuterRefs       []plansql.OuterRef
	OuterTables     map[string]bool
	ParsedInfo      *plansql.SelectInfo
	UnqualOuterCols map[string]string
}

func (e *CorrelatedInSubquery) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *CorrelatedInSubquery) EvalBool(b *batch.RecordBatch, row int) bool {
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false
	}

	sql := e.buildSQL(b, row)
	rows, err := e.Runner(sql)
	if err != nil {
		return e.Not
	}

	for _, r := range rows {
		for _, v := range r {
			if v != nil && compare(lv, v, CmpEq) {
				return !e.Not
			}
			break // first column only
		}
	}
	return e.Not
}

func (e *CorrelatedInSubquery) buildSQL(b *batch.RecordBatch, row int) string {
	vals := readOuterValues(b, row, e.OuterRefs)
	rewrittenWhere := plansql.RewriteOuterRefs(e.ParsedInfo.WhereExpr, e.OuterTables, vals)
	if len(e.UnqualOuterCols) > 0 {
		rewrittenWhere = plansql.RewriteUnqualifiedOuterRefs(rewrittenWhere, e.UnqualOuterCols, vals)
	}
	return plansql.RebuildSQL(e.ParsedInfo, rewrittenWhere)
}

// CorrelatedExistsSubquery evaluates a correlated EXISTS subquery per-row.
type CorrelatedExistsSubquery struct {
	Runner          SubqueryRunner
	Not             bool
	OuterRefs       []plansql.OuterRef
	OuterTables     map[string]bool
	ParsedInfo      *plansql.SelectInfo
	UnqualOuterCols map[string]string
}

func (e *CorrelatedExistsSubquery) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *CorrelatedExistsSubquery) EvalBool(b *batch.RecordBatch, row int) bool {
	sql := e.buildSQL(b, row)
	rows, err := e.Runner(sql)
	exists := err == nil && len(rows) > 0
	if e.Not {
		return !exists
	}
	return exists
}

func (e *CorrelatedExistsSubquery) buildSQL(b *batch.RecordBatch, row int) string {
	vals := readOuterValues(b, row, e.OuterRefs)
	rewrittenWhere := plansql.RewriteOuterRefs(e.ParsedInfo.WhereExpr, e.OuterTables, vals)
	if len(e.UnqualOuterCols) > 0 {
		rewrittenWhere = plansql.RewriteUnqualifiedOuterRefs(rewrittenWhere, e.UnqualOuterCols, vals)
	}
	return plansql.RebuildSQL(e.ParsedInfo, rewrittenWhere)
}

// readOuterValues reads correlated outer column values from the current batch row.
// Returns a map of "table.column" → value for use with RewriteOuterRefs.
func readOuterValues(b *batch.RecordBatch, row int, refs []plansql.OuterRef) map[string]any {
	vals := make(map[string]any, len(refs))
	for _, ref := range refs {
		// The outer column may be named as just "column" in the batch
		// (table qualifiers are stripped during projection). Try both.
		key := ref.Table + "." + ref.Column
		v := b.ColumnByName(ref.Column)
		if v == nil {
			// Try with table prefix (some queries preserve qualified names)
			v = b.ColumnByName(strings.ReplaceAll(key, ".", "_"))
		}
		if v != nil {
			vals[key] = v.GetValue(row)
		} else {
			vals[key] = nil
		}
	}
	return vals
}
