package expr

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// CorrelatedScalarSubquery evaluates a correlated scalar subquery per-row.
// Unlike ScalarSubquery, it cannot cache the result because the inner query
// depends on values from the outer row.
type CorrelatedScalarSubquery struct {
	Runner          SubqueryRunner
	OuterRefs       []plansql.OuterRef // correlated column references
	OuterTables     map[string]bool    // outer table aliases
	ParsedInfo      *plansql.SelectInfo
	UnqualOuterCols map[string]string // unqualified column → table mapping for outer refs
	// Decl is the DECLARED type of the subquery's single output column, and
	// DeclKnown says whether anything resolved it — ScalarSubquery's fields,
	// for the same reason and read by the same classifyOperand arm (#696,
	// #666). A correlated subquery re-runs per row and its declared TYPE does
	// not change with the row, so it is resolved once at compile time exactly
	// as the uncorrelated one is.
	//
	// Without it `d.a > (SELECT AVG(x.a) FROM decpair x WHERE x.id <> d.id)`
	// compared a DECIMAL column against a boxUnknown operand — that is, by the
	// BYTES of its rendered text — and answered 0 rows for PostgreSQL's 4.
	Decl                   batch.TypeID
	DeclKnown              bool
	DecPrecision, DecScale int
}

func (e *CorrelatedScalarSubquery) Eval(b *batch.RecordBatch, row int) any {
	sql, err := e.buildSQL(b, row)
	if err != nil {
		failEval(err)
	}
	rows, runErr := e.Runner(sql)
	if runErr != nil || len(rows) == 0 {
		return nil
	}
	for _, v := range rows[0] {
		return v
	}
	return nil
}

func (e *CorrelatedScalarSubquery) buildSQL(b *batch.RecordBatch, row int) (string, error) {
	vals, err := readOuterValues(b, row, e.OuterRefs)
	if err != nil {
		return "", err
	}
	rewrittenWhere := plansql.RewriteOuterRefs(e.ParsedInfo.WhereExpr, e.OuterTables, vals)
	if len(e.UnqualOuterCols) > 0 {
		rewrittenWhere = plansql.RewriteUnqualifiedOuterRefs(rewrittenWhere, e.UnqualOuterCols, vals)
	}
	return plansql.RebuildSQL(e.ParsedInfo, rewrittenWhere), nil
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
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *CorrelatedInSubquery) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull carries SQL's three-valued IN (#370): a NULL probe is
// UNKNOWN, and a miss against a result set containing a NULL is UNKNOWN —
// the NOT IN trap, same rule as the uncorrelated InSubquery.
func (e *CorrelatedInSubquery) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false, true
	}

	sql, err := e.buildSQL(b, row)
	if err != nil {
		failEval(err)
	}
	rows, runErr := e.Runner(sql)
	if runErr != nil {
		return e.Not, false
	}

	sawNull := false
	for _, r := range rows {
		for _, v := range r {
			if v == nil {
				sawNull = true
			} else if compare(lv, v, CmpEq) {
				return !e.Not, false
			}
			break // first column only
		}
	}
	if sawNull {
		return false, true
	}
	return e.Not, false
}

func (e *CorrelatedInSubquery) buildSQL(b *batch.RecordBatch, row int) (string, error) {
	vals, err := readOuterValues(b, row, e.OuterRefs)
	if err != nil {
		return "", err
	}
	rewrittenWhere := plansql.RewriteOuterRefs(e.ParsedInfo.WhereExpr, e.OuterTables, vals)
	if len(e.UnqualOuterCols) > 0 {
		rewrittenWhere = plansql.RewriteUnqualifiedOuterRefs(rewrittenWhere, e.UnqualOuterCols, vals)
	}
	return plansql.RebuildSQL(e.ParsedInfo, rewrittenWhere), nil
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
	sql, err := e.buildSQL(b, row)
	if err != nil {
		failEval(err)
	}
	rows, runErr := e.Runner(sql)
	exists := runErr == nil && len(rows) > 0
	if e.Not {
		return !exists
	}
	return exists
}

func (e *CorrelatedExistsSubquery) buildSQL(b *batch.RecordBatch, row int) (string, error) {
	vals, err := readOuterValues(b, row, e.OuterRefs)
	if err != nil {
		return "", err
	}
	rewrittenWhere := plansql.RewriteOuterRefs(e.ParsedInfo.WhereExpr, e.OuterTables, vals)
	if len(e.UnqualOuterCols) > 0 {
		rewrittenWhere = plansql.RewriteUnqualifiedOuterRefs(rewrittenWhere, e.UnqualOuterCols, vals)
	}
	return plansql.RebuildSQL(e.ParsedInfo, rewrittenWhere), nil
}

// readOuterValues reads correlated outer column values from the current batch row.
// Returns a map of "table.column" → value for use with RewriteOuterRefs.
//
// A reference the batch does not carry is an ERROR, never a NULL. Substituting
// NULL made every comparison in the subquery UNKNOWN, so the predicate matched
// nothing and the query answered 0 rows — the exact silent wrong answer of
// issue #347, where column pruning had dropped an outer column because the
// pruning walk did not descend into subqueries. Pruning now keeps those
// columns (plansql.OuterColumnCandidates), and this guard is what makes the
// NEXT pruning change fail loudly instead of quietly answering nothing: a miss
// must error, never skip.
//
// A column that IS present and holds SQL NULL still reads as nil, which is the
// correct answer and unaffected by this.
func readOuterValues(b *batch.RecordBatch, row int, refs []plansql.OuterRef) (map[string]any, error) {
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
		if v == nil {
			// ColumnByName is case-sensitive and correlation analysis
			// lowercases every name it reports, so a mixed-case column
			// ("SearchPhrase") never matches either spelling above.
			v = columnByNameFold(b, ref.Column)
		}
		if v == nil {
			v = columnByNameFold(b, strings.ReplaceAll(key, ".", "_"))
		}
		if v == nil {
			return nil, &MissingOuterColumnError{Ref: ref, Available: batchColumnNames(b)}
		}
		vals[key] = v.GetValue(row)
	}
	return vals, nil
}

// columnByNameFold is ColumnByName with ASCII case folding.
func columnByNameFold(b *batch.RecordBatch, name string) *batch.Vector {
	for i, col := range b.Schema {
		if strings.EqualFold(col.Name, name) {
			return b.Columns[i]
		}
	}
	return nil
}

func batchColumnNames(b *batch.RecordBatch) []string {
	names := make([]string, 0, len(b.Schema))
	for _, col := range b.Schema {
		names = append(names, col.Name)
	}
	return names
}

// MissingOuterColumnError reports a correlated subquery whose outer column is
// absent from the batch the outer query hands it — a planning defect (column
// pruning, projection, or a rename), not a data condition.
type MissingOuterColumnError struct {
	Ref       plansql.OuterRef
	Available []string
}

func (e *MissingOuterColumnError) Error() string {
	return fmt.Sprintf("correlated subquery references outer column %s.%s, which the outer query "+
		"does not carry (batch columns: %s); the outer query must project every column its "+
		"subqueries correlate on",
		e.Ref.Table, e.Ref.Column, strings.Join(e.Available, ", "))
}

// FatalEvalError satisfies the marker the pipeline drivers recover on. Expr's
// Eval/EvalBool have no error return, so a failure that must not be mistaken
// for a NULL travels as a panic carrying this value and is turned back into a
// query error at the pipeline boundary (see exec.FatalEvalPanic).
func (e *MissingOuterColumnError) FatalEvalError() error { return e }

// failEval aborts expression evaluation with err. Only for conditions where
// continuing would produce a wrong ANSWER rather than a NULL — there is no
// error channel through Expr.Eval, and a query that returns the wrong number
// is worse than one that fails.
func failEval(err error) {
	panic(err)
}
