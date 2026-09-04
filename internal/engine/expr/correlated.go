package expr

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
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
	if runErr != nil {
		// NOT NULL. A scalar subquery that could not be run has no value,
		// and NULL is a value — one that makes every comparison above it
		// UNKNOWN and the row silently vanish.
		failEval(&SubqueryRunFailedError{Kind: "scalar", SQL: sql, Err: runErr})
	}
	v, err := ScalarSubqueryValue(sql, rows)
	if err != nil {
		failEval(err)
	}
	return v
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

	sql, err := e.buildSQL(b, row)
	if err != nil {
		failEval(err)
	}
	rows, runErr := e.Runner(sql)
	if runErr != nil {
		// NOT `e.Not`. A membership test whose set could not be built has no
		// answer, and returning "not a member" is the third of the three
		// different wrong answers these evaluators gave to one event.
		failEval(&SubqueryRunFailedError{Kind: "IN", SQL: sql, Err: runErr})
	}

	// The EMPTY set decides before the probe's own NULL does. `x IN ()` is
	// FALSE and `x NOT IN ()` is TRUE for EVERY row, a NULL-keyed one
	// included: both rules of the three-valued reading are about a
	// COMPARISON, and over an empty set there is nothing to compare — the
	// same edge `exec.HashJoin`'s null-aware anti join guards with
	// `buildRows > 0` (#507). Reading the probe's NULL first answered UNKNOWN
	// there and dropped the row: 36 rows for PostgreSQL's 40 on a correlated
	// NOT IN whose every group is empty (#538/#578).
	if len(rows) == 0 {
		return e.Not, false
	}
	if lv == nil {
		return false, true
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
	if runErr != nil {
		// NOT "does not exist". `runErr == nil && len(rows) > 0` read a
		// failure as FALSE, so a re-run that raised — #679's quoted DECIMAL
		// against a BIGINT raises 22P02 — answered a confident 0 rows for
		// PostgreSQL's 3, and its NOT EXISTS twin answered every row.
		failEval(&SubqueryRunFailedError{Kind: "EXISTS", SQL: sql, Err: runErr})
	}
	exists := len(rows) > 0
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
		// The LITERAL, not the box. The box has lost the column's wadjet type
		// for half the type system — a DECIMAL boxes as its rendered text, a
		// DATE as a formatted string, a TIMESTAMP as a bare int64 — and a
		// renderer that reads the box re-types the value by what it looks
		// like: `a.w_d2 = b.k` became `'2.00' = b.k` and raised 22P02 for a
		// query PostgreSQL answers (#679). See outer_literal.go.
		lit, err := outerLiteral(v, row)
		if err != nil {
			return nil, err
		}
		vals[key] = lit
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

// SQLState is PostgreSQL's 42703 (undefined_column).
//
// The two ways to reach this error are a PLANNING defect — the outer query
// pruned a column its subquery correlates on — and a reference to a column
// the outer relation simply does not have, which is what a client sees when
// it writes `WHERE EXISTS (… WHERE s.id = t.nosuchcol)`. PostgreSQL answers
// the second with 42703, and a client cannot tell the two apart from the
// wire, so the code has to be the one the shape a client can actually write
// deserves. Without it this reached the client with no SQLSTATE at all on the
// embedded door and the pgwire layer's 42000 fallback on the wire.
func (e *MissingOuterColumnError) SQLState() string { return "42703" }

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

// A subquery that cannot be RUN is not a subquery that is FALSE.
//
// Three evaluators used to fold a run-time failure into an answer, and they
// folded it three different ways for one event: `CorrelatedExistsSubquery`
// read `runErr == nil && len(rows) > 0` (so a failure was "does not exist"),
// `CorrelatedScalarSubquery` returned NULL, and `CorrelatedInSubquery`
// returned `e.Not`. None of the three was an error the client could see, and
// the same file's `readOuterValues` documents the opposite rule for the
// neighbouring condition ("a reference the batch does not carry is an ERROR,
// never a NULL"). They fail through failEval now — protocol item 8: loud
// beats plausible, and an obviously-wrong 0 must not become a plausible wrong
// number either (#734, #679, #535).

// ScalarSubqueryValue reduces a scalar subquery's RESULT to the one value a
// scalar subquery is, and is the single place in the engine that decides what
// "one" means.
//
// PostgreSQL's rule, and now this engine's (ADR-0021 5):
//
//	NO rows      the value is SQL NULL. An absent row is not an error.
//	ONE row      that row's value.
//	MORE         SQLSTATE 21000, `more than one row returned by a subquery
//	             used as an expression`. Never the first row.
//
// Every evaluator used to take `rows[0]` and say nothing. That is a WRONG
// ANSWER wearing a plausible one — `WHERE n < (SELECT n FROM src)` over a
// two-row `src` answered against whichever row the runner happened to return
// first — and on the DML door it was worse than wrong: `DELETE FROM t WHERE
// n < (SELECT n FROM src)` emptied the table where PostgreSQL raises and
// deletes nothing.
//
// The MULTI-COLUMN case is deliberately not decided here. PostgreSQL refuses
// it at analysis time with 42601 (`subquery must return only one column`) and
// this engine does not; that is a separate gap, and picking a column out of a
// Go map — which is what the loop below does — is arbitrary for it either
// way. Nothing here makes that better or worse.
func ScalarSubqueryValue(sql string, rows []map[string]any) (any, error) {
	switch {
	case len(rows) == 0:
		return nil, nil // a genuine empty result IS SQL NULL
	case len(rows) > 1:
		return nil, &ScalarSubqueryRowsError{SQL: sql, Rows: len(rows)}
	}
	for _, v := range rows[0] {
		return v, nil
	}
	return nil, nil
}

// ScalarSubqueryRowsError reports a scalar subquery that returned more than
// one row.
//
// It is an ERROR and not a value because every value it could stand in for is
// a lie about the data, and because the row it would otherwise pick is
// whichever one the producer happened to emit first — a different answer on a
// different execution path for the same query. PostgreSQL's own wording and
// SQLSTATE, because a client branches on the code.
type ScalarSubqueryRowsError struct {
	SQL  string
	Rows int
}

// Error is PostgreSQL's own sentence, with what this site knows appended.
// Rows == 0 means the counter stopped as soon as it passed one — the DAG's
// producer reader does that, because how many more there are does not change
// the answer and reading on to find out costs a fetch per file.
func (e *ScalarSubqueryRowsError) Error() string {
	const pg = "more than one row returned by a subquery used as an expression"
	switch {
	case e.Rows > 0 && e.SQL != "":
		return fmt.Sprintf("%s\n  subquery returned %d rows: %s", pg, e.Rows, e.SQL)
	case e.SQL != "":
		return fmt.Sprintf("%s\n  subquery: %s", pg, e.SQL)
	}
	return pg
}

// SQLState is PostgreSQL's 21000 (cardinality_violation).
func (e *ScalarSubqueryRowsError) SQLState() string { return "21000" }

// FatalEvalError satisfies the marker the pipeline drivers recover on, so
// this reaches the client as a query error rather than taking the process.
func (e *ScalarSubqueryRowsError) FatalEvalError() error { return e }

// SubqueryRunFailedError reports a subquery whose standalone execution
// failed. It is a fatal evaluation error rather than a value because every
// value it could stand in for is a lie about the data.
type SubqueryRunFailedError struct {
	Kind string // "EXISTS", "IN", "scalar"
	SQL  string
	Err  error
}

func (e *SubqueryRunFailedError) Error() string {
	return fmt.Sprintf("%s subquery could not be executed: %v\n  subquery: %s",
		e.Kind, e.Err, e.SQL)
}

func (e *SubqueryRunFailedError) Unwrap() error { return e.Err }

// SQLState is the WRAPPED failure's, because the reason the subquery could
// not be run IS the query's error: `WHERE h > (SELECT AVG(h) FROM t WHERE
// SUM(h) > 0)` fails because the inner statement puts an aggregate in a
// WHERE, and PostgreSQL 17 answers 42803 for it. Without this the refusal
// reached the client with no SQLSTATE at all while the same inner statement
// run on its own carried one — loud, but not yet the error PostgreSQL gives.
//
// Empty when the wrapped failure carries no code, which is what the pgwire
// layer's own fallback expects.
func (e *SubqueryRunFailedError) SQLState() string { return sqlerr.StateOf(e.Err) }

// FatalEvalError satisfies the marker the pipeline drivers recover on, so
// this reaches the client as a query error rather than taking the process.
func (e *SubqueryRunFailedError) FatalEvalError() error { return e }

// DanglingSubqueryError reports a subquery about to be executed STANDALONE
// whose text still carries a qualified column reference that no FROM clause
// inside it provides.
//
// That is a correlated subquery the classifier did not recognize as one. Run
// standalone it does not fail: `expr.ResolveColumnRef` STRIPS the qualifier
// and retries the bare name, so `sub.g = typemx.g` rebinds to the inner
// relation's own column and reads constant TRUE, and `y.id = x.id * 2` — where
// the inner relation has no `id * 2` to rebind to — reads constant FALSE. One
// misclassification, two different confident wrong answers, decided by whether
// the two relations happen to share a column name (#734, #679, #535).
//
// plansql.DanglingTableRefs needs no outer scope to see this, which is what
// lets the check live HERE, at the site that has lost it, rather than
// depending on the classifier being repaired first.
type DanglingSubqueryError struct {
	Kind string
	SQL  string
	Refs []plansql.OuterRef
}

func (e *DanglingSubqueryError) Error() string {
	names := make([]string, 0, len(e.Refs))
	for _, r := range e.Refs {
		names = append(names, r.Table+"."+r.Column)
	}
	return fmt.Sprintf("%s subquery is correlated on %s but was planned as uncorrelated, "+
		"so it would run once against no outer row and answer a constant; "+
		"this query has no distributed or single-process lowering for that correlation"+
		"\n  subquery: %s",
		e.Kind, strings.Join(names, ", "), e.SQL)
}

// FatalEvalError satisfies the marker the pipeline drivers recover on.
func (e *DanglingSubqueryError) FatalEvalError() error { return e }

// SQLState is PostgreSQL's feature_not_supported. The query is legal SQL that
// this engine cannot lower, which is what 0A000 says; 42703 would claim the
// column does not exist, and it does — in the outer query.
func (e *DanglingSubqueryError) SQLState() string { return "0A000" }

// refuseDanglingSubquery fails the query when sql — about to be executed
// STANDALONE, with no outer row — still names a relation it does not read.
// Called once per query from the uncorrelated evaluators' resolveSlow, never
// per row.
func refuseDanglingSubquery(kind, sql string) {
	if refs := plansql.DanglingTableRefs(sql); len(refs) > 0 {
		failEval(&DanglingSubqueryError{Kind: kind, SQL: sql, Refs: refs})
	}
}
