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
	// Cols is the subquery's SELECT-list COLUMN COUNT, resolved from its own
	// plan at compile time, and it is asked BEFORE the subquery runs — see
	// refuseMultiColumnSubqueryByPlan.
	Cols            SubqueryColumnsFunc
	Runner          SubqueryRunner
	OuterRefs       []plansql.OuterRef // correlated column references
	OuterTables     map[string]bool    // outer table aliases
	ParsedInfo      *plansql.SelectInfo
	UnqualOuterCols map[string]string // unqualified column → table mapping for outer refs
	// Scope resolves a relation's COMPLETE column list, so the rebuilt text's
	// guard can tell a ROW FIELD PATH from a lost correlation (#866).
	Scope plansql.TableColumns
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
	// BEFORE THE RUN: how many columns the SELECT list has is decided at
	// analysis time on PostgreSQL, so it fires over an empty result too.
	refuseMultiColumnSubqueryByPlan(e.Cols, sql, false)
	// TWO ROWS, not the whole result: `> 1` is the entire cardinality rule
	// (ADR-0021 §5), so the second row is where the answer is already known.
	// The bound is on the READ and not on the rule — a third row raises the
	// same 21000 the tenth would.
	rows, runErr := e.Runner(plansql.AppendRowLimit(sql, e.ParsedInfo, 2))
	if runErr != nil {
		// NOT NULL. A scalar subquery that could not be run has no value,
		// and NULL is a value — one that makes every comparison above it
		// UNKNOWN and the row silently vanish.
		failEval(subqueryRunFailed("scalar", sql, runErr))
	}
	// COLUMNS BEFORE ROWS, which is PostgreSQL's order: how many columns a
	// subquery has is a property of its SELECT LIST and is decided at
	// analysis time there, before any row is read, so `(SELECT a, b FROM t)`
	// over a two-row `t` is 42601 and not 21000.
	refuseMultiColumnSubquery(sql, rows, false)
	if len(rows) > 1 {
		// Reported with no count: the read stopped on purpose, so this site
		// knows "more than one" and not how many more.
		failEval(&ScalarSubqueryRowsError{SQL: sql})
	}
	v, err := ScalarSubqueryValue(sql, rows)
	if err != nil {
		failEval(err)
	}
	return v
}

func (e *CorrelatedScalarSubquery) buildSQL(b *batch.RecordBatch, row int) (string, error) {
	return rerunSQL("scalar", b, row, e.OuterRefs, e.OuterTables, e.UnqualOuterCols,
		e.ParsedInfo, e.Scope)
}

// CorrelatedInSubquery checks if a value is in the result set of a correlated subquery.
type CorrelatedInSubquery struct {
	// Cols is the subquery's SELECT-list COLUMN COUNT — see
	// refuseMultiColumnSubqueryByPlan.
	Cols            SubqueryColumnsFunc
	Expr            Expr
	Runner          SubqueryRunner
	Not             bool
	OuterRefs       []plansql.OuterRef
	OuterTables     map[string]bool
	ParsedInfo      *plansql.SelectInfo
	UnqualOuterCols map[string]string
	// Scope resolves a relation's COMPLETE column list, so the rebuilt text's
	// guard can tell a ROW FIELD PATH from a lost correlation (#866).
	Scope plansql.TableColumns
	// SetBound bounds the membership set in ROWS, refusing past it rather
	// than truncating — a set short by one row is a different answer, and on
	// a write door it deletes the wrong rows. Zero is unbounded.
	//
	// It lives on this construct and not in the runner because a runner sees
	// SQL text and cannot tell which construct asked for it. IN is the one
	// that wants a SET; EXISTS wants a row and a scalar subquery is an error
	// past one, and both read a bounded number of rows by construction now
	// (plansql.AppendRowLimit). Bounding all three in the runner charged
	// those two for a set neither builds.
	SetBound int
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
	refuseMultiColumnSubqueryByPlan(e.Cols, sql, true)
	rows, runErr := e.Runner(sql)
	if runErr != nil {
		// NOT `e.Not`. A membership test whose set could not be built has no
		// answer, and returning "not a member" is the third of the three
		// different wrong answers these evaluators gave to one event.
		failEval(subqueryRunFailed("IN", sql, runErr))
	}
	if e.SetBound > 0 && len(rows) > e.SetBound {
		failEval(&InSetTooLargeError{SQL: sql, Rows: len(rows), Bound: e.SetBound})
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
		// PostgreSQL refuses a multi-column IN subquery outright (42601,
		// `subquery has too many columns`). Reading one column out of a Go
		// map instead answered a DIFFERENT column on different runs of the
		// same query, because map iteration order is randomized per range
		// statement. A one-column subquery whose pipeline emitted a hidden
		// ORDER BY key beside it is #875 and is trimmed where the pipeline
		// is built, so a row with two entries here is a genuine two-column
		// SELECT list.
		if len(r) > 1 {
			failEval(&SubqueryColumnsError{SQL: sql, Columns: len(r), InPredicate: true})
		}
		for _, v := range r {
			if v == nil {
				sawNull = true
			} else if compare(lv, v, CmpEq) {
				return !e.Not, false
			}
		}
	}
	if sawNull {
		return false, true
	}
	return e.Not, false
}

func (e *CorrelatedInSubquery) buildSQL(b *batch.RecordBatch, row int) (string, error) {
	return rerunSQL("IN", b, row, e.OuterRefs, e.OuterTables, e.UnqualOuterCols,
		e.ParsedInfo, e.Scope)
}

// CorrelatedExistsSubquery evaluates a correlated EXISTS subquery per-row.
type CorrelatedExistsSubquery struct {
	Runner          SubqueryRunner
	Not             bool
	OuterRefs       []plansql.OuterRef
	OuterTables     map[string]bool
	ParsedInfo      *plansql.SelectInfo
	UnqualOuterCols map[string]string
	// Scope resolves a relation's COMPLETE column list, so the rebuilt text's
	// guard can tell a ROW FIELD PATH from a lost correlation (#866).
	Scope plansql.TableColumns
}

func (e *CorrelatedExistsSubquery) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *CorrelatedExistsSubquery) EvalBool(b *batch.RecordBatch, row int) bool {
	sql, err := e.buildSQL(b, row)
	if err != nil {
		failEval(err)
	}
	// ONE ROW. EXISTS asks whether there is a row, so the first one answers
	// it; nothing above this line has ever looked at a second.
	rows, runErr := e.Runner(plansql.AppendRowLimit(sql, e.ParsedInfo, 1))
	if runErr != nil {
		// NOT "does not exist". `runErr == nil && len(rows) > 0` read a
		// failure as FALSE, so a re-run that raised — #679's quoted DECIMAL
		// against a BIGINT raises 22P02 — answered a confident 0 rows for
		// PostgreSQL's 3, and its NOT EXISTS twin answered every row.
		failEval(subqueryRunFailed("EXISTS", sql, runErr))
	}
	exists := len(rows) > 0
	if e.Not {
		return !exists
	}
	return exists
}

func (e *CorrelatedExistsSubquery) buildSQL(b *batch.RecordBatch, row int) (string, error) {
	return rerunSQL("EXISTS", b, row, e.OuterRefs, e.OuterTables, e.UnqualOuterCols,
		e.ParsedInfo, e.Scope)
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

// ScalarSubqueryValue is the shared scalar-result reducer (ADR-0021 §5):
// zero rows means NULL; one row means its value; more than one raises 21000,
// "more than one row returned by a subquery used as an expression".
// A multi-column result raises 42601, "subquery must return only one column";
// never pick an arbitrary column. Plan-time arity checks precede this backstop.
// Hidden ORDER BY keys are trimmed in physical.buildSubqueryPipelineFor
// before reduction, so only the SELECT list reaches here (#875).
// See docs/internals/scalar-subquery-result-cardinality.md for the design.
func ScalarSubqueryValue(sql string, rows []map[string]any) (any, error) {
	switch {
	case len(rows) == 0:
		return nil, nil // a genuine empty result IS SQL NULL
	case len(rows) > 1:
		return nil, &ScalarSubqueryRowsError{SQL: sql, Rows: len(rows)}
	case len(rows[0]) > 1:
		return nil, &SubqueryColumnsError{SQL: sql, Columns: len(rows[0])}
	}
	for _, v := range rows[0] {
		return v, nil
	}
	return nil, nil
}

// refuseMultiColumnSubqueryByPlan raises PostgreSQL's 42601 from the
// SUBQUERY'S OWN PLAN, before it is run.
//
// It is the primary guard and the row-count one below is the backstop, because
// PostgreSQL decides this during PARSE ANALYSIS: `subquery must return only
// one column` fires whatever the subquery would return, and an EMPTY one is
// not an exception. Counting the rows cannot reach that case —
// `(SELECT id, c_i64 FROM t WHERE id < 0)` returned no row, so there was
// nothing to count, and the scalar answered SQL NULL where PostgreSQL raises
// (round-1 P1). Asking the plan reaches it, and reaches a two-column subquery
// over a two-row relation in the right order as well.
//
// cols is nil when nothing could resolve the arity — a compile site with no
// planner — and the row-count backstop is then all there is.
func refuseMultiColumnSubqueryByPlan(cols SubqueryColumnsFunc, sql string, inPredicate bool) {
	if cols == nil {
		return
	}
	if n, ok := cols(sql); ok && n > 1 {
		failEval(&SubqueryColumnsError{SQL: sql, Columns: n, InPredicate: inPredicate})
	}
}

// refuseMultiColumnSubquery raises PostgreSQL's 42601 when a subquery used
// where ONE column is required returned more than one.
//
// It is called BEFORE the cardinality rule at every site that has both,
// because that is PostgreSQL's order: the column count is a property of the
// SELECT LIST and is decided at analysis time, so a two-column subquery over a
// two-row relation is `subquery must return only one column` and not
// `more than one row returned by a subquery used as an expression`.
//
// A ZERO-ROW result is the bound and it is stated rather than pretended away:
// with no row there is no map to count, so a multi-column subquery over an
// empty input keeps the SQL NULL it always answered where PostgreSQL still
// refuses. Deciding it needs the SELECT list's arity at compile time, which is
// the planner's to hand over.
func refuseMultiColumnSubquery(sql string, rows []map[string]any, inPredicate bool) {
	if len(rows) == 0 || len(rows[0]) <= 1 {
		return
	}
	failEval(&SubqueryColumnsError{SQL: sql, Columns: len(rows[0]), InPredicate: inPredicate})
}

// SubqueryColumnsError reports a subquery used where ONE column is required
// that returns more than one.
//
// PostgreSQL's own two sentences and its 42601: `subquery must return only
// one column` for a scalar expression subquery, `subquery has too many
// columns` for an IN / NOT IN one. A client branches on the code, and the two
// wordings are what a user sees on the server this engine speaks for.
type SubqueryColumnsError struct {
	SQL     string
	Columns int
	// InPredicate selects PostgreSQL's IN wording. False is the scalar
	// expression's.
	InPredicate bool
}

func (e *SubqueryColumnsError) Error() string {
	pg := "subquery must return only one column"
	if e.InPredicate {
		pg = "subquery has too many columns"
	}
	if e.SQL == "" {
		return pg
	}
	return fmt.Sprintf("%s\n  subquery returned %d columns: %s", pg, e.Columns, e.SQL)
}

// SQLState is PostgreSQL's 42601 (syntax_error).
func (e *SubqueryColumnsError) SQLState() string { return "42601" }

// FatalEvalError satisfies the marker the pipeline drivers recover on, so
// this reaches the client as a query error rather than taking the process.
func (e *SubqueryColumnsError) FatalEvalError() error { return e }

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
// Rows == 0 is for a caller that knows only "more than one" — it stopped
// counting, or never had the whole result — and the sentence stands on its own
// there, which is the part a client reads.
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

// InSetTooLargeError reports an IN-subquery whose result is past the row
// bound the caller set (expr.WithSetRowBound).
//
// It REFUSES rather than truncating because a membership set short by one row
// is not a smaller answer, it is a different one — and on a write door it
// deletes the wrong rows. 54000 is program_limit_exceeded, which is what this
// is: a limit this engine imposes, named in the message so the reader can
// raise it.
type InSetTooLargeError struct {
	SQL   string
	Rows  int
	Bound int
}

func (e *InSetTooLargeError) Error() string {
	return fmt.Sprintf(
		"a subquery in a DML predicate returned %d rows, past the %d-row bound "+
			"(WADJET_IN_SET_MAX); narrow the subquery or raise the bound\n  subquery: %s",
		e.Rows, e.Bound, e.SQL)
}

// SQLState is PostgreSQL's 54000 (program_limit_exceeded).
func (e *InSetTooLargeError) SQLState() string { return "54000" }

// FatalEvalError satisfies the marker the pipeline drivers recover on.
func (e *InSetTooLargeError) FatalEvalError() error { return e }

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

// subqueryRunFailed is what a subquery's failed run raises — EXCEPT when the
// failure is an AUTHORIZATION refusal, which is not an execution failure and
// does not wear that sentence.
//
// The identity may not read a relation the subquery names. That refusal is the
// product's one refusal: SQLSTATE 42501 carrying the shared decision's own text
// and nothing else, identical on every door (ADR-0034 item 6). Wrapping it gave
// the SAME operation two different messages depending on which arm answered —
// the DAG plans a scalar subquery into producer stages and refuses at PLAN
// time, unwrapped, while the single-process arm refuses while EVALUATING and
// wore "scalar subquery could not be executed: …" in front of it — and told the
// caller where the check fired rather than what it decided (#945).
//
// It travels as fatalEval so the pipeline drivers still recover it: a bare
// error carries no FatalEvalPanic marker and would re-raise as a panic.
func subqueryRunFailed(kind, sql string, err error) error {
	if sqlerr.StateOf(err) == "42501" {
		return fatalEval{err}
	}
	return &SubqueryRunFailedError{Kind: kind, SQL: sql, Err: err}
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
func refuseDanglingSubquery(kind, sql string, scope plansql.TableColumns) {
	if refs := plansql.DanglingTableRefsWithScope(sql, scope); len(refs) > 0 {
		failEval(&DanglingSubqueryError{Kind: kind, SQL: sql, Refs: refs})
	}
}

// WindowBorneCorrelationError reports a CORRELATED subquery whose body holds a
// WINDOW CALL.
//
// The per-row re-run substitutes the outer row's values into the subquery's
// WHERE clause and REBUILDS the statement around it (plansql.RebuildSQL),
// re-emitting every other clause as the text the parser recorded. A window
// call's recorded text is `<func>(<args>) OVER (...)` — WindowFuncNode.String
// collapses the OVER clause deliberately, which is why
// plansql.ReplaceWindowFuncs matches window nodes by POINTER and not by text —
// so the rebuilt statement does not parse, and the query died with
// `expected ')' after OVER clause` from a runner re-reading a statement nobody
// wrote. That is what a correlated subquery with a window in its SELECT list
// has always done here.
//
// #1045 is the SILENT half of the same fact. An outer reference inside a
// window call was invisible to the correlation walk, so
// `(SELECT 1+SUM(u.id) OVER () FROM users x WHERE x.id=1)` was planned
// UNCORRELATED, ran once, and `expr.ResolveColumnRef`'s qualifier strip
// rebound `u.id` to the inner relation's own `id`: 2, 2, 2 for
// PostgreSQL 17.11's 2, 3, 4. With the walk repaired the shape is correlated,
// and this is what a correlated subquery it cannot rebuild now answers.
//
// Raised at COMPILE time, once per query, because it is a property of the plan
// and not of a row. All three correlated constructs raise it: the rebuild is
// the same for a scalar subquery, an IN set and an EXISTS.
type WindowBorneCorrelationError struct {
	Kind string
	SQL  string
	Refs []plansql.OuterRef
}

func (e *WindowBorneCorrelationError) Error() string {
	names := make([]string, 0, len(e.Refs))
	for _, r := range e.Refs {
		names = append(names, r.Table+"."+r.Column)
	}
	return fmt.Sprintf("%s subquery is correlated on %s and holds a window function; the "+
		"per-row re-run rebuilds the subquery's text and a window call's OVER clause has no "+
		"rendering that survives that rebuild, so this query has no distributed or "+
		"single-process lowering for that correlation\n  subquery: %s",
		e.Kind, strings.Join(names, ", "), e.SQL)
}

// FatalEvalError satisfies the marker the pipeline drivers recover on.
func (e *WindowBorneCorrelationError) FatalEvalError() error { return e }

// SQLState is PostgreSQL's feature_not_supported, the code the three
// `window_*_correlated` shapes already answer with: the query is legal SQL
// this engine cannot lower.
func (e *WindowBorneCorrelationError) SQLState() string { return "0A000" }

// refuseWindowBorneCorrelation answers the error when a correlated subquery's
// body holds a window call, and nil otherwise. Every correlated construct asks
// it, because every one of them re-runs by rebuilding the same text.
//
// It takes the PARSED body rather than the text, so the three call sites pass
// the SelectInfo they are already holding to build their evaluator instead of
// parsing the same statement a second time.
func refuseWindowBorneCorrelation(kind, sql string, info *plansql.SelectInfo,
	refs []plansql.OuterRef) error {
	if len(refs) == 0 || info == nil || !plansql.HoldsWindowCall(info) {
		return nil
	}
	return &WindowBorneCorrelationError{Kind: kind, SQL: sql, Refs: refs}
}

// A PER-ROW RE-RUN SUBSTITUTES INTO EVERY CLAUSE IT REBUILDS, AND REFUSES WHAT
// IT COULD NOT REACH (#1044 round 2).
//
// rerunSQL is the ONE text a correlated re-run runs, for all three constructs.
// It reads the outer row's values, rewrites them into every clause
// plansql.RebuildSQLForRerun re-emits from an AST — the SELECT list, the
// WHERE, the HAVING, the GROUP BY, the ORDER BY and each JOIN's ON condition,
// six of them — and then asks whether the rebuilt statement still names a
// relation it does not read.
//
// That last question is ADR-0021 §1c's own guard, applied at the site §1c did
// not cover. §1c put it on the UNCORRELATED evaluators, because a subquery
// misclassified as uncorrelated runs text that still names the outer relation
// and the qualifier strip then answers a confident constant. A CORRELATED
// re-run can reach the same place from the other direction: a reference the
// substitution does not rewrite survives the rebuild, and running it would be
// the same silent wrong answer. It is refused instead. Every clause the
// rebuild renders from an AST is now substituted, and a GROUP BY or ORDER BY
// term whose rendering would be a bare numeric literal — the one thing those
// two clauses read as a select-list POSITION — is written as a CAST
// (plansql.ClauseTermText) rather than left behind, so what remains for that
// refusal is the post-condition of the rendering.
//
// scope tells a ROW FIELD PATH from a lost correlation, exactly as it does for
// the uncorrelated guard: `c_row.b` is a qualified reference whose qualifier
// is a COLUMN of the relation the subquery reads, so the subquery is
// self-contained and answers (#866, ADR-0022). A nil resolver leaves such a
// reference dangling, which is the safe direction and what this had before.
func rerunSQL(kind string, b *batch.RecordBatch, row int, refs []plansql.OuterRef,
	outerTables map[string]bool, unqual map[string]string,
	info *plansql.SelectInfo, scope plansql.TableColumns) (string, error) {
	vals, err := readOuterValues(b, row, refs)
	if err != nil {
		return "", err
	}
	rewrite := func(n plansql.Node) plansql.Node {
		out := plansql.RewriteOuterRefs(n, outerTables, vals)
		if len(unqual) > 0 {
			out = plansql.RewriteUnqualifiedOuterRefs(out, unqual, vals)
		}
		return out
	}
	sql, _ := plansql.RebuildSQLForRerun(info, rewrite)
	// The two clauses the rebuild re-emits as recorded TEXT, asked directly of
	// their own trees: an outer reference there did not move, and running the
	// statement with it still in place is the silent answer §1c refuses.
	if left := plansql.OuterRefsInUnsubstitutedClauses(info, outerTables, rewrite); len(left) > 0 {
		return "", &UnsubstitutedOuterRefError{Kind: kind, SQL: sql, Refs: left}
	}
	// And the belt: a rebuilt statement that still names a relation it does
	// not read is one this engine cannot run, whatever put the name there.
	if left := plansql.DanglingTableRefsWithScope(sql, scope); len(left) > 0 {
		return "", &UnsubstitutedOuterRefError{Kind: kind, SQL: sql, Refs: left}
	}
	return sql, nil
}

// UnsubstitutedOuterRefError reports a correlated subquery whose rebuilt text
// still names a relation it does not read — an outer reference the per-row
// substitution could not reach.
//
// The reachable clauses are SIX: the SELECT list, the WHERE, the HAVING, the
// GROUP BY, the ORDER BY and each JOIN's ON condition, each of which the
// rebuild renders from its own AST. What a GROUP BY or ORDER BY term cannot
// carry is one RENDERING — a bare numeric literal, which both engines read as
// the first SELECT ITEM rather than as the number one — and
// plansql.ClauseTermText writes such a value as a CAST instead, so this
// reports only a rendering that is bare in spite of it. Running the statement
// anyway is the silent answer ADR-0021 §1c refuses at the uncorrelated
// evaluators, so it is refused here too.
type UnsubstitutedOuterRefError struct {
	Kind string
	SQL  string
	Refs []plansql.OuterRef
}

func (e *UnsubstitutedOuterRefError) Error() string {
	names := make([]string, 0, len(e.Refs))
	for _, r := range e.Refs {
		names = append(names, r.Table+"."+r.Column)
	}
	return fmt.Sprintf("%s subquery is correlated on %s in a clause its per-row re-run "+
		"cannot substitute — the rebuild renders the SELECT list, the WHERE, the HAVING, "+
		"the GROUP BY, the ORDER BY and each JOIN's ON condition from their own trees, and "+
		"the substituted term still renders as a bare numeric literal, which a GROUP BY or "+
		"an ORDER BY reads as a select-list POSITION; this query has no distributed or "+
		"single-process lowering for that correlation\n  subquery: %s",
		e.Kind, strings.Join(names, ", "), e.SQL)
}

// FatalEvalError satisfies the marker the pipeline drivers recover on.
func (e *UnsubstitutedOuterRefError) FatalEvalError() error { return e }

// SQLState is PostgreSQL's feature_not_supported: the query is legal SQL this
// engine cannot lower.
func (e *UnsubstitutedOuterRefError) SQLState() string { return "0A000" }

// OuterLevelAggregateError reports a subquery holding an aggregate whose
// argument names ONLY the enclosing query.
//
// PostgreSQL puts an aggregate at the level of the deepest variable in its
// arguments, so `(SELECT MAX(u.id) FROM x)` and `(SELECT MAX((SELECT u.id))
// FROM x)` are the ENCLOSING query's aggregate — and 42803 there, because the
// enclosing SELECT list then carries an ungrouped column. This engine's
// per-row re-run would substitute the outer row's value and compute the
// aggregate at the INNER level instead, answering a number PostgreSQL does not
// give, so the shape is refused. `(SELECT SUM(x.visits + u.id) FROM x)` names
// an inner variable too and is the inner block's aggregate: it answers, and is
// not reported here.
type OuterLevelAggregateError struct {
	Kind string
	SQL  string
	Refs []plansql.OuterRef
}

func (e *OuterLevelAggregateError) Error() string {
	names := make([]string, 0, len(e.Refs))
	for _, r := range e.Refs {
		names = append(names, r.Table+"."+r.Column)
	}
	return fmt.Sprintf("%s subquery holds an aggregate whose argument names only the "+
		"enclosing query (%s); PostgreSQL puts such an aggregate at the ENCLOSING query's "+
		"level, and this engine has no lowering for an aggregate level above the block it is "+
		"written in\n  subquery: %s",
		e.Kind, strings.Join(names, ", "), e.SQL)
}

// FatalEvalError satisfies the marker the pipeline drivers recover on.
func (e *OuterLevelAggregateError) FatalEvalError() error { return e }

// SQLState is PostgreSQL's feature_not_supported.
func (e *OuterLevelAggregateError) SQLState() string { return "0A000" }

// refuseOuterLevelAggregate answers the error when a correlated subquery holds
// an aggregate the ENCLOSING query owns, and nil otherwise.
func refuseOuterLevelAggregate(kind, sql string, outerTables map[string]bool) error {
	refs := plansql.AggregatesOverOnlyOuterRefs(sql, outerTables)
	if len(refs) == 0 {
		return nil
	}
	return &OuterLevelAggregateError{Kind: kind, SQL: sql, Refs: refs}
}

// UnrebuildableBodyError reports a correlated subquery whose body the per-row
// re-run cannot write back out.
//
// The re-run substitutes the outer row's values and rebuilds the statement
// from its parts (plansql.RebuildSQLForRerun), which renders ONE select. Two
// bodies have no rendering there, and both used to be invisible rather than
// refused — the classifier read the top-level block's WHERE, HAVING and SELECT
// list only, so a reference written in a set-operation ARM was never seen and
// the subquery ran standalone, where the qualifier strip answered one constant
// per outer row (`(SELECT u.id FROM x WHERE x.id=1 UNION ALL SELECT u.id FROM
// y WHERE y.id=99)` was 1, 1, 1 for PostgreSQL 17.11's 1, 2, 3).
//
//   - a SET OPERATION: RebuildSQL has no arm for info.Union, so the rebuilt
//     text is not the statement the user wrote.
//   - a SELECT ITEM holding an AGGREGATE beside a nested scalar SUBQUERY THAT
//     NAMES THE ENCLOSING QUERY: the nested subquery's value is the outer
//     row's, so the item has no plan-time type. The refusal is on that SHAPE
//     and not on the declaration the item would get — a COUNT would fit the
//     default and answer, and is refused with the accumulators that cannot be
//     stored in it. See plansql.AggregateBesideANestedSubquery, which carries
//     the per-accumulator measurements and the uncorrelated case it excludes.
type UnrebuildableBodyError struct {
	Kind   string
	Reason string
	SQL    string
	Refs   []plansql.OuterRef
}

func (e *UnrebuildableBodyError) Error() string {
	names := make([]string, 0, len(e.Refs))
	for _, r := range e.Refs {
		names = append(names, r.Table+"."+r.Column)
	}
	return fmt.Sprintf("%s subquery is correlated on %s and %s, so its per-row re-run cannot "+
		"be written back out; this query has no distributed or single-process lowering for "+
		"that correlation\n  subquery: %s",
		e.Kind, strings.Join(names, ", "), e.Reason, e.SQL)
}

// FatalEvalError satisfies the marker the pipeline drivers recover on.
func (e *UnrebuildableBodyError) FatalEvalError() error { return e }

// SQLState is PostgreSQL's feature_not_supported.
func (e *UnrebuildableBodyError) SQLState() string { return "0A000" }

// refuseUnrebuildableBody answers the error when a correlated subquery's body
// is one the rebuild cannot render, and nil otherwise.
func refuseUnrebuildableBody(kind, sql string, info *plansql.SelectInfo,
	refs []plansql.OuterRef, outerTables map[string]bool) error {
	if len(refs) == 0 {
		return nil
	}
	switch {
	case plansql.HoldsSetOperation(info):
		return &UnrebuildableBodyError{Kind: kind, SQL: sql, Refs: refs,
			Reason: "its body is a SET OPERATION, which the rebuild renders no arm for"}
	case plansql.AggregateBesideANestedSubquery(sql, outerTables):
		return &UnrebuildableBodyError{Kind: kind, SQL: sql, Refs: refs,
			Reason: "a SELECT item holds an aggregate beside a nested subquery that names the " +
				"enclosing query, which leaves the item with no type until the outer row is known"}
	}
	return nil
}
