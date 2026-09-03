package expr

import (
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The guard for #347: a correlated reference the batch does not carry is an
// ERROR, not a NULL.
//
// The bug it backstops was a pruning defect — the outer query dropped a
// column only its subquery read — but the reason it was SILENT is here:
// readOuterValues wrote nil into the substitution map for a column it could
// not find, the rewriter replaced the reference with a NULL literal, and
// every comparison in the subquery went UNKNOWN. The query answered 0 rows
// with no error. Pruning is fixed; this makes the next pruning change fail
// loudly rather than quietly answering nothing.

// outerRefBatch is a two-column batch: the correlated column is "c_nationkey"
// and "SearchPhrase" exists only to pin the case-folding fallback.
func outerRefBatch() *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "c_nationkey", Type: parquet.TypeInt64},
		{Name: "SearchPhrase", Type: parquet.TypeString},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, int64(7))
	b.Columns[1].SetValue(0, "hello")
	b.Len = 1
	return b
}

func TestReadOuterValuesMissingColumnErrors(t *testing.T) {
	b := outerRefBatch()

	// The map holds the LITERAL the re-run's SQL will carry, not the Go box
	// the column happens to hand out (#679, outer_literal.go), so the
	// expectations are the literal's rendered text.
	tests := []struct {
		name    string
		refs    []plansql.OuterRef
		wantErr bool
		want    map[string]string
	}{
		{
			name: "present",
			refs: []plansql.OuterRef{{Table: "c1", Column: "c_nationkey"}},
			want: map[string]string{"c1.c_nationkey": "7"},
		},
		{
			// ColumnByName is case-sensitive and correlation analysis
			// lowercases every name it reports, so a mixed-case column would
			// otherwise miss and — before the guard — read as NULL.
			name: "case_folded",
			refs: []plansql.OuterRef{{Table: "h", Column: "searchphrase"}},
			want: map[string]string{"h.searchphrase": "'hello'"},
		},
		{
			// The #347 condition. Absent must not read as NULL.
			name:    "missing",
			refs:    []plansql.OuterRef{{Table: "c1", Column: "c_acctbal"}},
			wantErr: true,
		},
		{
			// One good reference does not excuse a missing sibling.
			name: "one_of_two_missing",
			refs: []plansql.OuterRef{
				{Table: "c1", Column: "c_nationkey"},
				{Table: "c1", Column: "c_custkey"},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vals, err := readOuterValues(b, 0, tc.refs)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("readOuterValues returned %v and no error; a correlated reference "+
						"the batch does not carry must ERROR, never resolve to NULL", vals)
				}
				var missing *MissingOuterColumnError
				if !errors.As(err, &missing) {
					t.Fatalf("error %v is not a *MissingOuterColumnError", err)
				}
				if !strings.Contains(err.Error(), missing.Ref.Column) {
					t.Errorf("error text %q does not name the missing column %q", err, missing.Ref.Column)
				}
				if len(missing.Available) != 2 {
					t.Errorf("Available = %v, want the batch's two columns", missing.Available)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for k, want := range tc.want {
				lit, ok := vals[k].(plansql.Node)
				if !ok {
					t.Fatalf("vals[%q] = %v (%T), want a rendered literal node", k, vals[k], vals[k])
				}
				if got := lit.String(); got != want {
					t.Errorf("vals[%q] renders %q, want %q", k, got, want)
				}
			}
		})
	}
}

// A column that IS present and holds SQL NULL still reads as nil — that is a
// data value, not a resolution failure, and the guard must not confuse them.
func TestReadOuterValuesPresentNullIsNotAnError(t *testing.T) {
	schema := []parquet.Column{{Name: "c_nationkey", Type: parquet.TypeInt64}}
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, nil)
	b.Len = 1

	vals, err := readOuterValues(b, 0, []plansql.OuterRef{{Table: "c1", Column: "c_nationkey"}})
	if err != nil {
		t.Fatalf("a present column holding NULL must not error: %v", err)
	}
	lit, ok := vals["c1.c_nationkey"].(plansql.Node)
	if !ok {
		t.Fatalf("vals = %v, want the key present with a rendered literal", vals)
	}
	if got := lit.String(); got != "null" {
		t.Errorf("vals[%q] renders %q, want the NULL literal", "c1.c_nationkey", got)
	}
}

// The three correlated evaluators all raise the guard rather than answering.
// Expr.Eval has no error return, so the failure travels as a panic carrying a
// value the pipeline recovers (exec.FatalEvalPanic); what this pins is that
// the value carries the error and that nothing returns a plausible NULL/false
// instead.
func TestCorrelatedEvaluatorsRaiseOnMissingOuterColumn(t *testing.T) {
	b := outerRefBatch()
	refs := []plansql.OuterRef{{Table: "c1", Column: "c_acctbal"}}
	outerTables := map[string]bool{"c1": true}
	runner := func(string) ([]map[string]any, error) {
		t.Error("the runner must not be reached: the outer value could not be resolved")
		return nil, nil
	}

	info := parseSelectInfo(t, "SELECT AVG(c_acctbal) FROM customer c2 WHERE c2.c_acctbal < c1.c_acctbal")

	evals := map[string]func(){
		"scalar": func() {
			e := &CorrelatedScalarSubquery{Runner: runner, OuterRefs: refs, OuterTables: outerTables, ParsedInfo: info}
			e.Eval(b, 0)
		},
		"exists": func() {
			e := &CorrelatedExistsSubquery{Runner: runner, OuterRefs: refs, OuterTables: outerTables, ParsedInfo: info}
			e.EvalBool(b, 0)
		},
		"not_exists": func() {
			e := &CorrelatedExistsSubquery{Runner: runner, Not: true, OuterRefs: refs, OuterTables: outerTables, ParsedInfo: info}
			e.EvalBool(b, 0)
		},
		"in": func() {
			e := &CorrelatedInSubquery{Expr: &Lit{Val: int64(1)}, Runner: runner, OuterRefs: refs,
				OuterTables: outerTables, ParsedInfo: info}
			e.EvalBool(b, 0)
		},
	}

	for name, eval := range evals {
		t.Run(name, func(t *testing.T) {
			err := recoverEvalError(eval)
			if err == nil {
				t.Fatal("evaluation returned a value; a correlated reference that cannot be " +
					"resolved must raise, not answer NULL/false")
			}
			var missing *MissingOuterColumnError
			if !errors.As(err, &missing) {
				t.Fatalf("raised %v, want a *MissingOuterColumnError", err)
			}
			// The value the pipeline boundary recovers on.
			var fatal interface{ FatalEvalError() error }
			if !errors.As(err, &fatal) {
				t.Fatal("the raised value does not implement FatalEvalError() — " +
					"exec.Pipeline.Run cannot turn it into a query error")
			}
			if fatal.FatalEvalError() == nil {
				t.Error("FatalEvalError() returned nil")
			}
		})
	}
}

// recoverEvalError runs fn and returns the error it raised, or nil.
func recoverEvalError(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
				return
			}
			panic(r)
		}
	}()
	fn()
	return nil
}

func parseSelectInfo(t *testing.T, sql string) *plansql.SelectInfo {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract %q: %v", sql, err)
	}
	return info
}
