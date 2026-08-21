package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// compileExprSQL parses and compiles a scalar expression, failing the test on
// either error.
func compileExprSQL(tb testing.TB, sql string) Expr {
	tb.Helper()
	node, err := plansql.ParseExpression(sql)
	if err != nil {
		tb.Fatalf("parse %q: %v", sql, err)
	}
	e, err := Compile(node)
	if err != nil {
		tb.Fatalf("compile %q: %v", sql, err)
	}
	return e
}

// TestThreeValuedLogicEval pins SQL's three-valued logic on the boxed Eval
// path: an operator whose answer is UNKNOWN produces nil, not false (#370).
// The expected values are PostgreSQL's (ADR-0012); every entry was run
// against PostgreSQL 17 via the differential oracle.
func TestThreeValuedLogicEval(t *testing.T) {
	b := testBatch()
	cases := []struct {
		sql  string
		want any // nil means SQL NULL / UNKNOWN
	}{
		// Comparisons against NULL are UNKNOWN.
		{"NULL = NULL", nil},
		{"NULL <> NULL", nil},
		{"1 = NULL", nil},
		{"NULL < 1", nil},
		{"1 = 1", true},
		{"1 = 2", false},

		// Boolean connectives: Kleene logic, not Go booleans.
		{"TRUE OR NULL", true},
		{"NULL OR TRUE", true},
		{"FALSE OR NULL", nil},
		{"NULL OR NULL", nil},
		{"TRUE AND NULL", nil},
		{"NULL AND TRUE", nil},
		{"FALSE AND NULL", false},
		{"NULL AND FALSE", false},
		{"TRUE AND TRUE", true},
		{"FALSE OR TRUE", true},

		// NOT of UNKNOWN stays UNKNOWN.
		{"NOT (NULL = NULL)", nil},
		{"NOT (1 = 1)", false},
		{"NOT (1 = 2)", true},

		// IN / NOT IN with a NULL in the list. The NOT IN row is the
		// dangerous one: answering true admits a row SQL excludes.
		{"1 IN (2, NULL)", nil},
		{"1 IN (1, NULL)", true},
		{"1 NOT IN (2, NULL)", nil},
		{"1 NOT IN (1, NULL)", false},
		{"1 NOT IN (2, 3)", true},
		{"NULL IN (1, 2)", nil},
		{"NULL NOT IN (1, 2)", nil},

		// LIKE with a NULL on either side.
		{"NULL LIKE 'a%'", nil},
		{"'a' LIKE NULL", nil},
		{"'abc' LIKE 'a%'", true},
		{"'abc' NOT LIKE NULL", nil},
		{"'abc' NOT LIKE 'z%'", true},

		// BETWEEN decomposes into >= AND <=, so a NULL bound can still
		// answer false when the other half already refuses.
		{"NULL BETWEEN 1 AND 2", nil},
		{"1 BETWEEN NULL AND 2", nil},
		{"5 BETWEEN NULL AND 2", false},
		{"5 NOT BETWEEN NULL AND 2", true},
		{"1 BETWEEN 0 AND 2", true},

		// IS NULL / IS TRUE / IS FALSE never answer NULL.
		{"NULL IS NULL", true},
		{"NULL IS NOT NULL", false},
		{"NULL IS TRUE", false},
		{"NULL IS NOT TRUE", true},
		{"NULL IS FALSE", false},
		{"NULL IS NOT FALSE", true},
		{"TRUE IS TRUE", true},
		{"FALSE IS TRUE", false},
		{"FALSE IS NOT TRUE", true},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			got := e.Eval(b, 0)
			if got != c.want {
				t.Errorf("Eval(%q) = %#v, want %#v", c.sql, got, c.want)
			}
		})
	}
}

// TestThreeValuedLogicEvalBool pins the WHERE-clause collapse: EvalBool keeps
// neither FALSE nor UNKNOWN rows, and NOT over UNKNOWN stays UNKNOWN — it
// must NOT admit the row (#370).
func TestThreeValuedLogicEvalBool(t *testing.T) {
	b := testBatch()
	cases := []struct {
		sql  string
		want bool
	}{
		{"NULL = NULL", false},
		{"1 NOT IN (2, NULL)", false}, // UNKNOWN: excluded, was admitted pre-fix
		{"1 NOT IN (2, 3)", true},
		{"NOT (NULL = NULL)", false}, // NOT UNKNOWN is UNKNOWN: excluded
		{"NOT (1 = 2)", true},
		{"FALSE OR NULL", false},
		{"TRUE OR NULL", true},
		{"TRUE AND NULL", false},
		{"NULL LIKE 'a%'", false},
		{"NOT (NULL LIKE 'a%')", false},
		{"5 NOT BETWEEN NULL AND 2", true},
		{"NOT (1 BETWEEN NULL AND 2)", false},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			be, ok := e.(BoolExpr)
			if !ok {
				t.Fatalf("compiled %q to %T, which is not a BoolExpr", c.sql, e)
			}
			if got := be.EvalBool(b, 0); got != c.want {
				t.Errorf("EvalBool(%q) = %v, want %v", c.sql, got, c.want)
			}
		})
	}
}

// nullableBoolBatch is one string column s = ['x', NULL] and one int column
// n = [1, NULL].
func nullableBoolBatch() *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "s", Type: parquet.TypeString},
		{Name: "n", Type: parquet.TypeInt64},
	}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, "x")
	b.Columns[0].SetValue(1, nil)
	b.Columns[1].SetValue(0, int64(1))
	b.Columns[1].SetValue(1, nil)
	b.Len = 2
	return b
}

// TestThreeValuedLogicOverColumns runs the same rules through column
// references, which is where ColEmptyStr / CmpTemporalLit / the typed
// comparison nodes can take over from the generic ones.
func TestThreeValuedLogicOverColumns(t *testing.T) {
	b := nullableBoolBatch()
	cases := []struct {
		sql      string
		row      int
		want     any
		wantBool bool
	}{
		{"s = ''", 0, false, false},
		{"s = ''", 1, nil, false},
		{"s <> ''", 1, nil, false},
		{"NOT (s = '')", 1, nil, false},
		{"n = 1", 0, true, true},
		{"n = 1", 1, nil, false},
		{"n NOT IN (2, NULL)", 0, nil, false},
		{"n IN (1, NULL)", 0, true, true},
		{"n IN (2, NULL)", 1, nil, false},
		{"s LIKE 'x%'", 1, nil, false},
		{"NOT (n = 1)", 1, nil, false},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			if got := e.Eval(b, c.row); got != c.want {
				t.Errorf("Eval(%q) row %d = %#v, want %#v", c.sql, c.row, got, c.want)
			}
			be, ok := e.(BoolExpr)
			if !ok {
				t.Fatalf("compiled %q to %T, which is not a BoolExpr", c.sql, e)
			}
			if got := be.EvalBool(b, c.row); got != c.wantBool {
				t.Errorf("EvalBool(%q) row %d = %v, want %v", c.sql, c.row, got, c.wantBool)
			}
		})
	}
}

// TestInSubqueryNullSet pins the NOT IN (SELECT nullable) trap on the
// uncorrelated subquery node: a probe that misses a set containing NULL is
// UNKNOWN, so its value is nil and a WHERE keeps nothing (#370).
func TestInSubqueryNullSet(t *testing.T) {
	b := testBatch()
	runner := func(string) ([]map[string]any, error) {
		return []map[string]any{
			{"v": int64(2)},
			{"v": nil},
		}, nil
	}

	probe := func(val int64, not bool) *InSubquery {
		return &InSubquery{Expr: &Lit{Val: val}, SQL: "q", Runner: runner, Not: not}
	}

	// 1 IN {2, NULL} → UNKNOWN.
	if got := probe(1, false).Eval(b, 0); got != nil {
		t.Errorf("1 IN {2, NULL}: Eval = %#v, want nil", got)
	}
	// 1 NOT IN {2, NULL} → UNKNOWN, and its WHERE collapse excludes the row.
	e := probe(1, true)
	if got := e.Eval(b, 0); got != nil {
		t.Errorf("1 NOT IN {2, NULL}: Eval = %#v, want nil", got)
	}
	if e.EvalBool(b, 0) {
		t.Error("1 NOT IN {2, NULL}: EvalBool = true, want false (row must be excluded)")
	}
	// 2 IN {2, NULL} → true: a match beats the NULL.
	if got := probe(2, false).Eval(b, 0); got != true {
		t.Errorf("2 IN {2, NULL}: Eval = %#v, want true", got)
	}
	// 2 NOT IN {2, NULL} → false.
	if got := probe(2, true).Eval(b, 0); got != false {
		t.Errorf("2 NOT IN {2, NULL}: Eval = %#v, want false", got)
	}

	// Control: no NULL in the set keeps NOT IN a plain anti-membership.
	plain := func(string) ([]map[string]any, error) {
		return []map[string]any{{"v": int64(2)}}, nil
	}
	ctrl := &InSubquery{Expr: &Lit{Val: int64(1)}, SQL: "q", Runner: plain, Not: true}
	if got := ctrl.Eval(b, 0); got != true {
		t.Errorf("1 NOT IN {2}: Eval = %#v, want true", got)
	}
}
