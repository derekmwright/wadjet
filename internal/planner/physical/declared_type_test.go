package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// nodeDeclaredType is where a projection's output vector type comes from, so a
// wrong answer here is a kernel writing values into a vector that cannot hold
// them — silently, since the write is simply dropped.
//
// The case that named this file is #331:
//
//	SELECT COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback') FROM nation
//
// came back as the integer 0 on every row. coalesce mirrors the type of the
// first argument that decides one; its argument 0 is nullif, which mirrors ITS
// argument 0 — a bare column, whose type is not known until the input schema
// arrives at runtime. Unable to decide, nullif answered with its numeric
// fallback and said nothing about that being a guess, so coalesce stopped
// there and never asked the string literal in argument 1.
//
// The fallback is not the defect: NULLIF(int_col, 1) as a projection is
// numeric and has nothing better to go on, which the last cases below pin.
func TestNodeDeclaredTypeThroughNestedCalls(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		want  parquet.TypeID
		wantC expr.Confidence
	}{
		{
			name: "nested guess does not stop the search — a later argument decides",
			sql:  "COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback')",
			// Float64 here is the bug: a string column typed numeric, whose
			// values are dropped on the way out.
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "two levels of nesting",
			sql:  "COALESCE(NULLIF(NULLIF(n_name, 'ALGERIA'), 'BRAZIL'), 'fallback')",
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "three levels of nesting",
			sql:  "COALESCE(NULLIF(NULLIF(NULLIF(n_name, 'a'), 'b'), 'c'), 'fallback')",
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "guess through a fixed-return call in between",
			sql:  "COALESCE(UPPER(NULLIF(n_name, 'ALGERIA')), 'fallback')",
			// upper() is RetString, so this one was never in doubt — the
			// control that says the recursion itself is not what broke.
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "ifnull mirrors two arguments, and skips the guess in the first",
			sql:  "IFNULL(NULLIF(n_name, 'ALGERIA'), 'fallback')",
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "greatest consults every argument",
			sql:  "GREATEST(NULLIF(n_name, 'ALGERIA'), 'MMM')",
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "nothing decides: the fallback answers, as a guess",
			sql:  "COALESCE(NULLIF(n_name, 'ALGERIA'), n_comment)",
			want: parquet.TypeFloat64, wantC: expr.Guessed,
		},
		{
			name: "a bare column decides nothing",
			sql:  "n_name",
			want: 0, wantC: expr.Undecided,
		},
		{
			name: "numeric nullif keeps its numeric fallback",
			sql:  "NULLIF(n_regionkey, 1)",
			want: parquet.TypeFloat64, wantC: expr.Guessed,
		},
		{
			name: "numeric stays numeric through a nested guess",
			sql:  "COALESCE(NULLIF(n_regionkey, 0), 1)",
			want: parquet.TypeInt64, wantC: expr.Decided,
		},
	}
	for _, tc := range tests {
		node, err := plansql.ParseExpression(tc.sql)
		if err != nil {
			t.Fatalf("%s: parse %q: %v", tc.name, tc.sql, err)
		}
		got, c := nodeDeclaredType(node)
		if got != tc.want || c != tc.wantC {
			t.Errorf("%s\n  %s\n  declared (%s, %s), want (%s, %s)",
				tc.name, tc.sql, got, c, tc.want, tc.wantC)
		}
	}
}

// The projection's own fallback is only reached when nothing decided a type at
// all: a polymorphic function's guess is an answer and outranks it. Without
// that, NULLIF(int_col, 1) would be typed String here — the fallback a
// projection starts from — and its numeric kernel would write into a vector
// with no Float64Data.
func TestInferProjectionTypeUsesAGuessOverItsOwnFallback(t *testing.T) {
	tests := []struct {
		sql  string
		want parquet.TypeID
	}{
		{"COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback')", parquet.TypeString},
		{"NULLIF(n_regionkey, 1)", parquet.TypeFloat64},
		{"COALESCE(NULLIF(n_name, 'ALGERIA'), n_comment)", parquet.TypeFloat64},
		{"n_name", parquet.TypeString}, // undecided: the caller's fallback
	}
	for _, tc := range tests {
		node, err := plansql.ParseExpression(tc.sql)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.sql, err)
		}
		if got := inferProjectionType(node, parquet.TypeString); got != tc.want {
			t.Errorf("%s: projection typed %s, want %s", tc.sql, got, tc.want)
		}
	}
}
