// SPDX-License-Identifier: MIT

package physical

import (
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// TestCastMadeExactInReadsTheSharedFunctionList holds the plan's quotient
// walk to expr.CastExactnessArgs — the list the runtime walk reads — at every
// registered function and every argument position: a function the walk
// passes through in one half and stops at in the other is the defect class
// (round / ceil / ceiling / floor / trunc passed here and stopped there, so
// the declaration and the kernel disagreed about `ceil(CAST(t.i AS
// NUMERIC)) / t.n`). expr's TestCastExactnessArgsEveryRegisteredNumericFunction
// states each name's disposition.
func TestCastMadeExactInReadsTheSharedFunctionList(t *testing.T) {
	names := expr.DefaultRegistry.Names()
	for _, n := range []string{"ceil", "ceiling", "floor", "round", "trunc", "truncate", "sign", "abs", "mod"} {
		if !expr.DefaultRegistry.Has(n) {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	col := func() plansql.Node { return &plansql.ColRef{Column: "i"} }
	cast := func() plansql.Node { return &plansql.CastNode{Inner: col(), TypeName: "INTEGER"} }
	const nargs = 3
	for _, n := range names {
		idx, _ := expr.CastExactnessArgs(n, nargs)
		want := map[int]bool{}
		for _, i := range idx {
			want[i] = true
		}
		for p := 0; p < nargs; p++ {
			args := []plansql.Node{col(), col(), col()}
			args[p] = cast()
			if got := castMadeExactIn(&plansql.FuncCallNode{Name: n, Args: args}); got != want[p] {
				t.Errorf("%s with the integer CAST at argument %d: physical castMadeExactIn = %v, expr.CastExactnessArgs says %v", n, p, got, want[p])
			}
		}
	}
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"ceil(CAST(i AS NUMERIC))", true},
		{"round(CAST(i AS NUMERIC), 2)", true},
		{"abs(round(CAST(i AS NUMERIC)))", true},
		{"round(CAST(i AS INTEGER) * 1.0)", true},
		{"sign(CAST(i AS NUMERIC)) * 4", true},
		{"COALESCE(trunc(CAST(i AS NUMERIC)), 0)", true},
		{"round(i)", false},
		{"round(CAST(i AS NUMERIC(10,0)))", false},
		{"sqrt(CAST(i AS NUMERIC))", false},
	} {
		node, err := plansql.ParseExpression(tc.sql)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.sql, err)
		}
		if got := castMadeExactIn(node); got != tc.want {
			t.Errorf("castMadeExactIn(%s) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}
