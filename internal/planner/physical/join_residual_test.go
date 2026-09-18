// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// buildJoinResidualFilter compiles an outer join's ON residual into a
// combined-row predicate (#358). These tests pin the evaluation rules:
// two-sided column resolution (bare, qualified, and build-alias forms),
// literal and arithmetic operands with integer division truncating the way
// the engine's `/` does, and SQL three-valued logic — UNKNOWN rejects, and
// NOT of UNKNOWN stays UNKNOWN.
//
// Since #1153 the evaluator IS the engine's expression compiler, so the same
// rules have to hold for a function call, a CAST, LIKE, IN, BETWEEN and CASE
// as well — the cells for those are in jrResidualFn below.

// residualFilter is the factory this file's cells evaluate through: one
// evaluator per call, the way exec.HashJoin.Probe mints one per probe.
func residualFilter(t *testing.T, filter, buildAlias string) exec.JoinResidual {
	t.Helper()
	newResidual, err := buildJoinResidualFilter(filter, buildAlias)
	if err != nil {
		t.Fatalf("filter %q did not compile: %v", filter, err)
	}
	return newResidual()
}

func residualBatch(t *testing.T, schema []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	t.Helper()
	src := exec.NewSliceSource(schema, rows)
	if err := src.Init(context.Background()); err != nil {
		t.Fatalf("source init: %v", err)
	}
	b, err := src.Next(context.Background())
	if err != nil || b == nil {
		t.Fatalf("source next: %v (batch=%v)", err, b)
	}
	return b
}

func TestBuildJoinResidualFilter(t *testing.T) {
	probe := residualBatch(t, []parquet.Column{
		{Name: "n_nationkey", Type: parquet.TypeInt64},
		{Name: "n_regionkey", Type: parquet.TypeInt64},
		{Name: "n_name", Type: parquet.TypeString},
	}, []map[string]any{
		{"n_nationkey": int64(7), "n_regionkey": int64(3), "n_name": "x"},
		{"n_nationkey": int64(1), "n_regionkey": int64(0), "n_name": "y"},
		{"n_nationkey": nil, "n_regionkey": int64(3), "n_name": "z"},
	})
	build := residualBatch(t, []parquet.Column{
		{Name: "r_regionkey", Type: parquet.TypeInt64},
		{Name: "r_name", Type: parquet.TypeString},
	}, []map[string]any{
		{"r_regionkey": int64(0), "r_name": "AFRICA"},
		{"r_regionkey": int64(2), "r_name": "ASIA"},
		{"r_regionkey": nil, "r_name": "NOWHERE"},
	})

	cases := []struct {
		name   string
		filter string
		pRow   int
		bRow   int
		want   bool
	}{
		// Cross-side comparison, qualified spellings on both sides.
		{"cross_side_true", "n.n_nationkey > r.r_regionkey", 0, 2 - 1, true}, // 7 > 2
		{"cross_side_false", "n.n_nationkey > r.r_regionkey", 1, 1, false},   // 1 > 2
		// Bare spellings resolve probe-first, then build.
		{"bare_names", "n_nationkey > r_regionkey", 0, 0, true},
		// Literal operand.
		{"literal_true", "r_regionkey < 3", 0, 1, true},
		{"literal_false", "r_regionkey < 2", 0, 1, false},
		// Arithmetic on the build operand — the #351/#358 expression key.
		{"expr_key_true", "n_regionkey = r_regionkey + 3", 0, 0, true},   // 3 = 0+3
		{"expr_key_false", "n_regionkey = r_regionkey + 3", 0, 1, false}, // 3 ≠ 2+3
		// Integer division truncates (PostgreSQL semantics, #369).
		{"int_div_truncates", "n_nationkey / 2 = 3", 0, 0, true}, // 7/2 = 3
		// NULL on either side is UNKNOWN → rejected.
		{"null_probe", "n_nationkey > r_regionkey", 2, 0, false},
		{"null_build", "n_nationkey > r_regionkey", 0, 2, false},
		// NOT of UNKNOWN stays UNKNOWN → rejected, not accepted.
		{"not_of_unknown", "NOT (n_nationkey > r_regionkey)", 2, 0, false},
		{"not_of_false", "NOT (n_nationkey > r_regionkey)", 1, 1, true},
		// Three-valued AND/OR: FALSE AND UNKNOWN = FALSE (reject),
		// TRUE OR UNKNOWN = TRUE (accept).
		{"true_or_unknown", "n_regionkey = 3 OR n_nationkey > r_regionkey", 2, 0, true},
		{"unknown_and_true", "n_nationkey > r_regionkey AND r_regionkey >= 0", 2, 0, false},
		// String comparison.
		{"string_eq", "r_name = 'ASIA'", 0, 1, true},
		{"string_lt", "n_name < r_name", 0, 0, false}, // "x" < "AFRICA" is false
		// IS NULL / IS NOT NULL never yield UNKNOWN.
		{"is_null", "r_regionkey IS NULL", 0, 2, true},
		{"is_not_null", "r_regionkey IS NOT NULL", 0, 2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := residualFilter(t, tc.filter, "r")
			if got := f(probe, tc.pRow, build, tc.bRow); got != tc.want {
				t.Fatalf("%q on probe[%d] × build[%d]: got %v, want %v", tc.filter, tc.pRow, tc.bRow, got, tc.want)
			}
		})
	}
}

// What CANNOT be evaluated at a join must fail to COMPILE, and the refusal
// must NAME the construct — the planner then refuses the query loudly, never
// drops the conjunct (the pre-#351 silent-drop is the defect class this whole
// path exists to bury). Since #1153 the list is short: a residual whose value
// depends on a RELATION this join does not have.
func TestBuildJoinResidualFilterRefusesWhatItCannotEvaluate(t *testing.T) {
	for _, tc := range []struct {
		name, filter, wantErr string
	}{
		{"scalar subquery", "n_nationkey = (SELECT max(r_regionkey) FROM region)", "subquery"},
		{"IN subquery", "n_nationkey IN (SELECT r_regionkey FROM region)", "subquery"},
		{"EXISTS", "EXISTS (SELECT 1 FROM region)", "EXISTS"},
		{"unparseable", "n_nationkey >", "does not parse"},
		{"unknown function", "no_such_function(n_name) = r_name", "no_such_function"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := buildJoinResidualFilter(tc.filter, "r")
			if err == nil {
				t.Fatalf("filter %q compiled to %v; it must be refused so the planner can error loudly", tc.filter, f != nil)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("filter %q refused with %q, which does not name %q", tc.filter, err, tc.wantErr)
			}
		})
	}
}

// A REFERENCE THAT RESOLVES ON NEITHER SIDE MAKES THE RESIDUAL UNKNOWN, and
// UNKNOWN REJECTS.
//
// The planner ships a residual's columns through NeededColumns, so a miss here
// is a plan bug — but the DISPOSITION of that bug is the thing to pin, because
// the two directions are opposites. Rejecting every candidate NULL-pads each
// preserved row of a LEFT join; accepting every candidate emits the join's
// whole CROSS PRODUCT. A materialization that declines (arc L1's lifted
// predicate under an enclosing star) reaches exactly this path, and five of
// that arc's pinned cells went from three padded rows to twelve when an
// unbound slot read its zero value instead of NULL.
func TestBuildJoinResidualFilterRejectsWhenAReferenceResolvesOnNeitherSide(t *testing.T) {
	probe := residualBatch(t, []parquet.Column{{Name: "p", Type: parquet.TypeInt64}},
		[]map[string]any{{"p": int64(1)}, {"p": int64(2)}})
	build := residualBatch(t, []parquet.Column{{Name: "b", Type: parquet.TypeInt64}},
		[]map[string]any{{"b": int64(1)}, {"b": int64(9)}})
	for _, tc := range []struct {
		filter string
		accept bool
	}{
		{"nosuchcol < b", false},           // one side unbound
		{"p < nosuchcol", false},           // the other side
		{"nosuchcol = othernosuch", false}, // both
		{"UPPER(nosuchcol) = 'X'", false},  // through a function
		{"nosuchcol IS NOT NULL", false},   // a predicate that is TRUE on a value
		// The reference is NULL, not "absent": what an expression DOES with a
		// NULL is the expression's business, and COALESCE answers 1. The claim
		// is that the slot reads NULL, not that every shape rejects.
		{"COALESCE(nosuchcol, 1) = 1", true},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			f := residualFilter(t, tc.filter, "r")
			for pr := 0; pr < 2; pr++ {
				for br := 0; br < 2; br++ {
					if got := f(probe, pr, build, br); got != tc.accept {
						t.Fatalf("%q on probe[%d] x build[%d]: got %v, want %v — an unbound "+
							"reference is SQL NULL", tc.filter, pr, br, got, tc.accept)
					}
				}
			}
		})
	}
}

// A self-join residual: both sides expose the same bare column names, so the
// build alias is what decides sidedness for its qualified references while
// the probe alias's references fall through to the probe by bare name.
func TestBuildJoinResidualFilterSelfJoinAliases(t *testing.T) {
	schema := []parquet.Column{
		{Name: "s_suppkey", Type: parquet.TypeInt64},
		{Name: "s_nationkey", Type: parquet.TypeInt64},
	}
	probe := residualBatch(t, schema, []map[string]any{
		{"s_suppkey": int64(1), "s_nationkey": int64(5)},
	})
	build := residualBatch(t, schema, []map[string]any{
		{"s_suppkey": int64(9), "s_nationkey": int64(5)},
		{"s_suppkey": int64(0), "s_nationkey": int64(5)},
	})
	f := residualFilter(t, "a.s_suppkey < b.s_suppkey", "b")
	if !f(probe, 0, build, 0) { // 1 < 9
		t.Error("probe 1 < build 9 rejected")
	}
	if f(probe, 0, build, 1) { // 1 < 0
		t.Error("probe 1 < build 0 accepted")
	}
}
