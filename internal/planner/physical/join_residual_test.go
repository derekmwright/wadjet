package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BuildJoinResidualFilter compiles an outer join's ON residual into a
// combined-row predicate (#358). These tests pin the evaluation rules:
// two-sided column resolution (bare, qualified, and build-alias forms),
// literal and arithmetic operands with integer division truncating the way
// the engine's `/` does, and SQL three-valued logic — UNKNOWN rejects, and
// NOT of UNKNOWN stays UNKNOWN.

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
			f := BuildJoinResidualFilter(tc.filter, "r")
			if f == nil {
				t.Fatalf("filter %q did not compile", tc.filter)
			}
			if got := f(probe, tc.pRow, build, tc.bRow); got != tc.want {
				t.Fatalf("%q on probe[%d] × build[%d]: got %v, want %v", tc.filter, tc.pRow, tc.bRow, got, tc.want)
			}
		})
	}
}

// A shape the interpreter does not evaluate must fail to COMPILE — the
// planner then refuses the query loudly, never drops the conjunct (the
// pre-#351 silent-drop is the defect class this whole path exists to bury).
func TestBuildJoinResidualFilterRefusesUnsupported(t *testing.T) {
	for _, filter := range []string{
		"lower(n_name) = r_name",                             // function call
		"n_name LIKE 'A%'",                                   // LIKE
		"n_nationkey IN (1, 2)",                              // IN
		"n_nationkey BETWEEN 1 AND 5",                        // BETWEEN
		"CASE WHEN n_nationkey > 1 THEN true ELSE false END", // CASE
	} {
		if f := BuildJoinResidualFilter(filter, "r"); f != nil {
			t.Errorf("filter %q compiled; it must be refused so the planner can error loudly", filter)
		}
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
	f := BuildJoinResidualFilter("a.s_suppkey < b.s_suppkey", "b")
	if f == nil {
		t.Fatal("self-join residual did not compile")
	}
	if !f(probe, 0, build, 0) { // 1 < 9
		t.Error("probe 1 < build 9 rejected")
	}
	if f(probe, 0, build, 1) { // 1 < 0
		t.Error("probe 1 < build 0 accepted")
	}
}
