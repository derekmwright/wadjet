// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ARC FR — AN ON CLAUSE IS NEVER SILENTLY DROPPED (#1229).
//
// A join key that resolves to no column of its side is encoded as ONE FLAG
// BYTE for every row. When NEITHER side resolves, both sides encode the same
// byte, every probe row matches every build row, and the join answers the
// CROSS PRODUCT with no error at all — a silent wrong ROW SET.
//
// The plan-time repair is `physical.SubtreeNaming.ownsKey`, which decides a
// key's side by QUALIFIER when the relation's column list is unknown. This is
// the backstop under it: whatever put the names there, a key pair that names
// columns and finds none on either side refuses.
//
// The ON-TRUE sentinel (`1 = 1`) is the one pair that resolves to nothing on
// both sides ON PURPOSE — it is how a cross product reaches this operator —
// and it is told apart by the key's own text.
func TestArcFRAJoinKeyPairThatResolvesToNothingRefuses(t *testing.T) {
	probeSchema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeString},
	}
	probeRows := []map[string]any{
		{"a": int64(1), "b": "p"}, {"a": int64(2), "b": "q"},
		{"a": int64(3), "b": "r"}, {"a": int64(4), "b": "s"},
	}
	buildSchema := []parquet.Column{
		{Name: "c", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"c": int64(2), "d": "x"}, {"c": int64(3), "d": "y"},
	}

	run := func(t *testing.T, leftKeys, rightKeys []string) (int, error) {
		t.Helper()
		hj := NewHashJoin(InnerJoin, leftKeys, rightKeys)
		hj.BuildFromRows(buildSchema, buildRows)
		sink := &CollectSink{}
		pipe := &Pipeline{
			Source: NewSliceSource(probeSchema, probeRows),
			Ops:    []UnaryOperator{hj.Probe()},
			Sink:   sink,
		}
		if err := pipe.Run(context.Background()); err != nil {
			return 0, err
		}
		return len(sink.Rows), nil
	}

	t.Run("neither_side_resolves_and_both_keys_name_columns", func(t *testing.T) {
		// The #1229 shape: the ON was written right-arm-first and each key
		// landed on the arm that does not have it. 4 × 2 = 8 rows is the
		// cross product; PostgreSQL answers 2 for this condition.
		n, err := run(t, []string{"r2.c"}, []string{"r1.a"})
		if err == nil {
			t.Fatalf("answered %d rows with the condition dropped; the pair names columns "+
				"neither side has and must refuse", n)
		}
		for _, want := range []string{"r2.c", "r1.a", "cross product"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal does not mention %q: %v", want, err)
			}
		}
	})

	t.Run("the_on_true_sentinel_still_crosses", func(t *testing.T) {
		// The optimizer's `1 = 1` reaches the operator as two LITERAL keys.
		// It MEANS the cross product and must keep answering one.
		n, err := run(t, []string{"1"}, []string{"1"})
		if err != nil {
			t.Fatalf("the ON-TRUE sentinel was refused: %v", err)
		}
		if n != 8 {
			t.Errorf("the ON-TRUE sentinel answered %d rows, want 8 (4 × 2)", n)
		}
	})

	t.Run("a_string_literal_sentinel_still_crosses", func(t *testing.T) {
		n, err := run(t, []string{"'x'"}, []string{"'x'"})
		if err != nil {
			t.Fatalf("a literal key pair was refused: %v", err)
		}
		if n != 8 {
			t.Errorf("a literal key pair answered %d rows, want 8", n)
		}
	})

	t.Run("one_side_resolving_is_not_this_refusal", func(t *testing.T) {
		// A key present on the probe and absent from the build matches
		// nothing rather than everything, which is a different question with
		// a different answer; this backstop does not claim it.
		n, err := run(t, []string{"a"}, []string{"zz"})
		if err != nil {
			t.Fatalf("a one-sided miss was refused by the cross-product backstop: %v", err)
		}
		if n != 0 {
			t.Errorf("a one-sided miss answered %d rows, want 0", n)
		}
	})

	t.Run("a_resolvable_pair_answers", func(t *testing.T) {
		n, err := run(t, []string{"a"}, []string{"c"})
		if err != nil {
			t.Fatalf("a resolvable pair was refused: %v", err)
		}
		if n != 2 {
			t.Errorf("answered %d rows, want 2", n)
		}
	})
}
