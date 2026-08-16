package expr

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The per-batch memo (tryEvalMemoized) must produce byte-identical output
// to the plain per-row fallback for the regexp family: duplicated inputs,
// NULLs, non-matching rows, and empty strings all covered.
func TestMemoizedRegexpEvalMatchesPerRow(t *testing.T) {
	const n = 64
	vals := []any{
		"https://www.example.com/path/x", // matching, duplicated heavily
		"http://other.org/y/z",           // matching
		"not-a-url",                      // non-matching
		"",                               // empty
		nil,                              // NULL
	}
	col := batch.NewVector(batch.TypeString, n)
	for i := 0; i < n; i++ {
		v := vals[i%len(vals)]
		if v == nil {
			col.Nulls.SetNull(i)
			col.BytesData.Set(i, nil)
		} else {
			col.BytesData.Set(i, []byte(v.(string)))
		}
	}
	b := &batch.RecordBatch{
		Schema:  []parquet.Column{{Name: "s", Type: parquet.TypeString}},
		Columns: []*batch.Vector{col},
		Len:     n,
	}

	for _, tc := range []struct {
		name string
		fc   *FuncCall
	}{
		{"replace", &FuncCall{Name: "regexp_replace", Args: []Expr{
			&ColRef{Name: "s"}, &Lit{Val: `^https?://(?:www\.)?([^/]+)/.*$`}, &Lit{Val: `\1`}}}},
		{"extract", &FuncCall{Name: "regexp_extract", Args: []Expr{
			&ColRef{Name: "s"}, &Lit{Val: `https?://([^/]+)`}, &Lit{Val: int64(1)}}}},
		{"like", &FuncCall{Name: "regexp_like", Args: []Expr{
			&ColRef{Name: "s"}, &Lit{Val: `^https`}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := tc.fc

			// Reference: plain per-row Eval.
			want := make([]any, n)
			for i := 0; i < n; i++ {
				want[i] = fc.Eval(b, i)
			}

			// Memoized batch path. regexp fns have no vec kernel, so
			// EvalVec routes through tryEvalMemoized; assert it engaged.
			out := batch.NewVector(batch.TypeString, n)
			if tc.name == "like" {
				out = batch.NewVector(batch.TypeBool, n)
			}
			if !fc.tryEvalMemoized(b, out, n) {
				t.Fatal("tryEvalMemoized did not engage")
			}
			for i := 0; i < n; i++ {
				var got any
				if out.Nulls.IsNull(i) {
					got = nil
				} else {
					got = out.GetValue(i)
				}
				if fmt.Sprint(got) != fmt.Sprint(want[i]) || (got == nil) != (want[i] == nil) {
					t.Fatalf("row %d: memo %v (%T), per-row %v (%T)", i, got, got, want[i], want[i])
				}
			}
		})
	}
}
