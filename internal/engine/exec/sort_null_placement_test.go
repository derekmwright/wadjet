package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// nullPlacementBatch builds a one-column batch where a nil entry is a NULL.
func nullPlacementBatch(t *testing.T, vals ...any) *batch.RecordBatch {
	t.Helper()
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"v": v}
	}
	return batch.FromRows([]parquet.Column{{Name: "v", Type: parquet.TypeInt64, Nullable: true}}, rows)
}

func drainNullable(t *testing.T, s *Sort) []any {
	t.Helper()
	ctx := context.Background()
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	var out []any
	for {
		b, err := s.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			out = append(out, r["v"])
		}
	}
	return out
}

func sameAnySlice(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] == nil || b[i] == nil {
			if a[i] != nil || b[i] != nil {
				return false
			}
			continue
		}
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSortNullPlacementAllDirections pins where NULLs land for every
// combination of direction and explicit placement, plus both defaults.
//
// #343: the multi-key less function applies DESC by NEGATING the comparison
// kernel's result, and the kernel is where null handling lived — so the
// negation flipped null placement along with the values. On a DESC key BOTH
// explicit spellings came out inverted (`NULLS LAST` put NULLs first,
// `NULLS FIRST` put them last) while the DESC default came out right by
// cancelling against the same negation, which is exactly the pattern a spot
// check survives: three of these six cases passed before the fix.
//
// The expectations are DuckDB's, verified against the SF0.01 fixture (see
// benchmarks/tpch/duckdb_compare_test.go, NullOrdering*).
func TestSortNullPlacementAllDirections(t *testing.T) {
	cases := []struct {
		name      string
		order     SortOrder
		nullsLast bool
		want      []any
	}{
		{"ASC NULLS FIRST", Ascending, false, []any{nil, nil, int64(1), int64(2), int64(3)}},
		{"ASC NULLS LAST", Ascending, true, []any{int64(1), int64(2), int64(3), nil, nil}},
		{"DESC NULLS FIRST", Descending, false, []any{nil, nil, int64(3), int64(2), int64(1)}},
		{"DESC NULLS LAST", Descending, true, []any{int64(3), int64(2), int64(1), nil, nil}},
		// The defaults, as resolveNullsLast / SortKeySpec.PlaceNullsLast
		// hand them to the executor: NULLS LAST in both directions.
		{"ASC default", Ascending, true, []any{int64(1), int64(2), int64(3), nil, nil}},
		{"DESC default", Descending, true, []any{int64(3), int64(2), int64(1), nil, nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSort([]SortKey{{Column: "v", Order: tc.order, NullsLast: tc.nullsLast}})
			if err := s.Consume(context.Background(), nullPlacementBatch(t, int64(2), nil, int64(3), nil, int64(1))); err != nil {
				t.Fatal(err)
			}
			got := drainNullable(t, s)
			if !sameAnySlice(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSortNullPlacementMultiKey pins the same rule on a secondary key, where
// the direction of key 1 must not leak into key 2's null placement.
func TestSortNullPlacementMultiKey(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	rows := []map[string]any{
		{"g": int64(1), "v": nil},
		{"g": int64(1), "v": int64(7)},
		{"g": int64(2), "v": nil},
		{"g": int64(2), "v": int64(9)},
	}
	s := NewSort([]SortKey{
		{Column: "g", Order: Descending, NullsLast: true},
		{Column: "v", Order: Descending, NullsLast: true},
	})
	if err := s.Consume(context.Background(), batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	var got []string
	for {
		b, err := s.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			if r["v"] == nil {
				got = append(got, "NULL")
			} else {
				got = append(got, "val")
			}
		}
	}
	// g DESC puts group 2 first; inside each group v DESC NULLS LAST puts
	// the value before the NULL.
	want := []string{"val", "NULL", "val", "NULL"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// TestSortNullPlacementExternalMerge runs the same matrix through the
// external-merge path, whose comparison kernels are resolved by a SECOND
// call site (newRunMerger). #330 fixed the wire spec and #343 the kernel
// choice; both had to be fixed in two places, and only one of them is
// exercised by an in-memory sort.
func TestSortNullPlacementExternalMerge(t *testing.T) {
	cases := []struct {
		name      string
		order     SortOrder
		nullsLast bool
		wantHead  any // first row after the merge
	}{
		{"ASC NULLS FIRST", Ascending, false, nil},
		{"ASC NULLS LAST", Ascending, true, int64(1)},
		{"DESC NULLS FIRST", Descending, false, nil},
		{"DESC NULLS LAST", Descending, true, int64(6)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			schema := []parquet.Column{{Name: "v", Type: parquet.TypeInt64, Nullable: true}}
			keys := []SortKey{{Column: "v", Order: tc.order, NullsLast: tc.nullsLast}}

			var runs []string
			for _, vals := range [][]any{{int64(2), nil, int64(5)}, {int64(1), int64(6), nil}} {
				b := nullPlacementBatch(t, vals...)
				path, err := sortBatchesToRun(dir, schema, []*batch.RecordBatch{b}, b.Len, keys, 0)
				if err != nil {
					t.Fatal(err)
				}
				runs = append(runs, path)
			}
			merger, runs, err := openRunMerger(dir, schema, keys, runs)
			if err != nil {
				t.Fatal(err)
			}
			defer merger.close()
			defer removeRunFiles(runs)

			b, err := merger.Next()
			if err != nil {
				t.Fatal(err)
			}
			if b == nil || b.Len == 0 {
				t.Fatal("merger produced no rows")
			}
			got := b.ToRows()[0]["v"]
			if got != tc.wantHead {
				t.Errorf("first merged row = %v, want %v", got, tc.wantHead)
			}
		})
	}
}
