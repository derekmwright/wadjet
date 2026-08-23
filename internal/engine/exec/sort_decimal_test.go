package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decimalSortSchema is a DECIMAL sort key plus an int tiebreaker that records
// the arrival order, so a comparator that reports every row equal is visible
// as "the input sequence came back".
var decimalSortSchema = []parquet.Column{
	{Name: "amt", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	{Name: "id", Type: parquet.TypeInt64},
}

// decimalSortInput is deliberately in neither numeric nor lexicographic order.
// The three orders differ on this data:
//
//	numeric:       -3.5  0.0  0.0001  2.0002  9.9  10.001  100.25
//	lexicographic: -3.5  0.0  0.0001  10.001  100.25  2.0002  9.9
//	input:         the sequence below
//
// The INPUT is spelled loosely — ParseDecimalString rescales it to the
// column's declared 4 — while the expectation is spelled at the declared
// scale, which is what FormatDecimal renders now that a DECIMAL's text form
// is its declared width (#453).
var decimalSortInput = []string{"10.001", "0.0001", "100.25", "-3.5", "9.9", "0.0", "2.0002"}

var decimalSortNumericAsc = []string{"-3.5000", "0.0000", "0.0001", "2.0002", "9.9000", "10.0010", "100.2500"}

func decimalSortRows(tb testing.TB, withNull bool) []map[string]any {
	tb.Helper()
	rows := make([]map[string]any, 0, len(decimalSortInput)+1)
	for i, s := range decimalSortInput {
		rows = append(rows, map[string]any{"amt": s, "id": int64(i)})
	}
	if withNull {
		rows = append(rows, map[string]any{"amt": nil, "id": int64(len(decimalSortInput))})
	}
	return rows
}

func decimalSortKeys(got []map[string]any) []string {
	out := make([]string, len(got))
	for i, r := range got {
		if r["amt"] == nil {
			out[i] = "<null>"
			continue
		}
		out[i] = fmt.Sprint(r["amt"])
	}
	return out
}

func assertDecimalOrder(tb testing.TB, arm string, got, want []string) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("%s: got %d rows %v, want %d rows %v", arm, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			tb.Fatalf("%s: ORDER BY over DECIMAL returned %v, want %v", arm, got, want)
		}
	}
}

// TestSortDecimalKeyNumericOrder is the regression test for #394. Every
// resolver in internal/engine/exec/kernel/sort.go fell through to a default
// comparator that reported every DECIMAL row EQUAL, so this sort returned its
// INPUT order; the other comparison path in the tree ordered the same column
// by its FORMATTED string, where "10.001" precedes "2.0002". All three of the
// operator's paths — the in-memory sort, the streaming k-way merge over
// spilled sorted runs, and the top-K compaction — must now agree on the one
// numeric answer.
func TestSortDecimalKeyNumericOrder(t *testing.T) {
	ctx := context.Background()

	t.Run("in-memory", func(t *testing.T) {
		s := NewSort([]SortKey{{Column: "amt", Order: Ascending}, {Column: "id", Order: Ascending}})
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(ctx, batch.FromRows(decimalSortSchema, decimalSortRows(t, false))); err != nil {
			t.Fatal(err)
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		assertDecimalOrder(t, "in-memory", decimalSortKeys(drainSortRows(t, s)), decimalSortNumericAsc)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("descending", func(t *testing.T) {
		s := NewSort([]SortKey{{Column: "amt", Order: Descending}, {Column: "id", Order: Ascending}})
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(ctx, batch.FromRows(decimalSortSchema, decimalSortRows(t, false))); err != nil {
			t.Fatal(err)
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		want := make([]string, len(decimalSortNumericAsc))
		for i := range want {
			want[i] = decimalSortNumericAsc[len(decimalSortNumericAsc)-1-i]
		}
		assertDecimalOrder(t, "descending", decimalSortKeys(drainSortRows(t, s)), want)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("nulls", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			nullsLast bool
			want      []string
		}{
			{"nulls-first", false, append([]string{"<null>"}, decimalSortNumericAsc...)},
			{"nulls-last", true, append(append([]string{}, decimalSortNumericAsc...), "<null>")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := NewSort([]SortKey{
					{Column: "amt", Order: Ascending, NullsLast: tc.nullsLast},
					{Column: "id", Order: Ascending},
				})
				if err := s.Init(ctx); err != nil {
					t.Fatal(err)
				}
				if err := s.Consume(ctx, batch.FromRows(decimalSortSchema, decimalSortRows(t, true))); err != nil {
					t.Fatal(err)
				}
				if err := s.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
				assertDecimalOrder(t, tc.name, decimalSortKeys(drainSortRows(t, s)), tc.want)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("external-merge", func(t *testing.T) {
		forceTinyRuns(t)
		s := newSortSpillHarness(t, []SortKey{
			{Column: "amt", Order: Ascending},
			{Column: "id", Order: Ascending},
		}, 512)
		// One row per batch, so every value crosses a run boundary and the
		// answer is decided by the k-way merge rather than one in-memory sort.
		for _, row := range decimalSortRows(t, false) {
			if err := s.Consume(ctx, batch.FromRows(decimalSortSchema, []map[string]any{row})); err != nil {
				t.Fatal(err)
			}
		}
		if len(s.runFiles) == 0 {
			t.Fatal("run-spill path was never exercised; budget/floor setup is wrong")
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		assertDecimalOrder(t, "external-merge", decimalSortKeys(drainSortRows(t, s)), decimalSortNumericAsc)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("top-k", func(t *testing.T) {
		s := NewSort([]SortKey{{Column: "amt", Order: Ascending}, {Column: "id", Order: Ascending}})
		s.Limit = 3
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		// One row per batch: len(s.batches) >= 4 forces compactTopKLocked,
		// so the answer goes through the top-N heap and not just the final
		// sort.
		for _, row := range decimalSortRows(t, false) {
			if err := s.Consume(ctx, batch.FromRows(decimalSortSchema, []map[string]any{row})); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		assertDecimalOrder(t, "top-k", decimalSortKeys(drainSortRows(t, s)), decimalSortNumericAsc[:3])
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
