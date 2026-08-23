package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #415 across the Sort operator's four paths. The in-memory sort, the
// streaming k-way merge over spilled sorted runs, the top-K heap and the
// external-merge run format must all put a container column in the SAME
// order — the failure #394 named for DECIMAL ("same query, three different
// sequences depending on which path answered") applies verbatim to ARRAY,
// ROW, MAP and VECTOR, which until now had NO order at all.

var containerSortSchema = []parquet.Column{
	{Name: "v", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	{Name: "id", Type: parquet.TypeInt64},
}

// containerSortInput is in neither ascending nor descending order, so a
// comparator that reports every row equal returns this sequence unchanged.
func containerSortInput() []map[string]any {
	vals := []any{
		[]any{"b"},
		[]any{"a", "b"},
		[]any{},
		[]any{"a"},
		[]any{"a", "a"},
		[]any{"c", "a"},
	}
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"v": v, "id": int64(i)}
	}
	return rows
}

// containerSortAscIDs is the id sequence of the ascending order:
// [] < ["a"] < ["a","a"] < ["a","b"] < ["b"] < ["c","a"].
var containerSortAscIDs = []int64{2, 3, 4, 1, 0, 5}

func containerSortIDs(tb testing.TB, got []map[string]any) []int64 {
	tb.Helper()
	out := make([]int64, len(got))
	for i, r := range got {
		v, ok := r["id"].(int64)
		if !ok {
			tb.Fatalf("row %d: id came back as %T (%v)", i, r["id"], r["id"])
		}
		out[i] = v
	}
	return out
}

func assertContainerOrder(tb testing.TB, arm string, got, want []int64) {
	tb.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		tb.Fatalf("%s: ORDER BY over ARRAY returned ids %v, want %v", arm, got, want)
	}
}

func TestSortArrayKeyLexicographicOrder(t *testing.T) {
	ctx := context.Background()

	t.Run("in-memory", func(t *testing.T) {
		s := NewSort([]SortKey{{Column: "v", Order: Ascending}, {Column: "id", Order: Ascending}})
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(ctx, batch.FromRows(containerSortSchema, containerSortInput())); err != nil {
			t.Fatal(err)
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		assertContainerOrder(t, "in-memory", containerSortIDs(t, drainSortRows(t, s)), containerSortAscIDs)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("descending", func(t *testing.T) {
		s := NewSort([]SortKey{{Column: "v", Order: Descending}, {Column: "id", Order: Ascending}})
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Consume(ctx, batch.FromRows(containerSortSchema, containerSortInput())); err != nil {
			t.Fatal(err)
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		want := make([]int64, len(containerSortAscIDs))
		for i := range want {
			want[i] = containerSortAscIDs[len(containerSortAscIDs)-1-i]
		}
		assertContainerOrder(t, "descending", containerSortIDs(t, drainSortRows(t, s)), want)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("nulls", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			nullsLast bool
			want      []int64
		}{
			{"nulls-first", false, append([]int64{6}, containerSortAscIDs...)},
			{"nulls-last", true, append(append([]int64{}, containerSortAscIDs...), 6)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows := append(containerSortInput(), map[string]any{"v": nil, "id": int64(6)})
				s := NewSort([]SortKey{
					{Column: "v", Order: Ascending, NullsLast: tc.nullsLast},
					{Column: "id", Order: Ascending},
				})
				if err := s.Init(ctx); err != nil {
					t.Fatal(err)
				}
				if err := s.Consume(ctx, batch.FromRows(containerSortSchema, rows)); err != nil {
					t.Fatal(err)
				}
				if err := s.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
				assertContainerOrder(t, tc.name, containerSortIDs(t, drainSortRows(t, s)), tc.want)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("external-merge", func(t *testing.T) {
		forceTinyRuns(t)
		s := newSortSpillHarness(t, []SortKey{
			{Column: "v", Order: Ascending},
			{Column: "id", Order: Ascending},
		}, 512)
		// One row per batch, so every value crosses a run boundary and the
		// k-way merge — reading containers back out of the run format —
		// decides the answer.
		for _, row := range containerSortInput() {
			if err := s.Consume(ctx, batch.FromRows(containerSortSchema, []map[string]any{row})); err != nil {
				t.Fatal(err)
			}
		}
		if len(s.runFiles) == 0 {
			t.Fatal("run-spill path was never exercised; budget/floor setup is wrong")
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		assertContainerOrder(t, "external-merge", containerSortIDs(t, drainSortRows(t, s)), containerSortAscIDs)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("top-k", func(t *testing.T) {
		s := NewSort([]SortKey{{Column: "v", Order: Ascending}, {Column: "id", Order: Ascending}})
		s.Limit = 3
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		for _, row := range containerSortInput() {
			if err := s.Consume(ctx, batch.FromRows(containerSortSchema, []map[string]any{row})); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		assertContainerOrder(t, "top-k", containerSortIDs(t, drainSortRows(t, s)), containerSortAscIDs[:3])
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
