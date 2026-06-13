package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// Regression tests for issue #147 (partial: filter strictness + dotted ROW
// access). Previously `attrs.score` parsed as a table-qualified reference,
// resolved nothing, and silently evaluated NULL in both SELECT and WHERE —
// and a typo'd WHERE column silently matched nothing, indistinguishable
// from an empty result.
func TestRowFieldResolutionAndFilterStrictness(t *testing.T) {
	ctx := context.Background()
	db, src := ndOpen(t)

	t.Run("dotted_select", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT id, attrs.score AS sc FROM events WHERE id < 30`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != 30 {
			t.Fatalf("rows = %d, want 30", len(res.Rows))
		}
		for _, row := range res.Rows {
			id := row["id"].(int64)
			want := src[id]
			if want.attrs == nil {
				if row["sc"] != nil {
					t.Fatalf("id %d: sc = %v, want NULL (NULL row)", id, row["sc"])
				}
				continue
			}
			wantScore := want.attrs.(map[string]any)["score"].(int64)
			if fmt.Sprintf("%v", row["sc"]) != fmt.Sprintf("%d", wantScore) {
				t.Fatalf("id %d: attrs.score = %v, want %d (dotted access silently NULL?)", id, row["sc"], wantScore)
			}
		}
	})

	t.Run("dotted_where", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT id FROM events WHERE attrs.score > 90`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		want := 0
		for _, r := range src {
			if r.attrs != nil && r.attrs.(map[string]any)["score"].(int64) > 90 {
				want++
			}
		}
		if len(res.Rows) != want {
			t.Fatalf("rows = %d, want %d (kernel filter can't see ROW fields?)", len(res.Rows), want)
		}
	})

	t.Run("dotted_where_conjunction_keeps_vectorized_half", func(t *testing.T) {
		// AND of a kernelizable comparison and a ROW-field comparison —
		// partial vectorization must not drop either predicate.
		res, err := db.Query(ctx, `SELECT id FROM events WHERE id < 1000 AND attrs.score > 90`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		want := 0
		for _, r := range src[:1000] {
			if r.attrs != nil && r.attrs.(map[string]any)["score"].(int64) > 90 {
				want++
			}
		}
		if len(res.Rows) != want {
			t.Fatalf("rows = %d, want %d", len(res.Rows), want)
		}
	})

	t.Run("unknown_filter_column_errors", func(t *testing.T) {
		_, err := db.Query(ctx, `SELECT id FROM events WHERE nosuchcol > 90`)
		if err == nil {
			t.Fatal("typo'd WHERE column must error, not silently match nothing")
		}
		if !strings.Contains(err.Error(), "nosuchcol") {
			t.Fatalf("error should name the column: %v", err)
		}
	})

	t.Run("qualified_table_refs_still_resolve", func(t *testing.T) {
		// Qualifier-strip fallback must keep working for real table aliases.
		res, err := db.Query(ctx, `SELECT e.id FROM events e WHERE e.grp = 'g1' LIMIT 5`)
		if err != nil {
			t.Fatalf("qualified ref query: %v", err)
		}
		if len(res.Rows) != 5 {
			t.Fatalf("rows = %d, want 5", len(res.Rows))
		}
	})
}
