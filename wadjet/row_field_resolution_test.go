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

// TestPlanTimeColumnValidation covers the #147 remainder: plan-time name
// binding that rejects unknown columns beyond the single-table WHERE case —
// notably the SELECT-list (legacy projection) path that previously emitted an
// all-NULL column, plus GROUP BY / ORDER BY — while never rejecting a valid
// query (the false-positive guard for dotted ROW access and ordinary refs).
func TestPlanTimeColumnValidation(t *testing.T) {
	ctx := context.Background()
	db, _ := ndOpen(t)

	t.Run("select_list_typo_errors", func(t *testing.T) {
		_, err := db.Query(ctx, `SELECT nosuchcol FROM events`)
		if err == nil {
			t.Fatal("typo'd SELECT column must error at plan time, not project all-NULL")
		}
		if !strings.Contains(err.Error(), "nosuchcol") {
			t.Fatalf("error should name the column: %v", err)
		}
	})

	t.Run("group_by_typo_errors", func(t *testing.T) {
		if _, err := db.Query(ctx, `SELECT count(*) FROM events GROUP BY nope`); err == nil {
			t.Fatal("typo'd GROUP BY column must error")
		}
	})

	t.Run("order_by_typo_errors", func(t *testing.T) {
		if _, err := db.Query(ctx, `SELECT id FROM events ORDER BY nope`); err == nil {
			t.Fatal("typo'd ORDER BY column must error")
		}
	})

	t.Run("dotted_row_select_not_false_positive", func(t *testing.T) {
		// attrs is a ROW column; attrs.score is field access, not an unknown table.
		if _, err := db.Query(ctx, `SELECT attrs.score FROM events LIMIT 1`); err != nil {
			t.Fatalf("dotted ROW access wrongly rejected: %v", err)
		}
	})

	t.Run("valid_query_unaffected", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT id, grp FROM events WHERE id < 5`)
		if err != nil {
			t.Fatalf("valid query rejected: %v", err)
		}
		if len(res.Rows) != 5 {
			t.Fatalf("rows = %d, want 5", len(res.Rows))
		}
	})
}
