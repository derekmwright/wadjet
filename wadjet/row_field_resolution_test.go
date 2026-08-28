package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

	// #604: a field path naming NO field of a ROW column must ERROR at plan
	// time (42703-class), the way an unknown top-level column already does —
	// not resolve to a column of NULLs indistinguishable from a genuinely
	// NULL field. attrs is ROW(name STRING, score INT64).
	t.Run("dotted_row_field_typo_errors", func(t *testing.T) {
		cases := []struct{ name, sql string }{
			{"select_list", `SELECT attrs.nosuch FROM events`},
			{"select_list_aliased", `SELECT attrs.nosuch AS x FROM events`},
			{"where", `SELECT id FROM events WHERE attrs.nosuch > 1`},
			{"order_by", `SELECT id FROM events ORDER BY attrs.nosuch`},
			{"group_by", `SELECT attrs.nosuch, count(*) FROM events GROUP BY attrs.nosuch`},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				_, err := db.Query(ctx, c.sql)
				if err == nil {
					t.Fatalf("nonexistent ROW field must error, not resolve to NULL: %s", c.sql)
				}
				if !strings.Contains(err.Error(), "nosuch") {
					t.Fatalf("error should name the field: %v", err)
				}
				if !strings.Contains(err.Error(), "record data type") {
					t.Fatalf("error should be the composite-field 42703 shape: %v", err)
				}
			})
		}
	})

	t.Run("dotted_row_valid_field_resolves", func(t *testing.T) {
		// The valid field must still return its value, not be caught by the
		// new refusal. score is INT64; row 0's attrs.score is 0 in the source.
		res, err := db.Query(ctx, `SELECT id, attrs.score AS sc FROM events WHERE id = 0`)
		if err != nil {
			t.Fatalf("valid ROW field wrongly rejected: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(res.Rows))
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

// TestRowFieldPathNestedPlanTimeValidation checks the #604 refusal over a ROW
// column whose fields include a NESTED ROW: a field path naming one of the
// declared fields — flat or the nested-ROW field itself — resolves, and one
// naming no field of the top-level ROW errors at plan time (42703-class),
// exactly as an unknown top-level column does.
func TestRowFieldPathNestedPlanTimeValidation(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	// rw ROW(flat INT64, sub ROW(deep STRING))
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "rw", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "flat", Type: parquet.TypeInt64, Nullable: true},
			{Name: "sub", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "deep", Type: parquet.TypeString, Nullable: true},
			}},
		}},
	}}
	if err := db.CreateTable(ctx, "nested_rw", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"k": int64(1), "rw": map[string]any{"flat": int64(10), "sub": map[string]any{"deep": "x"}}},
		{"k": int64(2), "rw": map[string]any{"flat": int64(20), "sub": map[string]any{"deep": "y"}}},
	}
	ing := db.NewIngester("nested_rw", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("valid_flat_field_resolves", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT k, rw.flat AS v FROM nested_rw ORDER BY k`)
		if err != nil {
			t.Fatalf("valid flat field rejected: %v", err)
		}
		if len(res.Rows) != 2 {
			t.Fatalf("rows = %d, want 2", len(res.Rows))
		}
	})

	t.Run("valid_nested_row_field_resolves", func(t *testing.T) {
		// sub is a field whose type is itself a ROW — a valid path.
		if _, err := db.Query(ctx, `SELECT rw.sub FROM nested_rw LIMIT 1`); err != nil {
			t.Fatalf("valid nested-ROW field rejected: %v", err)
		}
	})

	t.Run("nonexistent_field_errors", func(t *testing.T) {
		_, err := db.Query(ctx, `SELECT rw.nope FROM nested_rw`)
		if err == nil {
			t.Fatal("nonexistent ROW field must error, not resolve to NULL")
		}
		if !strings.Contains(err.Error(), "nope") || !strings.Contains(err.Error(), "record data type") {
			t.Fatalf("error should be the composite-field 42703 shape naming the field: %v", err)
		}
	})
}
