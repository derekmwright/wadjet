package wadjet

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Nested-types + Decimal correctness gate (issue #144).
//
// TPC-H exercises neither nested types (ARRAY/MAP/ROW) nor Decimal in the
// merge path, so every prior gate (SF0.01/SF1/SF100, the local harness) was
// structurally blind to the wrong-results class the 2026-06-12 sweep found
// (the advance-on-NULL family fixed in PR #123, the Decimal merge collapse,
// the NULL-group GROUP BY drop fixed in PR #142). This suite is the missing
// gate: a deterministic dataset with nested columns, interior AND trailing
// NULLs at batch boundaries, a nullable group key, and a Decimal measure,
// pushed through the full SQL surface — every expectation computed
// independently in Go from the same source rows.
//
// The dataset spans multiple 2048-row batches on purpose: pooled-batch
// reuse and offset bookkeeping bugs only fire on the second batch.

const ndRows = 5000 // > 2×2048 so the scan recycles pooled batches twice

type ndRow struct {
	id    int64
	grp   any // string or nil (NULL group key — the PR #142 class)
	amt   any // float64 or nil; DECIMAL(12,2) column
	tags  any // []any of string or nil (NULL array, incl. trailing rows)
	attrs any // map[string]any{name, score} or nil (NULL row)
}

func ndSource() []ndRow {
	rows := make([]ndRow, ndRows)
	for i := range rows {
		id := int64(i)
		r := ndRow{id: id}
		if id%7 != 3 {
			r.grp = fmt.Sprintf("g%d", id%3)
		}
		if id%11 != 5 {
			r.amt = float64(id%50) + 0.25
		}
		if id%5 != 4 { // id%5==4 → NULL array; hits the final row (4999)
			n := int(id % 3) // lengths 0,1,2 — zero-length ≠ NULL
			tags := make([]any, n)
			for j := 0; j < n; j++ {
				tags[j] = fmt.Sprintf("t%d-%d", id%4, j)
			}
			r.tags = tags
		}
		if id%6 != 2 { // id%6==2 → NULL row
			attrs := map[string]any{"score": id % 100}
			if id%13 != 7 { // id%13==7 → NULL name field inside a valid row
				attrs["name"] = fmt.Sprintf("n%d", id%10)
			}
			r.attrs = attrs
		}
		rows[i] = r
	}
	return rows
}

func ndOpen(t *testing.T) (*DB, []ndRow) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "grp", Type: parquet.TypeString, Nullable: true},
		{Name: "amount", Type: parquet.TypeDecimal, Nullable: true, Precision: 12, Scale: 2},
		{Name: "tags", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "attrs", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "name", Type: parquet.TypeString, Nullable: true},
			{Name: "score", Type: parquet.TypeInt64, Nullable: true},
		}},
	}}
	if err := db.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	src := ndSource()
	boxed := make([]map[string]any, len(src))
	for i, r := range src {
		boxed[i] = map[string]any{
			"id": r.id, "grp": r.grp, "amount": r.amt, "tags": r.tags, "attrs": r.attrs,
		}
	}
	ing := db.NewIngester("events", schema, nil, ingest.Config{MaxBufferRows: 10000, RowGroupSize: 1500})
	if err := ing.Ingest(ctx, boxed); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db, src
}

func TestNestedDecimalCorrectness(t *testing.T) {
	ctx := context.Background()
	db, src := ndOpen(t)

	t.Run("full_round_trip", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT id, grp, amount, tags, attrs FROM events`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != ndRows {
			t.Fatalf("rows = %d, want %d", len(res.Rows), ndRows)
		}
		byID := make(map[int64]map[string]any, len(res.Rows))
		for _, row := range res.Rows {
			byID[row["id"].(int64)] = row
		}
		for _, want := range src {
			got, ok := byID[want.id]
			if !ok {
				t.Fatalf("id %d missing from result", want.id)
			}
			// grp (nullable flat string)
			if want.grp == nil {
				if got["grp"] != nil {
					t.Fatalf("id %d: grp = %v, want NULL", want.id, got["grp"])
				}
			} else if got["grp"] != want.grp {
				t.Fatalf("id %d: grp = %v, want %v", want.id, got["grp"], want.grp)
			}
			// amount (Decimal boxes as its formatted string)
			if want.amt == nil {
				if got["amount"] != nil {
					t.Fatalf("id %d: amount = %v, want NULL", want.id, got["amount"])
				}
			} else {
				wantStr := fmt.Sprintf("%.2f", want.amt.(float64))
				if fmt.Sprintf("%v", got["amount"]) != wantStr {
					t.Fatalf("id %d: amount = %v, want %s", want.id, got["amount"], wantStr)
				}
			}
			// tags (nullable ARRAY(STRING)) — NULL vs empty must be distinct,
			// and element values exact (offset corruption reads neighbors'
			// bytes, so any drift shows up as concatenated garbage).
			if want.tags == nil {
				if got["tags"] != nil {
					t.Fatalf("id %d: tags = %#v, want NULL (trailing/interior null array)", want.id, got["tags"])
				}
			} else {
				wantTags := want.tags.([]any)
				gotTags, ok := got["tags"].([]any)
				if !ok {
					t.Fatalf("id %d: tags = %#v (%T), want []any", want.id, got["tags"], got["tags"])
				}
				if len(gotTags) != len(wantTags) {
					t.Fatalf("id %d: tags len = %d (%v), want %d (%v)", want.id, len(gotTags), gotTags, len(wantTags), wantTags)
				}
				for j := range wantTags {
					if fmt.Sprintf("%v", gotTags[j]) != wantTags[j].(string) {
						t.Fatalf("id %d: tags[%d] = %v, want %v", want.id, j, gotTags[j], wantTags[j])
					}
				}
			}
			// attrs (nullable ROW with nullable bytes-typed child)
			if want.attrs == nil {
				if got["attrs"] != nil {
					t.Fatalf("id %d: attrs = %#v, want NULL row", want.id, got["attrs"])
				}
			} else {
				wantAttrs := want.attrs.(map[string]any)
				gotAttrs, ok := got["attrs"].(map[string]any)
				if !ok {
					t.Fatalf("id %d: attrs = %#v (%T), want map", want.id, got["attrs"], got["attrs"])
				}
				if wn, ok := wantAttrs["name"]; ok {
					if fmt.Sprintf("%v", gotAttrs["name"]) != wn.(string) {
						t.Fatalf("id %d: attrs.name = %v, want %v", want.id, gotAttrs["name"], wn)
					}
				} else if gotAttrs["name"] != nil {
					t.Fatalf("id %d: attrs.name = %v, want NULL field", want.id, gotAttrs["name"])
				}
				if fmt.Sprintf("%v", gotAttrs["score"]) != fmt.Sprintf("%v", wantAttrs["score"]) {
					t.Fatalf("id %d: attrs.score = %v, want %v", want.id, gotAttrs["score"], wantAttrs["score"])
				}
			}
		}
	})

	t.Run("null_group_key_aggregate", func(t *testing.T) {
		// The PR #142 class: NULL group keys silently dropped by the
		// int/dual-int fast paths. Count per grp INCLUDING the NULL group.
		res, err := db.Query(ctx, `SELECT grp, COUNT(*) AS c FROM events GROUP BY grp`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		wantCounts := map[string]int64{}
		for _, r := range src {
			key := "<null>"
			if r.grp != nil {
				key = r.grp.(string)
			}
			wantCounts[key]++
		}
		gotCounts := map[string]int64{}
		for _, row := range res.Rows {
			key := "<null>"
			if row["grp"] != nil {
				key = fmt.Sprintf("%v", row["grp"])
			}
			gotCounts[key] += toI64(t, row["c"])
		}
		if len(gotCounts) != len(wantCounts) {
			t.Fatalf("groups = %v, want %v (NULL group dropped?)", gotCounts, wantCounts)
		}
		for k, want := range wantCounts {
			if gotCounts[k] != want {
				t.Fatalf("group %q count = %d, want %d", k, gotCounts[k], want)
			}
		}
	})

	t.Run("decimal_group_key", func(t *testing.T) {
		// Distinct Decimal keys must stay distinct (the sweep's extractValue
		// collapse encoded every Decimal key as "<nil>" — one merge group).
		res, err := db.Query(ctx, `SELECT amount, COUNT(*) AS c FROM events GROUP BY amount`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		want := map[string]int64{}
		for _, r := range src {
			key := "<null>"
			if r.amt != nil {
				key = fmt.Sprintf("%.2f", r.amt.(float64))
			}
			want[key]++
		}
		got := map[string]int64{}
		for _, row := range res.Rows {
			key := "<null>"
			if row["amount"] != nil {
				key = fmt.Sprintf("%v", row["amount"])
			}
			got[key] += toI64(t, row["c"])
		}
		if len(got) != len(want) {
			t.Fatalf("distinct decimal groups = %d, want %d", len(got), len(want))
		}
		for k, w := range want {
			if got[k] != w {
				t.Fatalf("amount %q count = %d, want %d", k, got[k], w)
			}
		}
	})

	t.Run("filter_on_row_field", func(t *testing.T) {
		// row_field() is the supported ROW accessor in predicates. (Dotted
		// `attrs.score` parses but resolves as an unknown table-qualified
		// column and silently evaluates NULL — tracked separately along
		// with unknown-column-silently-empty strictness.)
		res, err := db.Query(ctx, `SELECT id FROM events WHERE row_field(attrs, 'score') > 90`)
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
			t.Fatalf("rows = %d, want %d (NULL rows must not match)", len(res.Rows), want)
		}
	})

	t.Run("project_nested_accessors", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT id, element_at(tags, 1) AS t1, array_length(tags) AS n FROM events WHERE id < 20`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != 20 {
			t.Fatalf("rows = %d, want 20", len(res.Rows))
		}
		for _, row := range res.Rows {
			id := row["id"].(int64)
			r := src[id]
			if r.tags == nil {
				if row["t1"] != nil || row["n"] != nil {
					t.Fatalf("id %d: t1=%v n=%v, want NULLs for NULL array", id, row["t1"], row["n"])
				}
				continue
			}
			tags := r.tags.([]any)
			// Expression outputs box numerics as strings (evaluator
			// convention) — compare formatted, but NULL must stay NULL.
			if row["n"] == nil || fmt.Sprintf("%v", row["n"]) != fmt.Sprintf("%d", len(tags)) {
				t.Fatalf("id %d: array_length = %v, want %d", id, row["n"], len(tags))
			}
			if len(tags) == 0 {
				if row["t1"] != nil {
					t.Fatalf("id %d: element_at on empty = %v, want NULL", id, row["t1"])
				}
			} else if fmt.Sprintf("%v", row["t1"]) != tags[0].(string) {
				t.Fatalf("id %d: element_at(tags,1) = %v, want %v", id, row["t1"], tags[0])
			}
		}
	})

	t.Run("left_join_unmatched_nested", func(t *testing.T) {
		// The setVectorNull class: LEFT JOIN unmatched rows interleaved with
		// matched ones corrupted nested build columns ("aaabbb" instead of
		// "bbb"). Join a probe table where only some ids match.
		ctx := context.Background()
		probeSchema := parquet.Schema{Columns: []parquet.Column{
			{Name: "pid", Type: parquet.TypeInt64},
		}}
		if err := db.CreateTable(ctx, "probe", probeSchema, nil); err != nil {
			t.Fatal(err)
		}
		probe := make([]map[string]any, 0, 300)
		for i := 0; i < 300; i++ {
			probe = append(probe, map[string]any{"pid": int64(i * 20)}) // ids 0..5980, half miss (>= ndRows)
		}
		ing := db.NewIngester("probe", probeSchema, nil, ingest.Config{MaxBufferRows: 1000, RowGroupSize: 1000})
		if err := ing.Ingest(ctx, probe); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}

		res, err := db.Query(ctx, `
			SELECT p.pid, e.attrs, e.tags FROM probe p
			LEFT JOIN events e ON p.pid = e.id`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != 300 {
			t.Fatalf("rows = %d, want 300", len(res.Rows))
		}
		for _, row := range res.Rows {
			pid := row["pid"].(int64)
			if pid >= ndRows {
				// Unmatched: every build-side column must be NULL.
				if row["attrs"] != nil || row["tags"] != nil {
					t.Fatalf("pid %d unmatched: attrs=%#v tags=%#v, want NULLs", pid, row["attrs"], row["tags"])
				}
				continue
			}
			want := src[pid]
			if want.attrs == nil {
				if row["attrs"] != nil {
					t.Fatalf("pid %d: attrs = %#v, want NULL row", pid, row["attrs"])
				}
			} else {
				gotAttrs, ok := row["attrs"].(map[string]any)
				if !ok {
					t.Fatalf("pid %d: attrs = %#v (%T)", pid, row["attrs"], row["attrs"])
				}
				wantAttrs := want.attrs.(map[string]any)
				if wn, ok := wantAttrs["name"]; ok && fmt.Sprintf("%v", gotAttrs["name"]) != wn.(string) {
					t.Fatalf("pid %d: joined attrs.name = %v, want %v (matched/unmatched interleave corruption)",
						pid, gotAttrs["name"], wn)
				}
			}
			if want.tags == nil {
				if row["tags"] != nil {
					t.Fatalf("pid %d: tags = %#v, want NULL", pid, row["tags"])
				}
			} else if wantTags := want.tags.([]any); len(wantTags) > 0 {
				gotTags, ok := row["tags"].([]any)
				if !ok || len(gotTags) != len(wantTags) {
					t.Fatalf("pid %d: tags = %#v, want %v", pid, row["tags"], wantTags)
				}
				for j := range wantTags {
					if fmt.Sprintf("%v", gotTags[j]) != wantTags[j].(string) {
						t.Fatalf("pid %d: tags[%d] = %v, want %v", pid, j, gotTags[j], wantTags[j])
					}
				}
			}
		}
	})

	t.Run("sort_keeps_nested_attached", func(t *testing.T) {
		// The gatherVector class: sort/join gather wrote NOTHING for nested
		// columns — rows came back valid-marked with zeros or another row's
		// leftovers. Sort descending and verify nested values still belong
		// to their id.
		res, err := db.Query(ctx, `SELECT id, attrs, tags FROM events WHERE id < 1000 ORDER BY id DESC`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(res.Rows) != 1000 {
			t.Fatalf("rows = %d, want 1000", len(res.Rows))
		}
		if res.Rows[0]["id"].(int64) != 999 {
			t.Fatalf("first id = %v, want 999", res.Rows[0]["id"])
		}
		for _, row := range res.Rows {
			id := row["id"].(int64)
			want := src[id]
			if (want.attrs == nil) != (row["attrs"] == nil) {
				t.Fatalf("id %d after sort: attrs null mismatch (got %#v)", id, row["attrs"])
			}
			if want.attrs != nil {
				wantAttrs := want.attrs.(map[string]any)
				gotAttrs := row["attrs"].(map[string]any)
				if wn, ok := wantAttrs["name"]; ok && fmt.Sprintf("%v", gotAttrs["name"]) != wn.(string) {
					t.Fatalf("id %d after sort: attrs.name = %v, want %v", id, gotAttrs["name"], wn)
				}
			}
		}
	})

	t.Run("decimal_sum_by_group", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT grp, SUM(amount) AS s FROM events GROUP BY grp`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		want := map[string]float64{}
		for _, r := range src {
			key := "<null>"
			if r.grp != nil {
				key = r.grp.(string)
			}
			if r.amt != nil {
				want[key] += r.amt.(float64)
			}
		}
		got := map[string]float64{}
		for _, row := range res.Rows {
			key := "<null>"
			if row["grp"] != nil {
				key = fmt.Sprintf("%v", row["grp"])
			}
			got[key] = toF64(t, row["s"])
		}
		for k, w := range want {
			g, ok := got[k]
			if !ok {
				t.Fatalf("group %q missing from SUM result (groups: %v)", k, keys(got))
			}
			if diff := g - w; diff > 0.01 || diff < -0.01 {
				t.Fatalf("group %q SUM = %v, want %v", k, g, w)
			}
		}
	})

	t.Run("distinct_nullable_and_decimal", func(t *testing.T) {
		res, err := db.Query(ctx, `SELECT DISTINCT grp, amount FROM events WHERE id < 700`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		want := map[string]bool{}
		for _, r := range src[:700] {
			g, a := "<null>", "<null>"
			if r.grp != nil {
				g = r.grp.(string)
			}
			if r.amt != nil {
				a = fmt.Sprintf("%.2f", r.amt.(float64))
			}
			want[g+"|"+a] = true
		}
		got := map[string]bool{}
		for _, row := range res.Rows {
			g, a := "<null>", "<null>"
			if row["grp"] != nil {
				g = fmt.Sprintf("%v", row["grp"])
			}
			if row["amount"] != nil {
				a = fmt.Sprintf("%v", row["amount"])
			}
			got[g+"|"+a] = true
		}
		if len(got) != len(want) {
			t.Fatalf("DISTINCT cardinality = %d, want %d", len(got), len(want))
		}
		for k := range want {
			if !got[k] {
				t.Fatalf("DISTINCT missing combination %q", k)
			}
		}
	})
}

func toI64(tb testing.TB, v any) int64 {
	tb.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		tb.Fatalf("not numeric: %#v (%T)", v, v)
		return 0
	}
}

func toF64(tb testing.TB, v any) float64 {
	tb.Helper()
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case string:
		var f float64
		fmt.Sscanf(x, "%f", &f)
		return f
	default:
		tb.Fatalf("not numeric: %#v (%T)", v, v)
		return 0
	}
}

func keys(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
