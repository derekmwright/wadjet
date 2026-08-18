package test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// End-to-end for the offsets-shape evaluation class: the ClickBench Q28
// shape (AVG(LENGTH(url)) ... WHERE url <> ” GROUP BY k) and its siblings
// must return identical values with the lengths-only scan decode on and
// off. The lengths-only path never materializes url's bytes, so any answer
// difference is a defect in either the planner analysis or the decode.
//
// Rows deliberately include empty strings, NULLs and multibyte UTF-8 —
// LENGTH is a byte count here, so multibyte rows are where a rune-vs-byte
// slip would show.
func shapeOnlyDB(t *testing.T) (context.Context, *wadjet.DB) {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "url", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "hits", schema, nil); err != nil {
		t.Fatal(err)
	}
	const n = 5000
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		row := map[string]any{"k": int64(i % 7)}
		switch i % 10 {
		case 0:
			row["url"] = nil
		case 1:
			row["url"] = ""
		case 2:
			row["url"] = "日本語テキスト" // 21 bytes, 7 runes
		case 3:
			row["url"] = "émoji 😀"
		default:
			row["url"] = fmt.Sprintf("https://example.test/%d/%030d", i, i*7919)
		}
		rows = append(rows, row)
	}
	ing := db.NewIngester("hits", schema, nil, ingest.Config{MaxBufferRows: 100000, RowGroupSize: 1024})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, db
}

func TestShapeOnlyDecodeEndToEnd(t *testing.T) {
	ctx, db := shapeOnlyDB(t)
	decodesBefore := scan.LengthsOnlyColumnDecodes.Load()
	queries := []string{
		// ClickBench Q28's exact shape.
		"SELECT k, AVG(LENGTH(url)) AS l, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY k ORDER BY k",
		"SELECT k, SUM(LENGTH(url)) AS s FROM hits GROUP BY k ORDER BY k",
		"SELECT k, MAX(octet_length(url)) AS m FROM hits GROUP BY k ORDER BY k",
		"SELECT COUNT(url) AS c FROM hits",
		"SELECT COUNT(*) AS c FROM hits WHERE url IS NULL",
		"SELECT COUNT(*) AS c FROM hits WHERE url IS NOT NULL",
		"SELECT COUNT(*) AS c FROM hits WHERE url = ''",
		"SELECT k, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY k ORDER BY k",
		"SELECT MIN(LENGTH(url)) AS lo, MAX(LENGTH(url)) AS hi FROM hits",
		// Value uses that must keep the full decode — same answers either way.
		"SELECT k, MAX(url) AS u FROM hits GROUP BY k ORDER BY k",
		"SELECT k, MAX(char_length(url)) AS m FROM hits GROUP BY k ORDER BY k",
		"SELECT url, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY url ORDER BY url LIMIT 5",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			prev := scanLengthsOnlySet(t, false)
			off, err := db.Query(ctx, q)
			if err != nil {
				scanLengthsOnlySet(t, prev)
				t.Fatalf("toggle off: %v", err)
			}
			scanLengthsOnlySet(t, true)
			on, err := db.Query(ctx, q)
			scanLengthsOnlySet(t, prev)
			if err != nil {
				t.Fatalf("toggle on: %v", err)
			}
			assertSameRows(t, off.Rows, on.Rows)
		})
	}
	// Engagement: agreement between two identical full decodes proves
	// nothing about the lengths-only path.
	if scan.LengthsOnlyColumnDecodes.Load() == decodesBefore {
		t.Error("lengths-only decode never engaged across the query set")
	}
}

// scanLengthsOnlySet flips the lengths-only kill switch and returns the
// previous value.
func scanLengthsOnlySet(t *testing.T, on bool) bool {
	t.Helper()
	return scan.SetLengthsOnlyDecodeForTest(on)
}

func assertSameRows(t *testing.T, want, got []map[string]any) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("row count %d (lengths-only) vs %d (full decode)", len(got), len(want))
	}
	for i := range want {
		if len(want[i]) != len(got[i]) {
			t.Fatalf("row %d: column count %d vs %d", i, len(got[i]), len(want[i]))
		}
		for col, wv := range want[i] {
			gv, ok := got[i][col]
			if !ok {
				t.Fatalf("row %d: column %q missing under lengths-only decode", i, col)
			}
			if !sameScalar(wv, gv) {
				t.Fatalf("row %d column %q: full decode %v (%T), lengths-only %v (%T)", i, col, wv, wv, gv, gv)
			}
		}
	}
}

func sameScalar(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	af, aok := numericAny(a)
	bf, bok := numericAny(b)
	if aok && bok {
		return math.Abs(af-bf) < 1e-9
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func numericAny(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	}
	return 0, false
}

// TestShapeOnlyDecodeValuesAreCorrect pins the actual numbers, not just
// toggle agreement: byte lengths over a mixed ASCII/multibyte/empty/NULL
// column, with LENGTH(NULL) staying NULL rather than becoming 0.
func TestShapeOnlyDecodeValuesAreCorrect(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "url", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "hits", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"k": int64(1), "url": "abc"}, // 3
		{"k": int64(1), "url": "日本"},  // 6 bytes, 2 runes
		{"k": int64(1), "url": nil},   // NULL: skipped by AVG/SUM/COUNT(col)
		{"k": int64(2), "url": ""},    // 0
		{"k": int64(2), "url": "😀"},   // 4 bytes, 1 rune
		{"k": int64(3), "url": nil},   // group with only NULLs
	}
	ing := db.NewIngester("hits", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	prev := scanLengthsOnlySet(t, true)
	defer scanLengthsOnlySet(t, prev)

	r, err := db.Query(ctx, "SELECT k, SUM(LENGTH(url)) AS s, COUNT(url) AS c FROM hits GROUP BY k ORDER BY k")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("got %d groups, want 3: %v", len(r.Rows), r.Rows)
	}
	wantSum := []any{float64(9), float64(4), nil}
	wantCount := []float64{2, 2, 0}
	for i, row := range r.Rows {
		if wantSum[i] == nil {
			if row["s"] != nil {
				t.Errorf("group %d: SUM(LENGTH(url)) = %v, want NULL (all inputs NULL)", i, row["s"])
			}
		} else if got, _ := numericAny(row["s"]); got != wantSum[i] {
			t.Errorf("group %d: SUM(LENGTH(url)) = %v, want %v", i, row["s"], wantSum[i])
		}
		if got, _ := numericAny(row["c"]); got != wantCount[i] {
			t.Errorf("group %d: COUNT(url) = %v, want %v", i, row["c"], wantCount[i])
		}
	}
}
