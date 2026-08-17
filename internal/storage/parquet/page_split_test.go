package parquet

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// Page splitting (#300) must be invisible to readers: a file written with
// a small PageBufferSize must decode to exactly the rows a single-page
// file holds, across types, nulls, empty strings, and row-group
// boundaries — and the split must actually happen for eligible columns
// while bool/nested chunks stay single-page.

func splitTestRows(t *testing.T, n int, seed int64) ([]map[string]any, Schema) {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	schema := Schema{Columns: []Column{
		{Name: "i64", Type: TypeInt64},
		{Name: "i32", Type: TypeInt32, Nullable: true},
		{Name: "f64", Type: TypeFloat64},
		{Name: "s", Type: TypeString, Nullable: true},
		{Name: "b", Type: TypeBool},
	}}
	rows := make([]map[string]any, n)
	for i := range rows {
		row := map[string]any{
			"i64": int64(i)*7919 - 1000,
			"f64": float64(i) * 1.25,
			"b":   i%3 == 0,
		}
		if i%7 != 0 {
			row["i32"] = int32(i % 100000)
		}
		switch i % 5 {
		case 0:
			// null s
		case 1:
			row["s"] = ""
		case 2:
			row["s"] = fmt.Sprintf("v-%d-%060d", i, r.Intn(1<<30))
		default:
			row["s"] = fmt.Sprintf("x%d", i)
		}
		rows[i] = row
	}
	return rows, schema
}

func writeSplitFile(t *testing.T, rows []map[string]any, schema Schema, pageSize, rgSize int) []byte {
	t.Helper()
	var buf bytes.Buffer
	cfg := DefaultWriterConfig()
	cfg.PageBufferSize = pageSize
	cfg.RowGroupSize = rgSize
	w, err := NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func countPages(t *testing.T, data []byte, colName string) int {
	t.Helper()
	r, err := NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()
	colIdx := -1
	for i, leaf := range fr.Leaves() {
		if leaf.Name == colName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		t.Fatalf("column %s not found", colName)
	}
	pages := 0
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		pr := fr.ColumnPages(rg, colIdx)
		for {
			p, err := pr.NextPage()
			if err != nil {
				t.Fatalf("page walk %s rg %d: %v", colName, rg, err)
			}
			if p == nil {
				break
			}
			pages++
			p.Release()
		}
		pr.Close()
	}
	return pages
}

func TestPageSplitRoundTrip(t *testing.T) {
	const n = 20000
	rows, schema := splitTestRows(t, n, 42)
	split := writeSplitFile(t, rows, schema, 2048, 8192) // many pages, 3 row groups
	single := writeSplitFile(t, rows, schema, 1<<30, 8192)

	// Eligible columns must actually split; a file with 8192-row groups
	// and 2KB pages holds far more pages than row groups.
	for _, col := range []string{"i64", "i32", "f64", "s"} {
		if got := countPages(t, split, col); got <= 3 {
			t.Fatalf("column %s: %d pages, want > 3 (split did not fire)", col, got)
		}
	}
	// BOOLEAN stays single-page per chunk.
	if got := countPages(t, split, "b"); got != 3 {
		t.Fatalf("bool column: %d pages, want 3 (one per row group)", got)
	}

	for name, data := range map[string][]byte{"split": split, "single": single} {
		r, err := NewReaderFromBytes(data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := r.ReadRows(nil)
		if err != nil {
			t.Fatalf("%s ReadAll: %v", name, err)
		}
		if len(got) != n {
			t.Fatalf("%s: %d rows, want %d", name, len(got), n)
		}
		for i, row := range got {
			want := rows[i]
			for _, c := range []string{"i64", "i32", "f64", "s", "b"} {
				wv, has := want[c]
				gv := row[c]
				if !has {
					if gv != nil {
						t.Fatalf("%s row %d col %s: got %v, want NULL", name, i, c, gv)
					}
					continue
				}
				if fmt.Sprint(gv) != fmt.Sprint(wv) {
					t.Fatalf("%s row %d col %s: got %v (%T), want %v (%T)", name, i, c, gv, gv, wv, wv)
				}
			}
		}
	}
}

// Split and single-page files must agree under randomized shapes: row
// counts that land exactly on page/row-group boundaries, single rows,
// all-null chunks, and varied page sizes.
func TestPageSplitPropertyVsSinglePage(t *testing.T) {
	for seed := int64(0); seed < 6; seed++ {
		r := rand.New(rand.NewSource(seed))
		n := []int{1, 2, 100, 8192, 8193, 12000}[seed]
		rows, schema := splitTestRows(t, n, seed)
		if seed == 3 {
			// All-null string chunk: exercise zero-value pages.
			for i := range rows {
				delete(rows[i], "s")
			}
		}
		pageSize := []int{512, 1024, 4096}[r.Intn(3)]
		split := writeSplitFile(t, rows, schema, pageSize, 8192)
		single := writeSplitFile(t, rows, schema, 1<<30, 8192)

		rs, err := NewReaderFromBytes(split)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		gotS, err := rs.ReadRows(nil)
		if err != nil {
			t.Fatalf("seed %d split ReadAll: %v", seed, err)
		}
		rc, err := NewReaderFromBytes(single)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		gotC, err := rc.ReadRows(nil)
		if err != nil {
			t.Fatalf("seed %d single ReadAll: %v", seed, err)
		}
		if len(gotS) != len(gotC) || len(gotS) != n {
			t.Fatalf("seed %d: split %d single %d want %d", seed, len(gotS), len(gotC), n)
		}
		for i := range gotS {
			for _, c := range []string{"i64", "i32", "f64", "s", "b"} {
				if fmt.Sprint(gotS[i][c]) != fmt.Sprint(gotC[i][c]) {
					t.Fatalf("seed %d row %d col %s: split %v vs single %v", seed, i, c, gotS[i][c], gotC[i][c])
				}
			}
		}
	}
}
