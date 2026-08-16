package scan

import (
	"fmt"
	"os"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestNativeReadDictFallbackMixedPages drives the native columnar path over
// the shared mixed-encoding fixture (see internal/storage/parquet/testdata/
// gen_dict_fallback.py): column chunks whose data pages mix dictionary
// encoding and PLAIN (writer dict-overflow fallback). Values are pure
// functions of the row index, so every row is verified.
//
// Regression for resolveNativeDictionary being applied chunk-scoped: PLAIN
// fallback pages had their raw values consumed as dictionary indices.
func TestNativeReadDictFallbackMixedPages(t *testing.T) {
	const numRows = 20000

	data, err := os.ReadFile("../../storage/parquet/testdata/dict_fallback.parquet")
	if err != nil {
		t.Fatalf("read fixture (regen with gen_dict_fallback.py): %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	fr := r.FileReader()
	schema := r.Schema().Columns

	batches, err := ReadFileBatchesNative(fr, schema, nil)
	if err != nil {
		t.Fatalf("ReadFileBatchesNative: %v", err)
	}

	colIdx := make(map[string]int, len(schema))
	for i, c := range schema {
		colIdx[c.Name] = i
	}
	for _, name := range []string{"s", "i64", "i32", "f64", "sn"} {
		if _, ok := colIdx[name]; !ok {
			t.Fatalf("column %s missing from schema", name)
		}
	}

	asString := func(v any) (string, bool) {
		switch x := v.(type) {
		case string:
			return x, true
		case []byte:
			return string(x), true
		}
		return "", false
	}
	asInt64 := func(v any) (int64, bool) {
		switch x := v.(type) {
		case int64:
			return x, true
		case int32:
			return int64(x), true
		}
		return 0, false
	}

	bad := 0
	report := func(format string, args ...any) {
		bad++
		if bad <= 20 {
			t.Errorf(format, args...)
		}
	}

	row := 0
	for _, rb := range batches {
		for j := 0; j < rb.Len; j++ {
			i := row + j
			if s, ok := asString(rb.Columns[colIdx["s"]].GetValue(j)); !ok || s != fmt.Sprintf("s%07d", i) {
				report("row %d col s: got %v", i, rb.Columns[colIdx["s"]].GetValue(j))
			}
			if v, ok := asInt64(rb.Columns[colIdx["i64"]].GetValue(j)); !ok || v != int64(i)*1000003 {
				report("row %d col i64: got %v, want %d", i, rb.Columns[colIdx["i64"]].GetValue(j), int64(i)*1000003)
			}
			if v, ok := asInt64(rb.Columns[colIdx["i32"]].GetValue(j)); !ok || v != int64(i)*7 {
				report("row %d col i32: got %v, want %d", i, rb.Columns[colIdx["i32"]].GetValue(j), i*7)
			}
			if v, ok := rb.Columns[colIdx["f64"]].GetValue(j).(float64); !ok || v != float64(i)*0.5 {
				report("row %d col f64: got %v, want %v", i, rb.Columns[colIdx["f64"]].GetValue(j), float64(i)*0.5)
			}
			snv := rb.Columns[colIdx["sn"]].GetValue(j)
			if i%5 == 0 {
				if snv != nil {
					report("row %d col sn: want NULL, got %v", i, snv)
				}
			} else if s, ok := asString(snv); !ok || s != fmt.Sprintf("n%07d", i) {
				report("row %d col sn: got %v", i, snv)
			}
		}
		row += rb.Len
	}
	if row != numRows {
		t.Fatalf("read %d rows, want %d", row, numRows)
	}
	if bad > 20 {
		t.Errorf("... and %d more mismatches", bad-20)
	}
}
