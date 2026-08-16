package parquet

import (
	"fmt"
	"os"
	"testing"
)

// TestDictFallbackMixedPages reads testdata/dict_fallback.parquet, a
// pyarrow-written file whose column chunks mix dictionary-encoded and
// PLAIN data pages (dictionary-page overflow fallback — the layout real
// ClickBench hits parts use for high-cardinality columns). Every value
// is a pure function of the row index (see testdata/gen_dict_fallback.py),
// so the whole file is verified value-exactly.
//
// Regression: dictionary resolution was chunk-scoped — when a chunk had a
// dictionary, EVERY page's values were consumed as int32 indices, so the
// PLAIN fallback pages decoded as garbage or panicked with out-of-range
// indices.
// TestValidateDictIndices covers the pre-gather bounds check that turns
// corrupt/hostile dictionary indices into errors instead of panics.
func TestValidateDictIndices(t *testing.T) {
	cases := []struct {
		name    string
		indices []int32
		n       int
		numVals int
		wantErr bool
	}{
		{"in range", []int32{0, 1, 2}, 3, 3, false},
		{"empty", nil, 0, 0, false},
		{"index == numVals", []int32{0, 3}, 2, 3, true},
		{"negative index", []int32{-1}, 1, 3, true},
		{"short indices", []int32{0}, 2, 3, true},
		{"tail beyond n ignored", []int32{0, 99}, 1, 3, false},
	}
	for _, tc := range cases {
		if err := ValidateDictIndices(tc.indices, tc.n, tc.numVals); (err != nil) != tc.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", tc.name, err, tc.wantErr)
		}
	}
}

func TestDictFallbackMixedPages(t *testing.T) {
	const numRows = 20000

	f, err := os.Open("testdata/dict_fallback.parquet")
	if err != nil {
		t.Fatalf("open fixture (regen with testdata/gen_dict_fallback.py): %v", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(f, st.Size())
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}

	// Fixture sanity: every chunk must actually mix dictionary and PLAIN
	// encodings, or this test stops testing anything.
	fr := r.FileReader()
	for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
		rg := fr.RowGroupMeta(rgIdx)
		for _, cc := range rg.Columns {
			cm := cc.MetaData
			if cm == nil {
				t.Fatalf("rg%d: column chunk missing metadata", rgIdx)
			}
			hasDict, hasPlain := false, false
			for _, e := range cm.Encodings {
				switch e {
				case EncodingPlainDictionary, EncodingRLEDictionary:
					hasDict = true
				case EncodingPlain:
					hasPlain = true
				}
			}
			if !hasDict || !hasPlain {
				t.Fatalf("rg%d col %v: encodings %v do not mix dictionary+PLAIN — regenerate fixture with testdata/gen_dict_fallback.py",
					rgIdx, cm.PathInSchema, cm.Encodings)
			}
		}
	}

	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(rows) != numRows {
		t.Fatalf("got %d rows, want %d", len(rows), numRows)
	}

	asBytes := func(v any) ([]byte, bool) {
		switch x := v.(type) {
		case []byte:
			return x, true
		case string:
			return []byte(x), true
		}
		return nil, false
	}

	bad := 0
	report := func(format string, args ...any) {
		bad++
		if bad <= 20 {
			t.Errorf(format, args...)
		}
	}
	for i, row := range rows {
		if b, ok := asBytes(row["s"]); !ok || string(b) != fmt.Sprintf("s%07d", i) {
			report("row %d col s: got %v (%T)", i, row["s"], row["s"])
		}
		if v, ok := row["i64"].(int64); !ok || v != int64(i)*1000003 {
			report("row %d col i64: got %v (%T), want %d", i, row["i64"], row["i64"], int64(i)*1000003)
		}
		if v, ok := row["i32"].(int64); !ok || v != int64(i)*7 {
			report("row %d col i32: got %v (%T), want %d", i, row["i32"], row["i32"], i*7)
		}
		if v, ok := row["f64"].(float64); !ok || v != float64(i)*0.5 {
			report("row %d col f64: got %v (%T), want %v", i, row["f64"], row["f64"], float64(i)*0.5)
		}
		if i%5 == 0 {
			if v, present := row["sn"]; present && v != nil {
				report("row %d col sn: want NULL, got %v (%T)", i, v, v)
			}
		} else if b, ok := asBytes(row["sn"]); !ok || string(b) != fmt.Sprintf("n%07d", i) {
			report("row %d col sn: got %v (%T)", i, row["sn"], row["sn"])
		}
	}
	if bad > 20 {
		t.Errorf("... and %d more mismatches", bad-20)
	}
}
