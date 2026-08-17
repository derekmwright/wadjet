package parquet

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// TestPageSplitEmitFixture writes a page-split file + expected-rows JSON
// to WADJET_SPLIT_FIXTURE_DIR for cross-verification against reference
// readers (PyArrow; see the #300 commit). Skipped unless the env var is
// set — it is a fixture generator, not an assertion.
func TestPageSplitEmitFixture(t *testing.T) {
	dir := os.Getenv("WADJET_SPLIT_FIXTURE_DIR")
	if dir == "" {
		t.Skip("set WADJET_SPLIT_FIXTURE_DIR to emit the cross-check fixture")
	}
	schema := Schema{Columns: []Column{
		{Name: "i64", Type: TypeInt64},
		{Name: "i32", Type: TypeInt32, Nullable: true},
		{Name: "f64", Type: TypeFloat64},
		{Name: "s", Type: TypeString, Nullable: true},
	}}
	const n = 20000
	rows := make([]map[string]any, n)
	expected := make([][]any, n)
	for i := range rows {
		row := map[string]any{"i64": int64(i)*7919 - 1000, "f64": float64(i) * 1.25}
		exp := []any{int64(i)*7919 - 1000, nil, float64(i) * 1.25, nil}
		if i%7 != 0 {
			row["i32"] = int32(i % 100000)
			exp[1] = i % 100000
		}
		if i%5 != 0 {
			s := fmt.Sprintf("v-%d-%030d", i, i*31)
			if i%5 == 1 {
				s = ""
			}
			row["s"] = s
			exp[3] = s
		}
		rows[i] = row
		expected[i] = exp
	}
	f, err := os.Create(dir + "/split.parquet")
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultWriterConfig()
	cfg.PageBufferSize = 2048
	cfg.RowGroupSize = 8192
	w, err := NewWriter(f, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	ef, err := os.Create(dir + "/expected.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(ef).Encode(expected); err != nil {
		t.Fatal(err)
	}
	if err := ef.Close(); err != nil {
		t.Fatal(err)
	}
}
