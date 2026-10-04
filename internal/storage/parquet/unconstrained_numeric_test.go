// SPDX-License-Identifier: MIT

package parquet

import (
	"bytes"
	"encoding/json"
	"testing"
)

// A column created from an unconstrained numeric (ADR-0024 §10, #1541): the
// DDL door's bare `NUMERIC` / `DECIMAL` is DECIMAL(38,10) marked
// Unconstrained, a parameterized spelling is untouched, the CREATE TABLE AS
// rule keeps a source scale past 10, and a record written before the marker
// existed reads as constrained.
func TestUnconstrainedNumericColumnIsOneDeclaration(t *testing.T) {
	for _, c := range []struct {
		typ  string
		p, s int
		unc  bool
	}{
		{"NUMERIC", 38, 10, true},
		{"decimal", 38, 10, true},
		{" Numeric ", 38, 10, true},
		{"NUMERIC(10,2)", 10, 2, false},
		{"NUMERIC(5)", 5, 0, false},
		{"DECIMAL(38,0)", 38, 0, false},
		{"NUMERIC(38,10)", 38, 10, false},
	} {
		col, err := DeclaredColumn("v", c.typ, true)
		if err != nil {
			t.Fatalf("%s: %v", c.typ, err)
		}
		if col.Type != TypeDecimal || col.Precision != c.p || col.Scale != c.s || col.Unconstrained != c.unc || !col.Nullable {
			t.Errorf("%s declares %+v, want DECIMAL(%d,%d) unconstrained=%v", c.typ, col, c.p, c.s, c.unc)
		}
	}
	// A nested element keeps ResolveColumn's bare (38, 0): the rule is the
	// stored column's.
	arr, err := DeclaredColumn("a", "ARRAY(NUMERIC)", true)
	if err != nil {
		t.Fatal(err)
	}
	if e := arr.ElementType; e == nil || e.Precision != 38 || e.Scale != 0 || e.Unconstrained {
		t.Errorf("ARRAY(NUMERIC) element %+v, want DECIMAL(38,0) unmarked", e)
	}
	for _, c := range []struct{ in, want int }{{0, 10}, {2, 10}, {10, 10}, {12, 12}, {15, 15}, {40, 38}} {
		if col := UnconstrainedNumericColumn("v", c.in); col.Scale != c.want || col.Precision != 38 || !col.Unconstrained {
			t.Errorf("UnconstrainedNumericColumn(%d) = %+v, want DECIMAL(38,%d) marked", c.in, col, c.want)
		}
	}
	var old Column
	if err := json.Unmarshal([]byte(`{"name":"v","type":10,"nullable":true,"precision":38}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Unconstrained {
		t.Error("a record with no unconstrained key reads as unconstrained")
	}
	b, err := json.Marshal(UnconstrainedNumericColumn("v", 0))
	if err != nil {
		t.Fatal(err)
	}
	var back Column
	if err := json.Unmarshal(b, &back); err != nil || !back.Unconstrained || back.Scale != 10 {
		t.Errorf("the marker does not survive the record: %s → %+v (%v)", b, back, err)
	}
}

// The file footer's declared schema carries the marker to a reader that has
// only the file (a DAG stage's scan): the DECIMAL annotation still decides
// the type, precision and scale, and the marker is read only where the two
// agree.
func TestTheFooterCarriesTheUnconstrainedMarker(t *testing.T) {
	write := func(col Column) []byte {
		t.Helper()
		var buf bytes.Buffer
		w, err := NewWriter(&buf, Schema{Columns: []Column{{Name: "id", Type: TypeInt64}, col}}, DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRows([]map[string]any{{"id": int64(1), "v": Decimal128{Lo: 12500000000}}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	read := func(data []byte) Column {
		t.Helper()
		r, err := NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range r.Schema().Columns {
			if c.Name == "v" {
				return c
			}
		}
		t.Fatal("no column v")
		return Column{}
	}
	marked := UnconstrainedNumericColumn("v", 0)
	if got := read(write(marked)); !got.Unconstrained || got.Precision != 38 || got.Scale != 10 {
		t.Errorf("a marked column reads back %+v", got)
	}
	plain := Column{Name: "v", Type: TypeDecimal, Precision: 38, Scale: 10, Nullable: true}
	if got := read(write(plain)); got.Unconstrained {
		t.Errorf("an unmarked DECIMAL(38,10) reads back marked: %+v", got)
	}
}
