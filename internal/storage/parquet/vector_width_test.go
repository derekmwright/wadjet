package parquet

import (
	"bytes"
	"fmt"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// #886: a VECTOR(N) column stores exactly N components, or the write fails.
//
// A FIXED_LEN_BYTE_ARRAY leaf carries no per-value length. The column chunk is
// one run of bytes, cut every TypeLength bytes on the way back, so a value of
// the wrong width does not produce a short value: it MOVES THE BOUNDARY for
// every value after it. Measured at de5bc970, a VECTOR(2) fed []float32{1} and
// then {2,3,4} read back as [1,2] and [3,4] with write, Close and read all
// returning nil, and parquet-go saw the same regrouped components — the two
// errors cancelled in the byte total, which is exactly why no read-side length
// check can catch this and the width has to be held on the way in.
func TestAVectorColumnStoresExactlyItsDeclaredWidth(t *testing.T) {
	col := Column{Name: "x", Type: TypeVector, Nullable: true, Dimension: 2}

	// The cancelling pair from the issue: short then long, summing to the
	// expected byte count.
	assertVectorRefused(t, col, []map[string]any{
		{"x": []float32{1}},
		{"x": []float32{2, 3, 4}},
	})
	// And each half on its own, which no total can hide.
	assertVectorRefused(t, col, []map[string]any{{"x": []float32{1}}})
	assertVectorRefused(t, col, []map[string]any{{"x": []float32{1, 2, 3}}})
	assertVectorRefused(t, col, []map[string]any{{"x": []float32{}}})
	// A byte box is held to the same width.
	assertVectorRefused(t, col, []map[string]any{{"x": []byte{1, 2, 3, 4, 5}}})
	// A box that is not a vector at all.
	assertVectorRefused(t, col, []map[string]any{{"x": "1,2"}})
	assertVectorRefused(t, col, []map[string]any{{"x": int64(3)}})
}

// The correct widths still write, read back, and are what parquet-go sees.
func TestAVectorOfTheDeclaredWidthRoundTrips(t *testing.T) {
	col := Column{Name: "x", Type: TypeVector, Nullable: true, Dimension: 2}
	rows := []map[string]any{
		{"x": []float32{1, 2}},
		{"x": nil},
		{"x": []float32{3, 4}},
	}
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
	if err := nw.WriteMapRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := nw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("read %d rows, want 3", len(got))
	}
	assertVectorEquals(t, got[0]["x"], []float32{1, 2})
	if got[1]["x"] != nil {
		t.Errorf("the NULL vector read back as %v", got[1]["x"])
	}
	assertVectorEquals(t, got[2]["x"], []float32{3, 4})

	// The cross-check the issue used: a foreign reader must see three
	// fixed-width values of the declared length, not a re-cut run.
	f, err := goparquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("parquet-go: %v", err)
	}
	leaf := f.Root().Column("x")
	if leaf == nil {
		t.Fatal("parquet-go does not see column x")
	}
	if w := leaf.Type().Length(); w != 8 {
		t.Errorf("parquet-go sees a fixed width of %d bytes, want 8 (VECTOR(2) of float32)", w)
	}
}

func assertVectorRefused(t *testing.T, col Column, rows []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
	err := nw.WriteMapRows(rows)
	if err == nil {
		err = nw.Close()
	}
	if err == nil {
		r, rerr := NewReaderFromBytes(buf.Bytes())
		back := "a file that could not be opened"
		if rerr == nil {
			if got, rerr2 := r.ReadRows(nil); rerr2 == nil {
				back = vecRowsString(got)
			}
		}
		t.Fatalf("writing %v into a VECTOR(%d) column succeeded and produced %s",
			rows, col.Dimension, back)
	}
	if s := sqlerr.StateOf(err); s != "22023" && s != "42804" {
		t.Errorf("refusing %v: SQLSTATE %q, want 22023 or 42804: %v", rows, s, err)
	}
}

func assertVectorEquals(t *testing.T, got any, want []float32) {
	t.Helper()
	g, ok := got.([]float32)
	if !ok {
		t.Fatalf("read back %v (%T), want a []float32", got, got)
	}
	if len(g) != len(want) {
		t.Fatalf("read back %v, want %v", g, want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Fatalf("read back %v, want %v", g, want)
		}
	}
}

func vecRowsString(rows []map[string]any) string {
	return fmt.Sprintf("%v", rows)
}
