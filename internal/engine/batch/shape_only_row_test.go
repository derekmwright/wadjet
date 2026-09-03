package batch

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A SHAPE-ONLY column survives the row-shaped detour as a shape-only column.
//
// The scan decodes lengths and no bytes when the planner proves every use of a
// byte-array column reads its shape. The VECTOR paths carried that faithfully
// — copyShapeRange propagates the mark rather than moving bytes that do not
// exist — but the ROW paths could not: GetValue's only answer for such a row
// was the panic that says a value was read, so a grouped aggregate that
// buffers its input under memory pressure failed a query that answers with
// memory to spare (#791). The box is batch.ShapeOnlyLen: the length, and a
// refusal of the value.

func shapeOnlyTestColumn(t *testing.T, lens []int, nullAt map[int]bool) (*Vector, []parquet.Column) {
	t.Helper()
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString, Nullable: true}}
	v := NewVector(TypeString, len(lens))
	v.BytesData.ShapeOnly = true
	cur := uint32(0)
	for i, n := range lens {
		if nullAt[i] {
			v.Nulls.SetNull(i)
		} else {
			v.Nulls.SetValid(i)
			cur += uint32(n)
		}
		v.BytesData.Offsets[i+1] = cur
	}
	return v, schema
}

func TestShapeOnlyColumnRoundTripsThroughRows(t *testing.T) {
	lens := []int{0, 1, 7, 300, 2, 0, 11}
	nulls := map[int]bool{3: true, 5: true}
	v, schema := shapeOnlyTestColumn(t, lens, nulls)
	b := NewRecordBatch(schema, len(lens))
	b.Columns[0] = v
	b.Len = len(lens)

	rows := b.ToRows()
	if len(rows) != len(lens) {
		t.Fatalf("%d rows, want %d", len(rows), len(lens))
	}
	for i := range lens {
		got := rows[i]["s"]
		if nulls[i] {
			if got != nil {
				t.Errorf("row %d is NULL but boxed as %T:%v", i, got, got)
			}
			continue
		}
		n, ok := got.(ShapeOnlyLen)
		if !ok {
			t.Fatalf("row %d boxed as %T:%v — a shape-only row must box as ShapeOnlyLen, "+
				"and anything else is a value this column does not have", i, got, got)
		}
		if int(n) != lens[i] {
			t.Errorf("row %d carries length %d, want %d", i, n, lens[i])
		}
	}

	// Written back, it is a shape-only column again with the same lengths.
	out := FromRows(schema, rows)
	if !out.Columns[0].IsShapeOnly() {
		t.Fatal("the rebuilt column is NOT shape-only — the mark was lost, so the next " +
			"consumer reads an empty arena as if it held values")
	}
	if got := len(out.Columns[0].BytesData.Data); got != 0 {
		t.Fatalf("the rebuilt column has %d bytes of arena; a shape-only column has none", got)
	}
	for i := range lens {
		if nulls[i] {
			if !out.Columns[0].Nulls.IsNull(i) {
				t.Errorf("row %d lost its NULL", i)
			}
			continue
		}
		if got := out.Columns[0].BytesData.LengthAt(i); got != lens[i] {
			t.Errorf("row %d rebuilt with length %d, want %d", i, got, lens[i])
		}
	}
}

// The guard is not weakened by the box: a consumer that wants the BYTES still
// gets the diagnosis, at the same site, before and after the round trip.
func TestShapeOnlyColumnStillRefusesAValueRead(t *testing.T) {
	v, schema := shapeOnlyTestColumn(t, []int{3, 4}, nil)
	b := NewRecordBatch(schema, 2)
	b.Columns[0] = v
	b.Len = 2

	for _, tc := range []struct {
		name string
		col  func() *Vector
	}{
		{"before the round trip", func() *Vector { return v }},
		{"after it", func() *Vector { return FromRows(schema, b.ToRows()).Columns[0] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("reading a VALUE out of a shape-only column did not raise the guard")
				}
				if !strings.Contains(strings.ToLower(pstr(r)), "shape-only") {
					t.Fatalf("raised, but not by name: %v", r)
				}
			}()
			_ = tc.col().BytesData.Value(0)
		})
	}
}

// The destination's side of the same claim (ADR-0023 item 6: neither encoder
// may write bytes its own reader refuses). A shape-only length landing
// somewhere it cannot mean anything is refused, not written.
func TestShapeOnlyLengthIsRefusedByAWrongDestination(t *testing.T) {
	t.Run("a fixed-width column", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("an INT64 column accepted a shape-only length; it would have " +
					"become a number the row never held")
			}
		}()
		v := NewVector(TypeInt64, 1)
		v.SetValue(0, ShapeOnlyLen(7))
	})
	t.Run("a bytes column that already holds values", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("a column holding real bytes accepted a shape-only length; the two " +
					"encodings would then share one arena and every later row would read " +
					"from the wrong offset")
			}
			if !strings.Contains(strings.ToLower(pstr(r)), "shape-only") {
				t.Fatalf("raised, but not by name: %v", r)
			}
		}()
		v := NewVector(TypeString, 2)
		v.SetValue(0, "real")
		v.SetValue(1, ShapeOnlyLen(7))
	})
}

func pstr(r any) string {
	if e, ok := r.(error); ok {
		return e.Error()
	}
	if s, ok := r.(string); ok {
		return s
	}
	return ""
}

// TestShapeOnlyDestinationRefusesAValueWrite is the OTHER order of the same
// confusion, and it was silent until the round-0 review found it.
//
// TestShapeOnlyLengthIsRefusedByAWrongDestination above covers value-then-shape.
// This is shape-then-value: a column already marked shape-only, then real bytes
// written into it through one of the four value writers. The offsets then
// advance by the appended bytes while the earlier rows' offsets describe
// lengths of bytes that were never written, the pair goes DESCENDING, and
// LengthAt's defence against a malformed pair answers 0 — a wrong LENGTH, not
// an error. Measured before the guard: `LengthAt=0 want 7`.
//
// Before #791 this mix was loud by accident, because GetValue panicked on the
// shape rows; teaching the row boundary to box a length took that accident
// away. So the guard is explicit now, and it is the mirror of copyShapeRange's
// (ADR-0023 item 6).
func TestShapeOnlyDestinationRefusesAValueWrite(t *testing.T) {
	fresh := func() *Vector {
		v := NewVector(TypeString, 4)
		v.SetValue(0, ShapeOnlyLen(5))
		v.SetValue(1, ShapeOnlyLen(5))
		return v
	}
	src := NewVector(TypeString, 2)
	src.SetValue(0, "abcdefg")
	src.SetValue(1, "hij")

	for _, tc := range []struct {
		name  string
		write func(*Vector)
	}{
		{"SetValue", func(v *Vector) { v.SetValue(2, "abcdefg") }},
		{"Set", func(v *Vector) { v.BytesData.Set(2, []byte("abcdefg")) }},
		{"SetString", func(v *Vector) { v.BytesData.SetString(2, "abcdefg") }},
		{"SetFrom", func(v *Vector) { v.BytesData.SetFrom(2, &src.BytesData, 0) }},
		{"BulkCopy", func(v *Vector) { v.BytesData.BulkCopy(2, &src.BytesData, 0, 2) }},
		{"BulkSet", func(v *Vector) {
			v.BytesData.BulkSet(2, src.BytesData.Data, src.BytesData.Offsets, 2)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := fresh()
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("%s wrote real bytes into a shape-only column and did not raise "+
						"the guard; the offsets now disagree with the arena and LengthAt "+
						"answers %d for a row whose length is 7",
						tc.name, v.BytesData.LengthAt(2))
				}
				if !strings.Contains(strings.ToLower(pstr(r)), "shape-only") {
					t.Fatalf("raised, but not by name: %v", r)
				}
			}()
			tc.write(v)
		})
	}
}

// The row detour is the path #791 opened, so it gets the same fixture: a
// FromRows whose rows mix shape-only lengths and real values must be refused,
// not silently produce a column whose offsets lie.
func TestFromRowsRefusesMixedShapeAndValueRows(t *testing.T) {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString, Nullable: true}}
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("FromRows accepted a mix of shape-only lengths and real values in one " +
				"column; the result's offsets describe bytes that are not there")
		}
		if !strings.Contains(strings.ToLower(pstr(r)), "shape-only") {
			t.Fatalf("raised, but not by name: %v", r)
		}
	}()
	FromRows(schema, []map[string]any{
		{"s": ShapeOnlyLen(5)},
		{"s": ShapeOnlyLen(5)},
		{"s": "abcdefg"},
	})
}

// The two things that must NOT be refused, because both are how a shape-only
// column legitimately advances its offsets: a NULL row (WriteNullAt writes
// zero bytes through Set) and a zero-length shape row.
func TestShapeOnlyColumnStillAcceptsNullsAndEmptyRows(t *testing.T) {
	v := NewVector(TypeString, 4)
	v.SetValue(0, ShapeOnlyLen(5))
	v.SetValue(1, nil) // WriteNullAt -> BytesData.Set(1, nil)
	v.SetValue(2, ShapeOnlyLen(0))
	v.SetValue(3, ShapeOnlyLen(4))
	if !v.IsShapeOnly() {
		t.Fatal("the column stopped being shape-only")
	}
	if !v.Nulls.IsNull(1) {
		t.Error("row 1 lost its NULL")
	}
	for i, want := range []int{5, 0, 0, 4} {
		if got := v.BytesData.LengthAt(i); got != want {
			t.Errorf("row %d: LengthAt=%d want %d (offsets=%v)", i, got, want, v.BytesData.Offsets)
		}
	}
}
