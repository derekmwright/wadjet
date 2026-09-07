package parquet

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

// writeSchemaRefusalCases is the corpus both constructors are held to. Each
// entry is a column shape the writer cannot turn into a file a reader can
// read, with the substring its refusal must name.
//
// The container shapes are the ones TestNewWriterRefusesAStructurallyImpossible
// Schema already drove through NewWriter (#970 is that they were reachable
// through the OTHER constructor); the DECIMAL and VECTOR shapes are new
// (#969, #971).
var writeSchemaRefusalCases = []struct {
	name   string
	column Column
	want   string
}{
	{"MAP with Fields instead of ElementType", Column{
		Name: "m", Type: TypeMap, Nullable: true, Fields: []Column{
			{Name: "key", Type: TypeString}, {Name: "value", Type: TypeInt64},
		},
	}, "MAP needs ElementType"},
	{"MAP with no element at all", Column{Name: "m", Type: TypeMap, Nullable: true}, "MAP needs ElementType"},
	{"MAP whose element is not a ROW", Column{
		Name: "m", Type: TypeMap, Nullable: true, ElementType: &Column{Name: "kv", Type: TypeString},
	}, "MAP needs ElementType"},
	{"ARRAY with no element type", Column{Name: "a", Type: TypeArray, Nullable: true}, "ARRAY needs an ElementType"},
	{"ROW with no fields", Column{Name: "r", Type: TypeRow, Nullable: true}, "ROW needs at least one field"},

	{"VECTOR with no dimension", Column{Name: "v", Type: TypeVector, Nullable: true}, "positive Dimension"},
	{"VECTOR with a negative dimension", Column{
		Name: "v", Type: TypeVector, Nullable: true, Dimension: -1,
	}, "positive Dimension"},
	{"VECTOR one component past the int32 width", Column{
		Name: "v", Type: TypeVector, Nullable: true, Dimension: math.MaxInt32/4 + 1,
	}, "FIXED_LEN_BYTE_ARRAY type_length"},
	{"VECTOR at MaxInt32 components", Column{
		Name: "v", Type: TypeVector, Nullable: true, Dimension: math.MaxInt32,
	}, "FIXED_LEN_BYTE_ARRAY type_length"},

	{"DECIMAL with a negative scale", Column{
		Name: "d", Type: TypeDecimal, Nullable: true, Precision: 9, Scale: -1,
	}, "scale of -1 is negative"},
	{"DECIMAL whose scale exceeds its precision", Column{
		Name: "d", Type: TypeDecimal, Nullable: true, Precision: 4, Scale: 9,
	}, "scale of 9 is past the precision 4"},
	{"DECIMAL whose scale exceeds the unconstrained precision", Column{
		Name: "d", Type: TypeDecimal, Nullable: true, Precision: 0, Scale: 40,
	}, "scale of 40 is past the precision 38"},
	{"DECIMAL past 38 digits", Column{
		Name: "d", Type: TypeDecimal, Nullable: true, Precision: 50, Scale: 2,
	}, "precision of 50 is past the 38 digits"},
	{"DECIMAL with a negative precision", Column{
		Name: "d", Type: TypeDecimal, Nullable: true, Precision: -3, Scale: 2,
	}, "precision of -3 is not a precision"},

	// The same shapes one level down: the walk is recursive, and #969's
	// nested arm was reachable at f415faba (ROW(DECIMAL(9,-1)) produced a
	// file pyarrow refuses).
	{"ROW carrying a bad DECIMAL", Column{
		Name: "r", Type: TypeRow, Nullable: true, Fields: []Column{
			{Name: "d", Type: TypeDecimal, Nullable: true, Precision: 9, Scale: -1},
		},
	}, "scale of -1 is negative"},
	{"ARRAY of a bad VECTOR", Column{
		Name: "a", Type: TypeArray, Nullable: true, ElementType: &Column{
			Name: "element", Type: TypeVector, Nullable: true, Dimension: math.MaxInt32/4 + 1,
		},
	}, "FIXED_LEN_BYTE_ARRAY type_length"},
	{"MAP whose value is a bad DECIMAL", Column{
		Name: "m", Type: TypeMap, Nullable: true, ElementType: &Column{Name: "kv", Type: TypeRow, Fields: []Column{
			{Name: "key", Type: TypeString},
			{Name: "value", Type: TypeDecimal, Nullable: true, Precision: 50, Scale: 2},
		}},
	}, "precision of 50 is past the 38 digits"},
}

// #970 #969 #971: one validation, every constructor.
//
// NewWriter has always called ValidateWriteSchema; NewNativeWriter — exported,
// documented with a usage example, and the door most of this package's own
// tests use — did not. Measured at f415faba, the malformed MAP through the
// native door: WriteMapRows nil, Close nil, 623 bytes, and then neither this
// package ("row group 0 column 0 carries path [a] but schema leaf 0 is
// [m key_value a]") nor pyarrow ("Malformed schema: not enough elements")
// could open the result.
//
// The native constructor cannot return an error, so it latches: the first
// WriteMapRows and the Close both refuse, and NOTHING is written.
func TestEveryConstructorRefusesASchemaItCannotWrite(t *testing.T) {
	for _, tc := range writeSchemaRefusalCases {
		schema := Schema{Columns: []Column{tc.column, {Name: "after", Type: TypeInt64, Nullable: true}}}

		t.Run("NewWriter/"+tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewWriter(&buf, schema, DefaultWriterConfig())
			if err == nil {
				_ = w.Close()
				t.Fatalf("the schema was accepted; the file is %d bytes", buf.Len())
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), tc.column.Name) {
				t.Errorf("error %q does not name the column %q", err, tc.column.Name)
			}
			if buf.Len() != 0 {
				t.Errorf("%d bytes were written before the refusal", buf.Len())
			}
		})

		t.Run("NewNativeWriter/"+tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewNativeWriter(&buf, schema, DefaultWriterConfig())
			err := w.WriteMapRows([]map[string]any{{"after": int64(1)}})
			if err == nil {
				t.Fatalf("WriteMapRows accepted a row against a schema this writer cannot write "+
					"(#970); the buffer is %d bytes", buf.Len())
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if cerr := w.Close(); cerr == nil {
				t.Fatalf("Close finalized a file over a schema it cannot write; %d bytes", buf.Len())
			}
			if buf.Len() != 0 {
				t.Errorf("%d bytes were written through the native door despite the refusal", buf.Len())
			}
		})
	}
}

// The refusals are refusals of a SHAPE, not of a type: the boundary values on
// the legal side still write a file both readers open.
func TestTheValidBoundaryDeclarationsStillWrite(t *testing.T) {
	cases := []struct {
		name   string
		column Column
		row    map[string]any
		rows   int
	}{
		{"DECIMAL at 38 digits", Column{
			Name: "d", Type: TypeDecimal, Precision: 38, Scale: 2, Nullable: true,
		}, map[string]any{"d": "1.25"}, 1},
		{"DECIMAL with scale == precision", Column{
			Name: "d", Type: TypeDecimal, Precision: 38, Scale: 38, Nullable: true,
		}, map[string]any{"d": "0.25"}, 1},
		{"DECIMAL with the unconstrained precision", Column{
			Name: "d", Type: TypeDecimal, Scale: 2, Nullable: true,
		}, map[string]any{"d": "1.25"}, 1},
		{"DECIMAL(1,0)", Column{
			Name: "d", Type: TypeDecimal, Precision: 1, Scale: 0, Nullable: true,
		}, map[string]any{"d": "7"}, 1},
		{"VECTOR(3)", Column{
			Name: "v", Type: TypeVector, Dimension: 3, Nullable: true,
		}, map[string]any{"v": []float32{1, 2, 3}}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewWriter(&buf, Schema{Columns: []Column{tc.column}}, DefaultWriterConfig())
			if err != nil {
				t.Fatalf("a legal declaration was refused: %v", err)
			}
			if err := w.WriteRows([]map[string]any{tc.row}); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			if _, err := NewReaderFromBytes(buf.Bytes()); err != nil {
				t.Fatalf("reopening: %v", err)
			}
			mustPyArrowRead(t, buf.Bytes(), tc.rows, tc.name)
		})
	}

	// VECTOR at the widest dimension the format's type_length can carry:
	// declared only, because one value of it is two gigabytes. pyarrow opened
	// the empty file at f415faba as fixed_size_binary[2147483644] and this is
	// the boundary the refusal above sits one component past.
	t.Run("VECTOR at MaxInt32/4 components", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, Schema{Columns: []Column{{
			Name: "v", Type: TypeVector, Dimension: math.MaxInt32 / 4, Nullable: true,
		}}}, DefaultWriterConfig())
		if err != nil {
			t.Fatalf("the widest legal VECTOR was refused: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if _, err := NewReaderFromBytes(buf.Bytes()); err != nil {
			t.Fatalf("reopening: %v", err)
		}
		res, ran := pyArrowOpens(t, buf.Bytes())
		if !ran {
			t.Log("python3 with pyarrow is not importable here — skipping the PyArrow cross-check")
			return
		}
		if !res.OK {
			t.Fatalf("pyarrow refuses the widest legal VECTOR: %s", res.Err)
		}
		if !strings.Contains(res.Schema, "fixed_size_binary[2147483644]") {
			t.Fatalf("pyarrow sees %q, want a fixed_size_binary[2147483644] leaf", res.Schema)
		}
	})
}

// The two bound helpers, driven at their boundaries directly, so the exact
// component and digit where the refusal starts is pinned rather than inferred
// from a file.
func TestVectorAndDecimalBoundaries(t *testing.T) {
	vectorCases := []struct {
		dim     int
		want    int32
		wantErr bool
	}{
		{dim: 1, want: 4},
		{dim: 3, want: 12},
		{dim: math.MaxInt32 / 4, want: math.MaxInt32 - 3}, // 2147483644
		{dim: math.MaxInt32/4 + 1, wantErr: true},
		{dim: math.MaxInt32, wantErr: true},
		{dim: 0, wantErr: true},
		{dim: -1, wantErr: true},
	}
	for _, c := range vectorCases {
		got, err := vectorTypeLength(c.dim)
		if c.wantErr {
			if err == nil {
				t.Errorf("vectorTypeLength(%d) = %d with no error — the width was narrowed instead of "+
					"refused (#971)", c.dim, got)
			} else if got != 0 {
				t.Errorf("vectorTypeLength(%d) returned %d beside its error", c.dim, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("vectorTypeLength(%d) = error %v, want %d", c.dim, err, c.want)
		} else if got != c.want {
			t.Errorf("vectorTypeLength(%d) = %d, want %d", c.dim, got, c.want)
		}
	}

	decimalCases := []struct {
		p, s         int
		wantP, wantS int32
		wantErr      bool
		note         string
	}{
		{p: 9, s: 2, wantP: 9, wantS: 2},
		{p: 38, s: 38, wantP: 38, wantS: 38, note: "scale == precision is legal"},
		{p: 1, s: 0, wantP: 1, wantS: 0},
		{p: 0, s: 0, wantP: 38, wantS: 0, note: "0 is the unconstrained sentinel"},
		{p: 0, s: 38, wantP: 38, wantS: 38, note: "measured against the precision the FILE declares"},
		{p: 0, s: 39, wantErr: true},
		{p: 9, s: -1, wantErr: true},
		{p: 4, s: 9, wantErr: true},
		{p: 39, s: 2, wantErr: true},
		{p: -1, s: 0, wantErr: true},
	}
	for _, c := range decimalCases {
		gotP, gotS, err := checkDecimalDeclaration(c.p, c.s)
		if c.wantErr {
			if err == nil {
				t.Errorf("checkDecimalDeclaration(%d, %d) = (%d, %d) with no error — the declaration was "+
					"silently changed instead of refused (#969)", c.p, c.s, gotP, gotS)
			}
			continue
		}
		if err != nil {
			t.Errorf("checkDecimalDeclaration(%d, %d) = error %v, want (%d, %d) [%s]",
				c.p, c.s, err, c.wantP, c.wantS, c.note)
			continue
		}
		if gotP != c.wantP || gotS != c.wantS {
			t.Errorf("checkDecimalDeclaration(%d, %d) = (%d, %d), want (%d, %d) [%s]",
				c.p, c.s, gotP, gotS, c.wantP, c.wantS, c.note)
		}
	}
}
