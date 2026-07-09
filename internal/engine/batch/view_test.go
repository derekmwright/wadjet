package batch

import (
	"math"
	"reflect"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// buildVec constructs an owned vector from boxed values (nil = null) using
// the public SetValue path, so expected results can be derived with GetValue.
func buildVec(tb testing.TB, typ TypeID, scale int, vals []any) *Vector {
	tb.Helper()
	v := NewVectorWithScale(typ, len(vals), scale)
	for i, val := range vals {
		v.SetValue(i, val)
	}
	return v
}

// checkViewValues verifies the view resolves to the expected boxed values
// both before Flatten (view-aware GetValue) and after (owned storage).
func checkViewValues(tb testing.TB, view *Vector, want []any) {
	tb.Helper()
	if !view.IsView() {
		tb.Fatalf("expected a view vector")
	}
	if view.Len != len(want) {
		tb.Fatalf("view Len = %d, want %d", view.Len, len(want))
	}
	for i, w := range want {
		got := view.GetValue(i)
		if !reflect.DeepEqual(got, w) {
			tb.Fatalf("pre-flatten GetValue(%d) = %#v, want %#v", i, got, w)
		}
	}
	view.Flatten()
	if view.IsView() {
		tb.Fatalf("Flatten left the vector a view")
	}
	if view.Len != len(want) {
		tb.Fatalf("flattened Len = %d, want %d", view.Len, len(want))
	}
	for i, w := range want {
		got := view.GetValue(i)
		if !reflect.DeepEqual(got, w) {
			tb.Fatalf("post-flatten GetValue(%d) = %#v, want %#v", i, got, w)
		}
	}
}

func TestViewFlattenAllFlatTypes(t *testing.T) {
	cases := []struct {
		name  string
		typ   TypeID
		scale int
		vals  []any
	}{
		{"bool", TypeBool, 0, []any{true, nil, false, true}},
		{"int32", TypeInt32, 0, []any{int32(1), int32(-7), nil, int32(2048)}},
		{"int64", TypeInt64, 0, []any{int64(1), nil, int64(-9e15), int64(42)}},
		{"float32", TypeFloat32, 0, []any{float32(1.5), nil, float32(-0.25), float32(0)}},
		{"float64", TypeFloat64, 0, []any{1.5, nil, -2.75, 0.0}},
		{"string", TypeString, 0, []any{"alpha", "", nil, "a longer string value"}},
		{"bytes", TypeBytes, 0, []any{[]byte{1, 2, 3}, []byte{}, nil, []byte{0xff}}},
		{"timestamp", TypeTimestamp, 0, []any{int64(1700000000000), nil, int64(0), int64(-1)}},
		{"ipv4", TypeIPv4, 0, []any{"10.0.0.1", nil, "255.255.255.255", "0.0.0.0"}},
		{"ipv6", TypeIPv6, 0, []any{"::1", nil, "fe80::1", "2001:db8::ff"}},
		{"cidr", TypeCIDR, 0, []any{"10.0.0.0/8", nil, "192.168.1.0/24", "::/0"}},
		{"mac", TypeMAC, 0, []any{"aa:bb:cc:dd:ee:ff", nil, "00:00:00:00:00:01", "ff:ff:ff:ff:ff:ff"}},
		{"port", TypePort, 0, []any{int32(443), nil, int32(0), int32(65535)}},
		{"protocol", TypeProtocol, 0, []any{int32(6), int32(17), nil, int32(1)}},
		{"duration", TypeDuration, 0, []any{int64(1000), nil, int64(-5), int64(0)}},
		{"uuid", TypeUUID, 0, []any{"01234567-89ab-cdef-0123-456789abcdef", nil, "ffffffff-ffff-ffff-ffff-ffffffffffff", "00000000-0000-0000-0000-000000000000"}},
		{"date", TypeDate, 0, []any{"2026-07-08", nil, "1970-01-01", "1999-12-31"}},
		{"decimal", TypeDecimal, 2, []any{"123.45", nil, "-0.01", "0.00"}},
	}
	// Indices exercise permutation, duplication (1:N join expansion) and
	// repeats of null source rows.
	indices := []uint32{3, 0, 0, 2, 1, 3}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := buildVec(t, tc.typ, tc.scale, tc.vals)
			want := make([]any, len(indices))
			for i, si := range indices {
				want[i] = base.GetValue(int(si))
			}
			view := NewViewVector(base, append([]uint32(nil), indices...))
			checkViewValues(t, view, want)
		})
	}
}

func TestViewFlattenNested(t *testing.T) {
	t.Run("array", func(t *testing.T) {
		base := NewArrayVector(3, TypeInt64)
		base.SetValue(0, []any{int64(1), int64(2)})
		base.SetValue(1, nil)
		base.SetValue(2, []any{int64(7)})
		indices := []uint32{2, 0, 1, 0}
		want := make([]any, len(indices))
		for i, si := range indices {
			want[i] = base.GetValue(int(si))
		}
		view := NewViewVector(base, indices)
		checkViewValues(t, view, want)
	})
	t.Run("row", func(t *testing.T) {
		base := NewRowVector(3, []string{"a", "b"}, []TypeID{TypeInt64, TypeString})
		base.SetValue(0, map[string]any{"a": int64(1), "b": "x"})
		base.SetValue(1, nil)
		base.SetValue(2, map[string]any{"a": int64(3), "b": ""})
		indices := []uint32{1, 2, 0, 2}
		want := make([]any, len(indices))
		for i, si := range indices {
			want[i] = base.GetValue(int(si))
		}
		view := NewViewVector(base, indices)
		checkViewValues(t, view, want)
	})
	t.Run("row_null_first", func(t *testing.T) {
		// First flattened rows are null: ROW children must advance their
		// variable-length bookkeeping (the pre-allocated-children guarantee
		// in newOwnedLike) or later string rows read back shifted.
		base := NewRowVector(2, []string{"s"}, []TypeID{TypeString})
		base.SetValue(0, nil)
		base.SetValue(1, map[string]any{"s": "tail"})
		indices := []uint32{0, 0, 1}
		want := []any{base.GetValue(0), base.GetValue(0), base.GetValue(1)}
		view := NewViewVector(base, indices)
		checkViewValues(t, view, want)
	})
	t.Run("map", func(t *testing.T) {
		base := NewMapVector(2, TypeString, TypeInt64)
		base.SetValue(0, []any{map[string]any{"key": "k1", "value": int64(1)}})
		base.SetValue(1, []any{map[string]any{"key": "k2", "value": int64(2)}, map[string]any{"key": "k3", "value": int64(3)}})
		indices := []uint32{1, 0}
		want := []any{base.GetValue(1), base.GetValue(0)}
		view := NewViewVector(base, indices)
		checkViewValues(t, view, want)
	})
	t.Run("vector", func(t *testing.T) {
		base := NewVectorVector(3, 2)
		base.SetVector(0, []float32{1, 2})
		base.SetVector(1, []float32{3, 4})
		base.SetVector(2, []float32{5, 6})
		indices := []uint32{2, 2, 0}
		want := []any{base.GetValue(2), base.GetValue(2), base.GetValue(0)}
		view := NewViewVector(base, indices)
		checkViewValues(t, view, want)
	})
}

func TestViewOwnNullOverride(t *testing.T) {
	// Outer-join null-fill: own-null rows are null regardless of the (
	// meaningless) index value, for both flat and variable-length types.
	base := buildVec(t, TypeString, 0, []any{"a", "b", "c"})
	view := NewViewVector(base, []uint32{0, 1, 2, 0})
	view.Nulls.SetNull(1)
	view.Nulls.SetNull(3)
	want := []any{"a", nil, "c", nil}
	checkViewValues(t, view, want)

	ints := buildVec(t, TypeInt64, 0, []any{int64(10), int64(20)})
	iview := NewViewVector(ints, []uint32{1, 0})
	iview.Nulls.SetNull(0)
	checkViewValues(t, iview, []any{nil, int64(10)})
}

func TestViewComposition(t *testing.T) {
	// A view over a view composes indices and folds own-nulls; Base is
	// always the owned vector.
	base := buildVec(t, TypeInt64, 0, []any{int64(0), int64(10), int64(20), int64(30)})
	v1 := NewViewVector(base, []uint32{3, 2, 1, 0})
	v1.Nulls.SetNull(2) // logical row 2 of v1 (base row 1) nulled
	v2 := NewViewVector(v1, []uint32{2, 0, 1, 2})
	if v2.Base != base {
		t.Fatalf("composition did not collapse to the owned base")
	}
	want := []any{nil, int64(30), int64(20), nil}
	checkViewValues(t, v2, want)
}

func TestViewFloat64NaN(t *testing.T) {
	base := NewVector(TypeFloat64, 2)
	base.Float64Data[0] = math.NaN()
	base.Float64Data[1] = 1.0
	view := NewViewVector(base, []uint32{0, 1, 0})
	view.Flatten()
	if !math.IsNaN(view.Float64Data[0]) || !math.IsNaN(view.Float64Data[2]) {
		t.Fatalf("NaN not preserved through flatten: %v", view.Float64Data)
	}
	if view.Float64Data[1] != 1.0 {
		t.Fatalf("Float64Data[1] = %v, want 1.0", view.Float64Data[1])
	}
}

func TestViewFlattenCopiesStorage(t *testing.T) {
	// Flatten must produce an independent copy: mutating the base afterwards
	// must not change flattened values (the whole point of materialization).
	base := buildVec(t, TypeString, 0, []any{"one", "two"})
	view := NewViewVector(base, []uint32{1, 0})
	view.Flatten()
	base.BytesData.Reset()
	base.BytesData.Set(0, []byte("XXX"))
	base.BytesData.Set(1, []byte("YYY"))
	if got, _ := view.GetString(0); got != "two" {
		t.Fatalf("flattened value aliases base storage: got %q", got)
	}
}

func TestViewEmptyAndSingle(t *testing.T) {
	base := buildVec(t, TypeInt64, 0, []any{int64(5)})
	empty := NewViewVector(base, []uint32{})
	checkViewValues(t, empty, []any{})
	single := NewViewVector(base, []uint32{0})
	checkViewValues(t, single, []any{int64(5)})
}

func TestViewMemBytes(t *testing.T) {
	base := buildVec(t, TypeInt64, 0, make([]any, 1000))
	for i := range 1000 {
		base.SetValue(i, int64(i))
	}
	view := NewViewVector(base, make([]uint32, 500))
	got := view.MemBytes()
	// 500 uint32 indices + 500-bit own bitmap (8 words) — and crucially NOT
	// the base's 8000 data bytes (single-owner accounting).
	want := int64(500*4 + 8*8)
	if got != want {
		t.Fatalf("view MemBytes = %d, want %d", got, want)
	}
}

func TestViewEnsureLenPanics(t *testing.T) {
	base := buildVec(t, TypeInt64, 0, []any{int64(1)})
	view := NewViewVector(base, []uint32{0})
	defer func() {
		if recover() == nil {
			t.Fatalf("EnsureLen on a view did not panic")
		}
	}()
	view.EnsureLen(10)
}

func TestViewCopyValueFromViewSource(t *testing.T) {
	// Generic per-value copy (Compact, spill writers, accumulator adopt)
	// must read through a view source, honoring own-null overrides.
	base := buildVec(t, TypeString, 0, []any{"a", "b", "c"})
	view := NewViewVector(base, []uint32{2, 1, 0})
	view.Nulls.SetNull(1)
	dst := NewVector(TypeString, 3)
	for i := range 3 {
		dst.CopyValueFrom(i, view, i)
	}
	want := []any{"c", nil, "a"}
	for i, w := range want {
		if got := dst.GetValue(i); !reflect.DeepEqual(got, w) {
			t.Fatalf("dst[%d] = %#v, want %#v", i, got, w)
		}
	}
}

func TestViewAppendFromViewSource(t *testing.T) {
	base := buildVec(t, TypeInt64, 0, []any{int64(7), int64(8)})
	view := NewViewVector(base, []uint32{1, 0})
	view.Nulls.SetNull(0)
	dst := NewVectorLike(base)
	dst.AppendFrom(view, 0)
	dst.AppendFrom(view, 1)
	if got := dst.GetValue(0); got != nil {
		t.Fatalf("dst[0] = %#v, want nil", got)
	}
	if got := dst.GetValue(1); got != int64(7) {
		t.Fatalf("dst[1] = %#v, want 7", got)
	}
}

func TestBatchHasViewsFlattenViews(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}
	owned := NewRecordBatch(schema, 2)
	owned.Columns[0].SetValue(0, int64(1))
	owned.Columns[0].SetValue(1, int64(2))
	owned.Columns[1].SetValue(0, "x")
	owned.Columns[1].SetValue(1, "y")
	if owned.HasViews() {
		t.Fatalf("owned batch reports views")
	}

	b := &RecordBatch{
		Columns: []*Vector{
			NewViewVector(owned.Columns[0], []uint32{1, 0}),
			NewViewVector(owned.Columns[1], []uint32{1, 0}),
		},
		Schema: schema,
		Len:    2,
	}
	if !b.HasViews() {
		t.Fatalf("view batch does not report views")
	}
	b.FlattenColumn(0)
	if b.Columns[0].IsView() || !b.Columns[1].IsView() {
		t.Fatalf("FlattenColumn touched the wrong column")
	}
	b.FlattenViews()
	if b.HasViews() {
		t.Fatalf("FlattenViews left views behind")
	}
	if got, _ := b.Columns[0].GetInt64(0); got != 2 {
		t.Fatalf("flattened k[0] = %d, want 2", got)
	}
	if got, _ := b.Columns[1].GetString(1); got != "x" {
		t.Fatalf("flattened s[1] = %q, want x", got)
	}
}

func TestBatchCompactWithViewColumn(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString},
	}
	owned := NewRecordBatch(schema, 3)
	for i := range 3 {
		owned.Columns[0].SetValue(i, int64(i*10))
	}
	owned.Columns[1].SetValue(0, "a")
	owned.Columns[1].SetValue(1, "b")
	owned.Columns[1].SetValue(2, "c")

	b := &RecordBatch{
		Columns: []*Vector{
			NewViewVector(owned.Columns[0], []uint32{2, 1, 0}),
			NewViewVector(owned.Columns[1], []uint32{2, 1, 0}),
		},
		Schema: schema,
		Len:    3,
		Sel:    []uint32{0, 2},
	}
	out := b.Compact()
	if out.Len != 2 {
		t.Fatalf("compact Len = %d, want 2", out.Len)
	}
	if got := out.Columns[0].GetValue(0); got != int64(20) {
		t.Fatalf("compact k[0] = %#v, want 20", got)
	}
	if got := out.Columns[1].GetValue(1); got != "a" {
		t.Fatalf("compact v[1] = %#v, want a", got)
	}
}

func TestBatchResetClearsViews(t *testing.T) {
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	owned := NewRecordBatch(schema, 2)
	owned.Columns[0].SetValue(0, int64(1))
	owned.Columns[0].SetValue(1, int64(2))

	b := NewRecordBatch(schema, 2)
	b.Columns[0] = NewViewVector(owned.Columns[0], []uint32{1, 0})
	b.Reset(2)
	if b.HasViews() {
		t.Fatalf("Reset left a view column — pooled reuse would read stale indirection")
	}
}

func TestViewRowAtAndToRows(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}
	owned := NewRecordBatch(schema, 2)
	owned.Columns[0].SetValue(0, int64(1))
	owned.Columns[0].SetValue(1, int64(2))
	owned.Columns[1].SetValue(0, "x")
	owned.Columns[1].SetValue(1, "y")
	b := &RecordBatch{
		Columns: []*Vector{
			NewViewVector(owned.Columns[0], []uint32{1, 0}),
			NewViewVector(owned.Columns[1], []uint32{1, 0}),
		},
		Schema: schema,
		Len:    2,
	}
	row := b.RowAt(0)
	if row["k"] != int64(2) || row["s"] != "y" {
		t.Fatalf("RowAt(0) = %#v", row)
	}
	rows := b.ToRows()
	if len(rows) != 2 || rows[1]["k"] != int64(1) || rows[1]["s"] != "x" {
		t.Fatalf("ToRows = %#v", rows)
	}
}
