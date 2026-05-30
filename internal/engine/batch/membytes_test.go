package batch

import "testing"

// bitmapBytes returns the byte footprint of a freshly-constructed vector's null
// bitmap, so fixed-width expectations can be stated as data bytes + bitmap.
func bitmapBytes(t *testing.T, v *Vector) int64 {
	t.Helper()
	return int64(len(v.Nulls.Words())) * 8
}

func TestVectorMemBytes_FixedWidth(t *testing.T) {
	cases := []struct {
		name string
		typ  TypeID
		n    int
		want int64 // typed-slice bytes only; bitmap added separately
	}{
		{"int64", TypeInt64, 100, 100 * 8},
		{"int32", TypeInt32, 100, 100 * 4},
		{"bool", TypeBool, 100, 100 * 1},
		{"float64", TypeFloat64, 100, 100 * 8},
		{"float32", TypeFloat32, 100, 100 * 4},
		{"date", TypeDate, 100, 100 * 4},
		{"port", TypePort, 100, 100 * 4},
		{"timestamp", TypeTimestamp, 100, 100 * 8},
		{"ipv4", TypeIPv4, 100, 100 * 8}, // stored as int64, NOT uint32
		{"mac", TypeMAC, 100, 100 * 8},
		{"duration", TypeDuration, 100, 100 * 8},
		{"decimal", TypeDecimal, 100, 100 * 16}, // Int128
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := NewVector(tc.typ, tc.n)
			want := tc.want + bitmapBytes(t, v)
			if got := v.MemBytes(); got != want {
				t.Fatalf("%s: got %d, want %d (data=%d bitmap=%d)",
					tc.name, got, want, tc.want, bitmapBytes(t, v))
			}
		})
	}
}

func TestVectorMemBytes_String(t *testing.T) {
	v := NewVector(TypeString, 3)
	v.BytesData.Set(0, []byte("hello")) // 5
	v.BytesData.Set(1, []byte("world")) // 5
	v.BytesData.Set(2, []byte("!"))     // 1

	wantOffsets := int64(len(v.BytesData.Offsets)) * 4 // 4 offsets * 4
	wantData := int64(cap(v.BytesData.Data))           // arena cap, not len
	wantBitmap := bitmapBytes(t, v)
	want := wantOffsets + wantData + wantBitmap
	if got := v.MemBytes(); got != want {
		t.Fatalf("got %d, want %d (offsets=%d data=%d bitmap=%d)",
			got, want, wantOffsets, wantData, wantBitmap)
	}
}

func TestVectorMemBytes_Vector(t *testing.T) {
	const dim = 8
	v := NewVectorVector(10, dim)
	want := int64(10*dim)*4 + bitmapBytes(t, v)
	if got := v.MemBytes(); got != want {
		t.Fatalf("vector: got %d, want %d", got, want)
	}
}

func TestVectorMemBytes_RowRecursesAndCountsNames(t *testing.T) {
	// ROW(a int64, bb string) — children recurse, FieldNames counted.
	v := NewRowVector(10, []string{"a", "bb"}, []TypeID{TypeInt64, TypeString})
	got := v.MemBytes()

	// Floor: int64 child data + the two field-name charges. Strictly less than
	// the true value (which also includes both children's bitmaps + string
	// offsets + parent bitmap), so a `>=` check is a safe lower bound.
	floor := int64(10*8) + int64(len("a")+16) + int64(len("bb")+16)
	if got < floor {
		t.Fatalf("ROW MemBytes %d below floor %d", got, floor)
	}

	// Explicit exact check: parent bitmap + Σ children + Σ name charges.
	child0 := v.Children[0].MemBytes()
	child1 := v.Children[1].MemBytes()
	names := int64(len("a")+16) + int64(len("bb")+16)
	want := bitmapBytes(t, v) + child0 + child1 + names
	if got != want {
		t.Fatalf("ROW MemBytes got %d, want %d (c0=%d c1=%d names=%d bitmap=%d)",
			got, want, child0, child1, names, bitmapBytes(t, v))
	}
}

func TestVectorMemBytes_ArrayRecursesIntoChild(t *testing.T) {
	v := NewArrayVector(5, TypeInt64)
	// Append a few child elements so the child slice is non-empty.
	v.Child.Int64Data = append(v.Child.Int64Data, 1, 2, 3)
	want := bitmapBytes(t, v) + int64(len(v.Offsets))*4 + v.Child.MemBytes()
	if got := v.MemBytes(); got != want {
		t.Fatalf("ARRAY MemBytes got %d, want %d", got, want)
	}
}

func TestVectorMemBytes_NilSafe(t *testing.T) {
	var v *Vector
	if got := v.MemBytes(); got != 0 {
		t.Fatalf("nil vector MemBytes = %d, want 0", got)
	}
}

func TestBytesColumnMemBytes(t *testing.T) {
	bc := NewBytesColumn(3)
	bc.Set(0, []byte("abc"))
	bc.Set(1, []byte("de"))
	want := int64(len(bc.Offsets))*4 + int64(cap(bc.Data))
	if got := bc.MemBytes(); got != want {
		t.Fatalf("BytesColumn MemBytes got %d, want %d", got, want)
	}
}
