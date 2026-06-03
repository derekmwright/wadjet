package batch

import (
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestBitmapEnsureLen_DefaultsNonNullAndPreserves verifies that growing a
// bitmap defaults the new bits to non-null (1) while preserving existing null
// state, across word boundaries.
func TestBitmapEnsureLen_DefaultsNonNullAndPreserves(t *testing.T) {
	b := NewBitmap(0)
	b.EnsureLen(3)
	b.SetNull(1)

	// Grow well past a 64-bit word boundary.
	b.EnsureLen(200)

	if !b.IsNull(1) {
		t.Fatal("existing null bit at index 1 not preserved after grow")
	}
	for _, i := range []int{0, 2, 63, 64, 65, 128, 199} {
		if b.IsNull(i) {
			t.Fatalf("bit %d should default to non-null after grow", i)
		}
	}
	// Out-of-range still reads as null (defensive bounds).
	if !b.IsNull(200) {
		t.Fatal("index 200 is out of range and should read null")
	}

	// A null set after growth must stick, and HasNulls must reflect it.
	b.SetNull(150)
	if !b.IsNull(150) {
		t.Fatal("null set after grow not recorded")
	}
	if !b.HasNulls() {
		t.Fatal("HasNulls should be true after a SetNull")
	}
}

// TestBitmapEnsureLen_Idempotent confirms shrinking-or-equal requests are no-ops.
func TestBitmapEnsureLen_Idempotent(t *testing.T) {
	b := NewBitmap(0)
	b.EnsureLen(100)
	b.SetNull(50)
	b.EnsureLen(40) // <= len: no-op
	if !b.IsNull(50) {
		t.Fatal("EnsureLen with smaller n must not disturb existing bits")
	}
}

// TestVectorEnsureLen_FixedWidth grows a fixed-width vector incrementally,
// writing through both the realloc and the reslice-within-cap paths, and
// verifies values + nulls survive growth.
func TestVectorEnsureLen_FixedWidth(t *testing.T) {
	v := NewVector(TypeInt64, 0)
	// Write 5 values across two grows (forces at least one realloc).
	v.EnsureLen(3)
	for i := 0; i < 3; i++ {
		v.Int64Data[i] = int64(i + 1)
	}
	v.EnsureLen(5)
	v.Int64Data[3] = 40
	v.Nulls.SetNull(4) // leave index 4 data zero, marked null

	if v.Len != 5 {
		t.Fatalf("Len = %d, want 5", v.Len)
	}
	want := []int64{1, 2, 3, 40, 0}
	for i, w := range want {
		if v.Int64Data[i] != w {
			t.Fatalf("Int64Data[%d] = %d, want %d (data not preserved across grow)", i, v.Int64Data[i], w)
		}
	}
	for i := 0; i < 4; i++ {
		if v.Nulls.IsNull(i) {
			t.Fatalf("index %d unexpectedly null", i)
		}
	}
	if !v.Nulls.IsNull(4) {
		t.Fatal("index 4 should be null")
	}
}

// TestVectorEnsureLen_Bytes grows a string vector and verifies sequential
// SetFrom-style appends keep offset continuity after growth.
func TestVectorEnsureLen_Bytes(t *testing.T) {
	src := NewVector(TypeString, 2)
	src.BytesData.Set(0, []byte("alpha"))
	src.BytesData.Set(1, []byte("beta"))

	dst := NewVector(TypeString, 0)
	dst.EnsureLen(1)
	dst.BytesData.SetFrom(0, &src.BytesData, 0) // "alpha"
	dst.EnsureLen(2)
	dst.BytesData.SetFrom(1, &src.BytesData, 1) // "beta"

	if got := string(dst.BytesData.Value(0)); got != "alpha" {
		t.Fatalf("value 0 = %q, want alpha", got)
	}
	if got := string(dst.BytesData.Value(1)); got != "beta" {
		t.Fatalf("value 1 = %q, want beta", got)
	}
}

// TestRecordBatchEnsureCapacity_GrowsAllColumnsAndLen builds a batch row-by-row
// purely through EnsureCapacity growth (no pre-sizing) and checks the result is
// a well-formed batch.
func TestRecordBatchEnsureCapacity_GrowsAllColumnsAndLen(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	b := NewRecordBatch(schema, 0)

	const n = 100
	for i := 0; i < n; i++ {
		b.EnsureCapacity(i + 1)
		b.Columns[0].Int64Data[i] = int64(i * 10)
		b.Columns[1].BytesData.Set(i, []byte{byte('a' + i%26)})
	}

	if b.Len != n {
		t.Fatalf("Len = %d, want %d", b.Len, n)
	}
	if len(b.Columns[0].Int64Data) < n {
		t.Fatalf("id column len %d < %d", len(b.Columns[0].Int64Data), n)
	}
	for i := 0; i < n; i++ {
		if b.Columns[0].Int64Data[i] != int64(i*10) {
			t.Fatalf("id[%d] = %d, want %d", i, b.Columns[0].Int64Data[i], i*10)
		}
		if got := string(b.Columns[1].BytesData.Value(i)); got != string(rune('a'+i%26)) {
			t.Fatalf("name[%d] = %q, want %q", i, got, string(rune('a'+i%26)))
		}
	}
}
