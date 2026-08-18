package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The shape-only contract: a BytesColumn carrying lengths but no bytes must
// answer LengthAt, must refuse any value read loudly, must propagate rather
// than corrupt through the copy paths, and must never survive a pooled
// reuse cycle.

func shapeColumn(t *testing.T, lengths []int) *Vector {
	t.Helper()
	v := NewVector(TypeString, len(lengths))
	cur := uint32(0)
	for i, l := range lengths {
		cur += uint32(l)
		v.BytesData.Offsets[i+1] = cur
	}
	v.BytesData.ShapeOnly = true
	return v
}

func TestShapeOnlyLengthAt(t *testing.T) {
	lengths := []int{0, 3, 90, 1}
	v := shapeColumn(t, lengths)
	for i, want := range lengths {
		if got := v.BytesData.LengthAt(i); got != want {
			t.Errorf("row %d: LengthAt = %d, want %d", i, got, want)
		}
	}
}

func TestShapeOnlyValueReadPanics(t *testing.T) {
	v := shapeColumn(t, []int{5})
	for name, read := range map[string]func(){
		"Value":             func() { _ = v.BytesData.Value(0) },
		"StringValue":       func() { _ = v.BytesData.StringValue(0) },
		"UnsafeStringValue": func() { _ = v.BytesData.UnsafeStringValue(0) },
		"GetValue":          func() { _ = v.GetValue(0) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s on a shape-only column must panic", name)
				}
			}()
			read()
		})
	}
}

func TestShapeOnlyCopyPropagates(t *testing.T) {
	src := shapeColumn(t, []int{0, 7, 12, 3})
	dst := NewVector(TypeString, 4)

	dst.BytesData.BulkCopy(0, &src.BytesData, 0, 4)
	if !dst.BytesData.ShapeOnly {
		t.Fatal("BulkCopy must propagate the shape-only mark")
	}
	if len(dst.BytesData.Data) != 0 {
		t.Fatalf("BulkCopy materialized %d bytes", len(dst.BytesData.Data))
	}
	for i, want := range []int{0, 7, 12, 3} {
		if got := dst.BytesData.LengthAt(i); got != want {
			t.Errorf("BulkCopy row %d: length %d, want %d", i, got, want)
		}
	}

	// Row-at-a-time gather (Compact / projectGatherColumn) takes SetFrom.
	gathered := NewVector(TypeString, 2)
	gathered.BytesData.SetFrom(0, &src.BytesData, 3)
	gathered.BytesData.SetFrom(1, &src.BytesData, 1)
	if !gathered.BytesData.ShapeOnly {
		t.Fatal("SetFrom must propagate the shape-only mark")
	}
	if got := gathered.BytesData.LengthAt(0); got != 3 {
		t.Errorf("SetFrom row 0: length %d, want 3", got)
	}
	if got := gathered.BytesData.LengthAt(1); got != 7 {
		t.Errorf("SetFrom row 1: length %d, want 7", got)
	}
}

// CopyValueFrom is the gather entry (Compact, projectGatherColumn). A NULL
// row writes a zero offset rather than going through copyShapeRange, so the
// destination's offsets stop being globally monotonic — non-null rows must
// still report their true length, and null rows are never asked.
func TestShapeOnlyGatherWithNulls(t *testing.T) {
	src := shapeColumn(t, []int{11, 0, 22, 33})
	src.Nulls.SetNull(1)
	dst := NewVector(TypeString, 4)
	for di, si := range []int{0, 1, 2, 3} {
		dst.CopyValueFrom(di, src, si)
	}
	if !dst.BytesData.ShapeOnly {
		t.Fatal("CopyValueFrom must propagate the shape-only mark")
	}
	if !dst.Nulls.IsNull(1) {
		t.Fatal("null row lost across the gather")
	}
	for i, want := range map[int]int{0: 11, 2: 22, 3: 33} {
		if got := dst.BytesData.LengthAt(i); got != want {
			t.Errorf("row %d: length %d, want %d", i, got, want)
		}
	}
}

// A shape-only source copied into a destination that already holds real
// values would silently corrupt it — that combination must fail loudly.
func TestShapeOnlyCopyIntoValuesPanics(t *testing.T) {
	src := shapeColumn(t, []int{4})
	dst := NewVector(TypeString, 2)
	dst.BytesData.Set(0, []byte("real"))
	defer func() {
		if recover() == nil {
			t.Fatal("copying shape-only into a value-bearing column must panic")
		}
	}()
	dst.BytesData.SetFrom(1, &src.BytesData, 0)
}

func TestShapeOnlyClearedByPooledReuse(t *testing.T) {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString, Nullable: true}}
	pool := NewBatchPool(schema, 8)
	b := pool.GetForSize(4)
	b.Columns[0].BytesData.ShapeOnly = true
	b.Columns[0].BytesData.Offsets[1] = 90
	b.Release()

	reused := pool.GetForSize(4)
	if reused.Columns[0].BytesData.ShapeOnly {
		t.Fatal("shape-only mark survived a pooled reuse cycle")
	}
	// And the column must behave as a normal empty one again.
	reused.Columns[0].BytesData.Set(0, []byte("ok"))
	if got := string(reused.Columns[0].BytesData.Value(0)); got != "ok" {
		t.Fatalf("reused column value = %q, want %q", got, "ok")
	}
}
