package batch

import (
	"bytes"
	"testing"
)

// TestGetValueBytesDoesNotAliasArena pins the boxing contract of
// Vector.GetValue: the value it returns owns its storage. Every other arm
// already did (String materializes a string, IPv6/CIDR/UUID format, VECTOR
// make+copy); TypeBytes returned BytesData.Value(i) = Data[start:end], a live
// window into the producer's arena. Consumers box through GetValue precisely
// because they intend to KEEP the value past the batch — and none of them
// Claim the vector or Detach the batch — so every producer that reuses its
// output backing (Project's BatchPool, the join emit path, the scan
// row-group backing pool) rewrote the value under them.
func TestGetValueBytesDoesNotAliasArena(t *testing.T) {
	for _, typ := range []TypeID{TypeBytes, TypeString} {
		t.Run(typ.String(), func(t *testing.T) {
			v := NewVector(typ, 2)
			v.BytesData.Set(0, []byte("first-value"))
			v.BytesData.Set(1, []byte("second-val"))

			boxed := make([]string, 2)
			for i := range boxed {
				switch got := v.GetValue(i).(type) {
				case []byte:
					boxed[i] = string(got)
					// The box must not share storage with the arena.
					if len(got) > 0 && &got[0] == &v.BytesData.Data[v.BytesData.Offsets[i]] {
						t.Fatalf("row %d: GetValue returned the arena slice itself", i)
					}
				case string:
					boxed[i] = got
				default:
					t.Fatalf("row %d: GetValue returned %T", i, got)
				}
			}

			// Reuse the backing exactly as a producer does: ResetForWrite
			// empties the arena but keeps its capacity, so equal-length
			// writes land on the same bytes.
			v.ResetForWrite(2)
			v.BytesData.Set(0, []byte("OVERWRITTEN"))
			v.BytesData.Set(1, []byte("clobbered!"))

			for i, want := range []string{"first-value", "second-val"} {
				if boxed[i] != want {
					t.Errorf("row %d boxed value = %q after backing reuse, want %q", i, boxed[i], want)
				}
			}
		})
	}
}

// TestGetValueBytesPreservesEmptyAndNull keeps the copy from changing what
// the box looks like: an empty value stays a non-nil empty slice (Data[s:s]),
// a corrupt descending offset pair stays nil, and a NULL row stays nil —
// consumers type-switch and nil-check these.
func TestGetValueBytesPreservesEmptyAndNull(t *testing.T) {
	v := NewVector(TypeBytes, 3)
	v.BytesData.Set(0, nil)
	v.BytesData.Set(1, []byte("x"))
	v.BytesData.Set(2, []byte("y"))
	v.Nulls.SetNull(2)

	got0, ok := v.GetValue(0).([]byte)
	if !ok {
		t.Fatalf("row 0 boxed as %T, want []byte", v.GetValue(0))
	}
	if got0 == nil || len(got0) != 0 {
		t.Errorf("empty BYTES boxed as %#v, want a non-nil empty slice", got0)
	}
	if got1, _ := v.GetValue(1).([]byte); !bytes.Equal(got1, []byte("x")) {
		t.Errorf("row 1 = %q, want \"x\"", got1)
	}
	if got2 := v.GetValue(2); got2 != nil {
		t.Errorf("NULL row boxed as %#v, want nil", got2)
	}
}
