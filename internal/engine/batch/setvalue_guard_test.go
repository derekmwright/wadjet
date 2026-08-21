package batch

import (
	"errors"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #361's systemic half: a SetValue of a value the destination vector cannot
// hold must be LOUD. Before the guard the write vanished — the slot kept 0
// and was marked valid — which is the shared mechanism of #310, #327, #331,
// #333, #345, #353, #371 and #372. The guard panics with
// *TypeMismatchError, which implements exec.FatalEvalPanic so every
// pipeline seam converts it into a query error.

// mustMismatch asserts that fn panics with *TypeMismatchError and that the
// error is usable as an error value.
func mustMismatch(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("write of an unholdable value did not panic — the silent-drop guard is gone")
		}
		te, ok := r.(*TypeMismatchError)
		if !ok {
			t.Fatalf("panicked with %T (%v), want *TypeMismatchError", r, r)
		}
		if te.Error() == "" {
			t.Fatal("empty error message")
		}
		var asErr *TypeMismatchError
		if !errors.As(te.FatalEvalError(), &asErr) {
			t.Fatal("FatalEvalError must surface the error itself")
		}
	}()
	fn()
}

func TestSetValueGuardPanicsOnUnholdableValues(t *testing.T) {
	cases := []struct {
		name string
		typ  TypeID
		val  any
	}{
		// The family's canonical shapes.
		{"string into Float64 (#333/#372)", TypeFloat64, "ALGERIA"},
		{"string into Int64", TypeInt64, "x"},
		{"bool into Float64", TypeFloat64, true},
		{"string into Bool (#371's mirror)", TypeBool, "true"},
		{"bytes into Int32", TypeInt32, []byte{1}},
		{"string into Float32", TypeFloat32, "x"},
		{"bool into Timestamp", TypeTimestamp, false},
		{"string into Duration", TypeDuration, "1h"},
		{"bool into Date", TypeDate, true},
		{"bool into Decimal", TypeDecimal, true},
		{"int into IPv4", TypeIPv4, 7},
		{"int into IPv6", TypeIPv6, 7},
		{"int into CIDR", TypeCIDR, 7},
		{"bool into MAC", TypeMAC, true},
		{"string into Port", TypePort, "443"},
		{"int into UUID", TypeUUID, 7},
		{"string into Vector", TypeVector, "not a vector"},
		{"scalar into Array", TypeArray, int64(3)},
		// A ROW value into an ARRAY column: the old path returned without
		// advancing Offsets, so every LATER row read back shifted.
		{"map into Array", TypeArray, map[string]any{"a": 1}},
		{"slice into Row", TypeRow, []any{int64(1)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := NewVector(tc.typ, 4)
			if tc.typ == TypeVector {
				v.VectorDim = 2
				v.Float32Data = make([]float32, 8)
			}
			if tc.typ == TypeArray {
				v.Child = NewVector(TypeInt64, 4)
				v.Offsets = make([]int32, 5)
			}
			if tc.typ == TypeRow {
				v.Children = []*Vector{NewVector(TypeInt64, 4)}
				v.FieldNames = []string{"f"}
			}
			mustMismatch(t, func() { v.SetValue(0, tc.val) })
		})
	}
}

// The conversions SetValue performs on purpose must stay: nil is NULL,
// numeric widenings hold the value, and STRING/BYTES coerce everything
// through its string form (group keys rely on it).
func TestSetValueGuardLeavesConversionsAlone(t *testing.T) {
	t.Run("nil is NULL, never a panic", func(t *testing.T) {
		for _, typ := range []TypeID{TypeBool, TypeInt64, TypeFloat64, TypeString, TypeDate, TypeDecimal} {
			v := NewVector(typ, 2)
			v.SetValue(0, nil)
			if !v.Nulls.IsNull(0) {
				t.Fatalf("%s: nil did not write NULL", typ)
			}
		}
	})
	t.Run("int32 widens into Float64 (#361's shape)", func(t *testing.T) {
		v := NewVector(TypeFloat64, 2)
		v.SetValue(0, int32(41))
		if v.Float64Data[0] != 41 {
			t.Fatalf("got %v, want 41", v.Float64Data[0])
		}
	})
	t.Run("int64 narrows into Int32 as before", func(t *testing.T) {
		v := NewVector(TypeInt32, 2)
		v.SetValue(0, int64(7))
		if v.Int32Data[0] != 7 {
			t.Fatalf("got %v, want 7", v.Int32Data[0])
		}
	})
	t.Run("string coerces anything", func(t *testing.T) {
		v := NewVector(TypeString, 2)
		v.SetValue(0, int64(12))
		v.SetValue(1, true)
		if got := v.BytesData.StringValue(0); got != "12" {
			t.Fatalf("got %q, want 12", got)
		}
		if got := v.BytesData.StringValue(1); got != "true" {
			t.Fatalf("got %q, want true", got)
		}
	})
	t.Run("date accepts its string form", func(t *testing.T) {
		v := NewVector(TypeDate, 2)
		v.SetValue(0, "1970-01-02")
		if v.Int32Data[0] != 1 {
			t.Fatalf("got %v, want 1", v.Int32Data[0])
		}
	})
	t.Run("an unparseable IPv4 string is a value error, not a type panic", func(t *testing.T) {
		v := NewVector(TypeIPv4, 2)
		v.SetValue(0, "not-an-ip") // must not panic: the TYPE was right
	})
	t.Run("bool accepts numeric truthiness", func(t *testing.T) {
		v := NewVector(TypeBool, 3)
		v.SetValue(0, int64(1))
		v.SetValue(1, 0.0)
		v.SetValue(2, int32(2))
		if !v.BoolData[0] || v.BoolData[1] || !v.BoolData[2] {
			t.Fatalf("got %v %v %v, want true false true", v.BoolData[0], v.BoolData[1], v.BoolData[2])
		}
	})
}

// The guard's message names both sides, so the error a query surfaces
// localizes the defect without a debugger.
func TestTypeMismatchErrorMessage(t *testing.T) {
	e := &TypeMismatchError{Dst: parquet.TypeFloat64, Val: "s"}
	want := `batch: cannot store string into FLOAT64 vector (#361 silent-write guard)`
	if e.Error() != want {
		t.Fatalf("got %q, want %q", e.Error(), want)
	}
}
