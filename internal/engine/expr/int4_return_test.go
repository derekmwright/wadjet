package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestIntegerReturningScalarsDeclareInt4 pins #530: the integer-returning
// scalar functions declare int4 (batch.TypeInt32) and COMPUTE an int32 value,
// not float8. Before the fix each resolved to TypeFloat64, so the projection
// allocated a Float64 vector and pgwire declared OID 701 (float8) where
// PostgreSQL declares OID 23 (int4) — a JDBC/pgx client reading LENGTH as an
// Integer got a Double. The declaration and the value have to move together:
// declaring int4 over a float64 vector reads back NULL through the typed
// getter, so both halves are asserted here.
func TestIntegerReturningScalarsDeclareInt4(t *testing.T) {
	// Every one of these is `integer` (int4) in PostgreSQL. cardinality and
	// array_length were int8 (TypeInt64) before #530, which is still the wrong
	// width for a client that reads them as int4.
	for _, name := range []string{
		"length", "len", "octet_length", "bit_length",
		"char_length", "character_length",
		"strpos", "position", "codepoint", "width_bucket",
		"cardinality", "array_length",
	} {
		ret := DefaultRegistry.ReturnType(name)
		if !ret.Declared() {
			t.Errorf("%s: not registered / no declared return type", name)
			continue
		}
		got, conf := ret.Resolve(1, nil)
		if got != batch.TypeInt32 {
			t.Errorf("%s: declared return type %v, want INT32 (int4)", name, got)
		}
		if conf != Decided {
			t.Errorf("%s: confidence %v, want DECIDED", name, conf)
		}
	}
}

// TestIntegerReturningScalarsBoxAsInt32 checks the boxed per-row value type,
// the other half of #530 (the vec kernels are covered by the shape_funcs
// parity tests over an Int32 output vector).
func TestIntegerReturningScalarsBoxAsInt32(t *testing.T) {
	cases := []struct {
		name string
		args []any
	}{
		{"length", []any{"hello"}},
		{"octet_length", []any{"hello"}},
		{"bit_length", []any{"hello"}},
		{"char_length", []any{"héllo"}},
		{"strpos", []any{"hello", "llo"}},
		{"codepoint", []any{"A"}},
		{"cardinality", []any{[]any{1, 2, 3}}},
		{"width_bucket", []any{float64(5), float64(0), float64(10), float64(5)}},
	}
	for _, tc := range cases {
		fn := DefaultRegistry.Lookup(tc.name)
		if fn == nil {
			t.Errorf("%s: not registered", tc.name)
			continue
		}
		v := fn(tc.args)
		if _, ok := v.(int32); !ok {
			t.Errorf("%s%v boxed as %T (%v), want int32", tc.name, tc.args, v, v)
		}
	}
}
