// SPDX-License-Identifier: MIT

package expr

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestSubstrStartBelowOne pins PostgreSQL's SUBSTR window rule (#373): the
// result is the characters whose positions fall in [start, start+length), so
// a start below 1 consumes part of the length before the string begins.
func TestSubstrStartBelowOne(t *testing.T) {
	cases := []struct {
		name string
		args []any
		want any
	}{
		{"zero_start", []any{"abcdef", int64(0), int64(3)}, "ab"},
		{"negative_start", []any{"abcdef", int64(-2), int64(5)}, "ab"},
		{"negative_start_all_before", []any{"abcdef", int64(-5), int64(3)}, ""},
		{"in_range", []any{"abcdef", int64(1), int64(3)}, "abc"},
		{"no_length", []any{"abcdef", int64(3)}, "cdef"},
		{"zero_start_no_length", []any{"abcdef", int64(0)}, "abcdef"},
		{"negative_start_no_length", []any{"abcdef", int64(-2)}, "abcdef"},
		{"past_end", []any{"abcdef", int64(4), int64(100)}, "def"},
		{"start_past_end", []any{"abcdef", int64(9), int64(3)}, ""},
		{"zero_length", []any{"abcdef", int64(2), int64(0)}, ""},
		{"null_input", []any{nil, int64(1), int64(3)}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := fnSubstr(c.args); got != c.want {
				t.Errorf("fnSubstr(%v) = %#v, want %#v", c.args, got, c.want)
			}
		})
	}
}

// TestVecSubstrMatchesScalar holds the vectorized substr kernel to the scalar
// function's answers across the same window edge cases.
func TestVecSubstrMatchesScalar(t *testing.T) {
	starts := []int64{-5, -2, 0, 1, 2, 3, 4, 9}
	lengths := []int64{0, 1, 3, 100}
	const s = "abcdef"

	for _, length := range lengths {
		n := len(starts)
		src := batch.NewVector(batch.TypeString, n)
		startVec := batch.NewVector(batch.TypeInt64, n)
		lenVec := batch.NewVector(batch.TypeInt64, n)
		for i, st := range starts {
			src.BytesData.Set(i, []byte(s))
			startVec.Int64Data[i] = st
			lenVec.Int64Data[i] = length
		}
		out := batch.NewVector(batch.TypeString, n)
		vecSubstr([]*batch.Vector{src, startVec, lenVec}, out, n)
		for i, st := range starts {
			want := fnSubstr([]any{s, st, length})
			got := string(out.BytesData.Value(i))
			if got != want {
				t.Errorf("vecSubstr(%q, %d, %d) = %q, scalar answers %q", s, st, length, got, want)
			}
		}
	}

	// Two-argument form (no length).
	n := len(starts)
	src := batch.NewVector(batch.TypeString, n)
	startVec := batch.NewVector(batch.TypeInt64, n)
	for i, st := range starts {
		src.BytesData.Set(i, []byte(s))
		startVec.Int64Data[i] = st
	}
	out := batch.NewVector(batch.TypeString, n)
	vecSubstr([]*batch.Vector{src, startVec}, out, n)
	for i, st := range starts {
		want := fnSubstr([]any{s, st})
		got := string(out.BytesData.Value(i))
		if got != want {
			t.Errorf("vecSubstr(%q, %d) = %q, scalar answers %q", s, st, got, want)
		}
	}
}

// TestCastFractionalToIntegerRounds pins PostgreSQL's cast rule (#373): a
// cast from a fractional value to an integer ROUNDS half away from zero.
// TRUNC() remains the way to ask for truncation.
func TestCastFractionalToIntegerRounds(t *testing.T) {
	b := testBatch()
	cases := []struct {
		sql  string
		want any
	}{
		{"CAST(4.7 AS integer)", int64(5)},
		{"CAST(4.2 AS integer)", int64(4)},
		{"CAST(-4.7 AS integer)", int64(-5)},
		{"CAST(-4.2 AS integer)", int64(-4)},
		{"CAST(4.5 AS integer)", int64(5)},
		{"CAST(-4.5 AS integer)", int64(-5)},
		{"CAST(4.7 AS bigint)", int64(5)},
		{"CAST(4 AS integer)", int64(4)},
		{"CAST('42' AS integer)", int64(42)},
		// TRUNC over a bare fractional literal is a DECIMAL argument since
		// #1252's the earlier implementation made the literal itself numeric (`9b096b9e`), and
		// the earlier implementation's B1 fix moved this scalar function's own DECIMAL
		// declaration with it — so the result boxes as a decimal string at
		// scale 0, PostgreSQL's numeric, rather than the float64 this pin
		// held before the guard that kept a constant argument off the exact
		// path was removed (measured case B1, #1252).
		{"TRUNC(4.7)", "4"},
		{"TRUNC(-4.7)", "-4"},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			if got := e.Eval(b, 0); got != c.want {
				t.Errorf("Eval(%q) = %#v (%T), want %#v", c.sql, got, got, c.want)
			}
		})
	}
}

// TestCastIntFunctionRounds keeps the cast_int scalar alias on the same rule
// as CAST ... AS integer.
func TestCastIntFunctionRounds(t *testing.T) {
	if got := fnCastInt([]any{4.7}); got != int64(5) {
		t.Errorf("cast_int(4.7) = %#v, want 5", got)
	}
	if got := fnCastInt([]any{-4.7}); got != int64(-5) {
		t.Errorf("cast_int(-4.7) = %#v, want -5", got)
	}
}

// TestCastIntFunctionRangeChecksAFloat pins cast_int's float arm to the bound
// CAST uses: a float no bigint holds raises, where it wrapped to the minimum
// bigint (#1484).
func TestCastIntFunctionRangeChecksAFloat(t *testing.T) {
	for _, f := range []float64{9223372036854775808, math.NaN(), math.Inf(1), math.Inf(-1)} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("cast_int(%v) answered, want bigint out of range", f)
				}
			}()
			fnCastInt([]any{f})
		}()
	}
	if got := fnCastInt([]any{float64(-9223372036854775808)}); got != int64(math.MinInt64) {
		t.Errorf("cast_int(-2^63) = %#v, want the minimum bigint", got)
	}
}
