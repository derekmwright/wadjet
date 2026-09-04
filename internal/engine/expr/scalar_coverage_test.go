package expr

import (
	"fmt"
	"math"
	"testing"
)

// Tests for scalar functions at 0% or low coverage.

func TestFnLTrim(t *testing.T) {
	tests := []struct {
		name   string
		args   []any
		expect any
	}{
		{"nil", nil, nil},
		{"empty args", []any{}, nil},
		{"nil arg", []any{nil}, nil},
		{"spaces", []any{"  hello  "}, "hello  "},
		{"tabs", []any{"\t\thello"}, "hello"},
		{"mixed whitespace", []any{"\n\r\t hello"}, "hello"},
		{"no leading", []any{"hello"}, "hello"},
	}
	for _, tc := range tests {
		got := fnLTrim(tc.args)
		if got != tc.expect {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expect, got)
		}
	}
}

func TestFnRTrim(t *testing.T) {
	tests := []struct {
		name   string
		args   []any
		expect any
	}{
		{"nil", nil, nil},
		{"nil arg", []any{nil}, nil},
		{"spaces", []any{"  hello  "}, "  hello"},
		{"tabs", []any{"hello\t\t"}, "hello"},
		{"mixed whitespace", []any{"hello \n\r\t"}, "hello"},
		{"no trailing", []any{"hello"}, "hello"},
	}
	for _, tc := range tests {
		got := fnRTrim(tc.args)
		if got != tc.expect {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expect, got)
		}
	}
}

func TestFnMod(t *testing.T) {
	tests := []struct {
		name   string
		args   []any
		expect any
	}{
		{"nil", nil, nil},
		{"nil first", []any{nil, 2.0}, nil},
		{"nil second", []any{10.0, nil}, nil},
		{"basic mod", []any{10.0, 3.0}, math.Mod(10.0, 3.0)},
		{"exact divide", []any{9.0, 3.0}, math.Mod(9.0, 3.0)},
		{"negative", []any{-10.0, 3.0}, math.Mod(-10.0, 3.0)},
		// INTEGER arguments answer INTEGER remainder, in the argument's own
		// domain — `pg_typeof(mod(int8, int))` is bigint (#768). It is
		// checked separately below because this table's comparison is
		// float64-shaped.
	}
	for _, tc := range tests {
		got := fnMod(tc.args)
		if got == nil && tc.expect == nil {
			continue
		}
		if got == nil || tc.expect == nil {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expect, got)
			continue
		}
		gf, ok := got.(float64)
		if !ok {
			t.Errorf("%s: expected float64, got %T", tc.name, got)
			continue
		}
		ef := tc.expect.(float64)
		if gf != ef {
			t.Errorf("%s: expected %v, got %v", tc.name, ef, gf)
		}
	}

	// The INTEGER domain, which the float64-shaped table above cannot hold
	// (#768). PostgreSQL's `mod(int8, int)` is bigint and its remainder is
	// exact -- `MOD(-6, 3)` is 0, never math.Mod's signed -0.
	for _, tc := range []struct {
		name   string
		args   []any
		expect any
	}{
		{"int64 args", []any{int64(10), int64(3)}, int64(1)},
		{"int64 negative", []any{int64(-10), int64(3)}, int64(-1)},
		{"int64 exact, no signed zero", []any{int64(-6), int64(3)}, int64(0)},
		{"int32 args stay int32", []any{int32(10), int32(3)}, int32(1)},
		{"mixed widths widen", []any{int32(10), int64(3)}, int64(1)},
	} {
		got := fnMod(tc.args)
		if fmt.Sprintf("%T:%v", got, got) != fmt.Sprintf("%T:%v", tc.expect, tc.expect) {
			t.Errorf("%s: got %#v (%T), want %#v (%T)", tc.name, got, got, tc.expect, tc.expect)
		}
	}
	// A zero divisor used to fall to the float path and answer NaN — pinned
	// here as "not this rule's to change". PostgreSQL 17 answers 22012 for
	// `mod(1, 0)` and `mod(1.0, 0.0)` alike, and a NaN remainder is a value
	// no modulus has (#840's census).
	for _, args := range [][]any{
		{int64(10), int64(0)}, {int32(10), int32(0)}, {10.0, 0.0}, {int64(10), 0.0},
	} {
		state, msg := recoverFatalEvalForTest(t, func() { fnMod(args) })
		if state != "22012" || msg != "division by zero" {
			t.Errorf("MOD(%v) raised [%s] %s, want [22012] division by zero (live PostgreSQL 17.11)",
				args, state, msg)
		}
	}
}

func TestFnMapEntries(t *testing.T) {
	// nil args
	if fnMapEntries(nil) != nil {
		t.Error("expected nil for nil args")
	}
	if fnMapEntries([]any{nil}) != nil {
		t.Error("expected nil for nil arg")
	}
	// non-map
	if fnMapEntries([]any{"not a map"}) != nil {
		t.Error("expected nil for non-map")
	}
	// valid map
	m := map[string]any{"a": int64(1), "b": int64(2)}
	result := fnMapEntries([]any{m})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	entries, ok := result.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestFnTrim(t *testing.T) {
	if fnTrim(nil) != nil {
		t.Error("nil")
	}
	if fnTrim([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnTrim([]any{"  hello  "}) != "hello" {
		t.Error("expected trimmed")
	}
}

func TestFnReplace(t *testing.T) {
	if fnReplace(nil) != nil {
		t.Error("nil")
	}
	if fnReplace([]any{"hello world", "world", "Go"}) != "hello Go" {
		t.Error("expected replaced")
	}
	if fnReplace([]any{nil, "a", "b"}) != nil {
		t.Error("nil first")
	}
	if fnReplace([]any{"a", nil, "b"}) != nil {
		t.Error("nil second")
	}
	if fnReplace([]any{"a", "b", nil}) != nil {
		t.Error("nil third")
	}
}

func TestFnLeft(t *testing.T) {
	if fnLeft(nil) != nil {
		t.Error("nil")
	}
	if fnLeft([]any{nil, int64(3)}) != nil {
		t.Error("nil arg")
	}
	if fnLeft([]any{"hello", nil}) != nil {
		t.Error("nil count")
	}
	// Negative returns empty
	if fnLeft([]any{"hello", int64(-1)}) != "" {
		t.Error("negative")
	}
	// n >= len
	if fnLeft([]any{"hi", int64(10)}) != "hi" {
		t.Error("n >= len")
	}
	if fnLeft([]any{"hello", int64(3)}) != "hel" {
		t.Error("normal left")
	}
}

func TestFnRight(t *testing.T) {
	if fnRight(nil) != nil {
		t.Error("nil")
	}
	if fnRight([]any{"hello", int64(-1)}) != "" {
		t.Error("negative")
	}
	if fnRight([]any{"hi", int64(10)}) != "hi" {
		t.Error("n >= len")
	}
	if fnRight([]any{"hello", int64(3)}) != "llo" {
		t.Error("normal right")
	}
}

func TestFnEndsWith(t *testing.T) {
	if fnEndsWith(nil) != nil {
		t.Error("nil")
	}
	if fnEndsWith([]any{"hello", "lo"}) != true {
		t.Error("match")
	}
	if fnEndsWith([]any{"hello", "he"}) != false {
		t.Error("no match")
	}
}

func TestFnContains(t *testing.T) {
	if fnContains(nil) != nil {
		t.Error("nil")
	}
	if fnContains([]any{"hello world", "world"}) != true {
		t.Error("found")
	}
	if fnContains([]any{"hello", "xyz"}) != false {
		t.Error("not found")
	}
}

func TestFnRepeat(t *testing.T) {
	if fnRepeat(nil) != nil {
		t.Error("nil")
	}
	if fnRepeat([]any{"ab", int64(3)}) != "ababab" {
		t.Error("repeat")
	}
	if fnRepeat([]any{"ab", int64(-1)}) != "" {
		t.Error("negative")
	}
}

func TestFnAbs(t *testing.T) {
	if fnAbs(nil) != nil {
		t.Error("nil")
	}
	if fnAbs([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnAbs([]any{-5.0}) != 5.0 {
		t.Error("abs")
	}
}

func TestFnCeil(t *testing.T) {
	if fnCeil(nil) != nil {
		t.Error("nil")
	}
	if fnCeil([]any{3.2}) != 4.0 {
		t.Error("ceil")
	}
}

func TestFnFloor(t *testing.T) {
	if fnFloor(nil) != nil {
		t.Error("nil")
	}
	if fnFloor([]any{3.8}) != 3.0 {
		t.Error("floor")
	}
}

func TestFnPow(t *testing.T) {
	if fnPow(nil) != nil {
		t.Error("nil")
	}
	if fnPow([]any{nil, 2.0}) != nil {
		t.Error("nil first")
	}
	if fnPow([]any{2.0, nil}) != nil {
		t.Error("nil second")
	}
	if fnPow([]any{2.0, 3.0}) != 8.0 {
		t.Error("pow")
	}
	// PostgreSQL's dpow() refusals, all four measured live on 17.11. POWER(0,
	// -1) answered +Infinity here, which is worse than a NULL: it propagates
	// arithmetically and nothing downstream can see where it came from.
	for _, c := range []struct {
		name       string
		base, exp  float64
		state, msg string
	}{
		{"zero to a negative power", 0, -1, "2201F",
			"zero raised to a negative power is undefined"},
		{"negative to a non-integer power", -1, 0.5, "2201F",
			"a negative number raised to a non-integer power yields a complex result"},
		{"overflow", 2, 10000, "22003", "value out of range: overflow"},
	} {
		state, msg := recoverFatalEvalForTest(t, func() { fnPow([]any{c.base, c.exp}) })
		if state != c.state || msg != c.msg {
			t.Errorf("POWER(%v, %v) [%s] raised [%s] %s, want [%s] %s (live PostgreSQL 17.11)",
				c.base, c.exp, c.name, state, msg, c.state, c.msg)
		}
	}
	// THE BOUNDARY, attempted from outside — and the INFINITE-argument cells
	// are the ones the first version of this corpus lacked, which is why it
	// stayed green while `POWER(2, 'Infinity')` raised. PostgreSQL's dpow
	// suppresses the overflow when EITHER operand is already infinite, and it
	// has no underflow refusal that reaches the spelling a user writes.
	// Every value measured on 17.11.
	inf, neginf := math.Inf(1), math.Inf(-1)
	for _, c := range []struct {
		name            string
		base, exp, want float64
	}{
		{"zero to the zero", 0, 0, 1},
		{"zero to a positive power", 0, 5, 0},
		{"a negative base, integer exponent", -2, 3, -8},
		// An INFINITE operand: the result is infinite or zero and neither is
		// an overflow, because the input was already there.
		{"an infinite exponent", 2, inf, inf},
		{"a negative infinite exponent", 2, neginf, 0},
		{"an infinite base", inf, 2, inf},
		{"a fraction to an infinite power", 0.5, inf, 0},
		// UNDERFLOW to zero is a VALUE, not a refusal: PostgreSQL resolves
		// `POWER(0.5, 2000)` to power(numeric, numeric), which answers 0.
		// Refusing it would be ADR-0012 item 1's forbidden direction for the
		// spelling users write; see fnPow.
		{"a fraction to a large power underflows to zero", 0.5, 2000, 0},
		{"a tiny base cubed underflows to zero", 1e-200, 3, 0},
	} {
		if got := fnPow([]any{c.base, c.exp}); got != c.want {
			t.Errorf("POWER(%v, %v) [%s] = %v, want %v (live PostgreSQL 17.11)",
				c.base, c.exp, c.name, got, c.want)
		}
	}
}

func TestFnSqrt(t *testing.T) {
	if fnSqrt(nil) != nil {
		t.Error("nil")
	}
	if fnSqrt([]any{nil}) != nil {
		t.Error("nil arg")
	}
	// A negative argument RAISES since #840; it answered NULL here.
	state, msg := recoverFatalEvalForTest(t, func() { fnSqrt([]any{-1.0}) })
	if state != "2201F" || msg != "cannot take square root of a negative number" {
		t.Errorf("SQRT(-1) raised [%s] %s, want [2201F] cannot take square root of a "+
			"negative number (live PostgreSQL 17.11)", state, msg)
	}
	if fnSqrt([]any{4.0}) != 2.0 {
		t.Error("sqrt")
	}
	// The values PostgreSQL passes through rather than refusing: NaN, +Inf and
	// negative zero. They are the refusal's boundary, attempted from outside.
	if got := fnSqrt([]any{math.NaN()}); got == nil || !math.IsNaN(got.(float64)) {
		t.Errorf("SQRT(NaN) = %v, want NaN (PostgreSQL answers NaN)", got)
	}
	if got := fnSqrt([]any{math.Inf(1)}); got != math.Inf(1) {
		t.Errorf("SQRT(Infinity) = %v, want +Inf (PostgreSQL answers Infinity)", got)
	}
	if got := fnSqrt([]any{math.Copysign(0, -1)}); got != math.Copysign(0, -1) {
		t.Errorf("SQRT(-0.0) = %v, want -0 (PostgreSQL answers -0)", got)
	}
}

// TestFnConcat asserts the CONCAT() FUNCTION's rule and TestFnConcatOp the
// `||` OPERATOR's. They are two rules, and until #609 they were one function:
// this test asserted `||`'s rule under CONCAT's name, which is the shape a
// test takes when it was written from the implementation rather than from
// PostgreSQL. Every expectation below was read off live PostgreSQL 17.
func TestFnConcat(t *testing.T) {
	// empty args -> empty string (sb.String() with no writes)
	result := fnConcat([]any{})
	if result != "" {
		t.Errorf("expected empty string for no args, got %v", result)
	}
	// A NULL argument is IGNORED, and CONCAT of only NULLs is the empty
	// string — never NULL. PostgreSQL 17: `SELECT CONCAT(NULL)` = ''.
	if got := fnConcat([]any{nil}); got != "" {
		t.Errorf("CONCAT(NULL) = %#v, want \"\" (PostgreSQL 17 ignores a NULL argument)", got)
	}
	if got := fnConcat([]any{nil, nil}); got != "" {
		t.Errorf("CONCAT(NULL, NULL) = %#v, want \"\"", got)
	}
	// Single arg
	if fnConcat([]any{"hello"}) != "hello" {
		t.Error("single")
	}
	// PostgreSQL 17: `SELECT CONCAT('a', NULL, 'b')` = 'ab'.
	if got := fnConcat([]any{"a", nil, "b"}); got != "ab" {
		t.Errorf("CONCAT('a', NULL, 'b') = %#v, want \"ab\"", got)
	}
	// Multiple args all non-nil
	result = fnConcat([]any{"a", "b", "c"})
	if result != "abc" {
		t.Errorf("expected 'abc', got %v", result)
	}
}

// TestFnConcatOp is the RATCHET on the other half of #609: the `||` operator
// PROPAGATES NULL, which is the rule that was already right and which a fix
// aimed at CONCAT could only break.
func TestFnConcatOp(t *testing.T) {
	if got := fnConcatOp([]any{nil}); got != nil {
		t.Errorf("NULL || nothing = %#v, want nil (PostgreSQL 17 propagates)", got)
	}
	if got := fnConcatOp([]any{"a", nil, "b"}); got != nil {
		t.Errorf("'a' || NULL || 'b' = %#v, want nil", got)
	}
	if got := fnConcatOp([]any{"a", "b"}); got != "ab" {
		t.Errorf("'a' || 'b' = %#v, want \"ab\"", got)
	}
	// And the two are not the same function: the day a refactor points them
	// at one kernel again, this fails.
	if got := fnConcat([]any{"a", nil, "b"}); got == fnConcatOp([]any{"a", nil, "b"}) {
		t.Errorf("CONCAT and || answered the same thing (%#v) for a NULL argument; "+
			"they are two rules and #609 is that they shared one kernel", got)
	}
}

func TestFnLength(t *testing.T) {
	if fnLength(nil) != nil {
		t.Error("nil")
	}
	if fnLength([]any{nil}) != nil {
		t.Error("nil arg")
	}
	// length returns int32 (int4 in PostgreSQL, #530)
	if fnLength([]any{"hello"}) != int32(5) {
		t.Errorf("expected int32(5), got %v (%T)", fnLength([]any{"hello"}), fnLength([]any{"hello"}))
	}
}

func TestFnSubstr(t *testing.T) {
	if fnSubstr(nil) != nil {
		t.Error("nil")
	}
	if fnSubstr([]any{nil, int64(1)}) != nil {
		t.Error("nil arg")
	}
	// SQL is 1-indexed: substr("hello", 1) = "hello"
	result := fnSubstr([]any{"hello", int64(1)})
	if result != "hello" {
		t.Errorf("expected 'hello', got %v", result)
	}
	// substr("hello", 2) = "ello" (1-indexed, position 2 = index 1)
	result = fnSubstr([]any{"hello", int64(2)})
	if result != "ello" {
		t.Errorf("expected 'ello', got %v", result)
	}
	// 3-arg form: substr("hello", 2, 3) = "ell"
	result = fnSubstr([]any{"hello", int64(2), int64(3)})
	if result != "ell" {
		t.Errorf("expected 'ell', got %v", result)
	}
	// Start beyond length
	result = fnSubstr([]any{"hi", int64(10)})
	if result != "" {
		t.Errorf("expected empty for past-end, got %v", result)
	}
}

func TestFnReverse(t *testing.T) {
	if fnReverse(nil) != nil {
		t.Error("nil")
	}
	if fnReverse([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnReverse([]any{"hello"}) != "olleh" {
		t.Error("reverse")
	}
}

func TestFnRound(t *testing.T) {
	if fnRound(nil) != nil {
		t.Error("nil")
	}
	// No precision
	if fnRound([]any{3.14159}) != 3.0 {
		t.Error("round no precision")
	}
	// With precision
	result := fnRound([]any{3.14159, int64(2)})
	if result != 3.14 {
		t.Errorf("expected 3.14, got %v", result)
	}
}

// TestFnRoundHalfEven pins PostgreSQL's DOUBLE PRECISION rounding rule
// (#381): half TO EVEN, the opposite tie-break from fnRound's NUMERIC rule
// (half away from zero, TestFnRound above).
func TestFnRoundHalfEven(t *testing.T) {
	if fnRoundHalfEven(nil) != nil {
		t.Error("nil")
	}
	if fnRoundHalfEven([]any{nil}) != nil {
		t.Error("nil arg")
	}
	cases := []struct {
		v    float64
		want float64
	}{
		{0.5, 0.0},
		{1.5, 2.0},
		{2.5, 2.0},
		{-0.5, -0.0},
		{-1.5, -2.0},
	}
	for _, c := range cases {
		if got := fnRoundHalfEven([]any{c.v}); got != c.want {
			t.Errorf("fnRoundHalfEven(%v) = %v, want %v", c.v, got, c.want)
		}
	}
	// With precision, at another exact tie (0.125 * 100 = 12.5, half to
	// even rounds to 12): 0.375 * 100 = 37.5 rounds to 38, the other
	// direction, since 38 is the even neighbor this time.
	if got := fnRoundHalfEven([]any{0.125, int64(2)}); got != 0.12 {
		t.Errorf("fnRoundHalfEven(0.125, 2) = %v, want 0.12", got)
	}
	if got := fnRoundHalfEven([]any{0.375, int64(2)}); got != 0.38 {
		t.Errorf("fnRoundHalfEven(0.375, 2) = %v, want 0.38", got)
	}
}

func TestFnCardinality(t *testing.T) {
	if fnCardinality(nil) != nil {
		t.Error("nil")
	}
	if fnCardinality([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnCardinality([]any{"not-slice"}) != nil {
		t.Error("non-slice")
	}
	if fnCardinality([]any{[]any{1, 2, 3}}) != int32(3) {
		t.Error("expected int32(3)")
	}
}

func TestFnElementAt(t *testing.T) {
	if fnElementAt(nil) != nil {
		t.Error("nil")
	}
	if fnElementAt([]any{nil, int64(1)}) != nil {
		t.Error("nil array")
	}
	// 1-based index
	arr := []any{int64(10), int64(20), int64(30)}
	if fnElementAt([]any{arr, int64(2)}) != int64(20) {
		t.Error("element at 2")
	}
}

func TestFnArrayContains(t *testing.T) {
	if fnArrayContains(nil) != nil {
		t.Error("nil")
	}
	arr := []any{int64(1), int64(2), int64(3)}
	if fnArrayContains([]any{arr, int64(2)}) != true {
		t.Error("found")
	}
	if fnArrayContains([]any{arr, int64(5)}) != false {
		t.Error("not found")
	}
}

func TestFnArrayJoin(t *testing.T) {
	if fnArrayJoin(nil) != nil {
		t.Error("nil")
	}
	arr := []any{"a", "b", "c"}
	result := fnArrayJoin([]any{arr, ","})
	if result != "a,b,c" {
		t.Errorf("expected 'a,b,c', got %v", result)
	}
}

func TestFnArrayMin(t *testing.T) {
	if fnArrayMin(nil) != nil {
		t.Error("nil")
	}
	if fnArrayMin([]any{nil}) != nil {
		t.Error("nil arg")
	}
	arr := []any{int64(3), int64(1), int64(2)}
	result := fnArrayMin([]any{arr})
	if result != int64(1) {
		t.Errorf("expected 1, got %v", result)
	}
}

func TestFnArrayMax(t *testing.T) {
	if fnArrayMax(nil) != nil {
		t.Error("nil")
	}
	arr := []any{int64(1), nil, int64(3)}
	result := fnArrayMax([]any{arr})
	if result != int64(3) {
		t.Errorf("expected 3, got %v", result)
	}
	// nil first element with valid later
	arr2 := []any{nil, int64(5)}
	result2 := fnArrayMax([]any{arr2})
	if result2 != int64(5) {
		t.Errorf("expected 5, got %v", result2)
	}
}

func TestFnRowField(t *testing.T) {
	if fnRowField(nil) != nil {
		t.Error("nil")
	}
	if fnRowField([]any{nil, "key"}) != nil {
		t.Error("nil row")
	}
	row := map[string]any{"name": "test", "age": int64(25)}
	result := fnRowField([]any{row, "name"})
	if result != "test" {
		t.Errorf("expected 'test', got %v", result)
	}
	// Missing key
	result = fnRowField([]any{row, "missing"})
	if result != nil {
		t.Errorf("expected nil for missing key, got %v", result)
	}
}

func TestFnMapKeys(t *testing.T) {
	if fnMapKeys(nil) != nil {
		t.Error("nil")
	}
	if fnMapKeys([]any{nil}) != nil {
		t.Error("nil arg")
	}
	m := map[string]any{"a": 1, "b": 2}
	result := fnMapKeys([]any{m})
	if result == nil {
		t.Fatal("expected non-nil")
	}
	keys := result.([]any)
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestFnMapValues(t *testing.T) {
	if fnMapValues(nil) != nil {
		t.Error("nil")
	}
	if fnMapValues([]any{nil}) != nil {
		t.Error("nil arg")
	}
	m := map[string]any{"a": int64(1), "b": int64(2)}
	result := fnMapValues([]any{m})
	if result == nil {
		t.Fatal("expected non-nil")
	}
	vals := result.([]any)
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}
}

func TestFnMapFromEntries(t *testing.T) {
	if fnMapFromEntries(nil) != nil {
		t.Error("nil")
	}
	if fnMapFromEntries([]any{nil}) != nil {
		t.Error("nil arg")
	}
	entries := []any{
		map[string]any{"key": "a", "value": int64(1)},
		map[string]any{"key": "b", "value": int64(2)},
	}
	result := fnMapFromEntries([]any{entries})
	if result == nil {
		t.Fatal("expected map")
	}
	m := result.(map[string]any)
	if m["a"] != int64(1) || m["b"] != int64(2) {
		t.Fatalf("unexpected map: %v", m)
	}
}

func TestToSlice(t *testing.T) {
	// []any
	s, ok := toSlice([]any{1, 2})
	if !ok || len(s) != 2 {
		t.Error("[]any")
	}
	// []map[string]any
	s2, ok := toSlice([]map[string]any{{"a": 1}})
	if !ok || len(s2) != 1 {
		t.Error("[]map[string]any")
	}
	// unsupported type
	_, ok = toSlice("hello")
	if ok {
		t.Error("expected false for string")
	}
}

func TestSafeArg(t *testing.T) {
	args := []any{"a", "b"}
	if safeArg(args, 0) != "a" {
		t.Error("index 0")
	}
	if safeArg(args, 2) != nil {
		t.Error("out of bounds")
	}
	if safeArg(nil, 0) != nil {
		t.Error("nil args")
	}
}

func TestToMap(t *testing.T) {
	// map[string]any
	m, ok := toMap(map[string]any{"x": 1})
	if !ok || m["x"] != 1 {
		t.Error("map[string]any")
	}
	// unsupported type
	_, ok = toMap("not a map")
	if ok {
		t.Error("expected false for string")
	}
	_, ok = toMap(nil)
	if ok {
		t.Error("expected false for nil")
	}
}

func TestFnStartsWith(t *testing.T) {
	if fnStartsWith(nil) != nil {
		t.Error("nil")
	}
	if fnStartsWith([]any{nil, "he"}) != nil {
		t.Error("nil arg")
	}
	if fnStartsWith([]any{"hello", nil}) != nil {
		t.Error("nil prefix")
	}
	if fnStartsWith([]any{"hello", "he"}) != true {
		t.Error("match")
	}
	if fnStartsWith([]any{"hello", "lo"}) != false {
		t.Error("no match")
	}
}

func TestFnLog(t *testing.T) {
	if fnLog(nil) != nil {
		t.Error("nil")
	}
	if fnLog([]any{nil}) != nil {
		t.Error("nil arg")
	}
	// log10 of 100
	result := fnLog([]any{100.0})
	if result != 2.0 {
		t.Errorf("expected 2.0, got %v", result)
	}
	// log(base, val): log base 2 of 8
	result = fnLog([]any{2.0, 8.0})
	if result.(float64)-3.0 > 1e-10 {
		t.Errorf("expected ~3.0, got %v", result)
	}
	// The domain failures. Each answered NULL until #840; each carries the
	// SQLSTATE and the message PostgreSQL 17.11 gives for it, and the base-1
	// one is deliberately NOT a logarithm error: log(v)/log(1) divides by
	// zero and the server reports 22012.
	for _, c := range []struct {
		name       string
		args       []any
		state, msg string
	}{
		{"negative value", []any{-1.0}, "2201E", "cannot take logarithm of a negative number"},
		{"zero value", []any{0.0}, "2201E", "cannot take logarithm of zero"},
		{"base 1", []any{1.0, 8.0}, "22012", "division by zero"},
		{"base negative", []any{-1.0, 8.0}, "2201E", "cannot take logarithm of a negative number"},
		{"base zero", []any{0.0, 8.0}, "2201E", "cannot take logarithm of zero"},
		{"value negative with base", []any{2.0, -8.0}, "2201E",
			"cannot take logarithm of a negative number"},
		{"value zero with base", []any{2.0, 0.0}, "2201E", "cannot take logarithm of zero"},
	} {
		state, msg := recoverFatalEvalForTest(t, func() { fnLog(c.args) })
		if state != c.state || msg != c.msg {
			t.Errorf("LOG %s raised [%s] %s, want [%s] %s (live PostgreSQL 17.11)",
				c.name, state, msg, c.state, c.msg)
		}
	}
	// NaN passes through: PostgreSQL answers NaN, not an error.
	if got := fnLog([]any{math.NaN()}); got == nil || !math.IsNaN(got.(float64)) {
		t.Errorf("LOG(NaN) = %v, want NaN", got)
	}
}

func TestFnLn(t *testing.T) {
	if fnLn(nil) != nil {
		t.Error("nil")
	}
	if fnLn([]any{nil}) != nil {
		t.Error("nil arg")
	}
	// #840's headline pair.
	for _, c := range []struct {
		arg        float64
		state, msg string
	}{
		{0, "2201E", "cannot take logarithm of zero"},
		{-1, "2201E", "cannot take logarithm of a negative number"},
	} {
		state, msg := recoverFatalEvalForTest(t, func() { fnLn([]any{c.arg}) })
		if state != c.state || msg != c.msg {
			t.Errorf("LN(%v) raised [%s] %s, want [%s] %s (live PostgreSQL 17.11)",
				c.arg, state, msg, c.state, c.msg)
		}
	}
	// PostgreSQL answers Infinity for LN('Infinity') and NaN for LN('NaN').
	if got := fnLn([]any{math.Inf(1)}); got != math.Inf(1) {
		t.Errorf("LN(Infinity) = %v, want +Inf", got)
	}
	if got := fnLn([]any{math.NaN()}); got == nil || !math.IsNaN(got.(float64)) {
		t.Errorf("LN(NaN) = %v, want NaN", got)
	}
	result := fnLn([]any{math.E})
	if math.Abs(result.(float64)-1.0) > 1e-10 {
		t.Errorf("expected 1.0, got %v", result)
	}
}

func TestFnExp(t *testing.T) {
	if fnExp(nil) != nil {
		t.Error("nil")
	}
	if fnExp([]any{nil}) != nil {
		t.Error("nil arg")
	}
	result := fnExp([]any{0.0})
	if result != 1.0 {
		t.Errorf("expected 1.0, got %v", result)
	}
	result = fnExp([]any{1.0})
	if math.Abs(result.(float64)-math.E) > 1e-10 {
		t.Error("exp(1) != e")
	}
	// PostgreSQL's float8 result check, which every libm-backed function in
	// float.c runs: an infinite result from a finite argument OVERFLOWED and a
	// zero result from a non-zero argument UNDERFLOWED. Both answered a number
	// here — +Inf and 0 — where the server raises.
	for _, c := range []struct {
		arg float64
		msg string
	}{
		{1000, "value out of range: overflow"},
		{-1000, "value out of range: underflow"},
	} {
		state, msg := recoverFatalEvalForTest(t, func() { fnExp([]any{c.arg}) })
		if state != "22003" || msg != c.msg {
			t.Errorf("EXP(%v) raised [%s] %s, want [22003] %s (live PostgreSQL 17.11)",
				c.arg, state, msg, c.msg)
		}
	}
	// THE BOUNDARY. PostgreSQL's dexp handles NaN and the infinities
	// EXPLICITLY, before any range check — per POSIX `exp(-Inf)` is ZERO, not
	// an underflow. The first version of this check tested `arg != 0` for the
	// underflow, which is true of -Infinity, so `EXP(-Infinity)` RAISED where
	// the server answers 0. Only a FINITE argument is range-checked.
	for _, c := range []struct {
		name string
		arg  float64
		want float64
	}{
		{"positive infinity keeps its infinity", math.Inf(1), math.Inf(1)},
		{"negative infinity is ZERO, not an underflow", math.Inf(-1), 0},
	} {
		if got := fnExp([]any{c.arg}); got != c.want {
			t.Errorf("EXP(%v) [%s] = %v, want %v (live PostgreSQL 17.11)",
				c.arg, c.name, got, c.want)
		}
	}
	if got := fnExp([]any{math.NaN()}); got == nil || !math.IsNaN(got.(float64)) {
		t.Errorf("EXP(NaN) = %v, want NaN", got)
	}
}

func TestFnSign(t *testing.T) {
	if fnSign(nil) != nil {
		t.Error("nil")
	}
	if fnSign([]any{5.0}) != float64(1) {
		t.Error("positive")
	}
	if fnSign([]any{-5.0}) != float64(-1) {
		t.Error("negative")
	}
	if fnSign([]any{0.0}) != float64(0) {
		t.Error("zero")
	}
}

func TestFnGreatest(t *testing.T) {
	if fnGreatest(nil) != nil {
		t.Error("nil args")
	}
	if fnGreatest([]any{nil, nil}) != nil {
		t.Error("all nil")
	}
	result := fnGreatest([]any{1.0, 3.0, 2.0})
	if result != 3.0 {
		t.Errorf("expected 3.0, got %v", result)
	}
	result = fnGreatest([]any{nil, 5.0, nil})
	if result != 5.0 {
		t.Errorf("expected 5.0, got %v", result)
	}
}

func TestFnLeast(t *testing.T) {
	if fnLeast(nil) != nil {
		t.Error("nil args")
	}
	result := fnLeast([]any{3.0, 1.0, 2.0})
	if result != 1.0 {
		t.Errorf("expected 1.0, got %v", result)
	}
	result = fnLeast([]any{nil, 5.0, nil})
	if result != 5.0 {
		t.Errorf("expected 5.0, got %v", result)
	}
}

func TestFnCoalesceFunc(t *testing.T) {
	// Tests the fnCoalesce function (not the Coalesce Expr)
	if fnCoalesce(nil) != nil {
		t.Error("nil args")
	}
	if fnCoalesce([]any{nil, nil}) != nil {
		t.Error("all nil")
	}
	if fnCoalesce([]any{nil, "hello", "world"}) != "hello" {
		t.Error("first non-nil")
	}
	if fnCoalesce([]any{"first"}) != "first" {
		t.Error("single non-nil")
	}
}

func TestFnNullIf(t *testing.T) {
	if fnNullIf(nil) != nil {
		t.Error("nil")
	}
	if fnNullIf([]any{int64(1)}) != nil {
		t.Error("too few args")
	}
	// Equal: returns nil
	if fnNullIf([]any{int64(5), int64(5)}) != nil {
		t.Error("equal should return nil")
	}
	// Not equal: returns first arg
	if fnNullIf([]any{int64(5), int64(3)}) != int64(5) {
		t.Error("not equal should return first arg")
	}
	// Nil first arg
	result := fnNullIf([]any{nil, int64(5)})
	if result != nil {
		t.Error("nil first should return nil")
	}
}

func TestFnIfNull(t *testing.T) {
	if fnIfNull(nil) != nil {
		t.Error("nil")
	}
	if fnIfNull([]any{int64(1)}) != nil {
		t.Error("too few args")
	}
	if fnIfNull([]any{int64(5), int64(10)}) != int64(5) {
		t.Error("non-nil first")
	}
	if fnIfNull([]any{nil, int64(10)}) != int64(10) {
		t.Error("nil first returns second")
	}
}

func TestFnIf(t *testing.T) {
	if fnIf(nil) != nil {
		t.Error("nil")
	}
	if fnIf([]any{true, "yes"}) != nil {
		t.Error("too few args")
	}
	if fnIf([]any{true, "yes", "no"}) != "yes" {
		t.Error("true condition")
	}
	if fnIf([]any{false, "yes", "no"}) != "no" {
		t.Error("false condition")
	}
	if fnIf([]any{nil, "yes", "no"}) != "no" {
		t.Error("nil condition")
	}
}

func TestFnCastInt(t *testing.T) {
	if fnCastInt(nil) != nil {
		t.Error("nil")
	}
	if fnCastInt([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnCastInt([]any{3.14}) != int64(3) {
		t.Error("cast float to int")
	}
	if fnCastInt([]any{"42"}) != int64(42) {
		t.Error("cast string to int")
	}
}

func TestFnCastFloat(t *testing.T) {
	if fnCastFloat(nil) != nil {
		t.Error("nil")
	}
	if fnCastFloat([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnCastFloat([]any{int64(42)}) != float64(42) {
		t.Error("cast int to float")
	}
}

func TestFnCastString(t *testing.T) {
	if fnCastString(nil) != nil {
		t.Error("nil")
	}
	if fnCastString([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnCastString([]any{int64(42)}) != "42" {
		t.Error("cast int to string")
	}
	if fnCastString([]any{3.14}) != "3.14" {
		t.Error("cast float to string")
	}
}

func TestFnNow(t *testing.T) {
	result := fnNow(nil)
	if result == nil {
		t.Error("expected non-nil")
	}
	s, ok := result.(string)
	if !ok || len(s) < 10 {
		t.Errorf("expected RFC3339 string, got %v", result)
	}
}

func TestFnYear(t *testing.T) {
	if fnYear(nil) != nil {
		t.Error("nil")
	}
	if fnYear([]any{nil}) != nil {
		t.Error("nil arg")
	}
	result := fnYear([]any{"2024-06-15T10:30:00Z"})
	if result != float64(2024) {
		t.Errorf("expected 2024, got %v", result)
	}
}

func TestFnMonth(t *testing.T) {
	if fnMonth(nil) != nil {
		t.Error("nil")
	}
	result := fnMonth([]any{"2024-06-15T10:30:00Z"})
	if result != float64(6) {
		t.Errorf("expected 6, got %v", result)
	}
}

func TestFnDay(t *testing.T) {
	if fnDay(nil) != nil {
		t.Error("nil")
	}
	result := fnDay([]any{"2024-06-15T10:30:00Z"})
	if result != float64(15) {
		t.Errorf("expected 15, got %v", result)
	}
}

func TestFnHour(t *testing.T) {
	if fnHour(nil) != nil {
		t.Error("nil")
	}
	result := fnHour([]any{"2024-06-15T10:30:00Z"})
	if result != float64(10) {
		t.Errorf("expected 10, got %v", result)
	}
}

func TestFnMinute(t *testing.T) {
	if fnMinute(nil) != nil {
		t.Error("nil")
	}
	result := fnMinute([]any{"2024-06-15T10:30:00Z"})
	if result != float64(30) {
		t.Errorf("expected 30, got %v", result)
	}
}

func TestFnSecond(t *testing.T) {
	if fnSecond(nil) != nil {
		t.Error("nil")
	}
	result := fnSecond([]any{"2024-06-15T10:30:45Z"})
	if result != float64(45) {
		t.Errorf("expected 45, got %v", result)
	}
}

func TestFnDateTrunc(t *testing.T) {
	if fnDateTrunc(nil) != nil {
		t.Error("nil")
	}
	if fnDateTrunc([]any{nil, "2024-06-15T10:30:00Z"}) != nil {
		t.Error("nil unit")
	}
	if fnDateTrunc([]any{"year", nil}) != nil {
		t.Error("nil date")
	}
	// Year truncation
	result := fnDateTrunc([]any{"year", "2024-06-15T10:30:00Z"})
	if result == nil {
		t.Fatal("expected non-nil")
	}
	// Month
	result = fnDateTrunc([]any{"month", "2024-06-15T10:30:00Z"})
	if result == nil {
		t.Fatal("expected non-nil")
	}
	// Day
	result = fnDateTrunc([]any{"day", "2024-06-15T10:30:00Z"})
	if result == nil {
		t.Fatal("expected non-nil")
	}
	// Hour
	result = fnDateTrunc([]any{"hour", "2024-06-15T10:30:00Z"})
	if result == nil {
		t.Fatal("expected non-nil")
	}
	// Unknown unit: PostgreSQL 17.11 raises 22023 `unit "unknown" not
	// recognized for type timestamp without time zone` rather than answering
	// NULL, which is what this used to assert (#855). The full accept-set and
	// the message live in scalar_refusal_test.go.
	state, _ := recoverFatalEvalForTest(t, func() {
		fnDateTrunc([]any{"unknown", "2024-06-15T10:30:00Z"})
	})
	if state != "22023" {
		t.Errorf("unknown unit raised [%s], want [22023]", state)
	}
}

func TestFnExtract(t *testing.T) {
	if fnExtract(nil) != nil {
		t.Error("nil")
	}
	if fnExtract([]any{nil, "2024-06-15"}) != nil {
		t.Error("nil unit")
	}
	ts := "2024-06-15T10:30:45Z"
	tests := []struct {
		unit   string
		expect float64
	}{
		{"year", 2024},
		{"month", 6},
		{"day", 15},
		{"hour", 10},
		{"minute", 30},
		{"second", 45},
	}
	for _, tc := range tests {
		result := fnExtract([]any{tc.unit, ts})
		if result != tc.expect {
			t.Errorf("extract(%s): expected %v, got %v", tc.unit, tc.expect, result)
		}
	}
	// DOW / DOY
	if fnExtract([]any{"dow", ts}) == nil {
		t.Error("expected non-nil for dow")
	}
	if fnExtract([]any{"doy", ts}) == nil {
		t.Error("expected non-nil for doy")
	}
	// Unknown
	if fnExtract([]any{"unknown", ts}) != nil {
		t.Error("expected nil for unknown unit")
	}
}

func TestFnCurrentDate(t *testing.T) {
	result := fnCurrentDate(nil)
	if result == nil {
		t.Fatal("expected non-nil")
	}
	s := result.(string)
	if len(s) != 10 {
		t.Errorf("expected date format YYYY-MM-DD, got %q", s)
	}
}

func TestFnDateDiff(t *testing.T) {
	if fnDateDiff(nil) != nil {
		t.Error("nil")
	}
	if fnDateDiff([]any{nil, "2024-01-01"}) != nil {
		t.Error("nil first")
	}
	if fnDateDiff([]any{"2024-01-01", nil}) != nil {
		t.Error("nil second")
	}
	result := fnDateDiff([]any{"2024-01-10", "2024-01-01"})
	if result != float64(9) {
		t.Errorf("expected 9 days, got %v", result)
	}
}

func TestFnDateAdd(t *testing.T) {
	if fnDateAdd(nil) != nil {
		t.Error("nil")
	}
	if fnDateAdd([]any{nil, int64(5)}) != nil {
		t.Error("nil date")
	}
	result := fnDateAdd([]any{"2024-01-01", int64(10)})
	if result != "2024-01-11" {
		t.Errorf("expected 2024-01-11, got %v", result)
	}
}

func TestFnDateSub(t *testing.T) {
	if fnDateSub(nil) != nil {
		t.Error("nil")
	}
	if fnDateSub([]any{nil, int64(5)}) != nil {
		t.Error("nil date")
	}
	// Numeric days
	result := fnDateSub([]any{"2024-01-11", int64(10)})
	if result != "2024-01-01" {
		t.Errorf("expected 2024-01-01, got %v", result)
	}
	// Interval value
	result = fnDateSub([]any{"2024-03-15", IntervalValue{Months: 1}})
	if result != "2024-02-15" {
		t.Errorf("expected 2024-02-15, got %v", result)
	}
}

func TestFnDateAddInterval(t *testing.T) {
	// date_add with IntervalValue
	result := fnDateAdd([]any{"2024-01-01", IntervalValue{Days: 30}})
	if result != "2024-01-31" {
		t.Errorf("expected 2024-01-31, got %v", result)
	}
	result = fnDateAdd([]any{"2024-01-01", IntervalValue{Years: 1}})
	if result != "2025-01-01" {
		t.Errorf("expected 2025-01-01, got %v", result)
	}
}

func TestDateIntervalArithmetic(t *testing.T) {
	// Test BinOp with date - interval
	iv := IntervalValue{Days: 30}
	binop := &BinOp{
		Left:  &Lit{Val: "2026-03-18"},
		Right: &Lit{Val: iv},
		Op:    "-",
	}
	result := binop.Eval(nil, 0)
	if result != "2026-02-16" {
		t.Errorf("date - interval: expected 2026-02-16, got %v", result)
	}

	// Test date + interval
	binop.Op = "+"
	result = binop.Eval(nil, 0)
	if result != "2026-04-17" {
		t.Errorf("date + interval: expected 2026-04-17, got %v", result)
	}

	// Test interval + date (commutative for +)
	binop2 := &BinOp{
		Left:  &Lit{Val: iv},
		Right: &Lit{Val: "2026-03-18"},
		Op:    "+",
	}
	result = binop2.Eval(nil, 0)
	if result != "2026-04-17" {
		t.Errorf("interval + date: expected 2026-04-17, got %v", result)
	}

	// Test month interval
	binop3 := &BinOp{
		Left:  &Lit{Val: "2026-03-18"},
		Right: &Lit{Val: IntervalValue{Months: 3}},
		Op:    "-",
	}
	result = binop3.Eval(nil, 0)
	if result != "2025-12-18" {
		t.Errorf("date - 3 months: expected 2025-12-18, got %v", result)
	}

	// Test year interval
	binop4 := &BinOp{
		Left:  &Lit{Val: "2026-03-18"},
		Right: &Lit{Val: IntervalValue{Years: 1}},
		Op:    "+",
	}
	result = binop4.Eval(nil, 0)
	if result != "2027-03-18" {
		t.Errorf("date + 1 year: expected 2027-03-18, got %v", result)
	}

	// Test nil handling
	binop5 := &BinOp{
		Left:  &Lit{Val: nil},
		Right: &Lit{Val: iv},
		Op:    "-",
	}
	if binop5.Eval(nil, 0) != nil {
		t.Error("nil date should return nil")
	}
}

func TestFnToDate(t *testing.T) {
	if fnToDate(nil) != nil {
		t.Error("nil")
	}
	if fnToDate([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnToDate([]any{"2024-06-15"}) != "2024-06-15" {
		t.Error("expected same date back")
	}
	// Invalid date
	if fnToDate([]any{"not-a-date"}) != nil {
		t.Error("expected nil for invalid date")
	}
}

func TestParseDateValue(t *testing.T) {
	// String formats
	t1 := parseDateValue("2024-06-15")
	if t1.IsZero() {
		t.Error("expected valid date for YYYY-MM-DD")
	}
	t2 := parseDateValue("2024-06-15T10:30:00Z")
	if t2.IsZero() {
		t.Error("expected valid date for RFC3339")
	}
	// int64 (days since epoch)
	t3 := parseDateValue(int64(0))
	if t3.IsZero() {
		t.Error("expected valid date for int64(0)")
	}
	// int32
	t4 := parseDateValue(int32(365))
	if t4.IsZero() {
		t.Error("expected valid date for int32")
	}
	// float64
	t5 := parseDateValue(float64(365))
	if t5.IsZero() {
		t.Error("expected valid date for float64")
	}
	// Invalid
	t6 := parseDateValue("garbage")
	if !t6.IsZero() {
		t.Error("expected zero for garbage")
	}
	// Nil
	t7 := parseDateValue(nil)
	if !t7.IsZero() {
		t.Error("expected zero for nil")
	}
}

func TestFnUUIDVersion(t *testing.T) {
	if fnUUIDVersion(nil) != nil {
		t.Error("nil")
	}
	if fnUUIDVersion([]any{nil}) != nil {
		t.Error("nil arg")
	}
	// Valid UUID v4
	result := fnUUIDVersion([]any{"550e8400-e29b-41d4-a716-446655440000"})
	if result != float64(4) {
		t.Errorf("expected version 4, got %v", result)
	}
	// Invalid UUID
	if fnUUIDVersion([]any{"not-a-uuid"}) != nil {
		t.Error("expected nil for invalid UUID")
	}
}

func TestFnUUIDToString(t *testing.T) {
	if fnUUIDToString(nil) != nil {
		t.Error("nil")
	}
	if fnUUIDToString([]any{nil}) != nil {
		t.Error("nil arg")
	}
	// Already formatted UUID - pass through
	uuid := "550e8400-e29b-41d4-a716-446655440000"
	result := fnUUIDToString([]any{uuid})
	if result != uuid {
		t.Errorf("expected passthrough, got %v", result)
	}
	// Hex UUID without dashes
	hex := "550e8400e29b41d4a716446655440000"
	result = fnUUIDToString([]any{hex})
	if result == nil {
		t.Fatal("expected formatted UUID")
	}
	s := result.(string)
	if len(s) != 36 || s[8] != '-' {
		t.Errorf("expected formatted UUID, got %q", s)
	}
	// Invalid
	if fnUUIDToString([]any{"xyz"}) != nil {
		t.Error("expected nil for invalid")
	}
}

func TestToStringHelper(t *testing.T) {
	if toString("hello") != "hello" {
		t.Error("string")
	}
	if toString([]byte("bytes")) != "bytes" {
		t.Error("bytes")
	}
	if toString(42) != "42" {
		t.Error("int")
	}
	if toString(nil) != "<nil>" {
		t.Error("nil")
	}
}

func TestToFloat64AllTypes(t *testing.T) {
	if ToFloat64(float64(3.14)) != 3.14 {
		t.Error("float64")
	}
	if ToFloat64(float32(2.5)) != float64(float32(2.5)) {
		t.Error("float32")
	}
	if ToFloat64(int64(42)) != 42 {
		t.Error("int64")
	}
	if ToFloat64(int(10)) != 10 {
		t.Error("int")
	}
	if ToFloat64(int32(5)) != 5 {
		t.Error("int32")
	}
	if ToFloat64(true) != 1 {
		t.Error("bool true")
	}
	if ToFloat64(false) != 0 {
		t.Error("bool false")
	}
	if ToFloat64("3.14") != 3.14 {
		t.Error("string float")
	}
	if ToFloat64("not-a-number") != 0 {
		t.Error("invalid string")
	}
	if ToFloat64(nil) != 0 {
		t.Error("nil")
	}
}

func TestToBoolVal(t *testing.T) {
	if toBoolVal(nil) {
		t.Error("nil")
	}
	if !toBoolVal(true) {
		t.Error("bool true")
	}
	if toBoolVal(false) {
		t.Error("bool false")
	}
	if !toBoolVal(float64(1)) {
		t.Error("float64 1")
	}
	if toBoolVal(float64(0)) {
		t.Error("float64 0")
	}
	if !toBoolVal(int64(1)) {
		t.Error("int64 1")
	}
	if toBoolVal(int64(0)) {
		t.Error("int64 0")
	}
	if !toBoolVal(int(1)) {
		t.Error("int 1")
	}
	if toBoolVal(int(0)) {
		t.Error("int 0")
	}
	if !toBoolVal("hello") {
		t.Error("non-empty string")
	}
	if toBoolVal("") {
		t.Error("empty string")
	}
	// Default: struct should be truthy
	if !toBoolVal(struct{}{}) {
		t.Error("default should be true")
	}
}

func TestToInt64Safe(t *testing.T) {
	v, ok := toInt64Safe(int64(42))
	if !ok || v != 42 {
		t.Error("int64")
	}
	v, ok = toInt64Safe(int32(10))
	if !ok || v != 10 {
		t.Error("int32")
	}
	v, ok = toInt64Safe(int(5))
	if !ok || v != 5 {
		t.Error("int")
	}
	_, ok = toInt64Safe("string")
	if ok {
		t.Error("string should fail")
	}
	_, ok = toInt64Safe(nil)
	if ok {
		t.Error("nil should fail")
	}
}

func TestToFloat64Safe(t *testing.T) {
	v, ok := toFloat64Safe(float64(3.14))
	if !ok || v != 3.14 {
		t.Error("float64")
	}
	v, ok = toFloat64Safe(float32(2.5))
	if !ok {
		t.Error("float32")
	}
	v, ok = toFloat64Safe(int64(42))
	if !ok || v != 42 {
		t.Error("int64")
	}
	v, ok = toFloat64Safe(int32(10))
	if !ok || v != 10 {
		t.Error("int32")
	}
	_, ok = toFloat64Safe("string")
	if ok {
		t.Error("string should fail")
	}
	_, ok = toFloat64Safe(nil)
	if ok {
		t.Error("nil should fail")
	}
}
