package expr

import (
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
		{"int args", []any{int64(10), int64(3)}, math.Mod(10, 3)},
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
}

func TestFnSqrt(t *testing.T) {
	if fnSqrt(nil) != nil {
		t.Error("nil")
	}
	if fnSqrt([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnSqrt([]any{-1.0}) != nil {
		t.Error("negative")
	}
	if fnSqrt([]any{4.0}) != 2.0 {
		t.Error("sqrt")
	}
}

func TestFnConcat(t *testing.T) {
	// empty args -> empty string (sb.String() with no writes)
	result := fnConcat([]any{})
	if result != "" {
		t.Errorf("expected empty string for no args, got %v", result)
	}
	// nil arg returns nil (SQL convention)
	if fnConcat([]any{nil}) != nil {
		t.Error("nil arg should return nil")
	}
	// Single arg
	if fnConcat([]any{"hello"}) != "hello" {
		t.Error("single")
	}
	// Multiple args - any nil returns nil
	result = fnConcat([]any{"a", nil, "b"})
	if result != nil {
		t.Errorf("expected nil for concat with nil arg, got %v", result)
	}
	// Multiple args all non-nil
	result = fnConcat([]any{"a", "b", "c"})
	if result != "abc" {
		t.Errorf("expected 'abc', got %v", result)
	}
}

func TestFnLength(t *testing.T) {
	if fnLength(nil) != nil {
		t.Error("nil")
	}
	if fnLength([]any{nil}) != nil {
		t.Error("nil arg")
	}
	// length returns float64
	if fnLength([]any{"hello"}) != float64(5) {
		t.Errorf("expected 5.0, got %v (%T)", fnLength([]any{"hello"}), fnLength([]any{"hello"}))
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
	if fnCardinality([]any{[]any{1, 2, 3}}) != int64(3) {
		t.Error("expected 3")
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
	// negative value
	if fnLog([]any{-1.0}) != nil {
		t.Error("negative value")
	}
	// base 1
	if fnLog([]any{1.0, 8.0}) != nil {
		t.Error("base 1")
	}
	// base negative
	if fnLog([]any{-1.0, 8.0}) != nil {
		t.Error("base negative")
	}
	// val negative with base
	if fnLog([]any{2.0, -8.0}) != nil {
		t.Error("val negative")
	}
}

func TestFnLn(t *testing.T) {
	if fnLn(nil) != nil {
		t.Error("nil")
	}
	if fnLn([]any{nil}) != nil {
		t.Error("nil arg")
	}
	if fnLn([]any{-1.0}) != nil {
		t.Error("negative")
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
	// Unknown unit
	if fnDateTrunc([]any{"unknown", "2024-06-15T10:30:00Z"}) != nil {
		t.Error("expected nil for unknown unit")
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
