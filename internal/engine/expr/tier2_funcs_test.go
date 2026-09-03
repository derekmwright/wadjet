package expr

import (
	"math"
	"testing"
)

// --- String: padding and character ---

func TestLPad(t *testing.T) {
	fn := DefaultRegistry.Lookup("lpad")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"hi", float64(5)}, "   hi"},
		{[]any{"hi", float64(5), "."}, "...hi"},
		{[]any{"hello", float64(3)}, "hel"},
		{[]any{"hi", float64(7), "ab"}, "ababahi"},
		{[]any{nil, float64(5)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("lpad(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestRPad(t *testing.T) {
	fn := DefaultRegistry.Lookup("rpad")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"hi", float64(5)}, "hi   "},
		{[]any{"hi", float64(5), "."}, "hi..."},
		{[]any{"hello", float64(3)}, "hel"},
		{[]any{nil, float64(5)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("rpad(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestChrCodepoint(t *testing.T) {
	chr := DefaultRegistry.Lookup("chr")
	cp := DefaultRegistry.Lookup("codepoint")

	// ASCII
	if chr([]any{float64(65)}) != "A" {
		t.Error("chr(65) should be 'A'")
	}
	if cp([]any{"A"}) != int32(65) {
		t.Error("codepoint('A') should be int32(65)")
	}

	// Round-trip
	emoji := "🔥"
	code := cp([]any{emoji}).(int32)
	back := chr([]any{code})
	if back != emoji {
		t.Errorf("chr/codepoint round-trip failed for %s", emoji)
	}

	// NULL
	if chr([]any{nil}) != nil {
		t.Error("chr(nil) should be nil")
	}
	if cp([]any{nil}) != nil {
		t.Error("codepoint(nil) should be nil")
	}
}

func TestConcatWS(t *testing.T) {
	fn := DefaultRegistry.Lookup("concat_ws")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{",", "a", "b", "c"}, "a,b,c"},
		{[]any{"-", "hello", nil, "world"}, "hello-world"}, // skips NULLs
		{[]any{nil, "a", "b"}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("concat_ws(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestCharLength(t *testing.T) {
	fn := DefaultRegistry.Lookup("char_length")
	if fn([]any{"hello"}) != int32(5) {
		t.Error("char_length('hello') should be int32(5)")
	}
	// Multi-byte characters
	if fn([]any{"日本語"}) != int32(3) {
		t.Error("char_length('日本語') should be int32(3)")
	}
}

func TestTranslate(t *testing.T) {
	fn := DefaultRegistry.Lookup("translate")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"hello", "helo", "HELO"}, "HELLO"},
		{[]any{"abc", "abc", "xyz"}, "xyz"},
		{[]any{"abc", "ab", "x"}, "xc"}, // 'b' has no replacement → deleted
		{[]any{nil, "a", "b"}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("translate(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// --- Math: trigonometry ---

func TestPi(t *testing.T) {
	fn := DefaultRegistry.Lookup("pi")
	got := fn([]any{})
	if got != math.Pi {
		t.Errorf("pi() = %v, want %v", got, math.Pi)
	}
}

func TestDegreesRadians(t *testing.T) {
	deg := DefaultRegistry.Lookup("degrees")
	rad := DefaultRegistry.Lookup("radians")

	// 180 degrees = pi radians
	if math.Abs(deg([]any{math.Pi}).(float64)-180.0) > 1e-10 {
		t.Error("degrees(pi) should be 180")
	}
	if math.Abs(rad([]any{float64(180)}).(float64)-math.Pi) > 1e-10 {
		t.Error("radians(180) should be pi")
	}

	// Round-trip
	v := float64(45)
	result := deg([]any{rad([]any{v})}).(float64)
	if math.Abs(result-v) > 1e-10 {
		t.Errorf("degrees(radians(45)) = %v", result)
	}
}

func TestTrigFunctions(t *testing.T) {
	sin := DefaultRegistry.Lookup("sin")
	cos := DefaultRegistry.Lookup("cos")
	tan := DefaultRegistry.Lookup("tan")

	// sin(0) = 0, cos(0) = 1
	if sin([]any{float64(0)}) != float64(0) {
		t.Error("sin(0) should be 0")
	}
	if cos([]any{float64(0)}) != float64(1) {
		t.Error("cos(0) should be 1")
	}

	// sin²(x) + cos²(x) = 1
	x := float64(1.234)
	s := sin([]any{x}).(float64)
	c := cos([]any{x}).(float64)
	if math.Abs(s*s+c*c-1.0) > 1e-10 {
		t.Error("sin²+cos² should equal 1")
	}

	// tan(pi/4) ≈ 1
	if math.Abs(tan([]any{math.Pi / 4}).(float64)-1.0) > 1e-10 {
		t.Error("tan(pi/4) should be ~1")
	}
}

func TestInverseTrig(t *testing.T) {
	asin := DefaultRegistry.Lookup("asin")
	acos := DefaultRegistry.Lookup("acos")
	atan := DefaultRegistry.Lookup("atan")
	atan2 := DefaultRegistry.Lookup("atan2")

	// asin(1) = pi/2
	if math.Abs(asin([]any{float64(1)}).(float64)-math.Pi/2) > 1e-10 {
		t.Error("asin(1) should be pi/2")
	}
	// acos(1) = 0
	if math.Abs(acos([]any{float64(1)}).(float64)) > 1e-10 {
		t.Error("acos(1) should be 0")
	}
	// atan(1) = pi/4
	if math.Abs(atan([]any{float64(1)}).(float64)-math.Pi/4) > 1e-10 {
		t.Error("atan(1) should be pi/4")
	}
	// atan2(1, 1) = pi/4
	if math.Abs(atan2([]any{float64(1), float64(1)}).(float64)-math.Pi/4) > 1e-10 {
		t.Error("atan2(1,1) should be pi/4")
	}

	// Out of range RAISES since #840 — it answered nil, which is the same box
	// a NULL argument produces, so `WHERE ASIN(x) IS NULL` could not tell a
	// missing value from an impossible one. PostgreSQL 17.11 says 22003
	// "input is out of range" for both functions and both signs.
	for _, c := range []struct {
		name string
		fn   func([]any) any
		arg  float64
	}{
		{"asin(2)", asin, 2}, {"asin(-2)", asin, -2},
		{"acos(2)", acos, 2}, {"acos(-2)", acos, -2},
	} {
		state, msg := recoverFatalEvalForTest(t, func() { c.fn([]any{c.arg}) })
		if state != "22003" || msg != "input is out of range" {
			t.Errorf("%s raised [%s] %s, want [22003] input is out of range "+
				"(live PostgreSQL 17.11)", c.name, state, msg)
		}
	}
	// NaN is a value PostgreSQL answers with, not a domain failure: it is
	// neither less than -1 nor greater than 1, and the refusal is written so.
	if got := asin([]any{math.NaN()}); got == nil || !math.IsNaN(got.(float64)) {
		t.Errorf("ASIN(NaN) = %v, want NaN (PostgreSQL answers NaN)", got)
	}
}

func TestCbrt(t *testing.T) {
	fn := DefaultRegistry.Lookup("cbrt")
	if math.Abs(fn([]any{float64(27)}).(float64)-3.0) > 1e-10 {
		t.Error("cbrt(27) should be 3")
	}
	if math.Abs(fn([]any{float64(-8)}).(float64)-(-2.0)) > 1e-10 {
		t.Error("cbrt(-8) should be -2")
	}
}

func TestLog2(t *testing.T) {
	fn := DefaultRegistry.Lookup("log2")
	if fn([]any{float64(8)}) != float64(3) {
		t.Errorf("log2(8) = %v, want 3", fn([]any{float64(8)}))
	}
	// LOG2 is a wadjet extension — PostgreSQL has no such function — and it
	// takes LOG's refusal, because one engine cannot answer "what is the
	// logarithm of zero" two ways depending on which base a caller spells.
	for _, c := range []struct {
		arg float64
		msg string
	}{
		{-1, "cannot take logarithm of a negative number"},
		{0, "cannot take logarithm of zero"},
	} {
		state, msg := recoverFatalEvalForTest(t, func() { fn([]any{c.arg}) })
		if state != "2201E" || msg != c.msg {
			t.Errorf("LOG2(%v) raised [%s] %s, want [2201E] %s", c.arg, state, msg, c.msg)
		}
	}
}

func TestTruncate(t *testing.T) {
	fn := DefaultRegistry.Lookup("truncate")
	tests := []struct {
		args []any
		want float64
	}{
		{[]any{float64(3.789)}, 3.0},
		{[]any{float64(3.789), float64(2)}, 3.78},
		{[]any{float64(-3.789), float64(1)}, -3.7},
	}
	for _, tt := range tests {
		got := fn(tt.args).(float64)
		if math.Abs(got-tt.want) > 1e-10 {
			t.Errorf("truncate(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestRandom(t *testing.T) {
	fn := DefaultRegistry.Lookup("random")
	for i := 0; i < 10; i++ {
		v := fn([]any{}).(float64)
		if v < 0 || v >= 1 {
			t.Errorf("random() = %v, should be in [0, 1)", v)
		}
	}
	// rand is an alias
	if DefaultRegistry.Lookup("rand") == nil {
		t.Error("rand function not registered")
	}
}

// --- JSON ---

func TestJSONExtract(t *testing.T) {
	fn := DefaultRegistry.Lookup("json_extract")
	doc := `{"name":"alice","age":30,"address":{"city":"NYC"},"tags":["a","b"]}`

	tests := []struct {
		path string
		want any
	}{
		{"$.name", "alice"},
		{"$.age", float64(30)},
		{"$.address.city", "NYC"},
		{"$.tags[0]", "a"},
		{"$.tags[1]", "b"},
		{"$.missing", nil},
	}
	for _, tt := range tests {
		got := fn([]any{doc, tt.path})
		if got != tt.want {
			t.Errorf("json_extract($, %q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestJSONExtractScalar(t *testing.T) {
	fn := DefaultRegistry.Lookup("json_extract_scalar")
	doc := `{"name":"alice","nested":{"key":"val"}}`

	// Scalar values returned directly
	if fn([]any{doc, "$.name"}) != "alice" {
		t.Error("expected alice")
	}
	// Non-scalar returns nil
	if fn([]any{doc, "$.nested"}) != nil {
		t.Error("non-scalar should return nil")
	}
}

func TestJSONArrayLength(t *testing.T) {
	fn := DefaultRegistry.Lookup("json_array_length")
	if fn([]any{`[1,2,3]`}) != float64(3) {
		t.Error("expected 3")
	}
	if fn([]any{`[]`}) != float64(0) {
		t.Error("expected 0")
	}
	if fn([]any{`{}`}) != nil {
		t.Error("non-array should return nil")
	}
}

func TestJSONValid(t *testing.T) {
	fn := DefaultRegistry.Lookup("json_valid")
	if fn([]any{`{"key": "value"}`}) != true {
		t.Error("valid JSON should return true")
	}
	if fn([]any{`not json`}) != false {
		t.Error("invalid JSON should return false")
	}
}

// --- URL ---

func TestURLExtract(t *testing.T) {
	testURL := "https://example.com:8443/api/v1/query?limit=100&format=json"

	host := DefaultRegistry.Lookup("url_extract_host")
	port := DefaultRegistry.Lookup("url_extract_port")
	path := DefaultRegistry.Lookup("url_extract_path")
	proto := DefaultRegistry.Lookup("url_extract_protocol")
	query := DefaultRegistry.Lookup("url_extract_query")
	param := DefaultRegistry.Lookup("url_extract_parameter")

	if host([]any{testURL}) != "example.com" {
		t.Errorf("host = %v", host([]any{testURL}))
	}
	if port([]any{testURL}) != float64(8443) {
		t.Errorf("port = %v", port([]any{testURL}))
	}
	if path([]any{testURL}) != "/api/v1/query" {
		t.Errorf("path = %v", path([]any{testURL}))
	}
	if proto([]any{testURL}) != "https" {
		t.Errorf("protocol = %v", proto([]any{testURL}))
	}
	if query([]any{testURL}) != "limit=100&format=json" {
		t.Errorf("query = %v", query([]any{testURL}))
	}
	if param([]any{testURL, "limit"}) != "100" {
		t.Errorf("parameter = %v", param([]any{testURL, "limit"}))
	}
	if param([]any{testURL, "format"}) != "json" {
		t.Errorf("parameter format = %v", param([]any{testURL, "format"}))
	}
}

// --- Type introspection ---

func TestTypeof(t *testing.T) {
	fn := DefaultRegistry.Lookup("typeof")
	tests := []struct {
		arg  any
		want string
	}{
		{int64(42), "bigint"},
		{float64(3.14), "double"},
		{"hello", "varchar"},
		{true, "boolean"},
		{nil, "null"},
	}
	for _, tt := range tests {
		got := fn([]any{tt.arg})
		if got != tt.want {
			t.Errorf("typeof(%v) = %v, want %v", tt.arg, got, tt.want)
		}
	}
}

// --- Verify all Tier 2 functions registered ---

func TestTier2FunctionsRegistered(t *testing.T) {
	expected := []string{
		"lpad", "rpad", "chr", "codepoint", "concat_ws", "char_length", "translate",
		"pi", "degrees", "radians", "sin", "cos", "tan", "asin", "acos", "atan", "atan2",
		"cbrt", "log2", "truncate", "rand", "random",
		"json_extract", "json_extract_scalar", "json_array_length", "json_valid",
		"url_extract_host", "url_extract_port", "url_extract_path",
		"url_extract_protocol", "url_extract_query", "url_extract_parameter",
		"typeof",
	}
	for _, name := range expected {
		if !DefaultRegistry.Has(name) {
			t.Errorf("function %q not registered", name)
		}
	}
}
