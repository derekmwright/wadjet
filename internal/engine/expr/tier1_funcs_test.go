package expr

import (
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"math"
	"strings"
	"testing"
)

// --- String: regex and parsing ---

func TestSplitPart(t *testing.T) {
	fn := DefaultRegistry.Lookup("split_part")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"a-b-c", "-", float64(1)}, "a"},
		{[]any{"a-b-c", "-", float64(2)}, "b"},
		{[]any{"a-b-c", "-", float64(3)}, "c"},
		{[]any{"a-b-c", "-", float64(4)}, ""}, // past the end
		// NEGATIVE positions count from the END on PostgreSQL 14 and later,
		// and every one of them used to answer "" here (#855).
		{[]any{"a-b-c", "-", float64(-1)}, "c"},
		{[]any{"a-b-c", "-", float64(-3)}, "a"},
		{[]any{"a-b-c", "-", float64(-4)}, ""}, // past the start
		{[]any{"hello", ",", float64(1)}, "hello"},
		{[]any{nil, "-", float64(1)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("split_part(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
	// Position 0 names no field at all and is SQLSTATE 22023 on the server,
	// not the empty string this used to answer (#855).
	state, _ := recoverFatalEvalForTest(t, func() { fn([]any{"a-b-c", "-", float64(0)}) })
	if state != "22023" {
		t.Errorf("split_part(…, 0) raised [%s], want [22023] field position must not be zero", state)
	}
}

func TestStrPos(t *testing.T) {
	fn := DefaultRegistry.Lookup("strpos")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"hello world", "world"}, int32(7)},
		{[]any{"hello world", "hello"}, int32(1)},
		{[]any{"hello world", "xyz"}, int32(0)},
		{[]any{nil, "x"}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("strpos(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}

	// position is an alias
	pos := DefaultRegistry.Lookup("position")
	if pos == nil {
		t.Fatal("position function not registered")
	}
}

func TestRegexpLike(t *testing.T) {
	fn := DefaultRegistry.Lookup("regexp_like")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.1", `^\d+\.\d+\.\d+\.\d+$`}, true},
		{[]any{"hello", `^\d+$`}, false},
		{[]any{"abc123", `\d+`}, true},
		{[]any{nil, `.*`}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("regexp_like(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestRegexpExtract(t *testing.T) {
	fn := DefaultRegistry.Lookup("regexp_extract")
	tests := []struct {
		args []any
		want any
	}{
		// group 0 = full match
		{[]any{"user@example.com", `(\w+)@(\w+)\.(\w+)`}, "user@example.com"},
		// group 1 = first capture
		{[]any{"user@example.com", `(\w+)@(\w+)\.(\w+)`, float64(1)}, "user"},
		// group 2 = second capture
		{[]any{"user@example.com", `(\w+)@(\w+)\.(\w+)`, float64(2)}, "example"},
		// no match
		{[]any{"hello", `(\d+)`}, nil},
		{[]any{nil, `.*`}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("regexp_extract(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestRegexpReplace(t *testing.T) {
	fn := DefaultRegistry.Lookup("regexp_replace")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.1", `\d+`, "X"}, "X.X.X.X"},
		{[]any{"hello world", `\s+`, "-"}, "hello-world"},
		{[]any{nil, `.*`, ""}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("regexp_replace(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// --- Encoding ---

func TestToHex(t *testing.T) {
	fn := DefaultRegistry.Lookup("to_hex")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{float64(255)}, "ff"},
		{[]any{float64(16)}, "10"},
		{[]any{float64(0)}, "0"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("to_hex(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestFromHex(t *testing.T) {
	fn := DefaultRegistry.Lookup("from_hex")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"ff"}, float64(255)},
		{[]any{"10"}, float64(16)},
		{[]any{"0"}, float64(0)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("from_hex(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestBase64RoundTrip(t *testing.T) {
	toB64 := DefaultRegistry.Lookup("to_base64")
	fromB64 := DefaultRegistry.Lookup("from_base64")

	original := "Hello, World!"
	encoded := toB64([]any{original})
	if encoded != "SGVsbG8sIFdvcmxkIQ==" {
		t.Errorf("to_base64(%q) = %v", original, encoded)
	}
	decoded := fromB64([]any{encoded})
	if decoded != original {
		t.Errorf("from_base64(to_base64(%q)) = %v", original, decoded)
	}

	// NULL propagation
	if toB64([]any{nil}) != nil {
		t.Error("to_base64(NULL) should be NULL")
	}
	if fromB64([]any{nil}) != nil {
		t.Error("from_base64(NULL) should be NULL")
	}
}

// --- Date/time conversion ---

func TestFromUnixtime(t *testing.T) {
	fn := DefaultRegistry.Lookup("from_unixtime")
	got := fn([]any{float64(0)})
	if got != "1970-01-01T00:00:00Z" {
		t.Errorf("from_unixtime(0) = %v", got)
	}
	got = fn([]any{float64(1700000000)})
	if got == nil {
		t.Error("from_unixtime(1700000000) should not be nil")
	}
	if fn([]any{nil}) != nil {
		t.Error("from_unixtime(NULL) should be NULL")
	}
}

func TestToUnixtime(t *testing.T) {
	fn := DefaultRegistry.Lookup("to_unixtime")
	got := fn([]any{"1970-01-01T00:00:00Z"})
	if got != float64(0) {
		t.Errorf("to_unixtime('1970-01-01T00:00:00Z') = %v, want 0", got)
	}
	if fn([]any{nil}) != nil {
		t.Error("to_unixtime(NULL) should be NULL")
	}
}

func TestDateFormat(t *testing.T) {
	fn := DefaultRegistry.Lookup("date_format")
	got := fn([]any{"2026-03-15T14:30:00Z", "%Y-%m-%d"})
	if got != "2026-03-15" {
		t.Errorf("date_format = %v, want 2026-03-15", got)
	}
	got = fn([]any{"2026-03-15T14:30:00Z", "%H:%i:%s"})
	if got != "14:30:00" {
		t.Errorf("date_format = %v, want 14:30:00", got)
	}
}

func TestDateParse(t *testing.T) {
	fn := DefaultRegistry.Lookup("date_parse")
	got := fn([]any{"2026-03-15", "%Y-%m-%d"})
	if got == nil {
		t.Fatal("date_parse returned nil")
	}
	if !strings.HasPrefix(got.(string), "2026-03-15") {
		t.Errorf("date_parse = %v, want prefix 2026-03-15", got)
	}
}

// --- Hash ---

func TestMD5(t *testing.T) {
	fn := DefaultRegistry.Lookup("md5")
	h := md5.Sum([]byte("hello"))
	want := hex.EncodeToString(h[:])
	got := fn([]any{"hello"})
	if got != want {
		t.Errorf("md5('hello') = %v, want %v", got, want)
	}
	if fn([]any{nil}) != nil {
		t.Error("md5(NULL) should be NULL")
	}
}

func TestSHA256(t *testing.T) {
	fn := DefaultRegistry.Lookup("sha256")
	h := sha256.Sum256([]byte("hello"))
	want := hex.EncodeToString(h[:])
	got := fn([]any{"hello"})
	if got != want {
		t.Errorf("sha256('hello') = %v, want %v", got, want)
	}
}

func TestSHA512(t *testing.T) {
	fn := DefaultRegistry.Lookup("sha512")
	h := sha512.Sum512([]byte("hello"))
	want := hex.EncodeToString(h[:])
	got := fn([]any{"hello"})
	if got != want {
		t.Errorf("sha512('hello') = %v, want %v", got, want)
	}
}

// --- Bitwise ---

func TestBitwiseAnd(t *testing.T) {
	fn := DefaultRegistry.Lookup("bitwise_and")
	got := fn([]any{float64(0xFF), float64(0x0F)})
	if got != float64(0x0F) {
		t.Errorf("bitwise_and(0xFF, 0x0F) = %v, want %v", got, float64(0x0F))
	}
}

func TestBitwiseOr(t *testing.T) {
	fn := DefaultRegistry.Lookup("bitwise_or")
	got := fn([]any{float64(0xF0), float64(0x0F)})
	if got != float64(0xFF) {
		t.Errorf("bitwise_or(0xF0, 0x0F) = %v, want %v", got, float64(0xFF))
	}
}

func TestBitwiseXor(t *testing.T) {
	fn := DefaultRegistry.Lookup("bitwise_xor")
	got := fn([]any{float64(0xFF), float64(0x0F)})
	if got != float64(0xF0) {
		t.Errorf("bitwise_xor(0xFF, 0x0F) = %v, want %v", got, float64(0xF0))
	}
}

func TestBitwiseNot(t *testing.T) {
	fn := DefaultRegistry.Lookup("bitwise_not")
	got := fn([]any{float64(0)})
	if got != float64(-1) {
		t.Errorf("bitwise_not(0) = %v, want -1", got)
	}
}

// --- Verify all Tier 1 functions are registered ---

func TestTier1FunctionsRegistered(t *testing.T) {
	expected := []string{
		"split_part", "strpos", "position",
		"regexp_like", "regexp_extract", "regexp_replace",
		"to_hex", "from_hex", "to_base64", "from_base64",
		"from_unixtime", "to_unixtime", "date_format", "date_parse",
		"md5", "sha256", "sha512",
		"bitwise_and", "bitwise_or", "bitwise_xor", "bitwise_not",
	}
	for _, name := range expected {
		if !DefaultRegistry.Has(name) {
			t.Errorf("function %q not registered", name)
		}
	}
}

// --- Hex round-trip ---

func TestHexRoundTrip(t *testing.T) {
	toHex := DefaultRegistry.Lookup("to_hex")
	fromHex := DefaultRegistry.Lookup("from_hex")
	values := []float64{0, 1, 255, 1024, 65535}
	for _, v := range values {
		encoded := toHex([]any{v})
		decoded := fromHex([]any{encoded})
		if decoded != v {
			t.Errorf("hex round-trip failed for %v: encoded=%v, decoded=%v", v, encoded, decoded)
		}
	}
}

// --- Bitwise identity properties ---

func TestBitwiseIdentities(t *testing.T) {
	and := DefaultRegistry.Lookup("bitwise_and")
	or := DefaultRegistry.Lookup("bitwise_or")
	xor := DefaultRegistry.Lookup("bitwise_xor")

	a := float64(42)
	b := float64(73)

	// a & a == a
	if and([]any{a, a}) != a {
		t.Error("a & a should equal a")
	}
	// a | a == a
	if or([]any{a, a}) != a {
		t.Error("a | a should equal a")
	}
	// a ^ a == 0
	if xor([]any{a, a}) != float64(0) {
		t.Error("a ^ a should equal 0")
	}
	// (a & b) | (a ^ b) == a | b
	ab_and := and([]any{a, b}).(float64)
	ab_xor := xor([]any{a, b}).(float64)
	ab_or := or([]any{a, b}).(float64)
	if or([]any{ab_and, ab_xor}) != ab_or {
		t.Error("(a&b) | (a^b) should equal a|b")
	}
}

// --- NULL propagation across all Tier 1 ---

func TestTier1NullPropagation(t *testing.T) {
	fns := []string{
		"split_part", "strpos", "regexp_like", "regexp_extract", "regexp_replace",
		"to_hex", "from_hex", "to_base64", "from_base64",
		"from_unixtime", "to_unixtime", "date_format", "date_parse",
		"md5", "sha256", "sha512",
		"bitwise_and", "bitwise_or", "bitwise_xor", "bitwise_not",
	}
	for _, name := range fns {
		fn := DefaultRegistry.Lookup(name)
		if fn == nil {
			t.Errorf("function %q not found", name)
			continue
		}
		got := fn([]any{nil})
		if got != nil {
			t.Errorf("%s(nil) = %v, want nil", name, got)
		}
	}
}

// --- Regression: sql format conversion ---

func TestSqlFormatToGo(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"%Y-%m-%d", "2006-01-02"},
		{"%H:%i:%s", "15:04:05"},
		{"%Y-%m-%d %H:%i:%s", "2006-01-02 15:04:05"},
		{"%T", "15:04:05"},
	}
	for _, tt := range tests {
		got := sqlFormatToGo(tt.sql)
		if got != tt.want {
			t.Errorf("sqlFormatToGo(%q) = %q, want %q", tt.sql, got, tt.want)
		}
	}
}

// --- UnixTime round-trip ---

func TestUnixTimeRoundTrip(t *testing.T) {
	toUnix := DefaultRegistry.Lookup("to_unixtime")
	fromUnix := DefaultRegistry.Lookup("from_unixtime")

	epoch := float64(1700000000)
	ts := fromUnix([]any{epoch})
	result := toUnix([]any{ts})
	if math.Abs(result.(float64)-epoch) > 1 {
		t.Errorf("unix time round-trip: %v -> %v -> %v", epoch, ts, result)
	}
}
