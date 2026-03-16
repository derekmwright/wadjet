package expr

import (
	"strings"
	"testing"
)

// ── Regex ────────────────────────────────────────────────────────────────────

func TestRegexpCount(t *testing.T) {
	fn := DefaultRegistry.Lookup("regexp_count")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"aababc", "a"}, int64(3)},
		{[]any{"hello world", "[aeiou]"}, int64(3)},
		{[]any{"no match", "xyz"}, int64(0)},
		{[]any{nil, "a"}, nil},
		{[]any{"test", nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("regexp_count(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestRegexpExtractAll(t *testing.T) {
	fn := DefaultRegistry.Lookup("regexp_extract_all")
	got := fn([]any{"abc123def456", "[0-9]+"})
	if got == nil {
		t.Fatal("expected non-nil")
	}
	s := got.(string)
	if s != `["123","456"]` {
		t.Errorf("regexp_extract_all = %q, want %q", s, `["123","456"]`)
	}

	// No matches
	got = fn([]any{"hello", "[0-9]+"})
	if got != "[]" {
		t.Errorf("regexp_extract_all no match = %v, want []", got)
	}

	// Null propagation
	if fn([]any{nil, "a"}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestRegexpSplit(t *testing.T) {
	fn := DefaultRegistry.Lookup("regexp_split")
	got := fn([]any{"one,two,,three", ","})
	if got == nil {
		t.Fatal("expected non-nil")
	}
	s := got.(string)
	if s != `["one","two","","three"]` {
		t.Errorf("regexp_split = %q, want %q", s, `["one","two","","three"]`)
	}

	if fn([]any{nil, ","}) != nil {
		t.Error("expected nil for nil input")
	}
}

// ── String ───────────────────────────────────────────────────────────────────

func TestSplit(t *testing.T) {
	fn := DefaultRegistry.Lookup("split")
	got := fn([]any{"a.b.c", "."})
	if got == nil {
		t.Fatal("expected non-nil")
	}
	s := got.(string)
	if s != `["a","b","c"]` {
		t.Errorf("split = %q, want %q", s, `["a","b","c"]`)
	}

	if fn([]any{nil, "."}) != nil {
		t.Error("expected nil for nil input")
	}
}

// ── Bitwise Shifts ───────────────────────────────────────────────────────────

func TestBitwiseLeftShift(t *testing.T) {
	fn := DefaultRegistry.Lookup("bitwise_left_shift")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(1), int64(4)}, int64(16)},
		{[]any{int64(255), int64(8)}, int64(65280)},
		{[]any{int64(1), int64(64)}, nil}, // out of range
		{[]any{nil, int64(1)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("bitwise_left_shift(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestBitwiseRightShift(t *testing.T) {
	fn := DefaultRegistry.Lookup("bitwise_right_shift")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(16), int64(4)}, int64(1)},
		{[]any{int64(256), int64(4)}, int64(16)},
		// Logical shift: negative number should not sign-extend
		{[]any{int64(-1), int64(32)}, int64(4294967295)}, // upper 32 bits
		{[]any{nil, int64(1)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("bitwise_right_shift(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestBitwiseArithmeticShiftRight(t *testing.T) {
	fn := DefaultRegistry.Lookup("bitwise_arithmetic_shift_right")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(16), int64(4)}, int64(1)},
		// Arithmetic shift: negative number preserves sign
		{[]any{int64(-16), int64(2)}, int64(-4)},
		{[]any{nil, int64(1)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("bitwise_arithmetic_shift_right(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── UUID ─────────────────────────────────────────────────────────────────────

func TestUUID(t *testing.T) {
	fn := DefaultRegistry.Lookup("uuid")
	got := fn([]any{})
	if got == nil {
		t.Fatal("uuid() returned nil")
	}
	s := got.(string)
	if len(s) != 36 {
		t.Errorf("uuid length = %d, want 36: %q", len(s), s)
	}
	// Check format: 8-4-4-4-12
	parts := strings.Split(s, "-")
	if len(parts) != 5 {
		t.Errorf("uuid parts = %d, want 5: %q", len(parts), s)
	}

	// Verify uniqueness
	got2 := fn([]any{})
	if got == got2 {
		t.Error("uuid() returned same value twice")
	}
}

// ── Encoding ─────────────────────────────────────────────────────────────────

func TestToBase32(t *testing.T) {
	fn := DefaultRegistry.Lookup("to_base32")
	got := fn([]any{"hello"})
	if got != "NBSWY3DP" {
		t.Errorf("to_base32('hello') = %v, want NBSWY3DP", got)
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestFromBase32(t *testing.T) {
	fn := DefaultRegistry.Lookup("from_base32")
	got := fn([]any{"NBSWY3DP"})
	if got != "hello" {
		t.Errorf("from_base32('NBSWY3DP') = %v, want hello", got)
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestXXHash64(t *testing.T) {
	fn := DefaultRegistry.Lookup("xxhash64")
	got := fn([]any{"hello"})
	if got == nil {
		t.Fatal("xxhash64 returned nil")
	}
	s := got.(string)
	if len(s) != 16 {
		t.Errorf("xxhash64 length = %d, want 16: %q", len(s), s)
	}
	// Deterministic
	got2 := fn([]any{"hello"})
	if got != got2 {
		t.Errorf("xxhash64 not deterministic: %v vs %v", got, got2)
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestMurmur3(t *testing.T) {
	fn := DefaultRegistry.Lookup("murmur3")
	got := fn([]any{"hello"})
	if got == nil {
		t.Fatal("murmur3 returned nil")
	}
	s := got.(string)
	if len(s) != 32 {
		t.Errorf("murmur3 length = %d, want 32 (128-bit hex): %q", len(s), s)
	}
	// Deterministic
	got2 := fn([]any{"hello"})
	if got != got2 {
		t.Errorf("murmur3 not deterministic: %v vs %v", got, got2)
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

// ── Date/Time: ISO 8601 ─────────────────────────────────────────────────────

func TestFromISO8601Timestamp(t *testing.T) {
	fn := DefaultRegistry.Lookup("from_iso8601_timestamp")
	tests := []struct {
		input string
		want  int64
	}{
		{"2026-03-15T10:30:00Z", 1773570600000},
		{"2026-03-15T10:30:00+00:00", 1773570600000},
	}
	for _, tt := range tests {
		got := fn([]any{tt.input})
		if got != tt.want {
			t.Errorf("from_iso8601_timestamp(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
	if fn([]any{"not-a-date"}) != nil {
		t.Error("expected nil for invalid date")
	}
}

func TestFromISO8601Date(t *testing.T) {
	fn := DefaultRegistry.Lookup("from_iso8601_date")
	got := fn([]any{"2026-03-15"})
	if got != "2026-03-15" {
		t.Errorf("from_iso8601_date = %v, want 2026-03-15", got)
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestToISO8601(t *testing.T) {
	fn := DefaultRegistry.Lookup("to_iso8601")
	// 2026-03-15T10:30:00Z in millis
	got := fn([]any{int64(1773570600000)})
	expected := "2026-03-15T10:30:00Z"
	if got != expected {
		t.Errorf("to_iso8601 = %v, want %v", got, expected)
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestToMilliseconds(t *testing.T) {
	fn := DefaultRegistry.Lookup("to_milliseconds")
	got := fn([]any{"2026-03-15T10:30:00Z"})
	if got != int64(1773570600000) {
		t.Errorf("to_milliseconds = %v, want 1773570600000", got)
	}
	// Pass-through for int64
	got = fn([]any{int64(12345)})
	if got != int64(12345) {
		t.Errorf("to_milliseconds(int64) = %v, want 12345", got)
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestTimezoneHour(t *testing.T) {
	fn := DefaultRegistry.Lookup("timezone_hour")
	got := fn([]any{int64(0)}) // epoch in UTC
	if got == nil {
		t.Fatal("timezone_hour returned nil")
	}
	// In UTC, should be 0
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestTimezoneMinute(t *testing.T) {
	fn := DefaultRegistry.Lookup("timezone_minute")
	got := fn([]any{int64(0)})
	if got == nil {
		t.Fatal("timezone_minute returned nil")
	}
	if fn([]any{nil}) != nil {
		t.Error("expected nil for nil input")
	}
}

// ── Formatting ───────────────────────────────────────────────────────────────

func TestFormatNumber(t *testing.T) {
	fn := DefaultRegistry.Lookup("format_number")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(1234567)}, "1,234,567"},
		{[]any{float64(1234567.89), int64(2)}, "1,234,567.89"},
		{[]any{float64(-1234.5), int64(1)}, "-1,234.5"},
		{[]any{int64(42)}, "42"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("format_number(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── Registration ─────────────────────────────────────────────────────────────

func TestTrinoGapFunctionsRegistered(t *testing.T) {
	expected := []string{
		"regexp_count", "regexp_extract_all", "regexp_split",
		"split",
		"bitwise_left_shift", "bitwise_right_shift", "bitwise_arithmetic_shift_right",
		"uuid",
		"to_base32", "from_base32", "xxhash64", "murmur3",
		"from_iso8601_timestamp", "from_iso8601_date", "to_iso8601",
		"to_milliseconds", "timezone_hour", "timezone_minute",
		"format_number",
	}
	for _, name := range expected {
		if !DefaultRegistry.Has(name) {
			t.Errorf("function %q not registered", name)
		}
	}
}

func TestTrinoGapFunctionCount(t *testing.T) {
	count := len(DefaultRegistry.Names())
	// 254 (previous) + 19 new = 273
	if count < 273 {
		t.Errorf("total registered functions = %d, want >= 273", count)
	}
}
