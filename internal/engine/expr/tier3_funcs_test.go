package expr

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"
	"strings"
	"testing"
)

// --- String: distance and utility ---

func TestTier3Soundex(t *testing.T) {
	fn := DefaultRegistry.Lookup("soundex")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"Robert"}, "R163"},
		{[]any{"Rupert"}, "R163"},
		{[]any{"Ashcraft"}, "A226"},
		{[]any{"Tymczak"}, "T522"},
		{[]any{"A"}, "A000"},
		{[]any{""}, ""},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("soundex(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestTier3LevenshteinDistance(t *testing.T) {
	fn := DefaultRegistry.Lookup("levenshtein_distance")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"kitten", "sitting"}, float64(3)},
		{[]any{"", "abc"}, float64(3)},
		{[]any{"abc", ""}, float64(3)},
		{[]any{"abc", "abc"}, float64(0)},
		{[]any{"abc", "abd"}, float64(1)},
		{[]any{nil, "abc"}, nil},
		{[]any{"abc", nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("levenshtein_distance(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestTier3HammingDistance(t *testing.T) {
	fn := DefaultRegistry.Lookup("hamming_distance")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"karolin", "kathrin"}, float64(3)},
		{[]any{"abc", "abc"}, float64(0)},
		{[]any{"abc", "xyz"}, float64(3)},
		// Different lengths return nil
		{[]any{"ab", "abc"}, nil},
		{[]any{nil, "abc"}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("hamming_distance(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestTier3Normalize(t *testing.T) {
	fn := DefaultRegistry.Lookup("normalize")
	// Basic printable string passes through
	got := fn([]any{"hello world"})
	if got != "hello world" {
		t.Errorf("normalize('hello world') = %v", got)
	}
	// NULL propagation
	if fn([]any{nil}) != nil {
		t.Error("normalize(NULL) should be NULL")
	}
}

func TestTier3Format(t *testing.T) {
	fn := DefaultRegistry.Lookup("format")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"hello %s", "world"}, "hello world"},
		{[]any{"%d items", 42}, "42 items"},
		{[]any{"%.2f", 3.14159}, "3.14"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("format(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestTier3LcaseUcase(t *testing.T) {
	// lcase and ucase are aliases for lower and upper
	lcase := DefaultRegistry.Lookup("lcase")
	ucase := DefaultRegistry.Lookup("ucase")

	if lcase == nil {
		t.Fatal("lcase function not registered")
	}
	if ucase == nil {
		t.Fatal("ucase function not registered")
	}

	if lcase([]any{"HELLO"}) != "hello" {
		t.Errorf("lcase('HELLO') = %v, want 'hello'", lcase([]any{"HELLO"}))
	}
	if ucase([]any{"hello"}) != "HELLO" {
		t.Errorf("ucase('hello') = %v, want 'HELLO'", ucase([]any{"hello"}))
	}
	if lcase([]any{nil}) != nil {
		t.Error("lcase(NULL) should be NULL")
	}
	if ucase([]any{nil}) != nil {
		t.Error("ucase(NULL) should be NULL")
	}
}

func TestTier3ToUTF8FromUTF8(t *testing.T) {
	toUTF8 := DefaultRegistry.Lookup("to_utf8")
	fromUTF8 := DefaultRegistry.Lookup("from_utf8")

	// to_utf8 returns []byte
	got := toUTF8([]any{"hello"})
	bs, ok := got.([]byte)
	if !ok {
		t.Fatalf("to_utf8('hello') returned %T, want []byte", got)
	}
	if string(bs) != "hello" {
		t.Errorf("to_utf8('hello') = %v, want [104 101 108 108 111]", bs)
	}

	// from_utf8 converts []byte back to string
	roundTrip := fromUTF8([]any{bs})
	if roundTrip != "hello" {
		t.Errorf("from_utf8(to_utf8('hello')) = %v, want 'hello'", roundTrip)
	}

	// from_utf8 on a plain string returns it as-is
	if fromUTF8([]any{"world"}) != "world" {
		t.Error("from_utf8('world') should return 'world'")
	}

	// NULL propagation
	if toUTF8([]any{nil}) != nil {
		t.Error("to_utf8(NULL) should be NULL")
	}
	if fromUTF8([]any{nil}) != nil {
		t.Error("from_utf8(NULL) should be NULL")
	}
}

// --- Math: IEEE 754 and utility ---

func TestTier3E(t *testing.T) {
	fn := DefaultRegistry.Lookup("e")
	got := fn([]any{})
	if got != math.E {
		t.Errorf("e() = %v, want %v", got, math.E)
	}
}

func TestTier3Log10(t *testing.T) {
	fn := DefaultRegistry.Lookup("log10")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{float64(100)}, math.Log10(100)},
		{[]any{float64(1)}, float64(0)},
		{[]any{float64(1000)}, math.Log10(1000)},
		// Negative or zero returns nil
		{[]any{float64(0)}, nil},
		{[]any{float64(-5)}, nil},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if tt.want == nil {
			if got != nil {
				t.Errorf("log10(%v) = %v, want nil", tt.args, got)
			}
			continue
		}
		if math.Abs(got.(float64)-tt.want.(float64)) > 1e-10 {
			t.Errorf("log10(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestTier3Infinity(t *testing.T) {
	fn := DefaultRegistry.Lookup("infinity")
	got := fn([]any{}).(float64)
	if !math.IsInf(got, 1) {
		t.Errorf("infinity() = %v, want +Inf", got)
	}
}

func TestTier3NaN(t *testing.T) {
	fn := DefaultRegistry.Lookup("nan")
	got := fn([]any{}).(float64)
	if !math.IsNaN(got) {
		t.Errorf("nan() = %v, want NaN", got)
	}
}

func TestTier3IsNaN(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_nan")
	if fn([]any{math.NaN()}) != true {
		t.Error("is_nan(NaN) should be true")
	}
	if fn([]any{float64(42)}) != false {
		t.Error("is_nan(42) should be false")
	}
	if fn([]any{nil}) != nil {
		t.Error("is_nan(NULL) should be NULL")
	}
}

func TestTier3IsFinite(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_finite")
	if fn([]any{float64(42)}) != true {
		t.Error("is_finite(42) should be true")
	}
	if fn([]any{math.Inf(1)}) != false {
		t.Error("is_finite(+Inf) should be false")
	}
	if fn([]any{math.Inf(-1)}) != false {
		t.Error("is_finite(-Inf) should be false")
	}
	if fn([]any{math.NaN()}) != false {
		t.Error("is_finite(NaN) should be false")
	}
	if fn([]any{nil}) != nil {
		t.Error("is_finite(NULL) should be NULL")
	}
}

func TestTier3IsInfinite(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_infinite")
	if fn([]any{math.Inf(1)}) != true {
		t.Error("is_infinite(+Inf) should be true")
	}
	if fn([]any{math.Inf(-1)}) != true {
		t.Error("is_infinite(-Inf) should be true")
	}
	if fn([]any{float64(42)}) != false {
		t.Error("is_infinite(42) should be false")
	}
	if fn([]any{nil}) != nil {
		t.Error("is_infinite(NULL) should be NULL")
	}
}

func TestTier3WidthBucket(t *testing.T) {
	fn := DefaultRegistry.Lookup("width_bucket")
	tests := []struct {
		args []any
		want any
	}{
		// 5.0 in range [0, 10) with 5 buckets -> bucket 3
		{[]any{float64(5), float64(0), float64(10), float64(5)}, float64(3)},
		// Below lower bound -> bucket 0
		{[]any{float64(-1), float64(0), float64(10), float64(5)}, float64(0)},
		// At or above upper bound -> n+1
		{[]any{float64(10), float64(0), float64(10), float64(5)}, float64(6)},
		// Edge: first bucket
		{[]any{float64(0), float64(0), float64(10), float64(5)}, float64(1)},
		// Invalid: 0 buckets
		{[]any{float64(5), float64(0), float64(10), float64(0)}, nil},
		// Invalid: equal bounds
		{[]any{float64(5), float64(5), float64(5), float64(4)}, nil},
		{[]any{nil, float64(0), float64(10), float64(5)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("width_bucket(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestTier3FromBaseToBase(t *testing.T) {
	fromBase := DefaultRegistry.Lookup("from_base")
	toBase := DefaultRegistry.Lookup("to_base")

	// Binary
	if fromBase([]any{"1010", float64(2)}) != float64(10) {
		t.Errorf("from_base('1010', 2) = %v, want 10", fromBase([]any{"1010", float64(2)}))
	}
	if toBase([]any{float64(10), float64(2)}) != "1010" {
		t.Errorf("to_base(10, 2) = %v, want '1010'", toBase([]any{float64(10), float64(2)}))
	}

	// Octal
	if fromBase([]any{"17", float64(8)}) != float64(15) {
		t.Errorf("from_base('17', 8) = %v, want 15", fromBase([]any{"17", float64(8)}))
	}
	if toBase([]any{float64(15), float64(8)}) != "17" {
		t.Errorf("to_base(15, 8) = %v, want '17'", toBase([]any{float64(15), float64(8)}))
	}

	// Hexadecimal
	if fromBase([]any{"ff", float64(16)}) != float64(255) {
		t.Errorf("from_base('ff', 16) = %v, want 255", fromBase([]any{"ff", float64(16)}))
	}
	if toBase([]any{float64(255), float64(16)}) != "ff" {
		t.Errorf("to_base(255, 16) = %v, want 'ff'", toBase([]any{float64(255), float64(16)}))
	}

	// Round-trip: base 36
	original := float64(123456)
	encoded := toBase([]any{original, float64(36)})
	decoded := fromBase([]any{encoded, float64(36)})
	if decoded != original {
		t.Errorf("base-36 round-trip: %v -> %v -> %v", original, encoded, decoded)
	}

	// Invalid base
	if fromBase([]any{"10", float64(1)}) != nil {
		t.Error("from_base with base 1 should return nil")
	}
	if toBase([]any{float64(10), float64(37)}) != nil {
		t.Error("to_base with base 37 should return nil")
	}

	// NULL propagation
	if fromBase([]any{nil, float64(10)}) != nil {
		t.Error("from_base(NULL, 10) should be NULL")
	}
	if toBase([]any{nil, float64(10)}) != nil {
		t.Error("to_base(NULL, 10) should be NULL")
	}
}

func TestTier3BitCount(t *testing.T) {
	fn := DefaultRegistry.Lookup("bit_count")
	tests := []struct {
		arg  float64
		want float64
	}{
		{float64(0), float64(0)},
		{float64(1), float64(1)},
		{float64(7), float64(3)},
		{float64(255), float64(8)},
		{float64(1023), float64(10)},
	}
	for _, tt := range tests {
		got := fn([]any{tt.arg})
		expected := float64(bits.OnesCount64(uint64(int64(tt.arg))))
		if got != expected {
			t.Errorf("bit_count(%v) = %v, want %v", tt.arg, got, expected)
		}
	}
	if fn([]any{nil}) != nil {
		t.Error("bit_count(NULL) should be NULL")
	}
}

// --- Hash: additional ---

func TestTier3SHA1(t *testing.T) {
	fn := DefaultRegistry.Lookup("sha1")
	h := sha1.Sum([]byte("hello"))
	want := hex.EncodeToString(h[:])
	got := fn([]any{"hello"})
	if got != want {
		t.Errorf("sha1('hello') = %v, want %v", got, want)
	}
	if fn([]any{nil}) != nil {
		t.Error("sha1(NULL) should be NULL")
	}
}

func TestTier3CRC32(t *testing.T) {
	fn := DefaultRegistry.Lookup("crc32")
	want := float64(crc32.ChecksumIEEE([]byte("hello")))
	got := fn([]any{"hello"})
	if got != want {
		t.Errorf("crc32('hello') = %v, want %v", got, want)
	}
	if fn([]any{nil}) != nil {
		t.Error("crc32(NULL) should be NULL")
	}
}

func TestTier3HMACSHA256(t *testing.T) {
	fn := DefaultRegistry.Lookup("hmac_sha256")
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write([]byte("message"))
	want := hex.EncodeToString(mac.Sum(nil))
	got := fn([]any{"message", "secret"})
	if got != want {
		t.Errorf("hmac_sha256('message', 'secret') = %v, want %v", got, want)
	}
	if fn([]any{nil, "key"}) != nil {
		t.Error("hmac_sha256(NULL, 'key') should be NULL")
	}
	if fn([]any{"data", nil}) != nil {
		t.Error("hmac_sha256('data', NULL) should be NULL")
	}
}

func TestTier3HMACSHA512(t *testing.T) {
	fn := DefaultRegistry.Lookup("hmac_sha512")
	mac := hmac.New(sha512.New, []byte("secret"))
	mac.Write([]byte("message"))
	want := hex.EncodeToString(mac.Sum(nil))
	got := fn([]any{"message", "secret"})
	if got != want {
		t.Errorf("hmac_sha512('message', 'secret') = %v, want %v", got, want)
	}
	if fn([]any{nil, "key"}) != nil {
		t.Error("hmac_sha512(NULL, 'key') should be NULL")
	}
}

// --- Date: additional accessors ---

func TestTier3Quarter(t *testing.T) {
	fn := DefaultRegistry.Lookup("quarter")
	tests := []struct {
		arg  string
		want any
	}{
		{"2026-01-15T00:00:00Z", float64(1)},
		{"2026-03-31T00:00:00Z", float64(1)},
		{"2026-04-01T00:00:00Z", float64(2)},
		{"2026-07-15T00:00:00Z", float64(3)},
		{"2026-10-01T00:00:00Z", float64(4)},
		{"2026-12-31T00:00:00Z", float64(4)},
	}
	for _, tt := range tests {
		got := fn([]any{tt.arg})
		if got != tt.want {
			t.Errorf("quarter(%q) = %v, want %v", tt.arg, got, tt.want)
		}
	}
	if fn([]any{nil}) != nil {
		t.Error("quarter(NULL) should be NULL")
	}
}

func TestTier3Week(t *testing.T) {
	fn := DefaultRegistry.Lookup("week")
	// 2026-01-05 is a Monday in ISO week 2
	got := fn([]any{"2026-01-05T00:00:00Z"})
	if got != float64(2) {
		t.Errorf("week('2026-01-05') = %v, want 2", got)
	}
	if fn([]any{nil}) != nil {
		t.Error("week(NULL) should be NULL")
	}
}

func TestTier3DayOfWeek(t *testing.T) {
	fn := DefaultRegistry.Lookup("day_of_week")
	// 2026-03-16 is a Monday (Go Weekday: 1)
	got := fn([]any{"2026-03-16T00:00:00Z"})
	if got != float64(1) {
		t.Errorf("day_of_week('2026-03-16') = %v, want 1 (Monday)", got)
	}
	// Sunday = 0
	got = fn([]any{"2026-03-15T00:00:00Z"})
	if got != float64(0) {
		t.Errorf("day_of_week('2026-03-15') = %v, want 0 (Sunday)", got)
	}
	if fn([]any{nil}) != nil {
		t.Error("day_of_week(NULL) should be NULL")
	}
}

func TestTier3DayOfYear(t *testing.T) {
	fn := DefaultRegistry.Lookup("day_of_year")
	// January 1 is day 1
	got := fn([]any{"2026-01-01T00:00:00Z"})
	if got != float64(1) {
		t.Errorf("day_of_year('2026-01-01') = %v, want 1", got)
	}
	// December 31 of a non-leap year is day 365
	got = fn([]any{"2026-12-31T00:00:00Z"})
	if got != float64(365) {
		t.Errorf("day_of_year('2026-12-31') = %v, want 365", got)
	}
	if fn([]any{nil}) != nil {
		t.Error("day_of_year(NULL) should be NULL")
	}
}

func TestTier3LastDayOfMonth(t *testing.T) {
	fn := DefaultRegistry.Lookup("last_day_of_month")
	tests := []struct {
		arg  string
		want any
	}{
		{"2026-01-15T00:00:00Z", "2026-01-31"},
		{"2026-02-10T00:00:00Z", "2026-02-28"},
		{"2024-02-10T00:00:00Z", "2024-02-29"}, // leap year
		{"2026-04-20T00:00:00Z", "2026-04-30"},
		{"2026-12-05T00:00:00Z", "2026-12-31"},
	}
	for _, tt := range tests {
		got := fn([]any{tt.arg})
		if got != tt.want {
			t.Errorf("last_day_of_month(%q) = %v, want %v", tt.arg, got, tt.want)
		}
	}
	if fn([]any{nil}) != nil {
		t.Error("last_day_of_month(NULL) should be NULL")
	}
}

func TestTier3CurrentTimestamp(t *testing.T) {
	fn := DefaultRegistry.Lookup("current_timestamp")
	got := fn([]any{})
	if got == nil {
		t.Fatal("current_timestamp() returned nil")
	}
	s, ok := got.(string)
	if !ok {
		t.Fatalf("current_timestamp() returned %T, want string", got)
	}
	// Should be a valid RFC3339 timestamp starting with "20"
	if !strings.HasPrefix(s, "20") {
		t.Errorf("current_timestamp() = %v, expected to start with '20'", s)
	}
}

func TestTier3AtTimezone(t *testing.T) {
	fn := DefaultRegistry.Lookup("at_timezone")
	// Convert UTC to US/Eastern (UTC-5 or UTC-4 depending on DST)
	got := fn([]any{"2026-01-15T12:00:00Z", "America/New_York"})
	if got == nil {
		t.Fatal("at_timezone returned nil")
	}
	s := got.(string)
	// In January, New York is EST (UTC-5), so 12:00 UTC -> 07:00 EST
	if !strings.Contains(s, "07:00:00") {
		t.Errorf("at_timezone('2026-01-15T12:00:00Z', 'America/New_York') = %v, expected 07:00:00", s)
	}

	// Invalid timezone
	if fn([]any{"2026-01-15T12:00:00Z", "Invalid/Zone"}) != nil {
		t.Error("at_timezone with invalid timezone should return nil")
	}

	// NULL propagation
	if fn([]any{nil, "UTC"}) != nil {
		t.Error("at_timezone(NULL, 'UTC') should be NULL")
	}
	if fn([]any{"2026-01-15T12:00:00Z", nil}) != nil {
		t.Error("at_timezone(timestamp, NULL) should be NULL")
	}
}

func TestTier3HumanReadableSeconds(t *testing.T) {
	fn := DefaultRegistry.Lookup("human_readable_seconds")
	tests := []struct {
		arg  float64
		want string
	}{
		{float64(0), "0 seconds"},
		{float64(1), "1 second"},
		{float64(60), "1 minute"},
		{float64(3600), "1 hour"},
		{float64(86400), "1 day"},
		{float64(90061), "1 day, 1 hour, 1 minute, 1 second"},
		{float64(172800), "2 days"},
		{float64(7261), "2 hours, 1 minute, 1 second"},
	}
	for _, tt := range tests {
		got := fn([]any{tt.arg})
		if got != tt.want {
			t.Errorf("human_readable_seconds(%v) = %v, want %v", tt.arg, got, tt.want)
		}
	}
	if fn([]any{nil}) != nil {
		t.Error("human_readable_seconds(NULL) should be NULL")
	}
}

// --- Network: analytics ---

func TestTier3IsPrivateIP(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_private_ip")
	tests := []struct {
		arg  string
		want any
	}{
		{"192.168.1.1", true},
		{"10.0.0.1", true},
		{"172.16.0.1", true},
		{"8.8.8.8", false},
		{"1.1.1.1", false},
	}
	for _, tt := range tests {
		got := fn([]any{tt.arg})
		if got != tt.want {
			t.Errorf("is_private_ip(%q) = %v, want %v", tt.arg, got, tt.want)
		}
	}
	// Invalid IP
	if fn([]any{"not-an-ip"}) != nil {
		t.Error("is_private_ip('not-an-ip') should be nil")
	}
	if fn([]any{nil}) != nil {
		t.Error("is_private_ip(NULL) should be NULL")
	}
}

func TestTier3IsLoopbackIP(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_loopback_ip")
	if fn([]any{"127.0.0.1"}) != true {
		t.Error("is_loopback_ip('127.0.0.1') should be true")
	}
	if fn([]any{"::1"}) != true {
		t.Error("is_loopback_ip('::1') should be true")
	}
	if fn([]any{"192.168.1.1"}) != false {
		t.Error("is_loopback_ip('192.168.1.1') should be false")
	}
	if fn([]any{nil}) != nil {
		t.Error("is_loopback_ip(NULL) should be NULL")
	}
}

func TestTier3IPToIntIntToIP(t *testing.T) {
	ipToInt := DefaultRegistry.Lookup("ip_to_int")
	intToIP := DefaultRegistry.Lookup("int_to_ip")

	// 192.168.1.1 = 192*2^24 + 168*2^16 + 1*2^8 + 1 = 3232235777
	got := ipToInt([]any{"192.168.1.1"})
	want := float64(3232235777)
	if got != want {
		t.Errorf("ip_to_int('192.168.1.1') = %v, want %v", got, want)
	}

	// Reverse
	gotIP := intToIP([]any{want})
	if gotIP != "192.168.1.1" {
		t.Errorf("int_to_ip(%v) = %v, want '192.168.1.1'", want, gotIP)
	}

	// Round-trip with other IPs
	ips := []string{"10.0.0.1", "172.16.0.1", "255.255.255.255", "0.0.0.0"}
	for _, ip := range ips {
		n := ipToInt([]any{ip})
		back := intToIP([]any{n})
		if back != ip {
			t.Errorf("ip round-trip failed: %s -> %v -> %v", ip, n, back)
		}
	}

	// NULL propagation
	if ipToInt([]any{nil}) != nil {
		t.Error("ip_to_int(NULL) should be NULL")
	}
	if intToIP([]any{nil}) != nil {
		t.Error("int_to_ip(NULL) should be NULL")
	}

	// IPv6 not supported by ip_to_int
	if ipToInt([]any{"::1"}) != nil {
		t.Error("ip_to_int('::1') should be nil (IPv6 not supported)")
	}
}

func TestTier3IsIPv4(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_ipv4")
	if fn([]any{"192.168.1.1"}) != true {
		t.Error("is_ipv4('192.168.1.1') should be true")
	}
	if fn([]any{"::1"}) != false {
		t.Error("is_ipv4('::1') should be false")
	}
	if fn([]any{"not-an-ip"}) != false {
		t.Error("is_ipv4('not-an-ip') should be false")
	}
	if fn([]any{nil}) != nil {
		t.Error("is_ipv4(NULL) should be NULL")
	}
}

func TestTier3IsIPv6(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_ipv6")
	if fn([]any{"::1"}) != true {
		t.Error("is_ipv6('::1') should be true")
	}
	if fn([]any{"2001:db8::1"}) != true {
		t.Error("is_ipv6('2001:db8::1') should be true")
	}
	if fn([]any{"192.168.1.1"}) != false {
		t.Error("is_ipv6('192.168.1.1') should be false")
	}
	if fn([]any{"not-an-ip"}) != false {
		t.Error("is_ipv6('not-an-ip') should be false")
	}
	if fn([]any{nil}) != nil {
		t.Error("is_ipv6(NULL) should be NULL")
	}
}

// --- Verify all Tier 3 functions are registered ---

func TestTier3FunctionsRegistered(t *testing.T) {
	expected := []string{
		// String: distance and utility
		"soundex", "levenshtein_distance", "hamming_distance", "normalize",
		"format", "lcase", "ucase", "to_utf8", "from_utf8",
		// Math: IEEE 754 and utility
		"e", "log10", "infinity", "nan", "is_nan", "is_finite", "is_infinite",
		"width_bucket", "from_base", "to_base", "bit_count",
		// Hash: additional
		"sha1", "crc32", "hmac_sha256", "hmac_sha512",
		// Date: additional accessors
		"quarter", "week", "day_of_week", "day_of_year", "last_day_of_month",
		"current_timestamp", "at_timezone", "human_readable_seconds",
		// Network: analytics
		"is_private_ip", "is_loopback_ip", "ip_to_int", "int_to_ip",
		"is_ipv4", "is_ipv6",
	}
	for _, name := range expected {
		if !DefaultRegistry.Has(name) {
			t.Errorf("function %q not registered", name)
		}
	}
}

// --- NULL propagation across all Tier 3 ---

func TestTier3NullPropagation(t *testing.T) {
	// Functions that should return nil when given a single nil argument
	singleArgFns := []string{
		"soundex", "normalize", "format",
		"lcase", "ucase", "to_utf8", "from_utf8",
		"log10", "is_nan", "is_finite", "is_infinite",
		"bit_count",
		"sha1", "crc32",
		"quarter", "week", "day_of_week", "day_of_year", "last_day_of_month",
		"human_readable_seconds",
		"is_private_ip", "is_loopback_ip", "ip_to_int", "int_to_ip",
		"is_ipv4", "is_ipv6",
	}
	for _, name := range singleArgFns {
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

// --- Cross-function property tests ---

func TestTier3BaseRoundTrips(t *testing.T) {
	fromBase := DefaultRegistry.Lookup("from_base")
	toBase := DefaultRegistry.Lookup("to_base")

	// Round-trip across multiple bases
	bases := []float64{2, 8, 10, 16, 36}
	values := []float64{0, 1, 42, 255, 1000, 65535}
	for _, base := range bases {
		for _, v := range values {
			encoded := toBase([]any{v, base})
			decoded := fromBase([]any{encoded, base})
			if decoded != v {
				t.Errorf("base %.0f round-trip failed for %.0f: encoded=%v, decoded=%v",
					base, v, encoded, decoded)
			}
		}
	}
}

func TestTier3IEEE754Consistency(t *testing.T) {
	isNaN := DefaultRegistry.Lookup("is_nan")
	isFinite := DefaultRegistry.Lookup("is_finite")
	isInfinite := DefaultRegistry.Lookup("is_infinite")
	nanFn := DefaultRegistry.Lookup("nan")
	infFn := DefaultRegistry.Lookup("infinity")

	nan := nanFn([]any{})
	inf := infFn([]any{})

	// NaN is not finite and not infinite
	if isNaN([]any{nan}) != true {
		t.Error("is_nan(nan()) should be true")
	}
	if isFinite([]any{nan}) != false {
		t.Error("is_finite(nan()) should be false")
	}
	if isInfinite([]any{nan}) != false {
		t.Error("is_infinite(nan()) should be false")
	}

	// Infinity is not finite and not NaN
	if isInfinite([]any{inf}) != true {
		t.Error("is_infinite(infinity()) should be true")
	}
	if isFinite([]any{inf}) != false {
		t.Error("is_finite(infinity()) should be false")
	}
	if isNaN([]any{inf}) != false {
		t.Error("is_nan(infinity()) should be false")
	}

	// Normal number is finite
	if isFinite([]any{float64(42)}) != true {
		t.Error("is_finite(42) should be true")
	}
	if isNaN([]any{float64(42)}) != false {
		t.Error("is_nan(42) should be false")
	}
	if isInfinite([]any{float64(42)}) != false {
		t.Error("is_infinite(42) should be false")
	}
}

func TestTier3HMACDeterminism(t *testing.T) {
	hmac256 := DefaultRegistry.Lookup("hmac_sha256")
	hmac512 := DefaultRegistry.Lookup("hmac_sha512")

	// Same input produces same output
	r1 := hmac256([]any{"data", "key"})
	r2 := hmac256([]any{"data", "key"})
	if r1 != r2 {
		t.Error("hmac_sha256 should be deterministic")
	}

	r3 := hmac512([]any{"data", "key"})
	r4 := hmac512([]any{"data", "key"})
	if r3 != r4 {
		t.Error("hmac_sha512 should be deterministic")
	}

	// Different keys produce different output
	r5 := hmac256([]any{"data", "key1"})
	r6 := hmac256([]any{"data", "key2"})
	if r5 == r6 {
		t.Error("hmac_sha256 with different keys should produce different output")
	}
}

// Silence the "imported and not used" linter for fmt
var _ = fmt.Sprintf
