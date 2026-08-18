package pgwire

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"time"
)

// --- rendering ---

func TestRenderParamText(t *testing.T) {
	tests := []struct {
		name string
		in   string
		oid  uint32
		want string
	}{
		// The bug this file exists for: a numeric parameter written as a
		// quoted string compares a number to a string and matches nothing.
		{"int4", "2", oidInt4, "2"},
		{"int4 negative", "-7", oidInt4, "-7"},
		{"int8", "9007199254740993", oidInt8, "9007199254740993"},
		{"int2", "300", oidInt2, "300"},
		{"oid", "16384", oidOID, "16384"},
		{"float8", "90.5", oidFloat8, "90.5"},
		{"float4", "1.5", oidFloat4, "1.5"},
		{"float exponent", "1e3", oidFloat8, "1e3"},
		{"numeric", "12345.6789", oidNumeric, "12345.6789"},
		// A value that is not a number does not go out bare, whatever the
		// declared type says.
		{"int4 with junk stays quoted", "2; DROP TABLE users", oidInt4, "'2; DROP TABLE users'"},
		{"int4 empty stays quoted", "", oidInt4, "''"},

		{"bool true", "t", oidBool, "true"},
		{"bool true word", "true", oidBool, "true"},
		{"bool false", "f", oidBool, "false"},
		{"bool false word", "false", oidBool, "false"},
		{"bool one", "1", oidBool, "true"},
		{"bool junk stays quoted", "maybe", oidBool, "'maybe'"},

		{"text", "bob", oidText, "'bob'"},
		{"varchar", "bob", oidVarchar, "'bob'"},
		{"unknown oid", "bob", oidUnknown, "'bob'"},
		{"unknown oid numeric text", "2", oidUnknown, "'2'"},
		{"timestamp", "2026-01-02 03:04:05", oidTimestamp, "'2026-01-02 03:04:05'"},
		{"date", "2026-01-02", oidDate, "'2026-01-02'"},
		{"uuid", "0f8fad5b-d9cb-469f-a165-70867728950e", oidUUID,
			"'0f8fad5b-d9cb-469f-a165-70867728950e'"},

		// Quoting is by doubling, the only escape this lexer reads.
		{"embedded quote", "it's", oidText, "'it''s'"},
		{"quote termination attempt", "'; DROP TABLE users; --", oidText,
			"'''; DROP TABLE users; --'"},
		{"backslash is literal", `back\slash`, oidText, `'back\slash'`},
		{"backslash before quote", `\'`, oidText, `'\'''`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := renderParam([]byte(tt.in), false, tt.oid)
			if err != nil {
				t.Fatalf("renderParam(%q, text, %d): %v", tt.in, tt.oid, err)
			}
			if got != tt.want {
				t.Fatalf("renderParam(%q, text, %d) = %s, want %s", tt.in, tt.oid, got, tt.want)
			}
		})
	}
}

func TestRenderParamBinary(t *testing.T) {
	be16 := func(v int16) []byte { return binary.BigEndian.AppendUint16(nil, uint16(v)) }
	be32 := func(v int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }
	be64 := func(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

	tests := []struct {
		name string
		in   []byte
		oid  uint32
		want string
	}{
		{"int2", be16(300), oidInt2, "300"},
		{"int2 negative", be16(-300), oidInt2, "-300"},
		{"int4", be32(2), oidInt4, "2"},
		{"int4 negative", be32(-2147483648), oidInt4, "-2147483648"},
		{"int8", be64(9007199254740993), oidInt8, "9007199254740993"},
		{"int8 min", be64(math.MinInt64), oidInt8, "-9223372036854775808"},
		{"oid", be32(-1), oidOID, "4294967295"}, // oid is unsigned
		{"float8", be64(int64(math.Float64bits(90.5))), oidFloat8, "90.5"},
		{"float4", be32(int32(math.Float32bits(1.5))), oidFloat4, "1.5"},
		{"bool true", []byte{1}, oidBool, "true"},
		{"bool false", []byte{0}, oidBool, "false"},
		// Binary date/time count from 2000-01-01 UTC, days for date and
		// microseconds for timestamp.
		{"date", be32(9498), oidDate, "'2026-01-02'"},
		{"timestamp", be64(820638245000000), oidTimestamp, "'2026-01-02T03:04:05Z'"},
		{"timestamptz", be64(820638245000000), oidTimestampTZ, "'2026-01-02T03:04:05Z'"},
		{"uuid", []byte{
			0x0f, 0x8f, 0xad, 0x5b, 0xd9, 0xcb, 0x46, 0x9f,
			0xa1, 0x65, 0x70, 0x86, 0x77, 0x28, 0x95, 0x0e,
		}, oidUUID, "'0f8fad5b-d9cb-469f-a165-70867728950e'"},
		{"bytea", []byte{0xde, 0xad, 0xbe, 0xef}, oidBytea, `'\xdeadbeef'`},
		// Binary text is the same bytes as text text.
		{"text", []byte("it's"), oidText, "'it''s'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := renderParam(tt.in, true, tt.oid)
			if err != nil {
				t.Fatalf("renderParam(%x, binary, %d): %v", tt.in, tt.oid, err)
			}
			if got != tt.want {
				t.Fatalf("renderParam(%x, binary, %d) = %s, want %s", tt.in, tt.oid, got, tt.want)
			}
		})
	}
}

// TestRenderParamBinaryRejected covers the cases where the bytes cannot be
// read. Refusing is the point: quoting bytes whose meaning is unknown is how
// a silent wrong answer starts.
func TestRenderParamBinaryRejected(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		oid  uint32
	}{
		{"unknown oid", []byte{0, 0, 0, 2}, oidUnknown},
		{"unsupported oid", []byte{0, 0, 0, 2}, 3802}, // jsonb
		{"int4 wrong width", []byte{0, 2}, oidInt4},
		{"int8 wrong width", []byte{0, 0, 0, 2}, oidInt8},
		{"bool wrong width", []byte{0, 0}, oidBool},
		{"uuid wrong width", []byte{1, 2, 3}, oidUUID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := renderParam(tt.in, true, tt.oid); err == nil {
				t.Fatalf("renderParam(%x, binary, %d) = %s, want an error", tt.in, tt.oid, got)
			}
		})
	}
}

// TestRenderParamFloatSpecials keeps Inf and NaN, which have no unquoted SQL
// spelling, from being spliced in bare.
func TestRenderParamFloatSpecials(t *testing.T) {
	for _, v := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		raw := binary.BigEndian.AppendUint64(nil, math.Float64bits(v))
		got, err := renderParam(raw, true, oidFloat8)
		if err != nil {
			t.Fatalf("renderParam(%v): %v", v, err)
		}
		if !strings.HasPrefix(got, "'") {
			t.Fatalf("renderParam(%v) = %s, want a quoted literal", v, got)
		}
	}
}

// --- placeholder scanning and substitution ---

func TestSubstituteParams(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		literals []string
		want     string
	}{
		{
			name:     "single",
			sql:      "SELECT * FROM t WHERE a = $1",
			literals: []string{"2"},
			want:     "SELECT * FROM t WHERE a = 2",
		},
		{
			name:     "two in order",
			sql:      "SELECT * FROM t WHERE a = $1 AND b = $2",
			literals: []string{"2", "'bob'"},
			want:     "SELECT * FROM t WHERE a = 2 AND b = 'bob'",
		},
		{
			name:     "out of order",
			sql:      "SELECT * FROM t WHERE a = $2 AND b = $1",
			literals: []string{"'bob'", "2"},
			want:     "SELECT * FROM t WHERE a = 2 AND b = 'bob'",
		},
		{
			name:     "repeated placeholder",
			sql:      "SELECT * FROM t WHERE a = $1 OR b = $1",
			literals: []string{"2"},
			want:     "SELECT * FROM t WHERE a = 2 OR b = 2",
		},
		{
			// A per-parameter strings.Replace matched the "$1" inside "$10"
			// and rewrote it to `<value>0`.
			name: "ten parameters",
			sql:  "SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11 FROM t",
			literals: []string{
				"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
			},
			want: "SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 FROM t",
		},
		{
			name:     "placeholder inside a string literal is text",
			sql:      "SELECT * FROM t WHERE a = $1 AND b = 'costs $1 each'",
			literals: []string{"2"},
			want:     "SELECT * FROM t WHERE a = 2 AND b = 'costs $1 each'",
		},
		{
			name:     "doubled quote does not end the literal",
			sql:      "SELECT * FROM t WHERE b = 'it''s $1' AND a = $1",
			literals: []string{"2"},
			want:     "SELECT * FROM t WHERE b = 'it''s $1' AND a = 2",
		},
		{
			name:     "placeholder inside a quoted identifier is a name",
			sql:      `SELECT "col $1" FROM t WHERE a = $1`,
			literals: []string{"2"},
			want:     `SELECT "col $1" FROM t WHERE a = 2`,
		},
		{
			name:     "value containing a placeholder is not rescanned",
			sql:      "SELECT * FROM t WHERE a = $1 AND b = $2",
			literals: []string{"'$2'", "'x'"},
			want:     "SELECT * FROM t WHERE a = '$2' AND b = 'x'",
		},
		{
			name:     "no placeholders",
			sql:      "SELECT 1 FROM t",
			literals: nil,
			want:     "SELECT 1 FROM t",
		},
		{
			name:     "more placeholders than parameters is left to the parser",
			sql:      "SELECT * FROM t WHERE a = $1 AND b = $2",
			literals: []string{"2"},
			want:     "SELECT * FROM t WHERE a = 2 AND b = $2",
		},
		{
			name:     "lone dollar is not a placeholder",
			sql:      "SELECT * FROM t WHERE a = $ AND b = $1",
			literals: []string{"2"},
			want:     "SELECT * FROM t WHERE a = $ AND b = 2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := substituteParams(tt.sql, tt.literals); got != tt.want {
				t.Fatalf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestCountParamPlaceholders(t *testing.T) {
	tests := []struct {
		sql  string
		want int
	}{
		{"SELECT 1 FROM t", 0},
		{"SELECT * FROM t WHERE a = $1", 1},
		{"SELECT * FROM t WHERE a = $1 AND b = $2", 2},
		{"SELECT * FROM t WHERE a = $1 OR b = $1", 1},
		{"SELECT * FROM t WHERE a = $2", 2},
		{"SELECT $1, $10 FROM t", 10},
		{"SELECT * FROM t WHERE b = 'costs $9 each'", 0},
		{`SELECT "col $9" FROM t`, 0},
		{"SELECT * FROM t WHERE a = $", 0},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if got := countParamPlaceholders(tt.sql); got != tt.want {
				t.Fatalf("countParamPlaceholders(%q) = %d, want %d", tt.sql, got, tt.want)
			}
		})
	}
}

// TestSubstituteNullParams covers Describe's stand-in substitution, which now
// shares the placeholder scanner and so also leaves literals alone.
func TestSubstituteNullParams(t *testing.T) {
	tests := []struct {
		sql       string
		want      string
		wantFound bool
	}{
		{"SELECT 1 FROM t", "SELECT 1 FROM t", false},
		{"SELECT * FROM t WHERE a = $1", "SELECT * FROM t WHERE a = NULL", true},
		{
			"SELECT * FROM t WHERE a = $1 AND b = $2",
			"SELECT * FROM t WHERE a = NULL AND b = NULL",
			true,
		},
		{
			"SELECT * FROM t WHERE b = 'costs $1'",
			"SELECT * FROM t WHERE b = 'costs $1'",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got, found := substituteNullParams(tt.sql)
			if got != tt.want || found != tt.wantFound {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, found, tt.want, tt.wantFound)
			}
		})
	}
}

// TestPgEpochConstants pins the two binary date/time origins against values
// computed independently, so a wrong epoch cannot pass the table above by
// agreeing with itself.
func TestPgEpochConstants(t *testing.T) {
	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	micros := want.Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Microseconds()
	if micros != 820638245000000 {
		t.Fatalf("timestamp micros = %d, want 820638245000000", micros)
	}
	days := int32(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC).
		Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24)
	if days != 9498 {
		t.Fatalf("date days = %d, want 9498", days)
	}
}
