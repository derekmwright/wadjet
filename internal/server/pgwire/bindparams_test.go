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
		// FIX 5: ParseFloat("1e400") fails with strconv.ErrRange — the
		// grammar accepted it, only float64's exponent range (overflow to
		// +/-Inf) could not hold it — and used to fall all the way through
		// to quoteLiteral, writing a numeric-shaped string as a quoted TEXT
		// literal and comparing a DECIMAL column to text. wadjet's DECIMAL
		// is not bound to float64, so the text still splices bare. (Go's
		// ParseFloat treats UNDERFLOW differently — "1e-400" returns 0,
		// nil, no ErrRange — so only the overflow direction exercises this
		// path.)
		{"numeric past float64 range", "1e400", oidNumeric, "1e400"},
		{"numeric past float64 range negative", "-1e400", oidNumeric, "-1e400"},
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

		// bytea in TEXT format: byteain's two spellings, both denoting the
		// same BYTES. The literal carries the value's bytes, not the
		// spelling — `\x6869` is a two-byte value, and quoting the six
		// characters compares a string against a BYTES column (#570).
		//
		// It is written back in byteain's HEX form since #582. The engine
		// reads a literal beside a BYTES column through byteain too now, so
		// raw bytes would be decoded a SECOND time: the last cell is the one
		// that says so — under the old splice `a\b` went out as `'a\b'`, a
		// lone backslash, which byteain refuses. The hex form round-trips
		// whatever the bytes are.
		{"bytea hex", `\x6869`, oidBytea, `'\x6869'`},
		{"bytea hex empty", `\x`, oidBytea, `'\x'`},
		{"bytea hex uppercase digits", `\x4869`, oidBytea, `'\x4869'`},
		{"bytea escape form", "hi", oidBytea, `'\x6869'`},
		{"bytea escape octal", `\150\151`, oidBytea, `'\x6869'`},
		{"bytea escape doubled backslash", `a\\b`, oidBytea, `'\x615c62'`},

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
		// bytea's binary form IS the value's bytes, and the literal has to
		// carry those bytes — not their `\x` SPELLING, which is a
		// ten-character STRING that matches nothing against a BYTES column
		// (#570). Every byte here survives the lexer verbatim: lexString
		// slices the input rather than re-encoding runes, so 0xde does not
		// become U+FFFD on the way back in.
		{"bytea", []byte{0xde, 0xad, 0xbe, 0xef}, oidBytea, "'" + string([]byte{0xde, 0xad, 0xbe, 0xef}) + "'"},
		{"bytea with a quote and a NUL", []byte{'a', '\'', 0x00}, oidBytea, "'a''" + string([]byte{0x00}) + "'"},
		{"bytea empty", []byte{}, oidBytea, "''"},
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

// TestRenderBinaryNumericRoundTripsThroughAppendBinaryNumeric feeds
// appendBinaryNumeric's own encoding (server.go, the send side of the SAME
// wire format) into renderBinaryNumeric and checks the decoded literal is
// byte-identical to the source text. The two encoders were written
// independently and neither calls the other, so agreement here is a real
// cross-check of the header arithmetic, not a restatement.
//
// This is also the #464 regression: before the fix, oidNumeric fell into the
// text-bytes arm and this whole list decoded as garbage.
func TestRenderBinaryNumericRoundTripsThroughAppendBinaryNumeric(t *testing.T) {
	for _, s := range []string{
		"0.0", "0.00000", "1.0", "-1.0", "25.0", "-25.0",
		"0.5", "-0.5", "12.75", "-20.0", "3.1875",
		"0.0001", "-0.0001", "1234.5678", "12345.6", "99999999.99",
		// Past float64's exact range and past int64 entirely — the reason a
		// DECIMAL column exists, and the review's "wide 25-digit values".
		"9346828825.8671214869", "-9346828825.8671214869",
		"1234567890123456789012345.6789012345",
		"-1234567890123456789012345.6789012345",
		"9999999999999999999999999999.9999999999",
		"-9999999999999999999999999999.9999999999",
		// No fraction at all.
		"42", "-42", "0",
		// Trailing-zero dscale: the value is round, but the display scale
		// still names the digits after the point.
		"100.00", "0.00", "5.500",
	} {
		t.Run(s, func(t *testing.T) {
			buf := appendBinaryNumeric(nil, s)
			if len(buf) < 4 {
				t.Fatalf("appendBinaryNumeric(%q) = %d bytes", s, len(buf))
			}
			n := int32(uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3]))
			if n < 0 {
				t.Fatalf("appendBinaryNumeric(%q) encoded as NULL", s)
			}
			body := buf[4:]
			if int(n) != len(body) {
				t.Fatalf("declared length %d, body %d bytes", n, len(body))
			}

			got, err := renderBinaryNumeric(body)
			if err != nil {
				t.Fatalf("renderBinaryNumeric: %v", err)
			}
			if got != s {
				t.Fatalf("renderBinaryNumeric round trip = %q, want %q", got, s)
			}
		})
	}
}

// TestRenderBinaryNumericHeaderShapes pins the digit-group arithmetic
// directly against hand-built wire bytes, independent of appendBinaryNumeric,
// so a defect shared by both encoders could not hide behind their agreement.
func TestRenderBinaryNumericHeaderShapes(t *testing.T) {
	be16 := func(v int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(int16(v))) }
	header := func(ndigits, weight int, sign uint16, dscale int, digits ...int) []byte {
		var b []byte
		b = append(b, be16(ndigits)...)
		b = append(b, be16(weight)...)
		b = append(b, be16(int(sign))...)
		b = append(b, be16(dscale)...)
		for _, d := range digits {
			b = append(b, be16(d)...)
		}
		return b
	}

	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{"12345.6", header(3, 1, pgNumericSignPositive, 1, 1, 2345, 6000), "12345.6"},
		{"0.5", header(1, -1, pgNumericSignPositive, 1, 5000), "0.5"},
		{"-25.0", header(1, 0, pgNumericSignNegative, 1, 25), "-25.0"},
		{"zero with display scale", header(0, 0, pgNumericSignPositive, 2), "0.00"},
		{"zero no scale", header(0, 0, pgNumericSignPositive, 0), "0"},
		// Trimmed leading zero groups: 0.0001 stores one digit at weight -1,
		// not a zero group at weight 0 followed by the real one.
		{"0.0001", header(1, -1, pgNumericSignPositive, 4, 1), "0.0001"},
		// A gap between the last real digit group and dscale — the
		// trailing-zero-group case appendBinaryNumeric trims on send.
		{"100.00", header(1, 0, pgNumericSignPositive, 2, 100), "100.00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := renderBinaryNumeric(tt.raw)
			if err != nil {
				t.Fatalf("renderBinaryNumeric: %v", err)
			}
			if got != tt.want {
				t.Fatalf("renderBinaryNumeric(%s) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

// TestRenderBinaryNumericSpecialValues covers the values PostgreSQL's binary
// numeric format can carry that wadjet's DECIMAL cannot: NaN and the two
// infinities. Each must fail with an error naming the reason, never a value.
func TestRenderBinaryNumericSpecialValues(t *testing.T) {
	be16 := func(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }
	header := func(sign uint16) []byte {
		var b []byte
		b = append(b, be16(0)...) // ndigits
		b = append(b, be16(0)...) // weight
		b = append(b, be16(sign)...)
		b = append(b, be16(0)...) // dscale
		return b
	}

	tests := []struct {
		name    string
		raw     []byte
		wantErr string
	}{
		{"NaN", header(pgNumericSignNaN), "NaN"},
		{"positive infinity", header(pgNumericSignPosInf), "infinite"},
		{"negative infinity", header(pgNumericSignNegInf), "infinite"},
		{"unrecognized sign", header(0x1234), "sign"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := renderBinaryNumeric(tt.raw)
			if err == nil {
				t.Fatalf("renderBinaryNumeric(%s) = %q, want an error", tt.name, got)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("renderBinaryNumeric(%s) error = %q, want it to mention %q", tt.name, err, tt.wantErr)
			}
		})
	}
}

// TestRenderBinaryNumericMalformed covers bytes that cannot be a valid
// numeric under any interpretation — a short header, a declared length that
// disagrees with the digit count, and an out-of-range digit. Each must
// refuse rather than read past the field or wrap around into a wrong number.
func TestRenderBinaryNumericMalformed(t *testing.T) {
	be16 := func(v int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(int16(v))) }
	tests := []struct {
		name string
		raw  []byte
	}{
		{"too short", []byte{0, 1, 0, 0, 0, 0}},
		{"declared digit count too high", func() []byte {
			var b []byte
			b = append(b, be16(2)...)
			b = append(b, be16(0)...)
			b = append(b, be16(0)...)
			b = append(b, be16(0)...)
			b = append(b, be16(5)...) // only one digit present, not two
			return b
		}()},
		{"digit out of range", func() []byte {
			var b []byte
			b = append(b, be16(1)...)
			b = append(b, be16(0)...)
			b = append(b, be16(0)...)
			b = append(b, be16(0)...)
			b = append(b, be16(10000)...) // base-10000 digit must be 0-9999
			return b
		}()},
		// ndigits' high bit set (0xFFFF here) used to read as int16 -1 and
		// fail the (now-removed) "negative digit count" check directly. It
		// is unsigned now (FIX 1, see TestRenderBinaryNumericHighNdigits
		// below for the legitimate case this used to wrongly reject), so
		// this must still fail — but on the length check: ndigits=65535
		// declares 65535 digit groups and only the 8-byte header is here.
		{"declared ndigits with no digit bytes at all", func() []byte {
			var b []byte
			b = append(b, be16(-1)...) // 0xFFFF -> ndigits 65535 unsigned
			b = append(b, be16(0)...)
			b = append(b, be16(0)...)
			b = append(b, be16(0)...)
			return b
		}()},
		// dscale past NUMERIC_DSCALE_MASK (FIX 2): PostgreSQL's own bound is
		// 16383 digits after the decimal point; 16384 is one past it.
		{"dscale past 16383", func() []byte {
			var b []byte
			b = append(b, be16(0)...) // ndigits
			b = append(b, be16(0)...) // weight
			b = append(b, be16(int(pgNumericSignPositive))...)
			b = append(b, be16(16384)...) // dscale
			return b
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := renderBinaryNumeric(tt.raw); err == nil {
				t.Fatalf("renderBinaryNumeric(%s) = %q, want an error", tt.name, got)
			}
		})
	}
}

// TestRenderBinaryNumericHighNdigits pins FIX 1: ndigits is UNSIGNED on the
// wire (numeric_recv reads it with pq_getmsgint(..., uint16)), so a value
// with the high bit set is not a negative digit count, it's a legitimate
// large one. (1e131071+1)::numeric — a number at PostgreSQL's own documented
// maximum of 131072 digits before the decimal point — sends ndigits=32768,
// which the old signed read decoded as int16(-32768) and rejected outright
// as "negative digit count -32768". This constructs that exact shape: 32768
// real digit groups (weight=32767, so every position 0..weight has an
// actual wire-supplied digit — nothing implicit) and checks it decodes
// clean, not that it merely avoids erroring.
func TestRenderBinaryNumericHighNdigits(t *testing.T) {
	const ndigits = 32768
	const weight = 32767 // ndigits-1: every position is a real digit, no implicit padding
	raw := make([]byte, 8+2*ndigits)
	binary.BigEndian.PutUint16(raw[0:2], uint16(ndigits))
	binary.BigEndian.PutUint16(raw[2:4], uint16(int16(weight)))
	binary.BigEndian.PutUint16(raw[4:6], pgNumericSignPositive)
	binary.BigEndian.PutUint16(raw[6:8], 0) // dscale
	// digits[0] = 1, the rest 0: the value is 1 followed by `weight` more
	// base-10000 zero groups, i.e. 10^131071 — (1e131071+1)::numeric's
	// integer part exactly (its "+1" only affects the last digit group,
	// irrelevant to what this test is checking).
	binary.BigEndian.PutUint16(raw[8:10], 1)

	got, err := renderBinaryNumeric(raw)
	if err != nil {
		t.Fatalf("renderBinaryNumeric with ndigits=%d (0x%04x, negative as int16): %v", ndigits, uint16(ndigits), err)
	}
	const wantLen = 4 * ndigits // (weight+1) groups, 4 digits each, minus the 3 the first group's FormatInt omits... see below
	if len(got) != wantLen-3 {
		t.Fatalf("renderBinaryNumeric with ndigits=%d decoded to %d characters, want %d (131072 digits: '1' then %d zero groups)",
			ndigits, len(got), wantLen-3, weight)
	}
	if got[0] != '1' {
		t.Fatalf("renderBinaryNumeric with ndigits=%d = %q..., want it to start with 1", ndigits, got[:1])
	}
	for i := 1; i < len(got); i++ {
		if got[i] != '0' {
			t.Fatalf("renderBinaryNumeric with ndigits=%d: byte %d = %q, want '0'", ndigits, i, got[i])
		}
	}
}

// TestRenderBinaryNumericDscaleBoundary pins the FIX 2 edge: 16383
// (NUMERIC_DSCALE_MASK) accepted, 16384 rejected — the boundary
// TestRenderBinaryNumericMalformed's "dscale past 16383" case is one side
// of.
func TestRenderBinaryNumericDscaleBoundary(t *testing.T) {
	be16 := func(v int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(int16(v))) }
	header := func(dscale int) []byte {
		var b []byte
		b = append(b, be16(0)...) // ndigits
		b = append(b, be16(0)...) // weight
		b = append(b, be16(int(pgNumericSignPositive))...)
		b = append(b, be16(dscale)...)
		return b
	}
	if got, err := renderBinaryNumeric(header(16383)); err != nil {
		t.Errorf("renderBinaryNumeric with dscale=16383 (NUMERIC_DSCALE_MASK, the legal max): %v", err)
	} else if got != "0."+strings.Repeat("0", 16383) {
		t.Errorf("renderBinaryNumeric with dscale=16383 = %q (%d chars), want %d fraction digits", got, len(got), 16383)
	}
	if _, err := renderBinaryNumeric(header(16384)); err == nil {
		t.Error("renderBinaryNumeric with dscale=16384 (one past NUMERIC_DSCALE_MASK) succeeded, want an error")
	}
}

// TestRenderBinaryNumericResourceExhaustionShape re-runs the adversarial
// review's fuzz shape: a numeric parameter built to amplify a small wire
// payload into a large substituted-SQL string (weight=32767 maximizes the
// integer part, dscale at its now-capped legal max maximizes the fraction
// part), decoded repeatedly to confirm FIX 1 + FIX 2 together bound the
// output — no panic, no unbounded growth — rather than merely not
// crashing once. Bounded well under the review's 60-90s budget: this is CPU
// work on ~150KB of output per call, not I/O.
func TestRenderBinaryNumericResourceExhaustionShape(t *testing.T) {
	const weight = 32767                    // legal max: PG_INT16_MAX, matches NUMERIC_WEIGHT_MAX
	const dscale = 0x3FFF                   // legal max: NUMERIC_DSCALE_MASK
	const maxWant = 1 + 131072 + 1 + dscale // sign + integer part + '.' + fraction part

	raw := make([]byte, 8+2) // ndigits=1: minimal wire cost for the amplification
	binary.BigEndian.PutUint16(raw[0:2], 1)
	binary.BigEndian.PutUint16(raw[2:4], uint16(int16(weight)))
	binary.BigEndian.PutUint16(raw[4:6], pgNumericSignPositive)
	binary.BigEndian.PutUint16(raw[6:8], dscale)
	binary.BigEndian.PutUint16(raw[8:10], 1) // one real digit; everything else implicit zero

	// A few thousand decodes — not the reviewer's literal 65535-parameter
	// Bind message, which is a wire-parsing/allocation cost this function
	// does not pay (it decodes bytes already read off the wire); what this
	// function owns is the per-call string-building cost, which this loop
	// exercises repeatedly to catch anything superlinear in it.
	const iterations = 4000
	for i := 0; i < iterations; i++ {
		got, err := renderBinaryNumeric(raw)
		if err != nil {
			t.Fatalf("iteration %d: renderBinaryNumeric: %v", i, err)
		}
		if len(got) > maxWant {
			t.Fatalf("iteration %d: renderBinaryNumeric produced %d characters, want <= %d (PostgreSQL's own "+
				"131072-integer-digit / 16383-fraction-digit maximum)", i, len(got), maxWant)
		}
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
