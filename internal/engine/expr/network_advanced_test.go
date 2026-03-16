package expr

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
)

// ── ICMP Functions ──────────────────────────────────────────────────────────

func TestNetAdvICMPTypeName(t *testing.T) {
	fn := DefaultRegistry.Lookup("icmp_type_name")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(8)}, "Echo Request"},
		{[]any{int64(0)}, "Echo Reply"},
		{[]any{int64(3)}, "Destination Unreachable"},
		{[]any{int64(11)}, "Time Exceeded"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("icmp_type_name(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvICMPCodeName(t *testing.T) {
	fn := DefaultRegistry.Lookup("icmp_code_name")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(3), int64(3)}, "Port Unreachable"},
		{[]any{int64(3), int64(1)}, "Host Unreachable"},
		{[]any{int64(11), int64(0)}, "TTL Exceeded in Transit"},
		{[]any{nil, int64(0)}, nil},
		{[]any{int64(3), nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("icmp_code_name(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvIsICMPEcho(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_icmp_echo")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(8)}, true},
		{[]any{int64(0)}, true},
		{[]any{int64(3)}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_icmp_echo(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvICMPParse(t *testing.T) {
	fn := DefaultRegistry.Lookup("icmp_parse")
	// hex "0800" = bytes [0x08, 0x00] → type 8, code 0
	data := hexBytes("0800")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{data}, "8:0"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("icmp_parse(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvICMPType(t *testing.T) {
	fn := DefaultRegistry.Lookup("icmp_type")
	data := hexBytes("0800")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{data}, int64(8)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("icmp_type(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvICMPCode(t *testing.T) {
	fn := DefaultRegistry.Lookup("icmp_code")
	data := hexBytes("0301")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{data}, int64(1)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("icmp_code(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── IPv6 Functions ──────────────────────────────────────────────────────────

func TestNetAdvIPv6Scope(t *testing.T) {
	fn := DefaultRegistry.Lookup("ipv6_scope")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"::1"}, "loopback"},
		{[]any{"fe80::1"}, "link-local"},
		{[]any{"2001:db8::1"}, "global"},
		{[]any{"fc00::1"}, "unique-local"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ipv6_scope(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvIPv6Expand(t *testing.T) {
	fn := DefaultRegistry.Lookup("ipv6_expand")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"::1"}, "0000:0000:0000:0000:0000:0000:0000:0001"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ipv6_expand(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvIPv6Compress(t *testing.T) {
	fn := DefaultRegistry.Lookup("ipv6_compress")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"0000:0000:0000:0000:0000:0000:0000:0001"}, "::1"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ipv6_compress(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvIPv6ToEUI64(t *testing.T) {
	fn := DefaultRegistry.Lookup("ipv6_to_eui64")
	// MAC 00:11:22:33:44:55 → flip bit 1 of byte 0: 0x00 ^ 0x02 = 0x02
	// Insert FF:FE in middle: 02:11:22:FF:FE:33:44:55
	// Formatted: 0211:22ff:fe33:4455
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"00:11:22:33:44:55"}, "0211:22ff:fe33:4455"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ipv6_to_eui64(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvIs6to4(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_6to4")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"2002:c0a8:0101::1"}, true},
		{[]any{"2001:db8::1"}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_6to4(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvIsTeredo(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_teredo")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"2001:0000:4136:e378:8000:63bf:3fff:fdd2"}, true},
		{[]any{"2001:db8::1"}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_teredo(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvTeredoServer(t *testing.T) {
	fn := DefaultRegistry.Lookup("teredo_server")
	// 2001:0000:4136:e378:... → bytes 4-7 = 0x41,0x36,0xe3,0x78 = 65.54.227.120
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"2001:0000:4136:e378:8000:63bf:3fff:fdd2"}, "65.54.227.120"},
		{[]any{"2001:db8::1"}, nil}, // not a Teredo address
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("teredo_server(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvTeredoClient(t *testing.T) {
	fn := DefaultRegistry.Lookup("teredo_client")
	// Last 4 bytes: 3f,ff,fd,d2 XOR 0xFF each = c0,00,02,2d = 192.0.2.45
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"2001:0000:4136:e378:8000:63bf:3fff:fdd2"}, "192.0.2.45"},
		{[]any{"2001:db8::1"}, nil},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("teredo_client(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvSixto4Gateway(t *testing.T) {
	fn := DefaultRegistry.Lookup("sixto4_gateway")
	// 2002:c0a8:0101::1 → bytes 2-5 = 0xc0,0xa8,0x01,0x01 = 192.168.1.1
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"2002:c0a8:0101::1"}, "192.168.1.1"},
		{[]any{"2001:db8::1"}, nil}, // not a 6to4 address
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("sixto4_gateway(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── JA3 TLS Fingerprinting ─────────────────────────────────────────────────

func buildTestClientHello() []byte {
	// Build extensions
	var exts bytes.Buffer

	// SNI extension (type 0)
	binary.Write(&exts, binary.BigEndian, uint16(0))    // extension type: SNI
	sniName := []byte("test.example.com")
	sniListLen := 1 + 2 + len(sniName)
	binary.Write(&exts, binary.BigEndian, uint16(2+sniListLen)) // ext data len
	binary.Write(&exts, binary.BigEndian, uint16(sniListLen))   // SNI list len
	exts.WriteByte(0)                                            // host name type
	binary.Write(&exts, binary.BigEndian, uint16(len(sniName))) // name length
	exts.Write(sniName)

	// Supported Groups extension (type 10) with curves 0x0017, 0x0018
	binary.Write(&exts, binary.BigEndian, uint16(10))  // extension type
	binary.Write(&exts, binary.BigEndian, uint16(6))   // extension data length: 2 (list len) + 4 (2 curves)
	binary.Write(&exts, binary.BigEndian, uint16(4))   // named curve list length
	binary.Write(&exts, binary.BigEndian, uint16(0x0017)) // secp256r1
	binary.Write(&exts, binary.BigEndian, uint16(0x0018)) // secp384r1

	// EC Point Formats extension (type 11) with format 0
	binary.Write(&exts, binary.BigEndian, uint16(11))  // extension type
	binary.Write(&exts, binary.BigEndian, uint16(2))   // extension data length
	exts.WriteByte(1)                                   // format list length
	exts.WriteByte(0)                                   // uncompressed

	// Build ClientHello body
	var ch bytes.Buffer
	binary.Write(&ch, binary.BigEndian, uint16(0x0303)) // version TLS 1.2
	ch.Write(make([]byte, 32))                           // random
	ch.WriteByte(0)                                      // session_id_len = 0
	binary.Write(&ch, binary.BigEndian, uint16(4))       // cipher_suites_len = 4 (2 suites)
	binary.Write(&ch, binary.BigEndian, uint16(0xC02C))  // TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
	binary.Write(&ch, binary.BigEndian, uint16(0xC02B))  // TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
	ch.WriteByte(1)                                      // compression methods length = 1
	ch.WriteByte(0)                                      // null compression
	binary.Write(&ch, binary.BigEndian, uint16(exts.Len())) // extensions length
	ch.Write(exts.Bytes())

	// Handshake header
	var hs bytes.Buffer
	hs.WriteByte(1) // ClientHello type
	chLen := ch.Len()
	hs.WriteByte(byte(chLen >> 16))
	hs.WriteByte(byte(chLen >> 8))
	hs.WriteByte(byte(chLen))
	hs.Write(ch.Bytes())

	// TLS record
	var rec bytes.Buffer
	rec.WriteByte(22)                                      // Handshake content type
	binary.Write(&rec, binary.BigEndian, uint16(0x0301))   // TLS 1.0 record version
	binary.Write(&rec, binary.BigEndian, uint16(hs.Len())) // record length
	rec.Write(hs.Bytes())

	return rec.Bytes()
}

func buildTestServerHello() []byte {
	// Build ServerHello body
	var sh bytes.Buffer
	binary.Write(&sh, binary.BigEndian, uint16(0x0303)) // version TLS 1.2
	sh.Write(make([]byte, 32))                           // random
	sh.WriteByte(0)                                      // session_id_len = 0
	binary.Write(&sh, binary.BigEndian, uint16(0xC02C))  // selected cipher
	sh.WriteByte(0)                                      // compression = null
	// No extensions

	// Handshake header
	var hs bytes.Buffer
	hs.WriteByte(2) // ServerHello type
	shLen := sh.Len()
	hs.WriteByte(byte(shLen >> 16))
	hs.WriteByte(byte(shLen >> 8))
	hs.WriteByte(byte(shLen))
	hs.Write(sh.Bytes())

	// TLS record
	var rec bytes.Buffer
	rec.WriteByte(22)                                      // Handshake content type
	binary.Write(&rec, binary.BigEndian, uint16(0x0301))   // TLS 1.0 record version
	binary.Write(&rec, binary.BigEndian, uint16(hs.Len())) // record length
	rec.Write(hs.Bytes())

	return rec.Bytes()
}

func TestNetAdvJA3String(t *testing.T) {
	fn := DefaultRegistry.Lookup("ja3_string")
	data := buildTestClientHello()

	got := fn([]any{data})
	if got == nil {
		t.Fatal("ja3_string returned nil for valid ClientHello")
	}
	str, ok := got.(string)
	if !ok {
		t.Fatalf("ja3_string returned %T, want string", got)
	}
	// Should follow format: VERSION,CIPHERS,EXTENSIONS,CURVES,FORMATS
	parts := strings.Split(str, ",")
	if len(parts) != 5 {
		t.Errorf("ja3_string returned %d comma-separated parts, want 5: %q", len(parts), str)
	}
	// Version should be 771 (0x0303)
	if parts[0] != "771" {
		t.Errorf("ja3_string version = %q, want 771", parts[0])
	}
	// Ciphers should contain our two suites
	if !strings.Contains(parts[1], "49196") || !strings.Contains(parts[1], "49195") {
		t.Errorf("ja3_string ciphers = %q, expected 49196 and 49195 (0xC02C,0xC02B)", parts[1])
	}

	// Deterministic: call again, same result
	got2 := fn([]any{data})
	if got2 != got {
		t.Errorf("ja3_string not deterministic: %v vs %v", got, got2)
	}
}

func TestNetAdvJA3Fingerprint(t *testing.T) {
	fn := DefaultRegistry.Lookup("ja3_fingerprint")
	data := buildTestClientHello()

	got := fn([]any{data})
	if got == nil {
		t.Fatal("ja3_fingerprint returned nil for valid ClientHello")
	}
	hash, ok := got.(string)
	if !ok {
		t.Fatalf("ja3_fingerprint returned %T, want string", got)
	}
	if len(hash) != 32 {
		t.Errorf("ja3_fingerprint length = %d, want 32 (MD5 hex): %q", len(hash), hash)
	}
	// All characters should be hex digits
	for _, c := range hash {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("ja3_fingerprint contains non-hex character %c in %q", c, hash)
			break
		}
	}

	// Deterministic
	got2 := fn([]any{data})
	if got2 != got {
		t.Errorf("ja3_fingerprint not deterministic: %v vs %v", got, got2)
	}
}

func TestNetAdvJA3SString(t *testing.T) {
	fn := DefaultRegistry.Lookup("ja3s_string")
	data := buildTestServerHello()

	got := fn([]any{data})
	if got == nil {
		t.Fatal("ja3s_string returned nil for valid ServerHello")
	}
	str, ok := got.(string)
	if !ok {
		t.Fatalf("ja3s_string returned %T, want string", got)
	}
	// Format: VERSION,CIPHER,EXTENSIONS
	// 771 = 0x0303, 49196 = 0xC02C, no extensions = ""
	want := "771,49196,"
	if str != want {
		t.Errorf("ja3s_string = %q, want %q", str, want)
	}
}

func TestNetAdvJA3SFingerprint(t *testing.T) {
	fn := DefaultRegistry.Lookup("ja3s_fingerprint")
	data := buildTestServerHello()

	got := fn([]any{data})
	if got == nil {
		t.Fatal("ja3s_fingerprint returned nil for valid ServerHello")
	}
	hash, ok := got.(string)
	if !ok {
		t.Fatalf("ja3s_fingerprint returned %T, want string", got)
	}
	if len(hash) != 32 {
		t.Errorf("ja3s_fingerprint length = %d, want 32 (MD5 hex): %q", len(hash), hash)
	}
}

// ── Payload Search Functions ────────────────────────────────────────────────

func TestNetAdvPayloadContains(t *testing.T) {
	fn := DefaultRegistry.Lookup("payload_contains")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"Hello World", "World"}, true},
		{[]any{"Hello World", "xyz"}, false},
		{[]any{nil, "test"}, nil},
		{[]any{"test", nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("payload_contains(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvPayloadMatches(t *testing.T) {
	fn := DefaultRegistry.Lookup("payload_matches")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"GET /api/users HTTP/1.1", "GET /api/.*"}, true},
		{[]any{"POST /login", "GET /api/.*"}, false},
		{[]any{nil, ".*"}, nil},
		{[]any{"test", nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("payload_matches(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvPayloadOffset(t *testing.T) {
	fn := DefaultRegistry.Lookup("payload_offset")
	// hex "48656c6c6f" = "Hello", passed as raw bytes
	data := hexBytes("48656c6c6f")
	tests := []struct {
		args []any
		want any
	}{
		// offset=1, length=3 → bytes at [1:4] = 0x65,0x6c,0x6c → "656c6c"
		{[]any{data, int64(1), int64(3)}, "656c6c"},
		{[]any{nil, int64(0), int64(1)}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("payload_offset(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestNetAdvPayloadLength(t *testing.T) {
	fn := DefaultRegistry.Lookup("payload_length")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"Hello"}, int64(5)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("payload_length(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── Registration Check ──────────────────────────────────────────────────────

func TestNetAdvFunctionsRegistered(t *testing.T) {
	expected := []string{
		// ICMP (6)
		"icmp_type_name", "icmp_code_name", "is_icmp_echo",
		"icmp_parse", "icmp_type", "icmp_code",
		// IPv6 (9)
		"ipv6_scope", "ipv6_expand", "ipv6_compress",
		"ipv6_to_eui64", "is_6to4", "is_teredo",
		"teredo_server", "teredo_client", "sixto4_gateway",
		// JA3 (4)
		"ja3_fingerprint", "ja3_string",
		"ja3s_fingerprint", "ja3s_string",
		// Payload Search (4)
		"payload_contains", "payload_matches",
		"payload_offset", "payload_length",
	}
	if len(expected) != 23 {
		t.Fatalf("expected 23 function names, got %d", len(expected))
	}
	for _, name := range expected {
		if !DefaultRegistry.Has(name) {
			t.Errorf("function %q not registered in DefaultRegistry", name)
		}
	}
}

// ── NULL Propagation ────────────────────────────────────────────────────────

func TestNetAdvNullPropagation(t *testing.T) {
	singleArgFuncs := []string{
		// ICMP
		"icmp_type_name", "is_icmp_echo",
		"icmp_parse", "icmp_type", "icmp_code",
		// IPv6
		"ipv6_scope", "ipv6_expand", "ipv6_compress",
		"ipv6_to_eui64", "is_6to4", "is_teredo",
		"teredo_server", "teredo_client", "sixto4_gateway",
		// JA3
		"ja3_fingerprint", "ja3_string",
		"ja3s_fingerprint", "ja3s_string",
		// Payload
		"payload_length",
	}
	for _, name := range singleArgFuncs {
		fn := DefaultRegistry.Lookup(name)
		if fn == nil {
			t.Errorf("function %q not found in registry", name)
			continue
		}
		got := fn([]any{nil})
		if got != nil {
			t.Errorf("%s(nil) = %v, want nil", name, got)
		}
	}
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// hexBytes decodes a hex string into raw bytes.
func hexBytes(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic("bad hex in test: " + err.Error())
	}
	return b
}
