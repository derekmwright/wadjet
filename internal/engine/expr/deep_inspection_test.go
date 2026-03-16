package expr

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"testing"
)

// ── TCP Functions ───────────────────────────────────────────────────────────

func TestDPITCPFlagsToString(t *testing.T) {
	fn := DefaultRegistry.Lookup("tcp_flags_to_string")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(0x12)}, "SYN,ACK"},
		{[]any{int64(0x02)}, "SYN"},
		{[]any{int64(0x14)}, "RST,ACK"},
		{[]any{int64(0x10)}, "ACK"},
		{[]any{int64(0x01)}, "FIN"},
		{[]any{int64(0x18)}, "PSH,ACK"},
		{[]any{int64(0xFF)}, "FIN,SYN,RST,PSH,ACK,URG,ECE,CWR"},
		{[]any{int64(0x00)}, ""},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("tcp_flags_to_string(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestDPIHasTCPFlag(t *testing.T) {
	fn := DefaultRegistry.Lookup("has_tcp_flag")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(0x12), "SYN"}, true},
		{[]any{int64(0x12), "ACK"}, true},
		{[]any{int64(0x12), "RST"}, false},
		{[]any{int64(0x02), "SYN"}, true},
		{[]any{int64(0x02), "ACK"}, false},
		{[]any{int64(0x04), "RST"}, true},
		{[]any{int64(0x12), "syn"}, true},  // case-insensitive
		{[]any{int64(0x12), "BOGUS"}, nil}, // unknown flag name
		{[]any{nil, "SYN"}, nil},
		{[]any{int64(0x12), nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("has_tcp_flag(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestDPITCPFlagsFromString(t *testing.T) {
	fn := DefaultRegistry.Lookup("tcp_flags_from_string")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"SYN,ACK"}, int64(0x12)},
		{[]any{"SYN"}, int64(0x02)},
		{[]any{"RST,ACK"}, int64(0x14)},
		{[]any{"FIN"}, int64(0x01)},
		{[]any{"syn,ack"}, int64(0x12)}, // case-insensitive
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("tcp_flags_from_string(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestDPIIsTCPHandshake(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_tcp_handshake")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(0x02)}, true},  // SYN only → handshake initiation
		{[]any{int64(0x12)}, false}, // SYN+ACK → not handshake initiation
		{[]any{int64(0x10)}, false}, // ACK only
		{[]any{int64(0x04)}, false}, // RST only
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_tcp_handshake(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestDPIIsTCPReset(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_tcp_reset")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(0x04)}, true},  // RST
		{[]any{int64(0x14)}, true},  // RST+ACK
		{[]any{int64(0x10)}, false}, // ACK only
		{[]any{int64(0x02)}, false}, // SYN only
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_tcp_reset(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestDPITCPSessionID(t *testing.T) {
	fn := DefaultRegistry.Lookup("tcp_session_id")

	// Basic: verify it returns a string
	got := fn([]any{"192.168.1.1", "10.0.0.1", int64(12345), int64(80), int64(6)})
	s, ok := got.(string)
	if !ok || s == "" {
		t.Fatalf("tcp_session_id returned %v (%T), want non-empty string", got, got)
	}

	// Canonical ordering: swapped src/dst must give the same session ID
	gotSwapped := fn([]any{"10.0.0.1", "192.168.1.1", int64(80), int64(12345), int64(6)})
	if got != gotSwapped {
		t.Errorf("tcp_session_id not canonical: %v vs %v", got, gotSwapped)
	}

	// NULL propagation
	if fn([]any{nil, "10.0.0.1", int64(80), int64(443), int64(6)}) != nil {
		t.Error("tcp_session_id(nil, ...) should return nil")
	}
	if fn([]any{"10.0.0.1", nil, int64(80), int64(443), int64(6)}) != nil {
		t.Error("tcp_session_id(..., nil, ...) should return nil")
	}
}

func TestDPIFlowDirection(t *testing.T) {
	fn := DefaultRegistry.Lookup("flow_direction")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{"192.168.1.1", "8.8.8.8"}, "outbound"},    // private → public
		{[]any{"8.8.8.8", "192.168.1.1"}, "inbound"},     // public → private
		{[]any{"192.168.1.1", "10.0.0.1"}, "internal"},   // private → private
		{[]any{"8.8.8.8", "1.1.1.1"}, "transit"},         // public → public
		{[]any{"127.0.0.1", "8.8.8.8"}, "outbound"},     // loopback treated as private
		{[]any{nil, "8.8.8.8"}, nil},
		{[]any{"8.8.8.8", nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("flow_direction(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

// ── DNS Functions ───────────────────────────────────────────────────────────

// DNS query for "example.com" type A
// Transaction ID: 0xABCD, Flags: 0x0100 (standard query),
// QDCOUNT=1, ANCOUNT=0, NSCOUNT=0, ARCOUNT=0,
// QNAME: \x07example\x03com\x00, QTYPE=1 (A), QCLASS=1 (IN)
const dnsQueryHex = "abcd01000001000000000000076578616d706c6503636f6d0000010001"

// DNS response (flags changed to 0x8180: response + recursion available)
const dnsResponseHex = "abcd81800001000000000000076578616d706c6503636f6d0000010001"

func TestDPIDNSQueryName(t *testing.T) {
	fn := DefaultRegistry.Lookup("dns_query_name")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{dnsQueryHex}, "example.com"},
		{[]any{dnsResponseHex}, "example.com"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("dns_query_name(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIDNSQueryType(t *testing.T) {
	fn := DefaultRegistry.Lookup("dns_query_type")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{dnsQueryHex}, "A"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("dns_query_type(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIDNSIsResponse(t *testing.T) {
	fn := DefaultRegistry.Lookup("dns_is_response")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{dnsQueryHex}, false},
		{[]any{dnsResponseHex}, true},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("dns_is_response(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIDNSResponseCode(t *testing.T) {
	fn := DefaultRegistry.Lookup("dns_response_code")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{dnsQueryHex}, "NOERROR"},
		{[]any{dnsResponseHex}, "NOERROR"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("dns_response_code(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIDNSQuestionCount(t *testing.T) {
	fn := DefaultRegistry.Lookup("dns_question_count")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{dnsQueryHex}, int64(1)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("dns_question_count(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIDNSAnswerCount(t *testing.T) {
	fn := DefaultRegistry.Lookup("dns_answer_count")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{dnsQueryHex}, int64(0)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("dns_answer_count(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIDNSTransactionID(t *testing.T) {
	fn := DefaultRegistry.Lookup("dns_transaction_id")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{dnsQueryHex}, int64(0xABCD)},
		{[]any{dnsResponseHex}, int64(0xABCD)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("dns_transaction_id(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

// ── TLS Functions ───────────────────────────────────────────────────────────

// buildTLSClientHello constructs a minimal TLS ClientHello with the given SNI.
func buildTLSClientHello(sni string) []byte {
	// Build the SNI extension
	var sniExt bytes.Buffer
	binary.Write(&sniExt, binary.BigEndian, uint16(0))          // extension type: SNI (0x0000)
	sniName := []byte(sni)
	sniListLen := 1 + 2 + len(sniName) // type(1) + nameLen(2) + name
	binary.Write(&sniExt, binary.BigEndian, uint16(2+sniListLen)) // extension data length
	binary.Write(&sniExt, binary.BigEndian, uint16(sniListLen))   // SNI list length
	sniExt.WriteByte(0)                                           // host name type
	binary.Write(&sniExt, binary.BigEndian, uint16(len(sniName))) // name length
	sniExt.Write(sniName)                                         // hostname

	// Build ClientHello body
	var ch bytes.Buffer
	binary.Write(&ch, binary.BigEndian, uint16(0x0303)) // client version: TLS 1.2
	ch.Write(make([]byte, 32))                          // random (32 bytes of zeros)
	ch.WriteByte(0)                                     // session_id_len = 0
	binary.Write(&ch, binary.BigEndian, uint16(2))      // cipher_suites_len = 2
	binary.Write(&ch, binary.BigEndian, uint16(0x00FF)) // one cipher suite (TLS_EMPTY_RENEGOTIATION_INFO_SCSV)
	ch.WriteByte(1)                                     // compression methods length = 1
	ch.WriteByte(0)                                     // null compression
	binary.Write(&ch, binary.BigEndian, uint16(sniExt.Len())) // extensions length
	ch.Write(sniExt.Bytes())                                   // extensions data

	// Build Handshake header: type(1) + length(3)
	var hs bytes.Buffer
	hs.WriteByte(1) // ClientHello
	chLen := ch.Len()
	hs.WriteByte(byte(chLen >> 16))
	hs.WriteByte(byte(chLen >> 8))
	hs.WriteByte(byte(chLen))
	hs.Write(ch.Bytes())

	// Build TLS record: type(1) + version(2) + length(2) + payload
	var rec bytes.Buffer
	rec.WriteByte(22)                                            // content type: Handshake
	binary.Write(&rec, binary.BigEndian, uint16(0x0301))         // record version: TLS 1.0
	binary.Write(&rec, binary.BigEndian, uint16(hs.Len()))       // record length
	rec.Write(hs.Bytes())

	return rec.Bytes()
}

func TestDPITLSSNI(t *testing.T) {
	fn := DefaultRegistry.Lookup("tls_sni")

	clientHello := buildTLSClientHello("example.com")
	chHex := hex.EncodeToString(clientHello)

	tests := []struct {
		name string
		args []any
		want any
	}{
		{"valid_hex", []any{chHex}, "example.com"},
		{"valid_bytes", []any{clientHello}, "example.com"},
		{"non_tls", []any{"not-tls-data"}, nil},
		{"nil", []any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("tls_sni[%s](%v) = %v, want %v", tt.name, tt.args[0], got, tt.want)
		}
	}
}

func TestDPITLSVersion(t *testing.T) {
	fn := DefaultRegistry.Lookup("tls_version")

	clientHello := buildTLSClientHello("example.com")
	chHex := hex.EncodeToString(clientHello)

	tests := []struct {
		name string
		args []any
		want any
	}{
		{"from_clienthello", []any{chHex}, "TLS 1.0"}, // record header version is 0x0301
		{"nil", []any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("tls_version[%s] = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestDPITLSRecordType(t *testing.T) {
	fn := DefaultRegistry.Lookup("tls_record_type")

	clientHello := buildTLSClientHello("example.com")
	chHex := hex.EncodeToString(clientHello)

	tests := []struct {
		name string
		args []any
		want any
	}{
		{"handshake", []any{chHex}, "Handshake"},
		{"nil", []any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("tls_record_type[%s] = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestDPIIsTLSClientHello(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_tls_client_hello")

	clientHello := buildTLSClientHello("example.com")
	chHex := hex.EncodeToString(clientHello)

	tests := []struct {
		name string
		args []any
		want any
	}{
		{"valid_hex", []any{chHex}, true},
		{"valid_bytes", []any{clientHello}, true},
		{"non_tls", []any{"0000000000000000000000000000"}, false}, // 14 hex chars → 7 bytes, type ≠ 22
		{"nil", []any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_tls_client_hello[%s] = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestDPITLSHandshakeType(t *testing.T) {
	fn := DefaultRegistry.Lookup("tls_handshake_type")

	clientHello := buildTLSClientHello("example.com")
	chHex := hex.EncodeToString(clientHello)

	tests := []struct {
		name string
		args []any
		want any
	}{
		{"client_hello", []any{chHex}, "ClientHello"},
		{"non_handshake", []any{"1700000000000000000000000000"}, nil}, // type 23 = ApplicationData, not handshake
		{"nil", []any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("tls_handshake_type[%s] = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// ── HTTP Functions ──────────────────────────────────────────────────────────

const httpReq = "GET /api/v1/users?page=2 HTTP/1.1\r\nHost: api.example.com\r\nUser-Agent: curl/7.68\r\nContent-Type: application/json\r\nContent-Length: 42\r\nX-Request-ID: abc123\r\n\r\n"
const httpResp = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 1234\r\n\r\n"

func TestDPIHTTPMethod(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_method")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq}, "GET"},
		{[]any{"POST /data HTTP/1.1\r\n\r\n"}, "POST"},
		{[]any{httpResp}, nil}, // response has no method
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_method(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIHTTPPath(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_path")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq}, "/api/v1/users?page=2"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_path(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIHTTPHost(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_host")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq}, "api.example.com"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_host(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIHTTPStatusCode(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_status_code")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpResp}, int64(200)},
		{[]any{"HTTP/1.1 404 Not Found\r\n\r\n"}, int64(404)},
		{[]any{httpReq}, nil}, // not a response
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_status_code(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIHTTPStatusClass(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_status_class")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{int64(200)}, "2xx"},
		{[]any{int64(301)}, "3xx"},
		{[]any{int64(404)}, "4xx"},
		{[]any{int64(500)}, "5xx"},
		{[]any{int64(100)}, "1xx"},
		{[]any{int64(99)}, nil},  // out of range
		{[]any{int64(600)}, nil}, // out of range
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_status_class(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestDPIHTTPContentType(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_content_type")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq}, "application/json"},
		{[]any{httpResp}, "text/html"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_content_type(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIHTTPContentLength(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_content_length")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq}, int64(42)},
		{[]any{httpResp}, int64(1234)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_content_length(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIHTTPUserAgent(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_user_agent")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq}, "curl/7.68"},
		{[]any{httpResp}, nil}, // no User-Agent in response
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_user_agent(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIHTTPHeader(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_header")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq, "X-Request-ID"}, "abc123"},
		{[]any{httpReq, "Host"}, "api.example.com"},
		{[]any{httpReq, "x-request-id"}, "abc123"}, // case-insensitive
		{[]any{httpReq, "Nonexistent"}, nil},
		{[]any{nil, "Host"}, nil},
		{[]any{httpReq, nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_header(%v) = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestDPIHTTPVersion(t *testing.T) {
	fn := DefaultRegistry.Lookup("http_version")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq}, "HTTP/1.1"},
		{[]any{httpResp}, "HTTP/1.1"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("http_version(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIIsHTTPRequest(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_http_request")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpReq}, true},
		{[]any{httpResp}, false},
		{[]any{"PATCH /resource HTTP/1.1\r\n\r\n"}, true},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_http_request(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIIsHTTPResponse(t *testing.T) {
	fn := DefaultRegistry.Lookup("is_http_response")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{httpResp}, true},
		{[]any{httpReq}, false},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("is_http_response(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

// ── Packet Header Functions ─────────────────────────────────────────────────

// IPv4 header (20 bytes):
// Byte 0: 0x45 (version=4, IHL=5)
// Byte 1: 0xB8 (DSCP=46, ECN=0; 46<<2=184=0xB8)
// Bytes 2-3: total length = 60 (0x003C)
// Bytes 4-5: identification = 0x0000
// Bytes 6-7: flags+fragment offset = 0x0000
// Byte 8: TTL = 64 (0x40)
// Byte 9: protocol = 6 (TCP)
// Bytes 10-11: header checksum = 0x0000
// Bytes 12-15: src IP = 10.0.0.1
// Bytes 16-19: dst IP = 10.0.0.2
const ipv4HeaderHex = "45b8003c00000000400600000a0000010a000002"

func TestDPIIPHeaderLength(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_header_length")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{ipv4HeaderHex}, int64(20)}, // IHL=5, 5*4=20
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_header_length(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIIPTTL(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_ttl")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{ipv4HeaderHex}, int64(64)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_ttl(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIIPTotalLength(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_total_length")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{ipv4HeaderHex}, int64(60)},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_total_length(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

func TestDPIIPDSCP(t *testing.T) {
	fn := DefaultRegistry.Lookup("ip_dscp")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{ipv4HeaderHex}, int64(46)}, // 0xB8 >> 2 = 46
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ip_dscp(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

// Ethernet frame header (14 bytes): dst(6) + src(6) + EtherType(2)
// dst=ff:ff:ff:ff:ff:ff (broadcast), src=00:11:22:33:44:55, type=0x0800 (IPv4)
const ethernetIPv4Hex = "ffffffffffff0011223344550800"

func TestDPIEtherType(t *testing.T) {
	fn := DefaultRegistry.Lookup("ether_type")
	tests := []struct {
		args []any
		want any
	}{
		{[]any{ethernetIPv4Hex}, "IPv4"},
		{[]any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("ether_type(%v) = %v, want %v", tt.args[0], got, tt.want)
		}
	}
}

// VLAN-tagged Ethernet frame (16 bytes):
// dst(6) + src(6) + 0x8100(2) + VLAN tag(2)
// VLAN ID = 100 (0x0064), PCP=0, DEI=0 → tag bytes = 0x0064
const vlanFrameHex = "ffffffffffff001122334455810000640800"

func TestDPIVLANID(t *testing.T) {
	fn := DefaultRegistry.Lookup("vlan_id")
	tests := []struct {
		name string
		args []any
		want any
	}{
		{"vlan_100", []any{vlanFrameHex}, int64(100)},
		{"not_vlan", []any{ethernetIPv4Hex}, nil}, // EtherType 0x0800, not 0x8100
		{"nil", []any{nil}, nil},
	}
	for _, tt := range tests {
		got := fn(tt.args)
		if got != tt.want {
			t.Errorf("vlan_id[%s](%v) = %v, want %v", tt.name, tt.args[0], got, tt.want)
		}
	}
}

// ── Payload Analysis Functions ──────────────────────────────────────────────

func TestDPIPayloadEntropy(t *testing.T) {
	fn := DefaultRegistry.Lookup("payload_entropy")

	// All identical bytes → entropy should be 0.0
	uniform := strings.Repeat("A", 100)
	got := fn([]any{uniform})
	gotF, ok := got.(float64)
	if !ok {
		t.Fatalf("payload_entropy returned %T, want float64", got)
	}
	if gotF != 0.0 {
		t.Errorf("payload_entropy(uniform) = %v, want 0.0", gotF)
	}

	// High-entropy data (all 256 possible byte values equally distributed)
	var highEntropy []byte
	for i := 0; i < 256; i++ {
		highEntropy = append(highEntropy, byte(i))
	}
	got = fn([]any{highEntropy})
	gotF = got.(float64)
	if gotF < 7.9 || gotF > 8.1 {
		t.Errorf("payload_entropy(all-256-bytes) = %v, want ~8.0", gotF)
	}

	// Two distinct bytes in equal proportion → entropy should be 1.0
	twoBytes := bytes.Repeat([]byte{0x00, 0xFF}, 50)
	got = fn([]any{twoBytes})
	gotF = got.(float64)
	if math.Abs(gotF-1.0) > 0.01 {
		t.Errorf("payload_entropy(two-bytes) = %v, want ~1.0", gotF)
	}

	// Nil
	if fn([]any{nil}) != nil {
		t.Error("payload_entropy(nil) should return nil")
	}
}

func TestDPIPayloadHexDump(t *testing.T) {
	fn := DefaultRegistry.Lookup("payload_hex_dump")

	// Known bytes
	data := []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f} // "Hello"
	got := fn([]any{data, int64(5)})
	want := "48 65 6c 6c 6f"
	if got != want {
		t.Errorf("payload_hex_dump(Hello, 5) = %v, want %v", got, want)
	}

	// Truncated output (maxBytes < data length)
	got = fn([]any{data, int64(3)})
	want = "48 65 6c"
	if got != want {
		t.Errorf("payload_hex_dump(Hello, 3) = %v, want %v", got, want)
	}

	// Default max (no second arg)
	got = fn([]any{data})
	want = "48 65 6c 6c 6f"
	if got != want {
		t.Errorf("payload_hex_dump(Hello) = %v, want %v", got, want)
	}

	// Nil
	if fn([]any{nil}) != nil {
		t.Error("payload_hex_dump(nil) should return nil")
	}
}

// ── Cross-cutting: NULL Propagation ─────────────────────────────────────────

func TestDPINullPropagation(t *testing.T) {
	singleArgFuncs := []string{
		// TCP
		"tcp_flags_to_string", "tcp_flags_from_string",
		"is_tcp_handshake", "is_tcp_reset",
		// DNS
		"dns_query_name", "dns_query_type", "dns_is_response",
		"dns_response_code", "dns_question_count", "dns_answer_count",
		"dns_transaction_id",
		// TLS
		"tls_sni", "tls_version", "tls_record_type",
		"is_tls_client_hello", "tls_handshake_type",
		// HTTP
		"http_method", "http_path", "http_host", "http_status_code",
		"http_status_class", "http_content_type", "http_content_length",
		"http_user_agent", "http_version",
		"is_http_request", "is_http_response",
		// Packet headers
		"ip_header_length", "ip_ttl", "ip_total_length", "ip_dscp",
		"ether_type", "vlan_id",
		// Payload analysis
		"payload_entropy", "payload_hex_dump",
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

// ── Cross-cutting: Registration Check ───────────────────────────────────────

func TestDPIFunctionsRegistered(t *testing.T) {
	expected := []string{
		// TCP (7)
		"tcp_flags_to_string", "has_tcp_flag", "tcp_flags_from_string",
		"is_tcp_handshake", "is_tcp_reset", "tcp_session_id", "flow_direction",
		// DNS (7)
		"dns_query_name", "dns_query_type", "dns_is_response",
		"dns_response_code", "dns_question_count", "dns_answer_count",
		"dns_transaction_id",
		// TLS (5)
		"tls_sni", "tls_version", "tls_record_type",
		"is_tls_client_hello", "tls_handshake_type",
		// HTTP (12)
		"http_method", "http_path", "http_host", "http_status_code",
		"http_status_class", "http_content_type", "http_content_length",
		"http_user_agent", "http_header", "http_version",
		"is_http_request", "is_http_response",
		// Packet headers (6)
		"ip_header_length", "ip_ttl", "ip_total_length", "ip_dscp",
		"ether_type", "vlan_id",
		// Payload analysis (2)
		"payload_entropy", "payload_hex_dump",
	}
	if len(expected) != 39 {
		t.Fatalf("expected 39 function names, got %d", len(expected))
	}
	for _, name := range expected {
		if !DefaultRegistry.Has(name) {
			t.Errorf("function %q not registered in DefaultRegistry", name)
		}
	}
}
