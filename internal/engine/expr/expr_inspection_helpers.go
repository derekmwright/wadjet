// This file holds expr inspection helpers; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

// --- Deep inspection helpers ---

// toBytes converts a value to a byte slice, supporting hex strings and raw []byte.
func toBytes(v any) []byte {
	switch tv := v.(type) {
	case []byte:
		return tv
	case string:
		// Try hex decoding first
		if decoded, err := hex.DecodeString(tv); err == nil && len(tv)%2 == 0 && len(tv) > 0 {
			return decoded
		}
		// Fall back to raw bytes
		return []byte(tv)
	default:
		return []byte(fmt.Sprint(v))
	}
}

// parseDNSName parses a DNS wire-format name starting at the given offset.
func parseDNSName(data []byte, offset int) any {
	var parts []string
	for offset < len(data) {
		length := int(data[offset])
		if length == 0 {
			break
		}
		// Pointer (compression)
		if length&0xC0 == 0xC0 {
			if offset+1 >= len(data) {
				break
			}
			ptr := int(binary.BigEndian.Uint16(data[offset:offset+2])) & 0x3FFF
			rest := parseDNSName(data, ptr)
			if rest != nil {
				parts = append(parts, rest.(string))
			}
			return strings.Join(parts, ".")
		}
		offset++
		if offset+length > len(data) {
			break
		}
		parts = append(parts, string(data[offset:offset+length]))
		offset += length
	}
	if len(parts) == 0 {
		return nil
	}
	return strings.Join(parts, ".")
}

// dnsTypeName maps DNS query type numbers to names.
func dnsTypeName(qtype uint16) string {
	switch qtype {
	case 1:
		return "A"
	case 2:
		return "NS"
	case 5:
		return "CNAME"
	case 6:
		return "SOA"
	case 12:
		return "PTR"
	case 15:
		return "MX"
	case 16:
		return "TXT"
	case 28:
		return "AAAA"
	case 33:
		return "SRV"
	case 43:
		return "DS"
	case 46:
		return "RRSIG"
	case 48:
		return "DNSKEY"
	case 65:
		return "HTTPS"
	case 255:
		return "ANY"
	default:
		return fmt.Sprintf("TYPE%d", qtype)
	}
}

// parseTLSSNI extracts the SNI from a TLS ClientHello message.
func parseTLSSNI(data []byte) any {
	// TLS record: type(1) version(2) length(2) [record payload]
	if len(data) < 5 || data[0] != 22 { // not handshake
		return nil
	}
	// Handshake header: type(1) length(3)
	if len(data) < 9 || data[5] != 1 { // not ClientHello
		return nil
	}
	// ClientHello: version(2) random(32) session_id_len(1) ...
	offset := 9 // start of ClientHello body
	if offset+34 > len(data) {
		return nil
	}
	offset += 34 // skip version + random

	// Session ID
	if offset >= len(data) {
		return nil
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen

	// Cipher suites
	if offset+2 > len(data) {
		return nil
	}
	csLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2 + csLen

	// Compression methods
	if offset >= len(data) {
		return nil
	}
	compLen := int(data[offset])
	offset += 1 + compLen

	// Extensions
	if offset+2 > len(data) {
		return nil
	}
	extLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	extEnd := offset + extLen
	if extEnd > len(data) {
		extEnd = len(data)
	}

	for offset+4 <= extEnd {
		extType := binary.BigEndian.Uint16(data[offset : offset+2])
		eLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		offset += 4
		if extType == 0 { // SNI extension
			// SNI list: total_len(2) type(1) name_len(2) name(...)
			if offset+5 > extEnd {
				return nil
			}
			// skip list length (2 bytes)
			nameType := data[offset+2]
			nameLen := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if nameType != 0 { // must be hostname type
				return nil
			}
			if offset+5+nameLen > extEnd {
				return nil
			}
			return string(data[offset+5 : offset+5+nameLen])
		}
		offset += eLen
	}
	return nil
}

// tlsVersionString converts TLS major.minor to human-readable string.
func tlsVersionString(major, minor byte) string {
	if major == 3 {
		switch minor {
		case 0:
			return "SSL 3.0"
		case 1:
			return "TLS 1.0"
		case 2:
			return "TLS 1.1"
		case 3:
			return "TLS 1.2"
		case 4:
			return "TLS 1.3"
		}
	}
	return fmt.Sprintf("TLS %d.%d", major, minor)
}

// extractHTTPHeader extracts an HTTP header value by name (case-insensitive).
func extractHTTPHeader(payload, headerName string) any {
	// Find end of first line
	lines := strings.Split(payload, "\r\n")
	if len(lines) < 2 {
		lines = strings.Split(payload, "\n")
	}
	for _, line := range lines[1:] {
		if line == "" {
			break // end of headers
		}
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		name := strings.TrimSpace(line[:colon])
		if strings.EqualFold(name, headerName) {
			return strings.TrimSpace(line[colon+1:])
		}
	}
	return nil
}
