// This file holds expr dns fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/binary"
	"fmt"
)

// --- DNS parsing functions ---
// These work on raw DNS payload bytes (the UDP payload after the IP/UDP headers).

// fnDNSQueryName extracts the query name from a DNS query payload.
// dns_query_name(payload_hex) → 'www.example.com'
// DNS wire format: 2-byte ID, 2-byte flags, 2-byte QDCOUNT, ..., then QNAME
// QNAME is a sequence of length-prefixed labels ending with a 0-length label.
func fnDNSQueryName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 13 { // minimum DNS header + 1-byte name
		return nil
	}
	// Skip 12-byte DNS header
	offset := 12
	return parseDNSName(data, offset)
}

// fnDNSQueryType extracts the query type (A=1, AAAA=28, CNAME=5, MX=15, etc.).
func fnDNSQueryType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 13 {
		return nil
	}
	// Skip header, skip QNAME
	offset := 12
	for offset < len(data) {
		length := int(data[offset])
		if length == 0 {
			offset++
			break
		}
		offset += 1 + length
	}
	if offset+2 > len(data) {
		return nil
	}
	qtype := binary.BigEndian.Uint16(data[offset : offset+2])
	return dnsTypeName(qtype)
}

// fnDNSIsResponse checks if DNS packet is a response (QR bit set).
func fnDNSIsResponse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	// QR is bit 15 of the flags field (byte 2, bit 7)
	return data[2]&0x80 != 0
}

// fnDNSResponseCode extracts the RCODE from DNS flags.
// 0=NOERROR, 1=FORMERR, 2=SERVFAIL, 3=NXDOMAIN, 5=REFUSED
func fnDNSResponseCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	rcode := data[3] & 0x0F
	switch rcode {
	case 0:
		return "NOERROR"
	case 1:
		return "FORMERR"
	case 2:
		return "SERVFAIL"
	case 3:
		return "NXDOMAIN"
	case 4:
		return "NOTIMP"
	case 5:
		return "REFUSED"
	default:
		return fmt.Sprintf("RCODE_%d", rcode)
	}
}

// fnDNSQuestionCount returns the number of questions in a DNS packet.
func fnDNSQuestionCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 6 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[4:6]))
}

// fnDNSAnswerCount returns the number of answers in a DNS packet.
func fnDNSAnswerCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 8 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[6:8]))
}

// fnDNSTransactionID extracts the 16-bit transaction ID.
func fnDNSTransactionID(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[0:2]))
}
