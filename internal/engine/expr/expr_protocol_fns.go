// This file holds expr protocol fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"net"
	"strings"
)

// --- Protocol Deep Inspection Functions ---

// TCP flag constants (bitmask positions in TCP flags byte)
const (
	tcpFIN = 0x01
	tcpSYN = 0x02
	tcpRST = 0x04
	tcpPSH = 0x08
	tcpACK = 0x10
	tcpURG = 0x20
	tcpECE = 0x40
	tcpCWR = 0x80
)

// fnTCPFlagsToString converts a TCP flags bitmask to comma-separated names.
// tcp_flags_to_string(0x12) → 'SYN,ACK'
func fnTCPFlagsToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	var parts []string
	for _, f := range tcpFlagNames {
		if flags&f.mask != 0 {
			parts = append(parts, f.name)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ",")
}

// fnHasTCPFlag tests if a TCP flags bitmask has a specific flag set.
// has_tcp_flag(0x12, 'SYN') → true
func fnHasTCPFlag(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	name := strings.ToLower(toString(args[1]))
	mask, ok := tcpFlagLookup[name]
	if !ok {
		return nil
	}
	return flags&mask != 0
}

// fnTCPFlagsFromString converts flag names to bitmask.
// tcp_flags_from_string('SYN,ACK') → 0x12 (18)
func fnTCPFlagsFromString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	parts := strings.Split(toString(args[0]), ",")
	var result byte
	for _, p := range parts {
		mask, ok := tcpFlagLookup[strings.ToLower(strings.TrimSpace(p))]
		if ok {
			result |= mask
		}
	}
	return int64(result)
}

// fnIsTCPHandshake tests for SYN-only (connection initiation).
// is_tcp_handshake(flags) → true if SYN is set and ACK is not
func fnIsTCPHandshake(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	return flags&tcpSYN != 0 && flags&tcpACK == 0
}

// fnIsTCPReset tests for RST flag.
func fnIsTCPReset(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	return flags&tcpRST != 0
}

// fnTCPSessionID generates a canonical 5-tuple session key.
// tcp_session_id(src_ip, dst_ip, src_port, dst_port, protocol)
// Orders the IP/port pair so both directions map to the same key.
func fnTCPSessionID(args []any) any {
	if len(args) < 5 {
		return nil
	}
	for _, a := range args[:5] {
		if a == nil {
			return nil
		}
	}
	srcIP := toString(args[0])
	dstIP := toString(args[1])
	srcPort := ToInt64(args[2])
	dstPort := ToInt64(args[3])
	proto := ToInt64(args[4])

	// Canonical ordering: lower IP first, break ties by port
	if srcIP > dstIP || (srcIP == dstIP && srcPort > dstPort) {
		srcIP, dstIP = dstIP, srcIP
		srcPort, dstPort = dstPort, srcPort
	}
	return fmt.Sprintf("%s:%d-%s:%d/%d", srcIP, srcPort, dstIP, dstPort, proto)
}

// fnFlowDirection classifies a flow as 'inbound', 'outbound', or 'internal'.
// flow_direction(src_ip, dst_ip) — based on RFC 1918 private ranges
func fnFlowDirection(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	srcIP := net.ParseIP(toString(args[0]))
	dstIP := net.ParseIP(toString(args[1]))
	if srcIP == nil || dstIP == nil {
		return nil
	}
	srcPriv := srcIP.IsPrivate() || srcIP.IsLoopback()
	dstPriv := dstIP.IsPrivate() || dstIP.IsLoopback()
	switch {
	case srcPriv && dstPriv:
		return "internal"
	case srcPriv && !dstPriv:
		return "outbound"
	case !srcPriv && dstPriv:
		return "inbound"
	default:
		return "transit"
	}
}
