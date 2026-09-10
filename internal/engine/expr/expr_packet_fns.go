// This file holds expr packet fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// --- Packet header parsing ---

// fnIPHeaderLength returns the IP header length in bytes from the raw IP header.
// ip_header_length(payload_hex) — first nibble of byte 0 × 4
func fnIPHeaderLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	ihl := int64(data[0]&0x0F) * 4
	return ihl
}

// fnIPTTL extracts the TTL field from an IPv4 header.
func fnIPTTL(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 9 {
		return nil
	}
	return int64(data[8])
}

// fnIPTotalLength extracts the total length from an IPv4 header.
func fnIPTotalLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[2:4]))
}

// fnIPDSCP extracts the DSCP value from the IPv4 TOS byte.
// DSCP is the top 6 bits of byte 1.
func fnIPDSCP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(data[1] >> 2)
}

// fnEtherType identifies the EtherType from an Ethernet frame header.
// ether_type(frame_hex) — bytes 12-13 of the Ethernet header
func fnEtherType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 14 {
		return nil
	}
	et := binary.BigEndian.Uint16(data[12:14])
	switch et {
	case 0x0800:
		return "IPv4"
	case 0x0806:
		return "ARP"
	case 0x86DD:
		return "IPv6"
	case 0x8100:
		return "VLAN"
	case 0x8847:
		return "MPLS"
	case 0x88CC:
		return "LLDP"
	default:
		return fmt.Sprintf("0x%04X", et)
	}
}

// fnVLANID extracts the VLAN ID from an 802.1Q tagged frame.
// Expects raw Ethernet frame; VLAN tag starts at byte 14 if EtherType is 0x8100.
func fnVLANID(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 16 {
		return nil
	}
	et := binary.BigEndian.Uint16(data[12:14])
	if et != 0x8100 {
		return nil // not a VLAN-tagged frame
	}
	// VLAN ID is the lower 12 bits of bytes 14-15
	vlanID := binary.BigEndian.Uint16(data[14:16]) & 0x0FFF
	return int64(vlanID)
}

// fnPayloadEntropy estimates the Shannon entropy of a byte payload.
// Useful for detecting encrypted/compressed traffic vs plaintext.
// payload_entropy(data) → float64 (0.0 = uniform, ~8.0 = maximum entropy)
func fnPayloadEntropy(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) == 0 {
		return float64(0)
	}
	var freq [256]int
	for _, b := range data {
		freq[b]++
	}
	n := float64(len(data))
	entropy := 0.0
	for _, f := range freq {
		if f == 0 {
			continue
		}
		p := float64(f) / n
		entropy -= p * math.Log2(p)
	}
	return entropy
}

// fnPayloadHexDump returns the first N bytes as a hex dump string.
// payload_hex_dump(data, 16) → '48 65 6c 6c 6f 20 ...'
func fnPayloadHexDump(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	maxBytes := 32
	if len(args) >= 2 && args[1] != nil {
		maxBytes = int(ToInt64(args[1]))
	}
	if maxBytes > len(data) {
		maxBytes = len(data)
	}
	if maxBytes <= 0 {
		return ""
	}
	parts := make([]string, maxBytes)
	for i := 0; i < maxBytes; i++ {
		parts[i] = fmt.Sprintf("%02x", data[i])
	}
	return strings.Join(parts, " ")
}
