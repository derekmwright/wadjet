// This file holds expr network analytics; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/hex"
	"fmt"
	"net"
	"strings"
)

// --- Network: analytics ---

func fnIsPrivateIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsPrivate()
}

func fnIsLoopbackIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsLoopback()
}

func fnIPToInt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return nil // only IPv4
	}
	return float64(uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3]))
}

func fnIntToIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	n := uint32(ToFloat64(args[0]))
	return fmt.Sprintf("%d.%d.%d.%d", n>>24&0xFF, n>>16&0xFF, n>>8&0xFF, n&0xFF)
}

func fnIsIPv4(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return false
	}
	return ip.To4() != nil
}

func fnIsIPv6(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return false
	}
	return ip.To4() == nil
}

// ── CIDR / Subnet Operations ────────────────────────────────────────────────

func fnNetworkAddress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	return network.IP.String()
}

func fnBroadcastAddress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := network.IP.To4()
	if ip == nil {
		return nil
	}
	mask := network.Mask
	broadcast := make(net.IP, len(ip))
	for i := range ip {
		broadcast[i] = ip[i] | ^mask[i]
	}
	return broadcast.String()
}

func fnPrefixLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ones, _ := network.Mask.Size()
	return int64(ones)
}

func fnCIDRToRange(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := network.IP.To4()
	if ip == nil {
		return nil
	}
	mask := network.Mask
	first := network.IP.String()
	broadcast := make(net.IP, len(ip))
	for i := range ip {
		broadcast[i] = ip[i] | ^mask[i]
	}
	return first + "-" + broadcast.String()
}

func fnHostsInCIDR(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ones, bits := network.Mask.Size()
	hostBits := bits - ones
	if hostBits <= 0 {
		return int64(1)
	}
	if hostBits == 1 {
		return int64(2)
	}
	return int64(1<<uint(hostBits) - 2)
}

func fnCIDROverlap(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	_, net1, err1 := net.ParseCIDR(fmt.Sprint(args[0]))
	_, net2, err2 := net.ParseCIDR(fmt.Sprint(args[1]))
	if err1 != nil || err2 != nil {
		return nil
	}
	return net1.Contains(net2.IP) || net2.Contains(net1.IP)
}

func ipToUint32(ip net.IP) uint32 {
	ip4 := ip.To4()
	if ip4 == nil {
		return 0
	}
	return uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3])
}

func uint32ToIP(n uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", n>>24&0xFF, n>>16&0xFF, n>>8&0xFF, n&0xFF)
}

func fnIPInRange(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	lo := net.ParseIP(fmt.Sprint(args[1]))
	hi := net.ParseIP(fmt.Sprint(args[2]))
	if ip == nil || lo == nil || hi == nil {
		return nil
	}
	v := ipToUint32(ip)
	vLo := ipToUint32(lo)
	vHi := ipToUint32(hi)
	return v >= vLo && v <= vHi
}

func fnSameSubnet(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip1 := net.ParseIP(fmt.Sprint(args[0]))
	ip2 := net.ParseIP(fmt.Sprint(args[1]))
	prefixLen := int(ToInt64(args[2]))
	if ip1 == nil || ip2 == nil {
		return nil
	}
	ip1v4 := ip1.To4()
	ip2v4 := ip2.To4()
	if ip1v4 == nil || ip2v4 == nil {
		return nil
	}
	mask := net.CIDRMask(prefixLen, 32)
	for i := 0; i < 4; i++ {
		if ip1v4[i]&mask[i] != ip2v4[i]&mask[i] {
			return false
		}
	}
	return true
}

// ── IP Manipulation ─────────────────────────────────────────────────────────

func fnIPAdd(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	offset := ToInt64(args[1])
	v := uint32(int64(ipToUint32(ip)) + offset)
	return uint32ToIP(v)
}

func fnIPSubtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	offset := ToInt64(args[1])
	v := uint32(int64(ipToUint32(ip)) - offset)
	return uint32ToIP(v)
}

func fnIPDiff(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip1 := net.ParseIP(fmt.Sprint(args[0]))
	ip2 := net.ParseIP(fmt.Sprint(args[1]))
	if ip1 == nil || ip2 == nil {
		return nil
	}
	return int64(ipToUint32(ip1)) - int64(ipToUint32(ip2))
}

func fnIPBetween(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	lo := net.ParseIP(fmt.Sprint(args[1]))
	hi := net.ParseIP(fmt.Sprint(args[2]))
	if ip == nil || lo == nil || hi == nil {
		return nil
	}
	v := ipToUint32(ip)
	vLo := ipToUint32(lo)
	vHi := ipToUint32(hi)
	return v >= vLo && v <= vHi
}

func fnReverseDNS(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip4 := ip.To4(); ip4 != nil {
		return fmt.Sprintf("%d.%d.%d.%d.in-addr.arpa", ip4[3], ip4[2], ip4[1], ip4[0])
	}
	// IPv6: expand to full 32 nibbles reversed
	ip16 := ip.To16()
	nibbles := make([]string, 32)
	for i := 0; i < 16; i++ {
		nibbles[31-2*i] = fmt.Sprintf("%x", ip16[i]>>4)
		nibbles[30-2*i] = fmt.Sprintf("%x", ip16[i]&0x0f)
	}
	return strings.Join(nibbles, ".") + ".ip6.arpa"
}

func fnIsMulticastIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsMulticast()
}

func fnIsLinkLocalIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsLinkLocalUnicast()
}

func fnIsReservedIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast()
}

func fnIPToHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip4 := ip.To4(); ip4 != nil {
		return hex.EncodeToString(ip4)
	}
	return hex.EncodeToString(ip.To16())
}

// ── MAC Operations ──────────────────────────────────────────────────────────

func fnMACVendorOUI(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 3 {
		return nil
	}
	return strings.ToUpper(fmt.Sprintf("%02x:%02x:%02x", mac[0], mac[1], mac[2]))
}

func fnMACIsUnicast(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 1 {
		return nil
	}
	return mac[0]&0x01 == 0
}

func fnMACIsLocal(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 1 {
		return nil
	}
	return mac[0]&0x02 != 0
}

func fnMACFormat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	input := fmt.Sprint(args[0])
	sep := ":"
	if len(args) >= 2 && args[1] != nil {
		sep = fmt.Sprint(args[1])
	}
	// Strip any existing separators to get raw hex
	raw := strings.NewReplacer(":", "", "-", "", ".", "").Replace(input)
	if len(raw) != 12 {
		return nil
	}
	// Validate hex
	for _, c := range raw {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return nil
		}
	}
	raw = strings.ToLower(raw)
	parts := make([]string, 6)
	for i := 0; i < 6; i++ {
		parts[i] = raw[i*2 : i*2+2]
	}
	return strings.Join(parts, sep)
}

func fnPortName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	name, ok := wellKnownPorts[port]
	if !ok {
		return nil
	}
	return name
}

func fnIsWellKnownPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 0 && port <= 1023
}

func fnIsRegisteredPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 1024 && port <= 49151
}

func fnIsEphemeralPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 49152 && port <= 65535
}

func fnPortClass(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	switch {
	case port >= 0 && port <= 1023:
		return "well-known"
	case port >= 1024 && port <= 49151:
		return "registered"
	case port >= 49152 && port <= 65535:
		return "ephemeral"
	default:
		return nil
	}
}

func fnProtocolName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	num := ToInt64(args[0])
	name, ok := protocolNumToName[num]
	if !ok {
		return nil
	}
	return name
}

func fnProtocolNumber(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	name := strings.ToLower(fmt.Sprint(args[0]))
	num, ok := protocolNameToNum[name]
	if !ok {
		return nil
	}
	return num
}
