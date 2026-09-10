// This file holds expr ipv6 fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"net"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- IPv6 Functions ---

func fnIPv6Scope(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	switch {
	case ip.IsLoopback():
		return "loopback"
	case ip.IsLinkLocalUnicast():
		return "link-local"
	case ip.IsLinkLocalMulticast():
		return "link-local-multicast"
	case ip.IsMulticast():
		return "multicast"
	case ip.IsPrivate():
		if ip.To4() != nil {
			return "private"
		}
		return "unique-local"
	case ip.IsGlobalUnicast():
		return "global"
	case ip.IsUnspecified():
		return "unspecified"
	default:
		return "unknown"
	}
}

func fnIPv6Expand(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return nil
	}
	return fmt.Sprintf("%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
		ip16[0], ip16[1], ip16[2], ip16[3],
		ip16[4], ip16[5], ip16[6], ip16[7],
		ip16[8], ip16[9], ip16[10], ip16[11],
		ip16[12], ip16[13], ip16[14], ip16[15])
}

func fnIPv6Compress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	// The inverse of fnIPv6Expand, which already reads a dotted quad as the
	// v4-MAPPED address it is: compressing one gives `::ffff:a.b.c.d`, not the
	// bare quad net.IP.String() would hand back (#580).
	return batch.FormatIPv6(ip.To16())
}

func fnIPv6ToEUI64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(toString(args[0]))
	if err != nil || len(mac) != 6 {
		return nil
	}
	eui := make([]byte, 8)
	eui[0] = mac[0] ^ 0x02
	eui[1] = mac[1]
	eui[2] = mac[2]
	eui[3] = 0xFF
	eui[4] = 0xFE
	eui[5] = mac[3]
	eui[6] = mac[4]
	eui[7] = mac[5]
	return fmt.Sprintf("%02x%02x:%02x%02x:%02x%02x:%02x%02x",
		eui[0], eui[1], eui[2], eui[3], eui[4], eui[5], eui[6], eui[7])
}

func fnIs6to4(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return false
	}
	ip16 := ip.To16()
	return ip16 != nil && ip16[0] == 0x20 && ip16[1] == 0x02
}

func fnIsTeredo(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return false
	}
	ip16 := ip.To16()
	return ip16 != nil && ip16[0] == 0x20 && ip16[1] == 0x01 && ip16[2] == 0x00 && ip16[3] == 0x00
}

func fnTeredoServer(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x01 || ip16[2] != 0x00 || ip16[3] != 0x00 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[4], ip16[5], ip16[6], ip16[7])
}

func fnTeredoClient(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x01 || ip16[2] != 0x00 || ip16[3] != 0x00 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[12]^0xFF, ip16[13]^0xFF, ip16[14]^0xFF, ip16[15]^0xFF)
}

func fnSixto4Gateway(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x02 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[2], ip16[3], ip16[4], ip16[5])
}
