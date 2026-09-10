// This file holds expr network fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"net"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Network function implementations ---

func fnIPToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	ip := net.ParseIP(s)
	if ip == nil {
		return nil
	}
	// A v6 SPELLING renders through the engine's one v6 renderer, so a
	// v4-mapped address prints the way its column prints (#580). net.ParseIP
	// stores a dotted quad in the same sixteen bytes, so the input's own
	// spelling is what separates the two families — the test CidrSortKey
	// makes as well.
	if strings.Contains(s, ":") {
		return batch.FormatIPv6(ip.To16())
	}
	return ip.String()
}

func fnCIDRContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[1]))
	if ip == nil {
		return nil
	}
	return network.Contains(ip)
}

func fnIPVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip.To4() != nil {
		return float64(4)
	}
	return float64(6)
}

func fnMaskIP(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	octets := int(ToFloat64(args[1]))
	ip4 := ip.To4()
	if ip4 != nil {
		// Mask last N octets of IPv4
		if octets < 0 || octets > 4 {
			return nil
		}
		masked := make(net.IP, 4)
		copy(masked, ip4)
		for j := 0; j < octets && j < 4; j++ {
			masked[3-j] = 0
		}
		return masked.String()
	}
	return nil
}

func fnMACToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	hw, err := net.ParseMAC(s)
	if err != nil {
		return nil
	}
	return hw.String()
}

func fnIPSubnet(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	return network.IP.String()
}

func fnIPNetmask(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	mask := network.Mask
	// Convert mask to dotted notation for IPv4
	if len(mask) == 4 {
		return fmt.Sprintf("%d.%d.%d.%d", mask[0], mask[1], mask[2], mask[3])
	}
	// For IPv6, return hex representation
	return mask.String()
}
