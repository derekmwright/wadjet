package parquet

import "strings"

// PostgreSQL's ABBREVIATED IPv4 input grammar — the half of #627 that is not a
// notation widening but an address INFERENCE, which is why #579 deferred it.
//
// `'10/8'::cidr` is 10.0.0.0/8 and `'192.168'::cidr` is 192.168.0.0/24: the
// missing octets are zero and, when no prefix is written, the length comes
// from the CLASS of the first octet, then widens to cover the octets that were
// written. Go's net.ParseCIDR and net.ParseIP accept none of it, so every
// refusal built on them refused input PostgreSQL accepts — ADR-0012 item 1's
// one rule about the direction a divergence may point.
//
// The table below is measured on live PostgreSQL 17.11, not read from the
// source, and pgIPv4PtonTable in the test carries every cell:
//
//	'10'        10.0.0.0/8       '10.1'      10.1.0.0/16
//	'128'       128.0.0.0/16     '128.1'     128.1.0.0/16
//	'192'       192.0.0.0/24     '192.168'   192.168.0.0/24
//	'224'       224.0.0.0/4      '224.1'     224.1.0.0/16
//	'240'       240.0.0.0/32     '240.1'     240.1.0.0/32
//	'10/8'      10.0.0.0/8       '192.168/16'  192.168.0.0/16
//	'010.1'     10.1.0.0/16      '10.1.2.3'  10.1.2.3/32
//
// The widening to 8×octets applies from the second octet on: a bare '224' is
// /4 where the class-D default alone would say /4 and an 8×1 widening would
// say /8, and the server prints /4. That single cell is the whole difference
// between the two readings of the rule, which is why it is in the table.
//
// Refused, also measured: '10.' , '10..1', '256.1', '10.1.2.3.4', '0x0a.1',
// '10/33', and any leading, trailing or embedded whitespace. IPv6 has no
// abbreviation at all — `'2001:db8'::inet` is an error — so this is the v4
// grammar only.
// Exported so kernel.CidrSortKey and this package's CidrStatsSortKey read ONE
// implementation: those two keys must agree byte for byte or a row-group prune
// drops rows the filter keeps (kernel.TestCidrStatsSortKeyMatchesKernel), and a
// structural parser is exactly the case CidrSortKey's own doc says must not be
// duplicated.
func PgIPv4Pton(s string) (addr [4]byte, bits int, ok bool) {
	body, maskText, hasMask := strings.Cut(s, "/")
	if body == "" {
		return addr, 0, false
	}
	octets := 0
	for i := 0; i < len(body); {
		if octets == 4 {
			return addr, 0, false
		}
		start := i
		v := 0
		for i < len(body) && body[i] >= '0' && body[i] <= '9' {
			v = v*10 + int(body[i]-'0')
			if v > 255 {
				return addr, 0, false
			}
			i++
		}
		if i == start || i-start > 3 {
			return addr, 0, false // an empty octet, or more than three digits
		}
		addr[octets] = byte(v)
		octets++
		if i < len(body) {
			if body[i] != '.' {
				return addr, 0, false
			}
			i++
			if i == len(body) {
				return addr, 0, false // a trailing dot
			}
		}
	}
	if octets == 0 {
		return addr, 0, false
	}
	if hasMask {
		if maskText == "" || len(maskText) > 2 {
			return addr, 0, false
		}
		for i := 0; i < len(maskText); i++ {
			if maskText[i] < '0' || maskText[i] > '9' {
				return addr, 0, false
			}
			bits = bits*10 + int(maskText[i]-'0')
		}
		if bits > 32 {
			return addr, 0, false
		}
		return addr, bits, true
	}
	switch {
	case addr[0] >= 240:
		bits = 32
	case addr[0] >= 224:
		bits = 4
	case addr[0] >= 192:
		bits = 24
	case addr[0] >= 128:
		bits = 16
	default:
		bits = 8
	}
	if octets > 1 && bits < octets*8 {
		bits = octets * 8
	}
	return addr, bits, true
}
