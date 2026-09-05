package parquet

import "strings"

// PostgreSQL's IPv4 literal grammar — the INET one, which is the type an
// unquoted literal beside a network column resolves through on the server.
//
// This is the half of #627 that is a notation widening rather than an address
// inference, and the distinction is the whole rule. PostgreSQL has TWO v4
// parsers, and they are not the same grammar:
//
//	inet (inet_net_pton_ipv4)   the type EVERY comparison resolves through
//	cidr (inet_cidr_pton_ipv4)  the cidr TYPE's own input function
//
// `cd = '<literal>'` is not read by the cidr parser even when `cd` IS a cidr
// column: cidr carries no operators of its own, so the server resolves the
// pair through `=(inet, inet)` and reads the literal with inet's parser — the
// error text says so (`invalid input syntax for type inet: "239"` beside a
// cidr column). The cidr grammar's extras (a classful address INFERENCE for a
// maskless abbreviation, `0x`-hex input, and the refusal of bits set to the
// right of the mask) are reachable only through an explicit cast, which this
// engine does not implement, so this function does not carry them. When
// `CAST(<text> AS CIDR)` is implemented, THAT is where the cidr grammar goes,
// beside its own measured table — not here.
//
// The grammar below is measured on live PostgreSQL 17.11, cell by cell, and
// inet_pton_test.go carries the whole domain rather than samples:
//
//	'10.1.2.3'    10.1.2.3/32     a maskless address must be a whole quad
//	'10'          22P02           …so every abbreviation without a mask fails
//	'192.168'     22P02           (all 256 one-octet values, measured)
//	'10/8'        10.0.0.0/8      with a mask, 1-4 octets are enough
//	'192.168/16'  192.168.0.0/16
//	'10.1/8'      10.1.0.0/8      HOST BITS ARE KEPT — inet does not mask
//	'1/0'         1.0.0.0/0       …so these four are values, not errors
//	'255/1'       255.0.0.0/1
//	'172.31/12'   172.31.0.0/12
//	'10/15'       10.0.0.0/15     the mask may not name a byte not written:
//	'10/16'       22P02           bits/8 > octets is 22P02, measured 4x34
//	'10/008'      10.0.0.0/8      mask digits are digits, any number of them
//	'010.1.2.3'   10.1.2.3/32     and so are an octet's leading zeros
//	'10.1.2.3.'   10.1.2.3/32     one trailing dot is ignored
//	'10./8'       10.0.0.0/8
//
// Refused, also measured: '10.', '10..1', '256.1', '10.1.2.3.4', '0x0a',
// '0x10/8' (hex is cidr-only), '10/33', '10/', '/8', '10/+8', and any leading,
// trailing or embedded whitespace. IPv6 has no abbreviation at all —
// `'2001:db8'::inet` is an error — so this is the v4 grammar only, and the v6
// literals go through net.ParseCIDR unchanged.
//
// Exported so kernel.CidrSortKey and this package's CidrStatsSortKey read ONE
// implementation: those two keys must agree byte for byte or a row-group prune
// drops rows the filter keeps (kernel.TestCidrStatsSortKeyMatchesKernel), and a
// structural parser is exactly the case CidrSortKey's own doc says must not be
// duplicated.
func PgIPv4Pton(s string) (addr [4]byte, bits int, ok bool) {
	body, maskText, hasMask := strings.Cut(s, "/")
	// One trailing dot is ignored by the server's parser — '10.1.2.3.' is
	// 10.1.2.3/32 and '10.1./16' is 10.1.0.0/16 — while an EMPTY octet
	// ('10..1') is refused. Trimming it here keeps the octet loop's rule
	// ("a dot must be followed by a digit") exactly as strict as PostgreSQL's.
	body = strings.TrimSuffix(body, ".")
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
		if i == start {
			return addr, 0, false // an empty octet
		}
		addr[octets] = byte(v)
		octets++
		if i < len(body) {
			if body[i] != '.' {
				return addr, 0, false
			}
			i++
			if i == len(body) {
				return addr, 0, false // a SECOND trailing dot
			}
		}
	}
	if !hasMask {
		// inet performs no address inference: a maskless literal must name
		// every octet. This is the line that keeps `cd = '239'` a 22P02 here
		// exactly as it is there — the classful reading belongs to the cidr
		// TYPE, and nothing in this engine resolves a comparison through it.
		if octets != 4 {
			return addr, 0, false
		}
		return addr, 32, true
	}
	if maskText == "" {
		return addr, 0, false
	}
	for i := 0; i < len(maskText); i++ {
		if maskText[i] < '0' || maskText[i] > '9' {
			return addr, 0, false
		}
		bits = bits*10 + int(maskText[i]-'0')
		if bits > 32 {
			return addr, 0, false
		}
	}
	// The mask may not reach past the bytes the literal actually wrote:
	// '10/15' is 10.0.0.0/15 and '10/16' is 22P02. Measured over all four
	// octet counts × every mask 0-33.
	if bits/8 > octets {
		return addr, 0, false
	}
	return addr, bits, true
}
