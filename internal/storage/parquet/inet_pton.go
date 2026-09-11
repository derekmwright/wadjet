package parquet

import "strings"

// PgIPv4Pton shares INET grammar between filter and stats keys (#627, ADR-0012).
// Maskless input requires four octets; with a mask accept 1–4, retain host
// bits and require bits/8 <= octets. Allow leading zeros and one trailing dot.
// Reject hex, whitespace, malformed/empty octets and masks outside 0..32.
// CIDR's classful inference/hex/host-bit rejection belongs to its explicit
// input cast, not comparison INET grammar; that CAST still passes text through.
// IPv6 uses net.ParseCIDR; this parser is v4-only.
// CidrSortKey and CidrStatsSortKey must agree byte-for-byte or pruning loses rows.
// See docs/internals/parquet-postgres-inet-ipv4-grammar.md for the design.
func PgIPv4Pton(s string) (addr [4]byte, bits int, ok bool) {
	addr, bits, _, ok = pgIPv4Pton(s)
	return addr, bits, ok
}

// PgIPv4PtonQuad is PgIPv4Pton restricted to a literal that writes ALL FOUR
// octets — `'010.1.2.3'`, `'1.2.3.4/24'`, `'10.1.2.3.'` — and it is the
// question a site that does NOT know the column's type has to ask.
//
// The abbreviated forms are only meaningful beside a network column: `'10/8'`
// names 10.0.0.0/8 there and is a 22P02 beside a numeric one, so a type-blind
// site that read it as an address would order a DOUBLE column's rows by an
// address key. A whole quad has no such second reading — no numeric grammar
// accepts `1.2.3.4` — so it is safe to classify without the type, which is
// what kernel.CidrAddressText uses this for (round-3 review B3-1: the type-
// blind gate was Go's net.ParseCIDR, which refuses the leading-zero and
// trailing-dot quads this parser accepts, so the same literal answered on the
// single arm and raised on the DAG).
func PgIPv4PtonQuad(s string) (addr [4]byte, bits int, ok bool) {
	addr, bits, octets, ok := pgIPv4Pton(s)
	if !ok || octets != 4 {
		return addr, 0, false
	}
	return addr, bits, true
}

func pgIPv4Pton(s string) (addr [4]byte, bits, octets int, ok bool) {
	body, maskText, hasMask := strings.Cut(s, "/")
	// One trailing dot is ignored by the server's parser — '10.1.2.3.' is
	// 10.1.2.3/32 and '10.1./16' is 10.1.0.0/16 — while an EMPTY octet
	// ('10..1') is refused. Trimming it here keeps the octet loop's rule
	// ("a dot must be followed by a digit") exactly as strict as PostgreSQL's.
	body = strings.TrimSuffix(body, ".")
	if body == "" {
		return addr, 0, 0, false
	}
	for i := 0; i < len(body); {
		if octets == 4 {
			return addr, 0, 0, false
		}
		start := i
		v := 0
		for i < len(body) && body[i] >= '0' && body[i] <= '9' {
			v = v*10 + int(body[i]-'0')
			if v > 255 {
				return addr, 0, 0, false
			}
			i++
		}
		if i == start {
			return addr, 0, 0, false // an empty octet
		}
		addr[octets] = byte(v)
		octets++
		if i < len(body) {
			if body[i] != '.' {
				return addr, 0, 0, false
			}
			i++
			if i == len(body) {
				return addr, 0, 0, false // a SECOND trailing dot
			}
		}
	}
	if !hasMask {
		// inet performs no address inference: a maskless literal must name
		// every octet. This is the line that keeps `cd = '239'` a 22P02 here
		// exactly as it is there — the classful reading belongs to the cidr
		// TYPE, and nothing in this engine resolves a comparison through it.
		if octets != 4 {
			return addr, 0, 0, false
		}
		return addr, 32, octets, true
	}
	if maskText == "" {
		return addr, 0, 0, false
	}
	for i := 0; i < len(maskText); i++ {
		if maskText[i] < '0' || maskText[i] > '9' {
			return addr, 0, 0, false
		}
		bits = bits*10 + int(maskText[i]-'0')
		if bits > 32 {
			return addr, 0, 0, false
		}
	}
	// The mask may not reach past the bytes the literal actually wrote:
	// '10/15' is 10.0.0.0/15 and '10/16' is 22P02. Measured over all four
	// octet counts × every mask 0-33.
	if bits/8 > octets {
		return addr, 0, 0, false
	}
	return addr, bits, octets, true
}
