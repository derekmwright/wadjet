// SPDX-License-Identifier: MIT

package parquet

import (
	"encoding/binary"
	"math"
	"net"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// This file is the ONE text grammar of each network-native type, for every
// boundary that reads one: the writer (ingest, INSERT VALUES, COPY), the CAST
// at query time, a literal at plan time and a literal at run time.
//
// Before it there were three grammars per type and they disagreed in both
// directions — the writer refused `08-00-2b-01-02-03` and `{uuid}` that
// PostgreSQL accepts, the comparison kernels accepted a hyphen anywhere in a
// UUID that PostgreSQL refuses, and the CAST parsed nothing at all (#1092,
// #627). PostgreSQL 17.11's input functions are the accept-set, measured
// rather than recalled: inet for IPv4/IPv6/CIDR (a wadjet CIDR column holds
// host bits under a mask, which `cidr` itself refuses), macaddr for MAC, uuid
// for UUID. PORT and PROTOCOL have no PostgreSQL type; their oracle is this
// engine's own documented text form (docs/data-types.md).

// NetTextStatus classifies a literal against its type's input grammar. The
// three failures are three different answers on the wire and they are
// PostgreSQL's own: 22P02 for text that names no value of the type, 22003 for
// one it names that the type cannot carry, 0A000 for PostgreSQL-valid text
// naming a NETWORK where this engine's type holds a bare address.
type NetTextStatus int

const (
	NetTextOK NetTextStatus = iota
	NetTextSyntax
	NetTextRange
	NetTextPrefix
)

// NetworkTextValue reads s with typ's input grammar and returns the box the
// WRITER stores for that type: an int64 for IPv4/MAC, 16 bytes for IPv6/UUID,
// the validated text for CIDR, an int32 for PORT/PROTOCOL. ok=false in the
// second result means typ has no text grammar and the caller's own rule
// applies.
func NetworkTextValue(typ TypeID, s string) (any, NetTextStatus, bool) {
	switch typ {
	case TypeIPv4:
		n, st := PgIPv4Address(s)
		return n, st, true
	case TypeIPv6:
		b, st := PgIPv6Address(s)
		return b, st, true
	case TypeCIDR:
		if _, _, _, ok := PgInetPton(s); !ok {
			return nil, NetTextSyntax, true
		}
		return s, NetTextOK, true
	case TypeMAC:
		b, st := PgMACPton(s)
		if st != NetTextOK {
			return nil, st, true
		}
		var n uint64
		for _, c := range b {
			n = n<<8 | uint64(c)
		}
		return int64(n), NetTextOK, true
	case TypeUUID:
		raw, st := PgUUIDPton(s)
		if st != NetTextOK {
			return nil, st, true
		}
		out := make([]byte, 16)
		copy(out, raw[:])
		return out, NetTextOK, true
	case TypePort:
		n, st := PgPortText(s)
		return n, st, true
	case TypeProtocol:
		n, st := PgProtocolText(s)
		return n, st, true
	}
	return nil, NetTextOK, false
}

// NetworkTextError is the ONE message each status produces, so a literal
// refused at the writer, at the CAST and in a WHERE clause is refused with the
// same SQLSTATE and the same words. PostgreSQL names the type its own input
// function used, which for the three address types is inet.
func NetworkTextError(typ TypeID, s string, st NetTextStatus) error {
	switch st {
	case NetTextOK:
		return nil
	case NetTextRange:
		if typ == TypePort || typ == TypeProtocol {
			// These two carry a BOUND, not an octet: the message is the one
			// the INSERT VALUES door has always produced for the same value.
			lo, hi := NetworkIntBounds(typ)
			return sqlerr.New("22003", "%s value %s out of range [%d, %d]", typ, s, lo, hi)
		}
		return sqlerr.New("22003", "invalid octet value in %q value: %s",
			NetworkTextTypeName(typ), sqlerr.Quote(s))
	case NetTextPrefix:
		return sqlerr.New("0A000", "%s is not representable in an %s column: %s "+
			"(PostgreSQL reads it as %s; use a CIDR column, or an address this type can hold)",
			networkUnholdableKind(typ, s), typ.String(), sqlerr.Quote(s),
			networkUnholdableReading(typ, s))
	}
	return sqlerr.New("22P02", "invalid input syntax for type %s: %s",
		NetworkTextTypeName(typ), sqlerr.Quote(s))
}

// networkUnholdableKind and networkUnholdableReading name WHY a
// PostgreSQL-valid inet literal has no room in this column, so the one 0A000
// class can still say which of its two reasons applies: the literal names a
// NETWORK where the type holds a bare address, or it names an address of the
// OTHER FAMILY.
func networkUnholdableKind(typ TypeID, s string) string {
	if networkWrongFamily(typ, s) {
		return "an address of the other family"
	}
	return "a network prefix"
}

func networkUnholdableReading(typ TypeID, s string) string {
	if networkWrongFamily(typ, s) {
		return "an IPv6 address"
	}
	return "a network"
}

func networkWrongFamily(typ TypeID, s string) bool {
	family, _, _, ok := PgInetPton(s)
	if !ok {
		return false
	}
	switch typ {
	case TypeIPv4:
		return family != 0x04
	case TypeIPv6:
		// Never: an IPV6 column holds a v4 address as its v4-mapped form, so
		// the only thing it has no room for is a NETWORK.
		return false
	}
	return false
}

// NetworkTextTypeName is the type PostgreSQL's own message names for this
// column's grammar — `inet` for the three address types, because that is the
// input function the server resolves a comparison against them through.
func NetworkTextTypeName(typ TypeID) string {
	switch typ {
	case TypeIPv4, TypeIPv6, TypeCIDR:
		return "inet"
	case TypeMAC:
		return "macaddr"
	case TypeUUID:
		return "uuid"
	case TypePort:
		return "port"
	case TypeProtocol:
		return "protocol"
	}
	return typ.String()
}

// PgIPv4Address reads one IPv4 literal as an IPV4 column holds it: the int64
// of its four bytes. A HOST-width prefix decorates the address it names
// (`'10.0.0.1/32'` is `'10.0.0.1'` on the server); a NARROWER one names a
// network this type has no room for and is NetTextPrefix, not a syntax error,
// because the text is valid inet.
func PgIPv4Address(s string) (int64, NetTextStatus) {
	addr, bits, ok := PgIPv4Pton(s)
	if !ok {
		// Not a v4 form. If it is nevertheless valid `inet` — every v6
		// spelling is — then the TEXT is not the problem and this column's
		// type is: one class, one code (0A000), the same answer a NETWORK
		// gets. It used to be 22P02 for the wrong family and 0A000 for a
		// prefix, which is one class with two answers (review NT P2).
		if _, _, _, inet := PgInetPton(s); inet {
			return 0, NetTextPrefix
		}
		return 0, NetTextSyntax
	}
	if bits != 32 {
		return 0, NetTextPrefix
	}
	return int64(binary.BigEndian.Uint32(addr[:])), NetTextOK
}

// PgIPv6Address is PgIPv4Address's twin for the 16-byte type. A v4-shaped
// literal is stored as its v4-mapped form, which is what every reader of an
// IPV6 column already renders it back as.
func PgIPv6Address(s string) ([]byte, NetTextStatus) {
	body, mask, cut := strings.Cut(s, "/")
	if !strings.ContainsRune(body, ':') {
		// A v4-shaped body takes the v4 grammar, mask or no mask:
		// `'10.0.0.1/128'` is 22P02 on the server because 128 does not fit a
		// v4 address, and `'010.1.2.3'` is 10.1.2.3 there whether or not a
		// `/32` follows it. Reading the maskless spelling with net.ParseIP
		// instead gave one type two grammars: `'010.1.2.3/32'` was a value
		// and `'010.1.2.3'` was 22P02 (review NT B2).
		n, st := PgIPv4Address(s)
		if st != NetTextOK {
			return nil, st
		}
		var quad [4]byte
		binary.BigEndian.PutUint32(quad[:], uint32(n))
		return net.IP(quad[:]).To16(), NetTextOK
	}
	if cut {
		bits, ok := PgInet6MaskBits(mask)
		if !ok {
			return nil, NetTextSyntax
		}
		if bits != 128 {
			return nil, NetTextPrefix
		}
		s = body
	}
	ip := net.ParseIP(s)
	if ip == nil {
		return nil, NetTextSyntax
	}
	raw := ip.To16()
	if raw == nil {
		return nil, NetTextSyntax
	}
	return raw, NetTextOK
}

// PgInet6MaskBits reads the `/bits` of an inet6 literal exactly as
// PostgreSQL's inet6 input does, and its rule is NOT the v4 one. Measured on
// 17.11: `'::1/0'` and `'::1/64'` are values, `'::1/00'`, `'::1/064'`,
// `'::1/0128'`, `'::1/129'`, `'::1/abc'` and `'::1/'` are all 22P02 — no
// leading zeros ever, digits only, 0-128 — while the v4 side takes
// `'10.0.0.1/031'` as /31 and `'10/008'` as /8. Two parsers, two rules.
func PgInet6MaskBits(mask string) (int, bool) {
	if mask == "" || len(mask) > 3 {
		return 0, false
	}
	if len(mask) > 1 && mask[0] == '0' {
		return 0, false
	}
	bits := 0
	for i := 0; i < len(mask); i++ {
		if mask[i] < '0' || mask[i] > '9' {
			return 0, false
		}
		bits = bits*10 + int(mask[i]-'0')
	}
	if bits > 128 {
		return 0, false
	}
	return bits, true
}

// PgInetPton is the whole inet accept-set, for the one type that keeps the
// prefix: family, the address WITH its host bits, and the prefix length. It is
// the parse half of kernel.CidrSortKey, extracted so the writer validates a
// CIDR value against the same grammar the comparison reads it with — a CIDR
// column stored ANY text at all before this, `'zzz'` included, and a value
// with no place in the address order sorts and groups as raw bytes.
func PgInetPton(s string) (family byte, full []byte, ones int, ok bool) {
	t := s
	if !strings.ContainsRune(t, '/') {
		// A bare address is a host route. ':' is in every IPv6 text form and
		// in no IPv4 one, the same split net.ParseCIDR itself makes.
		if strings.ContainsRune(t, ':') {
			t += "/128"
		} else {
			t += "/32"
		}
	}
	if body, mask, cut := strings.Cut(s, "/"); cut && strings.ContainsRune(body, ':') {
		// inet6's mask grammar is not Go's: digits only, no leading zeros,
		// 0-128. net.ParseCIDR takes `'::1/064'`, which is 22P02 on the
		// server and is refused one function away in PgIPv6Address — one
		// grammar, two readings (review NT B3).
		if _, ok := PgInet6MaskBits(mask); !ok {
			return 0, nil, 0, false
		}
	}
	ip, ipnet, err := net.ParseCIDR(t)
	if err != nil || ipnet == nil {
		// PostgreSQL's ABBREVIATED v4 forms, which Go's parser does not read:
		// '10/8', '192.168/16', '10.1/8', and the leading zeros in
		// '010.1.2.3' (#627).
		a, bits, pok := PgIPv4Pton(s)
		if !pok {
			return 0, nil, 0, false
		}
		out := make([]byte, 4)
		copy(out, a[:])
		return 0x04, out, bits, true
	}
	n, size := ipnet.Mask.Size()
	if size == net.IPv4len*8 {
		if v4 := ip.To4(); v4 != nil {
			return 0x04, v4, n, true
		}
		return 0, nil, 0, false
	}
	if v6 := ip.To16(); v6 != nil {
		return 0x06, v6, n, true
	}
	return 0, nil, 0, false
}

// PgMACPton reads one macaddr literal exactly as PostgreSQL 17.11's macaddr_in
// does. That function is a LIST OF sscanf PATTERNS, not a separator count, and
// modelling it any other way gets both directions wrong: `'a:b:c:d:e:f'` and
// `'08002b010203'` are values there (this engine refused them), while
// `'08.00.2b.01.02.03'` and `'08002b:01:0203'` are 22P02 (a separator count
// took them). Each conversion skips leading whitespace and accepts an optional
// sign and an optional 0x prefix, which is C's `%x`; an octet outside 0..255 is
// 22003 `invalid octet value`, a distinct answer from a syntax error.
func PgMACPton(s string) ([6]byte, NetTextStatus) {
	var out [6]byte
	for _, p := range macaddrPatterns {
		vals, ok := scanHexFields(s, p.width, p.seps)
		if !ok {
			continue
		}
		for i, v := range vals {
			if v < 0 || v > 255 {
				return out, NetTextRange
			}
			out[i] = byte(v)
		}
		return out, NetTextOK
	}
	return out, NetTextSyntax
}

// macaddrPattern is one of macaddr_in's sscanf formats: six hex conversions of
// the same width (0 = `%x`, unbounded; 2 = `%2x`) with the literal separator
// seps[i] between conversion i and i+1, NUL meaning none.
type macaddrPattern struct {
	width int
	seps  string
}

// macaddrPatterns is macaddr_in's list, in its own order. Package-level, not a
// literal inside PgMACPton: this runs once per MAC value on the INGEST path,
// and a slice literal there is eight allocations per value written.
var macaddrPatterns = [...]macaddrPattern{
	{0, ":::::"},
	{0, "-----"},
	{2, "\x00\x00:\x00\x00"},
	{2, "\x00\x00-\x00\x00"},
	{2, "\x00.\x00.\x00"},
	{2, "\x00-\x00-\x00"},
	{2, "\x00\x00\x00\x00\x00"},
}

// scanHexFields is one sscanf pattern: six hex conversions of the given width
// with the literal separator seps[f] between conversion f and f+1 (NUL means
// none), then optional trailing whitespace and nothing else. The result is an
// ARRAY rather than a slice so a failed pattern attempt — six of the seven, for
// most inputs — costs no allocation on the ingest path.
func scanHexFields(s string, width int, seps string) ([6]int64, bool) {
	var vals [6]int64
	i := 0
	for f := 0; f < len(vals); f++ {
		v, next, ok := scanHex(s, i, width)
		if !ok {
			return vals, false
		}
		vals[f] = v
		i = next
		if f == len(vals)-1 {
			break
		}
		if sep := seps[f]; sep != 0 {
			if i >= len(s) || s[i] != sep {
				return vals, false
			}
			i++
		}
	}
	for i < len(s) && isSpaceByte(s[i]) {
		i++
	}
	return vals, i == len(s)
}

// scanHex is C's `%x` / `%nx` at offset i: leading whitespace, an optional
// sign, an optional 0x prefix, then hex digits. It reports the value, the
// offset past it, and whether a conversion happened at all. A value past the
// carrier saturates NEGATIVE so the caller's range check refuses it rather
// than wrapping into a plausible octet.
//
// The WIDTH counts every character the conversion consumes — the sign and the
// prefix included — which is C's rule and not an implementation detail: with
// the width counting digits only, `'08-002b010203'` matched the `%2x`x6
// pattern (`08`, `-0`+an extra digit, …) and this engine answered a MAC for
// text PostgreSQL refuses, at six comparison sites on three arms
// (coordinator.TestANetworkLiteralHasOneDispositionAtEverySite, the #579 pin).
func scanHex(s string, i, width int) (int64, int, bool) {
	for i < len(s) && isSpaceByte(s[i]) {
		i++
	}
	start := i
	room := func(n int) bool { return width == 0 || i-start+n <= width }
	neg := false
	if room(1) && i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	if room(3) && i+2 < len(s) && s[i] == '0' && (s[i+1] == 'x' || s[i+1] == 'X') &&
		isHexByte(s[i+2]) {
		i += 2
	}
	digits := i
	// THIRTY-TWO BITS, wrapping, because that is where C's `%x` puts the
	// value: an `int`. PostgreSQL 17.11 answers 00:00:00:00:00:00 for
	// `'100000000:0:0:0:0:0'` and 01:00:00:00:00:00 for `'100000001:…'` —
	// the truncation is visible in the value, not only in the accept-set, and
	// saturating instead refused a spelling the server takes (review NT P3).
	// C's `%x` converts with strtoul FIRST and narrows to `int` after, so
	// there are TWO truncations and they are not the same one: a value past
	// 2^64-1 saturates to ULONG_MAX (`'10000000000000000:0:0:0:0:0'` is
	// 22003 on 17.11, not 00:00:…), and only what survives that is taken
	// mod 2^32. Wrapping a uint32 alone answered a MAC for five spellings the
	// server refuses (review NT round 2, B1).
	var u64 uint64
	sat := false
	for i < len(s) && isHexByte(s[i]) && room(1) {
		if u64 > (math.MaxUint64-uint64(hexValue(s[i])))/16 {
			sat = true
		}
		u64 = u64*16 + uint64(hexValue(s[i]))
		i++
	}
	if i == digits {
		return 0, i, false
	}
	// The SIGN is strtoul's too, and it is applied INSIDE the 64-bit
	// accumulation — but not on top of a saturated one: an overflowing subject
	// sequence returns ULONG_MAX whether or not a minus preceded it, which is
	// why `'-10000000000000000:0:0:0:0:0'` is 22003 on 17.11 and
	// `'-1000000000000000:…'` (no overflow) is 00:00:00:00:00:00.
	if sat {
		u64 = math.MaxUint64 // strtoul's own answer for an overflowing subject
	} else if neg {
		u64 = -u64
	}
	return int64(int32(uint32(u64))), i, true
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

func isHexByte(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func hexValue(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		return c - 'A' + 10
	}
}

// PgUUIDPton reads one uuid literal exactly as PostgreSQL 17.11's uuid_in
// does: 32 hex digits in any case, optionally wrapped in BOTH braces, with a
// hyphen permitted after any group of four digits and nowhere else. Measured:
// `'a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11'` and
// `'a0eebc99-9c0b4ef8-bb6d6bb9bd380a11'` are values (this engine's CAST refused
// them), while `'a-0eebc99…'`, a doubled or trailing hyphen, one brace, and any
// surrounding whitespace are 22P02 (the comparison kernels took the first).
func PgUUIDPton(s string) ([16]byte, NetTextStatus) {
	var raw [16]byte
	t := s
	if len(t) >= 2 && t[0] == '{' && t[len(t)-1] == '}' {
		t = t[1 : len(t)-1]
	} else if strings.ContainsAny(t, "{}") {
		return raw, NetTextSyntax
	}
	n := 0
	prevHex := false
	for i := 0; i < len(t); i++ {
		c := t[i]
		if c == '-' {
			if !prevHex || n == 0 || n >= 32 || n%4 != 0 {
				return raw, NetTextSyntax
			}
			prevHex = false
			continue
		}
		if !isHexByte(c) || n == 32 {
			return raw, NetTextSyntax
		}
		if n%2 == 0 {
			raw[n/2] = hexValue(c) << 4
		} else {
			raw[n/2] |= hexValue(c)
		}
		n++
		prevHex = true
	}
	if n != 32 {
		return raw, NetTextSyntax
	}
	return raw, NetTextOK
}

// NetworkIntBounds is the RANGE a PORT or a PROTOCOL carries, in one place.
// It was written out three times — the INSERT VALUES door, the assignment
// converter and (not at all) the CAST — and the three disagreed: `'65536'` was
// refused at one door and stored at another.
func NetworkIntBounds(typ TypeID) (lo, hi int64) {
	switch typ {
	case TypePort:
		return 0, 65535
	case TypeProtocol:
		return 0, 255
	}
	return math.MinInt32, math.MaxInt32
}

// NetworkIntRangeError is that bound's refusal, in the wording all three doors
// now share.
func NetworkIntRangeError(typ TypeID, n int64) error {
	lo, hi := NetworkIntBounds(typ)
	return sqlerr.New("22003", "%s value %d out of range [%d, %d]", typ, n, lo, hi)
}

// PgPortText reads a PORT literal. The type's documented text form is a
// decimal number in 0..65535 (docs/data-types.md) and service NAMES are
// deliberately not resolved here: `port_name()` is the function that names a
// port, and a text form this parser does not produce is a text form it must
// not read back.
func PgPortText(s string) (int32, NetTextStatus) {
	return boundedDecimal(s, 0, 65535)
}

// PgProtocolText reads a PROTOCOL literal: a decimal number in 0..255, or the
// IANA protocol NAME case-insensitively — which is the type's own text form,
// the one `protocol_name()` produces, so `CAST(CAST(p AS TEXT) AS PROTOCOL)`
// round-trips for every value that has a name (#986).
func PgProtocolText(s string) (int32, NetTextStatus) {
	if n, ok := ProtocolNumberFromName(s); ok {
		return n, NetTextOK
	}
	return boundedDecimal(s, 0, 255)
}

// DecimalIntegerText is the TEXT GRAMMAR of PORT and PROTOCOL, without either
// type's range: surrounding whitespace (which PostgreSQL's own integer input
// ignores), an optional sign, decimal digits and nothing else.
//
// It is deliberately NARROWER than int4's input function, which also reads
// `0x1bb`, `0o17`, `0b101` and `1_000`. These two types do not have int4's
// text form — PROTOCOL's includes a NAME, which int4's certainly does not —
// and the one place that form is decided has to be one place: the CAST read
// int4's grammar through kernel.IntLitText and took `'0x1bb'` for a PORT the
// writer refused, which is the one-grammar claim failing one type over
// (review NT P4). The two DOMAINS still differ by door and that is ADR-0012's
// recorded split, not this function's business.
func DecimalIntegerText(s string) (int64, NetTextStatus) {
	t := strings.TrimSpace(s)
	if t == "" {
		return 0, NetTextSyntax
	}
	i := 0
	neg := false
	if t[0] == '+' || t[0] == '-' {
		neg = t[0] == '-'
		i++
	}
	if i == len(t) {
		return 0, NetTextSyntax
	}
	var v int64
	for ; i < len(t); i++ {
		if t[i] < '0' || t[i] > '9' {
			return 0, NetTextSyntax
		}
		v = v*10 + int64(t[i]-'0')
		if v > 1<<40 {
			v = 1 << 40 // far past any int4; the caller answers 22003
		}
	}
	if neg {
		v = -v
	}
	return v, NetTextOK
}

func boundedDecimal(s string, lo, hi int64) (int32, NetTextStatus) {
	v, st := DecimalIntegerText(s)
	if st != NetTextOK {
		return 0, st
	}
	if v < lo || v > hi {
		return 0, NetTextRange
	}
	return int32(v), NetTextOK
}

// protocolNumToName is the ONE IANA table this engine has. It lives here
// because both ends read it: `protocol_name()` in the expression layer renders
// a PROTOCOL value through it, and the type's text grammar above reads a name
// back through it. Two tables would agree only by inspection (the argument
// #966 already made for the TCP flag names).
var protocolNumToName = map[int64]string{
	1: "icmp", 2: "igmp", 6: "tcp", 17: "udp", 41: "ipv6",
	47: "gre", 50: "esp", 51: "ah", 58: "icmpv6", 89: "ospf",
	103: "pim", 132: "sctp",
}

var protocolNameToNum = func() map[string]int32 {
	m := make(map[string]int32, len(protocolNumToName)+1)
	for num, name := range protocolNumToName {
		m[name] = int32(num)
	}
	// IANA's own spelling for 58, beside the one this engine prints. Reading
	// a name it does not print costs nothing and refusing it would refuse the
	// name every other tool writes.
	m["ipv6-icmp"] = 58
	return m
}()

// ProtocolNameFromNumber is the protocol NAME an IP protocol number has, or
// ok=false when the table has none.
func ProtocolNameFromNumber(n int64) (string, bool) {
	name, ok := protocolNumToName[n]
	return name, ok
}

// ProtocolNumberFromName is its inverse, case-insensitively.
func ProtocolNumberFromName(name string) (int32, bool) {
	n, ok := protocolNameToNum[strings.ToLower(strings.TrimSpace(name))]
	return n, ok
}
