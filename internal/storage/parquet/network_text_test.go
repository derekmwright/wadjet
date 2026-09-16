package parquet

import "testing"

// The accept-sets below are MEASURED on PostgreSQL 17.11 (`postgres:17-alpine`,
// initdb --locale=C), one `SELECT <literal>::<type>::text` per row, not
// recalled from documentation. The two grammars with real combinatorics were
// also compared over a GENERATED DOMAIN rather than samples: every single- and
// double-separator split of the twelve MAC digits with each of `:`, `-` and
// `.`, plus the sign/prefix/whitespace forms (236 cells), and every single- and
// double-hyphen placement in a UUID's 32 digits, braced and upper-cased (1795
// cells). Zero divergences from the server at the tip; the rows here are the
// boundaries that domain is built around. Each table is the whole answer for its type:
// what the server takes, what it canonicalizes the value to, and what it
// refuses with which SQLSTATE class. They are the one grammar every boundary
// reads (#1092, #627, #986).

func TestMACTextGrammarIsPostgresMacaddr(t *testing.T) {
	for _, c := range []struct {
		in   string
		want string // "" = refused
		st   NetTextStatus
	}{
		// The seven spellings macaddr_in's sscanf patterns take.
		{"08:00:2b:01:02:03", "08002b010203", NetTextOK},
		{"08-00-2b-01-02-03", "08002b010203", NetTextOK},
		{"08002b:010203", "08002b010203", NetTextOK},
		{"08002b-010203", "08002b010203", NetTextOK},
		{"0800.2b01.0203", "08002b010203", NetTextOK},
		{"0800-2b01-0203", "08002b010203", NetTextOK},
		{"08002b010203", "08002b010203", NetTextOK},
		// Case, and the variable-width groups `%x` reads.
		{"08:00:2B:01:02:03", "08002b010203", NetTextOK},
		{"08002B010203", "08002b010203", NetTextOK},
		{"8:0:2b:1:2:3", "08002b010203", NetTextOK},
		{"008:0:2b:1:2:3", "08002b010203", NetTextOK},
		{"8-0-2b-1-2-3", "08002b010203", NetTextOK},
		{"0x08:00:2b:01:02:03", "08002b010203", NetTextOK},
		{"0X08:00:2b:01:02:03", "08002b010203", NetTextOK},
		{"08:00:2b:01:02:0x3", "08002b010203", NetTextOK},
		{"+8:0:2b:1:2:3", "08002b010203", NetTextOK},
		{"08:00:2b:01:02:0", "08002b010200", NetTextOK},
		// Whitespace: skipped before every conversion and after the last.
		{" 08:00:2b:01:02:03", "08002b010203", NetTextOK},
		{"08:00:2b:01:02:03 ", "08002b010203", NetTextOK},
		{"\t08:00:2b:01:02:03", "08002b010203", NetTextOK},
		{"08: 00:2b:01:02:03", "08002b010203", NetTextOK},
		{"0800. 2b01.0203", "08002b010203", NetTextOK},
		{"08002b: 010203", "08002b010203", NetTextOK},
		// The fixed-width patterns read at most two digits per conversion,
		// which is why these two are values and name the bytes they do.
		{"0800.2b01.203", "08002b012003", NetTextOK},
		{"800.2b01.0203", "80002b010203", NetTextOK},
		// A group regrouping no pattern spells.
		{"0800:2b01:0203", "", NetTextSyntax},
		{"08002b:01:0203", "", NetTextSyntax},
		{"08:002b:010203", "", NetTextSyntax},
		{"0800.2b01.0203.0405", "", NetTextSyntax},
		{"0800.2b010203", "", NetTextSyntax},
		{"08.00.2b.01.02.03", "", NetTextSyntax},
		{"0800-2b01.0203", "", NetTextSyntax},
		{"0800.2b01-0203", "", NetTextSyntax},
		{"08002b.010203", "", NetTextSyntax},
		{"08 :00:2b:01:02:03", "", NetTextSyntax},
		// The WIDTH of a `%2x` conversion counts the SIGN it consumes, so
		// this one matches no pattern: `%2x`x6 reads `08`, `-0`, `02`, `b0`,
		// `10`, `20` and leaves a digit behind. With the width counting
		// digits only it matched, and this engine answered a MAC at six
		// comparison sites on three arms for text the server refuses
		// (coordinator.TestANetworkLiteralHasOneDispositionAtEverySite).
		{"08-002b010203", "", NetTextSyntax},
		{"0800-2b010203", "", NetTextSyntax},
		{"-08002b010203", "", NetTextSyntax},
		// Too few, too many, garbage, trailing junk.
		{"08:00:2b:01:02", "", NetTextSyntax},
		{"08:00:2b:01:02:03:04", "", NetTextSyntax},
		{"zz:00:2b:01:02:03", "", NetTextSyntax},
		{"08:00:2b:01:02:03x", "", NetTextSyntax},
		{"08002b:010203extra", "", NetTextSyntax},
		{"08:00:2b:01:02:03:", "", NetTextSyntax},
		{":08:00:2b:01:02:03", "", NetTextSyntax},
		{"", "", NetTextSyntax},
		// C's `%x` converts with strtoul and narrows to `int` after, so it
		// truncates TWICE and the two are different: a subject sequence past
		// 2^64-1 SATURATES to ULONG_MAX, which narrows to -1 and is 22003,
		// and only what survives that is taken mod 2^32. Modelling the second
		// alone refused three spellings 17.11 takes (review NT P3); modelling
		// it WITHOUT the first accepted five it refuses, with a value nobody
		// wrote (review NT round 2, B1). The boundary is the VALUE, not the
		// digit count — twenty-two leading zeros are still the value they
		// precede — and the SIGN belongs to strtoul, which returns ULONG_MAX
		// for an overflowing subject whether or not a minus preceded it.
		{"100000000:0:0:0:0:0", "000000000000", NetTextOK},
		{"100000001:0:0:0:0:0", "010000000000", NetTextOK},
		{"10000000000:0:0:0:0:0", "000000000000", NetTextOK},
		{"-100000000:0:0:0:0:0", "000000000000", NetTextOK},
		{"0x100000001:0:0:0:0:0", "010000000000", NetTextOK},
		{"1000000ff:0:0:0:0:0", "ff0000000000", NetTextOK},
		{"-100000001:0:0:0:0:0", "", NetTextRange},
		{"100000100:0:0:0:0:0", "", NetTextRange},
		// The FIRST truncation: past 2^64-1, measured cell by cell on 17.11.
		{"1000000000000000:0:0:0:0:0", "000000000000", NetTextOK},       // 2^60, no overflow
		{"0000000000000000000001:0:0:0:0:0", "010000000000", NetTextOK}, // 22 digits, value 1
		{"-1000000000000000:0:0:0:0:0", "000000000000", NetTextOK},      // negated, no overflow
		{"10000000000000000:0:0:0:0:0", "", NetTextRange},               // 2^64
		{"10000000000000001:0:0:0:0:0", "", NetTextRange},
		{"100000000000000000:0:0:0:0:0", "", NetTextRange},
		{"fffffffffffffffff:0:0:0:0:0", "", NetTextRange},
		{"1000000000000000000000:0:0:0:0:0", "", NetTextRange},
		{"100000000000000000000000000000ff:0:0:0:0:0", "", NetTextRange},
		{"-10000000000000000:0:0:0:0:0", "", NetTextRange}, // the sign does not undo it
		{"0:0:0:0:0:10000000000000000", "", NetTextRange},  // the LAST field too
		// An octet the type cannot carry is 22003, a different answer.
		{"08:00:2b:01:02:100", "", NetTextRange},
		{"08:00:2b:01:02:-3", "", NetTextRange},
		{"-8:0:2b:1:2:3", "", NetTextRange},
		{"ffffffffffffffff:00:2b:01:02:03", "", NetTextRange},
	} {
		got, st := PgMACPton(c.in)
		if st != c.st {
			t.Errorf("PgMACPton(%q) status = %v, want %v (PostgreSQL 17.11)", c.in, st, c.st)
			continue
		}
		if st != NetTextOK {
			continue
		}
		if hexOf(got[:]) != c.want {
			t.Errorf("PgMACPton(%q) = %s, want %s (PostgreSQL 17.11)", c.in, hexOf(got[:]), c.want)
		}
	}
}

func TestUUIDTextGrammarIsPostgresUUID(t *testing.T) {
	const v = "a0eebc999c0b4ef8bb6d6bb9bd380a11"
	for _, c := range []struct {
		in   string
		want string
	}{
		{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", v},
		{"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", v},
		{"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}", v},
		{"a0eebc999c0b4ef8bb6d6bb9bd380a11", v},
		{"{a0eebc999c0b4ef8bb6d6bb9bd380a11}", v},
		// A hyphen after ANY group of four, which is the half this engine's
		// CAST refused.
		{"a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11", v},
		{"a0eebc99-9c0b4ef8-bb6d6bb9bd380a11", v},
		{"a0ee-bc999c0b-4ef8bb6d6bb9bd380a11", v},
		{"{a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11}", v},
		// A hyphen anywhere else, which the comparison kernels accepted.
		{"a-0eebc999c0b4ef8bb6d6bb9bd380a11", ""},
		{"a0e-ebc99-9c0b-4ef8-bb6d-6bb9bd380a11", ""},
		{"a0ee--bc999c0b4ef8bb6d6bb9bd380a11", ""},
		{"-a0eebc999c0b4ef8bb6d6bb9bd380a11", ""},
		{"a0eebc999c0b4ef8bb6d6bb9bd380a11-", ""},
		{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11-", ""},
		// One brace, no braces around nothing, underscores, whitespace, junk.
		{"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", ""},
		{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}", ""},
		{"{}", ""},
		{"{-}", ""},
		{"a0eebc99_9c0b_4ef8_bb6d_6bb9bd380a11", ""},
		{" a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", ""},
		{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 ", ""},
		{"zzeebc99-9c0b-4ef8-bb6d-6bb9bd380a11", ""},
		{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1", ""},
		{"0x0eebc999c0b4ef8bb6d6bb9bd380a11", ""},
		{"", ""},
	} {
		raw, st := PgUUIDPton(c.in)
		if c.want == "" {
			if st == NetTextOK {
				t.Errorf("PgUUIDPton(%q) accepted; PostgreSQL 17.11 answers 22P02", c.in)
			}
			continue
		}
		if st != NetTextOK {
			t.Errorf("PgUUIDPton(%q) refused; PostgreSQL 17.11 answers a value", c.in)
			continue
		}
		if hexOf(raw[:]) != c.want {
			t.Errorf("PgUUIDPton(%q) = %s, want %s", c.in, hexOf(raw[:]), c.want)
		}
	}
}

// TestNetworkTextValueIsOneGrammarPerType is the writer's entry point held to
// the same table the comparison sites read: a literal this engine stores is a
// literal PostgreSQL stores, and a literal it refuses is one PostgreSQL
// refuses. IPv4/IPv6 keep the one recorded divergence — a NETWORK is 0A000
// rather than a value, because the type holds a bare address (ADR-0012 item 5).
func TestNetworkTextValueIsOneGrammarPerType(t *testing.T) {
	for _, c := range []struct {
		typ TypeID
		in  string
		st  NetTextStatus
	}{
		{TypeIPv4, "10.0.0.1", NetTextOK},
		{TypeIPv4, "10.0.0.1/32", NetTextOK},
		{TypeIPv4, "010.1.2.3", NetTextOK},
		{TypeIPv4, "10.1.2.3.", NetTextOK},
		{TypeIPv4, "10/8", NetTextPrefix},
		{TypeIPv4, "192.168", NetTextSyntax},
		{TypeIPv4, "10.0.0.256", NetTextSyntax},
		{TypeIPv4, "zzz", NetTextSyntax},
		// A PostgreSQL-valid inet literal of the OTHER FAMILY is the SAME
		// class as a network: the text is fine and this column has no room
		// for it, which is 0A000 (review NT P2, ADR-0012 item 5).
		{TypeIPv4, "::1", NetTextPrefix},
		{TypeIPv4, "2001:db8::1", NetTextPrefix},
		{TypeIPv4, "::ffff:1.2.3.4", NetTextPrefix},
		{TypeIPv6, "10/8", NetTextPrefix},
		{TypeIPv6, "192.168/16", NetTextPrefix},
		// An IPV6 column HOLDS a v4 address, as its v4-mapped form, with or
		// without the `/32` PostgreSQL treats as decoration — one grammar,
		// not two (review NT B2).
		{TypeIPv6, "010.1.2.3", NetTextOK},
		{TypeIPv6, "010.1.2.3/32", NetTextOK},
		{TypeIPv6, "10.1.2.3.", NetTextOK},
		{TypeIPv6, "10.0.0.01", NetTextOK},
		// inet6's mask grammar reaches the CIDR type too, which read Go's
		// instead and stored `'::1/064'` — text that names no inet (B3).
		{TypeCIDR, "::1/064", NetTextSyntax},
		{TypeCIDR, "::1/00", NetTextSyntax},
		{TypeCIDR, "::1/128", NetTextOK},
		{TypeIPv6, "2001:db8::1", NetTextOK},
		{TypeIPv6, "::1", NetTextOK},
		{TypeIPv6, "2001:DB8::1", NetTextOK},
		{TypeIPv6, "::ffff:10.0.0.1", NetTextOK},
		{TypeIPv6, "2001:db8::1/128", NetTextOK},
		{TypeIPv6, "2001:db8::1/64", NetTextPrefix},
		{TypeIPv6, "::1/064", NetTextSyntax},
		{TypeIPv6, "10.0.0.1/128", NetTextSyntax},
		{TypeIPv6, "zzz", NetTextSyntax},
		{TypeCIDR, "192.168.1.0/24", NetTextOK},
		{TypeCIDR, "10.0.0.1/32", NetTextOK},
		{TypeCIDR, "192.168/16", NetTextOK},
		{TypeCIDR, "10.0.0.1", NetTextOK},
		{TypeCIDR, "2001:db8::1/64", NetTextOK},
		{TypeCIDR, "192.168", NetTextSyntax},
		{TypeCIDR, "zzz", NetTextSyntax},
		{TypeCIDR, "10.0.0.1/33", NetTextSyntax},
		{TypeMAC, "aa-bb-cc-dd-ee-ff", NetTextOK},
		{TypeMAC, "aabbccddeeff", NetTextOK},
		{TypeMAC, "aa:bb:cc:dd:ee", NetTextSyntax},
		{TypeUUID, "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}", NetTextOK},
		{TypeUUID, "a-0eebc999c0b4ef8bb6d6bb9bd380a11", NetTextSyntax},
		{TypePort, "443", NetTextOK},
		{TypePort, "0", NetTextOK},
		{TypePort, "65535", NetTextOK},
		{TypePort, "65536", NetTextRange},
		{TypePort, "-1", NetTextRange},
		{TypePort, "https", NetTextSyntax},
		{TypePort, "", NetTextSyntax},
		{TypeProtocol, "6", NetTextOK},
		{TypeProtocol, "udp", NetTextOK},
		{TypeProtocol, "TCP", NetTextOK},
		{TypeProtocol, "ipv6-icmp", NetTextOK},
		{TypeProtocol, "255", NetTextOK},
		{TypeProtocol, "256", NetTextRange},
		{TypeProtocol, "zzz", NetTextSyntax},
	} {
		_, st, ok := NetworkTextValue(c.typ, c.in)
		if !ok {
			t.Fatalf("NetworkTextValue(%s, %q): no grammar", c.typ, c.in)
		}
		if st != c.st {
			t.Errorf("NetworkTextValue(%s, %q) = %v, want %v", c.typ, c.in, st, c.st)
		}
	}
}

// TestProtocolNameRoundTripsThroughItsOwnText is #986's rule: every value the
// engine PRINTS as a name reads back as that value.
func TestProtocolNameRoundTripsThroughItsOwnText(t *testing.T) {
	for n := int64(0); n <= 255; n++ {
		name, ok := ProtocolNameFromNumber(n)
		if !ok {
			continue
		}
		got, st := PgProtocolText(name)
		if st != NetTextOK || int64(got) != n {
			t.Errorf("PgProtocolText(%q) = %d/%v, want %d", name, got, st, n)
		}
	}
}

func hexOf(b []byte) string {
	const d = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, d[c>>4], d[c&0x0f])
	}
	return string(out)
}
