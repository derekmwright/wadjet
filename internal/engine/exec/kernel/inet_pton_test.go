package kernel

import (
	"net"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #627: every cell measured on live PostgreSQL 17.11.
//
// The literal beside a network column is read by the INET parser — including
// beside a `cidr` column, because cidr has no operators of its own and the
// server resolves the pair through `=(inet, inet)`. So the table below is
// `SELECT '<in>'::inet`, not `::cidr`: the two grammars disagree about half of
// these inputs, and taking the cidr one made `cd = '239'` answer a row where
// the server raises 22P02 (round-2 review P-6) while `cd = '1/0'` refused
// where the server answers.
func TestNetworkLiteralGrammarIsPostgresInet(t *testing.T) {
	for _, c := range []struct {
		in   string
		want string // PostgreSQL's own `::inet` output
	}{
		{"10.1.2.3", "10.1.2.3/32"},
		{"010.1.2.3", "10.1.2.3/32"},
		{"0010.0020.0030.0040", "10.20.30.40/32"},
		{"10.1.2.3.", "10.1.2.3/32"},
		// With a mask, an abbreviation is a value — and the HOST BITS ARE
		// KEPT, which is the half of this the cidr grammar would refuse.
		{"10/8", "10.0.0.0/8"},
		{"10/15", "10.0.0.0/15"},
		{"010/8", "10.0.0.0/8"},
		{"192.168/16", "192.168.0.0/16"},
		{"10.1/8", "10.1.0.0/8"},
		{"10.1/23", "10.1.0.0/23"},
		{"1/0", "1.0.0.0/0"},
		{"255/1", "255.0.0.0/1"},
		{"172.31/12", "172.31.0.0/12"},
		{"10.1.2/24", "10.1.2.0/24"},
		{"10.0.0.1/8", "10.0.0.1/8"},
		{"0/0", "0.0.0.0/0"},
		{"224/4", "224.0.0.0/4"},
		{"239/8", "239.0.0.0/8"},
		// Mask digits are digits, however many there are, and one trailing
		// dot on the body is ignored.
		{"10/008", "10.0.0.0/8"},
		{"10/0008", "10.0.0.0/8"},
		{"1.2.3.4/00", "1.2.3.4/0"},
		{"10./8", "10.0.0.0/8"},
		{"10.1./16", "10.1.0.0/16"},
	} {
		t.Run(c.in, func(t *testing.T) {
			addr, bits, ok := parquet.PgIPv4Pton(c.in)
			if !ok {
				t.Fatalf("parquet.PgIPv4Pton(%q) refused; PostgreSQL 17.11 reads it as %s", c.in, c.want)
			}
			got := net.IP(addr[:]).String() + "/" + itoa(bits)
			if got != c.want {
				t.Errorf("parquet.PgIPv4Pton(%q) = %s, PostgreSQL 17.11 says %s", c.in, got, c.want)
			}
			// One key for one value: the abbreviated spelling and the address
			// it names must key identically, which is what makes
			// `cd = '10/8'` find the row holding '10.0.0.0/8'.
			ka, oka := CidrSortKey(c.in)
			kb, okb := CidrSortKey(c.want)
			if !oka || !okb || ka != kb {
				t.Errorf("CidrSortKey(%q) and CidrSortKey(%q) are two keys for one value", c.in, c.want)
			}
		})
	}

	// Refused on the server too, measured the same way. The first group is
	// the ABBREVIATION WITHOUT A MASK — the cidr type's address inference,
	// which inet does not have and which no comparison in this engine
	// resolves through. It is the round-2 P-6 superset, and it is the half
	// that made these four accepts look like a regression.
	for _, in := range []string{
		"10", "10.1", "10.1.2", "192.168", "239", "224", "0", "255", "010.1", "00010",
		// A mask that names a byte the literal never wrote.
		"10/16", "10/32", "10.1/24", "10.1.2/32", "10/016",
		// The cidr-only notations: hex input is `inet_cidr_pton_ipv4`'s.
		"0x10", "0x0a", "0xc0a80001", "0x10/8",
		// And the shapes both parsers refuse.
		"10.", "10..1", "256.1", "10.1.2.3.4", "0x0a.1", "10/33", " 10/8", "10 /8", "10/8 ",
		"", "/8", "zzz", "10.1.2.-3", "1e2", "10/", "10/x", "10/+8", "+10/8",
	} {
		t.Run("refused_"+in, func(t *testing.T) {
			if _, _, ok := parquet.PgIPv4Pton(in); ok {
				t.Errorf("parquet.PgIPv4Pton(%q) accepted; PostgreSQL 17.11 raises 22P02", in)
			}
		})
	}
}

// The whole domain, not samples. Two claims, each measured over every value
// rather than at a point:
//
//	SELECT count(*) FROM generate_series(0,255) i
//	 WHERE (i::text)::inet IS NOT NULL                      -> 0 accepted
//	SELECT i, (i::text || '/8')::inet FROM generate_series(0,255) i
//	                                                        -> i.0.0.0/8, all 256
//
// The maskless half is what round 1 got backwards: it read the CIDR type's
// classful table (`'239'::cidr` is 239.0.0.0/8) as the grammar of a
// comparison, and no comparison resolves through it. All 256 are 22P02 on
// inet, and this asserts every one of them.
func TestEveryFirstOctetFollowsPostgresInet(t *testing.T) {
	for i := 0; i < 256; i++ {
		in := strconv.Itoa(i)
		if _, _, ok := parquet.PgIPv4Pton(in); ok {
			t.Errorf("PgIPv4Pton(%q) accepted; PostgreSQL 17.11 raises "+
				"`invalid input syntax for type inet: %q`", in, in)
		}
		// With a mask the same octet is a value, and the mask is kept as
		// written — no classful inference in either direction.
		addr, bits, ok := parquet.PgIPv4Pton(in + "/8")
		if !ok || int(addr[0]) != i || addr[1] != 0 || addr[2] != 0 || addr[3] != 0 || bits != 8 {
			t.Errorf("PgIPv4Pton(%q) = %v/%d (ok=%v), PostgreSQL says %d.0.0.0/8",
				in+"/8", addr, bits, ok, i)
		}
		if _, bits, ok := parquet.PgIPv4Pton(in + "/15"); !ok || bits != 15 {
			t.Errorf("PgIPv4Pton(%q) refused or masked /%d; PostgreSQL says %d.0.0.0/15",
				in+"/15", bits, i)
		}
		if _, _, ok := parquet.PgIPv4Pton(in + "/16"); ok {
			t.Errorf("PgIPv4Pton(%q) accepted; PostgreSQL raises 22P02 — one octet "+
				"cannot carry a /16", in+"/16")
		}
		if _, bits, ok := parquet.PgIPv4Pton("10." + in + "/23"); !ok || bits != 23 {
			t.Errorf("PgIPv4Pton(%q) refused or masked /%d; PostgreSQL says 10.%d.0.0/23",
				"10."+in+"/23", bits, i)
		}
		if _, _, ok := parquet.PgIPv4Pton("10." + in + "/24"); ok {
			t.Errorf("PgIPv4Pton(%q) accepted; PostgreSQL raises 22P02", "10."+in+"/24")
		}
	}
}

// The octet-count × mask grid, transcribed from
//
//	SELECT (b || '/' || bits)::inet FROM (VALUES (1,'10'),(2,'10.1'),
//	       (3,'10.1.2'),(4,'10.1.2.3')) body(k,b), generate_series(0,33) bits
//
// 136 cells. The boundary moves by exactly eight bits per written octet and
// stops at 32, and it is a REFUSAL boundary in the direction that matters: a
// parser that ignored it would read `'10/32'` as 10.0.0.0/32, which the server
// does not accept at all.
func TestMaskMayNotNameAByteTheLiteralDidNotWrite(t *testing.T) {
	// One row per octet count; the string is masks 0..33, 'y' = accepted.
	for _, c := range []struct {
		body string
		want string
	}{
		{"10", "yyyyyyyyyyyyyyyynnnnnnnnnnnnnnnnnn"},
		{"10.1", "yyyyyyyyyyyyyyyyyyyyyyyynnnnnnnnnn"},
		{"10.1.2", "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyynn"},
		{"10.1.2.3", "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyn"},
	} {
		for m := 0; m <= 33; m++ {
			in := c.body + "/" + strconv.Itoa(m)
			_, bits, ok := parquet.PgIPv4Pton(in)
			if want := c.want[m] == 'y'; ok != want {
				t.Errorf("PgIPv4Pton(%q) accepted=%v, PostgreSQL 17.11 accepted=%v", in, ok, want)
			} else if ok && bits != m {
				t.Errorf("PgIPv4Pton(%q) masks /%d, PostgreSQL masks /%d", in, bits, m)
			}
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [3]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// A site that does NOT know the column's type must not read a MASKED
// abbreviation as an address, because under PostgreSQL's own rules the same
// text is a number, a syntax error, or a network depending on the type it
// lands beside (#627 regression, caught by the PostgreSQL oracle before it
// reached anyone).
//
// `expr.tryNetworkLit` wraps a comparison in a CmpNetworkLit whenever the
// literal parses as an address in ANY family, and lets the column's real type
// pick the branch at eval time. `SELECT 1.0 < '10/8'` is 22P02 on the server
// (the literal resolves through NUMERIC there, not inet), so a type-blind site
// that read '10/8' as 10.0.0.0/8 would order a DOUBLE column's rows as
// addresses — which is what the first version of this fix did with '3.1'.
func TestAnUntypedLiteralIsNotAnAddressJustBecauseCidrWouldReadIt(t *testing.T) {
	// Numbers and masked abbreviations, which are what a type-blind site must
	// not turn into addresses.
	for _, s := range []string{"3.1", "2", "10", "192.168", "10.1", "0", "1_0", "0x0A", "3",
		"10/8", "192.168/16", "1/0"} {
		if CidrAddressText(s) {
			t.Errorf("CidrAddressText(%q) says address; a site that does not know the "+
				"column's type must read this as a number", s)
		}
	}
	// Real addresses, which must keep reaching the network comparison.
	for _, s := range []string{
		"10.0.0.1", "10.0.0.0/8", "192.168.1.5/24", "2001:db8::1", "::1", "::ffff:10.0.0.1",
	} {
		if !CidrAddressText(s) {
			t.Errorf("CidrAddressText(%q) says not-an-address", s)
		}
	}
	// And the CIDR-TYPED sites keep the wide grammar: a masked abbreviation
	// still keys as the address it names, which is what makes
	// `c_cidr = '10/8'` find its row.
	for _, c := range [][2]string{{"10/8", "10.0.0.0/8"}, {"192.168/16", "192.168.0.0/16"}} {
		a, aok := CidrSortKey(c[0])
		b, bok := CidrSortKey(c[1])
		if !aok || !bok || a != b {
			t.Errorf("CidrSortKey(%q) and CidrSortKey(%q) are two keys for one value", c[0], c[1])
		}
	}
	// A maskless abbreviation is not an address at ANY site now, typed or
	// not: the classful reading belongs to the cidr type's input function.
	for _, s := range []string{"239", "192.168", "10", "224"} {
		if _, ok := CidrSortKey(s); ok {
			t.Errorf("CidrSortKey(%q) keyed it as an address; PostgreSQL raises "+
				"`invalid input syntax for type inet: %q` beside a cidr column", s, s)
		}
	}
}

// The plan-time refusal and the runtime one read ONE predicate, so a query
// refused at one site cannot be answered at the other (#627, #579). The three
// types wired here are the ones whose accept-set is now a superset of
// PostgreSQL's; IPv4 and IPv6 are deliberately absent and their boundary is
// asserted below.
func TestNetworkLiteralRefusalIsOnePredicate(t *testing.T) {
	for _, c := range []struct {
		typ     batch.TypeID
		text    string
		refused bool
	}{
		{batch.TypeCIDR, "10/8", false},
		{batch.TypeCIDR, "192.168/16", false},
		{batch.TypeCIDR, "192.168", true}, // PG's inet refuses it beside a cidr column
		{batch.TypeCIDR, "239", true},
		{batch.TypeCIDR, "10.0.0.0/8", false},
		{batch.TypeCIDR, "::ffff:10.0.0.1", false},
		{batch.TypeCIDR, "zzz", true},
		{batch.TypeCIDR, "10.1.2.3.4", true},
		{batch.TypeMAC, "08002b:010203", false},
		{batch.TypeMAC, "0800-2b01-0203", false},
		{batch.TypeMAC, "08:00:2B:01:02:03", false},
		{batch.TypeMAC, "zzz", true},
		{batch.TypeUUID, "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}", false},
		{batch.TypeUUID, "a0eebc999c0b4ef8bb6d6bb9bd380a11", false},
		{batch.TypeUUID, "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", false},
		{batch.TypeUUID, "zzz", true},
	} {
		t.Run(c.typ.String()+"_"+c.text, func(t *testing.T) {
			st, have := QuotedLitStatus(c.typ, c.text)
			if !have {
				t.Fatalf("%v has no literal rule; the plan-time refusal cannot be wired", c.typ)
			}
			if refused := st != NumConstOK; refused != c.refused {
				t.Errorf("QuotedLitStatus(%v, %q) refused=%v, want %v", c.typ, c.text, refused, c.refused)
			}
		})
	}

	// IPv4 and IPv6 have a plan-time rule too since round 2, and it answers
	// with TWO classes — the split QuotedLitStatus alone cannot express, so
	// the network-prefix half is NetworkPrefixLiteral's. Both are asked at the
	// same plan-time site, so a literal cannot be garbage at one evaluator and
	// a network at another.
	for _, c := range []struct {
		typ     batch.TypeID
		text    string
		refused bool // by QuotedLitStatus, i.e. 22P02
		prefix  bool // by NetworkPrefixLiteral, i.e. 0A000
	}{
		{batch.TypeIPv4, "10.0.0.1", false, false},
		{batch.TypeIPv4, "10.0.0.1/32", false, false},
		{batch.TypeIPv4, "10/8", false, true},
		{batch.TypeIPv4, "10.0.1/24", false, true},
		{batch.TypeIPv4, "zzz", true, false},
		{batch.TypeIPv4, "192.168", true, false}, // PG's inet refuses it too
		{batch.TypeIPv6, "2001:db8::1", false, false},
		{batch.TypeIPv6, "2001:db8::1/128", false, false},
		{batch.TypeIPv6, "::1/64", false, true},
		{batch.TypeIPv6, "zzz", true, false},
	} {
		t.Run("bare_address/"+c.typ.String()+"_"+c.text, func(t *testing.T) {
			st, have := QuotedLitStatus(c.typ, c.text)
			if !have {
				t.Fatalf("%v has no plan-time rule; the refusal cannot be decided once", c.typ)
			}
			if refused := st != NumConstOK; refused != c.refused {
				t.Errorf("QuotedLitStatus(%v, %q) refused=%v, want %v", c.typ, c.text, refused, c.refused)
			}
			if got := NetworkPrefixLiteral(c.typ, c.text); got != c.prefix {
				t.Errorf("NetworkPrefixLiteral(%v, %q) = %v, want %v", c.typ, c.text, got, c.prefix)
			}
		})
	}

	// A HOST-width prefix is the address itself on the server, and it is
	// representable here.
	if _, ok := IPv4LitKey("10.0.0.1/32"); !ok {
		t.Errorf(`IPv4LitKey("10.0.0.1/32") refused; '10.0.0.1/32'::inet = '10.0.0.1'::inet`)
	}
	if _, ok := IPv6LitKey("2001:db8::1/128"); !ok {
		t.Errorf(`IPv6LitKey("2001:db8::1/128") refused; '::1/128'::inet = '::1'::inet`)
	}
	if _, ok := IPv4LitKey("10/8"); ok {
		t.Errorf(`IPv4LitKey("10/8") answered; the deferral is that a NETWORK has no place in a bare address`)
	}
	if !IPv4PrefixLiteral("10/8") || IPv4PrefixLiteral("zzz") || IPv4PrefixLiteral("10.0.0.1/32") {
		t.Errorf("IPv4PrefixLiteral does not separate a network from garbage and from a host")
	}
	if !IPv6PrefixLiteral("2001:db8::/64") || IPv6PrefixLiteral("zzz") || IPv6PrefixLiteral("::1/128") {
		t.Errorf("IPv6PrefixLiteral does not separate a network from garbage and from a host")
	}
}
