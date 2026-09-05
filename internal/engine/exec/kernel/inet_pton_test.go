package kernel

import (
	"net"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #627: every cell measured on live PostgreSQL 17.11 (`SELECT '<in>'::cidr`).
// The abbreviated grammar is an address INFERENCE, not a notation widening,
// which is why it needs a measured table rather than a remembered rule.
func TestAbbreviatedInetGrammarMatchesPostgres(t *testing.T) {
	for _, c := range []struct {
		in   string
		want string // PostgreSQL's canonical cidr output
	}{
		{"10", "10.0.0.0/8"},
		{"10.1", "10.1.0.0/16"},
		{"128", "128.0.0.0/16"},
		{"128.1", "128.1.0.0/16"},
		{"192", "192.0.0.0/24"},
		{"192.1", "192.1.0.0/24"},
		{"192.168", "192.168.0.0/24"},
		{"192.168.0", "192.168.0.0/24"},
		{"192.168.1", "192.168.1.0/24"},
		// The one cell that separates the two readings of the widening rule:
		// a bare class-D octet keeps /4 and does not widen to /8.
		{"224", "224.0.0.0/4"},
		{"224.1", "224.1.0.0/16"},
		{"224.1.2.3", "224.1.2.3/32"},
		{"240", "240.0.0.0/32"},
		{"240.1", "240.1.0.0/32"},
		{"10/8", "10.0.0.0/8"},
		{"10/16", "10.0.0.0/16"},
		{"192.168/16", "192.168.0.0/16"},
		{"0/0", "0.0.0.0/0"},
		{"010.1", "10.1.0.0/16"},
		{"10.1.2.3", "10.1.2.3/32"},
		{"10.0.0.1/8", "10.0.0.1/8"},
	} {
		t.Run(c.in, func(t *testing.T) {
			addr, bits, ok := parquet.PgIPv4Pton(c.in)
			if !ok {
				t.Fatalf("parquet.PgIPv4Pton(%q) refused; PostgreSQL 17.11 reads it as %s", c.in, c.want)
			}
			got := net.IP(addr[:]).String() + "/" + itoa(bits)
			// PostgreSQL prints the MASKED network for cidr; the parser keeps
			// the host bits, which is what wadjet's CIDR column stores, so the
			// comparison is against the address it names either way.
			wantIP, wantNet, err := net.ParseCIDR(c.want)
			if err != nil {
				t.Fatalf("fixture %q: %v", c.want, err)
			}
			wantOnes, _ := wantNet.Mask.Size()
			if bits != wantOnes || !net.IP(addr[:]).Equal(wantIP) {
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

	// Refused on the server too, measured the same way.
	for _, in := range []string{
		"10.", "10..1", "256.1", "10.1.2.3.4", "0x0a.1", "10/33", " 10/8", "10 /8", "10/8 ",
		"", "/8", "zzz", "10.1.2.-3", "1e2",
	} {
		t.Run("refused_"+in, func(t *testing.T) {
			if _, _, ok := parquet.PgIPv4Pton(in); ok {
				t.Errorf("parquet.PgIPv4Pton(%q) accepted; PostgreSQL 17.11 raises 22P02", in)
			}
		})
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

// A site that does NOT know the column's type must not read the abbreviated
// grammar, because under it every bare number is an address (#627 regression,
// caught by the PostgreSQL oracle before it reached anyone).
//
// `expr.tryNetworkLit` wraps a comparison in a CmpNetworkLit whenever the
// literal parses as an address in ANY family, and lets the column's real type
// pick the branch at eval time — so handing it `'3.1'` as 3.1.0.0/16 made
// `CASE WHEN d_val < '3.1' THEN 1 ELSE 0 END = 1` order a DOUBLE column's rows
// as ADDRESSES: 15 rows where PostgreSQL 17.11 answers 8. PostgreSQL reads
// `'3.1'` as a cidr only when the target IS a cidr, which is exactly what
// these type-blind sites cannot know.
func TestAnUntypedLiteralIsNotAnAddressJustBecauseCidrWouldReadIt(t *testing.T) {
	// Numbers, which are what the regression turned into addresses.
	for _, s := range []string{"3.1", "2", "10", "192.168", "10.1", "0", "1_0", "0x0A", "3"} {
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
	// And the CIDR-TYPED sites keep the wide grammar: the same abbreviated
	// spellings still key as the addresses they name, which is what makes
	// `c_cidr = '10/8'` find its row.
	for _, c := range [][2]string{{"10/8", "10.0.0.0/8"}, {"192.168", "192.168.0.0/24"}} {
		a, aok := CidrSortKey(c[0])
		b, bok := CidrSortKey(c[1])
		if !aok || !bok || a != b {
			t.Errorf("CidrSortKey(%q) and CidrSortKey(%q) are two keys for one value", c[0], c[1])
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
		{batch.TypeCIDR, "192.168", false},
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

	// The BOUNDARY, attempted from both sides: IPv4 and IPv6 have no
	// plan-time rule at all, because a prefix narrower than the host width is
	// PostgreSQL-valid text this engine's bare-address types cannot hold, and
	// a refusal built on that parser would refuse valid input at plan time.
	for _, typ := range []batch.TypeID{batch.TypeIPv4, batch.TypeIPv6} {
		if _, have := QuotedLitStatus(typ, "10/8"); have {
			t.Errorf("%v has a plan-time literal rule; it must not until a prefix is "+
				"representable in it, or TestPlanTimeNeverRefusesPGValidNetworkLiteral's "+
				"`{v4, 10/8}` boundary becomes a refusal of PostgreSQL-valid text", typ)
		}
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
