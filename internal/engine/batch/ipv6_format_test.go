package batch

import (
	"net"
	"testing"
)

// The printed form of an IPv6 value is the value (ADR-0012 item 11), and this
// engine's IPv6 is PostgreSQL's inet. Every cell below is `SELECT '<in>'::inet`
// on live PostgreSQL 17.11 — including the two families where net.IP.String()
// answers something else, which is #580:
//
//	::ffff:10.0.0.1  Go collapses a v4-MAPPED address to `10.0.0.1`, a value the
//	                 comparison and ordering paths correctly say the column is NOT
//	::1.2.3.4        Go prints a v4-COMPATIBLE address as `::102:304`
//
// The `notGo` column marks those cells so a reader can see at a glance which
// ones the stdlib gets wrong; the assertion is the same for all of them.
func TestFormatIPv6PrintsWhatPostgresPrints(t *testing.T) {
	for _, c := range []struct {
		in, want string
		notGo    bool
	}{
		{in: "::ffff:10.0.0.1", want: "::ffff:10.0.0.1", notGo: true},
		{in: "::ffff:0.0.0.0", want: "::ffff:0.0.0.0", notGo: true},
		{in: "::ffff:255.255.255.255", want: "::ffff:255.255.255.255", notGo: true},
		{in: "::1.2.3.4", want: "::1.2.3.4", notGo: true},
		{in: "0:0:0:0:0:0:1.2.3.4", want: "::1.2.3.4", notGo: true},
		{in: "::2", want: "::2"},
		{in: "::1", want: "::1"},
		{in: "::", want: "::"},
		{in: "::ffff:0:1.2.3.4", want: "::ffff:0:102:304"},
		{in: "0:0:0:0:0:1:2:3", want: "::1:2:3"},
		{in: "1::", want: "1::"},
		{in: "::1:0:0:0:0:0:0", want: "0:1::"},
		{in: "2001:db8:0:0:1:0:0:1", want: "2001:db8::1:0:0:1"},
		{in: "64:ff9b::1.2.3.4", want: "64:ff9b::102:304"},
		{in: "2001:db8::1", want: "2001:db8::1"},
	} {
		t.Run(c.in, func(t *testing.T) {
			ip := net.ParseIP(c.in)
			if ip == nil {
				t.Fatalf("fixture %q does not parse", c.in)
			}
			if got := FormatIPv6(ip.To16()); got != c.want {
				t.Errorf("FormatIPv6(%q) = %q, PostgreSQL 17.11 prints %q", c.in, got, c.want)
			}
			// The rendering is read back by kernel.IPv6RowKey and
			// exec.boxedIPv6Compare, so it has to round-trip to the same
			// sixteen bytes or the comparison changes with the display.
			back := net.ParseIP(c.want)
			if back == nil || !back.To16().Equal(ip.To16()) {
				t.Errorf("%q does not read back to the same address", c.want)
			}
			if c.notGo && net.IP(ip.To16()).String() == c.want {
				t.Errorf("%q: this cell is marked as one net.IP.String() gets wrong "+
					"and it now agrees — re-measure and drop the marker", c.in)
			}
		})
	}
}

// A vector's GetValue is the site #580 was filed against; the length guard is
// what keeps a short or absent value from indexing past the arena.
func TestIPv6VectorRendersThroughTheSharedFormatter(t *testing.T) {
	v := NewVector(TypeIPv6, 2)
	v.SetValue(0, "::ffff:10.0.0.1")
	v.SetValue(1, "2001:db8::1")
	if got := v.GetValue(0); got != "::ffff:10.0.0.1" {
		t.Errorf("GetValue(0) = %v, want ::ffff:10.0.0.1 (PostgreSQL 17.11)", got)
	}
	if got := v.GetValue(1); got != "2001:db8::1" {
		t.Errorf("GetValue(1) = %v, want 2001:db8::1", got)
	}
	if got := FormatIPv6([]byte{1, 2, 3}); got != "" {
		t.Errorf("a non-16-byte value renders %q, want the empty string", got)
	}
}
