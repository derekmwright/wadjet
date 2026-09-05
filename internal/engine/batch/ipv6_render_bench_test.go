package batch

import (
	"net"
	"testing"
)

// The A/B for #580's renderer. IPv6 rendering is on no TPC-H or ClickBench
// path, so "profile in batches" has no arm that reaches it — this is the only
// place a regression in it can be seen.
//
// Two fixtures, because the two answer different questions: the MIXED one is
// two-thirds addresses whose output #580 did not change (a regression there is
// a regression for every IPv6 value, not just the ones being corrected), and
// the v4-mapped one is the family the fix exists for.
func benchIPv6Vector(tb testing.TB, mixed bool) *Vector {
	tb.Helper()
	lits := []string{"::ffff:10.0.0.1", "::ffff:192.168.1.1", "::ffff:255.255.255.255"}
	if mixed {
		lits = []string{
			"2001:db8::1", "fe80::1", "::1", "2001:db8:85a3::8a2e:370:7334",
			"::ffff:10.0.0.1", "64:ff9b::102:304", "2001:db8::1:0:0:1", "::",
			"ff02::2", "::ffff:192.168.1.1", "abcd:ef01:2345:6789:abcd:ef01:2345:6789",
			"::1.2.3.4",
		}
	}
	v := NewVector(TypeIPv6, 2048)
	v.Len = 2048
	for i := 0; i < 2048; i++ {
		ip := net.ParseIP(lits[i%len(lits)])
		if ip == nil {
			tb.Fatalf("fixture %q does not parse", lits[i%len(lits)])
		}
		v.BytesData.Set(i, []byte(ip.To16()))
	}
	return v
}

func benchGetValueIPv6(b *testing.B, mixed bool) {
	v := benchIPv6Vector(b, mixed)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < v.Len; i++ {
			sink = v.GetValue(i)
		}
	}
}

var sink any

func BenchmarkVectorGetValueIPv6Mixed(b *testing.B)    { benchGetValueIPv6(b, true) }
func BenchmarkVectorGetValueIPv6V4Mapped(b *testing.B) { benchGetValueIPv6(b, false) }
