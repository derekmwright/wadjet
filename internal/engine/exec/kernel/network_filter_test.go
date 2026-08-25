package kernel

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestFilterIPv4(t *testing.T) {
	schema := []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeIPv4},
	}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, "192.168.1.1")
	b.Columns[0].SetValue(1, "192.168.1.2")
	b.Columns[0].SetValue(2, "10.0.0.1")
	b.Columns[0].SetValue(3, "192.168.1.1")

	kern := ResolveFilterKernel(batch.TypeIPv4, OpEq, "192.168.1.1")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeIPv4")
	}
	outSel := make([]uint32, 0, 4)
	sel := kern(b.Columns[0], nil, 4, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
	if sel[0] != 0 || sel[1] != 3 {
		t.Fatalf("expected indices [0, 3], got %v", sel)
	}
}

func TestFilterIPv4LessThan(t *testing.T) {
	schema := []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeIPv4},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "10.0.0.1")
	b.Columns[0].SetValue(1, "172.16.0.1")
	b.Columns[0].SetValue(2, "192.168.1.1")

	kern := ResolveFilterKernel(batch.TypeIPv4, OpLt, "172.16.0.1")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeIPv4 OpLt")
	}
	outSel := make([]uint32, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 1 || sel[0] != 0 {
		t.Fatalf("expected [0] (10.0.0.1 < 172.16.0.1), got %v", sel)
	}
}

func TestFilterMAC(t *testing.T) {
	schema := []parquet.Column{
		{Name: "mac", Type: parquet.TypeMAC},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "aa:bb:cc:dd:ee:ff")
	b.Columns[0].SetValue(1, "11:22:33:44:55:66")
	b.Columns[0].SetValue(2, "aa:bb:cc:dd:ee:ff")

	kern := ResolveFilterKernel(batch.TypeMAC, OpEq, "aa:bb:cc:dd:ee:ff")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeMAC")
	}
	outSel := make([]uint32, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
}

func TestFilterIPv6(t *testing.T) {
	schema := []parquet.Column{
		{Name: "dst_ip", Type: parquet.TypeIPv6},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "::1")
	b.Columns[0].SetValue(1, "2001:db8::1")
	b.Columns[0].SetValue(2, "::1")

	kern := ResolveFilterKernel(batch.TypeIPv6, OpEq, "::1")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeIPv6")
	}
	outSel := make([]uint32, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
	if sel[0] != 0 || sel[1] != 2 {
		t.Fatalf("expected indices [0, 2], got %v", sel)
	}
}

func TestFilterCIDR(t *testing.T) {
	schema := []parquet.Column{
		{Name: "cidr", Type: parquet.TypeCIDR},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "10.0.0.0/8")
	b.Columns[0].SetValue(1, "172.16.0.0/12")
	b.Columns[0].SetValue(2, "10.0.0.0/8")

	kern := ResolveFilterKernel(batch.TypeCIDR, OpEq, "10.0.0.0/8")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeCIDR")
	}
	outSel := make([]uint32, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
}

// TestFilterCIDROrderingIsStructural is #492: the column stores CIDR as
// plain TEXT (parquet/schema.go), so comparing a literal against it directly
// was a LEXICAL byte comparison of that text — "10.0.0.0/24" sorts below
// "9.0.0.0/8" as text even though 10.x is the larger address. PostgreSQL
// orders inet/cidr STRUCTURALLY: family, then address bytes, then prefix
// length (verified against live PostgreSQL).
func TestFilterCIDROrderingIsStructural(t *testing.T) {
	schema := []parquet.Column{
		{Name: "cidr", Type: parquet.TypeCIDR},
	}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, "10.0.0.0/24") // numerically largest address
	b.Columns[0].SetValue(1, "9.0.0.0/8")   // numerically smallest address
	b.Columns[0].SetValue(2, "10.0.0.0/8")  // same address as row 0, smaller prefix
	b.Columns[0].SetValue(3, "::/0")        // IPv6: sorts after every IPv4 entry

	// > "9.0.0.0/8": every row except row 1 itself (numeric address order,
	// not "10.0.0.0/24" < "9.0.0.0/8" as lexical text would say).
	kern := ResolveFilterKernel(batch.TypeCIDR, OpGt, "9.0.0.0/8")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeCIDR OpGt")
	}
	outSel := make([]uint32, 0, 4)
	sel := kern(b.Columns[0], nil, 4, outSel)
	if len(sel) != 3 || sel[0] != 0 || sel[1] != 2 || sel[2] != 3 {
		t.Fatalf("> 9.0.0.0/8: expected [0 2 3], got %v", sel)
	}

	// < "10.0.0.0/24": same address, smaller prefix (row 2) and the smaller
	// address (row 1) — ascending prefix length at equal address is
	// PostgreSQL's rule, not the reverse.
	kern = ResolveFilterKernel(batch.TypeCIDR, OpLt, "10.0.0.0/24")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeCIDR OpLt")
	}
	outSel = make([]uint32, 0, 4)
	sel = kern(b.Columns[0], nil, 4, outSel)
	if len(sel) != 2 || sel[0] != 1 || sel[1] != 2 {
		t.Fatalf("< 10.0.0.0/24: expected [1 2], got %v", sel)
	}
}

func TestFilterPort(t *testing.T) {
	schema := []parquet.Column{
		{Name: "port", Type: parquet.TypePort},
	}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, int32(443))
	b.Columns[0].SetValue(1, int32(80))
	b.Columns[0].SetValue(2, int32(8080))
	b.Columns[0].SetValue(3, int32(443))

	kern := ResolveFilterKernel(batch.TypePort, OpEq, int64(443))
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypePort")
	}
	outSel := make([]uint32, 0, 4)
	sel := kern(b.Columns[0], nil, 4, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(sel))
	}
}

func TestFilterDuration(t *testing.T) {
	schema := []parquet.Column{
		{Name: "latency_ns", Type: parquet.TypeDuration},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, int64(1000000))    // 1ms
	b.Columns[0].SetValue(1, int64(5000000000)) // 5s
	b.Columns[0].SetValue(2, int64(500000))     // 0.5ms

	// Filter: latency > 1ms
	kern := ResolveFilterKernel(batch.TypeDuration, OpGt, int64(1000000))
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeDuration")
	}
	outSel := make([]uint32, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 1 || sel[0] != 1 {
		t.Fatalf("expected [1] (5s > 1ms), got %v", sel)
	}
}

func TestFilterUUID(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeUUID},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "550e8400-e29b-41d4-a716-446655440000")
	b.Columns[0].SetValue(1, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	b.Columns[0].SetValue(2, "550e8400-e29b-41d4-a716-446655440000")

	// A UUID column stores 16 RAW bytes and the SQL literal is text, so the
	// kernel converts. This assertion used to pass the RAW form, working
	// around the fact that the text form matched nothing — which was the
	// defect (#411), not the contract.
	for _, target := range []any{
		"550e8400-e29b-41d4-a716-446655440000", // canonical text
		"550e8400e29b41d4a716446655440000",     // dashless text
		b.Columns[0].BytesData.StringValue(0),  // already raw
	} {
		kern := ResolveFilterKernel(batch.TypeUUID, OpEq, target)
		if kern == nil {
			t.Fatalf("ResolveFilterKernel returned nil for TypeUUID (%v)", target)
		}
		outSel := make([]uint32, 0, 3)
		sel := kern(b.Columns[0], nil, 3, outSel)
		if len(sel) != 2 {
			t.Fatalf("target %q: expected 2 matches, got %d", target, len(sel))
		}
		if sel[0] != 0 || sel[1] != 2 {
			t.Fatalf("target %q: expected indices [0, 2], got %v", target, sel)
		}
	}
}

func TestFilterDate(t *testing.T) {
	schema := []parquet.Column{
		{Name: "event_date", Type: parquet.TypeDate},
	}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, "2026-03-15")
	b.Columns[0].SetValue(1, "2026-01-01")
	b.Columns[0].SetValue(2, "2026-06-01")

	// Filter: date > 2026-01-01 (days since epoch)
	// 2026-01-01 = 20454 days from epoch
	targetDays := b.Columns[0].Int32Data[1] // get the int32 representation of 2026-01-01
	kern := ResolveFilterKernel(batch.TypeDate, OpGt, int64(targetDays))
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeDate")
	}
	outSel := make([]uint32, 0, 3)
	sel := kern(b.Columns[0], nil, 3, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches (2026-03-15 and 2026-06-01 > 2026-01-01), got %d: %v", len(sel), sel)
	}
}

func TestFilterIPv4WithNulls(t *testing.T) {
	schema := []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeIPv4, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, 4)
	b.Columns[0].SetValue(0, "192.168.1.1")
	b.Columns[0].SetValue(1, nil)
	b.Columns[0].SetValue(2, "192.168.1.1")
	b.Columns[0].SetValue(3, nil)

	kern := ResolveFilterKernel(batch.TypeIPv4, OpEq, "192.168.1.1")
	if kern == nil {
		t.Fatal("ResolveFilterKernel returned nil for TypeIPv4")
	}
	outSel := make([]uint32, 0, 4)
	sel := kern(b.Columns[0], nil, 4, outSel)
	if len(sel) != 2 {
		t.Fatalf("expected 2 matches (nulls excluded), got %d: %v", len(sel), sel)
	}
}

// TestResolveLikeFilterKernelNetworkTypes is #497: ResolveLikeFilterKernel
// used to call vec.BytesData.UnsafeStringValue unconditionally, regardless
// of the column's real storage — an index-out-of-range PANIC for
// TypeIPv4/TypeMAC/TypePort/TypeProtocol (Int64Data/Int32Data-backed,
// BytesData empty) and a silent zero-match for TypeIPv6/TypeUUID
// (BytesData-backed but in their RAW binary form, not the text a LIKE
// pattern is written against). Every type here must both NOT panic and
// match the way its own CAST-AS-STRING text would.
func TestResolveLikeFilterKernelNetworkTypes(t *testing.T) {
	tests := []struct {
		name    string
		typ     batch.TypeID
		colType parquet.TypeID
		value   any
		pattern string
		want    bool
	}{
		{"ipv4 match", batch.TypeIPv4, parquet.TypeIPv4, "10.1.2.3", "10.%", true},
		{"ipv4 no match", batch.TypeIPv4, parquet.TypeIPv4, "10.1.2.3", "9.%", false},
		{"mac match", batch.TypeMAC, parquet.TypeMAC, "aa:bb:cc:dd:ee:ff", "aa:bb:%", true},
		{"mac no match", batch.TypeMAC, parquet.TypeMAC, "aa:bb:cc:dd:ee:ff", "11:%", false},
		{"port match", batch.TypePort, parquet.TypePort, int32(443), "44%", true},
		{"port no match", batch.TypePort, parquet.TypePort, int32(443), "80%", false},
		{"protocol match", batch.TypeProtocol, parquet.TypeProtocol, int32(6), "6", true},
		{"protocol no match", batch.TypeProtocol, parquet.TypeProtocol, int32(6), "17", false},
		{"ipv6 match", batch.TypeIPv6, parquet.TypeIPv6, "2001:db8::1", "2001:db8%", true},
		{"ipv6 no match", batch.TypeIPv6, parquet.TypeIPv6, "2001:db8::1", "::1%", false},
		{"uuid match", batch.TypeUUID, parquet.TypeUUID, "550e8400-e29b-41d4-a716-446655440000", "550e8400%", true},
		{"uuid no match", batch.TypeUUID, parquet.TypeUUID, "550e8400-e29b-41d4-a716-446655440000", "ffffffff%", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := []parquet.Column{{Name: "c", Type: tt.colType}}
			b := batch.NewRecordBatch(schema, 1)
			b.Columns[0].SetValue(0, tt.value)

			kern := ResolveLikeFilterKernel(tt.typ, tt.pattern, false)
			if kern == nil {
				t.Fatalf("ResolveLikeFilterKernel returned nil for %v", tt.typ)
			}
			outSel := make([]uint32, 0, 1)
			sel := kern(b.Columns[0], nil, 1, outSel)
			got := len(sel) == 1
			if got != tt.want {
				t.Errorf("LIKE %q against %v = %v, want %v (sel=%v)", tt.pattern, tt.value, got, tt.want, sel)
			}
		})
	}
}

// pgInetOrder is the total order live PostgreSQL 17 puts these values in.
//
// It was DERIVED, not reasoned out: the list was fed to
//
//	SELECT t FROM v ORDER BY t::inet, t
//
// in a postgres:17-alpine container, and this slice is that output verbatim.
// Ties (a value equal to the one before it under inet, broken here by text)
// are named in pgInetTies so the assertion below does not read a tie as an
// ordering.
//
// The set is chosen to separate PostgreSQL's network_cmp from every simpler
// rule: host-bearing prefixes at four mask lengths, bare addresses, /0 and
// /32 at the ends, v4 and v6 interleaved by family rather than by bytes, and
// a v4-MAPPED v6 address, which PostgreSQL still calls family 6.
var pgInetOrder = []string{
	"0.0.0.0/0",
	"0.0.0.0/32",
	"9.255.255.255/32",
	"10.0.0.0/8",
	"10.0.0.1/8",
	"10.255.255.255/8",
	"10.0.0.0/9",
	"10.0.0.1/32",
	"10.0.0.2",
	"10.128.0.0/9",
	"128.0.0.0/1",
	"192.168.1.0/24",
	"192.168.1.5/24",
	"192.168.1.255/24",
	"192.168.1.0/32",
	"192.168.1.5/32",
	"192.168.1.7",
	"255.255.255.255/32",
	"::/0",
	"::1/128",
	"::ffff:10.0.0.2",
	"::ffff:10.0.0.2/128",
	"2001:db8::/32",
	"2001:db8::1/32",
	"2001:db8::1/128",
	"2001:db8::9/128",
	"2001:db8::10/128",
	"2001:db8:ffff::/48",
	"fe80::1/64",
	"ffff::/16",
}

// pgInetTies names the adjacent pairs in pgInetOrder that PostgreSQL calls
// EQUAL — a bare address and its explicit host route are one value.
var pgInetTies = map[string]string{
	"::ffff:10.0.0.2/128": "::ffff:10.0.0.2",
}

// TestCidrSortKeyMatchesPostgresInetOrder is the semantics gate for #492's
// second pass: CidrSortKey must reproduce PostgreSQL's inet order — family,
// then the common bits under the SMALLER mask, then the mask length, then the
// FULL unmasked address — over host-bearing and canonical values alike.
//
// The first CidrSortKey keyed net.ParseCIDR's MASKED network, which threw the
// host bits away: '10.0.0.1/8' and '10.0.0.0/8' became one value, so
// `WHERE c_cidr = '10.0.0.1/8'` answered a row holding a DIFFERENT address.
// Comparing every adjacent pair here catches that (the two are adjacent), and
// comparing every PAIR catches a key that gets one step of the four right and
// another wrong.
func TestCidrSortKeyMatchesPostgresInetOrder(t *testing.T) {
	keys := make([]string, len(pgInetOrder))
	for i, s := range pgInetOrder {
		k, ok := CidrSortKey(s)
		if !ok {
			t.Fatalf("CidrSortKey(%q) refused a value PostgreSQL's inet accepts", s)
		}
		keys[i] = k
	}
	for i := 0; i < len(pgInetOrder); i++ {
		for j := i + 1; j < len(pgInetOrder); j++ {
			tied := pgInetTies[pgInetOrder[j]] == pgInetOrder[i]
			switch {
			case tied && keys[i] != keys[j]:
				t.Errorf("PostgreSQL calls %s and %s EQUAL; the keys differ",
					pgInetOrder[i], pgInetOrder[j])
			case !tied && !(keys[i] < keys[j]):
				t.Errorf("PostgreSQL orders %s BEFORE %s; the keys do not",
					pgInetOrder[i], pgInetOrder[j])
			}
		}
	}
}

// TestCidrSortKeyIsATotalOrder is the property the pinned table cannot state:
// the key must order EVERY pair consistently, not only the 30 PostgreSQL was
// asked about. A byte-string key is transitive and antisymmetric for free —
// what is not free is that every value produces one, at every mask length and
// in both families, so that no pair falls back to "neither is less".
func TestCidrSortKeyIsATotalOrder(t *testing.T) {
	var vals []string
	for _, a := range []string{"0.0.0.0", "10.0.0.1", "10.0.0.255", "10.128.0.1", "192.168.1.7", "255.255.255.255"} {
		for _, m := range []int{0, 1, 8, 9, 16, 24, 31, 32} {
			vals = append(vals, fmt.Sprintf("%s/%d", a, m))
		}
		vals = append(vals, a)
	}
	for _, a := range []string{"::", "::1", "2001:db8::1", "2001:db8::ffff", "fe80::1", "ffff::"} {
		for _, m := range []int{0, 16, 32, 64, 127, 128} {
			vals = append(vals, fmt.Sprintf("%s/%d", a, m))
		}
		vals = append(vals, a)
	}
	keys := make(map[string]string, len(vals))
	for _, v := range vals {
		k, ok := CidrSortKey(v)
		if !ok {
			t.Fatalf("CidrSortKey(%q) refused a well-formed value", v)
		}
		keys[v] = k
	}
	for _, a := range vals {
		for _, b := range vals {
			ka, kb := keys[a], keys[b]
			if a == b && ka != kb {
				t.Fatalf("%s does not key to itself", a)
			}
			if (ka < kb) && (kb < ka) {
				t.Fatalf("%s and %s are both less than each other", a, b)
			}
			for _, c := range vals {
				kc := keys[c]
				if ka < kb && kb < kc && !(ka < kc) {
					t.Fatalf("not transitive: %s < %s < %s", a, b, c)
				}
			}
		}
	}
}

// TestCidrSortKeyReadsABareAddressAsAHostRoute pins the literal shape the
// kernel used to answer with a match-nothing sentinel. PostgreSQL's inet
// reads '10.0.0.1' as '10.0.0.1/32' — verified live, the two are `=` — and a
// CIDR column holding either text must therefore answer the other.
func TestCidrSortKeyReadsABareAddressAsAHostRoute(t *testing.T) {
	for _, pair := range [][2]string{
		{"10.0.0.1", "10.0.0.1/32"},
		{"2001:db8::1", "2001:db8::1/128"},
		{"::ffff:10.0.0.2", "::ffff:10.0.0.2/128"},
	} {
		a, aok := CidrSortKey(pair[0])
		b, bok := CidrSortKey(pair[1])
		if !aok || !bok || a != b {
			t.Errorf("%q and %q must be one value; ok=%v/%v equal=%v",
				pair[0], pair[1], aok, bok, a == b)
		}
	}
	// A v4 literal and a v6 one are never the same value, family first.
	v4, _ := CidrSortKey("10.0.0.2")
	v6, _ := CidrSortKey("::ffff:10.0.0.2")
	if !(v4 < v6) {
		t.Error("PostgreSQL puts every v4 address below every v6 one, v4-mapped included")
	}
}

// TestCidrSortKeyKeepsHostBits is #492's own regression, stated as small as it
// gets: the two values differ only in a bit the mask covers up, and the first
// implementation made them one.
func TestCidrSortKeyKeepsHostBits(t *testing.T) {
	zero, _ := CidrSortKey("10.0.0.0/8")
	one, _ := CidrSortKey("10.0.0.1/8")
	if zero == one {
		t.Fatal("10.0.0.0/8 and 10.0.0.1/8 keyed the same: the host bits were masked away")
	}
	if !(zero < one) {
		t.Error("10.0.0.0/8 must sort below 10.0.0.1/8 — the full address is the last tiebreak")
	}
}

// TestResolveFilterKernelRefusesANonAddressLiteral: a CIDR or IPv6 column
// against a literal that is no address gets NO kernel, which is how this
// package asks the caller for a query error (exec.networkConstError, SQLSTATE
// 22P02). It used to get a match-nothing kernel for CIDR — so `c_cidr <>
// 'garbage'` dropped EVERY row — and, for IPv6, the empty raw address, which
// every stored address compares above.
func TestResolveFilterKernelRefusesANonAddressLiteral(t *testing.T) {
	for _, typ := range []batch.TypeID{batch.TypeCIDR, batch.TypeIPv6} {
		for _, op := range []CompareOp{OpEq, OpNe, OpLt, OpGt} {
			if k := ResolveFilterKernel(typ, op, "garbage"); k != nil {
				t.Errorf("%v op %v against 'garbage' returned a kernel; want nil so the caller raises", typ, op)
			}
		}
	}
	// A bare address IS an address: it must still get a kernel.
	if k := ResolveFilterKernel(batch.TypeCIDR, OpEq, "10.0.0.1"); k == nil {
		t.Error("a bare address against CIDR must resolve to a /32 host route, not a refusal")
	}
	// A v4 literal against IPv6 is an address too — a family comparison.
	if k := ResolveFilterKernel(batch.TypeIPv6, OpGt, "10.0.0.2"); k == nil {
		t.Error("a v4 literal against IPv6 must resolve; PostgreSQL compares the family")
	}
}

// TestIPv6KernelPutsAV4LiteralBelowEveryRow: PostgreSQL's inet compares the
// FAMILY first, so every v6 address is greater than every v4 one — including
// ::1, whose raw bytes are far below a v4-mapped address's. Reading the
// literal as its v4-mapped 16 bytes (what the arm did before IPv6LitKey) put
// it in the MIDDLE of the v6 range instead.
func TestIPv6KernelPutsAV4LiteralBelowEveryRow(t *testing.T) {
	schema := []parquet.Column{{Name: "c", Type: parquet.TypeIPv6}}
	rows := []string{"::", "::1", "::ffff:10.0.0.2", "2001:db8::1", "ffff::"}
	b := batch.NewRecordBatch(schema, len(rows))
	for i, r := range rows {
		b.Columns[0].SetValue(i, r)
	}
	for _, tc := range []struct {
		op   CompareOp
		want int
	}{
		{OpGt, len(rows)}, // every v6 row is above a v4 literal
		{OpLt, 0},
		{OpEq, 0},
	} {
		kern := ResolveFilterKernel(batch.TypeIPv6, tc.op, "10.0.0.2")
		if kern == nil {
			t.Fatal("no kernel for a v4 literal against IPv6")
		}
		got := len(kern(b.Columns[0], nil, len(rows), make([]uint32, 0, len(rows))))
		if got != tc.want {
			t.Errorf("op %v against '10.0.0.2': %d rows, want %d", tc.op, got, tc.want)
		}
	}
}

// TestFilterCIDRIgnoresAMalformedStoredValue: the column is unvalidated text,
// so a row can hold something that is not an address. It matches NOTHING, for
// every operator including `<>` — UNKNOWN, the answer a NULL row gets — and
// expr.CmpNetworkLit.evalCIDR answers the same row the same way. Falling
// through to a lexical text comparison on one path and not the other is the
// two-path defect this whole item exists to close.
func TestFilterCIDRIgnoresAMalformedStoredValue(t *testing.T) {
	schema := []parquet.Column{{Name: "c", Type: parquet.TypeCIDR}}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, "10.0.0.0/8")
	b.Columns[0].SetValue(1, "not-an-address")
	for _, op := range []CompareOp{OpEq, OpNe, OpLt, OpGt, OpLe, OpGe} {
		kern := ResolveFilterKernel(batch.TypeCIDR, op, "10.0.0.0/8")
		sel := kern(b.Columns[0], nil, 2, make([]uint32, 0, 2))
		for _, idx := range sel {
			if idx == 1 {
				t.Errorf("op %v admitted the malformed row", op)
			}
		}
	}
}

// TestInFilterCIDRUsesTheSameKeyAsTheComparison: `c = 'X'` and `c IN ('X')`
// must answer alike. The IN arm shared TypeString's raw-text set, so a
// non-canonical spelling of the same network was one value to `=` and another
// to IN.
func TestInFilterCIDRUsesTheSameKeyAsTheComparison(t *testing.T) {
	schema := []parquet.Column{{Name: "c", Type: parquet.TypeCIDR}}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, "10.0.0.1/8")
	b.Columns[0].SetValue(1, "2001:db8::1/64")
	for _, tc := range []struct {
		lit  string
		want int
	}{
		{"10.0.0.1", 0},          // bare form of row 0's address, but /32 != /8
		{"10.0.0.1/8", 0},        // row 0
		{"2001:0db8:0::1/64", 1}, // a non-canonical spelling of row 1
	} {
		kern := ResolveInFilterKernel(batch.TypeCIDR, []any{tc.lit}, false)
		if kern == nil {
			t.Fatalf("no IN kernel for %q", tc.lit)
		}
		sel := kern(b.Columns[0], nil, 2, make([]uint32, 0, 2))
		eq := ResolveFilterKernel(batch.TypeCIDR, OpEq, tc.lit)
		eqSel := eq(b.Columns[0], nil, 2, make([]uint32, 0, 2))
		if fmt.Sprint(sel) != fmt.Sprint(eqSel) {
			t.Errorf("IN (%q) selected %v but = %q selected %v", tc.lit, sel, tc.lit, eqSel)
		}
	}
	if k := ResolveInFilterKernel(batch.TypeCIDR, []any{"garbage"}, false); k != nil {
		t.Error("IN with a non-address member must return nil so the caller raises")
	}
}
