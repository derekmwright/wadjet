package kernel

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Every type the engine has must have an ANSWER here — a conversion or an
// explicit refusal — because a type that fell through to "pass it through
// raw" is exactly how #442 happened. This asserts the list is complete, so a
// 23rd type cannot inherit the old default by omission.
func TestStatsDomainValueCoversEveryType(t *testing.T) {
	// A literal of the shape a query carries for each type, and whether the
	// prune layer may use it.
	cases := map[batch.TypeID]struct {
		lit  any
		want any
		ok   bool
	}{
		batch.TypeBool:      {true, true, true},
		batch.TypeInt32:     {int64(7), int64(7), true},
		batch.TypeInt64:     {int64(7), int64(7), true},
		batch.TypeFloat32:   {1.5, 1.5, true},
		batch.TypeFloat64:   {1.5, 1.5, true},
		batch.TypeString:    {"abc", "abc", true},
		batch.TypeBytes:     {[]byte("abc"), "abc", true},
		batch.TypeTimestamp: {int64(1700000000000), int64(1700000000000), true},
		batch.TypeCIDR:      {"10.0.0.0/8", "10.0.0.0/8", true},
		batch.TypePort:      {int64(443), int64(443), true},
		batch.TypeProtocol:  {int64(6), int64(6), true},
		batch.TypeDuration:  {int64(1000), int64(1000), true},
		batch.TypeDate:      {"2021-03-04", int64(18690), true},
		batch.TypeIPv4:      {"10.0.5.220", int64(167773660), true},
		batch.TypeMAC:       {"aa:bb:cc:00:05:dc", int64(187723558159836), true},
		batch.TypeIPv6:      {"2001:db8::5dc", "\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\xdc", true},
		batch.TypeUUID: {"00000000-0000-4000-8000-0000000005dc",
			"\x00\x00\x00\x00\x00\x00\x40\x00\x80\x00\x00\x00\x00\x00\x05\xdc", true},
		batch.TypeDecimal: {0.25, int64(25), true},
		// Containers have no scalar bound: their statistics belong to leaves.
		batch.TypeArray:  {"x", nil, false},
		batch.TypeRow:    {"x", nil, false},
		batch.TypeMap:    {"x", nil, false},
		batch.TypeVector: {"x", nil, false},
	}
	// The whole TypeID range, so a type added to the enum lands here.
	for col := parquet.TypeBool; col <= parquet.TypeVector; col++ {
		tc, listed := cases[col]
		if !listed {
			t.Fatalf("type %s is not in the stats-domain table — add it, with the "+
				"conversion or the reason there is none", col)
		}
		scale := 0
		if col == batch.TypeDecimal {
			scale = 2
		}
		got, ok := StatsDomainValue(col, scale, tc.lit)
		if ok != tc.ok {
			t.Errorf("%s: StatsDomainValue(%#v) ok = %v, want %v", col, tc.lit, ok, tc.ok)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("%s: StatsDomainValue(%#v) = %#v, want %#v", col, tc.lit, got, tc.want)
		}
	}
}

// The DECIMAL rule: an integer literal is the WHOLE number (the kernel's
// reading, not the writer's), a literal the scale cannot hold is refused
// rather than rounded, and trailing zeros are not extra digits.
func TestStatsDomainValueDecimal(t *testing.T) {
	for _, tc := range []struct {
		lit   any
		scale int
		want  int64
		ok    bool
	}{
		{0.25, 2, 25, true},
		{"0.25", 2, 25, true},
		{"0.250", 2, 25, true}, // trailing zeros are not digits past the scale
		{"0.2500000", 2, 25, true},
		{-0.25, 2, -25, true},
		{0.0, 2, 0, true},
		{int64(3), 2, 300, true}, // the NUMBER three, unscaled 300
		{int64(-3), 2, -300, true},
		{1500.15, 4, 15001500, true},
		{"0.255", 2, 0, false}, // the column cannot hold it: refuse, never round
		{0.005, 2, 0, false},
		{"1e5", 2, 0, false}, // exponent form has no exact digit split here
		{"not a number", 2, 0, false},
		{"", 2, 0, false},
		{"9223372036854775807", 2, 0, false}, // past int64 once scaled
		{0.25, 0, 0, false},                  // scale 0 cannot hold a fraction
		{int64(3), 0, 3, true},
	} {
		got, ok := StatsDomainValue(batch.TypeDecimal, tc.scale, tc.lit)
		if ok != tc.ok {
			t.Errorf("decimal(%v) scale %d: ok = %v, want %v (got %#v)", tc.lit, tc.scale, ok, tc.ok, got)
			continue
		}
		if ok && got != any(tc.want) {
			t.Errorf("decimal(%v) scale %d = %#v, want %d", tc.lit, tc.scale, got, tc.want)
		}
	}
}

// An unparseable network or date literal must be REFUSED, not converted to a
// zero that then prunes as if it were a real bound.
func TestStatsDomainValueRefusesUnparseableLiterals(t *testing.T) {
	for _, tc := range []struct {
		typ batch.TypeID
		lit any
	}{
		{batch.TypeIPv6, "not an address"},
		{batch.TypeIPv6, int64(5)},
		{batch.TypeUUID, "zz"},
		{batch.TypeIPv4, "not an address"},
		{batch.TypeMAC, "zz:zz"},
		{batch.TypeDate, "not a date"},
		{batch.TypeBytes, 5},
	} {
		if got, ok := StatsDomainValue(tc.typ, 0, tc.lit); ok {
			t.Errorf("%s: %#v was accepted as %#v, want a refusal", tc.typ, tc.lit, got)
		}
	}
	if _, ok := StatsDomainValue(batch.TypeInt64, 0, nil); ok {
		t.Error("a nil literal was accepted")
	}
}

// The conversion has to be the SAME one the filter kernel applies, or the
// prune and the filter disagree about which rows the predicate wants.
func TestStatsDomainValueAgreesWithTheFilterKernel(t *testing.T) {
	if got, _ := StatsDomainValue(batch.TypeIPv6, 0, "2001:db8::5dc"); got != parseIPv6ToRawString("2001:db8::5dc") {
		t.Error("the IPv6 stats value is not the kernel's IPv6 literal")
	}
	if got, _ := StatsDomainValue(batch.TypeUUID, 0, "00000000-0000-4000-8000-0000000005dc"); got !=
		parseUUIDToRawString("00000000-0000-4000-8000-0000000005dc") {
		t.Error("the UUID stats value is not the kernel's UUID literal")
	}
	// The DECIMAL literal the kernel resolves at scale 2 is the same unscaled
	// integer this hands the prune.
	lit, residual := decimalLiteralAt(decimalLiteralText(0.25), 2)
	if residual != 0 {
		t.Fatalf("0.25 at scale 2 should be exact, residual %d", residual)
	}
	got, ok := StatsDomainValue(batch.TypeDecimal, 2, 0.25)
	if !ok || got != lit.ToInt64() {
		t.Errorf("decimal stats value %#v, kernel literal %d", got, lit.ToInt64())
	}
}

// The two's-complement range is ASYMMETRIC: -9223372036854775808 is an int64
// and +9223372036854775808 is not. A conversion that strips the sign and
// parses the MAGNITUDE therefore overflows on exactly one value in the range,
// and withholds the predicate that names the most negative unscaled decimal a
// column can hold — the one place a DECIMAL's minimum bound actually sits.
//
// Withholding is not a wrong answer, which is why nothing else here sees it:
// the row groups simply stop being skipped for that predicate. It is asserted
// at the bound and one step past it in BOTH directions, so the fix cannot
// drift into accepting a value int64 does not hold.
func TestStatsDomainValueDecimalAtTheInt64Bounds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lit   any
		scale int
		want  int64
		ok    bool
	}{
		{"max", "9223372036854775807", 0, math.MaxInt64, true},
		{"min", "-9223372036854775808", 0, math.MinInt64, true},
		{"min+1", "-9223372036854775807", 0, math.MinInt64 + 1, true},
		{"max-1", "9223372036854775806", 0, math.MaxInt64 - 1, true},
		// The same two bounds reached through the SCALE, which is how a
		// DECIMAL literal usually arrives.
		{"min scaled", "-922337203685477.5808", 4, math.MinInt64, true},
		{"max scaled", "922337203685477.5807", 4, math.MaxInt64, true},
		// An int64 literal renders through the same path.
		{"min as int64", int64(math.MinInt64), 0, math.MinInt64, true},
		{"max as int64", int64(math.MaxInt64), 0, math.MaxInt64, true},
		// One past the range in each direction stays WITHHELD: past int64 the
		// bound is a FIXED_LEN_BYTE_ARRAY the writer emits no statistics for.
		{"max+1", "9223372036854775808", 0, 0, false},
		{"min-1", "-9223372036854775809", 0, 0, false},
		{"max+1 scaled", "922337203685477.5808", 4, 0, false},
		{"min-1 scaled", "-922337203685477.5809", 4, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := StatsDomainValue(batch.TypeDecimal, tc.scale, tc.lit)
			if ok != tc.ok {
				t.Fatalf("decimal(%v) scale %d: ok = %v, want %v (got %#v)", tc.lit, tc.scale, ok, tc.ok, got)
			}
			if ok && got != any(tc.want) {
				t.Errorf("decimal(%v) scale %d = %#v, want %d", tc.lit, tc.scale, got, tc.want)
			}
		})
	}
}
