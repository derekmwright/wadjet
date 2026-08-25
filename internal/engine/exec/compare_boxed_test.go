package exec

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The #444 regression. One value has two comparators — the columnar one the
// sort and the sort-merge join take, and the boxed one the spilled/global
// window rows take — and the bug was that they imposed DIFFERENT orders. So
// the test is not "does the boxed comparator produce this list", it is "do
// the two agree on every pair", which is the property that was false.
//
// The shapes below are chosen for what the box DROPS: a ROW's declaration
// order (a Go map has none, so the old comparator sorted field NAMES and got
// a different answer whenever the declared order was not alphabetical) and a
// DECIMAL's scale (the box is text, and text orders "10.001" before
// "2.0002"). Containers of those carry the same loss one level down.

func rowZACol() parquet.Column {
	// Declared z-then-a: NOT alphabetical, which is the whole point.
	return parquet.Column{Name: "v", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
		{Name: "z", Type: parquet.TypeString, Nullable: true},
		{Name: "a", Type: parquet.TypeInt64, Nullable: true},
	}}
}

// boxedAgreementRows builds one row per value, keyed by the COLUMN's own
// name.
//
// batch.FromRows reads `row[col.Name]`, and this helper used to key every row
// "v" while assertBoxedAgreesWithColumnar's callers rename the column to the
// case name — so every value read back as NULL, every pair of NULLs compared
// equal on both sides, and the whole table agreed while proving nothing. It
// was found by adding cases that MUST fail without their fix (the address
// types, #569) and watching them pass.
func boxedAgreementRows(name string, vals []any) []map[string]any {
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{name: v}
	}
	return rows
}

func assertBoxedAgreesWithColumnar(t *testing.T, col parquet.Column, vals []any) {
	t.Helper()
	b := batch.FromRows([]parquet.Column{col}, boxedAgreementRows(col.Name, vals))
	if b == nil || len(b.Columns) != 1 {
		t.Fatalf("FromRows built no column for %s", col.Name)
	}
	vec := b.Columns[0]
	// The fixture must actually hold the values. A helper that silently
	// wrote nothing is what made this assertion vacuous once already, and a
	// column of NULLs compares equal on both sides for every pair.
	nonNull := 0
	for i := range vals {
		if vals[i] != nil && vec.GetValue(i) != nil {
			nonNull++
		}
	}
	if want := len(vals) - 1; nonNull < want {
		t.Fatalf("%s: fixture holds %d non-NULL values, want %d — FromRows did not store them, "+
			"so this comparison proves nothing", col.Name, nonNull, want)
	}
	columnar := kernel.ResolveSortCompare(col.Type)
	if columnar == nil {
		t.Fatalf("%v: no columnar comparator", col.Type)
	}
	boxed := newBoxedCompare(col)
	for i := range vals {
		for j := range vals {
			want := sign(columnar(vec, i, vec, j))
			got := sign(boxed(vec.GetValue(i), vec.GetValue(j)))
			if got != want {
				t.Errorf("%s: rows %d/%d — boxed %d, columnar %d\n  a=%v\n  b=%v",
					col.Name, i, j, got, want, vals[i], vals[j])
			}
		}
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	}
	return 0
}

func TestBoxedCompareAgreesWithColumnar(t *testing.T) {
	cases := []struct {
		name string
		col  parquet.Column
		vals []any
	}{
		{
			// The issue's own shape. Ordering by NAME compares `a` first and
			// puts {z:"b",a:1} below {z:"a",a:2}; PostgreSQL's record_cmp
			// compares `z` first and puts it above.
			name: "row_declared_out_of_alphabetical_order",
			col:  rowZACol(),
			vals: []any{
				nil,
				map[string]any{"z": "a", "a": int64(2)},
				map[string]any{"z": "b", "a": int64(1)},
				map[string]any{"z": "b", "a": int64(2)},
				map[string]any{"z": "b", "a": nil},
				map[string]any{"z": nil, "a": int64(9)},
			},
		},
		{
			name: "array_of_row_declared_out_of_alphabetical_order",
			col: parquet.Column{Name: "v", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeRow, Nullable: true,
					Fields: rowZACol().Fields}},
			vals: []any{
				nil,
				[]any{},
				[]any{map[string]any{"z": "a", "a": int64(2)}},
				[]any{map[string]any{"z": "b", "a": int64(1)}},
				[]any{map[string]any{"z": "b", "a": int64(1)}, map[string]any{"z": "a", "a": int64(0)}},
			},
		},
		{
			// A DECIMAL boxes as its formatted text, and text orders
			// "10.0010" before "2.0002".
			name: "decimal",
			col:  parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
			vals: []any{nil, "-10.0010", "-2.0002", "0.0000", "2.0002", "10.0010", "10.0011"},
		},
		{
			name: "array_of_decimal",
			col: parquet.Column{Name: "v", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}},
			vals: []any{
				nil,
				[]any{"2.0002"},
				[]any{"10.0010"},
				[]any{"10.0010", "2.0002"},
			},
		},
		{
			name: "row_of_decimal_and_row",
			col: parquet.Column{Name: "v", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "z", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
				{Name: "a", Type: parquet.TypeString, Nullable: true},
			}},
			vals: []any{
				nil,
				map[string]any{"z": "2.00", "a": "zzz"},
				map[string]any{"z": "10.00", "a": "aaa"},
				map[string]any{"z": "10.00", "a": "bbb"},
			},
		},
		{
			name: "map",
			col: parquet.Column{Name: "v", Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeString},
					{Name: "value", Type: parquet.TypeInt64, Nullable: true},
				}}},
			vals: []any{
				nil,
				map[string]any{},
				map[string]any{"a": int64(1)},
				map[string]any{"a": int64(2)},
				map[string]any{"a": int64(1), "b": int64(1)},
				map[string]any{"b": int64(0)},
			},
		},
		{
			// #446's shape: NaN at differing positions must not make the two
			// paths disagree either.
			name: "vector_with_nan",
			col:  parquet.Column{Name: "v", Type: parquet.TypeVector, Nullable: true, Dimension: 3},
			vals: []any{
				nil,
				[]float32{float32(math.NaN()), 0, 2},
				[]float32{0, 1, 2},
				[]float32{1, 0, 1},
				[]float32{1, float32(math.NaN()), 1},
				[]float32{float32(math.NaN()), float32(math.NaN()), float32(math.NaN())},
			},
		},
		{
			name: "array_of_float64_with_nan",
			col: parquet.Column{Name: "v", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeFloat64, Nullable: true}},
			vals: []any{
				nil,
				[]any{math.NaN(), 0.0, 2.0},
				[]any{0.0, 1.0, 2.0},
				[]any{1.0, 0.0, 1.0},
				[]any{math.NaN(), nil},
				[]any{math.NaN()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := tc.col
			col.Name = tc.name
			assertBoxedAgreesWithColumnar(t, col, tc.vals)
		})
	}
}

// TestBoxedRowComparePositionalNotAlphabetical pins the DIRECTION of the fix,
// not merely the agreement: with fields declared (z, a), the order is decided
// by z. The old name-ordered comparator answered the opposite here, so this
// case fails before the fix and passes after it.
func TestBoxedRowComparePositionalNotAlphabetical(t *testing.T) {
	cmp := newBoxedCompare(rowZACol())
	lo := map[string]any{"z": "a", "a": int64(2)}
	hi := map[string]any{"z": "b", "a": int64(1)}
	if got := cmp(lo, hi); got >= 0 {
		t.Fatalf("declared order is (z, a), so z decides: got %d, want < 0", got)
	}
	if got := cmp(hi, lo); got <= 0 {
		t.Fatalf("antisymmetry: got %d, want > 0", got)
	}
	// And the undeclared box really does order the other way — which is what
	// made the two paths disagree, and is why every production caller must
	// resolve the declaration.
	if got := compareAny(lo, hi); got <= 0 {
		t.Fatalf("compareAny with no declaration orders by name: got %d, want > 0", got)
	}
}

// TestBoxedCompareFallsBackWithoutADeclaration keeps the undeclared path
// honest: a column the schema does not name, or a ROW with no declared
// fields, still compares rather than reporting every pair equal.
func TestBoxedCompareFallsBackWithoutADeclaration(t *testing.T) {
	schema := []parquet.Column{{Name: "known", Type: parquet.TypeInt64}}
	cmp := newBoxedCompareFor(schema, "missing")
	if got := cmp(int64(1), int64(2)); got != -1 {
		t.Fatalf("unknown column: got %d, want -1", got)
	}
	bare := newBoxedCompare(parquet.Column{Name: "v", Type: parquet.TypeRow})
	if got := bare(map[string]any{"a": int64(1)}, map[string]any{"a": int64(2)}); got != -1 {
		t.Fatalf("ROW with no declared fields: got %d, want -1", got)
	}
}

// TestBoxedKeyAgreesWithCompareOnNaN holds the container-order contract's
// equality half: "compares equal" and "serializes alike" must name the same
// relation, or a drained partial aggregate splits one group in two and the
// query answers differently depending on how much memory it had. The
// comparators call all NaNs one value (#446), so the key writers must fold
// the payload too — raw IEEE bits do not.
func TestBoxedKeyAgreesWithCompareOnNaN(t *testing.T) {
	quiet := math.NaN()
	payload := math.Float64frombits(math.Float64bits(quiet) ^ 0xF)
	quiet32 := float32(math.NaN())
	payload32 := math.Float32frombits(math.Float32bits(quiet32) ^ 0xF)
	if !math.IsNaN(payload) || math.Float64bits(payload) == math.Float64bits(quiet) ||
		!math.IsNaN(float64(payload32)) || math.Float32bits(payload32) == math.Float32bits(quiet32) {
		t.Fatalf("test setup: needed a second, distinct NaN bit pattern")
	}

	cases := []struct {
		name string
		a, b any
	}{
		{"float64_payloads", quiet, payload},
		{"float32_payloads", quiet32, payload32},
		{"vector_payloads", []float32{quiet32, 1}, []float32{payload32, 1}},
		{"array_payloads", []any{quiet, 1.0}, []any{payload, 1.0}},
		{"row_payloads",
			map[string]any{"f": quiet},
			map[string]any{"f": payload}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmp := compareAny(tc.a, tc.b)
			eqKey := string(appendKeyValue(nil, tc.a)) == string(appendKeyValue(nil, tc.b))
			if (cmp == 0) != eqKey {
				t.Fatalf("compareAny = %d but keys equal = %v\n  a=%v\n  b=%v", cmp, eqKey, tc.a, tc.b)
			}
			if cmp != 0 {
				t.Fatalf("NaN equals NaN in PostgreSQL's float order: got %d", cmp)
			}
		})
	}

	// And two DIFFERENT vectors still serialize differently, so folding the
	// payload did not collapse anything else.
	x := appendKeyValue(nil, []float32{quiet32, 1})
	y := appendKeyValue(nil, []float32{quiet32, 2})
	if string(x) == string(y) {
		t.Fatal("distinct vectors must not share a key")
	}
}

// TestBoxedVectorCompareOrdersNaNLast is #446 reached through the BOXED
// comparator, which the spilled/global window rows take: it must agree with
// the columnar one rather than tie every NaN pair.
func TestBoxedVectorCompareOrdersNaNLast(t *testing.T) {
	a := []float32{float32(math.NaN()), 0, 2}
	b := []float32{0, 1, 2}
	c := []float32{1, 0, 1}
	if got := compareAny(a, b); got <= 0 {
		t.Errorf("[NaN,0,2] vs [0,1,2] = %d, want > 0", got)
	}
	if got := compareAny(b, c); got >= 0 {
		t.Errorf("[0,1,2] vs [1,0,1] = %d, want < 0", got)
	}
	if got := compareAny(a, c); got <= 0 {
		t.Errorf("[NaN,0,2] vs [1,0,1] = %d, want > 0", got)
	}
	if got := compareAny(math.NaN(), 1e308); got != 1 {
		t.Errorf("boxed float64 NaN vs finite = %d, want 1", got)
	}
}

// TestBoxedCompareAgreesWithColumnarForEveryFlatType is the table above
// widened from "the shapes whose box drops a DECLARATION" to EVERY flat type,
// which is what #569 showed was necessary.
//
// The window's MIN/MAX declined to declare an output type for twelve of the
// twenty-two types, so no query could reach the boxed comparator with an
// IPV4, IPV6 or MAC column at all and nothing here had a reason to name them.
// Once MIN/MAX declares from the input, the spilled and global (empty
// PARTITION BY) window paths compare those boxes — and Vector.GetValue
// renders all three for DISPLAY, where "9.0.0.1" sorts above "10.0.0.1" and
// "2001:db8::9" above "2001:db8::10" while the addresses do not.
//
// The types that need NO re-keying are in the table for the opposite reason:
// their agreement is a PROPERTY of the rendering (UUID and MAC are
// fixed-width lowercase hex, DATE is ISO, BYTES boxes as bytes), and a
// property nothing checks is a property that can be lost. Each value list
// deliberately contains a pair whose TEXT order and VALUE order differ where
// such a pair exists.
func TestBoxedCompareAgreesWithColumnarForEveryFlatType(t *testing.T) {
	cases := []struct {
		name string
		typ  parquet.TypeID
		vals []any
	}{
		{"bool", parquet.TypeBool, []any{nil, false, true}},
		{"int32", parquet.TypeInt32, []any{nil, int32(-9), int32(-2), int32(0), int32(2), int32(10)}},
		{"int64", parquet.TypeInt64, []any{nil, int64(-9), int64(0), int64(2), int64(10)}},
		{"float32", parquet.TypeFloat32, []any{nil, float32(-1.5), float32(0), float32(2), float32(10),
			float32(math.NaN())}},
		{"float64", parquet.TypeFloat64, []any{nil, -1.5, 0.0, 2.0, 10.0, math.NaN()}},
		{"string", parquet.TypeString, []any{nil, "", "10", "2", "alpha", "beta"}},
		// BYTES boxes as []byte and takes compareAny's bytes.Compare arm —
		// the arm whose ABSENCE was the documented reason BYTES was declined
		// (window.go's old prose); it has existed since #415.
		{"bytes", parquet.TypeBytes, []any{nil, []byte(nil), []byte("10"), []byte("2"), []byte("alpha")}},
		{"timestamp", parquet.TypeTimestamp, []any{nil, int64(0), int64(1_700_000_000_000), int64(1_700_000_061_000)}},
		// "9.x" above "10.x" as text, below it as an address.
		{"ipv4", parquet.TypeIPv4, []any{nil, "0.0.0.0", "9.255.255.255", "10.0.0.1", "10.0.0.2",
			"192.168.1.1", "255.255.255.255"}},
		// "2001:db8::9" above "2001:db8::10" as text, below it as an address.
		{"ipv6", parquet.TypeIPv6, []any{nil, "::", "::1", "2001:db8::9", "2001:db8::10", "2001:db8::1387",
			"ffff::1"}},
		// PostgreSQL inet order: the common bits, then the mask, then the
		// full unmasked address — none of which the stored text gives.
		{"cidr", parquet.TypeCIDR, []any{nil, "9.255.255.255/32", "10.0.0.0/8", "10.0.0.1/8",
			"192.168.1.5/24", "192.168.1.0/32", "192.168.255.0/24", "2001:db8::/32"}},
		{"mac", parquet.TypeMAC, []any{nil, "00:00:00:00:00:00", "aa:bb:cc:00:00:09", "aa:bb:cc:00:00:10",
			"ff:ff:ff:ff:ff:ff"}},
		{"port", parquet.TypePort, []any{nil, int32(0), int32(22), int32(1024), int32(65535)}},
		{"protocol", parquet.TypeProtocol, []any{nil, int32(0), int32(6), int32(17), int32(255)}},
		{"duration", parquet.TypeDuration, []any{nil, int64(-1), int64(0), int64(1_000_000)}},
		{"uuid", parquet.TypeUUID, []any{nil,
			"00000000-0000-4000-8000-000000000000",
			"00000000-0000-4000-8000-000000000009",
			"00000000-0000-4000-8000-000000001387",
			"ffffffff-ffff-4fff-bfff-ffffffffffff"}},
		{"date", parquet.TypeDate, []any{nil, "1970-01-01", "2009-12-31", "2010-01-01", "2024-02-29"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertBoxedAgreesWithColumnar(t,
				parquet.Column{Name: tc.name, Type: tc.typ, Nullable: true}, tc.vals)
		})
	}
}

// TestBoxedAddressCompareIsNotTheTextOrder pins the DIRECTION for the three
// types #569 re-keys, the way TestBoxedRowComparePositionalNotAlphabetical
// does for ROW. Each pair below compares one way as TEXT and the other way as
// an address, so a comparator that fell back to compareAny's string arm fails
// here rather than merely agreeing with a columnar comparator that was never
// consulted.
func TestBoxedAddressCompareIsNotTheTextOrder(t *testing.T) {
	cases := []struct {
		name     string
		typ      parquet.TypeID
		lo, hi   string
		wantText int // what a plain string compare answers for (lo, hi)
	}{
		{"ipv4", parquet.TypeIPv4, "9.255.255.255", "10.0.0.1", 1},
		{"ipv6", parquet.TypeIPv6, "2001:db8::9", "2001:db8::10", 1},
		{"cidr", parquet.TypeCIDR, "9.255.255.255/32", "10.0.0.0/8", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sign(compareAny(tc.lo, tc.hi)); got != tc.wantText {
				t.Fatalf("the premise no longer holds: compareAny(%q,%q) = %d, want %d — "+
					"pick a pair whose text order still disagrees with the address order",
					tc.lo, tc.hi, got, tc.wantText)
			}
			cmp := newBoxedCompare(parquet.Column{Name: "v", Type: tc.typ, Nullable: true})
			if got := sign(cmp(tc.lo, tc.hi)); got != -1 {
				t.Errorf("boxed %v compare(%q, %q) = %d, want -1 (the address order)",
					tc.typ, tc.lo, tc.hi, got)
			}
			if got := sign(cmp(tc.hi, tc.lo)); got != 1 {
				t.Errorf("boxed %v compare(%q, %q) = %d, want 1", tc.typ, tc.hi, tc.lo, got)
			}
		})
	}
}
