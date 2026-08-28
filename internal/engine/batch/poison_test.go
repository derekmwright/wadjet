package batch

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestPoisonVectorCoversEveryType is poison.go's missing gate: a table-driven
// check, one entry per one of the engine's 22 column types (CLAUDE.md's Type
// System list), that poisonVector actually overwrites the value arena a real
// query could read from — not just that SOME field on the vector changed.
//
// Each case builds a single-row vector holding a REAL, non-poison value,
// confirms poisonVector left it alone (sanity: the value really is what the
// case claims), pins the vector, calls poisonVector, and asserts the EXACT
// slot poison.go documents writing now holds the documented pattern. A
// forgotten type arm — the shape #391 had before poison.go existed, for the
// one arm (BYTES) that happened to have a real bug behind it — must fail
// this test directly: the assertions read the storage slot itself, not a
// downstream symptom (like a query's answer) that an unrelated bug could
// also produce.
func TestPoisonVectorCoversEveryType(t *testing.T) {
	type poisonCase struct {
		name  string
		col   parquet.Column
		value any
		// check runs once before poisoning (poisoned=false, must see the
		// real value) and once after (poisoned=true, must see the pattern
		// poison.go documents for that storage arm).
		check func(t *testing.T, v *Vector, poisoned bool)
	}

	dateDaysForTest := func(s string) int32 {
		d, ok := parseDateString(s)
		if !ok {
			t.Fatalf("parseDateString(%q) failed in test setup", s)
		}
		return d
	}

	int32Check := func(want int32) func(*testing.T, *Vector, bool) {
		return func(t *testing.T, v *Vector, poisoned bool) {
			t.Helper()
			w := want
			if poisoned {
				w = -1
			}
			if got := v.Int32Data[0]; got != w {
				t.Errorf("Int32Data[0] = %d, want %d (poisoned=%v)", got, w, poisoned)
			}
		}
	}
	int64Check := func(want int64) func(*testing.T, *Vector, bool) {
		return func(t *testing.T, v *Vector, poisoned bool) {
			t.Helper()
			w := want
			if poisoned {
				w = -1
			}
			if got := v.Int64Data[0]; got != w {
				t.Errorf("Int64Data[0] = %d, want %d (poisoned=%v)", got, w, poisoned)
			}
		}
	}
	// bytesCheck reads the same slot poisonVector documents scribbling: the
	// arena filled to CAPACITY, not length, since Reset truncates Data to
	// [:0] and a recycle appends into the same backing array.
	bytesCheck := func(want []byte) func(*testing.T, *Vector, bool) {
		return func(t *testing.T, v *Vector, poisoned bool) {
			t.Helper()
			d := v.BytesData.Data
			if cap(d) == 0 {
				t.Fatalf("BytesData.Data has zero capacity — the value was never written")
			}
			if !poisoned {
				got := d[v.BytesData.Offsets[0]:v.BytesData.Offsets[1]]
				if string(got) != string(want) {
					t.Fatalf("BytesData value = %q, want %q (test setup is wrong, not poison)", got, want)
				}
				return
			}
			full := d[:cap(d)]
			for i, b := range full {
				if b != poisonByte {
					t.Errorf("BytesData.Data[%d] = %#x, want poison byte %#x", i, b, poisonByte)
				}
			}
		}
	}
	macToInt64 := func(t *testing.T, s string) int64 {
		t.Helper()
		hw, err := net.ParseMAC(s)
		if err != nil {
			t.Fatalf("test setup: %v", err)
		}
		var n uint64
		for _, b := range hw {
			n = (n << 8) | uint64(b)
		}
		return int64(n)
	}
	ipv4Int64 := func(t *testing.T, s string) int64 {
		t.Helper()
		ip4 := net.ParseIP(s).To4()
		if ip4 == nil {
			t.Fatalf("test setup: %q is not a valid IPv4 address", s)
		}
		return int64(binary.BigEndian.Uint32(ip4))
	}

	cases := []poisonCase{
		{"Bool", parquet.Column{Name: "c", Type: parquet.TypeBool}, false,
			func(t *testing.T, v *Vector, poisoned bool) {
				want := false
				if poisoned {
					want = true
				}
				if got := v.BoolData[0]; got != want {
					t.Errorf("BoolData[0] = %v, want %v (poisoned=%v)", got, want, poisoned)
				}
			}},
		{"Int32", parquet.Column{Name: "c", Type: parquet.TypeInt32}, int32(7), int32Check(7)},
		{"Int64", parquet.Column{Name: "c", Type: parquet.TypeInt64}, int64(70000), int64Check(70000)},
		{"Float32", parquet.Column{Name: "c", Type: parquet.TypeFloat32}, float32(1.5),
			func(t *testing.T, v *Vector, poisoned bool) {
				want := float32(1.5)
				if poisoned {
					want = -1
				}
				if got := v.Float32Data[0]; got != want {
					t.Errorf("Float32Data[0] = %v, want %v (poisoned=%v)", got, want, poisoned)
				}
			}},
		{"Float64", parquet.Column{Name: "c", Type: parquet.TypeFloat64}, float64(2.5),
			func(t *testing.T, v *Vector, poisoned bool) {
				want := float64(2.5)
				if poisoned {
					want = -1
				}
				if got := v.Float64Data[0]; got != want {
					t.Errorf("Float64Data[0] = %v, want %v (poisoned=%v)", got, want, poisoned)
				}
			}},
		{"String", parquet.Column{Name: "c", Type: parquet.TypeString}, "real-string",
			bytesCheck([]byte("real-string"))},
		{"Bytes", parquet.Column{Name: "c", Type: parquet.TypeBytes}, []byte("real-bytes"),
			bytesCheck([]byte("real-bytes"))},
		{"Timestamp", parquet.Column{Name: "c", Type: parquet.TypeTimestamp}, int64(1_700_000_000_000),
			int64Check(1_700_000_000_000)},
		{"IPv4", parquet.Column{Name: "c", Type: parquet.TypeIPv4}, "10.1.2.3",
			int64Check(ipv4Int64(t, "10.1.2.3"))},
		{"IPv6", parquet.Column{Name: "c", Type: parquet.TypeIPv6}, "2001:db8::1",
			bytesCheck(net.ParseIP("2001:db8::1").To16())},
		{"CIDR", parquet.Column{Name: "c", Type: parquet.TypeCIDR}, "192.168.1.0/24",
			bytesCheck([]byte("192.168.1.0/24"))},
		{"MAC", parquet.Column{Name: "c", Type: parquet.TypeMAC}, "aa:bb:cc:dd:ee:ff",
			int64Check(macToInt64(t, "aa:bb:cc:dd:ee:ff"))},
		{"Port", parquet.Column{Name: "c", Type: parquet.TypePort}, int32(8080), int32Check(8080)},
		{"Protocol", parquet.Column{Name: "c", Type: parquet.TypeProtocol}, int32(6), int32Check(6)},
		{"Duration", parquet.Column{Name: "c", Type: parquet.TypeDuration}, int64(5_000_000),
			int64Check(5_000_000)},
		{"UUID", parquet.Column{Name: "c", Type: parquet.TypeUUID}, "00000000-0000-4000-8000-000000000001",
			bytesCheck(parseUUID("00000000-0000-4000-8000-000000000001"))},
		{"Date", parquet.Column{Name: "c", Type: parquet.TypeDate}, "2026-08-23",
			int32Check(dateDaysForTest("2026-08-23"))},
		{"Decimal", parquet.Column{Name: "c", Type: parquet.TypeDecimal, Precision: 18, Scale: 4}, float64(12.3456),
			func(t *testing.T, v *Vector, poisoned bool) {
				if poisoned {
					want := Int128{Lo: ^uint64(0), Hi: -1}
					if got := v.DecimalData.Data[0]; got != want {
						t.Errorf("DecimalData.Data[0] = %+v, want %+v", got, want)
					}
					return
				}
				want := Int128FromFloat64(12.3456, v.DecimalData.Scale)
				if got := v.DecimalData.Data[0]; got != want {
					t.Fatalf("DecimalData.Data[0] = %+v, want %+v (test setup is wrong, not poison)", got, want)
				}
			}},
		{"Array", parquet.Column{Name: "c", Type: parquet.TypeArray,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString}},
			[]any{"real-element"},
			func(t *testing.T, v *Vector, poisoned bool) {
				bytesCheck([]byte("real-element"))(t, v.Child, poisoned)
			}},
		{"Row", parquet.Column{Name: "c", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString},
			{Name: "b", Type: parquet.TypeInt64},
		}}, map[string]any{"a": "real-a", "b": int64(99)},
			func(t *testing.T, v *Vector, poisoned bool) {
				bytesCheck([]byte("real-a"))(t, v.Children[0], poisoned)
				int64Check(99)(t, v.Children[1], poisoned)
			}},
		{"Map", parquet.Column{Name: "c", Type: parquet.TypeMap, ElementType: &parquet.Column{
			Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64},
			},
		}}, []any{map[string]any{"key": "k0", "value": int64(42)}},
			func(t *testing.T, v *Vector, poisoned bool) {
				entry := v.Child // MAP is ARRAY(ROW("key","value")) — see schema.go
				bytesCheck([]byte("k0"))(t, entry.Children[0], poisoned)
				int64Check(42)(t, entry.Children[1], poisoned)
			}},
		{"Vector", parquet.Column{Name: "c", Type: parquet.TypeVector, Dimension: 4},
			[]float32{1, 2, 3, 4},
			func(t *testing.T, v *Vector, poisoned bool) {
				want := []float32{1, 2, 3, 4}
				if poisoned {
					want = []float32{-1, -1, -1, -1}
				}
				for i, w := range want {
					if got := v.Float32Data[i]; got != w {
						t.Errorf("Float32Data[%d] = %v, want %v (poisoned=%v)", i, got, w, poisoned)
					}
				}
			}},
	}

	seen := map[parquet.TypeID]bool{}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			b := FromRows([]parquet.Column{tc.col}, []map[string]any{{"c": tc.value}})
			v := b.Columns[0]
			tc.check(t, v, false)
			poisonVector(v)
			tc.check(t, v, true)
		})
		seen[tc.col.Type] = true
	}

	// Every TypeID the engine defines must have a case above — a 23rd type
	// added without a poison case here is exactly the coverage gap that let
	// #391 hide behind 19 of the previous 22 types having none at all.
	for _, typ := range []parquet.TypeID{
		parquet.TypeBool, parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32,
		parquet.TypeFloat64, parquet.TypeString, parquet.TypeBytes, parquet.TypeTimestamp,
		parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC,
		parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration, parquet.TypeUUID,
		parquet.TypeDate, parquet.TypeDecimal, parquet.TypeArray, parquet.TypeRow,
		parquet.TypeMap, parquet.TypeVector,
	} {
		if !seen[typ] {
			t.Errorf("no poison test case for type %v — add one", typ)
		}
	}
}
