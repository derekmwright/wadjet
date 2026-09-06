package parquet

import (
	"strings"
	"testing"
)

// #883 completeness: MapKeyCarrierText must have a decision for EVERY TypeID,
// so no carrier is silently mangled by a fmt.Sprint fall-through. The switch
// missed DECIMAL, then the network types, then BYTES; this enumerates all 22
// and asserts each is either rendered canonically, is a carrier that is already
// its own text, or is a type that cannot be a valid map key. A new TypeID with
// no entry here FAILS the exhaustiveness loop below.
func TestMapKeyCarrierTextCoversEveryType(t *testing.T) {
	ipv6 := []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01}
	uuid := []byte{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab}

	type expect struct {
		scale int32
		box   any
		want  string // rendered text for a valid key
		key   bool   // is a valid, round-trippable flat map key
	}
	table := map[TypeID]expect{
		TypeBool:      {0, true, "true", true},
		TypeInt32:     {0, int64(42), "42", true},
		TypeInt64:     {0, int64(42), "42", true},
		TypeFloat32:   {0, float64(1.5), "1.5", true},
		TypeFloat64:   {0, float64(1.5), "1.5", true},
		TypeString:    {0, "hi", "hi", true},
		TypeBytes:     {0, []byte("hi"), "hi", true},
		TypeTimestamp: {0, int64(1699920000000), "1699920000000", true},
		TypeIPv4:      {0, int64(0xC0A8010A), "192.168.1.10", true},
		TypeIPv6:      {0, ipv6, "2001:0db8:0000:0000:0000:0000:0000:0001", true},
		TypeCIDR:      {0, "10.0.0.0/8", "10.0.0.0/8", true},
		TypeMAC:       {0, int64(0xAABBCCDDEEFF), "aa:bb:cc:dd:ee:ff", true},
		TypePort:      {0, int64(8080), "8080", true},
		TypeProtocol:  {0, int64(6), "6", true},
		TypeDuration:  {0, int64(1000), "1000", true},
		TypeUUID:      {0, uuid, "12345678-1234-1234-1234-1234567890ab", true},
		TypeDate:      {0, int64(19675), "2023-11-14", true},
		TypeDecimal:   {4, int64(127500), "12.7500", true},
		// Not a valid flat map key: containers are never a leaf, VECTOR is
		// refused by batch.SetValue's #361 guard on every path (ADR-0018 §13).
		TypeArray:  {0, nil, "", false},
		TypeRow:    {0, nil, "", false},
		TypeMap:    {0, nil, "", false},
		TypeVector: {0, nil, "", false},
	}

	// Exhaustiveness: every TypeID from TypeBool..TypeVector has an entry.
	for ty := TypeBool; ty <= TypeVector; ty++ {
		e, ok := table[ty]
		if !ok {
			t.Fatalf("TypeID %s (%d) has no map-key decision — add a case to "+
				"MapKeyCarrierText and an entry here", ty, ty)
		}
		if !e.key {
			continue
		}
		got := MapKeyCarrierText(ty, e.scale, e.box)
		if got != e.want {
			t.Errorf("%s: MapKeyCarrierText = %q, want %q", ty, got, e.want)
		}
		// A []byte carrier must never leak the Go slice print form.
		if strings.HasPrefix(got, "[") {
			t.Errorf("%s: rendered %q — looks like a raw carrier print, not the value's text", ty, got)
		}
	}
}
