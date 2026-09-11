package exec

import "github.com/derekmwright/wadjet/internal/engine/batch"

// numericPromotable is the shared float64-reading domain for aggregate/window
// readers, matching grouped ResolveRowSum. No numeric reading means NULL,
// never a valid zero (#412).
// Include integer/FLOAT/DECIMAL, PORT/PROTOCOL and DURATION. DATE/TIMESTAMP
// remain for shipped-answer compatibility, not PostgreSQL SUM/AVG semantics.
// Exclude IPV4/MAC, BYTES/STRING/IPV6/CIDR/UUID/BOOL and containers.
// Plan-time rejection of unsupported accumulation (including DATE/TIMESTAMP)
// needs the shared output-type decision tracked with #392; do not drop them
// from one reader alone. Ordering has a separate, wider domain below.
// See docs/internals/numeric-promotion-domain.md for the design.
func numericPromotable(typ batch.TypeID) bool {
	switch typ {
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64,
		batch.TypeDecimal, batch.TypePort, batch.TypeProtocol, batch.TypeDuration,
		batch.TypeDate, batch.TypeTimestamp:
		return true
	}
	return false
}

// numericFloat64 reads one cell as a float64. The second result is false when
// the type has no numeric reading — the caller must then produce NULL, never
// a zero.
func numericFloat64(v *batch.Vector, i int) (float64, bool) {
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[i], true
	case batch.TypeFloat32:
		return float64(v.Float32Data[i]), true
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeDuration:
		return float64(v.Int64Data[i]), true
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return float64(v.Int32Data[i]), true
	case batch.TypeDecimal:
		return v.DecimalData.Data[i].ToFloat64(v.DecimalData.Scale), true
	}
	return 0, false
}

// orderKeyFloat64 is the wider table for an ORDERING key rather than a value:
// MIN_BY/MAX_BY pick a row by comparing this column, and the value they return
// comes from the other one. Ordering is meaningful for every type whose stored
// integer form orders like the value it represents, which adds IPV4, MAC and
// BOOL to the numeric set — an address compares as its integer, a MAC as its
// 48 bits, false before true.
//
// Left out: the byte-backed types (STRING, BYTES, IPV6, CIDR, UUID) and the
// containers. Those order fine, but not through a float64, so MIN_BY over such
// an ordering key still answers NULL. Tracked separately.
func orderKeyFloat64(v *batch.Vector, i int) (float64, bool) {
	switch v.Type {
	case batch.TypeIPv4, batch.TypeMAC:
		return float64(v.Int64Data[i]), true
	case batch.TypeBool:
		if v.BoolData[i] {
			return 1, true
		}
		return 0, true
	}
	return numericFloat64(v, i)
}

func orderKeyPromotable(typ batch.TypeID) bool {
	switch typ {
	case batch.TypeIPv4, batch.TypeMAC, batch.TypeBool:
		return true
	}
	return numericPromotable(typ)
}
