package exec

import (
	"math"
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A join key is built at the pair's COMMON type, not at each side's own
// storage type (#615, #650, #663).
//
// ADR-0023's rule is that a key and the comparator name ONE relation: "these
// two rows compare equal" and "these two rows key alike" have to be the same
// statement, or the join answers something the WHERE spelling of it does not.
// A comparison already resolves its operand pair to PostgreSQL's common type
// before comparing; the key did not. It called appendColumnValue with
// `v.Type` on each side independently, so `a.i = b.d` (INT64 against
// DECIMAL) built eight little-endian bytes on one side and a canonical
// decimal key on the other, and matched only where those byte strings
// coincided by accident — one pair over a ten-row fixture, not zero, which is
// why the failure reads as a wrong answer rather than as an empty one.
//
// The resolved type is decided at PLAN time (physical.resolveJoinKeyTypes,
// which is where the two sides' declared types are both in hand) and carried
// into the operator, on BOTH execution paths and into the shuffle's partition
// hash — a repartition that routes at the column's own width sends equal
// values to different partitions, which is the same defect one layer down.
//
// KeyTypeUnresolved means "this pair needs no widening": either the planner
// could not type one of the sides, or the two sides already agree. It is the
// value every existing caller gets, and it takes exactly the code path the
// key had before, byte for byte.
const KeyTypeUnresolved = batch.TypeID(-1)

// KeyTypeAt is the type the i'th key column of `types` must be encoded at,
// or the column's own type when the pair is unresolved or already agreed.
func KeyTypeAt(types []batch.TypeID, i int, own batch.TypeID) batch.TypeID {
	if i >= len(types) {
		return own
	}
	t := types[i]
	if t == KeyTypeUnresolved || t == own {
		return own
	}
	return t
}

// AppendWidenedKeyValue encodes v's row as key bytes AT `target`, which is
// the resolved common type of the pair this column is one half of.
//
// The `target == v.Type` case is the whole of the common case — every
// same-type join, which is every TPC-H join — and it is a single comparison
// ahead of the untouched appendColumnValue call. Nothing else here runs for
// it.
//
// The caller has already established that the row is NOT null and written the
// flag byte, exactly as it does for appendColumnValue.
func AppendWidenedKeyValue(buf []byte, v *batch.Vector, row int, target batch.TypeID) []byte {
	if target == v.Type {
		return appendColumnValue(buf, v, row, v.Type)
	}
	return appendCoercedKeyValue(buf, v, row, target)
}

// appendCoercedKeyValue is the arm that actually moves a value. It is split
// out so AppendWidenedKeyValue stays under the inliner's cost budget: the
// same-type case is then one comparison ahead of the call appendColumnValue
// always was, which is what keeps a same-type join — every TPC-H join — at
// its old cost.
func appendCoercedKeyValue(buf []byte, v *batch.Vector, row int, target batch.TypeID) []byte {
	switch target {
	case batch.TypeInt64:
		// INT32 ⊕ INT64 → INT64: the narrow side's four little-endian bytes
		// become eight. Exact, and the only integer rung there is.
		val, _ := intKeyFromVector(v, row)
		return append(buf,
			byte(val), byte(val>>8), byte(val>>16), byte(val>>24),
			byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
	case batch.TypeFloat64:
		val := keyFloat64bits(widenKeyFloat64(v, row))
		return append(buf,
			byte(val), byte(val>>8), byte(val>>16), byte(val>>24),
			byte(val>>32), byte(val>>40), byte(val>>48), byte(val>>56))
	case batch.TypeDecimal:
		// DECIMAL ⊕ INTEGER → DECIMAL. AppendDecimalKey is scale-normalized
		// (it strips trailing zero digits), so an integer keyed as its own
		// value at scale 0 is byte-identical to the same quantity stored at
		// any scale: 2 keys alike against DECIMAL(9,2) 2.00 and
		// DECIMAL(18,4) 2.0000. That is why this rung needs no (p,s) — a
		// key's scale is a property of the VALUE here, not of the column.
		if val, ok := intKeyFromVector(v, row); ok {
			return batch.AppendDecimalKey(buf, batch.Int128From(val), 0)
		}
	}
	// A pair the ladder does not describe reaching this far would be a
	// planner defect, and answering it with the narrow side's own encoding
	// is the pre-#615 behaviour — wrong, but not worse than wrong. Falling
	// back rather than panicking keeps a mis-resolved key a wrong answer in
	// one query instead of a killed server.
	return appendColumnValue(buf, v, row, v.Type)
}

// widenKeyFloat64 reads v's row as the float64 PostgreSQL would compare at.
//
// The DECIMAL arm is the one with a rounding question. PostgreSQL's
// `numeric::float8` is the CORRECTLY ROUNDED nearest double, and
// Int128.ToFloat64 computes `float64(unscaled) / math.Pow10(scale)`, which is
// correctly rounded only when both operands are themselves exact: |unscaled|
// below 2^53 and scale at most 22 (10^22 is the largest power of ten a
// float64 holds exactly). Inside that band — every DECIMAL(15,2) money
// column, every value this fixture or TPC-H holds — a single IEEE division of
// two exact operands is the correctly rounded quotient, so it agrees with
// PostgreSQL bit for bit. Outside it, two roundings can compound to an ulp,
// and an ulp is a join pair, so the wide arm re-reads the value through its
// EXACT decimal text and strconv.ParseFloat, which is correctly rounded by
// definition. That path allocates; it runs only for a DECIMAL key wider than
// 2^53 or at a scale past 22, against a float column.
func widenKeyFloat64(v *batch.Vector, row int) float64 {
	switch v.Type {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return float64(v.Int32Data[row])
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return float64(v.Int64Data[row])
	case batch.TypeFloat32:
		return float64(v.Float32Data[row])
	case batch.TypeFloat64:
		return v.Float64Data[row]
	case batch.TypeDecimal:
		d, scale := v.DecimalData.Data[row], v.DecimalData.Scale
		if scale >= 0 && scale <= 22 && d.FitsInt64() {
			if u := d.ToInt64(); u > -(1<<53) && u < 1<<53 {
				return float64(u) / math.Pow10(scale)
			}
		}
		f, err := strconv.ParseFloat(d.FormatDecimal(scale), 64)
		if err != nil {
			return d.ToFloat64(scale)
		}
		return f
	}
	return 0
}

// joinKeyUsesIntPath reports whether the resolved key types permit the
// integer hash fast path — the gate #615's panic came through.
//
// The flag used to be set from the BUILD side's storage type alone
// (tryEnableIntKey), and the probe loops then read the PROBE column's
// Int32Data / Int64Data with an unguarded `default:` arm. A DECIMAL or FLOAT
// probe against an integer build therefore indexed a nil slice:
// `index out of range [0] with length 0` out of inlineIntProbe and
// executeSemiAntiJoin, and the bloom's own guard (keyTypesAgree) disengaging
// the filter with "no integer storage" was the same condition caught one
// operator earlier.
//
// Resolved on the PAIR, the question answers itself: the int path is legal
// exactly when the pair's common type is an integer, and then BOTH sides are
// integer-class columns by construction, so every one of those loops is
// reading a slice that exists.
func joinKeyUsesIntPath(types []batch.TypeID, i int, own batch.TypeID) bool {
	t := KeyTypeAt(types, i, own)
	if t != own && !isIntKeyColumn(t) {
		return false
	}
	return isIntKeyColumn(own)
}
