package exec

import (
	"fmt"
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
	// A pair the ladder does not describe cannot reach here: physical
	// .joinKeyCommonType answers only INT64, FLOAT64 and DECIMAL, and each
	// arm above accepts every source type that can resolve to it. Arriving
	// anyway means the plan-time ladder and this encoder have parted company,
	// and the pre-#615 answer — the narrow side's own encoding — is the
	// silent wrong answer #615 IS. Raise instead; the query-scoped boundary
	// (ADR-0019) turns it into a query error on both the local pipeline and
	// the worker fragment, so it costs a query rather than a server.
	panic(fmt.Sprintf("join key: no encoding for %s at resolved common type %s "+
		"(exec.AppendWidenedKeyValue, #615)", v.Type, target))
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
	panic(fmt.Sprintf("join key: %s has no float64 reading at resolved common type FLOAT64 "+
		"(exec.widenKeyFloat64, #615)", v.Type))
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

// keyEncodingClass groups the types appendColumnValue encodes IDENTICALLY, so
// that "these two key columns produce comparable bytes" is a question with one
// answer rather than a list of pairs.
//
// It is deliberately coarser than the type: PORT, PROTOCOL and DATE key as an
// INT32's four little-endian bytes and IPv4, MAC, TIMESTAMP and DURATION as an
// INT64's eight, so a join between one of those and its carrier integer has
// always keyed alike and must keep doing so. STRING, BYTES, IPv6 and UUID
// share the length-prefixed bytes arm for the same reason.
func keyEncodingClass(t batch.TypeID) int {
	switch t {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return 1
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return 2
	case batch.TypeFloat32:
		return 3
	case batch.TypeFloat64:
		return 4
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return 5
	case batch.TypeCIDR:
		return 6
	case batch.TypeBool:
		return 7
	case batch.TypeDecimal:
		return 8
	case batch.TypeVector:
		return 9
	case batch.TypeArray, batch.TypeMap:
		return 10
	case batch.TypeRow:
		return 11
	}
	return 0
}

// checkProbeKeyTypes is the RUNTIME backstop under the plan-time resolution:
// the place where both sides' ACTUAL encodings are known at the same time.
//
// The planner resolves a key pair from DECLARED types, and a side it cannot
// type resolves to KeyTypeUnresolved — which is correct for every pair whose
// two sides agree and silently wrong for one whose sides do not. Rather than
// refuse at plan time on "cannot type" (which would refuse every join over a
// table function, an unannotated scan or a shape the declared-type walk does
// not cover, most of them perfectly well-typed at run time), the refusal
// lives HERE, where the question is decidable and the answer cannot be a
// false positive.
//
// Two conditions, and only these two:
//
//	R1 the integer fast path is on and a PROBE key column is not
//	   integer-class. This is #615's panic: tryEnableIntKey saw only the
//	   BUILD column, and inlineIntProbe / executeSemiAntiJoin / the bloom
//	   then indexed the probe column's nil Int32Data / Int64Data. An error
//	   here makes that index structurally unreachable.
//	R2 both key columns are on the numeric ladder, their key ENCODINGS
//	   differ, and the plan said no widening. This is #615's silent miss:
//	   eight little-endian bytes on one side and a canonical decimal key on
//	   the other, matching only where the byte strings coincide by accident.
//
// A pair the ladder does not describe (a STRING against an INT64, a DATE
// against a TIMESTAMP) is left exactly where it was: those are ill-typed
// joins wadjet has always answered with no matches, and turning them into
// errors is a separate question with a separate authority.
func (h *HashJoin) checkProbeKeyTypes(b *batch.RecordBatch) error {
	for i, pi := range h.probeKeyIdx {
		if pi < 0 || pi >= len(b.Columns) {
			continue
		}
		probeT := b.Columns[pi].Type
		if (h.useIntKey || h.useDualIntKey) && !isIntKeyColumn(probeT) {
			return fmt.Errorf("join key %q is %s on the probe side and the build side took the "+
				"integer key path: the pair's common type was not resolved at plan time "+
				"(exec.HashJoin.KeyTypes, #615)", h.LeftKeys[i], probeT)
		}
		bi := -1
		if i < len(h.buildKeyIdx) {
			bi = h.buildKeyIdx[i]
		}
		if bi < 0 || bi >= len(h.buildSchema) {
			continue
		}
		buildT := h.buildSchema[bi].Type
		if keyEncodingClass(probeT) == keyEncodingClass(buildT) {
			continue
		}
		if !joinKeyLadderType(probeT) || !joinKeyLadderType(buildT) {
			continue
		}
		if keyPairResolved(h.KeyTypes, i) {
			continue // the planner resolved it; the encoders are widening
		}
		return fmt.Errorf("join key %q is %s on the probe side and %q is %s on the build side, "+
			"and the pair's common type was not resolved at plan time: the two sides would be "+
			"keyed at two different encodings and match only by coincidence "+
			"(exec.HashJoin.KeyTypes, #615)",
			h.LeftKeys[i], probeT, h.RightKeys[i], buildT)
	}
	return nil
}

// keyPairResolved reports whether the planner decided a common type for this
// key pair. It is NOT "the resolved type differs from this side's": the
// resolved type equals one side's own whenever that side is already the wider
// one (a DECIMAL probe against an INT64 build resolves to DECIMAL), and
// reading that as unresolved made the backstop fire on a correctly widened
// join.
func keyPairResolved(types []batch.TypeID, i int) bool {
	return i < len(types) && types[i] != KeyTypeUnresolved
}

// joinKeyLadderType reports whether a type is on the numeric ladder
// physical.joinKeyCommonType describes — the only pairs #615 claims.
func joinKeyLadderType(t batch.TypeID) bool {
	switch t {
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32,
		batch.TypeFloat64, batch.TypeDecimal:
		return true
	}
	return false
}
