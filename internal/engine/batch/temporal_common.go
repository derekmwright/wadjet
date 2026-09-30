// SPDX-License-Identifier: MIT

package batch

// MillisPerDay is one DATE step in TIMESTAMP units: a DATE carries epoch
// days, a TIMESTAMP epoch milliseconds.
const MillisPerDay = 86_400_000

// TemporalCommonType is PostgreSQL's rule for a DATE meeting a TIMESTAMP
// (without time zone, the only TIMESTAMP this engine declares): the pair is
// compared as TIMESTAMP, the DATE promoted to its midnight — `date_eq_timestamp`
// and its siblings are the cross-type operators, and select_common_type
// resolves the pair to timestamp for a set operation or a CASE alike. So
// DATE '2024-01-02' equals TIMESTAMP '2024-01-02 00:00:00' and does not
// equal TIMESTAMP '2024-01-02 12:00:00'. ok is false for every other pair,
// the same-type pairs included.
//
// It is the ONE statement of the rule, and DateMidnightMillis is its one
// conversion. Every site where the pair meets asks here: the direct and
// scalar-subquery comparison's kernel (expr.dateTimestampOrder), the
// equi-join key ladder (physical.joinKeyCommonType, whose rung
// exec.AppendWidenedKeyValue encodes), the membership set
// (expr.memberTemporalSides), the stage DAG's inlined set, and the
// choice-fold refusal (physical.refuseFoldArms) (#1378).
func TemporalCommonType(a, b TypeID) (TypeID, bool) {
	if (a == TypeDate && b == TypeTimestamp) || (a == TypeTimestamp && b == TypeDate) {
		return TypeTimestamp, true
	}
	return 0, false
}

// TemporalPairType is TemporalCommonType for a COMPARISON's two operands,
// where a same-type pair is part of the rule too: a DATE meets a DATE at DATE
// and a TIMESTAMP a TIMESTAMP at TIMESTAMP, each box read in its own
// declaration's unit and never by its magnitude (#1427). It is a separate
// name because TemporalCommonType's ok=false for a same-type pair is what
// the join-key ladder and the choice-fold refusal read as "leave this pair
// alone".
func TemporalPairType(a, b TypeID) (TypeID, bool) {
	if a == b && (a == TypeDate || a == TypeTimestamp) {
		return a, true
	}
	return TemporalCommonType(a, b)
}

// DateMidnightMillis is the rule's one conversion: a DATE's epoch-day count
// as the TIMESTAMP of its midnight, in epoch milliseconds. It is exact on
// both sides of 1970 — day -1 (1969-12-31) is -86 400 000, that day's
// midnight, never a truncation toward zero — and it takes no floor because
// the promotion only ever goes this way: a TIMESTAMP is never read at DATE
// by the pair (a DATE parsed from text is floored to its day by its own
// input function before it arrives here).
func DateMidnightMillis(days int64) int64 {
	return days * MillisPerDay
}
