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
// It is the ONE statement of the rule: the direct comparison's kernel
// already reads the pair this way, and every carrier that turns the pair
// into a KEY asks here — the equi-join key ladder
// (physical.joinKeyCommonType, whose rung exec.AppendWidenedKeyValue
// encodes) and the membership set (expr.memberTemporalProbe) (#1378).
func TemporalCommonType(a, b TypeID) (TypeID, bool) {
	if (a == TypeDate && b == TypeTimestamp) || (a == TypeTimestamp && b == TypeDate) {
		return TypeTimestamp, true
	}
	return 0, false
}
