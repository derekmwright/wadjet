// SPDX-License-Identifier: MIT

package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A SCALAR SUBQUERY'S ANSWER IS THE BOX ITS DECLARED TYPE HAS ON THE ROW PATH
// (#1428, #1431). The runner hands a DATE over as its ISO text and a
// TIMESTAMP as a bare int64, and producedTemporal — the one answer to "which
// unit does this operand carry" — had no subquery arm, so CAST, extract and
// date arithmetic read the TIMESTAMP's milliseconds as a number. Both halves
// are asked here: the value Eval answers and the unit producedTemporal names.
func TestArcSSScalarAnswerIsItsDeclaredBox(t *testing.T) {
	cases := []struct {
		name string
		decl batch.TypeID
		row  any
		want any
		kind castTemporalKindT
	}{
		{"dateText9999", batch.TypeDate, "9999-12-31", int64(2_932_896), castToDateKind},
		{"dateText1969", batch.TypeDate, "1969-12-31", int64(-1), castToDateKind},
		{"tsMillisMinus1", batch.TypeTimestamp, int64(-1), int64(-1), castToTimestampKind},
		{"tsText", batch.TypeTimestamp, "1000-01-01 00:00:00", int64(-30_610_224_000_000), castToTimestampKind},
		{"intUntouched", batch.TypeInt64, int64(7), int64(7), castNotTemporal},
		{"decimalTextUntouched", batch.TypeDecimal, "2.25", "2.25", castNotTemporal},
		{"null", batch.TypeTimestamp, nil, nil, castToTimestampKind},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sq := &ScalarSubquery{SQL: "SELECT x FROM t", Decl: c.decl, DeclKnown: true,
				Runner: func(string) ([]map[string]any, error) {
					return []map[string]any{{"x": c.row}}, nil
				}}
			if got := sq.Eval(nil, 0); got != c.want {
				t.Errorf("Eval = %#v, want %#v", got, c.want)
			}
			if got := producedTemporal(sq, nil); got != c.kind {
				t.Errorf("producedTemporal = %v, want %v", got, c.kind)
			}
		})
	}
}

// A SAME-TYPE TEMPORAL PAIR IS READ BY THE PAIR RULE, NEVER BY MAGNITUDE
// (#1427). A DATE column's day count against a DATE operand still spelled as
// ISO text fell to compare(), whose magnitude guess read 9999-12-31's
// 2 932 896 days as milliseconds, and `d = (SELECT d …)` answered 0 rows.
func TestArcSSSameTypeTemporalPairReadsEachBoxInItsUnit(t *testing.T) {
	cases := []struct {
		name   string
		lk, rk boxKind
		lv, rv any
		want   int
	}{
		{"date9999DaysVsText", boxDate, boxDate, int64(2_932_896), "9999-12-31", 0},
		{"date9999TextVsDays", boxDate, boxDate, "9999-12-31", int64(2_932_896), 0},
		{"date1969DaysVsText", boxDate, boxDate, int64(-1), "1969-12-31", 0},
		{"date1000DaysVsLaterText", boxDate, boxDate, int64(-354_285), "1970-01-01", -1},
		{"tsMinus1VsText", boxTimestamp, boxTimestamp, int64(-1), "1969-12-31 23:59:59.999", 0},
		{"tsTextVsMillisLater", boxTimestamp, boxTimestamp, "1969-12-31 23:59:59.999", int64(0), -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !pairApplies(c.lk, c.rk, "", "") {
				t.Fatalf("pairApplies(%v, %v) = false: the pair is left to compare()'s guess", c.lk, c.rk)
			}
			got, ok, unknown := orderByKinds(c.lk, c.rk, c.lv, c.rv, "", "")
			if !ok || unknown || got != c.want {
				t.Errorf("orderByKinds = (%d, %v, %v), want (%d, true, false)", got, ok, unknown, c.want)
			}
		})
	}
}
