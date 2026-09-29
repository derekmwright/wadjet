// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A DATE JOIN KEY MEETS A TIMESTAMP ONE AT TIMESTAMP (#1378): PostgreSQL's
// `date = timestamp` promotes the DATE to its midnight, and the equi-join key
// ladder — the one rule every join, semi join and anti join's key pair is
// typed by, an IN or EXISTS decorrelated to one included — resolves the pair
// to TIMESTAMP in either order. At v0.25.2 the pair was declined, each side
// keyed at its own encoding (a day count against milliseconds), and `a.d =
// r.ts` as a key matched nothing where the same comparison as a filter
// matched. Same-type pairs stay unresolved: they key as they always did.
func TestArcDTJoinKeyLadderPromotesDateToTimestamp(t *testing.T) {
	cases := []struct {
		a, b, want parquet.TypeID
		ok         bool
	}{
		{parquet.TypeDate, parquet.TypeTimestamp, parquet.TypeTimestamp, true},
		{parquet.TypeTimestamp, parquet.TypeDate, parquet.TypeTimestamp, true},
		{parquet.TypeDate, parquet.TypeDate, 0, false},
		{parquet.TypeTimestamp, parquet.TypeTimestamp, 0, false},
		{parquet.TypeDate, parquet.TypeInt32, 0, false},
		{parquet.TypeTimestamp, parquet.TypeInt64, 0, false},
	}
	for _, c := range cases {
		got, ok := joinKeyCommonType(c.a, c.b)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("joinKeyCommonType(%v, %v) = (%v, %v), want (%v, %v)", c.a, c.b, got, ok, c.want, c.ok)
		}
	}
}
