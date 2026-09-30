// SPDX-License-Identifier: MIT

package expr

import (
	"testing"
)

// THE PAIR'S KERNEL READS EVERY SPELLING OF A DATE AND ASKS THE ONE RULE
// (#1378). A DATE scalar subquery hands its value over as ISO text;
// the kernel read a DATE side only as an int64, so `ts = (SELECT d …)` fell
// through to compare()'s magnitude guess, which read the TIMESTAMP epoch
// ms -1 (1969-12-31 23:59:59.999) as day -1 and called it equal to DATE
// 1969-12-31. Each case is (declared kinds, boxes) → PostgreSQL's order of
// the DATE's midnight against the instant; a box neither declaration reads
// raises rather than guess.
func TestArcDTPairKernelReadsEverySpellingAtTheRule(t *testing.T) {
	cases := []struct {
		name   string
		lk, rk boxKind
		lv, rv any
		want   int
	}{
		{"tsMinus1VsDateText", boxTimestamp, boxDate, int64(-1), "1969-12-31", 1},
		{"dateTextVsTsMinus1", boxDate, boxTimestamp, "1969-12-31", int64(-1), -1},
		{"dateDaysVsTsMinus1", boxDate, boxTimestamp, int64(-1), int64(-1), -1},
		{"dateInt32VsTsMidnight", boxDate, boxTimestamp, int32(-1), int64(-86_400_000), 0},
		{"tsPlus1VsDate1970", boxTimestamp, boxDate, int64(1), "1970-01-01", 1},
		{"tsMinus500000VsDate1969", boxTimestamp, boxDate, int64(-500_000), "1969-12-31", 1},
		{"tsTextVsDateText", boxTimestamp, boxDate, "1970-01-01 00:00:00", "1970-01-01", 0},
		{"date9999VsTs", boxDate, boxTimestamp, "9999-12-31", int64(253_402_214_400_000), 0},
	}
	for _, c := range cases {
		got, ok := dateTimestampOrder(c.lk, c.rk, c.lv, c.rv)
		if !ok || got != c.want {
			t.Errorf("%s: dateTimestampOrder = (%d, %v), want (%d, true)", c.name, got, ok, c.want)
		}
	}
	func() {
		defer func() {
			r := recover()
			if _, ok := r.(fatalEval); !ok {
				t.Errorf("an unreadable DATE box answered instead of raising (recovered %v)", r)
			}
		}()
		dateTimestampOrder(boxDate, boxTimestamp, 1.5, int64(0))
	}()
	if _, ok := dateTimestampOrder(boxDate, boxTimestamp, nil, int64(0)); ok {
		t.Errorf("a NULL side answered an order")
	}
}
