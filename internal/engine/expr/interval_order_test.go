// SPDX-License-Identifier: MIT

package expr

import (
	"math"
	"testing"
)

// #1713c: two INTERVAL values compare by PostgreSQL 17.11's interval_cmp
// value (a month is 30 days, a day 24 hours), never by their rendered text.
// The equal / ordered pairs are PostgreSQL's own answers
// (internal/coordinator/testdata/arc_1713c_interval_order_pg17.tsv); the
// int64-overflow cases exercise the exact big.Int path, where a wrapped sum
// would invert the order.
func TestIntervalCompareIsPostgresIntervalCmp(t *testing.T) {
	big := math.MaxInt64 / 12 // a year count whose month count still fits, but not its seconds
	cases := []struct {
		name string
		a, b IntervalValue
		want int
	}{
		{"1 day = 24 hours", IntervalValue{Days: 1}, IntervalValue{Hours: 24}, 0},
		{"1 month = 30 days", IntervalValue{Months: 1}, IntervalValue{Days: 30}, 0},
		{"1 month = 720 hours", IntervalValue{Months: 1}, IntervalValue{Hours: 720}, 0},
		{"1 year = 360 days", IntervalValue{Years: 1}, IntervalValue{Days: 360}, 0},
		{"1 year = 12 months", IntervalValue{Years: 1}, IntervalValue{Months: 12}, 0},
		{"1 hour = 3600 seconds", IntervalValue{Hours: 1}, IntervalValue{Seconds: 3600}, 0},
		{"2 days < 10 days", IntervalValue{Days: 2}, IntervalValue{Days: 10}, -1},
		{"25 hours > 1 day", IntervalValue{Hours: 25}, IntervalValue{Days: 1}, 1},
		{"1 month < 31 days", IntervalValue{Months: 1}, IntervalValue{Days: 31}, -1},
		{"-1 month = -30 days", IntervalValue{Months: -1}, IntervalValue{Days: -30}, 0},
		{"-2 hours > -1 day", IntervalValue{Hours: -2}, IntervalValue{Days: -1}, 1},
		{"huge years > 1 second", IntervalValue{Years: big}, IntervalValue{Seconds: 1}, 1},
		{"-huge years < -1 second", IntervalValue{Years: -big}, IntervalValue{Seconds: -1}, -1},
		{"huge years = same in months", IntervalValue{Years: big}, IntervalValue{Months: big * 12}, 0},
		{"huge days < huge days + 1s", IntervalValue{Days: math.MaxInt64 / 2}, IntervalValue{Days: math.MaxInt64 / 2, Seconds: 1}, -1},
	}
	ops := []struct {
		op   CmpOp
		want func(int) bool
	}{
		{CmpEq, func(c int) bool { return c == 0 }},
		{CmpNe, func(c int) bool { return c != 0 }},
		{CmpLt, func(c int) bool { return c < 0 }},
		{CmpLe, func(c int) bool { return c <= 0 }},
		{CmpGt, func(c int) bool { return c > 0 }},
		{CmpGe, func(c int) bool { return c >= 0 }},
	}
	for _, c := range cases {
		for _, o := range ops {
			if got := compare(c.a, c.b, o.op); got != o.want(c.want) {
				t.Errorf("%s: compare(%v, %v, %v) = %v, want %v", c.name, c.a, c.b, o.op, got, o.want(c.want))
			}
		}
	}
	// An OPAQUE interval (input the single-unit fields cannot hold) has no
	// value here: it keeps the text order it always had, never a guess.
	opaque := IntervalValue{text: "1 day 02:00:00"}
	if !compare(opaque, IntervalValue{text: "1 day 02:00:00"}, CmpEq) || compare(opaque, IntervalValue{Hours: 26}, CmpEq) {
		t.Errorf("an opaque interval must keep its text comparison")
	}
}
