// SPDX-License-Identifier: MIT

package expr

import (
	"testing"
	"time"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ivCatch (date_interval_operator_test.go) renders a value or its SQLSTATE.

// TestIntervalArithIsPostgres pins the interval operators to PostgreSQL
// 17.11's answers (#1713c r2; measured on postgres:17-alpine, --locale=C,
// IntervalStyle postgres, the same cells as coordinator's ar/* table):
// interval_pl / interval_mi / interval_um / interval_mul / interval_div, the
// fractional cascade month → days → clock, 22008 on overflow, and 0A000 for a
// result this type (whole seconds, finite) cannot hold.
func TestIntervalArithIsPostgres(t *testing.T) {
	d := func(n int) IntervalValue { return IntervalValue{Days: n} }
	mo := func(n int) IntervalValue { return IntervalValue{Months: n} }
	y := func(n int) IntervalValue { return IntervalValue{Years: n} }
	h := func(n int) IntervalValue { return IntervalValue{Hours: n} }
	s := func(n int) IntervalValue { return IntervalValue{Seconds: n} }
	for _, c := range []struct {
		name string
		f    func() any
		want string
	}{
		{"1 day * 2", func() any { return intervalScale(d(1), 2, false) }, "2 days"},
		{"1 day * 0", func() any { return intervalScale(d(1), 0, false) }, "00:00:00"},
		{"1 day * -3", func() any { return intervalScale(d(1), -3, false) }, "-3 days"},
		{"1 month * 1.5", func() any { return intervalScale(mo(1), 1.5, false) }, "1 mon 15 days"},
		{"1 year * 0.5", func() any { return intervalScale(y(1), 0.5, false) }, "6 mons"},
		{"1 month * 0.1", func() any { return intervalScale(mo(1), 0.1, false) }, "3 days"},
		{"1 day * 0.1", func() any { return intervalScale(d(1), 0.1, false) }, "02:24:00"},
		{"7 months * 0.3", func() any { return intervalScale(mo(7), 0.3, false) }, "2 mons 3 days"},
		{"1 month * 0.7", func() any { return intervalScale(mo(1), 0.7, false) }, "21 days"},
		{"1 year * 1.1", func() any { return intervalScale(y(1), 1.1, false) }, "1 year 1 mon 6 days"},
		{"-1 month * 1.5", func() any { return intervalScale(mo(-1), 1.5, false) }, "-1 mons -15 days"},
		{"1 month * 0.0333333333", func() any { return intervalScale(mo(1), 0.0333333333, false) }, "1 day"},
		{"25 hours * 2", func() any { return intervalScale(h(25), 2, false) }, "50:00:00"},
		{"1 second * 1.5", func() any { return intervalScale(s(1), 1.5, false) }, "ERR 0A000"},
		{"1 month * 1.0000001", func() any { return intervalScale(mo(1), 1.0000001, false) }, "ERR 0A000"},
		{"2147483647 days * 2", func() any { return intervalScale(d(2147483647), 2, false) }, "ERR 22008"},
		{"1 day * 1e300", func() any { return intervalScale(d(1), 1e300, false) }, "ERR 22008"},
		{"1 day / 2", func() any { return intervalScale(d(1), 2, true) }, "12:00:00"},
		{"1 month / 3", func() any { return intervalScale(mo(1), 3, true) }, "10 days"},
		{"1 day / 3", func() any { return intervalScale(d(1), 3, true) }, "08:00:00"},
		{"1 day / 1.5", func() any { return intervalScale(d(1), 1.5, true) }, "16:00:00"},
		{"3 days / -2", func() any { return intervalScale(d(3), -2, true) }, "-1 days -12:00:00"},
		{"-1 day / 1.5", func() any { return intervalScale(d(-1), 1.5, true) }, "-16:00:00"},
		{"1 year / 24", func() any { return intervalScale(y(1), 24, true) }, "15 days"},
		{"1 day / 0", func() any { return intervalScale(d(1), 0, true) }, "ERR 22012"},
		{"1 second / 2", func() any { return intervalScale(s(1), 2, true) }, "ERR 0A000"},
		{"1 year / 7", func() any { return intervalScale(y(1), 7, true) }, "ERR 0A000"},
		{"1 day + 1 hour", func() any { return intervalAdd(d(1), h(1), false) }, "1 day 01:00:00"},
		{"1 month - 30 days", func() any { return intervalAdd(mo(1), d(30), true) }, "1 mon -30 days"},
		{"-1 day - 1 month", func() any { return intervalAdd(d(-1), mo(1), true) }, "-1 mons -1 days"},
		{"2147483647 days + 1 day", func() any { return intervalAdd(d(2147483647), d(1), false) }, "ERR 22008"},
		{"-1 day", func() any { return intervalNeg(d(1)) }, "-1 days"},
		{"-(-2147483648 days)", func() any { return intervalNeg(d(-2147483648)) }, "ERR 22008"},
		{"interval * interval", func() any { v, _ := intervalArith("*", d(1), d(1)); return v }, "ERR 42883"},
		{"interval + 1", func() any { v, _ := intervalArith("+", d(1), int64(1)); return v }, "ERR 42883"},
		{"interval * 'abc'", func() any { v, _ := intervalArith("*", d(1), "abc"); return v }, "ERR 22P02"},
		{"interval + '1 hour'", func() any { v, _ := intervalArith("+", d(1), "1 hour"); return v }, "1 day 01:00:00"},
	} {
		if got := ivCatch(c.f); got != c.want {
			t.Errorf("%s = %s, PostgreSQL 17.11 %s", c.name, got, c.want)
		}
	}
	// A quoted operand that is not interval input is the date arms' to read.
	if _, ok := intervalArith("+", d(1), "2024-01-01"); ok {
		t.Error("interval + '2024-01-01' was taken as interval arithmetic")
	}
}

// TestIntervalPartIsPostgres pins EXTRACT / date_part of an interval to
// PostgreSQL 17.11's interval_part_common (#1713c r2).
func TestIntervalPartIsPostgres(t *testing.T) {
	for _, c := range []struct {
		unit string
		iv   IntervalValue
		want string
	}{
		{"day", IntervalValue{Days: 30}, "30"},
		{"hour", IntervalValue{Hours: 25}, "25"},
		{"month", IntervalValue{Months: 14}, "2"},
		{"year", IntervalValue{Months: 14}, "1"},
		{"year", IntervalValue{Months: -14}, "-1"},
		{"minute", IntervalValue{Minutes: -90}, "-30"},
		{"second", IntervalValue{Seconds: -90}, "-30"},
		{"quarter", IntervalValue{Months: -5}, "0"},
		{"quarter", IntervalValue{Months: 11}, "4"},
		{"epoch", IntervalValue{Years: 1}, "3.15576e+07"},
		{"epoch", IntervalValue{Months: 14}, "3.67416e+07"},
		{"millisecond", IntervalValue{Seconds: 5}, "5000"},
		{"decade", IntervalValue{Years: 25}, "2"},
		{"mon", IntervalValue{Months: 14}, "2"},
		{"week", IntervalValue{Days: 30}, "ERR 0A000"},
		{"dow", IntervalValue{Days: 30}, "ERR 0A000"},
		{"bogus", IntervalValue{Days: 30}, "ERR 22023"},
	} {
		if got := ivCatch(func() any { return intervalPart(c.unit, c.iv) }); got != c.want {
			t.Errorf("date_part(%q, %s) = %s, PostgreSQL 17.11 %s", c.unit, c.iv, got, c.want)
		}
	}
}

// TestAddIntervalClampsTheMonthEnd pins timestamp_pl_interval's order and
// clamp (#1713c r2): months first, a day past the new month's end clamped to
// its last day, then days, then the clock part.
func TestAddIntervalClampsTheMonthEnd(t *testing.T) {
	at := func(s string) time.Time {
		v, err := time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, c := range []struct {
		from string
		iv   IntervalValue
		sub  bool
		want string
	}{
		{"2024-01-31 00:00:00", IntervalValue{Months: 1}, false, "2024-02-29 00:00:00"},
		{"2023-01-31 00:00:00", IntervalValue{Months: 1}, false, "2023-02-28 00:00:00"},
		{"2024-03-31 10:00:00", IntervalValue{Months: 1}, true, "2024-02-29 10:00:00"},
		{"2024-02-29 00:00:00", IntervalValue{Years: 1}, false, "2025-02-28 00:00:00"},
		{"2000-02-29 00:00:00", IntervalValue{Years: 100}, false, "2100-02-28 00:00:00"},
		{"2024-03-31 00:00:00", IntervalValue{Months: -13}, false, "2023-02-28 00:00:00"},
		{"2024-12-31 00:00:00", IntervalValue{Months: 2}, false, "2025-02-28 00:00:00"},
		{"2024-01-31 23:00:00", IntervalValue{Months: 1, Hours: 2}, false, "2024-03-01 01:00:00"},
		{"2024-01-31 00:00:00", IntervalValue{Months: 1, Days: 1}, false, "2024-03-01 00:00:00"},
	} {
		if got := addInterval(at(c.from), c.iv, c.sub).Format("2006-01-02 15:04:05"); got != c.want {
			t.Errorf("%s ± %s (subtract %v) = %s, PostgreSQL 17.11 %s", c.from, c.iv, c.sub, got, c.want)
		}
	}
}

// TestIntervalLiteralRangeIsPostgres pins the interval input range at the
// literal (#1713c r2): 22015 for a field past its width, 22008 for a year
// count whose months leave int32.
func TestIntervalLiteralRangeIsPostgres(t *testing.T) {
	for _, c := range []struct {
		v    int
		unit string
		want string
	}{
		{178956970, "year", ""}, {178956971, "year", "22008"}, {-178956971, "year", "22008"},
		{2147483647, "year", "22008"}, {2147483648, "year", "22015"},
		{2147483647, "month", ""}, {2147483648, "month", "22015"}, {-2147483648, "month", ""},
		{2147483647, "day", ""}, {2147483648, "day", "22015"},
		{306783378, "week", ""}, {306783379, "week", "22015"},
		{2562047788, "hour", ""}, {2562047789, "hour", "22015"}, {-2562047789, "hour", "22015"},
		{153722867281, "minute", "22015"}, {9223372036854, "second", ""}, {9223372036855, "second", "22015"},
	} {
		_, err := intervalValueOf(&plansql.IntervalLit{Value: c.v, Unit: c.unit})
		if got := sqlerr.StateOf(err); got != c.want {
			t.Errorf("INTERVAL '%d %s': state %q (%v), PostgreSQL 17.11 %q", c.v, c.unit, got, err, c.want)
		}
	}
}
