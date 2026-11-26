// SPDX-License-Identifier: MIT

// This file holds EXTRACT / date_part of an interval; ADR-0012 governs the execution contracts.
package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// intervalPartUnits maps date_part's unit spellings (PostgreSQL's DecodeUnits
// table) to the field interval_part_common answers; "" marks a unit
// PostgreSQL knows but refuses for an interval (0A000).
var intervalPartUnits = func() map[string]string {
	m := map[string]string{}
	add := func(unit string, spellings ...string) {
		for _, s := range spellings {
			m[s] = unit
		}
	}
	add("microsecond", "microsecond", "microseconds", "us", "usec", "usecs", "useconds", "microsecon")
	add("millisecond", "millisecond", "milliseconds", "ms", "msec", "msecs", "mseconds", "millisecon")
	add("second", "second", "seconds", "s", "sec", "secs")
	add("minute", "minute", "minutes", "m", "min", "mins")
	add("hour", "hour", "hours", "h", "hr", "hrs")
	add("day", "day", "days", "d")
	add("month", "month", "months", "mon", "mons")
	add("quarter", "quarter", "qtr")
	add("year", "year", "years", "y", "yr", "yrs")
	add("decade", "decade", "decades", "dec", "decs")
	add("century", "century", "centuries", "c", "cent")
	add("millennium", "millennium", "millennia", "mil", "mils")
	add("epoch", "epoch")
	add("", "week", "weeks", "w", "dow", "doy", "isodow", "isoyear", "julian", "timezone",
		"timezone_h", "timezone_m", "timezone_hour", "timezone_minute", "day_of_week", "day_of_year")
	return m
}()

// intervalPart is EXTRACT / date_part of an INTERVAL, PostgreSQL 17.11's
// interval_part_common (#1713c): the fields as stored, the month count split
// into years and months, the clock part into hours (which may pass 24),
// minutes and seconds, each truncated toward zero; epoch counts a year as
// 365.25 days and a month as 30. A float64, the carrier EXTRACT and
// date_part share here (ADR-0024 §2c: EXTRACT is numeric on PostgreSQL and
// float-carried here). These answered NULL — the field functions read the
// interval as no instant.
func intervalPart(unit string, iv IntervalValue) any {
	field, known := intervalPartUnits[strings.ToLower(strings.TrimSpace(unit))]
	if !known {
		panic(fatalEval{sqlerr.New("22023", "unit %s not recognized for type interval", sqlerr.Quote(unit))})
	}
	if field == "" {
		panic(fatalEval{sqlerr.New("0A000", "unit %s not supported for type interval", sqlerr.Quote(unit))})
	}
	m, d, s := iv.pgParts()
	year, mon := m/12, m%12
	sec := s % 60
	switch field {
	case "microsecond":
		return float64(sec * 1_000_000)
	case "millisecond":
		return float64(sec * 1000)
	case "second":
		return float64(sec)
	case "minute":
		return float64(s / 60 % 60)
	case "hour":
		return float64(s / 3600)
	case "day":
		return float64(d)
	case "month":
		return float64(mon)
	case "quarter":
		return float64(mon/3 + 1)
	case "year":
		return float64(year)
	case "decade":
		return float64(year / 10)
	case "century":
		return float64(year / 100)
	case "millennium":
		return float64(year / 1000)
	}
	// epoch: 365.25 days a year, 30 a month, in whole seconds (exact: every
	// term is an integer number of seconds).
	return float64(year*1461*21600 + (mon*30+d)*86400 + s)
}

// intervalExtract is a field function (EXTRACT(field FROM x) is field(x)
// here) or date_part over an INTERVAL; ok=false for any other call.
func (e *FuncCall) intervalExtract(args []any) (any, bool) {
	if e.extractUnit != "" && len(args) == 1 {
		if iv, ok := args[0].(IntervalValue); ok {
			return intervalPart(e.extractUnit, iv), true
		}
		return nil, false
	}
	if len(args) != 2 {
		return nil, false
	}
	// The box first: a per-row type check, and the name only for an
	// interval (no per-row ToLower for every two-argument call).
	iv, ok := args[1].(IntervalValue)
	if !ok {
		return nil, false
	}
	switch strings.ToLower(e.Name) {
	case "date_part", "extract":
		if args[0] == nil {
			return nil, true
		}
		return intervalPart(toString(args[0]), iv), true
	}
	return nil, false
}
