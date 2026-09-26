// SPDX-License-Identifier: MIT

// This file holds expr interval; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// --- Interval type ---

// IntervalValue represents a SQL INTERVAL (e.g., INTERVAL '30' DAY).
type IntervalValue struct {
	Years   int
	Months  int
	Days    int
	Hours   int
	Minutes int
	Seconds int

	// text is PostgreSQL interval input this type's single-unit fields cannot
	// hold (`'1 day 02:00:00'`, `'00:30:00'`, `'1 year 2 mons'`), kept as the
	// text a CAST of it read. It prints as that text — what a text CAST to
	// INTERVAL answered before arc VL, PostgreSQL's own value whenever the
	// input is PostgreSQL's canonical output — and every consumer that would
	// APPLY it (addInterval, time_bucket) refuses 0A000, where the text used
	// to be read as its leading number of milliseconds.
	text string
}

// raiseIntervalNotApplicable is the refusal for an interval whose fields this
// engine cannot hold (IntervalValue.text).
func raiseIntervalNotApplicable(iv IntervalValue) {
	panic(fatalEval{sqlerr.New("0A000",
		"interval %s cannot be applied: only a single-unit interval (YEAR, MONTH, "+
			"WEEK, DAY, HOUR, MINUTE or SECOND) is supported in arithmetic", sqlerr.Quote(iv.text))})
}

// addInterval applies an interval to an instant.
func addInterval(t time.Time, iv IntervalValue, subtract bool) time.Time {
	if iv.text != "" {
		raiseIntervalNotApplicable(iv)
	}
	sign := 1
	if subtract {
		sign = -1
	}
	t = t.AddDate(sign*iv.Years, sign*iv.Months, sign*iv.Days)
	// The clock part in checked int64 SECONDS, never time.Duration (int64
	// nanoseconds, ±292 years): a wrapped Duration is an in-range instant the
	// range seam cannot tell from a right one.
	secs, ok := int64(0), true
	for _, f := range []struct{ n, unit int64 }{{int64(iv.Hours), 3600}, {int64(iv.Minutes), 60}, {int64(iv.Seconds), 1}} {
		p := f.n * f.unit
		if f.n != 0 && p/f.n != f.unit {
			ok = false
			break
		}
		if (p > 0 && secs > math.MaxInt64-p) || (p < 0 && secs < math.MinInt64-p) {
			ok = false
			break
		}
		secs += p
	}
	if !ok || secs > endEpochMilli/1000*2 || secs < -endEpochMilli/1000*2 {
		raiseTimestampOutOfRange()
	}
	return time.Unix(t.Unix()+int64(sign)*secs, int64(t.Nanosecond())).UTC()
}

// DurationNanos implements batch.DurationNanoser (arc CW round 6, B1): the
// DURATION carrier a container's ARRAY(INTERVAL) element writes into —
// this engine's one int64-backed column type with no (p,s) of its own to get
// wrong, already ordered as a plain int64 (kernel.CompareValuesAt groups
// DURATION with INT64). The number is PostgreSQL 17.11's interval_cmp metric
// (interval_cmp_value): a month is 30 days here, twelve of them a year, so
// `ARRAY[INTERVAL '1 year'] = ARRAY[INTERVAL '360 days']` and
// `ARRAY[INTERVAL '1 month'] = ARRAY[INTERVAL '30 days']` order equal exactly
// as PostgreSQL's own array_cmp does (measured, wadjet-pg-cw6) — converted to
// nanoseconds and added to the time-of-day, so "2 days" orders before
// "10 days" where the two intervals' RENDERED TEXT (the comparator's old
// last-resort fallback) does not.
//
// false is an OPAQUE interval (iv.text: a runtime CAST this engine's
// single-unit fields could not hold) or a value past int64 nanoseconds'
// range: the checked sum addInterval already uses above, so an adversarial
// literal refuses (the caller's #361 silent-write guard) rather than storing
// a wrapped or truncated order.
func (iv IntervalValue) DurationNanos() (int64, bool) {
	if iv.text != "" {
		return 0, false
	}
	months := int64(iv.Years)*12 + int64(iv.Months)
	total, ok := int64(0), true
	for _, f := range []struct{ n, unit int64 }{
		{months, 30 * 86400 * 1_000_000_000},
		{int64(iv.Days), 86400 * 1_000_000_000},
		{int64(iv.Hours), 3600 * 1_000_000_000},
		{int64(iv.Minutes), 60 * 1_000_000_000},
		{int64(iv.Seconds), 1_000_000_000},
	} {
		p := f.n * f.unit
		if f.n != 0 && p/f.n != f.unit {
			ok = false
			break
		}
		if (p > 0 && total > math.MaxInt64-p) || (p < 0 && total < math.MinInt64-p) {
			ok = false
			break
		}
		total += p
	}
	return total, ok
}

// String renders an INTERVAL as PostgreSQL's default (`postgres` style)
// output (EncodeInterval, INTSTYLE_POSTGRES): the month count as `N year(s)
// N mon(s)` (twelve months are a year: `INTERVAL '14 months'` is `1 year 2
// mons`), then `N day(s)`, each with its own sign and the singular only at
// exactly 1 (`-1 days`); then the clock part as a signed HH:MM:SS total
// (hours never carry into days: `25:00:00`), `+` before a positive field that
// follows a negative one (`-1 days +02:00:00`), and `00:00:00` for zero.
func (iv IntervalValue) String() string {
	if iv.text != "" {
		return iv.text
	}
	var b strings.Builder
	isZero, isBefore := true, false
	part := func(n int64, unit string) {
		if n == 0 {
			return
		}
		if !isZero {
			b.WriteByte(' ')
		}
		if isBefore && n > 0 {
			b.WriteByte('+')
		}
		fmt.Fprintf(&b, "%d %s", n, unit)
		if n != 1 {
			b.WriteByte('s')
		}
		isBefore, isZero = n < 0, false
	}
	months := int64(iv.Years)*12 + int64(iv.Months)
	part(months/12, "year")
	part(months%12, "mon")
	part(int64(iv.Days), "day")
	secs := int64(iv.Hours)*3600 + int64(iv.Minutes)*60 + int64(iv.Seconds)
	if secs != 0 || isZero {
		if !isZero {
			b.WriteByte(' ')
		}
		switch {
		case secs < 0:
			b.WriteByte('-')
			secs = -secs
		case isBefore:
			b.WriteByte('+')
		}
		fmt.Fprintf(&b, "%02d:%02d:%02d", secs/3600, secs/60%60, secs%60)
	}
	return b.String()
}

// pgIntervalInput reports whether s is text PostgreSQL's interval input
// (DecodeInterval / DecodeISO8601Interval) reads: `@`, signed numbers with or
// without a directly attached unit, unit words, `[-]h:mm[:ss[.f]]` clock
// fields, `Y-M` year-month fields and `ago`, or an ISO-8601 `P…` duration.
// It is a recognizer, not a reader: a CAST keeps such text as it was written
// (IntervalValue.text) when this engine's IntervalValue cannot hold it, and
// text PostgreSQL itself refuses is 22007 here too.
func pgIntervalInput(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return false
	}
	if (intervalISO.MatchString(s) && s != "p") || intervalISOAlt.MatchString(s) {
		return true
	}
	fields := strings.Fields(s)
	sawValue := false
	for i, f := range fields {
		switch {
		case f == "@" && i == 0:
		case f == "ago" && i == len(fields)-1 && sawValue:
		case intervalUnits[f]:
			if !sawValue {
				return false
			}
		case intervalClock.MatchString(f), intervalYearMonth.MatchString(f):
			sawValue = true
		default:
			m := intervalNumber.FindStringSubmatch(f)
			if m == nil || (m[2] != "" && !intervalUnits[m[2]]) {
				return false
			}
			sawValue = true
		}
	}
	return sawValue
}

var (
	intervalNumber    = regexp.MustCompile(`^([+-]?(?:\d+(?:\.\d*)?|\.\d+))([a-z]*)$`)
	intervalClock     = regexp.MustCompile(`^[+-]?\d+:\d{1,2}(?::\d{1,2}(?:\.\d*)?)?$`)
	intervalYearMonth = regexp.MustCompile(`^[+-]?\d+-\d+$`)
	intervalISO       = regexp.MustCompile(`^p(?:[+-]?\d+(?:\.\d+)?[ymwd])*(?:t(?:[+-]?\d+(?:\.\d+)?[hms])+)?$`)
	intervalISOAlt    = regexp.MustCompile(`^p\d{4}-\d{2}-\d{2}(?:t\d{2}:\d{2}:\d{2}(?:\.\d+)?)?$`)
	intervalUnits     = func() map[string]bool {
		m := map[string]bool{}
		for _, u := range strings.Fields(`c cent century centuries d day days dec decade decades decs
			h hour hours hr hrs m min mins minute minutes mon mons month months
			ms msec msecs msecond mseconds millisecond milliseconds millisecon
			us usec usecs usecond useconds microsecond microseconds microsecon
			mil mils millennium millennia millenniums s sec secs second seconds
			w week weeks y year years yr yrs`) {
			m[u] = true
		}
		return m
	}()
)

// castToIntervalText is CAST(text AS INTERVAL): the INTERVAL literal's own
// reading (plansql.ParseIntervalText, intervalValueOf) when the text is one of
// this engine's single-unit intervals; otherwise PostgreSQL interval input
// this type cannot hold stays the text it was (IntervalValue.text), and text
// PostgreSQL refuses is its 22007.
func castToIntervalText(x string) IntervalValue {
	if lit, err := plansql.ParseIntervalText(x); err == nil {
		if iv, err := intervalValueOf(lit); err == nil {
			return iv
		}
	}
	if pgIntervalInput(x) {
		return IntervalValue{text: x}
	}
	panic(fatalEval{sqlerr.New("22007", "invalid input syntax for type interval: %s", sqlerr.Quote(x))})
}
