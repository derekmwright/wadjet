// SPDX-License-Identifier: MIT

package expr

import (
	"math"
	"math/big"
	"strings"
)

// pgIntervalLiteralAccepted reports interval input text that PostgreSQL 17.11
// ACCEPTS (DecodeInterval, IntervalStyle postgres) and the INTERVAL literal
// reader cannot read: the literal reader asks it (plansql.IntervalInputAccepted)
// before raising 22007, and names its capability refusal only where this is
// true (ADR-0052 §8, the architect's rule 4 on 1680c: "invalid input syntax"
// is untrue of text PostgreSQL accepts; PostgreSQL's own 22007 / 22008 /
// 22015 must never be pre-empted, rule 3).
//
// It is deliberately NARROWER than PostgreSQL, never wider: a text it does not
// recognize keeps the reader's 22007, which is what the reader said before.
// What it accepts, and why PostgreSQL accepts it too:
//
//   - an optional leading `@` and an optional trailing `ago`;
//   - `number unit` / `numberunit` fields (a signed integer or decimal; the
//     unit words of intervalUnits), a `[+-]h:mm[:ss[.f]]` clock field with
//     minutes and seconds under 60, a `[+-]Y-M` year-month field, and a bare
//     integer immediately before a clock field (days: `3 4:05:06`);
//   - no two fields naming the same unit (PostgreSQL's field mask refuses a
//     repeated unit, `1 day 1 day`, and a clock beside an hour, minute or
//     second unit, 22007). Seconds, milliseconds and microseconds count as one
//     unit here, and a year-month field as the year and the month units: wider
//     masks than PostgreSQL's, so narrower acceptance;
//   - the value PostgreSQL would build stays inside its range at every step
//     (months and days int32, the clock part int64 microseconds, each partial
//     sum in the order written, with one unit of margin where a fraction
//     rounds), so its 22008 / 22015 is never claimed as an acceptance.
//
// The ISO-8601 `P…` forms and a single field without a unit are read by the
// reader's other arm and never reach here. Measured against PostgreSQL 17.11
// over TestIntervalLiteralAcceptedAgreesWithPostgres's matrix.
func pgIntervalLiteralAccepted(s string) bool {
	fields := strings.Fields(strings.ToLower(strings.TrimSpace(s)))
	if len(fields) > 0 && fields[0] == "@" {
		fields = fields[1:]
	}
	ago := false
	if len(fields) > 0 && fields[len(fields)-1] == "ago" {
		fields, ago = fields[:len(fields)-1], true
	}
	if len(fields) == 0 {
		return false
	}
	var acc ivAccum
	used := map[string]bool{}
	claim := func(units ...string) bool {
		for _, u := range units {
			if used[u] {
				return false
			}
		}
		for _, u := range units {
			used[u] = true
		}
		return true
	}
	for i := 0; i < len(fields); i++ {
		f := fields[i]
		switch {
		case intervalClock.MatchString(f):
			if !claim("hour", "minute", "second") || !acc.clock(f) {
				return false
			}
		case intervalYearMonth.MatchString(f):
			if !claim("year", "month") || !acc.yearMonth(f) {
				return false
			}
		default:
			m := intervalNumber.FindStringSubmatch(f)
			if m == nil {
				return false
			}
			num, unit := m[1], m[2]
			if unit == "" {
				if i+1 >= len(fields) {
					return false
				}
				next := fields[i+1]
				switch {
				case intervalUnits[next]:
					unit = next
					i++
				case intervalClock.MatchString(next) && !strings.Contains(num, "."):
					unit = "day"
				default:
					return false
				}
			}
			group, ok := ivUnitGroup(unit)
			if !ok || !claim(group) || !acc.add(num, unit) {
				return false
			}
		}
	}
	if ago {
		// `ago` negates every field: the negation must fit too (the int32
		// minimum has no positive).
		acc.months.Neg(&acc.months)
		acc.days.Neg(&acc.days)
		acc.micros.Neg(&acc.micros)
		return acc.ok()
	}
	return true
}

// ivUnitGroup is the field-mask unit a unit word claims.
func ivUnitGroup(unit string) (string, bool) {
	switch unit {
	case "y", "year", "years", "yr", "yrs":
		return "year", true
	case "mon", "mons", "month", "months":
		return "month", true
	case "w", "week", "weeks":
		return "week", true
	case "d", "day", "days":
		return "day", true
	case "h", "hour", "hours", "hr", "hrs":
		return "hour", true
	case "m", "min", "mins", "minute", "minutes":
		return "minute", true
	case "s", "sec", "secs", "second", "seconds",
		"ms", "msec", "msecs", "msecond", "mseconds", "millisecond", "milliseconds", "millisecon",
		"us", "usec", "usecs", "usecond", "useconds", "microsecond", "microseconds", "microsecon":
		return "second", true
	case "dec", "decs", "decade", "decades":
		return "decade", true
	case "c", "cent", "century", "centuries":
		return "century", true
	case "mil", "mils", "millennium", "millennia", "millenniums":
		return "millennium", true
	}
	return "", false
}

// ivAccum is the value PostgreSQL builds, in exact arithmetic: months, days
// and microseconds, each partial sum checked against its range.
type ivAccum struct {
	months, days, micros big.Rat
}

var (
	ivInt32Max = new(big.Rat).SetInt64(math.MaxInt32)
	ivInt32Min = new(big.Rat).SetInt64(math.MinInt32)
	ivInt64Max = new(big.Rat).SetInt64(math.MaxInt64)
	ivInt64Min = new(big.Rat).SetInt64(math.MinInt64)
	ivOne      = new(big.Rat).SetInt64(1)
)

// within: x inside [lo, hi], with one unit of margin when x is not whole (a
// fraction PostgreSQL rounds could round past the bound).
func within(x, lo, hi *big.Rat) bool {
	if !x.IsInt() {
		lo = new(big.Rat).Add(lo, ivOne)
		hi = new(big.Rat).Sub(hi, ivOne)
	}
	return x.Cmp(lo) >= 0 && x.Cmp(hi) <= 0
}

func (a *ivAccum) ok() bool {
	return within(&a.months, ivInt32Min, ivInt32Max) && within(&a.days, ivInt32Min, ivInt32Max) &&
		within(&a.micros, ivInt64Min, ivInt64Max)
}

// add accumulates `num unit`: a fraction of a month-based unit cascades into
// days (30 per month) and a fraction of a day-based unit into the clock, as
// PostgreSQL's AdjustFract* do.
func (a *ivAccum) add(num, unit string) bool {
	v, ok := new(big.Rat).SetString(num)
	if !ok {
		return false
	}
	group, _ := ivUnitGroup(unit)
	mul := func(x *big.Rat, k int64) *big.Rat { return new(big.Rat).Mul(x, new(big.Rat).SetInt64(k)) }
	switch group {
	case "millennium":
		a.monthsFrac(mul(v, 12000))
	case "century":
		a.monthsFrac(mul(v, 1200))
	case "decade":
		a.monthsFrac(mul(v, 120))
	case "year":
		a.monthsFrac(mul(v, 12))
	case "month":
		a.monthsFrac(v)
	case "week":
		a.daysFrac(mul(v, 7))
	case "day":
		a.daysFrac(v)
	case "hour":
		a.micros.Add(&a.micros, mul(v, 3_600_000_000))
	case "minute":
		a.micros.Add(&a.micros, mul(v, 60_000_000))
	default: // the seconds group
		switch {
		case strings.HasPrefix(unit, "u") || strings.HasPrefix(unit, "micro"):
			a.micros.Add(&a.micros, v)
		case strings.HasPrefix(unit, "ms") || strings.HasPrefix(unit, "milli"):
			a.micros.Add(&a.micros, mul(v, 1_000))
		default:
			a.micros.Add(&a.micros, mul(v, 1_000_000))
		}
	}
	return a.ok()
}

// monthsFrac adds a number of months, its fraction cascading into days.
func (a *ivAccum) monthsFrac(m *big.Rat) {
	whole := new(big.Rat).SetInt(new(big.Int).Quo(m.Num(), m.Denom()))
	a.months.Add(&a.months, whole)
	frac := new(big.Rat).Sub(m, whole)
	if frac.Sign() != 0 {
		a.daysFrac(new(big.Rat).Mul(frac, new(big.Rat).SetInt64(30)))
	}
}

// daysFrac adds a number of days, its fraction cascading into the clock.
func (a *ivAccum) daysFrac(d *big.Rat) {
	whole := new(big.Rat).SetInt(new(big.Int).Quo(d.Num(), d.Denom()))
	a.days.Add(&a.days, whole)
	frac := new(big.Rat).Sub(d, whole)
	if frac.Sign() != 0 {
		a.micros.Add(&a.micros, new(big.Rat).Mul(frac, new(big.Rat).SetInt64(86_400_000_000)))
	}
}

// clock adds a `[+-]h:mm[:ss[.f]]` field (minutes and seconds under 60).
func (a *ivAccum) clock(f string) bool {
	neg := strings.HasPrefix(f, "-")
	f = strings.TrimLeft(f, "+-")
	parts := strings.Split(f, ":")
	h, ok := new(big.Rat).SetString(parts[0])
	if !ok {
		return false
	}
	total := new(big.Rat).Mul(h, new(big.Rat).SetInt64(3_600_000_000))
	for i, p := range parts[1:] {
		v, ok := new(big.Rat).SetString(p)
		if !ok || v.Cmp(new(big.Rat).SetInt64(60)) >= 0 {
			return false
		}
		k := int64(60_000_000)
		if i == 1 {
			k = 1_000_000
		}
		total.Add(total, new(big.Rat).Mul(v, new(big.Rat).SetInt64(k)))
	}
	// PostgreSQL reads the clock's magnitude before its sign, so even the
	// int64 minimum's magnitude is out of its range (measured: `1 day
	// -2562047788:00:54.775808` is 22007).
	if !within(total, ivInt64Min, ivInt64Max) {
		return false
	}
	if neg {
		total.Neg(total)
	}
	a.micros.Add(&a.micros, total)
	return a.ok()
}

// yearMonth adds a `[+-]Y-M` field (both parts take the sign).
func (a *ivAccum) yearMonth(f string) bool {
	neg := strings.HasPrefix(f, "-")
	f = strings.TrimLeft(f, "+-")
	y, m, _ := strings.Cut(f, "-")
	yv, ok1 := new(big.Rat).SetString(y)
	mv, ok2 := new(big.Rat).SetString(m)
	if !ok1 || !ok2 || mv.Cmp(new(big.Rat).SetInt64(12)) >= 0 {
		return false
	}
	t := new(big.Rat).Add(new(big.Rat).Mul(yv, new(big.Rat).SetInt64(12)), mv)
	if neg {
		t.Neg(t)
	}
	a.months.Add(&a.months, t)
	return a.ok()
}
