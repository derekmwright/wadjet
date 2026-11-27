// SPDX-License-Identifier: MIT

// This file holds interval arithmetic; ADR-0012 governs the execution contracts.
package expr

import (
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Interval arithmetic as PostgreSQL 17.11 computes it (#1713c): an interval
// is three fields there — months (int32), days (int32) and a clock part
// (int64 microseconds) — and `+`, `-`, unary minus, `* float8` and
// `/ float8` work on the three fields apart (interval_pl, interval_mi,
// interval_um, interval_mul, interval_div), cascading a fractional month
// into days (a month is 30 days) and a fractional day into the clock
// (a day is 86400 seconds), never upward. The fields are this type's
// months, days and SECONDS: a result with a sub-second part, or PostgreSQL
// 17's infinite interval, is a value this type cannot hold and is refused
// 0A000 rather than rounded. Every overflow is PostgreSQL's 22008.
//
// These used to box 0 (`INTERVAL '1 day' * 2`, `/ 2`, `interval + interval`
// read both sides through ToFloat64) and unary minus -0, under a text
// declaration that printed it.

// maxIntervalSecs bounds the clock part: PostgreSQL holds it in int64
// microseconds, so a whole number of seconds fits while |s| * 1e6 does.
const maxIntervalSecs = math.MaxInt64 / 1_000_000

func raiseIntervalOutOfRange() {
	panic(fatalEval{sqlerr.New("22008", "interval out of range")})
}

func raiseIntervalSubSecond(op string) {
	panic(fatalEval{sqlerr.Refuse("interval-subsecond",
		"interval %s has a fractional-second result; only whole seconds are supported in an interval", op)})
}

func raiseIntervalOperator(op string, l, r string) {
	if l == "" {
		panic(fatalEval{sqlerr.New("42883", "operator does not exist: %s %s", op, r)})
	}
	panic(fatalEval{sqlerr.New("42883", "operator does not exist: %s %s %s", l, op, r)})
}

// pgParts is the value as PostgreSQL stores it: months, days, and the clock
// part in seconds. Only a value inside PostgreSQL's range reaches here (the
// literal and the CAST refuse one outside it, intervalValueOf).
func (iv IntervalValue) pgParts() (months, days, secs int64) {
	if iv.text != "" {
		raiseIntervalNotApplicable(iv)
	}
	months = int64(iv.Years)*12 + int64(iv.Months)
	days = int64(iv.Days)
	secs = int64(iv.Hours)*3600 + int64(iv.Minutes)*60 + int64(iv.Seconds)
	return months, days, secs
}

// intervalOfParts is the interval of PostgreSQL's three fields, refused 22008
// outside their range.
func intervalOfParts(months, days, secs int64) IntervalValue {
	if months < math.MinInt32 || months > math.MaxInt32 || days < math.MinInt32 || days > math.MaxInt32 ||
		secs < -maxIntervalSecs || secs > maxIntervalSecs {
		raiseIntervalOutOfRange()
	}
	return IntervalValue{Months: int(months), Days: int(days), Seconds: int(secs)}
}

// intervalAdd is interval_pl / interval_mi: field by field.
func intervalAdd(a, b IntervalValue, subtract bool) IntervalValue {
	am, ad, as := a.pgParts()
	bm, bd, bs := b.pgParts()
	if subtract {
		bm, bd, bs = -bm, -bd, -bs
	}
	return intervalOfParts(am+bm, ad+bd, as+bs)
}

// intervalNeg is interval_um: every field negated; the int32 minimum has no
// positive and is 22008.
func intervalNeg(a IntervalValue) IntervalValue {
	m, d, s := a.pgParts()
	return intervalOfParts(-m, -d, -s)
}

// pgTSRound is PostgreSQL's TSROUND: a double rounded to microseconds.
func pgTSRound(x float64) float64 { return math.RoundToEven(x*1e6) / 1e6 }

func fitsInt32(x float64) bool { return x >= math.MinInt32 && x < -float64(math.MinInt32) }
func fitsInt64(x float64) bool { return x >= math.MinInt64 && x < -float64(math.MinInt64) }

// intervalScale is interval_mul (div false) and interval_div (div true),
// PostgreSQL 17.11's double arithmetic step for step: the month and day
// products truncated toward zero, their fractions cascaded down through
// TSROUND, the clock part rounded half to even in microseconds.
func intervalScale(a IntervalValue, factor float64, div bool) IntervalValue {
	op := "multiplication"
	if div {
		op = "division"
		if factor == 0 {
			raiseDivisionByZero()
		}
	}
	m, d, s := a.pgParts()
	if math.IsNaN(factor) {
		raiseIntervalOutOfRange()
	}
	if math.IsInf(factor, 0) {
		if div {
			return IntervalValue{}
		}
		if m == 0 && d == 0 && s == 0 {
			raiseIntervalOutOfRange()
		}
		// PostgreSQL 17 answers an infinite interval, which this type has
		// no value for.
		panic(fatalEval{sqlerr.Refuse("interval-infinity", "interval %s has an infinite result; an infinite interval is not supported", op)})
	}
	apply := func(x float64) float64 {
		if div {
			return x / factor
		}
		return x * factor
	}
	monthD := apply(float64(m))
	if math.IsNaN(monthD) || !fitsInt32(monthD) {
		raiseIntervalOutOfRange()
	}
	rm := int64(int32(monthD))
	dayD := apply(float64(d))
	if math.IsNaN(dayD) || !fitsInt32(dayD) {
		raiseIntervalOutOfRange()
	}
	rd := int64(int32(dayD))
	monthRemainderDays := pgTSRound((apply(float64(m)) - float64(rm)) * 30)
	secRemainder := pgTSRound((apply(float64(d)) - float64(rd) + monthRemainderDays -
		float64(int32(monthRemainderDays))) * 86400)
	if math.Abs(secRemainder) >= 86400 {
		whole := int64(int32(secRemainder / 86400))
		rd += whole
		if rd < math.MinInt32 || rd > math.MaxInt32 {
			raiseIntervalOutOfRange()
		}
		secRemainder -= float64(whole * 86400)
	}
	rd += int64(int32(monthRemainderDays))
	if rd < math.MinInt32 || rd > math.MaxInt32 {
		raiseIntervalOutOfRange()
	}
	micros := math.RoundToEven(apply(float64(s)*1e6) + secRemainder*1e6)
	if math.IsNaN(micros) || !fitsInt64(micros) {
		raiseIntervalOutOfRange()
	}
	us := int64(micros)
	if us%1_000_000 != 0 {
		raiseIntervalSubSecond(op)
	}
	return intervalOfParts(rm, rd, us/1_000_000)
}

// intervalFactor reads the float8 operand of `interval * x` / `interval / x`:
// a number, or a quoted literal read as float8 (PostgreSQL resolves the
// unknown to interval_mul's float8 and refuses text that is not one, 22P02).
func intervalFactor(v any) (float64, bool) {
	switch x := v.(type) {
	case int64, int32, int, float64, float32:
		return ToFloat64(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
				return f, true
			}
			panic(fatalEval{sqlerr.New("22P02", "invalid input syntax for type double precision: %s", sqlerr.Quote(x))})
		}
		return f, true
	}
	return 0, false
}

// intervalOperandName names an operand's type for a 42883 sentence.
func intervalOperandName(v any) string {
	switch v.(type) {
	case IntervalValue:
		return "interval"
	case bool:
		return "boolean"
	case int32:
		return "integer"
	case int64, int:
		return "bigint"
	case float32, float64:
		return "double precision"
	}
	return ""
}

// intervalArith is a binary operator with an INTERVAL value on either side,
// answered as PostgreSQL does, or ok=false when the operator is not interval
// arithmetic here (date ± interval, which the caller's temporal arms own,
// and a quoted operand that does not read as an interval).
//
//   - interval ± interval, and interval ± a quoted literal that reads as an
//     interval (PostgreSQL resolves the unknown to interval);
//   - interval * number, number * interval, interval / number (a quoted
//     literal read as float8);
//   - interval with a number, a boolean or another interval under any other
//     operator: no such operator, 42883.
//
// quoted reports that the non-interval operand is a quoted LITERAL, SQL's
// unknown, which PostgreSQL types from the interval beside it. A string from
// anywhere else (a text column) is text: `±` leaves it to the caller's date
// arms as before, and `*` / `/` have no `interval * text` (42883).
func intervalArith(op string, lv, rv any, quoted bool) (any, bool) {
	li, lok := lv.(IntervalValue)
	ri, rok := rv.(IntervalValue)
	if !lok && !rok {
		return nil, false
	}
	switch {
	case lok && rok:
		if op == "+" || op == "-" {
			return intervalAdd(li, ri, op == "-"), true
		}
		raiseIntervalOperator(op, "interval", "interval")
	case op == "+" || op == "-":
		// A quoted operand that reads as interval input is an interval;
		// any other one is the date / timestamp the caller's arms read.
		other := rv
		if rok {
			other = lv
		}
		if s, ok := other.(string); ok {
			if !quoted || !intervalInputText(s) {
				return nil, false
			}
			oi := castToIntervalText(s)
			if lok {
				return intervalAdd(li, oi, op == "-"), true
			}
			return intervalAdd(oi, ri, op == "-"), true
		}
		if name := intervalOperandName(other); name != "" {
			if lok {
				raiseIntervalOperator(op, "interval", name)
			}
			raiseIntervalOperator(op, name, "interval")
		}
		return nil, false
	case op == "*" || (op == "/" && lok):
		iv, other := li, rv
		if rok {
			iv, other = ri, lv
		}
		if _, text := other.(string); text && !quoted {
			if lok {
				raiseIntervalOperator(op, "interval", "text")
			}
			raiseIntervalOperator(op, "text", "interval")
		}
		f, ok := intervalFactor(other)
		if !ok {
			if name := intervalOperandName(other); name != "" {
				if lok {
					raiseIntervalOperator(op, "interval", name)
				}
				raiseIntervalOperator(op, name, "interval")
			}
			return nil, false
		}
		return intervalScale(iv, f, op == "/"), true
	default:
		if name := intervalOperandName(rv); lok && name != "" {
			raiseIntervalOperator(op, "interval", name)
		}
		if name := intervalOperandName(lv); rok && name != "" {
			raiseIntervalOperator(op, name, "interval")
		}
	}
	return nil, false
}

// intervalInputText reports whether quoted text reads as PostgreSQL interval
// input — the literal's own grammar or PostgreSQL's interval syntax — and so
// is the interval an unknown literal beside an interval resolves to.
func intervalInputText(s string) bool {
	return pgIntervalInput(s)
}

// intervalPair reports an operand pair `+` / `-` reads as interval
// arithmetic before the date arms: two intervals, or an interval beside a
// quoted operand.
func intervalPair(lv, rv any) bool {
	_, li := lv.(IntervalValue)
	_, ri := rv.(IntervalValue)
	if li && ri {
		return true
	}
	if li {
		_, s := rv.(string)
		return s
	}
	if ri {
		_, s := lv.(string)
		return s
	}
	return false
}

// IsIntervalInputText is intervalInputText for the plan's declaration.
func IsIntervalInputText(s string) bool { return intervalInputText(s) }

// isIntervalBox reports an IntervalValue box; small enough to inline on the
// per-row paths that ask it of every value.
func isIntervalBox(v any) bool {
	_, ok := v.(IntervalValue)
	return ok
}

// eitherInterval reports an IntervalValue on either side of an operator.
func eitherInterval(lv, rv any) bool { return isIntervalBox(lv) || isIntervalBox(rv) }

// intervalArithFirst is the interval arithmetic BinOp.Eval asks before its
// date arms: `* / %` with an interval operand, and `±` over two intervals or
// an interval beside a quoted operand. Out of line, so the per-row Eval of
// every other operand pair carries only eitherInterval's two type checks.
//
//go:noinline
func intervalArithFirst(op string, lv, rv any, quoted bool) (any, bool) {
	if op == "*" || op == "/" || op == "%" || intervalPair(lv, rv) {
		return intervalArith(op, lv, rv, quoted)
	}
	return nil, false
}
