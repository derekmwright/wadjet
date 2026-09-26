// SPDX-License-Identifier: MIT

// This file holds expr temporal cast; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"math"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// --- CAST to a temporal type, and the arithmetic that reads its result ---

// castTemporalKindT names the destination types CAST resolves to a temporal
// VALUE rather than leaving to the generic conversions. TIME is deliberately
// absent: the engine has no time-of-day type to hold one, so `TIME '10:00:00'`
// keeps its text exactly as before.
type castTemporalKindT int

const (
	castNotTemporal castTemporalKindT = iota
	castToDateKind
	castToTimestampKind
)

func castTemporalKind(destType string) castTemporalKindT {
	return castTemporalKindLower(strings.ToLower(strings.TrimSpace(destType)))
}

// castTemporalKindLower is castTemporalKind for a caller that has already
// normalized the name — Cast.Eval, which needs the lowercased form for its own
// switch and runs once per row.
func castTemporalKindLower(dest string) castTemporalKindT {
	switch dest {
	case "date":
		return castToDateKind
	case "timestamp", "datetime", "timestamptz":
		return castToTimestampKind
	}
	return castNotTemporal
}

// castTemporal returns int64 epoch DAYS for DATE and MILLISECONDS for
// TIMESTAMP, identical to corresponding column boxes (#340).
// temporalOperand recovers declared units (#332); parseDateArg supplies the
// shared non-text reading (#322), so date arithmetic and casts agree.
// Text uses castTemporalText's shared parser: invalid syntax raises 22007,
// nonexistent dates 22008 (#836, #840). Use fatalEval's error channel
// (#347, #367, ADR-0012); unparseable non-text boxes keep their NULL.
// See docs/internals/temporal-cast-box-and-error-contract.md for the design.
func castTemporal(b *batch.RecordBatch, row int, operand Expr, v any, kind castTemporalKindT) any {
	src, ok := temporalOperand(b, row, operand, v)
	if !ok {
		src = v
	}
	// TEXT goes through the engine's ONE temporal accept-set, which answers
	// the VALUE and the refusal from the same function — see
	// castTemporalText. Every other box keeps parseDateArg's reading: a DATE
	// column arrives as a civilDate, a TIMESTAMP one as a time.Time, and a
	// bare number still reads as days since the epoch.
	if out, isText := castTemporalText(src, kind); isText {
		return out
	}
	// A NUMBER cast to DATE is a DAY COUNT, and it is answered in the DATE
	// carrier's own domain rather than through time.Date (#911).
	//
	// parseDateValue reads a bare number as `time.Date(1970,1,1).AddDate(0, 0,
	// n)`, and time.Date multiplies the day count by 86400 in an unmodulated
	// uint64: `(2^63-1)·86400 ≡ -86400 (mod 2^64)`, so the instant came back
	// at epoch minus one day and `9223372036854775807::DATE` answered
	// 1969-12-31. What reached batch.Vector.SetValue was the int64 -1, which
	// fits an int32 — so the store's own guard, which refuses 3000000000::DATE
	// with 22003, had nothing left to reject. The narrowing has to be decided
	// where the day count is still the number the query wrote.
	//
	// And the day count is a DATE only inside PostgreSQL's range: past it
	// is 22008 (dateDaysBox, the one range rule — temporal_range.go). A number
	// cast to TIMESTAMP reads the same day count and takes its midnight.
	if days, isNum := epochDayOperand(src); isNum {
		days = castIntInRange(days, "date")
		if kind == castToDateKind {
			return dateDaysBox(days)
		}
		return daysToInstant(days)
	}
	t, _, ok := parseDateArg(src)
	if !ok {
		return nil
	}
	if kind == castToDateKind {
		return dateBox(t)
	}
	return instantBox(t)
}

// epochDayOperand reads a bare NUMBER as the day count a DATE cast means, with
// no calendar arithmetic in between — the reading parseDateValue's numeric arms
// already have, minus the wrap.
//
// A float TRUNCATES toward zero, which is what `int(tv)` did there, and one
// with no int64 at all (`9223372036854775808` is parsed as a float64, being
// outside int64) raises rather than taking Go's implementation-defined
// conversion: on amd64 that yields MinInt64, whose 86400-multiple wraps to
// zero, and the cast answered 1970-01-01 for it.
func epochDayOperand(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return epochDayFromFloat(n), true
	case float32:
		return epochDayFromFloat(float64(n)), true
	}
	return 0, false
}

func epochDayFromFloat(f float64) int64 {
	t := math.Trunc(f)
	if math.IsNaN(t) || t >= 9223372036854775808.0 || t < -9223372036854775808.0 {
		raiseIntegerOutOfRange("date")
	}
	return int64(t)
}

// epochDaysOf floors an instant to the UTC day it falls in and returns that
// day's distance from 1970-01-01 — the DATE column representation.
func epochDaysOf(t time.Time) int64 {
	secs := t.UTC().Unix()
	days := secs / 86400
	if secs < 0 && secs%86400 != 0 {
		days--
	}
	return days
}

// dateArith handles date-date as whole days and date±n as epoch days (#340).
// temporalOperand treats DATE/TIMESTAMP columns, every temporal producer and date-shaped strings alike.
// Require WHOLE DAYS: instants with clocks decline to the caller's arithmetic path;
// timestamp subtraction needs INTERVAL, not an invented unit (#319, #322).
// date±INTERVAL belongs to intervalShift, whose result is the TIMESTAMP box (#322).
// ok=false means not date arithmetic; leave numeric fallback unchanged, including
// strings that do not parse as dates.
// See docs/internals/whole-day-date-arithmetic-boundary.md for the design.
func (e *BinOp) dateArith(b *batch.RecordBatch, row int, lv, rv any) (any, bool) {
	ld, lok := temporalOperand(b, row, e.Left, lv)
	if !lok {
		// `n + date`, the one reversed shape that means anything.
		if e.Op != "+" {
			return nil, false
		}
		n, nok := plainDayCount(lv)
		if !nok {
			return nil, false
		}
		rd, rok := temporalOperand(b, row, e.Right, rv)
		if !rok {
			return nil, false
		}
		rt, dateOnly, parsed := parseDateArg(rd)
		if !parsed || !dateOnly {
			return nil, false
		}
		return shiftDays(epochDaysOf(rt), n), true
	}
	lt, lDateOnly, lparsed := parseDateArg(ld)
	if !lparsed {
		return nil, false
	}
	if rd, rok := temporalOperand(b, row, e.Right, rv); rok {
		rt, rDateOnly, rparsed := parseDateArg(rd)
		if !rparsed || e.Op != "-" || !lDateOnly || !rDateOnly {
			return nil, false
		}
		return epochDaysOf(lt) - epochDaysOf(rt), true
	}
	n, nok := plainDayCount(rv)
	if !nok || !lDateOnly {
		return nil, false
	}
	if e.Op == "-" {
		if n == math.MinInt64 {
			panic(fatalEval{sqlerr.New("22008", "date out of range")})
		}
		n = -n
	}
	return shiftDays(epochDaysOf(lt), n), true
}

// plainDayCount reads the non-date side of `date ± n` as a whole number of
// days. A fractional float declines rather than truncating, so the caller
// falls through to ordinary arithmetic instead of quietly rounding a date; a
// whole float no DATE can be shifted by is 22008 (wholeDayCount).
func plainDayCount(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case float64:
		if n == math.Trunc(n) {
			return wholeDayCount(n), true
		}
	case float32:
		if float64(n) == math.Trunc(float64(n)) {
			return wholeDayCount(float64(n)), true
		}
	}
	return 0, false
}

// wholeDayCount is a whole float day count as int64; one no DATE can be
// shifted by (past any day the range holds, or with no int64 at all, where Go's
// conversion is implementation-defined) is 22008 here.
func wholeDayCount(f float64) int64 {
	if math.IsNaN(f) || f > float64(maxEpochDay-minEpochDay) || f < -float64(maxEpochDay-minEpochDay) {
		panic(fatalEval{sqlerr.New("22008", "date out of range")})
	}
	return int64(f)
}
