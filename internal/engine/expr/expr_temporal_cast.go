// This file holds expr temporal cast; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"math"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
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

// castTemporal is CAST(x AS DATE) / CAST(x AS TIMESTAMP).
//
// Both produce the box the corresponding COLUMN type produces: epoch DAYS for
// DATE, epoch MILLISECONDS for TIMESTAMP, both int64 — the same values
// ColRef.Eval hands out for a batch.TypeDate / batch.TypeTimestamp column, and
// the same values batch.Vector.SetValue stores back into one. That identity is
// the whole point of the fix (#340): until now the cast returned its argument
// unchanged, so `CAST('1996-01-10' AS DATE) - 1` subtracted 1 from the number
// ToFloat64 read out of the TEXT's leading digits and answered 1995.
//
// The operand resolves through temporalOperand — the #332 helper — so a DATE
// column arrives as a civilDate and a TIMESTAMP column as a time.Time, with
// the unit their bare int64 box has lost recovered from the declared column
// type; parseDateArg (#322) then reads whichever form arrived. Nothing here
// parses a column value itself, so the cast cannot disagree with date_add,
// date_diff or `date ± INTERVAL` about what a column means.
//
// TEXT that resolves to no instant at all RAISES — 22007 for text that is not
// a date, 22008 for a well-formed date naming no day — which is what
// PostgreSQL answers and what #836 and #840 are. #340 chose NULL because the
// expression layer had no per-row error channel; it has one (FatalEvalPanic,
// #347), the numeric casts have used it since #367, and #836 is the issue
// that noticed ADR-0012's residual text still said otherwise. Every non-text
// box that fails to parse keeps its NULL — see raiseTemporalCastRefusal for
// why that boundary is where PostgreSQL puts it.
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
	if kind == castToDateKind {
		if days, isNum := epochDayOperand(src); isNum {
			return castIntInRange(days, "date")
		}
	}
	t, _, ok := parseDateArg(src)
	if !ok {
		return nil
	}
	if kind == castToDateKind {
		return epochDaysOf(t)
	}
	return t.UTC().UnixMilli()
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

// dateArith is `date - date` and `date ± n`, the two shapes BinOp.Eval must
// recognize once CAST produces a real date (#340).
//
// Operands resolve through temporalOperand, so every form the engine has for a
// date is accepted on equal terms: a DATE/TIMESTAMP column, a CAST to one, and
// a date-shaped string — which is what a DATE column looks like when the
// catalog declares it VARCHAR, as the TPC-H fixtures do. `l_receiptdate -
// l_shipdate` is exactly that shape, and it answered NULL on every row.
//
//	date - date → the whole number of days between them (DuckDB: BIGINT)
//	date ± n    → the date n days away, as epoch days (DuckDB: DATE)
//
// Both are gated on the operands being whole DAYS. An instant carrying a clock
// declines and falls through to the arithmetic below, because
// timestamp-minus-timestamp is an INTERVAL in SQL and this engine has no
// interval column type to answer with — inventing a unit here is the mistake
// #319 and #322 were about. `date ± INTERVAL` is not handled here either: that
// is intervalShift, which keeps the rendered-string result #322 pinned for it.
//
// ok=false means "not date arithmetic" and leaves the caller's numeric path
// untouched — including the case where an operand is a string that does not
// parse as a date, which is how `'BUILDING' - 1` keeps its old answer.
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
		return epochDaysOf(rt.AddDate(0, 0, int(n))), true
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
		n = -n
	}
	return epochDaysOf(lt.AddDate(0, 0, int(n))), true
}

// plainDayCount reads the non-date side of `date ± n` as a whole number of
// days. A fractional float declines rather than truncating, so the caller
// falls through to ordinary arithmetic instead of quietly rounding a date.
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
			return int64(n), true
		}
	case float32:
		if float64(n) == math.Trunc(float64(n)) {
			return int64(n), true
		}
	}
	return 0, false
}
