// This file holds expr helpers; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// --- Helpers ---

// toString converts any value to string, avoiding fmt.Sprint for common types.
func toString(v any) string {
	switch tv := v.(type) {
	case string:
		return tv
	case []byte:
		return string(tv)
	default:
		return fmt.Sprint(v)
	}
}

// ToFloat64 converts any numeric value to float64.
func ToFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int:
		return float64(tv)
	case int32:
		return float64(tv)
	case bool:
		if tv {
			return 1
		}
		return 0
	default:
		// Try string → float for decimal formatted strings like "123.45"
		if s, ok := v.(string); ok {
			var f float64
			if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
				return f
			}
		}
		return 0
	}
}

// ToInt64 converts any numeric value to int64.
func ToInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int:
		return int64(tv)
	case int32:
		return int64(tv)
	case float64:
		return int64(tv)
	case float32:
		return int64(tv)
	case string:
		return parseTimestampToEpochMs(tv)
	default:
		return 0
	}
}

// parseTemporalInt64OK converts a date/timestamp string to the same int64
// unit as the reference value, with an explicit success signal.
// TypeDate columns use epoch days (small int64 values), TypeTimestamp
// columns use epoch milliseconds (large int64 values). The threshold
// 500_000 (~year 3339 in days) safely distinguishes the two.
//
// The signal is the whole point at the comparison site. Its callers used to
// test the RESULT — `if bi := parseTemporalInt64(ai, bs); bi != 0 || ai == 0`
// — which reads "the string parsed, OR the number is zero", and the second
// half is true of ANY unparseable string once the numeric side is zero. So
// `0 = '0.0001'` was TRUE, and after the box-sniffing branch above it was
// deleted, every `int_col = 'anything'` comparison against the row holding
// ZERO went the same way: `k = '2'` matched k=0 as well as nothing else
// (#504 review, B1). A parse either happened or it did not; that is what the
// branch needs to know.
func parseTemporalInt64OK(ref int64, s string) (int64, bool) {
	if ref < 500_000 && ref > -500_000 {
		// Reference is epoch days — parse the string as days too.
		return parseDateToEpochDaysCachedOK(s)
	}
	return parseTimestampToEpochMsCachedOK(s)
}

// Deterministic temporal-string parsers are called row-by-row from
// Cmp.EvalBool whenever a date/timestamp value is compared against a string.
// At SF100 the 22Q suite spent 4.24% of worker CPU (236s cum) re-parsing
// strings — every row of every filter re-walked the layout list — so the
// result is memoized.
//
// The memo is BOUNDED, and the reason is that its stated rationale was
// wrong about its own population. The original argument was "SQL queries
// have a fixed, tiny set of date literals … so a memoization cache stays
// trivially small and never grows unbounded". But the LITERAL shape never
// reaches here: compileCmp specializes a bare column against a string
// literal into CmpTemporalLit, which pre-parses once at compile time
// through the UNCACHED entry points, so `ts <= '1998-09-02'` adds zero
// entries. What does reach the memo is, by construction, the shape that
// specialization declined — a temporal value against another COLUMN's text
// — and those strings are DATA. The population is unbounded and the map is
// process-global with no eviction, so a query over a text column of
// timestamps added one entry per distinct value for the process's lifetime
// (#619).
//
// The bound is a generational reset rather than an LRU: the memo exists to
// collapse repetition WITHIN a scan, so dropping the whole generation when
// it fills costs a re-parse of the current working set and nothing else,
// for one counter and no eviction bookkeeping.
const temporalMemoCap = 4096

// temporalMemo is a string→temporalParseResult memo with a hard entry cap.
type temporalMemo struct {
	m sync.Map
	n atomic.Int64
}

func (c *temporalMemo) load(s string) (temporalParseResult, bool) {
	v, ok := c.m.Load(s)
	if !ok {
		return temporalParseResult{}, false
	}
	return v.(temporalParseResult), true
}

func (c *temporalMemo) store(s string, r temporalParseResult) {
	if c.n.Load() >= temporalMemoCap {
		// Drop the generation. Concurrent stores may overshoot the cap by
		// however many are in flight, which is bounded by the worker count
		// and is why the gate asserts a ceiling rather than an equality.
		c.m.Clear()
		c.n.Store(0)
	}
	if _, loaded := c.m.LoadOrStore(s, r); !loaded {
		c.n.Add(1)
	}
}

func (c *temporalMemo) entries() int {
	n := 0
	c.m.Range(func(_, _ any) bool { n++; return true })
	return n
}

func (c *temporalMemo) reset() {
	c.m.Clear()
	c.n.Store(0)
}

// temporalParseResult is a cached string→epoch conversion's outcome,
// including whether the parse succeeded — 0 is a valid epoch-days/epoch-ms
// result in its own right (the Unix epoch itself), so the cache cannot
// collapse "parsed to 0" and "did not parse" into the same cached value the
// way a bare int64 cache would.
type temporalParseResult struct {
	v  int64
	ok bool
}

// parseDateToEpochDaysCachedOK is parseDateToEpochDaysOK routed through
// dateEpochDaysCache, so parseTemporalInt64OK's row-by-row date comparisons
// serve a repeated literal from a map lookup instead of re-walking up to 4
// time.Parse layouts every row.
func parseDateToEpochDaysCachedOK(s string) (int64, bool) {
	if r, ok := dateEpochDaysCache.load(s); ok {
		return r.v, r.ok
	}
	days, ok := parseDateToEpochDaysOK(s)
	dateEpochDaysCache.store(s, temporalParseResult{v: days, ok: ok})
	return days, ok
}

// parseDateToEpochDaysOK is the uncached parse with an explicit success
// signal (0 is a valid result for the epoch itself). Used at expression
// compile time by compileCmp's temporal-literal specialization, where it
// runs once per literal rather than once per row, and by
// parseDateToEpochDaysCachedOK on a cache miss.
//
// The day count is computed from t.Unix() (civil-days arithmetic), not
// t.Sub(epoch): Sub returns a time.Duration, which saturates at
// ±math.MaxInt64 ns (~292 years) rather than reporting an overflow, so a
// 4-digit-year date before 1678 or after 2262 previously answered a
// silently CLAMPED day count (#451, same mechanism as
// kernel.parseDateToDays). No range check is needed here: the result
// compares against a DATE column's int32-range value, and an int64 outside
// that range simply never equals one — the value has nowhere further to
// truncate to.
// parseDateToEpochDaysOK reads a date literal through THE accept-set, so a
// DATE predicate and a TIMESTAMP predicate cannot disagree about what one
// literal names (review B2).
func parseDateToEpochDaysOK(s string) (int64, bool) {
	t, ok := parquet.ParseTimestampWallClock(s)
	if !ok {
		return 0, false
	}
	const secondsPerDay = 86400
	sec := t.Unix()
	days := sec / secondsPerDay
	if sec%secondsPerDay < 0 {
		days--
	}
	return days, true
}

// parseTimestampToEpochMs parses common timestamp string formats into epoch
// milliseconds, discarding the success signal ToInt64's callers have no use
// for (a failed parse and an epoch-zero result both read as 0 there, which
// is the existing contract for a non-temporal string reaching ToInt64).
func parseTimestampToEpochMs(s string) int64 {
	v, _ := parseTimestampToEpochMsCachedOK(s)
	return v
}

// parseTimestampToEpochMsCachedOK is parseTimestampToEpochMsOK routed
// through timestampEpochMsCache — the same cache ToInt64's
// parseTimestampToEpochMs already shares, so a literal parsed through either
// caller warms the other's lookup too, and there is exactly one cache of
// timestamp parses rather than one per caller.
func parseTimestampToEpochMsCachedOK(s string) (int64, bool) {
	if r, ok := timestampEpochMsCache.load(s); ok {
		return r.v, r.ok
	}
	ms, ok := parseTimestampToEpochMsOK(s)
	timestampEpochMsCache.store(s, temporalParseResult{v: ms, ok: ok})
	return ms, ok
}

// parseTimestampToEpochMsOK is the uncached parse with an explicit success
// signal (see parseDateToEpochDaysOK).
// parseTimestampToEpochMsOK reads a timestamp literal through THE accept-set.
//
// It used to carry its own copy of the layout list AND apply the literal's
// offset, so once #692 fixed the writer and the two kernels this one disagreed
// with them: a row written with `'2020-06-01T12:00:00+05:30'` could not be
// found by `WHERE t = ` that same literal (review B2). One function, one
// answer, on every path.
func parseTimestampToEpochMsOK(s string) (int64, bool) {
	t, ok := parquet.ParseTimestampWallClock(s)
	if !ok {
		return 0, false
	}
	return t.UnixMilli(), true
}

func toBool(e Expr, b *batch.RecordBatch, row int) bool {
	if be, ok := e.(BoolExpr); ok {
		return be.EvalBool(b, row)
	}
	v := e.Eval(b, row)
	return toBoolVal(v)
}

func toBoolVal(v any) bool {
	if v == nil {
		return false
	}
	switch tv := v.(type) {
	case bool:
		return tv
	case float64:
		return tv != 0
	case int64:
		return tv != 0
	case int:
		return tv != 0
	case string:
		return tv != ""
	default:
		return true
	}
}

// toInt64Safe converts a value to int64, returning false if not possible.
func toInt64Safe(v any) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int32:
		return int64(tv), true
	case int:
		return int64(tv), true
	default:
		return 0, false
	}
}

// toFloat64Safe converts a value to float64, returning false if not possible.
func toFloat64Safe(v any) (float64, bool) {
	switch tv := v.(type) {
	case float64:
		return tv, true
	case float32:
		return float64(tv), true
	case int64:
		return float64(tv), true
	case int32:
		return float64(tv), true
	default:
		return 0, false
	}
}

func compare(a, b any, op CmpOp) bool {
	// Fast path: both int64 (most common for column comparisons)
	if ai, ok := a.(int64); ok {
		if bi, ok := b.(int64); ok {
			switch op {
			case CmpEq:
				return ai == bi
			case CmpNe:
				return ai != bi
			case CmpLt:
				return ai < bi
			case CmpLe:
				return ai <= bi
			case CmpGt:
				return ai > bi
			case CmpGe:
				return ai >= bi
			}
		}
	}
	// Fast path: both float64. PostgreSQL's float order, like every other
	// float comparison in the tree (cmpFloat64Op above).
	if af, ok := a.(float64); ok {
		if bf, ok := b.(float64); ok {
			return cmpFloat64Op(af, bf, op)
		}
	}
	// Fast path: both string
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			switch op {
			case CmpEq:
				return as == bs
			case CmpNe:
				return as != bs
			case CmpLt:
				return as < bs
			case CmpLe:
				return as <= bs
			case CmpGt:
				return as > bs
			case CmpGe:
				return as >= bs
			}
		}
	}
	// A mixed number/text pair used to be read NUMERICALLY here whenever the
	// text PARSED as a number, so that a DECIMAL column — which boxes as its
	// rendered text (Vector.GetValue) — would order correctly against an
	// INT64 column (#476). Nothing in a box says which of the two it is,
	// though, so the same branch read a genuine STRING column's value
	// numerically too: `WHERE s = 1.5` matched the row holding "1.50" here
	// and matched nothing through the vectorized kernel, one predicate with
	// two answers (#504).
	//
	// The reading is a BINDING now, not a guess: expr.boxedPair resolves both
	// operands' DECLARED kinds and answers the DECIMAL pairs before anything
	// reaches this function. compare() is what remains for a pair with no
	// declaration to consult, and it treats a string as a string.
	//
	// Mixed int64/string: implicit date/timestamp casting.
	// TypeDate columns store epoch days (int32→int64), TypeTimestamp stores epoch ms.
	// Date strings ("YYYY-MM-DD") are exactly 10 chars with no time component.
	if ai, ok := a.(int64); ok {
		if bs, ok := b.(string); ok {
			if bi, ok := parseTemporalInt64OK(ai, bs); ok {
				switch op {
				case CmpEq:
					return ai == bi
				case CmpNe:
					return ai != bi
				case CmpLt:
					return ai < bi
				case CmpLe:
					return ai <= bi
				case CmpGt:
					return ai > bi
				case CmpGe:
					return ai >= bi
				}
			}
		}
	}
	if as, ok := a.(string); ok {
		if bi, ok := b.(int64); ok {
			if ai, ok := parseTemporalInt64OK(bi, as); ok {
				switch op {
				case CmpEq:
					return ai == bi
				case CmpNe:
					return ai != bi
				case CmpLt:
					return ai < bi
				case CmpLe:
					return ai <= bi
				case CmpGt:
					return ai > bi
				case CmpGe:
					return ai >= bi
				}
			}
		}
	}
	// Mixed numeric types — an INT64 literal against a FLOAT column, a
	// float32 box (CAST(x AS REAL)) against a float64 one, and every other
	// pair the two fast paths above do not catch.
	//
	// cmpFloat64Op, not Go's operators. The both-float64 fast path above
	// already answers in PostgreSQL's float total order (NaN greatest and
	// equal to itself, ADR-0012 item 8), and this branch did not — so
	// `f > 1` DROPPED the NaN rows while `f > 1.0` kept them, one predicate
	// with two answers decided by whether the literal was spelled with a
	// decimal point. It is also the ROW path, which is what the stage DAG
	// compiles every scan-pushed filter to, so the same query answered
	// differently distributed than in process. #459 closed this order for the
	// kernels, the keys and the both-float64 box pair; this arm was the one
	// escape left.
	if isNumeric(a) && isNumeric(b) {
		return cmpFloat64Op(ToFloat64(a), ToFloat64(b), op)
	}
	// Fall back to string comparison
	as := toString(a)
	bs := toString(b)
	switch op {
	case CmpEq:
		return as == bs
	case CmpNe:
		return as != bs
	case CmpLt:
		return as < bs
	case CmpLe:
		return as <= bs
	case CmpGt:
		return as > bs
	case CmpGe:
		return as >= bs
	}
	return false
}

func isNumeric(v any) bool {
	switch v.(type) {
	case float64, float32, int64, int, int32:
		return true
	default:
		return false
	}
}

func toTime(args []any) time.Time {
	if len(args) < 1 || args[0] == nil {
		return time.Time{}
	}
	return parseTime(args[0])
}

func parseTime(v any) time.Time {
	switch tv := v.(type) {
	case time.Time:
		return tv
	case int64:
		// UTC, matching the vectorized kernels (vecExtract/vecHour/…).
		// Local time made date_trunc/extract results depend on host TZ.
		return time.Unix(tv, 0).UTC()
	case float64:
		return time.Unix(int64(tv), 0).UTC()
	case string:
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, tv); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

// matchLike implements SQL LIKE pattern matching.
// % matches any sequence, _ matches a single character.
func matchLike(s, pattern string) bool {
	return matchLikeRecur(s, pattern, 0, 0)
}

func matchLikeRecur(s, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		switch pattern[pi] {
		case '%':
			pi++
			// Skip consecutive %
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi == len(pattern) {
				return true // trailing % matches everything
			}
			for i := si; i <= len(s); i++ {
				if matchLikeRecur(s, pattern, i, pi) {
					return true
				}
			}
			return false
		case '_':
			if si >= len(s) {
				return false
			}
			si++
			pi++
		default:
			if si >= len(s) || s[si] != pattern[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}
