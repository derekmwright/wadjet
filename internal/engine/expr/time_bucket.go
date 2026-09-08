package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TIME_BUCKET — PostgreSQL's `date_bin`, under the name every time-series
// engine spells it.
//
//	time_bucket(stride, source)          -- origin defaults to 1970-01-01
//	time_bucket(stride, source, origin)
//
// It answers the largest multiple of `stride` measured from `origin` that does
// not exceed `source`, which is what makes it the GROUP BY key of a
// downsampled bar: every row of one bucket maps to one instant, and that
// instant is a real TIMESTAMP (OID 1114) rather than text.
//
// PostgreSQL 17 is the oracle for every one of its answers, measured live on
// 2026-09-08:
//
//	date_bin('15 min','2020-02-11 15:44:17','2001-01-01') -> 2020-02-11 15:30:00
//	date_bin('15 min','2020-02-11 15:45:00','2001-01-01') -> 2020-02-11 15:45:00
//	date_bin('1 hour','1969-07-20 20:17:40','1970-01-01') -> 1969-07-20 20:00:00
//	date_bin('1 day','1969-12-30 23:59:59.999999',epoch)  -> 1969-12-30 00:00:00
//	date_bin('1 hour', ts, '2030-01-01 00:30:00')         -> bins aligned at :30
//	any NULL argument                                     -> NULL
//	a stride containing MONTHS or YEARS                   -> 0A000
//	a stride of zero or less                              -> 22008
//
// Two properties of that list are load-bearing and easy to get wrong:
//
//   - The division FLOORS toward negative infinity, not toward zero. Before
//     1970 a truncating division would answer the bucket ABOVE the row, so
//     every pre-epoch row would be filed under a bucket it does not belong to
//     and the bar for that bucket would mix two of them.
//   - The bucket boundary belongs to the bucket it OPENS. `15:45:00` with a
//     15-minute stride is `15:45:00`, not `15:30:00`.
//
// The stride is a wall-clock span, so this engine's UTC instants make every
// bucket exactly `stride` wide. There is no DST to make an hour bucket 3600 or
// 7200 seconds depending on the day, which is the ambiguity PostgreSQL itself
// avoids by refusing months and years: those are the calendar units whose
// length depends on WHERE they land, and neither engine will guess.
//
// The stride must be spelled as an INTERVAL literal. That keeps the accepted
// grammar exactly the one `plansql.parseIntervalLiteral` already defines —
// a single `N unit` pair — rather than growing a second interval parser here
// that would agree with the first only by inspection.
func fnTimeBucket(args []any) any {
	if len(args) < 2 || len(args) > 3 {
		raiseTimeBucketArity(len(args))
	}
	for _, a := range args {
		if a == nil {
			return nil
		}
	}
	iv, ok := args[0].(IntervalValue)
	if !ok {
		raiseTimeBucketStrideNotInterval()
	}
	strideMs := timeBucketStrideMillis(iv)

	src := parseTime(args[1])
	if src.IsZero() {
		return nil
	}
	originMs := int64(0) // 1970-01-01 00:00:00, PostgreSQL's conventional origin
	if len(args) == 3 {
		origin := parseTime(args[2])
		if origin.IsZero() {
			return nil
		}
		originMs = origin.UTC().UnixMilli()
	}
	binned, ok := binMillis(src.UTC().UnixMilli(), originMs, strideMs)
	if !ok {
		raiseTimestampOutOfRange()
	}
	// batch.FormatTimestamp is what formatInstant itself calls — the ONE
	// instant renderer. A TIMESTAMP vector materializes this text back into
	// the epoch milliseconds it stores, exactly as date_trunc's result does
	// (#868).
	return batch.FormatTimestamp(binned)
}

// timeBucketStrideMillis is the stride in milliseconds, with PostgreSQL's two
// refusals. It is a function rather than an expression because BOTH refusals
// are part of what time_bucket MEANS: a calendar stride has no fixed width and
// a non-positive one has no buckets, and answering either with a number would
// be a bar nobody can check.
func timeBucketStrideMillis(iv IntervalValue) int64 {
	if iv.Years != 0 || iv.Months != 0 {
		raiseCalendarStride()
	}
	ms := int64(iv.Days)*86_400_000 +
		int64(iv.Hours)*3_600_000 +
		int64(iv.Minutes)*60_000 +
		int64(iv.Seconds)*1_000
	if ms <= 0 {
		raiseNonPositiveStride()
	}
	return ms
}

// binMillis floors `src` to the largest `origin + k*stride` that does not
// exceed it, in epoch milliseconds. stride is > 0 by construction.
//
// ok=false means the arithmetic left the int64 instant range — reachable only
// with an origin and a source at opposite ends of it — and the caller raises
// rather than wrapping into a bucket on the other side of time.
func binMillis(src, origin, stride int64) (int64, bool) {
	delta, ok := subChecked(src, origin)
	if !ok {
		return 0, false
	}
	q := delta / stride
	if delta%stride != 0 && delta < 0 {
		// Floor toward negative infinity: Go's integer division truncates
		// toward zero, which for a pre-origin instant names the bucket ABOVE
		// the row.
		q--
	}
	off, ok := mulChecked(q, stride)
	if !ok {
		return 0, false
	}
	return addChecked(origin, off)
}

func addChecked(a, b int64) (int64, bool) {
	s := a + b
	if (a > 0 && b > 0 && s < 0) || (a < 0 && b < 0 && s >= 0) {
		return 0, false
	}
	return s, true
}

func subChecked(a, b int64) (int64, bool) {
	if b == math.MinInt64 {
		if a >= 0 {
			return 0, false
		}
		return a - b, true
	}
	return addChecked(a, -b)
}

func mulChecked(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	p := a * b
	if p/b != a {
		return 0, false
	}
	return p, true
}

// raiseCalendarStride is PostgreSQL's own refusal, SQLSTATE 0A000, measured on
// 17.11: `date_bin('1 month', …)` -> "timestamps cannot be binned into
// intervals containing months or years".
func raiseCalendarStride() {
	panic(fatalEval{sqlerr.New("0A000",
		"timestamps cannot be binned into intervals containing months or years")})
}

// raiseNonPositiveStride is PostgreSQL's 22008 for `date_bin('0 s', …)`.
func raiseNonPositiveStride() {
	panic(fatalEval{sqlerr.New("22008", "stride must be greater than zero")})
}

// raiseTimestampOutOfRange is PostgreSQL's 22008 for an instant the type
// cannot hold. Reached only when origin and source sit at opposite ends of the
// int64 millisecond range.
func raiseTimestampOutOfRange() {
	panic(fatalEval{sqlerr.New("22008", "timestamp out of range")})
}

// raiseTimeBucketStrideNotInterval refuses a first argument that is not an
// INTERVAL literal. 42804 is PostgreSQL's datatype_mismatch; the sentence
// names the spelling that works, because the alternative — a second interval
// parser living here — would agree with the one in the SQL parser only by
// inspection.
func raiseTimeBucketStrideNotInterval() {
	panic(fatalEval{sqlerr.New("42804",
		"time_bucket: the stride must be an INTERVAL literal, as in "+
			"time_bucket(INTERVAL '15' MINUTE, ts)")})
}

func raiseTimeBucketArity(n int) {
	panic(fatalEval{sqlerr.New("42883",
		"time_bucket takes 2 or 3 arguments (stride, source[, origin]), got %d", n)})
}
