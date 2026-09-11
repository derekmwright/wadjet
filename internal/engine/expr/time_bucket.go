package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TIME_BUCKET(stride, source[, origin]) returns the largest stride multiple
// from origin <= source as TIMESTAMP (OID 1114); origin defaults to epoch.
// Floor division toward negative infinity, including pre-epoch instants;
// a boundary belongs to the bucket it opens. Any NULL argument returns NULL.
// Strides are UTC wall-clock spans, with no DST adjustment; months/years
// raise 0A000 and nonpositive strides raise 22008.
// Require the existing INTERVAL literal grammar, one N unit pair;
// do not introduce a second interval parser.
// See docs/internals/time-bucket-stride-origin-design.md for the design.
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
