// SPDX-License-Identifier: MIT

// This file holds the ONE range rule for a constructed DATE or TIMESTAMP;
// ADR-0012 and ADR-0024 govern the execution contracts.
package expr

import (
	"math"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A DATE is carried as int32 epoch DAYS and a TIMESTAMP as int64 epoch
// MILLISECONDS, and both carriers hold values PostgreSQL's types do not: an
// int32 day count reaches the year 5,881,580 in either direction, and an int64
// millisecond count past ±292 million years — where Go's UnixMilli itself
// wraps. PostgreSQL's DATE spans 4714-11-24 BC … 5874897-12-31 and its
// TIMESTAMP 4714-11-24 00:00 BC … 294276-12-31 23:59:59.999999 (measured on
// 17.11: `'5874897-12-31'::date - '1970-01-01'` is 2145042905,
// `'4714-11-24 BC'::date - '1970-01-01'` is -2440588, and epoch(
// '294276-12-31 23:59:59.999') is 9224318015999.999 s); one step past either
// end is 22008 `date out of range` / `timestamp out of range`.
//
// Every place a temporal VALUE is constructed from a number or an instant —
// date arithmetic, an INTERVAL shift, a CAST, date_add / date_sub, the
// clock and parse functions — goes through dateDaysBox / dateBox /
// instantBox below, and the write path asks DateDaysInRange /
// TimestampMillisInRange rather than narrowing on its own. Before this rule a
// DATE computed past int32 was narrowed with `int32(t)` at the writer and
// stored as `-5877585-08-22` (`UPDATE … SET d = d + 2147483647`), and an
// INTERVAL shift past the millisecond carrier wrapped to year -284552024 (arc
// VL round-3 review B1) — the same family as #911, whose bigint→DATE cast was
// narrowed to int32 before the store: that close put the check where the day
// count is still the number the query wrote, and this one puts it where every
// temporal value is built.
const (
	minEpochDay = parquet.MinDateDay // 4714-11-24 BC
	maxEpochDay = parquet.MaxDateDay // 5874897-12-31

	minEpochMilli = parquet.MinTimestampMilli // 4714-11-24 00:00:00 BC
	endEpochMilli = parquet.EndTimestampMilli // 294277-01-01 00:00:00, exclusive
)

var (
	minInstant    = time.UnixMilli(minEpochMilli).UTC()
	endInstant    = time.UnixMilli(endEpochMilli).UTC()
	endDayInstant = time.Unix((maxEpochDay+1)*86400, 0).UTC()
)

// DateDaysInRange is the write path's question for a DATE-declared box: nil
// when n is a day PostgreSQL's DATE holds, else its 22008. It IS
// parquet.DateDaysInRange — one range question for the constructors here, the
// SQL write doors and the writer's own box normalisation (the embedded
// ingester API).
func DateDaysInRange(n int64) error { return parquet.DateDaysInRange(n) }

// TimestampMillisInRange is DateDaysInRange for a TIMESTAMP-declared box.
func TimestampMillisInRange(ms int64) error { return parquet.TimestampMillisInRange(ms) }

// dateDaysBox is the DATE box of an epoch-day count, refused 22008 outside
// PostgreSQL's range.
func dateDaysBox(n int64) int64 {
	if err := DateDaysInRange(n); err != nil {
		panic(fatalEval{err})
	}
	return n
}

// dateBox is the DATE box of the UTC day an instant falls in.
func dateBox(t time.Time) int64 {
	if t.Before(minInstant) || !t.Before(endDayInstant) {
		panic(fatalEval{sqlerr.New("22008", "date out of range")})
	}
	return epochDaysOf(t)
}

// instantBox is the TIMESTAMP box of an instant: UTC epoch milliseconds, what
// a TIMESTAMP column's ColRef.Eval hands out. Every TIMESTAMP-declared kernel
// returns it (arc VL round 3), so its value and its declaration agree; an
// instant PostgreSQL's TIMESTAMP cannot hold is 22008 here, before UnixMilli
// could wrap it.
func instantBox(t time.Time) int64 {
	if t.Before(minInstant) || !t.Before(endInstant) {
		raiseTimestampOutOfRange()
	}
	return t.UTC().UnixMilli()
}

// shiftDays is `day + n` in whole days, the DATE arithmetic: exact integer
// addition — never time.AddDate, whose day count is multiplied by 86400 in an
// unchecked integer — refused 22008 past the DATE range.
func shiftDays(day, n int64) int64 {
	if (n > 0 && day > math.MaxInt64-n) || (n < 0 && day < math.MinInt64-n) {
		panic(fatalEval{sqlerr.New("22008", "date out of range")})
	}
	return dateDaysBox(day + n)
}

// daysToInstant is the midnight of an epoch-day count as a TIMESTAMP box —
// `date::timestamp` — refused 22008 when the day lies outside the TIMESTAMP
// range (PostgreSQL: `date out of range for timestamp`).
func daysToInstant(n int64) int64 {
	if n < minEpochMilli/86400000 || n >= endEpochMilli/86400000 {
		panic(fatalEval{sqlerr.New("22008", "date out of range for timestamp")})
	}
	return n * 86400000
}
