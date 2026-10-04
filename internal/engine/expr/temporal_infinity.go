// SPDX-License-Identifier: MIT

// This file holds the expression layer's reading of the two infinite
// temporal values; ADR-0012 governs the execution contract.
package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// infiniteInstant is a temporal box holding one of its type's two infinite
// values — the carrier's extremes (parquet.DatePosInfinity …
// TimestampNegInfinity) — read where the row path would otherwise read an
// instant. It names no instant: time.Time would give the extreme integer a
// calendar date millions of years away, and every field, shift or difference
// computed from that date is a finite wrong value. So the readers
// (columnReading under temporalOperand / resolveTemporalArgs / the field
// kernels, and temporalBoxInstant)
// hand it ONLY to the operations PostgreSQL answers for an infinite argument,
// each of which answers it in its own body:
//
//   - CAST between DATE and TIMESTAMP keeps the sign (castTemporal);
//   - `± INTERVAL`, `date ± integer` keep the value (intervalShift,
//     dateArith, unknownTemporalArith);
//   - `date - date` and `timestamp - timestamp` raise 22008 (PostgreSQL:
//     `cannot subtract infinite dates`; a timestamp difference is an INTERVAL
//     there, infinite in 17, which this engine's INTERVAL cannot hold);
//   - date_trunc and timezone answer the value; EXTRACT / date_part answer
//     ±Infinity for year and epoch and NULL for the other fields
//     (extractInfinite);
//
// and every other consumer refuses it with 22008 (raiseInfiniteOperand)
// rather than computing on it.
type infiniteInstant struct {
	sign int  // +1 infinity, -1 -infinity
	date bool // the box was a DATE (else a TIMESTAMP)
}

// tsBox is the TIMESTAMP box of the same infinite value.
func (x infiniteInstant) tsBox() int64 {
	if x.sign > 0 {
		return parquet.TimestampPosInfinity
	}
	return parquet.TimestampNegInfinity
}

// dateBox is the DATE box (int64 epoch days, the row path's DATE box) of the
// same infinite value.
func (x infiniteInstant) dateBox() int64 {
	if x.sign > 0 {
		return int64(parquet.DatePosInfinity)
	}
	return int64(parquet.DateNegInfinity)
}

func (x infiniteInstant) typeName() string {
	if x.date {
		return "date"
	}
	return "timestamp"
}

// boxInfinity reads a row-path box whose producer declares kind.
func boxInfinity(kind castTemporalKindT, v any) (infiniteInstant, bool) {
	n, ok := v.(int64)
	if !ok {
		return infiniteInstant{}, false
	}
	switch kind {
	case castToDateKind:
		return dateInfinity(n)
	case castToTimestampKind:
		return timestampInfinity(n)
	}
	return infiniteInstant{}, false
}

func dateInfinity(n int64) (infiniteInstant, bool) {
	switch n {
	case int64(parquet.DatePosInfinity):
		return infiniteInstant{sign: 1, date: true}, true
	case int64(parquet.DateNegInfinity):
		return infiniteInstant{sign: -1, date: true}, true
	}
	return infiniteInstant{}, false
}

func timestampInfinity(ms int64) (infiniteInstant, bool) {
	switch ms {
	case parquet.TimestampPosInfinity:
		return infiniteInstant{sign: 1}, true
	case parquet.TimestampNegInfinity:
		return infiniteInstant{sign: -1}, true
	}
	return infiniteInstant{}, false
}

// textInfinity reads a text operand as an infinite value through the one
// grammar (the text a date-arithmetic operand or a function argument may
// arrive as).
func textInfinity(s string) (infiniteInstant, bool) {
	ms, err := parquet.ParseTimestampMillis(s)
	if err != nil {
		return infiniteInstant{}, false
	}
	return timestampInfinity(ms)
}

// raiseInfiniteOperand refuses an operation this engine does not define over
// an infinite value: 22008, the class PostgreSQL raises for the operations it
// refuses over one (`cannot subtract infinite dates`, `interval out of
// range`) — never the finite value the extreme integer would compute.
func raiseInfiniteOperand(op string, x infiniteInstant) {
	panic(fatalEval{sqlerr.New("22008", "%s is not defined for an infinite %s", op, x.typeName())})
}

// extractInfinite is EXTRACT / date_part of an infinite value, measured on
// PostgreSQL 17.11 for each field this engine extracts: year and epoch are
// ±Infinity, every other field (quarter, month, week, day, hour, minute,
// second, dow, doy) is NULL. ok=false for a unit this engine does not
// extract, which answers NULL as it does for a finite value.
func extractInfinite(unit string, x infiniteInstant) (float64, bool) {
	switch unit {
	case "year", "epoch":
		return math.Inf(x.sign), true
	}
	return 0, false
}

// extractFieldFuncs are the functions EXTRACT(field FROM x) is spelled as
// (the parser writes `EXTRACT(YEAR FROM ts)` as `year(ts)`), each with the
// EXTRACT field it answers: over an infinite value they answer what EXTRACT
// does (extractInfinite).
var extractFieldFuncs = map[string]string{
	"year": "year", "month": "month", "day": "day", "hour": "hour",
	"minute": "minute", "second": "second", "quarter": "quarter", "week": "week",
	"day_of_week": "dow", "day_of_year": "doy", "epoch": "epoch",
}

// infiniteExtract is a field function's answer over an infinite argument:
// ok=false when the call is not one, or its argument is finite.
func (e *FuncCall) infiniteExtract(args []any) (any, bool) {
	if e.extractUnit == "" || len(args) != 1 {
		return nil, false
	}
	x, inf := args[0].(infiniteInstant)
	if !inf {
		return nil, false
	}
	if f, ok := extractInfinite(e.extractUnit, x); ok {
		return f, true
	}
	return nil, true
}

// vecExtractInfinite writes row i of a vectorized field function over an
// infinite value.
func vecExtractInfinite(out *batch.Vector, i int, unit string, x infiniteInstant) {
	if f, ok := extractInfinite(unit, x); ok {
		out.Float64Data[i] = f
	} else {
		out.Nulls.SetNull(i)
	}
}
