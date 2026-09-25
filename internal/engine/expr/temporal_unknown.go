// SPDX-License-Identifier: MIT

// This file holds the operator resolution of an UNKNOWN-typed (quoted)
// operand beside a DATE or TIMESTAMP; ADR-0012 and ADR-0024 govern the
// execution contracts.
package expr

import (
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// UnknownTemporal is how PostgreSQL's operator resolution reads a `+` / `-`
// whose one operand is a DATE or a TIMESTAMP and whose other operand is an
// unknown-typed literal — a quoted string (or NULL) the statement did not
// type. PostgreSQL first assumes the unknown side has the OTHER side's type
// (func_select_candidate's rule 2.a): `date - date` and `timestamp -
// timestamp` exist, so the literal of `d - '2026-03-01'` / `ts - '…'` is read
// as a DATE / TIMESTAMP. `date + date` and `timestamp + timestamp` do not; for
// `timestamp + unknown` the one remaining candidate is `timestamp + interval`,
// and for `date + unknown` there are several (integer, interval, time) —
// 42725 `operator is not unique`.
//
// Before arc VL round 5 the quoted operand was read by the numeric path: its
// leading number was a day count or a MILLISECOND count (`ts + '1 day'`
// answered ts + 1 ms as a number, `d - '2026-03-01'` answered d − 2026 days,
// `DATE '…' + '1.5'` stored 20516.5 in a float column — round-4 review B3).
type UnknownTemporal int

const (
	// UnknownNotTemporal: not this shape; the ordinary rules stand.
	UnknownNotTemporal UnknownTemporal = iota
	// UnknownAmbiguous: `date + unknown`, `unknown + date` — 42725.
	UnknownAmbiguous
	// UnknownAsDate: `date - unknown`, `unknown - date` — the literal is a
	// DATE and the difference a count of days.
	UnknownAsDate
	// UnknownAsTimestamp: `timestamp - unknown`, `unknown - timestamp` — the
	// literal is a TIMESTAMP and the difference the documented milliseconds
	// (this engine has no INTERVAL type for `timestamp - timestamp`).
	UnknownAsTimestamp
	// UnknownAsInterval: `timestamp + unknown`, `unknown + timestamp` — the
	// literal is an INTERVAL and the sum a TIMESTAMP.
	UnknownAsInterval
)

// ResolveUnknownTemporal is the ONE resolution table: the typing rule
// (physical.temporalArithmetic), the declaration (physical.binOpTemporalType)
// and the kernel (BinOp.Eval) all read it. timestamp reports whether the
// typed operand is a TIMESTAMP (else a DATE).
func ResolveUnknownTemporal(op string, timestamp bool) UnknownTemporal {
	switch {
	case op == "+" && timestamp:
		return UnknownAsInterval
	case op == "+":
		return UnknownAmbiguous
	case op == "-" && timestamp:
		return UnknownAsTimestamp
	case op == "-":
		return UnknownAsDate
	}
	return UnknownNotTemporal
}

// UnknownTemporalAmbiguous is PostgreSQL's 42725 for `date + unknown`;
// litLeft places the unknown operand as the statement wrote it.
func UnknownTemporalAmbiguous(op string, litLeft bool) error {
	if litLeft {
		return sqlerr.New("42725", "operator is not unique: unknown %s date", op)
	}
	return sqlerr.New("42725", "operator is not unique: date %s unknown", op)
}

// CheckUnknownTemporalLiteral reads a quoted operand as the type r resolves it
// to, at plan time, and reports PostgreSQL's refusal of its text (22007 /
// 22008 for a DATE or TIMESTAMP, 22007 for an INTERVAL) — PostgreSQL coerces
// the constant while it analyses the statement, so the refusal does not
// depend on any row.
func CheckUnknownTemporalLiteral(r UnknownTemporal, text string) (err error) {
	defer func() {
		if p := recover(); p != nil {
			fe, ok := p.(fatalEval)
			if !ok {
				panic(p)
			}
			err = fe.err
		}
	}()
	switch r {
	case UnknownAsDate:
		_, err = parquet.ParseDateDays(text)
	case UnknownAsTimestamp:
		_, err = parquet.ParseTimestampMillis(text)
	case UnknownAsInterval:
		castToIntervalText(text)
	}
	return err
}

// unknownLiteralText is a compiled operand's text when it is a quoted
// (string) literal.
func unknownLiteralText(e Expr) (string, bool) {
	l, ok := e.(*Lit)
	if !ok {
		return "", false
	}
	s, ok := l.Val.(string)
	return s, ok
}

// unknownTemporalArith evaluates `+` / `-` with one quoted-literal operand and
// one DATE or TIMESTAMP operand by ResolveUnknownTemporal. ok is false for any
// other shape (including a TEXT column beside the literal, which keeps its
// reading).
func (e *BinOp) unknownTemporalArith(b *batch.RecordBatch, row int, lv, rv any) (any, bool) {
	lit, litLeft := unknownLiteralText(e.Left)
	other, ov := e.Right, rv
	if !litLeft {
		var ok bool
		if lit, ok = unknownLiteralText(e.Right); !ok {
			return nil, false
		}
		other, ov = e.Left, lv
	} else if _, both := unknownLiteralText(e.Right); both {
		return nil, false
	}
	d, ok := temporalOperand(b, row, other, ov)
	if !ok {
		return nil, false
	}
	var t time.Time
	isTimestamp := false
	switch x := d.(type) {
	case civilDate:
		t = x.t
	case time.Time:
		t, isTimestamp = x, true
	default:
		return nil, false
	}
	switch ResolveUnknownTemporal(e.Op, isTimestamp) {
	case UnknownAmbiguous:
		panic(fatalEval{UnknownTemporalAmbiguous(e.Op, litLeft)})
	case UnknownAsDate:
		days, _ := castTemporalText(lit, castToDateKind)
		diff := epochDaysOf(t) - days.(int64)
		if litLeft {
			diff = -diff
		}
		return diff, true
	case UnknownAsTimestamp:
		ms, _ := castTemporalText(lit, castToTimestampKind)
		diff := t.UnixMilli() - ms.(int64)
		if litLeft {
			diff = -diff
		}
		return float64(diff), true
	case UnknownAsInterval:
		return intervalShift(t, castToIntervalText(lit), false), true
	}
	return nil, false
}
