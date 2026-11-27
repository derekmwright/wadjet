// SPDX-License-Identifier: MIT

// This file holds the fail-closed guard on a timestamp difference at the
// expression's own minus operator (#1713d round 8; ADR-0012 temporal r16).
package expr

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// PostgreSQL types `timestamp - timestamp` (and `timestamp - date`, the date
// read as its midnight) as an INTERVAL. This engine has no interval value it
// could answer there: the arithmetic below the minus computes a number — a
// double of milliseconds on the numeric path, a whole day count when both
// instants sit on midnight (dateArith) — and a number is not PostgreSQL's
// answer to anything a statement can do with the difference.
//
// The statement rule (physical/validate_interval_use.go) refuses the
// difference at plan time with PostgreSQL's diagnostics wherever it can type
// both operands from the statement's AST. That walk cannot type every
// operand: the operand's type is sometimes known only to the plan (a scalar
// subquery's star body, an outer column inside a subquery body, a window
// function's default argument). So the guard below is the layer that makes
// the invariant hold: the minus operator itself refuses when its operands'
// DECLARED producers (producedTemporal — a column's type, a cast's
// destination, a function's fixed return type, a scalar subquery's declared
// output, date and interval arithmetic's result) are both temporal and one of
// them is a TIMESTAMP. `date - date` (an integer day count, as PostgreSQL)
// and every `± interval` shift are not differences and are untouched.
//
// It sits only at the SQL expression's minus (BinOp, BinOpNumeric, the
// quoted-literal arm): the engine's internal integer arithmetic on epoch
// values — window frames, time_bucket, join range conditions, scan pruning,
// sort keys — is Go code that never builds one of these nodes.
//
// Cost: the decision is taken once per node — at compile time where the
// operands' producers are already decided (a cast, a clock function, a
// declared scalar subquery), else when the node first meets a batch and its
// column operands resolve (BinOpNumeric.resolveModeSlow, BinOp's tsGuard). No
// per-row work is added to the typed numeric nodes; the boxed BinOp pays one
// atomic load per row once resolved.

// tsDiffGuardMessage is the guard's sentence.
const tsDiffGuardMessage = "a timestamp difference (timestamp - timestamp) cannot yet be computed: " +
	"PostgreSQL's answer is an interval, which this engine does not have; compare the timestamps " +
	"themselves instead, for example a.ts > b.ts + INTERVAL '30 days', or take a number of seconds " +
	"as extract(epoch FROM ts2) - extract(epoch FROM ts1)"

// TimestampDifferenceRefusal is the guard's 0A000.
func TimestampDifferenceRefusal() error {
	return sqlerr.New("0A000", "%s", tsDiffGuardMessage)
}

// isTimestampDifferenceRefusal reports the guard's own refusal: it is this
// engine's, never a refusal PostgreSQL raises while it folds a constant
// (ConstantEvalError lets the statement rule's diagnostic stand over it).
func isTimestampDifferenceRefusal(err error) bool {
	se, ok := err.(*sqlerr.Error)
	return ok && se.Message == tsDiffGuardMessage
}

// raiseTimestampDifference aborts the query with the guard's refusal.
func raiseTimestampDifference() {
	panic(fatalEval{TimestampDifferenceRefusal()})
}

// isTimestampDifferenceOperands reports `l - r` over two temporal producers,
// at least one a TIMESTAMP. b resolves column operands; nil leaves them
// undecided (not temporal), which is the compile-time question.
func isTimestampDifferenceOperands(l, r Expr, b *batch.RecordBatch) bool {
	lk := producedTemporal(l, b)
	if lk == castNotTemporal {
		return false
	}
	rk := producedTemporal(r, b)
	if rk == castNotTemporal {
		return false
	}
	return lk == castToTimestampKind || rk == castToTimestampKind
}

// timestampMinusQuoted reports `ts - '…'` / `'…' - ts`: a TIMESTAMP producer
// beside a quoted literal that is no interval input, which operator
// resolution reads as a TIMESTAMP (ResolveUnknownTemporal) — a timestamp
// difference, refused in unknownTemporalArith once the literal is read.
func timestampMinusQuoted(l, r Expr, b *batch.RecordBatch) bool {
	if s, ok := unknownLiteralText(r); ok {
		return !intervalInputText(s) && producedTemporal(l, b) == castToTimestampKind
	}
	if s, ok := unknownLiteralText(l); ok {
		return !intervalInputText(s) && producedTemporal(r, b) == castToTimestampKind
	}
	return false
}

// tsGuardState is a BinOp's resolved guard decision.
const (
	tsGuardUnresolved uint32 = iota
	tsGuardClear
	tsGuardRefuse
)

// tsGuardCheck decides, once per node, whether this minus is a timestamp
// difference, and refuses it if so. Any other operator is clear outright.
func (e *BinOp) tsGuardCheck(b *batch.RecordBatch) {
	g := &e.tsGuard
	switch g.Load() {
	case tsGuardClear:
		return
	case tsGuardRefuse:
		raiseTimestampDifference()
	}
	if e.Op != "-" {
		g.Store(tsGuardClear)
		return
	}
	if b == nil {
		// A constant evaluation (no batch): only non-column producers can
		// decide, and they are the compile-time check's. Not cached, so a
		// later batch still resolves the column operands.
		if isTimestampDifferenceOperands(e.Left, e.Right, nil) {
			raiseTimestampDifference()
		}
		return
	}
	if isTimestampDifferenceOperands(e.Left, e.Right, b) {
		g.Store(tsGuardRefuse)
		raiseTimestampDifference()
	}
	g.Store(tsGuardClear)
}

// refusingTimestampDifference is the node the compiler builds for a minus
// whose operands' producers already decide it is a timestamp difference: it
// raises the refusal when evaluated.
func refusingTimestampDifference(left, right Expr) *BinOp {
	e := &BinOp{Left: left, Right: right, Op: "-"}
	e.tsGuard.Store(tsGuardRefuse)
	return e
}

// minusOperands is the operands of a compiled `-` the guard can refuse (the
// boxed BinOp and the mode-resolved BinOpNumeric), nil otherwise.
func minusOperands(e Expr) (Expr, Expr) {
	switch v := e.(type) {
	case *BinOp:
		if v.Op == "-" {
			return v.Left, v.Right
		}
	case *BinOpNumeric:
		if v.Op == "-" {
			return v.Left, v.Right
		}
	}
	return nil, nil
}

// tsDiffNull answers IS NULL over a timestamp difference from its operands,
// which is PostgreSQL's answer (timestamp_mi is strict and, for finite
// operands, never NULL), without computing the difference the guard
// refuses. An infinite operand keeps the 22008 the difference raised before
// the guard (infiniteDateArith). ok=false when the minus is no timestamp
// difference: the caller evaluates it as before. Decided once per node.
func (e *IsNull) tsDiffNull(b *batch.RecordBatch, row int) (null, ok bool) {
	switch e.diffState.Load() {
	case tsGuardClear:
		return false, false
	case tsGuardUnresolved:
		if !isTimestampDifferenceOperands(e.diffL, e.diffR, b) && !timestampMinusQuoted(e.diffL, e.diffR, b) {
			if b != nil {
				e.diffState.Store(tsGuardClear)
			}
			return false, false
		}
		if b != nil {
			e.diffState.Store(tsGuardRefuse)
		}
	}
	lv := e.diffL.Eval(b, row)
	rv := e.diffR.Eval(b, row)
	if lv == nil || rv == nil {
		return true, true
	}
	// `ts - '<instant>'`: the literal is read as the difference read it,
	// so text that names no instant keeps its 22007.
	for _, k := range []Expr{e.diffL, e.diffR} {
		if s, quoted := unknownLiteralText(k); quoted {
			castTemporalText(s, castToTimestampKind)
			return false, true
		}
	}
	ld, lok := temporalOperand(b, row, e.diffL, lv)
	rd, rok := temporalOperand(b, row, e.diffR, rv)
	shell := &BinOp{Op: "-"}
	shell.infiniteDateArith(ld, lok, rd, rok, lv, rv)
	return false, true
}
