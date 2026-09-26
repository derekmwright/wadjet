// SPDX-License-Identifier: MIT

// This file holds the ONE answer to "which temporal type does this expression
// produce"; ADR-0012 and ADR-0024 govern the execution contracts.
package expr

import (
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A temporal value has ONE box on the row path, whatever produced it: a DATE
// is int64 epoch DAYS and a TIMESTAMP is int64 epoch MILLISECONDS — the boxes
// ColRef.Eval gives a DATE / TIMESTAMP column and Cast gives a temporal cast
// (#340). A bare int64 has lost its unit, so every consumer that must read one
// as an instant (the temporal-input functions, the text renderers, date
// arithmetic, the boxed comparison) recovers the unit from the PRODUCER — and
// producedTemporal is the one place that says which producers carry which
// unit. Before it existed each consumer recognised a CAST and a COLUMN and
// nothing else, so the clock functions boxed formatted TEXT to stay readable
// (a DATE-declared `current_date` whose value was the string "2026-09-24"),
// and date arithmetic's own DATE result was a unit-less number to its parent:
// `CURRENT_DATE + 1 - 1 - CURRENT_DATE` answered 18694, because the outer
// minus read the right operand's text as the number 2026, and
// `year(DATE '2026-01-01' + 1)` answered 1970 (arc VL round 3).
//
// The answer is the producer's DECLARATION: a registry function's fixed
// return type, a cast's destination, a column's type, and for the composite
// producers the rule over their operands' answers. It never reads the box.

// producedTemporal reports which temporal type e produces on the row path —
// castToDateKind (int64 epoch days), castToTimestampKind (int64 epoch
// milliseconds) — or castNotTemporal. b resolves column references; nil is
// allowed and answers castNotTemporal for them.
func producedTemporal(e Expr, b *batch.RecordBatch) castTemporalKindT {
	switch v := e.(type) {
	case *Cast:
		return castTemporalKind(v.DestType)
	case *ColRef:
		if b != nil {
			v.resolve(b)
		}
		switch v.valueType() {
		case batch.TypeDate:
			return castToDateKind
		case batch.TypeTimestamp:
			return castToTimestampKind
		}
		return castNotTemporal
	case *FuncCall:
		return funcProducedTemporal(v, b)
	case *BinOp:
		return arithProducedTemporal(v.Op, v.Left, v.Right, b)
	case *BinOpNumeric:
		return arithProducedTemporal(v.Op, v.Left, v.Right, b)
	case *Case:
		arms := make([]Expr, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return commonProducedTemporal(arms, b)
	case *Coalesce:
		return commonProducedTemporal(v.Args, b)
	}
	return castNotTemporal
}

// funcProducedTemporal is producedTemporal for a function call: a fixed
// DATE / TIMESTAMP declaration names the unit outright, a function that
// CHOOSES one of its arguments (COALESCE, GREATEST, NULLIF, IF, …) produces
// what its candidate arguments agree on, and the temporal-shift functions
// follow shiftProducedTemporal.
func funcProducedTemporal(f *FuncCall, b *batch.RecordBatch) castTemporalKindT {
	lower := strings.ToLower(f.Name)
	if dateShiftFuncs[lower] {
		return shiftProducedTemporal(f.Args, b)
	}
	f.resolveFn()
	if f.choiceArms == nil && !f.extremum && f.nullifArms == nil {
		return f.fixedTemporal
	}
	ret := DefaultRegistry.ReturnType(f.Name)
	if idx, poly := ret.SameAsArgs(len(f.Args)); poly {
		arms := make([]Expr, 0, len(idx))
		for _, i := range idx {
			if i >= 0 && i < len(f.Args) {
				arms = append(arms, f.Args[i])
			}
		}
		return commonProducedTemporal(arms, b)
	}
	if d, c := ret.Resolve(0, nil); c == Decided {
		switch d.ID {
		case batch.TypeDate:
			return castToDateKind
		case batch.TypeTimestamp:
			return castToTimestampKind
		}
	}
	return castNotTemporal
}

// dateShiftFuncs are the registry's `date ± n` spellings. Their result type
// follows their FIRST argument the way the operator's does: a DATE shifted by
// a whole number of days is a DATE, and everything else — a TIMESTAMP, a text
// instant, any INTERVAL shift — is a TIMESTAMP (PostgreSQL's `date +
// interval` is timestamp; its preferred datetime type for an unknown-typed
// argument is timestamp too). physical.funcReturnType applies the same rule
// to the declarations, and the kernels (dateShift) box the matching unit.
var dateShiftFuncs = map[string]bool{"date_add": true, "date_sub": true}

func shiftProducedTemporal(args []Expr, b *batch.RecordBatch) castTemporalKindT {
	if len(args) < 1 {
		return castNotTemporal
	}
	if len(args) >= 2 && producesInterval(args[1]) {
		return castToTimestampKind
	}
	if producedTemporal(args[0], b) == castToDateKind {
		return castToDateKind
	}
	return castToTimestampKind
}

// arithProducedTemporal is the operator rule, the runtime mirror of
// physical.binOpTemporalType: `date ± integer` and `integer + date` are a
// DATE (a VARCHAR column reads as a date, textDayOperand); `date ± interval`,
// `timestamp ± interval`, a text instant ± interval and `timestamp + '…'` are
// a TIMESTAMP; `date - date` is an integer day count, and nothing else is
// temporal.
// "integer" is the operand's own integer-ness (operandIsInt), never the
// spelling of a literal.
func arithProducedTemporal(op string, left, right Expr, b *batch.RecordBatch) castTemporalKindT {
	if op != "+" && op != "-" {
		return castNotTemporal
	}
	lk := producedTemporal(left, b)
	rk := producedTemporal(right, b)
	switch {
	case (lk != castNotTemporal || textOperand(left, b)) && producesInterval(right):
		return castToTimestampKind
	case op == "+" && (rk != castNotTemporal || textOperand(right, b)) && producesInterval(left):
		return castToTimestampKind
	case op == "+" && ((lk == castToTimestampKind && isUnknownLit(right)) || (rk == castToTimestampKind && isUnknownLit(left))):
		// `ts + '…'`: the quoted operand resolves to an INTERVAL
		// (ResolveUnknownTemporal), so the sum is a TIMESTAMP.
		return castToTimestampKind
	case (lk == castToDateKind || textDayOperand(left, b)) && rk == castNotTemporal && operandIsInt(right, b):
		return castToDateKind
	case op == "+" && (rk == castToDateKind || textDayOperand(right, b)) && lk == castNotTemporal && operandIsInt(left, b):
		return castToDateKind
	}
	return castNotTemporal
}

// isUnknownLit reports a quoted (string) literal operand.
func isUnknownLit(e Expr) bool {
	_, ok := unknownLiteralText(e)
	return ok
}

// producesInterval reports whether an operand is an INTERVAL: a literal, or a
// cast to one.
func producesInterval(e Expr) bool {
	if isIntervalLit(e) {
		return true
	}
	c, ok := e.(*Cast)
	return ok && strings.EqualFold(strings.TrimSpace(c.DestType), "interval")
}

// textDayOperand is the one TEXT operand date arithmetic reads as a day: a
// column declared VARCHAR, which is how the TPC-H fixtures spell every date —
// the rule physical.nodeTemporalKind applies to the declaration, so the two
// layers agree that `l_shipdate + 1` over such a column is a DATE.
func textDayOperand(e Expr, b *batch.RecordBatch) bool {
	cr, ok := e.(*ColRef)
	if !ok || b == nil {
		return false
	}
	cr.resolve(b)
	return cr.idx >= 0 && cr.valueType() == batch.TypeString
}

// textOperand is a text operand an INTERVAL shift reads as an instant: a text
// column, or a quoted literal (SQL's unknown, which PostgreSQL resolves to its
// preferred datetime type, timestamp).
func textOperand(e Expr, b *batch.RecordBatch) bool {
	if l, ok := e.(*Lit); ok {
		_, isText := l.Val.(string)
		return isText
	}
	return textDayOperand(e, b)
}

// commonProducedTemporal is the kind a choice among arms produces: the one
// kind every non-NULL arm agrees on. Arms that disagree — or that include a
// non-temporal value — answer castNotTemporal: no single unit reads them all.
func commonProducedTemporal(arms []Expr, b *batch.RecordBatch) castTemporalKindT {
	kind, have := castNotTemporal, false
	for _, a := range arms {
		if a == nil {
			continue
		}
		if lit, ok := a.(*Lit); ok {
			if _, quoted := lit.Val.(string); quoted || lit.Val == nil {
				// NULL, and a quoted literal — SQL's unknown, which takes the
				// type the other arms declare — constrain nothing.
				continue
			}
		}
		k := producedTemporal(a, b)
		if !have {
			kind, have = k, true
			continue
		}
		if k != kind {
			return castNotTemporal
		}
	}
	return kind
}

// temporalBoxInstant reads a temporal box in the unit its producer names: the
// civilDate / time.Time the instant consumers take. ok=false when e produces
// no temporal type or the box is not the int64 the unit is carried in.
func temporalBoxInstant(e Expr, b *batch.RecordBatch, v any) (any, castTemporalKindT, bool) {
	k := producedTemporal(e, b)
	if k == castNotTemporal {
		return nil, k, false
	}
	n, isInt := v.(int64)
	if !isInt {
		return nil, k, false
	}
	if k == castToDateKind {
		return civilDate{t: epochDayInstant(n)}, k, true
	}
	return epochMilliInstant(n), k, true
}

// renderTemporalBox renders a temporal box as the text its type prints:
// batch.FormatDate for a DATE, batch.FormatTimestamp for a TIMESTAMP. ok=false
// leaves v to its caller.
func renderTemporalBox(e Expr, b *batch.RecordBatch, v any) (string, bool) {
	n, isInt := v.(int64)
	if !isInt {
		return "", false
	}
	switch producedTemporal(e, b) {
	case castToDateKind:
		return batch.FormatDate(int32(n)), true
	case castToTimestampKind:
		return batch.FormatTimestamp(n), true
	}
	return "", false
}

// epochDayInstant is UTC midnight of epoch day n — a DATE box's instant.
func epochDayInstant(n int64) time.Time { return time.Unix(n*86400, 0).UTC() }

// epochMilliInstant is a TIMESTAMP box's instant.
func epochMilliInstant(n int64) time.Time { return time.UnixMilli(n).UTC() }
