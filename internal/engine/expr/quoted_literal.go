package expr

import (
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// This file is the expression layer's half of the one literal-vs-numeric-column
// rule (#646; kernel/numeric_literal.go carries the grammar and the reasoning).
//
// PostgreSQL types an unknown-typed literal FROM the column it meets and
// coerces it with that type's own input function, so a quoted literal that
// names no value of the column's type is a query ERROR — at the plan-time
// refusal, at the vectorized kernel, and at the BOXED sites this file serves:
// a simple CASE's WHEN, IS [NOT] DISTINCT FROM, GREATEST/LEAST and NULLIF.
//
// Those sites answered instead. `refuseArm` and `extremumRefusal` tested
// `kernel.DecimalLiteral.Numeric()` and the column's type against
// batch.TypeDecimal alone, so `NULLIF(int_col, 'abc')`, `int_col IS DISTINCT
// FROM 'NaN'`, `CASE WHEN int_col < 'NaN'` and `GREATEST(int_col, 'NaN')`
// returned every row where PostgreSQL raises 22P02 — #463's failure mode at
// the sites #465 reached but #517 did not.

// numericLitTypes is every column type kernel.QuotedLitStatus answers for,
// paired with the bit position the refusal mask gives it. numericLitSlot is
// the inverse and is a switch rather than a search: it runs per (best,
// candidate) pair inside GREATEST/LEAST, which cannot cache a settled flag at
// all (see litRefusalMask).
var numericLitTypes = [...]batch.TypeID{
	batch.TypeInt32, batch.TypeInt64,
	batch.TypeFloat32, batch.TypeFloat64,
	batch.TypeDecimal,
	batch.TypePort, batch.TypeProtocol, batch.TypeDuration,
}

func numericLitSlot(typ batch.TypeID) int {
	switch typ {
	case batch.TypeInt32:
		return 0
	case batch.TypeInt64:
		return 1
	case batch.TypeFloat32:
		return 2
	case batch.TypeFloat64:
		return 3
	case batch.TypeDecimal:
		return 4
	case batch.TypePort:
		return 5
	case batch.TypeProtocol:
		return 6
	case batch.TypeDuration:
		return 7
	}
	return -1
}

// litRefusalMask records, for one literal's TEXT, which column types refuse it
// — two bits per type, one saying "refused" and one saying the refusal is a
// RANGE failure rather than a SYNTAX one, so the site that raises can name
// PostgreSQL's own SQLSTATE without parsing the text again.
//
// It is precomputed once per NODE, at arming time, and that is the point. The
// question "does this text name a value of type T?" is fixed for the query;
// the question "what type is this column?" is fixed once one batch has
// answered it. Asking either PER ROW is what the first #505 fix did, and it
// cost +35% on a simple CASE over a DECIMAL column, +25% on IS DISTINCT FROM
// and +200% on an exponent-form literal — the regression
// decimal_order_bench_test.go exists to hold shut. GREATEST/LEAST cannot cache
// a settled flag at all (the best-so-far MOVES, so one column resolving says
// nothing about the next), which is why the answer has to be a mask lookup
// rather than a parse.
type litRefusalMask uint32

// quotedLitMask classifies text against every type in numericLitTypes. A zero
// mask means no column type refuses this text, which is the arming sites'
// "this pair can never raise" fast path.
func quotedLitMask(text string) litRefusalMask {
	var m litRefusalMask
	for i, typ := range numericLitTypes {
		st, ok := kernel.QuotedLitStatus(typ, text)
		if !ok || st == kernel.NumConstOK {
			continue
		}
		m |= 1 << uint(2*i)
		if st == kernel.NumConstRange {
			m |= 1 << uint(2*i+1)
		}
	}
	return m
}

// refuses reports whether typ refuses this text, and with which status.
func (m litRefusalMask) refuses(typ batch.TypeID) (kernel.NumConstStatus, bool) {
	i := numericLitSlot(typ)
	if i < 0 || m&(1<<uint(2*i)) == 0 {
		return kernel.NumConstOK, false
	}
	if m&(1<<uint(2*i+1)) != 0 {
		return kernel.NumConstRange, true
	}
	return kernel.NumConstSyntax, true
}

// quotedLitText is a QUOTED string literal's contents, and ok=false for every
// other operand — a numeric literal (which carries Lit.Text), a column, an
// expression. It is the shape the refusal arms on: PostgreSQL refuses an
// unknown-typed literal its column's type cannot read, and an unquoted numeric
// constant is not unknown-typed (`real = 1e40` is a double comparison that
// answers no rows, not a 22003).
//
// The empty string IS a legitimate value here — `f = ”` is PostgreSQL's 22P02
// — so callers must use the MASK, never the text, as the "nothing to refuse"
// sentinel.
func quotedLitText(e Expr) (string, bool) {
	lit, ok := e.(*Lit)
	if !ok || lit.Text != "" {
		return "", false
	}
	s, ok := lit.Val.(string)
	return s, ok
}

// RefuseNumericLiteral is the ONE refusal, as an error rather than a panic, so
// the plan-time binder (physical.refuseLiteralForType) and the row-at-a-time
// evaluators raise the identical SQLSTATE and the identical message for the
// identical query. nil means the type accepts the text — or has no rule.
//
// The two SQLSTATEs are PostgreSQL's and they are different answers: 22P02
// (invalid_text_representation) for text that names no value of the type,
// 22003 (numeric_value_out_of_range) for a number the type cannot carry.
// `real = '1e400'` is the second, not the first, and the WireProtocol oracle
// checks which one the wire says.
func RefuseNumericLiteral(typ batch.TypeID, text string) error {
	st, ok := kernel.QuotedLitStatus(typ, text)
	if !ok || st == kernel.NumConstOK {
		return nil
	}
	return numericLitError(typ, text, st)
}

func numericLitError(typ batch.TypeID, text string, st kernel.NumConstStatus) error {
	name, ok := kernel.NumericTypeName(typ)
	if !ok {
		return nil
	}
	if st == kernel.NumConstRange {
		return &NumericRangeError{Input: text, DestType: name}
	}
	return &InvalidLiteralError{Input: text, DestType: name}
}

// raiseQuotedLitRefusal aborts the query for a quoted literal the column type
// refuses, through the per-row error channel (fatalEval). It is
// raiseInvalidTextRepresentation parameterized by the type, and for DECIMAL it
// produces the byte-identical message that site already produced.
func raiseQuotedLitRefusal(typ batch.TypeID, text string, st kernel.NumConstStatus) {
	if err := numericLitError(typ, text, st); err != nil {
		panic(fatalEval{err})
	}
}

// quotedNumberOrder orders a NUMBER against a QUOTED literal under the rule
// the NUMBER's own type selects — PostgreSQL coerces the unknown-typed literal
// to that type and compares there, with no widening (#646).
//
// The TYPE decides, never the box, and here that is load-bearing rather than
// stylistic: ColRef.Eval WIDENS on the way out — a FLOAT32 column boxes as
// float64 and an INT32 one as int64 — so a box-driven rule would compare
// `r < '3.1'` at double width, which is a different predicate for every
// literal a real cannot represent, and would skip int4's range check. The
// widening is exact and order-preserving, so narrowing the box back inside the
// real arm recovers the stored value bit for bit (the same argument
// realLitSet.contains makes).
//
// typ comes from numberKindType: the boxed layer's DECLARATION where it read
// one, and the box otherwise (a numeric literal, a computed numeric
// expression), which is exact for those because none of the four shares a Go
// box with another once the KIND has separated DECIMAL and STRING out
// (ADR-0012 item 8).
//
// A literal the type refuses RAISES rather than falling through. Falling
// through is what made `CASE WHEN int_col < 'NaN'` answer every row: compare()
// finds no reading of an int64 against "NaN" and reports FALSE, which is a
// value answer to a question that has none.
func quotedNumberOrder(typ batch.TypeID, num any, text string) (int, bool) {
	switch typ {
	case batch.TypeFloat32:
		v, ok := numberAsFloat64(num)
		if !ok {
			return 0, false
		}
		f, st := kernel.FloatLitText(text, 32)
		if st != kernel.NumConstOK {
			raiseQuotedLitRefusal(typ, text, st)
		}
		return kernel.CompareFloat32(float32(v), float32(f)), true
	case batch.TypeFloat64:
		v, ok := numberAsFloat64(num)
		if !ok {
			return 0, false
		}
		f, st := kernel.FloatLitText(text, 64)
		if st != kernel.NumConstOK {
			raiseQuotedLitRefusal(typ, text, st)
		}
		return kernel.CompareFloat64(v, f), true
	case batch.TypeInt32, batch.TypeInt64, batch.TypePort, batch.TypeProtocol, batch.TypeDuration:
		v, ok := numberAsInt64(num)
		if !ok {
			return 0, false
		}
		return quotedIntOrder(typ, v, text)
	}
	return 0, false
}

// quotedIntOrder compares an integer column's value against a quoted literal,
// exactly. The int32 family range-checks the literal, because a value that
// would WRAP on narrowing is PostgreSQL's 22003 and not a comparison against
// the wrapped number (#536's rule, one site over).
func quotedIntOrder(typ batch.TypeID, v int64, text string) (int, bool) {
	n, st := kernel.IntLitText(text)
	if st == kernel.NumConstOK && int32Family(typ) && (n < math.MinInt32 || n > math.MaxInt32) {
		st = kernel.NumConstRange
	}
	if st != kernel.NumConstOK {
		raiseQuotedLitRefusal(typ, text, st)
	}
	switch {
	case v < n:
		return -1, true
	case v > n:
		return 1, true
	}
	return 0, true
}

func int32Family(typ batch.TypeID) bool {
	switch typ {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
		return true
	}
	return false
}

// numberAsFloat64 and numberAsInt64 read a boxed number in the domain its
// COLUMN declared, whatever width ColRef.Eval boxed it at. ok=false for a box
// that is not a number at all, which sends the caller back to compare().
func numberAsFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

func numberAsInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	}
	return 0, false
}

// refusesEveryNumericType reports whether this text is refused by EVERY column
// type that has a literal rule. It is the safe test where the operand types are
// only partly known: a literal no numeric type can read ('abc', ”) is refused
// whatever the unknown operand turns out to be, while one that only SOME types
// refuse ('3.1', 'NaN') is not.
func (m litRefusalMask) refusesEveryNumericType() bool {
	for i := range numericLitTypes {
		if m&(1<<uint(2*i)) == 0 {
			return false
		}
	}
	return true
}

// ResultIsDecimalText reports whether an expression's boxed result is a
// DECIMAL rendered as its TEXT, resolved from the expression's DECLARATIONS
// against this batch rather than from the box.
//
// It exists for the callers that must RE-SPELL a boxed value as SQL text — the
// coordinator's scalar-subquery substitution, which inlines the value into a
// filter expression the worker re-parses. A DECIMAL and a STRING both arrive
// as a Go string, and quoting a DECIMAL there makes it look like a literal a
// user wrote, which the numeric column it meets then reads with its OWN input
// function (ADR-0012 item 13): `HAVING COUNT(*) > (SELECT COUNT(*) * 0.3 …)`
// substituted `'0.0'` and asked bigint to read it, a 22P02 for a query
// PostgreSQL answers. This is item 8's rule — the declaration, never the box —
// at the one boundary that turns a value back into text.
func ResultIsDecimalText(e Expr, b *batch.RecordBatch) bool {
	k, _ := classifyOperand(e, b)
	return k == boxDecimal
}
