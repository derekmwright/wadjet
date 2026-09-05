package expr

import (
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
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

// RefuseNetworkPrefixLiteral is the OTHER way a network literal can fail to be
// a value its column can hold, and it is a different answer from a syntax
// error: `'10/8'` and `'::1/64'` are ordinary `inet` values on the server —
// NETWORKS — and an IPV4/IPV6 column holds a bare address with no room for a
// prefix. 0A000 (feature_not_supported), never 22P02, because the text is
// valid and the engine's TYPE is the limit.
//
// It is asked at PLAN time beside RefuseNumericLiteral and at runtime by
// exec.networkConstError, and both read kernel.NetworkPrefixLiteral, so one
// literal cannot be a network at one site and garbage at another. Before #627
// round 2 the 0A000 existed at ONE evaluator: the same query refused in a
// WHERE clause, answered inside a CASE, and on the DAG answered a WRONG NUMBER
// (the widened parser read the prefix as the address zero).
func RefuseNetworkPrefixLiteral(typ batch.TypeID, text string) error {
	if !kernel.NetworkPrefixLiteral(typ, text) {
		return nil
	}
	return sqlerr.New("0A000", "a network prefix is not representable in a %s column: %q "+
		"(PostgreSQL reads it as a network; use a CIDR column, or compare against the "+
		"address alone)", typ.String(), text)
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
	case batch.TypeDecimal:
		// The DECIMAL rung of the fold, which a COMPOSITE reaches: `GREATEST(
		// bigint, '3.1', numeric)` is a numeric comparison in PostgreSQL, and
		// the value that arrives can be either a DECIMAL's rendered TEXT or the
		// integer/float box of whichever arm won. Both are ordered against the
		// literal's exact digits — the same rule a bare DECIMAL column gets
		// (ADR-0012 item 6), reached through the same accept-set.
		st, _ := kernel.QuotedLitStatus(batch.TypeDecimal, text)
		if st != kernel.NumConstOK {
			raiseQuotedLitRefusal(batch.TypeDecimal, text, st)
		}
		// Both sides as TEXT, always. A number reaching this rung is an
		// INTEGER box (a float box would have made the fold a float rung), so
		// rendering it loses nothing, and batch.CompareDecimalTexts is the one
		// reader that also orders the literal's NaN and ±Infinity spellings by
		// PostgreSQL's rank (ADR-0024 item 6) — decimalTextOrder declines them,
		// which left `GREATEST(int, 'NaN', numeric)` with no reading at all.
		ls, ok := decimalOperandText(num)
		if !ok {
			return 0, false
		}
		return batch.CompareDecimalTexts(ls, text)
	case batch.TypeFloat32:
		// The LITERAL is read FIRST, before the box is looked at. Reading the
		// box first made the refusal depend on the DATA: `GREATEST(c_dec,
		// c_f64) = 'abc'` raised 22P02 on a row where the float arm won and
		// answered FALSE on a row where the decimal arm won, because only the
		// first had a box this could read. PostgreSQL coerces the literal at
		// parse analysis and raises for every row (#517's rule, which
		// ADR-0012 item 13 records as closed).
		f, st := kernel.FloatLitText(text, 32)
		if st != kernel.NumConstOK {
			raiseQuotedLitRefusal(typ, text, st)
		}
		v, ok := numberAsFloat64(num)
		if !ok {
			return 0, false
		}
		return kernel.CompareFloat32(float32(v), float32(f)), true
	case batch.TypeFloat64:
		f, st := kernel.FloatLitText(text, 64)
		if st != kernel.NumConstOK {
			raiseQuotedLitRefusal(typ, text, st)
		}
		v, ok := numberAsFloat64(num)
		if !ok {
			return 0, false
		}
		return kernel.CompareFloat64(v, f), true
	case batch.TypeInt32, batch.TypeInt64, batch.TypePort, batch.TypeProtocol, batch.TypeDuration:
		if _, st := kernel.IntLitText(text); st != kernel.NumConstOK {
			raiseQuotedLitRefusal(typ, text, st)
		}
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
//
// A DECIMAL's rendered TEXT counts as a number here, and it has to. A
// composite mixing a DECIMAL column with a FLOAT one folds to the FLOAT rung —
// PostgreSQL's `numeric` ∪ `float8` is float8 — but the VALUE that arrives on
// the rows where the decimal arm won is still that decimal's text. Declining
// it sent `COALESCE(c_dec, c_f64) > '9'` to compare()'s BYTE order, which
// answered 134 of 2491 rows: a byte ordering of a rendered decimal, which is
// #504's defect one composite up. Reading it as a float64 IS PostgreSQL's own
// numeric→float8 conversion for that fold.
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
	case string:
		return decimalTextAsFloat64(n)
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

// decimalOperandText renders an operand at the DECIMAL rung as the text
// batch.CompareDecimalTexts reads. A DECIMAL already IS its text; an integer
// renders exactly; a float renders through its shortest round-trip spelling,
// with the three specials spelled the way that reader recognises them.
func decimalOperandText(v any) (string, bool) {
	switch n := v.(type) {
	case string:
		return n, true
	case int64:
		return strconv.FormatInt(n, 10), true
	case int32:
		return strconv.FormatInt(int64(n), 10), true
	case int:
		return strconv.FormatInt(int64(n), 10), true
	case float64:
		return floatOperandText(n, 64)
	case float32:
		return floatOperandText(float64(n), 32)
	}
	return "", false
}

func floatOperandText(f float64, bits int) (string, bool) {
	switch {
	case f != f:
		return "NaN", true
	case math.IsInf(f, 1):
		return "Infinity", true
	case math.IsInf(f, -1):
		return "-Infinity", true
	}
	return strconv.FormatFloat(f, 'f', -1, bits), true
}

// numericBoxRepair orders a pair whose KINDS say numeric but whose BOXES no arm
// above could read together — in practice a DECIMAL's rendered TEXT meeting a
// Go number, which is what a composite mixing a DECIMAL column with a FLOAT or
// an INTEGER one produces on the rows where the decimal arm won.
//
// It exists because the alternative is compare()'s BYTE order over a rendered
// decimal: `COALESCE(dec, float) > '9'` answered 134 of 2491 rows that way, and
// the unquoted `> 9` spelling answered the same. The rule it applies is
// decimalTextOrder's, which is PostgreSQL's — exact against an integer, float64
// against a float, because `numeric <op> double precision` casts the numeric.
//
// ok=false leaves every pair it does not recognise exactly where it was: a
// genuine STRING column keeps #504's text rule, and an operand with no
// declaration keeps compare()'s own judgement.
func numericBoxRepair(lk, rk boxKind, lv, rv any) (int, bool) {
	if !numericPairKinds(lk, rk) {
		return 0, false
	}
	ls, lIsStr := lv.(string)
	rs, rIsStr := rv.(string)
	switch {
	case lIsStr && rIsStr:
		return batch.CompareDecimalTexts(ls, rs)
	case lIsStr:
		c, ok := decimalTextOrder(rv, ls)
		return -c, ok
	case rIsStr:
		return decimalTextOrder(lv, rs)
	}
	// Two Go numbers: compare at the wider domain, which is what every rung
	// above already did before one of them arrived as text.
	lf, lok := numberAsFloat64(lv)
	rf, rok := numberAsFloat64(rv)
	if !lok || !rok {
		return 0, false
	}
	return kernel.CompareFloat64(lf, rf), true
}

// numericPairKinds reports whether both operands' kinds say "a number", which
// is what makes a byte ordering of them wrong rather than merely unusual. A
// boxText operand is excluded by name: a genuine STRING column compares AS
// TEXT whatever its digits look like (ADR-0012 item 5).
func numericPairKinds(lk, rk boxKind) bool {
	ok := func(k boxKind) bool { return isNumericFoldKind(k) || k == boxQuoted }
	return ok(lk) && ok(rk)
}

// decimalTextAsFloat64 converts a DECIMAL's rendered text to the float64 the
// fold's rung compares at. It uses the DECIMAL grammar as the gate — the same
// accept-set every other DECIMAL site reads — so a genuine STRING column's
// value that is not a number still declines and keeps #504's text rule.
func decimalTextAsFloat64(s string) (float64, bool) {
	if !kernel.NewDecimalLiteral(s).Numeric() {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, false
	}
	return f, true
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
