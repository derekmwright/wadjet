package expr

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// This file is the declaration-driven half of the boxed comparison path.
//
// ADR-0012 item 8's rule is that a boxed value's comparison order follows the
// COLUMN'S DECLARATION, not the Go type its box happens to be. Two shapes make
// that rule impossible to follow from the box alone, because both arrive as a
// plain Go `string`:
//
//   - a DECIMAL column, which Vector.GetValue renders as its decimal TEXT, and
//   - a STRING column, whose value may look exactly like that text.
//
// `expr.decimalColCmp` already answers one of those pairs from the two
// columns' declarations — but it is bound in `NewCmp` alone, so the SAME two
// DECIMAL columns at a BOXED site (a simple `CASE d1 WHEN d2`, `d1 IS DISTINCT
// FROM d2`, `GREATEST(d1, d2)`) fell through `compare()`'s two-rendered-strings
// path and compared LEXICOGRAPHICALLY, where "10.001" sorts below "2.0002"
// (#506).
//
// The other direction was worse. `compare()` used to tell the two apart by
// SNIFFING: any string operand that PARSED as a number was read numerically
// against the other side. That made a genuine STRING column compare
// NUMERICALLY on the row path — `WHERE s = 1.5` found the row holding "1.50" —
// while the vectorized kernel compared the same predicate as text, so one
// query had two answers depending on which path it took (#504). The sniff is
// gone: a STRING column is classified from its declaration and compares as
// text on both paths.
//
// A boxedPair is armed from the operand EXPRESSIONS at construction, resolves
// each operand's DECLARED kind on the first batch that can answer it, and then
// applies one rule per kind pair. Nothing here reads a value's box to decide
// which RULE applies — only to decide whether the rule's text arm or its
// numeric arm is the one holding this row's value.

// boxKind is what an operand's declaration says its values are, independent of
// the Go box a particular row produces.
type boxKind int32

const (
	// boxUnknown: no declaration-driven reading applies. The pair falls
	// through to compare(), which is what it always did.
	boxUnknown boxKind = iota
	// boxDecimal: values from here are DECIMALs. A DECIMAL boxes as its
	// rendered text, so a string box from this operand is decimal text — the
	// distinction the sniff could not make.
	boxDecimal
	// boxNumber: a non-DECIMAL number whose DECLARED type this layer could not
	// read — a numeric literal, or a computed numeric expression. It never
	// boxes as a string, so its box alone identifies its width where that
	// matters.
	boxNumber
	// boxText: a genuine text value, which compares AS TEXT — bytewise,
	// wadjet's collation (ADR-0012 item 5) — whatever its digits look like.
	boxText
	// boxQuoted: a QUOTED string literal, which is a different thing from a
	// text value. PostgreSQL types such a literal as `unknown` and resolves
	// it FROM THE OTHER OPERAND, so `k > '2'` over a BIGINT column is an
	// integer comparison and `d = '12.75'` over a DECIMAL one is an exact
	// numeric comparison — while `s > '2'` over a text column is the text
	// comparison it looks like. Collapsing this into boxText is what dropped
	// PostgreSQL's rule for the first two (#504 review, B1).
	boxQuoted
	// boxCidr: values from here are CIDR. A CIDR column boxes as its stored
	// TEXT, which is indistinguishable in the box from a STRING column's —
	// the same problem boxDecimal has, one type family over. Its order is
	// PostgreSQL's inet (ADR-0012 item 10), and the stored text's byte order
	// is not that: "9.0.0.0/8" sorts ABOVE "10.0.0.0/24" as text and BELOW it
	// as an address, and a bare address and its own /32 host route are one
	// value the text calls two (#565).
	boxCidr
	// boxIPv6: values from here are IPv6. The column STORES the address's raw
	// 16 bytes, which the vectorized kernel compares directly, but
	// ColRef.Eval boxes the RENDERED text — and "2001:db8::9" sorts above
	// "2001:db8::10" as text and below it as an address, so the two sites
	// answered `a < z` opposite ways (#565).
	boxIPv6
	// boxBool: values from here are BOOL. A BOOL column against a QUOTED
	// literal reads the literal through PostgreSQL's boolean input grammar
	// (parse_bool: t/f/true/false/yes/no/y/n/on/off/1/0 and word prefixes),
	// because the unknown-typed literal is typed FROM the column — the same
	// rule boxNumber and the network kinds get for a quoted literal, one type
	// over (#574). The KIND is what keeps this apart from a TEXT column
	// against a BOOL literal (`s = TRUE`): that pair is boxText vs a
	// boxUnknown bool literal, which falls through to compare()'s toString
	// rendering, PostgreSQL-as-a-superset's existing answer. Reading the box
	// could not tell the two apart — both produce a Go bool and a Go string —
	// which is why the declaration decides, ADR-0012 item 8.
	boxBool
	// boxInt32/boxInt64/boxFloat32/boxFloat64: a non-DECIMAL number whose
	// DECLARED type IS known. They are separate kinds rather than one boxNumber
	// because the rule for a QUOTED literal meeting them is the COLUMN'S OWN
	// INPUT FUNCTION, and the four differ in what they accept and at what width
	// they compare (#646): '3.1' is a bigint's 22P02 and a real's 3.1; '1_000'
	// is a bigint's 1000 and a real's 22P02; '0x1p3' is a real's 8 and a
	// bigint's 22P02; and `r < '3.1'` compares at REAL width where `d < '3.1'`
	// compares at double.
	boxInt32
	boxInt64
	boxFloat32
	boxFloat64
	// boxTimestamp/boxDate: values from here are TEMPORAL, and the two are
	// separate kinds because they are separate DOMAINS — a TIMESTAMP is
	// epoch MILLISECONDS and a DATE is epoch DAYS. compare()'s fallback
	// picks the domain from the MAGNITUDE of the numeric side
	// (parseTemporalInt64OK: |ref| < 500_000 means days), which is a guess
	// that is wrong for every TIMESTAMP inside +/-500,000 ms of the epoch —
	// 1969-12-31T23:51:40Z through 1970-01-01T00:08:20Z, the sentinel band
	// log data is full of. `WHERE ts = '1970-01-01T00:00:01'` answered 1
	// through the pre-parsed CmpTemporalLit path and `WHERE ts = s`, with
	// the identical text in a STRING column, answered 1 where 2 rows match:
	// one predicate, two answers, decided by spelling (#826). The symmetric
	// hazard is a DATE beyond 500,000 epoch days, read as milliseconds.
	//
	// The domain is a property of the operand's DECLARATION and of nothing
	// else, which is what ADR-0012 item 8 says such a reading must come
	// from.
	boxTimestamp
	boxDate
)

// castNumericKind is the kind a CAST to a non-DECIMAL numeric type produces.
// boxUnknown for every other destination, including the ones whose spelling
// this layer does not recognise — a wrong kind is worse than no kind, which is
// the whole premise of ADR-0012 item 8.
func castNumericKind(c *Cast) boxKind {
	switch strings.ToLower(strings.TrimSpace(c.DestType)) {
	case "real", "float4":
		return boxFloat32
	case "double precision", "float8", "double", "float":
		return boxFloat64
	case "bigint", "int8":
		return boxInt64
	case "int", "integer", "int4", "smallint", "int2":
		return boxInt32
	}
	return boxUnknown
}

// isNumberKind reports whether a kind is a non-DECIMAL number — the four
// declared ones plus the undeclared boxNumber. Every rule that applied to
// boxNumber applies to all five; only the QUOTED-literal rule below cares
// which.
func isNumberKind(k boxKind) bool {
	switch k {
	case boxNumber, boxInt32, boxInt64, boxFloat32, boxFloat64:
		return true
	}
	return false
}

// numberKindType is a number-kind operand's effective type, and ok=false for
// boxNumber — the kind that says "a number whose DECLARED type this layer
// could not read".
//
// It used to fall back to the ROW'S BOX there, and that was the bug the #646
// review found. A box is a property of one row: `COALESCE(r_val, 0)` boxes an
// int64 on the row where r_val is NULL and a float64 everywhere else, so the
// literal beside it was coerced with the INTEGER input function on one row and
// the double one on the next — `COALESCE(r_val,0) = '3.1'` raised 22P02
// "bigint" when the NULL row was present and answered 0 rows when it was not.
// PostgreSQL resolves COALESCE's type ONCE (real here) and answers 1 row.
//
// So a kind with no declaration answers "no rule" and the caller falls through
// to compare(), which is the conservative side: a missed refusal, never a
// refusal at a width nothing resolved. joinOperandKinds now folds a composite's
// arms into a CONCRETE kind through the same ladder PostgreSQL uses, so the
// composites that matter reach here with a real type rather than boxNumber.
func numberKindType(k boxKind) (batch.TypeID, bool) {
	switch k {
	case boxInt32:
		return batch.TypeInt32, true
	case boxInt64:
		return batch.TypeInt64, true
	case boxFloat32:
		return batch.TypeFloat32, true
	case boxFloat64:
		return batch.TypeFloat64, true
	case boxDecimal:
		// DECIMAL is a rung of the fold like any other. Leaving it out was one
		// bug wearing two faces: `COALESCE(r_val, 0.0)` folded real with a
		// NUMERIC constant, found no rung, fell all the way to compare() and
		// ordered the rendered number against the literal BYTEWISE (`> '9'`
		// answered 2 of 9 rows); and `GREATEST(c_i64, '3.1', c_dec)` could not
		// fold bigint with numeric, so the pair kept bigint's own type and
		// refused a literal PostgreSQL reads as numeric.
		return batch.TypeDecimal, true
	}
	return 0, false
}

// numberKindOf is numberKindType's inverse: the kind that carries a declared
// numeric type, and boxNumber for a type this layer has no kind for.
func numberKindOf(t batch.TypeID) boxKind {
	switch t {
	case batch.TypeInt32:
		return boxInt32
	case batch.TypeInt64:
		return boxInt64
	case batch.TypeFloat32:
		return boxFloat32
	case batch.TypeFloat64:
		return boxFloat64
	case batch.TypeDecimal:
		return boxDecimal
	}
	return boxNumber
}

// isNumericFoldKind is isNumberKind plus DECIMAL: the kinds that have a rung
// on PostgreSQL's select_common_type ladder.
//
// It is a SECOND predicate rather than a widening of isNumberKind because the
// two answer different questions. isNumberKind asks "does a value from here
// arrive as a Go number", which decides whether the box can be read as one and
// which DECIMAL answers no to — it renders as text. This asks "does this kind
// have a place in the numeric fold", which DECIMAL answers yes to. Collapsing
// them would send a DECIMAL column's text box into the float arms.
func isNumericFoldKind(k boxKind) bool {
	return isNumberKind(k) || k == boxDecimal
}

// widerNumberKind folds two number kinds the way PostgreSQL's
// select_common_type does, through the one ladder widerNumericType states
// (INT32 < INT64 < DECIMAL < FLOAT32 < FLOAT64). ok=false when either side has
// no declared type, because a fold with an unknown operand is a LOWER BOUND on
// PostgreSQL's and a lower bound is what produces a refusal at the wrong
// width.
func widerNumberKind(a, b boxKind) (boxKind, bool) {
	ta, oka := numberKindType(a)
	tb, okb := numberKindType(b)
	if !oka || !okb {
		return boxNumber, false
	}
	return numberKindOf(widerNumericType(ta, tb)), true
}

// numericFoldOf folds one operand into a running common kind, and reports
// ok=false when the pair has no rung — the caller must then decline rather
// than guess, because a fold that skipped an operand is a LOWER BOUND on
// PostgreSQL's.
func numericFoldOf(kind, k boxKind) (boxKind, bool) {
	if k == kind {
		return kind, true
	}
	if !isNumericFoldKind(k) || !isNumericFoldKind(kind) {
		return boxUnknown, false
	}
	return widerNumberKind(k, kind)
}

// classifyOperandFold is classifyOperand's TYPE answer: for a composite that
// CHOOSES between arms it is PostgreSQL's select_common_type fold over them
// (`numeric ∪ float8` is float8), and for every other operand it is simply the
// kind. The two differ only where a DECIMAL arm meets a wider one — see
// boxOperand.fold for why both answers are needed at once.
func classifyOperandFold(e Expr, b *batch.RecordBatch) (boxKind, bool) {
	switch v := e.(type) {
	case *FuncCall:
		switch strings.ToLower(v.Name) {
		case "greatest", "least":
			return joinFoldKinds(v.Args, b)
		case "nullif":
			// NULLIF's VALUE is argument 0's and nothing else — its argument
			// 1 is compared and discarded — so the fold that types it is over
			// that one argument. Leaving it out was #761: the DECLARATION
			// folded `COALESCE(NULLIF(numeric, '…'), real)` to real, which is
			// PostgreSQL's answer, and this function classified the inner call
			// boxUnknown, so the outer choice found no fold, produced no box
			// mode, and handed the NULLIF's DECIMAL text to a FLOAT32 vector.
			// physical.argDeclaredType has had the same arm since #646; the
			// two recursion lists disagreeing is the whole defect.
			if len(v.Args) > 0 {
				return joinFoldKinds(v.Args[:1], b)
			}
		}
	case *Case:
		arms := make([]Expr, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return joinFoldKinds(arms, b)
	case *Coalesce:
		return joinFoldKinds(v.Args, b)
	}
	return classifyOperand(e, b)
}

// joinFoldKinds is joinOperandKinds with the pure ladder — no DECIMAL
// precedence — which is the TYPE PostgreSQL resolves for the call.
func joinFoldKinds(args []Expr, b *batch.RecordBatch) (boxKind, bool) {
	kind, have, settled := boxUnknown, false, true
	for _, a := range args {
		if a == nil {
			continue
		}
		if lit, ok := a.(*Lit); ok && lit.Val == nil {
			continue
		}
		k, s := classifyOperandFold(a, b)
		settled = settled && s
		if k == boxQuoted {
			continue
		}
		k = foldKind(a, k)
		if !have {
			kind, have = k, true
			continue
		}
		w, ok := numericFoldOf(kind, k)
		if !ok {
			return boxUnknown, settled
		}
		kind = w
	}
	if !have {
		return boxUnknown, true
	}
	return kind, settled
}

// classifyOperand reports an operand's declared kind and whether that answer
// is SETTLED — safe to cache for the rest of the query.
//
// Unsettled means "this batch cannot answer": a column name that resolves in
// no batch yet says nothing about the next one, so the caller must ask again.
// Every settled answer is a pure function of a declared type, which does not
// change across the batches of one query.
// declaredBoxKind maps a DECLARED column type to the kind that says how its
// box must be read. ok=false for every type whose box carries its own order
// already (see classifyOperand's fallthrough).
//
// It is shared by the column arm and the scalar-subquery arm so a value of one
// type cannot be classified two ways depending on where it came from — which
// is the whole premise of ADR-0012 item 8.
func declaredBoxKind(t batch.TypeID) (boxKind, bool) {
	switch t {
	case batch.TypeDecimal:
		return boxDecimal, true
	case batch.TypeInt32:
		return boxInt32, true
	case batch.TypeInt64:
		return boxInt64, true
	case batch.TypeFloat32:
		return boxFloat32, true
	case batch.TypeFloat64:
		return boxFloat64, true
	case batch.TypeString:
		return boxText, true
	case batch.TypeCIDR:
		return boxCidr, true
	case batch.TypeIPv6:
		return boxIPv6, true
	case batch.TypeBool:
		return boxBool, true
	case batch.TypeTimestamp:
		return boxTimestamp, true
	case batch.TypeDate:
		return boxDate, true
	}
	return boxUnknown, false
}

// isTemporalKind reports whether a kind names a temporal DOMAIN.
func isTemporalKind(k boxKind) bool { return k == boxTimestamp || k == boxDate }

func classifyOperand(e Expr, b *batch.RecordBatch) (boxKind, bool) {
	switch v := e.(type) {
	case *ScalarSubquery:
		// The subquery's DECLARED output type, resolved at compile time from
		// the plan (#696). Without it this operand was boxUnknown, so a
		// DECIMAL column on the other side compared as its RENDERED TEXT:
		// `a > (SELECT AVG(a) FROM decpair)` selected nothing because
		// "12.75" sorts below "7.570000" as bytes, where PostgreSQL selects
		// four rows. Equality survived only when the two scales happened to
		// render identically, which is why `= MAX` was right and every
		// ordering was not.
		//
		// A subquery nothing could type stays boxUnknown and SETTLED: the
		// answer does not depend on the batch, so re-asking on the next one
		// would only repeat it.
		if v.DeclKnown {
			if k, ok := declaredBoxKind(v.Decl); ok {
				return k, true
			}
		}
		return boxUnknown, true
	case *CorrelatedScalarSubquery:
		// Its declared TYPE does not change with the row it re-runs for, so
		// it is classified exactly as the uncorrelated form is (#666).
		if v.DeclKnown {
			if k, ok := declaredBoxKind(v.Decl); ok {
				return k, true
			}
		}
		return boxUnknown, true
	case *ColRef:
		v.resolve(b)
		if v.idx < 0 || v.idx >= len(b.Columns) {
			return boxUnknown, false
		}
		declared := v.typ
		if v.structField != "" {
			// A ROW field access reads a boxed value out of a container, and
			// the CONTAINER's declaration says nothing about the field. Its
			// own does: the field's declared type is resolved alongside it
			// (ColRef.fieldTyp), so a DECIMAL field compares as a decimal
			// and a STRING field as text, the same rules a column of that
			// type gets. Before #568 this arm answered boxUnknown outright
			// and every field path fell through to compare()'s guess.
			declared = v.fieldTyp
		}
		if k, ok := declaredBoxKind(declared); ok {
			return k, true
		}
		{
			// Every other type either boxes as a number of its own (PORT,
			// DURATION, and IPv4/MAC, whose box is the raw encoded int64 that
			// already sorts as the address does), as a formatted string whose
			// own order its text happens to give (DATE, UUID — UUID by the
			// fixed-width-hex accident ADR-0012 item 10 records), or as a
			// container. None of them is a DECIMAL-vs-number pair, and none
			// of them wants the text rule applied to a numeric literal, so
			// they keep compare()'s behaviour exactly.
			return boxUnknown, true
		}
	case *Lit:
		// Text is set only for a NUMERIC literal (compileLit), so it is both
		// the exact carrier and the test for "the user wrote a number here".
		if v.Text != "" {
			return boxNumber, true
		}
		switch v.Val.(type) {
		case string:
			return boxQuoted, true
		case int64, int32, int, float64, float32:
			return boxNumber, true
		}
		return boxUnknown, true
	case *FuncCall:
		// GREATEST/LEAST answer with ONE OF THEIR ARGUMENTS, so their kind is
		// the join of the arguments' kinds. NULLIF answers with argument 0 or
		// with NULL — argument 1 is compared and discarded — so its kind is
		// that one argument's. No other function is transparent this way; the
		// rest declare their own result type.
		//
		// NULLIF's arm is the KIND half of #761 and it is not optional beside
		// the TYPE half in classifyOperandFold. The type half alone makes the
		// outer choice produce a float box, so the query stops failing at the
		// store — and `GREATEST(NULLIF(numeric, '…'), real)` then answers
		// -3.50 where PostgreSQL answers -0.5, because the PAIR is still
		// unclassifiable and compare() orders two rendered numbers by BYTES
		// ("-3.50" sorts below "-0.5"). Trading a loud refusal for a silent
		// wrong value is the regression in kind the correctness protocol's
		// method 8 is about, so both arms land together.
		switch strings.ToLower(v.Name) {
		case "greatest", "least":
			return joinOperandKinds(v.Args, b)
		case "nullif":
			if len(v.Args) > 0 {
				return joinOperandKinds(v.Args[:1], b)
			}
		}
		return boxUnknown, true
	case *elementAtExpr:
		// element_at lifts a value OUT of a container, so its kind is the
		// container's ELEMENT kind — exactly the reason a ROW field path is
		// classified from the field above. The element type is on the vector
		// (Vector.Child), which is the only place it exists at this point,
		// so a DECIMAL element compares as a decimal instead of falling
		// through to compare()'s byte order: without it
		// `GREATEST(element_at(arr, 1), '2')` picked "2" over 12.75, because
		// "2" sorts above "12.75" as text.
		//
		// The COMPILED node, not the *FuncCall: element_at compiles to this
		// type (#607), so an arm keyed on the function call is dead code.
		return elementOperandKind(v.arg0, b)
	case *Case:
		// A CASE answers with one of its RESULTS (the operand and the WHEN
		// conditions only steer), so those are what decide its kind.
		arms := make([]Expr, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return joinOperandKinds(arms, b)
	case *Coalesce:
		return joinOperandKinds(v.Args, b)
	case *BinOpNumeric:
		// Exact fixed-point arithmetic boxes as its rendered TEXT, the same
		// as the DECIMAL column it computes over (ADR-0024 item 3, #555). So
		// the kind is boxDecimal exactly when the node resolved that mode:
		// without this, `WHERE d * 2 > 10` compared "25.50" against 10 by
		// BYTES, which is #504's failure one node up. Int and float modes
		// produce a real Go number and answer boxNumber, which is what they
		// answered before as an unclassified operand.
		v.resolveMode(b)
		if v.isDec {
			return boxDecimal, true
		}
		return boxNumber, true
	case *UnaryOp:
		// Unary ± answers its operand's kind: -d is a DECIMAL and boxes as
		// decimal text, for the same reason.
		if v.Op == "-" || v.Op == "+" {
			return classifyOperand(v.Operand, b)
		}
		return boxUnknown, true
	case *decimalScalarFn:
		// abs/ceil/floor/round/trunc/sign/mod over a DECIMAL answer a DECIMAL
		// and box as its text (#668). Over anything else they delegate to the
		// float FuncCall, whose box is a real number.
		if v.resolve(b) {
			return boxDecimal, true
		}
		return boxUnknown, true
	case *Cast:
		// A cast that NAMES a (p,s) produces a DECIMAL and boxes as its text,
		// so `WHERE CAST(x AS DECIMAL(10,2)) > 10` compares numerically
		// rather than by the bytes of "12.75".
		if castIsExactDecimal(v) {
			return boxDecimal, true
		}
		// A cast to any other NUMERIC type is a typed operand too, and saying
		// otherwise cost a fold: `COALESCE(numeric_col, CAST(k AS DOUBLE
		// PRECISION))` could not resolve its second arm, so the composite fell
		// back to the DECIMAL rung and read the literal with the numeric
		// grammar — 22P02 for `= '0x1.9p3'`, which PostgreSQL's float input
		// reads as 12.5.
		if k := castNumericKind(v); k != boxUnknown {
			return k, true
		}
		return boxUnknown, true
	}
	return boxUnknown, true
}

// elementOperandKind classifies element_at's result from its CONTAINER
// argument's element vector. Only a bare column argument answers: anything
// else has no vector to read a child type off, and a wrong kind is worse than
// no kind (the whole premise of ADR-0012 item 8).
//
// unsettled while the column does not resolve, for classifyOperand's reason —
// a name that resolves in no batch yet says nothing about the next one.
func elementOperandKind(container Expr, b *batch.RecordBatch) (boxKind, bool) {
	col, ok := container.(*ColRef)
	if !ok {
		return boxUnknown, true
	}
	col.resolve(b)
	if col.idx < 0 || col.idx >= len(b.Columns) {
		return boxUnknown, false
	}
	child := b.Columns[col.idx].Child
	if child == nil {
		return boxUnknown, true
	}
	if staticallyMap(container, b) {
		// A MAP materializes as ARRAY(ROW("key","value")), and element_at
		// answers the VALUE — so the element kind is the second field's,
		// never the entry row's.
		if len(child.Children) != 2 {
			return boxUnknown, true
		}
		child = child.Children[1]
	}
	switch child.Type {
	case batch.TypeDecimal:
		return boxDecimal, true
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64:
		return boxNumber, true
	case batch.TypeString:
		return boxText, true
	}
	return boxUnknown, true
}

// decimalVsUnclassifiable reports whether the first two armed operands are a
// DECIMAL against an operand whose declaration this layer could not read at
// all — a scalar subquery, a container element. It is the gate a site uses
// before reading two boxes as the numbers they name rather than as the
// strings they are.
//
// Both halves are load-bearing. "Either side is DECIMAL" alone would read a
// genuine STRING column numerically the moment a DECIMAL stood beside it, and
// that is #504's rule inverted: a TEXT column compares AS TEXT whatever its
// digits look like, which is what `s = a`, GREATEST(s, b), CASE s WHEN b and
// IS DISTINCT FROM all do. Every pair whose two declarations ARE known —
// decimal/decimal, decimal/number, decimal/quoted — is already answered by
// extremumArms.order and never reaches this.
func (a *extremumArms) decimalVsUnclassifiable(b *batch.RecordBatch) bool {
	if a == nil || len(a.ops) < 2 {
		return false
	}
	l, r := a.ops[0].resolve(b), a.ops[1].resolve(b)
	return (l == boxDecimal && r == boxUnknown) || (r == boxDecimal && l == boxUnknown)
}

// foldKind upgrades an arm's kind for the common-type fold: a NUMERIC LITERAL
// classifies as boxNumber (its declaration is its spelling, not a column's),
// and PostgreSQL folds its own type in — an unsuffixed constant is `numeric`
// once it carries a point or an exponent and the narrowest integer type that
// holds it otherwise. Without this, `COALESCE(real_col, 0)` folded real with a
// typeless arm and lost the width.
//
// A fold landing on DECIMAL answers boxNumber (numberKindOf has no DECIMAL
// rung), which makes the join fall through rather than claim the DECIMAL text
// rule for a value that is not decimal text.
func foldKind(e Expr, k boxKind) boxKind {
	if k != boxNumber {
		return k
	}
	lit, ok := e.(*Lit)
	if !ok {
		return k
	}
	t, ok := numericConstType(lit)
	if !ok {
		return k
	}
	return numberKindOf(t)
}

// joinOperandKinds is classifyOperand over a set of alternatives that one
// value is chosen from.
//
// The join folds the alternatives through PostgreSQL's own numeric ladder for
// every pair EXCEPT one: a DECIMAL arm keeps the kind decimal, because the
// kind's claim is about the BOX ("a string from here is decimal text") and
// that stays true however wide the fold's TYPE is. The TYPE question — which
// PostgreSQL answers float8 for `numeric ∪ float8` — is
// extremumArms.commonKind's, and it is what the LITERAL is coerced to; the two
// are deliberately separate answers to separate questions. A QUOTED literal
// alternative contributes
// nothing and takes the others' type, the way PostgreSQL resolves an
// unknown-typed literal from its context — `COALESCE(d, 'text')` is a numeric
// expression there, not an ambiguous one. Any other disagreement leaves the
// box ambiguous again and yields boxUnknown.
//
// A NULL alternative is SKIPPED outright. `COALESCE(d, NULL)` is a DECIMAL
// expression, and reading the NULL literal as its own kind poisoned the join
// to boxUnknown — which is how a DECIMAL column wrapped in a COALESCE started
// comparing as rendered text (#504 review, B2). NULL never reaches a
// comparison anyway: every caller short-circuits a nil operand first.
func joinOperandKinds(args []Expr, b *batch.RecordBatch) (boxKind, bool) {
	kind, have, settled := boxUnknown, false, true
	for _, a := range args {
		if a == nil {
			continue
		}
		if lit, ok := a.(*Lit); ok && lit.Val == nil {
			continue
		}
		k, s := classifyOperand(a, b)
		settled = settled && s
		if k == boxQuoted {
			continue // unknown-typed: takes whatever the others declare
		}
		k = foldKind(a, k)
		if !have {
			kind, have = k, true
			continue
		}
		switch {
		case k == kind:
		case k == boxDecimal || kind == boxDecimal:
			// A DECIMAL arm keeps the composite's KIND decimal even when the
			// fold's TYPE is wider. The two are different questions and
			// collapsing them was #646 round-3's blocker: PostgreSQL's
			// `numeric ∪ float8` is float8, so the literal beside such a
			// composite is coerced at the FLOAT rung (extremumArms.commonKind
			// answers that, through the ladder) — but the VALUE this operand
			// produces is still the DECIMAL's rendered TEXT on every row the
			// decimal arm wins, and a kind that says "float" leaves that text
			// with no reading at all. `COALESCE(dec, float) > '9'` then fell
			// to compare()'s BYTE order and answered 134 of 2491 rows, and the
			// UNQUOTED `> 9` spelling — which never reaches a literal rule at
			// all — answered the same way.
			//
			// The kind's only claim is "a string from here is decimal text",
			// and that claim is TRUE for this fold. Keeping it is what puts
			// the pair on a numeric arm at all.
			kind = boxDecimal
		case isNumericFoldKind(k) && isNumericFoldKind(kind):
			// Two numbers that are not the SAME number: an int column beside a
			// float one, or either beside a numeric literal. PostgreSQL resolves
			// ONE type for the whole expression (select_common_type) and the
			// value it produces is of THAT type, so the join folds through the
			// same ladder rather than dropping the width.
			//
			// Dropping it — collapsing to boxNumber — is what made
			// `COALESCE(r_val, 0) = '3.1'` read the literal with the INTEGER
			// input function on the row where r_val is NULL (the box is the
			// int64 0 there) and the double one everywhere else: 22P02 or 0 rows
			// depending on the DATA, where PostgreSQL resolves real once and
			// answers 1 row.
			w, ok := widerNumberKind(k, kind)
			if !ok {
				return boxUnknown, settled
			}
			kind = w
		default:
			return boxUnknown, settled
		}
	}
	if !have {
		return boxUnknown, true
	}
	return kind, settled
}

// boxOperand caches one operand's settled kind.
//
// The atomic is the same publish decimalLitCmp.notDecimal uses and for the
// same reason: these nodes are shared across parallel pipeline workers
// evaluating one batch, and concurrent writers can only ever agree, because
// the answer is a pure function of the operand's fixed declaration.
type boxOperand struct {
	expr Expr
	// kind holds int32(k)+1 once settled; 0 means "not settled yet".
	kind atomic.Int32
	// fold holds the operand's TYPE answer the same way, which differs from
	// kind for exactly one shape: a composite mixing a DECIMAL arm with a
	// FLOAT one. Its KIND stays decimal, because a string from it is decimal
	// text; its TYPE is PostgreSQL's fold, float8 — and the type is what a
	// QUOTED literal beside it is coerced to. Reading the kind for both made
	// `GREATEST(numeric, float8) = 'abc'` say "type numeric" where PostgreSQL
	// says "double precision", and would have REFUSED `= '0x1p3'`, which the
	// float input function reads as 8 and the numeric one does not.
	fold atomic.Int32
}

func (o *boxOperand) resolve(b *batch.RecordBatch) boxKind {
	if v := o.kind.Load(); v != 0 {
		return boxKind(v - 1)
	}
	k, settled := classifyOperand(o.expr, b)
	if settled {
		o.kind.Store(int32(k) + 1)
	}
	return k
}

// resolveFold is resolve for the operand's TYPE answer. See boxOperand.fold.
func (o *boxOperand) resolveFold(b *batch.RecordBatch) boxKind {
	if v := o.fold.Load(); v != 0 {
		return boxKind(v - 1)
	}
	k, settled := classifyOperandFold(o.expr, b)
	if settled {
		o.fold.Store(int32(k) + 1)
	}
	return k
}

// boxedPair binds two operands of a boxed comparison — a Cmp's generic arm, a
// simple CASE's operand against one WHEN, IS DISTINCT FROM's two sides, or one
// (best-so-far, candidate) pair of GREATEST/LEAST — together with their
// literal source texts.
//
// It is the one place that decides HOW two boxes compare, from what they
// declare. Sites build one per operand pair at construction time; resolution
// happens on the first batch that can answer it and is then a single atomic
// load per row.
type boxedPair struct {
	left, right  boxOperand
	lText, rText string
	// disarmed settles "no declaration-driven rule can ever apply to this
	// pair", which is every comparison between two ordinary columns of the
	// same family. It turns the whole check into one atomic load, keeping the
	// generic path's per-row cost where it was before this binding existed.
	disarmed atomic.Bool
}

func newBoxedPair(left, right Expr) *boxedPair {
	return &boxedPair{
		left:  boxOperand{expr: left},
		right: boxOperand{expr: right},
		lText: operandLitText(left), rText: operandLitText(right),
	}
}

// operandLitText is a LITERAL operand's source text, for either spelling a
// user can write a value in: the verbatim digits of a numeric literal
// (`Lit.Text`, set by compileLit) or the contents of a quoted string. Empty
// for every operand that is not a literal.
//
// One field for both because the rule that reads it is the same one —
// PostgreSQL compares a `numeric` column against `12.75` and against `'12.75'`
// identically — and the operand's KIND is what tells the two apart where it
// matters (a quoted literal is unknown-typed and takes the column's type; a
// numeric literal is already `numeric` and so makes a TEXT column's
// comparison a text one, per ADR-0012 item 5).
func operandLitText(e Expr) string {
	lit, ok := e.(*Lit)
	if !ok {
		return ""
	}
	if lit.Text != "" {
		return lit.Text
	}
	if s, ok := lit.Val.(string); ok {
		return s
	}
	return ""
}

// pairApplies reports whether any rule below can fire for this KIND pair. It
// depends only on the declarations, so a false answer is permanent.
func pairApplies(lk, rk boxKind, lText, rText string) bool {
	switch {
	// A PROVEN DECIMAL operand applies against ANY other kind. It used to
	// require the other side to be a number as well, which left a DECIMAL
	// column comparing as RENDERED TEXT against every operand this file
	// cannot classify — a scalar subquery, arithmetic, a CAST (#504 review,
	// B2). Nothing is lost by widening it: orderByKinds' decimalTextOrder
	// returns ok=false for a box that is not a number, so a genuine STRING
	// column on the other side still falls through to the lexical comparison
	// #504 settled.
	case lk == boxDecimal || rk == boxDecimal:
		return true
	// A TEXT column against a NUMERIC literal: the text comparison (#504).
	case lk == boxText && isNumberKind(rk) && rText != "":
		return true
	case rk == boxText && isNumberKind(lk) && lText != "":
		return true
	// A NUMBER column against a QUOTED literal: the numeric comparison,
	// because PostgreSQL types the unknown literal from the column (B1) — and
	// with the column's OWN input function, which is why the four declared
	// number kinds are separate (#646).
	//
	// The EMPTY quoted literal reaches here too, and must: `f = ''` is
	// PostgreSQL's 22P02, and requiring a non-empty text would send it to
	// compare() to answer silently. A numeric LITERAL's text is never empty
	// (compileLit sets it from the digits), so the boxText arms above keep
	// their guard.
	case isNumberKind(lk) && rk == boxQuoted:
		return true
	case isNumberKind(rk) && lk == boxQuoted:
		return true
	// A network column whose ORDER is the address's, against another column
	// of its own type or against a quoted literal. Two of them, or one and a
	// literal, is the pair no box can tell from two strings — the shape
	// ADR-0012 item 8 says must be answered from the DECLARATIONS (#565).
	case lk == boxCidr && rk == boxCidr, lk == boxIPv6 && rk == boxIPv6:
		return true
	case (lk == boxCidr || lk == boxIPv6) && rk == boxQuoted && rText != "":
		return true
	case (rk == boxCidr || rk == boxIPv6) && lk == boxQuoted && lText != "":
		return true
	// A BOOL column against a QUOTED literal: the literal is parsed through
	// PostgreSQL's boolean input grammar, not string-matched against the
	// bool rendered as "true"/"false" (#574). A bool LITERAL is boxUnknown,
	// so `s = TRUE` never reaches here.
	case lk == boxBool && rk == boxQuoted && rText != "":
		return true
	case rk == boxBool && lk == boxQuoted && lText != "":
		return true
	// A TEMPORAL operand meeting anything whose values can arrive as text.
	// The domain (epoch days vs epoch milliseconds) is the declaration's to
	// state; compare()'s fallback infers it from the value's magnitude and
	// gets it wrong inside +/-500,000 of the epoch (#826). Both a QUOTED
	// literal and a genuine STRING column reach here, because the defect is
	// the same in both spellings — and it was VISIBLE only in the second,
	// since the literal spelling is specialized into CmpTemporalLit at
	// compile time and pre-parses in the column's own domain.
	case isTemporalKind(lk) && (rk == boxQuoted || rk == boxText):
		return true
	case isTemporalKind(rk) && (lk == boxQuoted || lk == boxText):
		return true
	}
	return false
}

// temporalTextOrder compares a TEMPORAL operand against a text one in the
// temporal operand's OWN domain, which its kind names.
//
// tv is the temporal operand's box: a TIMESTAMP column boxes as int64 epoch
// milliseconds, a DATE column as its formatted "YYYY-MM-DD" text. other is
// the text operand's box, and otherText its literal source when it has one
// (a quoted literal's box and its text are the same string; a STRING column
// has no source text).
//
// Both sides are converted to the DECLARED domain's unit and compared there.
// Nothing here consults a magnitude: that is the whole point. A side that
// will not parse returns ok=false and the pair falls through to compare(),
// which is what it did before this rule existed.
func temporalTextOrder(kind boxKind, tv, other any, otherText string) (int, bool) {
	s, ok := other.(string)
	if !ok {
		s = otherText
		if s == "" {
			return 0, false
		}
	}
	switch kind {
	case boxTimestamp:
		ms, ok := tv.(int64)
		if !ok {
			return 0, false
		}
		rms, ok := parseTimestampToEpochMsCachedOK(s)
		if !ok {
			return 0, false
		}
		return cmpInt64(ms, rms), true
	case boxDate:
		// A DATE operand reaches here as its EPOCH-DAY count, which is what
		// ColRef.Eval hands out for a TypeDate column — not as the rendered
		// text this arm first assumed. Getting that wrong made the whole arm
		// dead code: tv.(string) never matched, the pair fell back to
		// compare()'s magnitude heuristic, and the DATE half of #826 was
		// never fixed at all. It looked fixed because a fixture inside the
		// band (1970-01-02 is day 1, and |1| < 500_000) is where the
		// heuristic is accidentally right; past 500,000 epoch days
		// (3500-01-01 is 558,821) `d = ds` answered 0 for 2 and `d < ds`
		// answered 3 for 1.
		//
		// Both spellings are accepted, because a DATE can also arrive as
		// text through a CAST or a rendered container element, and the
		// string form must not silently take the magnitude path either.
		var ld int64
		switch v := tv.(type) {
		case int64:
			ld = v
		case int32:
			ld = int64(v)
		case string:
			d, ok := parseDateToEpochDaysCachedOK(v)
			if !ok {
				return 0, false
			}
			ld = d
		default:
			return 0, false
		}
		rd, ok := parseDateToEpochDaysCachedOK(s)
		if !ok {
			return 0, false
		}
		return cmpInt64(ld, rd), true
	}
	return 0, false
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// netOrder orders two network-typed boxes by the address's own order, with
// each side re-keyed by the function its OPERAND's kind selects.
//
// unknown reports ADR-0012 item 10's rule for a stored value that names no
// address: the column is unvalidated text (internal/storage/ingest), so a row
// can hold something that is not an address, and a value with no place in the
// order has no defined comparison against one that does. That is UNKNOWN — it
// matches nothing for every operator, `<>` included, the answer a NULL row
// gets — and never a fallback to comparing the two TEXTS, which is how one bad
// row split the two evaluation sites apart in the first place.
func netOrder(lKey, rKey func(string) (string, bool), lv, rv any) (c int, ok, unknown bool) {
	ls, lIsStr := lv.(string)
	rs, rIsStr := rv.(string)
	if !lIsStr || !rIsStr {
		// Not the boxes this rule reads. Falling through is right: the
		// declaration says what the COLUMN is, and a box of another shape
		// from it is something compare() should judge on its own terms.
		return 0, false, false
	}
	lk, lParsed := lKey(ls)
	rk, rParsed := rKey(rs)
	if !lParsed || !rParsed {
		return 0, true, true
	}
	return strings.Compare(lk, rk), true, false
}

// netKeyFor is the re-key function one operand's kind selects, and the side it
// is on matters for IPv6: a STORED value is the address's own 16 bytes
// (kernel.IPv6RowKey), while a LITERAL dotted quad is a v4 address that
// PostgreSQL's family rule puts BELOW every v6 row (kernel.IPv6LitKey keys it
// to the empty string for exactly that). CIDR uses one function on both sides
// because a bare address is a /32 host route wherever it is written.
func netKeyFor(k boxKind, literal bool) func(string) (string, bool) {
	switch k {
	case boxCidr:
		return kernel.CidrSortKey
	case boxIPv6:
		if literal {
			return kernel.IPv6LitKey
		}
		return kernel.IPv6RowKey
	}
	return nil
}

// order compares two boxed values under the rule their DECLARATIONS select,
// returning -1, 0 or +1, or ok=false when no such rule applies and the caller
// must fall through to compare().
//
// The rules, each PostgreSQL's:
//
//   - DECIMAL against DECIMAL: the two exact decimals, at whatever scales the
//     columns declare — "1.50" and "1.5000" are one number (#477, #506).
//   - DECIMAL against a numeric LITERAL: the literal's exact source text
//     against the column's, because PostgreSQL types an unsuffixed decimal
//     literal as `numeric` and compares it at full precision (#452, #465).
//   - DECIMAL against a non-DECIMAL number: exact against an integer, float64
//     against a float, which is what `numeric <op> double precision` does —
//     it casts the numeric (#476).
//   - TEXT against a numeric LITERAL: the literal's source TEXT against the
//     column's value, bytewise. PostgreSQL refuses this pair outright —
//     verified live, `WHERE s = 1.5` over a text column is 42883 "operator
//     does not exist: text = numeric" — but that is an OVERLOAD RESOLUTION
//     failure, and wadjet has one generic comparison operator with no
//     overload set to fail resolution against, exactly the situation
//     ADR-0012 item 5 already records for unary minus over a quoted string.
//     So the pair gets the STRING column's own rule instead of a reading of
//     its digits, which is also what the vectorized kernel answers (#504).
//   - A NUMBER against a QUOTED literal: the NUMBER's rule, because
//     PostgreSQL types an unknown-typed literal from the operand it meets.
//     `k > '2'` over a BIGINT column is `k > 2` there, not a text comparison
//     and not a comparison against zero.
func (p *boxedPair) order(b *batch.RecordBatch, lv, rv any) (c int, ok, unknown bool) {
	if p == nil || p.disarmed.Load() {
		return 0, false, false
	}
	lk := p.left.resolve(b)
	rk := p.right.resolve(b)
	if !pairApplies(lk, rk, p.lText, p.rText) {
		// Only a SETTLED pair may disarm: an operand that no batch has
		// resolved yet is still going to answer on a later one.
		if p.left.kind.Load() != 0 && p.right.kind.Load() != 0 {
			p.disarmed.Store(true)
		}
		return 0, false, false
	}
	return orderByKindsFold(lk, rk, p.left.resolveFold(b), p.right.resolveFold(b),
		lv, rv, p.lText, p.rText)
}

// orderByKinds is order's rule table, with the kinds already resolved. It is
// separate because GREATEST/LEAST compare (best-so-far, candidate) pairs whose
// OPERANDS move between iterations: those sites resolve one boxOperand per
// ARGUMENT and assemble the pair per comparison, instead of holding a
// boxedPair for every ordered pair of arguments.
func orderByKinds(lk, rk boxKind, lv, rv any, lText, rText string) (c int, ok, unknown bool) {
	return orderByKindsFold(lk, rk, lk, rk, lv, rv, lText, rText)
}

// orderByKindsFold is orderByKinds with the operands' TYPE answers alongside
// their kinds. They differ only for a composite mixing a DECIMAL arm with a
// wider one: the KIND says "a string from here is decimal text" and picks the
// arm, the FOLD says float8 and is what the QUOTED literal is coerced to —
// PostgreSQL's own answer for `numeric ∪ float8`. Passing the kind for both is
// what named "type numeric" in a message PostgreSQL writes "double precision",
// and would have refused a literal only the float grammar reads.
func orderByKindsFold(lk, rk, lFold, rFold boxKind, lv, rv any, lText, rText string) (c int, ok, unknown bool) {
	ls, lIsStr := lv.(string)
	rs, rIsStr := rv.(string)
	// The kind picks which BOX shapes can appear; the FOLD picks the rung the
	// pair is compared at. lCo/rCo are the second answer, and every arm below
	// that reads a number consults them rather than the kind.
	lCo := literalCoercionKind(lk, lFold)
	rCo := literalCoercionKind(rk, rFold)
	switch {
	// A QUOTED literal against ANY numeric-kinded operand comes FIRST, before
	// the DECIMAL arms below.
	//
	// It used to come after them, and the ordering was the whole of #646's
	// round-4 blocker. A composite mixing a DECIMAL column with a FLOAT one
	// has kind boxDecimal and fold float8, so on every row the decimal arm won
	// the pair took the DECIMAL arm and was read with the DECIMAL grammar at
	// DECIMAL width — three wrong answers at once. `COALESCE(numeric, float8)
	// = '0xC.C'` answered 0 where PostgreSQL answers 4, because the float
	// input function reads that hex float as 12.75 and the numeric one refuses
	// it. `= '12.750000000000000001'` answered 0 where PostgreSQL answers 4,
	// because float8 rounds the literal onto the value and an exact decimal
	// comparison does not. And `= 'abc'` refused only on the rows the FLOAT
	// arm supplied, which is the row-dependent refusal ADR-0012 item 13 says
	// is closed — the DECIMAL arm compared before the literal was parsed at
	// all, so a row it could read never reached the refusal.
	//
	// quotedNumberOrder parses the literal first and reads a DECIMAL's
	// rendered text at whatever rung it is handed, so routing every quoted
	// pair through it settles the grammar, the width and the refusal together.
	case isNumericFoldKind(lk) && rk == boxQuoted:
		if typ, ok := numberKindType(lCo); ok {
			if c, ok := quotedNumberOrder(typ, lv, rText); ok {
				return c, true, false
			}
		}
	case isNumericFoldKind(rk) && lk == boxQuoted:
		if typ, ok := numberKindType(rCo); ok {
			if c, ok := quotedNumberOrder(typ, rv, lText); ok {
				return -c, true, false
			}
		}
	// The DECIMAL arms own a pair only when the FOLD is itself DECIMAL. A
	// decimal-kinded operand whose fold is wider is a number at that wider
	// rung, and numericBoxRepair below reads its text there.
	case lk == boxDecimal && rk == boxDecimal && lCo == boxDecimal && rCo == boxDecimal:
		if lIsStr && rIsStr {
			c, ok := batch.CompareDecimalTexts(ls, rs)
			return c, ok, false
		}
	case lk == boxDecimal && lIsStr && lCo == boxDecimal:
		if rText != "" {
			c, ok := batch.CompareDecimalTexts(ls, rText)
			return c, ok, false
		}
		if c, ok := decimalTextOrder(rv, ls); ok {
			return -c, true, false
		}
	case rk == boxDecimal && rIsStr && rCo == boxDecimal:
		if lText != "" {
			c, ok := batch.CompareDecimalTexts(lText, rs)
			return c, ok, false
		}
		if c, ok := decimalTextOrder(lv, rs); ok {
			return c, true, false
		}
	case lk == boxText && lIsStr && rText != "":
		return strings.Compare(ls, rText), true, false
	case rk == boxText && rIsStr && lText != "":
		return strings.Compare(lText, rs), true, false
	// Two network columns of one type, or one against a quoted literal, in
	// the ADDRESS's own order rather than the rendered text's (#565). The
	// literal's key comes from its own SIDE, which is what puts a dotted-quad
	// literal below every IPv6 row while a v4-MAPPED stored value stays among
	// them (netKeyFor).
	case lk == boxCidr && rk == boxCidr, lk == boxIPv6 && rk == boxIPv6:
		return netOrder(netKeyFor(lk, false), netKeyFor(rk, false), lv, rv)
	case (lk == boxCidr || lk == boxIPv6) && rk == boxQuoted:
		return netOrder(netKeyFor(lk, false), netKeyFor(lk, true), lv, rText)
	case (rk == boxCidr || rk == boxIPv6) && lk == boxQuoted:
		c, ok, unknown := netOrder(netKeyFor(rk, false), netKeyFor(rk, true), rv, lText)
		return -c, ok, unknown
	// A BOOL column against a QUOTED literal, in boolean order (FALSE <
	// TRUE), the literal read through PostgreSQL's input grammar (#574). A
	// literal that names no boolean is 22P02, raised by boolFromText — never
	// a fallthrough to a text comparison that would silently miss, which is
	// the same refusal the vectorized kernel makes (exec.boolConstError).
	case lk == boxBool && rk == boxQuoted:
		if lb, ok := lv.(bool); ok {
			return boolOrder(lb, boolFromText(rText)), true, false
		}
	case rk == boxBool && lk == boxQuoted:
		if rb, ok := rv.(bool); ok {
			return -boolOrder(rb, boolFromText(lText)), true, false
		}
	// A TEMPORAL operand against text, in the DECLARED domain (#826).
	case isTemporalKind(lk) && (rk == boxQuoted || rk == boxText):
		if c, ok := temporalTextOrder(lk, lv, rv, rText); ok {
			return c, true, false
		}
	case isTemporalKind(rk) && (lk == boxQuoted || lk == boxText):
		if c, ok := temporalTextOrder(rk, rv, lv, lText); ok {
			return -c, true, false
		}
	}
	// Nothing above could read this pair. If both KINDS say numeric, the boxes
	// are still orderable — a composite mixing a DECIMAL column with a FLOAT or
	// an INTEGER one folds to the wider rung and then hands that rung a
	// DECIMAL's rendered TEXT on the rows where the decimal arm won — and
	// ordering them by BYTES instead is the defect this whole layer exists to
	// remove (#504, #506).
	if c, ok := numericBoxRepair(lk, rk, lv, rv); ok {
		return c, true, false
	}
	// And if even that could not read a DECIMAL operand, say so loudly rather
	// than let compare() answer with byte order. Falling through is legitimate
	// for the pairs that have a TEXT rule — a genuine STRING column, or an
	// operand with no declaration at all — so those are excluded by name.
	raiseIfUnreadDecimal(lk, rk, lv, rv)
	return 0, false, false
}

// raiseIfUnreadDecimal is orderByKinds' backstop: it fires only when a
// boxDecimal operand meets another NUMERIC kind (or a quoted literal) and no
// arm above produced a reading.
//
// It should never fire. It exists because the alternative when it would have
// is a silent wrong ORDER over data — the failure mode that shipped three
// times in this file's history — and because the condition is cheap and only
// reachable on a path that is already returning "no rule".
func raiseIfUnreadDecimal(lk, rk boxKind, lv, rv any) {
	var other boxKind
	var dec any
	switch {
	case lk == boxDecimal:
		other, dec = rk, lv
	case rk == boxDecimal:
		other, dec = lk, rv
	default:
		return
	}
	if !isNumericFoldKind(other) && other != boxQuoted {
		return
	}
	// A DECIMAL column can hold text that is not a number at all: the column
	// is unvalidated (internal/storage/ingest), and ADR-0012 item 10 gives
	// that shape UNKNOWN rather than an error one type over. Only a value the
	// DECIMAL grammar accepts should have had a reading.
	if s, ok := dec.(string); ok && !kernel.NewDecimalLiteral(s).Numeric() {
		return
	}
	panic(fatalEval{fmt.Errorf(
		"internal: a DECIMAL operand reached the generic comparison with no numeric reading "+
			"(kinds %d/%d, values %T/%T) — ordering it by bytes would be a wrong answer", lk, rk, lv, rv)})
}

// literalCoercionKind is the kind a QUOTED literal beside this operand is
// coerced to: the operand's TYPE fold where it has a wider one, and its own
// kind otherwise. A fold that resolved nothing falls back to the kind, which
// is the conservative answer — the same one this had before the fold existed.
func literalCoercionKind(kind, fold boxKind) boxKind {
	if isNumericFoldKind(fold) {
		return fold
	}
	return kind
}

// boolOrder orders two booleans in PostgreSQL's boolean order, FALSE < TRUE,
// as a -1/0/1 comparison the caller applies its operator to.
func boolOrder(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}

// compare answers one comparison operator for this pair, falling through to
// compare() when the operands' declarations select no rule.
//
// That fallthrough is the whole reason the sniff could be deleted. An operand
// whose declaration nothing here can read — a CAST, a scalar subquery, an
// ordinary function's result — is compared by compare()'s own type rules,
// which is the documented stance for a value with no declaration to consult
// (ADR-0012 item 8's `compareAny` fallback). What it must NOT do is guess a
// declaration from the box, because that guess is what disagreed with the
// kernel.
//
// A nil pair is an unbound node — a Cmp assembled as a struct literal rather
// than through NewCmp, the same shape that leaves dec and decCols nil.
func (p *boxedPair) compare(b *batch.RecordBatch, lv, rv any, op CmpOp) bool {
	v, _ := p.compareNull(b, lv, rv, op)
	return v
}

// compareNull is compare with the third answer a network comparison can give:
// UNKNOWN, for a stored value that names no address (netOrder's own doc, and
// ADR-0012 item 10).
//
// Only Cmp reads the null half. The other sites — IS DISTINCT FROM, IN,
// BETWEEN, a simple CASE's WHEN — want the FALSE that compare() collapses it
// to, and each for its own reason rather than by accident: IS DISTINCT FROM is
// a total predicate, so `distinct = !compare(..., CmpEq)` answers TRUE for an
// unorderable pair exactly as it does for a NULL one; an IN list, a BETWEEN
// bound and an unmatched WHEN all treat "no" and "cannot say" alike, which is
// what a NULL operand already gets there.
func (p *boxedPair) compareNull(b *batch.RecordBatch, lv, rv any, op CmpOp) (val, null bool) {
	c, ok, unknown := p.order(b, lv, rv)
	switch {
	case unknown:
		return false, true
	case ok:
		return cmpOrder(c, op), false
	}
	return compare(lv, rv, op), false
}

// pairSetState lifts boxedPair's own one-pair disarm cache to a SET of pairs
// — the shape In (one pair per list member) and Between (one pair per bound)
// both have. Dispatching every member/bound through its own *boxedPair costs
// one atomic load and a method call per member per row even once every pair
// on the node is permanently disarmed, which is the common case: an IN list
// or a BETWEEN range over an ordinary INT/FLOAT/STRING column never has a
// DECIMAL or boxed-text rule to apply, so that per-member dispatch is pure
// overhead once the node has settled. This caches the SET's own answer the
// same way, so the per-row cost for an all-disarmed node collapses to the one
// atomic load below, matching a bare Cmp's.
type pairSetState struct {
	// state: 0 = still settling, 1 = every pair disarmed (fast path applies),
	// 2 = at least one pair is (and, being a pure function of declared kinds,
	// stays) armed, so the caller must keep dispatching through each pair.
	state atomic.Int32
}

// disarmed reports whether every pair in pairs has settled disarmed.
//
// It never performs a comparison and never derives a kind itself — it only
// reads the state a pair's own order() leaves behind once dispatched, so a
// caller must keep dispatching through each pairs[i].compare (as it always
// did) for as long as this returns false; that dispatch is what settles the
// individual pairs in the first place. Once every pair is settled, the
// answer is a pure function of their declared kinds (boxedPair's own
// contract) and is cached here forever, the same way boxedPair.disarmed
// itself is.
func (s *pairSetState) disarmed(pairs []*boxedPair) bool {
	if v := s.state.Load(); v != 0 {
		return v == 1
	}
	allDisarmed := true
	for _, p := range pairs {
		if p == nil || p.disarmed.Load() {
			continue
		}
		if p.left.kind.Load() != 0 && p.right.kind.Load() != 0 {
			// Both operands are settled and this pair is still not
			// disarmed: pairApplies is a pure function of settled kinds, so
			// it applies now and always will — this SET can never reach
			// all-disarmed, and there is no need to keep checking the rest.
			s.state.Store(2)
			return false
		}
		allDisarmed = false
	}
	if allDisarmed {
		s.state.Store(1)
		return true
	}
	return false
}
