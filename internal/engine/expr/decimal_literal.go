package expr

import (
	"math"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// decimalLitCmp binds a bare column reference to the numeric literals it is
// compared against, so that when the column turns out to be a DECIMAL the
// comparison is answered in the column's own domain — the unscaled integer at
// the column's scale — instead of through float64 on both sides.
//
// Two separate losses live on that float64 path, and this closes both (#452):
//
//   - the LITERAL: a float64 holds ~15-16 significant decimal digits, so
//     `= 493827160549382.7160549350` became `= 493827160549382.6875` and
//     matched nothing, while `>` gained the row it should have excluded.
//   - the COLUMN: ColRef.Eval boxes a DECIMAL as its rendered text, and
//     compare() has no numeric reading of text against a float64, so it fell
//     through to a LEXICOGRAPHIC comparison — "1339815.97" against
//     "1.33981597e+06" — which is not the same order and not the same
//     equality.
//
// The binding is decided at COMPILE time (the operand shapes) and applied per
// BATCH (the column's type and scale), because a column's type is not known
// until a batch arrives. Anything that is not a materialized DECIMAL column
// falls through to the generic path untouched, which is what keeps every
// other type answering exactly as before.
type decimalLitCmp struct {
	col  *ColRef
	lits []*kernel.DecimalLiteral // one per literal operand, in operand order
	flip bool                     // the literal was the LEFT operand

	// numeric caches lits[i].Numeric(), one bool per literal, decided once at
	// BIND time rather than read on every row. lit.Numeric() parses the
	// literal's text from scratch (isDecimalText → batch.DecimalTextAt) — a
	// full text parse, not the cached-by-scale resolution lit.Order() does —
	// and a literal's text is immutable for the query's lifetime, so calling
	// it per row bought nothing but the parse cost 2048 times over: measured
	// 12x on a plain `col > lit`, 59x on an exponent-form literal (the digit
	// walk is longer), 15x on IN, against a hoisted read of this slice.
	numeric []bool

	// notDecimal caches a settled "this column is not DECIMAL" answer. A
	// column's type cannot change across the batches of one query, so the
	// first batch that resolves the bound column to anything but DECIMAL
	// settles it for every row of every later batch too. vector() checks
	// this FIRST so the common case — every non-DECIMAL type, on the
	// row-at-a-time path — costs one atomic load instead of a
	// ColRef.resolve() call plus three field checks per row (measured
	// +5.4% Cmp / +13% In on a FLOAT64 column before this cache existed).
	//
	// Cmp/In/Between are shared across parallel pipeline workers evaluating
	// the same batch concurrently (see ColRef.resolved's doc comment), so
	// this needs the same atomic publish that ColRef itself uses — a plain
	// bool would be a data race. Concurrent writers can only ever agree
	// (the answer is a pure function of the column's fixed type), so a
	// losing racer's store is redundant, not wrong.
	notDecimal atomic.Bool

	// nonAddr is the source text of a STRING literal operand that names no
	// address, and nonAddrSet says whether there is one — a separate flag
	// because the EMPTY STRING is itself a non-address literal, so "" cannot
	// double as "there is none". `c_cidr = ''` is not a hypothetical: the
	// parser lowers it to a ColEmptyStr whose Fallback is this Cmp, so the
	// zero-length shape reaches the refusal through here and nowhere else.
	// It is the CIDR/IPv6/IPv4/MAC/UUID counterpart of the refusal order()
	// raises for a DECIMAL column, and it lives here rather than in a
	// binding of its own because this one already resolves the column: the
	// check costs nothing per row, running once on the batch that settles
	// notDecimal.
	//
	// tryNetworkLit takes every literal that parses as an IPv4, MAC, IPv6 or
	// CIDR address before NewCmp is ever reached, so a literal that arrives
	// here against one of those four column types is one no reading can
	// make sense of — `c_cidr <> 'garbage'`, which used to answer ZERO rows
	// through the kernel and EVERY row through this path (#492). UUID has no
	// such pre-filter (tryNetworkLit doesn't cover it — see its own doc), so
	// firstNonAddressLit checks UUID validity directly (#519).
	nonAddr    string
	nonAddrSet bool
}

// numericLit reports the exact source text of a constant operand that a
// DECIMAL column can be compared against.
//
// A numeric literal contributes its verbatim source Text: the float64 box the
// compiler built for arithmetic has already lost the digits past a double
// (ADR-0012 item 6). A STRING literal contributes its own text, because
// PostgreSQL types an unquoted-type literal from the other operand — `d =
// '12.75'` is a numeric comparison there, not a string one — and because a
// string that is NOT a number must reach the comparison to be REFUSED there
// rather than read as zero (#463). Whether the column is a DECIMAL at all is
// still decided per batch by vector(); nothing here changes what a string
// literal means against a string column.
func numericLit(e Expr) (*kernel.DecimalLiteral, bool) {
	lit, ok := e.(*Lit)
	if !ok {
		return nil, false
	}
	// Text is set for a NUMERIC literal and empty for every other kind, so it
	// is both the exact carrier and the test for "this operand is a number
	// the user wrote" — including one no Go numeric box can hold, which
	// compileLit boxes as its own text.
	if lit.Text != "" {
		return kernel.NewDecimalLiteral(lit.Text), true
	}
	if s, ok := lit.Val.(string); ok {
		return kernel.NewDecimalLiteral(s), true
	}
	return nil, false
}

// bareCol returns the operand as a plain column reference — not a ROW field
// access, which reads a boxed value out of a container rather than a column.
func bareCol(e Expr) (*ColRef, bool) {
	col, ok := e.(*ColRef)
	if !ok || col.structField != "" {
		return nil, false
	}
	return col, true
}

// refuseArm is the plan-shaped half of #463's refusal — SQLSTATE 22P02, never
// a value — for ONE operand pair: the bare-column operand, and the other
// operand's source text when that text does not name a number. A nil col
// means no refusal is possible for this pair whatever the column turns out to
// be, which is the overwhelmingly common case and the one that has to be free.
//
// It exists because the refusal's two questions have very different lifetimes.
// "Is the literal a number?" is fixed for the query — `kernel.DecimalLiteral.
// Numeric()` walks the digits from scratch — and "is the column a DECIMAL?" is
// fixed for the query too, once one batch has answered it. Asking both PER ROW
// is what the first #505 fix did, and it cost a `NewDecimalLiteral` allocation
// plus a full text parse on every row of every batch at three sites that are
// otherwise allocation-free: +35% on a simple CASE over a DECIMAL column, +25%
// on IS DISTINCT FROM, and +200% with 7x the bytes on an exponent-form literal
// — reintroducing exactly the regression `decimal_order_bench_test.go` was
// written to hold shut (it is why `decimalLitCmp.numeric` is a cached slice
// rather than a per-row `Numeric()` call).
//
// This is the refusal half of the boxed comparison's job, for the three sites
// (#465) that carry a literal's exact text into that comparison but never call
// Numeric() on it: Case's simple-CASE arm, IsDistinctFrom, and pickExtremum
// (GREATEST/LEAST). boxedPair's literal arm only fires for a literal ALREADY
// known to be numeric (compileLit sets Lit.Text for exactly that shape) — a
// non-numeric string like 'abc' carries no Text, so no arm matches it and the
// comparison falls through to compare()'s ordinary string comparison instead
// of refusing, which is #463's exact failure mode on the boxed path (#505).
//
// bindDecimalCmp's `d op lit` shape does not need this: NewCmp binds it at
// construction time and decimalLitCmp.order already refuses there. This is
// for the three sites that reach a DECIMAL column with no such binding.
type refuseArm struct {
	col  *ColRef
	text string
	// mask says which column types refuse this text, computed once from the
	// literal (quotedLitMask). It is what makes the rule type-parameterized
	// rather than DECIMAL-only without putting a parse back on the row loop.
	mask litRefusalMask

	// settled caches "this column's type does not refuse this literal", the
	// way decimalLitCmp.notDecimal does and for the same reason — including
	// why it is atomic: these nodes are shared across parallel pipeline
	// workers evaluating one batch, and concurrent writers can only ever
	// agree.
	settled atomic.Bool
}

// noRefusal is the shared arm for every pair that can never refuse. Sharing
// one value keeps arming allocation-free in the common case.
var noRefusal = &refuseArm{}

// armRefusal decides, from the operand SHAPES alone, whether this pair can
// ever raise — and against which column, with which text. It reproduces the
// operand-order rule the per-row version had: the LEFT operand is the
// candidate column when it is a bare one, and only when it is not is the right
// operand tried, so `lit op col` is covered and `col op col` refuses nothing.
func armRefusal(left, right Expr) *refuseArm {
	col, other, ok := refusalOperands(left, right)
	if !ok {
		return noRefusal
	}
	text, ok := quotedLitText(other)
	if !ok {
		return noRefusal
	}
	mask := quotedLitMask(text)
	if mask == 0 {
		return noRefusal
	}
	return &refuseArm{col: col, text: text, mask: mask}
}

func refusalOperands(left, right Expr) (*ColRef, Expr, bool) {
	if col, ok := bareCol(left); ok {
		return col, right, true
	}
	if col, ok := bareCol(right); ok {
		return col, left, true
	}
	return nil, nil, false
}

// check raises when the armed column's type refuses this literal in THIS
// batch. Once it has seen the column resolve to a type that accepts it, the
// whole call is one atomic load, which is the point.
func (a *refuseArm) check(b *batch.RecordBatch) {
	if a.col == nil || a.settled.Load() {
		return
	}
	a.col.resolve(b)
	if a.col.idx < 0 || a.col.idx >= len(b.Columns) {
		// Unresolved says nothing about the next batch, so do not settle.
		return
	}
	if st, refuse := a.mask.refuses(a.col.typ); refuse {
		raiseQuotedLitRefusal(a.col.typ, a.text, st)
	}
	a.settled.Store(true)
}

// caseArms is a simple CASE's per-WHEN binding: the refusal for that WHEN,
// and the declaration-driven comparison between the CASE's operand and that
// WHEN's value. Armed together and published as one immutable value.
//
// Per-WHEN rather than per-CASE because the refusal must still depend on the
// WHEN being REACHED: `CASE d WHEN 0.00 THEN 1 WHEN 'abc' THEN 2 END` answers
// 1 for the row holding 0.00, exactly as it did before the arms were hoisted.
// The comparisons are per-WHEN for the same reason a Cmp's is per-node: each
// pair has its own two declarations.
type caseArms struct {
	refuse []*refuseArm
	pairs  []*boxedPair
}

// extremumRefusal is pickExtremum's arming: the pairs it compares are
// (best-so-far, candidate) and the best-so-far MOVES, so the table is per
// ARGUMENT and the pair is assembled per iteration from it.
type extremumRefusal struct {
	// cols[i] is argument i when it is a bare column reference, else nil.
	cols []*ColRef
	// bad[i] is argument i's QUOTED literal text, and mask[i] which column
	// types refuse it (quotedLitMask). A pair refuses when one side has a col
	// and the other a non-zero mask that names the column's type.
	//
	// The MASK is the sentinel, not the text: `f = ''` is PostgreSQL's 22P02,
	// so an empty literal is a refusable value rather than "no literal here".
	bad  []string
	mask []litRefusalMask
	// litTyp[i] is a NUMERIC literal argument's own type, which PostgreSQL
	// folds into the common type alongside the columns: an unsuffixed constant
	// with a point or an exponent is `numeric`, otherwise it is the narrowest
	// integer type that holds it (ADR-0012 item 12's rule for a set-operation
	// literal arm, which is the same select_common_type). Without it,
	// `GREATEST(bigint_col, '3.1', 2.5)` would fold to bigint and refuse a
	// query PostgreSQL answers as numeric.
	litTyp []batch.TypeID
}

// extremumArms is pickExtremum's full arming: the refusal above, plus one
// declaration-driven operand per ARGUMENT and that argument's literal text.
//
// Per argument rather than per pair for the same reason the refusal is: the
// best-so-far MOVES, so the pair being compared is assembled per iteration.
// N arguments give N(N-1)/2 possible pairs and only N-1 are ever compared, so
// a boxedPair per pair would allocate for comparisons that never happen.
type extremumArms struct {
	refuse *extremumRefusal
	ops    []boxOperand
	texts  []string
	// common caches the CALL's folded kind — select_common_type over every
	// argument — as int32(k)+1, 0 while unsettled. It is what every pair
	// compares at, because PostgreSQL resolves GREATEST/LEAST's type ONCE and
	// coerces the unknown-typed literal to THAT: `GREATEST(bigint, '3.1',
	// double)` is a double comparison there and answers, where coercing the
	// literal per PAIR asked bigint's input function for it and raised 22P02.
	// Same publish boxOperand.kind uses, for the same reason.
	common atomic.Int32
}

// commonKind folds every non-quoted argument's kind through PostgreSQL's
// select_common_type ladder, and answers boxUnknown when the fold cannot be
// made — an argument whose declaration this layer cannot read makes the fold a
// LOWER BOUND on PostgreSQL's, and a lower bound is what refuses at the wrong
// width.
func (a *extremumArms) commonKind(b *batch.RecordBatch) boxKind {
	if v := a.common.Load(); v != 0 {
		return boxKind(v - 1)
	}
	kind, have, settled := boxUnknown, false, true
	for i := range a.ops {
		k := a.ops[i].resolve(b)
		if a.ops[i].kind.Load() == 0 {
			settled = false
		}
		if k == boxQuoted {
			continue // unknown-typed: it is the operand being coerced
		}
		k = foldKind(a.ops[i].expr, k)
		if !have {
			kind, have = k, true
			continue
		}
		w, ok := numericFoldOf(kind, k)
		if !ok {
			kind, have = boxUnknown, true
			break
		}
		kind = w
	}
	if !have {
		kind = boxUnknown
	}
	if settled {
		a.common.Store(int32(kind) + 1)
	}
	return kind
}

// materialize is the VALUE half of the same rule: the argument that wins is
// returned AT THE CALL'S TYPE, not in whatever box it arrived in.
//
// A QUOTED literal arrives as a Go string, and pickExtremum returned that
// string when the literal won — so `GREATEST(r_val, '16777217', d_val)`
// projected the four characters and the store raised "cannot store string into
// FLOAT64 vector", while the comparison that chose it had already been made at
// the wrong width. PostgreSQL answers the double 16777217.
func (a *extremumArms) materialize(b *batch.RecordBatch, idx int, v any) any {
	if a == nil || idx < 0 || idx >= len(a.ops) {
		return v
	}
	if a.ops[idx].resolve(b) != boxQuoted {
		return v
	}
	typ, ok := numberKindType(a.commonKind(b))
	if !ok {
		return v
	}
	text := a.texts[idx]
	// The value is materialized at the FOLD's type, never at the DECLARED one.
	// It feeds the COMPARISON above it as often as it feeds a store —
	// `GREATEST(r_val,'1e39',d_val) > 0` never projects anything — so narrowing
	// or refusing here would answer a different predicate than PostgreSQL's.
	// Making the STORE right is the declaration's business, and
	// expr.CommonDeclType folds a non-DECIMAL numeric list through this same
	// ladder now, so for those two the declaration and the fold agree and
	// nothing narrows at all.
	if typ == batch.TypeDecimal {
		// A DECIMAL's value IS its rendered text, which is the box it carries
		// everywhere else in the engine.
		return text
	}
	switch typ {
	case batch.TypeFloat64:
		f, st := kernel.FloatLitText(text, 64)
		if st != kernel.NumConstOK {
			return v
		}
		return f
	case batch.TypeFloat32:
		// float64 of the NARROWED value, which is how ColRef.Eval boxes a
		// real column — so a real-typed GREATEST and a real column agree on
		// the box as well as on the value.
		f, st := kernel.FloatLitText(text, 32)
		if st != kernel.NumConstOK {
			return v
		}
		return float64(float32(f))
	case batch.TypeInt32, batch.TypeInt64:
		n, st := kernel.IntLitText(text)
		if st != kernel.NumConstOK {
			return v
		}
		return n
	}
	return v
}

// armExtremumArms builds the per-argument table. It is always built — unlike
// armExtremum, which returns nil for a call that can never refuse — because
// the COMPARISON applies to every GREATEST/LEAST, not only to the ones holding
// a bad literal.
func armExtremumArms(argExprs []Expr) *extremumArms {
	a := &extremumArms{
		refuse: armExtremum(argExprs),
		ops:    make([]boxOperand, len(argExprs)),
		texts:  make([]string, len(argExprs)),
	}
	for i, e := range argExprs {
		a.ops[i].expr = e
		// operandLitText, not litText: a QUOTED literal argument is
		// unknown-typed and takes its neighbour's type, so `GREATEST(k, '2')`
		// is the integer comparison PostgreSQL makes it. litText saw only
		// numeric literals, which left the quoted spelling with no text and
		// therefore no rule.
		a.texts[i] = operandLitText(e)
	}
	return a
}

// order compares the values at argument indices li and ri under the rule
// their DECLARATIONS select, reports ok=false when none applies, and reports
// unknown when the rule that applies says the two have no comparable relation
// (a stored value that names no address — netOrder, ADR-0012 item 10).
//
// Both indices are bounds-checked against the armed table rather than assumed
// to line up: args can outrun argExprs when a vectorized caller evaluates
// values the expression list does not name, the same guard
// extremumRefusal.check carries.
func (a *extremumArms) order(b *batch.RecordBatch, li, ri int, lv, rv any) (c int, ok, unknown bool) {
	if a == nil || li < 0 || li >= len(a.ops) || ri < 0 || ri >= len(a.ops) {
		return 0, false, false
	}
	lk := a.ops[li].resolve(b)
	rk := a.ops[ri].resolve(b)
	// Every pair compares at the CALL's folded type, never at the type of the
	// argument this pair happens to hold: PostgreSQL coerces the unknown-typed
	// literal to select_common_type's answer once, for the whole call. Reading
	// each argument's own type instead made `GREATEST(r_key, '3.1', d_val)`
	// refuse with "for type bigint" on the (r_key, '3.1') pair, where
	// PostgreSQL folds to double precision and answers — the same finding #517
	// made about the refusal, one level down in the comparison.
	// Only the operand facing a QUOTED literal is retyped, and only then: the
	// fold exists to say what the unknown-typed literal is coerced to. A pair
	// of TYPED operands already has each side's own rule — retyping both to
	// the fold sent (an int64 box, a DECIMAL's text) to the two-DECIMALs arm,
	// which needs two strings, so it declined and pickExtremum picked the
	// winner with compare()'s byte order instead.
	if lk == boxQuoted || rk == boxQuoted {
		if ck := a.commonKind(b); isNumericFoldKind(ck) {
			switch {
			case lk == boxQuoted && rk == boxQuoted:
				// BOTH operands unknown-typed. PostgreSQL coerces every
				// unknown literal in the call to the resolved type, so this
				// pair is a comparison at ck too — and neither side being
				// retyped is how `GREATEST('3.1','12.75',a)` ordered its two
				// literals by BYTES and answered '3.1' where PostgreSQL
				// answers '12.75'.
				//
				// Only the LEFT is retyped: the arm below reads a TYPED
				// operand against a still-QUOTED one, so retyping both would
				// match no arm at all and fall back to the byte order this is
				// removing. The left operand's VALUE is its own text, which
				// every rung can read.
				lk = ck
			case rk == boxQuoted && isNumericFoldKind(lk):
				lk = ck
			case lk == boxQuoted && isNumericFoldKind(rk):
				rk = ck
			}
		}
	}
	if !pairApplies(lk, rk, a.texts[li], a.texts[ri]) {
		return 0, false, false
	}
	// The CALL's fold is the type for BOTH sides here, not each argument's
	// own: PostgreSQL resolves GREATEST/LEAST once and coerces every unknown
	// literal in it to that one type. Passing each operand's own fold put the
	// bare column's type back and re-refused `GREATEST(bigint,'3.1',numeric)`.
	ck := a.commonKind(b)
	return orderByKindsFold(lk, rk, ck, ck, lv, rv, a.texts[li], a.texts[ri])
}

// armExtremum returns nil — "this call can never refuse" — unless some
// argument is a QUOTED literal that SOME column type refuses (a non-zero
// quotedLitMask). That is the whole-node fast path, and it is the one every
// ordinary GREATEST/LEAST takes: nothing is resolved, nothing is parsed, and
// check() below returns on a nil receiver.
//
// There is deliberately no cached "settled" flag here, unlike refuseArm's.
// check() folds EVERY argument's column type on every call, so a flag would
// have to record which batch it was settled against; the work it would save is
// one atomic load per resolved column plus a mask lookup, and this node is
// only ever armed for a call that already holds a refusable literal.
func armExtremum(argExprs []Expr) *extremumRefusal {
	r := &extremumRefusal{
		cols:   make([]*ColRef, len(argExprs)),
		bad:    make([]string, len(argExprs)),
		mask:   make([]litRefusalMask, len(argExprs)),
		litTyp: make([]batch.TypeID, len(argExprs)),
	}
	any := false
	for i, e := range argExprs {
		if e == nil {
			continue
		}
		r.litTyp[i] = noLitType
		if col, ok := bareCol(e); ok {
			r.cols[i] = col
			continue
		}
		if text, ok := quotedLitText(e); ok {
			// Unknown-typed: it contributes nothing to the fold, which is
			// exactly what makes it the literal being coerced.
			if m := quotedLitMask(text); m != 0 {
				r.bad[i], r.mask[i] = text, m
				any = true
			}
			continue
		}
		if lit, ok := e.(*Lit); ok {
			if lit.Val == nil {
				continue // a NULL argument types nothing
			}
			if typ, ok := numericConstType(lit); ok {
				r.litTyp[i] = typ
				continue
			}
		}
	}
	if !any {
		return nil
	}
	return r
}

// noLitType marks a slot that contributes nothing to the common-type fold.
// TypeBool is the zero TypeID, so the absence has to be spelled out rather
// than left to the zero value.
const noLitType = batch.TypeVector + 1

// numericConstType is the type PostgreSQL gives an UNSUFFIXED numeric
// constant: `numeric` once it carries a decimal point or an exponent, and
// otherwise the narrowest integer type that holds it. Same rule as ADR-0012
// item 12's literal set-operation arm, and the same reason — a constant's type
// is part of what select_common_type resolves over.
func numericConstType(lit *Lit) (batch.TypeID, bool) {
	if lit.Text != "" {
		return NumericConstTypeOfText(lit.Text)
	}
	switch lit.Val.(type) {
	case int32:
		return batch.TypeInt32, true
	case int, int64:
		return batch.TypeInt64, true
	case float32:
		return batch.TypeFloat32, true
	case float64:
		return batch.TypeFloat64, true
	}
	return 0, false
}

// NumericConstTypeOfText is numericConstType over a constant's SPELLING alone,
// which is what the PLANNER has: physical.nodeDeclaredType types a literal from
// its AST text, long before a compiled *Lit with a box exists.
//
// It is exported so the declared-type fold (expr.CommonDeclType) and the boxed
// comparison layer resolve a constant's rung through ONE function. They fold
// the same composite and must not disagree about it: the comparison decides
// which argument wins and the declaration decides the vector the winner is
// stored in, and a disagreement between them is a value narrowed or wrapped on
// the way out (#724).
func NumericConstTypeOfText(text string) (batch.TypeID, bool) {
	if strings.ContainsAny(text, ".eE") {
		return batch.TypeDecimal, true
	}
	if n, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64); err == nil {
		if n >= math.MinInt32 && n <= math.MaxInt32 {
			return batch.TypeInt32, true
		}
		return batch.TypeInt64, true
	}
	return batch.TypeDecimal, true
}

// QuotedLitDecimalType is the fixed-point (p,s) a QUOTED literal contributes to
// a DECIMAL fold: its spelling's, exactly.
//
// It is deliberately NOT LiteralChoiceDecimalType, which is the same question
// for an UNSUFFIXED numeric constant and carries one extra qualification — that
// the box compileLit built round-trips the spelling. That qualification is
// about the box: a choice construct hands over whatever box the winning arm
// produced, and past a double's ~17 significant digits a numeric literal's box
// has already lost digits.
//
// A quoted literal has no such box. It arrives as its own TEXT, and the
// constructs that choose between arms hand that text on unchanged — a DECIMAL
// value IS its rendered text everywhere in this engine — so the spelling
// reaches the store intact and the fold may declare a (p,s) wide enough for it.
// `GREATEST(numeric(15,2), '12.750000000000000001')` therefore keeps every
// digit, which is what PostgreSQL answers.
//
// ok=false for a spelling the carrier cannot hold at all ('1e39' needs 40
// digits): the fold then declares the DECIMAL its typed operands agree on and
// the store raises 22003 rather than wrapping (ADR-0024 items 1 and 4).
func QuotedLitDecimalType(text string) (batch.DecimalType, bool) {
	t, _, ok := litDecimal(text)
	return t, ok
}

// check is refuseArm.check for the WHOLE call, not for one (best, candidate)
// pair — because that is what PostgreSQL asks.
//
// GREATEST/LEAST resolve ONE common type over EVERY argument
// (select_common_type) and coerce the unknown-typed literal to THAT, so the
// type in the message is not a property of whichever pair the values selected.
// Verified live on postgres:17-alpine over a table with a bigint, a real and a
// double column:
//
//	GREATEST(bigint, 'abc')                -> ... for type bigint
//	GREATEST(bigint, 'abc', double)        -> ... for type double precision
//	GREATEST(real,   'abc', bigint)        -> ... for type real
//
// Refusing against the pair's column instead named bigint for the second and
// third, which is a different type in the message for the same query — and it
// re-introduced #517's own finding one level down: a refusal that depends on
// which operand won a comparison is not a type rule.
// folded is the CALL's common type when the arms could fold one
// (extremumArms.commonKind), and foldedOK=false when they could not — an
// argument whose declaration this layer cannot read. Only the first case can
// refuse a literal that SOME numeric type accepts, because a fold that missed
// an argument is a LOWER BOUND on PostgreSQL's: `GREATEST(k, '3.1', d_val)`
// folds to double there and ANSWERS, and refusing it against the columns this
// layer happened to see was a PG-superset regression.
//
// Where the fold failed, only a literal EVERY numeric type refuses is safe to
// raise on — `GREATEST(k, 'abc', <a subquery>)` refuses whatever the subquery
// turns out to be — and the type NAMED in that message is the column-only
// fold, which can differ from PostgreSQL's when the unreadable argument would
// have widened it. A missed refusal is the conservative side; the plan-time
// binder catches the shapes it can prove.
func (r *extremumRefusal) check(b *batch.RecordBatch, folded batch.TypeID, foldedOK bool) {
	if r == nil {
		return
	}
	typ, ok := folded, foldedOK
	if !ok {
		typ, ok = r.commonType(b)
		if !ok {
			return
		}
	}
	for i, m := range r.mask {
		if m == 0 {
			continue
		}
		if !foldedOK && !m.refusesEveryNumericType() {
			continue
		}
		if st, refuse := m.refuses(typ); refuse {
			raiseQuotedLitRefusal(typ, r.bad[i], st)
		}
	}
}

// checkRefusal is extremumArms' own entry to it, so the refusal and the
// COMPARISON fold the call's type through exactly one function.
func (a *extremumArms) checkRefusal(b *batch.RecordBatch) {
	if a == nil {
		return
	}
	typ, ok := numberKindType(a.commonKind(b))
	a.refuse.check(b, typ, ok)
}

// commonType folds this call's COLUMN arguments to the one type PostgreSQL's
// select_common_type resolves them to, and reports ok=false when no argument
// is a column that resolves in this batch (nothing to refuse against yet — an
// unresolved name says nothing about the next batch).
func (r *extremumRefusal) commonType(b *batch.RecordBatch) (batch.TypeID, bool) {
	var out batch.TypeID
	have := false
	fold := func(t batch.TypeID) {
		if !have {
			out, have = t, true
			return
		}
		out = widerNumericType(out, t)
	}
	for i, col := range r.cols {
		if col != nil {
			col.resolve(b)
			if col.idx >= 0 && col.idx < len(b.Columns) {
				fold(col.typ)
			}
			continue
		}
		if i < len(r.litTyp) && r.litTyp[i] != noLitType {
			fold(r.litTyp[i])
		}
	}
	return out, have
}

// widerNumericType is PostgreSQL's numeric preference order for
// select_common_type, which ADR-0012 item 12 pins for set operations and which
// GREATEST/LEAST use too — with float4 inserted where live `pg_typeof` puts
// it: `GREATEST(bigint, real)` is REAL and `GREATEST(real, double)` is DOUBLE
// PRECISION, so real outranks numeric and double outranks real.
//
//	INT32 < INT64 < DECIMAL < FLOAT32 < FLOAT64
//
// The wadjet-native PORT/PROTOCOL rank with INT32 and DURATION with INT64:
// they ARE those storage types and their literal rule is the integer one.
// Two types with no rank at all (a STRING and a DATE, say) fold to the FIRST,
// which refuses nothing — the conservative answer, since a type with no rule
// contributes no refusal either way.
func widerNumericType(a, b batch.TypeID) batch.TypeID {
	ra, oka := numericRank(a)
	rb, okb := numericRank(b)
	if !oka || !okb {
		if oka {
			return b
		}
		return a
	}
	if rb > ra {
		return b
	}
	return a
}

func numericRank(t batch.TypeID) (int, bool) {
	switch t {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol:
		return 0, true
	case batch.TypeInt64, batch.TypeDuration:
		return 1, true
	case batch.TypeDecimal:
		return 2, true
	case batch.TypeFloat32:
		return 3, true
	case batch.TypeFloat64:
		return 4, true
	}
	return 0, false
}

// newDecimalLitCmp builds a binding and resolves every literal's Numeric()
// answer once, up front — see decimalLitCmp.numeric. nonAddr is resolved the
// same way, from the operand EXPRESSIONS, because a literal's text is fixed
// for the query's lifetime.
func newDecimalLitCmp(col *ColRef, lits []*kernel.DecimalLiteral, flip bool, operands ...Expr) *decimalLitCmp {
	numeric := make([]bool, len(lits))
	for i, lit := range lits {
		numeric[i] = lit.Numeric()
	}
	nonAddr, nonAddrSet := firstNonAddressLit(operands)
	return &decimalLitCmp{
		col: col, lits: lits, flip: flip, numeric: numeric,
		nonAddr: nonAddr, nonAddrSet: nonAddrSet,
	}
}

// firstNonAddressLit reports the text of the first QUOTED string literal in
// operands that parses as no address, and ok=false when there is none. The
// text can legitimately be "" — see decimalLitCmp.nonAddrSet.
//
// Quoted only: a bare numeric literal against an address column is a
// different refusal (PostgreSQL has no `inet = integer` operator at all,
// which is 42883, not 22P02) and is left where it was.
//
// Checks all five address families the DECIMAL-shaped binding can be asked
// to refuse against (#519, one type over from #492's CIDR/IPv6): a `col =
// lit` comparison never reaches here for a literal that parses as one of
// these (compileCmp's tryNetworkLit already built a CmpNetworkLit for it
// before NewCmp ever runs), but BETWEEN and IN have no such pre-filter —
// bindDecimalList binds straight off the operand list — so a literal that
// DOES name a valid address must still be excluded here or `c_ipv4 BETWEEN
// '10.0.0.1' AND '10.0.0.5'` would misreport its own bounds as unparseable.
func firstNonAddressLit(operands []Expr) (string, bool) {
	for _, e := range operands {
		lit, ok := e.(*Lit)
		if !ok || lit.Text != "" {
			continue
		}
		s, ok := lit.Val.(string)
		if !ok {
			continue
		}
		if _, ok := kernel.CidrSortKey(s); ok {
			continue
		}
		if _, ok := kernel.IPv6LitKey(s); ok {
			continue
		}
		if _, ok := ipv4LitToInt64(s); ok {
			continue
		}
		if _, ok := macLitToInt64(s); ok {
			continue
		}
		if parseUUIDHex(s) != nil {
			continue
		}
		return s, true
	}
	return "", false
}

// bindDecimalCmp binds `col op lit` or `lit op col`, in either operand order.
func bindDecimalCmp(left, right Expr) *decimalLitCmp {
	if col, ok := bareCol(left); ok {
		if lit, ok := numericLit(right); ok {
			return newDecimalLitCmp(col, []*kernel.DecimalLiteral{lit}, false, right)
		}
		return nil
	}
	if col, ok := bareCol(right); ok {
		if lit, ok := numericLit(left); ok {
			return newDecimalLitCmp(col, []*kernel.DecimalLiteral{lit}, true, left)
		}
	}
	return nil
}

// bindDecimalList binds `col IN (lit, ...)` / `col BETWEEN lit AND lit`. It
// binds only when EVERY operand is a numeric literal: a mixed list would need
// two comparison rules inside one predicate, and one rule per predicate is
// the property that keeps the paths from disagreeing.
func bindDecimalList(col Expr, values []Expr) *decimalLitCmp {
	c, ok := bareCol(col)
	if !ok || len(values) == 0 {
		return nil
	}
	lits := make([]*kernel.DecimalLiteral, len(values))
	for i, v := range values {
		lit, ok := numericLit(v)
		if !ok {
			return nil
		}
		lits[i] = lit
	}
	return newDecimalLitCmp(c, lits, false, values...)
}

// vector returns the batch's DECIMAL column for this binding, or nil when the
// exact path does not apply: an unresolved name, a column of any other type,
// or a VIEW whose values live in a base vector through an index (the decimal
// kernels read DecimalData directly, as every other kernel does).
func (d *decimalLitCmp) vector(b *batch.RecordBatch) *batch.Vector {
	if d.notDecimal.Load() {
		return nil
	}
	d.col.resolve(b)
	if d.col.idx < 0 || d.col.idx >= len(b.Columns) || d.col.typ != batch.TypeDecimal {
		if d.col.idx >= 0 && d.col.idx < len(b.Columns) {
			d.refuseNonAddress()
		}
		d.notDecimal.Store(true)
		return nil
	}
	v := b.Columns[d.col.idx]
	if v.Base != nil || len(v.DecimalData.Data) == 0 {
		return nil
	}
	return v
}

// refuseNonAddress raises 22P02 when the bound column turns out to be an
// ADDRESS column and a literal operand names no address.
//
// It runs on the one batch that settles notDecimal, so it costs nothing per
// row: the answer depends only on the column's type and the literal's text,
// neither of which changes across a query. The kernel path raises the same
// error for the same literal (exec.networkConstError), which is the property
// that makes the refusal a semantics decision rather than a path accident.
func (d *decimalLitCmp) refuseNonAddress() {
	if !d.nonAddrSet {
		return
	}
	switch d.col.typ {
	case batch.TypeCIDR:
		raiseInvalidTextRepresentation("cidr", d.nonAddr)
	case batch.TypeIPv6, batch.TypeIPv4:
		// PostgreSQL has one type, inet, for both v4 and v6 addresses.
		raiseInvalidTextRepresentation("inet", d.nonAddr)
	case batch.TypeMAC:
		raiseInvalidTextRepresentation("macaddr", d.nonAddr)
	case batch.TypeUUID:
		raiseInvalidTextRepresentation("uuid", d.nonAddr)
	}
}

// order compares the row's value against literal i, as -1, 0 or +1, with the
// operand order the expression was written in.
//
// A literal that is not a number ABORTS the query here rather than answering.
// The column is a DECIMAL by the time this runs, so PostgreSQL's answer to
// `d = 'abc'` is a 22P02, and the alternative — the old silent reading of an
// unparseable constant as the value zero — matched every row holding zero
// (#463). The kernel path raises the same error from decimalConstError, so
// both paths refuse the same query.
func (d *decimalLitCmp) order(vec *batch.Vector, row, i int) int {
	lit := d.lits[i]
	if !d.numeric[i] {
		raiseInvalidTextRepresentation("numeric", lit.Text())
	}
	c := lit.Order(vec, row)
	if d.flip {
		return -c
	}
	return c
}

// cmpOrder turns an ordering into the answer for one comparison operator.
func cmpOrder(c int, op CmpOp) bool {
	switch op {
	case CmpEq:
		return c == 0
	case CmpNe:
		return c != 0
	case CmpLt:
		return c < 0
	case CmpLe:
		return c <= 0
	case CmpGt:
		return c > 0
	case CmpGe:
		return c >= 0
	}
	return false
}

// decimalColCmp binds two BARE COLUMN operands so that when both turn out to
// be DECIMALs the comparison happens on the unscaled Int128s, at whatever
// scales the two columns declare.
//
// The boxed path cannot do this one. Both operands reach it as their RENDERED
// TEXT, and compare()'s two-strings fast path answers first — lexicographically,
// where "10.001" sorts below "2.0002" — so nothing about the BOX distinguishes
// two DECIMAL columns from two ordinary string columns. Only the DECLARATION
// does, which is ADR-0012 item 8's rule and the reason this is a binding
// rather than another branch in compare(). Two DECIMALs of different SCALE are
// the case that matters even for equality: "1.50" and "1.5000" are the same
// number and different text (#477).
type decimalColCmp struct {
	left, right *ColRef
	// notDecimal caches a settled "this pair is not two DECIMAL columns".
	// See decimalLitCmp.notDecimal for why it is atomic.
	notDecimal atomic.Bool
}

// bindDecimalCols binds `col op col`. Nil for every other operand shape.
func bindDecimalCols(left, right Expr) *decimalColCmp {
	l, ok := bareCol(left)
	if !ok {
		return nil
	}
	r, ok := bareCol(right)
	if !ok {
		return nil
	}
	return &decimalColCmp{left: l, right: r}
}

// vectors returns the batch's two DECIMAL columns for this binding, or nil
// when the exact path does not apply.
func (d *decimalColCmp) vectors(b *batch.RecordBatch) (*batch.Vector, *batch.Vector) {
	if d.notDecimal.Load() {
		return nil, nil
	}
	lv := decimalVectorOf(d.left, b)
	rv := decimalVectorOf(d.right, b)
	if lv == nil || rv == nil {
		// Only a resolved NON-DECIMAL type is permanent; an empty column in
		// one batch says nothing about the next.
		if (lv == nil && d.left.typ != batch.TypeDecimal) ||
			(rv == nil && d.right.typ != batch.TypeDecimal) {
			d.notDecimal.Store(true)
		}
		return nil, nil
	}
	return lv, rv
}

// decimalVectorOf resolves one bound column to its DECIMAL vector, or nil for
// any other type, an unresolved name, or a VIEW whose values live in a base
// vector through an index (the decimal kernels read DecimalData directly).
func decimalVectorOf(col *ColRef, b *batch.RecordBatch) *batch.Vector {
	col.resolve(b)
	if col.idx < 0 || col.idx >= len(b.Columns) || col.typ != batch.TypeDecimal {
		return nil
	}
	v := b.Columns[col.idx]
	if v.Base != nil || len(v.DecimalData.Data) == 0 {
		return nil
	}
	return v
}

// decimalTextOrder orders a NUMBER against a DECIMAL value that reached the
// boxed path as its rendered text, returning -1, 0 or +1 for num against the
// text, and ok=false when the text is not a number (an ordinary string
// column's value, which must keep comparing as a string).
//
// Two different rules, both PostgreSQL's:
//
//   - Against an INTEGER the comparison is EXACT. batch.DecimalTextAt at
//     scale 0 truncates the text to its integer part and reports the sign of
//     what it dropped, and ScaledDecimal.Order settles the tie with that
//     residual — so 3 vs "3.0000" is equal, 3 vs "3.0001" is less, and
//     nothing is rounded on either side. It saturates too, so a text value
//     wider than Int128 still orders (#462).
//   - Against a FLOAT the comparison is a float64 one, because that is what
//     PostgreSQL does with `numeric <op> double precision`: it casts the
//     numeric. Verified on live PostgreSQL — `9007199254740993::numeric =
//     9007199254740992::float8` is TRUE.
func decimalTextOrder(num any, text string) (int, bool) {
	switch v := num.(type) {
	case int64:
		return decimalTextOrderInt(v, text)
	case int:
		return decimalTextOrderInt(int64(v), text)
	case int32:
		return decimalTextOrderInt(int64(v), text)
	case float64:
		return decimalTextOrderFloat(v, text)
	case float32:
		return decimalTextOrderFloat(float64(v), text)
	}
	return 0, false
}

func decimalTextOrderInt(v int64, text string) (int, bool) {
	sd, ok := batch.DecimalTextAt(text, 0)
	if !ok {
		return 0, false
	}
	return sd.Order(batch.Int128From(v)), true
}

func decimalTextOrderFloat(v float64, text string) (int, bool) {
	// NaN and ±Infinity FIRST, because a FLOAT holds all three and its rule for
	// them is the ordinary float one — kernel.CompareFloat64, which IS
	// PostgreSQL's float order (ADR-0012 item 8). Falling through to the shape
	// test below sent them to compare()'s LEXICOGRAPHIC string comparison,
	// which agrees with PostgreSQL only when every value happens to be
	// non-negative: over {-5, 0, 5}, `f > '-Infinity'` dropped the two negative
	// rows because "-5" sorts below "-Infinity" as text. The DECIMAL rule is
	// the DIFFERENT one and is applied elsewhere (#534/ADR-0024 item 6): there
	// the literal is a BOUND, because the carrier has no such value.
	//
	// This is the DECIMAL-against-a-float-box arm now. A QUOTED literal meeting
	// a FLOAT column takes quotedNumberOrder instead (#646), which reads the
	// column's own input grammar at the column's own width and REFUSES what it
	// cannot read — this function's ok=false, which falls through to compare(),
	// is a value answer to a question PostgreSQL raises on.
	//
	// kernel.FloatSpecialText, not batch.DecimalSpecialText: float8 accepts a
	// SIGNED NaN and numeric does not, and that difference is PostgreSQL's.
	if f, ok := kernel.FloatSpecialText(text); ok {
		return kernel.CompareFloat64(v, f), true
	}
	// The numeric SHAPE is settled by the same parser the exact arm uses, so
	// the two agree about which strings are numbers at all: strconv.ParseFloat
	// alone would accept "NaN" and "Inf", which no DECIMAL column renders —
	// and which the arm above has already answered for.
	if _, ok := batch.DecimalTextAt(text, 0); !ok {
		return 0, false
	}
	// TrimSpace is safe here only because the gate above already refused
	// anything whose surrounding space is not PostgreSQL's C set: everything
	// that reaches this line trims identically under both rules.
	f, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	if err != nil {
		return 0, false
	}
	return kernel.CompareFloat64(v, f), true
}

// litText reports a numeric literal operand's exact source text, and "" for
// every other operand. Text is set only for a numeric literal (compileLit), so
// it doubles as the test for "this operand is a number the user wrote".
func litText(e Expr) string {
	if l, ok := e.(*Lit); ok {
		return l.Text
	}
	return ""
}

// negateLitText flips the sign of a numeric literal's source text, so folding
// a unary minus into the literal keeps the exact text alongside the box.
func negateLitText(text string) string {
	switch {
	case text == "":
		return ""
	case text[0] == '-':
		return text[1:]
	case text[0] == '+':
		return "-" + text[1:]
	default:
		return "-" + text
	}
}

// isFiniteNumericLitText reports whether a QUOTED string literal's content
// names a FINITE number. compileWithCtx's unary minus fold (compile.go) uses
// it to choose between folding `-'5.00'` into a literal and refusing `-'abc'`
// at compile time (#505).
//
// Deliberately the NARROW reader, unlike IsNumericLiteralText below: unary
// minus over NaN or an infinity is not a value in PostgreSQL either (`-'NaN'`
// is 42725, "operator is not unique: - unknown"), and folding it would produce
// the text '-NaN', which PostgreSQL's numeric input refuses outright. So
// `-'NaN'` stays the compile-time refusal it already was (kernel.
// FiniteDecimalText carries the reasoning).
func isFiniteNumericLitText(s string) bool {
	return kernel.FiniteDecimalText(s)
}

// IsNumericLiteralText reports whether a QUOTED string literal's content names
// a value a DECIMAL column can be COMPARED against: the plan-time refusal of a
// non-numeric constant against a DECIMAL column (#517) must accept and refuse
// exactly the strings the runtime refusal does, or a query would be refused at
// one and answered at the other — the two-path defect class the refusal exists
// to close.
//
// It is the DECIMAL arm of kernel.QuotedLitStatus, which is what every site
// calls now that the rule covers the whole numeric family (#646); this stays
// as the type's own predicate, and as the spelling the DECIMAL gates name.
//
// So it is `kernel.DecimalLiteral.Numeric()` itself, the runtime predicate,
// which since #534 accepts PostgreSQL's NaN and ±Infinity spellings alongside
// the finite numbers: none of the three is a value a DECIMAL column holds, all
// three are bounds it can be ordered against, and refusing them here would put
// the plan-time refusal back in front of a query PostgreSQL answers (ADR-0024
// item 6).
func IsNumericLiteralText(s string) bool { return kernel.NewDecimalLiteral(s).Numeric() }
