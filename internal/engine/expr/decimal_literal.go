package expr

import (
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

	// settled caches "this column is not a DECIMAL", the way
	// decimalLitCmp.notDecimal does and for the same reason — including why
	// it is atomic: these nodes are shared across parallel pipeline workers
	// evaluating one batch, and concurrent writers can only ever agree.
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
	lit, ok := numericLit(other)
	if !ok || lit.Numeric() {
		return noRefusal
	}
	return &refuseArm{col: col, text: lit.Text()}
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

// check raises when the armed column is a materialized DECIMAL in THIS batch.
// Once it has seen the column resolve to anything else the whole call is one
// atomic load, which is the point.
func (a *refuseArm) check(b *batch.RecordBatch) {
	if a.col == nil || a.settled.Load() {
		return
	}
	a.col.resolve(b)
	if a.col.idx < 0 || a.col.idx >= len(b.Columns) {
		// Unresolved says nothing about the next batch, so do not settle.
		return
	}
	if a.col.typ != batch.TypeDecimal {
		a.settled.Store(true)
		return
	}
	raiseInvalidTextRepresentation("numeric", a.text)
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
	// bad[i] is argument i's literal text when that text names no number,
	// else "". A pair refuses when one side has a col and the other a bad.
	bad []string
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
// their DECLARATIONS select, or reports ok=false when none applies.
//
// Both indices are bounds-checked against the armed table rather than assumed
// to line up: args can outrun argExprs when a vectorized caller evaluates
// values the expression list does not name, the same guard
// extremumRefusal.check carries.
func (a *extremumArms) order(b *batch.RecordBatch, li, ri int, lv, rv any) (int, bool) {
	if a == nil || li < 0 || li >= len(a.ops) || ri < 0 || ri >= len(a.ops) {
		return 0, false
	}
	lk := a.ops[li].resolve(b)
	rk := a.ops[ri].resolve(b)
	if !pairApplies(lk, rk, a.texts[li], a.texts[ri]) {
		return 0, false
	}
	return orderByKinds(lk, rk, lv, rv, a.texts[li], a.texts[ri])
}

// armExtremum returns nil — "this call can never refuse" — unless some
// argument is a literal naming no number. That is the whole-node fast path,
// and it is the one every ordinary GREATEST/LEAST takes: nothing is resolved,
// nothing is parsed, and check() below returns on a nil receiver.
//
// There is deliberately no cached "settled" flag here, unlike refuseArm's.
// The pairs this checks involve DIFFERENT columns as the best-so-far moves,
// so one column resolving to a non-DECIMAL says nothing about the next —
// caching it would disarm a real refusal in `GREATEST(s, 'abc', d)`.
func armExtremum(argExprs []Expr) *extremumRefusal {
	r := &extremumRefusal{cols: make([]*ColRef, len(argExprs)), bad: make([]string, len(argExprs))}
	any := false
	for i, e := range argExprs {
		if e == nil {
			continue
		}
		if col, ok := bareCol(e); ok {
			r.cols[i] = col
			continue
		}
		if lit, ok := numericLit(e); ok && !lit.Numeric() {
			r.bad[i] = lit.Text()
			any = true
		}
	}
	if !any {
		return nil
	}
	return r
}

// check is refuseArm.check for the (best, candidate) pair at indices bi and
// ci, applying the same left-operand-first rule armRefusal does.
func (r *extremumRefusal) check(b *batch.RecordBatch, bi, ci int) {
	// args can outrun argExprs (a vectorized caller evaluates values the
	// expression list does not name), so both indices are bounds-checked
	// against the armed table rather than assumed to line up.
	if r == nil || bi < 0 || bi >= len(r.cols) || ci < 0 || ci >= len(r.cols) {
		return
	}
	col, text := r.cols[bi], r.bad[ci]
	if col == nil {
		col, text = r.cols[ci], r.bad[bi]
	}
	if col == nil || text == "" {
		return
	}
	col.resolve(b)
	if col.idx < 0 || col.idx >= len(b.Columns) || col.typ != batch.TypeDecimal {
		return
	}
	raiseInvalidTextRepresentation("numeric", text)
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
	// The numeric SHAPE is settled by the same parser the exact arm uses, so
	// the two agree about which strings are numbers at all: strconv.ParseFloat
	// alone would accept "NaN" and "Inf", which no DECIMAL column renders.
	if _, ok := batch.DecimalTextAt(text, 0); !ok {
		return 0, false
	}
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

// isNumericLitText reports whether a QUOTED string literal's content names a
// number, using the same shape test a DECIMAL comparison's refusal already
// uses (kernel.DecimalLiteral.Numeric — isDecimalText → batch.DecimalTextAt).
// One test decides both "does this parse" questions: compileWithCtx's unary
// minus fold (compile.go) uses it to choose between folding `-'5.00'` into a
// literal and refusing `-'abc'` at compile time (#505).
func isNumericLitText(s string) bool {
	return kernel.NewDecimalLiteral(s).Numeric()
}

// IsNumericLiteralText is isNumericLitText for the planner, which asks the
// same question one layer up: the plan-time refusal of a non-numeric constant
// against a DECIMAL column (#517) must accept and refuse exactly the strings
// the runtime refusal does, or a query would be refused at one and answered at
// the other — the two-path defect class the refusal exists to close.
func IsNumericLiteralText(s string) bool { return isNumericLitText(s) }
