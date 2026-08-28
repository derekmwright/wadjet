package exec

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Predicate evaluates a row and returns true if it passes the filter.
type Predicate func(b *batch.RecordBatch, row int) bool

// CompareOp represents a comparison operation.
type CompareOp int

const (
	OpEq CompareOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpIsNull
	OpIsNotNull
)

// Filter is a UnaryOperator that filters rows using a selection vector.
type Filter struct {
	Pred   Predicate
	selBuf []uint32 // reusable selection vector to avoid per-batch allocation
}

func NewFilter(pred Predicate) *Filter {
	return &Filter{Pred: pred}
}

func (f *Filter) Init(_ context.Context) error { return nil }

func (f *Filter) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if cap(f.selBuf) < in.Len {
		f.selBuf = make([]uint32, 0, in.Len)
	}
	sel := f.selBuf[:0]

	if in.Sel != nil {
		for _, idx := range in.Sel {
			if f.Pred(in, int(idx)) {
				sel = append(sel, idx)
			}
		}
	} else {
		for i := 0; i < in.Len; i++ {
			if f.Pred(in, i) {
				sel = append(sel, uint32(i))
			}
		}
	}

	f.selBuf = sel // save grown slice for next call

	if len(sel) == 0 {
		return nil, nil
	}

	in.Sel = sel
	return in, nil
}

func (f *Filter) Close() error { return nil }

// Clone returns a new Filter that shares the same predicate closure but has
// its own scratch buffer, allowing concurrent Execute calls.
func (f *Filter) Clone() UnaryOperator {
	return &Filter{Pred: f.Pred}
}

// lazyColIdx resolves a column index once and publishes it to every
// goroutine that shares the closure holding it.
//
// A predicate closure is SHARED, not cloned: Filter.Clone gives each parallel
// worker its own scratch buffer and the same Pred, which is the whole point —
// the closure is meant to be stateless. A plain captured `cachedIdx` int is
// therefore a data race between the worker that resolves it and every worker
// that reads it. Pipeline.runParallel's single-threaded warm-up pass fills
// the cache first and usually hides this; a predicate the warm-up batch never
// reaches (a conjunct that short-circuits ahead of it) is resolved by the
// workers themselves, concurrently.
//
// The shape is expr.ColRef's, for its reason: an inlined acquire load in the
// innermost row loop, where sync.Once.Do costs a call plus a closure build
// per row. idx is written before resolved is stored, and read only after
// resolved loads true.
type lazyColIdx struct {
	resolved atomic.Bool
	mu       sync.Mutex
	idx      int
}

// get returns the index of name in b, resolving it on first use. A name the
// batch does not carry resolves to -1 and stays there, exactly as the
// captured-int version did.
func (c *lazyColIdx) get(b *batch.RecordBatch, name string) int {
	if c.resolved.Load() {
		return c.idx
	}
	return c.resolveSlow(b, name)
}

func (c *lazyColIdx) resolveSlow(b *batch.RecordBatch, name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.resolved.Load() {
		c.idx = b.ColumnIndex(name)
		c.resolved.Store(true)
	}
	return c.idx
}

// ColumnCompare creates a predicate that compares a column against a constant value.
func ColumnCompare(colName string, op CompareOp, value any) Predicate {
	return ColumnCompareLit(colName, op, value, "")
}

// ColumnCompareLit is ColumnCompare for a constant that came from a numeric
// literal, carrying the literal's exact source text so a DECIMAL column is
// compared in its own domain rather than against a float64 (#452).
func ColumnCompareLit(colName string, op CompareOp, value any, litText string) Predicate {
	if value == nil && op != OpIsNull && op != OpIsNotNull {
		// A nil constant is NOT the type's zero (mirrors ResolveFilterKernel's
		// guard in kernel/compare.go): fmt.Sprint(nil)="<nil>", toInt64(nil)=0,
		// toBool(nil)=false, parseIPv4FilterVal(nil)=0 would otherwise let a
		// NULL-literal comparison silently match the row holding the zero
		// value instead of matching none. No caller currently reaches this
		// with a nil value (producers lower a NULL literal to
		// MatchNothingFilter before it gets here) — this is a floor guard,
		// not a reachable path today.
		return func(*batch.RecordBatch, int) bool { return false }
	}
	col := &lazyColIdx{}
	// Every literal conversion below is a pure function of `value`, so it
	// happens ONCE, here, rather than behind a per-closure "resolved" flag
	// inside the predicate. Those flags were a data race, not a cache:
	// Filter.Clone hands the SAME closure to every parallel worker, so one
	// worker's `cachedNetStr = ...` raced every other worker's read of it —
	// and a string header is two words, so a torn read is a garbage
	// comparison or a segfault, not merely a stale value. Hoisting also
	// removes the branch from the row loop.
	strVal := fmt.Sprint(value)
	intVal := toInt64(value)
	floatVal := toFloat64(value)
	ipv4Val := parseIPv4FilterVal(value)
	macVal := parseMACFilterVal(value)
	ipv6Val := parseIPv6FilterVal(value)
	uuidVal, uuidOK := kernel.UUIDLiteralToRaw(strVal)
	// A BOOL column's literal is read through PostgreSQL's boolean input
	// grammar, exactly as the vectorized kernel does (#574). boolOK false
	// means the text names no boolean; the arm below raises 22P02 the same
	// way the UUID arm does, rather than the old `value.(bool)` — which
	// PANICKED on any string literal and, for a Go-bool parameter, ignored
	// the string spellings entirely.
	boolVal, boolOK := kernel.BoolFilterConst(value)
	bytesVal := bytesFilterVal(value)
	// Offsets-shape: comparing a string column against the empty string is
	// a zero-length test, not a byte compare — and it is what keeps such a
	// column eligible for the lengths-only scan decode.
	emptyTest := strVal == "" && (op == OpEq || op == OpNe)
	// A DECIMAL constant is resolved from TEXT at the column's scale. The
	// literal's own text when there is one — fmt.Sprint of the float64 box
	// has already lost the digits past a double (#452) — and the box's
	// rendering otherwise, which is what the kernel does with it too.
	decText := strVal
	if litText != "" {
		decText = litText
	}
	decLit := kernel.NewDecimalLiteral(decText)
	kernelOp := toKernelOp(op)
	return func(b *batch.RecordBatch, row int) bool {
		idx := col.get(b, colName)
		if idx < 0 {
			return false
		}
		v := b.Columns[idx]

		if op == OpIsNull {
			return v.Nulls.IsNull(row)
		}
		if op == OpIsNotNull {
			return !v.Nulls.IsNull(row)
		}

		if v.Nulls.IsNull(row) {
			return false
		}

		switch v.Type {
		case batch.TypeInt64, batch.TypeTimestamp:
			return compareInt64(v.Int64Data[row], intVal, op)
		case batch.TypeInt32:
			return compareInt64(int64(v.Int32Data[row]), intVal, op)
		case batch.TypeFloat64:
			return compareFloat64(v.Float64Data[row], floatVal, op)
		case batch.TypeFloat32:
			return compareFloat64(float64(v.Float32Data[row]), floatVal, op)
		case batch.TypeString:
			if emptyTest {
				empty := v.BytesData.LengthAt(row) == 0
				return empty == (op == OpEq)
			}
			return compareString(v.BytesData.UnsafeStringValue(row), strVal, op)
		case batch.TypeBool:
			// boolOK false means the literal names no boolean — the same
			// refusal networkConstError/boolConstError make for the
			// vectorized kernel path, raised here via the pipeline's
			// FatalEvalPanic shape since this path has no error return
			// (mirrors the TypeUUID arm above).
			if !boolOK {
				panic(fatalEvalError{sqlerr.New("22P02", "invalid input syntax for type boolean: %q", strVal)})
			}
			if op == OpEq {
				return v.BoolData[row] == boolVal
			}
			if op == OpNe {
				return v.BoolData[row] != boolVal
			}
		case batch.TypeIPv4:
			return compareInt64(v.Int64Data[row], ipv4Val, op)
		case batch.TypeMAC:
			return compareInt64(v.Int64Data[row], macVal, op)
		case batch.TypeIPv6:
			return compareString(v.BytesData.UnsafeStringValue(row), ipv6Val, op)
		case batch.TypeCIDR:
			return compareString(v.BytesData.UnsafeStringValue(row), strVal, op)
		case batch.TypePort, batch.TypeProtocol:
			return compareInt64(int64(v.Int32Data[row]), intVal, op)
		case batch.TypeDuration:
			return compareInt64(v.Int64Data[row], intVal, op)
		case batch.TypeUUID:
			// A UUID column stores 16 RAW bytes; the literal is 36 characters
			// of text. Comparing them directly could never match, so
			// `WHERE uuid_col = '…'` silently returned no rows.
			//
			// uuidOK false means the literal names no UUID at all —
			// `networkConstError`'s TypeUUID arm is this same rule for the
			// vectorized kernel path (#519); this is the last-resort
			// row-at-a-time path (ColumnCompareLit, reached from
			// physical/plan.go and worker/executor_fragment.go), which has
			// no error return of its own, so it raises the same 22P02 via the
			// pipeline's own FatalEvalPanic shape rather than silently
			// comparing against the empty string — which used to match
			// nothing for `=` and every row for `<>`, one bad literal
			// answering a whole query wrong in two different directions.
			if !uuidOK {
				panic(fatalEvalError{sqlerr.New("22P02", "invalid input syntax for type uuid: %q", strVal)})
			}
			return compareString(v.BytesData.UnsafeStringValue(row), uuidVal, op)
		case batch.TypeDate:
			return compareInt64(int64(v.Int32Data[row]), intVal, op)
		case batch.TypeBytes:
			// BYTES compares by bytes, like STRING over the same arena. The
			// row fallback had no arm and fell to `return false`, so a
			// predicate over a BYTES column that reached this path dropped
			// every row (same class as #401's missing kernel).
			return compareString(v.BytesData.UnsafeStringValue(row), bytesVal, op)
		case batch.TypeDecimal:
			// Exact, at the column's own scale — the same rule the
			// vectorized kernel applies, so the two paths cannot disagree.
			return decLit.Compare(v, row, kernelOp)
		}
		return false
	}
}

// bytesFilterVal renders a BYTES constant as the raw byte string the column
// stores, without going through fmt.Sprint (which renders []byte as "[1 2 3]").
func bytesFilterVal(value any) string {
	switch tv := value.(type) {
	case string:
		return tv
	case []byte:
		return string(tv)
	default:
		return fmt.Sprint(value)
	}
}

func parseIPv4FilterVal(value any) int64 {
	s := fmt.Sprint(value)
	ip := net.ParseIP(s)
	if ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			return int64(binary.BigEndian.Uint32(ip4))
		}
	}
	return 0
}

func parseMACFilterVal(value any) int64 {
	s := fmt.Sprint(value)
	hw, err := net.ParseMAC(s)
	if err != nil || len(hw) != 6 {
		return 0
	}
	var n uint64
	for _, b := range hw {
		n = (n << 8) | uint64(b)
	}
	return int64(n)
}

func parseIPv6FilterVal(value any) string {
	s := fmt.Sprint(value)
	ip := net.ParseIP(s)
	if ip != nil {
		return string(ip.To16())
	}
	return ""
}

// ColumnLike creates a predicate that evaluates col LIKE pattern using SQL LIKE semantics.
// The pattern uses % for any sequence of characters and _ for any single character.
func ColumnLike(colName, pattern string, not bool) Predicate {
	col := &lazyColIdx{}
	return func(b *batch.RecordBatch, row int) bool {
		idx := col.get(b, colName)
		if idx < 0 {
			return false
		}
		v := b.Columns[idx]
		if v.Nulls.IsNull(row) {
			return false
		}
		s := v.BytesData.UnsafeStringValue(row)
		result := matchLike(s, pattern)
		if not {
			return !result
		}
		return result
	}
}

// matchLike implements SQL LIKE pattern matching.
// % matches any sequence of characters, _ matches any single character.
func matchLike(s, pattern string) bool {
	return matchLikeRecur(s, pattern, 0, 0)
}

func matchLikeRecur(s, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		if pattern[pi] == '%' {
			pi++
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi == len(pattern) {
				return true
			}
			for i := si; i <= len(s); i++ {
				if matchLikeRecur(s, pattern, i, pi) {
					return true
				}
			}
			return false
		}
		if si >= len(s) {
			return false
		}
		if pattern[pi] == '_' || pattern[pi] == s[si] {
			si++
			pi++
		} else {
			return false
		}
	}
	return si == len(s)
}

// And combines predicates with logical AND.
func And(preds ...Predicate) Predicate {
	return func(b *batch.RecordBatch, row int) bool {
		for _, p := range preds {
			if !p(b, row) {
				return false
			}
		}
		return true
	}
}

// Or combines predicates with logical OR.
func Or(preds ...Predicate) Predicate {
	return func(b *batch.RecordBatch, row int) bool {
		for _, p := range preds {
			if p(b, row) {
				return true
			}
		}
		return false
	}
}

func compareInt64(a, b int64, op CompareOp) bool {
	switch op {
	case OpEq:
		return a == b
	case OpNe:
		return a != b
	case OpLt:
		return a < b
	case OpLe:
		return a <= b
	case OpGt:
		return a > b
	case OpGe:
		return a >= b
	default:
		return false
	}
}

// compareFloat64 is the row-at-a-time twin of the vectorized FLOAT kernel
// (kernel.compareFilterFloat), and answers in the same order: PostgreSQL's,
// where NaN is the greatest value and equal to itself. Go's own operators are
// IEEE754, so this path used to disagree with ORDER BY, with the group key,
// and — once the kernel moved — with the vectorized filter over the same
// predicate, which is the two-path divergence ADR-0012's consequence note
// records for DECIMAL.
func compareFloat64(a, b float64, op CompareOp) bool {
	return kernel.FloatCompareOp(a, b, toKernelOp(op))
}

func compareString(a, b string, op CompareOp) bool {
	switch op {
	case OpEq:
		return a == b
	case OpNe:
		return a != b
	case OpLt:
		return a < b
	case OpLe:
		return a <= b
	case OpGt:
		return a > b
	case OpGe:
		return a >= b
	default:
		return false
	}
}

// KernelFilter is a UnaryOperator that uses a pre-resolved typed filter kernel.
// The type dispatch happens once on first Execute; the inner loop has no type switches.
type KernelFilter struct {
	ColName string
	Op      CompareOp
	Value   any
	// LitText is the constant's exact source text, carried alongside the box
	// for the one type whose values a float64 cannot hold: a DECIMAL column's
	// kernel takes the TEXT and converts it at the column's own scale, so a
	// literal with more significant digits than a double survives (#452).
	// Used only when the resolved column is a DECIMAL — every other kernel
	// reads Value exactly as before. Empty for a non-numeric constant.
	LitText string
	// RowFallback, when non-nil, evaluates the original comparison row-at-
	// a-time. Used when ColName is a ROW-field access ("attrs.score") that
	// the typed kernel cannot evaluate — the planner attaches the compiled
	// expression's predicate, and resolution delegates to it instead of
	// silently matching nothing (issue #147).
	RowFallback Predicate

	colIdx      int
	kern        kernel.FilterKernel
	outSel      []uint32
	resolved    bool
	useFallback bool
	inner       *Filter
}

// NewKernelFilter creates a filter that uses typed kernels for comparison.
func NewKernelFilter(colName string, op CompareOp, value any) *KernelFilter {
	return &KernelFilter{ColName: colName, Op: op, Value: value}
}

// NewKernelFilterLit is NewKernelFilter for a constant that came from a
// numeric literal, carrying the literal's exact source text for a DECIMAL
// column (#452).
func NewKernelFilterLit(colName string, op CompareOp, value any, litText string) *KernelFilter {
	return &KernelFilter{ColName: colName, Op: op, Value: value, LitText: litText}
}

// decimalLitValue substitutes a numeric literal's exact source text for its
// float64 box when the column being compared is a DECIMAL, whose values a
// float64 cannot represent past ~15-16 significant digits. The DECIMAL
// kernels take their constant as text and convert it at the column's scale
// (kernel.compareFilterDecimal); every other type reads the box.
//
// A nil value must stay nil: ResolveFilterKernel's nil guard is what turns a
// NULL-literal comparison into "match nothing" instead of the type's zero,
// and substituting litText here for a nil value would hand it a non-nil
// string and skip that guard. No caller currently reaches this with a nil
// value and non-empty litText (producers lower a NULL literal to
// MatchNothingFilter before it gets here) — this is a floor guard, not a
// reachable path today.
func decimalLitValue(typ batch.TypeID, value any, litText string) any {
	if value == nil {
		return nil
	}
	if litText == "" {
		return value
	}
	switch typ {
	case batch.TypeDecimal:
		return litText
	case batch.TypeString:
		// A STRING column compares its bytes against the literal's, and the
		// literal's bytes are the ones the user WROTE: `s = 1.50` is a
		// different predicate from `s = 1.5` here, exactly as `s = '1.50'`
		// and `s = '1.5'` are. Rendering the float64 box instead would
		// collapse them, and would disagree with the row-at-a-time path,
		// which compares against Lit.Text (expr.boxedPair's text arm, #504).
		//
		// Only when the box is NOT already a string: a quoted literal has
		// litText empty, and a network literal that reached a STRING column
		// keeps its own text.
		if _, isStr := value.(string); !isStr {
			return litText
		}
	}
	return value
}

// decimalConstError is the query error a constant that is not a number earns
// when it reaches a DECIMAL column, and nil for every other case.
//
// It used to be no error at all: batch.ParseDecimalString read anything it
// could not parse as the value ZERO, so `WHERE d = 'abc'` — and `WHERE
// d = 1e400`, which the old float64 expansion could not read either —
// matched every row holding zero (#463). PostgreSQL refuses the query, and
// ADR-0012 makes PostgreSQL the authority on error-versus-not, so this is its
// SQLSTATE and its wording.
//
// A nil constant is NOT this case: it is UNKNOWN for every row, which
// ResolveFilterKernel's own nil guard already answers.
func decimalConstError(typ batch.TypeID, value any) error {
	if typ != batch.TypeDecimal || value == nil {
		return nil
	}
	if _, ok := kernel.DecimalConstText(value); ok {
		return nil
	}
	return sqlerr.New("22P02", "invalid input syntax for type numeric: %q", fmt.Sprint(value))
}

// boolConstError is decimalConstError's counterpart for a BOOL column whose
// kernel arm refuses a text literal it cannot read as a boolean
// (kernel.ParseBoolText / #574). PostgreSQL accepts t/f/true/false/yes/no/
// y/n/on/off/1/0 and their unambiguous prefixes, case-insensitively, as
// boolean text input, and raises 22P02 (`invalid input syntax for type
// boolean`) for anything else. Before this, the kernel read every string as
// FALSE and the row path string-matched only "true"/"false" — two silent
// wrong answers in opposite directions; now both read the grammar and both
// refuse the same unparseable literal here and in expr.compare.
//
// A nil constant is NOT this case: ResolveFilterKernel's own nil guard
// answers it as UNKNOWN for every row.
func boolConstError(typ batch.TypeID, value any) error {
	if typ != batch.TypeBool || value == nil {
		return nil
	}
	if _, ok := kernel.BoolFilterConst(value); ok {
		return nil
	}
	return sqlerr.New("22P02", "invalid input syntax for type boolean: %q", fmt.Sprint(value))
}

// rowFieldType reports the declared type of a ROW vector's named field. It is
// the exec-side counterpart of expr.ColRef.valueType, for the operators that
// hold a NAME rather than a resolved reference.
func rowFieldType(parent *batch.Vector, field string) (batch.TypeID, bool) {
	for i, n := range parent.FieldNames {
		if strings.EqualFold(n, field) && i < len(parent.Children) {
			return parent.Children[i].Type, true
		}
	}
	return 0, false
}

// networkConstError is decimalConstError's counterpart for the network types
// whose kernel arm refuses a literal it cannot read as an ADDRESS: TypeCIDR
// (kernel.CidrSortKey), TypeIPv6 (kernel.IPv6LitKey), and — since #519 closed
// the same gap one type over — TypeIPv4 (kernel.IPv4LitKey), TypeMAC
// (kernel.MACLitKey) and TypeUUID (kernel.UUIDLiteralToRaw).
//
// All five used to answer instead of refusing, and the answers were silently
// wrong in different directions: the CIDR arm returned a match-nothing
// kernel, so `c_cidr <> 'garbage'` dropped every row; the IPv6 arm read an
// unparseable literal as the empty raw address, which every stored address
// compares ABOVE; the IPv4/MAC arms read it as the encoded zero, which
// MATCHED every row holding the address 0.0.0.0 / 00:00:00:00:00:00; the
// UUID arm read it as the empty string, which matches nothing for `=` and
// EVERY row for `<>`. PostgreSQL refuses `'garbage'::inet` /
// `'garbage'::macaddr` / `'garbage'::uuid` with 22P02, and ADR-0012 item 1
// makes PostgreSQL the authority on error-versus-not, so this is its
// SQLSTATE and its wording.
//
// The row-at-a-time path raises the same error for the same literal
// (expr.CmpNetworkLit's CIDR/IPv6 arms and expr's Cmp binding via
// decimalLitCmp.refuseNonAddress, which covers IPv4/MAC/UUID too): one path
// erroring while the other answers is the two-path defect class.
func networkConstError(typ batch.TypeID, value any) error {
	if value == nil {
		return nil
	}
	switch typ {
	case batch.TypeCIDR:
		if _, ok := kernel.CidrSortKey(fmt.Sprint(value)); ok {
			return nil
		}
		return sqlerr.New("22P02", "invalid input syntax for type cidr: %q", fmt.Sprint(value))
	case batch.TypeIPv6:
		if _, ok := kernel.IPv6LitKey(fmt.Sprint(value)); ok {
			return nil
		}
		return sqlerr.New("22P02", "invalid input syntax for type inet: %q", fmt.Sprint(value))
	case batch.TypeIPv4:
		if _, ok := kernel.IPv4LitKey(fmt.Sprint(value)); ok {
			return nil
		}
		return sqlerr.New("22P02", "invalid input syntax for type inet: %q", fmt.Sprint(value))
	case batch.TypeMAC:
		if _, ok := kernel.MACLitKey(fmt.Sprint(value)); ok {
			return nil
		}
		return sqlerr.New("22P02", "invalid input syntax for type macaddr: %q", fmt.Sprint(value))
	case batch.TypeUUID:
		if _, ok := kernel.UUIDLiteralToRaw(fmt.Sprint(value)); ok {
			return nil
		}
		return sqlerr.New("22P02", "invalid input syntax for type uuid: %q", fmt.Sprint(value))
	}
	return nil
}

// dateConstError is decimalConstError's counterpart for a DATE value whose
// day count does not fit the int32 the DATE column encoding stores
// (kernel.DateLiteralDays / #451). PostgreSQL raises 22008
// (datetime_field_overflow) for a date outside its own representable range,
// and ADR-0012 item 1 makes PostgreSQL the authority on error-versus-not, so
// this is its SQLSTATE.
//
// No DATE STRING literal can reach it, and that is the point: #451's defect
// was that `d = '9999-12-31'` — the common SCD-2 end-of-time sentinel —
// silently compared as 2262-04-11, because parseDateToDays computed its day
// count through a time.Duration, which SATURATES at ±math.MaxInt64
// nanoseconds (~292 years) rather than reporting an overflow. That is fixed
// where it happened, in the arithmetic: kernel.parseDateToDays now counts
// civil days from t.Unix(), and 9999-12-31 is 2,932,896 days — about 700×
// inside int32 — so it is now simply CORRECT, not an error. Nor can any
// other literal reach the guard: every layout parseDateToDays accepts takes
// time.Parse's "2006", which is exactly four digits, so 9999-12-31 is the
// largest date a literal can spell and 0000-01-01 the smallest.
//
// The guard is still live for a caller that hands kernel.toDateInt32 a RAW
// day count — an int64 or int compared against a DATE column, which no
// parser bounds — and it is what keeps ResolveFilterKernel's "nil kernel,
// caller raises" convention honest for the type. Leave it in place: this is
// the layer that must not clamp, whatever the layer above happens to be able
// to produce today.
func dateConstError(typ batch.TypeID, value any) error {
	if typ != batch.TypeDate || value == nil {
		return nil
	}
	if _, err := kernel.DateLiteralDays(value); err != nil {
		return sqlerr.New("22008", "date out of range: %v", value)
	}
	return nil
}

// floatConstError is decimalConstError's counterpart for a FLOAT32 (real)
// column whose IN-list arm declines a literal that overflows real's range.
// PostgreSQL raises 22003 (numeric_value_out_of_range) for `real IN (1e40)`
// — the whole predicate, not a silently dropped element (verified on
// postgres:17) — and ADR-0012 makes PostgreSQL the authority on
// error-versus-not, so this is its SQLSTATE.
//
// It exists because the #549 fix narrows each IN literal to float32; a finite
// literal past FLT_MAX narrows to +Inf, and inserting that would make the
// predicate MATCH a genuine +Inf row (a false positive) instead of erroring.
// kernel.ResolveInFilterKernel returns a nil kernel for that list, and this
// turns the same condition into the error PostgreSQL gives. A literal that is
// itself ±Inf is a legal real value, not an overflow, and does not reach here.
func floatConstError(typ batch.TypeID, value any) error {
	if typ != batch.TypeFloat32 || value == nil {
		return nil
	}
	if kernel.Float32LitOverflow(value) {
		return sqlerr.New("22003", "%q is out of range for type real", fmt.Sprint(value))
	}
	return nil
}

func (f *KernelFilter) Init(_ context.Context) error { return nil }

func (f *KernelFilter) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.colIdx = in.ColumnIndex(f.ColName)
		if f.colIdx < 0 && strings.Contains(f.ColName, ".") {
			// Strip table alias qualifier (e.g. "n1.n_name" → "n_name")
			parts := strings.SplitN(f.ColName, ".", 2)
			f.colIdx = in.ColumnIndex(parts[1])
			if f.colIdx < 0 && f.RowFallback != nil {
				// ROW-field access: the qualifier names a ROW column whose
				// field the typed kernel cannot reach — delegate to the
				// row-at-a-time predicate.
				if pi := in.ColumnIndex(parts[0]); pi >= 0 && in.Columns[pi].Type == batch.TypeRow {
					// The literal is still checked against the FIELD's
					// declared type first — every check the resolved-column
					// path below makes. Delegating without them is how
					// `rw.cidr = 'not-a-cidr'` and `rw.dec = 'abc'` answered
					// ZERO ROWS where the same predicate on a CIDR or DECIMAL
					// COLUMN raises 22P02: the row predicate compares text
					// and finds no match, which is a value answer to a
					// question that has none (#568).
					if ft, ok := rowFieldType(in.Columns[pi], parts[1]); ok {
						if err := networkConstError(ft, f.Value); err != nil {
							return nil, err
						}
						if err := decimalConstError(ft, f.Value); err != nil {
							return nil, err
						}
						if err := boolConstError(ft, f.Value); err != nil {
							return nil, err
						}
					}
					f.useFallback = true
					f.inner = NewFilter(f.RowFallback)
				}
			}
		}
		if f.colIdx >= 0 {
			typ := in.Columns[f.colIdx].Type
			f.kern = kernel.ResolveFilterKernel(typ, toKernelOp(f.Op),
				decimalLitValue(typ, f.Value, f.LitText))
		}
		f.outSel = make([]uint32, 0, in.Len)
		f.resolved = true
	}

	if f.useFallback {
		return f.inner.Execute(ctx, in)
	}
	if f.colIdx < 0 {
		// An unresolvable filter column previously matched NOTHING — a
		// typo'd WHERE clause silently returned an empty result,
		// indistinguishable from genuinely empty data (issue #147).
		return nil, fmt.Errorf("filter column %q does not exist in the input schema", f.ColName)
	}
	if f.kern == nil {
		typ := in.Columns[f.colIdx].Type
		if err := decimalConstError(typ, decimalLitValue(typ, f.Value, f.LitText)); err != nil {
			return nil, err
		}
		if err := networkConstError(typ, f.Value); err != nil {
			return nil, err
		}
		if err := dateConstError(typ, f.Value); err != nil {
			return nil, err
		}
		if err := boolConstError(typ, f.Value); err != nil {
			return nil, err
		}
		// The column resolved; the TYPE has no comparison kernel. Reporting
		// that as "does not exist" sent every reader hunting a name-resolution
		// bug — that is how #401 was filed, when the real answer was that
		// ResolveFilterKernel had no DECIMAL arm.
		return nil, fmt.Errorf("filter on column %q: no comparison kernel for type %s",
			f.ColName, typ)
	}

	// When the batch already has a selection vector (e.g. from a prior filter
	// in an AND chain), compact it in-place: pass in.Sel as the output buffer.
	// This is safe because the kernel's write position never exceeds its read
	// position — each iteration reads sel[i] before potentially writing out[j]
	// where j <= i. Avoids allocating a separate output buffer per filter stage.
	outBuf := f.outSel
	if in.Sel != nil {
		outBuf = in.Sel
	}

	sel := f.kern(in.Columns[f.colIdx], in.Sel, in.Len, outBuf)
	if len(sel) == 0 {
		return nil, nil
	}

	in.Sel = sel
	return in, nil
}

func (f *KernelFilter) Close() error { return nil }

// Clone returns a new KernelFilter with the same parameters but fresh
// resolution state and scratch buffers for concurrent Execute calls.
func (f *KernelFilter) Clone() UnaryOperator {
	return &KernelFilter{ColName: f.ColName, Op: f.Op, Value: f.Value,
		LitText: f.LitText, RowFallback: f.RowFallback}
}

// InFilter uses a vectorized kernel for set membership testing (IN / NOT IN).
type InFilter struct {
	ColName string
	Values  []any
	Negate  bool
	// ValueTexts holds each value's exact literal source text, parallel to
	// Values, for a DECIMAL column — see KernelFilter.LitText (#452). Nil, or
	// an empty entry, keeps the boxed value.
	ValueTexts []string
	// RowFallback mirrors KernelFilter.RowFallback: the row-at-a-time
	// predicate for a ROW field path the set kernel cannot address, which
	// otherwise reported the field path as a column that does not exist
	// (#568).
	RowFallback Predicate
	// syntacticLen is the IN list's element count BEFORE any NULL member was
	// stripped for three-valued logic. PostgreSQL decides a `real IN (...)`'s
	// comparison WIDTH from this syntactic arity — it casts the whole `{...}`
	// array literal, NULLs included, to real[] when there is more than one
	// element — so `real IN (0.1, NULL)` narrows and matches even though one
	// non-NULL literal reaches the kernel (#549). Set by the constructors to
	// len(Values) and overridden by SetSyntacticLen at the NULL-strip site.
	syntacticLen int
	colIdx       int
	kern         kernel.FilterKernel
	outSel       []uint32
	resolved     bool
	useFallback  bool
	inner        *Filter
}

func NewInFilter(colName string, values []any, negate bool) *InFilter {
	return &InFilter{ColName: colName, Values: values, Negate: negate, syntacticLen: len(values)}
}

// SetSyntacticLen records the list's pre-NULL-strip element count, which the
// caller that does the stripping (planner inFilterForList) knows and the
// constructor cannot. It is the arity PostgreSQL decides a real IN list's width
// from (#549); leaving it at the constructor's len(Values) is correct only when
// no NULL was dropped.
func (f *InFilter) SetSyntacticLen(n int) { f.syntacticLen = n }

// NewInFilterLit is NewInFilter for a list of numeric literals, carrying each
// literal's exact source text for a DECIMAL column (#452).
func NewInFilterLit(colName string, values []any, texts []string, negate bool) *InFilter {
	return &InFilter{ColName: colName, Values: values, ValueTexts: texts, Negate: negate, syntacticLen: len(values)}
}

func (f *InFilter) Init(_ context.Context) error { return nil }

func (f *InFilter) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.colIdx = in.ColumnIndex(f.ColName)
		if f.colIdx < 0 && strings.Contains(f.ColName, ".") {
			parts := strings.SplitN(f.ColName, ".", 2)
			f.colIdx = in.ColumnIndex(parts[1])
			if f.colIdx < 0 && f.RowFallback != nil {
				if pi := in.ColumnIndex(parts[0]); pi >= 0 && in.Columns[pi].Type == batch.TypeRow {
					f.useFallback = true
					f.inner = NewFilter(f.RowFallback)
				}
			}
		}
		if f.colIdx >= 0 {
			typ := in.Columns[f.colIdx].Type
			f.kern = kernel.ResolveInFilterKernelArity(typ, f.kernelValues(typ), f.Negate, f.syntacticLen)
		}
		f.outSel = make([]uint32, 0, in.Len)
		f.resolved = true
	}
	// Returning (nil, nil) here matched NOTHING, which is the failure mode
	// #147 was filed for: an empty result indistinguishable from genuinely
	// empty data. It fired for real — seven column types had no arm in
	// ResolveInFilterKernel, so `WHERE bool_col IN (true)` dropped every row.
	if f.useFallback {
		return f.inner.Execute(ctx, in)
	}
	if f.colIdx < 0 {
		return nil, fmt.Errorf("IN filter column %q does not exist in the input schema", f.ColName)
	}
	if f.kern == nil {
		typ := in.Columns[f.colIdx].Type
		for _, v := range f.kernelValues(typ) {
			if err := decimalConstError(typ, v); err != nil {
				return nil, err
			}
			if err := networkConstError(typ, v); err != nil {
				return nil, err
			}
			if err := dateConstError(typ, v); err != nil {
				return nil, err
			}
			if err := boolConstError(typ, v); err != nil {
				return nil, err
			}
			if err := floatConstError(typ, v); err != nil {
				return nil, err
			}
		}
		return nil, fmt.Errorf("IN filter on column %q: no set-membership kernel for type %s",
			f.ColName, typ)
	}
	// Compact in-place when a prior selection exists (see KernelFilter.Execute).
	outBuf := f.outSel
	if in.Sel != nil {
		outBuf = in.Sel
	}
	sel := f.kern(in.Columns[f.colIdx], in.Sel, in.Len, outBuf)
	if len(sel) == 0 {
		return nil, nil
	}
	in.Sel = sel
	return in, nil
}

func (f *InFilter) Close() error { return nil }

// kernelValues is Values with each member's exact literal source text
// substituted for the two column types whose kernels take their constant as
// TEXT: a DECIMAL (#452) and a STRING (#504). Unchanged for every other type,
// which decimalLitValue decides.
//
// It used to return early for anything but a DECIMAL, so `s IN (2.00)` over a
// STRING column never reached the substitution that `s = 2.00` did: the
// numeric box became the empty string and the list matched nothing, while the
// equality matched one row. `x IN (v)` is `x = v` chained with OR and cannot
// answer differently (#504 review, non-blocker a).
func (f *InFilter) kernelValues(typ batch.TypeID) []any {
	if len(f.ValueTexts) != len(f.Values) {
		return f.Values
	}
	out := make([]any, len(f.Values))
	for i, v := range f.Values {
		out[i] = decimalLitValue(typ, v, f.ValueTexts[i])
	}
	return out
}

func (f *InFilter) Clone() UnaryOperator {
	return &InFilter{ColName: f.ColName, Values: f.Values,
		ValueTexts: f.ValueTexts, Negate: f.Negate, RowFallback: f.RowFallback,
		syntacticLen: f.syntacticLen}
}

// MatchNothingFilter admits no rows. It is the operator for a predicate that
// is UNKNOWN on every row whatever the data says — a comparison against a
// NULL literal, and its negation too, since NOT UNKNOWN is UNKNOWN. A WHERE
// admits only TRUE, so the answer is no rows.
//
// Saying that in the PLAN is the point. Lowering `col = NULL` to a typed
// kernel handed it a nil constant, which every coercion in
// kernel.ResolveFilterKernel turns into the column type's ZERO: `WHERE c_i64
// = NULL` answered the rows where the column is 0, `WHERE c_str = NULL` the
// rows where the string is empty (#450).
type MatchNothingFilter struct{}

func NewMatchNothingFilter() *MatchNothingFilter { return &MatchNothingFilter{} }

func (f *MatchNothingFilter) Init(_ context.Context) error { return nil }

// Execute returns the no-rows-survive signal every other filter uses when its
// selection comes back empty.
func (f *MatchNothingFilter) Execute(_ context.Context, _ *batch.RecordBatch) (*batch.RecordBatch, error) {
	return nil, nil
}

func (f *MatchNothingFilter) Close() error { return nil }

func (f *MatchNothingFilter) Clone() UnaryOperator { return &MatchNothingFilter{} }

// LikeFilter uses a vectorized kernel for SQL LIKE pattern matching.
type LikeFilter struct {
	ColName string
	Pattern string
	Negate  bool
	// RowFallback mirrors KernelFilter.RowFallback: the row-at-a-time
	// predicate for a ROW field path the kernel cannot address (#568).
	RowFallback Predicate
	colIdx      int
	kern        kernel.FilterKernel
	outSel      []uint32
	resolved    bool
	useFallback bool
	inner       *Filter
}

func NewLikeFilter(colName, pattern string, negate bool) *LikeFilter {
	return &LikeFilter{ColName: colName, Pattern: pattern, Negate: negate}
}

func (f *LikeFilter) Init(_ context.Context) error { return nil }

func (f *LikeFilter) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.colIdx = in.ColumnIndex(f.ColName)
		if f.colIdx < 0 && strings.Contains(f.ColName, ".") {
			parts := strings.SplitN(f.ColName, ".", 2)
			f.colIdx = in.ColumnIndex(parts[1])
			if f.colIdx < 0 && f.RowFallback != nil {
				// A ROW FIELD PATH the LIKE kernel cannot reach — same
				// delegation KernelFilter makes, and for the same reason:
				// without it the name resolved to nothing and every row was
				// dropped silently (#568).
				if pi := in.ColumnIndex(parts[0]); pi >= 0 && in.Columns[pi].Type == batch.TypeRow {
					f.useFallback = true
					f.inner = NewFilter(f.RowFallback)
				}
			}
		}
		if f.colIdx >= 0 {
			f.kern = kernel.ResolveLikeFilterKernel(in.Columns[f.colIdx].Type, f.Pattern, f.Negate)
		}
		f.outSel = make([]uint32, 0, in.Len)
		f.resolved = true
	}
	if f.useFallback {
		return f.inner.Execute(ctx, in)
	}
	if f.colIdx < 0 {
		return nil, nil
	}
	if f.kern == nil {
		if err := likeConstError(in.Columns[f.colIdx].Type); err != nil {
			return nil, err
		}
		return nil, nil
	}
	// Compact in-place when a prior selection exists (see KernelFilter.Execute).
	outBuf := f.outSel
	if in.Sel != nil {
		outBuf = in.Sel
	}
	sel := f.kern(in.Columns[f.colIdx], in.Sel, in.Len, outBuf)
	if len(sel) == 0 {
		return nil, nil
	}
	in.Sel = sel
	return in, nil
}

// likeConstError is decimalConstError/networkConstError's counterpart for
// LIKE against a container column (#522): PostgreSQL has no `~~` operator
// for any composite or array type, and kernel.ResolveLikeFilterKernel
// returns nil for exactly the four container types to signal it. Every
// other type still returns a kernel, so nil is unambiguous here.
func likeConstError(typ batch.TypeID) error {
	name, ok := containerLikeName(typ)
	if !ok {
		return nil
	}
	return sqlerr.New("42883", "operator does not exist: %s ~~ unknown", name)
}

// containerLikeName names typ the way PostgreSQL's own operator-does-not-
// exist error would for the closest such type, for the four container
// types LIKE refuses. ok is false for every other type.
func containerLikeName(typ batch.TypeID) (string, bool) {
	switch typ {
	case batch.TypeArray:
		return "array", true
	case batch.TypeMap:
		return "map", true
	case batch.TypeRow:
		return "record", true
	case batch.TypeVector:
		return "vector", true
	}
	return "", false
}

func (f *LikeFilter) Close() error { return nil }

func (f *LikeFilter) Clone() UnaryOperator {
	return &LikeFilter{ColName: f.ColName, Pattern: f.Pattern, Negate: f.Negate, RowFallback: f.RowFallback}
}

// NullCheckFilter is a vectorized filter for IS NULL / IS NOT NULL predicates.
// Scans the null bitmap directly — no per-row type switch or function call overhead.
type NullCheckFilter struct {
	ColName   string
	CheckNull bool // true = IS NULL, false = IS NOT NULL
	// RowFallback, when non-nil, evaluates the original IS [NOT] NULL
	// row-at-a-time for the shape this bitmap scan cannot serve: a ROW FIELD
	// PATH, whose null is the FIELD's and not the container's. Without it
	// the name resolved to nothing and `WHERE rw.f IS NULL` answered NO ROWS
	// on every input — an empty result indistinguishable from real data,
	// the #147 failure mode one level down (#568).
	RowFallback Predicate
	colIdx      int
	outSel      []uint32
	resolved    bool
	useFallback bool
	inner       *Filter
}

func NewNullCheckFilter(colName string, checkNull bool) *NullCheckFilter {
	return &NullCheckFilter{ColName: colName, CheckNull: checkNull}
}

func (f *NullCheckFilter) Init(_ context.Context) error { return nil }

func (f *NullCheckFilter) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.colIdx = in.ColumnIndex(f.ColName)
		if f.colIdx < 0 && strings.Contains(f.ColName, ".") {
			parts := strings.SplitN(f.ColName, ".", 2)
			f.colIdx = in.ColumnIndex(parts[1])
			if f.colIdx < 0 && f.RowFallback != nil {
				if pi := in.ColumnIndex(parts[0]); pi >= 0 && in.Columns[pi].Type == batch.TypeRow {
					f.useFallback = true
					f.inner = NewFilter(f.RowFallback)
				}
			}
		}
		f.outSel = make([]uint32, 0, in.Len)
		f.resolved = true
	}
	if f.useFallback {
		return f.inner.Execute(ctx, in)
	}
	if f.colIdx < 0 {
		if f.CheckNull {
			return nil, nil // column not found, nothing is null
		}
		return in, nil // column not found, treat as all non-null
	}

	col := in.Columns[f.colIdx]

	// Fast path: no nulls in this batch's column.
	if !col.Nulls.HasNulls() {
		if f.CheckNull {
			return nil, nil // IS NULL: no rows match
		}
		return in, nil // IS NOT NULL: all rows match
	}

	// Compact in-place when a prior selection exists.
	outBuf := f.outSel
	if in.Sel != nil {
		outBuf = in.Sel
	}
	sel := outBuf[:0]

	if f.CheckNull {
		if in.Sel != nil {
			for _, idx := range in.Sel {
				if col.Nulls.IsNullFast(int(idx)) {
					sel = append(sel, idx)
				}
			}
		} else {
			for i := 0; i < in.Len; i++ {
				if col.Nulls.IsNullFast(i) {
					sel = append(sel, uint32(i))
				}
			}
		}
	} else {
		if in.Sel != nil {
			for _, idx := range in.Sel {
				if !col.Nulls.IsNullFast(int(idx)) {
					sel = append(sel, idx)
				}
			}
		} else {
			for i := 0; i < in.Len; i++ {
				if !col.Nulls.IsNullFast(i) {
					sel = append(sel, uint32(i))
				}
			}
		}
	}

	if len(sel) == 0 {
		return nil, nil
	}
	in.Sel = sel
	return in, nil
}

func (f *NullCheckFilter) Close() error { return nil }

func (f *NullCheckFilter) Clone() UnaryOperator {
	return &NullCheckFilter{ColName: f.ColName, CheckNull: f.CheckNull, RowFallback: f.RowFallback}
}

func toKernelOp(op CompareOp) kernel.CompareOp {
	switch op {
	case OpEq:
		return kernel.OpEq
	case OpNe:
		return kernel.OpNe
	case OpLt:
		return kernel.OpLt
	case OpLe:
		return kernel.OpLe
	case OpGt:
		return kernel.OpGt
	case OpGe:
		return kernel.OpGe
	default:
		return kernel.OpEq
	}
}

func toInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int:
		return int64(tv)
	case int32:
		return int64(tv)
	case float64:
		return int64(tv)
	case string:
		return parseTimestampString(tv)
	default:
		return 0
	}
}

func parseTimestampString(s string) int64 {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

// ChainFilter applies a sequence of unary filter operators in order.
// Each operator narrows the selection vector before passing to the next.
type ChainFilter struct {
	Ops []UnaryOperator
}

func NewChainFilter(ops []UnaryOperator) *ChainFilter {
	return &ChainFilter{Ops: ops}
}

func (f *ChainFilter) Init(ctx context.Context) error {
	for _, op := range f.Ops {
		if err := op.Init(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (f *ChainFilter) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	for _, op := range f.Ops {
		var err error
		in, err = op.Execute(ctx, in)
		if err != nil {
			return nil, err
		}
		if in == nil {
			return nil, nil
		}
	}
	return in, nil
}

func (f *ChainFilter) Close() error {
	for _, op := range f.Ops {
		op.Close()
	}
	return nil
}

// Clone returns a new ChainFilter with cloned sub-operators.
// Sub-operators that implement Cloneable are cloned; others are shared.
func (f *ChainFilter) Clone() UnaryOperator {
	cloned := make([]UnaryOperator, len(f.Ops))
	for i, op := range f.Ops {
		if c, ok := op.(Cloneable); ok {
			cloned[i] = c.Clone()
		} else {
			cloned[i] = op
		}
	}
	return &ChainFilter{Ops: cloned}
}

// OrFilter evaluates two filter branches and unions their selection vectors.
// Both branches run on the same input batch; results are merged with dedup.
type OrFilter struct {
	Left, Right UnaryOperator
	mergeBuf    []uint32
	selCopy     []uint32 // scratch to snapshot origSel before branch evaluation
}

func NewOrFilter(left, right UnaryOperator) *OrFilter {
	return &OrFilter{Left: left, Right: right}
}

func (f *OrFilter) Init(ctx context.Context) error {
	if err := f.Left.Init(ctx); err != nil {
		return err
	}
	return f.Right.Init(ctx)
}

func (f *OrFilter) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	origLen := in.Len

	// Snapshot the original selection vector. Filter branches may compact
	// in.Sel in-place, so the right branch needs an independent copy.
	//
	// origSelWasNil records the "no prior selection" case, where the batch
	// convention is that a nil Sel means EVERY one of the Len rows is live.
	// A branch that matches all of them returns the batch with that same nil
	// Sel — indistinguishable, by the Sel alone, from a branch that matched
	// NONE (which returns a nil *batch). The all-rows reading is resolved
	// below from whether the branch returned a batch at all; without it,
	// `x IS NULL OR x IS NOT NULL` over a column with no nulls unioned
	// {none} with {all-as-nil-Sel} and answered ZERO rows.
	origSelWasNil := in.Sel == nil
	var savedSel []uint32
	if in.Sel != nil {
		if cap(f.selCopy) < len(in.Sel) {
			f.selCopy = make([]uint32, len(in.Sel))
		}
		savedSel = f.selCopy[:len(in.Sel)]
		copy(savedSel, in.Sel)
	}

	// Evaluate left branch (may compact in.Sel in-place)
	leftResult, err := f.Left.Execute(ctx, in)
	if err != nil {
		return nil, err
	}
	// A non-nil result with a nil Sel is "all rows matched" only when this
	// filter entered on all rows; with a prior selection a well-behaved
	// branch narrows and returns that selection, never a nil one.
	leftAll := origSelWasNil && leftResult != nil && leftResult.Sel == nil
	var leftSel []uint32
	if leftResult != nil {
		leftSel = leftResult.Sel
	}

	// Evaluate right branch on the saved copy of the original selection
	in.Sel = savedSel
	in.Len = origLen
	rightResult, err := f.Right.Execute(ctx, in)
	if err != nil {
		return nil, err
	}
	rightAll := origSelWasNil && rightResult != nil && rightResult.Sel == nil
	var rightSel []uint32
	if rightResult != nil {
		rightSel = rightResult.Sel
	}

	// OR with an all-rows branch is all the rows that entered this filter.
	if leftAll || rightAll {
		in.Sel = nil
		in.Len = origLen
		return in, nil
	}

	// Union selection vectors
	if len(leftSel) == 0 && len(rightSel) == 0 {
		return nil, nil
	}
	if len(leftSel) == 0 {
		in.Sel = rightSel
		return in, nil
	}
	if len(rightSel) == 0 {
		in.Sel = leftSel
		return in, nil
	}

	// Merge two sorted selection vectors (both are in ascending order)
	needed := len(leftSel) + len(rightSel)
	if cap(f.mergeBuf) < needed {
		f.mergeBuf = make([]uint32, 0, needed)
	}
	merged := f.mergeBuf[:0]
	i, j := 0, 0
	for i < len(leftSel) && j < len(rightSel) {
		if leftSel[i] < rightSel[j] {
			merged = append(merged, leftSel[i])
			i++
		} else if leftSel[i] > rightSel[j] {
			merged = append(merged, rightSel[j])
			j++
		} else {
			merged = append(merged, leftSel[i])
			i++
			j++
		}
	}
	for ; i < len(leftSel); i++ {
		merged = append(merged, leftSel[i])
	}
	for ; j < len(rightSel); j++ {
		merged = append(merged, rightSel[j])
	}
	f.mergeBuf = merged
	in.Sel = merged
	return in, nil
}

func (f *OrFilter) Close() error {
	f.Left.Close()
	f.Right.Close()
	return nil
}

func (f *OrFilter) Clone() UnaryOperator {
	var l, r UnaryOperator
	if c, ok := f.Left.(Cloneable); ok {
		l = c.Clone()
	} else {
		l = f.Left
	}
	if c, ok := f.Right.(Cloneable); ok {
		r = c.Clone()
	} else {
		r = f.Right
	}
	return &OrFilter{Left: l, Right: r}
}

func toFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int:
		return float64(tv)
	default:
		return 0
	}
}

// ColColFilter compares two columns element-wise using a vectorized kernel.
// Resolves column indices and kernel on first Execute; inner loop has no type switches.
type ColColFilter struct {
	LeftCol  string
	RightCol string
	Op       CompareOp
	// RowFallback, when non-nil, evaluates the original comparison row-at-
	// a-time. Used when the two columns resolve to DIFFERENT types: the
	// typed kernel reads both sides through the left column's storage slice,
	// so a FLOAT64-vs-INT32 comparison would index the right vector's empty
	// Float64Data and panic (issue #375). The compiled expression coerces
	// numeric types per SQL semantics, so it is the correct evaluator for
	// the mixed-type case.
	RowFallback Predicate

	leftIdx     int
	rightIdx    int
	kern        kernel.ColColFilterKernel
	outSel      []uint32
	resolved    bool
	useFallback bool
	inner       *Filter
}

func NewColColFilter(leftCol, rightCol string, op CompareOp) *ColColFilter {
	return &ColColFilter{LeftCol: leftCol, RightCol: rightCol, Op: op}
}

func (f *ColColFilter) Init(_ context.Context) error { return nil }

func (f *ColColFilter) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if !f.resolved {
		f.leftIdx = in.ColumnIndex(f.LeftCol)
		if f.leftIdx < 0 && strings.Contains(f.LeftCol, ".") {
			parts := strings.SplitN(f.LeftCol, ".", 2)
			f.leftIdx = in.ColumnIndex(parts[1])
		}
		f.rightIdx = in.ColumnIndex(f.RightCol)
		if f.rightIdx < 0 && strings.Contains(f.RightCol, ".") {
			parts := strings.SplitN(f.RightCol, ".", 2)
			f.rightIdx = in.ColumnIndex(parts[1])
		}
		if f.leftIdx >= 0 && f.rightIdx >= 0 {
			lt, rt := in.Columns[f.leftIdx].Type, in.Columns[f.rightIdx].Type
			if lt == rt {
				f.kern = kernel.ResolveColColFilterKernel(lt, toKernelOp(f.Op))
			} else if f.RowFallback != nil {
				// Mixed-type comparison: the kernel would read the right
				// vector through the left type's storage (issue #375).
				f.useFallback = true
				f.inner = NewFilter(f.RowFallback)
			} else {
				return nil, fmt.Errorf("ColColFilter: mismatched column types %s (%v) %v %s (%v) and no row fallback",
					f.LeftCol, lt, f.Op, f.RightCol, rt)
			}
		}
		f.outSel = make([]uint32, 0, in.Len)
		f.resolved = true
	}

	if f.useFallback {
		return f.inner.Execute(ctx, in)
	}
	if f.kern == nil {
		return nil, fmt.Errorf("ColColFilter: could not resolve kernel for %s %v %s (leftIdx=%d, rightIdx=%d)",
			f.LeftCol, f.Op, f.RightCol, f.leftIdx, f.rightIdx)
	}

	// Compact in-place when a prior selection exists (see KernelFilter.Execute).
	outBuf := f.outSel
	if in.Sel != nil {
		outBuf = in.Sel
	}

	sel := f.kern(in.Columns[f.leftIdx], in.Columns[f.rightIdx], in.Sel, in.Len, outBuf)
	if len(sel) == 0 {
		return nil, nil
	}

	in.Sel = sel
	return in, nil
}

func (f *ColColFilter) Close() error { return nil }

// Clone returns a new ColColFilter with the same parameters but fresh
// resolution state and scratch buffers for concurrent Execute calls.
func (f *ColColFilter) Clone() UnaryOperator {
	return &ColColFilter{LeftCol: f.LeftCol, RightCol: f.RightCol, Op: f.Op, RowFallback: f.RowFallback}
}
