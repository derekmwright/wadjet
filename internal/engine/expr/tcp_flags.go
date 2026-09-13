package expr

import (
	"math"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// THE TCP FLAG FAMILY (#966) — bitset semantics over the integer flag field,
// spelled as functions.
//
// A flow table's TCP flags are already an integer bitset, and every query that
// wants "SYN without ACK" writes it as bit arithmetic today. This family gives
// that arithmetic the names the TCP header gives it, without adding a type and
// without adding syntax:
//
//	tcp_flags_has_all(flags, 'SYN', 'ACK')  -- (flags & mask) = mask
//	tcp_flags_has_any(flags, 'SYN', 'ACK')  -- (flags & mask) <> 0
//	tcp_flags_has_none(flags, 'RST')        -- (flags & mask) = 0
//	tcp_flag_mask('SYN', 'ACK')             -- 18
//	tcp_flags(flags)                        -- {SYN,ACK}
//	tcp_flags_text(flags)                   -- 'SYN|ACK'
//
// FUNCTIONS, NOT SYNTAX. #966 offered `flags CONTAINS ALL ('SYN','ACK')` as an
// alternative. Every PostgreSQL client can already send a function call; a new
// infix keyword is a thing psql, DataGrip, Superset and every JDBC-shaped tool
// would have to learn, and it buys nothing a function call does not say.
//
// PostgreSQL's bit arithmetic is the VALUE ORACLE for the three predicates,
// measured on 17.11 over `f4 int` / `f8 bigint` holding
// 0, 2, 18, 16, 4, 511, 24, NULL, 20, 256:
//
//	(f & 18) = 18   t for 18 and 511, f for the rest, NULL for NULL
//	(f & 18) <> 0   t for 2,18,16,511,24,20;  f for 0,4,256;  NULL for NULL
//	(f & 18) = 0    the complement of the ANY row, NULL for NULL
//	(-1) & 18       18  — a negative value is a bit pattern, not a magnitude
//
// Three properties of that transcript are load-bearing:
//
//   - NULL flags give NULL, not FALSE, for all three — including has_none,
//     which reads as though an absent value should satisfy it. Under WHERE the
//     three-valued answer collapses to "not matched", as PostgreSQL's does.
//   - A negative flags value has every high bit set and matches accordingly.
//     Nothing here special-cases the sign.
//   - PostgreSQL's ZERO mask is vacuously ALL and vacuously NONE. This family
//     refuses an empty name list instead (22023), because an empty list is a
//     query that meant something and did not say it — recorded as a deliberate
//     divergence in ADR-0012 rather than left as a comment here.
//
// An UNRECOGNIZED NAME IS LOUD (22023, the name in the message). The
// alternative — dropping the bit — turns `tcp_flags_has_all('SYN','ACKK')`
// into `tcp_flags_has_all('SYN')`, a strictly larger row set that nothing
// downstream can tell from the intended one. That is #855's class, and
// `date_trunc`'s unknown unit is the refusal it is modelled on.

// tcpFlagBits is the name↔bit table in HEADER BIT ORDER, which is ascending
// value order. `tcp_flags` and `tcp_flags_text` render in this order, which is
// what makes `'SYN|ACK'` — and not `'ACK|SYN'` — the answer for 18.
//
// CITATION, precisely (#966 round 2, N1): RFC 9293 §3.1 defines the EIGHT
// control bits CWR, ECE, URG, ACK, PSH, RST, SYN, FIN and a four-bit reserved
// field above them. It does NOT define bit 8 under either name. That bit was
// RFC 3540's `NS` (ECN nonce sum, moved to Historic by RFC 8311), and the
// Accurate ECN work reuses it as `AE`. Both spellings are accepted as INPUT —
// accepting a name cannot produce a wrong value, and refusing one would be a
// loud error for a correct intent — and `AE` is the canonical rendering,
// because that is the name a current capture tool prints.
var tcpFlagBits = []struct {
	Name string
	Bit  int64
}{
	{"FIN", 1},
	{"SYN", 2},
	{"RST", 4},
	{"PSH", 8},
	{"ACK", 16},
	{"URG", 32},
	{"ECE", 64},
	{"CWR", 128},
	{"AE", 256},
}

// tcpFlagByName resolves a flag name, case-insensitively, to its bit.
var tcpFlagByName = func() map[string]int64 {
	m := make(map[string]int64, len(tcpFlagBits)+1)
	for _, f := range tcpFlagBits {
		m[f.Name] = f.Bit
	}
	m["NS"] = 256 // RFC 3540's name for bit 8; AE is the Accurate ECN name
	return m
}()

// tcpFlagNamesList is the accepted spelling list, for the refusal message.
var tcpFlagNamesList = func() string {
	names := make([]string, 0, len(tcpFlagBits))
	for _, f := range tcpFlagBits {
		names = append(names, f.Name)
	}
	return strings.Join(names, ", ")
}()

// TCPFlagMask folds flag names into the integer mask they name. It is exported
// because the scan-filter pushdown folds the same names at PLAN time, and the
// two must not be able to disagree about what 'SYN' is — one table, one fold.
//
// ok is false for an unrecognized name or an empty list; the CALLER decides
// whether that is a refusal (the evaluator) or a declined pushdown (the
// planner, which then leaves the conjunct in the exec filter and lets the
// evaluator raise).
func TCPFlagMask(names []string) (mask int64, bad string, ok bool) {
	if len(names) == 0 {
		return 0, "", false
	}
	for _, n := range names {
		bit, found := tcpFlagByName[strings.ToUpper(strings.TrimSpace(n))]
		if !found {
			return 0, n, false
		}
		mask |= bit
	}
	return mask, "", true
}

// TCPFlagPredicateMode is which of the three tests a folded mask belongs to.
type TCPFlagPredicateMode int

const (
	TCPFlagsAll TCPFlagPredicateMode = iota
	TCPFlagsAny
	TCPFlagsNone
)

// TCPFlagPredicate reports the mode a function name names, for the planner's
// recognizer and the compiler's typed-node rewrite. One list, so a function
// added to the registry without a mode here simply never specializes.
func TCPFlagPredicate(name string) (TCPFlagPredicateMode, bool) {
	switch strings.ToLower(name) {
	case "tcp_flags_has_all":
		return TCPFlagsAll, true
	case "tcp_flags_has_any":
		return TCPFlagsAny, true
	case "tcp_flags_has_none":
		return TCPFlagsNone, true
	}
	return 0, false
}

// TCPFlagsMatch is the one implementation of the three tests. Every arm of
// this arc calls it — the scalar evaluator, the typed row kernel, the scan's
// dictionary mask and the scan's plain-page walk — so a mask can never mean
// one thing in the scan and another above it.
func TCPFlagsMatch(flags, mask int64, mode TCPFlagPredicateMode) bool {
	switch mode {
	case TCPFlagsAll:
		return flags&mask == mask
	case TCPFlagsAny:
		return flags&mask != 0
	default:
		return flags&mask == 0
	}
}

// tcpFlagsValue reads the flags argument as the 64-bit pattern it is.
//
// An integer box is taken as itself. A float box is taken only when it holds
// an exact integer — a fractional value is not a bit pattern and there is no
// truncation here that could quietly answer for it. Anything else (a TEXT
// column, a rendered DECIMAL) is refused with the type named, because the
// alternative is to read it as zero and answer "no flags set" for every row.
func tcpFlagsValue(fn string, v any) int64 {
	if i, ok := toInt64Safe(v); ok {
		return i
	}
	switch f := v.(type) {
	case float64:
		if f == math.Trunc(f) && f >= -9.2e18 && f <= 9.2e18 {
			return int64(f)
		}
	case float32:
		// The same range guard the float64 arm carries: converting an
		// out-of-range float to int64 in Go is implementation-defined and
		// saturates, so 1e30 would arrive as a flags value rather than as a
		// refusal (#966 round 2, N4).
		g := float64(f)
		if g == math.Trunc(g) && g >= -9.2e18 && g <= 9.2e18 {
			return int64(g)
		}
	}
	raiseTCPFlagsNotInteger(fn, v)
	return 0
}

// tcpFlagNameArgs reads the name arguments. A NULL name makes the whole call
// NULL (strict, as every other function here is); ok is false in that case.
func tcpFlagNameArgs(args []any) ([]string, bool) {
	names := make([]string, 0, len(args))
	for _, a := range args {
		if a == nil {
			return nil, false
		}
		names = append(names, toString(a))
	}
	return names, true
}

// maskFromArgs folds the name arguments, raising the family's two refusals.
func maskFromArgs(fn string, args []any) (int64, bool) {
	names, ok := tcpFlagNameArgs(args)
	if !ok {
		return 0, false
	}
	mask, bad, ok := TCPFlagMask(names)
	if !ok {
		if len(names) == 0 {
			raiseTCPFlagListEmpty(fn)
		}
		raiseUnknownTCPFlagName(fn, bad)
	}
	return mask, true
}

// tcp_flag_mask('SYN','ACK') -> 18. The integer the bit arithmetic uses, so a
// user can write the arithmetic themselves against the same table.
func fnTCPFlagMask(args []any) any {
	mask, ok := maskFromArgs("tcp_flag_mask", args)
	if !ok {
		return nil
	}
	return int32(mask)
}

func fnTCPFlagsHasAll(args []any) any {
	return tcpFlagsPredicate("tcp_flags_has_all", TCPFlagsAll, args)
}
func fnTCPFlagsHasAny(args []any) any {
	return tcpFlagsPredicate("tcp_flags_has_any", TCPFlagsAny, args)
}
func fnTCPFlagsHasNone(args []any) any {
	return tcpFlagsPredicate("tcp_flags_has_none", TCPFlagsNone, args)
}

func tcpFlagsPredicate(fn string, mode TCPFlagPredicateMode, args []any) any {
	if len(args) < 2 {
		raiseTCPFlagListEmpty(fn)
	}
	mask, ok := maskFromArgs(fn, args[1:])
	if !ok {
		return nil
	}
	// The ORDER of these two is the family's rule, settled in #966 round 2 P4
	// and recorded in ADR-0012: the NAMES are folded into the mask FIRST, so
	// an unknown one is 22023 whatever the flags argument holds, and only then
	// does a NULL flags argument give NULL. PostgreSQL raises for the operator
	// equivalent — `NULL::bigint & 'x'::bigint` is 22P02, and so is
	// `'x'::int` under `WHERE false` — because a malformed mask operand is a
	// property of the QUERY and not of the rows.
	if args[0] == nil {
		return nil // NULL flags -> NULL, as PostgreSQL's (f & m) = m does
	}
	return TCPFlagsMatch(tcpFlagsValue(fn, args[0]), mask, mode)
}

// tcp_flags(flags) -> the names as an ARRAY of text, in header bit order.
//
// A bit outside the nine in tcpFlagBits is not a TCP flag and is not named.
// That is a rendering contract, not a silent loss: this function's job is to
// name TCP flags, and a garbage byte in one row of a telemetry column must not
// fail the query. `tcp_flags_has_*` are unaffected — they are bit arithmetic
// over the mask and never consult this table.
func fnTCPFlags(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := tcpFlagsValue("tcp_flags", args[0])
	out := make([]any, 0, len(tcpFlagBits))
	for _, f := range tcpFlagBits {
		if v&f.Bit != 0 {
			out = append(out, f.Name)
		}
	}
	return out
}

// tcp_flags_text(flags) -> 'SYN|ACK'. The same names as tcp_flags, joined —
// one string for a log line or a GROUP BY key. Zero flags is the empty string.
func fnTCPFlagsText(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := tcpFlagsValue("tcp_flags_text", args[0])
	var b strings.Builder
	for _, f := range tcpFlagBits {
		if v&f.Bit != 0 {
			if b.Len() > 0 {
				b.WriteByte('|')
			}
			b.WriteString(f.Name)
		}
	}
	return b.String()
}

// raiseUnknownTCPFlagName is the family's headline refusal: 22023
// (invalid_parameter_value), the class PostgreSQL's own DATE_TRUNC uses for a
// unit it does not know, with the offending name quoted and the nine accepted
// spellings listed.
func raiseUnknownTCPFlagName(fn, name string) {
	panic(fatalEval{errUnknownTCPFlagName(fn, name)})
}

// errUnknownTCPFlagName is the refusal as a VALUE, so the plan-time check and
// the per-row evaluator raise the same sentence under the same SQLSTATE. Two
// spellings of one refusal is how a query comes to fail differently depending
// on which layer noticed (ADR-0012 item 1).
func errUnknownTCPFlagName(fn, name string) error {
	return sqlerr.New("22023",
		"%s: TCP flag name %s not recognized; the names are %s (NS is accepted for AE)",
		fn, sqlerr.Quote(name), tcpFlagNamesList)
}

// raiseEmptyTCPFlagNameAt names the POSITION of an empty element in a
// comma-separated list, which is the one thing a caller cannot see from the
// value: quoting the name would quote nothing at all.
func raiseEmptyTCPFlagNameAt(fn string, pos int, text string) {
	panic(fatalEval{errEmptyTCPFlagNameAt(fn, pos, text)})
}

func errEmptyTCPFlagNameAt(fn string, pos int, text string) error {
	return sqlerr.New("22023",
		"%s: empty TCP flag name at position %d of %s", fn, pos, sqlerr.Quote(text))
}

func raiseTCPFlagListEmpty(fn string) {
	panic(fatalEval{errTCPFlagListEmpty(fn)})
}

func errTCPFlagListEmpty(fn string) error {
	return sqlerr.New("22023", "%s requires at least one TCP flag name", fn)
}

func raiseTCPFlagsNotInteger(fn string, v any) {
	panic(fatalEval{sqlerr.New("22023",
		"%s: the flags argument must be an integer, got %s", fn, describeFlagArg(v))})
}

func describeFlagArg(v any) string {
	switch v.(type) {
	case string:
		return "text"
	case []byte:
		return "bytea"
	case bool:
		return "boolean"
	case float64, float32:
		return "a fractional number"
	}
	return "a non-integer value"
}

// ---------------------------------------------------------------------------
// The typed row kernel.

// flagsTest is the row-level typed kernel for the three predicates: a bare
// column against a mask folded ONCE at compile time.
//
// It exists for the same reason ColEmptyStr and ColShapeLen do (shape_funcs.go)
// and is built the same way — a specialized BoolNullExpr holding the generic
// FuncCall as its Fallback, so every input shape it does not handle answers
// exactly as it did before. What it saves per row is the BOXING and the mask
// work: the value is read out of the column's own Int32Data/Int64Data slice
// rather than through an `any`, and the names were folded into one int64 at
// compile time instead of being re-read on every row.
//
// WHAT IT IS NOT, and an earlier version of this comment claimed it was
// (#966 round 2, P3): it is not a VECTOR kernel that dispatches once per batch
// and writes selection indices. `exec.Filter` calls a
// `func(*batch.RecordBatch, int) bool` — that is the seam every predicate in
// the engine goes through, not this family's — so EvalBoolNull is entered per
// row and its `switch v.Type` runs per row with it. The switch is also not
// pure dispatch: it is the SAFETY check that the column this batch carries is
// still the shape the kernel reads, which is exactly what ColEmptyStr's
// `v.Type != batch.TypeString` guard is, and hoisting it onto the cached
// ColRef type would trust a resolution made from a different batch.
//
// The vectorized flag kernel exists, and it is the one that matters: the scan
// evaluates the mask once per DICTIONARY ENTRY or over the plain page's own
// typed slice and hands back row spans (scan/flag_filter.go,
// scan/row_filter.go). This is the residual path for the rows that do not
// reach it — a non-catalog source, the kill switch off, a fragment's leftover
// conjunct. A batch-level boolean seam for exec.Filter would serve every
// operator and is not this family's to add.
//
// Restricted to TypeInt32 and TypeInt64 deliberately. A PORT or PROTOCOL
// column also stores in Int32Data, but its boxed form goes through the
// network-rendering path the Fallback takes, and a kernel that answered from
// the raw slice while the fallback answered from the rendered text would be
// two functions with one name. A VIEW vector (Base != nil) has nil typed
// slices and takes the fallback for the same reason.
type flagsTest struct {
	Col      *ColRef
	Mask     int64
	Mode     TCPFlagPredicateMode
	Fallback *FuncCall
}

// newFlagsTest builds the kernel when the call is one of the three predicates
// over a bare column reference with constant, resolvable flag names. Anything
// else — a computed flags expression, a name that is a column, an unknown name
// — returns nil and keeps the generic call, which raises at evaluation.
func newFlagsTest(fc *FuncCall) *flagsTest {
	mode, ok := TCPFlagPredicate(fc.Name)
	if !ok || len(fc.Args) < 2 {
		return nil
	}
	col, ok := fc.Args[0].(*ColRef)
	if !ok || col.structField != "" {
		return nil
	}
	names := make([]string, 0, len(fc.Args)-1)
	for _, a := range fc.Args[1:] {
		lit, ok := a.(*Lit)
		if !ok {
			return nil
		}
		s, ok := lit.Val.(string)
		if !ok {
			return nil
		}
		names = append(names, s)
	}
	mask, _, ok := TCPFlagMask(names)
	if !ok {
		return nil // an unknown name is the generic path's refusal to raise
	}
	return &flagsTest{Col: col, Mask: mask, Mode: mode, Fallback: fc}
}

func (e *flagsTest) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *flagsTest) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// fallback answers through the generic FuncCall, in the three-valued form the
// filter entry expects: NULL is (false, true), and a non-boolean answer — which
// this family never produces — is treated as UNKNOWN rather than as FALSE.
func (e *flagsTest) fallback(b *batch.RecordBatch, row int) (bool, bool) {
	v := e.Fallback.Eval(b, row)
	if v == nil {
		return false, true
	}
	if bv, ok := v.(bool); ok {
		return bv, false
	}
	return false, true
}

func (e *flagsTest) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	e.Col.resolve(b)
	if e.Col.idx < 0 || e.Col.idx >= len(b.Columns) || e.Col.structField != "" {
		return e.fallback(b, row)
	}
	v := b.Columns[e.Col.idx]
	if v.Base != nil {
		return e.fallback(b, row)
	}
	var flags int64
	switch v.Type {
	case batch.TypeInt32:
		if row >= len(v.Int32Data) {
			return e.fallback(b, row)
		}
		if v.Nulls.IsNullFast(row) {
			return false, true
		}
		flags = int64(v.Int32Data[row])
	case batch.TypeInt64:
		if row >= len(v.Int64Data) {
			return e.fallback(b, row)
		}
		if v.Nulls.IsNullFast(row) {
			return false, true
		}
		flags = v.Int64Data[row]
	default:
		return e.fallback(b, row)
	}
	return TCPFlagsMatch(flags, e.Mask, e.Mode), false
}

// ---------------------------------------------------------------------------
// The five TCP-flag inspection functions that predate this family.
//
// They carried their OWN name-to-bit table — eight entries, in a `byte` — and
// read the flags argument as `byte(ToInt64(v))`. Two tables for one thing in
// one package agree only by inspection, so they are folded onto the table
// above, and folding them fixes three things the byte carrier caused:
//
//   - THE AE/NS BIT WAS TRUNCATED AWAY. `tcp_flags_to_string(256)` answered
//     the empty string for a flags value with a bit set, and
//     `tcp_flags_to_string(511)` named eight flags of nine. A set bit rendered
//     as no flag at all is a wrong value, not a missing feature.
//   - `has_tcp_flag(f, 'AE')` answered NULL — twice over: the name was not in
//     the eight-entry table, and an unknown name answered NULL rather than
//     raising. NULL for "I do not know that name" is indistinguishable from
//     NULL for "the flags value is NULL".
//   - `tcp_flags_from_string('SYN,ACKK')` answered 2. A dropped name is a
//     silently SMALLER mask, which is a silently LARGER row set wherever the
//     result is compared — the same defect shape the new family refuses.
//
// The two spellings that change are now refusals (22023), which is what the
// rest of this family does with a name it does not know. `is_tcp_handshake`
// and `is_tcp_reset` are value-identical: their masks are all below 256, so
// the byte truncation never reached them.

// fnTCPFlagsToString converts a TCP flags bitmask to comma-separated names.
// tcp_flags_to_string(0x12) → 'SYN,ACK'. The COMMA separator is this
// function's documented rendering; tcp_flags_text uses a pipe.
func fnTCPFlagsToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := tcpFlagsValue("tcp_flags_to_string", args[0])
	parts := make([]string, 0, len(tcpFlagBits))
	for _, f := range tcpFlagBits {
		if v&f.Bit != 0 {
			parts = append(parts, f.Name)
		}
	}
	return strings.Join(parts, ",")
}

// fnHasTCPFlag tests if a TCP flags bitmask has a specific flag set.
// has_tcp_flag(0x12, 'SYN') → true. It is tcp_flags_has_any with one name.
//
// THE NAME IS READ BEFORE THE FLAGS ARGUMENT (#966 round 2, P4). It used to
// check both arguments for NULL first, so `has_tcp_flag(f, 'BOGUS')` answered
// NULL on a row whose f was NULL and raised 22023 on the next one — whether a
// typo is an error would have depended on the DATA, and the family's own
// three predicates already read the name first. PostgreSQL 17.11 decides it
// for the operator equivalent:
//
//	SELECT NULL::bigint & 'x'::bigint   ->  ERROR 22P02, invalid input syntax
//	SELECT 'x'::int FROM (VALUES (1)) t WHERE false  ->  the same ERROR
//
// The mask operand's spelling is converted whatever the rows are, and a NULL
// flags operand does not excuse it. A NULL *name* is different: that is a
// NULL mask operand, and `NULL & NULL` is NULL.
func fnHasTCPFlag(args []any) any {
	if len(args) < 2 || args[1] == nil {
		return nil
	}
	mask, ok := maskFromArgs("has_tcp_flag", args[1:2])
	if !ok || args[0] == nil {
		return nil
	}
	return TCPFlagsMatch(tcpFlagsValue("has_tcp_flag", args[0]), mask, TCPFlagsAny)
}

// fnTCPFlagsFromString converts a comma-separated name list to its bitmask.
// tcp_flags_from_string('SYN,ACK') → 18. An unrecognized name is 22023, not a
// quietly missing bit.
func fnTCPFlagsFromString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	text := toString(args[0])
	// An EMPTY STRING is a list of no names, and the mask of no names is 0 —
	// which is both the answer this function gave before #966 and the answer
	// PostgreSQL's bit arithmetic gives for a zero mask. A telemetry column
	// whose "no flags" spelling is the empty string must not fail the query.
	//
	// An empty ELEMENT is different: `'SYN,'` and `'SYN,,ACK'` are a list that
	// names something at a position and then names nothing there, which is a
	// slip rather than an empty list, and it is refused with the POSITION so
	// the message locates it (#966 round 2, P2).
	if strings.TrimSpace(text) == "" {
		return int64(0)
	}
	parts := strings.Split(text, ",")
	for i, part := range parts {
		if strings.TrimSpace(part) == "" {
			raiseEmptyTCPFlagNameAt("tcp_flags_from_string", i+1, text)
		}
	}
	mask, bad, ok := TCPFlagMask(parts)
	if !ok {
		raiseUnknownTCPFlagName("tcp_flags_from_string", strings.TrimSpace(bad))
	}
	return mask
}

// fnIsTCPHandshake tests for SYN set with ACK clear (connection initiation).
// Other bits are not consulted, so SYN+PSH is a handshake and SYN+ACK is not.
func fnIsTCPHandshake(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := tcpFlagsValue("is_tcp_handshake", args[0])
	return v&2 != 0 && v&16 == 0
}

// fnIsTCPReset tests for the RST flag.
func fnIsTCPReset(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return tcpFlagsValue("is_tcp_reset", args[0])&4 != 0
}
