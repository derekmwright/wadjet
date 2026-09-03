// Package expr provides a typed expression engine for evaluating SQL expressions
// against record batches. It replaces the string-based expression parsing with
// a compiled expression tree built from the SQL parser AST.
package expr

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"
	"math/rand"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/geoip"
	"golang.org/x/net/publicsuffix"
)

// Expr evaluates an expression against a record batch row, returning a typed value.
type Expr interface {
	Eval(b *batch.RecordBatch, row int) any
}

// BoolExpr evaluates a boolean expression (used for WHERE/HAVING/JOIN
// conditions). SQL's logic is THREE-valued and EvalBool is the two-valued
// COLLAPSE a filtering context applies: it answers true only for TRUE —
// FALSE and UNKNOWN rows are both kept out of a WHERE. The third value is
// carried by BoolNullExpr, and the two protocols must agree:
// EvalBool ≡ (val && !null) of EvalBoolNull.
type BoolExpr interface {
	EvalBool(b *batch.RecordBatch, row int) bool
}

// BoolNullExpr is the three-valued boolean protocol (#370): val is the
// answer and null reports UNKNOWN, in which case val is meaningless. Every
// boolean operator implements it — it is what lets NOT distinguish UNKNOWN
// (stays UNKNOWN, row excluded) from FALSE (becomes TRUE, row kept), and
// what a projection boxes into SQL NULL.
type BoolNullExpr interface {
	EvalBoolNull(b *batch.RecordBatch, row int) (val, null bool)
}

// evalBoolNull evaluates any expression on the three-valued protocol.
// Expressions without a native implementation (a boolean column, a function
// call) go through the boxed path, where nil is the UNKNOWN.
func evalBoolNull(e Expr, b *batch.RecordBatch, row int) (bool, bool) {
	if te, ok := e.(BoolNullExpr); ok {
		return te.EvalBoolNull(b, row)
	}
	v := e.Eval(b, row)
	if v == nil {
		return false, true
	}
	return toBoolVal(v), false
}

// boolNullBox is the one place the three-valued answer becomes a boxed SQL
// value: UNKNOWN is NULL.
func boolNullBox(val, null bool) any {
	if null {
		return nil
	}
	return val
}

// VecExpr evaluates an expression for an entire batch at once, writing results
// directly to the output vector. This avoids per-row interface dispatch and boxing.
type VecExpr interface {
	EvalVec(b *batch.RecordBatch, out *batch.Vector, n int)
}

// --- Leaf nodes ---

// ColRef reads a column value from the batch.
// Caches the column index and type after first resolution for zero-allocation
// reads on numeric types. Parallel pipeline workers share one *ColRef through
// the captured expression closures, so the resolution writes are published
// under a lock and read behind resolved.
type ColRef struct {
	Name string
	// resolved publishes idx/typ/structField: stored last under resolveMu,
	// and the only thing every accessor reads before using them. Every typed
	// accessor opens with resolve(), so this guard sits in the innermost row
	// loop of every expression — an inlined acquire load, where sync.Once.Do
	// cost a call plus a closure build per row.
	resolved    atomic.Bool
	resolveMu   sync.Mutex
	idx         int
	typ         batch.TypeID
	structField string // for ROW field access (e.g., "person.name" → structField="name")
	// fieldTyp is the DECLARED type of structField, and fieldIdx its position
	// among the container's children, both resolved alongside it. typ stays
	// the container's type — see resolveSlow — so these are the only place a
	// field path's own declaration is available (#568).
	fieldTyp batch.TypeID
	fieldIdx int
}

// ResolveColumnRef is the column lookup every ColRef performs, exported so a
// caller can ask whether a reference resolves WITHOUT evaluating it — which
// is what tells an absent column apart from a NULL one. It returns the batch
// column index (-1 when the name names nothing) and, for a ROW field path,
// the field name within it.
//
// Three spellings resolve, in order: the name exactly as written, the bare
// column after dropping a table qualifier, and a `row.field` path whose first
// part is a ROW column of the batch.
func ResolveColumnRef(b *batch.RecordBatch, name string) (idx int, structField string) {
	idx = b.ColumnIndex(name)
	if idx >= 0 {
		return idx, ""
	}
	if !strings.Contains(name, ".") {
		// A BARE reference the stream spells QUALIFIED. A join qualifies a
		// build column whose bare name the probe side also has, and
		// QualifyAllBuildCols qualifies every one of them for a self-join —
		// so `WITH c AS (SELECT id, a AS v FROM t) SELECT COUNT(*) FROM t x
		// JOIN c ON c.id = x.id JOIN t y ON c.id = y.id WHERE c.v > 1` hands
		// the filter above the second join the re-spelled `a`, while the
		// stream carries `t.a` and nothing called `a`. The predicate was
		// UNKNOWN on every row and the shuffled DAG answered ZERO (#700,
		// #726).
		//
		// The planner's own schema check has assumed this resolution since
		// #656 — physical.columnResolves matches a reference against a
		// qualified column by its bare part, in exactly this direction — so
		// implementing it here removes a disagreement between the checker and
		// the evaluator rather than adding a special case.
		//
		// Last resort and unambiguous only: an exact match already returned
		// above, and two columns whose bare names collide (`x.a` and `y.a`)
		// decline, keeping today's loud failure instead of guessing an arm.
		return uniqueQualifiedColumn(b, name), ""
	}
	parts := strings.SplitN(name, ".", 2)
	// Try unqualified (strip table prefix)
	if idx = b.ColumnIndex(parts[1]); idx >= 0 {
		return idx, ""
	}
	// Try as struct field access: parts[0] is a ROW column, parts[1] is field
	// name. The container is looked up with the same qualified fallback the
	// scalar branches use, because a JOIN qualifies a colliding column and
	// `c_row.b` then has to find `x.c_row` — asking only for the exact
	// spelling left the path unresolved and the branch below bound the FIELD
	// name to whatever scalar column happened to end in `.b`, which is
	// ADR-0022's violation exactly.
	parentIdx := b.ColumnIndex(parts[0])
	if parentIdx < 0 {
		parentIdx = uniqueQualifiedColumn(b, parts[0])
	}
	if parentIdx >= 0 && b.Columns[parentIdx].Type == batch.TypeRow {
		return parentIdx, parts[1]
	}
	// A reference whose QUALIFIER names a ROW column of this batch is a FIELD
	// PATH and nothing else (ADR-0022): if the field is not one of that
	// container's children the answer is "no such thing", never some other
	// column that happens to share the field's name. rowColumnNamed is the
	// whole of that test — it finds a ROW spelled `parts[0]` or
	// `<qualifier>.parts[0]`, including the AMBIGUOUS case the lookup above
	// declines.
	//
	// Asking `parentIdx >= 0` here instead was INVERTED: the ROW arm above
	// has already returned, so this point is reached only when the qualifier
	// names a column that is NOT a ROW — an ordinary scalar an arm happens to
	// have called `c` — and the refusal then swallowed every qualified
	// reference whose qualifier collided with such a name:
	//
	//	WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
	//	SELECT COUNT(*) FROM decpair t
	//	JOIN (SELECT id, b AS c FROM decpair) z ON z.id = t.id
	//	JOIN c ON c.id = t.id WHERE c.dv > 1
	//	-- PostgreSQL 5 · single 5 · DAG broadcast 5 · DAG shuffled 0
	//
	// The same query with the arm publishing `zz` answered 5, which is what
	// says the collision was the whole of it.
	if rowColumnNamed(b, parts[0]) {
		return -1, ""
	}
	// A QUALIFIED reference the stream spells under a DIFFERENT qualifier.
	//
	// The mirror of the bare branch above, and the same disagreement one
	// spelling over. `QualifyAllBuildCols` renames every build column to the
	// build's TABLE alias, so a CTE or derived table on the build side of a
	// self-join publishes `decpair.dv` while every consumer above the join
	// spells it with the arm's own alias:
	//
	//	WITH c AS (SELECT id, SUM(f) * 2 AS dv FROM t GROUP BY id)
	//	SELECT COUNT(*) FROM t x JOIN c ON c.id = x.id JOIN t y ON c.id = y.id
	//	WHERE c.dv > 1
	//	-- PostgreSQL 6 · single 6 · both DAG arms 0, in silence (#762)
	//
	// The BARE spelling of that same predicate (`WHERE dv > 1`) already
	// answered 6, through the branch above — which is what says the
	// qualifier is the whole of it and not the carrying.
	//
	// physical.columnResolves has accepted this direction since #656 (its
	// last loop compares the two names by their bare parts), so the
	// planner's checker believed in a resolution the evaluator did not
	// implement, and every check waved the plan through. Implementing it
	// removes that disagreement rather than adding a special case — the same
	// argument the bare branch above was added under.
	//
	// Last resort and UNAMBIGUOUS ONLY: two arms that both spell it
	// (`p.w` and `q.w` on one stream, with `x.w` asked for) decline and keep
	// the loud failure rather than guessing an arm, which is the direction
	// #742 is about.
	if idx = uniqueQualifiedColumn(b, parts[1]); idx >= 0 {
		return idx, ""
	}
	return -1, ""
}

// rowColumnNamed reports whether any column of b is a ROW spelled `parts[0]`
// or `<qualifier>.parts[0]`. It is the ambiguous case uniqueQualifiedColumn
// declines: two arms carrying a ROW of the same name give a field path no
// single container, and binding the FIELD name to a scalar instead is worse
// than answering nothing.
func rowColumnNamed(b *batch.RecordBatch, name string) bool {
	for i := range b.Schema {
		if b.Columns[i].Type != batch.TypeRow {
			continue
		}
		n := b.Schema[i].Name
		if strings.EqualFold(n, name) {
			return true
		}
		if dot := strings.IndexByte(n, '.'); dot >= 0 && strings.EqualFold(n[dot+1:], name) {
			return true
		}
	}
	return false
}

// uniqueQualifiedColumn returns the index of the ONE column of b spelled
// `<qualifier>.<bare>`, or -1 when none or more than one matches. See the
// bare-reference branch of ResolveColumnRef for why this direction exists.
func uniqueQualifiedColumn(b *batch.RecordBatch, bare string) int {
	found := -1
	for i := range b.Schema {
		n := b.Schema[i].Name
		dot := strings.IndexByte(n, '.')
		if dot < 0 || !strings.EqualFold(n[dot+1:], bare) {
			continue
		}
		if found >= 0 {
			return -1 // two arms spell it; the reference is ambiguous here
		}
		found = i
	}
	return found
}

// resolve performs first-time column lookup. Idempotent.
func (e *ColRef) resolve(b *batch.RecordBatch) {
	if !e.resolved.Load() {
		e.resolveSlow(b)
	}
}

func (e *ColRef) resolveSlow(b *batch.RecordBatch) {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	e.idx, e.structField = ResolveColumnRef(b, e.Name)
	if e.idx >= 0 {
		e.typ = b.Columns[e.idx].Type
	}
	if e.structField != "" {
		e.fieldIdx = -1
		for i, name := range b.Columns[e.idx].FieldNames {
			if strings.EqualFold(name, e.structField) && i < len(b.Columns[e.idx].Children) {
				e.fieldIdx = i
				break
			}
		}
		// The FIELD's own declared type, kept apart from e.typ on purpose.
		// e.typ stays the CONTAINER's (TypeRow), because it is what every
		// typed kernel keyed on this reference indexes storage with —
		// b.Columns[e.idx] is the ROW vector, whose Int64Data/BytesData are
		// empty, so a field type there sends the network comparator and the
		// float vector path reading off the end of a zero-length slice.
		// fieldTyp is the DECLARATION half of the same reference, and it is
		// what the declaration-driven comparison rules ask for (#568).
		e.fieldTyp = batch.TypeRow
		if e.fieldIdx >= 0 {
			e.fieldTyp = b.Columns[e.idx].Children[e.fieldIdx].Type
		}
	}
	e.resolved.Store(true)
}

// fieldVector resolves a ROW field to the CHILD VECTOR that holds it and the
// row index within it, or reports that the field has no value here (the
// container is NULL, or the field is not one of its children).
//
// A view is followed to its base the way Vector.GetValue follows one: a
// view's children are not addressable by the view's own row index, only the
// base's are.
func (e *ColRef) fieldVector(b *batch.RecordBatch, row int) (*batch.Vector, int, bool) {
	v := b.Columns[e.idx]
	for {
		if v.Nulls.IsNullFast(row) {
			return nil, 0, false
		}
		if v.Base == nil {
			break
		}
		row = int(v.Indices[row])
		v = v.Base
	}
	if e.fieldIdx < 0 || e.fieldIdx >= len(v.Children) {
		return nil, 0, false
	}
	return v.Children[e.fieldIdx], row, true
}

// fieldValue boxes a ROW field's value the way a COLUMN of that type boxes
// its own — through the child vector's typed storage, not through the
// container's map.
//
// The distinction is not cosmetic. Vector.GetValue renders an IPv4 or a MAC
// as its TEXT, while ColRef.Eval hands back the raw int64 a column of that
// type stores. Reading the field out of the container's boxed map therefore
// produced a value that could not be compared against the identical value in
// a column: `WHERE rw.ip = ip_col` matched NO rows with both sides holding
// "9.0.0.1", because one was int64 and the other a string (#568). Going
// through the child vector makes a field path and a column interchangeable
// wherever a boxed value travels.
func (e *ColRef) fieldValue(b *batch.RecordBatch, row int) any {
	fv, r, ok := e.fieldVector(b, row)
	if !ok {
		return nil
	}
	return boxVectorValue(fv, r)
}

// valueType is the declared type of the VALUE this reference yields: the
// FIELD's for a ROW field path, the column's otherwise. Every renderer that
// has to undo Eval's typed boxing asks for this rather than for typ, which
// for a field path names the CONTAINER.
func (e *ColRef) valueType() batch.TypeID {
	if e.structField != "" {
		return e.fieldTyp
	}
	return e.typ
}

// valueVector resolves this reference to the VECTOR that actually holds its
// value and the row index within it: the column itself, or — for a ROW FIELD
// PATH — the container's child.
//
// It is the seam every family that has to UNDO ColRef.Eval's boxing needs.
// Those families all read the reference's vector directly (columnInstant,
// Vector.GetValue) because the box has lost something the storage still
// knows: an IPv4's dotted-quad text, a DATE's unit. Each of them used to
// reach for b.Columns[cr.idx] and skip a field path outright, which left the
// field boxed as a raw number in exactly the places the number is wrong —
// UPPER(rw.ipv4) rendered "150994945" and DATE_TRUNC('month', rw.date) read
// an epoch DAY as epoch SECONDS and answered 1970-01-01 (#568).
//
// Pair it with valueType(): the two together are "what this reference
// declares and where its bytes are", for a column and a field alike.
func (e *ColRef) valueVector(b *batch.RecordBatch, row int) (*batch.Vector, int, bool) {
	if e.idx < 0 || e.idx >= len(b.Columns) {
		return nil, 0, false
	}
	if e.structField == "" {
		return b.Columns[e.idx], row, true
	}
	return e.fieldVector(b, row)
}

// displayValue is the value as Vector.GetValue renders it — the text form for
// the types Eval boxes as a number. Reports false when this reference cannot
// be resolved to a vector at all, in which case the caller keeps the box it
// already has.
func (e *ColRef) displayValue(b *batch.RecordBatch, row int) (any, bool) {
	v, r, ok := e.valueVector(b, row)
	if !ok {
		return nil, false
	}
	return v.GetValue(r), true
}

// vectorFloat64 and vectorInt64 are EvalFloat64's and EvalInt64's typed reads
// applied to whichever vector actually holds the value — the ROW field's
// child, for a field path.
//
// The arms mirror the column ones exactly, and must keep mirroring them.
// Routing the field through the BOXED value instead (toFloat64Safe of
// ColRef.Eval's result) is what these replace, and it lost the one type whose
// box is not a number: a DECIMAL boxes as its rendered TEXT, so `rw.dec + 1`
// answered NULL where `dec_col + 1` answers the sum (#568). The column arms
// are left untouched on purpose — EvalFloat64 is the numeric hot path, and
// this costs it nothing.
func vectorFloat64(v *batch.Vector, row int) (float64, bool) {
	if v.Nulls.IsNullFast(row) {
		return 0, false
	}
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[row], true
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return float64(v.Int64Data[row]), true
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return float64(v.Int32Data[row]), true
	case batch.TypeFloat32:
		return float64(v.Float32Data[row]), true
	case batch.TypeDecimal:
		return v.DecimalData.Data[row].ToFloat64(v.DecimalData.Scale), true
	default:
		return 0, false
	}
}

func vectorInt64(v *batch.Vector, row int) (int64, bool) {
	if v.Nulls.IsNullFast(row) {
		return 0, false
	}
	switch v.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return v.Int64Data[row], true
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(v.Int32Data[row]), true
	case batch.TypeFloat64:
		return int64(v.Float64Data[row]), true
	case batch.TypeFloat32:
		return int64(v.Float32Data[row]), true
	default:
		return 0, false
	}
}

// boxVectorValue is ColRef.Eval's typed boxing, applied to an arbitrary
// vector. The two must agree: it is what makes a ROW field's box identical to
// the box a column of the same type produces.
func boxVectorValue(v *batch.Vector, row int) any {
	if v.Nulls.IsNullFast(row) {
		return nil
	}
	switch v.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return v.Int64Data[row]
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(v.Int32Data[row])
	case batch.TypeFloat64:
		return v.Float64Data[row]
	case batch.TypeFloat32:
		return float64(v.Float32Data[row])
	case batch.TypeBool:
		return v.BoolData[row]
	case batch.TypeString:
		val, ok := v.GetString(row)
		if !ok {
			return nil
		}
		return val
	default:
		return v.GetValue(row)
	}
}

func (e *ColRef) Eval(b *batch.RecordBatch, row int) any {
	e.resolve(b)
	if e.idx < 0 || e.idx >= len(b.Columns) {
		return nil
	}
	// Struct field access: extract named field from ROW value
	if e.structField != "" {
		return e.fieldValue(b, row)
	}
	v := b.Columns[e.idx]
	// Use typed accessors to avoid boxing where possible for numeric hot paths.
	// For comparisons and arithmetic, the caller will use ToFloat64/ToInt64
	// which handle int64/float64 natively without re-boxing.
	switch e.typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		if v.Nulls.IsNullFast(row) {
			return nil
		}
		return v.Int64Data[row]
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		if v.Nulls.IsNullFast(row) {
			return nil
		}
		return int64(v.Int32Data[row])
	case batch.TypeFloat64:
		if v.Nulls.IsNullFast(row) {
			return nil
		}
		return v.Float64Data[row]
	case batch.TypeFloat32:
		if v.Nulls.IsNullFast(row) {
			return nil
		}
		return float64(v.Float32Data[row])
	case batch.TypeBool:
		if v.Nulls.IsNullFast(row) {
			return nil
		}
		return v.BoolData[row]
	case batch.TypeString:
		val, ok := v.GetString(row)
		if !ok {
			return nil
		}
		return val
	default:
		return v.GetValue(row)
	}
}

// EvalFloat64 reads the column value as float64 without any boxing.
// Returns (0, false) if null or column not found.
// Uses cached column type to dispatch directly to the typed data slice,
// avoiding the extra function call and redundant type switch in GetNumericFloat64.
func (e *ColRef) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	e.resolve(b)
	if e.idx < 0 || e.idx >= len(b.Columns) {
		return 0, false
	}
	if e.structField != "" {
		fv, r, ok := e.fieldVector(b, row)
		if !ok {
			return 0, false
		}
		return vectorFloat64(fv, r)
	}
	v := b.Columns[e.idx]
	if v.Nulls.IsNullFast(row) {
		return 0, false
	}
	switch e.typ {
	case batch.TypeFloat64:
		return v.Float64Data[row], true
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return float64(v.Int64Data[row]), true
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return float64(v.Int32Data[row]), true
	case batch.TypeFloat32:
		return float64(v.Float32Data[row]), true
	case batch.TypeDecimal:
		return v.DecimalData.Data[row].ToFloat64(v.DecimalData.Scale), true
	default:
		return 0, false
	}
}

// EvalFloat64Vec evaluates the column for all rows [0, n) into dst.
func (e *ColRef) EvalFloat64Vec(b *batch.RecordBatch, dst []float64, n int) bool {
	e.resolve(b)
	if e.structField != "" {
		hasNulls := false
		for i := 0; i < n; i++ {
			f, ok := e.EvalFloat64(b, i)
			if !ok {
				hasNulls = true
			}
			dst[i] = f
		}
		return hasNulls
	}
	if e.idx < 0 || e.idx >= len(b.Columns) {
		for i := 0; i < n; i++ {
			dst[i] = 0
		}
		return true
	}
	v := b.Columns[e.idx]
	switch e.typ {
	case batch.TypeFloat64:
		copy(dst[:n], v.Float64Data[:n])
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for i := 0; i < n; i++ {
			dst[i] = float64(v.Int64Data[i])
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for i := 0; i < n; i++ {
			dst[i] = float64(v.Int32Data[i])
		}
	case batch.TypeFloat32:
		for i := 0; i < n; i++ {
			dst[i] = float64(v.Float32Data[i])
		}
	case batch.TypeDecimal:
		scale := v.DecimalData.Scale
		for i := 0; i < n; i++ {
			dst[i] = v.DecimalData.Data[i].ToFloat64(scale)
		}
	default:
		for i := 0; i < n; i++ {
			dst[i] = 0
		}
		return true
	}
	return v.Nulls.HasNulls()
}

// EvalString reads the column value as string without boxing.
func (e *ColRef) EvalString(b *batch.RecordBatch, row int) (string, bool) {
	e.resolve(b)
	if e.idx < 0 || e.idx >= len(b.Columns) {
		return "", false
	}
	if e.structField != "" {
		v := e.fieldValue(b, row)
		if v == nil {
			return "", false
		}
		if s, ok := v.(string); ok {
			return s, true
		}
		return fmt.Sprint(v), true
	}
	return b.Columns[e.idx].GetString(row)
}

// EvalInt64 reads the column value as int64 without boxing.
func (e *ColRef) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	e.resolve(b)
	if e.idx < 0 || e.idx >= len(b.Columns) {
		return 0, false
	}
	if e.structField != "" {
		fv, r, ok := e.fieldVector(b, row)
		if !ok {
			return 0, false
		}
		return vectorInt64(fv, r)
	}
	v := b.Columns[e.idx]
	if v.Nulls.IsNullFast(row) {
		return 0, false
	}
	switch e.typ {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return v.Int64Data[row], true
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return int64(v.Int32Data[row]), true
	case batch.TypeFloat64:
		return int64(v.Float64Data[row]), true
	case batch.TypeFloat32:
		return int64(v.Float32Data[row]), true
	default:
		return 0, false
	}
}

// Lit returns a constant value.
type Lit struct {
	Val any
	// Text is the numeric literal's source text, kept verbatim. Val is the
	// literal boxed for arithmetic — an int64 where one is exact, a float64
	// otherwise — and a float64 carries ~15-16 significant decimal digits
	// where a DECIMAL(38,10) column carries 38, so the box alone cannot say
	// which number was written (#452). Comparisons against a DECIMAL column
	// read this instead and compare in the column's own domain; everything
	// else keeps reading Val and is unchanged. Empty for a non-numeric
	// literal.
	Text string
}

func (e *Lit) Eval(_ *batch.RecordBatch, _ int) any {
	return e.Val
}

func (e *Lit) EvalFloat64(_ *batch.RecordBatch, _ int) (float64, bool) {
	if e.Val == nil {
		return 0, false
	}
	return ToFloat64(e.Val), true
}

func (e *Lit) EvalInt64(_ *batch.RecordBatch, _ int) (int64, bool) {
	if e.Val == nil {
		return 0, false
	}
	return ToInt64(e.Val), true
}

// EvalFloat64Vec fills dst[0:n] with the literal value.
func (e *Lit) EvalFloat64Vec(_ *batch.RecordBatch, dst []float64, n int) bool {
	if e.Val == nil {
		for i := 0; i < n; i++ {
			dst[i] = 0
		}
		return true // all null
	}
	v := ToFloat64(e.Val)
	for i := 0; i < n; i++ {
		dst[i] = v
	}
	return false
}

// --- Typed expression interfaces for zero-boxing hot paths ---

// Float64Expr evaluates to float64 without boxing.
type Float64Expr interface {
	EvalFloat64(b *batch.RecordBatch, row int) (float64, bool)
}

// Int64Expr evaluates to int64 without boxing.
type Int64Expr interface {
	EvalInt64(b *batch.RecordBatch, row int) (int64, bool)
}

// VecFloat64Expr evaluates an expression for all rows [0, n) at once,
// writing results to dst. Returns true if any output is null.
// Eliminates per-row function call overhead (~5 calls/row/expression).
type VecFloat64Expr interface {
	EvalFloat64Vec(b *batch.RecordBatch, dst []float64, n int) bool
}

// arithOp is a pre-resolved opcode for arithmetic operations.
// Using an integer switch instead of string comparison eliminates
// per-row string matching in BinOpFloat64/BinOpInt64 hot paths.
type arithOp uint8

const (
	arithAdd arithOp = iota
	arithSub
	arithMul
	arithDiv
	arithMod
	arithUnknown
)

func resolveArithOp(op string) arithOp {
	switch op {
	case "+":
		return arithAdd
	case "-":
		return arithSub
	case "*":
		return arithMul
	case "/":
		return arithDiv
	case "%":
		return arithMod
	default:
		return arithUnknown
	}
}

// --- Interval type ---

// IntervalValue represents a SQL INTERVAL (e.g., INTERVAL '30' DAY).
type IntervalValue struct {
	Years   int
	Months  int
	Days    int
	Hours   int
	Minutes int
	Seconds int
}

// addInterval applies an interval to an instant.
func addInterval(t time.Time, iv IntervalValue, subtract bool) time.Time {
	sign := 1
	if subtract {
		sign = -1
	}
	t = t.AddDate(sign*iv.Years, sign*iv.Months, sign*iv.Days)
	return t.Add(time.Duration(sign) * (time.Duration(iv.Hours)*time.Hour +
		time.Duration(iv.Minutes)*time.Minute +
		time.Duration(iv.Seconds)*time.Second))
}

// dateAddInterval applies an interval to a date/timestamp string.
func dateAddInterval(dateStr string, iv IntervalValue, subtract bool) string {
	t := parseDateValue(dateStr)
	if t.IsZero() {
		return ""
	}
	t = addInterval(t, iv, subtract)
	// Return date format if no time component, otherwise RFC3339.
	if iv.Hours == 0 && iv.Minutes == 0 && iv.Seconds == 0 &&
		t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 {
		return t.Format("2006-01-02")
	}
	return t.Format(time.RFC3339)
}

// --- Arithmetic ---

// BinOp is a binary arithmetic expression (generic, uses ToFloat64).
type BinOp struct {
	Left, Right Expr
	Op          string // +, -, *, /, %
	// dec is the EXACT fixed-point arm (ADR-0024 item 3, #555). This node is
	// where operands with no typed protocol meet — a negated column, a CAST —
	// and a DECIMAL is one of them, because its box is text and nothing about
	// it satisfies Float64Expr for compileBinOp to see. Resolved once, against
	// the first batch. See binop_decimal.go.
	dec decArm
}

func (e *BinOp) Eval(b *batch.RecordBatch, row int) any {
	// Exact fixed-point arithmetic, ahead of the ToFloat64 pair below: `-a + b`
	// over two DECIMAL columns is a numeric expression in PostgreSQL and an
	// exact one here, where reading both boxes as doubles loses every digit
	// past the sixteenth.
	if v, ok := e.binOpDecimalBox(b, row); ok {
		return v
	}
	lv := e.Left.Eval(b, row)
	rv := e.Right.Eval(b, row)
	if lv == nil || rv == nil {
		return nil
	}

	// Date ± interval arithmetic. Subtraction is not commutative, so only the
	// date-on-the-left form takes an interval on the right; `interval + date`
	// is the one reversed shape that means anything.
	if e.Op == "+" || e.Op == "-" {
		if iv, ok := rv.(IntervalValue); ok {
			if dv, ok := temporalOperand(b, row, e.Left, lv); ok {
				return intervalShift(dv, iv, e.Op == "-")
			}
		}
		if iv, ok := lv.(IntervalValue); ok && e.Op == "+" {
			if dv, ok := temporalOperand(b, row, e.Right, rv); ok {
				return intervalShift(dv, iv, false)
			}
		}
		// date - date and date ± n. Declines for everything that is not
		// date arithmetic, leaving the numeric path below untouched (#340).
		if res, ok := e.dateArith(b, row, lv, rv); ok {
			return res
		}
	}

	lf := ToFloat64(lv)
	rf := ToFloat64(rv)
	switch e.Op {
	case "+":
		return lf + rf
	case "-":
		return lf - rf
	case "*":
		return lf * rf
	case "/":
		// int / int is INTEGER division, truncating toward zero (#369,
		// PostgreSQL semantics per ADR-0012). This generic node is where
		// operands without a typed protocol arrive — a CAST, a negated
		// column — so the rule must hold here too, not only in the typed
		// kernels. A float on either side keeps float division.
		if li, lok := toInt64Safe(lv); lok {
			if ri, rok := toInt64Safe(rv); rok {
				if ri == 0 {
					return nil
				}
				// The CHECKED divide, like every other integer arm: this node
				// is where operands with no typed protocol arrive, so
				// MinInt64 / -1 reached it too and wrapped back to MinInt64
				// where PostgreSQL raises `bigint out of range` (#637, #555
				// review).
				return divInt64Checked(li, ri)
			}
		}
		if rf == 0 {
			// Both operands are non-NULL here (checked above), so this is a
			// genuine zero divisor: PostgreSQL refuses it and so do we (#367).
			raiseDivisionByZero()
		}
		return lf / rf
	case "%":
		if rf == 0 {
			raiseDivisionByZero()
		}
		// math.Mod — see BinOpFloat64's arithMod arm for why truncating to
		// integers first was both wrong and a crash (#555 review, N1).
		return math.Mod(lf, rf)
	default:
		return nil
	}
}

// BinOpFloat64 is a typed binary op that operates on float64 without boxing.
// Uses a pre-resolved arithOp opcode for the hot EvalFloat64 path to avoid
// per-row string comparison on the Op field. The opcode is resolved lazily
// so external callers can construct BinOpFloat64 directly with only Op
// populated.
//
// opReady is a double-checked atomic flag rather than sync.Once: Once.Do
// builds a closure and loads the done flag on EVERY row, and that closure
// keeps resolveOpCode too big to inline. This form is small enough that the
// compiler inlines resolveOpCode straight into EvalFloat64 (verified with
// -gcflags='-m'), the same guard BinOpNumeric.resolveMode and ColRef.resolve
// already use. opReady publishes opCode: set last under opMu, read first
// (and alone) by EvalFloat64 — concurrent pipeline workers share one
// *BinOpFloat64 through a captured closure, same as those two.
type BinOpFloat64 struct {
	Left, Right Float64Expr
	Op          string
	opCode      arithOp
	opReady     atomic.Bool
	opMu        sync.Mutex
	vecBuf      []float64 // scratch buffer for vectorized evaluation
}

func (e *BinOpFloat64) Eval(b *batch.RecordBatch, row int) any {
	v, ok := e.EvalFloat64(b, row)
	if !ok {
		return nil
	}
	return v
}

func (e *BinOpFloat64) resolveOpCode() {
	if !e.opReady.Load() {
		e.resolveOpCodeSlow()
	}
}

// resolveOpCodeSlow runs exactly once per node.
func (e *BinOpFloat64) resolveOpCodeSlow() {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	if e.opReady.Load() {
		return
	}
	e.opCode = resolveArithOp(e.Op)
	e.opReady.Store(true)
}

func (e *BinOpFloat64) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	lf, lok := e.Left.EvalFloat64(b, row)
	if !lok {
		return 0, false
	}
	rf, rok := e.Right.EvalFloat64(b, row)
	if !rok {
		return 0, false
	}
	e.resolveOpCode()
	switch e.opCode {
	case arithAdd:
		return lf + rf, true
	case arithSub:
		return lf - rf, true
	case arithMul:
		return lf * rf, true
	case arithDiv:
		if rf == 0 {
			// A NULL divisor returned above already; a zero here is genuine.
			raiseDivisionByZero()
		}
		return lf / rf, true
	case arithMod:
		if rf == 0 {
			raiseDivisionByZero()
		}
		// math.Mod, not `int64(lf) % int64(rf)`: truncating both operands to
		// integers first answered 1 for `7.9 % 3.0` where every engine with
		// the operator answers 1.9, and — worse — it PANICKED with an integer
		// divide by zero for a divisor whose integer part is 0, which is
		// every `x % 0.5` (#555 review, N1). PostgreSQL has no `%` over
		// double precision at all, so this pair is a superset either way; a
		// superset that crashes the query is not one.
		return math.Mod(lf, rf), true
	default:
		return 0, false
	}
}

// CloneVec creates a deep copy of the BinOpFloat64 tree with fresh scratch
// buffers. Required for parallel pipeline execution where multiple workers
// must not share mutable vecBuf state. Stateless leaf nodes (ColRef, Literal)
// are shared; only BinOpFloat64 nodes (which own vecBuf) are cloned.
func (e *BinOpFloat64) CloneVec() *BinOpFloat64 {
	clone := &BinOpFloat64{
		Op:     e.Op,
		opCode: e.opCode,
		// vecBuf intentionally nil — each clone allocates on first use
	}
	if child, ok := e.Left.(*BinOpFloat64); ok {
		clone.Left = child.CloneVec()
	} else {
		clone.Left = e.Left
	}
	if child, ok := e.Right.(*BinOpFloat64); ok {
		clone.Right = child.CloneVec()
	} else {
		clone.Right = e.Right
	}
	return clone
}

// EvalFloat64Vec evaluates left and right operands in bulk, then applies the
// arithmetic op in a tight loop. Eliminates ~5 function calls per row.
func (e *BinOpFloat64) EvalFloat64Vec(b *batch.RecordBatch, dst []float64, n int) bool {
	e.resolveOpCode()

	// Fused (column op constant) fast path: one typed convert+op loop, no
	// constant-vector fill and no separate column→float64 conversion pass.
	// ClickBench Q30 (90 SUM(col + k) expressions over one int16 column)
	// spent 20% of CPU materializing constant vectors and 25% re-converting
	// the same column per expression; this path removes both.
	if cr, ok := e.Left.(*ColRef); ok {
		if lit, ok2 := e.Right.(*Lit); ok2 && lit.Val != nil {
			if done, hasNull := fusedColConstFloat64(b, cr, ToFloat64(lit.Val), e.opCode, false, dst, n); done {
				return hasNull
			}
		}
	}
	if lit, ok := e.Left.(*Lit); ok && lit.Val != nil {
		if cr, ok2 := e.Right.(*ColRef); ok2 {
			if done, hasNull := fusedColConstFloat64(b, cr, ToFloat64(lit.Val), e.opCode, true, dst, n); done {
				return hasNull
			}
		}
	}

	// Check if children support vectorized evaluation
	leftVec, leftOK := e.Left.(VecFloat64Expr)
	rightVec, rightOK := e.Right.(VecFloat64Expr)
	if !leftOK || !rightOK {
		// Fallback to per-row evaluation
		hasNull := false
		for i := 0; i < n; i++ {
			v, ok := e.EvalFloat64(b, i)
			dst[i] = v
			if !ok {
				hasNull = true
			}
		}
		return hasNull
	}

	// Evaluate right into dst, left into scratch buffer.
	// Reuse scratch slice across calls; grow only if needed.
	rightNull := rightVec.EvalFloat64Vec(b, dst, n)
	if cap(e.vecBuf) < n {
		e.vecBuf = make([]float64, n)
	}
	tmp := e.vecBuf[:n]
	leftNull := leftVec.EvalFloat64Vec(b, tmp, n)

	// Apply op in tight loop (compiler can auto-vectorize these)
	switch e.opCode {
	case arithAdd:
		for i := 0; i < n; i++ {
			dst[i] = tmp[i] + dst[i]
		}
	case arithSub:
		for i := 0; i < n; i++ {
			dst[i] = tmp[i] - dst[i]
		}
	case arithMul:
		for i := 0; i < n; i++ {
			dst[i] = tmp[i] * dst[i]
		}
	case arithDiv:
		// A zero divisor slot is either a NULL row (whose placeholder is 0)
		// or a genuine zero. This loop cannot tell the two apart, so it
		// reports "has nulls" and lets the caller's per-row pass decide:
		// EvalFloat64 answers NULL for the NULL row and raises 22012
		// (division by zero) for the genuine one. Before this, a zero
		// divisor silently produced 0 (#367).
		sawZero := false
		for i := 0; i < n; i++ {
			if dst[i] != 0 {
				dst[i] = tmp[i] / dst[i]
			} else {
				sawZero = true
			}
		}
		if sawZero {
			return true
		}
	case arithMod:
		sawZero := false
		for i := 0; i < n; i++ {
			if dst[i] != 0 {
				dst[i] = float64(int64(tmp[i]) % int64(dst[i]))
			} else {
				sawZero = true
			}
		}
		if sawZero {
			return true
		}
	}

	return leftNull || rightNull
}

// fusedColConstFloat64 computes dst = col op c (or c op col when constFirst)
// in a single typed loop. Returns done=false when the column type or op has
// no fused kernel — the caller falls through to the generic two-pass path.
// Division keeps the fused path only when the CONSTANT is the (non-zero)
// divisor; per-element zero-divisor handling stays on the generic path.
func fusedColConstFloat64(b *batch.RecordBatch, cr *ColRef, c float64, op arithOp, constFirst bool, dst []float64, n int) (bool, bool) {
	switch op {
	case arithAdd, arithSub, arithMul:
	case arithDiv:
		if constFirst || c == 0 {
			return false, false
		}
	default:
		return false, false
	}
	cr.resolve(b)
	if cr.idx < 0 || cr.idx >= len(b.Columns) {
		return false, false
	}
	v := b.Columns[cr.idx]

	apply := func(get func(i int) float64) {
		switch {
		case op == arithAdd:
			for i := 0; i < n; i++ {
				dst[i] = get(i) + c
			}
		case op == arithSub && !constFirst:
			for i := 0; i < n; i++ {
				dst[i] = get(i) - c
			}
		case op == arithSub:
			for i := 0; i < n; i++ {
				dst[i] = c - get(i)
			}
		case op == arithMul:
			for i := 0; i < n; i++ {
				dst[i] = get(i) * c
			}
		case op == arithDiv:
			for i := 0; i < n; i++ {
				dst[i] = get(i) / c
			}
		}
	}

	// Monomorphic loops per storage type: the closure indirection above
	// would defeat the point, so each case runs its own tight loop.
	switch cr.typ {
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		src := v.Int32Data
		switch {
		case op == arithAdd:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) + c
			}
		case op == arithSub && !constFirst:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) - c
			}
		case op == arithSub:
			for i := 0; i < n; i++ {
				dst[i] = c - float64(src[i])
			}
		case op == arithMul:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) * c
			}
		default:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) / c
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		src := v.Int64Data
		switch {
		case op == arithAdd:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) + c
			}
		case op == arithSub && !constFirst:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) - c
			}
		case op == arithSub:
			for i := 0; i < n; i++ {
				dst[i] = c - float64(src[i])
			}
		case op == arithMul:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) * c
			}
		default:
			for i := 0; i < n; i++ {
				dst[i] = float64(src[i]) / c
			}
		}
	case batch.TypeFloat64:
		apply(func(i int) float64 { return v.Float64Data[i] })
	case batch.TypeFloat32:
		apply(func(i int) float64 { return float64(v.Float32Data[i]) })
	default:
		return false, false
	}
	return true, v.Nulls.HasNulls()
}

// BinOpInt64 is a typed binary op that operates on int64 without boxing.
// opCode is resolved lazily through the same double-checked atomic.Bool
// guard as BinOpFloat64 — see that type for why sync.Once doesn't fit the
// inliner and this does.
type BinOpInt64 struct {
	Left, Right Int64Expr
	Op          string
	opCode      arithOp
	opReady     atomic.Bool
	opMu        sync.Mutex
}

func (e *BinOpInt64) resolveOpCode() {
	if !e.opReady.Load() {
		e.resolveOpCodeSlow()
	}
}

// resolveOpCodeSlow runs exactly once per node.
func (e *BinOpInt64) resolveOpCodeSlow() {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	if e.opReady.Load() {
		return
	}
	e.opCode = resolveArithOp(e.Op)
	e.opReady.Store(true)
}

func (e *BinOpInt64) Eval(b *batch.RecordBatch, row int) any {
	v, ok := e.EvalInt64(b, row)
	if !ok {
		return nil
	}
	return v
}

func (e *BinOpInt64) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return 0, false
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return 0, false
	}
	e.resolveOpCode()
	// Checked: an integer result with no int64 is 22003, PostgreSQL's
	// `bigint out of range`, and never the wrapped number Go's operators
	// answer (#637 — int_overflow.go).
	switch e.opCode {
	case arithAdd:
		return addInt64Checked(lv, rv), true
	case arithSub:
		return subInt64Checked(lv, rv), true
	case arithMul:
		return mulInt64Checked(lv, rv), true
	case arithDiv:
		// Operands are non-NULL here, so a zero divisor is a genuine one.
		return divInt64Checked(lv, rv), true
	case arithMod:
		return modInt64Checked(lv, rv), true
	default:
		return 0, false
	}
}

// EvalFloat64 allows BinOpInt64 to be used as Float64Expr (int→float promotion).
func (e *BinOpInt64) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	v, ok := e.EvalInt64(b, row)
	return float64(v), ok
}

// UnaryOp is a unary arithmetic expression (negation).
type UnaryOp struct {
	Operand Expr
	Op      string // -, +
}

func (e *UnaryOp) Eval(b *batch.RecordBatch, row int) any {
	// A DECIMAL operand negates EXACTLY, on the carrier, and boxes as its
	// rendered text — the same box a DECIMAL column produces. Asked first,
	// because the numeric arms below reach a decimal only through
	// ToFloat64 of that text (ADR-0024, #555).
	if v, ok := e.unaryDecimalBox(b, row); ok {
		return v
	}
	v := e.Operand.Eval(b, row)
	if v == nil {
		return nil
	}
	// Negating an integer stays an integer (SQL and Go agree), which is what
	// keeps `-int_col / 2` on the integer-division path (#369).
	switch e.Op {
	case "-":
		if i, ok := toInt64Safe(v); ok {
			return -i
		}
		return -ToFloat64(v)
	case "+":
		if i, ok := toInt64Safe(v); ok {
			return i
		}
		return ToFloat64(v)
	default:
		return v
	}
}

// --- Comparisons (return bool but implement Expr for composability) ---

// CmpOp represents a comparison operator.
type CmpOp int

const (
	CmpEq CmpOp = iota
	CmpNe
	CmpLt
	CmpLe
	CmpGt
	CmpGe
)

// Cmp is a comparison expression.
type Cmp struct {
	Left, Right Expr
	Op          CmpOp
	// dec is the DECIMAL-column-against-numeric-literal binding, set by
	// NewCmp. Nil for every other operand shape — and for a Cmp assembled as
	// a struct literal, which is why the compiler builds them through NewCmp
	// (decimal_literal.go).
	dec *decimalLitCmp
	// decCols is the two-bare-columns binding, for the pair whose boxes carry
	// no way to tell a DECIMAL from a string (#477).
	decCols *decimalColCmp
	// pair is the GENERIC path's declaration-driven binding, for the operand
	// shapes dec and decCols do not cover: a COMPOSITE operand meeting a
	// DECIMAL — `GREATEST(d_2, d_4) = d_4`, the outer comparison of #506's own
	// repro, where both sides arrive as rendered text and compare()'s
	// two-strings fast path answered LEXICOGRAPHICALLY. It also carries the
	// operands' literal source texts, hoisted out of the row loop.
	//
	// It replaces the old notText cache and keeps its property: a pair whose
	// declarations can never select a rule disarms itself on the first batch
	// that settles both operands, so the generic path's per-row cost stays one
	// atomic load. Unlike notText it does not need dec or decCols to have
	// matched first, because it settles from DECLARATIONS rather than from
	// what one row's box happened to be.
	pair *boxedPair
}

// NewCmp builds a comparison, binding the two operand shapes that cannot be
// answered from the boxed values: a DECIMAL column against a numeric literal,
// and two DECIMAL columns against each other.
func NewCmp(left, right Expr, op CmpOp) *Cmp {
	return &Cmp{Left: left, Right: right, Op: op,
		dec: bindDecimalCmp(left, right), decCols: bindDecimalCols(left, right),
		pair: newBoxedPair(left, right)}
}

func (e *Cmp) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Cmp) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *Cmp) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if e.dec != nil {
		if vec := e.dec.vector(b); vec != nil {
			if vec.Nulls.IsNullFast(row) {
				return false, true // a comparison against NULL is UNKNOWN (#370)
			}
			return cmpOrder(e.dec.order(vec, row, 0), e.Op), false
		}
	}
	if e.decCols != nil {
		if lv, rv := e.decCols.vectors(b); lv != nil {
			if lv.Nulls.IsNullFast(row) || rv.Nulls.IsNullFast(row) {
				return false, true // a comparison against NULL is UNKNOWN (#370)
			}
			return cmpOrder(kernel.CompareDecimalAt(lv, row, rv, row), e.Op), false
		}
	}
	lv := e.Left.Eval(b, row)
	rv := e.Right.Eval(b, row)
	if lv == nil || rv == nil {
		return false, true // a comparison against NULL is UNKNOWN (#370)
	}
	// compareNull, not compare: a network comparison has a third answer for a
	// stored value that names no address, and it is the one a NULL row gets
	// (ADR-0012 item 10, #565). Every other pair answers null=false here, so
	// this is the same two values it always was.
	return e.pair.compareNull(b, lv, rv, e.Op)
}

// CmpTemporalLit compares a bare column against a string literal that
// parses as a date/timestamp, without per-row parsing, cache lookups, or
// boxing — the generic path spent 3.2% of SF100 worker CPU inside the
// date-parse memo's sync.Map.Load (interface-key hashing dominated;
// 2026-07-25 re-rank). The literal is parsed once into BOTH temporal
// units at compile time; the unit is chosen from the column's resolved
// type per batch. Every non-fast sub-case (non-temporal column, the
// epoch-zero literal guard) delegates to the generic compare() with the
// original operand order, keeping semantics bit-identical with Cmp.
type CmpTemporalLit struct {
	Col  *ColRef
	Lit  string // original literal text (generic-fallback operand)
	Op   CmpOp
	Flip bool  // literal was the LEFT operand: evaluate as (lit OP col)
	days int64 // literal as epoch days
	ms   int64 // literal as epoch milliseconds
}

func (e *CmpTemporalLit) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *CmpTemporalLit) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *CmpTemporalLit) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	e.Col.resolve(b)
	var lit int64
	switch e.Col.typ {
	case batch.TypeDate:
		lit = e.days
	case batch.TypeTimestamp:
		lit = e.ms
	default:
		// Non-temporal column: exact generic semantics (string columns
		// compare lexically, numeric columns take the numeric paths).
		return e.genericFallback(b, row)
	}
	v, ok := e.Col.EvalInt64(b, row)
	if !ok {
		return false, true // NULL / unresolved — a comparison against NULL is UNKNOWN
	}
	if lit == 0 && v != 0 {
		// The generic guard (`bi != 0 || ai == 0`) treats an epoch-zero
		// literal against a nonzero column as a parse failure and falls
		// through to stringified comparison. Preserve that bit-exactly.
		return e.genericFallback(b, row)
	}
	a, bv := v, lit
	if e.Flip {
		a, bv = lit, v
	}
	switch e.Op {
	case CmpEq:
		return a == bv, false
	case CmpNe:
		return a != bv, false
	case CmpLt:
		return a < bv, false
	case CmpLe:
		return a <= bv, false
	case CmpGt:
		return a > bv, false
	case CmpGe:
		return a >= bv, false
	}
	return false, false
}

func (e *CmpTemporalLit) genericFallback(b *batch.RecordBatch, row int) (bool, bool) {
	cv := e.Col.Eval(b, row)
	if cv == nil {
		return false, true
	}
	if e.Flip {
		return compare(e.Lit, cv, e.Op), false
	}
	return compare(cv, e.Lit, e.Op), false
}

// CmpNetworkLit compares a bare column against a string literal that parses
// as an IPv4 address, a MAC address, an IPv6 address, or a CIDR network,
// without per-row parsing or boxing — CmpTemporalLit's counterpart for
// network types, and for the same reason: ColRef.Eval boxes a TypeIPv4/
// TypeMAC column as its raw encoded int64 (the representation arithmetic and
// column-to-column ordering comparisons depend on — see networkTextFuncs) and
// a TypeIPv6/TypeCIDR column as rendered TEXT (Vector.GetValue's default
// case), so `ip_col = '10.0.0.1'` boxed the column as a decimal digit string
// and the literal as itself, and `ipv6_col < '2001:db8::10'` boxed the
// column's address as text and compared it LEXICALLY against the literal —
// neither is the address's own order (issues found via README verification
// and #492). Column type is unknown at compile time, so the literal is
// pre-parsed into every encoding here and the right one picked per batch
// from the column's resolved type; a non-network column (or a network
// column whose type doesn't match the literal's parse) delegates to the
// generic compare() with the original operand order, keeping semantics
// bit-identical with Cmp in every sub-case — matching CmpTemporalLit's own
// contract. Comparing the pre-parsed encodings (not as formatted strings) is
// also what keeps ordering (<, >) correct: IPv4's big-endian uint32, MAC's
// packed 48 bits, and IPv6's raw 16 bytes all sort the same as the address
// itself, and CIDR's structural key (kernel.CidrSortKey) sorts the same as
// PostgreSQL's inet order — where a dotted-quad, colon-hex, or CIDR-notation
// STRING would sort lexically and disagree with it (e.g. "9.0.0.1" >
// "10.0.0.1" as text).
type CmpNetworkLit struct {
	Col    *ColRef
	Lit    string // original literal text (generic-fallback operand)
	Op     CmpOp
	Flip   bool // literal was the LEFT operand: evaluate as (lit OP col)
	ipv4   int64
	ipv4ok bool
	mac    int64
	macok  bool
	ipv6   string // raw 16 bytes, the column's own TypeIPv6 storage form
	ipv6ok bool
	cidr   string // kernel.CidrSortKey's structural key
	cidrok bool
}

func (e *CmpNetworkLit) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *CmpNetworkLit) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *CmpNetworkLit) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	e.Col.resolve(b)
	// valueType, not typ: this node is BUILT at compile time, where
	// structField is not yet known (it is resolved on the first batch), so a
	// ROW field path always reaches here. Keying on the container's TypeRow
	// dropped every field path into genericFallback — `rw.cidr > '10.0.0.0/8'`
	// compared the stored TEXT lexically where the same value in a column is
	// compared by kernel.CidrSortKey's inet order, and a malformed literal
	// answered rows instead of 22P02 (#568).
	switch e.Col.valueType() {
	case batch.TypeIPv4:
		if !e.ipv4ok {
			return e.genericFallback(b, row)
		}
		return e.evalInt64(b, row, e.ipv4)
	case batch.TypeMAC:
		if !e.macok {
			return e.genericFallback(b, row)
		}
		return e.evalInt64(b, row, e.mac)
	case batch.TypeIPv6:
		if !e.ipv6ok {
			// The literal is an address of no family this column can hold
			// (a MAC), so there is no comparison to make. The kernel path
			// answers the same query with 22P02 (exec.networkConstError);
			// falling back to a lexical text compare here instead is the
			// two-path divergence #492 exists to close.
			raiseInvalidTextRepresentation("inet", e.Lit)
		}
		return e.evalRawBytes(b, row, e.ipv6)
	case batch.TypeCIDR:
		if !e.cidrok {
			raiseInvalidTextRepresentation("cidr", e.Lit)
		}
		return e.evalCIDR(b, row)
	default:
		// Non-network column: exact generic semantics.
		return e.genericFallback(b, row)
	}
}

// evalInt64 is the IPv4/MAC arm: the column boxes as its raw encoded int64,
// which sorts the same as the address (big-endian uint32 / packed 48 bits).
func (e *CmpNetworkLit) evalInt64(b *batch.RecordBatch, row int, lit int64) (bool, bool) {
	v, ok := e.Col.EvalInt64(b, row)
	if !ok {
		return false, true // NULL / unresolved — a comparison against NULL is UNKNOWN
	}
	a, bv := v, lit
	if e.Flip {
		a, bv = lit, v
	}
	return cmpInt64Op(a, bv, e.Op), false
}

// evalRawBytes is the IPv6 arm: the column stores the address's raw 16
// bytes (batch.Vector's TypeIPv6 storage), which a Go string comparison
// orders byte-for-byte — the address's own big-endian numeric order — unlike
// the RENDERED text ColRef.Eval would hand back instead.
func (e *CmpNetworkLit) evalRawBytes(b *batch.RecordBatch, row int, lit string) (bool, bool) {
	v, ok := e.colRawBytes(b, row)
	if !ok {
		return false, true
	}
	a, bv := v, lit
	if e.Flip {
		a, bv = lit, v
	}
	return cmpStringOp(a, bv, e.Op), false
}

// evalCIDR is the CIDR arm: the column stores plain TEXT (parquet/
// schema.go), so its own value is re-keyed through the identical
// kernel.CidrSortKey (PostgreSQL's inet order) the literal already went
// through at compile time —
// anything else risks the row and the literal disagreeing about what
// "structural order" means, which is the two-implementation defect #492's
// own doc comment warns CidrSortKey's export exists to prevent.
func (e *CmpNetworkLit) evalCIDR(b *batch.RecordBatch, row int) (bool, bool) {
	raw, ok := e.colRawBytes(b, row)
	if !ok {
		return false, true
	}
	key, ok := kernel.CidrSortKey(raw)
	if !ok {
		// The column does NOT enforce the CIDR shape — it is unvalidated
		// text (internal/storage/ingest) — so a stored value can fail to
		// re-key. A value with no place in the order has no defined
		// comparison against one that does, which is UNKNOWN, and that is
		// exactly what kernel.compareFilterCIDR answers for the same row.
		// Falling back to a LEXICAL text comparison here (what this arm did
		// when #492 introduced it) made one malformed row enough to split
		// the two paths apart again.
		return false, true
	}
	a, bv := key, e.cidr
	if e.Flip {
		a, bv = e.cidr, key
	}
	return cmpStringOp(a, bv, e.Op), false
}

// colRawBytes reads the column's raw BytesData at row — the address's own
// storage, never the rendered text ColRef.Eval would produce — and whether
// the row is non-NULL. e.Col must already be resolved.
func (e *CmpNetworkLit) colRawBytes(b *batch.RecordBatch, row int) (string, bool) {
	v, r, ok := e.Col.valueVector(b, row)
	if !ok {
		return "", false
	}
	if v.Nulls.IsNullFast(r) {
		return "", false
	}
	return v.BytesData.UnsafeStringValue(r), true
}

func (e *CmpNetworkLit) genericFallback(b *batch.RecordBatch, row int) (bool, bool) {
	cv := e.Col.Eval(b, row)
	if cv == nil {
		return false, true
	}
	if e.Flip {
		return compare(e.Lit, cv, e.Op), false
	}
	return compare(cv, e.Lit, e.Op), false
}

// cmpStringOp applies a comparison operator to two strings, byte-for-byte —
// CmpNetworkLit's IPv6/CIDR arms feed it keys already re-encoded so that
// byte order IS the intended order (raw address bytes, or CidrSortKey's
// structural key), not the general-purpose collation cmpOrder/compare() use
// for an ordinary STRING column.
func cmpStringOp(a, b string, op CmpOp) bool {
	switch op {
	case CmpEq:
		return a == b
	case CmpNe:
		return a != b
	case CmpLt:
		return a < b
	case CmpLe:
		return a <= b
	case CmpGt:
		return a > b
	case CmpGe:
		return a >= b
	}
	return false
}

// CmpInt64 is a typed comparison that operates on int64 without boxing.
type CmpInt64 struct {
	Left, Right Int64Expr
	Op          CmpOp
}

func (e *CmpInt64) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

// EvalBoolNull: a not-ok typed operand is a NULL (the operands here are
// provably int-typed at compile time), so the comparison is UNKNOWN.
func (e *CmpInt64) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return false, true
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return false, true
	}
	return cmpInt64Op(lv, rv, e.Op), false
}

func (e *CmpInt64) EvalBool(b *batch.RecordBatch, row int) bool {
	lv, lok := e.Left.EvalInt64(b, row)
	if !lok {
		return false
	}
	rv, rok := e.Right.EvalInt64(b, row)
	if !rok {
		return false
	}
	return cmpInt64Op(lv, rv, e.Op)
}

// IsDistinctFrom implements PostgreSQL's NULL-safe (in)equality, IS [NOT]
// DISTINCT FROM (#374). Unlike Cmp, it never answers UNKNOWN: NULL
// participates as a value here rather than propagating, so two NULLs are
// NOT DISTINCT (equal) and a NULL against a non-NULL value IS DISTINCT.
// "NULL IS DISTINCT FROM NULL" is FALSE, never NULL — the one case a
// COALESCE-based workaround gets wrong for a real sentinel value.
type IsDistinctFrom struct {
	Left, Right Expr
	Not         bool // true for IS NOT DISTINCT FROM

	// arms holds the two operand-shaped bindings this node needs: the
	// non-numeric-literal refusal (refuseArm) and the declaration-driven
	// comparison (boxedPair). Armed from the operand SHAPES on first
	// evaluation and reused for every row after.
	//
	// Armed lazily rather than at construction because this node is also
	// built by direct struct literal, and a refusal that only existed on the
	// compiler's path would be a refusal the compiler's tests alone see.
	arms atomic.Pointer[isDistinctArms]
}

// isDistinctArms is IsDistinctFrom's per-node binding, published as one
// immutable value so a row reads one atomic pointer rather than two.
type isDistinctArms struct {
	refuse *refuseArm
	pair   *boxedPair
}

func (e *IsDistinctFrom) armed() *isDistinctArms {
	if a := e.arms.Load(); a != nil {
		return a
	}
	a := &isDistinctArms{refuse: armRefusal(e.Left, e.Right), pair: newBoxedPair(e.Left, e.Right)}
	e.arms.Store(a)
	return a
}

func (e *IsDistinctFrom) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *IsDistinctFrom) EvalBool(b *batch.RecordBatch, row int) bool {
	v, _ := e.EvalBoolNull(b, row)
	return v
}

// EvalBoolNull always reports null=false: DISTINCT FROM is total over NULL
// inputs, which is the entire point of the operator.
func (e *IsDistinctFrom) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv := e.Left.Eval(b, row)
	rv := e.Right.Eval(b, row)
	a := e.armed()
	// The refusal runs BEFORE the NULL cases, for the reason evalNullIf runs
	// it first: it is a property of the operand pair's DECLARATIONS, not of
	// this row's values. PostgreSQL coerces the unknown-typed literal at parse
	// analysis, so `NULL IS DISTINCT FROM 'abc'` over a double column is 22P02
	// there — and leaving it in the default arm meant a column whose rows are
	// all NULL answered instead (#646).
	a.refuse.check(b)
	var distinct bool
	switch {
	case lv == nil && rv == nil:
		distinct = false
	case lv == nil || rv == nil:
		distinct = true
	default:
		// A DECIMAL column against a numeric literal is compared at the
		// literal's full precision, not through its float64 box: `d IS
		// DISTINCT FROM <a literal naming d exactly>` answered 200 rows where
		// PostgreSQL answers 199 (#465). A literal that is not a number is a
		// query ERROR against a DECIMAL column, never a value (#463) — this
		// boxed site carried the exact-text comparison but not the refusal
		// (#505): `d IS DISTINCT FROM 'abc'` answered every row instead.
		//
		// TWO DECIMAL COLUMNS are the pair no box can be dispatched on: both
		// arrive as rendered text, indistinguishable from two strings, so
		// `d_2 IS DISTINCT FROM d_4` compared them LEXICOGRAPHICALLY and
		// called two spellings of one number distinct (#506). The pair is
		// bound from the operands' declarations instead.
		distinct = !a.pair.compare(b, lv, rv, CmpEq)
	}
	if e.Not {
		return !distinct, false
	}
	return distinct, false
}

func cmpInt64Op(lv, rv int64, op CmpOp) bool {
	switch op {
	case CmpEq:
		return lv == rv
	case CmpNe:
		return lv != rv
	case CmpLt:
		return lv < rv
	case CmpLe:
		return lv <= rv
	case CmpGt:
		return lv > rv
	case CmpGe:
		return lv >= rv
	default:
		return false
	}
}

// CmpFloat64 is a typed comparison that operates on float64 without boxing.
type CmpFloat64 struct {
	Left, Right Float64Expr
	Op          CmpOp
}

func (e *CmpFloat64) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

// EvalBoolNull: a not-ok typed operand is a NULL (the operands here are
// provably float-typed at compile time), so the comparison is UNKNOWN.
func (e *CmpFloat64) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv, lok := e.Left.EvalFloat64(b, row)
	if !lok {
		return false, true
	}
	rv, rok := e.Right.EvalFloat64(b, row)
	if !rok {
		return false, true
	}
	return cmpFloat64Op(lv, rv, e.Op), false
}

func (e *CmpFloat64) EvalBool(b *batch.RecordBatch, row int) bool {
	lv, lok := e.Left.EvalFloat64(b, row)
	if !lok {
		return false
	}
	rv, rok := e.Right.EvalFloat64(b, row)
	if !rok {
		return false
	}
	return cmpFloat64Op(lv, rv, e.Op)
}

// cmpFloat64Op compares two float64 operands in PostgreSQL's float order —
// NaN greatest and equal to itself, -0.0 equal to +0.0 — which is the order
// the vectorized kernel, the group key, ORDER BY and the window peer groups
// all use (ADR-0012 item 8). Go's operators are IEEE754, so spelling them
// directly here made `CASE WHEN f = f` and `HAVING f > 1e300` answer
// differently from the same predicate in a WHERE clause.
func cmpFloat64Op(lv, rv float64, op CmpOp) bool {
	switch op {
	case CmpEq:
		return kernel.FloatEq(lv, rv)
	case CmpNe:
		return kernel.FloatNe(lv, rv)
	case CmpLt:
		return kernel.FloatLt(lv, rv)
	case CmpLe:
		return kernel.FloatLe(lv, rv)
	case CmpGt:
		return kernel.FloatGt(lv, rv)
	case CmpGe:
		return kernel.FloatGe(lv, rv)
	default:
		return false
	}
}

// IsNull checks if an expression is null.
type IsNull struct {
	Operand Expr
	Not     bool // IS NOT NULL
}

func (e *IsNull) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *IsNull) EvalBool(b *batch.RecordBatch, row int) bool {
	v := e.Operand.Eval(b, row)
	if e.Not {
		return v != nil
	}
	return v == nil
}

// EvalBoolNull: IS [NOT] NULL never answers UNKNOWN — it is the operator
// SQL provides to ASK about NULL.
func (e *IsNull) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	return e.EvalBool(b, row), false
}

// IsBool is `x IS [NOT] TRUE/FALSE`. Distinct from Cmp because it is a
// NULL-test like IS NULL, not a comparison: NULL IS TRUE answers FALSE and
// NULL IS NOT TRUE answers TRUE, where a comparison against NULL would be
// UNKNOWN (#370 — the Cmp spelling was right only while Cmp itself had no
// UNKNOWN).
type IsBool struct {
	Operand Expr
	Want    bool // TRUE or FALSE spelling
	Not     bool // IS NOT
}

func (e *IsBool) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *IsBool) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := evalBoolNull(e.Operand, b, row)
	match := !null && v == e.Want
	if e.Not {
		return !match
	}
	return match
}

func (e *IsBool) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	return e.EvalBool(b, row), false
}

// --- Logical operators ---
//
// The connectives implement Kleene's strong three-valued logic on the
// EvalBoolNull protocol. Their EvalBool forms keep the original two-valued
// short-circuits: collapsing UNKNOWN to false BEFORE the connective gives
// the same filter answer as collapsing after it for AND and OR (min/max
// logic), so the hot path pays nothing — NOT is the one operator where the
// two orders disagree, and it evaluates on the three-valued protocol.

// And is a logical AND.
type And struct {
	Left, Right Expr
}

func (e *And) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *And) EvalBool(b *batch.RecordBatch, row int) bool {
	return toBool(e.Left, b, row) && toBool(e.Right, b, row)
}

// EvalBoolNull: FALSE AND anything is FALSE; otherwise a NULL operand makes
// it UNKNOWN. Short-circuits on a FALSE left operand.
func (e *And) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv, lnull := evalBoolNull(e.Left, b, row)
	if !lnull && !lv {
		return false, false
	}
	rv, rnull := evalBoolNull(e.Right, b, row)
	if !rnull && !rv {
		return false, false
	}
	if lnull || rnull {
		return false, true
	}
	return true, false
}

// Or is a logical OR.
type Or struct {
	Left, Right Expr
}

func (e *Or) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Or) EvalBool(b *batch.RecordBatch, row int) bool {
	return toBool(e.Left, b, row) || toBool(e.Right, b, row)
}

// EvalBoolNull: TRUE OR anything is TRUE; otherwise a NULL operand makes it
// UNKNOWN. Short-circuits on a TRUE left operand.
func (e *Or) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	lv, lnull := evalBoolNull(e.Left, b, row)
	if !lnull && lv {
		return true, false
	}
	rv, rnull := evalBoolNull(e.Right, b, row)
	if !rnull && rv {
		return true, false
	}
	if lnull || rnull {
		return false, true
	}
	return false, false
}

// Not is a logical NOT.
type Not struct {
	Operand Expr
}

func (e *Not) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

// EvalBool: NOT must see the third value — collapsing first turned
// NOT (UNKNOWN) into true and admitted rows SQL excludes, which was the
// dangerous half of #370 (`1 NOT IN (2, NULL)` answering true).
func (e *Not) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull: NOT UNKNOWN stays UNKNOWN.
func (e *Not) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	v, null := evalBoolNull(e.Operand, b, row)
	if null {
		return false, true
	}
	return !v, false
}

// --- Special SQL expressions ---

// In checks if a value is in a set.
type In struct {
	Expr   Expr
	Values []Expr
	Not    bool
	// dec binds a DECIMAL column against an all-numeric-literal list; see
	// NewCmp and decimal_literal.go.
	dec *decimalLitCmp
	// f32 binds a FLOAT32 column against a multi-element all-numeric-literal
	// list, which PostgreSQL compares at REAL width and this path compared at
	// double (#633); see real_in_width.go.
	f32 *realLitSet
	// pairs is one declaration-driven binding per LIST MEMBER, for the lists
	// dec declines: a mixed list, a member that is not a literal, or a column
	// that is not a DECIMAL. `x IN (v)` is `x = v` chained with OR, so it has
	// to take the same comparison rule — `s = 2.00` answered one row while
	// `s IN (2.00)` answered none, one predicate with two readings (#504
	// review, non-blocker a).
	pairs []*boxedPair
	// disarm caches "none of pairs carries a declaration-driven rule",
	// collapsing the per-row cost to one atomic load once every member has
	// settled — see pairSetState. Mixed lists (some member armed) keep
	// dispatching through pairs[i].compare exactly as before.
	disarm pairSetState
}

// NewIn builds a set-membership test, binding the DECIMAL-column-against-
// numeric-literals shape, the FLOAT32-column-against-a-multi-element-list
// shape, and one boxed pair per member.
func NewIn(e Expr, values []Expr, not bool) *In {
	pairs := make([]*boxedPair, len(values))
	for i, v := range values {
		pairs[i] = newBoxedPair(e, v)
	}
	return &In{Expr: e, Values: values, Not: not,
		dec: bindDecimalList(e, values), f32: bindRealLitList(e, values), pairs: pairs}
}

func (e *In) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *In) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull: `x IN (a, b, NULL)` is the chained OR of comparisons, so a
// match answers TRUE, and a miss with a NULL anywhere in the list is
// UNKNOWN — never FALSE. NOT IN is its Kleene negation, which is why
// `1 NOT IN (2, NULL)` must not answer true: PostgreSQL's reading is "I
// don't know, so no" (#370).
func (e *In) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if e.dec != nil {
		if vec := e.dec.vector(b); vec != nil {
			if vec.Nulls.IsNullFast(row) {
				return false, true
			}
			// Every member is a numeric literal here, so the list holds no
			// NULL and the three-valued reading below cannot arise.
			for i := range e.Values {
				if e.dec.order(vec, row, i) == 0 {
					return !e.Not, false
				}
			}
			return e.Not, false
		}
	}
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false, true
	}
	// A REAL-typed operand against a multi-element literal list compares at
	// REAL width, not at the double width this boxed path would otherwise use
	// (#633). A FLOAT32 column boxes as float64(float32), so narrowing it back
	// recovers the stored value exactly; a CAST to REAL boxes as a float32
	// already. The list's own NULL rule is unchanged — a miss with a NULL
	// member is UNKNOWN, never FALSE.
	if e.f32.applies(b) {
		if f, ok := realBox(lv); ok {
			if e.f32.contains(f) {
				return !e.Not, false
			}
			if e.f32.sawNull {
				return false, true
			}
			return e.Not, false
		}
	}
	sawNull := false
	// Once every pair on this node is confirmed disarmed, skip the per-pair
	// dispatch entirely — one atomic load instead of one boxedPair.compare
	// call per member per row (see pairSetState).
	fast := e.disarm.disarmed(e.pairs)
	for i, v := range e.Values {
		rv := v.Eval(b, row)
		if rv == nil {
			sawNull = true
			continue
		}
		var eq bool
		switch {
		case fast:
			eq = compare(lv, rv, CmpEq)
		case i < len(e.pairs):
			eq = e.pairs[i].compare(b, lv, rv, CmpEq)
		default:
			eq = compare(lv, rv, CmpEq)
		}
		if eq {
			return !e.Not, false
		}
	}
	if sawNull {
		return false, true
	}
	return e.Not, false
}

// Between checks if a value is between two bounds.
type Between struct {
	Expr    Expr
	Low, Hi Expr
	Not     bool
	// dec binds a DECIMAL column against two numeric-literal bounds; see
	// NewCmp and decimal_literal.go.
	dec *decimalLitCmp
	// loPair/hiPair are the declaration-driven bindings for the two bounds,
	// for the shapes dec declines. `x BETWEEN a AND b` is `x >= a AND x <= b`
	// and must read its bounds the way those two comparisons do.
	loPair, hiPair *boxedPair
	// pairs is {loPair, hiPair}, built once so the disarm check below never
	// allocates a slice on the hot path.
	pairs []*boxedPair
	// disarm caches "neither bound carries a declaration-driven rule" — see
	// pairSetState and In.disarm.
	disarm pairSetState
}

// NewBetween builds a range test, binding the DECIMAL-column-against-
// numeric-literals shape and one boxed pair per bound.
func NewBetween(e, low, hi Expr, not bool) *Between {
	loPair := newBoxedPair(e, low)
	hiPair := newBoxedPair(e, hi)
	return &Between{Expr: e, Low: low, Hi: hi, Not: not,
		dec:    bindDecimalList(e, []Expr{low, hi}),
		loPair: loPair, hiPair: hiPair,
		pairs: []*boxedPair{loPair, hiPair}}
}

func (e *Between) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Between) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull: BETWEEN is defined as (x >= lo AND x <= hi), so a NULL
// bound does not force UNKNOWN — the other half can still answer FALSE
// (`5 BETWEEN NULL AND 2` is false, and NOT BETWEEN flips it to true).
func (e *Between) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if e.dec != nil {
		if vec := e.dec.vector(b); vec != nil {
			if vec.Nulls.IsNullFast(row) {
				return false, true
			}
			// Both bounds are numeric literals here, so neither half can be
			// UNKNOWN and BETWEEN is the plain conjunction.
			if e.dec.order(vec, row, 0) < 0 || e.dec.order(vec, row, 1) > 0 {
				return e.Not, false
			}
			return !e.Not, false
		}
	}
	v := e.Expr.Eval(b, row)
	if v == nil {
		return false, true
	}
	lo := e.Low.Eval(b, row)
	hi := e.Hi.Eval(b, row)
	// Three-valued AND over the two half-comparisons.
	geNull := lo == nil
	leNull := hi == nil
	// Once both bounds are confirmed disarmed, skip the per-bound dispatch —
	// one atomic load instead of two boxedPair.compare calls per row (see
	// pairSetState).
	fast := e.disarm.disarmed(e.pairs)
	if !geNull {
		var ge bool
		if fast {
			ge = compare(v, lo, CmpGe)
		} else {
			ge = e.loPair.compare(b, v, lo, CmpGe)
		}
		if !ge {
			return e.Not, false
		}
	}
	if !leNull {
		var le bool
		if fast {
			le = compare(v, hi, CmpLe)
		} else {
			le = e.hiPair.compare(b, v, hi, CmpLe)
		}
		if !le {
			return e.Not, false
		}
	}
	if geNull || leNull {
		return false, true
	}
	return !e.Not, false
}

// Like performs SQL LIKE pattern matching.
type Like struct {
	Expr    Expr
	Pattern Expr
	Not     bool
}

func (e *Like) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *Like) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

// EvalBoolNull: LIKE with NULL on either side is UNKNOWN, and NOT LIKE
// stays UNKNOWN with it (#370). A container-shaped operand is a query ERROR
// (#522) rather than a value, matched or not — see containerLikeKind.
func (e *Like) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	v := e.Expr.Eval(b, row)
	p := e.Pattern.Eval(b, row)
	if v == nil || p == nil {
		return false, true
	}
	if kind, ok := containerLikeKind(e.Expr, v); ok {
		raiseNoLikeOperator(kind)
	}
	result := matchLike(toString(boxedTextOperand(b, row, e.Expr, v)), toString(p))
	if e.Not {
		return !result, false
	}
	return result, false
}

// containerLikeKind reports whether v is a container-shaped LIKE operand and
// names its PostgreSQL-ish kind, for raiseNoLikeOperator (#522: PostgreSQL
// has no `~~` operator for any composite or array type, and wadjet has never
// committed to a text form for one either — kernel.ResolveLikeFilterKernel
// makes the matching refusal on the WHERE-clause kernel path).
//
// A bare column reference resolves exactly, from its declared type. Every
// other operand shape (a nested expression, a literal) falls back to the
// boxed value's Go shape: []any is ARRAY (or MAP, which boxes as a list of
// entry ROWs the same way — GetValue's TypeArray/TypeMap case — so the
// fallback cannot tell them apart and names the more common of the two),
// map[string]any is ROW, and []float32 is VECTOR.
func containerLikeKind(operand Expr, v any) (string, bool) {
	if cr, ok := operand.(*ColRef); ok && cr.structField == "" {
		switch cr.typ {
		case batch.TypeArray:
			return "array", true
		case batch.TypeMap:
			return "map", true
		case batch.TypeRow:
			return "record", true
		case batch.TypeVector:
			return "vector", true
		}
	}
	switch v.(type) {
	case []any:
		return "array", true
	case map[string]any:
		return "record", true
	case []float32:
		return "vector", true
	}
	return "", false
}

// Case is a CASE WHEN ... THEN ... ELSE ... END expression.
type Case struct {
	Operand Expr       // optional: CASE <operand> WHEN ...
	Whens   []CaseWhen // WHEN condition THEN result
	Else    Expr       // optional ELSE clause

	// arms holds one refusal and one declaration-driven comparison per WHEN
	// — see caseArms for why the arming is per-WHEN and not per-CASE. Armed
	// lazily, like IsDistinctFrom's, and for the same reason.
	arms atomic.Pointer[caseArms]

	// dch is the DECIMAL box mode: whether the result branches fold to a
	// DECIMAL, so a branch that answers an INTEGER hands over the value's
	// TEXT rather than a carrier (choice_decimal.go, #695).
	dch decimalChoice
}

// boxMode reports what this CASE's chosen box must be rewritten to so it
// survives the vector the plan declared. The arms slice is built only on the
// resolution path — Go evaluates a call's arguments when the call is reached,
// and the fast path returns first — so the row loop allocates nothing.
func (e *Case) boxMode(b *batch.RecordBatch) choiceBoxMode {
	if e.dch.ready.Load() {
		return e.dch.mode
	}
	return e.dch.resolveSlow(b, caseResultArms(e))
}

func (e *Case) armed() *caseArms {
	if r := e.arms.Load(); r != nil {
		return r
	}
	r := &caseArms{
		refuse: make([]*refuseArm, len(e.Whens)),
		pairs:  make([]*boxedPair, len(e.Whens)),
	}
	for i, w := range e.Whens {
		r.refuse[i] = armRefusal(e.Operand, w.Cond)
		r.pairs[i] = newBoxedPair(e.Operand, w.Cond)
	}
	e.arms.Store(r)
	return r
}

// CaseWhen is a single WHEN clause in a CASE expression.
type CaseWhen struct {
	Cond   Expr // the condition (or value to compare against operand)
	Result Expr
}

func (e *Case) Eval(b *batch.RecordBatch, row int) any {
	v := e.eval(b, row)
	if v == nil {
		return v
	}
	return choiceBox(e.boxMode(b), v)
}

func (e *Case) eval(b *batch.RecordBatch, row int) any {
	if e.Operand != nil {
		// Simple CASE: CASE x WHEN v1 THEN r1 ...
		opVal := e.Operand.Eval(b, row)
		arms := e.armed()
		for i, w := range e.Whens {
			whenVal := w.Cond.Eval(b, row)
			// The WHEN value's exact source text settles a match a float64
			// box cannot: `CASE d WHEN <a literal naming d exactly>` answered
			// the ELSE branch (#465). A WHEN value that is not a number is a
			// query ERROR against a DECIMAL operand, never a silent ELSE
			// (#463/#505): `CASE d WHEN 'abc'` answered 0 instead of refusing.
			//
			// And when BOTH sides are DECIMAL columns, neither box says so —
			// two rendered DECIMALs are two strings as far as compare() can
			// tell, so `CASE d_2 WHEN d_4` took the ELSE branch for the row
			// where the two hold the same number at different scales (#506).
			// The pair is bound from the operands' declarations instead.
			if opVal != nil && whenVal != nil {
				arms.refuse[i].check(b)
				if arms.pairs[i].compare(b, opVal, whenVal, CmpEq) {
					return w.Result.Eval(b, row)
				}
			}
		}
	} else {
		// Searched CASE: CASE WHEN cond1 THEN r1 ...
		for _, w := range e.Whens {
			if toBool(w.Cond, b, row) {
				return w.Result.Eval(b, row)
			}
		}
	}
	if e.Else != nil {
		return e.Else.Eval(b, row)
	}
	return nil
}

// Coalesce returns the first non-null argument.
type Coalesce struct {
	Args []Expr

	// dch is the DECIMAL box mode — see Case.dch (#695).
	dch decimalChoice
}

func (e *Coalesce) Eval(b *batch.RecordBatch, row int) any {
	for _, arg := range e.Args {
		v := arg.Eval(b, row)
		if v == nil {
			continue
		}
		return choiceBox(e.boxMode(b), v)
	}
	return nil
}

func (e *Coalesce) boxMode(b *batch.RecordBatch) choiceBoxMode {
	if e.dch.ready.Load() {
		return e.dch.mode
	}
	return e.dch.resolveSlow(b, e.Args)
}

// --- Scalar functions ---

// ArrayLitExpr evaluates to a []any containing the evaluated elements.
type ArrayLitExpr struct {
	Elements []Expr
}

func (e *ArrayLitExpr) Eval(b *batch.RecordBatch, row int) any {
	result := make([]any, len(e.Elements))
	for i, elem := range e.Elements {
		result[i] = elem.Eval(b, row)
	}
	return result
}

// FuncCall represents a scalar function call.
//
// Note: this struct holds NO per-call mutable state. A previous version cached
// an args buffer on the receiver to avoid per-call allocation, but that was
// unsafe under parallel pipeline execution: aggPreProject closures (and other
// wrapped-expression paths) capture the same *FuncCall by pointer rather than
// cloning it per worker, so concurrent goroutines stomped on the shared args
// buffer and produced non-deterministic Q02 row counts at SF0.01 (and worse
// at SF100). The vecFn / prepared lookup caches are still guarded by sync.Once
// (resolved once per BATCH, off EvalVec — not once per row, so the guard
// never sat in a row loop). fn is different: it is resolved off Eval, the
// per-row entry point every one of the 273 scalar functions reaches, so it
// uses the same double-checked atomic.Bool guard as BinOpFloat64/BinOpInt64's
// opCode and BinOpNumeric's mode — small enough that resolveFn inlines into
// Eval (verified with -gcflags='-m'), where sync.Once.Do's closure-plus-load
// did not.
type FuncCall struct {
	Name string
	Args []Expr

	// fnReady publishes fn and the argument-family flags below it: set last
	// under fnMu, read first (and alone) by Eval.
	fnReady atomic.Bool
	fnMu    sync.Mutex
	fn      ScalarFunc
	// Argument-family flags, resolved with fn under fnReady: this function
	// reads its arguments as text (stringInputFuncs) / as network addresses
	// (networkTextFuncs) / as instants (temporalInputFuncs) / as instants
	// that must remember whether they came from a DATE or a TIMESTAMP
	// column (dateArithFuncs, which render their result). Resolved once
	// rather than re-looked-up per row.
	wantsText        bool
	wantsNetworkText bool
	wantsInstant     bool
	wantsDateKind    bool
	// This function picks the extremum of its arguments (GREATEST / LEAST),
	// so it is evaluated with the argument EXPRESSIONS in hand: a numeric
	// literal's exact source text settles an ordering its float64 box cannot
	// (#465). extremumOp is CmpGt for GREATEST and CmpLt for LEAST.
	extremum   bool
	extremumOp CmpOp
	// extremumArms is the per-argument binding for those arguments, armed
	// once alongside extremumOp under fnMu: the non-numeric-literal refusal
	// and the declaration-driven comparison, both keyed by argument index
	// because the (best-so-far, candidate) pair moves between iterations.
	extremumArms *extremumArms
	// nullifArms is the same binding for NULLIF, whose EQUALITY test is the
	// same question GREATEST/LEAST's ordering is: `NULLIF(d_2, d_4)` over two
	// DECIMAL columns at different scales boxes them as their rendered TEXT,
	// and compare() then reads "12.75" and "12.7500" as different strings —
	// so NULLIF answered 12.75 where PostgreSQL answers NULL. The other boxed
	// comparison sites over that pair were bound in #506; this one was
	// missed, and was invisible until a projected NULLIF over a DECIMAL could
	// run at all (ADR-0024 item 2).
	nullifArms *extremumArms
	// choiceArms is the argument list this call CHOOSES its value from, read
	// off the registry's polymorphic declaration (Ret.SameAsArgs) so it
	// cannot drift from the type fold: GREATEST/LEAST/COALESCE/IFNULL mirror
	// every argument, NULLIF argument 0 alone, IF its two branches. nil for
	// every function that declares its own type, which is what keeps the
	// DECIMAL box check off the other 270-odd per-row paths. See dch.
	choiceArms []Expr
	// dch is the DECIMAL box mode for those arms — see Case.dch (#695).
	dch decimalChoice

	vecOnce sync.Once
	vecFn   VecScalarFunc
	// The declared return type, resolved with vecFn: it names the typed
	// slice the kernel writes, which EvalVec checks the output vector can
	// actually hold before handing it over.
	vecRet   batch.TypeID
	vecRetOK bool
	// This function reads its arguments as text (stringInputFuncs), so a
	// non-byte-array column has no bytes for the kernel to read.
	vecTextFn bool
	// The argument positions this function does NOT read as text
	// (typedArgPositions); nil when every argument is text.
	vecTypedArgs map[int]bool

	// Compile-once state for literal-argument regexp_replace (see
	// regexp_prepared.go). Built lazily under prepOnce; nil when the call
	// shape doesn't qualify.
	prepOnce sync.Once
	prepared *preparedRegexp
}

// stringInputFuncs are scalar functions whose arguments are string-typed.
// A TypeDate ColRef argument evaluates to its raw epoch-day int64 (the
// representation every comparison/arithmetic path depends on), so these
// functions must render it through batch.FormatDate first — otherwise
// SUBSTR(date_col, 1, 4) substrings the DIGITS of the day number
// (issue #273: SF100's date32 columns grouped Q07/Q08/Q09 day-granular;
// string-date test data never exercised the path). A TypeIPv4/TypeMAC
// ColRef argument has the identical shape — a raw encoded int64, not the
// dotted-quad/colon-hex address text CAST and every other function-argument
// site render — so FuncCall.Eval also runs formatNetworkArgs for every entry
// here (#500: `length(ipv4_col)` answered the DIGIT COUNT of the address's
// raw number). Keyed lowercase, the registry's convention. Timestamps are
// excluded deliberately: they have no canonical string form today (GetValue
// emits raw epoch-ms), and these functions must stay consistent with
// result-output rendering.
var stringInputFuncs = map[string]bool{
	// The `||` operator is here for the same reason `concat` is: it reads
	// every argument as text, so an IPv4/MAC/DATE column on either side has
	// to be rendered rather than read as its raw encoded integer. Splitting
	// it out of `concat` for #609 would otherwise have silently taken that
	// rendering away from `ipv4_col || '/24'` (#500's defect, restored).
	ConcatOpFunc: true,

	"upper": true, "lower": true, "concat": true, "length": true,
	"len": true, "substr": true, "substring": true, "trim": true,
	"ltrim": true, "rtrim": true, "replace": true, "reverse": true,
	"left": true, "right": true, "starts_with": true, "ends_with": true,
	"contains": true, "split_part": true, "strpos": true, "lpad": true,
	"rpad": true, "cast_string": true,
}

// typedArgPositions names, for a stringInputFuncs entry, the argument
// positions that are NOT read as text — a count, an index, a width. Every
// other position of every other entry IS read as text, including all of
// concat's, which is variadic.
//
// The distinction is load-bearing at the vec dispatch in FuncCall.EvalVec: a
// text position must hold a byte-array-shaped vector before the kernel may
// index its offsets, and a typed position must NOT be checked that way or
// SUBSTR(s, 1, 4) would lose the vec path to its integer arguments. Getting
// that split wrong is not a wrong answer, it is a dead server (#509): the
// kernel indexes an offsets array a BIGINT column does not have.
//
// Absent from this table means "every argument is text". That default is the
// safe direction — the worst it costs a mis-classified function is the
// per-row path.
var typedArgPositions = map[string]map[int]bool{
	"substr":     {1: true, 2: true}, // (text, start, length)
	"substring":  {1: true, 2: true},
	"left":       {1: true}, // (text, count)
	"right":      {1: true},
	"split_part": {2: true}, // (text, delimiter, field)
	"lpad":       {1: true}, // (text, width, fill)
	"rpad":       {1: true},
}

// formatTemporalArgs rewrites boxed TypeDate ColRef argument values to
// their canonical ISO form for string-input functions. Only direct column
// references are covered — a nested expression's output type isn't known
// here (and nothing in the TPC-H or observed customer shapes feeds a
// computed date into a string function).
func (e *FuncCall) formatTemporalArgs(args []any) {
	for i, a := range e.Args {
		// A CAST to a temporal type boxes its result exactly as the matching
		// column does (#340), so it needs the same rendering before a string
		// function reads it — otherwise SUBSTR(CAST(d AS DATE), 1, 4)
		// substrings the epoch-day digits, which is the #273 defect reached
		// through the cast instead of through the column.
		if c, ok := a.(*Cast); ok {
			v, isInt := args[i].(int64)
			if !isInt {
				continue
			}
			switch castTemporalKind(c.DestType) {
			case castToDateKind:
				args[i] = batch.FormatDate(int32(v))
			case castToTimestampKind:
				args[i] = batch.FormatTimestamp(v)
			}
			continue
		}
		// valueType: a ROW FIELD PATH of type DATE boxes as the same epoch
		// day a DATE column does, so it needs the same rendering, and typ
		// names the CONTAINER (#568).
		cr, ok := a.(*ColRef)
		if !ok || cr.valueType() != batch.TypeDate {
			continue
		}
		if v, ok := args[i].(int64); ok {
			args[i] = batch.FormatDate(int32(v))
		}
	}
}

// networkTextFuncs are the scalar functions whose bodies parse an argument
// as network-address TEXT — every function that reaches net.ParseIP,
// net.ParseMAC, net.ParseCIDR, or (mac_format) hand-rolled hex-with-
// separators parsing on a stringified argument. ColRef.Eval boxes a
// TypeIPv4/TypeMAC column as its raw encoded int64 — the representation
// arithmetic, GROUP BY keys and column-to-column ordering comparisons all
// depend on, exactly as TypeDate's epoch-day int64 does for
// stringInputFuncs above — so a function in this set that reads a typed
// network column argument saw a decimal digit string instead of a
// dotted-quad or colon-hex address and silently answered NULL (cidr_contains,
// ip_to_string, mac_vendor_oui, ...). Keyed lowercase, the registry's
// convention. TypeIPv6/TypeCIDR/TypeUUID columns are not affected: ColRef.Eval
// already renders those through Vector.GetValue's default case — for a
// FUNCTION ARGUMENT, which is all this registry is about. That is not a
// claim about comparison ORDERING against those three: see CmpNetworkLit's
// doc and tryNetworkLit (compile.go) for why `<`/`>` against a literal is a
// separate, still-open question (#492) that this rendering does not answer.
var networkTextFuncs = map[string]bool{
	"broadcast_address": true, "cidr_contains": true, "cidr_overlap": true,
	"cidr_to_range": true, "flow_direction": true,
	"geoip_asn": true, "geoip_city": true, "geoip_continent": true,
	"geoip_country": true, "geoip_country_name": true, "geoip_latitude": true,
	"geoip_longitude": true, "geoip_org": true, "geoip_postal_code": true,
	"geoip_subdivision": true, "geoip_timezone": true,
	"hosts_in_cidr": true, "ip_add": true, "ip_between": true, "ip_diff": true,
	"ip_in_range": true, "ip_netmask": true, "ip_subnet": true,
	"ip_subtract": true, "ip_to_hex": true, "ip_to_int": true,
	"ip_to_string": true, "ip_version": true,
	"ipv6_compress": true, "ipv6_expand": true, "ipv6_scope": true,
	"ipv6_to_eui64": true, "is_6to4": true, "is_ipv4": true, "is_ipv6": true,
	"is_link_local_ip": true, "is_loopback_ip": true, "is_multicast_ip": true,
	"is_private_ip": true, "is_reserved_ip": true, "is_teredo": true,
	"mac_is_local": true, "mac_is_unicast": true, "mac_to_string": true,
	"mac_vendor_oui": true, "mac_format": true, "mask_ip": true,
	"network_address": true, "prefix_length": true, "reverse_dns": true,
	"same_subnet": true, "sixto4_gateway": true, "teredo_client": true,
	"teredo_server": true,
}

// formatNetworkArgs rewrites boxed TypeIPv4/TypeMAC ColRef argument values
// to their canonical text form (dotted-quad / colon-hex) for
// networkTextFuncs AND stringInputFuncs. Reads through the column directly —
// like resolveTemporalArgs's columnInstant, and unlike formatTemporalArgs
// above — because formatIPv4/formatMAC are batch-package-internal;
// Vector.GetValue is the exported boundary that already renders them
// correctly (it is what a bare `SELECT ip_col` reads through, via
// exec.ColumnRef). Only direct column references are covered, matching
// formatTemporalArgs: a nested expression's output type isn't known here.
//
// stringInputFuncs (length/concat/upper/starts_with/...) went unfixed by
// #484, which only taught networkTextFuncs (ip_to_string, cidr_contains, ...)
// this rewrite: a SEPARATE registry, so `length(ipv4_col)` kept reading the
// raw encoded int64 and answering the DIGIT COUNT of the address's number
// instead of its text (#500). TypeIPv6/TypeCIDR/TypeUUID need no entry here:
// ColRef.Eval already falls through to Vector.GetValue's default case for
// those three (see the type switch there), which is the correct rendering
// for a function argument same as it is for CAST — only TypeIPv4/TypeMAC
// take the raw-int64 fast path that needs unwinding. TypePort/TypeProtocol
// need none either: their canonical text IS their raw number, so the box
// ColRef.Eval already returns is already the right string.
func (e *FuncCall) formatNetworkArgs(b *batch.RecordBatch, row int, args []any) {
	for i, a := range e.Args {
		cr, ok := a.(*ColRef)
		if !ok {
			continue
		}
		// valueType, not typ: a ROW FIELD PATH boxes exactly as a column of
		// the FIELD's type does, so it needs the same unwinding, and typ
		// names the CONTAINER (#568).
		if t := cr.valueType(); t != batch.TypeIPv4 && t != batch.TypeMAC {
			continue
		}
		if dv, ok := cr.displayValue(b, row); ok {
			args[i] = dv
		}
	}
}

// temporalInputFuncs are the scalar functions that read an argument as an
// INSTANT — every function whose body reaches parseTime/toTime/parseDateArg.
// They are the counterpart of stringInputFuncs and exist for the same reason:
// ColRef.Eval
// boxes a temporal column as a bare number (epoch DAYS for TypeDate, epoch
// MILLISECONDS for TypeTimestamp), and a bare number has lost its unit.
// parseTime reads an int64 as SECONDS, so 9568 days became 9568 seconds and
// YEAR(l_shipdate) answered 1970 for every row of a decade — no error, no
// null, one bogus GROUP BY bucket (issue #319).
//
// The unit cannot be recovered downstream: distinguishing days from seconds by
// magnitude is a guess, and a wrong guess is worse than the current bug. It
// CAN be recovered here, where the argument is still a column reference whose
// vector knows its own type — so this family, and only this family, resolves
// its column arguments through columnInstant before the function runs.
//
// Keyed lowercase, the registry's convention. Deliberately excluded:
// from_unixtime / timezone_hour / timezone_minute, whose argument is a number
// in its own right, not a column instant.
var temporalInputFuncs = map[string]bool{
	"year": true, "month": true, "day": true,
	"hour": true, "minute": true, "second": true,
	"quarter": true, "week": true,
	"day_of_week": true, "day_of_year": true,
	"last_day_of_month": true,
	"date_trunc":        true, "extract": true,
	"epoch": true, "to_unixtime": true, "date_format": true,
	"at_timezone": true, "timezone": true,
	// The date-arithmetic family, held back from the #319 fix and settled
	// by issue #322. It reads its date through parseDateValue, which takes
	// a bare int64 as epoch DAYS: right for a DATE column, and off by a
	// factor of ~86.4 million for a TIMESTAMP column, which boxes epoch
	// MILLISECONDS. Resolving the column here retires the guess for good.
	"date_add": true, "date_sub": true, "date_diff": true, "to_date": true,
}

// dateArithFuncs is the subset of temporalInputFuncs whose RESULT format
// depends on WHICH temporal type the argument came from: date_add over a DATE
// renders a calendar date, over a TIMESTAMP it renders an instant with the
// input's time-of-day intact. The date-part family needs no such distinction
// — the year of a day and the year of an instant are the same number — so
// only these calls tag a resolved DATE column as a civilDate.
var dateArithFuncs = map[string]bool{
	"date_add": true, "date_sub": true, "date_diff": true, "to_date": true,
}

// civilDate is a DATE column's value resolved to the instant it denotes (UTC
// midnight of that day), carrying the one fact the instant alone cannot: its
// column has no time-of-day to preserve. Only resolveTemporalArgs mints one,
// and only parseDateArg / parseDateValue read it; every other consumer sees a
// plain time.Time, so this stays inside the date-arithmetic family.
type civilDate struct{ t time.Time }

// resolveTemporalArgs rewrites a boxed DATE/TIMESTAMP column argument to the
// instant it denotes, so the function body's parseTime sees an unambiguous
// time.Time instead of a unit-less number. Resolution goes through
// columnInstant — the same resolver the vectorized kernels use — which is what
// makes the two paths agree by construction rather than by coincidence.
//
// Only direct column references are covered, matching formatTemporalArgs: a
// nested expression's output type isn't known here, and a literal or a
// computed value already carries its own unambiguous form (text, or a number
// that means seconds). Columns of any other type are left alone, so
// year(int_col) keeps reading its int64 as epoch seconds exactly as before.
//
// For the date-arithmetic family a resolved DATE column is tagged civilDate,
// because those functions render their result and a DATE must render as a
// calendar date (issue #322).
func (e *FuncCall) resolveTemporalArgs(b *batch.RecordBatch, row int, args []any) {
	for i, a := range e.Args {
		if args[i] == nil {
			continue
		}
		// A CAST to a temporal type is a column value in all but name once
		// #340 made it box epoch days / epoch milliseconds, and it loses the
		// unit at exactly the same point. Resolve it here for the same reason
		// and by the same rule, or YEAR(CAST(d AS DATE)) reads 9505 days as
		// 9505 seconds and answers 1970 — the #319 defect, reached through
		// the cast instead of through the column.
		if c, ok := a.(*Cast); ok {
			v, isInt := args[i].(int64)
			if !isInt {
				continue
			}
			switch castTemporalKind(c.DestType) {
			case castToDateKind:
				t := time.Unix(v*86400, 0).UTC()
				if e.wantsDateKind {
					args[i] = civilDate{t: t}
				} else {
					args[i] = t
				}
			case castToTimestampKind:
				args[i] = time.UnixMilli(v).UTC()
			}
			continue
		}
		cr, ok := a.(*ColRef)
		if !ok {
			continue
		}
		// A ROW FIELD PATH loses its unit at the same point a column does,
		// and recovers it the same way — from the vector that holds the
		// value. valueType/valueVector are that pair for both (#568).
		vt := cr.valueType()
		if vt != batch.TypeDate && vt != batch.TypeTimestamp {
			continue
		}
		src, r, ok := cr.valueVector(b, row)
		if !ok {
			continue
		}
		if t, ok := columnInstant(src, r); ok {
			if e.wantsDateKind && vt == batch.TypeDate {
				args[i] = civilDate{t: t}
				continue
			}
			args[i] = t
		}
	}
}

// temporalOperand resolves the date side of `date ± interval` to a value
// intervalShift can read, and reports whether the operand is a date at all.
//
// It is resolveTemporalArgs for the binary-operator path, and it exists for
// the same reason: ColRef.Eval boxes a DATE column as its epoch-DAY number and
// a TIMESTAMP column as its epoch-MILLISECOND number, and a bare number has
// lost the unit that says which. Recovering it here — where the operand is
// still a column reference whose vector knows its declared type — is what
// #322 did for date_add/date_sub arguments; the operator never got it, so
// `o_orderdate - INTERVAL '90' DAY` fell through to the numeric path and
// projected the raw day number (issue #332). A resolved DATE is tagged
// civilDate for the same reason it is there: the result renders, and a whole
// day must render as a calendar date.
//
// A CAST to a temporal type is resolved the same way and for the same reason:
// it now BOXES its result the way the matching column type does (epoch days /
// epoch milliseconds, see castTemporal), so the unit lives in the destination
// type rather than in the number. Without this case `DATE '1998-12-01' -
// INTERVAL '90' DAY` — TPC-H Q1's filter, and every typed date literal, which
// the parser lowers to a CAST — would have fallen straight through to numeric
// arithmetic once #340 stopped the cast passing its text along.
//
// Text passes through as text, keeping the string path's own rendering.
// Everything else — a bare integer, a computed expression, a column of any
// other type — declines, and the caller's numeric arithmetic runs unchanged.
func temporalOperand(b *batch.RecordBatch, row int, e Expr, v any) (any, bool) {
	if s, ok := v.(string); ok {
		return s, true
	}
	if c, ok := e.(*Cast); ok {
		n, isInt := v.(int64)
		if !isInt {
			return nil, false
		}
		switch castTemporalKind(c.DestType) {
		case castToDateKind:
			return civilDate{t: time.Unix(n*86400, 0).UTC()}, true
		case castToTimestampKind:
			return time.UnixMilli(n).UTC(), true
		}
		return nil, false
	}
	cr, ok := e.(*ColRef)
	if !ok {
		return nil, false
	}
	// Same rule as resolveTemporalArgs, for the operator instead of the
	// function: a ROW FIELD PATH's unit lives in the FIELD's declaration and
	// its value in the child vector (#568).
	vt := cr.valueType()
	if vt != batch.TypeDate && vt != batch.TypeTimestamp {
		return nil, false
	}
	src, r, ok := cr.valueVector(b, row)
	if !ok {
		return nil, false
	}
	t, ok := columnInstant(src, r)
	if !ok {
		return nil, false
	}
	if vt == batch.TypeDate {
		return civilDate{t: t}, true
	}
	return t, true
}

func (e *FuncCall) resolveFn() {
	if !e.fnReady.Load() {
		e.resolveFnSlow()
	}
}

// resolveFnSlow runs exactly once per node: the registry lookup plus the
// three argument-family flags derived from it.
func (e *FuncCall) resolveFnSlow() {
	e.fnMu.Lock()
	defer e.fnMu.Unlock()
	if e.fnReady.Load() {
		return
	}
	lower := strings.ToLower(e.Name)
	e.fn = DefaultRegistry.Lookup(e.Name)
	e.wantsText = stringInputFuncs[lower]
	e.wantsNetworkText = networkTextFuncs[lower]
	e.wantsInstant = temporalInputFuncs[lower]
	e.wantsDateKind = dateArithFuncs[lower]
	switch lower {
	case "greatest":
		e.extremum, e.extremumOp = true, CmpGt
		e.extremumArms = armExtremumArms(e.Args)
	case "least":
		e.extremum, e.extremumOp = true, CmpLt
		e.extremumArms = armExtremumArms(e.Args)
	case "nullif":
		if len(e.Args) >= 2 {
			e.nullifArms = armExtremumArms(e.Args)
		}
	}
	if idx, poly := DefaultRegistry.ReturnType(e.Name).SameAsArgs(len(e.Args)); poly {
		arms := make([]Expr, 0, len(idx))
		for _, i := range idx {
			if i >= 0 && i < len(e.Args) {
				arms = append(arms, e.Args[i])
			}
		}
		e.choiceArms = arms
	}
	e.fnReady.Store(true)
}

func (e *FuncCall) Eval(b *batch.RecordBatch, row int) any {
	e.resolveFn()
	if e.fn == nil {
		return nil
	}
	// Allocate args on every call so concurrent goroutines sharing this
	// *FuncCall don't stomp on each other. For small N (the common case)
	// the Go compiler routinely stack-allocates this slice, so the cost
	// vs the old per-receiver cache is negligible.
	args := make([]any, len(e.Args))
	for i, a := range e.Args {
		args[i] = a.Eval(b, row)
	}
	if e.wantsText {
		e.formatTemporalArgs(args)
		// stringInputFuncs (length/concat/upper/starts_with/... — #500) has
		// the identical gap networkTextFuncs already closed for a different
		// function family: a TypeIPv4/TypeMAC ColRef argument boxes as its
		// raw encoded int64, and formatNetworkArgs is the one rewrite that
		// turns it back into address text, so it runs for both families
		// rather than being duplicated.
		e.formatNetworkArgs(b, row, args)
	}
	if e.wantsNetworkText {
		e.formatNetworkArgs(b, row, args)
	}
	if e.wantsInstant {
		e.resolveTemporalArgs(b, row, args)
	}
	var out any
	switch {
	case e.extremum:
		out = pickExtremum(b, args, e.extremumOp, e.extremumArms)
	case e.nullifArms != nil:
		out = evalNullIf(b, args, e.nullifArms)
	default:
		out = e.fn(args)
	}
	if e.choiceArms == nil || out == nil {
		// Not a construct that CHOOSES between its arguments. Every other
		// function declares its own result type, so nothing here can be a
		// DECIMAL answered through an integer box (#695).
		return out
	}
	return choiceBox(e.boxMode(b), out)
}

// boxMode reports what this call's chosen box must be rewritten to — see
// Case.boxMode (#695, #555).
func (e *FuncCall) boxMode(b *batch.RecordBatch) choiceBoxMode {
	if e.dch.ready.Load() {
		return e.dch.mode
	}
	return e.dch.resolveSlow(b, e.choiceArms)
}

// evalNullIf is fnNullIf with the argument EXPRESSIONS in hand, so the
// equality test uses the rule the two DECLARATIONS select instead of
// compare()'s reading of two boxes.
//
// A DECIMAL boxes as its rendered text, so compare() called "12.75" and
// "12.7500" — the same number at two scales — different, and NULLIF returned
// its first argument where PostgreSQL returns NULL. extremumArms.order is the
// binding #506 built for exactly this pair; NULLIF is the site it did not
// reach, because the equality is inside a registry function rather than in a
// comparison node. A pair the binding does not apply to (two strings, a text
// column against a quoted literal) falls through to compare() unchanged.
// evalNullIf answers NULLIF's value AT THE CALL'S TYPE.
//
// The materialization is the same one GREATEST/LEAST have had since #646 and
// NULLIF never did: argument 0 can be a QUOTED literal, which arrives as a Go
// string, and PostgreSQL resolves NULLIF's type from the operator its two
// arguments select — `NULLIF('0xC.C', real_col)` is 12.75 there, and the four
// characters had nowhere to go here (#724). It is a no-op for every argument
// that is not a quoted literal, which is every shape the pre-#724 corpus held.
func evalNullIf(b *batch.RecordBatch, args []any, arms *extremumArms) any {
	v := evalNullIfChosen(b, args, arms)
	if v == nil {
		return nil
	}
	return arms.materialize(b, 0, v)
}

func evalNullIfChosen(b *batch.RecordBatch, args []any, arms *extremumArms) any {
	if len(args) < 2 {
		return nil
	}
	// The same refusal every sibling site makes: a literal that names no
	// number beside a DECIMAL column is 22P02 on PostgreSQL and at wadjet's
	// `=`, GREATEST, LEAST and simple CASE, and NULLIF was the one site that
	// answered instead — `NULLIF(d, 'abc')` returned every row. It runs
	// BEFORE the NULL short-circuit for the reason the other sites run it
	// first: the refusal is a property of the operand PAIR's DECLARATIONS,
	// not of the row's values (ADR-0012 item 6's neighbourhood).
	arms.checkRefusal(b)
	if args[0] == nil || args[1] == nil {
		return args[0]
	}
	if c, ok, unknown := arms.order(b, 0, 1, args[0], args[1]); ok {
		// unknown is "these two have no comparable relation" (a stored value
		// naming no address, ADR-0012 item 10). They are not equal, so the
		// first argument stands — the same answer NULL-vs-value gives.
		if !unknown && c == 0 {
			return nil
		}
		return args[0]
	}
	// The pair rule did not apply, which happens when one operand has no
	// declaration this layer can read — a scalar subquery, a container
	// element. compare() would then order two DECIMAL renderings by BYTES,
	// and "12.75" is not "12.7500" bytewise though they are one number:
	// `NULLIF(d, (SELECT d4 …))` answered the row where PostgreSQL answers
	// NULL. A DECIMAL against an UNCLASSIFIABLE operand is therefore
	// compared as the numbers the two boxes name.
	//
	// Against a TEXT operand it is not: a STRING column compares AS TEXT
	// whatever its digits look like (#504), which is what every sibling site
	// does for the same pair, and reading it numerically here made NULLIF
	// disagree with `=`, GREATEST, CASE and IS DISTINCT FROM on one commit.
	if arms.decimalVsUnclassifiable(b) {
		if ls, ok := args[0].(string); ok {
			if rs, ok := args[1].(string); ok {
				if c, ok := batch.CompareDecimalTexts(ls, rs); ok {
					if c == 0 {
						return nil
					}
					return args[0]
				}
			}
		}
	}
	if compare(args[0], args[1], CmpEq) {
		return nil
	}
	return args[0]
}

// pickExtremum is fnGreatest/fnLeast with the argument EXPRESSIONS in hand.
//
// fnGreatest orders through compare(), which reads a DECIMAL column's box as
// text and a numeric literal's as a float64 and has no exact reading of that
// pair — so `GREATEST(d, lit)` picked the wrong operand whenever the literal
// named a value a double cannot hold (#465). The ordering rule is otherwise
// unchanged, NULL skipping included: PostgreSQL's GREATEST/LEAST ignore NULL
// arguments and answer NULL only when every argument is NULL.
//
// A candidate that is not a number is a query ERROR against a DECIMAL
// argument, never a value PostgreSQL would just skip past (#463/#505):
// `GREATEST(d, 'abc')` answered 'abc' as the extremum instead of refusing.
// The refusal checks the pair actually being compared THIS iteration — best
// so far against the new candidate — the same as the comparison itself.
//
// Two DECIMAL arguments are the pair the boxes cannot distinguish from two
// strings, so `GREATEST(d_2, d_4)` picked the LEXICOGRAPHICALLY greater
// rendering (#506). arms.order answers that pair — and every mixed
// DECIMAL/number pair — from the arguments' declarations; anything it declines
// falls through to the literal-side carry-through, exactly as before.
func pickExtremum(b *batch.RecordBatch, args []any, op CmpOp, arms *extremumArms) any {
	// The refusal runs BEFORE the loop, because it is a property of the call's
	// arguments and not of the pairs the values happen to select. Firing it
	// inside the loop is what made `GREATEST(k, 'abc', d)` raise and
	// `LEAST(k, 'abc', d)` answer on the same three arguments (#517), and it
	// also skipped a call with fewer than two non-NULL arguments entirely,
	// where PostgreSQL still raises.
	arms.checkRefusal(b)
	var best any
	bestIdx := -1
	for i, a := range args {
		if a == nil {
			continue
		}
		if best == nil {
			best, bestIdx = a, i
			continue
		}
		better := false
		if arms != nil {
			c, ok, unknown := arms.order(b, i, bestIdx, a, best)
			switch {
			case unknown:
				// A candidate with no place in the order — a stored value
				// that names no address (ADR-0012 item 10) — is SKIPPED, the
				// way PostgreSQL's GREATEST/LEAST skip a NULL argument, and
				// is never text-ordered against the best so far. Residual:
				// such a value arriving FIRST becomes the running best and
				// nothing displaces it, so the answer is the malformed value
				// (#565).
				better = false
			case ok:
				better = cmpOrder(c, op)
			default:
				better = compare(a, best, op)
			}
		} else {
			better = compare(a, best, op)
		}
		if better {
			best, bestIdx = a, i
		}
	}
	// The winner comes back AT THE CALL'S TYPE. A quoted literal arrives as a
	// Go string, and returning that string projected four characters into a
	// FLOAT64 vector; PostgreSQL answers the number.
	return arms.materialize(b, bestIdx, best)
}

// EvalVec evaluates the function for an entire batch, writing results to out.
// Falls back to per-row Eval if no vectorized implementation exists or if
// argument types can't be resolved to column vectors.
func (e *FuncCall) EvalVec(b *batch.RecordBatch, out *batch.Vector, n int) {
	e.vecOnce.Do(func() {
		e.vecFn = DefaultRegistry.LookupVec(e.Name)
		// No argument types to consult here, so a polymorphic declaration
		// answers with its fallback. That is a guess, and it is used the
		// same way a decision is: the guard below re-checks it against the
		// output vector anyway, and a mismatch costs the per-row path.
		vecDecl, c := DefaultRegistry.ReturnType(e.Name).Resolve(0, nil)
		e.vecRet = vecDecl.ID
		e.vecRetOK = c != Undecided
		lower := strings.ToLower(e.Name)
		e.vecTextFn = stringInputFuncs[lower]
		e.vecTypedArgs = typedArgPositions[lower]
	})
	if e.vecFn == nil {
		if e.tryEvalMemoized(b, out, n) {
			return
		}
		for i := 0; i < n; i++ {
			out.SetValue(i, e.Eval(b, i))
		}
		return
	}

	// Resolve argument vectors: ColRef → column directly, Lit → const vector.
	// Allocated per-call to avoid data races across parallel pipeline clones.
	argVecs := make([]*batch.Vector, len(e.Args))
	for i, arg := range e.Args {
		switch a := arg.(type) {
		case *ColRef:
			a.resolve(b)
			if a.idx < 0 || a.idx >= len(b.Columns) {
				e.evalVecPerRow(b, out, n)
				return
			}
			// A ROW FIELD PATH is not vectorizable through these kernels: its
			// idx points at the CONTAINER column, so argVecs[i] below would
			// hand the kernel the ROW vector instead of the field's child —
			// vecYear then read an empty Int64Data and answered NULL, while
			// the same query with only one temporal function took the
			// per-row path and answered correctly (#568). The per-row path
			// resolves the field through ColRef.Eval / resolveTemporalArgs,
			// which already handle a field path; send every field-path arg
			// there, for every vec kernel at once.
			if a.structField != "" {
				e.evalVecPerRow(b, out, n)
				return
			}
			// String-input vec kernels read BytesData; a TypeDate column
			// has none and must render through the (fixed) per-row path
			// (issue #273). The same is true of a column that is not stored
			// as a byte array at all: `SELECT UPPER(int_col)` indexed a nil
			// offsets array and killed the process.
			//
			// TypeIPv4/TypeMAC get the identical guard for the identical
			// reason (#500): both box as a raw encoded int64 with no bytes
			// arena at all, and formatNetworkArgs — the rewrite that turns
			// that back into address text — only runs on the per-row Eval
			// path, never here.
			//
			// Every argument the kernel reads AS TEXT gets that check, not
			// just position 0. Position 0 alone was the premise "the one
			// argument every function in stringInputFuncs reads as text",
			// true for substr/left/right and false for the rest: concat
			// reads all of them, replace three, starts_with/ends_with/
			// contains two. `CONCAT(text_col, int_col)` therefore handed
			// vecConcat a BIGINT vector with a zero-length Offsets slice and
			// took the whole server down on ordinary single-table SQL (#509).
			if e.vecTextFn && (a.typ == batch.TypeDate || a.typ == batch.TypeIPv4 || a.typ == batch.TypeMAC ||
				(!e.vecTypedArgs[i] && !vecTextReadable(b.Columns[a.idx], n))) {
				e.evalVecPerRow(b, out, n)
				return
			}
			argVecs[i] = b.Columns[a.idx]
		case *Lit:
			cv := makeConstVector(a.Val, n)
			if cv == nil {
				e.evalVecPerRow(b, out, n)
				return
			}
			argVecs[i] = cv
		default:
			// Complex expression arg (nested functions, CASE, etc.) —
			// fall back to per-row for safety. Extending to nested VecExpr
			// requires knowing each arg's output type at eval time.
			e.evalVecPerRow(b, out, n)
			return
		}
	}

	// A vec kernel writes a typed slice of out directly — out.Float64Data[i],
	// out.BoolData[i], out.BytesData.Set(i, …) — and which slice is fixed by
	// the function's declared return type. When out cannot hold that type the
	// write runs off the end of a zero-length slice and panics the whole
	// process, every connection with it: four separate functions shipped that
	// way (#310). The declaration is now what types the projection, so the
	// two agree by construction; this guard is what makes that structural
	// rather than a promise, for every kernel at once and every caller that
	// hands a vec expression an output vector of its own choosing. A mismatch
	// costs the per-row path — a slower answer, not a dead server.
	if !vecOutputHolds(out, e.vecRet, e.vecRetOK, n) {
		e.evalVecPerRow(b, out, n)
		return
	}

	e.vecFn(argVecs, out, n)
}

// vecTextReadable reports whether a kernel that reads its argument as text can
// read this vector's bytes directly: values stored as a plain byte array, with
// an offsets array covering the batch. Anything else — an int column, a
// dictionary view whose typed slices are nil — goes through the per-row path,
// which is view- and type-aware.
func vecTextReadable(v *batch.Vector, n int) bool {
	return v != nil && byteArrayShaped(v) && len(v.BytesData.Offsets) > n
}

// vecOutputHolds reports whether out is backed by the storage a kernel
// declared to return t writes into. ok=false (a dynamic declaration) can never
// be safe: nothing says which slice the kernel writes.
func vecOutputHolds(out *batch.Vector, t batch.TypeID, ok bool, n int) bool {
	if out == nil || !ok {
		return false
	}
	switch t {
	case batch.TypeBool:
		return len(out.BoolData) >= n
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return len(out.Int32Data) >= n
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return len(out.Int64Data) >= n
	case batch.TypeFloat32:
		return len(out.Float32Data) >= n
	case batch.TypeFloat64:
		return len(out.Float64Data) >= n
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		return len(out.BytesData.Offsets) > n
	case batch.TypeVector:
		return out.Type == batch.TypeVector && out.VectorDim > 0 &&
			len(out.Float32Data) >= n*out.VectorDim
	}
	return out.Type == t
}

// memoizableFuncs: deterministic, per-row-expensive string→scalar
// functions with no vec kernel. Their batch inputs repeat heavily on real
// data (ClickBench Referer: ~3x duplication within a 2048-row batch), so
// the per-row fallback dedups inputs per batch and evaluates once per
// distinct value. Only shapes where every argument except the first is a
// literal qualify — the memo key is then just the first argument.
var memoizableFuncs = map[string]bool{
	"regexp_replace": true,
	"regexp_extract": true,
	"regexp_like":    true,
	"regexp_matches": true,
}

// tryEvalMemoized runs the per-row fallback with a per-batch input memo.
// Returns false when the call shape doesn't qualify (caller falls through
// to the plain per-row loop).
//
// Memo ownership (load-bearing for the zero-copy keys below): the map is a
// local of this call. One evaluation of one batch, by one goroutine, owns
// it exclusively and it dies at return — parallel pipeline clones share
// the *FuncCall but never the memo. Keys are therefore zero-copy views
// into the input column's arena (map assign stores the string header, it
// does not copy the bytes), which is sound on two invariants:
//
//   - the input batch outlives this call — it is the caller's live batch;
//   - nothing mutates the input column while the call runs. The output
//     vector is a separate pooled batch's column (project.go / plan.go
//     both write into out.Columns[j] of a freshly-obtained batch); an
//     output aliasing the input would already corrupt the pre-existing
//     zero-copy probe and the sequential offset writes, so no-alias is a
//     precondition of this path, not a new one.
//
// Values live under the same rule and can also be views into the input
// (replaceAll returns its argument when nothing matches, and a whole-match
// single-group replacement returns a substring); they are copied into the
// output arena on the way out.
func (e *FuncCall) tryEvalMemoized(b *batch.RecordBatch, out *batch.Vector, n int) bool {
	if len(e.Args) == 0 || !memoizableFuncs[strings.ToLower(e.Name)] {
		return false
	}
	cr, ok := e.Args[0].(*ColRef)
	if !ok {
		return false
	}
	for _, a := range e.Args[1:] {
		if _, isLit := a.(*Lit); !isLit {
			return false
		}
	}
	cr.resolve(b)
	if cr.idx < 0 || cr.idx >= len(b.Columns) {
		return false
	}
	vec := b.Columns[cr.idx]
	if vec.Type != batch.TypeString && vec.Type != batch.TypeBytes {
		return false
	}
	hasNulls := vec.Nulls.HasNulls()
	// Prepared fast path (regexp_prepared.go): literal-argument
	// regexp_replace evaluates as a direct string→string call — compiled
	// pattern and pre-parsed replacement template, no []any boxing.
	prep := e.preparedReplace()
	if out.Type == batch.TypeString {
		e.evalMemoizedStrings(b, vec, out, n, prep, hasNulls)
		return true
	}
	memo := make(map[string]any, n/2)
	for i := 0; i < n; i++ {
		if hasNulls && vec.Nulls.IsNullFast(i) {
			out.SetValue(i, e.Eval(b, i))
			continue
		}
		s := vec.BytesData.UnsafeStringValue(i)
		if v, hit := memo[s]; hit {
			out.SetValue(i, v)
			continue
		}
		var v any
		if prep != nil {
			v = prep.replaceAll(s)
		} else {
			v = e.Eval(b, i)
		}
		out.SetValue(i, v)
		memo[s] = v
	}
	return true
}

// memoStr is the typed memo value for a string-producing memoized call:
// the result plus its NULL flag, stored by value in the map. The generic
// map[string]any charges an interface box per DISTINCT input, and then a
// string→[]byte conversion per ROW inside Vector.SetValue on the way to
// the output arena. Typed, a distinct input costs the match and nothing
// else, and a row costs an append.
type memoStr struct {
	s    string
	null bool
}

// memoStrPool recycles the typed memo's map storage across batches: at
// 2048 rows a freshly-made map is ~90 KB, which was the largest single
// allocation left on this path once the boxing and key clones were gone.
//
// The map is cleared before it goes back, so a checkout always starts
// empty — no result and no view into a retired batch's arena survives into
// the next batch (which would be both a stale-answer bug and a reason
// pooled batch arenas stayed reachable).
var memoStrPool = sync.Pool{
	New: func() any { return make(map[string]memoStr, batch.DefaultBatchSize/2) },
}

// evalMemoizedStrings is tryEvalMemoized's loop for a string output
// column: typed memo, and results written straight into the output
// BytesColumn. Same per-batch memo lifetime as the generic loop — nothing
// here may outlive the batch, and nothing does: memo values can be
// zero-copy views into the input arena (replaceAll returns its argument
// unchanged when the pattern doesn't match), and SetString copies them
// into the output arena on the way out.
func (e *FuncCall) evalMemoizedStrings(b *batch.RecordBatch, vec, out *batch.Vector, n int, prep *preparedRegexp, hasNulls bool) {
	// Only engine-sized batches recycle their map. An oversized batch
	// (GetForSize's escape hatch) would leave a map whose bucket count —
	// and therefore whose clear cost — outlives it, charged to every
	// normal batch after it.
	var memo map[string]memoStr
	if n <= batch.DefaultBatchSize {
		memo = memoStrPool.Get().(map[string]memoStr)
		defer func() {
			clear(memo)
			memoStrPool.Put(memo)
		}()
	} else {
		memo = make(map[string]memoStr, n/2)
	}
	for i := 0; i < n; i++ {
		if hasNulls && vec.Nulls.IsNullFast(i) {
			out.SetValue(i, e.Eval(b, i))
			continue
		}
		s := vec.BytesData.UnsafeStringValue(i)
		mv, hit := memo[s]
		if !hit {
			if prep != nil {
				mv = memoStr{s: prep.replaceAll(s)}
			} else {
				switch tv := e.Eval(b, i).(type) {
				case string:
					mv = memoStr{s: tv}
				case nil:
					mv = memoStr{null: true}
				default:
					// Mirrors Vector.SetValue's coercion for a string
					// column, so a memoizable function that returns
					// something other than a string can't make the typed
					// path diverge from the generic one.
					mv = memoStr{s: fmt.Sprint(tv)}
				}
			}
			memo[s] = mv
		}
		if mv.null {
			out.WriteNullAt(i)
			continue
		}
		out.Nulls.SetValid(i)
		out.BytesData.SetString(i, mv.s)
	}
}

func (e *FuncCall) evalVecPerRow(b *batch.RecordBatch, out *batch.Vector, n int) {
	for i := 0; i < n; i++ {
		out.SetValue(i, e.Eval(b, i))
	}
}

// makeConstVector creates a vector filled with a constant value.
func makeConstVector(val any, n int) *batch.Vector {
	switch v := val.(type) {
	case float64:
		vec := batch.NewVector(batch.TypeFloat64, n)
		for i := 0; i < n; i++ {
			vec.Float64Data[i] = v
		}
		return vec
	case int64:
		vec := batch.NewVector(batch.TypeInt64, n)
		for i := 0; i < n; i++ {
			vec.Int64Data[i] = v
		}
		return vec
	case int:
		return makeConstVector(int64(v), n)
	case string:
		vec := batch.NewVector(batch.TypeString, n)
		b := []byte(v)
		for i := 0; i < n; i++ {
			vec.BytesData.Set(i, b)
		}
		return vec
	case bool:
		vec := batch.NewVector(batch.TypeBool, n)
		for i := 0; i < n; i++ {
			vec.BoolData[i] = v
		}
		return vec
	case nil:
		vec := batch.NewVector(batch.TypeString, n)
		for i := 0; i < n; i++ {
			vec.Nulls.SetNull(i)
		}
		return vec
	default:
		return nil
	}
}

// numericFuncCall wraps a FuncCall to implement Float64Expr and Int64Expr
// for functions known to return numeric values. Created by the compiler for
// functions like extract, year, length, abs, ceil, floor, round, etc.
type numericFuncCall struct {
	*FuncCall
}

func (e *numericFuncCall) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	v := e.Eval(b, row)
	if v == nil {
		return 0, false
	}
	return ToFloat64(v), true
}

func (e *numericFuncCall) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	v := e.Eval(b, row)
	if v == nil {
		return 0, false
	}
	return int64(ToFloat64(v)), true
}

// ScalarFunc is a scalar function implementation.
type ScalarFunc func(args []any) any

// VecScalarFunc is a vectorized scalar function that operates on entire columns
// at once, reading from input vectors and writing to an output vector.
// This avoids per-row interface dispatch and boxing overhead.
type VecScalarFunc func(args []*batch.Vector, out *batch.Vector, n int)

// FuncRegistry is a concurrent-safe registry of scalar functions.
type FuncRegistry struct {
	mu         sync.RWMutex
	funcs      map[string]ScalarFunc
	rets       map[string]Ret // declared return type, one per registered function
	vecFuncs   map[string]VecScalarFunc
	vecReturns map[string]func() int // funcs returning VECTOR; value yields the dimension
}

// NewFuncRegistry creates a new empty function registry.
func NewFuncRegistry() *FuncRegistry {
	return &FuncRegistry{
		funcs:      make(map[string]ScalarFunc),
		rets:       make(map[string]Ret),
		vecFuncs:   make(map[string]VecScalarFunc),
		vecReturns: make(map[string]func() int),
	}
}

// Register adds or replaces a scalar function. ret declares the type the
// function's results are stored as; the planner types projections from it (see
// Ret). Registering without a declaration does not compile, and registering
// the zero value panics here rather than letting a mistyped output vector
// reach a kernel.
func (r *FuncRegistry) Register(name string, fn ScalarFunc, ret Ret) {
	if !ret.Declared() {
		panic(fmt.Sprintf("expr: function %q registered without a declared return type", name))
	}
	r.mu.Lock()
	r.funcs[strings.ToLower(name)] = fn
	r.rets[strings.ToLower(name)] = ret
	r.mu.Unlock()
}

// Unregister removes a scalar function. Returns true if it existed.
func (r *FuncRegistry) Unregister(name string) bool {
	r.mu.Lock()
	_, existed := r.funcs[strings.ToLower(name)]
	delete(r.funcs, strings.ToLower(name))
	delete(r.rets, strings.ToLower(name))
	r.mu.Unlock()
	return existed
}

// ReturnType returns the declared return type of a function. An unregistered
// name yields the zero Ret, which reports Declared() == false and resolves to
// "caller keeps its fallback".
func (r *FuncRegistry) ReturnType(name string) Ret {
	r.mu.RLock()
	ret := r.rets[strings.ToLower(name)]
	r.mu.RUnlock()
	return ret
}

// Lookup returns the function with the given name, or nil if not found.
func (r *FuncRegistry) Lookup(name string) ScalarFunc {
	r.mu.RLock()
	fn := r.funcs[strings.ToLower(name)]
	r.mu.RUnlock()
	return fn
}

// RegisterVec adds a vectorized implementation for a scalar function. A vec
// kernel writes a typed slice of the output vector, so the function it
// accelerates must already be registered with the return type that names that
// slice — registering a kernel for an undeclared function is the exact setup
// that panicked the server four times, and panics here instead.
func (r *FuncRegistry) RegisterVec(name string, fn VecScalarFunc) {
	r.mu.Lock()
	if !r.rets[strings.ToLower(name)].Declared() {
		r.mu.Unlock()
		panic(fmt.Sprintf("expr: vec kernel registered for %q before its return type was declared", name))
	}
	r.vecFuncs[strings.ToLower(name)] = fn
	r.mu.Unlock()
}

// LookupVec returns the vectorized function with the given name, or nil if not found.
func (r *FuncRegistry) LookupVec(name string) VecScalarFunc {
	r.mu.RLock()
	fn := r.vecFuncs[strings.ToLower(name)]
	r.mu.RUnlock()
	return fn
}

// RegisterVecReturn marks a function as returning a VECTOR. dimFn is evaluated
// lazily (at plan time) to obtain the output dimensionality — embed(), for
// example, derives it from the configured embedding provider.
func (r *FuncRegistry) RegisterVecReturn(name string, dimFn func() int) {
	r.mu.Lock()
	r.vecReturns[strings.ToLower(name)] = dimFn
	r.mu.Unlock()
}

// VecReturnDim reports whether the named function returns a VECTOR and, if so,
// its current output dimension. ok is false for non-vector-returning functions.
func (r *FuncRegistry) VecReturnDim(name string) (dim int, ok bool) {
	r.mu.RLock()
	dimFn := r.vecReturns[strings.ToLower(name)]
	r.mu.RUnlock()
	if dimFn == nil {
		return 0, false
	}
	return dimFn(), true
}

// Has returns true if a function with the given name exists.
func (r *FuncRegistry) Has(name string) bool {
	r.mu.RLock()
	_, ok := r.funcs[strings.ToLower(name)]
	r.mu.RUnlock()
	return ok
}

// Names returns all registered function names.
func (r *FuncRegistry) Names() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.funcs))
	for name := range r.funcs {
		names = append(names, name)
	}
	r.mu.RUnlock()
	return names
}

// DefaultRegistry is the global function registry used by the expression engine.
var DefaultRegistry = NewFuncRegistry()

// builtin pairs an implementation with its declared return type. Both fields
// are positional in the table below, so an entry that names an implementation
// without saying what it returns does not compile — which is the point: the
// planner reads these declarations to type projections, and for four
// generations of this bug the two lived in different files with nothing
// linking them (#310).
type builtin struct {
	fn  ScalarFunc
	ret Ret
}

func init() {
	builtins := map[string]builtin{
		// String functions
		"upper":  {fnUpper, RetString},
		"lower":  {fnLower, RetString},
		"concat": {fnConcat, RetString},
		// ConcatOpFunc is the `||` OPERATOR, not a function anyone can call:
		// its name is punctuation, which no SQL identifier — delimited or
		// not — can be. It is registered here rather than compiled inline so
		// it inherits the declared String return type and the vec dispatch
		// (#328), while keeping CONCAT's NULL rule off it (#609).
		ConcatOpFunc: {fnConcatOp, RetString},
		"length":     {fnLength, RetInt32},
		"len":        {fnLength, RetInt32},
		// length() has always counted BYTES here (see fnLength), so
		// octet_length is an exact alias and bit_length is 8x. The rune-counting
		// member of the family is char_length/character_length below. These
		// three names were reachable from the parser and typed numeric by the
		// planner but had no implementation, so they evaluated to NULL.
		"octet_length": {fnOctetLength, RetInt32},
		"bit_length":   {fnBitLength, RetInt32},
		"substr":       {fnSubstr, RetString},
		"substring":    {fnSubstr, RetString},
		"trim":         {fnTrim, RetString},
		"ltrim":        {fnLTrim, RetString},
		"rtrim":        {fnRTrim, RetString},
		"replace":      {fnReplace, RetString},
		"reverse":      {fnReverse, RetString},
		"left":         {fnLeft, RetString},
		"right":        {fnRight, RetString},

		// Math functions
		"abs":   {fnAbs, RetFloat64},
		"ceil":  {fnCeil, RetFloat64},
		"floor": {fnFloor, RetFloat64},
		"round": {fnRound, RetFloat64},
		// Half-to-even ROUND for a DOUBLE PRECISION/REAL/FLOAT operand
		// (#381). compileFuncCallNode rewrites ROUND(CAST(x AS double
		// precision)) to call this instead; it is not part of ROUND's
		// documented surface but is harmless to reach directly by name.
		"round_half_even": {fnRoundHalfEven, RetFloat64},
		"pow":             {fnPow, RetFloat64},
		"power":           {fnPow, RetFloat64},
		"sqrt":            {fnSqrt, RetFloat64},
		"mod":             {fnMod, RetFloat64},
		"log":             {fnLog, RetFloat64},
		"ln":              {fnLn, RetFloat64},
		"exp":             {fnExp, RetFloat64},

		// Conditional
		"coalesce": {fnCoalesce, RetSameAsArg(batch.TypeFloat64)},
		"nullif":   {fnNullIf, RetSameAsArg(batch.TypeFloat64, 0).TypeOverAllArgs()},
		"ifnull":   {fnIfNull, RetSameAsArg(batch.TypeString, 0, 1)},
		"if":       {fnIf, RetSameAsArg(batch.TypeString, 1, 2).Control(0)},

		// Type casting
		"cast_int":    {fnCastInt, RetInt64},
		"cast_float":  {fnCastFloat, RetFloat64},
		"cast_string": {fnCastString, RetString},

		// Network functions
		"ip_to_string":  {fnIPToString, RetString},
		"cidr_contains": {fnCIDRContains, RetBool},
		"ip_version":    {fnIPVersion, RetFloat64},
		"mask_ip":       {fnMaskIP, RetString},
		"mac_to_string": {fnMACToString, RetString},
		"ip_subnet":     {fnIPSubnet, RetString},
		"ip_netmask":    {fnIPNetmask, RetString},

		// Date/time functions
		"now":          {fnNow, RetString},
		"year":         {fnYear, RetFloat64},
		"month":        {fnMonth, RetFloat64},
		"day":          {fnDay, RetFloat64},
		"hour":         {fnHour, RetFloat64},
		"minute":       {fnMinute, RetFloat64},
		"date_trunc":   {fnDateTrunc, RetString},
		"extract":      {fnExtract, RetFloat64},
		"current_date": {fnCurrentDate, RetString},
		"date_diff":    {fnDateDiff, RetFloat64},

		// Session / catalog information (see the SessionUser block below)
		"current_user":     {fnCurrentUser, RetString},
		"session_user":     {fnCurrentUser, RetString},
		"user":             {fnCurrentUser, RetString},
		"current_role":     {fnCurrentUser, RetString},
		"current_catalog":  {fnCurrentCatalog, RetString},
		"current_database": {fnCurrentCatalog, RetString},
		"current_schema":   {fnCurrentSchema, RetString},
		"current_schemas":  {fnCurrentSchemas, RetString},
		"version":          {fnVersion, RetString},

		"date_add": {fnDateAdd, RetString},
		"date_sub": {fnDateSub, RetString},
		"to_date":  {fnToDate, RetString},

		// UUID functions
		"uuid_version":   {fnUUIDVersion, RetFloat64},
		"uuid_to_string": {fnUUIDToString, RetString},

		// Additional string functions
		"starts_with": {fnStartsWith, RetBool},
		"ends_with":   {fnEndsWith, RetBool},
		"contains":    {fnContains, RetBool},
		"repeat":      {fnRepeat, RetString},

		// Additional math functions
		"sign":     {fnSign, RetFloat64},
		"greatest": {fnGreatest, RetSameAsArg(batch.TypeFloat64)},
		"least":    {fnLeast, RetSameAsArg(batch.TypeFloat64)},

		// Additional date/time functions
		"second": {fnSecond, RetFloat64},

		// String: regex and parsing
		"split_part":     {fnSplitPart, RetString},
		"strpos":         {fnStrPos, RetInt32},
		"position":       {fnStrPos, RetInt32},
		"regexp_like":    {fnRegexpLike, RetBool},
		"regexp_extract": {fnRegexpExtract, RetString},
		"regexp_replace": {fnRegexpReplace, RetString},

		// Encoding
		"to_hex":      {fnToHex, RetString},
		"from_hex":    {fnFromHex, RetFloat64},
		"to_base64":   {fnToBase64, RetString},
		"from_base64": {fnFromBase64, RetString},

		// Date/time conversion
		"from_unixtime": {fnFromUnixtime, RetString},
		"to_unixtime":   {fnToUnixtime, RetFloat64},
		"date_format":   {fnDateFormat, RetString},
		"date_parse":    {fnDateParse, RetString},

		// Hash
		"md5":    {fnMD5, RetString},
		"sha256": {fnSHA256, RetString},
		"sha512": {fnSHA512, RetString},

		// Bitwise
		"bitwise_and": {fnBitwiseAnd, RetFloat64},
		"bitwise_or":  {fnBitwiseOr, RetFloat64},
		"bitwise_xor": {fnBitwiseXor, RetFloat64},
		"bitwise_not": {fnBitwiseNot, RetFloat64},

		// String: padding and character
		"lpad":             {fnLPad, RetString},
		"rpad":             {fnRPad, RetString},
		"chr":              {fnChr, RetString},
		"codepoint":        {fnCodepoint, RetInt32},
		"concat_ws":        {fnConcatWS, RetString},
		"char_length":      {fnCharLength, RetInt32},
		"character_length": {fnCharLength, RetInt32},
		"translate":        {fnTranslate, RetString},

		// Math: trigonometry
		"pi":       {fnPi, RetFloat64},
		"degrees":  {fnDegrees, RetFloat64},
		"radians":  {fnRadians, RetFloat64},
		"sin":      {fnSin, RetFloat64},
		"cos":      {fnCos, RetFloat64},
		"tan":      {fnTan, RetFloat64},
		"asin":     {fnAsin, RetFloat64},
		"acos":     {fnAcos, RetFloat64},
		"atan":     {fnAtan, RetFloat64},
		"atan2":    {fnAtan2, RetFloat64},
		"cbrt":     {fnCbrt, RetFloat64},
		"log2":     {fnLog2, RetFloat64},
		"truncate": {fnTruncate, RetFloat64},
		"rand":     {fnRandom, RetFloat64},
		"random":   {fnRandom, RetFloat64},

		// JSON
		"json_extract":        {fnJSONExtract, RetDynamic},
		"json_extract_scalar": {fnJSONExtractScalar, RetDynamic},
		"json_array_length":   {fnJSONArrayLength, RetFloat64},
		"json_valid":          {fnJSONValid, RetBool},

		// URL
		"url_extract_host":      {fnURLExtractHost, RetString},
		"url_extract_port":      {fnURLExtractPort, RetFloat64},
		"url_extract_path":      {fnURLExtractPath, RetString},
		"url_extract_protocol":  {fnURLExtractProtocol, RetString},
		"url_extract_query":     {fnURLExtractQuery, RetString},
		"url_extract_parameter": {fnURLExtractParameter, RetString},

		// Type introspection
		"typeof": {fnTypeof, RetString},

		// String: distance and utility
		"soundex":              {fnSoundex, RetString},
		"levenshtein_distance": {fnLevenshtein, RetFloat64},
		"hamming_distance":     {fnHamming, RetFloat64},
		"normalize":            {fnNormalize, RetString},
		"format":               {fnFormat, RetString},
		"lcase":                {fnLower, RetString},
		"ucase":                {fnUpper, RetString},
		"to_utf8":              {fnToUTF8, RetBytes},
		"from_utf8":            {fnFromUTF8, RetString},

		// Math: IEEE 754 and utility
		"e":            {fnE, RetFloat64},
		"log10":        {fnLog10, RetFloat64},
		"infinity":     {fnInfinity, RetFloat64},
		"nan":          {fnNaN, RetFloat64},
		"is_nan":       {fnIsNaN, RetBool},
		"is_finite":    {fnIsFinite, RetBool},
		"is_infinite":  {fnIsInfinite, RetBool},
		"width_bucket": {fnWidthBucket, RetInt32},
		"from_base":    {fnFromBase, RetFloat64},
		"to_base":      {fnToBase, RetString},
		"bit_count":    {fnBitCount, RetFloat64},

		// Hash: additional
		"sha1":        {fnSHA1, RetString},
		"crc32":       {fnCRC32, RetFloat64},
		"hmac_sha256": {fnHMACSHA256, RetString},
		"hmac_sha512": {fnHMACSHA512, RetString},

		// Date: additional accessors
		"quarter":           {fnQuarter, RetFloat64},
		"week":              {fnWeek, RetFloat64},
		"day_of_week":       {fnDayOfWeek, RetFloat64},
		"day_of_year":       {fnDayOfYear, RetFloat64},
		"last_day_of_month": {fnLastDayOfMonth, RetString},
		"current_timestamp": {fnCurrentTimestamp, RetString},
		"at_timezone":       {fnAtTimezone, RetString},
		// epoch: the rewrite target of EXTRACT(EPOCH FROM ts).
		// timezone: the rewrite target of `ts AT TIME ZONE zone`, zone first,
		// matching PostgreSQL's own canonical form.
		"epoch":                    {fnEpoch, RetFloat64},
		"timezone":                 {fnTimezone, RetString},
		"pg_postmaster_start_time": {fnPgPostmasterStartTime, RetString},
		"human_readable_seconds":   {fnHumanReadableSeconds, RetString},

		// Network: analytics
		"is_private_ip":  {fnIsPrivateIP, RetBool},
		"is_loopback_ip": {fnIsLoopbackIP, RetBool},
		"ip_to_int":      {fnIPToInt, RetFloat64},
		"int_to_ip":      {fnIntToIP, RetString},
		"is_ipv4":        {fnIsIPv4, RetBool},
		"is_ipv6":        {fnIsIPv6, RetBool},

		// Network: CIDR / subnet operations
		"network_address":   {fnNetworkAddress, RetString},
		"broadcast_address": {fnBroadcastAddress, RetString},
		"prefix_length":     {fnPrefixLength, RetInt64},
		"cidr_to_range":     {fnCIDRToRange, RetString},
		"hosts_in_cidr":     {fnHostsInCIDR, RetInt64},
		"cidr_overlap":      {fnCIDROverlap, RetBool},
		"ip_in_range":       {fnIPInRange, RetBool},
		"same_subnet":       {fnSameSubnet, RetBool},

		// Network: IP manipulation
		"ip_add":           {fnIPAdd, RetString},
		"ip_subtract":      {fnIPSubtract, RetString},
		"ip_diff":          {fnIPDiff, RetInt64},
		"ip_between":       {fnIPBetween, RetBool},
		"reverse_dns":      {fnReverseDNS, RetString},
		"is_multicast_ip":  {fnIsMulticastIP, RetBool},
		"is_link_local_ip": {fnIsLinkLocalIP, RetBool},
		"is_reserved_ip":   {fnIsReservedIP, RetBool},
		"ip_to_hex":        {fnIPToHex, RetString},

		// Network: MAC operations
		"mac_vendor_oui": {fnMACVendorOUI, RetString},
		"mac_is_unicast": {fnMACIsUnicast, RetBool},
		"mac_is_local":   {fnMACIsLocal, RetBool},
		"mac_format":     {fnMACFormat, RetString},

		// Network: port classification
		"port_name":          {fnPortName, RetString},
		"is_well_known_port": {fnIsWellKnownPort, RetBool},
		"is_registered_port": {fnIsRegisteredPort, RetBool},
		"is_ephemeral_port":  {fnIsEphemeralPort, RetBool},
		"port_class":         {fnPortClass, RetString},

		// Network: protocol
		"protocol_name":   {fnProtocolName, RetString},
		"protocol_number": {fnProtocolNumber, RetInt64},

		// Deep inspection: TCP
		"tcp_flags_to_string":   {fnTCPFlagsToString, RetString},
		"has_tcp_flag":          {fnHasTCPFlag, RetBool},
		"tcp_flags_from_string": {fnTCPFlagsFromString, RetInt64},
		"is_tcp_handshake":      {fnIsTCPHandshake, RetBool},
		"is_tcp_reset":          {fnIsTCPReset, RetBool},
		"tcp_session_id":        {fnTCPSessionID, RetString},
		"flow_direction":        {fnFlowDirection, RetString},

		// Deep inspection: DNS
		"dns_query_name":     {fnDNSQueryName, RetString},
		"dns_query_type":     {fnDNSQueryType, RetString},
		"dns_is_response":    {fnDNSIsResponse, RetBool},
		"dns_response_code":  {fnDNSResponseCode, RetString},
		"dns_question_count": {fnDNSQuestionCount, RetInt64},
		"dns_answer_count":   {fnDNSAnswerCount, RetInt64},
		"dns_transaction_id": {fnDNSTransactionID, RetInt64},

		// Deep inspection: TLS
		"tls_sni":             {fnTLSSNI, RetString},
		"tls_version":         {fnTLSVersion, RetString},
		"tls_record_type":     {fnTLSRecordType, RetString},
		"is_tls_client_hello": {fnIsTLSClientHello, RetBool},
		"tls_handshake_type":  {fnTLSHandshakeType, RetString},

		// Deep inspection: HTTP
		"http_method":         {fnHTTPMethod, RetString},
		"http_path":           {fnHTTPPath, RetString},
		"http_host":           {fnHTTPHost, RetString},
		"http_status_code":    {fnHTTPStatusCode, RetInt64},
		"http_status_class":   {fnHTTPStatusClass, RetString},
		"http_content_type":   {fnHTTPContentType, RetString},
		"http_content_length": {fnHTTPContentLength, RetInt64},
		"http_user_agent":     {fnHTTPUserAgent, RetString},
		"http_header":         {fnHTTPHeader, RetString},
		"http_version":        {fnHTTPVersion, RetString},
		"is_http_request":     {fnIsHTTPRequest, RetBool},
		"is_http_response":    {fnIsHTTPResponse, RetBool},

		// Deep inspection: packet headers
		"ip_header_length": {fnIPHeaderLength, RetInt64},
		"ip_ttl":           {fnIPTTL, RetInt64},
		"ip_total_length":  {fnIPTotalLength, RetInt64},
		"ip_dscp":          {fnIPDSCP, RetInt64},
		"ether_type":       {fnEtherType, RetString},
		"vlan_id":          {fnVLANID, RetInt64},

		// Deep inspection: payload analysis
		"payload_entropy":  {fnPayloadEntropy, RetFloat64},
		"payload_hex_dump": {fnPayloadHexDump, RetString},

		// ICMP
		"icmp_type_name": {fnICMPTypeName, RetString},
		"icmp_code_name": {fnICMPCodeName, RetString},
		"is_icmp_echo":   {fnIsICMPEcho, RetBool},
		"icmp_parse":     {fnICMPParse, RetString},
		"icmp_type":      {fnICMPType, RetInt64},
		"icmp_code":      {fnICMPCode, RetInt64},

		// IPv6
		"ipv6_scope":     {fnIPv6Scope, RetString},
		"ipv6_expand":    {fnIPv6Expand, RetString},
		"ipv6_compress":  {fnIPv6Compress, RetString},
		"ipv6_to_eui64":  {fnIPv6ToEUI64, RetString},
		"is_6to4":        {fnIs6to4, RetBool},
		"is_teredo":      {fnIsTeredo, RetBool},
		"teredo_server":  {fnTeredoServer, RetString},
		"teredo_client":  {fnTeredoClient, RetString},
		"sixto4_gateway": {fnSixto4Gateway, RetString},

		// JA3 TLS fingerprinting
		"ja3_fingerprint":  {fnJA3Fingerprint, RetString},
		"ja3_string":       {fnJA3String, RetString},
		"ja3s_fingerprint": {fnJA3SFingerprint, RetString},
		"ja3s_string":      {fnJA3SString, RetString},

		// Payload search
		"payload_contains": {fnPayloadContains, RetBool},
		"payload_matches":  {fnPayloadMatches, RetBool},
		"payload_offset":   {fnPayloadOffset, RetString},
		"payload_length":   {fnPayloadLength, RetInt64},

		// Regex: additional
		"regexp_count":       {fnRegexpCount, RetInt64},
		"regexp_extract_all": {fnRegexpExtractAll, RetString},
		"regexp_split":       {fnRegexpSplit, RetString},

		// String: additional
		"split": {fnSplit, RetString},

		// Bitwise: shifts
		"bitwise_left_shift":             {fnBitwiseLeftShift, RetInt64},
		"bitwise_right_shift":            {fnBitwiseRightShift, RetInt64},
		"bitwise_arithmetic_shift_right": {fnBitwiseArithmeticShiftRight, RetInt64},

		// UUID: generation
		"uuid": {fnUUID, RetString},

		// Encoding: additional
		"to_base32":   {fnToBase32, RetString},
		"from_base32": {fnFromBase32, RetString},
		"xxhash64":    {fnXXHash64, RetString},
		"murmur3":     {fnMurmur3, RetString},

		// Date/time: ISO 8601
		"from_iso8601_timestamp": {fnFromISO8601Timestamp, RetInt64},
		"from_iso8601_date":      {fnFromISO8601Date, RetString},
		"to_iso8601":             {fnToISO8601, RetString},
		"to_milliseconds":        {fnToMilliseconds, RetInt64},
		"timezone_hour":          {fnTimezoneHour, RetInt64},
		"timezone_minute":        {fnTimezoneMinute, RetInt64},

		// Formatting
		"format_number": {fnFormatNumber, RetString},

		// GeoIP / ASN lookup (requires MaxMind MMDB databases)
		"geoip_country":      {fnGeoipCountry, RetString},
		"geoip_country_name": {fnGeoipCountryName, RetString},
		"geoip_city":         {fnGeoipCity, RetString},
		"geoip_subdivision":  {fnGeoipSubdivision, RetString},
		"geoip_postal_code":  {fnGeoipPostalCode, RetString},
		"geoip_latitude":     {fnGeoipLatitude, RetFloat64},
		"geoip_longitude":    {fnGeoipLongitude, RetFloat64},
		"geoip_timezone":     {fnGeoipTimezone, RetString},
		"geoip_continent":    {fnGeoipContinent, RetString},
		"geoip_asn":          {fnGeoipASN, RetInt64},
		"geoip_org":          {fnGeoipOrg, RetString},

		// Byte/rate formatting
		"format_bytes": {fnFormatBytes, RetString},
		"parse_bytes":  {fnParseBytes, RetInt64},
		"format_rate":  {fnFormatRate, RetString},
		"parse_rate":   {fnParseRate, RetInt64},

		// Array/nested type functions (Trino-compatible)
		"cardinality":    {fnCardinality, RetInt32},
		"array_length":   {fnCardinality, RetInt32},
		"element_at":     {fnElementAt, RetDynamic},
		"array_contains": {fnArrayContains, RetBool},
		"array_join":     {fnArrayJoin, RetString},
		"array_min":      {fnArrayMin, RetDynamic},
		"array_max":      {fnArrayMax, RetDynamic},

		// ROW/struct functions
		"row_field":    {fnRowField, RetDynamic},
		"struct_field": {fnRowField, RetDynamic},

		// Domain parsing (DNS threat hunting)
		"registered_domain": {fnRegisteredDomain, RetString},
		"tld":               {fnTLD, RetString},
		"subdomain":         {fnSubdomain, RetString},
		"domain_depth":      {fnDomainDepth, RetFloat64},

		// URL encoding/decoding
		"url_encode": {fnURLEncode, RetString},
		"url_decode": {fnURLDecode, RetString},

		// String analysis
		"entropy": {fnEntropy, RetFloat64},

		// MAP functions
		"map_keys":         {fnMapKeys, RetArray},
		"map_values":       {fnMapValues, RetArray},
		"map_entries":      {fnMapEntries, RetArray},
		"map_from_entries": {fnMapFromEntries, RetMap},
	}
	for name, b := range builtins {
		DefaultRegistry.Register(name, b.fn, b.ret)
	}

	// Vectorized implementations: operate on entire columns instead of per-row.
	vecBuiltins := map[string]VecScalarFunc{
		"upper":        vecUpper,
		"lower":        vecLower,
		"length":       vecLength,
		"len":          vecLength,
		"octet_length": vecOctetLength,
		"bit_length":   vecBitLength,
		// Rune counting needs the bytes — no offsets fast path exists.
		"char_length":      vecCharLength,
		"character_length": vecCharLength,
		"trim":             vecTrim,
		"ltrim":            vecLTrim,
		"rtrim":            vecRTrim,
		"substr":           vecSubstr,
		"substring":        vecSubstr,
		"replace":          vecReplace,
		"reverse":          vecReverse,
		"left":             vecLeft,
		"right":            vecRight,
		"concat":           vecConcat,
		ConcatOpFunc:       vecConcatOp,
		"starts_with":      vecStartsWith,
		"ends_with":        vecEndsWith,
		"contains":         vecContains,
		"abs":              vecAbs,
		"ceil":             vecCeil,
		"floor":            vecFloor,
		"round":            vecRound,
		"round_half_even":  vecRoundHalfEven,
		"year":             vecYear,
		"month":            vecMonth,
		"day":              vecDay,
		"hour":             vecHour,
		"extract":          vecExtract,
	}
	for name, fn := range vecBuiltins {
		DefaultRegistry.RegisterVec(name, fn)
	}
}

// RegisterFunc registers a custom scalar function in the default registry.
// ret declares what the function returns; see Ret.
func RegisterFunc(name string, fn ScalarFunc, ret Ret) {
	DefaultRegistry.Register(name, fn, ret)
}

// --- String function implementations ---

func fnUpper(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.ToUpper(toString(args[0]))
}

func fnLower(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.ToLower(toString(args[0]))
}

// ConcatOpFunc is the registry name of the `||` OPERATOR's implementation.
//
// It is punctuation on purpose. A registry entry is reachable from SQL only
// if a query can spell its name, and no SQL identifier — bare or delimited —
// is `||`: the lexer reads those two bytes as an operator token before any
// identifier rule sees them. So the operator's NULL-propagating kernels
// cannot be invoked as a function, and `CONCAT` cannot reach them (#609).
// The census carries a fixture that attempts both spellings, because
// "unspellable" is a claim and the protocol's method 10 says a claim gets a
// fixture rather than a comment.
const ConcatOpFunc = "||"

// fnConcat is the CONCAT() FUNCTION, which IGNORES NULL arguments —
// `CONCAT('a', NULL, 'b')` is `ab` and `CONCAT(NULL, NULL)` is the EMPTY
// STRING, never NULL (PostgreSQL 17; ADR-0012 makes it the authority). It is
// fnConcatWS without a separator, which is where the rule was already right
// (#609).
//
// The `||` OPERATOR is the other rule and has its own kernels below: it
// propagates NULL. The two were one function until #609, which is why
// making this one NULL-tolerant is only half the fix.
func fnConcat(args []any) any {
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			continue
		}
		sb.WriteString(toString(a))
	}
	return sb.String()
}

// fnConcatOp is the `||` OPERATOR: NULL in any operand makes the whole
// expression NULL. Registered under a name no SQL identifier can spell, so
// the operator and the function cannot be confused for one another by a
// query — compile.go lowers `||` to it (#609).
func fnConcatOp(args []any) any {
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			return nil
		}
		sb.WriteString(toString(a))
	}
	return sb.String()
}

func fnLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return int32(len(toString(args[0])))
}

func fnSubstr(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	start := int(ToFloat64(args[1])) - 1 // SQL is 1-indexed
	if len(args) >= 3 && args[2] != nil {
		length := int(ToFloat64(args[2]))
		lo, hi := substrWindow(start, length, len(s))
		return s[lo:hi]
	}
	if start < 0 {
		start = 0
	}
	if start >= len(s) {
		return ""
	}
	return s[start:]
}

// substrWindow clamps SUBSTR's [start, start+length) character window (both
// 0-based here) into valid slice bounds for a string of length n. The window
// rule is PostgreSQL's (#373): a start below position 1 consumes part of the
// length before the string begins — SUBSTR('abcdef', 0, 3) selects positions
// 0,1,2 of which only 1 and 2 exist, so 'ab' and not 'abc'. A non-positive
// or overflowed window is empty rather than an error (PostgreSQL errors only
// on a negative LENGTH, which this engine's scalar layer cannot raise).
func substrWindow(start, length, n int) (int, int) {
	end := start + length
	if length < 0 || end < start { // negative length or overflow
		end = start
	}
	if start < 0 {
		start = 0
	}
	if start > n {
		start = n
	}
	if end < start {
		end = start
	}
	if end > n {
		end = n
	}
	return start, end
}

func fnTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimSpace(toString(args[0]))
}

func fnLTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimLeft(toString(args[0]), " \t\n\r")
}

func fnRTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimRight(toString(args[0]), " \t\n\r")
}

func fnReplace(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2]))
}

func fnReverse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	runes := []rune(toString(args[0]))
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func fnLeft(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	return s[:n]
}

func fnRight(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	return s[len(s)-n:]
}

func fnStartsWith(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.HasPrefix(toString(args[0]), toString(args[1]))
}

func fnEndsWith(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.HasSuffix(toString(args[0]), toString(args[1]))
}

func fnContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.Contains(toString(args[0]), toString(args[1]))
}

func fnRepeat(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	return strings.Repeat(toString(args[0]), n)
}

// --- Math function implementations ---

func fnAbs(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Abs(ToFloat64(args[0]))
}

func fnCeil(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Ceil(ToFloat64(args[0]))
}

func fnFloor(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Floor(ToFloat64(args[0]))
}

func fnRound(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	precision := 0
	if len(args) >= 2 && args[1] != nil {
		precision = int(ToFloat64(args[1]))
	}
	pow := math.Pow(10, float64(precision))
	return math.Round(v*pow) / pow
}

// fnRoundHalfEven is ROUND for a DOUBLE PRECISION (and REAL/FLOAT) operand.
// PostgreSQL rounds NUMERIC half AWAY from zero — fnRound's math.Round,
// above — but rounds DOUBLE PRECISION half TO EVEN ("banker's rounding"),
// which is what distinguishes ROUND(0.5) = 1 from ROUND(CAST(0.5 AS double
// precision)) = 0 (#381). compileFuncCallNode routes here when ROUND's
// argument is an explicit CAST to a binary float type; fnRound alone can't
// make this decision because Wadjet has no numeric tower — a NUMERIC
// literal and a DOUBLE PRECISION value are both a bare float64 by the time
// either kernel sees them, so the type distinction has to be resolved by
// the caller, before that boxing.
//
// This is unrelated to CAST(x AS integer)'s own rounding rule (#373, half
// away from zero): PostgreSQL specifies CAST and ROUND independently, and a
// tie-breaking rule chosen for one operation says nothing about the other —
// don't be tempted to unify them.
func fnRoundHalfEven(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	precision := 0
	if len(args) >= 2 && args[1] != nil {
		precision = int(ToFloat64(args[1]))
	}
	pow := math.Pow(10, float64(precision))
	return math.RoundToEven(v*pow) / pow
}

func fnPow(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return math.Pow(ToFloat64(args[0]), ToFloat64(args[1]))
}

func fnSqrt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v < 0 {
		return nil
	}
	return math.Sqrt(v)
}

func fnMod(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return math.Mod(ToFloat64(args[0]), ToFloat64(args[1]))
}

func fnLog(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v <= 0 {
		return nil
	}
	if len(args) >= 2 && args[1] != nil {
		// LOG(base, value)
		base := v
		val := ToFloat64(args[1])
		if base <= 0 || base == 1 || val <= 0 {
			return nil
		}
		return math.Log(val) / math.Log(base)
	}
	return math.Log10(v)
}

func fnLn(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v <= 0 {
		return nil
	}
	return math.Log(v)
}

func fnExp(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Exp(ToFloat64(args[0]))
}

func fnSign(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	switch {
	case v > 0:
		return float64(1)
	case v < 0:
		return float64(-1)
	default:
		return float64(0)
	}
}

// fnGreatest and fnLeast order their arguments through compare, the same
// type-aware comparison =, < and > use — not ToFloat64, which is 0 for every
// string and therefore ranked every string argument equal, so GREATEST over
// two string columns always answered with argument 0. That was invisible while
// the projection was typed numeric and printed 0 for all of them (#333); with
// the type right, the ordering is what is left to be wrong.
func fnGreatest(args []any) any {
	var best any
	for _, a := range args {
		if a == nil {
			continue
		}
		if best == nil || compare(a, best, CmpGt) {
			best = a
		}
	}
	return best
}

func fnLeast(args []any) any {
	var best any
	for _, a := range args {
		if a == nil {
			continue
		}
		if best == nil || compare(a, best, CmpLt) {
			best = a
		}
	}
	return best
}

// --- Conditional function implementations ---

func fnCoalesce(args []any) any {
	for _, a := range args {
		if a != nil {
			return a
		}
	}
	return nil
}

func fnNullIf(args []any) any {
	if len(args) < 2 {
		return nil
	}
	if args[0] != nil && args[1] != nil && compare(args[0], args[1], CmpEq) {
		return nil
	}
	return args[0]
}

func fnIfNull(args []any) any {
	if len(args) < 2 {
		return nil
	}
	if args[0] != nil {
		return args[0]
	}
	return args[1]
}

func fnIf(args []any) any {
	if len(args) < 3 {
		return nil
	}
	if args[0] != nil && toBoolVal(args[0]) {
		return args[1]
	}
	return args[2]
}

// --- Type casting ---

func fnCastInt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// Same rule as CAST(x AS integer): round half away from zero (#373).
	if i, ok := toInt64Safe(args[0]); ok {
		return i
	}
	return int64(math.Round(ToFloat64(args[0])))
}

func fnCastFloat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0])
}

func fnCastString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return fmt.Sprint(args[0])
}

// --- Date/time functions ---

func fnNow(args []any) any {
	return time.Now().Format(time.RFC3339)
}

func fnYear(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Year())
}

func fnMonth(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Month())
}

func fnDay(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Day())
}

func fnHour(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Hour())
}

func fnMinute(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Minute())
}

func fnSecond(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Second())
}

func fnDateTrunc(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	unit := strings.ToLower(fmt.Sprint(args[0]))
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	switch unit {
	case "year":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "quarter":
		q1 := time.Month((int(t.Month())-1)/3*3 + 1)
		return time.Date(t.Year(), q1, 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "week":
		// ISO convention (DuckDB/Postgres): truncate to Monday.
		d := t
		for d.Weekday() != time.Monday {
			d = d.AddDate(0, 0, -1)
		}
		return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "hour":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Format(time.RFC3339)
	case "minute":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()).Format(time.RFC3339)
	case "second":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location()).Format(time.RFC3339)
	default:
		return nil
	}
}

func fnExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	unit := strings.ToLower(fmt.Sprint(args[0]))
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	// Unit set kept identical to vecExtract's, so the two paths answer the
	// same question — quarter/week/epoch used to exist only in the kernel.
	switch unit {
	case "year":
		return float64(t.Year())
	case "quarter":
		return float64((int(t.Month())-1)/3 + 1)
	case "month":
		return float64(t.Month())
	case "week":
		_, week := t.ISOWeek()
		return float64(week)
	case "day":
		return float64(t.Day())
	case "hour":
		return float64(t.Hour())
	case "minute":
		return float64(t.Minute())
	case "second":
		return float64(t.Second())
	case "dow", "dayofweek":
		return float64(t.Weekday())
	case "doy", "dayofyear":
		return float64(t.YearDay())
	case "epoch":
		return float64(t.Unix())
	default:
		return nil
	}
}

func fnCurrentDate(args []any) any {
	return time.Now().Format("2006-01-02")
}

// --- Session / catalog information functions ---
//
// PostgreSQL clients (pgJDBC, DataGrip, psql, Superset) open a connection by
// asking who and where they are: current_user, current_schema,
// current_database. These are answered here rather than only in the pgwire
// introspection layer so that a query mixing them with real columns — or
// selecting three of them at once — executes as an ordinary query with an
// ordinary result shape.
//
// The values are server constants. ScalarFunc is func([]any) any and
// DefaultRegistry is process-global, so a scalar function cannot see the
// calling connection's identity; a per-session answer would need a
// context-carrying evaluation path that does not exist. The constants match
// what pgwire reports for an unauthenticated session.
const (
	// SessionUser is the user name reported by current_user / session_user /
	// user / current_role.
	SessionUser = "wadjet"
	// SessionCatalog is the database name reported by current_catalog /
	// current_database().
	SessionCatalog = "wadjet"
	// SessionSchema is the schema reported by current_schema.
	SessionSchema = "public"
	// ServerVersion is the answer to version(). PostgreSQL drivers parse the
	// leading "PostgreSQL <major>" to decide which protocol features and
	// catalog queries they may use, so the string keeps that prefix.
	ServerVersion = "PostgreSQL 15.0 (Wadjet analytical query engine)"
)

func fnVersion(args []any) any { return ServerVersion }

func fnCurrentUser(args []any) any { return SessionUser }

func fnCurrentCatalog(args []any) any { return SessionCatalog }

func fnCurrentSchema(args []any) any { return SessionSchema }

// fnCurrentSchemas mirrors PostgreSQL's current_schemas(include_implicit),
// which returns the search path as a text array. pgJDBC calls it during
// connection setup; the value is rendered in PostgreSQL's array text format.
func fnCurrentSchemas(args []any) any {
	includeImplicit := false
	if len(args) > 0 {
		switch v := args[0].(type) {
		case bool:
			includeImplicit = v
		case string:
			includeImplicit = strings.EqualFold(v, "true") || strings.EqualFold(v, "t")
		}
	}
	if includeImplicit {
		return "{pg_catalog," + SessionSchema + "}"
	}
	return "{" + SessionSchema + "}"
}

// fnDateDiff returns the number of whole days between two instants.
// Usage: date_diff(date1, date2) → integer
//
// The difference is taken in integer milliseconds, so no day count rounds
// through a float64, and a partial day truncates toward the PAST rather than
// toward zero — otherwise a negative difference would round the other way
// from a positive one, and the same pair of instants would answer differently
// on either side of the epoch.
func fnDateDiff(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t1, _, ok1 := parseDateArg(args[0])
	t2, _, ok2 := parseDateArg(args[1])
	if !ok1 || !ok2 {
		return nil
	}
	const msPerDay = 24 * 60 * 60 * 1000
	diff := t1.UnixMilli() - t2.UnixMilli()
	days := diff / msPerDay
	if diff < 0 && diff%msPerDay != 0 {
		days--
	}
	return float64(days)
}

// fnDateAdd adds days (or an interval) to a date or timestamp.
// Usage: date_add(date, days) → date string
//
//	date_add(date, interval) → date string
func fnDateAdd(args []any) any { return dateShift(args, false) }

// fnDateSub subtracts days (or an interval) from a date or timestamp.
// Usage: date_sub(date, days) → date string
//
//	date_sub(date, interval) → date string
func fnDateSub(args []any) any { return dateShift(args, true) }

// dateShift is the shared body of date_add / date_sub.
//
// A numeric second argument counts DAYS — what a DATE argument has always
// meant here, and what Spark/Hive date_add means; an INTERVAL keeps its own
// unit. The result preserves the input's time-of-day: before issue #322 both
// functions formatted the result "2006-01-02" unconditionally, so a TIMESTAMP
// argument silently lost its clock on the way out. A whole-day argument still
// renders as a calendar date.
//
// The interval branch is intervalShift, shared verbatim with `date ± INTERVAL`
// in BinOp.Eval so date_sub(d, INTERVAL '90' DAY) and d - INTERVAL '90' DAY
// cannot disagree (issue #332).
func dateShift(args []any, subtract bool) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	if iv, ok := args[1].(IntervalValue); ok {
		return intervalShift(args[0], iv, subtract)
	}
	t, dateOnly, ok := parseDateArg(args[0])
	if !ok {
		return nil
	}
	days := int(ToFloat64(args[1]))
	if subtract {
		days = -days
	}
	return formatDateResult(t.AddDate(0, 0, days), dateOnly)
}

// intervalShift applies an INTERVAL to a date-valued operand. It is the shared
// body of `date ± INTERVAL` (BinOp.Eval) and date_add / date_sub with an
// interval argument (dateShift) — one function, so the operator and the
// function family cannot answer the same question differently (issue #332).
//
// Two instant renderers already coexist, and this deliberately keeps it at two:
// a TEXT operand goes on rendering RFC3339 through dateAddInterval (TPC-H Q1's
// `DATE '1998-12-01' - INTERVAL '90' DAY` is a text operand and its output is
// pinned), while a resolved temporal COLUMN renders through formatDateResult —
// batch.FormatTimestamp, the engine's one timestamp renderer, which is what
// #322 settled for date_add over a TIMESTAMP column. A third renderer here
// would make the output format depend on how the value reached the operator.
func intervalShift(v any, iv IntervalValue, subtract bool) any {
	if ds, ok := v.(string); ok {
		return dateAddInterval(ds, iv, subtract)
	}
	t, dateOnly, ok := parseDateArg(v)
	if !ok {
		return nil
	}
	// An interval carrying a time component makes the result an instant even
	// when the input was a whole day.
	dateOnly = dateOnly && iv.Hours == 0 && iv.Minutes == 0 && iv.Seconds == 0
	return formatDateResult(addInterval(t, iv, subtract), dateOnly)
}

// parseDateArg resolves a date-arithmetic argument to the instant it denotes,
// and reports whether that instant is a whole DAY — which is what decides how
// the result renders (see formatDateResult).
//
// A temporal COLUMN arrives here already resolved, as a time.Time (TIMESTAMP)
// or a civilDate (DATE), because resolveTemporalArgs converts it while the
// declared column type is still in hand. So a bare number reaching this point
// is genuinely a bare number, and keeps the days-since-epoch reading it has
// always had in this family — unchanged by #319, and unchanged here.
func parseDateArg(v any) (time.Time, bool, bool) {
	switch tv := v.(type) {
	case civilDate:
		return tv.t, true, true
	case time.Time:
		return tv, false, true
	case string:
		// A bare "2006-01-02" is a whole day; any layout with a clock is
		// an instant whose clock the result keeps.
		if t, err := time.Parse("2006-01-02", tv); err == nil {
			return t, true, true
		}
		t := parseDateValue(tv)
		if t.IsZero() {
			return time.Time{}, false, false
		}
		return t, false, true
	}
	t := parseDateValue(v)
	if t.IsZero() {
		return time.Time{}, false, false
	}
	return t, true, true
}

// formatDateResult renders a date-arithmetic result. A whole day stays a
// calendar date; an instant goes through batch.FormatTimestamp, the engine's
// one timestamp renderer, so date_add over a TIMESTAMP column reads exactly
// the way the column itself does.
func formatDateResult(t time.Time, dateOnly bool) string {
	t = t.UTC()
	if dateOnly {
		return t.Format("2006-01-02")
	}
	return batch.FormatTimestamp(t.UnixMilli())
}

// fnToDate converts a date, a timestamp or a string to a calendar date.
func fnToDate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseDateValue(args[0])
	if t.IsZero() {
		return nil
	}
	return t.Format("2006-01-02")
}

// parseDateValue parses a date from various formats.
//
// A temporal column argument arrives already resolved to its instant (see
// resolveTemporalArgs), as a time.Time or — for a DATE column — a civilDate;
// a bare number is a bare number, and still reads as days since the epoch.
func parseDateValue(v any) time.Time {
	switch tv := v.(type) {
	case time.Time:
		return tv
	case civilDate:
		return tv.t
	case string:
		for _, layout := range []string{"2006-01-02", time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, tv); err == nil {
				return t
			}
		}
	case int32:
		// Days since epoch
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	case int64:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	case float64:
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(tv))
	}
	return time.Time{}
}

// fnUUIDVersion extracts the version from a UUID string.
func fnUUIDVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	raw := parseUUIDHex(s)
	if raw == nil {
		return nil
	}
	return float64(raw[6] >> 4)
}

// fnUUIDToString formats raw UUID bytes as a standard string.
func fnUUIDToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// Already formatted?
	if len(s) == 36 && s[8] == '-' {
		return s
	}
	raw := parseUUIDHex(s)
	if raw == nil {
		return nil
	}
	var buf [36]byte
	hex.Encode(buf[0:8], raw[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], raw[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], raw[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], raw[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], raw[10:16])
	return string(buf[:])
}

// parseUUIDHex parses a UUID string (with or without dashes) into 16 bytes.
func parseUUIDHex(s string) []byte {
	clean := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			clean = append(clean, s[i])
		}
	}
	if len(clean) != 32 {
		return nil
	}
	raw := make([]byte, 16)
	_, err := hex.Decode(raw, clean)
	if err != nil {
		return nil
	}
	return raw
}

// ipv4LitToInt64 parses a dotted-quad IPv4 literal into the int64 encoding
// TypeIPv4 columns use — batch.Vector.SetValue's TypeIPv4 case and
// writer.go's prepareRows agree on this: the address's big-endian uint32
// widened to int64. ok is false for anything that isn't a valid IPv4 text
// form (including an IPv6 literal — To4 refuses it).
func ipv4LitToInt64(s string) (int64, bool) {
	ip := net.ParseIP(s)
	if ip == nil {
		return 0, false
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return 0, false
	}
	return int64(binary.BigEndian.Uint32(ip4)), true
}

// macLitToInt64 parses a MAC literal into the int64 encoding TypeMAC columns
// use — batch.Vector.SetValue's TypeMAC case: the six bytes packed
// big-endian into the low 48 bits.
func macLitToInt64(s string) (int64, bool) {
	hw, err := net.ParseMAC(s)
	if err != nil || len(hw) != 6 {
		return 0, false
	}
	var n uint64
	for _, b := range hw {
		n = (n << 8) | uint64(b)
	}
	return int64(n), true
}

// --- Network function implementations ---

func fnIPToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	ip := net.ParseIP(s)
	if ip == nil {
		return nil
	}
	return ip.String()
}

func fnCIDRContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[1]))
	if ip == nil {
		return nil
	}
	return network.Contains(ip)
}

func fnIPVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip.To4() != nil {
		return float64(4)
	}
	return float64(6)
}

func fnMaskIP(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	octets := int(ToFloat64(args[1]))
	ip4 := ip.To4()
	if ip4 != nil {
		// Mask last N octets of IPv4
		if octets < 0 || octets > 4 {
			return nil
		}
		masked := make(net.IP, 4)
		copy(masked, ip4)
		for j := 0; j < octets && j < 4; j++ {
			masked[3-j] = 0
		}
		return masked.String()
	}
	return nil
}

func fnMACToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	hw, err := net.ParseMAC(s)
	if err != nil {
		return nil
	}
	return hw.String()
}

func fnIPSubnet(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	return network.IP.String()
}

func fnIPNetmask(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	mask := network.Mask
	// Convert mask to dotted notation for IPv4
	if len(mask) == 4 {
		return fmt.Sprintf("%d.%d.%d.%d", mask[0], mask[1], mask[2], mask[3])
	}
	// For IPv6, return hex representation
	return mask.String()
}

// --- Helpers ---

// toString converts any value to string, avoiding fmt.Sprint for common types.
func toString(v any) string {
	switch tv := v.(type) {
	case string:
		return tv
	case []byte:
		return string(tv)
	default:
		return fmt.Sprint(v)
	}
}

// ToFloat64 converts any numeric value to float64.
func ToFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int:
		return float64(tv)
	case int32:
		return float64(tv)
	case bool:
		if tv {
			return 1
		}
		return 0
	default:
		// Try string → float for decimal formatted strings like "123.45"
		if s, ok := v.(string); ok {
			var f float64
			if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
				return f
			}
		}
		return 0
	}
}

// ToInt64 converts any numeric value to int64.
func ToInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int:
		return int64(tv)
	case int32:
		return int64(tv)
	case float64:
		return int64(tv)
	case float32:
		return int64(tv)
	case string:
		return parseTimestampToEpochMs(tv)
	default:
		return 0
	}
}

// parseTemporalInt64OK converts a date/timestamp string to the same int64
// unit as the reference value, with an explicit success signal.
// TypeDate columns use epoch days (small int64 values), TypeTimestamp
// columns use epoch milliseconds (large int64 values). The threshold
// 500_000 (~year 3339 in days) safely distinguishes the two.
//
// The signal is the whole point at the comparison site. Its callers used to
// test the RESULT — `if bi := parseTemporalInt64(ai, bs); bi != 0 || ai == 0`
// — which reads "the string parsed, OR the number is zero", and the second
// half is true of ANY unparseable string once the numeric side is zero. So
// `0 = '0.0001'` was TRUE, and after the box-sniffing branch above it was
// deleted, every `int_col = 'anything'` comparison against the row holding
// ZERO went the same way: `k = '2'` matched k=0 as well as nothing else
// (#504 review, B1). A parse either happened or it did not; that is what the
// branch needs to know.
func parseTemporalInt64OK(ref int64, s string) (int64, bool) {
	if ref < 500_000 && ref > -500_000 {
		// Reference is epoch days — parse the string as days too.
		return parseDateToEpochDaysCachedOK(s)
	}
	return parseTimestampToEpochMsCachedOK(s)
}

// Deterministic temporal-string parsers are called row-by-row from
// Cmp.EvalBool whenever a date/timestamp column is compared against a string
// literal (e.g., `l_shipdate <= '1998-09-02'`). At SF100 the 22Q suite spent
// 4.24% of worker CPU (236s cum) parsing the SAME literal strings over and
// over — every row of every filter re-walked the layout list. SQL queries
// have a fixed, tiny set of date literals (TPC-H Q03/Q04/Q06/Q07/Q10/Q12/
// Q14/Q15/Q20: ~1-3 literals each; suite-wide < 20 distinct strings), so a
// memoization cache stays trivially small and never grows unbounded.
var (
	dateEpochDaysCache    sync.Map // map[string]temporalParseResult → epoch days
	timestampEpochMsCache sync.Map // map[string]temporalParseResult → epoch milliseconds
)

// temporalParseResult is a cached string→epoch conversion's outcome,
// including whether the parse succeeded — 0 is a valid epoch-days/epoch-ms
// result in its own right (the Unix epoch itself), so the cache cannot
// collapse "parsed to 0" and "did not parse" into the same cached value the
// way a bare int64 cache would.
type temporalParseResult struct {
	v  int64
	ok bool
}

// parseDateToEpochDaysCachedOK is parseDateToEpochDaysOK routed through
// dateEpochDaysCache, so parseTemporalInt64OK's row-by-row date comparisons
// serve a repeated literal from a map lookup instead of re-walking up to 4
// time.Parse layouts every row.
func parseDateToEpochDaysCachedOK(s string) (int64, bool) {
	if v, ok := dateEpochDaysCache.Load(s); ok {
		r := v.(temporalParseResult)
		return r.v, r.ok
	}
	days, ok := parseDateToEpochDaysOK(s)
	dateEpochDaysCache.Store(s, temporalParseResult{v: days, ok: ok})
	return days, ok
}

// parseDateToEpochDaysOK is the uncached parse with an explicit success
// signal (0 is a valid result for the epoch itself). Used at expression
// compile time by compileCmp's temporal-literal specialization, where it
// runs once per literal rather than once per row, and by
// parseDateToEpochDaysCachedOK on a cache miss.
//
// The day count is computed from t.Unix() (civil-days arithmetic), not
// t.Sub(epoch): Sub returns a time.Duration, which saturates at
// ±math.MaxInt64 ns (~292 years) rather than reporting an overflow, so a
// 4-digit-year date before 1678 or after 2262 previously answered a
// silently CLAMPED day count (#451, same mechanism as
// kernel.parseDateToDays). No range check is needed here: the result
// compares against a DATE column's int32-range value, and an int64 outside
// that range simply never equals one — the value has nowhere further to
// truncate to.
func parseDateToEpochDaysOK(s string) (int64, bool) {
	for _, layout := range []string{
		"2006-01-02",
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			const secondsPerDay = 86400
			sec := t.Unix()
			days := sec / secondsPerDay
			if sec%secondsPerDay < 0 {
				days--
			}
			return days, true
		}
	}
	return 0, false
}

// parseTimestampToEpochMs parses common timestamp string formats into epoch
// milliseconds, discarding the success signal ToInt64's callers have no use
// for (a failed parse and an epoch-zero result both read as 0 there, which
// is the existing contract for a non-temporal string reaching ToInt64).
func parseTimestampToEpochMs(s string) int64 {
	v, _ := parseTimestampToEpochMsCachedOK(s)
	return v
}

// parseTimestampToEpochMsCachedOK is parseTimestampToEpochMsOK routed
// through timestampEpochMsCache — the same cache ToInt64's
// parseTimestampToEpochMs already shares, so a literal parsed through either
// caller warms the other's lookup too, and there is exactly one cache of
// timestamp parses rather than one per caller.
func parseTimestampToEpochMsCachedOK(s string) (int64, bool) {
	if v, ok := timestampEpochMsCache.Load(s); ok {
		r := v.(temporalParseResult)
		return r.v, r.ok
	}
	ms, ok := parseTimestampToEpochMsOK(s)
	timestampEpochMsCache.Store(s, temporalParseResult{v: ms, ok: ok})
	return ms, ok
}

// parseTimestampToEpochMsOK is the uncached parse with an explicit success
// signal (see parseDateToEpochDaysOK).
func parseTimestampToEpochMsOK(s string) (int64, bool) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli(), true
		}
	}
	return 0, false
}

func toBool(e Expr, b *batch.RecordBatch, row int) bool {
	if be, ok := e.(BoolExpr); ok {
		return be.EvalBool(b, row)
	}
	v := e.Eval(b, row)
	return toBoolVal(v)
}

func toBoolVal(v any) bool {
	if v == nil {
		return false
	}
	switch tv := v.(type) {
	case bool:
		return tv
	case float64:
		return tv != 0
	case int64:
		return tv != 0
	case int:
		return tv != 0
	case string:
		return tv != ""
	default:
		return true
	}
}

// toInt64Safe converts a value to int64, returning false if not possible.
func toInt64Safe(v any) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int32:
		return int64(tv), true
	case int:
		return int64(tv), true
	default:
		return 0, false
	}
}

// toFloat64Safe converts a value to float64, returning false if not possible.
func toFloat64Safe(v any) (float64, bool) {
	switch tv := v.(type) {
	case float64:
		return tv, true
	case float32:
		return float64(tv), true
	case int64:
		return float64(tv), true
	case int32:
		return float64(tv), true
	default:
		return 0, false
	}
}

func compare(a, b any, op CmpOp) bool {
	// Fast path: both int64 (most common for column comparisons)
	if ai, ok := a.(int64); ok {
		if bi, ok := b.(int64); ok {
			switch op {
			case CmpEq:
				return ai == bi
			case CmpNe:
				return ai != bi
			case CmpLt:
				return ai < bi
			case CmpLe:
				return ai <= bi
			case CmpGt:
				return ai > bi
			case CmpGe:
				return ai >= bi
			}
		}
	}
	// Fast path: both float64. PostgreSQL's float order, like every other
	// float comparison in the tree (cmpFloat64Op above).
	if af, ok := a.(float64); ok {
		if bf, ok := b.(float64); ok {
			return cmpFloat64Op(af, bf, op)
		}
	}
	// Fast path: both string
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			switch op {
			case CmpEq:
				return as == bs
			case CmpNe:
				return as != bs
			case CmpLt:
				return as < bs
			case CmpLe:
				return as <= bs
			case CmpGt:
				return as > bs
			case CmpGe:
				return as >= bs
			}
		}
	}
	// A mixed number/text pair used to be read NUMERICALLY here whenever the
	// text PARSED as a number, so that a DECIMAL column — which boxes as its
	// rendered text (Vector.GetValue) — would order correctly against an
	// INT64 column (#476). Nothing in a box says which of the two it is,
	// though, so the same branch read a genuine STRING column's value
	// numerically too: `WHERE s = 1.5` matched the row holding "1.50" here
	// and matched nothing through the vectorized kernel, one predicate with
	// two answers (#504).
	//
	// The reading is a BINDING now, not a guess: expr.boxedPair resolves both
	// operands' DECLARED kinds and answers the DECIMAL pairs before anything
	// reaches this function. compare() is what remains for a pair with no
	// declaration to consult, and it treats a string as a string.
	//
	// Mixed int64/string: implicit date/timestamp casting.
	// TypeDate columns store epoch days (int32→int64), TypeTimestamp stores epoch ms.
	// Date strings ("YYYY-MM-DD") are exactly 10 chars with no time component.
	if ai, ok := a.(int64); ok {
		if bs, ok := b.(string); ok {
			if bi, ok := parseTemporalInt64OK(ai, bs); ok {
				switch op {
				case CmpEq:
					return ai == bi
				case CmpNe:
					return ai != bi
				case CmpLt:
					return ai < bi
				case CmpLe:
					return ai <= bi
				case CmpGt:
					return ai > bi
				case CmpGe:
					return ai >= bi
				}
			}
		}
	}
	if as, ok := a.(string); ok {
		if bi, ok := b.(int64); ok {
			if ai, ok := parseTemporalInt64OK(bi, as); ok {
				switch op {
				case CmpEq:
					return ai == bi
				case CmpNe:
					return ai != bi
				case CmpLt:
					return ai < bi
				case CmpLe:
					return ai <= bi
				case CmpGt:
					return ai > bi
				case CmpGe:
					return ai >= bi
				}
			}
		}
	}
	// Mixed numeric types — an INT64 literal against a FLOAT column, a
	// float32 box (CAST(x AS REAL)) against a float64 one, and every other
	// pair the two fast paths above do not catch.
	//
	// cmpFloat64Op, not Go's operators. The both-float64 fast path above
	// already answers in PostgreSQL's float total order (NaN greatest and
	// equal to itself, ADR-0012 item 8), and this branch did not — so
	// `f > 1` DROPPED the NaN rows while `f > 1.0` kept them, one predicate
	// with two answers decided by whether the literal was spelled with a
	// decimal point. It is also the ROW path, which is what the stage DAG
	// compiles every scan-pushed filter to, so the same query answered
	// differently distributed than in process. #459 closed this order for the
	// kernels, the keys and the both-float64 box pair; this arm was the one
	// escape left.
	if isNumeric(a) && isNumeric(b) {
		return cmpFloat64Op(ToFloat64(a), ToFloat64(b), op)
	}
	// Fall back to string comparison
	as := toString(a)
	bs := toString(b)
	switch op {
	case CmpEq:
		return as == bs
	case CmpNe:
		return as != bs
	case CmpLt:
		return as < bs
	case CmpLe:
		return as <= bs
	case CmpGt:
		return as > bs
	case CmpGe:
		return as >= bs
	}
	return false
}

func isNumeric(v any) bool {
	switch v.(type) {
	case float64, float32, int64, int, int32:
		return true
	default:
		return false
	}
}

func toTime(args []any) time.Time {
	if len(args) < 1 || args[0] == nil {
		return time.Time{}
	}
	return parseTime(args[0])
}

func parseTime(v any) time.Time {
	switch tv := v.(type) {
	case time.Time:
		return tv
	case int64:
		// UTC, matching the vectorized kernels (vecExtract/vecHour/…).
		// Local time made date_trunc/extract results depend on host TZ.
		return time.Unix(tv, 0).UTC()
	case float64:
		return time.Unix(int64(tv), 0).UTC()
	case string:
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, tv); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

// matchLike implements SQL LIKE pattern matching.
// % matches any sequence, _ matches a single character.
func matchLike(s, pattern string) bool {
	return matchLikeRecur(s, pattern, 0, 0)
}

func matchLikeRecur(s, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		switch pattern[pi] {
		case '%':
			pi++
			// Skip consecutive %
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi == len(pattern) {
				return true // trailing % matches everything
			}
			for i := si; i <= len(s); i++ {
				if matchLikeRecur(s, pattern, i, pi) {
					return true
				}
			}
			return false
		case '_':
			if si >= len(s) {
				return false
			}
			si++
			pi++
		default:
			if si >= len(s) || s[si] != pattern[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}

// --- Subquery expressions ---

// SubqueryRunner executes a SQL subquery and returns its result rows.
// Each row is a map of column name to value.
type SubqueryRunner func(sql string) ([]map[string]any, error)

// ScalarSubquery evaluates a subquery that returns a single scalar value.
// Example: WHERE price > (SELECT AVG(price) FROM products)
// Uncorrelated: executed once and result cached.
//
// The cache is shared by every parallel pipeline worker — one compiled
// expression tree is captured by all of them (Pipeline.runParallel) — so it
// is published the same way ColRef publishes its resolution: written under
// resolveMu, released by an atomic store, and never read before that store
// is observed. A plain `if !cached { cached = true; ... }` raced: a worker
// that saw the flag before the value was written compared against a nil
// threshold, dropped every row of its batches, and the query answered a
// different row count on every run (#398).
type ScalarSubquery struct {
	SQL    string
	Runner SubqueryRunner
	// Decl is the DECLARED type of the subquery's single output column, and
	// DeclKnown says whether anything resolved it (#696). It carries no value
	// and changes no evaluation: it exists so the boxed comparison can read
	// this operand as the number it IS.
	//
	// A DECIMAL boxes as its rendered TEXT, so without a declaration the pair
	// `a > (SELECT AVG(a) FROM decpair)` had a proven DECIMAL on one side and
	// an unclassifiable box on the other, fell through to compare()'s
	// LEXICOGRAPHIC rule, and answered 0 rows for PostgreSQL's 4 because
	// "12.75" sorts below "7.570000". DecPrecision/DecScale go with a DECIMAL
	// Decl for the same reason every other declaration carries them.
	Decl                   batch.TypeID
	DeclKnown              bool
	DecPrecision, DecScale int
	// resolved publishes val: stored last under resolveMu, and the only
	// thing an evaluating goroutine reads before using val.
	resolved  atomic.Bool
	resolveMu sync.Mutex
	val       any
}

func (e *ScalarSubquery) Eval(_ *batch.RecordBatch, _ int) any {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	return e.val
}

func (e *ScalarSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	defer e.resolved.Store(true)
	rows, err := e.Runner(e.SQL)
	if err != nil || len(rows) == 0 {
		e.val = nil
		return
	}
	// Return first column of first row
	for _, v := range rows[0] {
		e.val = v
		break
	}
}

// MemoryAccountant is the minimal per-task memory-budget hook InSubquery
// uses to charge its uncorrelated membership set (ADR-0006, #528). It is
// declared here rather than importing internal/engine/memory: expr has no
// other reason to depend on that package, and *memory.Tracker already has
// exactly this method set, so a caller that holds one satisfies this
// interface with no adapter — the seam CompileWithBudget threads through
// costs no new package dependency.
type MemoryAccountant interface {
	// Reserve charges n bytes against the budget, returning an error (which
	// InSubquery treats as a query error, never a silent no-op) if doing so
	// would exceed it.
	Reserve(n int64) error
	// Release returns n previously reserved bytes.
	Release(n int64)
}

// InSubquery checks if a value is in the result set of a subquery.
// Example: WHERE user_id IN (SELECT user_id FROM active_users)
// Uncorrelated: executed once and result set cached in a hash set for O(1) lookup.
type InSubquery struct {
	Expr   Expr
	SQL    string
	Runner SubqueryRunner
	Not    bool
	// Budget charges the membership set resolveSlow builds to the caller's
	// per-task memory tracker (ADR-0006, #528). nil (CompileWithRunner,
	// CompileWithScope, etc.) keeps the map unbudgeted, exactly as before
	// #528 — every shape that decorrelates into a semi join never reaches
	// this type at all (its build side is already budgeted and spillable);
	// only tryDecorrelateInSubquery's DECLINED shapes do, and only a
	// computed inner select item is unbounded (a LIMIT/OFFSET or an
	// ungrouped-aggregate inner item is bounded by construction).
	//
	// TODAY IT IS ALWAYS NIL IN PRODUCTION: CompileWithBudget, the only
	// entry point that sets it, has no non-test caller. Wiring it into
	// Planner.makeSubqueryRunner is #531 — and whoever does that MUST wire
	// Release with it. Nothing calls Release now (see its doc), which is
	// harmless only because nothing charges: the moment a real tracker is
	// threaded in, every uncorrelated IN-subquery in a task LEAKS its
	// charge for the task's lifetime, and a task that plans several of them
	// runs out of budget for work that has already finished. The compile
	// side is one line; the release side is a lifetime question about where
	// a compiled Expr tree is torn down, and it is the harder half.
	Budget MemoryAccountant
	// resolved publishes the set: stored last under resolveMu. Same
	// contract, and the same defect, as ScalarSubquery's (#398) — an
	// unsynchronized flag let a parallel worker probe a half-built map.
	resolved  atomic.Bool
	resolveMu sync.Mutex
	// setNull records a NULL in the subquery's result set: a probe that
	// misses such a set is UNKNOWN, not false — the `NOT IN (SELECT
	// nullable_col ...)` trap (#370).
	setNull bool
	// emptySet records that the subquery returned NOTHING — not one value,
	// not even a NULL. It is a distinct case from setNull and from a
	// populated set, because an empty set is the one where a NULL PROBE key
	// is decided rather than UNKNOWN (see EvalBoolNull).
	emptySet bool
	intSet   map[int64]struct{}
	strSet   map[string]struct{}
	// fltSet is keyed by kernel.KeyFloat64Bits, not by the raw float64: a Go
	// map can never find a NaN key (NaN != NaN), and PostgreSQL's float order
	// says NaN EQUALS itself and -0.0 equals +0.0 (ADR-0012 item 8). Keyed
	// raw, `real IN (SELECT double …)` dropped the NaN row that the same
	// predicate as a JOIN — whose key canonicalises the bits (ADR-0023
	// item 1) — matched. It is a key; it folds what the comparator folds.
	fltSet map[uint64]struct{}
	// decSet is strSet keyed by batch.CanonicalDecimalText instead of by the
	// raw rendering, and it is consulted only when the PROBE is declared
	// DECIMAL. A DECIMAL boxes as its text at its own scale, so
	// `COALESCE(a, b) IN (SELECT COALESCE(a, b) FROM t WHERE …)` compared
	// "12.75" against the set's "12.7500" and answered zero rows where
	// PostgreSQL answers four — the row-at-a-time twin of #474, and a
	// two-path split besides, since the stage DAG lowers the same predicate
	// to a semi join keyed through the columnar encoding and got it right.
	// The gate is the DECLARATION, never the box's shape: a genuine STRING
	// column holding numeric-looking text still compares AS TEXT (#504).
	decSet map[string]struct{}
	// setNumericKind records what the SET's members are, which is half of the
	// rung this predicate compares at (#615 F2). PostgreSQL resolves
	// `probe IN (SELECT …)` by the same OPERATOR ladder a join key uses —
	// int ⊕ numeric → numeric, anything ⊕ float8 → float8, real ⊕ int →
	// float8 — and this type only ever consulted the set that matched the
	// probe's own box. Every cross-rung pair therefore missed EVERY member:
	// `numeric IN (SELECT float8)` answered 0 where PostgreSQL answers 7,
	// and its NOT IN answered 7 where PostgreSQL answers 0 — inventing rows,
	// not just dropping them.
	setNumericKind inSetKind
	// probe caches the settled kind of e.Expr, the same way every other
	// declaration-driven comparison site caches its operands'.
	probe boxOperand
	vals  []any // fallback for mixed types
	// chargedBytes is exactly what was handed to Budget.Reserve, guarded by
	// resolveMu, so Release returns exactly that many bytes exactly once.
	chargedBytes int64
}

func (e *InSubquery) Eval(b *batch.RecordBatch, row int) any {
	return boolNullBox(e.EvalBoolNull(b, row))
}

func (e *InSubquery) EvalBool(b *batch.RecordBatch, row int) bool {
	v, null := e.EvalBoolNull(b, row)
	return v && !null
}

func (e *InSubquery) EvalBoolNull(b *batch.RecordBatch, row int) (bool, bool) {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	// An EMPTY set is a real answer and not an absence, and it is checked
	// BEFORE the probe's own NULL because the NULL-keyed row is exactly the
	// one the general rule gets wrong: `x IN ()` is FALSE for every row and
	// `x NOT IN ()` is TRUE for every row, the NULL-keyed one included,
	// because an empty set offers no comparison to be UNKNOWN about. This is
	// the same boundary the semi/anti lowering states (#507) and the
	// materialized IN-set renders as a constant (ADR-0021 §2); the subquery-
	// predicate route stated it nowhere and dropped the NULL-keyed row.
	if e.emptySet {
		return e.Not, false
	}
	lv := e.Expr.Eval(b, row)
	if lv == nil {
		return false, true
	}
	// The RUNG first: `x IN (SELECT y …)` is `x = y` quantified, so it is
	// resolved by PostgreSQL's operator ladder over (probe, set) — not by
	// whichever typed set happens to match the probe's Go box, which is what
	// this used to do and why every cross-rung pair missed every member
	// (#615 F2).
	if e.setNumericKind != inSetOther {
		_, probeIsInt := toInt64SafeStrict(lv)
		switch inSubqueryRung(e.probe.resolve(b), probeIsInt, e.setNumericKind) {
		case inSetDecimal:
			// EXACT, at the value's own digits: numeric ⊕ integer.
			if key, ok := inSubqueryDecimalKey(lv); ok && e.decSet != nil {
				if _, found := e.decSet[key]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		case inSetFloat:
			// float8, the numeric category's preferred type. A DECIMAL probe
			// reads through its exact text, which is `numeric::float8`.
			if fv, ok := inSubqueryFloat(lv); ok && e.fltSet != nil {
				if _, found := e.fltSet[kernel.KeyFloat64Bits(fv)]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		case inSetInt:
			if iv, ok := toInt64Safe(lv); ok && e.intSet != nil {
				if _, found := e.intSet[iv]; found {
					return !e.Not, false
				}
				return e.missAnswer()
			}
		}
	}
	// Fast path: typed hash lookup
	if e.intSet != nil {
		if iv, ok := toInt64Safe(lv); ok {
			if _, found := e.intSet[iv]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	if e.strSet != nil {
		if sv, ok := lv.(string); ok {
			// A DECIMAL probe keys by VALUE, not by rendering: see decSet.
			if e.decSet != nil && e.probe.resolve(b) == boxDecimal {
				if key, ok := batch.CanonicalDecimalText(sv); ok {
					if _, found := e.decSet[key]; found {
						return !e.Not, false
					}
					return e.missAnswer()
				}
			}
			if _, found := e.strSet[sv]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	if e.fltSet != nil {
		if fv, ok := toFloat64Safe(lv); ok {
			if _, found := e.fltSet[kernel.KeyFloat64Bits(fv)]; found {
				return !e.Not, false
			}
			return e.missAnswer()
		}
	}
	// Fallback: linear scan for mixed types
	for _, rv := range e.vals {
		if rv != nil && compare(lv, rv, CmpEq) {
			return !e.Not, false
		}
	}
	return e.missAnswer()
}

// resolveSlow runs the subquery once and builds the probe set. Idempotent.
// inSetKind is what an IN-subquery's materialised value set holds. It is the
// SET half of the operator ladder; boxOperand.resolve gives the probe half.
type inSetKind int8

const (
	inSetOther   inSetKind = iota
	inSetInt               // int64 members: an INTEGER in PostgreSQL's terms
	inSetFloat             // float64 members: float8
	inSetDecimal           // decimal TEXT members: numeric
)

// inSubqueryRung is the type this predicate compares at, by PostgreSQL's
// operator resolution — the same ladder physical.joinKeyCommonType applies to
// a join key, because `x IN (SELECT y …)` IS `x = y` quantified:
//
//	numeric ⊕ integer  -> numeric   (exact, at the value's own digits)
//	numeric ⊕ float8   -> float8
//	integer ⊕ float8   -> float8
//	real    ⊕ anything -> float8    (a real boxes as a float64 already)
//
// probeIsInt distinguishes the two boxNumber cases at the row: an int64 box
// against a decimal set is the exact rung, a float box against the same set is
// the float one.
func inSubqueryRung(probe boxKind, probeIsInt bool, set inSetKind) inSetKind {
	switch set {
	case inSetInt:
		switch {
		case probe == boxDecimal:
			return inSetDecimal
		case probe == boxNumber && !probeIsInt:
			return inSetFloat
		}
		return inSetInt
	case inSetDecimal:
		switch {
		case probe == boxDecimal:
			return inSetDecimal
		case probe == boxNumber && probeIsInt:
			return inSetDecimal
		case probe == boxNumber:
			return inSetFloat
		}
		return inSetOther
	case inSetFloat:
		if probe == boxDecimal || probe == boxNumber {
			return inSetFloat
		}
		return inSetOther
	}
	return inSetOther
}

// inSubqueryDecimalKey reads a probe box as a canonical DECIMAL key: a
// DECIMAL boxes as its rendered text, an integer as an int64, and the two are
// one numeric value to PostgreSQL.
func inSubqueryDecimalKey(lv any) (string, bool) {
	switch v := lv.(type) {
	case string:
		return batch.CanonicalDecimalText(v)
	default:
		if iv, ok := toInt64Safe(lv); ok {
			return batch.CanonicalDecimalText(strconv.FormatInt(iv, 10))
		}
		_ = v
	}
	return "", false
}

// inSubqueryFloat reads a probe box as the float64 PostgreSQL would compare
// at. A DECIMAL's text goes through ParseFloat, which is the correctly
// rounded `numeric::float8`.
func inSubqueryFloat(lv any) (float64, bool) {
	if sv, ok := lv.(string); ok {
		f, err := strconv.ParseFloat(sv, 64)
		return f, err == nil
	}
	return toFloat64Safe(lv)
}

// toInt64SafeStrict is toInt64Safe restricted to boxes that ARE integers, so
// a float64 box does not answer true for a whole-numbered value — the rung
// for `float8 IN (SELECT numeric)` is float8 whether or not the row happens
// to hold 2.0.
func toInt64SafeStrict(lv any) (int64, bool) {
	switch v := lv.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	}
	return 0, false
}

func (e *InSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	defer e.resolved.Store(true)
	// Bound here rather than at construction so every caller gets it: the
	// write is under resolveMu and published by the Store above, the same
	// release the value set itself rides.
	e.probe.expr = e.Expr
	rows, err := e.Runner(e.SQL)
	if err != nil {
		// An empty set: every probe misses, which is what the pre-#398
		// code answered on the failing call and on every call after it.
		e.emptySet = true
		return
	}
	// Collect values and detect predominant type for hash set
	var rawVals []any
	{
		for _, r := range rows {
			for _, v := range r {
				if v != nil {
					rawVals = append(rawVals, v)
				} else {
					e.setNull = true
				}
				break // first column only
			}
		}
		// Build typed hash set. Use toInt64Safe/toFloat64Safe to normalize
		// all integer types (int32, int64, int) and float types (float32, float64).
		if len(rawVals) > 0 {
			if _, ok := toInt64Safe(rawVals[0]); ok {
				// An INTEGER set, carried in all three spellings the ladder
				// can ask for: exactly (intSet), as canonical DECIMAL text
				// for a numeric probe, and as float64 for a float one. Every
				// conversion here is exact — an int64 that does not survive
				// float64 is the 2^53 case, and PostgreSQL rounds it too.
				e.setNumericKind = inSetInt
				e.intSet = make(map[int64]struct{}, len(rawVals))
				e.decSet = make(map[string]struct{}, len(rawVals))
				e.fltSet = make(map[uint64]struct{}, len(rawVals))
				for _, v := range rawVals {
					if iv, ok := toInt64Safe(v); ok {
						e.intSet[iv] = struct{}{}
						e.fltSet[kernel.KeyFloat64Bits(float64(iv))] = struct{}{}
						if key, ok := batch.CanonicalDecimalText(strconv.FormatInt(iv, 10)); ok {
							e.decSet[key] = struct{}{}
						}
					} else {
						e.vals = rawVals
						e.intSet, e.decSet, e.fltSet = nil, nil, nil
						e.setNumericKind = inSetOther
						break
					}
				}
			} else if _, ok := rawVals[0].(string); ok {
				e.strSet = make(map[string]struct{}, len(rawVals))
				e.decSet = make(map[string]struct{}, len(rawVals))
				allDecimal := true
				for _, v := range rawVals {
					if sv, ok := v.(string); ok {
						e.strSet[sv] = struct{}{}
						if key, ok := batch.CanonicalDecimalText(sv); ok {
							e.decSet[key] = struct{}{}
						} else {
							allDecimal = false
						}
					} else {
						e.vals = rawVals
						e.strSet, e.decSet = nil, nil
						allDecimal = false
						break
					}
				}
				// A set every one of whose members is decimal text IS a
				// numeric set, and a FLOAT probe compares against it at
				// float8. ParseFloat is the correctly-rounded reading, which
				// is what `numeric::float8` does.
				if allDecimal && e.strSet != nil {
					e.setNumericKind = inSetDecimal
					e.fltSet = make(map[uint64]struct{}, len(rawVals))
					for sv := range e.strSet {
						if fv, err := strconv.ParseFloat(sv, 64); err == nil {
							e.fltSet[kernel.KeyFloat64Bits(fv)] = struct{}{}
						}
					}
				}
			} else if _, ok := toFloat64Safe(rawVals[0]); ok {
				// A FLOAT set is float8 against everything (PostgreSQL's
				// preferred type of the numeric category), so it gets NO
				// decimal view: an exact comparison against it would be a
				// different predicate.
				e.setNumericKind = inSetFloat
				e.fltSet = make(map[uint64]struct{}, len(rawVals))
				for _, v := range rawVals {
					if fv, ok := toFloat64Safe(v); ok {
						e.fltSet[kernel.KeyFloat64Bits(fv)] = struct{}{}
					} else {
						e.vals = rawVals
						e.fltSet = nil
						e.setNumericKind = inSetOther
						break
					}
				}
			} else {
				e.vals = rawVals
			}
		}
		// Nothing at all came back — not a value and not a NULL.
		e.emptySet = len(rawVals) == 0 && !e.setNull
	}
	e.chargeMemory()
}

// chargeMemory reserves the membership set's estimated heap footprint
// against Budget (ADR-0006, #528). A nil Budget (every compile path except
// CompileWithBudget) is a no-op, exactly the pre-#528 behavior — this is
// what makes the fix opt-in rather than a change to every existing caller.
//
// Called once, from inside resolveSlow's already-held resolveMu, after the
// set this query holds has been decided, so the estimate matches exactly
// what stays reachable for the life of this InSubquery.
//
// AFTER, which bounds what this can do. The map is already built and already
// resident by the time a byte is charged, so a subquery large enough to
// exhaust the machine exhausts it before Reserve is ever called: this does
// not PREVENT that OOM, and #528's issue text describing it as doing so is
// wrong. What it does do is make the set VISIBLE to the task's budget — it
// counts against every later allocation the task makes, and a set that is
// over budget on its own turns into a query error instead of a permanently
// unaccounted resident map that every other operator then has to fit
// alongside. That is worth having and is what ADR-0006 asks of a structure
// that cannot spill; it is not the same claim.
//
// Charging as the set is BUILT — a Reserve per N rows inside resolveSlow's
// accumulation loop, refusing partway — is what would actually bound peak
// resident bytes. It needs resolveSlow to have somewhere to put a partial
// failure and a caller that can act on one, so it is a change to this
// type's contract rather than to this function. Not attempted here.
func (e *InSubquery) chargeMemory() {
	if e.Budget == nil {
		return
	}
	n := inSubqueryMemBytes(e.intSet, e.strSet, e.fltSet, e.vals)
	if n <= 0 {
		return
	}
	if err := e.Budget.Reserve(n); err != nil {
		// A giant uncorrelated IN-subquery is exactly the shape ADR-0006
		// exists to refuse rather than let grow an unbudgeted map until the
		// process OOMs (#528) — raised the same way every other expr-level
		// query error is (fatal.go), since EvalBoolNull has no error return
		// to propagate one through.
		panic(fatalEval{fmt.Errorf("IN subquery membership set: %w", err)})
	}
	e.chargedBytes = n
}

// Release returns any bytes charged to Budget in resolveSlow, and is
// idempotent — a caller that does not know whether resolveSlow ever ran, or
// has already called Release, may call it any number of times.
//
// It has NO CALLER outside this package's tests. That is not an oversight
// left to be tidied later: it is the unfinished half of #531. While Budget
// is always nil in production both halves are no-ops and nothing is wrong,
// but wiring CompileWithBudget without also finding a teardown point for
// this call converts an unbudgeted map into a permanently-charged one, which
// is a worse failure than the one #528 set out to fix — the bytes are
// returned to the OS by GC and never returned to the tracker. See Budget's
// doc for where that seam is.
func (e *InSubquery) Release() {
	if e.Budget == nil {
		return
	}
	e.resolveMu.Lock()
	n := e.chargedBytes
	e.chargedBytes = 0
	e.resolveMu.Unlock()
	if n > 0 {
		e.Budget.Release(n)
	}
}

// inSubqueryMapEntryOverhead approximates a Go map's per-entry bookkeeping:
// a bucket holds up to 8 (key, value) pairs plus one tophash byte and an
// amortized share of an overflow-bucket pointer per entry, which rounds up
// to about this many bytes on top of the key/value payload itself. This
// does not need to be exact — Reserve's job is to catch an unbounded
// subquery before it outgrows the task's budget, not to account every byte
// precisely — and rounding up is the safe direction for a budget check.
const inSubqueryMapEntryOverhead = 16

// inSubqueryMemBytes estimates the heap footprint of whichever membership
// set resolveSlow built — at most one of intSet/strSet/fltSet/vals is
// non-nil, matching resolveSlow's mutually exclusive construction.
func inSubqueryMemBytes(intSet map[int64]struct{}, strSet map[string]struct{}, fltSet map[uint64]struct{}, vals []any) int64 {
	switch {
	case intSet != nil:
		return int64(len(intSet)) * (8 + inSubqueryMapEntryOverhead)
	case fltSet != nil:
		return int64(len(fltSet)) * (8 + inSubqueryMapEntryOverhead)
	case strSet != nil:
		var n int64
		for k := range strSet {
			n += int64(len(k)) + 16 /* string header */ + inSubqueryMapEntryOverhead
		}
		return n
	case len(vals) > 0:
		var n int64
		for _, v := range vals {
			n += 16 // interface header (type word + data word)
			if s, ok := v.(string); ok {
				n += int64(len(s))
			} else {
				n += 8 // scalar payload estimate
			}
		}
		return n
	}
	return 0
}

// missAnswer is the IN answer for a probe the set does not contain: FALSE
// (or TRUE under NOT) for a NULL-free set, UNKNOWN when the set held a NULL.
func (e *InSubquery) missAnswer() (bool, bool) {
	if e.setNull {
		return false, true
	}
	return e.Not, false
}

// ExistsSubquery evaluates to true if a subquery returns any rows.
// Example: WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
// Uncorrelated: executed once and result cached.
type ExistsSubquery struct {
	SQL    string
	Runner SubqueryRunner
	Not    bool
	// resolved publishes exists: stored last under resolveMu. Same
	// contract, and the same defect, as ScalarSubquery's (#398).
	resolved  atomic.Bool
	resolveMu sync.Mutex
	exists    bool
}

func (e *ExistsSubquery) Eval(b *batch.RecordBatch, row int) any {
	return e.EvalBool(b, row)
}

func (e *ExistsSubquery) EvalBool(_ *batch.RecordBatch, _ int) bool {
	if !e.resolved.Load() {
		e.resolveSlow()
	}
	if e.Not {
		return !e.exists
	}
	return e.exists
}

func (e *ExistsSubquery) resolveSlow() {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved.Load() {
		return
	}
	rows, err := e.Runner(e.SQL)
	e.exists = err == nil && len(rows) > 0
	e.resolved.Store(true)
}

// Cast wraps an expression with explicit type conversion.
type Cast struct {
	Operand  Expr
	DestType string // "int", "float", "string", "date", "timestamp"

	// boolSrc caches the OPERAND's declared type for a cast to BOOLEAN, which
	// selects the conversion rule (cast_bool.go). Zero means "not resolved
	// yet"; nothing else in this node needs it, and no other destination
	// reads it.
	boolSrc atomic.Int32
	// decDest caches the parsed DECIMAL destination, for the same reason:
	// `DECIMAL(10, 2)` is fixed for the query and re-parsing the type name
	// per row cost a string walk on every value (cast_decimal.go).
	decDest castDecimalState
}

func (e *Cast) Eval(b *batch.RecordBatch, row int) any {
	v := e.Operand.Eval(b, row)
	if v == nil {
		return nil
	}
	// One normalization for both the temporal check and the switch: this
	// runs per row, and a WHERE over a typed date literal evaluates it once
	// per row of the scan.
	dest := strings.ToLower(e.DestType)
	if k := castTemporalKindLower(strings.TrimSpace(dest)); k != castNotTemporal {
		return castTemporal(b, row, e.Operand, v, k)
	}
	// A DECIMAL destination is resolved before the switch because its type
	// name CARRIES its parameters — `decimal(10, 2)` matches no case label,
	// and used to reach `default: return v`, which passed the value through
	// with the (p,s) silently ignored (ADR-0024 item 3, #555).
	if d, ok := e.decimalDestination(); ok {
		return e.castToDecimal(b, row, v, d)
	}
	switch dest {
	case "int", "integer", "int4", "bigint", "int8", "signed", "smallint", "int2":
		// A string that does not read as a number is refused, not coerced to
		// 0: PostgreSQL raises 22P02 invalid_text_representation and ADR-0012
		// makes it the authority on error-versus-not. The per-row error
		// channel #340 lacked exists now — FatalEvalPanic, #347 — which is
		// what this raise rides.
		//
		// A value that DOES read as a number then follows PostgreSQL's other
		// rule (#373): a fractional cast to an integer type ROUNDS, half away
		// from zero. TRUNC() is how a caller asks for truncation. An
		// already-integral value passes through untouched.
		// An exact DECIMAL is read on its own carrier, not through a double:
		// strconv.ParseFloat loses every digit past the sixteenth, so a
		// DECIMAL(38,10) holding 493827160549382.7160549350 came back as the
		// nearest double's integer part, and a value past the destination's
		// range came back as whatever the float conversion produced instead
		// of the refusal PostgreSQL gives (ADR-0024 item 4).
		if i, ok := castDecimalToInt(v, dest); ok {
			return i
		}
		if s, ok := stringOperand(v); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				typ := "integer"
				if dest == "bigint" {
					typ = "bigint"
				}
				raiseInvalidTextRepresentation(typ, s)
			}
			return castIntInRange(int64(math.Round(f)), dest)
		}
		// EVERY source gets the destination's range, integers included:
		// `CAST(99999 AS SMALLINT)` answered 99999 because an integer box
		// returned before the check, and PostgreSQL raises `smallint out of
		// range` for it (#555 review).
		if i, ok := toInt64Safe(v); ok {
			return castIntInRange(i, dest)
		}
		return castIntInRange(int64(math.Round(ToFloat64(v))), dest)
	case "real", "float4":
		// REAL is float4, a NARROWER type than the float64 every other
		// numeric box in this engine carries — and this arm used to sit
		// beside "float"/"double" and answer ToFloat64, so `CAST(x AS REAL)`
		// was a NO-OP. PostgreSQL types the result float4 and rounds the
		// value to it, which changes the answer of anything that compares it:
		//
		//	r_val = CAST(3.1 AS REAL)  ->  Filter: (r_val = '3.1'::real) -> the row
		//	CAST(1.0/3 AS REAL)        ->  0.33333334, not 0.3333333333333333
		//
		// FLOAT is deliberately NOT here. PostgreSQL's bare `float` is
		// `double precision` (float(1..24) is real, float(25..53) is double,
		// and an unqualified FLOAT is the latter) — verified with pg_typeof —
		// so only the two spellings that really name float4 narrow.
		f := ToFloat64(v)
		// PostgreSQL refuses a conversion that loses the value outright rather
		// than answering an infinity or a zero (float.c's overflow and
		// underflow checks, both SQLSTATE 22003). A value that is ALREADY
		// infinite, or already zero, is representable and passes through.
		// kernel.Float32FitOf is the one place that rule lives — the IN-list
		// refusals read it too, so `CAST(x AS REAL)` and `x IN (lit)` cannot
		// disagree about what a real can hold.
		if fit := kernel.Float32FitOf(f); fit != kernel.Float32Fits {
			raiseRealConversionError(e.Operand, f, fit)
		}
		return float32(f)
	case "float", "double", "float8", "double precision", "float64":
		return ToFloat64(v)
	case "bool", "boolean":
		// The conversion the operand's DECLARATION selects, not the one its
		// Go box suggests — see cast_bool.go for what each source type
		// answers and why the box cannot decide it.
		return e.castToBool(b, v)
	case "char", "varchar", "text", "string":
		// A BYTES operand boxes as a raw []byte — both here and from
		// GetValue, since ColRef.Eval has no divergent fast path for
		// TypeBytes the way it does for the four types boxedTextOperand
		// resolves — and PostgreSQL's `bytea::text` is `\x` followed by
		// LOWERCASE hex, under the default bytea_output = hex. That is the
		// rendering, per ADR-0012 item 1: PostgreSQL gives BYTES a printed
		// form, so wadjet does not invent a second one.
		//
		// Two earlier answers were both wrong. fmt.Sprint's default verb
		// printed Go's slice-of-decimal-bytes debug notation
		// ("[98 121 116 ...]"), and the raw bytes as a Go string — which
		// agreed with kernel.likeTextRenderer but not with PostgreSQL —
		// produced, for 0xff 0xfe 0x00 0x41, a string that is invalid UTF-8
		// and holds an embedded NUL. No PostgreSQL server can put a NUL in
		// a text-format DataRow field, and libpq TRUNCATES at one, so the
		// same query answered four bytes to pgx and two to psql. The hex
		// form is pure ASCII and has neither problem (#570).
		//
		// LIKE deliberately does NOT follow it here: PostgreSQL's `~~` over
		// bytea is BYTEWISE (verified live — `'\xfffe0041'::bytea LIKE
		// '%A%'` is true, matching the 0x41 byte, not the letter in a hex
		// spelling), so kernel.likeTextRenderer keeps matching the raw
		// bytes. The two disagree in PostgreSQL, so they disagree here.
		text := boxedTextOperand(b, row, e.Operand, v)
		if raw, ok := text.([]byte); ok {
			return `\x` + hex.EncodeToString(raw)
		}
		if s, ok := stringOperand(text); ok {
			return s
		}
		return fmt.Sprint(text)
	default:
		return v
	}
}

// boxedTextOperand renders a bare-column operand as the text the column's
// own value PRINTS as — which is, by construction, the text the vectorized
// kernel matches/renders against (kernel.likeTextRenderer's default arm is
// fmt.Sprint(Vector.GetValue(i)), and its per-type arms were written to agree
// with that rendering; CAST AS STRING's other arms and every scalar function
// argument already use the same GetValue rendering for every OTHER type,
// via ColRef.Eval's own default case). Mirrors temporalOperand's contract:
// only a bare column reference is resolved, and every other operand shape
// (an already-string value, a nested expression, a literal) passes v through
// unchanged.
//
// ColRef.Eval boxes four types differently from GetValue, for speed on the
// numeric paths that dominate it, and all four made a caller here match or
// render a DIFFERENT STRING from the one the scan's kernel or the plain
// projection would — the same query answering two ways depending on which
// evaluator reached the column:
//
//	IPv4, MAC  the raw encoded int64, so `ipv4_col LIKE '10.%'` matched the
//	           digits of that integer instead of the address text, and
//	           `CAST(ipv4_col AS STRING)` stringified the number
//	DATE       the epoch DAY, so `c_date LIKE '20%'` was false for
//	           2011-02-02 and true for the day number 20123, and
//	           `CAST(c_date AS STRING)` answered "15007" instead of the date
//	FLOAT32    widened to float64, so 1/7 printed 0.1428571492433548 here and
//	           0.14285715 through the kernel or a bare projection
//
// The IPv4/MAC LIKE pair was fixed with #497; DATE and FLOAT32's LIKE
// rendering were found by the review of it. This function used to be two
// near-identical copies — likeOperand (LIKE's call site, all four types) and
// networkOperand (Cast's, IPv4/MAC only) — which is exactly the two-
// implementation drift ADR-0012 keeps calling out elsewhere (CidrSortKey,
// appendColumnValue): CAST(date_col AS STRING) and CAST(f32_col AS STRING)
// were still wrong through networkOperand's narrower list (#521) after LIKE
// had already been fixed for the identical types. One function, every
// caller, closes both at once: `wadjet.TestLikeAnswersTheSameAtBothSites`
// sweeps every flat type through the LIKE call site so a fifth type that
// starts boxing differently is a failing test rather than another quiet
// divergence.
func boxedTextOperand(b *batch.RecordBatch, row int, operand Expr, v any) any {
	cr, ok := operand.(*ColRef)
	if !ok {
		return v
	}
	// A ROW FIELD PATH boxes exactly as a column of the field's type does
	// (ColRef.fieldValue), so it needs the same undoing — and gets it from
	// the same list, keyed on the FIELD's type (#568).
	switch cr.valueType() {
	case batch.TypeIPv4, batch.TypeMAC, batch.TypeDate, batch.TypeFloat32:
	default:
		return v
	}
	dv, ok := cr.displayValue(b, row)
	if !ok {
		return v
	}
	return dv
}

// stringOperand reports v's text when it is a string or byte slice — the two
// shapes a text column or literal reaches Cast.Eval in.
func stringOperand(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}

// --- String: regex and parsing ---

func fnSplitPart(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	parts := strings.Split(toString(args[0]), toString(args[1]))
	idx := int(ToFloat64(args[2])) - 1 // SQL is 1-indexed
	if idx < 0 || idx >= len(parts) {
		return ""
	}
	return parts[idx]
}

func fnStrPos(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	pos := strings.Index(toString(args[0]), toString(args[1]))
	if pos < 0 {
		return int32(0)
	}
	return int32(pos + 1) // 1-based
}

func fnRegexpLike(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	matched, err := regexp.MatchString(toString(args[1]), toString(args[0]))
	if err != nil {
		return nil
	}
	return matched
}

func fnRegexpExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(toString(args[1]))
	if re == nil {
		return nil
	}
	group := 0
	if len(args) >= 3 && args[2] != nil {
		group = int(ToFloat64(args[2]))
	}
	matches := re.FindStringSubmatch(toString(args[0]))
	if matches == nil || group >= len(matches) {
		return nil
	}
	return matches[group]
}

// regexpCache caches compiled patterns process-wide. Scalar regexp
// functions previously called regexp.Compile PER ROW — ClickBench Q29
// (REGEXP_REPLACE over 100M Referers) spent its 117s recompiling one
// pattern 100M times. sync.Map: read-mostly, a handful of distinct
// patterns per workload.
var regexpCache sync.Map // pattern string → *regexp.Regexp (nil for invalid)

func compileRegexpCached(pattern string) *regexp.Regexp {
	if v, ok := regexpCache.Load(pattern); ok {
		re, _ := v.(*regexp.Regexp)
		return re
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		re = nil
	}
	regexpCache.Store(pattern, re)
	return re
}

func fnRegexpReplace(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	re := compileRegexpCached(toString(args[1]))
	if re == nil {
		return nil
	}
	return re.ReplaceAllString(toString(args[0]), sqlBackrefsToGo(toString(args[2])))
}

// sqlBackrefsToGo converts SQL-style backreferences (\1 … \9, the
// POSIX/DuckDB/Postgres convention) in a replacement string to Go's ${N}
// form. \\ stays a literal backslash escape for a following digit.
func sqlBackrefsToGo(repl string) string {
	if !strings.ContainsRune(repl, '\\') {
		return repl
	}
	var b strings.Builder
	b.Grow(len(repl) + 4)
	for i := 0; i < len(repl); i++ {
		c := repl[i]
		if c == '\\' && i+1 < len(repl) {
			next := repl[i+1]
			if next >= '1' && next <= '9' {
				b.WriteString("${")
				b.WriteByte(next)
				b.WriteString("}")
				i++
				continue
			}
			if next == '\\' {
				b.WriteByte('\\')
				i++
				continue
			}
		}
		// Go's Expand treats $ specially — escape literal dollars.
		if c == '$' {
			b.WriteString("$$")
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// --- Encoding functions ---

func fnToHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return fmt.Sprintf("%x", int64(ToFloat64(args[0])))
}

func fnFromHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	var n int64
	_, err := fmt.Sscanf(toString(args[0]), "%x", &n)
	if err != nil {
		return nil
	}
	return float64(n)
}

func fnToBase64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return base64.StdEncoding.EncodeToString([]byte(toString(args[0])))
}

func fnFromBase64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(toString(args[0]))
	if err != nil {
		return nil
	}
	return string(decoded)
}

// --- Date/time conversion functions ---

func fnFromUnixtime(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	epoch := int64(ToFloat64(args[0]))
	return time.Unix(epoch, 0).UTC().Format(time.RFC3339)
}

func fnToUnixtime(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	return float64(t.Unix())
}

func fnDateFormat(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	return t.Format(sqlFormatToGo(toString(args[1])))
}

func fnDateParse(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t, err := time.Parse(sqlFormatToGo(toString(args[1])), toString(args[0]))
	if err != nil {
		return nil
	}
	return t.Format(time.RFC3339)
}

// sqlFormatToGo converts SQL date format specifiers to Go time layout.
func sqlFormatToGo(format string) string {
	r := strings.NewReplacer(
		"%Y", "2006",
		"%m", "01",
		"%d", "02",
		"%H", "15",
		"%i", "04",
		"%s", "05",
		"%S", "05",
		"%M", "January",
		"%b", "Jan",
		"%W", "Monday",
		"%a", "Mon",
		"%p", "PM",
		"%T", "15:04:05",
	)
	return r.Replace(format)
}

// --- Hash functions ---

func fnMD5(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := md5.Sum([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnSHA256(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha256.Sum256([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnSHA512(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha512.Sum512([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

// --- Bitwise functions ---

func fnBitwiseAnd(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) & int64(ToFloat64(args[1])))
}

func fnBitwiseOr(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) | int64(ToFloat64(args[1])))
}

func fnBitwiseXor(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) ^ int64(ToFloat64(args[1])))
}

func fnBitwiseNot(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return float64(^int64(ToFloat64(args[0])))
}

// --- String: padding and character ---

func fnLPad(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	n := int(ToFloat64(args[1]))
	pad := " "
	if len(args) >= 3 && args[2] != nil {
		pad = toString(args[2])
	}
	if len(pad) == 0 || n <= len(s) {
		if n < 0 {
			return ""
		}
		if n <= len(s) {
			return s[:n]
		}
		return s
	}
	var sb strings.Builder
	for sb.Len()+len(s) < n {
		sb.WriteString(pad)
	}
	result := sb.String()
	need := n - len(s)
	if len(result) > need {
		result = result[:need]
	}
	return result + s
}

func fnRPad(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	n := int(ToFloat64(args[1]))
	pad := " "
	if len(args) >= 3 && args[2] != nil {
		pad = toString(args[2])
	}
	if len(pad) == 0 || n <= len(s) {
		if n < 0 {
			return ""
		}
		if n <= len(s) {
			return s[:n]
		}
		return s
	}
	var sb strings.Builder
	sb.WriteString(s)
	for sb.Len() < n {
		sb.WriteString(pad)
	}
	result := sb.String()
	if len(result) > n {
		result = result[:n]
	}
	return result
}

func fnChr(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	code := int(ToFloat64(args[0]))
	if code < 0 || code > 0x10FFFF {
		return nil
	}
	return string(rune(code))
}

func fnCodepoint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if len(s) == 0 {
		return nil
	}
	runes := []rune(s)
	return int32(runes[0])
}

func fnConcatWS(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	sep := toString(args[0])
	parts := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		if a == nil {
			continue // skip nulls
		}
		parts = append(parts, toString(a))
	}
	return strings.Join(parts, sep)
}

func fnCharLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return int32(len([]rune(toString(args[0]))))
}

func fnTranslate(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	s := toString(args[0])
	from := []rune(toString(args[1]))
	to := []rune(toString(args[2]))
	mapping := make(map[rune]rune)
	for i, r := range from {
		if i < len(to) {
			mapping[r] = to[i]
		} else {
			mapping[r] = -1 // mark for deletion
		}
	}
	var sb strings.Builder
	for _, r := range s {
		if repl, ok := mapping[r]; ok {
			if repl >= 0 {
				sb.WriteRune(repl)
			}
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// --- Math: trigonometry ---

func fnPi(args []any) any {
	return math.Pi
}

func fnDegrees(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0]) * 180.0 / math.Pi
}

func fnRadians(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0]) * math.Pi / 180.0
}

func fnSin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Sin(ToFloat64(args[0]))
}

func fnCos(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Cos(ToFloat64(args[0]))
}

func fnTan(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Tan(ToFloat64(args[0]))
}

func fnAsin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v < -1 || v > 1 {
		return nil
	}
	return math.Asin(v)
}

func fnAcos(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v < -1 || v > 1 {
		return nil
	}
	return math.Acos(v)
}

func fnAtan(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Atan(ToFloat64(args[0]))
}

func fnAtan2(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return math.Atan2(ToFloat64(args[0]), ToFloat64(args[1]))
}

func fnCbrt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Cbrt(ToFloat64(args[0]))
}

func fnLog2(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v <= 0 {
		return nil
	}
	return math.Log2(v)
}

func fnTruncate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	decimals := 0
	if len(args) >= 2 && args[1] != nil {
		decimals = int(ToFloat64(args[1]))
	}
	pow := math.Pow(10, float64(decimals))
	return math.Trunc(v*pow) / pow
}

func fnRandom(args []any) any {
	return rand.Float64()
}

// --- JSON functions ---

func fnJSONExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	jsonStr := toString(args[0])
	path := toString(args[1])
	result := jsonPathExtract(jsonStr, path)
	if result == nil {
		return nil
	}
	// Return as JSON string for non-scalar values
	switch v := result.(type) {
	case string:
		return v
	case float64:
		return v
	case bool:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return string(b)
	}
}

func fnJSONExtractScalar(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	result := jsonPathExtract(toString(args[0]), toString(args[1]))
	if result == nil {
		return nil
	}
	switch v := result.(type) {
	case string:
		return v
	case float64:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return nil // non-scalar
	}
}

func fnJSONArrayLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	var arr []any
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return nil
	}
	return float64(len(arr))
}

func fnJSONValid(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return json.Valid([]byte(toString(args[0])))
}

// jsonPathExtract extracts a value from a JSON string using a simple dot-path.
// Supports: $.key, $.key.nested, $.key[0], $.key[0].nested
func jsonPathExtract(jsonStr, path string) any {
	var data any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil
	}
	// Strip leading "$." or "$"
	if strings.HasPrefix(path, "$.") {
		path = path[2:]
	} else if strings.HasPrefix(path, "$") {
		path = path[1:]
	}
	if path == "" {
		return data
	}
	return navigateJSON(data, path)
}

func navigateJSON(data any, path string) any {
	current := data
	for path != "" {
		// Parse next segment
		var segment string
		dotIdx := strings.IndexAny(path, ".[")
		if dotIdx < 0 {
			segment = path
			path = ""
		} else if path[dotIdx] == '.' {
			segment = path[:dotIdx]
			path = path[dotIdx+1:]
		} else {
			// '[' found
			segment = path[:dotIdx]
			path = path[dotIdx:]
		}

		if segment != "" {
			obj, ok := current.(map[string]any)
			if !ok {
				return nil
			}
			current = obj[segment]
			if current == nil {
				return nil
			}
		}

		// Handle array index [N]
		if strings.HasPrefix(path, "[") {
			end := strings.Index(path, "]")
			if end < 0 {
				return nil
			}
			idxStr := path[1:end]
			path = path[end+1:]
			if strings.HasPrefix(path, ".") {
				path = path[1:]
			}
			var idx int
			if _, err := fmt.Sscanf(idxStr, "%d", &idx); err != nil {
				return nil
			}
			arr, ok := current.([]any)
			if !ok || idx < 0 || idx >= len(arr) {
				return nil
			}
			current = arr[idx]
		}
	}
	return current
}

// --- URL functions ---

func fnURLExtractHost(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Hostname()
}

func fnURLExtractPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil || u.Port() == "" {
		return nil
	}
	var port int
	if _, err := fmt.Sscanf(u.Port(), "%d", &port); err != nil {
		return nil
	}
	return float64(port)
}

func fnURLExtractPath(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Path
}

func fnURLExtractProtocol(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Scheme
}

func fnURLExtractQuery(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.RawQuery
}

func fnURLExtractParameter(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Query().Get(toString(args[1]))
}

// --- Type introspection ---

func fnTypeof(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return "null"
	}
	switch args[0].(type) {
	case int64:
		return "bigint"
	case int32:
		return "integer"
	case int:
		return "integer"
	case float64:
		return "double"
	case float32:
		return "real"
	case string:
		return "varchar"
	case bool:
		return "boolean"
	case []byte:
		return "varbinary"
	default:
		return fmt.Sprintf("%T", args[0])
	}
}

// --- String: distance and utility ---

func fnSoundex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.ToUpper(toString(args[0]))
	if len(s) == 0 {
		return ""
	}
	result := make([]byte, 4)
	result[0] = s[0]
	codes := map[byte]byte{
		'B': '1', 'F': '1', 'P': '1', 'V': '1',
		'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
		'D': '3', 'T': '3',
		'L': '4',
		'M': '5', 'N': '5',
		'R': '6',
	}
	idx := 1
	lastCode := codes[s[0]]
	for i := 1; i < len(s) && idx < 4; i++ {
		code, ok := codes[s[i]]
		if ok && code != lastCode {
			result[idx] = code
			idx++
			lastCode = code
		} else if !ok {
			lastCode = 0
		}
	}
	for idx < 4 {
		result[idx] = '0'
		idx++
	}
	return string(result)
}

func fnLevenshtein(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := []rune(toString(args[0]))
	t := []rune(toString(args[1]))
	m, n := len(s), len(t)
	if m == 0 {
		return float64(n)
	}
	if n == 0 {
		return float64(m)
	}
	prev := make([]int, n+1)
	curr := make([]int, n+1)
	for j := 0; j <= n; j++ {
		prev[j] = j
	}
	for i := 1; i <= m; i++ {
		curr[0] = i
		for j := 1; j <= n; j++ {
			cost := 1
			if s[i-1] == t[j-1] {
				cost = 0
			}
			ins := curr[j-1] + 1
			del := prev[j] + 1
			sub := prev[j-1] + cost
			min := ins
			if del < min {
				min = del
			}
			if sub < min {
				min = sub
			}
			curr[j] = min
		}
		prev, curr = curr, prev
	}
	return float64(prev[n])
}

func fnHamming(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	a := toString(args[0])
	b := toString(args[1])
	if len(a) != len(b) {
		return nil
	}
	dist := 0
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			dist++
		}
	}
	return float64(dist)
}

func fnNormalize(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// NFC normalization: collapse combining characters
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsPrint(r) {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

func fnFormat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	format := toString(args[0])
	fmtArgs := make([]any, 0, len(args)-1)
	for _, a := range args[1:] {
		fmtArgs = append(fmtArgs, a)
	}
	return fmt.Sprintf(format, fmtArgs...)
}

func fnToUTF8(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return []byte(toString(args[0]))
}

func fnFromUTF8(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	switch v := args[0].(type) {
	case []byte:
		if utf8.Valid(v) {
			return string(v)
		}
		return nil
	case string:
		return v
	default:
		return fmt.Sprint(args[0])
	}
}

// --- Math: IEEE 754 and utility ---

func fnE(args []any) any {
	return math.E
}

func fnLog10(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v <= 0 {
		return nil
	}
	return math.Log10(v)
}

func fnInfinity(args []any) any {
	return math.Inf(1)
}

func fnNaN(args []any) any {
	return math.NaN()
}

func fnIsNaN(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.IsNaN(ToFloat64(args[0]))
}

func fnIsFinite(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	return !math.IsInf(v, 0) && !math.IsNaN(v)
}

func fnIsInfinite(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.IsInf(ToFloat64(args[0]), 0)
}

func fnWidthBucket(args []any) any {
	if len(args) < 4 || args[0] == nil || args[1] == nil || args[2] == nil || args[3] == nil {
		return nil
	}
	value := ToFloat64(args[0])
	bound1 := ToFloat64(args[1])
	bound2 := ToFloat64(args[2])
	n := int(ToFloat64(args[3]))
	if n <= 0 || bound1 == bound2 {
		return nil
	}
	if value < bound1 {
		return int32(0)
	}
	if value >= bound2 {
		return int32(n + 1)
	}
	width := (bound2 - bound1) / float64(n)
	bucket := int((value-bound1)/width) + 1
	if bucket > n {
		bucket = n + 1
	}
	return int32(bucket)
}

func fnFromBase(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	base := int(ToFloat64(args[1]))
	if base < 2 || base > 36 {
		return nil
	}
	n, err := strconv.ParseInt(s, base, 64)
	if err != nil {
		return nil
	}
	return float64(n)
}

func fnToBase(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	n := int64(ToFloat64(args[0]))
	base := int(ToFloat64(args[1]))
	if base < 2 || base > 36 {
		return nil
	}
	return strconv.FormatInt(n, base)
}

func fnBitCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	n := int64(ToFloat64(args[0]))
	return float64(bits.OnesCount64(uint64(n)))
}

// --- Hash: additional ---

func fnSHA1(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha1.Sum([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnCRC32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return float64(crc32.ChecksumIEEE([]byte(toString(args[0]))))
}

func fnHMACSHA256(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	mac := hmac.New(sha256.New, []byte(toString(args[1])))
	mac.Write([]byte(toString(args[0])))
	return hex.EncodeToString(mac.Sum(nil))
}

func fnHMACSHA512(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	mac := hmac.New(sha512.New, []byte(toString(args[1])))
	mac.Write([]byte(toString(args[0])))
	return hex.EncodeToString(mac.Sum(nil))
}

// --- Date: additional accessors ---

func fnQuarter(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64((int(t.Month())-1)/3 + 1)
}

func fnWeek(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	_, week := t.ISOWeek()
	return float64(week)
}

func fnDayOfWeek(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.Weekday())
}

func fnDayOfYear(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	return float64(t.YearDay())
}

func fnLastDayOfMonth(args []any) any {
	t := toTime(args)
	if t.IsZero() {
		return nil
	}
	firstOfNext := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
	last := firstOfNext.AddDate(0, 0, -1)
	return last.Format("2006-01-02")
}

func fnCurrentTimestamp(args []any) any {
	return time.Now().Format(time.RFC3339)
}

func fnAtTimezone(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	tz := toString(args[1])
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil
	}
	return t.In(loc).Format(time.RFC3339)
}

// processStart is when this process began, captured once at package
// initialization.
var processStart = time.Now()

// fnPgPostmasterStartTime implements pg_postmaster_start_time(). DataGrip asks
// for it while opening a connection (`select round(extract(epoch from
// pg_postmaster_start_time() at time zone 'UTC')) as startup_time`) to label
// the session with the server's uptime.
//
// PostgreSQL reports when the postmaster — the process that owns the cluster —
// started. `wadjet serve` is that process, so process start is the honest
// answer. The value is a timestamp in the representation now() and
// current_timestamp use, RFC3339 text: the scalar registry is func([]any) any
// with no type channel, and every temporal function downstream (parseTime,
// epoch, timezone) reads that form.
func fnPgPostmasterStartTime(args []any) any {
	return processStart.Format(time.RFC3339)
}

// fnEpoch implements EXTRACT(EPOCH FROM ts), which the parser rewrites to
// epoch(ts): seconds since 1970-01-01T00:00:00Z. It is derived from the
// resolved instant, never from the column's raw stored number — a DATE column
// holds days and a TIMESTAMP column holds milliseconds, so passing the stored
// value through unchanged answered 9568 for a 1996 date (issue #319).
// resolveTemporalArgs converts those columns to a time.Time before this runs;
// text timestamps are parsed by parseTime as they always were.
func fnEpoch(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := parseTime(args[0])
	if t.IsZero() {
		return nil
	}
	return float64(t.Unix())
}

// fnTimezone implements PostgreSQL's timezone(zone, timestamp), the canonical
// form of the `<timestamp> AT TIME ZONE <zone>` operator that the parser
// rewrites to this call.
//
// PostgreSQL gives the operator two directions, chosen by the input type:
//
//	timestamptz AT TIME ZONE zone → timestamp   (absolute instant → wall clock in zone)
//	timestamp   AT TIME ZONE zone → timestamptz (wall clock in zone → absolute instant)
//
// Wadjet has one timestamp type and its values are absolute instants
// (vectors hold epoch seconds, the scalar layer passes RFC3339 text), so only
// the first direction has a meaning here. But PostgreSQL's result for that
// direction is a *naive* timestamp, which this type system cannot represent.
// Rendering the instant in the zone instead — keeping the offset, so the
// instant is preserved — disagrees with PostgreSQL for everything downstream
// that reads the naive result as UTC: PostgreSQL's EXTRACT(EPOCH FROM ts AT
// TIME ZONE 'America/New_York') is the zone's offset away from EXTRACT(EPOCH
// FROM ts), while an instant-preserving conversion leaves it equal.
//
// So the zone is restricted to UTC, the case where the two readings coincide:
// an instant and its UTC wall clock are the same count of seconds since the
// epoch, and the EXTRACT(EPOCH FROM …) round trip is exact. Every other zone
// is rejected rather than converted with a wrong sign — as a compile-time
// error when the zone is a literal (see compileFuncCallNode) and as NULL here
// when it is not. Widening this means giving the type system a naive-timestamp
// type first.
func fnTimezone(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	if !isUTCZone(toString(args[0])) {
		return nil
	}
	t := parseTime(args[1])
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

// isUTCZone reports whether a zone name is one of the spellings of UTC that
// AT TIME ZONE accepts. All of these are zero offset with no DST rule, so the
// conversion fnTimezone declines to guess at does not arise for them.
func isUTCZone(zone string) bool {
	switch strings.ToUpper(strings.TrimSpace(zone)) {
	case "UTC", "GMT", "Z", "ETC/UTC", "ETC/GMT":
		return true
	}
	return false
}

func fnHumanReadableSeconds(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	total := int64(ToFloat64(args[0]))
	if total < 0 {
		total = -total
	}
	days := total / 86400
	total %= 86400
	hours := total / 3600
	total %= 3600
	minutes := total / 60
	seconds := total % 60
	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%d day%s", days, plural(days)))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d hour%s", hours, plural(hours)))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d minute%s", minutes, plural(minutes)))
	}
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%d second%s", seconds, plural(seconds)))
	}
	return strings.Join(parts, ", ")
}

func plural(n int64) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// --- Network: analytics ---

func fnIsPrivateIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsPrivate()
}

func fnIsLoopbackIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsLoopback()
}

func fnIPToInt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return nil // only IPv4
	}
	return float64(uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3]))
}

func fnIntToIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	n := uint32(ToFloat64(args[0]))
	return fmt.Sprintf("%d.%d.%d.%d", n>>24&0xFF, n>>16&0xFF, n>>8&0xFF, n&0xFF)
}

func fnIsIPv4(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return false
	}
	return ip.To4() != nil
}

func fnIsIPv6(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return false
	}
	return ip.To4() == nil
}

// ── CIDR / Subnet Operations ────────────────────────────────────────────────

func fnNetworkAddress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	return network.IP.String()
}

func fnBroadcastAddress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := network.IP.To4()
	if ip == nil {
		return nil
	}
	mask := network.Mask
	broadcast := make(net.IP, len(ip))
	for i := range ip {
		broadcast[i] = ip[i] | ^mask[i]
	}
	return broadcast.String()
}

func fnPrefixLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ones, _ := network.Mask.Size()
	return int64(ones)
}

func fnCIDRToRange(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ip := network.IP.To4()
	if ip == nil {
		return nil
	}
	mask := network.Mask
	first := network.IP.String()
	broadcast := make(net.IP, len(ip))
	for i := range ip {
		broadcast[i] = ip[i] | ^mask[i]
	}
	return first + "-" + broadcast.String()
}

func fnHostsInCIDR(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	_, network, err := net.ParseCIDR(fmt.Sprint(args[0]))
	if err != nil {
		return nil
	}
	ones, bits := network.Mask.Size()
	hostBits := bits - ones
	if hostBits <= 0 {
		return int64(1)
	}
	if hostBits == 1 {
		return int64(2)
	}
	return int64(1<<uint(hostBits) - 2)
}

func fnCIDROverlap(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	_, net1, err1 := net.ParseCIDR(fmt.Sprint(args[0]))
	_, net2, err2 := net.ParseCIDR(fmt.Sprint(args[1]))
	if err1 != nil || err2 != nil {
		return nil
	}
	return net1.Contains(net2.IP) || net2.Contains(net1.IP)
}

func ipToUint32(ip net.IP) uint32 {
	ip4 := ip.To4()
	if ip4 == nil {
		return 0
	}
	return uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3])
}

func uint32ToIP(n uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", n>>24&0xFF, n>>16&0xFF, n>>8&0xFF, n&0xFF)
}

func fnIPInRange(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	lo := net.ParseIP(fmt.Sprint(args[1]))
	hi := net.ParseIP(fmt.Sprint(args[2]))
	if ip == nil || lo == nil || hi == nil {
		return nil
	}
	v := ipToUint32(ip)
	vLo := ipToUint32(lo)
	vHi := ipToUint32(hi)
	return v >= vLo && v <= vHi
}

func fnSameSubnet(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip1 := net.ParseIP(fmt.Sprint(args[0]))
	ip2 := net.ParseIP(fmt.Sprint(args[1]))
	prefixLen := int(ToInt64(args[2]))
	if ip1 == nil || ip2 == nil {
		return nil
	}
	ip1v4 := ip1.To4()
	ip2v4 := ip2.To4()
	if ip1v4 == nil || ip2v4 == nil {
		return nil
	}
	mask := net.CIDRMask(prefixLen, 32)
	for i := 0; i < 4; i++ {
		if ip1v4[i]&mask[i] != ip2v4[i]&mask[i] {
			return false
		}
	}
	return true
}

// ── IP Manipulation ─────────────────────────────────────────────────────────

func fnIPAdd(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	offset := ToInt64(args[1])
	v := uint32(int64(ipToUint32(ip)) + offset)
	return uint32ToIP(v)
}

func fnIPSubtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	offset := ToInt64(args[1])
	v := uint32(int64(ipToUint32(ip)) - offset)
	return uint32ToIP(v)
}

func fnIPDiff(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	ip1 := net.ParseIP(fmt.Sprint(args[0]))
	ip2 := net.ParseIP(fmt.Sprint(args[1]))
	if ip1 == nil || ip2 == nil {
		return nil
	}
	return int64(ipToUint32(ip1)) - int64(ipToUint32(ip2))
}

func fnIPBetween(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	lo := net.ParseIP(fmt.Sprint(args[1]))
	hi := net.ParseIP(fmt.Sprint(args[2]))
	if ip == nil || lo == nil || hi == nil {
		return nil
	}
	v := ipToUint32(ip)
	vLo := ipToUint32(lo)
	vHi := ipToUint32(hi)
	return v >= vLo && v <= vHi
}

func fnReverseDNS(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip4 := ip.To4(); ip4 != nil {
		return fmt.Sprintf("%d.%d.%d.%d.in-addr.arpa", ip4[3], ip4[2], ip4[1], ip4[0])
	}
	// IPv6: expand to full 32 nibbles reversed
	ip16 := ip.To16()
	nibbles := make([]string, 32)
	for i := 0; i < 16; i++ {
		nibbles[31-2*i] = fmt.Sprintf("%x", ip16[i]>>4)
		nibbles[30-2*i] = fmt.Sprintf("%x", ip16[i]&0x0f)
	}
	return strings.Join(nibbles, ".") + ".ip6.arpa"
}

func fnIsMulticastIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsMulticast()
}

func fnIsLinkLocalIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsLinkLocalUnicast()
}

func fnIsReservedIP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast()
}

func fnIPToHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(fmt.Sprint(args[0]))
	if ip == nil {
		return nil
	}
	if ip4 := ip.To4(); ip4 != nil {
		return hex.EncodeToString(ip4)
	}
	return hex.EncodeToString(ip.To16())
}

// ── MAC Operations ──────────────────────────────────────────────────────────

func fnMACVendorOUI(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 3 {
		return nil
	}
	return strings.ToUpper(fmt.Sprintf("%02x:%02x:%02x", mac[0], mac[1], mac[2]))
}

func fnMACIsUnicast(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 1 {
		return nil
	}
	return mac[0]&0x01 == 0
}

func fnMACIsLocal(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(fmt.Sprint(args[0]))
	if err != nil || len(mac) < 1 {
		return nil
	}
	return mac[0]&0x02 != 0
}

func fnMACFormat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	input := fmt.Sprint(args[0])
	sep := ":"
	if len(args) >= 2 && args[1] != nil {
		sep = fmt.Sprint(args[1])
	}
	// Strip any existing separators to get raw hex
	raw := strings.NewReplacer(":", "", "-", "", ".", "").Replace(input)
	if len(raw) != 12 {
		return nil
	}
	// Validate hex
	for _, c := range raw {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return nil
		}
	}
	raw = strings.ToLower(raw)
	parts := make([]string, 6)
	for i := 0; i < 6; i++ {
		parts[i] = raw[i*2 : i*2+2]
	}
	return strings.Join(parts, sep)
}

// ── Port Classification ─────────────────────────────────────────────────────

var wellKnownPorts = map[int64]string{
	20: "ftp-data", 21: "ftp", 22: "ssh", 23: "telnet", 25: "smtp",
	53: "dns", 67: "dhcp", 68: "dhcp-client", 80: "http", 110: "pop3",
	123: "ntp", 143: "imap", 161: "snmp", 162: "snmp-trap", 443: "https",
	445: "smb", 465: "smtps", 514: "syslog", 587: "submission", 636: "ldaps",
	993: "imaps", 995: "pop3s", 1433: "mssql", 1521: "oracle", 3306: "mysql",
	3389: "rdp", 5432: "postgresql", 5900: "vnc", 6379: "redis",
	8080: "http-alt", 8443: "https-alt", 9200: "elasticsearch", 27017: "mongodb",
}

func fnPortName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	name, ok := wellKnownPorts[port]
	if !ok {
		return nil
	}
	return name
}

func fnIsWellKnownPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 0 && port <= 1023
}

func fnIsRegisteredPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 1024 && port <= 49151
}

func fnIsEphemeralPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	return port >= 49152 && port <= 65535
}

func fnPortClass(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	port := ToInt64(args[0])
	switch {
	case port >= 0 && port <= 1023:
		return "well-known"
	case port >= 1024 && port <= 49151:
		return "registered"
	case port >= 49152 && port <= 65535:
		return "ephemeral"
	default:
		return nil
	}
}

// ── Protocol ────────────────────────────────────────────────────────────────

var protocolNumToName = map[int64]string{
	1: "icmp", 2: "igmp", 6: "tcp", 17: "udp", 41: "ipv6",
	47: "gre", 50: "esp", 51: "ah", 58: "icmpv6", 89: "ospf",
	103: "pim", 132: "sctp",
}

var protocolNameToNum map[string]int64

func init() {
	protocolNameToNum = make(map[string]int64, len(protocolNumToName))
	for num, name := range protocolNumToName {
		protocolNameToNum[name] = num
	}
}

func fnProtocolName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	num := ToInt64(args[0])
	name, ok := protocolNumToName[num]
	if !ok {
		return nil
	}
	return name
}

func fnProtocolNumber(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	name := strings.ToLower(fmt.Sprint(args[0]))
	num, ok := protocolNameToNum[name]
	if !ok {
		return nil
	}
	return num
}

// --- Protocol Deep Inspection Functions ---

// TCP flag constants (bitmask positions in TCP flags byte)
const (
	tcpFIN = 0x01
	tcpSYN = 0x02
	tcpRST = 0x04
	tcpPSH = 0x08
	tcpACK = 0x10
	tcpURG = 0x20
	tcpECE = 0x40
	tcpCWR = 0x80
)

var tcpFlagNames = []struct {
	mask byte
	name string
}{
	{tcpFIN, "FIN"},
	{tcpSYN, "SYN"},
	{tcpRST, "RST"},
	{tcpPSH, "PSH"},
	{tcpACK, "ACK"},
	{tcpURG, "URG"},
	{tcpECE, "ECE"},
	{tcpCWR, "CWR"},
}

var tcpFlagLookup = map[string]byte{
	"fin": tcpFIN, "syn": tcpSYN, "rst": tcpRST, "psh": tcpPSH,
	"ack": tcpACK, "urg": tcpURG, "ece": tcpECE, "cwr": tcpCWR,
}

// fnTCPFlagsToString converts a TCP flags bitmask to comma-separated names.
// tcp_flags_to_string(0x12) → 'SYN,ACK'
func fnTCPFlagsToString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	var parts []string
	for _, f := range tcpFlagNames {
		if flags&f.mask != 0 {
			parts = append(parts, f.name)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ",")
}

// fnHasTCPFlag tests if a TCP flags bitmask has a specific flag set.
// has_tcp_flag(0x12, 'SYN') → true
func fnHasTCPFlag(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	name := strings.ToLower(toString(args[1]))
	mask, ok := tcpFlagLookup[name]
	if !ok {
		return nil
	}
	return flags&mask != 0
}

// fnTCPFlagsFromString converts flag names to bitmask.
// tcp_flags_from_string('SYN,ACK') → 0x12 (18)
func fnTCPFlagsFromString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	parts := strings.Split(toString(args[0]), ",")
	var result byte
	for _, p := range parts {
		mask, ok := tcpFlagLookup[strings.ToLower(strings.TrimSpace(p))]
		if ok {
			result |= mask
		}
	}
	return int64(result)
}

// fnIsTCPHandshake tests for SYN-only (connection initiation).
// is_tcp_handshake(flags) → true if SYN is set and ACK is not
func fnIsTCPHandshake(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	return flags&tcpSYN != 0 && flags&tcpACK == 0
}

// fnIsTCPReset tests for RST flag.
func fnIsTCPReset(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	flags := byte(ToInt64(args[0]))
	return flags&tcpRST != 0
}

// fnTCPSessionID generates a canonical 5-tuple session key.
// tcp_session_id(src_ip, dst_ip, src_port, dst_port, protocol)
// Orders the IP/port pair so both directions map to the same key.
func fnTCPSessionID(args []any) any {
	if len(args) < 5 {
		return nil
	}
	for _, a := range args[:5] {
		if a == nil {
			return nil
		}
	}
	srcIP := toString(args[0])
	dstIP := toString(args[1])
	srcPort := ToInt64(args[2])
	dstPort := ToInt64(args[3])
	proto := ToInt64(args[4])

	// Canonical ordering: lower IP first, break ties by port
	if srcIP > dstIP || (srcIP == dstIP && srcPort > dstPort) {
		srcIP, dstIP = dstIP, srcIP
		srcPort, dstPort = dstPort, srcPort
	}
	return fmt.Sprintf("%s:%d-%s:%d/%d", srcIP, srcPort, dstIP, dstPort, proto)
}

// fnFlowDirection classifies a flow as 'inbound', 'outbound', or 'internal'.
// flow_direction(src_ip, dst_ip) — based on RFC 1918 private ranges
func fnFlowDirection(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	srcIP := net.ParseIP(toString(args[0]))
	dstIP := net.ParseIP(toString(args[1]))
	if srcIP == nil || dstIP == nil {
		return nil
	}
	srcPriv := srcIP.IsPrivate() || srcIP.IsLoopback()
	dstPriv := dstIP.IsPrivate() || dstIP.IsLoopback()
	switch {
	case srcPriv && dstPriv:
		return "internal"
	case srcPriv && !dstPriv:
		return "outbound"
	case !srcPriv && dstPriv:
		return "inbound"
	default:
		return "transit"
	}
}

// --- DNS parsing functions ---
// These work on raw DNS payload bytes (the UDP payload after the IP/UDP headers).

// fnDNSQueryName extracts the query name from a DNS query payload.
// dns_query_name(payload_hex) → 'www.example.com'
// DNS wire format: 2-byte ID, 2-byte flags, 2-byte QDCOUNT, ..., then QNAME
// QNAME is a sequence of length-prefixed labels ending with a 0-length label.
func fnDNSQueryName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 13 { // minimum DNS header + 1-byte name
		return nil
	}
	// Skip 12-byte DNS header
	offset := 12
	return parseDNSName(data, offset)
}

// fnDNSQueryType extracts the query type (A=1, AAAA=28, CNAME=5, MX=15, etc.).
func fnDNSQueryType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 13 {
		return nil
	}
	// Skip header, skip QNAME
	offset := 12
	for offset < len(data) {
		length := int(data[offset])
		if length == 0 {
			offset++
			break
		}
		offset += 1 + length
	}
	if offset+2 > len(data) {
		return nil
	}
	qtype := binary.BigEndian.Uint16(data[offset : offset+2])
	return dnsTypeName(qtype)
}

// fnDNSIsResponse checks if DNS packet is a response (QR bit set).
func fnDNSIsResponse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	// QR is bit 15 of the flags field (byte 2, bit 7)
	return data[2]&0x80 != 0
}

// fnDNSResponseCode extracts the RCODE from DNS flags.
// 0=NOERROR, 1=FORMERR, 2=SERVFAIL, 3=NXDOMAIN, 5=REFUSED
func fnDNSResponseCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	rcode := data[3] & 0x0F
	switch rcode {
	case 0:
		return "NOERROR"
	case 1:
		return "FORMERR"
	case 2:
		return "SERVFAIL"
	case 3:
		return "NXDOMAIN"
	case 4:
		return "NOTIMP"
	case 5:
		return "REFUSED"
	default:
		return fmt.Sprintf("RCODE_%d", rcode)
	}
}

// fnDNSQuestionCount returns the number of questions in a DNS packet.
func fnDNSQuestionCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 6 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[4:6]))
}

// fnDNSAnswerCount returns the number of answers in a DNS packet.
func fnDNSAnswerCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 8 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[6:8]))
}

// fnDNSTransactionID extracts the 16-bit transaction ID.
func fnDNSTransactionID(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[0:2]))
}

// --- TLS inspection functions ---

// fnTLSSNI extracts the Server Name Indication from a TLS ClientHello.
// tls_sni(payload_hex) → 'www.example.com'
func fnTLSSNI(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	return parseTLSSNI(data)
}

// fnTLSVersion extracts the TLS version from a TLS record header.
// Returns human-readable version: 'TLS 1.0', 'TLS 1.2', 'TLS 1.3', etc.
func fnTLSVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 5 {
		return nil
	}
	// TLS record: type(1) + version(2) + length(2)
	major := data[1]
	minor := data[2]
	return tlsVersionString(major, minor)
}

// fnTLSRecordType identifies the TLS record content type.
// 20=ChangeCipherSpec, 21=Alert, 22=Handshake, 23=ApplicationData
func fnTLSRecordType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	switch data[0] {
	case 20:
		return "ChangeCipherSpec"
	case 21:
		return "Alert"
	case 22:
		return "Handshake"
	case 23:
		return "ApplicationData"
	default:
		return fmt.Sprintf("Unknown(%d)", data[0])
	}
}

// fnIsTLSClientHello tests if payload starts with a TLS ClientHello.
func fnIsTLSClientHello(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	// TLS record: type 22 (Handshake), then version, then length, then handshake type 1 (ClientHello)
	if len(data) < 6 {
		return false
	}
	return data[0] == 22 && data[5] == 1
}

// fnTLSHandshakeType returns the handshake message type.
// 1=ClientHello, 2=ServerHello, 11=Certificate, 12=ServerKeyExchange, etc.
func fnTLSHandshakeType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 6 || data[0] != 22 {
		return nil
	}
	hsType := data[5]
	switch hsType {
	case 0:
		return "HelloRequest"
	case 1:
		return "ClientHello"
	case 2:
		return "ServerHello"
	case 4:
		return "NewSessionTicket"
	case 11:
		return "Certificate"
	case 12:
		return "ServerKeyExchange"
	case 13:
		return "CertificateRequest"
	case 14:
		return "ServerHelloDone"
	case 15:
		return "CertificateVerify"
	case 16:
		return "ClientKeyExchange"
	case 20:
		return "Finished"
	default:
		return fmt.Sprintf("Unknown(%d)", hsType)
	}
}

// --- HTTP parsing functions ---
// These work on raw HTTP request/response payloads (text protocol).

// fnHTTPMethod extracts the HTTP method from a request payload.
// http_method(payload) → 'GET', 'POST', etc.
func fnHTTPMethod(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	sp := strings.IndexByte(s, ' ')
	if sp < 0 || sp > 7 {
		return nil
	}
	method := s[:sp]
	switch method {
	case "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH", "TRACE", "CONNECT":
		return method
	default:
		return nil
	}
}

// fnHTTPPath extracts the request path from an HTTP request.
// http_path('GET /api/v1/users HTTP/1.1\r\n...') → '/api/v1/users'
func fnHTTPPath(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	sp1 := strings.IndexByte(s, ' ')
	if sp1 < 0 {
		return nil
	}
	rest := s[sp1+1:]
	sp2 := strings.IndexByte(rest, ' ')
	if sp2 < 0 {
		// try newline
		sp2 = strings.IndexByte(rest, '\r')
		if sp2 < 0 {
			sp2 = strings.IndexByte(rest, '\n')
		}
	}
	if sp2 < 0 {
		return rest
	}
	return rest[:sp2]
}

// fnHTTPHost extracts the Host header from an HTTP request.
func fnHTTPHost(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "host")
}

// fnHTTPStatusCode extracts the status code from an HTTP response.
// http_status_code('HTTP/1.1 200 OK\r\n...') → 200
func fnHTTPStatusCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if !strings.HasPrefix(s, "HTTP/") {
		return nil
	}
	sp1 := strings.IndexByte(s, ' ')
	if sp1 < 0 {
		return nil
	}
	rest := s[sp1+1:]
	sp2 := strings.IndexAny(rest, " \r\n")
	codeStr := rest
	if sp2 >= 0 {
		codeStr = rest[:sp2]
	}
	code, err := strconv.Atoi(codeStr)
	if err != nil {
		return nil
	}
	return int64(code)
}

// fnHTTPStatusClass classifies HTTP status: '1xx', '2xx', '3xx', '4xx', '5xx'.
func fnHTTPStatusClass(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	code := int(ToInt64(args[0]))
	switch {
	case code >= 100 && code < 200:
		return "1xx"
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		return nil
	}
}

// fnHTTPContentType extracts Content-Type header value.
func fnHTTPContentType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "content-type")
}

// fnHTTPContentLength extracts Content-Length header as integer.
func fnHTTPContentLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	val := extractHTTPHeader(toString(args[0]), "content-length")
	if val == nil {
		return nil
	}
	n, err := strconv.ParseInt(val.(string), 10, 64)
	if err != nil {
		return nil
	}
	return n
}

// fnHTTPUserAgent extracts User-Agent header.
func fnHTTPUserAgent(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), "user-agent")
}

// fnHTTPHeader extracts any HTTP header by name.
// http_header(payload, 'X-Forwarded-For')
func fnHTTPHeader(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return extractHTTPHeader(toString(args[0]), strings.ToLower(toString(args[1])))
}

// fnHTTPVersion extracts the HTTP version from a request or response.
// http_version('GET / HTTP/1.1\r\n...') → 'HTTP/1.1'
func fnHTTPVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	// Response: starts with HTTP/
	if strings.HasPrefix(s, "HTTP/") {
		sp := strings.IndexAny(s, " \r\n")
		if sp < 0 {
			return s
		}
		return s[:sp]
	}
	// Request: HTTP version is after the second space on the first line
	line := s
	if nl := strings.IndexByte(s, '\r'); nl >= 0 {
		line = s[:nl]
	} else if nl := strings.IndexByte(s, '\n'); nl >= 0 {
		line = s[:nl]
	}
	sp := strings.LastIndex(line, " HTTP/")
	if sp < 0 {
		return nil
	}
	return line[sp+1:]
}

// fnIsHTTPRequest tests if payload looks like an HTTP request.
func fnIsHTTPRequest(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	for _, m := range []string{"GET ", "POST ", "PUT ", "DELETE ", "HEAD ", "OPTIONS ", "PATCH ", "TRACE ", "CONNECT "} {
		if strings.HasPrefix(s, m) {
			return true
		}
	}
	return false
}

// fnIsHTTPResponse tests if payload looks like an HTTP response.
func fnIsHTTPResponse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.HasPrefix(toString(args[0]), "HTTP/")
}

// --- Packet header parsing ---

// fnIPHeaderLength returns the IP header length in bytes from the raw IP header.
// ip_header_length(payload_hex) — first nibble of byte 0 × 4
func fnIPHeaderLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	ihl := int64(data[0]&0x0F) * 4
	return ihl
}

// fnIPTTL extracts the TTL field from an IPv4 header.
func fnIPTTL(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 9 {
		return nil
	}
	return int64(data[8])
}

// fnIPTotalLength extracts the total length from an IPv4 header.
func fnIPTotalLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 4 {
		return nil
	}
	return int64(binary.BigEndian.Uint16(data[2:4]))
}

// fnIPDSCP extracts the DSCP value from the IPv4 TOS byte.
// DSCP is the top 6 bits of byte 1.
func fnIPDSCP(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(data[1] >> 2)
}

// fnEtherType identifies the EtherType from an Ethernet frame header.
// ether_type(frame_hex) — bytes 12-13 of the Ethernet header
func fnEtherType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 14 {
		return nil
	}
	et := binary.BigEndian.Uint16(data[12:14])
	switch et {
	case 0x0800:
		return "IPv4"
	case 0x0806:
		return "ARP"
	case 0x86DD:
		return "IPv6"
	case 0x8100:
		return "VLAN"
	case 0x8847:
		return "MPLS"
	case 0x88CC:
		return "LLDP"
	default:
		return fmt.Sprintf("0x%04X", et)
	}
}

// fnVLANID extracts the VLAN ID from an 802.1Q tagged frame.
// Expects raw Ethernet frame; VLAN tag starts at byte 14 if EtherType is 0x8100.
func fnVLANID(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 16 {
		return nil
	}
	et := binary.BigEndian.Uint16(data[12:14])
	if et != 0x8100 {
		return nil // not a VLAN-tagged frame
	}
	// VLAN ID is the lower 12 bits of bytes 14-15
	vlanID := binary.BigEndian.Uint16(data[14:16]) & 0x0FFF
	return int64(vlanID)
}

// fnPayloadEntropy estimates the Shannon entropy of a byte payload.
// Useful for detecting encrypted/compressed traffic vs plaintext.
// payload_entropy(data) → float64 (0.0 = uniform, ~8.0 = maximum entropy)
func fnPayloadEntropy(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) == 0 {
		return float64(0)
	}
	var freq [256]int
	for _, b := range data {
		freq[b]++
	}
	n := float64(len(data))
	entropy := 0.0
	for _, f := range freq {
		if f == 0 {
			continue
		}
		p := float64(f) / n
		entropy -= p * math.Log2(p)
	}
	return entropy
}

// fnPayloadHexDump returns the first N bytes as a hex dump string.
// payload_hex_dump(data, 16) → '48 65 6c 6c 6f 20 ...'
func fnPayloadHexDump(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	maxBytes := 32
	if len(args) >= 2 && args[1] != nil {
		maxBytes = int(ToInt64(args[1]))
	}
	if maxBytes > len(data) {
		maxBytes = len(data)
	}
	if maxBytes <= 0 {
		return ""
	}
	parts := make([]string, maxBytes)
	for i := 0; i < maxBytes; i++ {
		parts[i] = fmt.Sprintf("%02x", data[i])
	}
	return strings.Join(parts, " ")
}

// --- Deep inspection helpers ---

// toBytes converts a value to a byte slice, supporting hex strings and raw []byte.
func toBytes(v any) []byte {
	switch tv := v.(type) {
	case []byte:
		return tv
	case string:
		// Try hex decoding first
		if decoded, err := hex.DecodeString(tv); err == nil && len(tv)%2 == 0 && len(tv) > 0 {
			return decoded
		}
		// Fall back to raw bytes
		return []byte(tv)
	default:
		return []byte(fmt.Sprint(v))
	}
}

// parseDNSName parses a DNS wire-format name starting at the given offset.
func parseDNSName(data []byte, offset int) any {
	var parts []string
	for offset < len(data) {
		length := int(data[offset])
		if length == 0 {
			break
		}
		// Pointer (compression)
		if length&0xC0 == 0xC0 {
			if offset+1 >= len(data) {
				break
			}
			ptr := int(binary.BigEndian.Uint16(data[offset:offset+2])) & 0x3FFF
			rest := parseDNSName(data, ptr)
			if rest != nil {
				parts = append(parts, rest.(string))
			}
			return strings.Join(parts, ".")
		}
		offset++
		if offset+length > len(data) {
			break
		}
		parts = append(parts, string(data[offset:offset+length]))
		offset += length
	}
	if len(parts) == 0 {
		return nil
	}
	return strings.Join(parts, ".")
}

// dnsTypeName maps DNS query type numbers to names.
func dnsTypeName(qtype uint16) string {
	switch qtype {
	case 1:
		return "A"
	case 2:
		return "NS"
	case 5:
		return "CNAME"
	case 6:
		return "SOA"
	case 12:
		return "PTR"
	case 15:
		return "MX"
	case 16:
		return "TXT"
	case 28:
		return "AAAA"
	case 33:
		return "SRV"
	case 43:
		return "DS"
	case 46:
		return "RRSIG"
	case 48:
		return "DNSKEY"
	case 65:
		return "HTTPS"
	case 255:
		return "ANY"
	default:
		return fmt.Sprintf("TYPE%d", qtype)
	}
}

// parseTLSSNI extracts the SNI from a TLS ClientHello message.
func parseTLSSNI(data []byte) any {
	// TLS record: type(1) version(2) length(2) [record payload]
	if len(data) < 5 || data[0] != 22 { // not handshake
		return nil
	}
	// Handshake header: type(1) length(3)
	if len(data) < 9 || data[5] != 1 { // not ClientHello
		return nil
	}
	// ClientHello: version(2) random(32) session_id_len(1) ...
	offset := 9 // start of ClientHello body
	if offset+34 > len(data) {
		return nil
	}
	offset += 34 // skip version + random

	// Session ID
	if offset >= len(data) {
		return nil
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen

	// Cipher suites
	if offset+2 > len(data) {
		return nil
	}
	csLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2 + csLen

	// Compression methods
	if offset >= len(data) {
		return nil
	}
	compLen := int(data[offset])
	offset += 1 + compLen

	// Extensions
	if offset+2 > len(data) {
		return nil
	}
	extLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	extEnd := offset + extLen
	if extEnd > len(data) {
		extEnd = len(data)
	}

	for offset+4 <= extEnd {
		extType := binary.BigEndian.Uint16(data[offset : offset+2])
		eLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		offset += 4
		if extType == 0 { // SNI extension
			// SNI list: total_len(2) type(1) name_len(2) name(...)
			if offset+5 > extEnd {
				return nil
			}
			// skip list length (2 bytes)
			nameType := data[offset+2]
			nameLen := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if nameType != 0 { // must be hostname type
				return nil
			}
			if offset+5+nameLen > extEnd {
				return nil
			}
			return string(data[offset+5 : offset+5+nameLen])
		}
		offset += eLen
	}
	return nil
}

// tlsVersionString converts TLS major.minor to human-readable string.
func tlsVersionString(major, minor byte) string {
	if major == 3 {
		switch minor {
		case 0:
			return "SSL 3.0"
		case 1:
			return "TLS 1.0"
		case 2:
			return "TLS 1.1"
		case 3:
			return "TLS 1.2"
		case 4:
			return "TLS 1.3"
		}
	}
	return fmt.Sprintf("TLS %d.%d", major, minor)
}

// extractHTTPHeader extracts an HTTP header value by name (case-insensitive).
func extractHTTPHeader(payload, headerName string) any {
	// Find end of first line
	lines := strings.Split(payload, "\r\n")
	if len(lines) < 2 {
		lines = strings.Split(payload, "\n")
	}
	for _, line := range lines[1:] {
		if line == "" {
			break // end of headers
		}
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		name := strings.TrimSpace(line[:colon])
		if strings.EqualFold(name, headerName) {
			return strings.TrimSpace(line[colon+1:])
		}
	}
	return nil
}

// --- ICMP Functions ---

var icmpTypeNames = map[int]string{
	0:  "Echo Reply",
	3:  "Destination Unreachable",
	4:  "Source Quench",
	5:  "Redirect",
	8:  "Echo Request",
	9:  "Router Advertisement",
	10: "Router Solicitation",
	11: "Time Exceeded",
	12: "Parameter Problem",
	13: "Timestamp Request",
	14: "Timestamp Reply",
	17: "Address Mask Request",
	18: "Address Mask Reply",
	30: "Traceroute",
}

var icmpUnreachableCodes = map[int]string{
	0:  "Network Unreachable",
	1:  "Host Unreachable",
	2:  "Protocol Unreachable",
	3:  "Port Unreachable",
	4:  "Fragmentation Needed",
	5:  "Source Route Failed",
	6:  "Destination Network Unknown",
	7:  "Destination Host Unknown",
	9:  "Network Administratively Prohibited",
	10: "Host Administratively Prohibited",
	13: "Communication Administratively Prohibited",
}

var icmpRedirectCodes = map[int]string{
	0: "Redirect for Network",
	1: "Redirect for Host",
	2: "Redirect for TOS and Network",
	3: "Redirect for TOS and Host",
}

var icmpTimeExceededCodes = map[int]string{
	0: "TTL Exceeded in Transit",
	1: "Fragment Reassembly Time Exceeded",
}

func fnICMPTypeName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	name, ok := icmpTypeNames[t]
	if !ok {
		return fmt.Sprintf("Type %d", t)
	}
	return name
}

func fnICMPCodeName(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	c := int(ToInt64(args[1]))
	switch t {
	case 3:
		if name, ok := icmpUnreachableCodes[c]; ok {
			return name
		}
	case 5:
		if name, ok := icmpRedirectCodes[c]; ok {
			return name
		}
	case 11:
		if name, ok := icmpTimeExceededCodes[c]; ok {
			return name
		}
	}
	if c == 0 {
		if name, ok := icmpTypeNames[t]; ok {
			return name
		}
	}
	return fmt.Sprintf("Type %d Code %d", t, c)
}

func fnIsICMPEcho(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	return t == 0 || t == 8
}

func fnICMPParse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return fmt.Sprintf("%d:%d", data[0], data[1])
}

func fnICMPType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	return int64(data[0])
}

func fnICMPCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(data[1])
}

// --- IPv6 Functions ---

func fnIPv6Scope(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	switch {
	case ip.IsLoopback():
		return "loopback"
	case ip.IsLinkLocalUnicast():
		return "link-local"
	case ip.IsLinkLocalMulticast():
		return "link-local-multicast"
	case ip.IsMulticast():
		return "multicast"
	case ip.IsPrivate():
		if ip.To4() != nil {
			return "private"
		}
		return "unique-local"
	case ip.IsGlobalUnicast():
		return "global"
	case ip.IsUnspecified():
		return "unspecified"
	default:
		return "unknown"
	}
}

func fnIPv6Expand(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return nil
	}
	return fmt.Sprintf("%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
		ip16[0], ip16[1], ip16[2], ip16[3],
		ip16[4], ip16[5], ip16[6], ip16[7],
		ip16[8], ip16[9], ip16[10], ip16[11],
		ip16[12], ip16[13], ip16[14], ip16[15])
}

func fnIPv6Compress(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	return ip.String()
}

func fnIPv6ToEUI64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	mac, err := net.ParseMAC(toString(args[0]))
	if err != nil || len(mac) != 6 {
		return nil
	}
	eui := make([]byte, 8)
	eui[0] = mac[0] ^ 0x02
	eui[1] = mac[1]
	eui[2] = mac[2]
	eui[3] = 0xFF
	eui[4] = 0xFE
	eui[5] = mac[3]
	eui[6] = mac[4]
	eui[7] = mac[5]
	return fmt.Sprintf("%02x%02x:%02x%02x:%02x%02x:%02x%02x",
		eui[0], eui[1], eui[2], eui[3], eui[4], eui[5], eui[6], eui[7])
}

func fnIs6to4(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return false
	}
	ip16 := ip.To16()
	return ip16 != nil && ip16[0] == 0x20 && ip16[1] == 0x02
}

func fnIsTeredo(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return false
	}
	ip16 := ip.To16()
	return ip16 != nil && ip16[0] == 0x20 && ip16[1] == 0x01 && ip16[2] == 0x00 && ip16[3] == 0x00
}

func fnTeredoServer(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x01 || ip16[2] != 0x00 || ip16[3] != 0x00 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[4], ip16[5], ip16[6], ip16[7])
}

func fnTeredoClient(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x01 || ip16[2] != 0x00 || ip16[3] != 0x00 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[12]^0xFF, ip16[13]^0xFF, ip16[14]^0xFF, ip16[15]^0xFF)
}

func fnSixto4Gateway(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ip := net.ParseIP(toString(args[0]))
	if ip == nil {
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil || ip16[0] != 0x20 || ip16[1] != 0x02 {
		return nil
	}
	return fmt.Sprintf("%d.%d.%d.%d", ip16[2], ip16[3], ip16[4], ip16[5])
}

// --- JA3 TLS Fingerprinting ---

func fnJA3Fingerprint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ja3str := buildJA3String(toBytes(args[0]))
	if ja3str == "" {
		return nil
	}
	hash := md5.Sum([]byte(ja3str))
	return hex.EncodeToString(hash[:])
}

func fnJA3String(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	result := buildJA3String(toBytes(args[0]))
	if result == "" {
		return nil
	}
	return result
}

func fnJA3SFingerprint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ja3s := buildJA3SString(toBytes(args[0]))
	if ja3s == "" {
		return nil
	}
	hash := md5.Sum([]byte(ja3s))
	return hex.EncodeToString(hash[:])
}

func fnJA3SString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	result := buildJA3SString(toBytes(args[0]))
	if result == "" {
		return nil
	}
	return result
}

func buildJA3String(data []byte) string {
	if len(data) < 5 || data[0] != 22 {
		return ""
	}
	if len(data) < 9 || data[5] != 1 {
		return ""
	}
	offset := 9
	if offset+2 > len(data) {
		return ""
	}
	tlsVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 34 // version + random
	if offset >= len(data) {
		return ""
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen
	if offset+2 > len(data) {
		return ""
	}
	csLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+csLen > len(data) {
		return ""
	}
	var ciphers []string
	for i := 0; i < csLen; i += 2 {
		cs := int(binary.BigEndian.Uint16(data[offset+i : offset+i+2]))
		if !isGREASE(uint16(cs)) {
			ciphers = append(ciphers, strconv.Itoa(cs))
		}
	}
	offset += csLen
	if offset >= len(data) {
		return ""
	}
	compLen := int(data[offset])
	offset += 1 + compLen

	var extensions, ellipticCurves, ecPointFormats []string
	if offset+2 <= len(data) {
		extTotalLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		extEnd := offset + extTotalLen
		if extEnd > len(data) {
			extEnd = len(data)
		}
		for offset+4 <= extEnd {
			extType := binary.BigEndian.Uint16(data[offset : offset+2])
			extLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
			offset += 4
			if !isGREASE(extType) {
				extensions = append(extensions, strconv.Itoa(int(extType)))
			}
			if offset+extLen > extEnd {
				break
			}
			if extType == 10 && extLen >= 2 {
				listLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
				for j := 2; j+1 < 2+listLen && offset+j+1 < extEnd; j += 2 {
					curve := binary.BigEndian.Uint16(data[offset+j : offset+j+2])
					if !isGREASE(curve) {
						ellipticCurves = append(ellipticCurves, strconv.Itoa(int(curve)))
					}
				}
			}
			if extType == 11 && extLen >= 1 {
				fmtLen := int(data[offset])
				for j := 1; j < 1+fmtLen && offset+j < extEnd; j++ {
					ecPointFormats = append(ecPointFormats, strconv.Itoa(int(data[offset+j])))
				}
			}
			offset += extLen
		}
	}
	return fmt.Sprintf("%d,%s,%s,%s,%s",
		tlsVersion,
		strings.Join(ciphers, "-"),
		strings.Join(extensions, "-"),
		strings.Join(ellipticCurves, "-"),
		strings.Join(ecPointFormats, "-"))
}

func buildJA3SString(data []byte) string {
	if len(data) < 5 || data[0] != 22 {
		return ""
	}
	if len(data) < 9 || data[5] != 2 {
		return ""
	}
	offset := 9
	if offset+2 > len(data) {
		return ""
	}
	tlsVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 34
	if offset >= len(data) {
		return ""
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen
	if offset+2 > len(data) {
		return ""
	}
	cipher := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 3 // cipher + compression

	var extensions []string
	if offset+2 <= len(data) {
		extTotalLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		extEnd := offset + extTotalLen
		if extEnd > len(data) {
			extEnd = len(data)
		}
		for offset+4 <= extEnd {
			extType := binary.BigEndian.Uint16(data[offset : offset+2])
			extLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
			offset += 4
			if !isGREASE(extType) {
				extensions = append(extensions, strconv.Itoa(int(extType)))
			}
			offset += extLen
		}
	}
	return fmt.Sprintf("%d,%d,%s", tlsVersion, cipher, strings.Join(extensions, "-"))
}

func isGREASE(v uint16) bool {
	return v&0x0f0f == 0x0a0a
}

// --- Payload Search Functions ---

func fnPayloadContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	data := toBytes(args[0])
	pattern := toBytes(args[1])
	if len(pattern) == 0 {
		return true
	}
	for i := 0; i <= len(data)-len(pattern); i++ {
		found := true
		for j := 0; j < len(pattern); j++ {
			if data[i+j] != pattern[j] {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func fnPayloadMatches(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	matched, err := regexp.MatchString(toString(args[1]), toString(args[0]))
	if err != nil {
		return nil
	}
	return matched
}

func fnPayloadOffset(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	data := toBytes(args[0])
	off := int(ToInt64(args[1]))
	length := int(ToInt64(args[2]))
	if off < 0 || length <= 0 || off+length > len(data) {
		return nil
	}
	return hex.EncodeToString(data[off : off+length])
}

func fnPayloadLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return int64(len(toBytes(args[0])))
}

// ── Regex: Additional ───────────────────────────────────────────────────────

func fnRegexpCount(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(fmt.Sprint(args[1]))
	if re == nil {
		return nil
	}
	matches := re.FindAllString(fmt.Sprint(args[0]), -1)
	return int64(len(matches))
}

func fnRegexpExtractAll(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(fmt.Sprint(args[1]))
	if re == nil {
		return nil
	}
	matches := re.FindAllString(fmt.Sprint(args[0]), -1)
	if matches == nil {
		return "[]"
	}
	// Return as JSON array string (no native array type yet)
	parts := make([]string, len(matches))
	for i, m := range matches {
		escaped, _ := json.Marshal(m)
		parts[i] = string(escaped)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func fnRegexpSplit(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(fmt.Sprint(args[1]))
	if re == nil {
		return nil
	}
	parts := re.Split(fmt.Sprint(args[0]), -1)
	jsonParts := make([]string, len(parts))
	for i, p := range parts {
		escaped, _ := json.Marshal(p)
		jsonParts[i] = string(escaped)
	}
	return "[" + strings.Join(jsonParts, ",") + "]"
}

// ── String: Additional ──────────────────────────────────────────────────────

func fnSplit(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	parts := strings.Split(fmt.Sprint(args[0]), fmt.Sprint(args[1]))
	jsonParts := make([]string, len(parts))
	for i, p := range parts {
		escaped, _ := json.Marshal(p)
		jsonParts[i] = string(escaped)
	}
	return "[" + strings.Join(jsonParts, ",") + "]"
}

// ── Bitwise: Shifts ─────────────────────────────────────────────────────────

func fnBitwiseLeftShift(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := int64(ToFloat64(args[0]))
	shift := int64(ToFloat64(args[1]))
	if shift < 0 || shift >= 64 {
		return nil
	}
	return int64(v << uint(shift))
}

func fnBitwiseRightShift(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := int64(ToFloat64(args[0]))
	shift := int64(ToFloat64(args[1]))
	if shift < 0 || shift >= 64 {
		return nil
	}
	// Logical right shift (unsigned)
	return int64(uint64(v) >> uint(shift))
}

func fnBitwiseArithmeticShiftRight(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := int64(ToFloat64(args[0]))
	shift := int64(ToFloat64(args[1]))
	if shift < 0 || shift >= 64 {
		return nil
	}
	// Arithmetic right shift (preserves sign bit)
	return int64(v >> uint(shift))
}

// ── UUID: Generation ────────────────────────────────────────────────────────

func fnUUID(args []any) any {
	// Generate a random UUID v4
	var buf [16]byte
	for i := range buf {
		buf[i] = byte(rand.Intn(256))
	}
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// ── Encoding: Additional ────────────────────────────────────────────────────

var base32Encoding = base32.StdEncoding.WithPadding(base32.NoPadding)

func fnToBase32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return base32Encoding.EncodeToString([]byte(fmt.Sprint(args[0])))
}

func fnFromBase32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data, err := base32Encoding.DecodeString(fmt.Sprint(args[0]))
	if err != nil {
		// Try with padding
		data, err = base32.StdEncoding.DecodeString(fmt.Sprint(args[0]))
		if err != nil {
			return nil
		}
	}
	return string(data)
}

func fnXXHash64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// XXHash64 implementation using FNV-like approach
	// Using a simple but correct XXHash64 implementation
	data := []byte(fmt.Sprint(args[0]))
	h := xxhash64Sum(data)
	return fmt.Sprintf("%016x", h)
}

// xxhash64Sum computes XXHash64 of data with seed 0.
func xxhash64Sum(data []byte) uint64 {
	const (
		prime1 uint64 = 0x9E3779B185EBCA87
		prime2 uint64 = 0x14DEF9DEA2F79CD6
		prime3 uint64 = 0x165667B19E3779F9
		prime4 uint64 = 0x85EBCA77C2B2AE63
		prime5 uint64 = 0x27D4EB2F165667C5
	)

	n := len(data)
	var h uint64

	if n >= 32 {
		v1 := prime1 + prime2
		v2 := prime2
		v3 := uint64(0)
		var v4 uint64
		v4 -= prime1

		for len(data) >= 32 {
			v1 = xxh64Round(v1, binary.LittleEndian.Uint64(data[0:8]))
			v2 = xxh64Round(v2, binary.LittleEndian.Uint64(data[8:16]))
			v3 = xxh64Round(v3, binary.LittleEndian.Uint64(data[16:24]))
			v4 = xxh64Round(v4, binary.LittleEndian.Uint64(data[24:32]))
			data = data[32:]
		}

		h = bits.RotateLeft64(v1, 1) + bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) + bits.RotateLeft64(v4, 18)
		h = xxh64MergeRound(h, v1)
		h = xxh64MergeRound(h, v2)
		h = xxh64MergeRound(h, v3)
		h = xxh64MergeRound(h, v4)
	} else {
		h = prime5
	}

	h += uint64(n)

	for len(data) >= 8 {
		k := binary.LittleEndian.Uint64(data[0:8])
		k *= prime2
		k = bits.RotateLeft64(k, 31)
		k *= prime1
		h ^= k
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
		data = data[8:]
	}

	for len(data) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(data[0:4])) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		data = data[4:]
	}

	for len(data) > 0 {
		h ^= uint64(data[0]) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
		data = data[1:]
	}

	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32

	return h
}

func xxh64Round(acc, input uint64) uint64 {
	const prime1 uint64 = 0x9E3779B185EBCA87
	const prime2 uint64 = 0x14DEF9DEA2F79CD6
	acc += input * prime2
	acc = bits.RotateLeft64(acc, 31)
	acc *= prime1
	return acc
}

func xxh64MergeRound(acc, val uint64) uint64 {
	const prime1 uint64 = 0x9E3779B185EBCA87
	const prime4 uint64 = 0x85EBCA77C2B2AE63
	val = xxh64Round(0, val)
	acc ^= val
	acc = acc*prime1 + prime4
	return acc
}

func fnMurmur3(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := []byte(fmt.Sprint(args[0]))
	h := murmur3Hash128(data)
	return fmt.Sprintf("%016x%016x", h[0], h[1])
}

// murmur3Hash128 computes MurmurHash3 x64_128 with seed 0.
func murmur3Hash128(data []byte) [2]uint64 {
	const (
		c1 uint64 = 0x87c37b91114253d5
		c2 uint64 = 0x4cf5ad432745937f
	)

	var h1, h2 uint64
	nblocks := len(data) / 16

	for i := 0; i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint64(data[i*16:])
		k2 := binary.LittleEndian.Uint64(data[i*16+8:])

		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
		h1 = bits.RotateLeft64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		h2 = bits.RotateLeft64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	tail := data[nblocks*16:]
	var k1, k2 uint64
	switch len(tail) {
	case 15:
		k2 ^= uint64(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(tail[8])
		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough
	case 8:
		k1 ^= uint64(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(tail[0])
		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint64(len(data))
	h2 ^= uint64(len(data))

	h1 += h2
	h2 += h1

	// fmix64
	fmix := func(h uint64) uint64 {
		h ^= h >> 33
		h *= 0xff51afd7ed558ccd
		h ^= h >> 33
		h *= 0xc4ceb9fe1a85ec53
		h ^= h >> 33
		return h
	}

	h1 = fmix(h1)
	h2 = fmix(h2)

	h1 += h2
	h2 += h1

	return [2]uint64{h1, h2}
}

// ── Date/Time: ISO 8601 ─────────────────────────────────────────────────────

func fnFromISO8601Timestamp(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	// Try common ISO 8601 formats
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	return nil
}

func fnFromISO8601Date(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return nil
	}
	return t.Format("2006-01-02")
}

func fnToISO8601(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms).UTC()
	return t.Format(time.RFC3339)
}

func fnToMilliseconds(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	// Try parsing as ISO 8601 first
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	// If already a number, return as-is
	if v, ok := args[0].(int64); ok {
		return v
	}
	return nil
}

func fnTimezoneHour(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms)
	_, offset := t.Zone()
	return int64(offset / 3600)
}

func fnTimezoneMinute(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms)
	_, offset := t.Zone()
	return int64((offset % 3600) / 60)
}

// ── Formatting ──────────────────────────────────────────────────────────────

func fnFormatNumber(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])

	// Default: format with commas, no decimal places
	decimals := 0
	if len(args) >= 2 && args[1] != nil {
		decimals = int(ToFloat64(args[1]))
	}

	// Format the number
	negative := v < 0
	if negative {
		v = -v
	}

	// Format with specified decimal places
	s := strconv.FormatFloat(v, 'f', decimals, 64)

	// Split integer and decimal parts
	parts := strings.SplitN(s, ".", 2)
	intPart := parts[0]

	// Add comma separators to integer part
	var result strings.Builder
	if negative {
		result.WriteByte('-')
	}
	for i, c := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(c)
	}
	if len(parts) == 2 {
		result.WriteByte('.')
		result.WriteString(parts[1])
	}

	return result.String()
}

// ── GeoIP / ASN Functions ───────────────────────────────────────────────────

// geoipParseIP extracts a net.IP from the first argument.
func geoipParseIP(args []any) net.IP {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return net.ParseIP(fmt.Sprint(args[0]))
}

func fnGeoipCountry(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Country.ISOCode == "" {
		return nil
	}
	return rec.Country.ISOCode
}

func fnGeoipCountryName(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	name := rec.Country.Names["en"]
	if name == "" {
		return nil
	}
	return name
}

func fnGeoipCity(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	name := rec.City.Names["en"]
	if name == "" {
		return nil
	}
	return name
}

func fnGeoipSubdivision(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || len(rec.Subdivisions) == 0 {
		return nil
	}
	name := rec.Subdivisions[0].Names["en"]
	if name == "" {
		// Fall back to ISO code
		if rec.Subdivisions[0].ISOCode != "" {
			return rec.Subdivisions[0].ISOCode
		}
		return nil
	}
	return name
}

func fnGeoipPostalCode(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Postal.Code == "" {
		return nil
	}
	return rec.Postal.Code
}

func fnGeoipLatitude(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	if rec.Location.Latitude == 0 && rec.Location.Longitude == 0 {
		return nil
	}
	return rec.Location.Latitude
}

func fnGeoipLongitude(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	if rec.Location.Latitude == 0 && rec.Location.Longitude == 0 {
		return nil
	}
	return rec.Location.Longitude
}

func fnGeoipTimezone(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Location.TimeZone == "" {
		return nil
	}
	return rec.Location.TimeZone
}

func fnGeoipContinent(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Continent.Code == "" {
		return nil
	}
	return rec.Continent.Code
}

func fnGeoipASN(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupASN(ip)
	if rec == nil || rec.Number == 0 {
		return nil
	}
	return int64(rec.Number)
}

func fnGeoipOrg(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupASN(ip)
	if rec == nil || rec.Organization == "" {
		return nil
	}
	return rec.Organization
}

// --- Byte/rate formatting functions ---

var byteUnits = []struct {
	threshold float64
	suffix    string
}{
	{1152921504606846976, "EiB"},
	{1125899906842624, "PiB"},
	{1099511627776, "TiB"},
	{1073741824, "GiB"},
	{1048576, "MiB"},
	{1024, "KiB"},
}

var byteUnitsSI = []struct {
	threshold float64
	suffix    string
}{
	{1e18, "EB"},
	{1e15, "PB"},
	{1e12, "TB"},
	{1e9, "GB"},
	{1e6, "MB"},
	{1e3, "KB"},
}

// fnFormatBytes formats a byte count into human-readable form.
// format_bytes(bytes)           → '1.50 GiB'  (IEC binary, default)
// format_bytes(bytes, 'si')     → '1.61 GB'   (SI decimal)
// format_bytes(bytes, 'iec')    → '1.50 GiB'  (IEC binary)
func fnFormatBytes(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v < 0 {
		return "-" + fnFormatBytes([]any{-v, safeArg(args, 1)}).(string)
	}

	units := byteUnits
	if len(args) >= 2 && args[1] != nil && strings.ToLower(toString(args[1])) == "si" {
		units = byteUnitsSI
	}

	for _, u := range units {
		if v >= u.threshold {
			val := v / u.threshold
			if val >= 100 {
				return fmt.Sprintf("%.0f %s", val, u.suffix)
			} else if val >= 10 {
				return fmt.Sprintf("%.1f %s", val, u.suffix)
			}
			return fmt.Sprintf("%.2f %s", val, u.suffix)
		}
	}
	return fmt.Sprintf("%.0f B", v)
}

// fnParseBytes parses a human-readable byte string back to numeric bytes.
// parse_bytes('1.5 GiB') → 1610612736
// parse_bytes('500 MB')   → 500000000
func fnParseBytes(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	s = strings.ToUpper(s)

	multipliers := map[string]float64{
		"B":  1,
		"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "PB": 1e15, "EB": 1e18,
		"KIB": 1024, "MIB": 1048576, "GIB": 1073741824,
		"TIB": 1099511627776, "PIB": 1125899906842624, "EIB": 1152921504606846976,
		// Also handle Kbps-style (bits)
		"BPS": 0.125, "KBPS": 125, "MBPS": 125000, "GBPS": 125000000,
		"KIBPS": 128, "MIBPS": 131072, "GIBPS": 134217728,
	}

	// Try each suffix from longest to shortest
	for _, suffix := range []string{
		"GIBPS", "MIBPS", "KIBPS", "GBPS", "MBPS", "KBPS", "BPS",
		"EIB", "PIB", "TIB", "GIB", "MIB", "KIB",
		"EB", "PB", "TB", "GB", "MB", "KB", "B",
	} {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(suffix)])
			if numStr == "" {
				return nil
			}
			num := ToFloat64(numStr)
			return int64(num * multipliers[suffix])
		}
	}
	// No unit — assume raw bytes
	return int64(ToFloat64(s))
}

// fnFormatRate formats a byte-per-second rate into human-readable form.
// format_rate(bytes_per_sec)            → '1.50 Gbps'  (bits/sec, default)
// format_rate(bytes_per_sec, 'bytes')   → '192.00 MiB/s'  (bytes/sec)
// format_rate(bytes_per_sec, 'si')      → '1.50 Gbps'  (SI bits/sec)
func fnFormatRate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	bytesPerSec := ToFloat64(args[0])

	mode := "bits"
	if len(args) >= 2 && args[1] != nil {
		mode = strings.ToLower(toString(args[1]))
	}

	if mode == "bytes" || mode == "byte" {
		// Format as bytes/sec using IEC units
		formatted := fnFormatBytes([]any{bytesPerSec})
		if formatted == nil {
			return nil
		}
		return formatted.(string) + "/s"
	}

	// Format as bits/sec (SI)
	bitsPerSec := bytesPerSec * 8

	type rateUnit struct {
		threshold float64
		suffix    string
	}
	units := []rateUnit{
		{1e12, "Tbps"},
		{1e9, "Gbps"},
		{1e6, "Mbps"},
		{1e3, "Kbps"},
	}

	for _, u := range units {
		if bitsPerSec >= u.threshold {
			val := bitsPerSec / u.threshold
			if val >= 100 {
				return fmt.Sprintf("%.0f %s", val, u.suffix)
			} else if val >= 10 {
				return fmt.Sprintf("%.1f %s", val, u.suffix)
			}
			return fmt.Sprintf("%.2f %s", val, u.suffix)
		}
	}
	return fmt.Sprintf("%.0f bps", bitsPerSec)
}

// fnParseRate parses a human-readable rate string back to bytes per second.
// parse_rate('1.5 Gbps')   → 187500000  (bytes/sec)
// parse_rate('100 Mbps')   → 12500000   (bytes/sec)
// parse_rate('10 MiB/s')   → 10485760   (bytes/sec)
func fnParseRate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	upper := strings.ToUpper(s)

	// Bits-per-second units → convert to bytes/sec
	bitRates := []struct {
		suffix     string
		bitsPerSec float64
	}{
		{"TBPS", 1e12},
		{"GBPS", 1e9},
		{"MBPS", 1e6},
		{"KBPS", 1e3},
		{"BPS", 1},
	}

	for _, r := range bitRates {
		if strings.HasSuffix(upper, r.suffix) {
			numStr := strings.TrimSpace(upper[:len(upper)-len(r.suffix)])
			if numStr == "" {
				return nil
			}
			bps := ToFloat64(numStr) * r.bitsPerSec
			return int64(bps / 8) // bits to bytes
		}
	}

	// Bytes-per-second units (MiB/s, GB/s, etc.)
	byteRates := []struct {
		suffix      string
		bytesPerSec float64
	}{
		{"TIB/S", 1099511627776},
		{"GIB/S", 1073741824},
		{"MIB/S", 1048576},
		{"KIB/S", 1024},
		{"TB/S", 1e12},
		{"GB/S", 1e9},
		{"MB/S", 1e6},
		{"KB/S", 1e3},
		{"B/S", 1},
	}

	for _, r := range byteRates {
		if strings.HasSuffix(upper, r.suffix) {
			numStr := strings.TrimSpace(upper[:len(upper)-len(r.suffix)])
			if numStr == "" {
				return nil
			}
			return int64(ToFloat64(numStr) * r.bytesPerSec)
		}
	}

	// No unit — assume bytes/sec
	return int64(ToFloat64(s))
}

func safeArg(args []any, i int) any {
	if i < len(args) {
		return args[i]
	}
	return nil
}

// --- Array/nested type function implementations ---

// toSlice converts a value to []any, handling both []any and []map[string]any.
func toSlice(v any) ([]any, bool) {
	switch tv := v.(type) {
	case []any:
		return tv, true
	case []map[string]any:
		out := make([]any, len(tv))
		for i, m := range tv {
			out[i] = m
		}
		return out, true
	default:
		return nil, false
	}
}

// cardinality(array) / array_length(array) — returns the number of elements
func fnCardinality(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	return int32(len(arr))
}

// element_at(array, index) — returns the element at 1-based index (Trino convention)
// For MAPs: element_at(map, key) returns the value for the given key.
// Negative indices count from the end.
//
// A MAP value and an ARRAY of ROW("key","value") (e.g. map_entries()'s output)
// share the same runtime shape — []any of {key,value} rows — so this
// value-only entry point CANNOT tell a MAP key lookup from an array index and
// treats every slice positionally. The MAP-vs-ARRAY choice is made from the
// COMPILED type of the first argument instead, in elementAtExpr, which the
// compiler wraps every element_at / m['k'] subscript in (#607). A genuine Go
// map (map_from_entries, constructed literals) is unambiguous and keyed here
// directly.
func fnElementAt(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	if m, ok := args[0].(map[string]any); ok {
		return m[fmt.Sprint(args[1])]
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	idx := int(ToInt64(args[1]))
	if idx > 0 {
		idx-- // convert 1-based to 0-based
	} else if idx < 0 {
		idx = len(arr) + idx // negative index from end
	} else {
		return nil // 0 is invalid in 1-based indexing
	}
	if idx < 0 || idx >= len(arr) {
		return nil
	}
	return arr[idx]
}

// array_contains(array, element) — returns true if array contains element
func fnArrayContains(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	target := args[1]
	for _, elem := range arr {
		if elem == target || fmt.Sprint(elem) == fmt.Sprint(target) {
			return true
		}
	}
	return false
}

// array_join(array, delimiter) — joins array elements into a string
func fnArrayJoin(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	delim := toString(args[1])
	parts := make([]string, 0, len(arr))
	for _, elem := range arr {
		if elem != nil {
			parts = append(parts, fmt.Sprint(elem))
		}
	}
	return strings.Join(parts, delim)
}

// array_min(array) — returns the minimum element
func fnArrayMin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok || len(arr) == 0 {
		return nil
	}
	min := arr[0]
	for _, elem := range arr[1:] {
		if elem == nil {
			continue
		}
		if min == nil || fmt.Sprint(elem) < fmt.Sprint(min) {
			min = elem
		}
	}
	return min
}

// row_field(row, 'field_name') — extracts a named field from a ROW/struct value
func fnRowField(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	row, ok := args[0].(map[string]any)
	if !ok {
		return nil
	}
	field := toString(args[1])
	return row[field]
}

// --- MAP function implementations ---

// toMap extracts key-value pairs from a MAP value.
// MAPs are stored as []any where each element is map[string]any{"key":k, "value":v}
// or as map[string]any directly.
// elementAtExpr evaluates element_at(x, k) / the x[k] subscript, choosing MAP
// key lookup vs. ARRAY positional index from the COMPILED type of x rather than
// the runtime value's shape. A MAP column materializes as an
// ARRAY(ROW("key","value")) []any and map_entries() returns that identical
// shape, so a value-only heuristic misroutes one of them; the static type does
// not (#607).
type elementAtExpr struct {
	arg0, arg1 Expr
	// resolved publishes isMap, decided on the first Eval because a ColRef
	// needs the batch to know its column's declared type. Set last under mu.
	resolved atomic.Bool
	mu       sync.Mutex
	isMap    bool
}

func (e *elementAtExpr) Eval(b *batch.RecordBatch, row int) any {
	container := e.arg0.Eval(b, row)
	key := e.arg1.Eval(b, row)
	if container == nil || key == nil {
		return nil
	}
	if !e.resolved.Load() {
		e.resolveDispatch(b)
	}
	if e.isMap {
		return mapElementAt(container, key)
	}
	return fnElementAt([]any{container, key})
}

func (e *elementAtExpr) resolveDispatch(b *batch.RecordBatch) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.resolved.Load() {
		return
	}
	e.isMap = staticallyMap(e.arg0, b)
	e.resolved.Store(true)
}

// staticallyMap reports whether e is known from its COMPILED/declared type to
// evaluate to a MAP — never from a runtime value's shape, which a MAP shares
// with an ARRAY of {key,value} rows. b supplies a ColRef its column's declared
// type. A FuncCall is a MAP when its registered return type fixes it to one
// (map_from_entries is RetMap; map_entries is RetArray, so map_entries(m)[i]
// correctly indexes). Anything else is treated as an ARRAY.
func staticallyMap(e Expr, b *batch.RecordBatch) bool {
	switch a := e.(type) {
	case *ColRef:
		a.resolve(b)
		return a.typ == batch.TypeMap
	case *FuncCall:
		t, c := DefaultRegistry.ReturnType(a.Name).Resolve(len(a.Args), nil)
		return c != Undecided && t.ID == batch.TypeMap
	default:
		return false
	}
}

// mapElementAt looks key up in a MAP value in either materialized shape: a Go
// map (map_from_entries, constructed literals) or the ARRAY(ROW("key","value"))
// entry rows a MAP column produces through batch.Vector.GetValue. Keys compare
// by string form, covering the MAP's string and integer key types. A duplicate
// key returns the LAST match, matching toMap (the shape map_keys/map_values and
// a MAP GROUP BY key go through, which last-write-wins into a Go map). An
// absent key returns NULL.
func mapElementAt(m, key any) any {
	if gm, ok := m.(map[string]any); ok {
		return gm[fmt.Sprint(key)]
	}
	entries, ok := toSlice(m)
	if !ok {
		return nil
	}
	want := fmt.Sprint(key)
	var val any
	found := false
	for _, entry := range entries {
		row, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if fmt.Sprint(row["key"]) == want {
			val, found = row["value"], true
		}
	}
	if !found {
		return nil
	}
	return val
}

func toMap(v any) (map[string]any, bool) {
	switch tv := v.(type) {
	case map[string]any:
		return tv, true
	case []any:
		// ARRAY(ROW("key","value")) representation
		m := make(map[string]any, len(tv))
		for _, entry := range tv {
			if row, ok := entry.(map[string]any); ok {
				key := fmt.Sprint(row["key"])
				m[key] = row["value"]
			}
		}
		return m, true
	case []map[string]any:
		m := make(map[string]any, len(tv))
		for _, row := range tv {
			key := fmt.Sprint(row["key"])
			m[key] = row["value"]
		}
		return m, true
	default:
		return nil, false
	}
}

// map_keys(map) — returns the keys as an array
func fnMapKeys(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	keys := make([]any, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// map_values(map) — returns the values as an array
func fnMapValues(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	vals := make([]any, 0, len(m))
	for _, v := range m {
		vals = append(vals, v)
	}
	return vals
}

// map_entries(map) — returns array of ROW(key, value) entries
func fnMapEntries(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	m, ok := toMap(args[0])
	if !ok {
		return nil
	}
	entries := make([]any, 0, len(m))
	for k, v := range m {
		entries = append(entries, map[string]any{"key": k, "value": v})
	}
	return entries
}

// map_from_entries(array_of_rows) — constructs a map from ROW(key, value) entries
func fnMapFromEntries(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok {
		return nil
	}
	m := make(map[string]any, len(arr))
	for _, entry := range arr {
		if row, ok := entry.(map[string]any); ok {
			key := fmt.Sprint(row["key"])
			m[key] = row["value"]
		}
	}
	return m
}

// array_max(array) — returns the maximum element
func fnArrayMax(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	arr, ok := toSlice(args[0])
	if !ok || len(arr) == 0 {
		return nil
	}
	max := arr[0]
	for _, elem := range arr[1:] {
		if elem == nil {
			continue
		}
		if max == nil || fmt.Sprint(elem) > fmt.Sprint(max) {
			max = elem
		}
	}
	return max
}

// --- Vectorized scalar function implementations ---
//
// These operate on entire columns (batch.Vector) instead of per-row values,
// eliminating interface dispatch, boxing/unboxing, and string↔[]byte conversion.

// vecReadFloat64 reads a float64 from a vector at the given index,
// handling both Float64 and Int64 source types.
func vecReadFloat64(v *batch.Vector, i int) float64 {
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[i]
	case batch.TypeInt64, batch.TypeTimestamp:
		return float64(v.Int64Data[i])
	case batch.TypeInt32:
		return float64(v.Int32Data[i])
	case batch.TypeFloat32:
		return float64(v.Float32Data[i])
	default:
		return 0
	}
}

func vecUpper(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	totalBytes := int(src.BytesData.Offsets[n] - src.BytesData.Offsets[0])
	out.BytesData.PreAllocBytes(totalBytes)

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		start := len(out.BytesData.Data)
		out.BytesData.Data = append(out.BytesData.Data, b...)
		for j := start; j < len(out.BytesData.Data); j++ {
			c := out.BytesData.Data[j]
			if c >= 'a' && c <= 'z' {
				out.BytesData.Data[j] = c - 32
			}
		}
		out.BytesData.Offsets[i+1] = uint32(len(out.BytesData.Data))
	}
}

func vecLower(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	totalBytes := int(src.BytesData.Offsets[n] - src.BytesData.Offsets[0])
	out.BytesData.PreAllocBytes(totalBytes)

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		start := len(out.BytesData.Data)
		out.BytesData.Data = append(out.BytesData.Data, b...)
		for j := start; j < len(out.BytesData.Data); j++ {
			c := out.BytesData.Data[j]
			if c >= 'A' && c <= 'Z' {
				out.BytesData.Data[j] = c + 32
			}
		}
		out.BytesData.Offsets[i+1] = uint32(len(out.BytesData.Data))
	}
}

// vecLength is the offsets-shape kernel for length()/len(): a byte count
// read straight off the offsets array. Non-byte-array inputs (length() of a
// numeric or temporal column, which used to index a nil Offsets slice and
// panic) fall through to the boxed per-row definition. See shape_funcs.go.
func vecLength(args []*batch.Vector, out *batch.Vector, n int) {
	vecShapeLenScaled(args, out, n, 1)
}

func vecTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		// Trim ASCII whitespace from both ends
		lo, hi := 0, len(b)
		for lo < hi && (b[lo] == ' ' || b[lo] == '\t' || b[lo] == '\n' || b[lo] == '\r') {
			lo++
		}
		for hi > lo && (b[hi-1] == ' ' || b[hi-1] == '\t' || b[hi-1] == '\n' || b[hi-1] == '\r') {
			hi--
		}
		out.BytesData.Set(i, b[lo:hi])
	}
}

func vecLTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		lo := 0
		for lo < len(b) && (b[lo] == ' ' || b[lo] == '\t' || b[lo] == '\n' || b[lo] == '\r') {
			lo++
		}
		out.BytesData.Set(i, b[lo:])
	}
}

func vecRTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		hi := len(b)
		for hi > 0 && (b[hi-1] == ' ' || b[hi-1] == '\t' || b[hi-1] == '\n' || b[hi-1] == '\r') {
			hi--
		}
		out.BytesData.Set(i, b[:hi])
	}
}

func vecSubstr(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	hasLen := len(args) >= 3

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		start := int(vecReadFloat64(args[1], i)) - 1 // SQL is 1-indexed
		if hasLen {
			// PostgreSQL's window rule; must match fnSubstr (#373).
			lo, hi := substrWindow(start, int(vecReadFloat64(args[2], i)), len(b))
			out.BytesData.Set(i, b[lo:hi])
			continue
		}
		if start < 0 {
			start = 0
		}
		if start >= len(b) {
			out.BytesData.Set(i, nil)
			continue
		}
		out.BytesData.Set(i, b[start:])
	}
}

func vecReplace(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls() || args[1].Nulls.HasNulls() || args[2].Nulls.HasNulls()

	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || args[1].Nulls.IsNullFast(i) || args[2].Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		s := src.BytesData.StringValue(i)
		old := args[1].BytesData.StringValue(i)
		new := args[2].BytesData.StringValue(i)
		result := strings.ReplaceAll(s, old, new)
		out.BytesData.Set(i, []byte(result))
	}
}

func vecReverse(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		rev := make([]byte, len(b))
		for j, k := 0, len(b)-1; j <= k; j, k = j+1, k-1 {
			rev[j], rev[k] = b[k], b[j]
		}
		out.BytesData.Set(i, rev)
	}
}

func vecLeft(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		count := int(vecReadFloat64(args[1], i))
		if count < 0 {
			out.BytesData.Set(i, nil)
		} else if count >= len(b) {
			out.BytesData.Set(i, b)
		} else {
			out.BytesData.Set(i, b[:count])
		}
	}
}

func vecRight(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		count := int(vecReadFloat64(args[1], i))
		if count < 0 {
			out.BytesData.Set(i, nil)
		} else if count >= len(b) {
			out.BytesData.Set(i, b)
		} else {
			out.BytesData.Set(i, b[len(b)-count:])
		}
	}
}

// vecConcat is fnConcat's kernel: a NULL argument contributes nothing and
// the row is never NULL. The row and vector paths are separately reachable —
// which one runs depends on whether every argument is a byte-array-shaped
// vector (see FuncCall.EvalVec) — so both carry the rule (#609).
func vecConcat(args []*batch.Vector, out *batch.Vector, n int) {
	for i := 0; i < n; i++ {
		total := 0
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				continue
			}
			total += int(arg.BytesData.Offsets[i+1] - arg.BytesData.Offsets[i])
		}
		buf := make([]byte, 0, total)
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				continue
			}
			buf = append(buf, arg.BytesData.Value(i)...)
		}
		out.BytesData.Set(i, buf)
	}
}

// vecConcatOp is fnConcatOp's kernel: the `||` operator, NULL-propagating.
func vecConcatOp(args []*batch.Vector, out *batch.Vector, n int) {
	for i := 0; i < n; i++ {
		isNull := false
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				isNull = true
				break
			}
		}
		if isNull {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		// Calculate total length then build in one shot
		total := 0
		for _, arg := range args {
			total += int(arg.BytesData.Offsets[i+1] - arg.BytesData.Offsets[i])
		}
		buf := make([]byte, 0, total)
		for _, arg := range args {
			buf = append(buf, arg.BytesData.Value(i)...)
		}
		out.BytesData.Set(i, buf)
	}
}

func vecStartsWith(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	prefix := args[1]
	hasNulls := src.Nulls.HasNulls() || prefix.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || prefix.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.Value(i)
		p := prefix.BytesData.Value(i)
		result := len(s) >= len(p)
		if result {
			for j := 0; j < len(p); j++ {
				if s[j] != p[j] {
					result = false
					break
				}
			}
		}
		out.BoolData[i] = result
	}
}

func vecEndsWith(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	suffix := args[1]
	hasNulls := src.Nulls.HasNulls() || suffix.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || suffix.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.Value(i)
		p := suffix.BytesData.Value(i)
		result := len(s) >= len(p)
		if result {
			off := len(s) - len(p)
			for j := 0; j < len(p); j++ {
				if s[off+j] != p[j] {
					result = false
					break
				}
			}
		}
		out.BoolData[i] = result
	}
}

func vecContains(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	sub := args[1]
	hasNulls := src.Nulls.HasNulls() || sub.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || sub.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.StringValue(i)
		p := sub.BytesData.StringValue(i)
		out.BoolData[i] = strings.Contains(s, p)
	}
}

// --- Vectorized math functions ---

func vecAbs(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Abs(vecReadFloat64(src, i))
	}
}

func vecCeil(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Ceil(vecReadFloat64(src, i))
	}
}

func vecFloor(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Floor(vecReadFloat64(src, i))
	}
}

func vecRound(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	precision := 0
	if len(args) >= 2 {
		precision = int(vecReadFloat64(args[1], 0))
	}
	pow := math.Pow(10, float64(precision))
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.Round(vecReadFloat64(src, i)*pow) / pow
	}
}

// vecRoundHalfEven is the vectorized counterpart of fnRoundHalfEven — see
// its comment for the DOUBLE PRECISION half-to-even rule (#381).
func vecRoundHalfEven(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	precision := 0
	if len(args) >= 2 {
		precision = int(vecReadFloat64(args[1], 0))
	}
	pow := math.Pow(10, float64(precision))
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = math.RoundToEven(vecReadFloat64(src, i)*pow) / pow
	}
}

// --- Vectorized date/time functions ---

func vecYear(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Year())
	}
}

func vecMonth(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Month())
	}
}

func vecDay(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Day())
	}
}

func vecHour(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		out.Float64Data[i] = float64(t.Hour())
	}
}

func vecExtract(args []*batch.Vector, out *batch.Vector, n int) {
	if len(args) < 2 {
		return
	}
	// First arg is the unit string (constant in practice)
	unit := strings.ToLower(args[0].BytesData.StringValue(0))
	src := args[1]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			continue
		}
		t, ok := columnInstant(src, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		switch unit {
		case "year":
			out.Float64Data[i] = float64(t.Year())
		case "quarter":
			out.Float64Data[i] = float64((t.Month()-1)/3 + 1)
		case "month":
			out.Float64Data[i] = float64(t.Month())
		case "week":
			_, week := t.ISOWeek()
			out.Float64Data[i] = float64(week)
		case "day":
			out.Float64Data[i] = float64(t.Day())
		case "hour":
			out.Float64Data[i] = float64(t.Hour())
		case "minute":
			out.Float64Data[i] = float64(t.Minute())
		case "second":
			out.Float64Data[i] = float64(t.Second())
		case "dow", "dayofweek":
			out.Float64Data[i] = float64(t.Weekday())
		case "doy", "dayofyear":
			out.Float64Data[i] = float64(t.YearDay())
		case "epoch":
			out.Float64Data[i] = float64(t.Unix())
		default:
			// fnExtract returns nil for an unrecognized unit. Leaving the
			// pooled vector's stale contents in place instead answered with
			// whatever the previous batch wrote there.
			out.Nulls.SetNull(i)
		}
	}
}

// --- Domain parsing functions ---
// These use the Public Suffix List (golang.org/x/net/publicsuffix) to correctly
// handle multi-part TLDs like .co.uk, .com.au, .gov.uk etc.

// cleanDomain strips any trailing dot and lowercases for consistent handling.
func cleanDomain(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.TrimSuffix(s, ".")
	return s
}

// fnRegisteredDomain extracts the registered domain (eTLD+1) from a hostname.
// registered_domain('mail.google.com') → 'google.com'
// registered_domain('sub.example.co.uk') → 'example.co.uk'
func fnRegisteredDomain(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	rd, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return nil
	}
	return rd
}

// fnTLD extracts the effective top-level domain (public suffix) from a hostname.
// tld('mail.google.com') → 'com'
// tld('sub.example.co.uk') → 'co.uk'
func fnTLD(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	suffix, _ := publicsuffix.PublicSuffix(domain)
	return suffix
}

// fnSubdomain extracts the subdomain portion (everything before the registered domain).
// subdomain('mail.google.com') → 'mail'
// subdomain('a.b.c.example.co.uk') → 'a.b.c'
// subdomain('example.com') → ” (no subdomain)
func fnSubdomain(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	rd, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return nil
	}
	if domain == rd {
		return ""
	}
	// domain = "a.b.c.example.co.uk", rd = "example.co.uk"
	// subdomain = "a.b.c"
	return strings.TrimSuffix(domain, "."+rd)
}

// fnDomainDepth returns the number of labels (dot-separated parts) in a domain.
// domain_depth('mail.google.com') → 3
// domain_depth('a.b.c.evil.com') → 5
// Useful for detecting DGA domains which tend to have unusual depth.
func fnDomainDepth(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	return float64(strings.Count(domain, ".") + 1)
}

// --- URL encoding/decoding ---

func fnURLEncode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return url.QueryEscape(toString(args[0]))
}

func fnURLDecode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	decoded, err := url.QueryUnescape(toString(args[0]))
	if err != nil {
		return nil
	}
	return decoded
}

// --- String entropy ---

// fnEntropy computes the Shannon entropy of a string in bits per character.
// High entropy (>4.5) suggests encoded, encrypted, or random data.
// Low entropy (<3.0) suggests natural language or repetitive patterns.
// entropy('aaaa') → 0.0
// entropy('hello world') → ~2.85
// entropy('a3f8b2c9e1d7') → ~3.58
func fnEntropy(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if len(s) == 0 {
		return float64(0)
	}
	freq := make(map[rune]int)
	total := 0
	for _, r := range s {
		freq[r]++
		total++
	}
	entropy := 0.0
	ft := float64(total)
	for _, count := range freq {
		p := float64(count) / ft
		entropy -= p * math.Log2(p)
	}
	return entropy
}

// columnInstant resolves row i of a vector to the UTC instant it denotes, and
// reports whether it could. It is THE definition of "what time is stored in
// this column", and both evaluation paths go through it: the vectorized
// date-part kernels below call it directly, and the scalar path reaches it
// through (*FuncCall).resolveTemporalArgs. Divergence between the two paths is
// what let this defect survive — one shared resolver makes agreement
// structural rather than something a test has to keep rediscovering.
//
// A raw stored number carries no unit, and each type stores a different one:
//
//	TypeDate      Int32Data, days since the epoch
//	TypeTimestamp Int64Data, MILLISECONDS since the epoch (what the parquet
//	              writer emits — file_writer.go encodes TimestampMillis — and
//	              what the comparison path assumes, parseTemporalInt64OK)
//	String/Bytes  text, parsed
//	anything else Int64Data read as seconds, the only defensible reading of
//	              an untyped integer and what parseTime(int64) has always done
//
// Reading Int64Data unconditionally was right only for a timestamp-in-seconds
// column: a DATE column has nothing in Int64Data at all, so every row came
// back as 1970 — silently, with no error and no null, collapsing a decade of
// `GROUP BY EXTRACT(YEAR FROM d)` into one bogus bucket (issue #319).
func columnInstant(src *batch.Vector, i int) (time.Time, bool) {
	switch src.Type {
	case batch.TypeString, batch.TypeBytes:
		t := parseTime(src.BytesData.StringValue(i))
		if t.IsZero() {
			return time.Time{}, false
		}
		return t, true
	case batch.TypeDate:
		if i < len(src.Int32Data) {
			// Days since the Unix epoch.
			return time.Unix(int64(src.Int32Data[i])*86400, 0).UTC(), true
		}
	case batch.TypeTimestamp:
		if i < len(src.Int64Data) {
			// Milliseconds since the Unix epoch.
			return time.UnixMilli(src.Int64Data[i]).UTC(), true
		}
	default:
		if i < len(src.Int64Data) {
			return time.Unix(src.Int64Data[i], 0).UTC(), true
		}
	}
	return time.Time{}, false
}

// --- CAST to a temporal type, and the arithmetic that reads its result ---

// castTemporalKindT names the destination types CAST resolves to a temporal
// VALUE rather than leaving to the generic conversions. TIME is deliberately
// absent: the engine has no time-of-day type to hold one, so `TIME '10:00:00'`
// keeps its text exactly as before.
type castTemporalKindT int

const (
	castNotTemporal castTemporalKindT = iota
	castToDateKind
	castToTimestampKind
)

func castTemporalKind(destType string) castTemporalKindT {
	return castTemporalKindLower(strings.ToLower(strings.TrimSpace(destType)))
}

// castTemporalKindLower is castTemporalKind for a caller that has already
// normalized the name — Cast.Eval, which needs the lowercased form for its own
// switch and runs once per row.
func castTemporalKindLower(dest string) castTemporalKindT {
	switch dest {
	case "date":
		return castToDateKind
	case "timestamp", "datetime", "timestamptz":
		return castToTimestampKind
	}
	return castNotTemporal
}

// castTemporal is CAST(x AS DATE) / CAST(x AS TIMESTAMP).
//
// Both produce the box the corresponding COLUMN type produces: epoch DAYS for
// DATE, epoch MILLISECONDS for TIMESTAMP, both int64 — the same values
// ColRef.Eval hands out for a batch.TypeDate / batch.TypeTimestamp column, and
// the same values batch.Vector.SetValue stores back into one. That identity is
// the whole point of the fix (#340): until now the cast returned its argument
// unchanged, so `CAST('1996-01-10' AS DATE) - 1` subtracted 1 from the number
// ToFloat64 read out of the TEXT's leading digits and answered 1995.
//
// The operand resolves through temporalOperand — the #332 helper — so a DATE
// column arrives as a civilDate and a TIMESTAMP column as a time.Time, with
// the unit their bare int64 box has lost recovered from the declared column
// type; parseDateArg (#322) then reads whichever form arrived. Nothing here
// parses a column value itself, so the cast cannot disagree with date_add,
// date_diff or `date ± INTERVAL` about what a column means.
//
// An operand that resolves to no instant at all becomes NULL. When #340 chose
// that, the expression layer had no per-row error channel; it has one now
// (FatalEvalPanic, #347), and the numeric casts use it — CAST('abc' AS
// integer) raises 22P02 per ADR-0012's PostgreSQL-decides-error-versus-not
// rule (#367). The temporal casts deliberately keep NULL for the moment:
// their unparseable inputs are pinned across date_cast_test.go and the
// change is a semantics flip of its own, to be made on its own issue rather
// than as a rider. Until then this is TRY_CAST's rule (DuckDB raises; its
// TRY_CAST returns NULL) and a documented divergence from PostgreSQL.
func castTemporal(b *batch.RecordBatch, row int, operand Expr, v any, kind castTemporalKindT) any {
	src, ok := temporalOperand(b, row, operand, v)
	if !ok {
		src = v
	}
	t, _, ok := parseDateArg(src)
	if !ok {
		return nil
	}
	if kind == castToDateKind {
		return epochDaysOf(t)
	}
	return t.UTC().UnixMilli()
}

// epochDaysOf floors an instant to the UTC day it falls in and returns that
// day's distance from 1970-01-01 — the DATE column representation.
func epochDaysOf(t time.Time) int64 {
	secs := t.UTC().Unix()
	days := secs / 86400
	if secs < 0 && secs%86400 != 0 {
		days--
	}
	return days
}

// dateArith is `date - date` and `date ± n`, the two shapes BinOp.Eval must
// recognize once CAST produces a real date (#340).
//
// Operands resolve through temporalOperand, so every form the engine has for a
// date is accepted on equal terms: a DATE/TIMESTAMP column, a CAST to one, and
// a date-shaped string — which is what a DATE column looks like when the
// catalog declares it VARCHAR, as the TPC-H fixtures do. `l_receiptdate -
// l_shipdate` is exactly that shape, and it answered NULL on every row.
//
//	date - date → the whole number of days between them (DuckDB: BIGINT)
//	date ± n    → the date n days away, as epoch days (DuckDB: DATE)
//
// Both are gated on the operands being whole DAYS. An instant carrying a clock
// declines and falls through to the arithmetic below, because
// timestamp-minus-timestamp is an INTERVAL in SQL and this engine has no
// interval column type to answer with — inventing a unit here is the mistake
// #319 and #322 were about. `date ± INTERVAL` is not handled here either: that
// is intervalShift, which keeps the rendered-string result #322 pinned for it.
//
// ok=false means "not date arithmetic" and leaves the caller's numeric path
// untouched — including the case where an operand is a string that does not
// parse as a date, which is how `'BUILDING' - 1` keeps its old answer.
func (e *BinOp) dateArith(b *batch.RecordBatch, row int, lv, rv any) (any, bool) {
	ld, lok := temporalOperand(b, row, e.Left, lv)
	if !lok {
		// `n + date`, the one reversed shape that means anything.
		if e.Op != "+" {
			return nil, false
		}
		n, nok := plainDayCount(lv)
		if !nok {
			return nil, false
		}
		rd, rok := temporalOperand(b, row, e.Right, rv)
		if !rok {
			return nil, false
		}
		rt, dateOnly, parsed := parseDateArg(rd)
		if !parsed || !dateOnly {
			return nil, false
		}
		return epochDaysOf(rt.AddDate(0, 0, int(n))), true
	}
	lt, lDateOnly, lparsed := parseDateArg(ld)
	if !lparsed {
		return nil, false
	}
	if rd, rok := temporalOperand(b, row, e.Right, rv); rok {
		rt, rDateOnly, rparsed := parseDateArg(rd)
		if !rparsed || e.Op != "-" || !lDateOnly || !rDateOnly {
			return nil, false
		}
		return epochDaysOf(lt) - epochDaysOf(rt), true
	}
	n, nok := plainDayCount(rv)
	if !nok || !lDateOnly {
		return nil, false
	}
	if e.Op == "-" {
		n = -n
	}
	return epochDaysOf(lt.AddDate(0, 0, int(n))), true
}

// plainDayCount reads the non-date side of `date ± n` as a whole number of
// days. A fractional float declines rather than truncating, so the caller
// falls through to ordinary arithmetic instead of quietly rounding a date.
func plainDayCount(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case float64:
		if n == math.Trunc(n) {
			return int64(n), true
		}
	case float32:
		if float64(n) == math.Trunc(float64(n)) {
			return int64(n), true
		}
	}
	return 0, false
}
