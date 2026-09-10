// This file holds expr leaf; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

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
// Four spellings resolve, in order: the name exactly as written; the bare
// reference the stream spells qualified; a `row.field` path whose qualifier
// names a ROW column of the batch THAT DECLARES THE FIELD; and only then the
// bare column left after dropping a table qualifier.
func ResolveColumnRef(b *batch.RecordBatch, name string) (idx int, structField string) {
	// ResolveColumnIndex, not ColumnIndex: the reference arrives FOLDED from
	// the lexer (#731) and the batch may carry the catalog's own CamelCase
	// spelling (`WatchID`), which is byte-exact everywhere else. The rule and
	// why a delimited reference does NOT fold are in
	// internal/engine/batch/schema.go.
	idx = b.ResolveColumnIndex(name)
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
	// A `row.field` path, asked BEFORE the qualifier is stripped (ADR-0022:
	// a ROW field path is not a column reference).
	//
	// Stripping first answers with whatever OTHER relation in the stream
	// publishes a column of the FIELD's name, which is a different column's
	// values under the reference the query wrote:
	//
	//	SELECT n.id, c_row.b FROM typemx_nested n JOIN decpair d ON n.id = d.id
	//	-- the field's values 11, NULL, NULL, 44, … ; wadjet answered
	//	--   decpair.b's DECIMALs on all four arms, in silence (#769)
	//
	// The container is looked up with the same qualified fallback the scalar
	// branches use, because a JOIN qualifies a colliding column and `c_row.b`
	// then has to find `x.c_row`.
	//
	// The container must DECLARE the field. That is what keeps the reorder
	// off an ordinary qualified reference whose qualifier happens to name a
	// ROW column of the stream: a relation aliased like a container is still
	// read as a relation, exactly as before, and a field path naming no field
	// keeps the answer it had (#604 is untouched).
	if pi, _, ok := b.RowFieldPath(name); ok {
		return pi, parts[1]
	}
	// Try unqualified (strip table prefix)
	if idx = b.ResolveColumnIndex(parts[1]); idx >= 0 {
		return idx, ""
	}
	// A ROW container that does not declare the field: the path still names
	// the container, and #604's NULL is what answers for it — unchanged.
	parentIdx := b.ResolveColumnIndex(parts[0])
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
