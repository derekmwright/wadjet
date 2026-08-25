package exec

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Expression computes a value for a row in a batch.
type Expression func(b *batch.RecordBatch, row int) any

// ColumnRef creates an expression that reads a column value.
// The column index is resolved on first call and cached for subsequent rows.
//
// The cache is a lazyFieldIdx and not a captured int because Project.Clone
// copies the ProjectColumn structs but SHARES this closure with every
// parallel worker — see lazyColIdx (filter.go) for the race that is.
func ColumnRef(name string) Expression {
	col := &lazyFieldIdx{}
	return func(b *batch.RecordBatch, row int) any {
		idx, field := col.get(b, name)
		if idx < 0 {
			return nil
		}
		v := b.Columns[idx]
		if field < 0 {
			return v.GetValue(row)
		}
		return rowFieldValue(v, field, row)
	}
}

// lazyFieldIdx is lazyColIdx for a name that may be a ROW FIELD PATH: it
// publishes the column index AND, for a field path, the child position within
// the container.
//
// ColumnRef resolved by b.ColumnIndex alone, so `c_row.b` answered -1 and the
// projection emitted NULL for every row — silently, and only on the STAGE
// DAG, where the fragment's OpProject is built from expression TEXT and this
// is the evaluator it gets (the single-process pipeline compiles an
// expr.ColRef, which has resolved ROW fields since #147). One query, two
// answers, decided by which path it took (#568).
type lazyFieldIdx struct {
	resolved atomic.Bool
	mu       sync.Mutex
	idx      int
	field    int
}

// get resolves name against b on first use. The order mirrors
// expr.ColRef.resolveSlow exactly — the whole dotted spelling names a column
// of its own first (a flat Zeek "id.orig_h"), then the bare name, and only
// then is the qualifier read as a ROW column — so the two evaluators can
// never disagree about WHICH value a name denotes.
func (c *lazyFieldIdx) get(b *batch.RecordBatch, name string) (int, int) {
	if c.resolved.Load() {
		return c.idx, c.field
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resolved.Load() {
		return c.idx, c.field
	}
	c.field = -1
	c.idx = b.ColumnIndex(name)
	if c.idx < 0 {
		if dot := strings.IndexByte(name, '.'); dot > 0 && dot < len(name)-1 {
			c.idx = b.ColumnIndex(name[dot+1:])
			if c.idx < 0 {
				if pi := b.ColumnIndex(name[:dot]); pi >= 0 && b.Columns[pi].Type == batch.TypeRow {
					for j, fn := range b.Columns[pi].FieldNames {
						if strings.EqualFold(fn, name[dot+1:]) && j < len(b.Columns[pi].Children) {
							c.idx, c.field = pi, j
							break
						}
					}
				}
			}
		}
	}
	c.resolved.Store(true)
	return c.idx, c.field
}

// rowFieldValue reads field j of a ROW vector at row, following a view to its
// base the way Vector.GetValue does — a view's children are not addressable
// by the view's own row index. The box is GetValue's, which is what
// ColumnRef already returns for a column.
func rowFieldValue(v *batch.Vector, field, row int) any {
	for {
		if v.Nulls.IsNullFast(row) {
			return nil
		}
		if v.Base == nil {
			break
		}
		row = int(v.Indices[row])
		v = v.Base
	}
	if field >= len(v.Children) {
		return nil
	}
	return v.Children[field].GetValue(row)
}

// Literal creates an expression that returns a constant.
func Literal(val any) Expression {
	return func(_ *batch.RecordBatch, _ int) any {
		return val
	}
}

// Float64Expression evaluates to float64 without boxing.
type Float64Expression func(b *batch.RecordBatch, row int) (float64, bool)

// Int64Expression evaluates to int64 without boxing.
type Int64Expression func(b *batch.RecordBatch, row int) (int64, bool)

// VecFloat64Expression evaluates float64 for all rows at once.
type VecFloat64Expression func(b *batch.RecordBatch, dst []float64, n int) bool

// VecExpression evaluates an entire column at once, writing to the output vector.
// More general than VecFloat64Expression — handles any output type (string, int, etc.).
type VecExpression func(b *batch.RecordBatch, out *batch.Vector, n int)

// ProjectColumn defines an output column of a projection.
type ProjectColumn struct {
	Name            string
	Type            parquet.TypeID
	Expr            Expression
	Float64Eval     Float64Expression           // optional typed path (avoids interface{} boxing)
	Int64Eval       Int64Expression             // optional typed path
	VecFloat64Eval  VecFloat64Expression        // optional vectorized path (entire column at once)
	VecFloat64Clone func() VecFloat64Expression // creates a clone with independent scratch buffers
	VecEval         VecExpression               // optional vectorized evaluation for any output type
	SourceCol       string                      // source column name for type resolution on renames
	DirectCopy      string                      // if set, bulk copy this input column (no per-row eval)
	// SourceIdx names the input column by POSITION, for a projection whose
	// source cannot be identified by name: an output list may legally carry
	// two columns of the same name (`SELECT abs(a), abs(b)` — PostgreSQL
	// calls both `abs`), and a name-keyed copy then gives the second one the
	// first one's values. SourceIdxSet is required because 0 is a valid
	// index, the same reason ProjectExprSpec carries TypeKnown.
	SourceIdx    int
	SourceIdxSet bool
	Dimension    int // VECTOR output dimensionality (e.g. embed()); 0 = not a vector
	// Computed marks an output whose value comes from Expr rather than from
	// an input column of the same name. Such an output must NOT be typed by
	// looking its own name up in the input: when the alias shadows an input
	// column, that lookup types the vector from a column the value paths
	// never read. Only the planner can tell the two apart — Expression is
	// an opaque func here.
	Computed bool
}

// Project is a UnaryOperator that selects and computes columns.
type Project struct {
	Projections       []ProjectColumn
	cachedSchema      []parquet.Column // reused across batches after first resolution
	outPool           *batch.BatchPool // output batch pool — eliminates per-batch allocation
	directSrcIdx      []int            // resolved source col indices for DirectCopy (-1 = per-row eval)
	directResolved    bool
	vecCompact        bool // true if a VecEval-only projection requires compacting away input selection
	vecCompactChecked bool
}

// needsVecCompaction reports whether any projection has a vectorized evaluator
// but no per-row typed path (e.g. embed() → VECTOR). Such a projection can only
// run through the batched VecEval path (non-sel branch); under a selection
// vector it would otherwise fall back to per-row Expr eval. Computed once.
func (p *Project) needsVecCompaction() bool {
	if !p.vecCompactChecked {
		for _, proj := range p.Projections {
			if proj.VecEval != nil && proj.Float64Eval == nil && proj.Int64Eval == nil {
				p.vecCompact = true
				break
			}
		}
		p.vecCompactChecked = true
	}
	return p.vecCompact
}

func NewProject(projections []ProjectColumn) *Project {
	return &Project{Projections: projections}
}

func (p *Project) Init(_ context.Context) error { return nil }

func (p *Project) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	schema := p.cachedSchema
	if schema == nil {
		// Resolve output types from the input batch schema rather than using the
		// pre-declared type (which may be a placeholder). This ensures float columns
		// don't get written into string vectors, which would panic.
		schema = make([]parquet.Column, len(p.Projections))
		for i, proj := range p.Projections {
			typ := proj.Type
			// An EXPLICIT source beats a same-name match, because an output
			// name can shadow a DIFFERENT input column: in
			// `SELECT n_name AS n_regionkey, n_regionkey AS r`, the first
			// output is named n_regionkey while its value comes from the
			// string n_name. Typing it by output name picked the shadowed
			// int column, and the value paths — which always read the
			// SOURCE — then wrote a string into an int vector: silent
			// zeros on the per-row path, a BulkCopy panic on the
			// DirectCopy path the DAG's fragments take (#323).
			//
			// DirectCopy resolves exactly as the bulk-copy path below does,
			// so the output vector's type always matches the vector that
			// copy writes. Its idx is -1 both for a name that resolves to
			// nothing (the copy path turns that into an error) and for a
			// ROW field, which stays on the per-row route — both fall
			// through to the resolutions below, as before.
			srcIdx := -1
			// A positional source is exact and beats every name-based
			// resolution below, which cannot tell two same-named columns
			// apart.
			if proj.SourceIdxSet && proj.SourceIdx >= 0 && proj.SourceIdx < len(in.Schema) {
				srcIdx = proj.SourceIdx
			}
			if srcIdx < 0 && proj.DirectCopy != "" {
				srcIdx, _ = resolvePlainColumn(in, proj.DirectCopy)
			}
			if srcIdx < 0 && proj.SourceCol != "" {
				// Resolve the source the way the value paths do, so a
				// renamed reference that only matches after qualifier
				// fallback (SELECT "id.orig_h" AS src over an aggregate
				// that emitted orig_h) reports the input column's type
				// instead of the planner's placeholder.
				srcIdx = columnIndexFallback(in, proj.SourceCol)
			}
			if srcIdx < 0 && !proj.Computed {
				// No explicit source resolved. A projection that is not a
				// rename names its own column here (SELECT n_name), so the
				// same-name lookup is what upgrades the planner's
				// placeholder type to the real one.
				//
				// A COMPUTED output is excluded: its value comes from Expr,
				// so an input column that happens to share its alias
				// (SELECT UPPER(n_name) AS n_regionkey) describes a
				// different column entirely. Typing from it produced an
				// all-zero int column for UPPER/SUBSTR and a panic for
				// concatenation, whose vectorized kernel writes into
				// BytesData the mistyped vector does not have (#327).
				srcIdx = in.ColumnIndex(proj.Name)
			}
			if srcIdx >= 0 {
				typ = in.Schema[srcIdx].Type
			}
			col := parquet.Column{
				Name:     proj.Name,
				Type:     typ,
				Nullable: true,
			}
			// A ROW FIELD PATH resolves to no column INDEX at all — the
			// value comes out of a container, not out of a column — so
			// every resolution above leaves the planner's placeholder
			// standing, and that placeholder is STRING. An INT64 field
			// then came back as the string "9", a CIDR field sorted by
			// its stored text, and pgwire declared OID 25 for both
			// (#568). The parent ROW carries the field's whole
			// declaration; take it wholesale, so a DECIMAL field keeps
			// its (p,s) and a nested field its own Fields.
			if srcIdx < 0 {
				if fc, ok := fieldPathColumn(in, projSourceName(proj)); ok {
					fc.Name, fc.Nullable = proj.Name, true
					schema[i] = fc
					continue
				}
			}
			// Preserve type metadata for parameterized types — including
			// nested structure: without Fields/ElementType the pooled
			// destination vectors have nil Children/Child and every nested
			// value was silently dropped (rows still marked valid).
			if srcIdx >= 0 {
				col.Dimension = in.Schema[srcIdx].Dimension
				col.Scale = in.Schema[srcIdx].Scale
				col.Precision = in.Schema[srcIdx].Precision
				col.Fields = in.Schema[srcIdx].Fields
				col.ElementType = in.Schema[srcIdx].ElementType
			}
			// Computed VECTOR projections (e.g. embed(text)) don't resolve to an
			// input column, so carry their dimension from the projection itself.
			// This sizes the pooled output vector's Float32Data so both the
			// batched VecEval path and the per-row SetVector path can write.
			if col.Type == parquet.TypeVector && col.Dimension == 0 && proj.Dimension > 0 {
				col.Dimension = proj.Dimension
			}
			schema[i] = col
		}
		p.cachedSchema = schema
	}

	// VecEval-only projections (e.g. embed() → VECTOR) have no per-row typed
	// path, so the selection-vector branch below would fall back to one-row-at-
	// a-time Expr eval — for embed() that is one provider API call per row,
	// defeating SQL-level batching, and the per-row SetVector cannot self-
	// correct the output dimension (leaving rows valid-but-empty). Compact the
	// selection away so the batched VecEval path runs. The copy is paid only
	// when such a projection is present (the common filtered-numeric case is
	// untouched).
	if in.Sel != nil && p.needsVecCompaction() {
		in = in.Compact()
	}

	activeLen := in.ActiveLen()
	// Use pooled output batch to avoid per-batch allocation. Downstream sinks
	// that store batches (Sort) call Detach() which unlinks from the pool,
	// while sinks that only read (Aggregate) allow proper pool recycling.
	if p.outPool == nil {
		p.outPool = batch.NewBatchPool(schema, batch.DefaultBatchSize)
	}
	out := p.outPool.GetForSize(activeLen)

	// Resolve DirectCopy column indices on first batch.
	if !p.directResolved {
		p.directSrcIdx = make([]int, len(p.Projections))
		for j, proj := range p.Projections {
			p.directSrcIdx[j] = -1
			if proj.SourceIdxSet && proj.SourceIdx >= 0 && proj.SourceIdx < len(in.Columns) {
				p.directSrcIdx[j] = proj.SourceIdx
				continue
			}
			if proj.DirectCopy != "" {
				idx, ok := resolvePlainColumn(in, proj.DirectCopy)
				if !ok {
					// A projected plain column that resolves to NOTHING —
					// not bare, not qualifier-stripped, not a ROW field —
					// previously emitted an all-NULL column, so a typo'd
					// SELECT item looked like a real (empty) column
					// (issue #147). Expression projections and the
					// fallback resolutions stay on the per-row eval path.
					return nil, fmt.Errorf("column %q does not exist in the input schema", proj.DirectCopy)
				}
				p.directSrcIdx[j] = idx
			}
		}
		p.directResolved = true
	}

	// Use typed evaluation paths per-column to avoid interface{} boxing.
	// This applies to both selection-vector and non-sel paths.
	if in.Sel != nil {
		for j, proj := range p.Projections {
			col := out.Columns[j]
			if srcIdx := p.directSrcIdx[j]; srcIdx >= 0 {
				projectGatherColumn(col, in.Columns[srcIdx], in.Sel)
			} else if proj.Float64Eval != nil {
				for outRow, idx := range in.Sel {
					v, ok := proj.Float64Eval(in, int(idx))
					if ok {
						col.Float64Data[outRow] = v
					} else {
						col.Nulls.SetNull(outRow)
					}
				}
			} else if proj.Int64Eval != nil {
				for outRow, idx := range in.Sel {
					v, ok := proj.Int64Eval(in, int(idx))
					if ok {
						col.Int64Data[outRow] = v
					} else {
						col.Nulls.SetNull(outRow)
					}
				}
			} else {
				for outRow, idx := range in.Sel {
					col.SetValue(outRow, proj.Expr(in, int(idx)))
				}
			}
		}
	} else {
		for j, proj := range p.Projections {
			col := out.Columns[j]
			if srcIdx := p.directSrcIdx[j]; srcIdx >= 0 {
				projectCopyColumn(col, in.Columns[srcIdx], in.Len)
			} else if proj.VecEval != nil {
				proj.VecEval(in, col, in.Len)
			} else if proj.VecFloat64Eval != nil {
				// VecFloat64Eval returns hasNull when any input row is null;
				// when that happens the float buffer holds a placeholder (0)
				// for those rows, so fall back to per-row Float64Eval to set
				// the null bits on the output vector. Otherwise NULL/x silently
				// emits 0 and the projection swallows the null.
				if proj.VecFloat64Eval(in, col.Float64Data, in.Len) && proj.Float64Eval != nil {
					for i := 0; i < in.Len; i++ {
						if _, ok := proj.Float64Eval(in, i); !ok {
							col.Nulls.SetNull(i)
						}
					}
				}
			} else if proj.Float64Eval != nil {
				for i := 0; i < in.Len; i++ {
					v, ok := proj.Float64Eval(in, i)
					if ok {
						col.Float64Data[i] = v
					} else {
						col.Nulls.SetNull(i)
					}
				}
			} else if proj.Int64Eval != nil {
				for i := 0; i < in.Len; i++ {
					v, ok := proj.Int64Eval(in, i)
					if ok {
						col.Int64Data[i] = v
					} else {
						col.Nulls.SetNull(i)
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					col.SetValue(i, proj.Expr(in, i))
				}
			}
		}
	}

	return out, nil
}

func (p *Project) Close() error { return nil }

// resolvePlainColumn resolves a projected plain-column reference against the
// input schema and reports whether it is servable at all.
//
// idx >= 0 is a bulk-copy source. idx == -1 with ok=true means the name is
// reachable only through per-row evaluation (a ROW field like
// "attrs.score"). ok=false means the name resolves to NOTHING, which the
// caller turns into an error rather than an all-NULL column (issue #147).
//
// The plain-column lookup is columnIndexFallback — the same bidirectional
// qualified↔bare resolution every other operator uses. A join emits the
// self-joined table copy that lands on the PROBE side under its bare name
// while qualifying the build side ("n_name" + "n2.n_name"), so a downstream
// reference to "n1.n_name" only resolves after the qualifier strip. Without
// it the projection fell to the per-row ColumnRef path, which misses the
// same way and silently emits NULLs (#314: Q07's supp_nation). The output
// TYPE for such a rename already resolved through the same fallback in the
// schema pass above; this makes the value path agree with it.
//
// The ROW-parent check runs BEFORE the fallback so "attrs.score" keeps
// extracting the ROW field even when an unrelated bare "score" column is
// also in scope.
// projSourceName is the input spelling a projection reads its value from,
// resolved in the same order the schema pass resolves its source index:
// the bulk-copy name, then the rename's source, then the output's own name
// for a projection that is not computed.
func projSourceName(proj ProjectColumn) string {
	if proj.DirectCopy != "" {
		return proj.DirectCopy
	}
	if proj.SourceCol != "" {
		return proj.SourceCol
	}
	if !proj.Computed {
		return proj.Name
	}
	return ""
}

// fieldPathColumn resolves name as a ROW FIELD PATH against b and returns the
// FIELD's declaration — the answer resolvePlainColumn cannot give, because a
// field is not a column and has no index.
//
// The resolution order mirrors expr.ColRef.resolveSlow step for step, because
// that is what actually produces the VALUE: the full dotted spelling names a
// column of its own first (a flat Zeek "id.orig_h"), then the bare name, and
// only then is the qualifier read as a ROW column. A type resolved in a
// different order than the value would describe a different column.
//
// The declared schema answers first because it is the richer half — a DECIMAL
// field's (p,s), a nested field's own Fields — and the child VECTOR is the
// fallback for a batch whose schema lost that metadata.
func fieldPathColumn(b *batch.RecordBatch, name string) (parquet.Column, bool) {
	dot := strings.IndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 {
		return parquet.Column{}, false
	}
	if b.ColumnIndex(name) >= 0 || b.ColumnIndex(name[dot+1:]) >= 0 {
		return parquet.Column{}, false
	}
	pi := b.ColumnIndex(name[:dot])
	if pi < 0 || pi >= len(b.Columns) || b.Columns[pi].Type != batch.TypeRow {
		return parquet.Column{}, false
	}
	field := name[dot+1:]
	if pi < len(b.Schema) {
		if fc, ok := b.Schema[pi].Field(field); ok {
			return fc, true
		}
	}
	parent := b.Columns[pi]
	for j, fn := range parent.FieldNames {
		if !strings.EqualFold(fn, field) || j >= len(parent.Children) {
			continue
		}
		child := parent.Children[j]
		return parquet.Column{
			Name:      field,
			Type:      child.Type,
			Nullable:  true,
			Scale:     child.DecimalData.Scale,
			Dimension: child.VectorDim,
		}, true
	}
	return parquet.Column{}, false
}

func resolvePlainColumn(b *batch.RecordBatch, name string) (int, bool) {
	if idx := b.ColumnIndex(name); idx >= 0 {
		return idx, true
	}
	if dot := strings.IndexByte(name, '.'); dot > 0 {
		if pi := b.ColumnIndex(name[:dot]); pi >= 0 && b.Columns[pi].Type == batch.TypeRow {
			return -1, true
		}
	}
	if idx := columnIndexFallback(b, name); idx >= 0 {
		return idx, true
	}
	return -1, false
}

// Clone returns a new Project that shares the same (immutable) projections.
// Each clone gets its own pool (created lazily on first Execute).
func (p *Project) Clone() UnaryOperator {
	cloned := make([]ProjectColumn, len(p.Projections))
	copy(cloned, p.Projections)
	for i, proj := range cloned {
		if proj.VecFloat64Clone != nil {
			cloned[i].VecFloat64Eval = proj.VecFloat64Clone()
		}
	}
	return &Project{Projections: cloned}
}

// projectCopyColumn bulk-copies n rows from src to dst using type-specific memcpy.
// Eliminates per-row function call overhead for pass-through column projections.
func projectCopyColumn(dst, src *batch.Vector, n int) {
	switch src.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		copy(dst.Int64Data[:n], src.Int64Data[:n])
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		copy(dst.Int32Data[:n], src.Int32Data[:n])
	case batch.TypeFloat64:
		copy(dst.Float64Data[:n], src.Float64Data[:n])
	case batch.TypeFloat32:
		copy(dst.Float32Data[:n], src.Float32Data[:n])
	case batch.TypeBool:
		copy(dst.BoolData[:n], src.BoolData[:n])
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.BulkCopy(0, &src.BytesData, 0, n)
	case batch.TypeDecimal:
		copy(dst.DecimalData.Data[:n], src.DecimalData.Data[:n])
	case batch.TypeVector:
		dim := src.VectorDim
		if dim > 0 {
			dst.VectorDim = dim
			copy(dst.Float32Data[:n*dim], src.Float32Data[:n*dim])
		}
	default:
		// Fallback for nested/unknown types: per-row copy
		for i := 0; i < n; i++ {
			dst.SetValue(i, src.GetValue(i))
		}
	}
	dst.Nulls.CopyFrom(&src.Nulls, n)
}

// GatherColumn copies selected rows from src to contiguous dst positions.
// Exported for use by aggPreProject materialize.
func GatherColumn(dst, src *batch.Vector, sel []uint32) {
	projectGatherColumn(dst, src, sel)
}

// projectGatherColumn copies selected rows from src to contiguous dst positions.
// Used when the input has a selection vector.
func projectGatherColumn(dst, src *batch.Vector, sel []uint32) {
	switch src.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for i, idx := range sel {
			dst.Int64Data[i] = src.Int64Data[idx]
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for i, idx := range sel {
			dst.Int32Data[i] = src.Int32Data[idx]
		}
	case batch.TypeFloat64:
		for i, idx := range sel {
			dst.Float64Data[i] = src.Float64Data[idx]
		}
	case batch.TypeFloat32:
		for i, idx := range sel {
			dst.Float32Data[i] = src.Float32Data[idx]
		}
	case batch.TypeBool:
		for i, idx := range sel {
			dst.BoolData[i] = src.BoolData[idx]
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		// Pre-calculate total byte size to avoid growslice
		totalBytes := 0
		srcOff := src.BytesData.Offsets
		for _, idx := range sel {
			totalBytes += int(srcOff[idx+1] - srcOff[idx])
		}
		dst.BytesData.PreAllocBytes(totalBytes)
		for i, idx := range sel {
			dst.BytesData.SetFrom(i, &src.BytesData, int(idx))
		}
	case batch.TypeDecimal:
		for i, idx := range sel {
			dst.DecimalData.Data[i] = src.DecimalData.Data[idx]
		}
	case batch.TypeVector:
		dim := src.VectorDim
		if dim > 0 {
			dst.VectorDim = dim
			for i, idx := range sel {
				srcOff := int(idx) * dim
				dstOff := i * dim
				copy(dst.Float32Data[dstOff:dstOff+dim], src.Float32Data[srcOff:srcOff+dim])
			}
		}
	default:
		for i, idx := range sel {
			dst.SetValue(i, src.GetValue(int(idx)))
		}
	}
	// Gather null bits
	srcWords := src.Nulls.Words()
	if src.Nulls.HasNulls() {
		for i, idx := range sel {
			if srcWords[idx/64]&(1<<(idx%64)) == 0 {
				dst.Nulls.SetNull(i)
			}
		}
	}
}

// ArithExpr creates an arithmetic expression between two expressions.
func ArithExpr(left, right Expression, op string) Expression {
	return func(b *batch.RecordBatch, row int) any {
		lv := left(b, row)
		rv := right(b, row)
		if lv == nil || rv == nil {
			return nil
		}

		lf := toFloat64(lv)
		rf := toFloat64(rv)

		switch op {
		case "+":
			return lf + rf
		case "-":
			return lf - rf
		case "*":
			return lf * rf
		case "/":
			if rf == 0 {
				// Operands are non-NULL here: a genuine zero divisor is a
				// query error (SQLSTATE 22012), not a NULL (#367).
				panic(fatalEvalError{sqlerr.New("22012", "division by zero")})
			}
			return lf / rf
		default:
			return nil // unknown op → SQL NULL (validated at plan time)
		}
	}
}
