// This file holds computed columns for the physical planner, governed by ADR-0026.
package physical

import (
	"context"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"strings"
)

// NewComputedColumnsOp returns an operator that passes every input column
// through and appends the computed ones.
//
// The type is aggPreProject, named for its first caller. It is exported
// through a constructor rather than moved because it has a second caller now
// with the same need and none of the aggregate's context: the window
// fragment, which must compute an expression PARTITION BY key before
// exec.Window can resolve it by name (#585) and which — like the pre-
// aggregate projection — cannot narrow the batch, since the window's output
// is every input column plus its own.
func NewComputedColumnsOp(cols []exec.ProjectColumn) exec.UnaryOperator {
	return NewComputedColumnsOpWithMeta(cols, nil)
}

// NewComputedColumnsOpWithMeta is NewComputedColumnsOp plus the full
// declaration of each computed column, for a caller that has one.
//
// exec.ProjectColumn carries a bare TypeID plus (p,s) and a VECTOR dimension,
// which is enough for the arithmetic these columns were built for and not
// enough for a ROW FIELD PATH of a container type: without Fields/ElementType
// the computed vector is minted with nil Children/Child and every value
// written into it is dropped (#568 for the aggregate's pre-projection, #618
// for the window's keys). A nil meta is the pre-#568 contract — "Name/Type is
// the whole declaration" — and is what NewComputedColumnsOp's other caller,
// the worker's fragment builder, passes.
func NewComputedColumnsOpWithMeta(cols []exec.ProjectColumn, meta []parquet.Column) exec.UnaryOperator {
	// shareOutputs, which here means per-CALL computed vectors rather than
	// the pooled ones. The aggregate consumes each batch's values before the
	// next Execute overwrites them; the window RETAINS every batch it is
	// given (Consume detaches and buffers), so a pooled vector would leave
	// every stored batch pointing at the LAST batch's key values — one
	// constant key column, and a window over one partition. That is #585's
	// symptom produced by its own fix, and it showed up the moment the
	// window's input was an aggregate emitting a batch per group.
	return &aggPreProject{computed: cols, meta: meta, shareOutputs: true}
}

// aggPreProject is a UnaryOperator that passes through all input columns
// and adds computed expression columns for aggregate inputs.
type aggPreProject struct {
	computed []exec.ProjectColumn
	// meta, when set, is the full declaration of each computed column,
	// aligned with computed. An entry is PRESENT when its Name is set — not
	// when its Type is non-zero, which would have read a declared BOOL
	// (parquet.TypeBool is TypeID 0) as "no declaration", the same
	// zero-value-versus-unset collision ProjectExprSpec.TypeKnown exists for
	// (#445). exec.ProjectColumn carries a bare TypeID, which
	// is enough for the arithmetic these columns were built for and not
	// enough for a ROW FIELD PATH of a parameterized type: a DECIMAL field
	// needs its (p,s) and a container field its own shape, or the output
	// vector reads back wrong (#568). Empty means "Name/Type is the whole
	// declaration", which is what every pre-#568 caller passes.
	meta              []parquet.Column
	cachedSchema      []parquet.Column   // cached output schema (computed once)
	cachedOutput      *batch.RecordBatch // most recent output (NOT reused — fresh struct each Execute call to avoid clobbering downstream's stored references; only the underlying Vectors are pooled via computedVectors)
	computedVectors   []*batch.Vector    // pooled computed-column vectors (reused across calls, sized to computedCap)
	computedCap       int                // row capacity of cached computed vectors
	canPassSelThrough bool               // true if all computed columns are numeric (no BytesColumn)
	checkedSelPass    bool               // true after first call resolves canPassSelThrough
	matPool           *batch.BatchPool   // pool for materialize buffers (avoids per-call allocation)
	shareOutputs      bool               // per-call vector allocation (partitioned-agg sharing)
}

// rowFieldDecl resolves name as a ROW FIELD PATH against b and returns the
// FIELD's declaration. It answers only when the qualifier really names a ROW
// column of the batch — `batch.RowFieldPath` is the one place that question is
// settled for the whole engine — so a qualified reference to a PLAIN column
// (`p.g`) falls through untouched.
//
// The batch's own schema is the source, not the vector: a worker's input comes
// from a scan, which carries the declaration. A batch whose schema lost it is
// left to the caller's fallback rather than reconstructed here, because the
// only producer that loses it is a spill file, and that is fixed at the file
// (#865).
func rowFieldDecl(b *batch.RecordBatch, name string) (parquet.Column, bool) {
	dot := strings.IndexByte(name, '.')
	if dot < 0 {
		return parquet.Column{}, false
	}
	pi, _, ok := b.RowFieldPath(name)
	if !ok || pi >= len(b.Schema) {
		return parquet.Column{}, false
	}
	return b.Schema[pi].Field(name[dot+1:])
}

// columnMeta is the declaration of computed column k: the full parquet.Column
// when the builder supplied one, else the field's own when the column names a
// ROW FIELD PATH it computed from, else the Name/Type pair every caller before
// #568 relied on.
func (a *aggPreProject) columnMeta(in *batch.RecordBatch, k int, c exec.ProjectColumn) parquet.Column {
	if k < len(a.meta) && a.meta[k].Name != "" {
		m := a.meta[k]
		m.Name, m.Nullable = c.Name, true
		return m
	}
	// A builder with no catalog cannot supply meta: the DAG's worker has the
	// stage spec's TEXT and nothing else. It names the SOURCE it computed
	// from instead, and the field's declaration is read off the parent ROW in
	// the batch — the same ladder exec.Project walks for the same reason
	// (#568). Without it a windowed container field path on a worker got an
	// output vector with no children and dropped every value, while the
	// single-process pipeline (which does have meta) answered (#618).
	if in != nil && c.SourceCol != "" {
		if fc, ok := rowFieldDecl(in, c.SourceCol); ok {
			fc.Name, fc.Nullable = c.Name, true
			return fc
		}
	}
	col := parquet.Column{Name: c.Name, Type: c.Type, Nullable: true}
	if c.Type == parquet.TypeVector {
		col.Dimension = c.Dimension
	}
	if c.Type == parquet.TypeDecimal {
		// A computed DECIMAL's (p,s), for the same reason a computed
		// VECTOR's dimension rides here: nothing downstream can recover it,
		// and a DECIMAL vector with no scale reads every value back at 10^0
		// (ADR-0024 item 2).
		col.Precision, col.Scale = c.Precision, c.Scale
	}
	return col
}

func (a *aggPreProject) Init(_ context.Context) error { return nil }

// ReusesOutputBuffers marks the pre-projection's computed vectors as reused
// across Execute calls — its batches must not be shared across partition
// owners (exec.BufferReusingOperator) unless shared-output mode is on.
func (a *aggPreProject) ReusesOutputBuffers() bool { return !a.shareOutputs }

// EnableSharedOutputs switches to per-call computed-vector allocation so
// this op's batches can be shared across partition owners
// (exec.OutputSharingAware). Costs the pooled-buffer reuse; only the
// partitioned-aggregation pipeline enables it.
func (a *aggPreProject) EnableSharedOutputs() { a.shareOutputs = true }

// Clone returns a copy with deep-cloned VecFloat64Eval expression trees.
// Each parallel worker must have its own BinOpFloat64.vecBuf scratch buffers
// to avoid data races during concurrent vectorized evaluation.
func (a *aggPreProject) Clone() exec.UnaryOperator {
	clonedComputed := make([]exec.ProjectColumn, len(a.computed))
	copy(clonedComputed, a.computed)
	for i, c := range clonedComputed {
		if c.VecFloat64Clone != nil {
			clonedComputed[i].VecFloat64Eval = c.VecFloat64Clone()
		}
	}
	return &aggPreProject{computed: clonedComputed, meta: a.meta, shareOutputs: a.shareOutputs}
}

func (a *aggPreProject) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	// Check once whether all computed columns are numeric. If so, we can keep
	// the selection vector and write computed values at sparse indices — avoiding
	// the full materialize copy. BytesColumn.Set requires sequential writes,
	// so string-typed computed columns still need materialize.
	if !a.checkedSelPass {
		a.checkedSelPass = true
		a.canPassSelThrough = true
		for _, c := range a.computed {
			switch c.Type {
			case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
				a.canPassSelThrough = false
			}
		}
	}

	hasSel := in.Sel != nil
	if hasSel && !a.canPassSelThrough {
		in = a.materialize(in)
		hasSel = false
	}
	// When vectorized eval is available, materialize to get the dense non-sel
	// path. The materialize copy cost (~24KB for 500 rows × 6 cols) is far
	// cheaper than per-row Float64Eval (0.65s vs 0.07s vectorized at SF1).
	if hasSel && a.hasVecFloat64() {
		in = a.materialize(in)
		hasSel = false
	}
	// Same trade for the exact fixed-point kernel: it writes rows 0..n-1
	// densely, so it needs the selection compacted away. Paying one gather
	// buys back four allocations per computed DECIMAL cell — and it also
	// keeps the kernel from raising this expression's 22003/22012 for a row
	// the filter excluded (#705).
	if hasSel && a.hasVecDecimal() {
		in = a.materialize(in)
		hasSel = false
	}

	// Cache output schema on first call (avoids per-batch allocation)
	if a.cachedSchema == nil {
		schema := make([]parquet.Column, 0, len(in.Schema)+len(a.computed))
		schema = append(schema, in.Schema...)
		for k, c := range a.computed {
			schema = append(schema, a.columnMeta(in, k, c))
		}
		a.cachedSchema = schema
	}

	computedOffset := len(in.Schema)

	// We reuse the COMPUTED vectors across Execute calls (the typed data
	// slices and null bitmaps are sized to a.computedCap) but we MUST NOT
	// reuse the *RecordBatch struct itself, because downstream sinks may
	// store batch pointers across calls (CollectSink does, the reverse-
	// bloom bridge does, etc.). Mutating a previously-returned batch's
	// Len / Columns silently corrupts whatever the sink has — manifesting
	// as Q05's panic in CollectSink.ToRows when the sink iterated a batch
	// whose Len had been bumped past the underlying column data's length
	// by a subsequent Execute call.
	//
	// Allocate a fresh RecordBatch struct every call. The struct itself is
	// tiny (header only); the heavy state (Vectors, BytesColumn buffers)
	// is still pooled via a.computedVectors.
	if a.shareOutputs || a.computedVectors == nil || in.Len > a.computedCap {
		a.computedVectors = make([]*batch.Vector, len(a.computed))
		for k, c := range a.computed {
			a.computedVectors[k] = batch.NewColumnVector(a.columnMeta(in, k, c), in.Len)
		}
		a.computedCap = in.Len
	} else {
		// Reuse computed vectors — reset null bitmaps and bytes columns
		for k := range a.computed {
			col := a.computedVectors[k]
			col.Len = in.Len
			col.Nulls.ResetNonNull(in.Len)
			switch col.Type {
			case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
				col.BytesData.Reset()
			}
		}
	}

	cols := make([]*batch.Vector, len(a.cachedSchema))
	// Pass-through columns share Vector pointers with the input. Detach the
	// input from its pool so prev.Release() becomes a no-op — the shared
	// column data must remain valid while this output is consumed downstream.
	in.Detach()
	for j := 0; j < len(in.Schema); j++ {
		cols[j] = in.Columns[j]
	}
	for k := range a.computed {
		cols[computedOffset+k] = a.computedVectors[k]
	}
	a.cachedOutput = &batch.RecordBatch{
		Schema:  a.cachedSchema,
		Columns: cols,
		Len:     in.Len,
		Sel:     in.Sel,
	}

	// Compute expression columns.
	// When selection vector is present and all computed columns are numeric,
	// write values only at selected indices (avoiding full materialize).
	for k, c := range a.computed {
		col := a.computedVectors[k]
		if hasSel {
			if col.Type == parquet.TypeDecimal {
				// The checked writer for a value-producing DECIMAL store,
				// the same rule and the same ordering exec.Project applies: a
				// materialized GROUP BY key or aggregate input over
				// GREATEST/COALESCE/CASE is a VALUE, and SetValue's
				// saturating DECIMAL arms would make a wrong one silently
				// (ADR-0024 item 4).
				exec.DecimalBoxedCells.Add(int64(len(in.Sel)))
				for _, idx := range in.Sel {
					if err := col.SetComputedChecked(int(idx), c.Expr(in, int(idx))); err != nil {
						return nil, err
					}
				}
			} else if c.Float64Eval != nil {
				for _, idx := range in.Sel {
					v, ok := c.Float64Eval(in, int(idx))
					if ok {
						col.Float64Data[idx] = v
					} else {
						col.Nulls.SetNull(int(idx))
					}
				}
			} else if c.Int64Eval != nil {
				for _, idx := range in.Sel {
					v, ok := c.Int64Eval(in, int(idx))
					if ok {
						col.Int64Data[idx] = v
					} else {
						col.Nulls.SetNull(int(idx))
					}
				}
			} else {
				for _, idx := range in.Sel {
					col.SetValue(int(idx), c.Expr(in, int(idx)))
				}
			}
		} else {
			if col.Type == parquet.TypeDecimal {
				// Exact fixed-point arithmetic writes carriers directly:
				// no box to check, because nothing is converted (ADR-0024
				// item 3). Its own errors travel the per-row panic channel.
				// A false report means the exact mode did not apply to this
				// batch, and the checked writer below answers instead --
				// the same order exec.Project uses (#705, #825).
				if c.VecDecimalEval != nil && c.VecDecimalEval(in, col, in.Len) {
					continue
				}
				exec.DecimalBoxedCells.Add(int64(in.Len))
				for i := 0; i < in.Len; i++ {
					if err := col.SetComputedChecked(i, c.Expr(in, i)); err != nil {
						return nil, err
					}
				}
			} else if c.VecEval != nil {
				c.VecEval(in, col, in.Len)
			} else if c.VecFloat64Eval != nil {
				c.VecFloat64Eval(in, col.Float64Data, in.Len)
			} else if c.Float64Eval != nil {
				for i := 0; i < in.Len; i++ {
					v, ok := c.Float64Eval(in, i)
					if ok {
						col.Float64Data[i] = v
					} else {
						col.Nulls.SetNull(i)
					}
				}
			} else if c.Int64Eval != nil {
				for i := 0; i < in.Len; i++ {
					v, ok := c.Int64Eval(in, i)
					if ok {
						col.Int64Data[i] = v
					} else {
						col.Nulls.SetNull(i)
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					col.SetValue(i, c.Expr(in, i))
				}
			}
		}
	}

	return a.cachedOutput, nil
}

// materialize compacts a batch with a selection vector into a dense batch
// with only the selected rows, removing the selection vector.
// Uses a pooled batch to avoid per-call allocation overhead. GatherColumn
// handles both data and null bitmap gathering internally.
func (a *aggPreProject) materialize(in *batch.RecordBatch) *batch.RecordBatch {
	n := len(in.Sel)
	if a.matPool == nil {
		a.matPool = batch.NewBatchPool(in.Schema, batch.DefaultBatchSize)
	}
	out := a.matPool.GetForSize(n)
	for j := range in.Schema {
		exec.GatherColumn(out.Columns[j], in.Columns[j], in.Sel)
	}
	return out
}

// hasVecFloat64 returns true if any computed column has vectorized eval.
func (a *aggPreProject) hasVecFloat64() bool {
	for _, c := range a.computed {
		if c.VecEval != nil || c.VecFloat64Eval != nil {
			return true
		}
	}
	return false
}

// hasVecDecimal reports whether any computed column carries the exact
// fixed-point vector kernel.
func (a *aggPreProject) hasVecDecimal() bool {
	for _, c := range a.computed {
		if c.VecDecimalEval != nil {
			return true
		}
	}
	return false
}

func (a *aggPreProject) Close() error { return nil }
