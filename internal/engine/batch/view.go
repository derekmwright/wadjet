package batch

// View (dictionary) vectors — the late-materialization primitive.
//
// A view vector holds no typed storage: logical row i resolves to Base row
// Indices[i]. Creating a view over N rows costs one uint32 slice + one null
// bitmap instead of copying every column value; Flatten performs the exact
// gather that eager materialization would have done, moved to first touch.
// Gathering FROM a view composes index arrays (see NewViewVector), so a
// column passing through a chain of joins is copied once, at its final
// consumer, instead of once per join.
//
// Ownership contract: a view does NOT keep Base alive — whoever mints the
// view must guarantee Base's storage outlives it (the hash join pins its
// probe input and already owns its build batches). Views are read-only;
// writers must Flatten first. See docs/design/late-materialization.md.

// NewViewVector creates a view over base addressing rows through indices.
// The indices slice is adopted, not copied — callers must not mutate it
// afterwards. If base is itself a view, the indirection is composed away
// (newIndices[i] = base.Indices[indices[i]], own-nulls folded), so Base on
// the returned vector is always an owned vector.
//
// The view starts all-valid: row i's nullness defers to base through
// indices. Callers that need null-injection (outer-join fill) mark rows
// with v.Nulls.SetNull(i); those rows' index values are ignored.
func NewViewVector(base *Vector, indices []uint32) *Vector {
	v, _ := NewViewVectorReuse(base, indices, nil)
	return v
}

// NewViewVectorReuse is NewViewVector with a caller-owned composition buffer.
//
// When base is itself a view its indirection has to be folded into a NEW
// index array, and at join-emit widths that array is a large-object
// allocation per column per output batch — one of the spans the Go heap lock
// serializes on. Passing composeBuf lets a caller that owns the resulting
// view's lifetime keep that array across batches. The returned slice is the
// array the view ADOPTED: it stays live for as long as the view does, so only
// a caller that knows the view is dead may re-pass it (see probeEmitBuf's
// ownership rule in package exec). nil comes back when no composition was
// needed and `indices` was adopted directly.
func NewViewVectorReuse(base *Vector, indices, composeBuf []uint32) (*Vector, []uint32) {
	v := &Vector{
		Type:  base.Type,
		Len:   len(indices),
		Nulls: NewBitmap(len(indices)),
	}
	var adopted []uint32
	if base.Base != nil {
		// Compose: fold the base view's indirection and own-nulls into this
		// view so Base is always owned and reads are single-hop.
		composed := composeBuf
		if cap(composed) >= len(indices) {
			composed = composed[:len(indices)]
		} else {
			composed = make([]uint32, len(indices))
		}
		for i, x := range indices {
			composed[i] = base.Indices[x]
			if base.Nulls.IsNullFast(int(x)) {
				v.Nulls.SetNull(i)
			}
		}
		indices = composed
		adopted = composed
		base = base.Base
	}
	v.Base = base
	v.Indices = indices
	// Mirror metadata that generic code inspects without touching storage.
	v.VectorDim = base.VectorDim
	v.DecimalData.Scale = base.DecimalData.Scale
	v.FieldNames = base.FieldNames
	return v, adopted
}

// IsView reports whether the vector is a view (owns no typed storage).
func (v *Vector) IsView() bool {
	return v.Base != nil
}

// Flatten materializes a view in place: owned storage is allocated, values
// are gathered from Base through Indices (own-null rows become nulls), and
// the view fields are cleared. Aliases of the *Vector see the flattened
// form. No-op on owned vectors.
func (v *Vector) Flatten() {
	base := v.Base
	if base == nil {
		return
	}
	n := len(v.Indices)
	idx := v.Indices
	own := v.Nulls
	ownNulls := own.HasNulls()
	baseNulls := base.Nulls.HasNulls()
	flat := newOwnedLike(base, n)
	// Typed kernels for the flat-storage types (per the no-per-row-type-switch
	// rule); nested and VECTOR shapes take the generic per-value copier.
	switch base.Type {
	case TypeBool:
		if !ownNulls && !baseNulls {
			for di, si := range idx {
				flat.BoolData[di] = base.BoolData[si]
			}
		} else {
			for di, si := range idx {
				if (ownNulls && own.IsNullFast(di)) || (baseNulls && base.Nulls.IsNullFast(int(si))) {
					flat.Nulls.SetNull(di)
					continue
				}
				flat.BoolData[di] = base.BoolData[si]
			}
		}
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		if !ownNulls && !baseNulls {
			for di, si := range idx {
				flat.Int32Data[di] = base.Int32Data[si]
			}
		} else {
			for di, si := range idx {
				if (ownNulls && own.IsNullFast(di)) || (baseNulls && base.Nulls.IsNullFast(int(si))) {
					flat.Nulls.SetNull(di)
					continue
				}
				flat.Int32Data[di] = base.Int32Data[si]
			}
		}
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		if !ownNulls && !baseNulls {
			for di, si := range idx {
				flat.Int64Data[di] = base.Int64Data[si]
			}
		} else {
			for di, si := range idx {
				if (ownNulls && own.IsNullFast(di)) || (baseNulls && base.Nulls.IsNullFast(int(si))) {
					flat.Nulls.SetNull(di)
					continue
				}
				flat.Int64Data[di] = base.Int64Data[si]
			}
		}
	case TypeFloat32:
		if !ownNulls && !baseNulls {
			for di, si := range idx {
				flat.Float32Data[di] = base.Float32Data[si]
			}
		} else {
			for di, si := range idx {
				if (ownNulls && own.IsNullFast(di)) || (baseNulls && base.Nulls.IsNullFast(int(si))) {
					flat.Nulls.SetNull(di)
					continue
				}
				flat.Float32Data[di] = base.Float32Data[si]
			}
		}
	case TypeFloat64:
		if !ownNulls && !baseNulls {
			for di, si := range idx {
				flat.Float64Data[di] = base.Float64Data[si]
			}
		} else {
			for di, si := range idx {
				if (ownNulls && own.IsNullFast(di)) || (baseNulls && base.Nulls.IsNullFast(int(si))) {
					flat.Nulls.SetNull(di)
					continue
				}
				flat.Float64Data[di] = base.Float64Data[si]
			}
		}
	case TypeDecimal:
		if !ownNulls && !baseNulls {
			for di, si := range idx {
				flat.DecimalData.Data[di] = base.DecimalData.Data[si]
			}
		} else {
			for di, si := range idx {
				if (ownNulls && own.IsNullFast(di)) || (baseNulls && base.Nulls.IsNullFast(int(si))) {
					flat.Nulls.SetNull(di)
					continue
				}
				flat.DecimalData.Data[di] = base.DecimalData.Data[si]
			}
		}
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		// Pre-size the destination arena to avoid growslice per SetFrom.
		srcOff := base.BytesData.Offsets
		totalBytes := 0
		for _, si := range idx {
			totalBytes += int(srcOff[si+1] - srcOff[si])
		}
		flat.BytesData.PreAllocBytes(totalBytes)
		for di, si := range idx {
			if (ownNulls && own.IsNullFast(di)) || (baseNulls && base.Nulls.IsNullFast(int(si))) {
				flat.Nulls.SetNull(di)
				flat.BytesData.Set(di, nil)
				continue
			}
			flat.BytesData.SetFrom(di, &base.BytesData, int(si))
		}
	default:
		// ARRAY/MAP/ROW/VECTOR: nested-aware per-value copy.
		for di, si := range idx {
			if ownNulls && own.IsNullFast(di) {
				flat.WriteNullAt(di)
			} else {
				flat.CopyValueFrom(di, base, int(si))
			}
		}
	}
	v.Base = nil
	v.Indices = nil
	v.Len = n
	v.Nulls = flat.Nulls
	v.BoolData = flat.BoolData
	v.Int32Data = flat.Int32Data
	v.Int64Data = flat.Int64Data
	v.Float32Data = flat.Float32Data
	v.Float64Data = flat.Float64Data
	v.BytesData = flat.BytesData
	v.DecimalData = flat.DecimalData
	v.VectorDim = flat.VectorDim
	v.Offsets = flat.Offsets
	v.Child = flat.Child
	v.Children = flat.Children
	v.FieldNames = flat.FieldNames
}

// newOwnedLike allocates an owned vector of src's type and nested structure
// with n pre-allocated rows, ready for sequential CopyValueFrom/WriteNullAt
// writes at positions 0..n-1. ROW children are pre-allocated at n rows so
// null rows advance through the indexed-write path (an append-built child
// would misalign if the first rows were nulls written before the child
// existed); ARRAY/MAP element storage stays append-built, which is what
// CopyValueFrom expects.
func newOwnedLike(src *Vector, n int) *Vector {
	v := NewVectorWithScale(src.Type, n, src.DecimalData.Scale)
	switch src.Type {
	case TypeVector:
		v.VectorDim = src.VectorDim
		if src.VectorDim > 0 {
			v.Float32Data = make([]float32, n*src.VectorDim)
		}
	case TypeArray, TypeMap:
		if src.Child != nil {
			v.Child = NewVectorLike(src.Child)
		}
	case TypeRow:
		v.Children = make([]*Vector, len(src.Children))
		for j, ch := range src.Children {
			v.Children[j] = newOwnedLike(ch, n)
		}
		v.FieldNames = append([]string(nil), src.FieldNames...)
	}
	return v
}

// HasViews reports whether any column of the batch is a view vector.
func (b *RecordBatch) HasViews() bool {
	for _, col := range b.Columns {
		if col.Base != nil {
			return true
		}
	}
	return false
}

// FlattenViews materializes every view column in place. Call before
// retaining a batch past the pipeline's per-batch cycle (Sort/Window/sink
// Consume) or handing it to code that reads typed storage directly.
func (b *RecordBatch) FlattenViews() {
	for _, col := range b.Columns {
		col.Flatten()
	}
}

// FlattenColumn materializes a single column if it is a view. Use for
// column-granular consumers (a filter touching one column) so the remaining
// columns stay lazy.
func (b *RecordBatch) FlattenColumn(i int) {
	b.Columns[i].Flatten()
}
