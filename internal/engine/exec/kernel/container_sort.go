package kernel

import "github.com/derekmwright/wadjet/internal/engine/batch"

// Total orders for the four container types — ARRAY, MAP, ROW and VECTOR.
//
// # Why they exist
//
// The three resolvers in sort.go enumerated the scalar types and ended in a
// default that reported EVERY pair equal. All four containers landed there,
// so `ORDER BY arr_col` was a stable no-op that returned input order, and
// SortMergeJoin — which uses these same kernels for key EQUALITY, not merely
// for ordering — matched every left row against every right row on a
// container key. Silent, both times (#415). DECIMAL was the fifth type in
// that default and the only one #394 fixed.
//
// # What order, and who decided it (ADR-0012: PostgreSQL decides semantics)
//
//   - ARRAY compares LEXICOGRAPHICALLY, element by element, and on a tie of
//     the common prefix the SHORTER array is less. That is PostgreSQL's
//     array_cmp verbatim (utils/adt/arrayfuncs.c).
//
//   - ROW compares FIELD BY FIELD in declaration order, PostgreSQL's
//     record_cmp. Field NAMES are not part of the order — PG compares
//     composites positionally — and within one column they are constant
//     anyway. A field-count difference, reachable only between two
//     differently shaped ROW columns, breaks the tie last so the order stays
//     total.
//
//   - MAP compares ENTRY BY ENTRY over the stored (key, value) rows. This is
//     a WADJET-DEFINED total order: PostgreSQL has no MAP type, hstore has no
//     btree ordering, and jsonb's is a different structure. It is
//     well-defined because MAP entries are stored in KEY ORDER — mapEntryRows
//     (batch/vector.go) and the parquet writer's sortedMapKeys both sort, for
//     exactly this reason — so two equal maps present their entries in the
//     same sequence and the comparison is key-then-value lexicographic.
//     Storage-wise a MAP is an ARRAY of entry ROWs, so it takes the same code.
//
//   - VECTOR compares lexicographically over its float32 elements, then by
//     dimension. PostgreSQL has no VECTOR type either; this is the ARRAY rule
//     applied to the fixed-width layout, so a VECTOR and an ARRAY of the same
//     numbers sort the same way.
//
// A FLOAT element — a VECTOR's, or an ARRAY(FLOAT32/FLOAT64)'s — is compared
// with CompareFloat32/CompareFloat64 (float_order.go), PostgreSQL's float
// order: NaN above everything and equal to itself. Ordering elements with a
// bare `<`/`>` pair instead makes a NaN tie against whatever sits opposite it,
// which is a per-POSITION tie that does not compose across a multi-element
// container, so the "total order" was not transitive whenever a NaN appeared
// at differing positions (#446).
//
// # NULLs
//
// Two levels, deliberately different:
//
//   - The COLUMN's null placement is the caller's, carried by which of the
//     three resolvers ran — NULLS FIRST, NULLS LAST, or a column known
//     null-free. Same as every scalar type.
//
//   - An ELEMENT null inside a container follows PostgreSQL: a NULL element
//     sorts AFTER a non-NULL one and two NULL elements are equal
//     (array_cmp/record_cmp both do this). It does not track the column's
//     placement, because PG's does not either, and a DESC key reverses it by
//     negating the whole result — again as PG does.
//
// # Equality
//
// These comparators must agree with the group-key serializer (appendKeyValue,
// exec/sort.go): a drained partial aggregate merges two keys iff their bytes
// match, and a sort-merge join joins two rows iff the comparator says 0.
// Both are injective over the same structure — element count, then each
// element, recursively — so "compares equal" and "serializes alike" name the
// same relation.

// CompareValuesAt orders two values of the same type, WITHOUT consulting
// either row's null bit. Callers that can see a NULL use compareElemAt (for a
// container's elements) or one of the resolvers (for a column).
//
// The type comes from a: two vectors reaching one comparator always carry the
// same type — the sort compares one column against itself and the join's
// planner gate requires identical key types.
func CompareValuesAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	switch a.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return sortCompareInt64NoNulls(a, ai, b, bi)
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return sortCompareInt32NoNulls(a, ai, b, bi)
	case batch.TypeFloat64:
		return sortCompareFloat64NoNulls(a, ai, b, bi)
	case batch.TypeFloat32:
		return sortCompareFloat32NoNulls(a, ai, b, bi)
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeUUID:
		return sortCompareStringNoNulls(a, ai, b, bi)
	case batch.TypeCIDR:
		// PostgreSQL's inet order (#520), not the stored text's byte order:
		// an ARRAY(CIDR)/ROW element comparison — ORDER BY arr_cidr, or a
		// sort-merge join keyed on one — used to fall into the case above
		// and order '10.0.0.1' after '10.0.0.1/32' as text while `=` already
		// calls them one address (#492). GROUP BY's own key for the same
		// column already goes through CidrOrderKey (appendColumnValue,
		// aggregate.go), so an unfixed element comparator here disagreed
		// with the key that groups the very values it orders.
		return sortCompareCIDRNoNulls(a, ai, b, bi)
	case batch.TypeBool:
		return sortCompareBoolNoNulls(a, ai, b, bi)
	case batch.TypeDecimal:
		return CompareDecimalAt(a, ai, b, bi)
	case batch.TypeArray, batch.TypeMap:
		return compareListAt(a, ai, b, bi)
	case batch.TypeRow:
		return compareRowAt(a, ai, b, bi)
	case batch.TypeVector:
		return compareVectorAt(a, ai, b, bi)
	}
	return 0
}

// compareElemAt compares one element of a container, applying PostgreSQL's
// in-container null rule: NULL sorts after non-NULL, NULL == NULL.
func compareElemAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	if a == nil || b == nil {
		return 0
	}
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN || bN {
		switch {
		case aN && bN:
			return 0
		case aN:
			return 1
		default:
			return -1
		}
	}
	return CompareValuesAt(a, ai, b, bi)
}

// compareListAt compares one ARRAY or MAP row: element-wise over the common
// prefix, then by length.
func compareListAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	as, ae := listRange(a, ai)
	bs, be := listRange(b, bi)
	if a.Child == nil || b.Child == nil {
		// A container declared or decoded without its child vector carries
		// no element to compare. Length is all there is; it at least keeps
		// unequal shapes unequal instead of collapsing the column.
		return compareLen(ae-as, be-bs)
	}
	n := ae - as
	if m := be - bs; m < n {
		n = m
	}
	for k := 0; k < n; k++ {
		if c := compareElemAt(a.Child, as+k, b.Child, bs+k); c != 0 {
			return c
		}
	}
	return compareLen(ae-as, be-bs)
}

// compareRowAt compares one ROW: field by field in declaration order, then by
// field count.
func compareRowAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	n := len(a.Children)
	if m := len(b.Children); m < n {
		n = m
	}
	for k := 0; k < n; k++ {
		if c := compareElemAt(a.Children[k], ai, b.Children[k], bi); c != 0 {
			return c
		}
	}
	return compareLen(len(a.Children), len(b.Children))
}

// compareVectorAt compares one VECTOR: element-wise over the common prefix,
// then by dimension.
//
// Elements go through CompareFloat32 — PostgreSQL's float order, NaN greatest
// and NaN == NaN — not a bare `<`/`>` pair. The bare pair ties a NaN against
// WHATEVER sits opposite it, which is a per-POSITION tie and does not compose
// into a transitive whole-vector relation: [NaN,0,2] < [0,1,2] < [1,0,1] <
// [NaN,0,2] was a cycle this comparator used to report (#446). The scalar
// FLOAT32/FLOAT64 columns take the same rule, so a VECTOR of NaNs is ordered
// exactly as consistently as a float column of NaNs — both totally.
func compareVectorAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	ad, bd := a.VectorDim, b.VectorDim
	if ad <= 0 || bd <= 0 {
		return compareLen(ad, bd)
	}
	n := ad
	if bd < n {
		n = bd
	}
	ao, bo := ai*ad, bi*bd
	if ao+n > len(a.Float32Data) || bo+n > len(b.Float32Data) {
		return 0
	}
	for k := 0; k < n; k++ {
		if c := CompareFloat32(a.Float32Data[ao+k], b.Float32Data[bo+k]); c != 0 {
			return c
		}
	}
	return compareLen(ad, bd)
}

// listRange returns row i's [start, end) element span, or an empty span when
// the offsets do not cover the row.
func listRange(v *batch.Vector, i int) (int, int) {
	if i < 0 || i+1 >= len(v.Offsets) {
		return 0, 0
	}
	return int(v.Offsets[i]), int(v.Offsets[i+1])
}

func compareLen(x, y int) int {
	if x < y {
		return -1
	}
	if x > y {
		return 1
	}
	return 0
}

func sortCompareContainer(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return -1
	}
	if bN {
		return 1
	}
	return CompareValuesAt(a, ai, b, bi)
}

func sortCompareContainerNullsLast(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	aN := a.Nulls.IsNullFast(ai)
	bN := b.Nulls.IsNullFast(bi)
	if aN && bN {
		return 0
	}
	if aN {
		return 1
	}
	if bN {
		return -1
	}
	return CompareValuesAt(a, ai, b, bi)
}
