// SPDX-License-Identifier: MIT

package kernel

import (
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Container comparisons must agree with group-key serialization equality
// (#415, #394; ADR-0012). ARRAY is lexicographic, shorter on prefix ties;
// ROW compares fields positionally, ignoring names, with count as final tie-break.
// MAP uses key-ordered entry ROWs, key then value, matching both mapEntryRows
// and sortedMapKeys; VECTOR compares float32 elements then dimension.
// Floats use CompareFloat32/64: NaN greatest/equal to itself (#446).
// Column NULL placement belongs to the resolver; element NULL is AFTER values
// and equals NULL regardless of column placement. DESC negates the entire result.
// Sort-merge equality and appendKeyValue must name the same recursive relation.
// See docs/internals/kernel-container-total-order.md for the design.

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
	if a.Type != b.Type {
		if c, ok := compareMixedLeafAt(a, ai, b, bi); ok {
			return c
		}
	}
	return CompareValuesAt(a, ai, b, bi)
}

// compareMixedLeafAt orders two container ELEMENTS of different numeric types
// — an int[] beside a float8[] or a numeric[], met as a sort-merge join key or
// under any other comparator that hands the kernel two element vectors — at
// their common type, batch.CommonContainerColumn's rule (arc CW round 5): an
// integer beside a DECIMAL is compared exactly at the common scale, anything
// beside a float as a double (a REAL pair as a REAL). Before it the pair was
// ordered by the LEFT child's kernel over the right child's storage — the
// INT64 kernel indexing an empty Int64Data (round-4 review B1: XX000 index
// out of range on the sort-merge arm). ok is false for a pair with no common
// type; CompareValuesAt keeps its own answer for that.
func compareMixedLeafAt(a *batch.Vector, ai int, b *batch.Vector, bi int) (int, bool) {
	common, ok := batch.CommonContainerColumn(leafDecl(a), leafDecl(b))
	if !ok {
		return 0, false
	}
	switch common.Type {
	case batch.TypeInt64:
		x, okx := leafInt64(a, ai)
		y, oky := leafInt64(b, bi)
		if !okx || !oky {
			return 0, false
		}
		return cmpInt64(x, y), true
	case batch.TypeDecimal:
		xv, xs, okx := leafDecimal(a, ai)
		yv, ys, oky := leafDecimal(b, bi)
		if !okx || !oky {
			return 0, false
		}
		return CompareDecimalValues(xv, xs, yv, ys), true
	case batch.TypeFloat32:
		x, okx := leafFloat64(a, ai)
		y, oky := leafFloat64(b, bi)
		if !okx || !oky {
			return 0, false
		}
		return CompareFloat64(float64(float32(x)), float64(float32(y))), true
	case batch.TypeFloat64:
		x, okx := leafFloat64(a, ai)
		y, oky := leafFloat64(b, bi)
		if !okx || !oky {
			return 0, false
		}
		return CompareFloat64(x, y), true
	}
	return 0, false
}

func cmpInt64(x, y int64) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

// leafDecl is a leaf element vector's declaration, as far as the common-type
// rule reads it: its type, and a DECIMAL's scale (the precision the vector
// does not carry is the widest, which moves no digit).
func leafDecl(v *batch.Vector) parquet.Column {
	c := parquet.Column{Type: v.Type, Nullable: true}
	if v.Type == batch.TypeDecimal {
		c.Precision, c.Scale = batch.MaxDecimalPrecision, v.DecimalData.Scale
	}
	return c
}

func leafInt64(v *batch.Vector, i int) (int64, bool) {
	switch v.Type {
	case batch.TypeInt64:
		return v.Int64Data[i], true
	case batch.TypeInt32:
		return int64(v.Int32Data[i]), true
	}
	return 0, false
}

func leafDecimal(v *batch.Vector, i int) (batch.Int128, int, bool) {
	switch v.Type {
	case batch.TypeDecimal:
		return v.DecimalData.Data[i], v.DecimalData.Scale, true
	case batch.TypeInt64, batch.TypeInt32:
		x, _ := leafInt64(v, i)
		return batch.Int128From(x), 0, true
	}
	return batch.Int128{}, 0, false
}

func leafFloat64(v *batch.Vector, i int) (float64, bool) {
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[i], true
	case batch.TypeFloat32:
		return float64(v.Float32Data[i]), true
	case batch.TypeInt64, batch.TypeInt32:
		x, _ := leafInt64(v, i)
		return float64(x), true
	case batch.TypeDecimal:
		// The correctly rounded double of the exact value, as PostgreSQL's
		// numeric::float8 is: through the value's own text.
		f, err := strconv.ParseFloat(v.DecimalData.Data[i].FormatDecimal(v.DecimalData.Scale), 64)
		return f, err == nil
	}
	return 0, false
}

// compareListAt compares one ARRAY or MAP row: element-wise over the common
// prefix, then by length. Two arrays of arrays are multi-dimensional and order
// by compareMultiDimAt instead.
func compareListAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	if a.Type == batch.TypeArray && b.Type == batch.TypeArray && a.Child != nil && b.Child != nil &&
		a.Child.Type == batch.TypeArray && b.Child.Type == batch.TypeArray {
		return compareMultiDimAt(a, ai, b, bi)
	}
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

// compareMultiDimAt is PostgreSQL's array_cmp for a MULTI-DIMENSIONAL array,
// which this engine holds as an array of arrays (round 4, P2): the FLATTENED
// leaves element-wise (a NULL leaf after every value, equal to another NULL),
// then the leaf count, then the dimensions. So `{{1,2},{3,4}}` > `{{1,2,3}}`
// — 1,2,3 tie and four leaves beat three — where the element-wise order of
// the outer array put `{1,2}` < `{1,2,3}` first. The dimensions step reads
// each level's lengths in order (a rectangular value's are its dims; a ragged
// one's keep two different shapes apart), so the order is 0 exactly when the
// two values are identical — what DISTINCT, GROUP BY and the join keys call
// equal.
func compareMultiDimAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {
	al, as := flattenListAt(a, ai, nil, nil)
	bl, bs := flattenListAt(b, bi, nil, nil)
	n := min(len(al), len(bl))
	for k := 0; k < n; k++ {
		if c := compareElemAt(al[k].v, al[k].i, bl[k].v, bl[k].i); c != 0 {
			return c
		}
	}
	if c := compareLen(len(al), len(bl)); c != 0 {
		return c
	}
	m := min(len(as), len(bs))
	for k := 0; k < m; k++ {
		if c := compareLen(as[k], bs[k]); c != 0 {
			return c
		}
	}
	return compareLen(len(as), len(bs))
}

type leafAt struct {
	v *batch.Vector
	i int
}

// flattenListAt appends row i's leaves (the elements of its innermost arrays,
// in order) and its shape (each array's length in pre-order; -1 for a NULL
// inner array) to leaves and shape.
func flattenListAt(v *batch.Vector, i int, leaves []leafAt, shape []int) ([]leafAt, []int) {
	s, e := listRange(v, i)
	shape = append(shape, e-s)
	if v.Child == nil {
		return leaves, shape
	}
	if v.Child.Type != batch.TypeArray {
		for k := s; k < e; k++ {
			leaves = append(leaves, leafAt{v.Child, k})
		}
		return leaves, shape
	}
	for k := s; k < e; k++ {
		if v.Child.Nulls.IsNullFast(k) {
			shape = append(shape, -1)
			continue
		}
		leaves, shape = flattenListAt(v.Child, k, leaves, shape)
	}
	return leaves, shape
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
