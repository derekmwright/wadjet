package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #415: the three resolvers ended in a default that reported every pair
// equal, and ARRAY, ROW, MAP and VECTOR all landed there. These tests pin the
// total order each type now has, its null placement at both levels, and the
// property SortMergeJoin actually depends on — that "compares 0" means
// "equal", not "unorderable".

func containerBatch(t *testing.T, col parquet.Column, rows []map[string]any) *batch.Vector {
	t.Helper()
	b := batch.FromRows([]parquet.Column{col}, rows)
	if b == nil || len(b.Columns) != 1 {
		t.Fatalf("FromRows built no column for %s", col.Name)
	}
	return b.Columns[0]
}

func arrayCol() parquet.Column {
	return parquet.Column{Name: "v", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}}
}

func rowCol() parquet.Column {
	return parquet.Column{Name: "v", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
		{Name: "a", Type: parquet.TypeString, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func mapCol() parquet.Column {
	return parquet.Column{Name: "v", Type: parquet.TypeMap, Nullable: true,
		ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "key", Type: parquet.TypeString},
			{Name: "value", Type: parquet.TypeInt64, Nullable: true},
		}}}
}

func vectorCol() parquet.Column {
	return parquet.Column{Name: "v", Type: parquet.TypeVector, Nullable: true, Dimension: 3}
}

// assertOrder checks that rows[i] < rows[j] for every i<j under the
// nulls-first kernel, and that the nulls-last kernel agrees on every non-null
// pair. The input must be in ascending order with the NULLs (if any) leading.
func assertOrder(t *testing.T, col parquet.Column, rows []map[string]any) {
	t.Helper()
	vec := containerBatch(t, col, rows)
	first := ResolveSortCompare(col.Type)
	last := ResolveSortCompareNullsLast(col.Type)
	free := ResolveSortCompareNoNulls(col.Type)
	if first == nil || last == nil || free == nil {
		t.Fatalf("%v: a resolver returned nil", col.Type)
	}
	for i := 0; i < len(rows); i++ {
		if c := first(vec, i, vec, i); c != 0 {
			t.Errorf("row %d does not equal itself: %d", i, c)
		}
		for j := i + 1; j < len(rows); j++ {
			if c := first(vec, i, vec, j); c >= 0 {
				t.Errorf("nulls-first: row %d (%v) vs row %d (%v) = %d, want < 0",
					i, rows[i]["v"], j, rows[j]["v"], c)
			}
			if c := first(vec, j, vec, i); c <= 0 {
				t.Errorf("nulls-first: asymmetry at rows %d/%d", i, j)
			}
			iNull, jNull := vec.Nulls.IsNullFast(i), vec.Nulls.IsNullFast(j)
			if !iNull && !jNull {
				if c := last(vec, i, vec, j); c >= 0 {
					t.Errorf("nulls-last: row %d vs row %d = %d, want < 0", i, j, c)
				}
				if c := free(vec, i, vec, j); c >= 0 {
					t.Errorf("no-nulls: row %d vs row %d = %d, want < 0", i, j, c)
				}
			}
			if iNull && !jNull {
				if c := last(vec, i, vec, j); c <= 0 {
					t.Errorf("nulls-last: NULL row %d vs row %d = %d, want > 0", i, j, c)
				}
			}
		}
	}
}

func TestSortCompareArrayLexicographic(t *testing.T) {
	// Ascending: NULL (nulls first), then empty, then lexicographic, with a
	// prefix before its extension and a NULL ELEMENT after a non-NULL one
	// (PostgreSQL's array_cmp rule).
	assertOrder(t, arrayCol(), []map[string]any{
		{"v": nil},
		{"v": []any{}},
		{"v": []any{"a"}},
		{"v": []any{"a", "a"}},
		{"v": []any{"a", "b"}},
		{"v": []any{"a", nil}},
		{"v": []any{"b"}},
	})
}

func TestSortCompareRowFieldwise(t *testing.T) {
	assertOrder(t, rowCol(), []map[string]any{
		{"v": nil},
		{"v": map[string]any{"a": "x", "b": int64(1)}},
		{"v": map[string]any{"a": "x", "b": int64(2)}},
		{"v": map[string]any{"a": "x", "b": nil}}, // NULL field sorts last within the row
		{"v": map[string]any{"a": "y", "b": int64(0)}},
		{"v": map[string]any{"a": nil, "b": int64(0)}}, // leading NULL field dominates
	})
}

func TestSortCompareMapEntrywise(t *testing.T) {
	// MAP entries are stored in KEY ORDER, so the comparison is
	// key-then-value lexicographic over the entry list.
	assertOrder(t, mapCol(), []map[string]any{
		{"v": nil},
		{"v": map[string]any{}},
		{"v": map[string]any{"a": int64(1)}},
		{"v": map[string]any{"a": int64(2)}},
		{"v": map[string]any{"a": int64(2), "b": int64(0)}},
		{"v": map[string]any{"b": int64(0)}},
	})
}

func TestSortCompareVectorLexicographic(t *testing.T) {
	assertOrder(t, vectorCol(), []map[string]any{
		{"v": nil},
		{"v": []float32{-1, 0, 0}},
		{"v": []float32{0, 0, 0}},
		{"v": []float32{0, 0, 1}},
		{"v": []float32{0, 1, -5}},
		{"v": []float32{1, -9, -9}},
	})
}

// TestSortCompareContainerEqualityIsEquality is the property SortMergeJoin
// rests on: a 0 from these kernels means the two values ARE the same value.
// The old default returned 0 for every pair, which turned an inner join on a
// container key into a cross product.
func TestSortCompareContainerEqualityIsEquality(t *testing.T) {
	cases := []struct {
		name string
		col  parquet.Column
		rows []map[string]any
		// equal[i] is the index this row must compare 0 against.
		equal []int
	}{
		{"array", arrayCol(), []map[string]any{
			{"v": []any{"a", "b"}},
			{"v": []any{"a", "b"}},
			{"v": []any{"a", "c"}},
		}, []int{1, 0, -1}},
		{"row", rowCol(), []map[string]any{
			{"v": map[string]any{"a": "x", "b": int64(1)}},
			{"v": map[string]any{"a": "x", "b": int64(1)}},
			{"v": map[string]any{"a": "x", "b": int64(2)}},
		}, []int{1, 0, -1}},
		{"map", mapCol(), []map[string]any{
			{"v": map[string]any{"k": int64(1)}},
			{"v": map[string]any{"k": int64(1)}},
			{"v": map[string]any{"k": int64(2)}},
		}, []int{1, 0, -1}},
		{"vector", vectorCol(), []map[string]any{
			{"v": []float32{1, 2, 3}},
			{"v": []float32{1, 2, 3}},
			{"v": []float32{1, 2, 4}},
		}, []int{1, 0, -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vec := containerBatch(t, tc.col, tc.rows)
			cmp := ResolveSortCompare(tc.col.Type)
			for i, want := range tc.equal {
				for j := range tc.rows {
					got := cmp(vec, i, vec, j)
					shouldTie := i == j || j == want
					if shouldTie != (got == 0) {
						t.Errorf("compare(%d, %d) = %d; equal expected: %v", i, j, got, shouldTie)
					}
				}
			}
		})
	}
}

// TestResolveSortCompareCoversEveryType: the default arm now means "not a
// type this engine has", so every real type must resolve to a non-nil kernel
// on all three resolvers. That is what makes sort_merge_join.go's `cmp == nil`
// guard a live refusal rather than dead code.
func TestResolveSortCompareCoversEveryType(t *testing.T) {
	all := []batch.TypeID{
		batch.TypeBool, batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64,
		batch.TypeString, batch.TypeBytes, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeIPv6,
		batch.TypeCIDR, batch.TypeMAC, batch.TypePort, batch.TypeProtocol, batch.TypeDuration,
		batch.TypeUUID, batch.TypeDate, batch.TypeDecimal, batch.TypeArray, batch.TypeRow,
		batch.TypeMap, batch.TypeVector,
	}
	if len(all) != 22 {
		t.Fatalf("the type list is %d long, not 22 — add the new type here and to the resolvers", len(all))
	}
	for _, typ := range all {
		if ResolveSortCompare(typ) == nil {
			t.Errorf("ResolveSortCompare(%v) = nil", typ)
		}
		if ResolveSortCompareNullsLast(typ) == nil {
			t.Errorf("ResolveSortCompareNullsLast(%v) = nil", typ)
		}
		if ResolveSortCompareNoNulls(typ) == nil {
			t.Errorf("ResolveSortCompareNoNulls(%v) = nil", typ)
		}
	}
	// A TypeID the engine does not have must be refused, not tied.
	if ResolveSortCompare(batch.TypeID(200)) != nil {
		t.Error("an unknown type resolved to a comparator; the SMJ nil guard is dead again")
	}
}

func arrayCidrCol() parquet.Column {
	return parquet.Column{Name: "v", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeCIDR, Nullable: true}}
}

// TestSortCompareArrayCidrUsesInetOrder is the regression for
// CompareValuesAt's TypeCIDR arm: before this fix a CIDR element fell into
// the String/Bytes/IPv6/UUID group above and ordered by raw stored TEXT, so
// `ORDER BY arr_cidr` (or a sort-merge join keyed on one) disagreed with the
// element-vs-element `=`/`<`/`>` comparison (#492, ADR-0012 item 10) and with
// the column's own GROUP BY key (exec.appendColumnValue's ARRAY path already
// re-keys a CIDR leaf through CidrOrderKey via appendNestedElem).
func TestSortCompareArrayCidrUsesInetOrder(t *testing.T) {
	assertOrder(t, arrayCidrCol(), []map[string]any{
		{"v": nil},
		{"v": []any{}},
		// 9.0.0.0/8 sorts BELOW 10.0.0.0/8 in inet order (the differing
		// leading octet decides); text order says the opposite ('9' > '1').
		{"v": []any{"9.0.0.0/8"}},
		{"v": []any{"10.0.0.0/8"}},
		{"v": []any{"10.0.0.1"}},
	})

	// A bare address and its own /32 are one inet value: CompareValuesAt must
	// call them EQUAL, the one property assertOrder's strict "<" chain above
	// cannot exercise directly.
	vec := containerBatch(t, arrayCidrCol(), []map[string]any{
		{"v": []any{"10.0.0.1"}},
		{"v": []any{"10.0.0.1/32"}},
	})
	if c := CompareValuesAt(vec, 0, vec, 1); c != 0 {
		t.Errorf("CompareValuesAt(10.0.0.1, 10.0.0.1/32) = %d, want 0 (one inet value)", c)
	}
}
