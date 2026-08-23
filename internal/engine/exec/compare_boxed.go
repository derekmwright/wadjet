package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Boxed comparison of Vector.GetValue values, driven by the column's
// DECLARATION.
//
// # The defect this replaces (#444)
//
// There were two comparators for one value. The columnar one
// (kernel.CompareValuesAt) orders a ROW's fields POSITIONALLY, which is
// PostgreSQL's record_cmp and what ORDER BY, the sort-merge join and the
// in-memory window all take. The boxed one (compareAny) ordered them by
// field NAME, because Vector.GetValue renders a ROW as a map[string]any and a
// Go map has no declaration order to read. The two therefore disagreed on
// every ROW column whose declared field order is not alphabetical — `ROW(b
// INT64, a STRING)` sorted on `b` down one path and on `a` down the other —
// and the same split hit DECIMAL, which boxes as its formatted string and so
// ordered "10.001" before "2.0002" lexicographically where the columnar path
// orders it numerically.
//
// # What replaces it
//
// One rule, stated once: the order is the declared column's, exactly as
// kernel/container_sort.go documents it. The boxed comparator is RESOLVED
// FROM the declaration — a closure per column, built once, no per-value type
// switch (the codebase's typed-kernel rule applied to the boxed path) — so a
// ROW walks `col.Fields` in order, an ARRAY/MAP walks `col.ElementType`, and
// a DECIMAL parses back to its unscaled Int128 and compares numerically.
// Every production caller of the boxed path has the declaration: the
// row-oriented window spill and its MIN/MAX deque take it from `w.schema`,
// and the global (empty-PARTITION-BY) window evaluator from the pass schema
// it already resolves input indices against.
//
// compareAny remains, as the DYNAMIC comparator for a value whose declaration
// is not available, and this file bottoms out in it for every scalar. Its ROW
// arm still orders by name, because the box is genuinely all there is to go
// on there — but no production path reaches it any more, which is what makes
// the positional rule the only one that decides a query's answer.
//
// # NULLs
//
// Two levels, the same two kernel/container_sort.go draws. A COLUMN-level
// NULL sorts FIRST (newBoxedCompare), matching compareAny's long-standing
// top-level rule and the nulls-first sort resolvers. An ELEMENT NULL inside a
// container sorts LAST (boxedElemCompare), which is PostgreSQL's
// array_cmp/record_cmp rule and what compareElemAt applies columnar-side.

// boxedCompare orders two boxed values from one column. -1, 0 or 1.
type boxedCompare func(a, b any) int

// newBoxedCompare returns the comparator for a column declared as col, with
// the COLUMN-level null rule: a NULL sorts before every value.
func newBoxedCompare(col parquet.Column) boxedCompare {
	inner := boxedValueCompare(col)
	return func(a, b any) int {
		switch {
		case a == nil && b == nil:
			return 0
		case a == nil:
			return -1
		case b == nil:
			return 1
		}
		return inner(a, b)
	}
}

// boxedElemCompare is newBoxedCompare's in-container twin: a NULL element
// sorts AFTER a non-NULL one and two NULLs are equal.
func boxedElemCompare(col parquet.Column) boxedCompare {
	inner := boxedValueCompare(col)
	return func(a, b any) int {
		switch {
		case a == nil && b == nil:
			return 0
		case a == nil:
			return 1
		case b == nil:
			return -1
		}
		return inner(a, b)
	}
}

// boxedValueCompare returns the null-blind comparator for col's declared
// type. Only the four types whose box loses information the order needs are
// resolved here; everything else is compareAny, whose dynamic dispatch is
// already the columnar order for the scalars, for VECTOR and for an ARRAY of
// scalars.
func boxedValueCompare(col parquet.Column) boxedCompare {
	switch col.Type {
	case parquet.TypeRow:
		return boxedRowCompare(col.Fields)
	case parquet.TypeArray, parquet.TypeMap:
		// A MAP is stored, and boxed, as an ARRAY of its (key, value) entry
		// ROWs, so the two share a declaration shape (ElementType) and a
		// comparison. See newVectorFromColumn (batch/batch.go).
		return boxedListCompare(col.ElementType)
	case parquet.TypeDecimal:
		return boxedDecimalCompare(col.Scale)
	default:
		return compareAny
	}
}

// boxedRowCompare compares two boxed ROWs field by field in DECLARED order —
// PostgreSQL's record_cmp, and compareRowAt's rule for the columnar form of
// the same value.
//
// A declaration with no fields cannot say what that order is; the box's own
// name order is then all there is, so it falls back to compareAny. That is
// the pre-#444 behaviour, kept only for the case where nothing better exists.
func boxedRowCompare(fields []parquet.Column) boxedCompare {
	if len(fields) == 0 {
		return compareAny
	}
	names := make([]string, len(fields))
	cmps := make([]boxedCompare, len(fields))
	for i, f := range fields {
		names[i] = f.Name
		cmps[i] = boxedElemCompare(f)
	}
	return func(a, b any) int {
		am, aok := a.(map[string]any)
		bm, bok := b.(map[string]any)
		if !aok || !bok {
			return compareAny(a, b)
		}
		for i, name := range names {
			if c := cmps[i](am[name], bm[name]); c != 0 {
				return c
			}
		}
		// Field count breaks the last tie, as compareRowAt's compareLen
		// does. Two boxes from one column always agree here; two differently
		// shaped ROWs are what keeps the order total.
		return cmpInt(len(am), len(bm))
	}
}

// boxedListCompare compares two boxed ARRAYs (or MAPs) element-wise over the
// common prefix, then by length — PostgreSQL's array_cmp, and compareListAt's
// rule columnar-side.
func boxedListCompare(elem *parquet.Column) boxedCompare {
	var ec boxedCompare
	if elem == nil {
		// A container declared without its element type: the elements still
		// compare, dynamically, exactly as they did before this file.
		ec = compareAnyElem
	} else {
		ec = boxedElemCompare(*elem)
	}
	return func(a, b any) int {
		av, aok := a.([]any)
		bv, bok := b.([]any)
		if !aok || !bok {
			return compareAny(a, b)
		}
		n := min(len(av), len(bv))
		for i := 0; i < n; i++ {
			if c := ec(av[i], bv[i]); c != 0 {
				return c
			}
		}
		return cmpInt(len(av), len(bv))
	}
}

// boxedDecimalCompare compares two boxed DECIMALs NUMERICALLY, by parsing the
// formatted string back to the unscaled Int128 the column stores — what
// kernel.CompareDecimalAt compares columnar-side.
//
// The box is text, and text orders "10.001" before "2.0002" (#394's split,
// reached through the boxed path instead of the kernel's). The declared scale
// is what makes the parse exact and the round trip lossless: FormatDecimal
// renders exactly `scale` fraction digits and ParseDecimalString reads
// exactly that many back.
func boxedDecimalCompare(scale int) boxedCompare {
	return func(a, b any) int {
		as, aok := a.(string)
		bs, bok := b.(string)
		if !aok || !bok {
			return compareAny(a, b)
		}
		av := batch.ParseDecimalString(as, scale)
		bv := batch.ParseDecimalString(bs, scale)
		switch {
		case av.Less(bv):
			return -1
		case bv.Less(av):
			return 1
		}
		return 0
	}
}

// columnByName returns the declaration for a named column, and whether the
// schema carries one. A name the schema does not know leaves the caller with
// the dynamic comparator rather than a wrong declared one.
func columnByName(schema []parquet.Column, name string) (parquet.Column, bool) {
	for _, c := range schema {
		if c.Name == name {
			return c, true
		}
	}
	return parquet.Column{}, false
}

// newBoxedCompareFor resolves the boxed comparator for a named column of
// schema, falling back to the dynamic one when the schema does not name it.
func newBoxedCompareFor(schema []parquet.Column, name string) boxedCompare {
	col, ok := columnByName(schema, name)
	if !ok {
		return compareAny
	}
	return newBoxedCompare(col)
}
