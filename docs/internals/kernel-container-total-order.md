# Kernel container total order

Source: internal/engine/exec/kernel/container_sort.go — func CompareValuesAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {, moved 2026-09-11 (#1026)

Total orders for the four container types — ARRAY, MAP, ROW and VECTOR.

# Why they exist

The three resolvers in sort.go enumerated the scalar types and ended in a
default that reported EVERY pair equal. All four containers landed there,
so `ORDER BY arr_col` was a stable no-op that returned input order, and
SortMergeJoin — which uses these same kernels for key EQUALITY, not merely
for ordering — matched every left row against every right row on a
container key. Silent, both times (#415). DECIMAL was the fifth type in
that default and the only one #394 fixed.

# What order, and who decided it (ADR-0012: PostgreSQL decides semantics)

  - ARRAY compares LEXICOGRAPHICALLY, element by element, and on a tie of
    the common prefix the SHORTER array is less. That is PostgreSQL's
    array_cmp verbatim (utils/adt/arrayfuncs.c).

  - ROW compares FIELD BY FIELD in declaration order, PostgreSQL's
    record_cmp. Field NAMES are not part of the order — PG compares
    composites positionally — and within one column they are constant
    anyway. A field-count difference, reachable only between two
    differently shaped ROW columns, breaks the tie last so the order stays
    total.

  - MAP compares ENTRY BY ENTRY over the stored (key, value) rows. This is
    a WADJET-DEFINED total order: PostgreSQL has no MAP type, hstore has no
    btree ordering, and jsonb's is a different structure. It is
    well-defined because MAP entries are stored in KEY ORDER — mapEntryRows
    (batch/vector.go) and the parquet writer's sortedMapKeys both sort, for
    exactly this reason — so two equal maps present their entries in the
    same sequence and the comparison is key-then-value lexicographic.
    Storage-wise a MAP is an ARRAY of entry ROWs, so it takes the same code.

  - VECTOR compares lexicographically over its float32 elements, then by
    dimension. PostgreSQL has no VECTOR type either; this is the ARRAY rule
    applied to the fixed-width layout, so a VECTOR and an ARRAY of the same
    numbers sort the same way.

A FLOAT element — a VECTOR's, or an ARRAY(FLOAT32/FLOAT64)'s — is compared
with CompareFloat32/CompareFloat64 (float_order.go), PostgreSQL's float
order: NaN above everything and equal to itself. Ordering elements with a
bare `<`/`>` pair instead makes a NaN tie against whatever sits opposite it,
which is a per-POSITION tie that does not compose across a multi-element
container, so the "total order" was not transitive whenever a NaN appeared
at differing positions (#446).

# NULLs

Two levels, deliberately different:

  - The COLUMN's null placement is the caller's, carried by which of the
    three resolvers ran — NULLS FIRST, NULLS LAST, or a column known
    null-free. Same as every scalar type.

  - An ELEMENT null inside a container follows PostgreSQL: a NULL element
    sorts AFTER a non-NULL one and two NULL elements are equal
    (array_cmp/record_cmp both do this). It does not track the column's
    placement, because PG's does not either, and a DESC key reverses it by
    negating the whole result — again as PG does.

# Equality

These comparators must agree with the group-key serializer (appendKeyValue,
exec/sort.go): a drained partial aggregate merges two keys iff their bytes
match, and a sort-merge join joins two rows iff the comparator says 0.
Both are injective over the same structure — element count, then each
element, recursively — so "compares equal" and "serializes alike" name the
same relation.
