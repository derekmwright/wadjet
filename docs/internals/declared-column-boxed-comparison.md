# Declared column boxed comparison

Source: internal/engine/exec/compare_boxed.go — boxedCompare, moved 2026-09-11 (#1026)

Boxed comparison of Vector.GetValue values, driven by the column's
DECLARATION.

# The defect this replaces (#444)

There were two comparators for one value. The columnar one
(kernel.CompareValuesAt) orders a ROW's fields POSITIONALLY, which is
PostgreSQL's record_cmp and what ORDER BY, the sort-merge join and the
in-memory window all take. The boxed one (compareAny) ordered them by
field NAME, because Vector.GetValue renders a ROW as a map[string]any and a
Go map has no declaration order to read. The two therefore disagreed on
every ROW column whose declared field order is not alphabetical — `ROW(b
INT64, a STRING)` sorted on `b` down one path and on `a` down the other —
and the same split hit DECIMAL, which boxes as its formatted string and so
ordered "10.001" before "2.0002" lexicographically where the columnar path
orders it numerically.

# What replaces it

One rule, stated once: the order is the declared column's, exactly as
kernel/container_sort.go documents it. The boxed comparator is RESOLVED
FROM the declaration — a closure per column, built once, no per-value type
switch (the codebase's typed-kernel rule applied to the boxed path) — so a
ROW walks `col.Fields` in order, an ARRAY/MAP walks `col.ElementType`, and
a DECIMAL parses back to its unscaled Int128 and compares numerically.
Every production caller of the boxed path has the declaration: the
row-oriented window spill and its MIN/MAX deque take it from `w.schema`,
and the global (empty-PARTITION-BY) window evaluator from the pass schema
it already resolves input indices against.

compareAny remains, as the DYNAMIC comparator for a value whose declaration
is not available, and this file bottoms out in it for every scalar. Its ROW
arm still orders by name, because the box is genuinely all there is to go
on there — but no production path reaches it any more, which is what makes
the positional rule the only one that decides a query's answer.

# NULLs

Two levels, the same two kernel/container_sort.go draws. A COLUMN-level
NULL sorts FIRST (newBoxedCompare), matching compareAny's long-standing
top-level rule and the nulls-first sort resolvers. An ELEMENT NULL inside a
container sorts LAST (boxedElemCompare), which is PostgreSQL's
array_cmp/record_cmp rule and what compareElemAt applies columnar-side.
