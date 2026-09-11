# Kernel exact decimal ordering

Source: internal/engine/exec/kernel/sort.go — func CompareDecimalAt(a *batch.Vector, ai int, b *batch.Vector, bi int) int {, moved 2026-09-11 (#1026)

CompareDecimalAt orders two DECIMAL values by NUMERIC value, which is what
PostgreSQL's `numeric` ordering means and what every other comparator in
this file already does for its type. Before this arm existed, DECIMAL fell
through the three resolvers' defaults to a comparator that reports every
row equal, so `ORDER BY dec_col` was a stable no-op that returned input
order, and a sort-merge join on a DECIMAL key matched every row against
every row. The other path in the tree — compareAny over Vector.GetValue —
compares the FORMATTED string instead, where "10.001" sorts before
"2.0002". Same query, three different sequences depending on which path
answered (#394).

The comparison is EXACT at every scale. Equal scales compare the unscaled
Int128s directly — that is every sort over one column, every sorted run and
every k-way merge over runs. Unequal scales, reachable where two separately
declared DECIMAL columns meet, rescale the smaller-scale operand by
10^(delta) and compare the unscaled integers; if that product overflows
Int128 the two are compared as big.Int rather than approximated.

Exactness is not a nicety here: SortMergeJoin uses this comparator for key
EQUALITY (sort_merge_join.go), so an approximate answer is a spurious JOIN
MATCH. The float64 rescale this replaced held to 2^53 unscaled units and
then started reporting 9007199254740993 and 9007199254740992.0 — which
differ by one unscaled unit at the common scale — as the same key.
