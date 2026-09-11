# Array length dimensional boundary

Source: internal/engine/expr/expr_array_fns.go — func fnArrayLength(args []any) any {, moved 2026-09-11 (#1026)

array_length(array, dim) — the length of the ARRAY along dimension `dim`,
which is a DIFFERENT function from cardinality and was registered as an
alias of it (#637).

Two things that alias got wrong, both measured live on postgres:17.11:

	array_length(ARRAY[]::int[], 1)   NULL   -- was 0
	array_length(ARRAY[1,2,3], 2)     NULL   -- was 3: the dimension was IGNORED
	array_length(ARRAY[1,2,3], 0)     NULL
	array_length(ARRAY[1,2,3], -1)    NULL
	array_length(ARRAY[1,2,3], NULL)  NULL
	array_length(NULL::int[], 1)      NULL
	array_length(ARRAY[1,2,3], 1)     3

NULL is PostgreSQL's answer for "that dimension does not exist", and an
EMPTY array has no dimension 1 — which is why the first row is NULL and
`cardinality` of the same array is 0. The two functions disagree there on
purpose and the alias made them agree.

This engine's ARRAY is one-dimensional (parquet.Column.ElementType is a
single element type, and an ARRAY of ARRAY is a nested ELEMENT rather than a
second dimension), so any `dim` other than 1 is NULL. The one-argument
spelling PostgreSQL does not have keeps cardinality's answer, so nothing
that called `array_length(a)` changes.
