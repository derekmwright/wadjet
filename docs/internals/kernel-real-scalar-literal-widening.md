# Kernel real scalar literal widening

Source: internal/engine/exec/kernel/compare.go — func compareFilterFloat32Widen(val float64, op CompareOp) FilterKernel {, moved 2026-09-11 (#1026)

compareFilterFloat32Widen compares a FLOAT32 (`real`) column against a
float64 constant AT DOUBLE WIDTH, widening every row's value instead of
narrowing the constant — PostgreSQL's rule for `real <op> <numeric literal>`
(#631).

PostgreSQL has no `real <op> numeric-literal` operator to resolve to: an
unsuffixed decimal constant is `numeric`, an integer constant is `integer`,
and both resolve the comparison through `float8`, so the COLUMN is the side
that moves. Verified with EXPLAIN VERBOSE on postgres:17 for all six
operators and for an integer literal:

	real = 3.1        ->  Filter: (r_val = '3.1'::double precision)
	real > 3.1        ->  Filter: (r_val > '3.1'::double precision)
	real = 3          ->  Filter: (r_val = '3'::double precision)
	real = 3.1::numeric -> Filter: (r_val = '3.1'::double precision)

The narrowing this replaces (`float32(toFloat64(value))`) is a DIFFERENT
predicate whenever the literal is not exactly representable in float32,
which is most literals: over a column holding real(i)+0.1, PostgreSQL
answers `= 3.1` with NO rows (float64(float32(3.1)) != 3.1) where the
narrowing answered the row, and `< 3.1` with the row 3.1 INCLUDED where the
narrowing excluded it as equal. It is not only an equality question — all
six operators move a row across the boundary.

Widening also makes the ROW-GROUP PRUNE and the filter read one predicate.
scan.CanPruneRowGroup compares a float32 statistics bound against the
float64 literal through compareValuesOK, which widens — so under the old
narrowing kernel a row group whose max was exactly float32(3.1) was pruned
for `= 3.1` while the kernel would have MATCHED its rows (ADR-0018's "a
prune must not read the predicate differently from the filter").

The loop is compareFilterFloat's, with float64() on the load: the constant's
NaN-ness still picks the shape (see that function for why the non-NaN case
must keep resolveFloatConstPred2's non-capturing two-argument form), and the
widening conversion is one register instruction per row.
