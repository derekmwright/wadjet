# Kernel real in list syntactic width

Source: internal/engine/exec/kernel/compare.go — if syntacticLen <= 1 && !listHasQuotedConst(values) {, moved 2026-09-11 (#1026)

PostgreSQL's `real IN (...)` is ARITY-DEPENDENT, and the two arities
compare at DIFFERENT widths (both verified with EXPLAIN VERBOSE on
postgres:17):

	multi-element  →  real = ANY('{...}'::real[])   -- NARROW to real
	single-element →  real = 'x'::double precision  -- WIDEN to double

So the fix for #549 (the multi-element list matching nothing because
it compared at float64 width) narrows ONLY when the SYNTACTIC list
held more than one element. The decision is syntacticLen, NOT
len(values): a NULL member is stripped before the kernel sees the
list, and PostgreSQL still casts the whole `{...}` to real[] when the
SOURCE had >1 element, so `real IN (0.1, NULL)` narrows and matches
even though only 0.1 reaches here. A truly single-element list keeps
the historical WIDENING path, which already matched PostgreSQL:
`f IN (0.1)` → 0 rows (0.1 is not representable in float32, so the
widened column value differs), and `f IN (1e40)` → 0 rows with NO
error (1e40 is a finite double that widens, misses, and never becomes
the +Inf a real cast would).

Single-element IN and the scalar `=` kernel now AGREE — both widen
(#631 fixed `=`) — but they are still separate kernels, because the
MULTI-element arity does not: `real IN (16777217, 99)` narrows and
matches the row holding 16777216, while `real = 16777217` widens and
matches nothing (both verified on postgres:17). IN is therefore not
lowered to a chain of `=` for this type, and the tests do NOT assert
IN == OR-of-equals for real.
A QUOTED member narrows at BOTH arities: it is unknown-typed, so
PostgreSQL coerces it straight to real and `r IN ('3.1')` plans as
`r = '3.1'::real` where `r IN (3.1)` plans as `r = '3.1'::double
precision` (both verified with EXPLAIN VERBOSE, #646). The widening
arm below is therefore for a SINGLE UNQUOTED member only.
