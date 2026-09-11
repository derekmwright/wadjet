# Scalar domain error table

Source: internal/engine/expr/scalar_refusal.go — func raiseUnitNotRecognized(unit string) {, moved 2026-09-11 (#1026)

Four scalar functions manufactured a value where PostgreSQL raises (#855).

The pattern is the same in all four: an argument outside the function's
domain fell to `return nil` (SQL NULL) or to `return ""`, so a query that
PostgreSQL refuses came back with a plausible answer and nothing downstream
could tell it from a real one. ADR-0012 item 1 makes PostgreSQL the
authority on error-versus-not, and the per-row channel that carries a
refusal out of an evaluator has existed since #347 (fatal.go).

PostgreSQL 17.11, measured live with VERBOSITY verbose:

	DATE_TRUNC('bogus', ts)          22023  unit "bogus" not recognized for type
	                                        timestamp without time zone
	WIDTH_BUCKET(1,0,10,0)           2201G  count must be greater than zero
	WIDTH_BUCKET(1,5,5,3)            2201G  lower bound cannot equal upper bound
	SPLIT_PART('a,b,c', ',', 0)      22023  field position must not be zero
	CHR(0)                           54000  null character not permitted
	CHR(-1)                          22023  character number must be positive
	CHR(1114112)                     54000  requested character too large for
	                                        encoding: 1114112

CHR(0) is the sharpest of them operationally: no text-format DataRow field
can carry a NUL and libpq truncates at one, so the same query answered two
lengths to two clients — #570's shape, reintroduced through a function.
