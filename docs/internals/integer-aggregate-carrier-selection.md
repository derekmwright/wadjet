# Integer aggregate carrier selection

Source: internal/engine/exec/agg_accumulators.go — aggIntExact, moved 2026-09-11 (#1026)

aggIntExact reports whether an aggregate over an INTEGER column accumulates
in the Int128 carrier because PostgreSQL answers it in numeric (#784).

	SUM(int2/int4) -> bigint    exact in int64; here only when the DECLARATION
	                            says numeric, which is AVG's decomposed SUM leg
	SUM(int8)      -> numeric   an int64 sum WRAPS past 2^63
	AVG(int*)      -> numeric   the float64 mean loses integer digits past 2^53

Taken from the live server (`pg_typeof(sum(c_i32))` = bigint,
`pg_typeof(sum(c_i64))` = numeric, `pg_typeof(avg(c_i32))` = numeric): the
two SUM rules differ because int4's sum has a wider integer type to grow
into and int8's does not. WHICH input types those rules cover is
IntegerAccOutputType's answer, not a list repeated here: the window
operator and both planner declarations ask the same function, and a list
that drifted from it would be a carrier disagreeing with a declaration.

It is the ONE predicate every accumulation path consults, so the flat
scatter arrays, the row updaters, the batch kernels, the spill run's latched
encodings and the output schema cannot disagree about which carrier a value
is in — the disagreement class ADR-0027 decision 3 exists for.
The DECLARATION is the second half of the test, and it is what keeps this
off the plumbing. A MERGE stage re-aggregates a partial COUNT as a SUM over
an int64 column (buildFragmentAggregate) and declares int64 for it: that
column is the fold's row count, not a user's SUM(int8), and giving it the
numeric carrier would put the total in SumDec while the emit reads SumI64 —
the two-carrier disagreement in its purest form. So the carrier follows the
declared output type, and a planner that could not resolve the input at all
keeps the float64 it always had, with the accumulator agreeing.
