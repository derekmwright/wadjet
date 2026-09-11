# Numeric promotion domain

Source: internal/engine/exec/numeric_promote.go — numericPromotable, moved 2026-09-11 (#1026)

One promotion table for "read this column cell as a float64".

Three places needed it and each had grown its own list, so the same query
answered differently depending on which one it reached:

	resolveFloat64Extractor (aggregate.go) — STDDEV/VARIANCE/MEDIAN/PERCENTILE/
	    MODE/CORR/COVAR, and MIN_BY/MAX_BY's ordering key. Its default returned
	    nil, which updateGroup reads as "skip every row", so the aggregate
	    answered NULL.
	vecFloat64 (window.go) — window SUM/AVG. Its default returned 0 AND marked
	    the row valid, so the window answered a wrong number rather than no
	    number. It now returns whether a numeric reading EXISTS and the window
	    writes NULL when one does not (#412) — sharing the table was not enough
	    on its own, because the caller was free to throw the answer away.
	kernel.ResolveRowSum (kernel/agg.go) — the grouped SUM/AVG. Its list is the
	    one this table matches, because it is the one TPC-H exercises and the
	    one the two-path and DuckDB gates have always compared.

What is IN, and on what grounds:

	INT32/INT64/FLOAT32/FLOAT64/DECIMAL  numeric, obviously.
	PORT, PROTOCOL                       integer-backed quantities; PostgreSQL
	                                     sums and averages smallint/integer.
	DURATION                             int64; PostgreSQL has sum(interval)
	                                     and avg(interval).
	DATE, TIMESTAMP                      NOT on PostgreSQL's authority: it has
	                                     no sum(date), avg(date), sum(timestamp)
	                                     or avg(timestamp) either. They are in
	                                     because ResolveRowSum already had them
	                                     and the TPC-H and DuckDB gates run over
	                                     them, so removing them changes shipped
	                                     answers; this table's job is to make the
	                                     three readers AGREE, not to relitigate
	                                     what is summable. Keeping them is
	                                     status-quo preservation and is stated as
	                                     such rather than dressed as a semantic.
	                                     Their honest home is orderKeyFloat64
	                                     below, where an ORDERING over a date is
	                                     meaningful and a SUM of one is not.

What is OUT: IPV4 and MAC. PostgreSQL has no sum, avg or stddev over inet or
macaddr, so producing a number would be inventing a semantic; producing NULL
is what the grouped path already does — and, since #412, what the window
does too. Making it a plan-time ERROR is the right answer for the whole
group (DATE and TIMESTAMP included, which is why they cannot simply be
dropped here first) and is left open: it needs the same output-type decision
#392 needs. BYTES, STRING, IPV6, CIDR, UUID, BOOL and the containers are out
on PostgreSQL's authority, with no such caveat.
