# Aggregate extra argument arity

Source: internal/planner/logical/agg_extra_args.go — parseAggExtraArgs, moved 2026-09-11 (#1026)

parseAggExtraArgs fills a's arguments past the first from the parsed
call, and reports a query error when the call's arity is wrong for its
function.

Only the first argument used to reach the planner at all: the SELECT
parser kept Args[0] and dropped the rest. Every function below then
answered with something plausible instead of failing —

	CORR(x, y)            no second column, so the covariance state was
	                      never updated: NULL, on every path
	MIN_BY(v, ord)        no ordering column: NULL
	STRING_AGG(c, '::')   separator silently ",", so a 15000-row answer
	                      was 14999 characters short of the right one
	PERCENTILE_CONT(f, c) worse than a wrong fraction: with the fraction
	                      written first, it was the FRACTION that became
	                      the aggregated column. `PERCENTILE_CONT(0.9,
	                      o_totalprice)` aggregated the constant 0.9 over
	                      15000 rows — 13500 on the DAG, NULL in process

The arity check is what keeps the fields below trustworthy downstream:
a spec for one of these functions either carries its extra argument or
the query did not get planned.
