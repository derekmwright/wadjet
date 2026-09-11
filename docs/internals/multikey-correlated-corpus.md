# Multikey correlated corpus

Source: internal/oracle/multikey/multikey.go — package multikey, moved 2026-09-11 (#1026)

Package multikey is the fixture and query corpus for correlated subqueries
that correlate on MORE THAN ONE column.

Why it exists. Every correlated-subquery entry in every other corpus here
correlates on exactly ONE equality. #562 is what that blind spot cost: a
two-column correlated EXISTS answered ZERO rows, its NOT EXISTS twin
answered EVERY row, and neither the type matrix, the TPC-H corpus, the
DuckDB fingerprint corpus, the PostgreSQL oracle nor the shape fuzzer
contained a single query that could show it. The defect was in the build
side's NDV narrowing (dedupSemiAntiBuildSide): it read the join keys out of
the condition TEXT with a split on " and " while a decorrelation renders
" AND ", so it kept the FIRST conjunct's key and projected the build side
down to that one column — deleting the column the second conjunct compares.

A two-column correlated existence check is what a BI client emits for a
compound-key lookup, so the shape is ordinary and the failure was total and
silent.

This package holds no assertions and no expected answers beyond the ones
PostgreSQL gave: three gates consume it.

	wadjet.TestMultiKeyCorrelatedSubqueries      — the embedded engine
	coordinator.TestMultiKeyCorrelatedTwoPath    — stage DAG vs single process
	(PostgresSetup renders the same fixture for the container that decided
	every Want below.)
