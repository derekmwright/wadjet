# Sql outer column pruning candidates

Source: internal/planner/sql/correlation.go — func OuterColumnCandidates(subquerySQL string) []string {, moved 2026-09-11 (#1026)

OuterColumnCandidates returns the column names a subquery may read from the
query that ENCLOSES it: every reference it cannot resolve against its own
FROM clause. Subqueries nested inside it are walked too, so a correlation
two levels down is reported at the top.

It exists for column pruning. A column a correlated subquery reads is a
column the outer query NEEDS, even when it appears nowhere in the outer
SELECT list or WHERE clause — and the pruning walk had no case for a
subquery node at all, so it never saw one. The outer batch then carried no
such column, readOuterValues substituted NULL for it, every comparison
against that NULL was UNKNOWN, and the query answered 0 rows with no
indication anything had gone wrong (issue #347).

It is deliberately over-inclusive where it cannot be sure. An unqualified
name is reported even though the subquery's own FROM may well supply it,
because deciding that needs a catalog this package does not have. Naming a
column the outer relation does not have costs nothing — the caller filters
candidates against the scan's own schema (sanitizeScanNeeds) — while
missing one it does have is the wrong answer above. A reference qualified
by one of the subquery's own tables or aliases is the one case it can rule
out, and does.

A subquery that does not parse yields no candidates: the expression
compiler parses the same text and declines to build a correlated evaluator
for it, and the runtime guard in readOuterValues fails loudly if one is
built anyway.
