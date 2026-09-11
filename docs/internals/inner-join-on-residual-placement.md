# Inner join on residual placement

Source: internal/planner/logical/join_predicates.go — liftInnerJoinOnResiduals, moved 2026-09-11 (#1026)

liftInnerJoinOnResiduals moves every ON-clause conjunct that is not a
cross-side equality out of an inner or cross join's condition and into a
filter above it (#336).

The physical planner represents a join condition as key column pairs:
parseJoinKeys splits JoinCond on AND, keeps the parts containing "=", and
discards the rest in silence. `a.s_nationkey = b.s_nationkey AND
a.s_suppkey < b.s_suppkey` therefore joined on the first conjunct and
answered as if the second had never been written — 494 rows for a 197-row
query, which is how self-join deduplication and band joins are spelled.

For an inner join ON and WHERE are interchangeable, so the residual is
exact above the join, and it lands on the path that already carries WHERE
residuals correctly. pushdownPredicates, which runs next, then pushes back
down whatever is single-sided.

Outer joins are left alone: their ON clause is evaluated BEFORE the
NULL-padding, so a residual moved above the join would delete rows the join
is required to preserve. There is no equivalent placement for it in the
current plan vocabulary — the executor has no residual predicate on an
outer join's probe — so that shape stays broken and is tracked separately.
