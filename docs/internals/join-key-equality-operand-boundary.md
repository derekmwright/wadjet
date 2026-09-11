# Join key equality operand boundary

Source: internal/planner/logical/join_predicates.go — isJoinKeyEquality, moved 2026-09-11 (#1026)

isJoinKeyEquality reports whether a top-level ON conjunct is an equality
between two BARE COLUMN REFERENCES — the only shape parseJoinKeys turns
into a key pair, and so the only shape that survives being left in
JoinCond.

The operands are what distinguishes this from "is an equality". The join
executor matches on column NAMES, so an operand that is not a column has no
representation there: `n.n_regionkey = r.r_regionkey + 3` used to reach the
executor with "r.r_regionkey + 3" as a key column, which resolves to
nothing and matches nothing — 0 rows for a 10-row query, on both execution
paths (#351). Lifting it into the filter above the join is exact for an
inner join and is the same treatment #336 gave the non-equality conjuncts.

An equality against a LITERAL takes the same route. extractJoinCondPredicates
would otherwise push it to the child that owns it, which lands it in the
same place; running it through the residual path means the single-conjunct
case (`ON n.n_regionkey = 1`, which that pass declines because it has
nothing to split) is covered too — it was another 0-row answer.

Which SIDE each column lives on is still decided later
(physical.parseJoinKeys, then FixKeyAssignment). The point here is only to
separate "the join can represent this" from "the join will mis-execute this".
