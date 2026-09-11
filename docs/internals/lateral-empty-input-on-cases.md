# Lateral empty input on cases

Source: internal/planner/logical/lateral_empty_input.go — lateralEmptyInputCase, moved 2026-09-11 (#1026)

lateralEmptyInputPlan is which of the three shapes this lateral join is, and
it exists because the join's OWN `ON` is part of the semantics rather than
decoration.

PostgreSQL evaluates the lateral subquery ONCE PER OUTER ROW — an ungrouped
aggregate over an empty input still yields one row — and THEN applies the
join condition to that (outer row, lateral row) pair, with the join's kind
deciding what happens to a pair the condition rejects. Three cases follow:

  - No written ON (or `ON true`): the condition rejects nothing, so making
    the join LEFT on the correlation and defaulting the COUNT outputs IS
    the semantics, for the INNER and the LEFT spelling alike.
    (lateralPadOnly)

  - A written ON on an INNER join: the padded row must still be TESTED. An
    inner join's ON and a WHERE are the same filter, so the join becomes
    LEFT on the CORRELATION alone — giving every outer row its lateral row
    — and the ON moves into the enclosing WHERE, where the same default
    substitution reaches it. `ON s.n = 0` then keeps the unmatched row,
    which is what PostgreSQL does and what the decorrelation alone cannot.
    (lateralPadThenFilter)

  - A written ON on an OUTER join: a pair the ON rejects must be KEPT with
    the lateral side NULL, which needs the lateral columns nulled per
    column rather than filtered — a CASE per output over a schema this pass
    does not have. NOT REPAIRED: the join is left exactly as it was written
    and answers what it answered before this repair existed, which for
    every ON that an unmatched outer row would fail is PostgreSQL's answer.
    The one shape it still gets wrong — an ON the DEFAULT row would pass,
    `LEFT JOIN LATERAL … ON s.n = 0` — is pinned in the census with
    PostgreSQL's answer beside it. (lateralNoRepair)

A fourth condition cuts across all three and is checked first: if any join
LATER in the FROM clause is a RIGHT or a FULL join, nothing is repaired at
all. Such a join manufactures rows in which the lateral's columns are NULL,
and neither the COALESCE nor the moved ON can tell those from rows the
lateral produced. See the comment on that branch.

A forced LEFT plus an unconditional default, with no case analysis at all,
is what turned six PostgreSQL-correct answers into wrong ones: `ON s.n > 5`
answered three rows for PostgreSQL's none, and printed 0 for counts of 2.
