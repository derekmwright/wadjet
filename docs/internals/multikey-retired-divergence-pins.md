# Multikey retired divergence pins

Source: internal/oracle/multikey/multikey.go — }, moved 2026-09-11 (#1026)
Superseded: All four historical loud divergences described here were subsequently removed from pins; the current map is empty and those corpus entries are gated outright.

The three notin_* entries were pinned here under #578 and are gone: a
correlated NOT IN is no longer lowered to an anti join, which answers
the two-valued question, so the predicate stays a subquery and
expr.CorrelatedInSubquery.EvalBoolNull carries the three-valued rule
per outer row. They are gated outright now.

LOUD SINCE 2026-09-02 (#734/#679/#535, the consumer half). These four
used to answer a WRONG NUMBER; they now FAIL the query. The re-run
their per-row predicate produced could not be executed — the dropped
column-alias list makes the rebuilt SQL unparseable, and the
un-decorrelated CTE reference dangles — and the evaluator that ate that
failure and returned a boolean constant fails instead. The divergence
from PostgreSQL is unchanged and the pins stand; what changed is that
the user is told. Both harnesses ask about the DISPOSITION before the
rows, so a pinned entry that errors is a logged divergence and one that
starts ANSWERING PostgreSQL's number still fails the pin.
derived_exists_colalias and derived_in_colalias were pinned here under
#613 and are gone: a derived table's column-alias list `(…) AS b(kk,nn)`
is applied on both arms now, so both answer PostgreSQL's 23 and 36. The
corpus entries stay, because they are the only ones that reach the list
through a CORRELATED subquery — the site it was lost at last.

cte_probe_base_build and cte_referenced_twice were pinned here under
#535 and are gone: a CTE reference is a named SCOPE and the four
outer-scope collectors read it off the subtree root now, so a
correlated EXISTS over one is decorrelated like any other. Both are
gated outright.
