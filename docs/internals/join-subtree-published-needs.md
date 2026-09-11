# Join subtree published needs

Source: internal/planner/logical/optimizer.go — subtreePublishedColumns, moved 2026-09-11 (#1026)

subtreePublishedColumns is collectSubtreeColumns plus the output names the
subtree's own Projects MINT — a renamed or computed column, which no scan
stores.

It answers a DIFFERENT question from collectSubtreeColumns, which is why it
is a different function rather than a widening of it. That one asks "which
BASE columns does this relation carry", and its other callers — semi/anti
dedup, join reordering, comma-join lifting — attribute a predicate to a
relation with it; widening it there made Q17's semi leg read wider columns
than its inner sibling and cost the shared-subplan dedup a whole lineitem
scan. This one asks "which names can this side SUPPLY to an operator above
the join", and a renamed or computed output is one of them.

The gap it closes: `WITH c AS (SELECT id, a AS v FROM t) SELECT COUNT(*)
FROM c JOIN t x ON c.id = x.id JOIN t y ON c.id = y.id WHERE c.v > 1` needs
`v` above BOTH joins. No scan stores `v`, so it was in neither side's
available set, the partition below put it in neither probeNeeds nor
buildNeeds, and the INNER join's NeededColumns — which becomes its
OutputFilter — dropped the column the filter above the OUTER join was about
to read. The single-process path failed with `filter column "c.v" does not
exist in the input schema`, the SHUFFLED DAG answered ZERO rows in silence,
and the broadcast DAG answered correctly, because only the first two narrow
to that list (#700, #726).

One join hid it: there the join whose needs are partitioned is the one the
Project feeds directly, so the alias never had to survive a SECOND
partition. The DERIVED-table spelling hides it too, because
pushdownPredicates swaps the filter below the Project and substitutes the
alias away — a CTE's Project is a materialization fence and declines that
swap, which is why the CTE spelling is the one that breaks.

Claiming a minted name can only make a side claim MORE, so it can only push
down a need that used to be dropped and never withhold one. A name pushed to
a side that cannot supply it is already tolerated: it is dropped again at the
scan by sanitizeScanNeeds, and deleted at the window that mints it by
pushColumnNeeds' NodeWindow arm (#694 R1).
