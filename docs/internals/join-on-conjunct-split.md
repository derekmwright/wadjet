# A join's ON clause splits on its AST

Source: internal/planner/logical/join_conjuncts.go — splitJoinConjuncts,
added 2026-09-18 (arc JR, #1178)

splitJoinConjuncts splits an ON clause into its top-level AND terms ON THE AST,
which is what tells an AND that JOINS two conditions from an AND that is PART
of one.

`splitOnAnd`, which this replaces at the join sites, cut the RENDERED text at
every " AND ". BETWEEN carries one of its own: `ON a.x BETWEEN b.lo AND b.hi`
was cut into `a.x BETWEEN b.lo` and `b.hi`, the first of which parses as
nothing and the second as a literal, so the join was refused for a condition
PostgreSQL evaluates and the same predicate in WHERE answers (#1178). A string
literal containing ' AND ', a parenthesised OR, and a CASE with an AND inside
it are the same cut on other spellings.

## What the terms are

A conjunct is rendered back from its node, which round-trips: `QuoteIdent`
keeps a delimited or CamelCase name re-parseable (#731), and an OR conjunct is
re-parenthesised so re-joining the terms with " AND " cannot change how they
associate — AND binds tighter, so `x = y AND a OR b` would otherwise
re-associate. NOT, BETWEEN, IN and the comparisons all bind tighter than AND
already.

A `ParenNode` is looked THROUGH only when it wraps an AND — `(a AND b) AND c`
is three terms — and kept otherwise, so `(a OR b)` stays one term WITH its
parentheses.

## The two boundaries

An expression that does not parse at all keeps the TEXTUAL split, which is
where the physical key parser still refuses it loudly.

A condition with nothing to split keeps its ORIGINAL text, byte for byte. That
is deliberate rather than an optimization: the overwhelmingly common ON clause
must reach the physical planner exactly as the parser handed it over, so this
change cannot move a spelling that some other site matches on.

## The four sites

`logical.takeJoinCondResiduals` (the inner/cross lift),
`logical.routeOuterJoinOnResiduals` (the outer-join residual route),
`logical.extractJoinCondPredicates` (single-sided pushdown) and
`logical.collectJoinEdges` (one join-reorder edge per conjunct). The last keeps
its own guard — it splits only when every term is a self-contained comparison —
so a parenthesised OR and a BETWEEN now arrive whole and simply fail that test
instead of being cut through.

`physical.flattenJoinConjuncts` already did this one layer down, over the same
clause; the two must keep agreeing.
