# Deferred star column alias lists

Source: internal/planner/logical/column_alias_defer.go — deferColumnAliasesOverStar, moved 2026-09-11 (#1026)

A COLUMN-ALIAS LIST OVER A `SELECT *` BODY is applied where the star's width
is known, not guessed at where it is not.

`(…) AS b(kk, nn)` and `WITH c(kk) AS (…)` rename a relation's LEADING output
columns positionally, and PostgreSQL applies the list whatever the body's
SELECT list looks like: `WITH c(kk) AS (SELECT * FROM lat_ord)` publishes
`kk, customer, total`. The two appliers in builder.go decline over a star,
for a reason that was true when they were written — the star's width is a
catalog question the builder cannot ask, and renaming the wrong columns is a
wrong ANSWER rather than a missing one (ADR-0012).

`ExpandStarProjections` answers that question one pass later, so the list is
DEFERRED to it rather than dropped: the builder wraps the body in a Project
carrying the star ITSELF and records the list on that node, the optimizer
expands the star there like any other, and this pass renames the leading
projections of the result. One expansion site, one rename rule
(`plansql.OverlayColumnAliases`), and no second model of a star's width.

Neither failure mode is silent, and neither can be raised here — `Optimize`
returns no error — so the node keeps its marker and
`RefuseUnappliedColumnAliasLists` turns it into the refusal at the two plan
entries, beside the star and ordinal refusals that live there for the same
reason: an OVERLONG list is PostgreSQL's own 42P10, and a list over a star
the expansion DECLINED (a bare `*` over a join, ADR-0012, #810) is one 0A000
sentence. Dropping that second one silently is what made every reference to
a name the list renames TO read NULL.
