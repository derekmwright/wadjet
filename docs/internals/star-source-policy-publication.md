# Star source policy publication

Source: internal/planner/logical/star_expansion.go — StarSourceColumns, moved 2026-09-11 (#1026)

StarSourceColumns is the ONE list a star expands from: what the relation the
star names PUBLISHES to the plan above it, for THIS identity. qualifier is
"" for a bare `*` (the star's source is the single scan below it) and the
relation's name for `alias.*`. nil means "not knowable here", and the caller
leaves the star unexpanded, which is a refusal one pass later.

PUBLISHES, not "declares in the catalog". Where an ABAC column policy applies
the plan carries a SECURITY PROJECTION directly above the scan (#859,
ADR-0033 decision 1): it drops every DENIED column and replaces every MASKED
one with its mask, and it is what every consumer above the scan reads. A star
is such a consumer. Reading the scan's catalog-annotated ScanColumns past that
projection published a denied column's NAME to an identity the policy denies
it to — `SELECT a.* FROM e7emp a` came back with a `salary` column on the
embedded, pgwire and HTTP doors — and the column read NULL only because the
name resolved to nothing above the barrier, which is an accident of the
resolver and not the policy working.

Every star spelling asks THIS function, so the answer cannot differ between
`*` beside an item, `a.*` alone, `a.*` beside an item, a derived table's or a
CTE's star, a star under a positional ORDER BY, or a star nested inside any of
them: a star is its source in its position, and its source is what the plan
below it publishes.
