# Bare star output declaration

Source: internal/planner/physical/star_declared_schema.go — starOnlyDeclaredOutputSchema, moved 2026-09-11 (#1026)

starOnlyDeclaredOutputSchema is declaredOutputSchema for `SELECT *` — the
one SELECT list that produces no Project node to read.

logical.BuildFromSelect skips the projection entirely when the list is a
bare star (builder.go's `if !isStarOnly(info.Columns)`), because the star
selects the input unchanged and a projection would be the identity. The
consequence is that findOutputProjectionNode finds nothing, declaredOutput
Schema answers nil, and a `SELECT *` that returns ZERO rows reaches the
client with no columns at all: psql prints nothing and JDBC's executeQuery
throws "No results were returned by the query" (#846). Every other zero-row
shape has been described from the plan since #416 — `SELECT c0 FROM t WHERE
false` declares c0 — so this was the one hole, and it is the shape a BI
tool opens a table with.

The columns are the star's SOURCE columns, resolved exactly the way
logical.ExpandStarProjections resolves them for a star that DOES share its
SELECT list with another item: the lone scan below, its catalog-annotated
ScanColumns in schema order, with the types AnnotateScanColumns left beside
them. Same source, same order, so the declared answer and the executed one
describe one result.

It is NOT only a description. declaredOutputSchema also feeds
subqueryOutputColumn (plan.go, #696), which picks the COMPARISON RULE for a
scalar subquery on every row — so `d = (SELECT * FROM one_row)` over two
DECIMALs of different scale answered ZERO rows without a declaration, where
PostgreSQL 17 and the named spelling `(SELECT v FROM one_row)` both answer
one. Declaring the star hands that call the same column the named spelling
has always handed it, which is why an approximation here would not be free
and why the walk DECLINES rather than guesses
(wadjet.TestStarScalarSubqueryComparesLikeItsNamedSpelling, round-1 P2).

What it declines, and why the boundary is exactly here:

  - Anything with a Project below the pass-through nodes. Not this
    function's case at all — findOutputProjectionNode answers it, and
    `SELECT * FROM (SELECT c0 AS x FROM t) s` must publish `x`, not `c0`.
  - A star over a JOIN, which is starJoinDeclaredOutputSchema's case below.
    It is answered by CALLING the operator's own namer rather than by
    copying it, which is why it can be answered at all: a name spelled two
    ways by two namers is ADR-0026's defect, and a declaration that
    disagreed with the non-empty answer would be worse than none.
  - A star over an Aggregate, a Window, or a table function. The emitted
    names there are the operator's, not the catalog's.

ok=false means "not a bare star over a resolvable scan", and the ordinary
projection walk answers (with its own nil, where there is no Project).
