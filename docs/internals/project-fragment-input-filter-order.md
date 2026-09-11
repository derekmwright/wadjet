# Project fragment input filter order

Source: internal/coordinator/dag_fragments.go — buildProjectFragment, moved 2026-09-11 (#1026)

buildProjectFragment translates a project stage's task into a fragment:

	[OpShuffleSource, OpFilter?, OpProject?, <sink>]

The stage exists for the shapes where a Project or a Filter has nowhere
else to go: a predicate above a projection that was itself materialized
onto the producing fragment (so the producer's own filter slot runs
UNDERNEATH it), a predicate above a deduped `cte-alias` whose target is
shared with another reference of the same CTE, and a predicate above a CTE
body's terminal that other references also read (#656).

Filter BEFORE project, the scan fragment's order: the predicate is written
against this stage's INPUT — the producer's output columns — and a SELECT
list attached here is written over that same input and must not narrow it
away before the filter has run. `WITH c AS (… GROUP BY g+1) SELECT gk*10 AS
gk10 FROM c WHERE gk > 3` needs exactly that order.

One task, reading every partition of its input: a projection and a filter
are per-row, so any partitioning would be exact, and Singleton is the
simple answer for a stage the planner emits only where nothing ran at all
before.
