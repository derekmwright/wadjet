# Aggregate scope preserving wrappers

Source: internal/planner/logical/agg_scope_wrapper.go — AggScopePreservingWrapper, moved 2026-09-11 (#1026)

AggScopePreservingWrapper reports whether a node standing between an
aggregate and a consumer above it leaves the aggregate's OWN output columns
visible, under their own names.

This is THE list. ADR-0026 §4 states it once and names the walks that read
it, because every one of them is asking a consumer's version of the same
question and every one of them had grown its own answer:

  - physical.aggregateUnderOutput — the gather's OutputRenames;
  - physical.findAggregateAncestor — the single-process projection;
  - physical.groupKeysPublishedBelow — whether an aggregate DIRECTLY BELOW
    already publishes a key, so the one above must not re-materialize it;
  - AggregateOverGroupRows (this package) — whether a Project's INPUT rows
    are one per GROUP, which decides whether a predicate above it may be
    substituted below (#774).

It lives here rather than in `physical` because the fourth reader is in this
package and `physical` imports `logical`, not the other way round. The
physical package's `aggScopePreservingWrapper` is a thin delegation, and
`TestAggScopePreservingWrapperIsReadByEveryWalk` drives all four.

A WINDOW is on the list: exec.Window APPENDS its output to its input and
renames nothing, so every column the aggregate published is still there
under its own name. A Filter (HAVING), a Sort and a LIMIT are on it for the
same reason — they drop or reorder ROWS and rename no column.

An Aggregate is not: it replaces its child's schema with its own keys and
outputs, which is why it is the walks' TARGET rather than a wrapper. Neither
is a Project: what a Project does to the schema is the caller's own
question, so each walk keeps its own rule for it. NodeDistinct is
deliberately absent — rewriteDistinctAsGroupBy lowers a DISTINCT above a
grouped query into a second Aggregate, so the node does not stand there, and
admitting a kind no fixture produces would put an untested path on the
default route (correctness protocol, method 10).
