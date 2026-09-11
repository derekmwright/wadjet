# In subquery literal sets

Source: internal/planner/physical/in_subquery_set.go — ErrInSubqueryDistributed, moved 2026-09-11 (#1026)

```go
// `WHERE x IN (SELECT …)` on the stage DAG
// ------------------------------------------------------------------
//
// An IN-subquery has one distributed lowering: logical.tryDecorrelateInSubquery
// turns it into a semi/anti join. When that rewrite DECLINES — a subquery
// carrying LIMIT/OFFSET (#482), an ungrouped aggregate item, or a computed one
// (#516) — the IN stays a subquery PREDICATE, and the stage DAG had nothing to
// execute one with: the filter shipped to the worker verbatim and failed with
// "IN subquery requires a SubqueryRunner". The single-process path answered
// every one of those correctly, so it was a two-path divergence in which the
// distributed side ERRORED (#524).
//
// resolveSubqueryAST handled a scalar SubqueryNode and fell through `default:`
// for InExpr. It no longer does. An UNCORRELATED IN-subquery is a SET-valued
// producer, and the set is the whole of what the predicate needs: executed
// once on the coordinator, its rows become the literal list the expression
// layer already evaluates — with NOT IN's three-valued rule (#370), which is
// the same rule #507 gave the semi-join lowering. The subquery runs AS
// WRITTEN, so its LIMIT, OFFSET and ORDER BY mean what they say, which is
// exactly why #482 made the rewrite decline in the first place.
//
// Two bounds, and crossing either is a typed REFUSAL rather than a guess:
//
//   - The set must fit. A declined shape can be unbounded
//     (`IN (SELECT b.id + 0 FROM huge)`), and inlining a million literals into
//     a filter expression is not a plan. maxInlinedInSetRows caps it.
//   - Every value must have a literal spelling that survives the round trip
//     through the filter's text. Integers, floats, strings, booleans and NULL
//     do; a value this cannot render honestly is refused rather than
//     approximated.
//
// The refusal routes the query to the coordinator-local single-process
// pipeline, which owns IN-subquery semantics — the same handoff #359 makes for
// correlated subqueries and #466 for an unstageable DISTINCT. A slower right
// answer beats an error, and both beat a different one.
```
