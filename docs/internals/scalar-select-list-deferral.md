# Scalar select list deferral

Source: internal/planner/physical/scalar_projection_lowering.go — lowerProjectionSubquery, moved 2026-09-11 (#1026)

```go
// The SELECT-list half of the scalar-subquery deferral (#659).
//
// A scalar subquery in a PREDICATE has had a distributed lowering since Q11:
// walkStages puts the filter text through resolveFilterSubqueries, which
// replaces the subquery with a `:scalar_N` placeholder and hands back the SQL
// to emit as a PRODUCER STAGE; the coordinator awaits that stage, reads its
// single row, and substitutes the literal into the filter text before
// dispatch. The same subquery in the SELECT LIST had none: the projection was
// attached verbatim, the worker's expression compiler has no SubqueryRunner,
// and every task failed with `subqueries require a SubqueryRunner`. Refusing
// the plan and routing the query to the coordinator-local pipeline made it
// RIGHT (ADR-0021, `ErrScalarSubqueryProjectionDistributed`) at the cost of
// running the whole query single-process.
//
// This is the lowering, and it is the SAME machinery rather than a second one.
// That matters for more than economy: the alternative — execute the subquery
// at plan time and splice a literal — is what `scalarDeferAll` moved AWAY
// from, because a value accumulated on the coordinator's single-process
// pipeline and one accumulated across stages differ in float order (the Q15
// SF0.1 zero-row root cause). A producer stage accumulates the way the outer
// query does.
//
// Two things the predicate path does not need and this one does:
//
//   - **The spec keeps the item's ORIGINAL name.** `ProjectExprSpec.Name` is
//     what `extractOutputRenames` maps to the user's alias, and that pass reads
//     the LOGICAL projection, which this lowering does not touch. So the spec's
//     Expr carries `:scalar_N` and its Name carries the item's own text.
//   - **The spec is typed the way the SINGLE PATH types the item, and
//     deliberately not from the producer.** The producer's plan knows the
//     value's type and the projection could declare it — measured, that makes
//     `SELECT (SELECT MAX(c_i64) FROM t)` a bigint on the DAG, which is
//     PostgreSQL's answer. The single-process pipeline answers a STRING for
//     the same query (`expr.ScalarSubquery` declares nothing, so the item
//     falls to the projection's string fallback), and a lowering that made
//     the two paths disagree about a column's TYPE would be trading a cost
//     for a divergence. So the item is typed exactly as it is typed today and
//     the box defect is recorded and filed instead. The one thing this must
//     NOT do is declare a type it does not know: an undeclared spec that
//     claims TypeKnown takes TypeID zero, which is BOOLEAN, and the first cut
//     of this returned `true` for that query.
//
// FOUR declines, and each one is a mechanism rather than a shape on a list.
// An item whose subquery survives `resolveSubqueryAST` — a CASE arm, a
// function argument, anywhere the walker's `default:` returns the node
// unwalked. A CORRELATED subquery, which `resolveSubqueryAST` itself parks as
// `ErrCorrelatedSubqueryDistributed`: only the local pipeline can re-run one
// per outer row. A producer whose value has no lossless literal spelling
// (scalarProducerValueIsLiteralSafe). And a producer that would be awaited by
// a stage it READS (attachProjectionScalarDependencies). All four keep today's
// disposition: refused, routed local, right.
```
