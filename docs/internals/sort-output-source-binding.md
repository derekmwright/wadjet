# Sort output source binding

Source: internal/planner/physical/published_identity.go — respellSortKeysOverProducerOutput, moved 2026-09-11 (#1026)

```go
// An ORDER BY term names an OUTPUT column, and the producing stage has its own
// name for that output's value (#947).
//
// `SELECT DISTINCT a AS b, b AS a FROM t ORDER BY a` publishes two columns
// whose names are SWAPPED relative to their sources. PostgreSQL binds the term
// to the OUTPUT column `a`, whose value is the source `b`. On the DAG the
// SELECT list is a Project, which `walkStages` emits no stage for (ADR-0025):
// the sort is folded onto the producing aggregate, whose output publishes the
// SOURCE names `a` and `b`, and the key `a` bound the source `a` — the right
// rows in the wrong sequence, on both DAG arms, where the single-process path
// and PostgreSQL agree. Without the DISTINCT or the GROUP BY there is no
// aggregate to fold onto and all four arms agree, which is what says this is
// the fold's binding and not the parser's.
//
// The rule is the one this file applies everywhere: the consumer asks the
// producer what it CALLS the value. The term names output item i; that item's
// source expression is what the producer publishes; bind THAT.
//
// WHICH relation the key addresses depends on where the fragment runs the
// projection relative to the sort, and that is a fact about the EXECUTOR
// rather than a property of the plan: `fragmentProjectsBeforeSorting` mirrors
// the builders. Where the projection runs FIRST the key addresses the
// projection's OUTPUT NAMES, so the term binds the output column it names;
// where the sort runs first it addresses the projection's INPUT, so the term
// binds that output item's SOURCE.
//
// The boundary is a fact in both directions: the re-spell only fires where the
// key AS IT STANDS binds something ELSE in the same relation. A key that binds
// nothing there is left to resolveDerivedAliasSortKeys, which owns it; a key
// that already binds the same column is right.
```
