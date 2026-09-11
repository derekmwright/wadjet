# Literal in list null semantics

Source: internal/planner/physical/filter_plan.go — inFilterForList, moved 2026-09-11 (#1026)

```go
// inFilterForList builds the IN / NOT IN operator for a list of literals,
// applying SQL's NULL rule to the LIST — which is not the same rule as for a
// scalar comparison, and is the one that surprises people:
//
//	`x IN (a, NULL)` is TRUE where x = a and UNKNOWN everywhere else, because
//	TRUE dominates the disjunction. A NULL member therefore drops out; with
//	nothing else left the whole test is UNKNOWN and nothing qualifies.
//
//	`x NOT IN (a, NULL)` is `x <> a AND x <> NULL`, and the second conjunct is
//	UNKNOWN for every row: the result is FALSE or UNKNOWN, never TRUE. A NULL
//	anywhere in a NOT IN list empties the answer (#450).
//
// An empty list with no NULL in it is left alone — that is a different shape
// and the set kernel already answers it.
//
// RESIDUAL (real NOT IN + NULL + over-range literal only): `real NOT IN (1e40,
// NULL)` short-circuits to MatchNothing below on the NULL rule (#450) before any
// literal is examined, so PostgreSQL's 22003 for the over-range 1e40 in the
// real[] cast is not raised — wadjet answers empty. The positive `IN (1e40,
// NULL)` is NOT affected: it keeps the over-range literal, carries the syntactic
// arity of 2 (SetSyntacticLen below), narrows to real[], and raises 22003 like
// PostgreSQL. Surfacing the error on the NOT-IN path would mean checking the
// over-range literal before the MatchNothing short-circuit; left as a documented
// residual (obscure — a NULL in a NOT IN already empties the answer).
```
