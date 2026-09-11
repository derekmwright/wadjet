# Relation scope through wrappers

Source: internal/planner/physical/output_rename_resolve.go — relationScopeSubtree, moved 2026-09-11 (#1026)

```go
// relationScopeSubtree descends through JOINs to the arm that answers to
// name, and stops at the first node that neither is a two-arm join nor
// preserves the scope — a Project there is the scope's own SELECT list and
// must not be walked past.
//
// It also descends through the ROW-narrowing wrappers a join can wear, which
// it did not and which cost a wrong answer (#742). A residual WHERE above the
// join puts a Filter between the outer Project and the join — that is what a
// CTE arm's predicate produces, because a CTE's Project is a materialization
// fence the predicate cannot be pushed through, where the derived-table
// spelling of the same query pushes it into the arm's own scan and leaves the
// join directly below. With the Filter there the walk stopped at it, returned
// the WHOLE join subtree as the "scope", and the caller's bare lookup then
// took the first arm that answered — the other arm's column, silently:
//
//	WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
//	SELECT x.id AS xid, x.w AS xw, y.w AS yw
//	FROM (SELECT id, a AS w FROM decpair) x
//	JOIN (SELECT id, a * 100 AS w FROM decpair) y ON x.id = y.id
//	JOIN c ON c.id = x.id WHERE c.dv > 1
//	-- `y.w` resolved to `a`, which is X's w, on the shuffled lowering
//
// The test for descending is the one `resolveRenameSource` above already
// applies, because these are two walks over one tree asking one question, and
// where they disagree about what a scope is, one of them is wrong.
// `resolveRenameSource` consumes a Project (it IS the rename), stops at an
// Aggregate (its outputs are its own GroupBy/OutputCol names, so a bare lookup
// below it resolves against the wrong schema), splits at a Join, and descends
// through every other single-child node. `scopePreservingWrapper` is that same
// set written out: Filter, Sort, Limit, Distinct and Window all narrow rows or
// APPEND columns without renaming an existing one and without changing which
// relations are below them, so descending asks the same question one level
// down. A set operation is never a candidate — it has two or more children, and
// it re-roots the output naming onto its first arm — and Project and Aggregate
// stay stops for the reasons above.
//
// WINDOW was the omission, and it cost the same wrong answer one node over
// (round 4 of #742): a window in the SELECT list puts a Window between the
// outer Project and the join, the walk stopped there, and the qualified
// duplicate alias captured on both DAG arms:
//
//	SELECT x.id AS xid, x.w AS xw, y.w AS yw, SUM(y.w) OVER () AS s
//	FROM (SELECT id, a AS w FROM decpair) x
//	JOIN (SELECT id, a * 100 AS w FROM decpair) y ON x.id = y.id
//	-- PostgreSQL 12.75 | 1275.00 · both DAG arms answered yw = 12.75
```
