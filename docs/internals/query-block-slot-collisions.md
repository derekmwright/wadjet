# Query block slot collisions

Source: internal/planner/physical/slot_collision.go — renameCollidingSlots, moved 2026-09-11 (#1026)

```go
// renameCollidingSlots renumbers the planner's own hidden slots past any
// STORED column of the same name, and past a slot ANOTHER BLOCK of the same
// query already minted.
//
// A table written before the namespace was reserved — or by any binary that
// did not enforce it — may carry a column called `__win_0`. Such a table stays
// READABLE: refusing it at read time made every query against it fail,
// `SELECT *` included, which is a trap rather than a guard rail (the DDL and
// ingest doors refuse the name being CREATED, which is where the reservation
// belongs). Readable means the planner and the stored column can meet in one
// query, and then the planner's slot has to move:
//
//	SELECT __win_0, SUM(id) OVER () AS w FROM oldtab
//
// Here the window mints `__win_0`, the scan emits a column of that name, and
// exec.Window appends its output beside it — #694's collision exactly, with
// the planner on the other side of it. Renumbering the SLOT is the repair,
// because the stored column is the one the user can see and name.
//
// The SECOND collision is between two blocks of one query (#747). The window
// slot counter lives in `logical.BuildFromSelectWithCTEs`, which recurses per
// SELECT BLOCK, so every block starts at zero and two sibling subqueries mint
// the SAME `__win_0`:
//
//	SELECT p.w AS pw, q.w AS qw
//	  FROM (SELECT id, SUM(b) OVER () AS w FROM t) p
//	  JOIN (SELECT id, SUM(a) OVER () AS w FROM t) q ON p.id = q.id
//	-- PostgreSQL pw=49.2400 qw=52.9900; the DAG answered p's window TWICE
//
// Both arms carry a column called `__win_0` into the join, the projection
// above it resolves each reference to that one name, and one window's value
// is published under both output columns. Three siblings collapsed on EVERY
// path, single-process included, because the third arm's slot won.
//
// ADR-0025 recorded the opposite — "the blocks' slots are already distinct,
// the allocator is per query" — and no fixture attempted it, which is method
// 10 of the correctness protocol exactly. The allocator is per BLOCK; this is
// the pass that makes the claim true, at the first point where the whole
// query's slots are visible in one tree.
//
// It runs after AnnotateScanColumns, which is what puts a table's real column
// list on the Scan node; before that pass there is no schema to collide with.
// It is idempotent: a second run sees slots that are already distinct and
// renames nothing.
```
