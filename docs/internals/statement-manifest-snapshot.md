# Statement manifest snapshot

Source: internal/planner/physical/manifest_snapshot.go — ManifestSnapshot, moved 2026-09-11 (#1026)

```go
// ManifestSnapshot pins each table's manifest, and its aggregated column
// stats, to ONE catalog read apiece for the life of one statement (#502) —
// regardless of how many scan nodes name that table (a self-join, two
// subqueries) or how many logical.Optimize passes re-annotate the plan.
//
// Without it, AnnotateScanColumns, walkStages, estimateSubtreeBytes and a
// local-fastpath scan's Init each call catalog.GetManifest independently,
// and AnnotateScanColumns and a dynamic-filter NDV lookup each call
// catalog.AggregateColumnStats independently too — the #483 review measured
// 9 NATS-KV reads for a single-table SELECT, because NATSKVAdapter
// implements no RevisionReader, so Catalog's own revision-validated cache
// (manifestWithRevision) can never serve a hit against it and every one of
// those calls pays a full manifest fetch (694KB / 0.8ms for a 600-file
// manifest, +60% on a pgwire point-query micro).
//
// The floor this snapshot reaches is TWO reads per table per statement, not
// one: GetManifest and AggregateColumnStats are separate Catalog operations
// pinned separately, and AggregateColumnStats reads the manifest a SECOND
// time internally (to key its own revision-validated cache) rather than
// accepting an already-fetched one — Catalog has no API for that today.
// Sharing the manifest object between the two would need one, which is a
// Catalog-level change past this fix's scope; filed as #540. What
// this DOES fix, completely, is the N-scales-with-scan-nodes-and-passes
// growth: 9 reads for one table's SELECT, or 2×(distinct tables) for any
// statement naming more than one table, however many times each is scanned
// or re-annotated.
//
// It is also a correctness fix, not only a performance one (#491's review):
// a table scanned by more than one node in the same statement — a
// self-join, two subqueries — can have its scans straddle a concurrent
// write when each reads the manifest independently. The first scan's read
// can land before a DELETE commits and the second's after, and
// collectStageDeletes (internal/coordinator/delete_markers.go) unions the
// two snapshots FIRST-WINS on a file both saw — keeping the STALE, smaller
// marker set for every task that reads it, so rows the second scan's
// manifest already knew were deleted come back. Pinning the manifest per
// table per statement makes every scan node of that table share one
// ScanDeletes snapshot, so the union is over identical maps and
// first-wins is a genuine no-op rather than a race window.
//
// A watch-based revision cache was considered and rejected in the issue
// that asked for this fix: NATS watch delivery is asynchronous relative to
// a Put's return, which reintroduces #483's staleness window with a
// smaller (but still real) gap, and fails that fix's own tests.
//
// Callers attach one to every Planner instance built for a statement
// (Planner.ManifestSnapshot) before planning begins. NewPlanner gives every
// new Planner its own fresh snapshot, so a caller that builds exactly one
// Planner per statement (the embedded wadjet.DB path, the worker's local
// executor, the HTTP server) gets the pin for free; forSubquery's shallow
// copy shares the parent's snapshot with every child/subquery planner for
// the same reason it shares the catalog and the memory budget. A caller
// that builds SEVERAL Planner instances for one statement — the
// coordinator's scan-annotation passes, which construct a fresh Planner on
// every logical.Optimize iteration, and its main distributed/local-fastpath
// planner — must explicitly assign the SAME *ManifestSnapshot to each one;
// otherwise each keeps its own default and the pin only holds within a
// single Planner's own calls, not across the whole statement.
```
