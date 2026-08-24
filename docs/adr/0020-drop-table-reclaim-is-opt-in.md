# ADR-0020: DROP TABLE's physical reclaim is guarded and opt-in

Status: Accepted (landed 2026-08-24, following #494's naming fix and an
adversarial review of its first attempt at physical reclaim)

## Context

`DropTable` has always been metadata-only: it removes a table's name and
manifest keys, which is what makes it immediately invisible to new queries,
but its data files stayed in the object store forever. #494's naming fix
(every chunk/compaction output now suffixed with a full UUIDv7 instead of a
truncated or counter-based id) removed the collision hazard that made
physical reclaim unsafe to even consider, so the same change added a
grace-then-delete path: `DropTable` snapshots the exact file paths its
manifest held, and a background sweep deletes them once
`DefaultDropTableGrace` (30m) has elapsed.

An adversarial review of that first attempt reproduced live data loss in
two shapes, both hitting the same missing check:

- **Drop-then-re-register.** #278 documents AddFiles as deliberately
  idempotent so a harness/bench loader or Iceberg catalog discovery can
  re-register a path it already knows about. Drop a table, then re-run the
  loader — it recreates the table and re-registers the very same object
  paths, which are still physically present (that is the whole point of
  the grace). The scheduled delete still fires and deletes them out from
  under the live, freshly-recreated table.
- **Iceberg `RefreshTable`.** Every metadata refresh drops and recreates
  the catalog table over the *same* warehouse data files. The scheduled
  delete from the drop half of that same call deletes the files the
  refreshed table now references.

Both are the identical bug: the delete list was built once, at drop time,
and nothing checked it against the world as it stood *at delete time*.

## Decision

**A load-bearing live-manifest guard, plus two smaller layers, all in
`catalog.Catalog`, and the sweep that calls it is opt-in.**

1. **Live-manifest guard (load-bearing).** Before deleting anything,
   `FlushDroppedTableFiles` builds the set of every path referenced by
   *every* current table's manifest (`ListTables` + `GetManifest`, once
   per call, only when there is something pending to check) and skips any
   pending path that appears in it. This is what makes both reproduced
   cases safe: a re-registered or re-discovered path is live in some
   table's manifest right now, regardless of which incarnation originally
   owned it.
2. **Table-prefix scoping (defense in depth).** A pending path is only
   ever a delete candidate if it falls under its own table's
   `partition.TablePrefix(name)` — `tables/<name>/...`. An
   Iceberg-registered table's warehouse files, or anything registered
   against a foreign store/bucket via
   `iceberg.NewCatalogIntegrationWithStore`, never take that shape, so
   they can never reach the delete call even if the guard above somehow
   missed them.
3. **Recreated-object guard**, mirroring `compaction.Compactor`'s own
   `deleteFromStore`/`FlushDeferredDeletes`: a path whose object was
   modified after the drop was recorded is skipped, since something has
   legitimately written there since.
4. **Commit ordering.** `DropTable` appends to its pending-delete list only
   *after* the metadata put that actually removes the table succeeds. A
   DROP that fails partway through schedules nothing for deletion — it
   stays exactly as recoverable as before the call.
5. **Bounded pending list.** The in-memory list of pending drops is capped
   (`maxPendingTableDrops`); past the cap the *oldest* entry is evicted —
   its files are never scheduled for deletion and leak — rather than ever
   deleting outside the guards above. A leak is a storage-hygiene cleanup
   problem; an incorrect delete is unrecoverable data loss.

**Wiring is opt-in** (`compaction.BackgroundConfig.ReclaimDroppedTables`,
default `false`), not unconditional the way the compactor's own deferred
deletes are. The reason is a real wiring gap, not caution for its own
sake: a `*Catalog` is not unique per process. `cmd/wadjet/main.go`'s
standalone mode runs a `BackgroundCompactor` against one `*Catalog`, but
its pgwire server opens a separate `wadjet.DB` (via `wadjet.Open`) with its
*own* `*Catalog` — so a DROP issued over psql or the embedded API is
invisible to that compactor's sweep no matter what. Turning reclaim on
unconditionally would have reclaimed dropped tables' files for one entry
point while silently never reclaiming them for another, which is worse
than not reclaiming at all: it looks fixed until someone reads the wiring.
An explicit, off-by-default flag makes "not reclaimed yet" the honest
default everywhere, and turning it on is safe wherever it runs — the
guards above hold regardless of which `*Catalog` instance calls
`FlushDroppedTableFiles`.

## Consequences

- Every one of the guards above has a permanent regression test
  (`internal/storage/catalog/catalog_test.go`,
  `internal/storage/compaction/gc_test.go`,
  `internal/iceberg/drop_reclaim_test.go`), including both cases the
  review reproduced.
- With `ReclaimDroppedTables` left at its default, dropped tables' files
  leak in the object store until an operator enables the flag or cleans
  them up by other means. This is a known, accepted gap, not a silent one
  — `--reclaim-dropped-tables`'s help text and this ADR both say so.
- Wiring reclaim into every process that can run a DROP (a small sweeper
  goroutine on any `*Catalog` with a store, or routing DROP-recorded paths
  through the compactor's catalog) is future work, not required to ship
  the guard safely.
