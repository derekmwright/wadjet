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

A second adversarial review of the guarded redo found a third shape, which
neither of those guards addresses because it never involves a recreate at
all: **reclaim deleted files the engine never wrote.** `DropTable`
snapshotted *every* path in the dropped manifest, and a manifest is full of
paths the catalog merely points at. `cmd/tpch-bench` registers its dataset
under `--data-prefix "tables/"`, `cmd/clickbench-bench` under
`--s3-prefix "tables/hits/"`, `internal/harness`'s `s3_catalog` primes the
same way, and all of them go through `AddFiles`, the registration path.
Those objects are operator-staged reference data in buckets that are
explicitly do-not-wipe, and they take exactly the `tables/<name>/...` shape
the prefix layer permits. One `DROP` of a bench table, one grace period,
one process with the flag on, and the shared SF10/SF100/ClickBench datasets
are gone.

## Decision

**Ownership marking, then a load-bearing live-manifest guard re-verified
per pending entry, plus two smaller layers, all in `catalog.Catalog`, and
the sweep that calls it is opt-in.**

0. **Ownership marking (bounds the blast radius).** `FileEntry` carries
   `EngineWritten bool` (`json:"engine_written,omitempty"`), stamped in the
   two places that mint a path themselves — `AddNewFiles` (ingest's
   `chunk_<uuid>`, compaction's `compacted_<uuid>`) and `SwapFileForGC`'s
   `rewrite_<uuid>` output. `AddFiles`, the registration path, never stamps
   it. `DropTable` snapshots **only marked entries**, so an operator-staged
   object that wadjet merely registered can never enter `pendingDrops`,
   whatever shape its path takes. Absent means not owned, which makes every
   manifest written before this field existed — and every future
   registration — safe by default rather than by remembering to check.
   Unmarked entries leak on `DROP`; that is the documented trade, and it is
   the right one, since the alternative is deleting somebody else's bytes.
1. **Live-manifest guard, re-observed per pending entry (load-bearing).**
   Before deleting anything, `FlushDroppedTableFiles` builds the set of
   every path referenced by *every* current table's manifest
   (`ListTables` + `GetManifest`, and only when a pending entry is
   actually past its grace — during the grace window there is something
   pending but nothing due, and observing then costs a full catalog read
   for a round that cannot delete anything) and skips any pending path
   that appears in it. That is what
   makes both reproduced re-registration cases safe: a re-registered or
   re-discovered path is live in some table's manifest right now,
   regardless of which incarnation originally owned it.

   Building the set **once** and deleting against it was itself the
   review's second reproduced loss. It is a time-of-check/time-of-use
   check: the set was built at the top of the call and the delete loop
   never looked again, so a re-registration landing after the build and
   before the `Delete` was invisible to it — the ordinary interleaving of
   a five-minute background sweep against any process that re-registers,
   `iceberg.CatalogIntegration.RefreshTable` included, whose
   drop/re-register window spans an S3 metadata read. So the catalog is
   re-observed immediately before **each pending entry's** delete batch
   (once per entry, not per path — the point is to be current at the
   moment of deletion, and per-path re-reads would multiply KV traffic by
   a table's file count for no further narrowing; #483's revision-keyed
   manifest cache makes a repeat observation a revision probe, not a
   download). The protected set only ever grows within a flush: a path
   seen live at any observation is off limits for the rest of that round.

   The re-observation also carries the set of **table names**, which the
   path set cannot substitute for: `CreateTable` publishes a name and an
   empty manifest *before* any `AddFiles` registers a path into it, so a
   re-created table is live and protected by nothing for that interval. A
   name absent from the first observation that appears in a later one is
   "the world changed under us": that entry is left alone entirely and its
   files leak. A name that was already live in the *first* observation is
   not treated that way — the live table's files are protected precisely,
   by path, so an earlier incarnation's genuinely dead files stay
   reclaimable (the drop → recreate → insert case, covered by a test).

   **This is a narrowing, not a closure, and the record should say so.**
   `pendingDrops` is in-process and the re-observation is a read. A
   re-registration performed by a *different* `*Catalog` instance — a
   separate process, or standalone's pgwire `wadjet.DB` against the
   compactor's catalog — that lands between an entry's re-observation and
   its `Delete` is still invisible. Closing it would take a lease or a
   catalog-side tombstone that a re-registering writer must clear, which
   is a bigger design than this one. Layer 0 is the layer that does not
   depend on timing at all, which is why the blast radius is bounded
   there and not here.
2. **Table-prefix scoping (defense in depth, and a CONVENTION — not an
   impossibility).** A pending path is only ever a delete candidate if it
   falls under its own table's `partition.TablePrefix(name)` —
   `tables/<name>/...`. The first version of this record claimed foreign
   data "never takes that shape". That is not true, and the record should
   not have said it:

   - `iceberg/reader.go`'s `resolvePath` strips the scheme *and the
     bucket* off an absolute data-file URI, so an Iceberg table named
     `events` whose manifests point at
     `s3://somebucket/tables/events/part-0.parquet` resolves to exactly
     `tables/events/part-0.parquet` — the guarded shape, character for
     character. Nothing stops a warehouse from being laid out that way.
   - `iceberg/catalog.go`'s three best-effort rollbacks (`RegisterTable`,
     `DiscoverAndRegister`, `RefreshTable` each call
     `_ = ci.catalog.DropTable(...)` when registration fails partway)
     schedule whatever the partial registration had already written into
     the manifest — warehouse paths included.

   Both are safe, but they are safe because of **layer 0**: everything
   Iceberg registers goes through `AddFiles`, so none of it is marked
   engine-written and none of it can enter `pendingDrops` at all,
   whatever shape the path takes. Prefix scoping is a cheap second
   opinion for paths that *are* owned; it is not the thing standing
   between an Iceberg warehouse and a delete, and treating it as one
   would be exactly the mistake the first version of this ADR made.
3. **Recreated-object guard**, mirroring `compaction.Compactor`'s own
   `deleteFromStore`/`FlushDeferredDeletes`: a path whose object was
   modified after the drop was recorded is skipped, since something has
   legitimately written there since.
4. **Commit ordering: metadata first, cleanup after.** The put that removes
   the name from `meta.Tables` is the write that constitutes the DROP —
   `ListTables`, `GetTable` and the planner all resolve through it — so it
   goes first, and the `table.<name>` / `manifest.<name>` key deletes are
   cleanup of metadata nothing can reach any more. The reverse order put
   the failure window in the worst place: a failed put left the table
   *listed* in `meta` with its keys already gone, every read of it failed,
   and because the live-manifest guard refuses to delete against a picture
   it cannot complete, that one failure **bricked reclaim catalog-wide,
   permanently, for every other table**. Meta-first makes a failed DROP a
   no-op.

   The order it does open a window on is a concurrent `CreateTable`
   claiming the freed name before the cleanup deletes run; deleting *its*
   keys would brick a live table. So the cleanup re-reads `meta` and, if
   the name is back, leaves both keys alone — the new incarnation has
   already overwritten them, so there is nothing of ours left to clean.
   Both halves have a test.

   `DropTable` also appends to its pending-delete list only *after* the
   metadata put succeeds. A DROP that fails schedules nothing for
   deletion — it stays exactly as recoverable as before the call, which is
   asserted on `pendingDrops` directly. (The test that used to cover this
   asserted only that the files survived, which they did — because the
   half-dropped table made every later flush abort. It passed on the
   strength of the bug it was meant to exclude.)
5. **Bounded pending list, and empty unless something will consume it.**
   The in-memory list of pending drops is capped at
   `maxPendingDropPaths` — denominated in **paths, not entries**, because
   one dropped table can hold a single chunk or an SF100 lineitem's worth
   of files, and a cap of N entries bounds memory only if every table is
   the same size. Past the cap the *oldest* entries are evicted, their
   slots zeroed so the backing array stops retaining their path slices —
   their files are never scheduled for deletion and leak — rather than
   ever deleting outside the guards above. A leak is a storage-hygiene
   cleanup problem; an incorrect delete is unrecoverable data loss.

   More to the point, the list does not grow at all on a catalog nobody
   sweeps. Reclaim is opt-in and a `*Catalog` is not unique per process,
   so on most of them nothing will ever consume a pending-drop entry;
   `DropTable` records only where a flusher has declared itself
   (`Catalog.EnableDropReclaim`, called by
   `compaction.NewBackgroundCompactor` at construction when
   `ReclaimDroppedTables` is set). The default configuration therefore
   costs exactly nothing, and *which* catalogs reclaim is a structural
   fact rather than a comment.

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

## The invariant, with its qualifiers

Written out so nobody has to infer it from five bullet points:

> **At the moment `FlushDroppedTableFiles` observes it, a path it deletes
> was written by this engine, was referenced by a table that no longer
> exists, is referenced by no table that does exist, lies under that
> table's own prefix in this catalog's own bucket, and carries no object
> modification later than the drop that scheduled it.**

Every clause is load-bearing, and two qualifiers are not decoration:

- *"At the moment it observes it"* is the TOCTOU residual. Observation and
  deletion are not atomic, and cannot be made so from inside one process:
  `pendingDrops` is in-process while the catalog it consults is shared.
  Re-observing per pending entry shrinks the window to one entry's delete
  batch, and layer 0 makes what is inside that window bytes this engine
  wrote — but the window is not zero. A re-registration by another
  `*Catalog` instance landing inside it is still invisible.
- *"was written by this engine"* is what the invariant leans on when
  timing gives out, which is why ownership, not the live-manifest guard,
  is the layer described first.

## Two limitations an operator has to know about

**In-flight queries and the grace period.** `DefaultDropTableGrace` is 30
minutes for the same reason `compaction.DefaultDeleteGrace` is: a query
dispatched against the table's last manifest resolved its file list at
dispatch time and keeps reading those exact paths until it finishes. No
*new* query can be racing — the table is gone from `meta.Tables` — so the
grace only has to outlive work already in flight. But nothing enforces
that it does: wadjet's `--query-timeout` **defaults to `0`, unlimited**, so
a long analytical query can outlive any grace you pick, and it will fail
on a missing object rather than return a wrong answer.

> Operator rule: keep the query timeout **at or below** `DropGrace`, or
> raise `DropGrace` above the longest query you allow. `--query-timeout=0`
> with reclaim enabled means a sufficiently long query can be killed by a
> DROP that ran half an hour into it.

**Reclaim is per-`*Catalog`, not per-cluster.** See the wiring paragraph
above: a DROP through a catalog with no flusher declared records nothing
and leaks. That is deliberate, and it is why the default is off.

## Consequences

- Every one of the guards above has a permanent regression test
  (`internal/storage/catalog/drop_reclaim_test.go`,
  `internal/storage/catalog/catalog_test.go`,
  `internal/storage/compaction/gc_test.go`,
  `internal/iceberg/drop_reclaim_test.go`), including all three cases the
  reviews reproduced, and each was confirmed to fail with its own fix
  backed out.
- Reclaim collects only what the engine wrote. A catalog whose tables
  were all registered rather than ingested — every bench and harness
  topology — reclaims **nothing**, by design, and its DROPs are still
  metadata-only. That is a leak, and the right one: the alternative is
  deleting an operator's staged data.
- With `ReclaimDroppedTables` left at its default, dropped tables' files
  leak in the object store until an operator enables the flag or cleans
  them up by other means. This is a known, accepted gap, not a silent one
  — `--reclaim-dropped-tables`'s help text and this ADR both say so.
- Wiring reclaim into every process that can run a DROP (a small sweeper
  goroutine on any `*Catalog` with a store, or routing DROP-recorded paths
  through the compactor's catalog) is future work, not required to ship
  the guard safely.
