# ADR-0020: DROP TABLE's physical reclaim is guarded and opt-in

Status: Accepted (landed 2026-08-24, following #494's naming fix and TWO
adversarial reviews of physical reclaim: the first reproduced the
re-registration and Iceberg-refresh losses, the second found that reclaim
deleted files the engine never wrote, that the live-manifest guard was a
time-of-check/time-of-use check, and that a failed DROP bricked reclaim
catalog-wide. Every layer below has a regression test confirmed to fail
with its own fix backed out.)

Amended 2026-09-05: this record now also governs COMPACTION's two physical
schedules — its manifest publication and its deferred-delete queue — after an
adversarial review reproduced four losses there (#893, #894, #895, #896). See
"Amendment, 2026-09-05" below for the rule and what it does not close.

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
   `pendingDrops` is in-process, and the re-observation is a read; nothing
   serializes it against a concurrent write. `dropMu` guards only
   `pendingDrops` itself, not the delete loop's `Head`/`Delete` calls
   against the store, so the residual is **not** scoped to a *different*
   `*Catalog` instance — the first version of this record said that, and
   it understated the exposure. Any writer not serialized with the delete
   loop is invisible inside the window, including a second goroutine
   calling `AddFiles` on this **same** `*Catalog` while the delete loop is
   mid-entry: a reviewer's probe against one instance reproduced exactly
   that, deleting a file `AddFiles` had just re-registered. The window is
   one pending entry's **whole delete batch** — every `Head`+`Delete` pair
   over that entry's `pd.paths`, not a single call — so a multi-file table
   widens it. Closing it would take a lease or a catalog-side tombstone
   that a registering writer must clear, which is a bigger design than
   this one. Layer 0 is the layer that does not depend on timing at all,
   which is why the blast radius is bounded there and not here.

   Concretely, where this is reachable today: `cmd/wadjet`'s standalone
   mode has no in-process caller that runs `AddFiles` against the same
   `*Catalog` a `BackgroundCompactor` sweeps — its pgwire server opens its
   own separate `wadjet.DB` — so the window is unreachable through that
   binary as shipped. An embedder calling `db.Catalog().AddFiles` directly
   alongside its own `compaction.NewBackgroundCompactor(db.Catalog(),
   ...)` reaches it: that registration call is exactly the "second
   goroutine on this same `*Catalog`" case above.
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
  `pendingDrops` is in-process while the catalog it consults is shared,
  and nothing serializes the delete loop against a write to that catalog.
  Re-observing per pending entry shrinks the window to one entry's whole
  delete batch, and layer 0 makes what is inside that window bytes this
  engine wrote — but the window is not zero. Any writer not serialized
  with the delete loop can land inside it, including another goroutine on
  this **same** `*Catalog` instance (see the decision section above for
  which processes can actually reach that today).
- *"was written by this engine"* is what the invariant leans on when
  timing gives out, which is why ownership, not the live-manifest guard,
  is the layer described first.

## Amendment, 2026-09-05: compaction publication is one validated transaction, and retirement needs proof

Status: Accepted (landed 2026-09-05, following an adversarial review of the
compaction publication path that reproduced four losses 5/5 with no sleeps and
no probabilistic race: #893, #894, #895, #896. Every rule below has a
regression test confirmed to fail with its own hunk backed out — eleven
single-hunk reverts, each turning its named gate red.)

This record already governed one physical-deletion schedule, `DROP TABLE`'s.
Compaction has two more — its own deferred-delete queue and its manifest
publication — and neither was held to the same standard. The review found
that the publication was not a transaction at all and that the queue's
eligibility test did not mean what its name said.

### The rule

> **A compaction commit is ONE conditional manifest transaction, validated
> against the input file identities and the delete-marker snapshot it was cut
> from. It either publishes a replacement that reflects every committed
> delete, or it fails leaving the previous snapshot intact. An object is
> physically retired only with proof that no live manifest references it, and
> doubt preserves bytes.**

Four sentences, and each one was a reproduced loss:

1. **One transaction.** `mergeGroup` uploaded the replacement, committed
   `RemoveFiles`, and then separately committed `AddNewFiles`. Each is a CAS;
   the pair is not. A failure at the second left the table with the inputs
   gone and the replacement unpublished — **zero visible rows**, and
   unrecoverable by retrying, because the compactor selects its inputs from
   the manifest it just emptied (#893). Even with both writes succeeding, a
   reader landing between them answered from the missing-file manifest.
   `catalog.CommitCompaction` is now the single write, and `SwapFileForGC` is
   a thin wrapper over it so the GC rewrite and the ordinary merge cannot
   drift apart.

2. **Validated against the input identities.** `RemoveFiles` treats an input
   that is already gone as success, and `AddNewFiles` accepts a second
   distinct UUID output beside the first, so two compactors publishing
   replacements for the same originals both succeeded and the next pass merged
   both copies: **every row twice** (#895). Atomicity alone does not fix this
   — a single atomic write of a stale plan is still wrong. The commit now
   refuses (`ErrCompactionInputMoved`) unless every input is still in the
   partition. `gcInProgress` was never a substitute: it is one Compactor
   instance's map, it guards only `ForceCompactFile`, and it cannot exclude an
   independent compactor or the CLI.

3. **Validated against the delete-marker snapshot.** A DELETE that committed
   after the output was written and before it was published was undone by the
   publication, both operations reporting success (#894). Two shapes of the
   same missing check: ordinary compaction's `RemoveFiles` strips ALL of a
   path's markers including ones the output never applied; forced GC's
   `SwapFileForGC` kept the unapplied ones under the OLD path, where no reader
   can apply them because that file no longer exists — and the next sweep
   removed them as orphans, making the loss permanent. The comment there
   claimed the rows stayed visible "for at most one GC cycle"; that claim was
   wrong, and **removing a marker cannot remove a row from a file that already
   contains it**. The commit now refuses (`ErrCompactionDeletesAdvanced`)
   unless the manifest's marker set for each input is exactly the set the
   output was cut from, and a GC rewrite applies **all** of a file's current
   markers or none — which also means applying a DELETE that landed after the
   GC scan, because that is the right answer and not a TOCTOU hazard.

   Both predicates are exact, not conservative. A concurrent write that did
   not touch this partition's files leaves the commit valid; a "the revision
   moved" test would fail on any unrelated ingest. This is the same shape
   ADR-0030 settled for DML, in the other direction: #691 gave the DML side
   its validation, and the compaction side had none, so the inverse
   interleaving — DML commits first, compaction commits a stale rewrite — was
   still open. It is now closed on both sides.

4. **A refusal is not a failure, and the loser cleans up after itself.** The
   refused writer deletes its own output and replans from the manifest that
   replaced the one it read. Deleting is safe there and ONLY there: a conflict
   is computed before the CAS is attempted, so "nothing was published" is a
   fact. A publication ERROR is not — an update that times out may still have
   been applied — so those bytes are kept and left to the orphan sweep. The
   replan is bounded, so a table under continuous DML is left to the next
   sweep rather than spun on.

   `Result.PublicationConflicts` and `Compactor.PublicationConflicts()` make
   the refusal observable, because the row set after a lost race is the same
   row set as after a race nobody entered. Rows alone cannot tell "the loser
   detected the conflict and discarded its output" from "the loser silently
   did nothing", so the gates assert the counter beside the rows.

5. **Retirement needs proof.** Compaction's deferred-delete queue holds the
   paths ONE table stopped referencing and deleted their bytes on that alone.
   That is not the same claim as "nothing references this object": compact
   `events`, register one of its unchanged originals into a live `archive`
   through `AddFiles` during the grace, flush — and `archive`'s manifest is
   left naming an object the store no longer has (#896). The queue's only
   guard was the object's `LastModified`, and registering unchanged bytes does
   not move it. No `DROP`, no overwrite, no second process and no race is
   needed; the immediate-delete path had no check at all.

   `catalog.Catalog.RetireObjects` is now the only way an object leaves the
   bucket on a compaction schedule, and it establishes eligibility in this
   order:

   - the **retirement mark**, taken before anything is read. A registration
     naming a marked path is refused with `ErrPathRetiring`; a path with a
     registration already in flight is not marked at all and is reported
     unproven. This is the half a reference check cannot cover alone — a
     check is a read, and a read cannot exclude a write that lands after it.
     `AddFiles`, `AddNewFiles` and `CommitDML` take the other end of it.
   - the **live-manifest reference check** across every table
     (`liveCatalogState`, layer 1 above), taken while the mark is held.
   - the **recreated-object check** the queue already had.

   Anything unproven deletes nothing and is requeued. A path a live manifest
   names is reported as such and dropped from the queue, since that reference
   will not go away by waiting.

### What this does NOT close

- **The retirement mark is in-process**, and that is the honest scope for
  what it guards: the deferred-delete queue it serves is itself process-local,
  so no other process's compactor ever holds these paths. It closes the
  window against a registration through this same `*Catalog` — the one #896
  reproduced, and the one an embedder running a `BackgroundCompactor` beside
  its own `AddFiles` calls reaches. A cross-process registration into a shared
  catalog racing a physical delete would need a catalog-side lease, and that
  is a bigger design than this one.
- **`FlushDroppedTableFiles` still has its own guards** rather than sharing
  `RetireObjects`. Consolidating the two lifecycle checks is the obvious next
  step and is deliberately not taken here: the DROP path's residual is
  documented above and unchanged, and folding two schedules together is a
  change with its own failure modes. Its TOCTOU residual (layer 1's
  qualifier) is exactly what the retirement mark now closes for compaction's
  queue, so the consolidation would narrow it there too.
- **`RemoveFiles` keeps its old, unvalidated behaviour** and stays what its
  doc says it is: the low-level primitive, no longer the compaction
  publication path.

### What changes for an operator

The shared-bucket caution in the Consequences below is narrower than it was.
Compaction's deferred deletes can no longer remove an object that **another
live table in the same catalog** references — that is now checked, not
assumed. What is unchanged is the rest of that bullet: compaction still
consumes registered originals it merged into an engine-written replacement,
and a `DROP` with reclaim enabled still removes that replacement. The operator
rule stands.

### Gates

- `internal/storage/compaction/publication_safety_test.go` —
  `TestAFailedReplacementPublicationLeavesTheOldSnapshotQueryable` (ordinary,
  rewrite, all-rows-deleted), `TestNoReaderEverSeesAPartialCompactionPublication`
  (every manifest revision written during a compaction is read as a reader at
  that revision would read it), `TestACommittedDeleteSurvivesTheCompactionThatRacedIt`
  (ordinary, rewrite, forced GC, plus the next-GC orphan check and the
  conflict counter), `TestCompetingCompactorsPublishEachRowExactlyOnce` (six
  arms, plus "the loser left no output in the store").
- `internal/storage/compaction/retire_safety_test.go` —
  `TestADeferredRetirementNeverDeletesAnObjectALiveTableReferences`: the
  filing's four-step schedule, the immediate-grace path, a registration racing
  the flush from inside the flush's own catalog read, and DROP reclamation
  unchanged.
- `internal/storage/catalog/compaction_commit_test.go` and `retire_test.go` —
  the two preconditions in both directions, the exactness claim (an unrelated
  write must NOT refuse), the mark/in-flight interlock, and the
  recreated-object guard.
- Two existing tests were rewritten because they asserted the defective
  contract: `TestForceCompactFile_ConcurrentDeletePreserved` required the
  concurrent marker to SURVIVE the swap (which is #894, asserted on the
  metadata rather than on the rows), and `TestSwapFileForGC_AtomicSwap`
  required a partial marker application to succeed.

Every schedule is deterministic — a second operation runs from inside the
object store's `Put` or the KV's `Get`, at the instant the defect needs — so
there are no sleeps and nothing probabilistic to replicate away.

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
- Reclaim collects only what the engine wrote — but "the engine wrote"
  is not the same claim as "the engine wrote at registration time", and
  this record used to conflate them. `AddFiles`-registered inputs
  themselves are never reclaimed: they are never marked, so `DropTable`
  never schedules them, on every bench and harness topology. **That stops
  being the whole story once background compaction has run.**
  `--background-compaction` defaults to **true**, and
  `compaction.Compactor.mergeGroup` merges a table's small registered
  files into a `compacted_<uuid>` output written through `AddNewFiles` —
  engine-written — while deleting the originals outright via
  `deleteFromStore`, unconditionally and independent of
  `--reclaim-dropped-tables`. Once that has run once, the table's live
  manifest holds only the engine-written compacted copy, and a DROP with
  reclaim enabled removes it: **a compacted bench or harness table's data
  does leave the bucket.** The inputs leak, by design; the engine-minted
  copy that replaced them does not.

  Operator rule: do not enable `--reclaim-dropped-tables` on a catalog
  over a shared or do-not-wipe bucket unless background compaction is
  also disabled there (`--background-compaction=false`) — the two
  defaults combined is what turns a DROP into deleting data an operator
  staged and never asked wadjet to own.
- With `ReclaimDroppedTables` left at its default, dropped tables' files
  leak in the object store until an operator enables the flag or cleans
  them up by other means. This is a known, accepted gap, not a silent one
  — `--reclaim-dropped-tables`'s help text and this ADR both say so.
- Wiring reclaim into every process that can run a DROP (a small sweeper
  goroutine on any `*Catalog` with a store, or routing DROP-recorded paths
  through the compactor's catalog) is future work, not required to ship
  the guard safely.
