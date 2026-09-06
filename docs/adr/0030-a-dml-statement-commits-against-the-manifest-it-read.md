# ADR-0030: A DML statement commits against the manifest it read

Status: Accepted (2026-09-03, the DML-hygiene arc: #691, with #815's
one-implementation merge underneath it). Amended 2026-09-03 (arc D3, #835):
the rule now covers statement-vs-STATEMENT as well as
statement-vs-compaction — see "Amendment" at the end.

## Context

Wadjet's tables are merge-on-read. A row exists if some file in the
manifest holds it and no delete marker names that (file, row). A
`DELETE`/`UPDATE`/`MERGE` therefore does three things in sequence:

1. read the manifest,
2. scan the files it names, recording **which row of which file** it
   affected,
3. commit the delete markers for those (file, row) pairs — and, for
   `UPDATE` and `MERGE`, the replacement rows.

Steps 1 and 3 were two unrelated observations of the same key.
`GetManifest` read a revision and threw it away (`catalog.go:408`).
`AddDeleteMarkers` did CAS, but on a revision it read **at commit time** —
so the CAS covered microseconds while the window that mattered was the
whole statement. And it never looked at `manifest.Partitions` at all: the
incoming `FilePath` was a map key and nothing else, so a marker naming a
file the table no longer had was merged in without a word.

Compaction lands in exactly that window and removes exactly those files. At
the time, `compaction.mergeGroup` was `RemoveFiles` (which strips the markers
for the paths it removes) followed by `AddNewFiles` — two CAS writes whose
pair was not one. (Since 2026-09-05 it is a single validated transaction,
`catalog.CommitCompaction`; see ADR-0020's amendment. That closes the mirror
image of this record's defect — DML committing first and compaction then
publishing a stale rewrite — which this record's DML-side validation could
not reach.) Reproduced deterministically on all three doors, with the real
compactor inside a real `db.Execute`:

| statement, with a compaction inside it | reported | table afterwards |
|---|---|---|
| `DELETE FROM t WHERE id = 1` | `DELETE 1` | `1:10 2:20 3:30` — **the row is still there** |
| `UPDATE t SET n = 99 WHERE id = 1` | `UPDATE 1` | `1:10` **and** `1:99` |
| `MERGE … WHEN MATCHED THEN UPDATE …` | `MERGE 1` | the row, twice |

Every one a success tag over a wrong table.

The `UPDATE` and `MERGE` rows have a second cause, independent of the
first. Those statements commit **twice**: the ingester registers each
flushed replacement file through `AddNewFiles`, and the markers follow in a
separate call. So even a correctly *refused* marker commit left the
replacement rows published beside the originals they were meant to replace
— a wrong table, reported as an error. The ordering was itself the fix for
#647 (marker-first lost rows outright); it converted loss into duplication
and the comment said so, calling the transactional commit "a known separate
issue". This is that issue.

The read side had already solved its half one layer over:
`WithManifestSnapshot` gives SELECT statement-level snapshot isolation
(`TestManifestSnapshotClosesTheDeleteMarkerRace`). `grep ManifestSnapshot
wadjet/*.go` matched nothing.

## Decision

**A DML statement's whole manifest change — the files it wrote and the
markers that supersede what they replace — is one CAS, and that CAS refuses
a marker for a file the manifest no longer holds.**

`Catalog.CommitDML(ctx, table, newFiles, markers)` is the entry point, and
it is the only one the DML executors use.

1. **Validation.** Every marker names a file the manifest still holds at
   commit time; otherwise the commit fails with `ErrDMLTargetMoved`, naming
   the file. The predicate is exactly right rather than merely
   conservative: unrelated concurrent traffic leaves this statement's
   markers valid and does not fail it, while a compaction that rewrote the
   files it read does. A blunt "the revision moved" test would have been
   wrong in both directions — it fails on any unrelated write, and an
   `UPDATE`'s own ingest moves the revision.

2. **Atomicity.** `Ingester.DeferManifestCommit` holds the flushed files
   out of the manifest so they ride in the same CAS as the markers. A
   refused statement has published nothing.

3. **Retry, then 40001.** A refused statement is redone whole — re-read the
   manifest, rescan, re-commit — up to `dmlCommitAttempts` (5). Redoing the
   scan against the manifest that replaced the one it read is the only way
   to answer the statement correctly; that is what "one CAS against the
   revision you read" means for a statement that must also write. A
   statement that keeps losing reports **40001** (`serialization_failure`),
   PostgreSQL's class for "retry this". It is the codebase's first 40001.

`AddDeleteMarkers` keeps its old, unvalidated behaviour and its doc now
says what it is: the low-level primitive for a caller holding a manifest
right now (the GC and their tests; compaction publishes through
`CommitCompaction` since ADR-0020's 2026-09-05 amendment). It is not a DML entry
point, and since #815 there is exactly one DML implementation, so no door
can reach the old shape by accident.

## Alternatives rejected

- **Validate in `AddDeleteMarkers` instead.** It is the shared site, but it
  is shared with callers that legitimately mint markers against a manifest
  they hold — roughly eighteen existing call sites pass synthetic paths.
  Validating there converts a correct low-level primitive into one with two
  meanings, and buys nothing `CommitDML` does not already give: the DML
  door is the only door that reads a manifest, does work, and commits
  later.

- **Validate, refuse, and stop (no retry).** This was the first shape
  considered and it fails the issue's own headline: `DELETE` must delete.
  A `DELETE` that reports 40001 because a background compactor happened to
  run is a correct answer to nobody's question when redoing the scan is
  cheap and exact.

- **Snapshot the manifest for the whole statement**, as SELECT does. It
  fixes the *read* half — the statement would keep scanning the files it
  first saw — and does nothing for the *commit*: the markers still name
  files the table no longer has. Statement-level snapshot isolation is the
  right thing for a reader; a writer needs its commit validated.

- **Make the ingester's file registration part of a broader transaction
  API.** Larger than this arc, and unnecessary: the DML statement is the
  only caller that needs two changes to land together.

## Consequences

- Gate: `TestDMLCommitsAgainstTheManifestItRead`
  (`internal/server/pgwire/dml_manifest_race_test.go`) — seven statements ×
  three doors, each with a real full-table rewrite driven from inside the
  statement's own manifest read through the public `Config.MetaKV` seam. It
  asserts the tag AND the table state, and `assertRaceFired` fails the test
  if the interleaving did not actually happen, so a future change to the
  manifest-read path cannot quietly turn it into an ordinary DML test
  (method 10). Confirmed to fail with the validation backed out, with
  exactly the table states in the context table above.
  `TestCommitDMLRefusesAMarkerForAFileTheManifestLost` pins the catalog's
  half directly.

- **The residual is bytes, not rows, and the leak is PERMANENT absent an
  operator.** A refused attempt has already written its parquet objects to
  the store; they stay there, referenced by nothing. This record used to say
  "until the orphan sweep reclaims them" — there is no such sweep:
  `docs/ingestion.md` states that wadjet ships no orphan-Parquet reaper and
  that periodic cleanup is the operator's job. Measured at 10 unreferenced
  chunk objects from two retried statements. It cannot become a wrong
  answer — the manifest is the only thing that decides which rows exist
  (ADR-0020's layer-0 reasoning) — but it is bytes an operator has to
  collect, not bytes something collects for them.

- **Statement-vs-statement was left open here and is closed by the amendment
  below.** Two writers racing each other — not a compactor, but two `UPDATE`s
  over the same rows — both succeeded, and the second one's markers were valid
  because the files did not move. The outcome was DUPLICATION: each read the
  row at its own revision, each wrote a replacement, each marked the copy it
  read, and the key ended up present twice (measured:
  `[1:111:a 1:222:a 2:20:b 3:30:c]`). This record first called it "lost
  update", which is the wrong failure mode — nothing is lost and nothing wins.
  It said closing it needs a conflict rule over ROWS, which this record did not
  decide; the amendment decides it. ADR-0020's honesty requirement (`:112-128`)
  applied in reverse: the original record was a closure of the
  compaction-window shape and a narrowing of nothing else, and it said which.

## Amendment (2026-09-03, arc D3, #835): the rule is over ROWS as well as files

### What the file-level rule missed

`CommitDML` validated that every marker names a file the manifest **still
holds**, and nothing else. Two statements over the same row leave that
predicate satisfied — neither removed a file — so both committed. Measured on
v0.18.22 with the second statement run to completion inside the first's own
manifest read:

| A (outer) ‖ B (inner, commits first) | B | A | table afterwards |
|---|---|---|---|
| `UPDATE n=111 WHERE id=1` ‖ `UPDATE n=222 WHERE id=1` | `UPDATE 1` | `UPDATE 1` | `1:111` **and** `1:222` |
| `UPDATE n=n+1 WHERE id=1` ‖ `UPDATE n=n+1 WHERE id=1` | `UPDATE 1` | `UPDATE 1` | `1:11` **and** `1:11` |
| `UPDATE n=111 WHERE id=1` ‖ `DELETE WHERE id=1` | `DELETE 1` | `UPDATE 1` | the deleted row is **back** |
| `DELETE WHERE id=1` ‖ `UPDATE n=222 WHERE id=1` | `UPDATE 1` | `DELETE 1` | `DELETE 1`, and the row is **still readable** |
| `UPDATE n=111 WHERE id=1` ‖ `MERGE … UPDATE SET n=s.n` | `MERGE 1` | `UPDATE 1` | `1:100` **and** `1:111` |

The family is wider than #835's title: the record's "duplication" is one of
five outcomes, and two of the others resurrect a row a `DELETE` reported as
deleted.

### The decision

**A DML statement's commit is refused if another statement has already
superseded a (file, row) this one is superseding** — `ErrDMLRowSuperseded`,
raised inside the same CAS, beside the file-level `ErrDMLTargetMoved`. The
statement is then redone whole through the path #691 built, so the outcome is
one of the serial orders PostgreSQL could have produced. The redo bound
(`dmlCommitAttempts` = 5) and the 40001 after it are unchanged.

**The predicate is a per-(file, row) marker-set test, and it is exact.** It
rests on an invariant the DML door already keeps for #674's reason: every
scan filters through `DeletedRowsByFile` before it matches, so a statement
NEVER mints a marker for a row the manifest it read had already marked. A
marker that collides at commit time therefore collides with a statement that
committed *in the window*, and with nothing else.

### Alternatives rejected

- **A lock around the statement, or around the table.** This is the bandaid
  the issue names. It answers the same headline and takes concurrent DML on
  DIFFERENT rows down with it, which is the majority of concurrent DML. The
  gate asserts the difference directly: the disjoint-row cases must commit
  with **zero** redos (`DB.DMLRedos()`), which a lock cannot do and which rows
  alone cannot distinguish.

- **A row VERSION carried in the manifest.** A per-row version column would
  also work and would additionally catch a statement that read a row it did
  not mark. It costs a new manifest field, a migration for every existing
  table, and a per-row cost on every commit; the marker set is already in the
  manifest, already read by every DML statement, and already exact for the
  shape that is wrong. ADR-0018's rule — the file is the input, do not invent
  a second source of truth — applies to the manifest too.

- **Compare the manifest REVISION the statement read.** Wrong in both
  directions, for the reasons the original record gives: an unrelated write
  moves the revision, and an UPDATE's own ingest moves it.

- **Serialize at the catalog with a CAS on a per-row key.** That is a lock
  with extra steps, and it makes the catalog's key space a function of table
  cardinality.

### What is still open, and it is not a race

- **No unique constraint.** Two `INSERT`s of the same key, or two `MERGE`s
  that both take the `WHEN NOT MATCHED` arm, both insert. Neither mints a
  marker, so no conflict rule can see them. PostgreSQL behaves the same way
  without a unique index, and wadjet has no unique indexes; this is a missing
  CONSTRAINT, not a missing conflict rule, and it is out of this record's
  scope.
- **No transactions.** The rule is per statement. `BEGIN`/`COMMIT` are
  accepted and ignored, so nothing spans two statements.
- **The byte leak is unchanged.** A refused attempt's parquet objects stay in
  the store, unreferenced, and there is no orphan sweep (see Consequences
  above). A row conflict makes a redo more likely than a compaction race did,
  so the leak is reachable more often — it is still bytes, never rows.

### Gate

`TestConcurrentDMLLeavesOneOfTwoSerialOrders`
(`internal/server/pgwire/dml_statement_race_test.go`) — eleven interleavings ×
three doors. Each asserts the table is **one of** the states a serial order
could produce, the tag is one of the tags that order carries, AND whether the
statement redid itself (`DB.DMLRedos()`), which is the boundary claim: the
disjoint-row cells must commit with zero redos.
`TestCommitDMLRefusesAMarkerForARowAlreadySuperseded` pins the catalog's half
directly, including that a partially overlapping marker batch is refused
WHOLE. Both were confirmed to fail with the row check backed out, with exactly
the tables in the amendment's table above (33 and 1 failing subtests).

`TestConcurrentDMLStormReports40001` covers exhaustion, and its assertion is
the ERROR'S CAUSE and the REDO COUNT rather than the class. The class alone
could not gate it: the fixture's always-armed hook bumps the manifest revision
inside `CommitDML`'s own CAS loop as well, so with the row check backed out the
commit exhausts its own `maxRetries` and returns a PRE-EXISTING 40001 ("DML
commit failed after 10 CAS retries") that satisfies a class assertion — and
satisfies "the table is intact" too, because the statement never commits either
way. It passed 3/3 reverted. It now asserts that the 40001 carries
`ErrDMLRowSuperseded`, that its text is the STATEMENT's give-up rather than the
catalog's, and that the statement redid itself its full bound; reverted, it
fails 3/3 naming all three.

## Amendment (2026-09-06, arc INGEST-CORR, #919): the ingest flush commits against the incarnation it buffered

### What the rule missed

The rule read "a DML *statement* commits against the manifest it read". The
micro-batch ingester (`internal/storage/ingest.Ingester`) is the other writer,
and it held no incarnation identity at all. It carries a table NAME and a
schema, and its flush called `Catalog.AddNewFiles` by name; `addFiles` loaded
whichever manifest currently answers to that name and CAS-updated it, with no
check that it is the table the buffered rows were accepted for.

`DROP TABLE t` deletes `manifest.t`; `CREATE TABLE t` writes a fresh empty one
under the same key. An ingester that buffered rows for the first incarnation,
then flushed after the drop-and-recreate, registered its file into the *new*
table's manifest — the new table now contained rows nobody inserted into it.
Reproduced deterministically (`TestReviewOldIngesterWritesRecreatedTable`): the
flush returned `nil` and `events` held the stale `id:123`. This is distinct
from closed #483 (a stale manifest *cache*): revision freshness works, and
reading the *newest* manifest is exactly why the stale producer reaches the new
table.

### The decision

**A table's manifest carries a per-CREATE `Incarnation` identity, stamped by
`CreateTable`, and an ingest flush commits against the incarnation the rows were
buffered for.** An ingester binds to the incarnation the first time it retains a
row (`TableIncarnation`), and its flush goes through
`Catalog.AddNewFilesForIncarnation`, which — INSIDE the CAS, atomic with the
write — refuses with `ErrTableIncarnationChanged` if the live manifest's
incarnation is not the one bound. The check is inside the CAS on purpose: a
lookup before the upload would still race a DROP+CREATE landing between the
check and the commit. A refused flush writes nothing into the new table; the
error reaches the ingester's owner, which discards or re-routes the buffered
rows. The uploaded parquet object is an unreferenced orphan — bytes, never rows,
the same residual this record already accepts for a refused DML retry.

An empty expected incarnation skips the check: a manifest written before this
field existed carries none, and refusing every legacy ingest would be worse than
the bug this closes for the new tables that do carry one. `AddNewFiles` keeps
its unguarded behaviour for compaction and delete-marker GC, which mint their
paths against a manifest they are holding right now — the same split this record
draws between `CommitDML` and `AddDeleteMarkers`.

### Alternatives rejected

- **Check the table exists before uploading.** A pre-upload lookup is not atomic
  with the commit: a DROP+CREATE landing between the lookup and the CAS passes
  the lookup and still writes into the new table. The guard must be inside the
  CAS, which is where the incarnation is compared.
- **Carry the incarnation in `TableMeta` and compare that.** The flush already
  reads and writes the *manifest* under CAS; the incarnation belongs where the
  atomic write is. Comparing `TableMeta` would be a second read of a second key,
  not atomic with the manifest CAS.
- **Bind at `New()`.** The constructor reports no error and reads no catalog. An
  ingester that is constructed, sits idle across a recreate, and only then
  buffers its first row should bind to the incarnation it actually buffered
  against — so the binding is taken lazily, at the first retained row.

### Gate

`TestReviewOldIngesterWritesRecreatedTable`
(`internal/storage/ingest/adversarial_review_test.go`) — the same-schema
recreate, asserting the new table is empty; confirmed to fail reverted (the
flush wrote `id:123`). `TestReviewOldIngesterRefusedOnIncompatibleRecreate`
asserts the refusal is BY incarnation (`errors.Is(err,
catalog.ErrTableIncarnationChanged)`), not by a schema clash, over an
incompatible recreate. `TestReviewFreshIngesterAfterRecreateWrites` is the
boundary: a fresh ingester after the recreate writes normally, so the guard
refuses only the stale producer, not every ingest.
