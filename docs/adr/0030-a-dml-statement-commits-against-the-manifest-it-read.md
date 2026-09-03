# ADR-0030: A DML statement commits against the manifest it read

Status: Accepted (2026-09-03, the DML-hygiene arc: #691, with #815's
one-implementation merge underneath it).

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

Compaction lands in exactly that window and removes exactly those files.
`compaction.mergeGroup` is `RemoveFiles` (which strips the markers for the
paths it removes) followed by `AddNewFiles`. Reproduced deterministically
on all three doors, with the real compactor inside a real `db.Execute`:

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
right now (the GC, compaction, and their tests). It is not a DML entry
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

- **What is still not closed, and it is not lost-update.** Two writers racing
  each other — not a compactor, but two `UPDATE`s over the same rows — both
  succeed, and the second one's markers are valid because the files did not
  move. The outcome is DUPLICATION: each reads the row at its own revision,
  each writes a replacement, each marks the copy it read, and the key ends up
  present twice (measured: `[1:111:a 1:222:a 2:20:b 3:30:c]`). This record
  first called it "lost update", which is the wrong failure mode — nothing is
  lost and nothing wins. Closing it needs a conflict rule over ROWS, which
  this record does not decide. ADR-0020's honesty requirement
  (`:112-128`) applied in reverse: this one is a closure of the
  compaction-window shape and a narrowing of nothing else, and the record
  says which.
