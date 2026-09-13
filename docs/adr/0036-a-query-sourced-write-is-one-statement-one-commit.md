# ADR-0036: A query-sourced write is one statement with one commit, and its schema is the plan's declared output

Status: Accepted
Date: 2026-09-12
Issue: #1024

## Context

Until now every write door in this engine was ROW-oriented: `INSERT … VALUES`,
`COPY … FROM STDIN` over pgwire, and the Go/HTTP/gRPC micro-batch ingester.
`CREATE TABLE foo AS SELECT * FROM bar` was refused at the parser (`CREATE
TABLE: expected '(' after table name`) and `INSERT INTO t SELECT …` with
`expected VALUES`. A user who wanted a query's result as a table had to read it
out, decide its schema by hand, `CREATE TABLE` it, and write the rows back in.

That is three decisions a user should not have to make, and two of them are
ones the engine already makes better:

- **What the columns are called and what type they have.** A query's DECLARED
  OUTPUT is the one list every arm agrees on (ADR-0026 §8), and it carries the
  names (including PostgreSQL's `?column?` for an unaliased expression), the
  types, and a DECIMAL's `(precision, scale)` from one inference. A schema
  re-derived by hand is a second inference that can disagree.
- **What this identity may see.** The security projection is applied at the
  scan and a denied column does not exist above it (ADR-0033, #994). A schema
  taken from the CATALOG rather than from the enforced plan would put a denied
  column into a brand-new relation that carries no policy of its own — an
  irreversible disclosure, not a wrong answer.

The third decision, atomicity, has no good manual answer at all: a
create-then-load pair leaves an empty table behind when the load fails, and a
load that commits per flushed file leaves half a result behind when the query
does.

## Decision

**A statement whose SOURCE is a query is one statement: the query runs through
the ordinary planner, its DECLARED OUTPUT is the schema, its rows go through
the ordinary ingest path, and the whole thing commits in ONE catalog write or
none.**

Four parts.

**1. The query is not special.** `CREATE TABLE … AS <select>` and `INSERT INTO
… <select>` carry a whole SELECT statement, and that statement is parsed by the
ONE parser a bare SELECT goes through (`plansql.parseSelectStatement`, extracted
from `parseDispatch` for this reason) and planned by the ordinary planner and
optimizer. A SELECT that answers X on its own answers X inside a write. There
is no second SELECT dialect inside a write, and no clause of one is skipped: a
second call site that re-ran three of the five post-parse passes would be a
second dialect, and `resolvePositionalRefs` alone decides what `GROUP BY 1`
means.

**2. The schema is the plan's declared output, after enforcement — and ONE
derivation.**
`ingest.TableSchemaForQuery` turns `[]parquet.Column` into the new table's
schema. It never infers `NOT NULL` (PostgreSQL does not); it applies the
optional column-name list POSITIONALLY and accepts a SHORT one (PostgreSQL
does); it refuses a duplicate name `42701` and a too-long list `42601`; and it
asks `parquet.ValidateWriteSchema`, so a declared type the writer cannot store
is refused at CREATE rather than at the first flush (ADR-0018 §14). The list it
is given comes from the EXECUTED plan — `QueryResult.OutputSchema`, which is
the CollectSink's schema. `WITH NO DATA`, which does not execute, takes its
TYPES from `Planner.DeclaredOutputSchema` over the same enforced plan and its
NAMES from the same CollectSink, by building the physical plan and not running
it: the sink answers `Schema()` from its plan-time hint when no batch was
consumed. Two rounds of review were spent on the consequence of taking the
names from anywhere else — the walk spells an unaliased item by its expression
TEXT, so `WITH NO DATA` declared `"n + 1"` where `WITH DATA` and PostgreSQL say
`?column?`, and with the name went the duplicate rule. Both are post-
enforcement, which is what makes rule 2 of the Context hold.

**2b. Every value goes through the one assignment conversion.** (Added
2026-09-13, the round-2 review.) A CREATE needs none — its target columns ARE
the query's declared output — but an APPEND has two type lists, and the value
has to cross between them. It crosses through `assignEvaluatedValue`, the
converter `INSERT … VALUES` and `UPDATE … SET` have used since #647/#678, at the
STATEMENT door: the one place that holds both the source's declared type (the
plan's output schema) and the target's (the catalog). Handing the query's BOX
to the writer instead is not a smaller version of this — it is a different
answer, because `parquet.DecimalValueFromBox` reads an integer box as the
already-UNSCALED carrier (ADR-0018 §4) and stored a BIGINT 5 into a
`DECIMAL(18,4)` as 0.0005, and because no leaf check narrows PORT to its uint16
or PROTOCOL to its uint8, so `PORT 500000` landed in a table.

**3. One commit, and a failure reclaims.** The Ingester's
`DeferManifestCommit` (built for #691) holds every flushed file out of the
manifest, and the statement ends in one catalog write:

- `catalog.CreateTableWithFiles` for a create — the table's FIRST manifest
  already holds the rows, so the name never resolves to an empty table. The
  three catalog records a table is (`table.<name>`, `manifest.<name>`, the
  `meta` list) are one commit or none: every payload is ENCODED before any key
  is written, and a write that fails after an earlier one succeeded rolls the
  earlier ones back. Readers do not agree about which record IS the table —
  `GetTable` reads the first, `DropTable` goes through the last — so a partial
  write leaves not a half-table but a WEDGED NAME, one that cannot be read,
  created or dropped (the round-2 review reached it from ordinary SQL);
- `catalog.CommitIngest` for an append — one CAS, validated INSIDE the CAS
  against the table incarnation the STATEMENT read (#919, ADR-0030).

A statement that fails retires the objects it uploaded through
`catalog.RetireObjects`, which deletes only what no live manifest references.
That is stricter than the residual ADR-0030 accepts for a refused DML retry
("bytes, never rows"), and it can be: a retry may legitimately re-run and needs
its objects, while a failed statement is over.

**4. The tag is PostgreSQL's, and the split is the QUERY.** `SELECT <n>` when
the query ran and wrote n rows; the bare `CREATE TABLE AS` when it did not —
`WITH NO DATA`, or an `IF NOT EXISTS` over a taken name; `INSERT 0 <n>` for an
append. Measured on PostgreSQL 17.11, all four. A query-sourced write is
therefore a WRITE at every door: pgwire's `isWriteSQL` routes it to the Execute
path, because a CTAS routed as a query would answer `SELECT 1` over the one row
the embedded door boxes a tag into — #816's defect, for a statement #816 did
not have.

## What this record does NOT decide, and the mechanism for it

**The query of a query-sourced write does not run on the stage DAG.** Every
write in this engine is single-process: `Coordinator.ExecuteSQL` refuses every
statement that is not a SELECT, and `pgwire.shouldRouteThroughCoord` routes
only SELECT/WITH to it, so `INSERT`, `UPDATE`, `DELETE` and `MERGE` have always
executed on the node that received them. MERGE's own source read
(`db.Query("SELECT * FROM <source>")`) is exactly this shape. A CTAS joins that
family rather than breaking it.

The boundary is LOUD where it bites, and since the round-2 review it is also
REACHABLE: the statement gathers the whole result before it writes, and the SIZE
OF THAT RESULT is bounded by `DefaultQuerySourcedWriteBytes` (64 MiB, the same
number `--local-fastpath-bytes` uses for a gathered result) or by
`Config.MemoryBudget` when one is set. Past it the statement is `53400` naming
the bound (`querySourceError`), never a silently truncated table. It had been
unreachable: `CollectSink.MaxBytes` was set only inside `internal/coordinator`,
which refuses these statements `0A000` before it gets there, so a 99 MiB CTAS
was never refused at all.

**The bound is on the RESULT, not on the process.** Measured in the round-2
review: a 94 MiB result refused against a 64 MiB bound still peaked at 1400 MiB
of Go heap, and the IDENTICAL query as a plain unbounded `SELECT` peaked at
1500 MiB. The remainder is `DB.Query`'s `map[string]any` per row — the pattern
`CollectSink`'s own comment records as having held 21 GB at SF10 Q18 — and it
belongs to reading a result of that size, not to writing it. Nothing here
claims otherwise, and the statement is bounded by the same arithmetic every
other reader of a large result is.

STREAMING the result into the writer is the step that removes the bound rather
than enforcing it, and it is the smaller half of the deferral above: the writer
is a `Sink`, `CollectSink` is not a `MergeableSink` so a pipeline shares ONE
sink across its morsel workers, and a mutex-protected sink that boxes each batch
into the ingester as it arrives is all the shape needs. What it costs is the
schema: the table's declaration would come from the first batch rather than from
the completed plan. The `WITH NO DATA` arm already reads that declaration from
the same place — it BUILDS the physical plan and takes `CollectSink.Schema()`
from it without running it, which is what round 3 settled after the round-2 fix
left a star over a subquery naming its inner unaliased item by expression text —
so the agreement between the two arms is already a shared derivation rather than
two that happen to match. `TestTheTwoArmsOfACreateDeclareOneTable` (21 shapes,
including eight stars over a CTE or a derived table) is the gate for it.

Lifting it is a separate arc, and the mechanism is written down here so it is
not redesigned: `Coordinator.ExecuteSQL` gains a branch for `QueryCreateTable`
and `QueryInsert` that runs the inner query through its OWN path — recursing
into `ExecuteSQL`, which picks the local fast path or the DAG — takes
`SQLResult.OutputSchema()` as the declaration and the gathered batches as the
rows, and calls the SAME `ingest.WriteQueryRows`. What must move FIRST is the
statement's DECISION half: authorize, the 42P07 / `IF NOT EXISTS` test, the
schema derivation, the tag. Those live in `wadjet/ctas.go` today and
`internal/coordinator` cannot import that package. They belong in a package
both can call. A second copy of them inside the coordinator is the split #815
closed for the four DML verbs and is not acceptable.

The per-worker PARALLEL write — each stage writing its own files under the new
table — is the step after that, and the manifest already accepts the multi-file
commit it needs (`CommitIngest` takes a list).

## Alternatives rejected

- **Create the table, then load it.** The obvious shape, and it has an empty
  table visible to every concurrent reader for the length of the load, plus an
  empty table left behind when the load fails. Wadjet has no transactions to
  hide either. Seeding the first manifest costs one new catalog entry point and
  removes both.

- **Rewrite the query to cast each item to the target column's type**, the way
  PostgreSQL's `transformAssignedExpr` does, so `INSERT INTO … SELECT` accepts
  every pair PostgreSQL accepts. Wrapping the projection in a derived table
  (`SELECT CAST(c0 AS T0), … FROM (<inner>) x`) reuses the whole cast machinery
  and is the right long-term answer — but the inner query may publish two
  columns of one name, which the outer references cannot address, so the
  rewrite is wrong for exactly the shapes it would have to be right for. Until
  the projection can be cast at the PLAN rather than in text, the narrower
  assignment set plus a `42804` naming both types is the honest position
  (ADR-0012's divergence list).

- **Convert values at the writer.** The same thing one layer down and worse: a
  DECIMAL box carries an unscaled integer and no scale, so the writer cannot
  know what it is converting FROM. That is how a `DECIMAL(12,3)` 1.500 becomes
  0.1500.

- **Derive the new table's schema from the catalog for a `SELECT *`.** Cheaper,
  and it re-introduces #994: the catalog's column list is not the list this
  identity may read.

- **Refuse `WITH NO DATA` and let the user write `WHERE false`.** They are not
  the same statement. `WITH NO DATA` does not EXECUTE the query, so a query
  that would raise on row five still declares the table — which is the whole
  point of the clause and is what PostgreSQL does.

## Consequences

- Gates (round 2): `wadjet.TestBothWriteDoorsStoreTheSameNumber` and
  `TestADecimalSourceIsAssignedAtItsValue` (the two doors' stored VALUES, pair
  by pair, against each other and against PostgreSQL 17.11);
  `TestAQuerySourcedWriteRefusesTheSameRangesTheOtherDoorsDo`;
  `TestACreateWhoseCommitFailsLeavesTheNameFree` (a failure injected at each of
  the three catalog keys in turn, asserting the name stays REUSABLE);
  `TestANonFiniteValueIsStoredAndItsBoundIsDropped`;
  `TestACancelAfterAFileLandsStillReclaimsIt` (over a store that honours the
  context, which MemStore does not); `TestAQuerySourcedWriteRefusesPastItsGatherBudget`;
  `TestTheTwoArmsOfACreateDeclareOneTable` and `TestTheDuplicateNameRuleHoldsOnBothArms`;
  `TestIfNotExistsIsHonouredOnBothFormsOfCreateTable`;
  `TestADefinitionListAndAQueryCannotBothBeWritten`; and the oracle wire arm's
  `CreatedTableSchema` — `information_schema.columns` after the same CTAS on
  both engines, both arms.

- Gates: `wadjet.TestACreatedTableHoldsExactlyWhatTheQueryAnswered` and
  `TestACreatedTableHoldsEveryColumnOneAtATime` (all 22 types plus the DECIMAL
  widths and the containers, both read paths, with a PyArrow cross-check of the
  written file — the compaction gate's rule for a second writer);
  `TestACreatedTableDeclaresWhatTheQueryDeclares` (sixteen query shapes, the
  declaration compared with the bare query's);
  `TestAQuerySourcedWriteRefusesWhatPostgresRefuses` (every refusal measured on
  17.11 first); `TestAFailedQuerySourcedWriteLeavesNothingBehind`,
  `TestTwoCreatesRacingForOneNameLeaveOneTable`,
  `TestACreateReadsOneManifestOfItsSource`;
  `ingest.TestTableSchemaForQuery`, `TestAssignableToColumn`,
  `TestARefusedCommitReclaimsWhatItUploaded`;
  `server.TestACreateTableAsSelectReadsThroughThePolicedList` (the nine doors);
  `pgwire.TestAQuerySourcedWriteSendsPostgresCommandTag` (both protocols);
  `coordinator.TestATableAQueryWroteReadsTheSameOnEveryArm` and
  `TestTheCoordinatorRefusesAQuerySourcedWriteByName`; and six entries in the
  PostgreSQL oracle's wire command-tag corpus.

- `CREATE TABLE … AS SELECT` creates an UNPARTITIONED table. PostgreSQL's
  grammar has no `PARTITION BY` on this statement either; a partitioned target
  is created with the declared form and filled with `INSERT INTO … SELECT`.

- The embedded API and the gRPC `Query` RPC now render a write's answer through
  `ExecResult.Tag()` rather than an inline format, so they stop reporting
  `INSERT 1` where pgwire and REST report `INSERT 0 1`.
  `docs/api-reference.md` has promised the tag does not depend on the door
  since review B8; it is now true on all four.

## Related

- ADR-0026 §8 (the DAG publishes the plan's column set and order)
- ADR-0030 (a DML statement commits against the manifest it read)
- ADR-0033 / ADR-0034 (a column policy is a plan-time projection; every door
  authorizes before it acts)
- ADR-0018 §14 (a writer refuses what it cannot write exactly)
- ADR-0012 (the divergence list: the assignment-cast set, the self-join star
  superset, the missing NOTICE)
