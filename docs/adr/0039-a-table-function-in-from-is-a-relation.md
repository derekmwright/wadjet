# ADR-0039: A table function in FROM is a relation, and where its columns come from decides where a reference to a missing one is refused

Status: Accepted (2026-09-19, #1210 / #1203 / #1211, arc TF)

## Context

A table function — `generate_series`, `unnest`, `read_json`, `read_csv`,
`read_parquet`, `postgres_scan` — is a FROM item like any other. Every layer
above it reads a relation: a projection names its columns, a filter compares
them, an aggregate types them, a correlated subquery binds the outer row
beside them.

Through v0.22.0 it was not treated as one. `physical.resolveSource` set
`into.open = true` for every table-function FROM item, and
`physical.annotateScanColumns` skipped `IsTableFunc` outright. Two silences
followed from that one omission, and a third from a neighbouring layer:

- **A reference to a column it does not publish answered NULL for every
  row** (#1210, priority:high). An open scope proves nothing absent, so the
  binder let the reference through; the projection compiled it to a column
  reference that resolves against the batch, found nothing, and answered NULL.
  `SELECT zz FROM read_json('x.json')` and `SELECT generate_series FROM
  generate_series(1,2) AS g(x)` both did this where PostgreSQL 17.11 raises
  `42703`.
- **Its column's TYPE never reached its consumers** (#1211). With no
  `ScanColTypes`, the aggregate result-type rules had nothing to read and fell
  to the float64 rule: `SUM(x) FROM generate_series(1,3) gs(x)` declared and
  boxed `double precision` where 17.11 declares `bigint` — ADR-0024's "an
  integer SUM is an exact type", lost at the relation.
- **A correlated scalar subquery over one answered 0** (#1203). The per-row
  re-run rebuilds the subquery's TEXT, and the rebuild wrote every FROM item
  as its NAME. For a table function the name alone is not the relation, so
  `generate_series(1,2) AS g(x)` was re-emitted as `generate_series g(x)`,
  which re-parses as a base table nothing declares.

## Decision

**A table function in FROM is a relation, and it publishes a column list.**
Every rule that holds for a base table's columns holds for its: an unknown
column is `42703` naming it, the declared type rides the column into every
consumer, the alias clause's column list renames positionally, a qualified
star expands, and a relation with no rows still publishes its columns.

**WHERE the column list comes from decides WHERE a missing column is
refused**, and the line is drawn by AUTHORIZATION, not by convenience.

1. **A function whose SIGNATURE declares its columns is a plan-time
   relation.** `generate_series` and `unnest` compute over their own
   arguments; their column list is a pure function of the call
   (`physical.tableFuncDeclaredSchema`). The binder closes the scope, so the
   refusal is `42703` at plan time exactly as over a base table, the
   over-long alias list is `42P10` before anything runs, and the annotation
   pass stamps the same `ScanColumns` / `ScanColTypes` a catalog table gets —
   one body for both (`physical.stampScanSchema`), so a column of a given
   type is annotated identically whichever relation it came from.

2. **A function whose columns are its INPUT's is refused at its FIRST
   BATCH, loudly.** Every file and database reader is in this class. The
   refusal names the column and lists what the relation publishes, and its
   class is PostgreSQL's:

       42703 column "zz" does not exist: the table function "read_json" publishes a, b

3. **The planner does not open a reader's input to learn its schema.** This
   is the reason for (2), and it is ADR-0034's ordering, not an efficiency
   argument. The statement's column binding runs BEFORE the table-function
   capability is authorized on every door: `auth.ValidateStatementColumns`
   precedes `auth.EnforcePlanPolicies` on the embedded, coordinator and HTTP
   doors, and on the coordinator door `physical.AnnotateScanColumns` does
   too. Sampling the file or the remote query at either point would read it
   for an identity that may not be allowed to — the property #943's gate
   asserts with a path that errors if opened and an httptest server that
   fails the test if it is hit. A plan-time schema for readers is therefore
   not available until that ordering changes, and until it does the
   first-batch refusal is the honest answer.

4. **The names checked at the first batch are the ones the operator DIRECTLY
   ABOVE the relation asks of it** — a Filter, a Project, a Sort, an
   Aggregate (`physical.guardTableFuncColumns`). That position is what makes
   them certain: the batch the source publishes IS the relation, nothing has
   renamed or minted anything yet, and a reference that resolves to no column
   of it resolves to nothing at all. A consumer further up reads a schema
   some operator has already changed, and its names are not this relation's
   to answer for. An enumeration that cannot be made with certainty — a
   subquery, a window call, a positional sort term, a predicate carried as
   text — makes NO check, because a refusal built from an incomplete
   enumeration refuses a column that is there.

5. **An integer table function publishes the width PostgreSQL's overload
   publishes.** `generate_series(int4,int4)` and `unnest` over int4-fitting
   literals declare `integer`; wider arguments declare `bigint`. The
   declaration is what the aggregate rules read, so the SUM is `bigint` and
   not `numeric`, which is what 17.11 declares for the same call — and the
   SOURCE fills the vector the declaration names, because a declaration its
   carrier does not match is ADR-0024 §2a's own hazard.

6. **A correlated re-run rebuilds the CALL, not the name.** The parser
   records the call's argument list verbatim (`plansql.TableRef.FuncCallText`)
   and the rebuild emits it, with `WITH ORDINALITY` and the alias clause. The
   arguments cannot be reconstructed from the parsed `FuncArgs`: the lexer
   strips a string literal's quotes, so by then a path and an identifier are
   the same bytes.

## Consequences

The boundaries this leaves are recorded on `docs/postgres-differences.md`, and
each is a consequence of (3), not an oversight:

- a reader that produces NO batch is never measured, so an unknown column over
  an empty file answers zero rows where PostgreSQL raises, and `EXPLAIN` over
  such a statement does not refuse — the same boundary #1184's `42P10` has;
- an aggregate over a reader's column declares `double precision`, and a
  qualified star over one is `0A000`, because both need the column list at
  plan time;
- a QUALIFIED reference to a reader's column through a JOIN arm is still a
  NULL: the consumer of a join arm is the join, whose bare names belong to
  both arms, and the join node's accumulated need set can name a projection
  OUTPUT through a derived alias (ADR-0026 §4b), so a refusal built from it
  would not meet (4)'s certainty rule.

Closing all three is one change — a post-authorization annotation pass both
doors reach — and it moves a coordinator call site, which is why this ADR
states the ordering rather than working around it.

`generate_series` also stopped negating the caller's step: a call whose bounds
run the other way from its step is an EMPTY relation on 17.11, and a positive
default step was flipped whenever start > stop. That is a value rule of the
function, not of this position, and it lives with the rest of them in
`docs/sql-reference.md`.

## Gates

- `wadjet.TestArcTFATableFunctionInFromIsARelation` — 24 value/refusal cells
  and 9 declaration cells over `generate_series`, `unnest`, `read_json`,
  `read_csv` and `read_parquet`, every expectation measured on PostgreSQL
  17.11.
- `coordinator.TestArcTFATableFunctionIsARelationOnEveryArm` — the same
  property on single / single+budget / dag / dag-shuffled / dag+morsel4, with
  the pre-existing `distributed` pin (a table function as the only FROM item
  is not a DAG stage) carried per cell rather than chased.
- `sql.TestArcTFATableFunctionReadsASignedNumberAsOneArgument`,
  `physical.TestGenerateSeries_DescendingBoundsWithTheDefaultStepAreEmpty`.
