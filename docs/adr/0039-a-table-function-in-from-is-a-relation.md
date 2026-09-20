# ADR-0039: A table function in FROM is a relation, and where its columns come from decides where a reference to a missing one is refused

Status: Accepted (2026-09-19, #1210 / #1203 / #1211, arc TF; amended the same
day after the arc's round-1 review — §4a, §6a and the Consequences)

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

   **A TERM IS NOT A COLUMN NAME.** The plan carries some of those needs as
   rendered TEXT — a GROUP BY term is the term's text, and an ordinal or a
   select alias arrives already resolved to the select item's text — so every
   such field is PARSED and the references it actually makes are taken. Read
   as names they ask for a column no relation has: `GROUP BY a + 1` over a
   reader was `42703 column "a + 1" does not exist`, a new refusal on eight
   spellings PostgreSQL answers and this engine answered right (the round-1
   review's B3). A term that will not parse cannot be enumerated with
   certainty, and takes the same exit as everything else that cannot.

4a. **A JOIN ARM is held to its columns too** (added after round 1;
   `physical.stampTableFuncRequiredColumns`). Over a join the consumer's
   input is the join's OUTPUT and a bare name there may belong to either arm,
   so round 1 made no check at all and a reference to a column a reader does
   not publish answered NULL for every row — the very thing this ADR's rule
   forbids. Two classes of name are certain directly above a join:

   - a reference QUALIFIED by the arm's own alias. The consumer sits DIRECTLY
     above the join, so nothing between them has minted a column under that
     qualifier — which is what separates this from the accumulated need set
     ADR-0026 §4b warns about, where a derived alias can qualify a
     projection's output.
   - a BARE reference no OTHER arm can provide, decidable only when every
     other arm declares its columns. With a SECOND reader in the join neither
     can be held to a bare name, and the check is declined outright.

   The join's own condition is qualified per arm by construction and is read
   the same way. The names travel on the logical Scan
   (`logical.Node.FuncRequiredColumns`), because a join arm's source is
   wrapped where the arm is built and not above the join.

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

6a. **A JOIN ARM IS A FROM ITEM**, and §6 applies to it unchanged (added
   after round 1). The rebuild wrote a join's right arm as its bare NAME
   while writing the comma-separated items as calls, so a correlated subquery
   whose table function is a join ARM answered 0 for every outer row — §6's
   own defect, one clause lower. `JoinInfo.RightTableRef` carries the whole
   item, so the arm is emitted exactly as a FROM item is.

7. **A DECLARED int4 key meeting a relation with no declarations at all is
   keyed at int8.** The pair cannot be widened from declarations — a reader
   has none, by §3 — so an int4 key met an int8 vector at the operator and
   was refused (#615). This is ADR-0024 §2a's rule for an unknown integer
   width applied to a join key: guessing narrow is the unsafe direction, and
   the wide key holds every value the narrow one does.

8. **A series ends at the CARRIER's edge.** `s.cur += s.step` wraps at
   int64's boundary and a wrapped counter sits on the other side of the
   bound, so neither loop exit was reached and the source emitted batches
   forever, every row after the wrap a value the series does not contain. The
   step is taken only when it stays inside the carrier.

## Consequences

The boundaries this leaves are recorded on `docs/postgres-differences.md`, and
each is a consequence of (3), not an oversight:

- a reader that produces NO batch is never measured, so an unknown column over
  an empty file answers zero rows where PostgreSQL raises, and `EXPLAIN` over
  such a statement does not refuse — the same boundary #1184's `42P10` has;
- an aggregate over a reader's column declares `double precision`, and a
  qualified star over one is `0A000`, because both need the column list at
  plan time;
- a bare reference to a reader's column through a join that holds a SECOND
  reader **answers NULL for every row** where PostgreSQL raises 42703. That
  is a silent WRONG VALUE, not a superset: `SELECT zz FROM read_json('a.json')
  b JOIN read_json('a.json') c ON b.a = c.a` returns rows with a NULL `zz`.
  §4a's certainty rule is what leaves it — neither arm can be held to a bare
  name when both have unknown column lists, and declining is the only honest
  answer a guard built on certainty can give — but declining to CHECK is not
  the same as being right, and this consequence is the one place the ADR's own
  rule ("never a NULL at run time") does not yet hold. Filed as #1229. A
  QUALIFIED reference in that join IS refused, which is the workaround and the
  measurement that isolates it.

Closing the first three is one change — a post-authorization annotation pass
both doors reach — and it moves a coordinator call site, which is why this
ADR states the ordering rather than working around it.

`generate_series` also stopped negating the caller's step: a call whose bounds
run the other way from its step is an EMPTY relation on 17.11, and a positive
default step was flipped whenever start > stop. That is a value rule of the
function, not of this position, and it lives with the rest of them in
`docs/sql-reference.md`.

## Gates

- `wadjet.TestArcTFATableFunctionInFromIsARelation` — 31 value/refusal
  subtests and 9 declaration subtests over `generate_series`, `unnest`,
  `read_json`, `read_csv` and `read_parquet`, every expectation measured on
  PostgreSQL 17.11.
- `wadjet.TestArcTFAnExpressionTermOverAReaderIsNotAColumnName` (§4's term
  rule), `wadjet.TestArcTFAReaderAsAJoinArmIsHeldToItsColumns` (§4a),
  `wadjet.TestArcTFACorrelatedSubqueryOverAJoinArmBindsTheOuterRow` (§6a and
  §7), `wadjet.TestArcTFANegatedSortTermSortsByTheNegation`,
  `physical.TestArcTFTheSeriesEndsAtTheCarriersEdge` (§8).
- `coordinator.TestArcTFATableFunctionIsARelationOnEveryArm` — the same
  property on single / single+budget / dag / dag-shuffled / dag+morsel4, with
  the pre-existing `distributed` pin (a table function as the only FROM item
  is not a DAG stage) carried per cell rather than chased.
- `sql.TestArcTFATableFunctionReadsASignedNumberAsOneArgument`,
  `physical.TestGenerateSeries_DescendingBoundsWithTheDefaultStepAreEmpty`.
