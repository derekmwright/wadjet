# ADR-0039: A table function in FROM is a relation, and where its columns come from decides where a reference to a missing one is refused

Status: Accepted (2026-09-19, #1210 / #1203 / #1211 / #1202, arc TF; amended
the same day after the arc's round-1 review — §4a, §6a, §7, §8 and the
Consequences; the Consequences amended again after the closure review, which
measured the two-reader residue as a silent wrong VALUE rather than an
unchecked shape — #1229; amended 2026-09-20 by arc FR, which CLOSED §3's
deferral: §3 is now the authorization ORDER and the plan-time schema it
buys, §9 is the join key's side, and the Consequences are the boundaries that
remain — #1229 / #1230 / #1231; amended 2026-09-22 by arc RP, whose paragraph
in §3 is what a row past the readers' inference sample does — #1242 / #1243 /
#1247; amended 2026-09-23 by arc FR2, whose paragraphs in §3 are the glob as
a sequence of files, the sample's type reading every sampled value, and the
plan-time refusal of an input that cannot be opened — #1262 / #1240 / #1260 /
#1261 / #1245 / #1248 / #1259)

Related: ADR-0034 (the authorization ordering §3 rests on), ADR-0024 (§5's
declared width and §7's key widening), ADR-0012 §5 (the divergences this
position leaves, each on the differences page).

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

2. **A function whose columns are its INPUT's AND whose input is not read at
   plan time is refused at its FIRST BATCH, loudly.** After §3 that is an
   `http(s)` source and the database readers; a LOCAL file reader is refused
   at plan time like a base table. The refusal names the column and lists
   what the relation publishes, and its class is PostgreSQL's:

       42703 column "zz" does not exist: the table function "read_json" publishes a, b

3. **THE CAPABILITY IS AUTHORIZED FIRST, AND THEN THE PLANNER READS THE
   INPUT.** Through v0.23.0 this clause said the opposite, and it was right
   about the ordering it found: the statement's column binding ran BEFORE the
   table-function capability was authorized on every door, so sampling the
   file there would have read it for an identity that may not be allowed to.
   The answer is to fix the ORDER, not to leave the relation without a
   schema, and this clause now states the order.

   `auth.AuthorizeTableFunctions` is the FIRST thing every statement door
   does — the embedded `Query`, `EXPLAIN` and the CTAS / `INSERT … SELECT`
   declaration; the coordinator's `ExecuteSQL` and its async / `EXPLAIN`
   entry; the HTTP query and `EXPLAIN` doors, which pgwire, gRPC and MCP
   reach through the first two. It decides the statement's own FROM items and
   installs two things on the context: the `logical.TableFuncGuard` that
   every LATER call is decided by (a scalar / `IN` / `EXISTS` subquery, a CTE
   body, a scan the optimizer mints — all of them SQL text at this point),
   and `physical.ContextWithReaderSchemaProbe`, the record that this door
   authorized.

   The reader-schema resolver is **fail-closed on that record**: with no
   probe on the context it reads nothing and the relation keeps the open
   scope and the first-batch refusal of (2), and with one it still asks the
   guard about THAT call before it opens anything. The property is
   measurable, not only structural: `physical.ReaderSchemaReads` counts the
   opens, and the gates assert it does not move for a refused identity on any
   door.

   **What is read is bounded, and it is a SAMPLE.** A Parquet footer is exact
   and no page is decoded. `read_json` and `read_csv` read ONE BATCH through
   the very reader the query uses, so the plan-time schema and the run-time
   schema are the same inference rather than two guesses — but that inference
   is the readers' own 100-ROW sample (`csv.sampleSize`,
   `json.defaultSampleSize`), and it describes the whole file. A row past the
   sample with a non-NULL value that does not fit its column refuses with
   SQLSTATE `22P02` (arc RP, #1242, #1243). Every CSV and JSON read path
   checks before conversion or vector writes, against the sample the schema
   was ACTUALLY inferred from (the JSON stream reader's sample stops at
   8 MiB, so it can be fewer than 100 rows). The error carries the SQLSTATE
   PostgreSQL's COPY raises for the same field (22P02; 22003 out of range;
   22007 for a timestamp) and names the reader, input (across a glob, the
   matched file and its own row), 1-based data row, column, value and both
   types. A CSV field is read with PostgreSQL's input function for bigint,
   double precision and boolean (the kernel's, which CAST uses), and the
   sample infers a number — only in plain decimal spelling; `0x`/`0o`/`0b`
   prefixes and digit underscores keep an identifier-like column text — with
   the same int8/float8 functions, so a spelling inside the sample and its
   twin past it never disagree (a spelling neither holds, such as `1e-400`,
   infers text). Integer to
   fractional number is a mismatch; text columns accept every value as text.
   JSON null and UNQUOTED empty CSV fields are NULL; a quoted empty CSV field
   is the empty string, as COPY reads it (arc FR2, #1259, which reads CSV
   with COPY's record grammar and refuses an unterminated quote and a record
   of the wrong width with 22P04, #1248; blank lines and mixed line endings
   stay answered, ADR-0012 §5). COUNT(*) refuses when the reader reaches the row; a LIMIT that
   stops reading before it need not refuse. A key first seen past the sample
   remains absent from the inferred column list.

   **THE SAMPLE'S TYPE READS EVERY VALUE THE SAMPLE HOLDS** (arc FR2). The
   one-grammar rule holds INSIDE the sample as well as past it: a sample
   mixing booleans and numbers is text (it inferred bigint and read the
   boolean NULL or 1, #1260), and a nested column's element and field types
   are merged across every sampled occurrence at every depth (they were the
   first occurrence's, and `[1.5]` after `[1]` read `[1]`, #1261). So no
   sampled value is converted into a type that cannot hold it, and read_csv
   refuses a field that does not parse wherever it is.

   **A GLOB IS A SEQUENCE OF FILES** (arc FR2, #1262, #1240). Through
   v0.24.0 a glob was its matched files' BYTES run together and handed to a
   single-file decoder: a CSV header per file had to be re-parsed out of the
   stream (arc RP's special case), a JSON array glob stopped at the first
   file's `]` and dropped every later file's rows, and a Parquet glob of two
   files was unreadable. Every reader now takes the matched FILES in name
   order and decodes each on its own — its own CSV record state and header,
   its own JSON document, its own Parquet footer — one file open at a time,
   with ONE schema across them: the CSV/JSON sample is the first 100 rows of
   the sequence (crossing files when the first is short, as DuckDB's does),
   a later file's value past it that does not fit is refused naming that
   file and its row, and a Parquet glob's columns are the first file's
   footer's, a later file held to them by name (42703 for a missing column,
   42804 for another type — DuckDB casts). A directory a glob matches is not
   one of its files. A later CSV file whose header names the first file's
   columns in another order is read by name; one naming a different set is
   22P04 naming the file. A Parquet member is read ROW GROUP by row group
   through the staged reader, so memory follows the row group, not the file. The plan-time read and the execution read the same
   sequence, so this is not a new schema source: `parquetFooterSchema` has
   read the first file's footer since arc FR.

   **AN INPUT THAT CANNOT BE OPENED IS REFUSED AT PLAN TIME** (arc FR2,
   #1245). The resolver used to DECLINE it — the rereadable check stat'd a
   missing path and answered "not a regular file" — so the error came at the
   first batch with no SQLSTATE, and EXPLAIN printed a plan. It now asks
   first, after the capability guard, whether the input — every regular
   member of a glob — can be opened, and
   one that cannot is the statement's answer with COPY FROM's class: 58P01
   (missing, or a glob matching no file), 42501 (not readable), 42809 (a
   directory). EXPLAIN is refused with it. The execution's open carries the
   same classes for the inputs not read at plan time.

   **Nor is the plan-time read taken over an input that can be read ONCE.**
   It opens the input and the execution opens it again, so it is taken only
   over a REGULAR file — and, for a glob, only when EVERY match is one. A
   FIFO, a character device, a socket or a process substitution publishes no
   plan-time schema and is opened exactly once; consuming its first batch here
   left the execution reading a different stream (zero rows where the
   statement answered three) or blocking forever on an `open(2)` the
   statement's context cannot interrupt. An `http(s)` source is NOT read at
   plan time either — a plan-time fetch is a second request for every
   statement and would make `EXPLAIN` reach the network — and neither is a
   database connector, whose schema is a remote query's. All of them keep (2).

   **An input that CHANGES between the plan's read and the run's is LOUD**
   (`physical.withPlanTimeSchema`): a column the plan read that the arriving
   batch does not publish is 42703 naming it, a column that arrives at another
   type is 42804 naming both. It is a guard on the BATCH SCHEMA — a file
   replaced or truncated between the two opens — and not on a value inside a
   batch: both readers infer once per file, so the later rows of ONE file
   never carry another schema, and the paragraph above is what happens to
   them.

   **An EMPTY input is not the same as an absent column list.** A Parquet
   footer and a CSV header row declare columns with no rows, and that is an
   ordinary empty relation. An input that declares nothing at all — a
   zero-byte JSON or CSV — is `0A000` naming the function and its input,
   because a result with no columns is not an answer this engine has at any
   door. PostgreSQL permits a zero-column relation and this engine does not;
   the divergence is ADR-0012's, not this position's.

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

9. **A JOIN KEY'S SIDE IS ITS QUALIFIER WHEN THE RELATION'S COLUMNS ARE
   UNKNOWN** (added 2026-09-20, #1229). `physical.SubtreeNaming.ownsKey`
   decided a key's side from the column SETS, and a relation this planner
   could not read at plan time contributes an EMPTY set. So in a join between
   two such relations NEITHER arm owned EITHER key, `assignJoinKeySides` left
   the pair in the order the `ON` was written, and each key was resolved
   against the arm that does not have it. Two misses encode the same flag
   byte for every row, so every probe row matched every build row: `ON r2.c =
   r1.a` between a four-row and a two-row reader answered EIGHT where
   PostgreSQL answers two, while `ON r1.a = r2.c` answered two — the same
   condition, two row sets.

   The QUALIFIER already decided the side. `r2.c` is r2's column whether or
   not the planner knows what r2 publishes, so a qualifier naming a relation
   with an unknown column list OWNS the key, under every name the enclosing
   query can write it by: the FROM alias, a CTE reference's alias and name, a
   derived alias. A relation whose columns ARE known is unchanged — there the
   column set is the better answer, and the qualifier alone would claim a
   name the relation does not have.

   This is INDEPENDENT of §3: it holds for a relation with no plan-time
   schema, which is what the gate forces with
   `WADJET_TEST_NO_READER_SCHEMA=1`. The backstop under it is
   `exec.HashJoin.checkKeyPairResolved`: a key pair that names columns and
   resolves to none on EITHER side refuses, naming both keys and what each
   side publishes. The optimizer's ON-TRUE sentinel (`1 = 1`) is the one pair
   whose keys are LITERALS and resolve to nothing on purpose; it is told
   apart by the key's own text and still crosses.

## Consequences

The boundaries this leaves are recorded on `docs/postgres-differences.md`, and
each is a consequence of where a column list comes from:

- an `http(s)` reader and a database connector keep §2 whole: their unknown
  column is 42703 at the FIRST BATCH, their over-long alias list is 42P10
  there, one that produces no batch is never measured, and an aggregate over
  their column declares `double precision` because the result-type rules have
  nothing to read. `EXPLAIN` over such a statement does not refuse;
- a reader whose input declares NO columns is `0A000` naming the function and
  the input. PostgreSQL permits a zero-column relation (`CREATE TABLE t ()`)
  and this engine does not, at any door. Through v0.23.0 the same shape was
  `XX000 the result has no columns at all` — the engine reporting an internal
  invariant for a file the caller can see is empty;
- a file whose LATER rows carry a value the sample did not type is refused
  by the reader itself, with COPY's SQLSTATE for the field (§3's sample
  paragraph). Through arc FR the JSON reader wrote a string past the sample
  into a numeric column's storage and the query failed as a RECOVERED PANIC
  (`XX000 index out of range`); arc RP made it `22P02` naming the file, row,
  column and both types (#1243);
- a table function is still not a DAG stage (`stage scan-0 has no
  dependencies and no ScanFiles`). That is `distributed` and arc PT's pin;
  the five-arm gates carry it per cell rather than chasing it. A DAG fragment
  re-planned on a WORKER has no authorization record on its context, so a
  reader replanned there would take the first-batch path — unreachable while
  the pin stands, and named here so it is not discovered as a surprise when
  the pin is closed.

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

Arc FR's, for §3 and §9:

- `wadjet.TestArcFRAFileReaderIsARelationWithASchema` — the qualified star,
  the empty relation, the zero-byte refusal, an unknown column through eleven
  paths with a mirror set of the same paths answering, and the declarations
  (SUM numeric, MIN/MAX bigint, AVG numeric, COUNT bigint), every expectation
  measured on PostgreSQL 17.11 over an ordinary relation of the same schema.
  24 of its cells fail at 0c0d33b6.
- `wadjet.TestArcFRAJoinBetweenTwoReadersKeysOnItsCondition` — §9, 96 cells
  over three readers and their mixes × both operand orders × INNER / LEFT /
  comma / CTE / derived, run with `WADJET_TEST_NO_READER_SCHEMA=1` so the
  repair is exercised on a relation with NO plan-time schema. 50 cells fail
  at 0c0d33b6.
- `exec.TestArcFRAJoinKeyPairThatResolvesToNothingRefuses` — §9's backstop
  and the ON-TRUE sentinel that must still cross.
- `physical.TestArcFRAReaderSchemaIsReadOnlyUnderAnAuthorizedContext` — the
  seam: a bare context reads nothing, a refusing guard reads nothing, an HTTP
  source, a connector and a non-regular input read nothing, and the answer is
  cached per call.
- `physical.TestArcFRAnInputThatChangesBetweenThePlanAndTheRunIsLoud` — the
  batch-schema backstop, driven DIRECTLY because the condition it guards (the
  input changing between the two opens) cannot be forced from the SQL door
  inside one statement. It makes no claim about a value past the sample.
- `wadjet.TestArcFRAnInputThatCanBeReadOnceIsReadOnce` and
  `server.TestArcFRAFifoFedReaderAnswersOnTheWire` — a FIFO-fed `read_json`
  and `read_csv` answer their rows on the embedded door and on the wire, with
  a regular-file control that still gets its schema.
- `server.TestArcFRNoDoorOpensAReaderBeforeTheIdentityIsAuthorized` and
  `coordinator.TestArcFRTheCoordinatorDoorAuthorizesBeforeItReads` — the
  ORDER, per door (embedded, pgwire, HTTP, gRPC, coordinator fast path,
  coordinator DAG) × eleven shapes, asserted with `ReaderSchemaReads`.
- `coordinator.TestArcFRAFileReaderIsARelationOnEveryArm` — §9 on single /
  single+budget / dag / dag-shuffled / dag+morsel4, with arc PT's
  `distributed` pin carried per cell.

Arc FR2's, for §3's glob, sample and unopenable-input paragraphs:

- `wadjet.TestArcFR2AGlobIsASequenceOfFiles` — five reader formats × one
  file, a glob of 2, a glob of 100, empty files first / middle / last, a
  sample crossing files, a later file that disagrees × the plan-time and
  first-batch schema paths; `wadjet.TestArcFR2EachFileIsItsOwnDocument`;
  `physical.TestArcFR2AParquetGlobReadsEveryFileByItsOwnFooter` (PyArrow
  files); `physical.TestArcFR2AGlobHoldsOneFileOpenAtATime`;
  `json.TestArcFR2EveryFileIsItsOwnJSONDocument`; `physical.FuzzArcFR2Glob`.
- `csv.TestArcFR2CSVRecordGrammarIsPostgreSQLs` (48 cells from 17.11's
  COPY) and `csv.TestArcFR2AMalformedCSVIsRefusedWhereverItIs`.
- `wadjet.TestArcFR2TheSampleReadsEveryValueItTyped`.
- `server.TestArcFR2AnUnopenableReaderInputIsRefusedOnEveryDoor` and
  `coordinator.TestArcFR2AnUnopenableReaderInputIsRefusedOnTheCoordinator`.
