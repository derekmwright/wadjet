# ADR-0012: PostgreSQL is the SQL semantics authority; DuckDB is the performance goal and an oracle

Status: Accepted (2026-08-19)

## Context

Wadjet ships the PostgreSQL wire protocol, and DuckDB serves as both the
performance bar and the differential correctness oracle. Those two facts
pull in different directions the moment the engines disagree about what a
query *means* rather than how fast it runs.

SQL leaves a surprising amount implementation-defined, and the two engines
resolve it differently. Default NULL placement is the worked example:
PostgreSQL sorts NULLS LAST for ASC and NULLS FIRST for DESC; DuckDB's
`default_null_order` is NULLS LAST in both directions. Neither is wrong.

The question surfaced during the 2026-08-18/19 correctness work and was
escalated for a decision, which exposed the real problem: **the answer was
already implied by a commitment nobody had written down.** CLAUDE.md states
pgwire compatibility is non-negotiable because Superset, psql and JDBC depend
on it; once that holds, semantics follow. ADR-0001 exists because settled
questions were getting re-asked, and this was one.

Escalation is not a neutral cost here. A semantics question presented as an
open preference arrives stripped of the commitment that settles it, so it can
be answered wrongly by someone without that context — and the gates then
*encode* the wrong answer. The stored DuckDB baseline is regenerated from
whatever the oracle says, so a wrong choice becomes self-verifying within
minutes, and the eventual correction shows up red. That is the poisoned-
baseline failure mode (`baseline-local-small.json` held a signature captured
from a broken engine, so a *correct* engine failed our own gate) one level up.

## Decision

1. **PostgreSQL decides semantics.** Where PostgreSQL and DuckDB disagree
   about the meaning of a query — result values, NULL handling, ordering
   placement, type coercion, error-vs-not, catalog introspection — wadjet
   follows PostgreSQL. The wire protocol is a behavioral contract, not just a
   byte format: clients and the tools above them encode PostgreSQL's behavior
   in their query generation.

2. **DuckDB remains the performance goal and a correctness oracle.** It finds
   real bugs (dozens during the 2026-08 correctness arc) and sets the speed
   bar. Neither role makes it the semantic authority.

3. **On a semantic divergence, configure the oracle — do not exempt the
   entry.** An exemption blinds the gate permanently to real bugs in exactly
   the queries most likely to have them; a configured oracle still compares
   every row. The differential gate runs DuckDB with
   `default_null_order='nulls_last_on_asc_first_on_desc'`.

4. **A decision a gate encodes must be asserted where regeneration cannot
   rewrite it.** `TestOracleIsConfiguredForPostgresSemantics` fails if either
   oracle invocation loses that setting, because otherwise deleting one line
   silently changes what "ground truth" means.

5. **Deliberate divergences from PostgreSQL.** (Amended 2026-08-23: collation
   was not the only one — the other three below were never PostgreSQL's call
   to begin with, and are recorded here so a future gate does not mistake
   them for undecided.)

   - **An unnamed derived column is referenced by the BLOCK's spelling, not
     by its published name.** (Added 2026-09-04, #732.) An output column with
     no alias is PUBLISHED under PostgreSQL's `FigureColname` — `?column?` for
     an operator expression, the function's name for a call, the ARGUMENT's
     name for a cast — on every door and both engines. That is the name a
     client reads out of RowDescription, and it agrees with PostgreSQL.

     What does not is the name an ENCLOSING query may use for such a column of
     a derived table. PostgreSQL takes `"?column?"`; wadjet takes the inner
     block's own spelling (`"g + 1"`) and refuses `"?column?"` with 42703,
     naming the column that does exist. The two names are deliberately not one
     string: inside the plan a name is a HANDLE that a sort key, a HAVING and
     an aggregate's `OutputCol` all resolve against, and two unnamed items in
     one block would then answer to one handle. PostgreSQL has that ambiguity
     too and REFUSES it (42702); every resolver here would silently take the
     first, which is the trade this whole territory exists to avoid.

     Both halves are name-only and both are LOUD: a reference wadjet cannot
     resolve is an error, never a different column. Gated at
     `coordinator.TestAnUnnamedDerivedColumnCannotBeReferencedByItsPublishedName`,
     with the block's own spelling beside it as the control.
   - **A call PostgreSQL rewrites keeps the name the query wrote, except for
     TRIM.** (Added 2026-09-04, #732.) PostgreSQL labels an unaliased call
     after the function it RESOLVED to: `trim(' a ')` is `btrim`, which wadjet
     matches, and `SUBSTRING(s FROM 2 FOR 3)` — which wadjet's grammar does not
     accept at all — would be `substring`. A second such rewrite belongs on
     this list only with the measurement beside it.
   - **A multi-statement simple-query string is not one transaction.**
     (Added 2026-09-03, #711.) PostgreSQL's simple query protocol wraps a
     string carrying several statements in an IMPLICIT TRANSACTION, so a
     failure in the third statement rolls the first two back — measured:
     `INSERT …; SELECT 1/0` leaves the table untouched. Wadjet runs the same
     string as a sequence with the same tags and the same stop-at-the-first-
     error rule, and the statements that already committed STAY.

     It is not a sequencing divergence and cannot be closed by the sequencing:
     wadjet has no transactions at all — `BEGIN` and `COMMIT` are accepted and
     ignored on every door — so there is nothing to roll back with. What
     changes if transactions are ever implemented is this entry, not the
     splitter. Recorded here so a future gate does not read it as undecided.
     The value half agrees: which statements run, in what order, with which
     command tags, and that a syntax error anywhere runs none of them, are
     all PostgreSQL's answers and are gated per door in the DML census and in
     `internal/server/pgwire/multi_statement_test.go`.
   - **Collation.** Wadjet compares and sorts strings with BINARY collation,
     not PostgreSQL's locale-dependent collation. Locale-sensitive comparison
     costs real work on every string compare and sort, surprises analysts
     more than it helps, and no BI client depends on it. Where the oracle
     needs to agree, use a `C`-collation database rather than exempting
     string ordering.
   - **MIN/MAX over BOOL, UUID, MACADDR, BYTEA and ROW.** (Widened
     2026-08-25, #569: BOOL was the only one recorded, and the rest are the
     same class.) PostgreSQL's `min`/`max` are defined over exactly 22 input
     types, enumerated live from `pg_proc` on postgres:17-alpine:
     `anyarray`, `anyenum`, `bigint`, `character`, `date`, `double
     precision`, `inet`, `integer`, `interval`, `money`, `numeric`, `oid`,
     `pg_lsn`, `real`, `smallint`, `text`, `tid`, `time`/`timetz`,
     `timestamp`/`timestamptz`, `xid8`. `boolean`, `uuid`, `macaddr` and
     `bytea` are NOT among them — each errors with "function min(...) does
     not exist" — and neither is `record`: `min(ROW(…))` errors the same way
     (verified live), so wadjet's MIN/MAX over its ROW type is in this set
     too. All five are EXTENSIONS, not divergences PostgreSQL took a position
     on. `bool_and`/`bool_or` remain available and are still the
     PostgreSQL-idiomatic spelling for the boolean question.

     The consequence for the gates is the part worth writing down: those
     five types cannot be gated against PostgreSQL at all, in any shape —
     grouped, windowed or otherwise. `internal/oracle/typematrix` is their
     differential coverage (wadjet against itself across the stage DAG, the
     kill switches and the pooled/poisoned batch arms), and the four
     PostgreSQL DOES have among wadjet's network types — CIDR, IPV6 and IPV4,
     all of which map onto `inet`, plus DECIMAL onto `numeric` — are gated
     live in `benchmarks/tpch`'s `net_probe`/`dec_probe` fixtures.
   - **MAP and VECTOR ordering.** Both are wadjet-only types — PostgreSQL has
     neither — so their total orders (`internal/engine/exec/kernel/
     container_sort.go`) are wadjet-defined, not a choice against a
     PostgreSQL answer. This bullet is ONLY MAP and VECTOR: ROW is `record`,
     which PostgreSQL HAS as a type but offers no `min`/`max` over (it is in
     the extension set above), and ARRAY maps to PostgreSQL's `anyarray`,
     which DOES have `min`/`max` — so ARRAY is the one container whose
     ordering is a choice measurable against a PostgreSQL answer, even though
     no fixture gates it there today.
   - **MIN/MAX over BYTES.** (Corrected 2026-08-25, #570. The original said
     this matched "PostgreSQL's own `min(bytea)`", which does not exist:
     verified live, `min(bytea)` raises "function min(bytea) does not
     exist", exactly as `min(boolean)` does.) So this is the same kind of
     deliberate extension the BOOL bullet above is, over a type whose order
     wadjet defines anyway — bytewise, which is what every bytea comparison
     uses and what PostgreSQL's own `bytea` operators use. The output type
     still follows the INPUT type and declares bytea (OID 17), which is what
     the extension has to do to be self-consistent; early declared-schema
     code guessed STRING for every MIN/MAX before the input-typed fix.
     `ByteaMinMax` in the wire arm's error list pins it, so the claim is
     checkable rather than remembered.

   - **A window's MIN/MAX declares its input's type too.** (Added
     2026-08-25, #569.) `MIN(c) OVER (…)` and `MIN(c) … GROUP BY g` are the
     same question asked twice and must answer under the same type, so
     `exec.WindowMinMaxType` names every type the engine has, exactly as
     `exec.minMaxOutputType` does for the grouped form. They differ on TWO
     types, INT32 and FLOAT32: the grouped aggregate widens INT32 to INT64
     and FLOAT32 to FLOAT64 because its accumulator is the wider type, while
     the window copies the value and keeps INT32 and FLOAT32 — which are
     PostgreSQL's own answers, `min(integer)` returning `integer` and
     `min(real)` returning `real`.
     Choosing the value in the DECLARED type's order is the other half of the
     same rule; a boxed comparator that reads a rendered address or a
     formatted decimal is not that order (`internal/engine/exec/
     compare_boxed.go`).
   - **A window SUM/AVG over an INTEGER column answers in float64 — wrong
     DIGITS, not only a wrong declaration.** (Added 2026-09-04, #813, arc F1;
     CORRECTED 2026-09-05 after the arc's round-1 review, which measured what
     the first version of this entry asserted without measuring.) PostgreSQL
     declares `sum(int4) over ()` bigint and `sum(int8) over ()` /
     `avg(int) over ()` numeric, and since #784 the GROUPED spelling of each
     answers exactly that — so wadjet's two spellings of one question
     disagree, which is the thing `windowSpecOutputType`'s own comment says
     must not happen.

     The disagreement is not confined to the type. `exec.windowAccOutputType`
     gives an integer input a FLOAT64 accumulator, so past 2^53 the window
     spelling loses digits the grouped spelling keeps. Measured over the
     `numwidth` fixture, whose `w_i64` deliberately carries values past that
     range, against live postgres:17-alpine:

     | query | wadjet, all arms | PostgreSQL 17 |
     |---|---|---|
     | `AVG(w_i64) OVER ()` | `1000800157666874.2` float8 | `1000800157666874.2222` numeric |
     | `AVG(w_i64)` (grouped control) | `1000800157666874.2222` DECIMAL(38,4) | the same |
     | `SUM(w_i64) OVER (ORDER BY … ROWS …)`, row 4 | `9007199271518226` | `9007199271518227` |
     | `SUM(w_i64)` (grouped control) | `9007201419001868` DECIMAL(38,0) | the same |

     A VALUE divergence is never allowed by this ADR, and this one is not
     being allowed — it is RECORDED, pinned fail-on-agree, and deferred with
     its mechanism, because the repair is an exact integer accumulator in the
     window operator (`decimalFrameAcc` reads `DecimalData` directly and needs
     a per-type cell reader; `windowAccOutputType` and `windowOutputColumn`
     need the integer rules; `SUM(int4) → bigint` needs an INT64 output path
     neither frame has) and not a declaration. Declaring the exact type over
     today's carrier would be the #361 silent-write class on top of it.

     Pinned on the VALUE, not on the declaration, in
     `coordinator.TestF1AWindowDeclaresTheSameTypeThroughADerivedTable`: the
     cell asserts the float64 digits wadjet answers and names PostgreSQL's, so
     it fails the day the accumulator becomes exact. The CAST spelling
     (`CAST(SUM(x) OVER () AS BIGINT)`) already declares its target on every
     path and is a cell there.
   - **A column-alias list over a `SELECT *` is not applied.** (Added
     2026-09-04, #613.) `(…) AS b(kk, nn)` renames a derived table's columns
     positionally, and PostgreSQL applies it whatever the subquery's SELECT
     list looks like: `SELECT * FROM (SELECT * FROM t) AS b(kk, nn)` publishes
     `kk | nn | …` there. Wadjet leaves the list alone when the subquery's
     SELECT list carries a `*`, so it publishes the inner names, because the
     star's width is a catalog question `logical.applyColumnAliases` cannot
     ask — `ExpandStarProjections` answers it later, and renaming the wrong
     columns would be a wrong ANSWER rather than a missing one. The VALUES and
     the arity refusal are unaffected; only the star spelling diverges, and
     only in the published NAMES.

   - **`ORDER BY <name>` over two output columns of that name is answered,
     not refused.** (Added 2026-09-03, #557.) An output slot's identity is its
     POSITION: two output columns may share a NAME — PostgreSQL answers
     `SELECT abs(a), abs(b)` with two columns called `abs`, and `SELECT n_name
     AS u, n_comment AS u` with two called `u` — and neither the values nor a
     positional `ORDER BY N` may collapse them.

     PostgreSQL goes one step further and refuses `ORDER BY u` when `u` names
     two of them: 42702 `ORDER BY "u" is ambiguous`, verified live. Wadjet
     resolves it to the first, which is a superset — it answers a statement
     PostgreSQL declines rather than answering it differently — and is the
     same shape as the HAVING-sees-output-aliases entry above: this binder
     puts output names in one scope and a bare name resolves to the first
     match there. The POSITIONAL spelling, which is the one that was WRONG
     (`ORDER BY 2` sorted by column 1 on every arm), is PostgreSQL's answer
     now.

   - **A FOLDED identifier resolves case-insensitively when exactly one
     column matches.** (Added 2026-09-03, #731.) An UNQUOTED identifier folds
     to lower case at the lexer and a DELIMITED one keeps its bytes, which is
     PostgreSQL's rule exactly (`plansql.FoldIdent`, ASCII `A-Z` only —
     verified live on a UTF8 postgres:17-alpine, where `CREATE TABLE t (Ä int)`
     stores `Ä` and `SELECT 1 AS Ä` publishes `Ä`). PostgreSQL then matches the
     folded name EXACTLY against the catalog, so a column stored as `"WatchID"`
     is unreachable as `watchid` — `SELECT WatchID FROM hits` there is 42703
     `column "watchid" does not exist`.

     Wadjet resolves it. Its tables come from parquet and ingest, where
     CamelCase column names are ordinary — ClickBench's `hits` has `WatchID`,
     `UserID`, `EventTime`, and all 43 of its queries spell them that way — so
     a folded reference that misses byte-exact resolves case-insensitively
     when EXACTLY ONE column of the input matches. Two matches resolve to
     nothing and the caller reports the miss; within one table that cannot
     happen, because `catalog.checkDistinctColumnNames` already refuses a
     schema whose columns collide under `parquet.FoldName`, and across
     relations the planner refuses first with 42702 `column reference "g" is
     ambiguous`.

     **That 42702 is PostgreSQL's answer only when the two columns are
     spelled the SAME.** Where they differ ONLY BY CASE, PostgreSQL has no
     ambiguity to report and answers, because it never folded them together:
     over `clt4("MixedCol")` and `clt5(mixedcol)`, `SELECT mixedcol FROM
     clt4, clt5 WHERE clt4.k = clt5.k` is 900, 901 on postgres:17 (and so is
     the delimited `SELECT "mixedcol"`), while `SELECT k`, where both
     relations really do spell one name, is 42702 there exactly as it is
     here — all four measured live. Wadjet refuses all three, because the
     fold that makes `MixedCol` reachable as `mixedcol` also makes the two
     columns one NAME, and one name across two relations is ambiguous.

     So this is a divergence in the REFUSING direction, not a superset: the
     bare reference is the one spelling the concession cannot serve, and the
     QUALIFIED spelling is the one that works — `clt4.MixedCol` and
     `clt5.mixedcol` each resolve to their own relation's column on every
     arm, which is the identity rule ADR-0026 states and what
     `internal/oracle/collide`'s `case_colliding_columns_*` entries assert
     against live PostgreSQL in both FROM orders. A user who hits the 42702
     qualifies the reference; PostgreSQL accepts that spelling too, so the
     qualified form is portable and the bare one is not.

     **A TABLE name takes the same concession**, and it has to: the fold this
     arc put in the lexer applies to a relation reference as much as to a
     column one, and wadjet's table names come from parquet and ingest where a
     mixed-case name is ordinary. Without it `FROM MyTab` is 42P01 against a
     table this engine itself created — PostgreSQL's rule, but a BREAKING
     change for every catalog written before the fold rather than a semantic
     improvement, and the catalogs are the user's data. So a relation
     reference resolves byte-exact first and then to the one registered table
     matching it case-insensitively (`catalog.ResolveTableName`), with the
     same three boundaries the column rule has:

     - a DELIMITED reference carrying an upper-case letter takes no
       concession, so `FROM "MYTAB"` is 42P01 here as in PostgreSQL;
     - a byte-exact match always wins, so `MyTab` beside `mytab` behaves
       exactly as PostgreSQL does (the folded reference reads `mytab`);
     - TWO tables differing only by case resolve to NOTHING, and the 42P01
       names both candidates. PostgreSQL has no ambiguity class for relations
       and cannot reach this state — it folds at the catalog — so inventing a
       SQLSTATE it never emits would be a second divergence; what is true is
       that no unique relation has that name, which is what 42P01 says.

     The concession is READ-ONLY. `CreateTable` and the DDL door key
     byte-exact, because minting a name is not referencing one and creating
     `mytab` when `MyTab` exists must make a second table rather than
     silently open the first. The doors that reference an EXISTING table —
     the planner's scan annotation, the column binder, and the INSERT /
     UPDATE / DELETE / MERGE / COPY paths — canonicalize the name ONCE to the
     catalog's spelling, so the write lands on the table the read resolved; a
     door that conceded on the lookup and then keyed the manifest byte-exact
     would write somewhere else in silence.

     The rule is one function, `batch.ResolveColumnIndex`
     (`internal/engine/batch/schema.go`), and the SCHEMA stays byte-exact:
     `batch.ColumnIndex` still compares `col.Name == name`, `SELECT *` still
     publishes the catalog's own spellings, and every producer writes the name
     it was given. What folds is the RESOLVER.

     **A DELIMITED reference CARRYING AN UPPER-CASE LETTER does NOT get the
     concession**, and that boundary is what keeps the divergence a superset
     rather than a different answer: such a reference can only have been
     written between double quotes, so it resolves byte-exact only, and
     `SELECT "G"` over a column `g` is **42703** here as it is in PostgreSQL.
     It used to answer a column of NULLs. The refusal fires only where the
     scope carries a BASE TABLE's own spelling
     (`physical.colScope.refuseDelimitedMiss`), because a planner pass may
     have lowercased an alias before registering it and refusing on a spelling
     the scope no longer has would break `SELECT id AS "Kk" … ORDER BY "Kk"`.

     An ALL-LOWER-CASE delimited reference DOES get the concession, and that
     is a second divergence rather than an oversight. Nothing below the parser
     carries a `Quoted` bit: the resolver infers delimitedness from the name
     itself — an upper-case letter can only have survived the fold by being
     quoted — and `"watchid"` is indistinguishable from `watchid` by that
     test. So `SELECT "watchid"` reads a column stored `WatchID` here where
     PostgreSQL is 42703. Same superset direction as the unquoted spelling,
     and gated as a control at `wadjet/identifier_case_test.go`. Closing it
     means threading a `Quoted` bit from the lexer to the batch resolver,
     which is a carrier change, not a rule change.

     **A QUALIFIER never gets the concession, in either case.** A qualifier is
     a RELATION's name or alias and PostgreSQL matches those byte-exactly
     against what the FROM clause declared: a delimited alias `"T"` is not
     reachable as `t`, and `SELECT "T".x FROM t` is 42P01. Folding the whole
     dotted name bound a reference to the WRONG RELATION — over `FROM rvc t,
     rvd2 "T"` the join emits `g` and `T.g`, and `t.g` fold-matched `T.g` and
     answered the other relation's row. `batch.foldedNameMatches` judges the
     two halves by the two rules, and `physical.colScope.exactQuals` refuses a
     delimited qualifier the FROM never declared.

     Everything else this arc moved is PostgreSQL's answer rather than a
     divergence: `SELECT G` publishes `g`, `SELECT g AS Foo` publishes `foo`,
     `SELECT 1 AS Desc` publishes `desc`, `AS "Foo"` publishes `Foo`, a table
     alias and a CTE name fold, and a DDL declaration now keeps a delimited
     column name's bytes (`parquet.DeclaredColumn` lowercased every
     declaration before, so the one spelling PostgreSQL guarantees was the one
     that could not be stored).

   - **An UNPARENTHESISED `container.field` is a ROW FIELD PATH, where
     PostgreSQL requires `(container).field`.** (Added 2026-09-04, #769;
     ADR-0022.) PostgreSQL reads a bare `c_row.b` as *table* `c_row`, *column*
     `b`, and raises 42P01 `missing FROM-clause entry for table "c_row"` — the
     parenthesised `(n.c_row).b` is the only spelling it accepts. Wadjet
     ANSWERS the bare one, resolving the field out of its container, and that
     is a superset in the accepting direction: every query PostgreSQL answers
     answers the same here, and the spelling PostgreSQL refuses is one it
     cannot mean anything else by.

     **PostgreSQL's own spelling parses too** (2026-09-04 round 3):
     `(c_row).b` becomes the SAME reference the bare form does, so the
     superset is two spellings of one meaning rather than two meanings. The
     one PostgreSQL spelling wadjet does NOT accept is a container the
     reference QUALIFIES — `(x.c_row).b` — which needs a three-part identity
     the engine does not carry and is REFUSED with `0A000` naming the
     derived-table workaround. That is a divergence in the REFUSING direction
     and it is listed here for that reason; ADR-0022 carries the mechanism.

     The boundary is what keeps it a superset rather than a second answer. A
     dotted reference is a field path ONLY where its qualifier names a ROW
     column of the stream **that declares the field** (`batch.RowFieldPath`,
     the one place the engine asks); an ordinary qualified reference to a
     relation is untouched, and a qualifier naming a container that does not
     declare the field is refused with PostgreSQL's own wording (`could not
     identify column "nosuch" in record data type`).

     **Two arms spelling the container alike are REFUSED, and there the
     PARENTHESISED spelling is the anchor.** `SELECT x.id, (c_row).b FROM n x
     JOIN n y ON x.id = y.id + 1` is `column reference "c_row" is ambiguous`,
     42702, on PostgreSQL 17 — measured — so the ambiguity has an answer to
     follow even though the unparenthesised spelling does not. Wadjet raises
     the same class with the same wording, at plan time, and the message names
     the QUALIFIER because a container is a column and the ambiguity is the
     container's (`physical.colScope.check`). It answered ONE of the two arms
     until 2026-09-04, and which one depended on the plan shape.

     Asking that question BEFORE stripping the qualifier is the whole of it,
     and asking it after was a wrong VALUE rather than a divergence: beside a
     join arm publishing a column of the FIELD's name, `c_row.b` answered
     THAT arm's column on every arm and declared its type on the wire.
     Gated at `internal/coordinator/derived_arm_join_chain_two_path_test.go`
     (`join-arm-publishes-the-field-name*`) and
     `internal/engine/batch/row_field_path_test.go`.

   - **TIMESTAMP is `timestamp without time zone`, and a literal's offset is
     DISCARDED.** (Added 2026-09-03, #692; this entry records a divergence
     CLOSED, not one kept.) Wadjet declares TIMESTAMP as PostgreSQL's
     `timestamp without time zone` on the wire and therefore has to mean what
     that type means: `'2020-01-01T05:30:00+05:30'` is `2020-01-01 05:30:00`.
     It used to be read as the INSTANT the offset names and normalized to UTC
     — `time.Parse` yields a fixed +05:30 zone and `UnixMilli` converts it —
     so wadjet stored `2020-01-01 00:00:00` and answered a different question
     than the literal asked. Verified live: `'…+05:30'::timestamp` is
     `2020-01-01 05:30:00` and `'…+05:30'::timestamptz AT TIME ZONE 'UTC'` is
     `2020-01-01 00:00:00`, which is what wadjet was storing.

     The rule is one function, `parquet.ParseTimestampMillis`, and the two
     comparison kernels reach it through
     `parquet.ParseTimestampMillisOrZero` instead of keeping copies of the
     layout list. They had two copies and both had DRIFTED from the writer's,
     so the space-separated millisecond form stored fine and no predicate
     could read it back — a literal that STORES has to be a literal a
     predicate over the same column reads the same way, and one function is
     the only thing that keeps that true. The classification goes with it:
     a timestamp whose FIELDS name no instant (2020-02-30, month 13, hour 25)
     is **22008**, text that is not a timestamp at all is **22007**, exactly
     as DATE has classified since #560. Hour 24 and second 60 are NOT
     field-range failures: PostgreSQL reads them as the next day and the next
     minute, and so does this — refusing input PostgreSQL accepts is what item
     1 forbids, and the first pass refused both.

     `CAST(<bad literal> AS TIMESTAMP)` **raises** the same pair of codes since
     #836/#840. The residual this paragraph used to record — "the CAST path has
     no per-row error channel for a temporal conversion, so it produces a value
     or nothing, and nothing is NULL" — was FALSE when it was written: the
     channel is `expr.fatalEval`, the numeric casts had used it since #367, and
     #836 is the issue that read the tree instead of the record. Method 9 in the
     correctness-fix protocol is about exactly this direction of error. The
     census pin is deleted; both engines answer 22008 for an impossible day and
     22007 for text that is not a timestamp.

     **Residual, kept.** Sub-millisecond precision is TRUNCATED to the
     millisecond the column stores — `.123456` reads back `.123` — which is a
     declared-type property of TIMESTAMP here and a stored-value divergence
     from PostgreSQL's microseconds.
   - **PORT and PROTOCOL are `int4` on the wire; DURATION is `int8`
     NANOSECONDS.** (Decided 2026-09-03, #834.) All three declared OID 25
     (`text`) while the engine compared them NUMERICALLY, which is item 2's
     exact shape: one type declared, another behaved as. Under `text`,
     `port > 5` is the operand pair #721 refuses and PostgreSQL rejects with
     42883 — yet wadjet answered it, and refusing would have broken a
     legitimate wadjet-native comparison. Declaring the numeric OID dissolves
     the asymmetry instead of choosing a side of it.

     The TEXT on the wire does not change: a PORT is a uint16 and a PROTOCOL a
     uint8, both stored in `Int32Data` and boxed as an int32, and a DURATION is
     an int64 — all three already rendered as plain integers (`443`, `6`,
     `1500000000`), which is exactly int4's and int8's text form.
     `appendBinaryValue`'s own int32/int64 arms already write the 4 and 8 bytes
     the OIDs promise, so no encoder arm was needed the way `date`, `numeric`
     and `uuid` needed one — those box as TEXT and these box as the number.
     `numericOID` in bindparams already covers int4 and int8, so an inbound
     `WHERE port_col = $1` binds as a bare SQL number rather than a quoted
     string.

     **DURATION is int8 and not `interval`, and that is the open alternative.**
     PostgreSQL's `interval` (OID 1186) is MICROSECOND precision with its own
     text and binary forms, so declaring it would change the RENDERING as well
     as the type — a wadjet DURATION counts nanoseconds, which is the unit
     `schema.go` defines and `Vector.GetValue` reads back. Moving to `interval`
     would need a unit decision first and is recorded here as the way out, not
     taken.

     The SQLSTATE messages moved with the declaration:
     `kernel.NumericTypeName` renders `integer` and `bigint` for these types,
     so `port_col = 'abc'` is `invalid input syntax for type integer: "abc"` —
     the name a client can look up in `pg_type`, where it used to say `port`,
     a type name nothing resolves. `pgFormatType` follows for the synthetic
     `pg_attribute` rows, because an introspecting client reads BOTH and a
     catalog saying `text` beside a RowDescription saying int4 is the same
     contradiction one layer down.

     Gated by the wire arm over `net_probe`, whose PostgreSQL twin declares
     `integer` / `integer` / `bigint`: the OIDs, the sizes, the text, the
     binary bytes, and an integer bound parameter against a PORT column in
     both the declared and the inferred spellings.
   - **A temporal CAST over a box with no temporal reading keeps NULL.**
     (Added 2026-09-03, #836/#840.) `CAST(<text> AS DATE|TIMESTAMP)` raises
     22007/22008 for text naming no instant. Every OTHER box that fails to
     parse — a boolean, a container — keeps the NULL it had, because
     PostgreSQL answers those at PARSE time with **42846**
     (`cannot cast type boolean to date`): it is a TYPE-PAIR refusal, not a
     data exception, and minting 22007 for it would put a data-exception code
     on a type error. The boundary is attempted from the outside by
     `expr.TestCastTemporalRefusalStopsAtText`.
   - **A CAST to a NETWORK type does not read its text.** (Added 2026-09-03,
     #839's census.) `CAST('abc' AS IPV4|IPV6|CIDR|MACADDR)` returns the text
     under a STRING declaration; PostgreSQL raises 22P02 for its
     inet/cidr/macaddr equivalents. The reason it is recorded rather than fixed
     is a rule this ADR already enforces elsewhere: the engine has ONE text
     accept-set per type and it lives in one function (`parquet.ParseDateDays`
     for dates). The network types have none — the ingest boundary type-checks
     the Go box and not the text, and the abbreviated-literal divergence above
     shows the literal accept-set already differs from PostgreSQL's — so a
     validator written inside `Cast.Eval` would give one engine two answers to
     "is this an address". Pinned by
     `expr.TestCastToANetworkTypeStillPassesThrough`, which fails the day the
     cast starts refusing. UUID and the float family were the same shape and
     ARE fixed: both had a single unambiguous accept-set to read.
   - **The CAST door reads the engine's ONE temporal accept-set, and inherits
     its refusals.** (Added 2026-09-03, #840; the accept-set is #639's and
     #641's. Amended 2026-09-04: #641 landed and the last residual here closed
     itself, which is what a shared accept-set is FOR.) `CAST(<text> AS DATE|TIMESTAMP)` takes both its VALUE and its
     refusal from `parquet.ParseDateDays` / `ParseTimestampMillis` — the same
     function the ingest boundary, the parquet writers, the row→batch builder
     and the filter kernel read — so a literal that STORES is a literal a
     predicate reads the same way and a CAST reads the same way. Three
     consequences follow, and all three are the accept-set's rather than the
     cast's:

     - A DMY spelling is **22007 here and 22008 in PostgreSQL**. Both engines
       REFUSE `'31/12/1996'`; PostgreSQL's DateStyle ISO, MDY reads the leading
       field as a month and calls month 31 a field-range failure, while wadjet
       refuses every spelling whose field ORDER DateStyle would decide, so it
       is "not a date" and the class differs.
     - `'0000-01-01'` and `'2024-001-01'` are **REFUSED, with PostgreSQL's own
       codes and messages** — 22008 for a year the calendar does not have
       (1 BC sits immediately before 1 AD there) and 22007 for a three-digit
       month, which PostgreSQL reads as a day-of-year and then rejects. This
       started as the entry's one open residual, and the way it closed is the
       point of the entry: the refusal was added ONCE, in
       `parquet.ParseDateDays` (**#641**, the storage arc), and the CAST door
       inherited it with no change in `expr` at all. **The two doors have
       converged**: the ingest boundary, `INSERT … VALUES`, a predicate and a
       CAST now give the same answer to "is this a date", including these two
       spellings. The pin that recorded the residual fired the day #641 landed
       and was deleted — that firing is the fix's proof. The cells live in
       `expr.TestCastToDateRaisesForTextThatNamesNoDay` now, with
       `expr.TestCastToDateStillReadsTheYearsAroundTheRefusal` asserting the
       boundary the new refusal must not cross: `0001-01-01`, `10000-01-01`
       and the compact `19960110` all still answer, as they do on 17.11.
     - The compact 8-digit form `'20240101'` is ACCEPTED, as PostgreSQL accepts
       it. It briefly RAISED — #836's first pass took only the error CODE from
       the shared parser and left the value to a narrower reading — which is
       the same two-answers-for-one-question defect pointing the other way: a
       cast refusing what the ingest boundary stores.
   - **A TEXT value compared against a NUMBER.** (Added 2026-08-24, #504.)
     PostgreSQL refuses the pair outright — verified live, `WHERE s = 1.5`
     over a `text` column is 42883 "operator does not exist: text = numeric",
     and so are `>`, `text = bigint`, `CASE s WHEN 1.5`, `s IS DISTINCT FROM
     1.5` and `GREATEST(s, 1.5)`. That is an OVERLOAD RESOLUTION failure, the
     same class as the unary-minus bullet below: wadjet has ONE generic
     comparison operator and no overload set to fail resolution against, so
     reproducing 42883 would mean building the overload machinery first.

     Wadjet instead gives the pair the column's own rule: a STRING column
     compares its BYTES against the literal's SOURCE TEXT, on the vectorized
     path and the row-at-a-time path alike. The literal's text is the carrier
     for the same reason item 6 makes it one — `s = 1.50` and `s = 1.5` are
     different predicates, exactly as `s = '1.50'` and `s = '1.5'` already
     were.

     This is a narrower divergence than it looks. Every answer it produces is
     PostgreSQL's answer to the QUOTED spelling of the same predicate, checked
     entry by entry against live postgres:17-alpine over a `text COLLATE "C"`
     column: `s = '1.5'` 1, `s > '1.5'` 4, `s > '10'` 2, `s < '10'` 2, and the
     three boxed sites likewise. The only thing wadjet does that PostgreSQL
     does not is RESOLVE the unquoted spelling at all.

     What was there before was neither rule. `compare()` read any string
     operand that PARSED as a number numerically — a guess about where the box
     came from, since a DECIMAL column and a STRING column both box as Go
     strings — while the vectorized kernel rendered the numeric constant as
     the EMPTY STRING and compared against that. So `WHERE s = 1.5` found the
     row holding "1.50" through a projected CASE and no rows at all through a
     scan-pushed filter, and `WHERE s > 1.5` admitted every row including
     "1.5" itself. One predicate, two answers, decided by which lowering the
     query happened to take. The guess is gone: `expr.boxedPair` selects the
     rule from the operands' DECLARED types (item 8), and
     `kernel.toString`/`exec.decimalLitValue` give the kernel the same
     literal text the row path uses.

     **This rule is about a NUMERIC literal meeting a TEXT column, and it does
     not run backwards.** (Added 2026-08-25, from the #504 review; the opposite
     pair is item 13's whole subject as of #646.) A QUOTED
     literal meeting a NUMBER column is the opposite pair and takes the
     opposite rule: PostgreSQL types an unknown-typed literal FROM the operand
     it meets, so `WHERE k > '2'` over a BIGINT column is the integer
     comparison `k > 2` — not a text comparison, and not the comparison
     against ZERO the constant used to become. Deleting the box-sniffing
     branch removed all three of the readings it was doing at once, and only
     the DECIMAL one was re-stated; `boxedPair` carries a distinct `boxQuoted`
     kind for exactly this reason, so the two directions cannot be collapsed
     again. Verified live on `k BIGINT` 0..11: `k > '2'` 9, `k >= '2'` 10,
     `k < '2'` 2, `k = '2'` 1, and the same for a FLOAT column under the
     float rule.

     Two more of the box-sniff's jobs came back with it. `compare()`'s
     temporal branch guarded itself with "the string parsed OR the number is
     zero", which is true of ANY unparseable string against a zero — so
     `0 = '0.0001'` was TRUE, and once the sniff above it was gone every
     `int_col = 'anything'` matched the row holding zero. It asks the parser
     whether it parsed now (`parseTemporalInt64OK`). And `IN`/`BETWEEN` are
     `=` and `>=`/`<=` chained, so they take the same binding: `s = 2.00` and
     `s IN (2.00)` disagreed until they did.

     `kernel.toString`'s empty string was not only for numbers. A BOOL
     constant reached it too, so `WHERE s = TRUE` compared every row against
     `""` on the scan path and matched the row spelled `""` rather than the
     one spelled `"true"` — a wrong ROW, not a wrong count. It renders
     `true`/`false`, which is PostgreSQL's own `boolean::text` (the
     single-letter `t` is psql's display) and what the row path's `fmt.Sprint`
     already produced.

     **The divergence is a READ-side concession, and a DML statement's
     qualifying predicate does not get it.** (Added 2026-09-03, #721.) The
     entry above was reasoned entirely about the query path, where its
     consequence is a wrong COUNT for a spelling PostgreSQL refuses to
     resolve. On a DELETE the same rule DESTROYS ROWS:

         DELETE FROM pr WHERE name > 5     PostgreSQL 42883
                                           wadjet DELETE 3, table EMPTIED

     `"a" > "5"` is true for every row (0x61 > 0x35), so wadjet answered
     PostgreSQL's answer to a DIFFERENT predicate and emptied a three-row
     table. Nobody wrote that consequence down because no fixture attempted
     it — the issue that reported the class even claimed it was "the SAFE
     direction … no row is destroyed", which the measurement refutes.

     So the rule now splits by what the predicate DECIDES. A SELECT keeps the
     byte rule and its reasoning: the overload machinery this entry says would
     be needed is still not built, and every answer the rule gives is
     PostgreSQL's answer to the quoted spelling. A DML statement's WHERE
     refuses the pair with **42883**, before any row is read, at
     `wadjet.refuseDMLLiteralPairs`. Two more pairs go with it, for the same
     reason: any non-BOOL column against a BOOLEAN literal (`id = true`,
     PostgreSQL's `bigint = boolean`, which the DML door answered `DELETE 0`
     and the SELECT door 22P02 — two doors disagreeing about one predicate),
     and a numeric column against a quoted literal naming no value of it,
     which the runtime already refuses but only once a ROW reaches it, so
     `DELETE FROM empty WHERE id = 'abc'` answered `DELETE 0`.

     The asymmetry is the point, not an oversight: a concession whose cost is
     an answer is not the same decision as a concession whose cost is the
     table. Temporal and network columns against a number are deliberately
     NOT refused — those parsers are stricter than PostgreSQL's input grammar
     and refusing on them would reject input PostgreSQL accepts, which item 1
     forbids.

     The refused pairs are: an unquoted NUMBER against any column type that is
     not a numeric family — STRING, BYTES, BOOL, TIMESTAMP, DATE and the
     network types, since PostgreSQL refuses the OPERATOR for all of them —
     a BOOLEAN literal against a non-BOOL column, and a quoted literal naming
     no value of a numeric column. PORT, PROTOCOL and DURATION keep answering:
     they are wadjet-native, PostgreSQL has no such type and therefore no
     opinion, and the superset rule applies. The first pass listed only STRING
     and BYTES and justified the rest with an argument about quoted input
     reaching a type parser; that argument cannot apply to an unquoted number,
     and the measurement refuted it — `DELETE … WHERE ts > 5` EMPTIED a
     TIMESTAMP table, as did the BOOL and IPv4 spellings.

     Every half is pinned in the DML census
     (`internal/server/pgwire/dml_census_test.go`): the DML entries assert the
     class PostgreSQL gives — 42883 for a refused OPERATOR, 22P02 for a quoted
     literal naming no value, which are two different conditions and word
     themselves differently — the two SELECT entries stay pinned as this
     divergence, and the boundary entries assert both the pairs that must keep
     working and the pairs that must keep refusing. All four DML verbs are
     covered: MERGE's `WHEN … AND` condition runs the same check as a DELETE's
     and an UPDATE's WHERE.

     **The equivalent question for CIDR is open, and must not be answered one
     site at a time.** (Added 2026-08-25, #546.) A CIDR value is stored as
     TEXT, and every KEY and every column-to-column comparison uses that text
     while the column-to-LITERAL comparison re-keys through
     `kernel.CidrSortKey` (#492) — so `WHERE c = '10.0.0.1'` finds both
     spellings of one address and `WHERE c = d`, `GROUP BY c`, `DISTINCT` and
     every set operation find neither. PostgreSQL says they are one value
     (`inet '10.0.0.1' = inet '10.0.0.1/32'` is TRUE). Both wadjet paths agree
     with each other today, so keying the local set operation by inet ALONE
     would create the divergence it looks like it closes; the fix moves the
     whole key layer and the shuffle router together, the way #459 did for
     floats — predicate kernels, hash keys, and the set-operation key
     (`physical.keyValueText`, `internal/planner/physical/set_op_key.go`) all
     at once, not one at a time. (Amended 2026-08-25. Closed 2026-08-25 — see
     item 10's #546 and #565 residuals for the landed fix and the
     column-to-column comparison it turned out to share a cause with.)

   - **Unary minus over a QUOTED string literal.** (Added 2026-08-24, #505.)
     PostgreSQL refuses EVERY `-'…'` form, numeric-looking or not, with
     42725 "operator is not unique: - unknown" — verified live: both
     `SELECT -'5'` and `SELECT -'abc'` error there, because unary minus
     resolves before the comparison can type the literal from context and
     several overloads match an unknown-typed operand equally well. Wadjet
     has ONE generic unary-minus operator and so has no overload ambiguity
     to report. It folds the literal instead: a numeric-looking string
     becomes a negated `Lit` carrying its exact text (so `d = -'5.00'`
     enters the same exact-DECIMAL path `d = -5.00` does, per item 6), and a
     non-numeric one is refused with 22P02. The visible consequence is that
     `SELECT -'5'` SUCCEEDS here and answers `-5` typed **varchar** — the
     literal keeps its own type, since nothing in that statement asks for a
     number — where PostgreSQL errors and, in the shapes where it does
     resolve (`-'5'::numeric`), answers a numeric. This is a deliberate
     extension of item 6's convention for the column type it exists for, not
     a position against PostgreSQL's overload resolution; reproducing 42725
     would mean building the overload ambiguity first.

   - **The grouping rule has no functional-dependency escape here.** (Added
     2026-08-25, #590.) PostgreSQL refuses a non-aggregated SELECT / HAVING /
     ORDER BY expression that is not one of the grouped expressions — 42803 —
     with one relaxation: a column FUNCTIONALLY DEPENDENT on a grouped
     PRIMARY KEY is allowed, so `SELECT id, name FROM t GROUP BY id` works
     when `id` is the primary key and fails when it is not. Wadjet has no
     primary keys and no unique constraints, so there is nothing for the
     relaxation to apply to and every such reference is refused. This is the
     STRICTER end of PostgreSQL's own rule, not a divergence from it: every
     query wadjet refuses here, a PostgreSQL table without the matching
     primary key refuses too. Should key constraints ever arrive, the
     relaxation arrives with them rather than being invented separately.

   - **HAVING sees the SELECT list's output aliases; PostgreSQL's does not.**
     (Added 2026-08-25, from the #591 corpus work.) PostgreSQL makes an
     output alias visible to GROUP BY and ORDER BY but NOT to HAVING or WHERE
     — verified live, `SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING c > 1`
     is 42703 "column \"c\" does not exist" there. Wadjet resolves it, because
     its binder puts output names in one scope shared by GROUP BY, HAVING,
     QUALIFY and ORDER BY, and because the spelling is what a user writing
     the query expects to work.

     This one cannot be recorded the way the others are. The PostgreSQL
     oracle's semantics arm FAILS when the oracle refuses a query — an entry
     PostgreSQL cannot answer is not ground truth for anything — so a
     corpus entry is not available to carry it, and the wire arm's error
     list is for statements BOTH engines should refuse. It is gated in
     `internal/planner/physical/validate_grouping_test.go` instead, where the
     question is about wadjet's own rule. Recorded here so a later reading of
     the oracle's silence does not mistake the extension for an oversight.
   - **CAST(<integer> AS BOOLEAN) over the WHOLE integer family.** (Added
     2026-08-25, #592.) PostgreSQL HAS this cast, for exactly one width:
     `1::int4::boolean` is `t`, while `int8`, `int2`, `float8` and `numeric`
     to boolean are all 42846 "cannot cast type ... to boolean" (verified
     live on postgres:17-alpine). Wadjet applies the int4 rule — 0 is FALSE,
     every other value TRUE, NULL is NULL — to INT32 and INT64 alike, and
     refuses everything else with the same 42846 PostgreSQL raises.

     The int8 half is the divergence, and it is a divergence from an
     OMISSION rather than from a position: PostgreSQL's own `1::bigint::int::bool`
     answers, so nothing about the meaning of the cast changes with the
     width — only whether a pg_cast row exists. Refusing it would mean
     `CAST(c AS BOOLEAN)` erroring on a BIGINT column and answering on an
     INTEGER one holding the identical values, and BIGINT is what wadjet's
     `BIGINT` declaration and its integer literals produce, so the cast
     would be unreachable for most columns while its twin worked.

     **FLOAT and DECIMAL are refused, not extended.** That asymmetry is the
     point: PostgreSQL declines float truthiness deliberately (there is no
     cast to omit — a float has no int4 twin whose rule this would be
     borrowing), and the value wadjet used to answer there was one nothing
     agreed on. `SELECT (f)::BOOLEAN` came back TRUE/FALSE through
     `Vector.SetValue`'s coercion while `WHERE (f)::BOOLEAN` excluded every
     row, which is the same two-path failure this item closes.

     **STRING is PostgreSQL's boolean input function exactly**
     (`parse_bool_with_len`, `expr.parseBoolText`): case-insensitive, C
     whitespace trimmed, any non-empty PREFIX of "true"/"false"/"yes"/"no",
     plus "on"/"off" and the single characters "1" and "0". `'tr'` and
     `'fals'` ARE values there; `'o'` alone is not, because it cannot choose
     between "on" and "off". A string that names no boolean is SQLSTATE
     22P02, never a value and never a match-nothing predicate — #463's rule
     for DECIMAL, one type family over.

     **The rule is selected from the operand's DECLARATION, never from its
     Go box** — item 8's boxed-value rule. A DECIMAL column and a STRING
     column both box as a Go string, so a box-driven cast would give
     `DECIMAL(9,0)` holding 1 the answer TRUE where PostgreSQL refuses
     outright, and DATE/IPv4/MAC (boxed as their raw integer encodings)
     would take the integer arm. `expr.castBoolDeclared` resolves the
     declaration from the batch and caches it, the way `boxedPair` does.

   - **NaN and ±Infinity are DECIMAL comparison literals only, never stored
     values.** (Added 2026-08-29, #534; the rule is ADR-0024 item 6 and this
     is the divergence it produces.) PostgreSQL's `numeric` HAS all three —
     NaN above every non-NaN and equal only to itself, ±Infinity since
     PostgreSQL 14 — and wadjet's finite Int128-at-a-fixed-scale carrier has
     no bit pattern for any of them. So `d = 'NaN'`, `d < 'NaN'`,
     `d <= 'Infinity'` and `d > '-Infinity'` are ACCEPTED and answer by
     PostgreSQL's order over a column that holds none of them (0 rows, every
     non-NULL row, every non-NULL row, every non-NULL row), resolved through
     the same `ScaledDecimal.Sat` bound item 6 gives a finite literal wider
     than the carrier.

     As a VALUE the three are refused with 22003 and a message naming
     ADR-0024, at the sites that produce one through the CHECKED reader:
     `batch.ParseDecimalStringChecked` and everything above it —
     `Vector.SetValueChecked`, `FromRowsChecked`, and through them the
     single-process set-operation adapter. PostgreSQL raises that same 22003
     for the infinities against a constrained `numeric(p,s)` ("cannot hold an
     infinite value", verified live); NaN it stores, and refusing that is the
     divergence. Two sites do NOT reach that reader yet and are recorded
     rather than claimed: `CAST(x AS DECIMAL(p,s))` is still ADR-0024 item 1's
     declared-STRING no-op, so `CAST('NaN' AS DECIMAL(9,2))` yields the string
     "NaN" and `CAST('NaN' AS DECIMAL)` a float64 NaN until the CAST evaluator
     lands (#555); and the UNCHECKED WRITE PATHS store 0 for them exactly as
     they do for `'abc'` — ADR-0024 item 4's residual, which is a property of
     the whole type and not of these three. There are TWO of those paths and
     both are named because only one of them is on the line a user's INSERT
     actually takes: `batch.ParseDecimalString` via `Vector.SetValue` (the
     row-to-batch adapter), and `parquet.decimalUnscaledInt64` /
     `decimalFLBABytes` in the file writer, whose string arm routes every
     value through `strconv.ParseFloat` — 0 on error, 0 for a NaN or an
     infinity, 0 for `' 3.50 '` because ParseFloat refuses the surrounding
     space, and float64's ~16 significant digits for everything else. That
     second one is the ingest path a client reaches and it is tracked as
     #647. The accepted spellings are PostgreSQL's own input
     grammar and nothing wider — case-insensitive `nan` with NO sign,
     case-insensitive `infinity`/`inf` with an optional adjacent sign, C
     whitespace trimmed — so `'+NaN'`, `'Infin'` and `'abc'` all stay 22P02.
     `ORDER BY` and `GROUP BY` need nothing: no such value can be stored, so
     no comparator or key ever meets one.
   - **A JOIN's ON condition can reference comma-join siblings; PostgreSQL rejects this.** (Closed #617.) A join predicate like `SELECT ... FROM a, b JOIN c ON a.k = c.k WHERE ...` references a sibling of the comma join in its ON clause. PostgreSQL 17 rejects this with "invalid reference to FROM-clause entry"; wadjet answers it, matching DuckDB. This is a strict SUPERSET: errors on PostgreSQL, runs on wadjet; not a value divergence and not a wire-protocol violation. Gated against DuckDB and the two-path oracle (PostgreSQL offers no value to assert). #593 fixed the prior silent-zero wrong answer in this shape. The reject-like-PostgreSQL alternative was considered and declined because no client should rely on the error and the planner lacks the ON-scope validation it would require.

   - **An aggregate of the OUTER level inside a subquery's WHERE is refused;
     PostgreSQL accepts it.** (Added 2026-09-03, #809.) `HAVING (SELECT MAX(k)
     FROM dim d WHERE d.k = SUM(t.g)) > 0` is legal SQL — the aggregate
     belongs to the enclosing query's grouped level — and PostgreSQL 17
     answers it. This engine has never answered it on any path: the subquery
     is re-run standalone and its own level-local placement rule fires, since
     nothing at that site holds the outer scope. It is a LOWERING gap and it
     is LOUD (42803, PostgreSQL's own SQLSTATE and message), not a wrong
     value, and it is recorded here so the next reader does not mistake the
     refusal for a decision. The plan-time rule added by #809 is deliberately
     written so it does NOT reach this shape — it refuses only an aggregate
     that names no relation outside the subquery's own FROM — so nothing that
     could one day answer is refused earlier because of it. Pinned as
     `boundary_outer_level_aggregate_inside_a_subquery_is_refused` in the
     correlation census; the day it answers, the pin fails.

   - **An EXACT decimal operator raises where PostgreSQL answers, past the
     carrier.** (Added 2026-09-02, #749.) `+ - * %` keep PostgreSQL's scale
     and cap the precision at 38 (ADR-0024 §3's 2026-09-02 amendment), so a
     result needing more than 38 digits at that scale is a 22003 rather than a
     value rounded to fewer fraction digits. The reachable case is
     `DECIMAL(38,10) × DECIMAL(38,10)`, whose type is `(38,20)`: a product past
     10^18 raises where PostgreSQL's unbounded numeric answers. It is item 1's
     finite carrier, not a new position, and it is the trade #749 makes
     deliberately — a right value of the wrong type is worse than an error.
     Pinned in the PostgreSQL corpus as `WideDecimalSquaredRowCount` under the
     kind `pgDivergenceCarrier`, which ratchets.

   - **DISTINCT is accepted for aggregates PostgreSQL refuses it for.** (Added
     2026-09-02, #703.) Wadjet honours `AGG(DISTINCT x)` for every aggregate
     its parser accepts it on. Three of those PostgreSQL either refuses or does
     not have: `MEDIAN(DISTINCT a)`, `APPROX_DISTINCT(DISTINCT a)` and
     `MIN_BY(DISTINCT a, b)` — PostgreSQL spells the first with `WITHIN GROUP`
     and answers `cannot use DISTINCT with WITHIN GROUP`, and has neither of
     the others. A strict SUPERSET: an error there, an answer here, never a
     different value for a query both engines accept. The DISTINCT semantics
     for the aggregates PostgreSQL DOES accept it on are PostgreSQL's, values
     and ordering both — `STRING_AGG(DISTINCT s, ',')` sorts the distinct
     values, which is what PostgreSQL's dedup produces and what wadjet emits
     since #703's review round.

   - **An unaliased expression is named after its own TEXT, not `?column?`.**
     (Added 2026-09-03, #732 — the DECISION, taken and recorded rather than
     implemented.) PostgreSQL's naming rule for a SELECT item with no `AS` is
     five rules, measured live on 17:

     | shape | PostgreSQL | wadjet |
     |---|---|---|
     | a bare column, `SELECT c` | `c` | `c` |
     | a function call, `SELECT SUBSTR(c,1,2)` | `substr` | `substr` |
     | an aggregate, `SELECT COUNT(*)` | `count` | `count(*)` |
     | a CASE | `case` | its full text |
     | a CAST, `SELECT CAST(g AS BIGINT)` | `g` — its OPERAND's name, and only the TARGET TYPE when the operand has none | its full text |
     | anything else, `SELECT g + 1` | `?column?` | `g + 1` |

     Wadjet keeps the expression's own text. It is MORE informative than
     `?column?` for the last row — a client showing a result set gets `g + 1`
     instead of a placeholder — and adopting PostgreSQL's rule renames columns
     this repository's own corpora assert: `benchmarks/tpch/postgres_compare_test.go`
     records that the semantics corpus is deliberately name-blind BECAUSE of
     this. The rule is recorded here so a future gate does not read the
     divergence as undecided, and `wireCorpus`'s `field_names` pins carry it
     per entry.

     What is NOT deliberate, and was fixed: the two ENGINES naming one query
     differently. The DAG folded the case of that text and the single-process
     path did not, so `SUM(a) OVER () + 1` arrived as `sum(a) over (...) + 1`
     from one and `sum(a) OVER (...) + 1` from the other (#744). Whatever rule
     this item settles on, both paths send it —
     `coordinator.TestBothPathsNameAnUnaliasedExpressionTheSameWay`.

   - **A qualified GROUP BY term with an unqualified select item.** (Added
     2026-09-03, #738.) `SELECT g + 1 ... GROUP BY typemx.g + 1` is answered by
     PostgreSQL and refused here with 42803. The MIRROR — a qualified select
     item over a bare key — is answered, because a qualifier is spelling in a
     single-relation block; this direction is not, because the aggregate would
     have to evaluate `typemx.g + 1` over a batch whose column is `g`. It
     cannot, and making the identity match on both sides produced a NULL key
     for every group. A loud refusal for a shape the engine cannot compute
     beats a plausible NULL (correctness-protocol method 8). Gated by
     `coordinator.TestTheIdentityErasesAQualifierAndATypeSynonym`'s
     `boundary_qualified_key_bare_select_item`.

   - **`SELECT *` over a join or a USING join, in three places.** (Added
     2026-09-03, #810 / #655.) A star over a JOIN is left unexpanded —
     `logical.ExpandStarProjections` declines it because guessing a join's
     column set would silently change which columns a query returns — and
     three shapes are refused as a consequence, all loudly and all answered by
     PostgreSQL:
     `SELECT * FROM a JOIN b ORDER BY 1` (42P10), `SELECT * FROM a JOIN b USING (c)`
     (0A000, because USING merges the joined column into ONE output column),
     and a `USING` clause following another join on the same FROM item (0A000).
     Lifting them needs an ORDERED model of a join's emitted columns; they
     should be lifted together.

     **The bound is narrower than "not a single base-table scan", and the
     record said the wider thing.** Measured on all three arms, `routed=none`:
     a positional reference over a star whose FROM is a DERIVED TABLE answers —
     `SELECT * FROM (SELECT * FROM zzp) x ORDER BY 1`, an explicit column list
     inside, aliased columns inside, and a derived table nested two deep all
     answer. Only a derived table whose OWN FROM is a join refuses, which is
     the join case one level down. The true statement is "a star over a join,
     or over a derived table whose own FROM is a join". The engine's 42P10
     message carried the same over-broad phrase and is corrected with this
     entry (`logical.RefuseUnresolvedOrdinalSortKeys`); the refusal itself did
     not change.

   - **Abbreviated CIDR and inet literals.** (Added 2026-09-03, #627.)
     PostgreSQL reads `'10'::cidr` as 10.0.0.0/8 and `'192.168'::cidr` as
     192.168.0.0/24 — CLASSFUL address inference, and a `cidr`-only grammar:
     `'192.168'::inet` is 22P02 there. Wadjet refuses every abbreviated form
     with 22P02. Reproducing `inet_net_pton`'s legacy class inference
     bit-exactly is its own decision with its own oracle corpus.

     The MAC and UUID halves of the same issue are NOT divergences: every
     spelling PostgreSQL accepts is accepted, at every comparison site, and
     nothing more. That last clause is the part the first version of this entry
     asserted without holding — `pgMACGroupedHex` counted SEPARATORS rather
     than GROUP SIZES, so twelve hex digits with one or two separators anywhere
     parsed, and six spellings PostgreSQL refuses with 22P02
     (`0-8-002b010203`, `0:8002b010203`, `08-002b010203`, `08002b:01:0203`,
     `08:002b:010203`, `08002b:0102:03`) were answered. Measured on 17.11: the
     grouped-hex grammar is 6+6 and 4+4+4 and nothing else. The equality is
     enforced now and the six are cells in the refused half of
     `coordinator.TestANetworkLiteralHasOneDispositionAtEverySite`, rendered at
     all seven sites — but the 22P02 refusal is ASSERTED at two of them (`eq`
     and `in`, on all three arms) and the other five are pinned as ANSWERING.
     Four of those five (`case`, `is_distinct`, `greatest`, `least`) answer
     because the boxed-pair comparators reach them while the refusal lives in
     the kernel and the row-at-a-time path; the fifth (`empty_scan`) answers for
     a different reason — the refusal is per ROW, so a predicate no row reaches
     never raises (the gate states both, at
     `arc_a2_network_literal_two_path_test.go:154-158` and `:62-64`). That
     residual is what #579 recorded before it closed COMPLETED on 2026-08-28,
     and **#627 is the open tracker that carries it**: its body names the same
     data-dependent runtime refusal (`exec/filter.go`'s `networkConstError`),
     states that #579's original defect "is therefore still open for the
     network types", and prescribes the unification — plan-time and runtime on
     the same predicate, with a guard test asserting the disposition "at every
     site (WHERE =, IS DISTINCT FROM, IN, simple CASE, GREATEST/LEAST)", which
     is this entry's site list. So this entry, the gate's pins and #627 are one
     record, and it fires when #627 closes. A fixture at every site, a refusal asserted
     at two: that distinction is the same one this entry's first version lost,
     and a bound with no fixture is how the defect survived review.

   - **A `VARCHAR(n)` or `CHAR(n)` cast ENFORCES its `n` and still does not
     DECLARE it; a short `CHAR(n)` is not padded.** (Added 2026-09-03 as #708's
     other half; the VALUE half CLOSED by #838 on the same day, the rest open.)
     Measured live on 17.11 against the same query through the embedded API:

     | shape | PostgreSQL 17.11 | wadjet |
     |---|---|---|
     | `CAST('abcdef' AS VARCHAR(4))` | `abcd` | `abcd` |
     | `CAST('abcdef' AS CHAR(4))` | `abcd` | `abcd` |
     | `CAST('éàüxyz' AS VARCHAR(3))` | `éàü` | `éàü` |
     | `CAST('abcdef' AS VARCHAR(0))` | 22023 | 22023 |
     | `\gdesc` of the first | `character varying(4)`, atttypmod 8, OID 1043 | unconstrained STRING, OID 25 |
     | `CAST('ab' AS CHAR(4))` | `ab  ` (padded) | `ab` |
     | `LENGTH(CAST('ab' AS CHAR(4)))` | 2 | 2 |
     | `CAST('ab' AS CHAR(4)) \|\| 'x'` | `abx` | `abx` |
     | `CAST('ab' AS CHAR(4)) = 'ab'` | true | true |
     | `CREATE TABLE t (v VARCHAR(4))` | accepted, atttypmod 8 | accepted, `n` not stored |
     | `INSERT` of a too-long value into it | 22001 | accepted |

     **The order was value-first, and that is the decision this entry records.**
     Declaring `character varying(4)` while returning six characters is a worse
     lie than declaring nothing: a client that trusts the description to size a
     buffer is then wrong in the direction that overflows. `expr.Cast.Eval`
     matches the lowered type name exactly, so `varchar(4)` matched no case
     label at all and the cast reached `default: return v` — the length was
     parsed by the SQL parser and dropped. It now truncates to n CHARACTERS,
     which is what PostgreSQL counts.

     **CHAR(n) truncates and does NOT pad, deliberately.** PostgreSQL's bpchar
     pads the stored value to n and then strips trailing blanks for `length()`,
     for `||` and for every comparison — the four rows above are all measured.
     This engine has one `TypeString` and no bpchar type, so padding would put
     the blanks into GROUP BY keys, join keys and equality, where PostgreSQL
     strips them: it would buy a right rendering with a WRONG ROW SET. The
     residual is therefore the rendered value of a SHORT `CHAR(n)`, and the
     three consumer rows above are the fixtures that say why.

     **Still open: the DECLARATION.** `physical.declaredTypmod` answers only
     for DECIMAL and `pgwire.TypeMod` has no string arm, so RowDescription
     says unconstrained `text` where PostgreSQL says `character varying(4)`.
     Closing it means carrying the length beside `batch.TypeString` from the
     cast's declared type through `ColumnMeta` to `TypeMod` (n+4) and moving
     the OID to 1043 / 1042 for a length-carrying string only — a plain TEXT
     column must stay 25.

     **Still open: DDL keeps no `n`.** `parquet.ParseTypeID` used to REFUSE
     `VARCHAR(4)` outright ("unknown type"), so a PostgreSQL user's ordinary
     `VARCHAR(255)` failed the whole CREATE TABLE; it is accepted now and the
     length is dropped, which makes an INSERT past n a superset (PostgreSQL
     raises 22001) rather than a refused table.

     **An INVALID modifier is refused identically by both doors**, because
     both read it with `parquet.StringTypeLength` — one accept-set, the model
     `ParseDateDays` set. Measured live and asserted at both doors:

     | modifier | SQLSTATE | message |
     |---|---|---|
     | `VARCHAR(0)`, `CHAR(0)` | 22023 | `length for type varchar \| char must be at least 1` |
     | `VARCHAR(10485761)` | 22023 | `length for type varchar cannot exceed 10485760` |
     | `VARCHAR(abc)` | 42601 | `syntax error at or near "abc"` |
     | `VARCHAR(-1)` | 42601 | `syntax error at or near "-"` |
     | `TEXT(5)` | 42601 | `type modifier is not allowed for type "text"` |

     Two SPELLING differences on the DDL door, both recorded rather than
     fixed: it prefixes `column "v": `, and the DDL lexer folds an unquoted
     identifier to upper case before the type name is read, so `VARCHAR(abc)`
     echoes `"ABC"` there and `"abc"` from a CAST. The code and the rule are
     the same; only the echoed token's case differs, and moving it would mean
     changing when the lexer folds.

     **The DECLARATION half closed on 2026-09-04.** `physical.declaredStringLength`
     is `declaredTypmod`'s twin for a modifier that is a LENGTH rather than a
     (p,s), the answer rides to the wire through `ColumnMeta.StringLength` on
     both the single-process and the DAG path (`CollectSink.SchemaHintStringLength`
     and `Stage.OutputStringLength`, exactly as the unconstrained-DECIMAL map
     does), and `pgwire.TypeMod` sends n+4 under OID 1043. `CAST(x AS
     VARCHAR(4))` is `character varying(4)`, atttypmod 8 — PostgreSQL 17.11's
     own \gdesc.

     The first cut of that read the LENGTH as what makes a column varchar, so
     `CAST(x AS VARCHAR)` with no length declared `text` (25). It is 1043 at
     atttypmod -1 on the server, measured: the OID follows the destination
     NAME and the typmod follows the length, and conflating them made the same
     cast change its declared TYPE when its modifier was dropped. The channel
     carries all three answers now — a positive length, `character varying`
     unconstrained (`parquet.StringLengthUnconstrainedVarchar`), and `text` —
     and `parquet.VarcharNoLength` is the one place the family's spellings are
     named (round-1 review, P2). Bare `CHAR` is deliberately not in it: the
     server reads that as `character(1)` and TRUNCATES, which is part of the
     bpchar residual below rather than a declaration question.

     **What stays**: the bpchar family, as ONE residual with one mechanism.
     `CAST(x AS CHAR(n))` declares `character varying(n)` and not `character(n)`
     (OID 1042), does not PAD a short value to n, and reads bare `CHAR` as the
     unparameterized string where the server reads `character(1)`. All three are
     the same fact: this engine has one `TypeString` and no bpchar, and
     PostgreSQL's bpchar pads the stored value and then strips the blanks again
     for `length()`, for `||` and for every comparison — all three measured
     live. Padding without the stripping would move those three cells AWAY from
     the server (a wrong ROW SET for a right rendering), and declaring 1042
     would name a type whose defining behaviours are not implemented. The
     structural fix is a distinct blank-padded string type the comparison,
     length and concatenation kernels dispatch on; it is its own change.

     Pinned by `wadjet.TestStringCastEnforcesItsLengthAndStillDropsTheDeclaration`
     (the values, the declared length per destination, and the three bpchar
     consumers that assert the agreement padding would cost) and
     `pgwire.TestVarcharCastDeclaresItsLengthOnTheWire` (OID and atttypmod, with
     the unparameterized VARCHAR spellings asserted at 1043/-1, `TEXT` and a
     non-string column as the controls, and a `residual_bare_char` cell). User-facing: `docs/data-types.md`
     §`VARCHAR(n)` and `CHAR(n)`, `docs/sql-reference.md` §Casts and errors.

   - **An integer result with no room in its declared type FAILS; it is never
     a wrapped number — and the check is at the STORE, not in the kernel.**
     (Added 2026-09-04, round-1 review P1.)

     `ABS(<int4 column>)` at -2147483648 answered -2147483648 where PostgreSQL
     17.11 raises `integer out of range` (22003), while the int8 twin already
     raised. Neither evaluator was wrong. `ColRef.Eval` widens an INT32 column
     to an int64 box on purpose (this list's "every integer spelling is INT64"
     superset), so the kernel computed 2147483648 — correct arithmetic — and
     `batch.SetValue`'s TypeInt32 arm narrowed it back with a bare
     `int32(tv)`. A different number wearing the right type: item 9's class.

     The refusal therefore belongs at the seam every such kernel crosses, and
     `batch.IntegerRangeError` raises there with the exec.FatalEvalPanic
     contract #361's TypeMismatchError already uses. A check inside ABS would
     have covered one function and, in the vectorized half, would have been
     DEAD CODE: the registry declares `abs` float64 while the planner declares
     the projection int4, so `FuncCall.EvalVec`'s `vecOutputHolds` guard sends
     every such call to the per-row path and `vecAbsDomain`'s narrow arms are
     unreachable from a projection. Measured, not inferred.

     **What stays a superset**: `-<int4 column>` at the floor and
     `<int4 column> * 2` still ANSWER, in int64, where PostgreSQL raises —
     this list's widening divergence, unchanged and now pinned in the same
     gate so the new refusal cannot swallow it silently.

     **And the PREDICATE position**, which is the same superset reached a
     different way: `WHERE ABS(<int4 column>) > 2147483646` at the floor
     ANSWERS the row where PostgreSQL raises `integer out of range`
     (measured). A predicate has no int4-declared output column for the
     operand to be stored into, so it never crosses `batch.SetValue` and the
     store's refusal cannot reach it; the comparison sees the widened int64
     the kernel computed, which is the exact value. Base-identical, and
     deliberately not changed — refusing there would reject a row wadjet can
     evaluate exactly, the wrong direction for this list. The int8 twin DOES
     raise in the predicate position, because `expr.absKeepsDomain` raises
     wherever it runs, so the two widths disagree here on purpose. Pinned by
     the same gate's `predicate_position_answers_the_widened_value` cell,
     whose bound is chosen so a wrapped value fails it as loudly as a refusal
     would.

     Gated by `coordinator.TestIntegerMinimumIsLoudOnEveryArm` (nine shapes ×
     five arms over the `intmin` fixture), `batch.TestInt32StoreKeepsTheNumberOrRefuses`
     (the seam with no plan) and `pgwire.TestIntegerOutOfRangeReaches22003OnTheWire`
     (the wire).

   - **A CAST to a type name this engine does not have is 42704, on both
     doors — including PostgreSQL type names it has no type for.** (Added
     2026-09-04, from #652.)

     `CAST(1 AS bogustype)` answered the string "1" under OID 25 and
     `CAST(<float column> AS bogustype)` answered its digits the same way:
     `expr.Cast.Eval`'s switch fell to `default: return v` and
     `physical.inferCastType`'s to `default: return TypeString`, so the two
     layers agreed with EACH OTHER about a column PostgreSQL 17.11 says cannot
     be described at all (`type "bogustype" does not exist`). That is item 9's
     class reached through the type system: a measurement published as text.

     `expr.KnownCastDest` is the accept-set, and it is the UNION of the two
     doors — every name `parquet.ParseTypeID` takes as a column type, plus the
     PostgreSQL spellings `Cast.Eval` implements that no column type answers
     to (`int4`, `smallint`, `real`, `double precision`, `signed`,
     `timestamptz`, …). One type name, one disposition, which is the property
     the length-modifier entry above already states.

     **What diverges**: three PostgreSQL type names this engine has no type
     for AND renders differently — `bytea`, `money`, `inet` — are refused where
     the server answers. The `CREATE TABLE` door has refused those all along
     (with no SQLSTATE at all until this change; it carries 42704 now, from
     `parquet.ParseTypeID`), and the alternative on the CAST door was a value
     under a `text` declaration that is not the server's: `abc` for
     `\x616263`, `1.5` for `$1.50`, `192.168.1.1` for `192.168.1.1/32`. Loud
     beats plausible THERE, and only there.

     **What does not**: `time`, `json` and `xml`. The first cut of this refused
     them too, and their text is what the server answers byte for byte —
     `CAST('12:34:56' AS time)` is `12:34:56` on both engines. Turning a right
     answer loud is the direction this list does not permit, and
     `expr.TestUnknownCastTypeIsRefusedAndKnownOnesStillAnswer` had been left
     in the tree saying so; it was passing only because it called `Cast.Eval`
     while the refusal had moved to the compile (round-1 review, B4). They are
     accepted, pass their text through, and are declared `text` — a
     DECLARATION divergence over a right value, the same class as every other
     pass-through destination.

     **What stays a superset**: a destination this engine HAS but does not
     CONVERT still returns its operand unchanged — the network types,
     `DURATION`, `BYTES`, `VECTOR(n)`, the containers, and non-address text
     cast to `IPV4`/`IPV6`/`CIDR`/`MACADDR`. This pass refuses names that name
     NOTHING, not casts that are unimplemented, and the boundary is asserted
     in both directions.

     Gated by `wadjet.TestUnknownCastDestinationIsUndefinedObject` (the
     refusals, 24 destination controls, and the unimplemented-but-named
     pass-throughs) and two `runWireErrors` entries. User-facing:
     `docs/sql-reference.md` §Casts and errors.

   - **A timestamp-VALUED scalar function DECLARES text where PostgreSQL
     declares timestamp.** (Added 2026-09-04, from #544's residual; the
     rendering half CLOSED 2026-09-04.)

     `DATE_TRUNC('day', ts)` answered `2023-11-14T00:00:00Z` where PostgreSQL
     17.11 answers `2023-11-14 00:00:00`, and so did `NOW`,
     `CURRENT_TIMESTAMP`, `FROM_UNIXTIME`, `DATE_PARSE`, `TIMEZONE`,
     `PG_POSTMASTER_START_TIME` and interval arithmetic over a text operand.

     The first version of this entry said the fix had to wait for a
     TIMESTAMP-valued function result, because "the format is a per-function
     choice rather than a property of a type, and `batch.FormatTimestamp` has
     nothing to be called from". That was wrong on its own terms: a function
     that formats its own text can be given the ONE formatter to call, and the
     twelve `date_trunc` arms plus the six other sites now call
     `expr.formatInstant`, which is `batch.FormatTimestamp`. One renderer is
     exactly the property #544 states, and for every function whose result is
     derived from DATA — `date_trunc`, `from_unixtime`, `date_parse`,
     `timezone`, interval arithmetic over a text operand — the VALUE is
     PostgreSQL's byte for byte. The three CLOCK functions are the exception
     and have their own entry below.

     Two functions deliberately keep an ISO 8601 rendering: `TO_ISO8601`,
     whose name is its format contract, and `AT_TIMEZONE`, whose result is a
     wall clock in another zone that a zoneless rendering would publish as
     UTC — the same misreading `TIMEZONE` declines a non-UTC zone rather than
     commit.

     **What still diverges** is the DECLARATION. `date_trunc` RETURNS
     `timestamp` (OID 1114) on the server and `text` (OID 25) here: the scalar
     registry is `func([]any) any` with one static `Ret` per entry, so a
     function result cannot be TIMESTAMP-typed at all. A client that asks what
     the column IS gets the wrong answer, and no amount of formatting fixes
     that — which is why it is recorded rather than bandaged (protocol rule
     11). The structural fix is a type channel in the registry: ~14 entries
     return the engine's TIMESTAMP box and carry `parquet.TypeTimestamp`, and
     the planner's declared type follows.

     Gated by `wadjet.TestTimestampHasOneRenderingAtEverySite` — seven
     function-result value cells for the closed half, and the
     `residual_date_trunc_declares_string_not_timestamp` pin for the open one,
     which fails the day a timestamp-valued function declares TIMESTAMP.

   - **`NOW`, `CURRENT_TIMESTAMP` and `PG_POSTMASTER_START_TIME` render a
     ZONELESS instant at MILLISECOND resolution where PostgreSQL renders a
     `timestamptz` with an offset at microsecond resolution.** (Added
     2026-09-04, from #544's second pass; round-2 review, B2r2.)

     Measured on PostgreSQL 17.11:

     ```
     SELECT now()::text                      2026-09-04 21:21:01.708284+00
     SELECT pg_typeof(now())                 timestamp with time zone
     SELECT pg_postmaster_start_time()::text 2026-08-31 20:43:01.076093+00
     ```

     and here: `2026-09-04 21:21:01.708`, declared `text`. Two facts differ,
     and both are structural rather than a formatting choice.

     **No `timestamptz`.** This engine has one TIMESTAMP type, an instant with
     no zone, so there is no offset to print. PostgreSQL's `timestamptz` is
     also an instant with no stored zone — it renders in the session's
     `TimeZone` — so the two hold the SAME value and disagree about what the
     text says about it.

     **Millisecond carrier.** A wadjet instant is epoch milliseconds
     (`batch.FormatTimestamp`), PostgreSQL's is microseconds. Three fractional
     digits is the most any rendering here can carry; see `docs/data-types.md`
     §Timestamp, "The resolution is the millisecond".

     Before #544's second pass these three answered RFC3339
     (`2026-09-04T21:01:38Z`), which named the zone and still was not
     PostgreSQL's text. Routing them through the one renderer is what "one
     rendering" requires — a second dialect for three functions is the thing
     it forbids — and the offset it costs is recorded here rather than traded
     for a second formatter. Closing it means a zone-aware timestamp type and
     a microsecond carrier, which is its own change.

     `expr.TestPgPostmasterStartTime` parses the engine's rendering and
     `pgwire.startupTimeIsThisProcess` asserts the value through the DataGrip
     opening sequence; neither pins the offset, because there is none to pin.

6. **A numeric literal's carrier is its TEXT, not a float64.** (Added
   2026-08-23, from #452.) PostgreSQL types an unsuffixed decimal literal as
   `numeric` and compares it at full precision, so `WHERE d = 493827160549382.7160549350`
   must find the row holding that value. A float64 carries ~15-16 significant
   decimal digits and a `DECIMAL(38,10)` carries 38, so the box the compiler
   builds for arithmetic cannot also be the record of which number was
   written: it is a different number by the time it meets the column, and the
   damage is not uniform — the rounded literal landed just BELOW the stored
   value, so `=` matched nothing, `>` gained a row, `<>` gained it back, and
   `>=` and `<` agreed by luck. Four operators agreeing is not partly right.

   Three rules follow. They hold for a bare DECIMAL column compared, matched
   against an IN list, or bounded by BETWEEN, against a numeric literal — the
   vectorized kernel, the row-at-a-time expression, the raw-text predicate,
   and the row-group prune all bind that one shape. (Amended 2026-08-24,
   #465: `CASE d WHEN lit`, `d IS DISTINCT FROM lit` and
   `GREATEST`/`LEAST(d, lit)` now hold them too. Those three compare through
   the BOXED path, where the column is rendered text and the literal is the
   float64 box, so they carry the literal's `Text` into the comparison and
   order the two exact decimals — `expr.boxedPair`'s literal arm and
   `batch.CompareDecimalTexts`. (That carry-through was `expr.compareWithText`
   until #504 replaced it: it applied the exact reading to ANY string box
   against a literal's text, which read a genuine STRING column numerically.
   The rule it applied is unchanged; what selects it is now the operands'
   declared kinds.) An arithmetic-wrapped operand — `d + 0 = lit`
   — still does not: arithmetic over DECIMAL goes through float64 before any
   comparison sees it, which is the separate limit recorded at the end of this
   item.)

   - The literal's source text travels with its box (`expr.Lit.Text`,
     `logical.Predicate.ValueText`, `exec.KernelFilter.LitText`) and is what
     a DECIMAL comparison converts, at the COLUMN's scale.
   - A literal the column's scale cannot hold exactly — `0.255` against a
     `DECIMAL(9,2)` — equals nothing, and still has its place in the ORDER:
     it sits strictly between two representable values, so `> 0.255` excludes
     the row holding `0.25` that `>= 0.25` admits. Truncating the literal
     instead would answer a different question; the residual of the discarded
     digits is carried rather than dropped.
   - A literal the CARRIER cannot hold at that scale — `1e39`, or `10^30`
     against a `DECIMAL(38,10)`, whose unscaled integer needs more than the
     128 bits `Int128` has — keeps its place in the order by SATURATING: it
     compares strictly greater (or strictly less, when negative) than every
     value the column can hold, which is what it is. It never wraps and never
     errors. Narrowing it two's-complement instead put it back INSIDE the
     ordinary range as a plausible number of either sign, so `WHERE d < 1e39`
     — true of every row — selected none of them (#462). The order this
     produces is the order of the exact rationals, saturation included, which
     `batch.TestScaledDecimalOrderIsTransitiveAtTheBoundary` asserts over
     every triple of stored values and constants either side of the ends.
   - One conversion serves the prune and the filter
     (`kernel.StatsDomainValue`, `kernel.decimalLiteralAt`), because a prune
     that reads the predicate differently from the filter deletes rows the
     filter would have kept.
   - **A DECIMAL meeting a value of another type is compared by VALUE, and
     the rule is the other type's.** (Added 2026-08-24, #476/#477.) A DECIMAL
     column boxes as its rendered TEXT, so every boxed comparison against it
     used to fall through to a LEXICOGRAPHIC one, where "9" sorts above "10".
     Against an INTEGER the comparison is exact (`expr.decimalTextOrder`, the
     same `ScaledDecimal` carrier); against a FLOAT it is a float64
     comparison, because PostgreSQL's `numeric <op> double precision` casts
     the numeric; against another DECIMAL it is the unscaled Int128s at their
     two scales (`kernel.CompareDecimalValues`), which no box can be
     dispatched on — two rendered DECIMALs are indistinguishable from two
     strings — so that pair is bound from the column DECLARATIONS, per item 8.

     **Every boxed site is now bound from the declarations, not from one
     call site.** (Amended 2026-08-24, #506.) `expr.bindDecimalCols` used to
     be called from `NewCmp` alone, so a direct `d1 op d2` got the exact
     Int128 comparison while the SAME two columns at a BOXED site — a simple
     `CASE d1 WHEN d2 THEN ...`, `d1 IS DISTINCT FROM d2`,
     `GREATEST(d1, d2)`, and a Cmp whose operand is a COMPOSITE like
     `GREATEST(d1, d2) = d2` — fell through `compare()`'s two-rendered-strings
     path and compared LEXICOGRAPHICALLY, the same defect #477 fixed for the
     direct comparison. Verified against live postgres:17-alpine on the
     `declit` fixture: `CASE d_2 WHEN d_4 THEN 1 ELSE 0 END = 1` answered 0
     against PostgreSQL's 1, `GREATEST(d_2, d_4) = d_4` answered 167 against
     101, `LEAST(d_2, d_4) = d_2` 164 against 98, and
     `d_2 IS DISTINCT FROM d_4` 199 against 198.

     `expr.boxedPair` (`internal/engine/expr/boxed_pair.go`) is the binding.
     It classifies each operand's DECLARED kind — DECIMAL, a non-DECIMAL
     number, genuine text, or unknown — resolving a bare column from the
     batch and a composite (`GREATEST`/`LEAST`, `CASE`, `COALESCE`) from the
     join of the alternatives one of its values comes from, and then applies
     one rule per kind PAIR. The kinds are cached once settled, and a pair
     whose declarations can select no rule disarms itself, so the generic
     path's per-row cost stays one atomic load. Four sites hold one:
     `Cmp`'s generic arm, `Case`'s simple-CASE arm (one per WHEN),
     `IsDistinctFrom`, and `pickExtremum` (one operand per ARGUMENT, since
     the best-so-far moves between iterations). Nothing in it reads a box to
     decide which RULE applies — only to decide which side of the chosen rule
     holds this row's value, which is what item 8's boxed-value rule requires
     generally.

     **The classification covers BOTH arithmetic nodes, not only the typed
     one.** (Amended 2026-09-04, #849 round-3 residual / #555.) Exact
     fixed-point arithmetic boxes its result exactly as a DECIMAL COLUMN
     boxes one — the value's rendered text — so an arithmetic node is a
     `boxDecimal` operand whenever its exact arm resolved. `expr.BinOpNumeric`
     answered that from its mode; the GENERIC `expr.BinOp` did not, and it is
     the node that needed to, because it is where every operand with no typed
     protocol arrives: a negated column, a CAST, a scalar function, and a
     CHOOSING construct, none of which satisfies `Float64Expr` for
     `compileBinOp` to build the typed node from. So `(COALESCE(a, 0) + 1) > 1`
     compared `"1.00"` against `"1"` by BYTES and admitted the rows whose
     value is exactly 1 — 8 rows where PostgreSQL 17.11 answers 5 — and
     `GREATEST(COALESCE(a, 0) + 1, 2)` picked 2 over 13.75. Every boxed
     consumer was affected alike (`IN`, `BETWEEN`, `IS DISTINCT FROM`, a
     simple CASE, GREATEST/LEAST), and every producer that reaches this node:
     `-a + 1`, `CAST(a AS DECIMAL(9,2)) + 1` and `ABS(a) + 1` had the exact
     arm since #555 and were wrong at those sites for as long; the choosing
     constructs joined them when `0214d48b` gave them the exact kernel.
     The node's INT mode answers `boxNumber` for the same reason its typed
     sibling does, and its remaining modes stay unclassified on purpose: this
     node also evaluates date ± interval, and a shifted date is not a number,
     so declaring one would be a WRONG declaration rather than a missing one.
   - **A constant that is not a number is a query ERROR, never a value.**
     (Added 2026-08-24, #463.) The conversion used to answer ZERO for
     anything it could not parse, so `WHERE d = 'abc'` — and `WHERE d = 1e400`,
     which the float64 expansion could not read either — matched every row
     holding zero. PostgreSQL refuses both spellings of "this is not a
     number" with SQLSTATE 22P02, so wadjet raises the same, from the
     vectorized filter (`exec.decimalConstError`) and from the row-at-a-time
     comparison (`expr.raiseInvalidTextRepresentation`) alike: one path
     erroring while the other answers is the two-path defect class. Exponent
     form is not that case — `1e400` IS a number, and is now read as one, by
     folding the exponent into the scaling instead of through
     `strconv.ParseFloat`.
   - **The #463 refusal reaches the #465 boxed sites and a negated string
     literal.** (Added 2026-08-24, #505.) `CASE d WHEN 'abc'`,
     `d IS DISTINCT FROM 'abc'` and `GREATEST`/`LEAST(d, 'abc')` answered
     instead of erroring: the boxed sites' exact-text arm only fires when the
     literal already looks numeric (`Lit.Text` set by `compileLit`), so a
     non-numeric string never reached ANY refusal on these three sites and
     fell through to `compare()`'s ordinary string comparison instead.
     `expr.refuseArm` closes it: whichever operand resolves, in THIS batch,
     to a materialized DECIMAL column is checked against the other operand's
     literal text before the comparison runs, raising the same 22P02
     `decimalLitCmp.order` already raises for the direct-comparison shapes —
     so the refusal depends only on the column's REAL type, never on the
     operand's Go box, the way item 8's boxed-value rule requires generally.
     (#517 lifted the same question to bind time, where it also stops
     depending on a row and on operand order; this runtime half remains for
     the shapes the binder cannot prove.)

     `d = -'abc'` and `d = -'1e400'` were the same failure mode wearing a
     `UnaryOp`: a unary minus over a STRING literal was deliberately left
     unfolded and evaluated through the generic numeric-coercion path,
     which reads anything unparseable as the float64 zero — both matched
     the row holding 0.00, #463's exact failure mode on the one shape #463
     never touched. Compile-time folding (`compileWithCtx`'s `UnaryOp` case,
     `expr/compile.go`) now treats a QUOTED string literal the way an
     UNQUOTED one already was: a numeric-looking string folds into a
     negated `Lit` carrying its exact text — so `d = -'5.00'` enters the
     same `DecimalLiteral` path `d = -5.00` does, saturating past the
     carrier exactly as this item already specifies rather than matching
     the wrong row — and a non-numeric string is refused at COMPILE time.

     (Corrected 2026-08-24. That last sentence used to continue "before any
     row, batch, or short-circuiting conjunct exists to hide it", which
     described the refusal and not the QUERY. The refusal was raised at
     compile time and then SWALLOWED: the physical planner's six compile
     sites fall back to copying an input column when an expression will not
     compile, and only `expr.IsUnknownFunc` was exempt, so `SELECT -'abc'`
     came back as `column "-'abc'" does not exist in the input schema` with
     no SQLSTATE. A refused literal is now its own error TYPE
     (`expr.InvalidLiteralError`, SQLSTATE 22P02) and
     `expr.IsCompileRefusal` names both classes the planner must propagate;
     `TestRefusedLiteralReachesTheClientAsItsOwnError` gates the SELECT,
     WHERE, ORDER BY, GROUP BY and aggregate-argument forms. With that, the
     sentence is true of the fold — and of nothing else.)

     **The refusal is a PLAN-TIME check now, not a per-row one.** (Amended
     2026-08-24, #517.) It used to live inside the comparison, so it
     depended on two things a type rule may not depend on:

     - **A row.** `d = 'abc'` against an EMPTY table, or behind a conjunct
       no row survives to (`k > 100000 AND d IS DISTINCT FROM 'abc'`),
       answered zero rows instead of erroring, on this shape and on the
       original #463 shape alike: the comparison — and therefore the
       refusal — never ran.
     - **Which operand won.** `GREATEST`/`LEAST` compare (best-so-far,
       candidate) pairs, and a pair refused only when a DECIMAL column was on
       one side and the bad literal on the other. Which argument is the
       running best depends on the VALUES, so the SAME three arguments
       refused under one and answered under the other: `GREATEST(k, 'abc',
       d)` raised and `LEAST(k, 'abc', d)` returned a row.

     PostgreSQL resolves an unknown-typed literal's type from the column's
     DECLARATION and refuses at parse/bind time, independent of any row
     existing and of any operand order — verified live for all nine shapes,
     `=`, `<>`, IN, BETWEEN, the three boxed sites, and both extremum
     orders, each with the same 22P02 and the same message wadjet raises.

     `physical.checkLiteralTypes` is the check, in the AST binder
     (`validate.go`) that already resolves every column reference against the
     catalog before a plan exists. `colScope` carries which columns a BASE
     TABLE declares DECIMAL, and the refusal fires only when the column
     PROVABLY resolves to one in a CLOSED scope — the same conservatism the
     rest of the binder is built on, since a false positive breaks a working
     query. The runtime refusals STAY, for the shapes the binder cannot
     prove (an open scope, a derived table or CTE column, an expression it
     does not parse); both call `expr.IsNumericLiteralText`, so the two
     cannot disagree about which strings are numbers.

     **What this does NOT cover, deliberately.** The same silent reading of
     an unparseable constant as the type's ZERO is still live for the
     INTEGER and FLOAT families — `WHERE k = 'abc'` over a BIGINT column
     matches the rows holding 0, where PostgreSQL raises 22P02 "invalid
     input syntax for type bigint". That is #463's failure mode on the
     families #463 never covered, tracked as #536 and pinned in the wire
     corpus, and closing it means carrying the destination TYPE into the
     refusal rather than extending its timing. `'NaN'` is a third case:
     PostgreSQL's numeric HAS a NaN and wadjet's exact carrier does not, so
     `d = 'NaN'` is refused here and answered there (#534).

     **The refusal's cost is a plan-time cost, not a per-row one.**
     (Added 2026-08-24, from the #505 review.) The first version asked both
     of the refusal's questions on EVERY ROW — allocating a
     `kernel.DecimalLiteral` and re-walking the literal's digits to answer
     "is this a number", then re-resolving the column to answer "is this a
     DECIMAL" — which cost +42% on a simple CASE over a DECIMAL column,
     +37% on IS DISTINCT FROM, and +69% with 7x the bytes on an
     exponent-form literal. Both answers are fixed for the query's lifetime,
     which is why `decimalLitCmp.numeric` was already a cached slice rather
     than a per-row `Numeric()` call, and the same discipline now applies at
     the three boxed sites (`expr.refuseArm`, `caseArms`,
     `extremumRefusal`): the literal is judged once when the node is first
     evaluated, and the column's answer is cached the way
     `decimalLitCmp.notDecimal` caches its own. Residual overhead against
     removing the check entirely is inside the noise. A correctness fix that
     re-introduces a documented performance regression has not finished.

   Arithmetic over DECIMAL still goes through float64, and so do MIN/MAX/SUM
   over a DECIMAL column. That is a separate, visible limit — comparison is
   where a rounded value silently changes the ROW SET, which is why it is
   settled here first.

7. **Semantics decisions are technical, not product.** They are made and
   executed, then reported — not escalated. An existing project commitment
   settles everything downstream of it; check for the commitment before
   drafting the question.

8. **Float ordering follows PostgreSQL, not IEEE754, in every ORDER/
   PARTITION/peer/key context; a boxed value's comparison order follows the
   column's declaration, not the box's Go type.** (Added 2026-08-23, #444/
   #446 follow-up.)

   - **Float order.** PostgreSQL's `float8_cmp_internal`/
     `float4_cmp_internal` give FLOAT a total order that IEEE754's own
     comparison operators do not: NaN sorts ABOVE every other value and
     equals itself, and -0.0 equals +0.0. `ORDER BY`, `GROUP BY`'s peer
     grouping, window PARTITION/peer groups, and any key built to represent
     "the same value for merge/comparison purposes" now apply that rule —
     `kernel.CompareFloat64`/`CompareFloat32`
     (`internal/engine/exec/kernel/float_order.go`) is the one place the
     rule is stated, every scalar FLOAT32/FLOAT64 comparator and the
     VECTOR/ARRAY(FLOAT) element comparators are built on it, and the boxed
     k-way MERGE key a spilled aggregate's drain step reifies
     (`appendKeyValue`/`keyFloat32bits`/`keyFloat64bits`,
     `internal/engine/exec/sort.go`) is canonicalized to agree: two values
     the comparator calls equal must also serialize alike, or a query's
     answer depends on how much memory it had (the same failure mode
     `appendKeyValue`'s BYTES/ARRAY/ROW fix addressed for a different type
     class).
   - **A boxed value's order is the column's DECLARATION, not a property of
     its Go box.** `Vector.GetValue` erases declaration order — a ROW boxes
     as `map[string]any`, which has none — so a comparator that dispatches
     on the BOX's own Go type (a `map[string]any`'s keys, sorted
     alphabetically) can disagree with the COLUMNAR comparator, which reads
     the real declared field order. `internal/engine/exec/compare_boxed.go`
     resolves the boxed comparator FROM the declaration (a closure built
     once per column), so both paths order a ROW's fields positionally and a
     DECIMAL numerically, matching PostgreSQL's `record_cmp`. The dynamic
     fallback (`compareAny`, used only when no declaration is available)
     still orders a ROW by field name — no production path reaches it — and
     is not addressed by this decision.
   - **What is now covered, and what is left.** (Updated 2026-08-24, #459's
     close.) The predicate kernels (`=`, `>`, `IN` — `internal/engine/expr/
     expr.go`'s `cmpFloat64Op`/`cmpFloat32Op`, `internal/engine/exec/kernel/
     compare.go`'s `ResolveFilterKernel`), the PRIMARY (non-spilled) GROUP
     BY/DISTINCT hash key (`internal/engine/exec/aggregate.go`'s
     `typedRowHash`/`serializeGroupKey`/`appendColumnValue`), and the
     hash-join key (`internal/engine/exec/join.go`'s `buildKeyFromBatch`/
     `buildProbeKey`) now compare/hash the canonical bits — a `WHERE f = f`
     over a NaN row, and a `GROUP BY`/`DISTINCT`/hash-join over `{-0.0,
     0.0}`, agree with PostgreSQL in the single-process engine. MIN/MAX over
     a NaN column was fixed earlier and separately (`kernel.CompareFloat64`
     in the accumulator loop, #457). The DISTRIBUTED half of the same rule
     closed alongside: `hashRowsIntoPartitions`
     (`internal/worker/partitioned_shuffle_sink.go`) is the shuffle's own
     router, keyed independently of the in-process hash above, and its
     scalar FLOAT32/FLOAT64 arms moved with #459 — its VECTOR arm did not,
     because a VECTOR element's canonicalization lives in a different
     function (`appendVectorKey`, aggregate.go) that #459 did not touch;
     the router disagreeing with that key for one type was the same
     defect class one type over (`hashVectorValue` too, kept in step per
     its own comment requiring the two hash the same byte stream), closed
     in the same fold-in that closed #459. Nothing named in this item's
     original list remains open. Three findings adjacent to it — not float
     ordering — surfaced during the same work and are tracked separately
     rather than folded in: RIGHT/FULL joins losing a NULL-keyed BUILD row
     on the integer key paths (#496), `BuildFromRows` routing a dual-int key
     join down the string branch (#498), and cross-scale DECIMAL set
     operations not deduplicating (#499). A float row-group statistics bound
     can also HIDE a NaN
     that this order says must have kept the row group in a `>`/`>=`/`<>`
     prune — that is a pruning-input question, not an ordering one, and is
     recorded in ADR-0018's territory instead (its §5).

9. **Exact numeric aggregates: what MIN/MAX/SUM/AVG over a DECIMAL — and
   over an INTEGER — answer.** (Added 2026-08-23, #455; the integer half added
   2026-09-02, #784.) PostgreSQL's `min`/`max`/`sum`/`avg` over
   `numeric` are exact and answer in `numeric`. Wadjet's were answering in
   `float64`: the accumulators were already exact Int128 at the column's
   scale, but the declared OUTPUT type was a double, so everything past ~16
   significant digits was gone before any consumer saw it —
   `MAX(numeric(38,10))` returned `9.777777778877776e+14` for
   `977777777887777.7577887713`, and `HAVING MAX(d) = <that value>` therefore
   matched nothing. The contract now:

   - **MIN/MAX(DECIMAL(p,s)) → DECIMAL(p,s).** The answer is a value the
     column holds, so it keeps the column's own precision and scale. Exact,
     and identical to PostgreSQL.
   - **SUM(DECIMAL(p,s)) → DECIMAL(38,s).** Exact, accumulated in Int128 at
     the input's scale. The declared precision is the carrier's full width
     rather than the input's, because a sum genuinely exceeds its column's
     precision and a narrower declaration would hand the parquet writer a
     leaf too small for the value.
   - **SUM overflow is an ERROR, not a wrapped total.** PostgreSQL's numeric
     is unbounded; wadjet's exact carrier is 128 bits, which holds every
     DECIMAL(38) value but not every sum of them (two values near 10^38
     suffice). A wrapped sum is a different number wearing the right type, so
     the query fails with a message naming the aggregate. This is a
     deliberate, documented limit of the carrier — not a semantic
     disagreement with PostgreSQL. The flag is STICKY, so a running total
     that leaves the range and comes back — `+9e37, +9e37, -9e37`, whose
     exact total is representable — also fails; refusing a sum we did carry
     exactly is the conservative side of a limit whose other side is a
     wrapped number nobody can see is wrong.
   - **AVG(DECIMAL(p,s)) → DECIMAL(38, min(s+4, 38))**, computed as exact
     division of the Int128 sum by the row count, rounded half away from
     zero. This is a **deliberate divergence in the number of digits kept**:
     PostgreSQL's numeric division picks a scale giving at least 16
     significant digits (and never below the dividend's scale), so its answer
     may carry more or fewer fractional digits than wadjet's. Both are exact
     to the digits they keep and agree to `min(both scales)`. A fixed
     increment — the Spark and SQL Server rule — is the honest choice for a
     128-bit carrier: the digits kept do not depend on the magnitude of the
     answer, so the same query over more rows cannot silently change the
     scale of its own output column. An average with no exact 128-bit value
     is an error, for SUM overflow's reason.
   - **INTEGER inputs answer PostgreSQL's own result types, with one
     narrowing and one carrier limit.** (#784.) Taken from the live server:
     `pg_typeof(sum(int4))` is `bigint`, `pg_typeof(sum(int8))` is `numeric`,
     and `pg_typeof(avg(int2|int4|int8))` is `numeric`. Wadjet answered
     `double precision` for all of them, so `SUM` over a BIGINT column past
     2^53 lost integer digits before any client saw them. The rules now:

     - `SUM(int4-class) → BIGINT`; `SUM(int8) → DECIMAL(38,0)`, accumulated in
       the same exact Int128 carrier a DECIMAL sum uses; `AVG(any integer) →
       DECIMAL(38, AvgScale(0))`.
     - **The AVG scale is the DECIMAL rule's, not PostgreSQL's**, and for the
       DECIMAL rule's reason: PostgreSQL's numeric division picks a
       magnitude-dependent scale — sixteen fractional digits over
       `nation.n_regionkey`, none at all over a quotient already carrying
       nineteen integer digits — and a scale that depends on the values makes
       the same query over more rows change the type of its own output
       column. Wadjet answers at the fixed `batch.AvgScale(0) = 4`. Both are
       exact to the digits they keep and agree to `min(both scales)`: the
       same class as the DECIMAL AVG bullet above, and what the wire oracle's
       `SumAvgOverInteger` float-render pin cites.

       **RE-AFFIRMED 2026-09-02** after the arc-A round-0 pass re-opened it
       as a candidate defect and measured PostgreSQL 17 directly. The rule
       there is `select_div_scale`: at least sixteen SIGNIFICANT digits, so
       the FRACTIONAL digit count moves with the magnitude of the answer, and
       the output column's declared typmod is **−1** — unconstrained, no
       `(p,s)` at all. Measured: `AVG(c_i32)` over 5 000 rows answers
       `7497.6449875724937862` (16 fractional digits), `AVG(c_i64)` answers
       `2499158148.41289523` (8), `AVG` of three 1s answers
       `1.00000000000000000000` (20), and `AVG(numeric(18,4))` of 1 and 2
       answers `1.5000000000000000` (16) — so the INPUT scale is not what
       decides either. Wadjet answers `7497.6450`, `2499158148.4129`,
       `1.0000`, `1.5000` at its fixed scale 4.
       This is **not representable** on wadjet's model, not merely
       unimplemented: a column carries ONE declared scale on a 38-digit
       carrier, and sixteen fractional digits over a quotient with twenty-three
       integer digits needs thirty-nine. Adopting PostgreSQL's rule is a
       carrier-and-column-model change, not a constant. The piece that IS a
       real metadata divergence is the unconstrained numeric DECLARATION on
       the wire, which this ADR already tracks as #542. If anything here is
       worth doing it is RAISING `batch.AvgScaleIncrement` — a bigger fixed
       number of fractional digits, still fixed — and that is a benchmarked
       type-width change rather than a correctness fix.
     - **A COMPUTED integer argument is declared by its own WIDTH**, the way a
       bare column is. (Amended 2026-09-03, #841; this bullet used to read
       "declared BIGINT, not numeric".) Wadjet declares every integer
       expression INT64 (ADR-0024's recorded divergence), so the declared
       TypeID cannot tell int4 from int8 — but the AST plus the column
       declarations can, and `physical.aggInputIsWideInteger` reads them: an
       expression that provably carries an int8-domain operand (an INT64
       column, an INT64 ROW field, an integer literal outside int4's range, or
       any of those inside arithmetic, a CASE arm or a choice function) gets
       PostgreSQL's `sum(int8) → numeric`; everything else keeps `bigint`.
       That keeps `SUM(CASE WHEN … THEN 1 ELSE 0 END)` — TPC-H Q12's shape and
       a BI staple, `bigint` on the live server — where it was, and closes the
       residual the next bullet used to record. A shape the walk cannot see
       through keeps the int4 reading, so nothing moves on a shape nobody can
       point at. A bare column is unaffected either way: it is typed from the
       column's real width.
     - **What is left of the narrowing's residual is an ERROR, not a wrapped
       total**, by the SUM-overflow rule three bullets up and for its exact
       reason. Since #841 the shapes whose width can be READ answer
       PostgreSQL's exact numeric instead — `SUM(-int8_col)` and
       `SUM(CASE … ELSE int8_col END)` were pinned as refusals and now answer
       — so what remains is a computed integer sum past 2^63 that the width
       walk could not see through. It has no int64 to land in; PostgreSQL's
       numeric answers it and wadjet's declared BIGINT cannot, so the query
       fails with SQLSTATE 22003 naming the aggregate. It was silent until 2026-09-02:
       over a column whose total is exactly 2^64, `SUM(b)` answered
       18446744073709551616 and `SUM(-b)` answered 0 — one question, two
       spellings, and no way to see which one was lying. Gated on all five
       execution arms by
       `coordinator.TestIntegerSumOverflowIsLoudOnEveryArm`, which asserts
       both halves: the wrapping spellings refuse AND the exact ones still
       answer.

   - **STDDEV / VARIANCE / CORR / COVAR / MEDIAN / PERCENTILE over a DECIMAL
     stay float64.** PostgreSQL answers those in `numeric` too. This is a
     KNOWN, deliberate deviation, recorded rather than hidden: they need
     square roots and running means, which an exact fixed-point tower does
     not provide, and the oracle's float tolerance covers the difference.
     Reopening it means building the tower, not widening an accumulator.
   - **Both execution paths and the DAG's partial/final merge answer the
     same thing.** The partial ships SUM as an Int128 DECIMAL plus a COUNT,
     and the final divides — `internal/worker/avg_fold.go`. A DECIMAL
     aggregate that answered exactly in one process and approximately across
     three workers would be the two-path defect class all over again
     (ADR-0018 §3).
   - **The oracle compares these entries EXACTLY.** MIN/MAX/SUM over a
     DECIMAL are compared digit for digit on both engines
     (`pgCase.exactNumeric`), because a float-rendered comparison is what let
     the defect ship green. AVG keeps the float comparison, for the scale
     contract above and for no other reason.

10. **A network-literal comparison follows the ADDRESS's own order and
    PostgreSQL's `inet` rules — item 8's boxed-value rule applied to IPv6
    and CIDR — and a literal that names no address is a query ERROR.**
    (Added 2026-08-24, #492. Rewritten 2026-08-24 after review: the first
    pass got the ordering RULE wrong for CIDR and invented a match-nothing
    answer for a literal it could not parse.) `tryNetworkLit`/
    `CmpNetworkLit` (`internal/engine/expr`) already pre-parsed an IPv4 or
    MAC literal into its column's raw int64 encoding at compile time, so
    ordering compared the address numerically. IPv6 and CIDR literals had
    no such preparser: `compileCmp` fell back to a plain `*expr.Cmp`, whose
    generic path compares the column's RENDERED TEXT lexically —
    `"2001:db8::9" > "2001:db8::10"` as text (`'1' < '9'` byte-wise), the
    opposite of the numeric truth, and not even a total order (a value can
    fail BOTH `<` and `>` against the same literal). CIDR was worse: even
    the KERNEL's scan-pushdown path (`ResolveFilterKernel`'s `TypeCIDR`
    case) was lexical, because the column stores CIDR as plain text
    (`parquet/schema.go`) with no raw-byte form to fall back on the way
    IPv6 already had.

    **The order is PostgreSQL's `inet`, not its `cidr`.** Those are two
    types there and only one of them can hold what wadjet's column holds:
    `'10.0.0.1/8'::cidr` is an ERROR in PostgreSQL ("Value has bits set to
    right of mask") while `'10.0.0.1/8'::inet` is an ordinary value, and
    wadjet's CIDR column is unvalidated text (`internal/storage/ingest`)
    into which host-bearing prefixes are routinely written — they are what
    most network telemetry carries. Choosing `cidr`'s semantics would mean
    declaring most of the real data invalid; choosing `inet`'s means every
    value has a place in the order. `inet` it is.

    `network_cmp_internal` (`src/backend/utils/adt/network.c`) compares, in
    order: the address FAMILY; the common bits under the SMALLER of the two
    prefix lengths; the prefix length; the FULL, UNMASKED address. Three
    consequences a simpler rule gets wrong, all verified against live
    PostgreSQL 17:

	'9.255.255.255/32' < '10.0.0.0/8'    — common bits decide before the mask
	'192.168.1.5/24'   < '192.168.1.0/32' — the MASK outranks the address
	'10.0.0.0/8'       < '10.0.0.1/8'    — host bits are KEPT, ordered last

    `kernel.CidrSortKey` is that order as a byte string —
    `[family][address masked to its own prefix][prefix length][full unmasked
    address]` — and is the ONE implementation both the kernel's
    scan-pushdown path and `expr.CmpNetworkLit`'s generic-evaluation path
    call, exported for exactly the reason its own doc comment gives: two
    structural parsers maintained separately is the two-path defect class
    this item closes, not a shape to reintroduce by duplicating it.
    `TestCidrSortKeyMatchesPostgresInetOrder` pins the whole order against a
    PostgreSQL-derived table of host-bearing and canonical values, v4 and
    v6, at mixed prefix lengths.

    **The first pass keyed the MASKED network alone**, which threw the host
    bits away: `10.0.0.1/8` and `10.0.0.0/8` became one value, so
    `WHERE c_cidr = '10.0.0.1/8'` answered a row holding a DIFFERENT
    address. Every value in the corpus was a canonical `192.168.N.0/24`, so
    no gate could see it; the fixture now mixes canonical and host-bearing
    prefixes at four mask lengths, with host-bearing addresses INSIDE
    networks the fixture also holds (`typematrix.cidrValue`).

    **A BARE address is a /32 or /128 host route**, which is what
    PostgreSQL's inet does with the same input (`'10.0.0.1'::inet =
    '10.0.0.1/32'::inet` is true). The first pass could not parse one, and
    answered `c_cidr = '10.0.0.1'` with a match-nothing kernel through the
    scan while the row-at-a-time path compared the text — so a WHERE clause
    and a SELECT list disagreed about the same row.

    **A literal that names NO address is a query ERROR — SQLSTATE 22P02 —
    never a value and never a match-nothing kernel.** This is #463's rule
    for DECIMAL, one type family over, and for the same reason: a
    match-nothing answer to `c_cidr <> 'garbage'` deletes every row of a
    query that cannot mean anything, silently. Both paths raise it: the
    kernel returns no kernel and `exec.networkConstError` turns that into
    the error (the mechanism `compareFilterDecimal` already used), and the
    row path raises from `CmpNetworkLit`'s CIDR/IPv6 arms for a literal that
    parses as some OTHER address kind, or from the binding `Cmp` already
    carries for a literal that parses as nothing at all. The lexical
    `genericFallback` is gone from both arms. The comment that claimed
    `parseIPv4ToInt64`/`parseIPv6ToRawString` already answered such a
    literal with a match-nothing sentinel was simply wrong — the first
    returns 0, which MATCHES the rows holding `0.0.0.0` — and TypeIPv4,
    TypeMAC and TypeUUID still take that silent path. That is the same
    defect one type over, filed as #519 rather than widened into this fix.

    **A malformed STORED value is UNKNOWN, not an error and not a text
    comparison.** The column is unvalidated, so a row can hold something
    that is not an address. It matches nothing for every operator, `<>`
    included — the answer a NULL row gets — on both paths. Raising instead
    would fail a whole query over one bad row in a column the engine never
    promised to validate; falling back to a lexical comparison on one path
    only (what `evalCIDR` did) is how one bad row split the two paths apart.

    **That UNKNOWN rule is a PREDICATE rule, not a value rule.** A malformed
    row is invisible to `WHERE c_cidr = anything` and `WHERE c_cidr <>
    anything`, but it is still an ORDINARY value everywhere a query retains
    or orders values rather than testing them against one: GROUP BY gives it
    its own group, ORDER BY gives it a definite position, and MIN/MAX can
    return it. `kernel.CidrOrderKey` is exactly this split, by construction —
    it is `CidrSortKey`'s `ok` collapsed to always succeed, falling back to
    the row's own raw text when the value does not parse, so "no defined
    place in `WHERE`'s order" becomes "the row's own bytes decide its place"
    the moment the consumer is a key or a comparator instead of a predicate.
    One consequence worth naming because it looks like a bug and is not:
    `MAX(c_cidr)` can return a value that no `WHERE c_cidr = '<that text>'`
    will ever match — a malformed value sorts (as its raw text, which a
    real address's inet-ordered key does not collide with except by exact
    byte match) and predicates refuse it. PostgreSQL has no equivalent case
    to verify against, because its `inet` input function rejects a
    malformed literal before a row can ever hold one; wadjet's column is
    unvalidated text, so this asymmetry is the shape the "don't fail a whole
    query over one bad row" decision above takes once #520 gave GROUP BY/
    ORDER BY/MIN-MAX their own inet-ordered key.

    **IPv6 against a v4-shaped literal is a FAMILY comparison.**
    PostgreSQL's inet puts every v4 address below every v6 one
    (`'255.255.255.255'::inet < '::'::inet`), including below a v4-MAPPED v6
    address, which it still calls family 6. `kernel.IPv6LitKey` keys a v4
    literal to the EMPTY string — shorter than, and a prefix of, every
    16-byte row value, so it compares strictly below all of them with no
    per-row re-keying. The kernel used to read that literal as its v4-mapped
    16 bytes, landing it in the MIDDLE of the v6 range, while the expr path
    fell through to a lexical text compare: two paths, two orders, neither
    PostgreSQL's.

    IPv6's ordinary case needed the same preparse CIDR did: the literal is
    the address's raw 16 bytes, a fixed-width big-endian encoding, so Go's
    own string ordering IS the address's numeric order.

    UUID needed no fix: its literal always zero-pads to a fixed 32-hex-digit
    form, so lexical order of that FIXED-WIDTH text happens to equal the
    address's own byte order — an accident of representation pinned by a
    test (`TestUUIDOrderingIsCorrectByHexAccident`) rather than relied on
    silently, because it does not generalize to IPv6's variable-width
    `::`-compressed form or CIDR's variable-width prefix notation, which is
    exactly why those two needed a real fix and UUID did not.

    **One key, every consumer of the comparison.** `IN` shares it
    (`kernel.inFilterKeyed`), because `c = 'X'` and `c IN ('X')` answering
    differently is the two-kernel version of the same defect. The row-group
    PRUNE does not, and cannot: the footer bounds are the address TEXT's
    extremes and the inet-order extremes are different rows, so TypeCIDR is
    withheld from pruning entirely — ADR-0018 §6, which is where that
    reasoning lives; restoring it needs an inet-ordered bound written at
    WRITE time, filed as #523.

    **Residual, closed 2026-08-25 (#520): `ORDER BY`, `GROUP BY`, DISTINCT,
    COUNT(DISTINCT), MIN/MAX and hash-join keys over a CIDR column used TEXT
    order.** The two engines agreed with each other there, so no two-path
    gate saw it, but PostgreSQL sorts those by inet order too — '10.0.0.1'
    and '10.0.0.1/32' are one value there, so text-order GROUP BY/DISTINCT
    answered TWO groups/values for a pair `=` already called one. Closing it
    needed an inet-ordered comparator in the sort kernels, the group/hash
    key (`appendColumnValue` and the spill/boxed key path), the
    declaration-resolved boxed comparator, MIN/MAX (both the batch kernel
    and `Accumulator.Merge`, which combines partials across a scan-batch or
    parallel-worker boundary and had the identical TEXT-order bug one level
    up), and BOTH partition routers — the distributed shuffle sink and a
    second, LOCAL one (`legacyCompositeHash`, the morsel-parallel
    aggregation router), which was the hardest of the sites to find because
    it only misroutes rows across a `workers > 1` boundary. `kernel.CidrOrderKey`
    is the one implementation every site now calls, the same breadth item
    8's float rule needed for its own primitive (`kernel.KeyFloat64Bits`).

    **Residual, closed 2026-08-25 (#546): the single-process SET OPERATION
    dedupped a CIDR column by its raw stored TEXT** while the stage DAG —
    which lowers a set operation to a `GroupByAll` aggregate, so #520's key
    already reached it — dedupped it by inet. The identical `UNION` answered
    4 rows locally and 3 on the DAG. `physical.keyValueText`'s
    `parquet.TypeCIDR` arm is the missing consumer, and it takes
    `CidrOrderKey` rather than `CidrSortKey` for the reason the split above
    gives: this is a KEY, so a value that names no address needs a position
    rather than a refusal, and discarding those to the empty string would
    dedup three DISTINCT malformed values into one member.

    **Residual, closed 2026-08-25 (#565): a column-to-COLUMN comparison was
    inet-ordered on the VECTORIZED kernel alone.** `WHERE c = d` answered
    PostgreSQL's 2 in process and 0 on the stage DAG, and — the sharper
    form, needing no second engine — one process selected two rows through
    the kernel and then said `SELECT c = d` was FALSE about those same rows.
    `expr.compare()`'s both-string fast path is what a projection and a later
    DAG stage's re-parsed filter reach, and it compared the stored texts.

    The fix is item 8's boxed-value rule applied to this family: the pair is
    bound from the two operands' DECLARATIONS (`expr/boxed_pair.go`'s
    `boxCidr`/`boxIPv6` kinds), which is the only place the answer exists,
    since a CIDR value and a STRING value are the same Go box. That reaches
    the three sites #506 needed for DECIMAL — a simple `CASE`'s operand, `IS
    DISTINCT FROM`, `GREATEST`/`LEAST` — which were wrong on BOTH arms and
    so invisible to every two-path gate.

    Two findings from sweeping the other network types on that shape:

    - **IPv6 column-to-column ORDERING had the identical split**, and is
      fixed with it. The kernel compares the stored raw 16 bytes;
      `ColRef.Eval` boxes the RENDERED text, and "2001:db8::9" sorts above
      "2001:db8::10" as text and below it as an address.
      `kernel.IPv6RowKey` re-keys the rendering back to the stored bytes —
      exactly, v4-MAPPED addresses included, which is why it is NOT
      `IPv6LitKey`: a LITERAL dotted quad is a v4 address and keys below
      every v6 row by family, while a STORED one is a v4-mapped v6 address
      and keys among them. IPv4, MAC and UUID were checked on the same shape
      and AGREE — the first two box the raw encoded int64, UUID by the
      fixed-width-hex accident recorded above — and are pinned that way.
    - **A malformed STORED value was compared by TEXT at both col-col
      sites**, the kernel included, contradicting this item's own UNKNOWN
      rule. `colColFilterCidr` keyed through `CidrOrderKey`, whose text
      fallback is right for a KEY and wrong for a PREDICATE — the split this
      item draws, missed at one site — so `WHERE c = 'garbage'` and
      `WHERE c = d` followed two different rules about the same bad row.
      Both take `CidrSortKey` and UNKNOWN now.

    **Open residuals, filed 2026-08-25.** Three places a CIDR value still
    does not get the order this item specifies. None is a predicate, which is
    why none of them is fixed here:

    - **#568: a ROW FIELD PATH is declared STRING, so `ORDER BY rw.c` sorts
      by text** — `9.0.0.0/8` LAST, where `ORDER BY rw` over the same values
      puts it FIRST. The sort is not what is wrong: `colRefDeclaredType`
      resolves the field name against the input's own COLUMNS, where it is
      not one, and never consults the parent's `parquet.Column.Fields`, so
      the projection keeps its STRING default and `ResolveSortCompare` is
      handed a STRING column. It is not a CIDR defect — `ORDER BY rw.n`
      sorts an INT64 field as text by the same mechanism — which is why it
      is its own issue rather than a line in this list.
      `wadjet.TestRowFieldPathLosesTheFieldsDeclaredType` pins it.
    - **#569: windowed MIN/MAX declares FLOAT64 and fails the query** for
      CIDR and seven other types, where the plain aggregate answers
      correctly. `exec.WindowMinMaxType`'s allow-list is deliberate — the
      window picks its answer with `compareAny`, which dispatches on the Go
      box and has no type tag to route a CIDR to `CidrOrderKey`, so the
      FLOAT64 declaration turns a silently wrong ordering into a loud
      failure. Widening it means giving that comparison the declaration
      (`exec.newBoxedCompare`), which is this item's rule again at a site
      that never had it.
    - **#523: the row-group PRUNE**, unchanged and recorded above.

    The two-path divergence this item closes was reproducible directly: the
    single-process engine answered 16 rows and the stage DAG answered 2 for
    the same `WHERE c_ipv6 < '2001:db8::10'` — both engines compile the
    predicate through the identical `expr.Compile` (the worker's
    `compileFilterExprs` calls it too, never a separate re-implementation),
    so the fix lives entirely in `internal/engine/expr` and
    `internal/engine/exec/kernel` and reaches both paths through the shape
    both already shared. `internal/oracle/typematrix`'s corpus, which had
    deliberately excluded an ordering literal comparison against IPv6/CIDR
    to avoid gating an already-known bug, now includes one
    (`litcmp_ord_c_ipv6`, `litcmp_ord_c_cidr`) plus the shapes the second
    pass needed (`litcmp_bare_c_cidr`, `litcmp_hostbits_c_cidr`,
    `litcmp_xfamily_c_ipv6` and their ordering forms).

11. **LIKE renders the column to TEXT for every type — the value's own
    printed form, identical at both evaluation sites — rather than refusing
    the way PostgreSQL does.** (Added 2026-08-24, #497. Scoped and corrected
    2026-08-24 after review: the original claimed the rendering matches
    `CAST AS STRING` across all 22 types, which is true of the seven types
    the fix was about and false of DATE.)

    PostgreSQL's `inet`/`cidr`/`macaddr` — and its `date`, `numeric`,
    `integer` and `boolean` — refuse LIKE outright (verified live:
    `'10.0.0.1'::inet LIKE '10.%'` raises "operator does not exist: inet ~~
    unknown"). That is not a semantics PostgreSQL DECIDED for wadjet's own
    network types to disagree with (item 1's territory); it is PostgreSQL
    not having the "these types are text everywhere" contract #484 already
    built for them. **Wadjet renders and matches.** The reasons, in order:
    the rendering contract already exists for the six network types and
    UUID; the engine has answered `int_col LIKE '1%'` since before there was
    a decision to make, and turning that into an error is a breaking change
    with no correctness payoff; and the invariant that actually failed here
    was never "does this operator exist" but "does it read the right backing
    store, and does it answer the same at both sites".

    **What the text is.** The value's own printed form — `Vector.GetValue`'s
    rendering, which is what the projection shows: `2011-02-02` for a DATE,
    `10.0.0.1` for an IPv4, `1.0001` for a DECIMAL, epoch MILLISECONDS for a
    TIMESTAMP, `0.14285715` for a FLOAT32 (float32 shortest round-trip, not
    its float64 widening). For the six network types and UUID this is also
    exactly what `CAST AS STRING` and every scalar function argument produce.
    (Amended 2026-08-25, #521: it did NOT hold for DATE — `CAST(c_date AS
    STRING)` answered the epoch DAY, `15007`, where the projection and LIKE
    answered `2011-02-02` — and the review that closed it found the identical
    gap for FLOAT32, where CAST answered the float64-widened digits instead
    of the float32-shortest-round-trip form. Both were `expr.Cast.Eval`'s
    string-family case reading the operand through `ColRef.Eval`'s raw-box
    fast path (epoch-day int32 for DATE, float64-widened for FLOAT32)
    instead of the column's own rendering — the identical mechanism LIKE's
    operand had via `likeOperand`, which #497's review had already fixed
    for these same two types on the LIKE side and left CAST's separate,
    narrower `networkOperand` (IPv4/MAC only) unfixed. The two resolvers
    are now one function, `boxedTextOperand`, shared by both call sites —
    the two-implementation drift this ADR calls out elsewhere (`CidrSortKey`,
    `appendColumnValue`) for the same reason: two structural renderers
    maintained separately is how one gets fixed and the other doesn't. The
    claim now holds for every flat type PostgreSQL gives wadjet a printed
    form to agree on, verified against live PostgreSQL 17: `date::text` and
    a `real` column's `::text` match wadjet's rendering exactly.
    `wadjet.TestCastStringAgreesWithPostgresAcrossFixture` sweeps every flat
    type across the type-matrix fixture; item 11 below records LIKE's own
    sweep, `wadjet.TestLikeAnswersTheSameAtBothSites`, which already covered
    every flat type and is what caught FLOAT32 for LIKE in the first place.)

    **A FLOAT32 concatenated as TEXT is rendered at FLOAT64 WIDTH, and that
    is a recorded divergence.** (Added 2026-09-02, #609's review.) The
    paragraph above says a FLOAT32's text is `0.14285715`, its shortest
    round-trip, and that is what `CAST` and `LIKE` answer — they share
    `boxedTextOperand`, whose type list has FLOAT32 on it since #521. The
    FUNCTION-ARGUMENT path does not: `expr.ColRef.Eval` widens a FLOAT32 to
    float64 on the way out and the text kernels stringify that, so
    `CONCAT(c_f32, 'x')` and `c_f32 || 'x'` both answer
    `0.1428571492433548x` where PostgreSQL 17 answers `0.14285715x`
    (`(1.0::real/7.0::real)::text || 'x'`, verified live).

    It is recorded rather than left to be re-found because it is one type's
    text having TWO renderings inside one engine — the shape this item exists
    to close — and because the #609 split of `||` from `CONCAT` is where a
    reader will look for it. BOTH spellings answer alike, which is precisely
    what says the split did not cause it: the divergence is older than the
    split and belongs to the float32 renderer on the function-argument path,
    not to either concatenation kernel. Pinned as the arc-A census cell
    `#609/boundary_float32_concat_widens_on_both_spellings`, which asserts
    the two spellings AGREE and carries PostgreSQL's answer beside them, so
    the day either moves the cell fails. The fix is to render a FLOAT32
    argument at its own width where every other boxed type is already
    rendered (`FuncCall.Eval`'s `stringInputFuncs` rewrite, beside the
    IPv4/MAC/DATE arms), not in `fnConcat` or `fnConcatOp`.

    **Both sites must render alike, and two did not.** `ResolveLikeFilterKernel`
    (`internal/engine/exec/kernel/compare.go`) unconditionally read the
    column's `BytesData`, with no per-type dispatch at all — the same shape
    of gap `ResolveFilterKernel` (the `=`/`<`/`>` kernel) is explicitly built
    to avoid. TypeIPv4/TypeMAC/TypePort/TypeProtocol store into
    `Int64Data`/`Int32Data`, so this INDEXED AN EMPTY BACKING STORE — a panic
    that is not the one deliberate `FatalEvalPanic` shape the pipeline
    drivers convert back into a query error, so it re-raised untouched all
    the way up: a process killer, not a wrong answer. TypeIPv6/TypeUUID
    store into `BytesData` but as the address's RAW binary form, so the
    pattern silently matched nothing. `likeTextRenderer` now resolves a
    per-type row-to-text function once per column — the same
    resolve-once-dispatch-in-the-loop discipline every other kernel here
    follows — covering every one of the 22 types (a default arm renders any
    other type's own boxed value, never indexing `BytesData` on a column
    that does not have it).

    The row-at-a-time `expr.Like` path had the same class of gap for the
    four types `ColRef.Eval` boxes differently from `GetValue`. #497 closed
    two of them (TypeIPv4/TypeMAC, through the shared `networkOperand`
    resolver `Cast`'s string-family case already used) and left two open,
    which the review found: `c_date LIKE '20%'` matched 4949 rows through
    the scan and 83 through a projection — the epoch DAY, not the date —
    and `c_f32 LIKE '%1%'` differed by 237 rows, a float64-widened rendering
    of a float32. `expr.likeOperand` closes both, and
    `wadjet.TestLikeAnswersTheSameAtBothSites` sweeps EVERY flat type
    through both sites so a fifth type that starts boxing differently is a
    failing test rather than another quiet divergence. An enumerated list of
    "types that box differently" is the same shape of gap #497 was filed
    for; the sweep is what makes the list checkable.

    **Containers refuse, closed 2026-08-25 (#522).** ARRAY, ROW, MAP and
    VECTOR used to reach the default arm and match Go's own `fmt.Sprint` of
    the boxed value (`[1 2 3]`, `map[k0:0]`) — both sites agreed on it, so it
    was not a two-path divergence, but it was not a text form the engine
    produces anywhere else and not a contract: it would have changed if the
    boxing did. Of the two honest answers the open question named — define a
    real container text, or refuse the way PostgreSQL refuses its own
    composite/array types — the decision is REFUSE, on rule 1 above: this is
    not one of the seven types wadjet has its own reason to diverge from
    PostgreSQL for (they have no PostgreSQL equivalent at all), it is an
    ordinary composite/array value, and PostgreSQL's answer for `LIKE`
    against one is unambiguous (verified live: `ARRAY[1,2,3] LIKE '1'` and a
    composite-typed value both raise "operator does not exist: <type> ~~
    unknown", SQLSTATE 42883/undefined_function). Inventing a text form
    wadjet has never committed to anywhere else would be manufacturing a
    contract this ADR's own rules argue against creating casually.
    `kernel.ResolveLikeFilterKernel` returns nil for the four container
    types (`exec.LikeFilter` turns that into the 42883, the same shape
    `decimalConstError`/`networkConstError` already use for a different type
    family) and `expr.Like.EvalBoolNull` raises the same error from the row
    path (`containerLikeKind`/`raiseNoLikeOperator`) — both verified against
    live PostgreSQL 17, and `wadjet.TestLikeAnswersTheSameAtBothSites` (no
    longer skipping non-flat columns) and
    `wadjet.TestLikeAgainstContainerRefusesWithPostgresErrorCode` pin the
    SQLSTATE at both sites.

    **Both refusals are RUNTIME, not plan-time — the same open question
    #517 tracks for DECIMAL, one type family over.** `WHERE id > 100000 AND
    col = 'garbage'` answers 0 rows SILENTLY rather than raising, when no row
    satisfies `id > 100000`: row-group pruning (or simply zero matching
    rows) means the scan never delivers a batch to the `col = 'garbage'`
    filter at all, so `KernelFilter.Execute`/`expr.Like.EvalBoolNull` — and
    item 10's own `networkConstError`/`CmpNetworkLit` raise for a literal
    that names no address — never RUN for that column, and a refusal that
    only fires when data flows through it is not the same guarantee as one
    PostgreSQL's planner gives by type-checking the literal against the
    column's declared type before any row is read. #517 names the identical
    mechanism for DECIMAL's boxed CASE/IS DISTINCT FROM/GREATEST sites —
    those still ANSWER for a malformed literal reaching them, the same way a
    container or a malformed network literal still answers through a
    conjunct no row survives to. Neither this pair nor #517 is fixed here;
    #517's own "fix direction" (a plan-time check against the column's
    declaration, before any row is scanned) is the shape a fix for any of
    these would need to take, and it is one check for all of them precisely
    because the column's declaration is known before any type's differing
    runtime code path matters. (Unrelated to this pair, and mentioned only
    because it was found alongside them: #544, where `CAST(timestamp AS
    STRING)` and LIKE rendered epoch milliseconds rather than the instant
    pgwire renders. That one is FIXED — see the amendment at the end of item
    11 — and this sentence is left in place only so the pairing that found it
    still reads.)

    **Item 6's PLAN-TIME refusal has the identical gap, for the identical
    structural reason.** (Added 2026-08-25, #579.) `checkLiteralTypes`/
    `refuseLiteralAgainstColumn` (`internal/planner/physical/
    validate_literal.go`) ask `colScope` whether a column is DECIMAL — a
    plain BOOLEAN (`colScope.addQualifiedTyped`'s `isDecimal bool`,
    `internal/planner/physical/validate.go`), not the column's declared
    `parquet.TypeID` — so there is no way to ask the same question for CIDR
    or any other network type even in principle; this is not a rule that
    excludes them, it is a bool that only ever meant one type. A non-numeric
    literal against a CIDR column therefore never reaches item 6's #517
    fix and falls all the way back to THIS item's runtime-only story: it is
    evaluated as ordinary text at the three boxed sites (CASE, IS DISTINCT
    FROM, GREATEST/LEAST) and only refuses at the scan-filter runtime layer,
    and only once a row actually reaches it. Verified live against
    postgres:17 on a `cidrtest(id BIGINT, c CIDR)` fixture: `WHERE id >
    100000 AND c = 'zzz'` raises 22P02 there unconditionally and here only
    when a row satisfies `id > 100000`; `CASE c WHEN 'zzz' THEN 1 ELSE 0
    END = 1`, `c IS DISTINCT FROM 'zzz'` and `GREATEST(c, 'zzz')` all answer
    here and all raise there. Widening `colScope` to carry the column's
    declared type (or an enum covering DECIMAL/network/neither) instead of
    a bare bool, with a per-type literal validator alongside
    `expr.IsNumericLiteralText`, is the fix shape #579 records.

    **BYTES is bytea, and it is the one type where LIKE and CAST
    deliberately disagree.** (Added 2026-08-25, #570. Recorded as an open
    divergence when this item was written; closed the same day.)

    PostgreSQL's `bytea::text` is `\x` followed by lowercase hex, under the
    default `bytea_output = hex` — a setting `expr.pgcompat` already reports
    to a client that asks for it. Wadjet had THREE renderings of one BYTES
    value and none of them was that: the embedded projection handed back a
    `[]byte` (`Vector.GetValue`'s TypeBytes arm), `CAST AS STRING` and LIKE
    handed back the RAW BYTES as a Go string, and the WIRE handed back Go's
    `%v`, `[255 254 0 65]`, under OID **25** because `pgwire.pgTypeOID` had
    no BYTES case and `formatPgValueTyped` no `[]byte` case. `oidBytea = 17`
    existed in the tree and was used only for inbound Bind parameters and
    the `pg_type` catalog row.

    The CAST rendering was not only wrong, it was a HAZARD. For the four
    bytes `0xff 0xfe 0x00 0x41` the raw form is invalid UTF-8 and holds an
    embedded NUL. PostgreSQL cannot represent that in a text-format field at
    all — `text` rejects `\0`, so no PG server ever puts one inside a
    DataRow text field — and libpq TRUNCATES at it: `PQgetvalue` returns a
    NUL-terminated `char*`, so a length-aware client (pgx, JDBC) read four
    bytes and a strlen-based one read two. One query, two answers, decided
    by the client library.

    **What landed.** Rule 1 decides all of it, because PostgreSQL does give
    BYTES a printed form:

    - `pgTypeOID` returns **17 (bytea)**, `pgTypeSize` -1 and `pgTypeMod` -1
      (bytea is varlena and takes no modifier), and `pgFormatType` answers
      `bytea` so the catalog stops contradicting the wire — the same pairing
      #454 had to restore for DECIMAL.
    - `formatPgValueTyped` renders `\x` + lowercase hex, and
      `appendBinaryValue` writes the raw bytes, which IS bytea's binary form
      (`byteasend`). The binary arm is what the OID change makes
      load-bearing: under OID 25 the `%v` fallback was at least
      self-consistent, because the binary form of a text column is its
      bytes.
    - `CAST(b AS STRING)` renders the same `\x` hex, which removes the
      embedded-NUL hazard for free — hex is pure ASCII. This REVERSES the
      direction of the change that made CAST agree with `likeTextRenderer`
      by handing back the raw bytes; agreeing with each other was the right
      half, agreeing on the raw bytes was not.
    - A bytea **Bind parameter** decodes to BYTES. pgwire has no
      bound-parameter path below the parser, so Bind renders each parameter
      as a LITERAL, and it was writing the SPELLING of the bytes
      (`'\x6869'`) — a ten-character string compared against a two-byte
      column, matching nothing. `bindparams.decodeByteaText` reads both of
      `byteain`'s spellings (hex, and the escape form with `\\` and
      `\ooo`) and the binary form is the bytes themselves; a malformed
      spelling is an error, never a fallback to the raw characters.

    **LIKE does NOT follow the CAST, and that is the PostgreSQL answer.**
    `~~` over bytea EXISTS there and is BYTEWISE — verified live,
    `'\xfffe0041'::bytea LIKE '%A%'` is TRUE, matching the 0x41 BYTE and not
    a digit of the hex spelling. So PostgreSQL itself renders one way and
    matches another for this type, and `kernel.likeTextRenderer` keeps
    matching the raw bytes. The "identical at both evaluation sites" claim
    this item makes is about LIKE's two sites (the kernel and the row path),
    which still agree; the additional claim that CAST agrees with LIKE holds
    for every flat type EXCEPT this one, and only because PostgreSQL breaks
    it first. `wadjet.TestCastStringPinsPostgresRenderingPerType` pins the
    hex spelling and `TestCastStringAgreesWithPostgresAcrossFixture` builds
    the BYTES expectation from the projected bytes rather than from CAST, so
    an implementation and a comparator agreeing on the same wrong hex still
    fail.

    **The engine's own answers were already PostgreSQL's**, which is why the
    fix moved the wire and the CAST and nothing else: comparison and
    ordering over BYTES are bytewise, an unknown-typed literal beside a
    BYTES column is read as its bytes (`b = 'hi'` finds the same row on both
    engines), `LENGTH`/`OCTET_LENGTH` count octets, and GROUP BY, DISTINCT,
    joins and NULL handling all agree. That is a finding of the coverage
    below, not an assumption.

    **The coverage hole is closed, and it found a second defect.** `bytea`
    appeared NOWHERE in `benchmarks/` or `internal/oracle/` — the pg-oracle
    ran only over the TPC-H fixture, which has no BYTES column, so
    `EngineSemantics` had never compared the value and `WireProtocol` had
    never compared the OID. The fixture now has a `bytea_probe` table
    (`postgres_oracle_test.go`, PG side `bytea`, wadjet side BYTES) holding
    the empty value, four NULs, the invalid-UTF-8-with-embedded-NUL value
    above, ASCII text, single high bytes, a prefix pair, and NULLs. Over it
    run 33 gated semantics entries (plus the one pin below), 10 wire-metadata
    entries and 2 in the wire error list; the DuckDB arm loads the SAME rows
    — not a second copy of "the same" fixture — for 13 more BLOB entries. Standing that
    up immediately failed a query the type matrix had never asked:
    `ResolveColColFilterKernel`'s string arm listed STRING, IPv6 and UUID
    but not BYTES, and because two BYTES columns share a TypeID the
    mixed-type row-at-a-time fallback did not apply either — so
    `WHERE b_val = b_other` came back as "could not resolve kernel", the
    identical shape #477 found for two DECIMALs. This is the coverage-matrix
    argument in concrete form: the gap was not in what the gates ASSERT but
    in which types they had a value for.

    **What is not fixed, and is pinned rather than forgotten.** A BYTES
    LITERAL does not exist: `b_val = '\x6869'` in a hand-written statement
    compares six characters, where PostgreSQL's byteain reads two bytes
    (#582, `BytesEqHexSpelledLiteral`). The Bind path escapes this only
    because it has a declared parameter OID to decode against. And the
    scalar function layer has no BYTES notion at all — every function reads
    its operand through `expr.toString`, so `UPPER(b)` ANSWERS where
    PostgreSQL raises 42883, and `b || b` returns TEXT where PostgreSQL
    returns bytea, which puts the raw-bytes-under-OID-25 hazard back in
    through a derived value (#583, `ByteaTextFunctionOverBytes` and
    `ByteaConcat`). `OCTET_LENGTH` over bytea declares float8 for #530's
    reason, unrelated to this type.

    **A TIMESTAMP renders its INSTANT through CAST and LIKE.** (Amended
    2026-09-02, #544.) The paragraph above says the text is "the value's own
    printed form — `Vector.GetValue`'s rendering … epoch MILLISECONDS for a
    TIMESTAMP". That sentence recorded the defect as the rule. `GetValue`
    keeps a TIMESTAMP as its raw epoch-ms int64 deliberately — it is also
    the GROUP BY key, the aggregate and window spill row encoding, the
    window comparator, and the row map an UPDATE re-ingests — but that box
    is a COMPUTE form, not a display one, and pgwire has converted it to
    PostgreSQL's `timestamp` on the way out since #321. So one column
    answered `2023-11-14 22:13:20` when projected over the wire and
    `1700000000000` when CAST, on one connection. Both text sites now render
    from the DECLARED type instead: `expr.boxedTextOperand` (CAST and the
    row-path LIKE) and `kernel.likeTextRenderer` (the single-process LIKE
    kernel, whose default arm was that `GetValue` call — which is why
    `c_ts LIKE '2023%'` was false on the single-process path and true on the
    DAG). `Vector.GetValue` is unchanged, and so is the embedded API's int64
    box for a projected TIMESTAMP: that is a third question — what the Go
    API hands back — bounded by the same five consumers, and not this one.

    **A SUB-SECOND timestamp prints the MINIMAL fraction, as PostgreSQL
    does.** (Recorded as a divergence 2026-09-02 by the review of #544;
    CLOSED 2026-09-04.) The server prints `2023-11-14 22:13:20.5` for `.500`
    and `.25` for `.250` — verified live — and `batch.FormatTimestamp` printed
    three digits always. It is the same function pgwire's send path calls, so
    the padding was what a CLIENT saw and not only what CAST answered; one
    change to that one renderer closed both doors, which is the property #544
    is about. `batch.TestFormatTimestamp` carries six fraction cells including
    the ones a trim could break (`.001`, `.999`, and every whole second, so a
    trim that left a bare point behind fails), and `pgTextOf` in
    `wadjet/cast_string_rendering_test.go` still answers PostgreSQL's rule
    written from the server rather than from this function, so the sweep's
    reference stays independent of the implementation.

    **DURATION is NOT fixed, and the reason is a type-model gap rather than
    an omission.** `CAST(c_dur AS STRING)` answers the raw nanosecond count
    (`1000000`) where PostgreSQL's `interval` — the type wadjet's DDL accepts
    as a synonym for DURATION — answers `00:00:00.001`. It is left alone
    because wadjet has no canonical TEXT for the type for a CAST to agree
    with: `pgTypeOID` maps DURATION to OID 25 (text) and the send path writes
    the nanosecond count, `docs/sql-reference.md` documents the type as
    "integer nanoseconds", and the INSERT coercion reads one. Rendering only
    the CAST would make the same column answer two ways again — precisely the
    defect this amendment closes for TIMESTAMP, re-opened one type over.
    Closing it means choosing DURATION's text form ONCE and moving the wire
    (OID 1186), the send path, the row reader, the INSERT coercion and the
    reference doc together. That is a lead, not a CAST-site fix.

12. **A set operation's result type is the COMMON type of its arms, and every
    arm is MOVED into it — never reinterpreted.** (Added 2026-08-25, #533.)

    The numeric ladder, verified against live postgres:17-alpine and pinned
    by `physical.TestSetOpWidenLadder`:

        INT32 → INT64 → DECIMAL → FLOAT64

    `numeric UNION ALL bigint` is **numeric** (an integer converts to numeric
    implicitly and not back); `numeric UNION ALL double precision` is
    **double precision** (float8 is the PREFERRED type of PostgreSQL's
    numeric category). Arm ORDER changes neither, there or here. Nothing
    outside the numeric family widens: rendering a number as text to make two
    arms line up would answer a different query.

    **A pair with NO common type is REFUSED at PLAN time, with SQLSTATE
    42804 and PostgreSQL's own sentence, on both execution paths and in
    either arm order.** (Amended 2026-09-04, #648.)

        UNION types numeric and text cannot be matched: result column "a"

    `UNION` there whatever the `ALL`, and `INTERSECT` / `EXCEPT` under their
    own names; measured live on 17.11 for numeric ∪ text, bigint ∪ text,
    double precision ∪ text, boolean ∪ bigint, uuid ∪ text, timestamp ∪ text
    and text ∪ bytea. Before this the DAG refused with a message of its own
    carrying no SQLSTATE and the single-process path let the arms meet at
    RUNTIME: `text UNION ALL numeric` answered a STRING column of rendered
    decimals silently, the same pair the other way round failed mid-execution
    with 22P02 on the first row of text that is not a number, and `numeric
    INTERSECT text` answered one row by comparing decimals against text as
    text. The column NAME rides after PostgreSQL's sentence rather than
    inside it: a set operation's arms correspond by POSITION, and the
    position is the localization PostgreSQL's own message does not carry.

    **The question asked is PostgreSQL's ALGORITHM, not a list of pairs.**
    (Rewritten 2026-09-04, round-2 review of #648.) The first draft of this
    refusal exempted a pair only when the numeric ladder widened it, when the
    two mapped to the same `pgTypeName`, or when it was DATE/TIMESTAMP — and
    that hand-written list refused pairs PostgreSQL MATCHES: thirteen ordered
    column pairs and eight literal idioms that ANSWERED on the single-process
    path (which is also the coordinator's local fast path, so the default for
    a small query) became a plan-time 42804. The rule is the documented one
    ("Type Conversion → UNION, CASE, and Related Constructs"):

    1. every arm the same type → that type;
    2. an arm whose select item is an UNKNOWN-typed literal — a quoted string
       or `NULL` — has no type of its own and takes the others'. `SELECT
       c_ipv4 … UNION ALL SELECT '10.0.0.9'` is inet, `c_mac ∪ 'aa:bb:…'`
       macaddr, `c_date ∪ '2010-01-01'` date, `c_uuid ∪ '000…'` uuid,
       `c_dec ∪ '0'` numeric, `'1.5' ∪ numeric` numeric, `c_ipv4 ∪ NULL`
       inet, and two unknown literals resolve to text — all measured live on
       17.11;
    3. arms whose types are in different CATEGORIES have no common type, and
       the statement is 42804 in either order and for the whole node;
    4. within a category the result is the type every arm implicitly casts to.

    **The unknown-literal rule is applied at every door that DECLARES the
    column, not only at the one that executes it.** (Amended 2026-09-04,
    round-3 review of #648.) There are three: the stage DAG's arm projection
    (`reconcileSetOpArmTypes` stamps the resolved type on the literal arm's
    spec), the DAG's declared output schema (`setOpDeclaredOutputSchema` skips
    unknown arms in its fold) and the single-process path's schema
    reconciliation (`setOpResolveUnknownLiteralArms`, ahead of
    `unifySetOpSchemas`). With the rule on the first two only, the same
    statement declared two different types depending on which door answered —
    which is the fast-path byte threshold, not anything the query says:

        SELECT '1.5' AS v FROM decpair WHERE id = 1 UNION ALL SELECT a FROM decpair
          single : STRING       (OID 25)    rendering 1.5
          DAG    : DECIMAL(9,2) (OID 1700)  rendering 1.50
          PostgreSQL 17.11: numeric

    A fourth door types a NESTED set-operation arm
    (`setOpNodeResultTypes`), which is what three arms with the literal FIRST
    lower to: without the mask it counted the literal's STRING as a type of its
    own, returned "unknown" for the whole inner node, and the DAG then REFUSED
    a query the single-process path answered.

    **A QUOTED literal whose resolved type cannot be built from TEXT is
    refused at plan time with 0A000.** (Amended 2026-09-04, round-3 review of
    #648.) PostgreSQL parses such a literal with the resolved type's own input
    function, so `c_ts ∪ '2010-01-01 00:00:00'` is timestamp, `c_bool ∪ 'true'`
    boolean and `c_port ∪ 'notaport'` is 22P02 — all measured live on 17.11.
    Wadjet's literal arm produces the constant as a STRING box and that box
    reaches the result column's vector unchanged, so this works for exactly the
    types whose vector has a text arm. `batch.VectorAcceptsText` names them —
    STRING, BYTES, IPV4, IPV6, CIDR, MAC, UUID, DATE, DECIMAL — and it is held
    to what `SetValue` actually does by
    `batch.TestVectorAcceptsTextIsWhatSetValueDoes`, which writes a string into
    a vector of every one of the 22 types and compares; a list somebody keeps
    by hand is what cost this rule a review round already.

    The other nine — BOOL, INT32, INT64, FLOAT32, FLOAT64, TIMESTAMP, PORT,
    PROTOCOL, DURATION — failed the #361 silent-write guard with NO SQLSTATE
    ("batch: cannot store string into TIMESTAMP vector", which the pgwire door
    reports as XX000, "the server broke") mid-execution on the single-process
    path, and on the stage DAG after THREE retries of a deterministic parse
    failure. They are refused at PLAN time now, with the 0A000 the carrier gap
    takes and for the same reason: PostgreSQL answers the query and this engine
    does not yet. A bare NULL is unaffected — it has no text to parse and every
    vector takes one — and so is an UNQUOTED literal.

    Closing it means giving the literal its resolved type at PLAN time rather
    than at the vector: parse the text into the target type's own box in the
    arm's rows (the single path, beside `setOpLiteralRows`) and rewrite the
    arm's projection expression to the target's literal spelling (the DAG,
    beside `reconcileSetOpArmTypes`' stamp) — which also makes unparseable text
    22P02 the way PostgreSQL reports it, rather than a refusal of the whole
    statement.

    **The result's TYPMOD is the typed arm's, where PostgreSQL's is
    unconstrained.** `'1.5' ∪ numeric(9,2)` is `numeric` with typmod −1 there,
    so PostgreSQL renders the literal `1.5` beside the column's `2.00`; wadjet
    declares DECIMAL(9,2) and renders `1.50`. A wadjet DECIMAL vector has ONE
    scale — "unconstrained" is not a carrier it has — and the alternative,
    resolving one arm's scale away, is #532's truncation. It is the same answer
    `c_dec ∪ '0'` already gives with the typed arm on the LEFT. A declared-TYPMOD
    divergence, listed here with the declared-WIDTH one below.

    **The category table**, wadjet's declared type to the category PostgreSQL
    puts its WIRE type in, measured live:

    | category | wadjet types | note |
    |---|---|---|
    | N numeric | INT32 INT64 FLOAT32 FLOAT64 DECIMAL **PORT PROTOCOL DURATION** | PORT/PROTOCOL declare int4 and DURATION int8 (#834), so `c_port ∪ c_i64` is bigint ∪ integer there and answers |
    | S string | STRING | |
    | B boolean | BOOL | |
    | D datetime | DATE TIMESTAMP | `date ∪ timestamp` → timestamp, both orders |
    | I network | IPV4 IPV6 CIDR | `inet ∪ inet` → inet, `inet ∪ cidr` → inet, both orders, values preserved |
    | U other | BYTES UUID MAC ARRAY ROW MAP VECTOR | PostgreSQL puts bytea, uuid and macaddr in one category too, and with no implicit conversion between them its step 6 still fails — `uuid ∪ bytea` is "UNION could not convert type bytea to uuid" — so each matches only itself here |

    Two members of the int4 family that are not the same TypeID (a PORT beside
    an INT32, a PORT beside a PROTOCOL) resolve to **bigint** here where
    PostgreSQL resolves int4. No value moves — no integer this engine stores in
    an int4 carrier is outside int8 — and the engine has no CAST spelling that
    produces an INT32 carrier, so declaring int4 would put the type on a box
    that is not one. A declared-WIDTH divergence, listed here with the rest.

    **A pair PostgreSQL RESOLVES that this engine cannot CARRY is refused
    LOUDLY, with SQLSTATE 0A000 and never with 42804.** The refusal is this
    engine saying what it does not do yet — `feature_not_supported`, the class
    the order-by-unselected-aggregate refusal and ADR-0021 §1e's container
    refusals already use — because PostgreSQL ANSWERS the query. Unclassified it
    reached a client as XX000, which says the server broke.

    The pairs, in full — there are 14 ordered ones. The list is COMPUTED from
    the predicate by `physical.SetOpCarrierGapPairs`, and
    `coordinator.TestTheCarrierGapListIsTheCodes` READS THIS PARAGRAPH: it
    parses the count in the sentence you are reading and the type names in the
    table below, and fails when either disagrees with the computed set. An
    earlier draft named two pairs while the code refused twenty, and the draft
    after it said EIGHT while its own table listed fourteen — a paragraph a test
    does not read is a paragraph nothing keeps true.

    | pair | PostgreSQL resolves | why there is no carrier |
    |---|---|---|
    | DATE ∪ TIMESTAMP, both orders | timestamp | no DATE → TIMESTAMP promotion in the arm coercion |
    | IPV4 ∪ IPV6, IPV4 ∪ CIDR, IPV6 ∪ CIDR, all orders | inet | no inet-family carrier that holds two of them |
    | PORT ∪ DECIMAL, PROTOCOL ∪ DECIMAL, DURATION ∪ DECIMAL, both orders | numeric | DecimalCoercion reads an INT32/INT64 unscaled carrier and setOpDecimalTarget has no digit count for these |

    The INTEGER and FLOAT rungs DO carry PORT, PROTOCOL and DURATION — an
    earlier draft refused REAL beside them, which turned three shapes that
    answered PostgreSQL's `real` rows into hard errors. The single-process path used to
    ANSWER them, and the answer was CORRUPT: measured, `c_date ∪ c_ts` rendered
    every timestamp as `-2207656-04-19`, and `c_ipv4 ∪ c_ipv6` and
    `c_ipv4 ∪ c_cidr` rendered every row of the second arm as `0.0.0.0`. Both
    paths refuse now, with a message naming the two CARRIERS and saying that
    PostgreSQL resolves the pair and wadjet does not yet — silent wrong → loud,
    and never PostgreSQL's SQLSTATE for a query PostgreSQL answers. Closing it
    means a real DATE → TIMESTAMP promotion and an inet-family carrier, which
    is a typing feature and not a repair.

    **PostgreSQL's refusal wins over wadjet's, and every column is resolved
    before either is reported.** A column with no common type is a fact about
    the QUERY; a column whose common type this engine cannot carry is a fact
    about this engine. Reporting whichever the walk met first made the
    disposition depend on column ORDER — `SELECT c_date, c_dec … UNION ALL
    SELECT c_ts, c_str …` failed mid-execution with 22P02 where PostgreSQL is
    42804, while the same query with its two columns swapped refused at plan
    time.
    **When the common type is DECIMAL, the (p,s) is:**

    - **scale = max over the arms.** The only choice that moves no value; a
      narrower one DROPS digits the wider arm holds.
    - **precision = max over the arms of (precision − scale), plus that
      scale**, capped at 38. Rebuilt from the widest INTEGER part rather than
      taken as max(precision), because max(precision) is not a bound on the
      widened values: `DECIMAL(18,2)` alongside `DECIMAL(9,4)` needs 16
      integer digits at scale 4 — 20 — where max(precision) would declare 18
      and hand the parquet writer a leaf too small for its own values
      (ADR-0018 §4's encoding rule keys off precision). For every pair whose
      integer parts order the same way as their precisions the two rules
      agree, `(9,2)/(18,4)/(38,10)` included.
    - An INTEGER arm contributes its whole range's digits (10 for INT32, 19
      for INT64) at scale 0.

    **The arms are moved, and that is the whole point.** A DECIMAL value is
    an unscaled integer plus the column's declared scale (ADR-0018 §4), and on
    the stage DAG the two travel apart: each arm's task writes its own `.wshf`
    file carrying its own scale in the header, and the task that reads several
    such files writes ONE file under the schema of the first batch it saw.
    Reconciling only the `TypeID` reconciles nothing for two DECIMALs — they
    ARE the same TypeID — so the wider arm's unscaled integer was read at the
    narrower arm's scale and every value from it came back 100× too large,
    silently (#533). `exec.DecimalCoerce` multiplies the carrier instead, in
    the union arm's own fragment, before the rows meet. A CAST cannot do this
    job: the cast evaluator's DECIMAL destination produces a float64, which is
    the precision loss the exact carrier exists to prevent.

    **A value with no exact carrier at the output scale is an ERROR.** Same
    rule and same reason as SUM's overflow in item 9: a wrapped value is a
    different number wearing the right type, and nothing downstream can see
    that it is wrong. The bound checked is the DECLARED PRECISION (`10^p`),
    not the Int128 carrier's — they are different bounds and only the first is
    the type. A DECIMAL(38,2) whose unscaled value lands in `[10^38, 2^127-1]`
    fits the carrier and not the declaration, and admitting it would write a
    number the declared type cannot hold into a column the parquet writer
    sizes from that precision (ADR-0018 §4).

    **The 38-digit cap is a RANGE REDUCTION, and it costs answers** (#552).
    The cap can pull the output precision below what an arm's own values need,
    so a value BOTH arms held before the union becomes a hard failure after
    it: `DECIMAL(38,0)` holding `10^30` beside `DECIMAL(11,10)` resolves to
    `DECIMAL(38,10)`, and `10^30` at scale 10 needs `10^40`. PostgreSQL's
    numeric is unbounded and never reaches this. Worse, a filter or a `LIMIT`
    above the union does NOT rescue the query — the coercion runs in the arm's
    own fragment, ahead of the post-filter and ahead of the Singleton `LIMIT`
    stage — so `… WHERE v < 100` over that union fails on a row it does not
    want. This is the honest side of a 128-bit carrier and it is RECORDED
    rather than hidden; `TestSetOpDecimalCapIsARangeReduction` pins all three
    shapes so a future widening shows up as that test failing.

    **A computed DECIMAL expression is typed DECIMAL** (#555, closed across
    ADR-0024 and #695/#724). `d + d`, `COALESCE(d, d)`, `CAST(d AS DECIMAL)`
    and now arithmetic OVER a choice (`COALESCE(d, 0) * 2`) all declare and
    execute as exact fixed-point. The paragraph that stood here recorded the
    opposite and was right when it was written: those expressions resolved to
    FLOAT64 and answered float-rounded values where PostgreSQL answers
    `numeric`, and `COALESCE` over two DECIMALs failed outright at #361's
    store guard.

    **What is left is the CARRIER, and it is a recorded divergence rather
    than a gap** (ADR-0024 item 1). PostgreSQL's `numeric` is unbounded and
    has `NaN`/`±Infinity`; wadjet's is 38 digits of Int128 with neither. So a
    fold that lands on DECIMAL and meets a value outside it —
    `GREATEST(numeric(15,2), '1e39')`, or the same with `'NaN'` or
    `'Infinity'` — raises 22003 where PostgreSQL answers. 84 such composites
    are LISTED, with PostgreSQL's answer beside each, in
    `coordinator.nfCarrierRefusals`, and the gate asserts each still refuses,
    so the list fails if the carrier ever grows. Most of them appeared to
    agree with PostgreSQL before #724 for the wrong reason: the quoted
    literal made the whole call declare `text`, so its own characters went out
    unread.

    **The dedup key is the columnar one.** UNION, INTERSECT and EXCEPT decide
    membership by EQUALITY, so their key must agree with the comparator. On
    the DAG they already do — the operation lowers to a hash aggregate whose
    DECIMAL key is `batch.AppendDecimalKey` at the column's scale (#474), the
    canonical minimal-scale form — and after this the arms reach it at one
    scale anyway. The single-process path keys a boxed row by its RENDERED
    TEXT and needs its own fix (#499); the two paths are held to one answer by
    `internal/coordinator/setop_decimal_scale_two_path_test.go`.

    **Where an arm cannot be resolved, the query is REFUSED at plan time,
    naming the column.** (Amended 2026-08-29, #551.) Leaving every arm as
    written was the earlier answer and it is a SILENT WRONG ANSWER: an earlier
    draft said `shuffleWriter.writeChunk`'s scale check turns that residual
    into a failed task, and it does not — that check sees a SINGLE writer
    handed two scales, while in a union stage each arm writes its own
    consistent file and the reinterpretation happens in the downstream stage
    that reads several of them (ADR-0010 carries the corrected statement).

    The refusal covers TWO conditions, because the resolution can fail in two
    places: an arm typed DECIMAL whose `(p,s)` nothing resolved (#458's
    "unconstrained" sentinel is the reachable one), and an arm with NO resolved
    type at all sitting beside a DECIMAL arm — which is the condition the SQL
    shapes actually take, and which the first draft of this amendment did not
    cover. Both are witnessed end-to-end by
    `TestSetOpUnresolvableDecimalArmIsRefused`; an assertion over a hand-built
    arm state is not evidence that a refusal is reachable.

    **The single-process path ANSWERS what the DAG refuses here.** It re-reads
    each row's rendered text under a `max(scale)` fallback, which moves no
    value on that path, so an unresolvable arm is a divergence in WHICH
    ANSWER EXISTS, not in what the answer is. PostgreSQL answers too. That is
    the price of the refusal and it is recorded rather than hidden.

    Most of the shapes that used to reach it are resolved instead.
    `physical.setOpArmDecls` is the set operation's own view of an arm's
    columns:

    - A JOIN keeps a PER-SIDE answer. `inputColTypes` / `inputColDecimal`
      merge the two sides and delete any name they disagree about — right for
      a TypeID and exactly wrong here, since that disagreement IS the fact
      being reconciled — so each side's columns are keyed under its own
      relation names and the QUALIFIED spelling the projection carries
      resolves against the right one (#551). A DERIVED TABLE or CTE on one
      side keys under its SCOPE name the same way: without that its Project
      emitted only bare names, the merge deleted the contested one, and
      `SUM(v)` over `(SELECT s.dx FROM (SELECT id, dx FROM b) s JOIN a …
      UNION ALL SELECT dx FROM a)` answered 5151.0000 where PostgreSQL
      answers 102.0000.
    - A PROJECT is descended INTO, so a DERIVED-TABLE arm resolves through the
      names its subplan emits (#554), with `setOpArmComputedSource` rewriting
      a forwarded COMPUTED column into the expression that builds it. A nested
      SET OPERATION behind such a Project reads its own reconciled result
      types.
    - A ROW FIELD PATH resolves through the FIELD's declaration on ADR-0022's
      terms, and the spec carries the type because nothing downstream resolves
      a field path by name — it is materialized the way a computed expression
      is.
    - A numeric LITERAL arm takes its spelling's `(p,s)`. PostgreSQL types a
      constant numeric whenever it carries a decimal point OR an exponent
      (`1.23456`, `1.`, `1e2`, `1.5e-2` — verified live against 17.11 with
      `pg_typeof`; there is no float8 constant syntax) and types an INTEGER
      constant numeric once no integer type holds it. The arm's expression is
      REWRITTEN to the literal's plain decimal TEXT, because the evaluator
      folds a numeric literal into a float64 and `1234567890123456.78` is not
      one: declaring DECIMAL over that box would put an exact type on an
      already-rounded number. `litDeclType` is scoped to the arm, because the
      type of a literal inside an ARITHMETIC expression is decided with
      ADR-0024 item 3's decimal arithmetic. The single-process path builds the
      literal arm's vector from the declared-type layer and so still resolves
      float8 there — the values agree wherever float64 holds them, the wire
      OID does not, and closing that half is the literal `(p,s)` work in
      `expr`.

      **Amended 2026-09-04 (#665, #683): the single-process path resolves it
      too.** The set-operation adapter restates a literal column as the DECIMAL
      its spelling names and replaces its box with the literal's plain decimal
      TEXT before the arms meet, which is the shape every reader below already
      expects from a DECIMAL — `batch.FromRowsChecked` parses it at the
      resolved scale and `setOpCheckedDecimalText` range-checks it, so this
      path also raises the 22003 the DAG raises for a literal the union's own
      type cannot hold (item 7). `SELECT a FROM t UNION ALL SELECT
      1234567890123456.78` came back exact from the DAG and
      `1.2345678901234568e+15` here; both are exact now. What is still float8
      is a literal typed by something OTHER than the arm — inside a DERIVED
      TABLE, in arithmetic — which is the declared-type layer's rule and is
      pinned in `coordinator.TestANumericLiteralSetOperationArmIsExactOnBothPaths`.
      A literal NO DECIMAL this engine declares can hold — more than 38 digits
      — is **22003** beside an arm whose values are exact, and not the float8
      it used to silently fold to: `SELECT a FROM t UNION ALL SELECT
      123456789012345678901234567890123456789.5` answered
      `1.2345678901234568e+38`, a rounded number under an exact type, on both
      paths. Beside a FLOAT arm PostgreSQL resolves double precision and that
      float8 IS the answer, so the refusal is scoped to an exact result
      (round-2 review of #683).

    **A computed DECIMAL expression does NOT reach the refusal**, and saying it
    did was wrong in both directions. `d + d` and `COALESCE(d, d)` are declared
    FLOAT64 by the arithmetic rule that has not landed yet, so the pair
    resolves FLOAT64 and the query answers with float-rounded values;
    `CAST(d AS DECIMAL(12,3))` is declared STRING by `inferCastType` and meets
    the LADDER's refusal ("the arms disagree on the type … and neither widens
    into the other"), not this one. Both are #555's typing gap, and the rung
    is not what has to change.

    **Two carrier properties are deliberate divergences, not defects.** A
    wadjet DECIMAL column has ONE declared scale, so the narrow arm's rows
    render with the unified scale's trailing zeros (`12.7500` beside
    `12.7501`) where PostgreSQL's variable-scale numeric prints `12.75` — the
    same number, the same row set, the class item 9 already records for AVG's
    digits. And PostgreSQL declares a cross-scale set operation's numeric
    result UNCONSTRAINED on the wire (`\gdesc` says `numeric`, typmod −1,
    where a single-arm `SELECT` of the same column says `numeric(9,2)`);
    wadjet still declares a real (p,s) there. That is wire METADATA only and
    is tracked separately (#542), not a value difference.

    **The single-process path does not yet resolve two different TypeIDs**
    (#541): it builds the result under the FIRST arm's schema, so an integer
    arm under a DECIMAL first arm reads as an unscaled carrier (1 becomes
    0.0001) and a DECIMAL arm under a FLOAT64 first arm fails the store
    outright. `unifySetOpSchemas` should delegate to `setOpWiden` and
    `setOpDecimalTarget` rather than keep its own rule, so the two paths
    cannot drift on what the output type IS. It also SATURATES rather than
    failing when the value it re-reads has no Int128 at the result scale
    (#553) — `batch.DecimalTextAt` returns `Int128Max` with a `Sat` flag the
    caller discards, so `10^30` comes back as
    `17014118346046923173168730371.5884105727`. Saturation is right where it
    was built, in #462's COMPARISON path, where an out-of-range literal
    genuinely orders above every value the column holds; as a stored VALUE it
    is a lie, and the rule above (a value with no exact carrier is an error)
    is what the value-producing callers owe.

13. **A QUOTED literal meeting a NUMERIC column is coerced with THAT COLUMN'S
    OWN INPUT FUNCTION — one rule, parameterized by the column's TypeID, at
    every comparison site and on both execution paths.** (Added 2026-08-29,
    #646; closes #634 for the integer family.)

    PostgreSQL types an unknown-typed literal FROM the operand it meets and
    then runs that type's input function over the text. There is no widening
    anywhere in it. Read off `EXPLAIN VERBOSE` on postgres:17-alpine over a
    `real` column:

        r = '3.1'                  ->  (r = '3.1'::real)
        r IN ('3.1')               ->  (r = '3.1'::real)
        r IN ('3.1','7.1')         ->  (r = ANY ('{3.1,7.1}'::real[]))
        r BETWEEN '3.1' AND '100'  ->  (r >= '3.1'::real) AND (r <= '100'::real)
        CASE WHEN r < '3.1'        ->  (r < '3.1'::real)
        CASE r WHEN '3.1'          ->  CASE r WHEN '3.1'::real
        GREATEST(r, '3.1')         ->  GREATEST(r, '3.1'::real)
        NULLIF(r, '3.1')           ->  NULLIF(r, '3.1'::real)
        r IS DISTINCT FROM '3.1'   ->  (r IS DISTINCT FROM '3.1'::real)

    **That is the OPPOSITE direction from item 8's unquoted literal, and both
    are PostgreSQL's.** An unsuffixed decimal constant is `numeric`, which has
    no `real <op> numeric` operator to resolve to, so the comparison goes
    through float8 and the COLUMN moves (#631). A quoted one is `unknown`,
    which is coerced directly, so the LITERAL moves. Over a column holding
    `real(3.1)`, `r = 3.1` selects nothing and `r = '3.1'` selects that row —
    one number, two spellings, two predicates. Both spellings are in the
    oracle corpus side by side for exactly that reason, and the two kernels
    stay separate: `kernel.ResolveFilterKernel` picks between them on the
    constant's Go box, a `string` for the quoted spelling and a float64/int64
    for the numeric one. Reading the box is right HERE and nowhere else — item
    8's caution is about a VALUE's ORDER, where a box cannot tell a DECIMAL
    from a STRING; "which literal did the user write" is the one thing the box
    does carry and the declaration does not.

    **What this replaces is a silent zero.** `kernel.toFloat64` has no string
    arm at all, so every quoted constant against a FLOAT column read as 0.0:
    `real = '3.1'` matched the row holding 0.0, `real = 'abc'` matched it too,
    `real IN ('3.1','7.1')` matched nothing, and `f > '-Infinity'` asked
    `> 0.0` and dropped every negative row. That is #463's failure mode on the
    type family #536 (integers) and #574 (BOOL) had already closed, and the
    boxed sites — `NULLIF(int_col,'abc')`, `int_col IS DISTINCT FROM 'NaN'`,
    `CASE WHEN int_col < 'NaN'`, `GREATEST(int_col,'NaN')` — answered every row
    for the integer family too, because `expr.refuseArm` and
    `extremumRefusal` tested `batch.TypeDecimal` alone.

    **The accept-sets, live from postgres:17-alpine.** They differ, and every
    difference is observable:

        text      bigint            real                numeric
        '3.1'     22P02             3.1                 3.1
        '1_000'   1000              22P02               1000  (wadjet 22P02, #634)
        '0x1A'    26                26                  26    (wadjet 22P02, #634)
        '0o17'    15                22P02               22P02
        '0x1p3'   22P02             8                   22P02 (wadjet 22P02)
        'NaN'     22P02             NaN                 a BOUND (ADR-0024 item 6)
        '+NaN'    22P02             NaN                 22P02
        '1e400'   22P02             22003               a very large number
        '1e39'    22P02             22003               a number
        '7e-46'   22P02             22003 (underflow)   a number
        ''        22P02             22P02               22P02

    - **INTEGER** is PostgreSQL 16's `pg_strtoint*`: C whitespace trimmed, an
      optional sign, `0x`/`0o`/`0b` radix prefixes, underscore separators
      between digits, and leading-zero DECIMAL (`'007'` is seven, not fifteen).
      Neither Go base matches it — base 10 refuses the radix forms and base 0
      reads `'017'` as octal — so `kernel.parseIntText` is a dedicated parser.
      Its one asymmetry is PostgreSQL's: an underscore may not be first in a
      decimal (`'_1000'` is 22P02) but MAY be first after a radix prefix
      (`'0x_1A'` is 26), because PostgreSQL's own source puts the
      "not first" check in the decimal branch alone. Refusing these was a
      PG-superset regression (#634); it is closed.
    - **FLOAT** is `float4in`/`float8in`, which are `strtod` plus PostgreSQL's
      special spellings. So C99 HEX floats are values (`'0x10'` is 16,
      `'0x1p3'` is 8, `'0x.8p1'` is 1) and underscores are NOT (`'1_000'` is
      22P02 there even though the integer and numeric inputs take it).
      Go's `ParseFloat` disagrees on both and on a third point — it is silent
      about UNDERFLOW, answering a plain 0 for `'1e-400'` where PostgreSQL
      raises 22003 — so `kernel.FloatLitText` supplies the binary exponent Go's
      hex syntax requires, refuses underscores, and decides underflow from the
      DIGITS. `real`'s range boundary is its smallest DENORMAL, PostgreSQL's
      own: `'1e-45'` is a value and `'7e-46'` is 22003.
    - **DECIMAL** keeps its own grammar (ADR-0024 item 6) and is the only one
      with no RANGE failure: a literal past the Int128 carrier SATURATES into
      its place in the order rather than erroring (#462).

    **Two SQLSTATEs, and they are different answers.** 22P02 for text that
    names no value, 22003 for a number the type cannot carry. The wording
    differs by family too, and it is reproduced rather than tidied: the integer
    inputs prefix the literal (`value "3000000000" is out of range for type
    integer`) and the float ones do not (`"1e400" is out of range for type
    real`). A QUOTED literal's message names its TEXT VERBATIM, where a
    numeric->real cast names the numeric's DIGITS — so `real IN ('1e40',3.1)`
    says `"1e40"` and `real IN (1e40,3.1)` says the forty-one digits, both
    verified live.

    **One predicate serves every site**, which is the property rather than the
    coverage: `kernel.QuotedLitStatus(typ, text)` is read by the plan-time
    refusal (`physical.refuseLiteralForType` via `expr.RefuseNumericLiteral`),
    the vectorized kernel's scalar and IN arms, the row-at-a-time
    `exec.ColumnCompareLit`, the boxed sites' refusal masks
    (`expr.litRefusalMask`), and the row-group prune
    (`kernel.StatsDomainValue`, which now converts a quoted literal at the
    column's own width instead of handing the prune layer a Go string it
    declines). A query refused at one site and answered at another is the
    two-path defect class the refusal exists to close.

    **The refusal is not a property of a row.** PostgreSQL coerces at parse
    analysis, so an unreachable conjunct (`r_key < 0 AND r_val = 'abc'`) and
    one that only ever meets NULLs still error. That is why the plan-time
    binder carries it and the runtime refusals are the backstop for the shapes
    the binder cannot prove — #517's rule, one type family wider. NULLIF joins
    GREATEST/LEAST at that binder, because its equality test is the same
    question their ordering is.

    **Inside a COMPOSITE the type is the CALL'S, folded once over every
    argument.** (Added 2026-08-29 from this item's own review.)
    GREATEST/LEAST, CASE and COALESCE resolve one type through
    `select_common_type` and coerce the unknown-typed literal to THAT, so
    `GREATEST(bigint, '3.1', double precision)` is a double comparison and
    answers where the bigint input function would raise 22P02. Three things
    have to read that one fold or they disagree with each other: the REFUSAL
    (`extremumArms.checkRefusal`), the COMPARISON of every (best, candidate)
    pair — which is pairwise and must NOT use the pair's own types — and the
    VALUE, because the argument that wins comes back at the call's type and a
    quoted literal arrives as a Go string. Reading each argument's own type
    instead produced all three failures at once: a PG-superset regression
    (`GREATEST(k, '3.1', d)` refused), a silent width error
    (`GREATEST(r, '16777217', d)` read at real width), and a crash (the
    literal's string stored into a FLOAT64 vector).

    A kind that has NO declared type must answer "no rule" rather than fall
    back to the ROW'S BOX. `COALESCE(real_col, 0)` boxes an int64 on the row
    where the column is NULL and a float64 on every other, so a box-driven
    reading coerced the literal with the INTEGER input function on one row and
    the double one on the next — `COALESCE(r,0) = '3.1'` raised 22P02 with the
    NULL row present and answered zero rows without it, where PostgreSQL
    resolves real once and answers one row. Where the fold cannot be made —
    an argument this layer cannot type — only a literal EVERY numeric type
    refuses may raise, because a partial fold is a LOWER BOUND and a lower
    bound is what refuses at the wrong width.

    **DECIMAL is a rung of that fold, and a NUMERIC CONSTANT arm carries its
    own type into it.** (Added 2026-08-29 from this item's second review.) The
    ladder is `select_common_type`'s, the same one item 12 pins for set
    operations, with `float4` where live `pg_typeof` puts it:

        INT32 < INT64 < DECIMAL < FLOAT32 < FLOAT64

    Leaving DECIMAL off it was one defect with two faces. A composite holding a
    DECIMAL column could not fold at all, so each pair kept its own type and
    `GREATEST(bigint, '3.1', numeric)` asked BIGINT's input function for a
    literal PostgreSQL reads as numeric — a refusal where PostgreSQL answers.
    And a NUMERIC-typed CONSTANT arm — an unsuffixed literal with a point or an
    exponent, which PostgreSQL types `numeric` — made the join answer "no kind
    at all", so `COALESCE(real_col, 0.0) > '9'` fell through to the generic
    comparison and ordered the rendered number against the literal BYTEWISE.
    The two predicates the fold has to keep apart are visible in one pair:
    `COALESCE(real, 0.0)` is REAL and `COALESCE(bigint, 0.0)` is NUMERIC.

    Only the operand FACING the quoted literal is retyped to the fold. A pair
    of typed operands already has each side's own rule, and retyping both sent
    an integer box and a DECIMAL's text to the two-DECIMALs comparison, which
    needs two strings — so it declined and the extremum picked its winner by
    byte order instead.

    **The fold decides the READING, not only the literal's grammar.** (Added
    2026-08-30 from this item's fourth review.) The kind and the type are two
    answers, and applying the second to the literal alone left the comparison
    on the first. A composite whose kind is DECIMAL and whose fold is float8
    took the DECIMAL arm on every row the decimal arm supplied — read with the
    DECIMAL grammar, at DECIMAL width, before the literal was parsed at all —
    and that is three wrong answers wearing one cause:

    - a literal only the fold's grammar reads. `COALESCE(numeric, float8) =
      '0xC.C'` answered none where PostgreSQL answers 4: the float input
      function reads that hex float as 12.75 and the numeric one refuses it.
    - the wrong WIDTH. `= '12.750000000000000001'` answered none where
      PostgreSQL answers 4, because float8 rounds the literal onto the value
      and an exact decimal comparison does not.
    - a ROW-DEPENDENT refusal, which this item says is closed. `= 'abc'`
      raised only on the rows the FLOAT arm supplied.

    So every QUOTED pair is routed through the fold's rung first, and the
    DECIMAL arms own a pair only when the fold is itself DECIMAL. A CAST to a
    numeric type is a typed operand for the same fold (`castNumericKind`);
    saying otherwise made `COALESCE(numeric, CAST(k AS DOUBLE PRECISION))`
    unresolvable and sent it back to the DECIMAL rung.

    **And the refusal is decided at PLAN time.** A composite is typed there by
    the fold over its arms (`physical.foldArgTypes`), so the literal is parsed
    once against the fold's grammar before any row exists — which is the only
    way `WHERE id > 100 AND COALESCE(numeric, float8) = 'abc'` raises, as
    PostgreSQL does, over a range holding no rows at all. The binder recurses
    CHILDREN FIRST so the innermost failing coercion is the one reported, which
    is the order PostgreSQL analyses in.

    A gate can pass for a reason that does not generalise, and one here did:
    the row-independence test used GREATEST, which evaluates its float operand
    as a candidate on every row, so it refused whatever the range. COALESCE and
    CASE take their value from ONE arm and did not. Both, and an empty range,
    are gated now.

    **The DECLARED type of such a call is still `decided[0]`, not the fold**
    (`expr.CommonDeclType`), so a projection of `GREATEST(real, …, double)`
    narrows the double answer back into a real vector. The cause is one line
    further up: `physical.nodeDeclaredType` types a QUOTED string literal as
    `Decl(TypeString), Decided`, so a call holding one has a NON-NUMERIC
    decider and `CommonDeclType` falls back to `decided[0]` instead of folding.
    PostgreSQL types that literal `unknown` and resolves the call from the
    other arguments.

    That deferral does not merely narrow — it WRAPS. A folded double `1e39`
    stored into an INT64 vector is int64's MINIMUM, and so is a NaN: #462's
    failure mode, which item 6 forbids and ADR-0024 item 4 makes a 22003, and
    it reaches GROUP BY keys built from the same projection. It cannot be
    fixed from the comparison layer — the materialized value feeds the
    COMPARISON as often as it feeds a store, so narrowing or refusing there
    answers a different predicate than PostgreSQL's — so the six shapes are
    pinned LOUDLY in
    `coordinator.TestExtremumWinnerIsMaterializedAtTheCallsType`, with
    PostgreSQL's answer recorded beside each, and filed as #724. Deleting
    those pins is the declaration fix's proof.

    **The boxed layer resolves the column's WIDTH from its DECLARATION.**
    `expr.ColRef.Eval` widens on the way out — a FLOAT32 column boxes as
    float64 and an INT32 one as int64 — so a box-driven rule would compare
    `r < '3.1'` at double width and skip int4's range check. `boxKind` carries
    `boxInt32`/`boxInt64`/`boxFloat32`/`boxFloat64` for exactly this, with
    `boxNumber` kept for an operand whose declaration this layer cannot read;
    the widening is exact and order-preserving, so narrowing the box back
    inside the real arm recovers the stored value bit for bit.

    **Residuals, named rather than claimed.** DECIMAL still refuses `'1_000'`,
    `'0x1A'` and the other PostgreSQL 16 numeric-input forms that this closes
    for the integer family — that half of #634 needs `parquet.DecimalTextParts`
    to move, which is ADR-0024 item 6's grammar and its own change. The NETWORK
    types are still not wired into the plan-time refusal (#627): their parsers
    are STRICTER than PostgreSQL's, so refusing on them would reject PG-valid
    input. And TIMESTAMP deliberately keeps its own string grammar — a quoted
    string against a TIMESTAMP column is a timestamp, not a number (#493).

## Consequences

- `ORDER BY x DESC` places NULLs first (changed 2026-08-19). The default had
  been unreachable before that: the DESC comparator negated the kernel's null
  handling along with its values, so the code *declared* PostgreSQL's rule and
  *emitted* DuckDB's.
- The differential gate keeps full strength across the ordering corpus rather
  than carrying exemptions.
- A DECIMAL predicate answers the same on the kernel path and the row-at-a-time
  path (changed 2026-08-23). The row path had compared a DECIMAL column's
  RENDERED TEXT against a float64 literal and, finding no numeric reading of
  that pair, fell through to a lexicographic string comparison — so
  `CASE WHEN d = 1339815.97` was false for the row holding exactly that value
  even at a scale a float64 represents perfectly.
- Open questions in the same territory, to be decided by this ADR's rule and
  recorded when they are: integer overflow behavior, `timestamptz` and the
  session TimeZone GUC, empty-string versus NULL, identifier case folding.
- DuckDB cannot adjudicate "would a PostgreSQL client expect this," so a
  PostgreSQL differential arm (small scale, plus a pgwire protocol comparison)
  is the natural completion of this decision. See ADR-0013.

## Related

- ADR-0001 (record architecture decisions — the re-asked-question problem)
- ADR-0013 (correctness gates and their deliberate boundaries)
- CLAUDE.md, "Don't skip pgwire compatibility"
- `benchmarks/tpch/oracle_semantics_test.go`, `internal/planner/physical/plan.go`
  (`resolveNullsLast`), `internal/distributed/messages.go` (`PlaceNullsLast`)
- `internal/engine/exec/kernel/decimal_literal.go` (the literal, resolved at a
  column's scale), `internal/engine/expr/decimal_literal.go` (the row path's
  bindings and the boxed comparisons), `wadjet/decimal_literal_test.go` (the
  operator sweep at three scales, both paths)
- `internal/engine/batch/decimal.go` (`ScaledDecimal`, `DecimalTextAt`,
  `CompareDecimalTexts` — one carrier and one text comparison for every
  DECIMAL predicate), `internal/engine/exec/kernel/compare.go`
  (`colColFilterDecimal`, `DecimalConstText`),
  `internal/engine/exec/kernel/sort.go` (`CompareDecimalValues`)
- #462 (a literal past the carrier wrapped two's complement), #463 (an
  unreadable literal answered ZERO), #465 (CASE / IS DISTINCT FROM /
  GREATEST / LEAST did not carry the literal's text), #476 (a boxed DECIMAL
  against a number compared lexicographically), #477 (two DECIMAL columns had
  no kernel at all), #505 (the #463 refusal did not reach the #465 boxed
  sites or a negated string literal), #506 (two DECIMAL columns still compare
  lexically at the same three boxed sites — open), #517 (the boxed-site
  refusal is per-row, not plan-time — open) — the work items 6's amendments
  record
- #492 (IPv6/CIDR literal ordering was lexical-text, not numeric/structural,
  and disagreed between the single-process engine and the stage DAG) and
  #497 (LIKE against a network-native type or UUID panicked or silently
  matched nothing) — item 10 and item 11 above record the settled position
  for; `internal/engine/exec/kernel/compare.go` (`CidrSortKey`,
  `likeTextRenderer`), `internal/engine/expr/compile.go` (`tryNetworkLit`,
  `kernel.IPv6LitKey`), `internal/engine/expr/expr.go` (`CmpNetworkLit`,
  `Like.EvalBoolNull`), `internal/oracle/typematrix/typematrix.go`
  (`networkOrdLit`)
- #521 (CAST AS STRING did not render DATE or FLOAT32 the way the projection
  and LIKE already did — closed, item 11's amendment above) and #520 (ORDER
  BY/GROUP BY/DISTINCT/COUNT(DISTINCT)/MIN/MAX/hash-join keys over a CIDR
  column still used TEXT order — item 10's own "known residual," now
  closed) — `internal/engine/expr/expr.go` (`boxedTextOperand`),
  `internal/engine/exec/kernel/compare.go` (`CidrOrderKey`),
  `internal/engine/exec/kernel/sort.go`, `internal/engine/exec/kernel/agg.go`,
  `internal/engine/exec/kernel/types.go` (`Accumulator.Merge`),
  `internal/engine/exec/aggregate.go` (`appendColumnValue`),
  `internal/engine/exec/partitioned_agg.go` (`legacyCompositeHash`),
  `internal/worker/partitioned_shuffle_sink.go`
- #546 (the single-process set operation dedupped CIDR by stored TEXT while
  the stage DAG dedupped it by inet — closed) and #565 (a column-to-COLUMN
  CIDR comparison, and IPv6's ordering, were inet-ordered on the vectorized
  kernel only, so a WHERE clause and a projection of the same comparison
  disagreed inside one process — closed) — `internal/planner/physical/
  set_op_key.go` (`keyValueText`), `internal/engine/expr/boxed_pair.go`
  (`boxCidr`, `boxIPv6`, `netOrder`, `netKeyFor`, `compareNull`),
  `internal/engine/exec/kernel/compare.go` (`IPv6RowKey`,
  `colColFilterCidr`), `internal/coordinator/cidr_col_col_two_path_test.go`,
  `internal/engine/expr/network_col_col_test.go`
- #568 (a ROW field path is declared STRING, so `ORDER BY rw.c` sorts CIDR by
  text and `ORDER BY rw.n` sorts an INT64 by text, while `ORDER BY rw` is
  correct — open, pinned by
  `wadjet.TestRowFieldPathLosesTheFieldsDeclaredType`), #569 (windowed
  MIN/MAX declares FLOAT64 for eight types and fails the query where the
  plain aggregate answers — open), #570 (BYTES is not `bytea` on the wire,
  and `CAST AS STRING` yields an invalid-UTF-8 string with an embedded NUL
  that libpq truncates — open) — items 10 and 11's open residual lists
- #566 (a GROUP BY over an ARRAY/ROW/MAP/VECTOR column fails the query past a
  partial-aggregate spill — open, pinned by
  `exec.TestContainerGroupByPastASpillFails`), and the merge key underneath
  it: `internal/engine/exec/sort.go` (`appendKeyElemWithMeta` — a container's
  elements were written in the TOP-LEVEL encoding on the meta path, so
  ARRAY[1,23] and ARRAY[12,3] were one key)
- #522 (LIKE against a container column matched Go's own `fmt.Sprint` of the
  boxed value, an unspecified text form — item 11's own open question,
  settled as a refusal, closed) — `internal/engine/exec/kernel/compare.go`
  (`ResolveLikeFilterKernel`), `internal/engine/exec/filter.go`
  (`likeConstError`), `internal/engine/expr/expr.go` (`containerLikeKind`),
  `internal/engine/expr/fatal.go` (`raiseNoLikeOperator`)
- #444 (boxed ROW comparator ordered fields by name, not declared position),
  #446 (VECTOR/ARRAY(FLOAT) comparators not transitive under NaN) — the work
  item 8 above records the settled position for
- #459 (predicate kernels, the primary GROUP BY/DISTINCT hash key, and
  hash-join keys compared floats as raw IEEE754 — closed), #457 (MIN/MAX over
  a NaN column — closed) — item 8's remainder, now closed; see "What is now
  covered, and what is left" above for the distributed VECTOR-router
  follow-on that closed alongside
- `internal/engine/exec/kernel/float_order.go`, `internal/engine/exec/
  compare_boxed.go`, `internal/engine/exec/kernel/
  container_order_property_test.go` (the P1-P4 total-order property test)
- Exact numeric aggregates (item 9): `internal/engine/batch/decimal.go`
  (`AddChecked`, `AvgScale`, `DecimalAvg`), `internal/engine/exec/kernel/types.go`
  (`Accumulator.FinalSum`/`FinalAvg`/`FinalMin`/`FinalMax`),
  `internal/engine/exec/aggregate.go` (`outputSchema`, `minMaxOutputType`),
  `internal/planner/physical/plan.go` (`aggSpecOutputType`),
  `internal/worker/avg_fold.go`
