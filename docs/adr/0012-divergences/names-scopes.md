# ADR-0012 divergences: Names, scopes, joins and stars

Name resolution and scoping: identifier case folding, storable names, published output names, column-alias lists, USING merges, star expansion and duplicate-name handling. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**Every result set describes its columns or is refused** (E02)
PostgreSQL sends a RowDescription for every statement that produces a result set, and two empty column lists compare equal, so a result with no columns is indistinguishable from a legitimate empty answer. All four places a result set is assembled refuse it with `sqlerr.EmptyResultColumns` (XX000): `wadjet.DB.Query`, `Coordinator.ExecuteSQL`, the HTTP query door and `Coordinator.GetQueryResults`, whose zero-row results are described from `physical.Planner.DeclaredOutputSchema`. A star over a join is expanded into the FROM clause's arms in written order (ADR-0026 §9), so nested joins, LATERALs and recursive CTEs (ADR-0021 §1o-b) declare their columns with no rows. What remains is a LATERAL whose own list names one column twice: over an ungrouped aggregate the zero-row star is XX000, over an expression key it is 0A000.

**Published name and reference name of an unnamed column** (E07, E52)
An unaliased output column is published under PostgreSQL's `FigureColname` on every door and both engines: `?column?` for an operator expression, the function's name for a call, `count` for `COUNT(*)`, `case` for a CASE, the operand's name for a CAST (measured at tip; E52's table recording the expression's text as the published name no longer describes the engine). An enclosing query references such a column of a derived table by the inner block's spelling (`"g + 1"`), not by `"?column?"`, because inside the plan a name is a handle that sort keys, HAVING and aggregate `OutputCol` resolve against, and two unnamed items would otherwise answer to one handle. Both halves are loud: an unresolved reference is an error, never a different column. Both paths name one query identically (`coordinator.TestBothPathsNameAnUnaliasedExpressionTheSameWay`, #744).

**Rewritten calls keep PostgreSQL's label where measured** (E10)
PostgreSQL labels an unaliased call after the function it resolved to, and the parser's own rewrites carry that label: `trim(' a ')` is `btrim`, `POSITION(x IN y)` is `position` (not `strpos`), an array subscript `arr[1]` is `arr` (not `element_at`), and `SUBSTRING(s FROM 2 FOR 3)` is `substring` (accepted and measured at tip). A further rewrite joins this list only with the measurement beside it.

**A name is a storage location** (E08)
A relation's data lives at `tables/<name>/…` and a partition column becomes a `<col>=<value>/` component, so the catalog refuses `/`, `\`, NUL, `.`, `..` and a leading `.` (42602) and names over 63 bytes (42622), where PostgreSQL truncates; 63 is `NAMEDATALEN - 1`, so at or under it the two agree byte for byte. `..` inside a component (`"x..y"`) is accepted. The store enforces the same rule on every keyed operation (`objstore.CheckObjectAccess`), identically across MemStore, FileStore and S3, so a key one store refuses all three refuse.

**Case folding and the resolver concession** (E34)
An unquoted identifier folds to lower case at the lexer (`plansql.FoldIdent`, ASCII A-Z only) and a delimited one keeps its bytes, which is PostgreSQL's rule. The resolver, `batch.ResolveColumnIndex`, then matches a folded name that misses byte-exact case-insensitively when exactly one column matches; the schema itself stays byte-exact. A delimited reference carrying an upper-case letter takes no concession (`SELECT "G"` over `g` is 42703, `physical.colScope.refuseDelimitedMiss`), and a qualifier never does (`batch.foldedNameMatches`, `physical.colScope.exactQuals`). Table names take the same read-only concession through `catalog.ResolveTableName`; CREATE keys byte-exact, and every writing door canonicalizes the name once so writes land where reads resolve. Closing the all-lower-case delimited divergence means carrying a `Quoted` bit from the lexer to the resolver.

**Duplicate names inside one source** (E32, E89)
An output slot's identity is its position; a block may publish one name twice. A qualified reference into such a block is 42702, decided within one source's own list by `colScope.noteSourceDuplicates` and recorded in `colScope.dupQualified` (a per-qualifier count cannot serve, because two relations may fold to one qualifier key). A qualified star over it is refused 0A000 because its expansion emits one reference per column and two references spelled alike bind the first. The bare star reads by position and answers PostgreSQL's pair. Closing the qualified star needs a block's column addressed by position (#1076).

**USING merges and the star** (E25, E26, E54, A10, A11)
`logical.usingJoinStarColumns` publishes the USING columns once and first, then each arm's remaining columns; the merged value is the side never NULL-extended, `COALESCE(l.c, r.c)` for a FULL join, and a name shared outside the USING list is published twice. `plansql.bindMergedUsingKeys` binds a bare sort or window key to the merged expression at parse time; elsewhere a bare merged reference is resolved by the binder's scope in `internal/planner/physical`, which sees two columns and refuses 42702. A FULL join's merged key is computed, and `logical.hiddenSortProjection` materializes a computed sort key only beside a named select list, so a star-only list refuses it. A positional ORDER BY over a star over a join answers through `ResolveStarJoinOrdinalSortKeys`; a set operation is countable (`plansql.BlockOutputColumns`); still refused are a chain of USING joins, an arm publishing one name twice, and an arm whose own list is not knowable.

**Which side builds must not decide a name** (A14)
The star over a join is expanded at Optimize step 1, before any pass that reorders the join, into qualified references published under each column's own name, so order and names are the query's on every arm and on the wire, and `markCoPathingSelfJoinBuilds` no longer shapes a published name. FROM items whose own names do not address their columns still publish the plan's order: a block whose items resolve to one name, a set-operation arm, a LATERAL arm and a table function; each keeps its values. On the distributed arms a derived block a star reads emits its projection as the stage's column set (`physical.starReadBlockProjections`, `Stage.ProjectExprs`).

**Refused rather than guessed: alias lists and GROUP BY qualifiers** (E22, E23, E24, E53)
A column-alias list over a star is deferred to `ExpandStarProjections` (`logical.ApplyDeferredColumnAliases`) and applied wherever the width is known; over a star the expansion declines it is refused 0A000. A list that repeats a name is refused 42701 because renaming is positional and no scope holds two columns under one name. A list on a WITH-query reference is refused 0A000 because the query's block is materialized and the rename would land on a Project above it. A qualified GROUP BY term under a bare select item is 42803 because the aggregate cannot evaluate `typemx.g + 1` over a batch whose column is `g`.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT WatchID FROM hits` | ERROR 42703 column "watchid" does not exist (column stored as "WatchID") | reads WatchID: a folded name resolves case-insensitively when exactly one column matches | PG 42703 | kept superset | 2026-09-03 · [E34](#e34), P055 | #731 | — |
| **r2** `SELECT "watchid" FROM hits` | ERROR 42703 (delimited lower-case name matches byte-exact only) | reads WatchID: an all-lower-case delimited name cannot be told from a folded one and takes the concession | PG 42703 | kept superset | 2026-09-03 · [E34](#e34), P055 | #731 | — |
| **r3** `SELECT * FROM MyTab` | ERROR 42P01 when the table is stored as "MyTab" | reads the one table matching case-insensitively (catalog.ResolveTableName); two such tables are 42P01 naming both | PG 42P01 | kept superset | 2026-09-03 · [E34](#e34), P055 | #731 | — |
| **r4** `SELECT mixedcol FROM clt4, clt5 WHERE clt4.k = clt5.k` | 900, 901 (clt4."MixedCol" and clt5.mixedcol are two names) | ERROR 42702 column reference is ambiguous; qualify it (clt4.MixedCol, clt5.mixedcol) | 42702 | refusal | 2026-09-03 · [E34](#e34), P055 | #731 | — |
| **r5** `SELECT n_name AS u, n_comment AS u FROM nation ORDER BY u` | ERROR 42702 ORDER BY "u" is ambiguous | answers, sorted by the first output column named u | PG 42702 | kept superset | 2026-09-03 · [E31](#e31), P054 | #557 | — |
| **r6** `SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING c > 1` | ERROR 42703 column "c" does not exist | answers: HAVING resolves the output alias | PG 42703 | kept superset | 2026-08-25 · [E45](#e45), P062 | #591 | — |
| **r7** `SELECT * FROM a, b JOIN c ON a.k = c.k` | ERROR invalid reference to FROM-clause entry | answers: an ON sees relations the FROM clause declared earlier, comma items included | PG 42P01 | kept superset | 2026-09-23 · [E48](#e48), P065 | #617, #1220 | `physical.TestArcRSAQualifiedReferenceNamesOneRelationInScope`, `coordinator.TestArcRSAQualifiedReferenceNamesOneRelationOnEveryArm` |
| **r8** `SELECT * FROM a FULL JOIN b ON a.n < b.n` | ERROR FULL JOIN is only supported with merge-joinable or hash-joinable join conditions | answers the defined result: a LEFT JOIN b ON p plus b rows no a row satisfies | PG 0A000 | kept superset | 2026-09-18 · [E29](#e29), P088 | #1153 | `coordinator.TestJRAOuterJoinOnResidualsAgreeOnFiveArms` |
| **r9** `SELECT "?column?" FROM (SELECT g + 1 FROM typemx) d` | the column's value (the published name is also the reference name) | ERROR 42703 naming the column that exists; SELECT "g + 1" answers (measured) | 42703 | refusal | 2026-09-04 · [E07](#e07), [E52](#e52), P051 | #732 | `coordinator.TestAnUnnamedDerivedColumnCannotBeReferencedByItsPublishedName` |
| **r10** `SELECT g + 1 FROM typemx GROUP BY typemx.g + 1`; over a join, `SELECT i + 1 … FROM ss_t t JOIN ss_i u ON … GROUP BY t.i + 1` and `SELECT t.i + 1 … GROUP BY i + 1` | answers | the embedded engine (`wadjet.DB`, `wadjet serve`) answers PostgreSQL's rows (a term is the key by its binding, ADR-0047 stage 1); through the coordinator (the stage-DAG arms) ERROR 42803 until ADR-0047 stage 5 | 42803 (coordinator) | refusal | 2026-09-03 · [E53](#e53), P071; narrowed to the DAG arms 2026-10-06 | #738, #1524 | `coordinator.TestTheIdentityErasesAQualifierAndATypeSynonym`, `coordinator.TestArcGKGroupKeySpellingEveryArm` (j/*, ci1/innerJoin*) |
| **r11** `CREATE TABLE "a/b" (x INT)` | creates the table (any identifier is legal) | ERROR 42602 invalid_name for /, \, NUL, a name that is . or .., or begins with . | 42602 | refusal | 2026-09-05 · [E08](#e08), P052 | — | `server.TestNoDoorCreatesARelationWhoseNameIsNotStorable`, `server.TestNoDoorCreatesAColumnWhoseNameIsNotStorable`, `objstore.TestEveryStoreRefusesABadKeyOnEveryOperation` |
| **r12** `CREATE TABLE t_name_over_sixty_three_bytes_long_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (x INT)` | creates it truncated to 63 bytes with NOTICE 42622 | ERROR 42622 name_too_long | 42622 | refusal | 2026-09-05 · [E08](#e08), P053 | — | `server.TestNoDoorCreatesARelationWhoseNameIsTooLong` |
| **r13** `SELECT * FROM t a(k, k)` | accepts the list; 42702 at every reference to k (42P10 on a table function narrower than the list) | ERROR 42701 at the list | 42701 | refusal | 2026-09-18 · [E23](#e23), P039, P123 | #959, #1184 | `sql.TestArcPSParserReadsTheColumnAliasListGrammar` |
| **r14** `WITH c AS (SELECT 1 AS a, 2 AS b) SELECT * FROM c z(x, y)` | x=1, y=2 | ERROR 0A000; WITH c(x, y) AS (…) answers | 0A000 | refusal | 2026-09-18 · [E24](#e24), P122 | #959, #1158 | `coordinator.TestArcPSGrammarAnswersTheSameOnEveryArm` |
| **r15** `SELECT * FROM (SELECT * FROM ka JOIN kb ON ka.k = kb.k) AS b(p, q, r, s)` | p, q, r, s renamed positionally | ERROR 0A000 (measured); a list over a star of a base table, CTE, derived table or set operation applies | 0A000 | refusal | 2026-09-07 · [E22](#e22), P113 | #613, #958 | `coordinator.TestArcK1AColumnAliasListRenamesPositionally` |
| **r16** `SELECT id FROM a JOIN b USING (id)` | the merged id | ERROR 42702; a.id answers; in ORDER BY or a window key the bare name binds the merge | 42702 | refusal | 2026-09-18 · [E25](#e25), P118 | #655 | — |
| **r17** `SELECT * FROM psb FULL JOIN psa USING (id) ORDER BY id` | 1, 2, 3 (ordered by COALESCE of the two sides) | ERROR 0A000; a named select list, or no ORDER BY, answers | 0A000 | refusal | 2026-09-18 · [E26](#e26), P119 | #655 | — |
| **r18** `SELECT COUNT(*) OVER (PARTITION BY id) FROM psb FULL JOIN psa USING (id)` | answers, partitioned by the merged key | ERROR 0A000, drawn on the shape; a window argument binds the merge | 0A000 | refusal | 2026-09-18 · [E26](#e26), P120 | #655 | — |
| **r19** `SELECT * FROM ua JOIN ub USING (id) JOIN uc USING (id)` | id, v, w, z | ERROR 0A000 (measured): a chain of USING joins is not expanded | 0A000 | refusal | 2026-09-18 · [E54](#e54), [A11](../0012-amendments.md#a11), P115 | #810, #655, #1177 | `coordinator.TestSRAStarPublishesItsArmsOwnColumns` |
| **r20** `SELECT * FROM (SELECT id, v AS x, v AS x FROM ua) s JOIN ub USING (id)` | id, x, x, w | ERROR 0A000 (measured): an arm that publishes one name twice | 0A000 | refusal | 2026-09-18 · [A11](../0012-amendments.md#a11), P115 | #655, #1177 | `coordinator.TestSRAStarPublishesItsArmsOwnColumns` |
| **r21** `SELECT * FROM lat_ord o JOIN LATERAL (SELECT i.id FROM lat_item i WHERE i.order_id = o.id) l USING (id)` | id, customer, total | id, customer, total, id (measured): the merge is not applied over a LATERAL | — | value divergence | — · P116 | #1177 | — |
| **r22** `SELECT * FROM zzp JOIN zzj USING (id, d92)` | d92 declared numeric (common type) | d92 declared numeric(9,2), the left arm's (measured); value the left arm's on both | — | value divergence | — · P117 | #1177 | — |
| **r23** `SELECT * FROM na NATURAL JOIN nb` | joins on the shared columns | ERROR 0A000 NATURAL JOIN is not supported (measured); write ON or USING | 0A000 | documented gap | — · P121, P143 | #655 | — |
| **r24** `SELECT x.* FROM (SELECT a.id, b.id FROM lat_item a JOIN lat_item b ON a.order_id = b.order_id) x` | both id columns | ERROR 0A000 (measured); the bare star answers the pair; x.id is 42702 as in PostgreSQL | 0A000 | refusal | 2026-09-13 · [E89](#e89), [E32](#e32), P140 | #1094 | `coordinator.TestArcO2ADerivedBlockPublishesItsVisibleList`, `coordinator.TestArcJ1AQualifiedStarExpandsFromTheRelationsOutput` |
| **r25** `SELECT * FROM t, LATERAL (SELECT MAX(x) AS m, MIN(x) AS m FROM u WHERE u.k = t.k) s WHERE false` | header with every column, zero rows | ERROR XX000: the join carries the empty-input pad marker the declaration will not publish | XX000 | refusal | 2026-09-25 · [E02](#e02), P048 | #1008, #1010, #1013 | — |
| **r26** `SELECT * FROM t, LATERAL (SELECT t.a AS m, t.b AS m) s WHERE false` | declares both columns, the second named m | declares both columns, the second named s.m (the non-empty result's name too) | — | value divergence | 2026-09-25 · [E02](#e02), P048 | #1013 | `server.TestN1AnEmptyColumnListIsRefusedOnTheHTTPDoor`, `pgwire.TestN1AnEmptyColumnListIsRefusedOnTheWire` |
| **r27** `SELECT * FROM a JOIN (SELECT k FROM b UNION ALL SELECT k FROM c) u ON a.k = u.k` | FROM order, names bare | a set-operation, LATERAL or table-function arm, or a block whose items resolve to one name, publishes the plan's order; values unchanged | — | documented gap | 2026-09-13 · [A14](../0012-amendments.md#a14) | #997, #1012 | `coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan` |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E02

ADR lines 306-371. Catalog rows: r25, r26. Stated in [Mechanisms](#mechanisms).

- **A result with NO COLUMNS AT ALL is refused, where PostgreSQL answers
  with a header and zero rows.** (Added 2026-09-08, #1008 / #1010.)
  PostgreSQL sends a RowDescription with fields for every statement that
  produces a result set, whether or not it returns rows, and clients depend
  on it. Wadjet derives a `SELECT *`'s columns from the DATA and falls back
  to a plan-time declaration (`physical.declaredOutputSchema`, #416, #846,
  #978) — and three shapes were past its bound: a star over a join whose
  SIDES contain a join (`starJoinDeclaredOutputSchema` declined there, which
  is three or more relations and equally TWO or more LATERALs), a star over
  a LATERAL whose subquery is an UNGROUPED AGGREGATE — whose join carries
  the pad marker the declaration will not publish — and a star over a
  RECURSIVE CTE. With no rows to read a schema off, those returned a result
  with zero columns and no error.

  **Ordinary nested joins are CLOSED (2026-09-13, #997/#1012).** A
  star over a join is now EXPANDED into the FROM clause's arms in written
  order (ADR-0026 §9), so it is an ordinary SELECT list and the ordinary
  projection walk declares it — at any join depth, because the expansion is
  per ARM rather than per operator. `SELECT * FROM a JOIN b JOIN c WHERE
  false` declares its columns on every arm and on the async door
  (`coordinator.TestN1AnAsyncResultDeclaresItsColumns`,
  `TestN1AResultWithNoColumnsIsRefused`). **Amended 2026-09-24 :** recursive CTEs now retain the seed declaration even when empty
  (ADR-0021 §1o-b); the ungrouped-aggregate LATERAL boundary remains.
  **Amended 2026-09-25 (#1013):** a star over a LATERAL
  join is expanded into the FROM arms' own lists too, the lateral's read
  as `s.*` reads it (ADR-0026 §8l), so a zero-row star over two or more
  LATERALs, a LATERAL beside another join, and an ungrouped-aggregate
  LATERAL declares its columns on every arm and door. A LATERAL whose own
  list names one column twice is not enumerable by name, so the star still
  reads the join's output. Over an UNGROUPED AGGREGATE body it refuses
  (XX000: the join carries the pad marker); over a PLAIN body it was said
  to refuse too, and measured it answered, declaring the duplicate ONCE
  (four columns for PostgreSQL's five, the lateral binding measurement). **Amended 2026-09-25:**
  the block's own list is declared as written, so the empty result
  declares both columns, the second as `s.m` — the join's qualified name
  for a duplicate, the non-empty result's name too (FC-JP-13); over an
  expression key the star is refused 0A000 (the join's key column cannot be
  kept out of it). `server.TestN1AnEmptyColumnListIsRefusedOnTheHTTPDoor`
  and `pgwire.TestN1AnEmptyColumnListIsRefusedOnTheWire` hold the
  declaring half.

  A SINGLE LATERAL that is not an ungrouped aggregate is not among them and
  answers with its columns, in the plain, `GROUP BY` and `LEFT JOIN LATERAL`
  spellings alike — measured on four arms.

  That is not a smaller answer, it is the engine failing to describe its
  own output, and at the client it cannot be told from a query that
  legitimately found nothing — which is exactly how two silent wrong
  answers reached a client (#1008, #1010) without any value comparison
  noticing, because two empty column lists compare equal. All FOUR places a
  result set is assembled now refuse it (`sqlerr.EmptyResultColumns`,
  XX000: nothing about the STATEMENT is wrong, so the class cannot blame
  the client, and this is not a feature declined but the engine failing to
  describe itself): the embedded `wadjet.DB.Query`,
  `Coordinator.ExecuteSQL`, the HTTP query door — which runs its own
  pipeline and so inherits nothing — and `Coordinator.GetQueryResults`, the
  async door, whose zero-row results are described from
  `physical.Planner.DeclaredOutputSchema` because it has no batch to read a
  schema off at all.

  The divergence is in the REFUSING direction and it replaces an answer
  that was not PostgreSQL's either. DECLARING those three shapes' columns
  is what closes it, and it is the recorded follow-up; until then the
  engine says so out loud rather than handing back a table with no shape.

### E07

ADR lines 436-456. Catalog rows: r9. Stated in [Mechanisms](#mechanisms).

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

### E08

ADR lines 457-510. Catalog rows: r11, r12. Stated in [Mechanisms](#mechanisms).

- **A relation or column name must be one component of an object key.**
  (Added 2026-09-05, CodeQL go/path-injection #23/#24/#25.) PostgreSQL
  accepts almost anything inside a double-quoted identifier —
  `CREATE TABLE "../../../tmp/x"`, `"a/b"`, `".hidden"` and `".."` are all
  legal relation names there — because a PostgreSQL relation is a row in
  `pg_class` and never a filename. A wadjet relation IS a location: its data
  lives at `tables/<name>/…` in the object store, and a partition key's
  column name becomes a `<col>=<value>/` component below it.

  Wadjet therefore refuses, at CREATE and on every door, a relation or
  column name that contains `/`, `\` or a NUL byte, that IS `.` or `..`, or
  that begins with `.` — SQLSTATE 42602 `invalid_name`. A `..` INSIDE a
  component (`"x..y"`) is accepted, because it names a real directory; the
  danger is a component that IS `..`, not the two characters. Everything
  else PostgreSQL accepts is still accepted, spaces and embedded quotes
  included.

  A name over **63 bytes** is refused too, with SQLSTATE 42622
  `name_too_long`. PostgreSQL TRUNCATES rather than refusing — measured
  live, an 80-byte name becomes 63 bytes with
  `NOTICE 42622 identifier "…" will be truncated to "…"` — and wadjet
  cannot, because two names truncated to one would be two tables at ONE
  storage location. 63 is PostgreSQL's own effective length
  (`NAMEDATALEN - 1`), which is what makes the sentence below exact: at or
  under it the two engines agree byte for byte, and over it PostgreSQL's own
  answer is already lossy. Without the bound a 300-byte name was accepted at
  CREATE and then failed every write with `ENAMETOOLONG` — a table whose
  data has no home, the failure this whole entry exists to prevent.

  The refusal is name-only and LOUD: no query answers differently, nothing
  is silently rewritten, and no name that IS accepted behaves differently
  from PostgreSQL. The alternative is a table whose data has no home — and,
  measured, worse than that: with `storage.type: file` the lexer's verbatim
  delimited identifier reached `filepath.Join`, which CLEANS its result, so
  `CREATE TABLE "../../../tmp/x"` wrote parquet files anywhere the process
  could reach. The store enforces the same rule at the key
  (`objstore.CheckObjectAccess`, which every store calls on EVERY operation
  — read, write, head, delete and list — not only on the write), because a
  store cannot know what its keys were made of; the catalog's check is the
  one that can tell a person what is wrong. Both halves are literal: a key
  one store refuses is a key all three refuse, and on the same operations,
  so a table cannot work against `MemStore` in a test and fail against a
  filesystem or S3 store in production.

  Gated at `server.TestNoDoorCreatesARelationWhoseNameIsNotStorable` (three
  doors, refused and accepted names beside each other),
  `server.TestNoDoorCreatesARelationWhoseNameIsTooLong` (the 63/64-byte
  boundary from both sides, three doors),
  `server.TestNoDoorCreatesAColumnWhoseNameIsNotStorable`,
  `objstore.TestNoStoreAcceptsAKeyThatCanLeaveItsBucket` /
  `TestATableNamedWithATraversalCannotWriteOutsideTheStore`, and
  `objstore.TestEveryStoreRefusesABadKeyOnEveryOperation` (every store ×
  every keyed operation, plus the bucket on `List` / `BucketExists` /
  `MakeBucket`).

### E10

ADR lines 536-544. Stated in [Mechanisms](#mechanisms).

- **A call PostgreSQL rewrites keeps the name the query wrote, except where
  the rewrite is measured.** (Added 2026-09-04, #732; amended 2026-09-05.)
  PostgreSQL labels an unaliased call after the function it RESOLVED to, and
  three of this parser's own rewrites now carry that label: `trim(' a ')` is
  `btrim`, `POSITION(x IN y)` is `position` (not the `strpos` it lowers to),
  and an ARRAY SUBSCRIPT `arr[1]` is `arr` (not `element_at`).
  `SUBSTRING(s FROM 2 FOR 3)` — which wadjet's grammar does not accept at
  all — would be `substring`. A further such rewrite belongs on this list
  only with the measurement beside it.

### E22

ADR lines 979-1002. Catalog rows: r15. Stated in [Mechanisms](#mechanisms).

- **A column-alias list over a `SELECT *` is not applied.** (Added
  2026-09-04, #613. CLOSED and RE-SCOPED 2026-09-07, #958: the
  entry was right about the reason and wrong about the symptom, and the
  shape it now covers is much narrower.)

  `(…) AS b(kk, nn)` renames a derived table's columns positionally, and
  PostgreSQL applies it whatever the subquery's SELECT list looks like:
  `SELECT * FROM (SELECT * FROM t) AS b(kk, nn)` publishes `kk | nn | …`
  there. The star's width is a catalog question `logical.applyColumnAliases`
  cannot ask, so the list was dropped — and the entry said the relation then
  published the inner names. It did not: every reference to a name the list
  renames TO was LOUD (`sort: key column "kk" does not exist in the input
  schema`), or, through a derived table, silently NULL.

  The list is now DEFERRED to `ExpandStarProjections`, which answers the
  width one pass later (`logical.ApplyDeferredColumnAliases`), so the common
  shapes — a star over a base table, a CTE, a derived table, a set operation
  — are applied and agree with PostgreSQL, and the arity refusal moves with
  them. What remains is a list over a star the expansion DECLINES, which is
  the entry below's shape: a bare `*` over a JOIN. There the list cannot be
  applied truthfully and is REFUSED in one sentence (0A000) rather than
  dropped. Gated by
  `coordinator.TestArcK1AColumnAliasListRenamesPositionally`.

### E23

ADR lines 1003-1017. Catalog rows: r13. Stated in [Mechanisms](#mechanisms).

- **A column-alias list that REPEATS a name is refused (42701), where
  PostgreSQL accepts the list.** (Added 2026-09-18, #959.)

  PostgreSQL accepts `FROM t a(k, k)` and refuses every REFERENCE to `k`
  with 42702 `column reference "k" is ambiguous` — a scope that holds two
  columns under one name and answers a star over them. This planner renames
  POSITIONALLY and has no such scope: the star over that relation expanded
  to the same qualified reference twice and published the SECOND column's
  values under both names, which is a wrong VALUE where PostgreSQL answers
  the right ones. The list is refused at the spelling instead. Narrower
  than PostgreSQL, loud, and recorded; closing it means the block's
  published list travelling by POSITION rather than by name, which is the
  same work `logical.projectionOutputNames` names for #1076. Gated by
  `sql.TestArcPSParserReadsTheColumnAliasListGrammar`.

### E24

ADR lines 1018-1032. Catalog rows: r14. Stated in [Mechanisms](#mechanisms).

- **A column-alias list on a reference to a `WITH` query is refused
  (0A000).** (Added 2026-09-18, #959/#1158.)

  `WITH c AS (…) SELECT * FROM c z(x, y)` renames the query's output in the
  ENCLOSING scope. A list on a NAMED relation is lowered to the
  derived-table spelling at parse time — which is what an alias clause with
  a column list means, and which puts the binder that builds the enclosing
  scope and the logical builder on one tree (`sub_block.go`, #851) — but a
  CTE reference's block is MATERIALIZED, so the rename lands on a Project
  above it and every renamed reference resolves against the block's own
  columns and reads NULL. Refused rather than answered wrong; the
  definition's list, `WITH c(x, y) AS (…)`, is PostgreSQL's other spelling
  and answers. Gated by
  `coordinator.TestArcPSGrammarAnswersTheSameOnEveryArm`.

### E25

ADR lines 1033-1057. Catalog rows: r16. Stated in [Mechanisms](#mechanisms).

- **A BARE reference to a `JOIN … USING` join's MERGED column is 42702
  outside a sort or window key, where PostgreSQL answers.** (Added
  2026-09-18, #655; narrowed the same day by the earlier measurement's
  B1.)

  USING merges the joined column into one, so `SELECT id FROM a JOIN b
  USING (id)` is not ambiguous in PostgreSQL. The merge is stated here for
  the STAR — the parser records the USING list on the join and
  `logical.usingJoinStarColumns` publishes the merged column once and first
  — but a bare reference in a SELECT item, a WHERE, a GROUP BY, a HAVING or
  a DISTINCT is resolved by the binder's scope in
  `internal/planner/physical`, which reads the two arms' columns and sees
  two `id`s. Qualifying the reference (`a.id`) answers. Loud, never a wrong
  value.

  A SORT or WINDOW key is the exception and it had to be: those two are the
  only places a bare reference was BOUND rather than refused, and it was
  bound to the LEFT arm. For an INNER or LEFT join that is the merged value
  and the answer was right; for a RIGHT or FULL join it is not, and
  `psb FULL JOIN psa USING (id) ORDER BY id` came back in the order 2, 3, 1
  where PostgreSQL 17.11 answers 1, 2, 3 — under `LIMIT 1` a different ROW
  and under `OFFSET 1` a different ROW SET. `plansql.bindMergedUsingKeys`
  now binds such a key to the merged EXPRESSION at parse time, where both
  sides' names are known without a catalog.

### E26

ADR lines 1058-1086. Catalog rows: r17, r18. Stated in [Mechanisms](#mechanisms).

- **A bare `SELECT *` over a FULL `JOIN … USING` cannot be ORDERED BY the
  merged column (0A000), where PostgreSQL answers.** (Added 2026-09-18 by
  the name scope validation, #655.)

  The consequence of the entry above. A FULL join's merged value is
  `COALESCE(l.c, r.c)` — a COMPUTED key — and `logical.hiddenSortProjection`
  materializes a computed sort key beside a NAMED select list, which a
  star-only list is not (its own bound, whose comment already records that
  lifting it for a star over a join is a measured follow-up). So the key is
  bound correctly and then refused, loudly, rather than bound to the left
  arm. A named select list carries it and answers; the same statement
  without the ORDER BY answers; a RIGHT join's merged key is a plain
  reference and answers. The same applies to a window PARTITION BY or
  window ORDER BY key, whose slot holds a column NAME — though a window
  ARGUMENT is an EXPRESSION and does take the merge, for a FULL join as
  readily as for a RIGHT one.

  THE REFUSAL IS DRAWN ON THE SHAPE, not on the data, and on a fixture where
  the merged partitioning and the left arm's COINCIDE it withdraws an answer
  that happened to be right. `COUNT(*) OVER (PARTITION BY id)` over
  `psb FULL JOIN psa USING (id)` answered PostgreSQL's 1, 1, 1 at base,
  because every partition there is one row whichever side the key binds; it
  is 0A000 now. The SHAPE is wrong at base in general — with two rows the
  left arm does not match, the same statement answered 2, 2 for
  PostgreSQL's 1, 1, and `ROW_NUMBER() OVER (ORDER BY id)` answered 1, 2, 3
  for 2, 3, 1 — so the refusal replaces a base-WRONG answer rather than a
  right one, and it cannot tell the two fixtures apart without the data.
  Measured both ways by the earlier measurement (P1-r2).

### E27

ADR lines 1087-1099. Moved to the log: [A07](../0012-amendments.md#a07).

  *(Moved to the amendment log: [A07](../0012-amendments.md#a07).)*

### E29

ADR lines 1111-1122. Catalog rows: r8.

- **A `FULL JOIN` on a non-equi `ON` condition ANSWERS, where PostgreSQL
  refuses.** (Added 2026-09-18, #1153.)

  `FULL JOIN b ON a.n < b.n` raises `FULL JOIN is only supported with
  merge-joinable or hash-joinable join conditions` on PostgreSQL 17.11,
  which has no executor for the shape. The join is still DEFINED there —
  `(a LEFT JOIN b ON p) UNION ALL (b WHERE NOT EXISTS (a WHERE p))` — and
  that definition is what this engine answers; all fourteen shapes were
  measured against it. A superset, kept (the PG-rejects-but-we-answer
  class), and each cell says so in
  `coordinator.TestJRAOuterJoinOnResidualsAgreeOnFiveArms`.

### E31

ADR lines 1139-1155. Catalog rows: r5.

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

### E32

ADR lines 1156-1188. Catalog rows: r24. Stated in [Mechanisms](#mechanisms).

- **A QUALIFIED reference into a block that publishes the name TWICE binds
  the first, where PostgreSQL refuses it.** (Added 2026-09-13.
  **CLOSED 2026-09-18, #1094.**)
  `SELECT d.*, x.id FROM (SELECT * FROM lat_ord o JOIN lat_item li ON …) d
  JOIN lat_ord x ON x.id = d.id` is 42702 `column reference "id" is
  ambiguous` on postgres:17 — `d` publishes `id` twice, because a star over
  a join publishes every arm's own list (ADR-0026 §9) — and this binder
  resolved `d.id` to the FIRST of the two and answered. It became REACHABLE
  with §9: before it, such a block published the join's stream and the
  reference resolved against that instead.

  It raises 42702 now. `colScope.srcCount` could not say it — it counts
  SOURCES, and this is ONE source counted twice — and neither could a
  per-(qualifier, column) COUNT, because `quals` is keyed on the FOLDED
  qualifier and two DIFFERENT relations may fold to one key (`FROM clt1 t,
  clt2 "T"` is two sources under `t`, and counting their columns together
  refused `t.c1`, which PostgreSQL answers). So the duplicate is decided
  WITHIN one source's own list — `colScope.noteSourceDuplicates`, once per
  source, after its columns are registered — and only the VERDICT is
  recorded, in `colScope.dupQualified`; the qualified branch of
  `resolveRef` refuses on it. The message names the COLUMN, because the
  qualifier names exactly one relation. It is the other half of ADR-0026
  §9's duplicate-published-name rule: the qualified STAR over such a block
  already declined rather than bind the first item twice, and the explicit
  list was the half left open — the wrong-VALUE one, since which column
  answered was the block's item order. The BARE star over the same block is
  untouched and still answers both columns by position, which is
  PostgreSQL's answer. Gate:
  `coordinator.TestSRAStarPublishesItsArmsOwnColumns`'s `dupname/*` cells,
  `pgwire.TestSRTheWireDeclaresAStarsOwnArms` and
  `server.TestArcSRAStarOverAPolicedArmNeverPublishesTheOtherArmsValue`'s
  `dupname_reference_into_a_policed_block`.

### E33

ADR lines 1189-1204. Moved to the log: [A08](../0012-amendments.md#a08).

  *(Moved to the amendment log: [A08](../0012-amendments.md#a08).)*

### E34

ADR lines 1205-1323. Catalog rows: r1, r2, r3, r4. Stated in [Mechanisms](#mechanisms).

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

### E45

ADR lines 1869-1885. Catalog rows: r6.

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

### E48

ADR lines 1968-1973. Catalog rows: r7.

- **A JOIN's ON condition can reference comma-join siblings; PostgreSQL rejects this.** (Closed #617; briefly reversed 2026-09-20, #1220; **restored 2026-09-23 by the BX hotfix.**) A join predicate like `SELECT ... FROM a, b JOIN c ON a.k = c.k` references a sibling of the comma join in its ON clause. PostgreSQL 17 rejects this with `invalid reference to FROM-clause entry`; wadjet answers it, matching DuckDB. This is a strict SUPERSET: errors on PostgreSQL, runs on wadjet; not a value divergence and not a wire-protocol violation. Gated against DuckDB and the two-path oracle (PostgreSQL offers no value to assert). #593 fixed the prior silent-zero wrong answer in this shape.

  The #1220 measurement found the planner had no ON-scope validation at all, and the missing restriction let an ON clause ALSO name a relation the statement joins LATER (`FROM a JOIN b ON c.x = a.x JOIN c ON …`) — not a superset of anything, a typo answered with rows; 82 of 200 SQLancer databases at seed 1 stop on that shape. The validation was built (`physical.relationCensus` / `visibleAtJoin`) to fix the LATER case, but scoped visibility to the ON's own FROM item, which also refused the EARLIER-comma-sibling case #617 had already settled as an answered superset — conflating "not yet written" with "written elsewhere, on the page already."

  The BX hotfix told the two apart: `visibleAtJoin` is POSITIONAL over the whole census now, not per-item — anything the FROM clause has already declared, in a comma item or a join, is visible to a later ON; only what is written AFTER remains out of scope. Both of PostgreSQL's sentences still apply to what stays refused: a relation joined LATER is `missing FROM-clause entry`; a base table reachable only through an alias, named by its own hidden name, is `invalid reference to FROM-clause entry`. Gated by `physical.TestArcRSAQualifiedReferenceNamesOneRelationInScope` and `coordinator.TestArcRSAQualifiedReferenceNamesOneRelationOnEveryArm`.

### E52

ADR lines 2015-2044. Catalog rows: r9. Stated in [Mechanisms](#mechanisms).

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

### E53

ADR lines 2045-2056. Catalog rows: r10. Stated in [Mechanisms](#mechanisms).

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
- (Narrowed 2026-10-06, #1524, ADR-0047 stage 1.) The binder records which
  column each reference names, and the group-key match compares those
  bindings: on the single-process engine `g` and `typemx.g` are one column,
  so the mirror answers PostgreSQL's rows (the alike spelling's, asserted by
  the same test's `boundary_qualified_key_alike_control`), and so does the
  bare-against-qualified pair over a JOIN (`coordinator.
  TestArcGKGroupKeySpellingEveryArm` j/*, ci1/innerJoin*). The stage-DAG
  arms plan an AST the coordinator does not bind and keep the 42803 until
  ADR-0047 stage 5.

### E54

ADR lines 2057-2119. Catalog rows: r19. Stated in [Mechanisms](#mechanisms). Moved to the log: [A10](../0012-amendments.md#a10), [A11](../0012-amendments.md#a11).

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

  *(Moved to the amendment log: [A10](../0012-amendments.md#a10).)*

  *(Moved to the amendment log: [A11](../0012-amendments.md#a11).)*

### E68

ADR lines 2725-2849. Moved to the log: [A14](../0012-amendments.md#a14).

  *(Moved to the amendment log: [A14](../0012-amendments.md#a14).)*

### E69

ADR lines 2850-2881. Moved to the log: [A15](../0012-amendments.md#a15).

  *(Moved to the amendment log: [A15](../0012-amendments.md#a15).)*

### E89

ADR lines 4238-4261. Catalog rows: r24. Stated in [Mechanisms](#mechanisms).

- **A QUALIFIED star over a block that publishes TWO columns of one name is
  REFUSED, where PostgreSQL answers the pair.** (Added 2026-09-13, the derived column publication —
  a WRONG → LOUD move, measured at `0193c4e9`.) `SELECT x.* FROM (SELECT
  a.id, b.id FROM lat_item a JOIN lat_item b …) x` published the FIRST `id`
  twice on the single-process arms (`1,1 | 1,1 | …` for PostgreSQL's
  `1,1 | 1,2 | …`), the block's inner join stream on the DAG arms, and
  `(SELECT order_id AS k, amount AS k …)` published a wrong TYPE with it.
  The expansion emits one column REFERENCE per published column, and two
  references spelled alike both bind the first column of that name, so the
  list is one this pass cannot state and the star is refused (`0A000`). The
  BARE star over the same block reads the relation by POSITION and answers
  PostgreSQL's pair, which is what makes this the qualified spelling's own.
  Closing it needs the same positional list as the entry above. The
  explicit-list spelling (`SELECT x.id FROM (SELECT a.id, b.id …) x`) still
  ANSWERS where PostgreSQL raises `42702 column reference "id" is
  ambiguous`, and that half is open. Gated as a refusal in
  `coordinator.TestArcO2ADerivedBlockPublishesItsVisibleList`'s `o2Refuses`,
  four cells on five arms, and — for the spelling where the duplicate comes
  from a body the block did not write, `(SELECT * FROM lat_ord o JOIN
  lat_item li ON …) d` publishing `id` in positions 1 and 4 —
  `coordinator.TestArcJ1AQualifiedStarExpandsFromTheRelationsOutput`'s
  `a-derived-table-whose-body-is-a-star-over-a-join`, with the
  distinct-names control beside it.
