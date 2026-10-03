# ADR-0012 amendment log

The dated amendments ADR-0012 §5 carried as prose until 2026-09-29, moved here verbatim, in date order. Each block names the [catalog](0012-divergences/README.md) rows it created or changed; the current position is always the catalog row, and a block here is its history.

## Moved blocks

### E00

The §5 header note (ADR lines 57-61 at 0da8399a); it opened the list the catalog now holds.

5. **Deliberate divergences from PostgreSQL.** (Amended 2026-08-23: collation
   was not the only one — the other three below were never PostgreSQL's call
   to begin with, and are recorded here so a future gate does not mistake
   them for undecided.)

### A12

Catalog: [Network types](0012-divergences/network.md#catalog) rows r6, r7; from entry [E61](0012-divergences/network.md#e61), ADR lines 2547-2573 at 0da8399a; dated 2026-09-05.

(Amended 2026-09-05 after review.) The refusal is decided at TYPING time,
for every type and every site. The first version of this arc put the
0A000 in one evaluator — `exec.networkConstError`, which fires only when
the vectorized filter declines to build a kernel — and left every other
evaluator reading the widened accept-set, so the same query refused in a
WHERE clause, ANSWERED inside a CASE, and on the DAG answered a WRONG
NUMBER: `c_ipv4 > '10.0.1/24'` gave every non-null row, because the
prefix was read as the address zero. A disposition that depends on which
evaluator a plan happens to choose is not a disposition.

So `kernel.QuotedLitStatus` has an IPv4/IPv6 arm too, and the two classes
are separated where the literal is classified rather than where it is
compared:

  'zzz'        names no address at all                   22P02
  '10/8'       names a NETWORK a bare-address type
               has no room for                           0A000

A maskless abbreviation (`'192.168'`) is the SYNTAX class, because
`'192.168'::inet` is a 22P02 on the server — the abbreviation without a
mask is a cidr-only grammar there.
`kernel.TestNetworkLiteralRefusalIsOnePredicate` asserts both classes,
`physical.TestPlanTimeNeverRefusesPGValidNetworkLiteral` asserts that a
SYNTAX refusal never eats PG-valid text AND that a representability
refusal is 0A000 at all six sites, and
`coordinator.TestArcF3ExprTypingOnEveryArm`'s census asserts the SQLSTATE
on three arms across nine sites.

### A13

Catalog: [Numbers and DECIMAL](0012-divergences/numeric-decimal.md#catalog) rows r18; from entry [E64](0012-divergences/numeric-decimal.md#e64), ADR lines 2663-2682 at 0da8399a; dated 2026-09-05.

(Corrected 2026-09-05 after review. The first version of this entry named
the stage boundary as the seam and said a stage's output materializes to
PARQUET, whose DECIMAL leaf carries one scale. Both halves are wrong: a
stage's output is `.wshf`, wadjet's own versioned exchange format, whose
schema header already carries per-column DECIMAL `Scale` and `Precision`
— and the per-row scale demonstrably DOES cross that boundary today when
it rides in a value's text, since
`CAST(COALESCE(d152, 12.3456789012345) AS VARCHAR)` prints PostgreSQL's
exact per-row text on the single, dag and dag-shuffled arms alike. An ADR
may not describe a mechanism the code does not have.)

The deferral stands on COST, which is the honest reason: a per-value
dscale touches 74 non-test `DecimalData.Scale` sites across more than
thirty files — the group-key encoder, the sort keys, the spill format,
the WSHF writer, the container codec and the gather among them — and a
widened declared scale cannot produce both `12.75` and `12.3456789012345`
in one column, while trimming trailing zeros would move a right cell to a
wrong one (the server prints `1.00` for a scale-2 column row). It is its
own arc. Pinned by `coordinator.TestLiteralScaleInADecimalFold`.

### A01

Catalog: [Aggregates and windows](0012-divergences/aggregates-windows.md#catalog) rows r4; from entry [E17](0012-divergences/aggregates-windows.md#e17), ADR lines 634-711 at 0da8399a; dated 2026-09-07.

- **CLOSED 2026-09-07 (#987, arc K2): a window SUM/AVG over an INTEGER
  column is EXACT and declares what PostgreSQL declares.** The entry below
  is kept as the record of what the divergence was and how it was measured;
  it is no longer a divergence, and the pins that held it are deleted.

  The repair is one table and one accumulator.
  `exec.IntegerAccOutputType` is now the single rule — PostgreSQL's
  `sum(int4) -> bigint`, `sum(int8) -> numeric`, `avg(int*) -> numeric` —
  and four sites ask it: the grouped aggregate's declaration
  (`physical.aggIntegerOutputType`), the window's declaration
  (`physical.windowSpecOutputType`), the window operator's runtime
  correction of that declaration (`exec.windowAccOutputType`) and the
  grouped carrier predicate (`exec.aggIntExact`).
  `exec.windowExactFrames` accumulates an integer input in the same Int128
  carrier `kernel.sumRowInt64Decimal` uses for the grouped spelling, read
  through a per-type cell reader resolved once per partition, in EVERY
  frame form — `OVER ()`, `PARTITION BY`, a running `ORDER BY` frame, a
  sliding `ROWS`/`RANGE` frame including its exit SUBTRACTION — and in both
  spilled evaluators (`window_global.go`'s two passes and
  `window_external.go`'s walker, which re-enters the same code). A total
  the declaration cannot hold is 22003, which is PostgreSQL's own SQLSTATE
  for `bigint out of range`, measured live.

  The census below now asserts PostgreSQL's own types and digits on all
  four arms, plus the three frame forms the filing did not name; the two
  `TestF1AWindowDeclaresTheSameTypeThroughADerivedTable` pins are gone; and
  `exec.TestTheWindowAndGroupedIntegerSumWriteTheSameValue` is the seam that
  makes a future drift between the two producers a failing test rather than
  a report.

  A **COMPUTED** argument takes the same rule, read from the ARGUMENT's own
  width rather than from the column it is materialized into (added
  2026-09-08, #987 review B1). PostgreSQL's SUM rule is by input width, and
  it reads the EXPRESSION: `sum(CASE WHEN … THEN 1 ELSE 0 END)` is bigint
  there because the CASE is int4, which is TPC-H Q12's shape. Every integer
  expression in this engine computes in int64 (ADR-0024's recorded
  widening), so the materialized column declares INT64 for int4 arithmetic
  and int8 arithmetic alike and cannot answer the question. The GROUPED
  path walks the argument's AST for it (`physical.aggComputedInputDecl`
  over `aggInputIsWideInteger`); the window did not carry an AST at all,
  fell to the float8 fallback at plan time and was widened to
  `DECIMAL(38,0)` by the operator — so six expressions went out under OID
  1700 windowed and OID 20 grouped, which is this same entry's defect with
  the spellings' roles swapped. `logical.WindowExpr` now carries
  `InputExpr` as `logical.AggExpr` already did, and
  `physical.windowComputedArgDecl` asks the same two functions over the
  same declarations. One `int8` operand anywhere in the expression makes
  the whole of it numeric, in both spellings, as PostgreSQL does.

  A **CAST is an operand whose width is its TARGET's** (added 2026-09-08,
  #987 review round 3, B1). `aggInputIsWideInteger` had no `CastNode` arm,
  so it fell off its end and read every int8 operand written under a cast
  as int4: `SUM(bigint_col::bigint)` declared bigint in BOTH spellings
  where PostgreSQL declares numeric — this arc's own defect one node
  deeper, and #841's grouped half since it shipped. It was a DISPOSITION
  move as well as a declaration: past int64 the bigint reading refused
  `22003` a query PostgreSQL answers, while the identical query one cast
  away answered it exactly, and "PostgreSQL answers and we refuse" is the
  direction this ADR does not allow — the permitted superset runs the
  other way. The arm reads the TARGET NAME
  (`physical.castTargetIsWideInteger`), not `nodeDeclaredType`'s answer:
  every integer cast spelling lands on INT64 there (item 12's recorded OID
  divergence), which cannot tell `::int4` from `::bigint`. A non-integer
  target leaves the integer table, so `SUM(x::numeric)` is numeric and
  `SUM(x::float8)` double, as PostgreSQL has them. Gated in both spellings
  by the census's cast cells — including a 10^5-row total that now ANSWERS
  — and by the pgwire OID gate's seven cast entries. Of the census's
  TWELVE cast cells EIGHT fail when the arm is disabled alone, and of the
  wire gate's seven FOUR do; the rest are controls that cannot move,
  because a NARROWING cast, a cast to a non-integer target and an UNCAST
  column answer the same either way — which is what makes them controls.

  The operator's runtime correction honors a bigint declaration over an
  int64-carried input for that reason: no VECTOR can tell `SUM(i32 * 1)`
  from `SUM(i64 * 1)`, only the plan can. A total that does not fit the
  declaration is 22003, never a wrapped number — loud in the one direction
  the inference could be wrong.

### A14

Catalog: [Names, scopes, joins and stars](0012-divergences/names-scopes.md#catalog) rows r27; from entry [E68](0012-divergences/names-scopes.md#e68), ADR lines 2725-2849 at 0da8399a; dated 2026-09-07.

- **CLOSED 2026-09-07 by arc K3 (#984): on the DISTRIBUTED arms `SELECT *`
  showed the STAGE's stream and not the query's projection.** A Project
  emits no stage, so a derived block's SELECT list was not a relation on the
  DAG and a star above the join published the Scan's or the Aggregate's own
  column list. Four spellings were silently wrong on both DAG arms — a
  source column published TWICE (`order_id, order_id AS oid` lost `oid`), a
  RENAME (`order_id AS k` published as `order_id`), an ALIAS OVER AN
  AGGREGATE (`CAST(COUNT(*) AS VARCHAR) AS n` published `__agg_0` to the
  client beside `n`) and a COMPUTED item (`amount * 2 AS d` published the
  source `amount` too, because the materialization that computes it is
  deliberately additive). A named SELECT list over every one of those blocks
  was right on all four arms throughout, because each consumer resolves its
  own column; the star is the consumer with no names.

  A derived block a star reads now emits its PROJECTION as the stage's
  column set — by position, under the block's own names
  (`physical.starReadBlockProjections` / `publishBlockProjection`,
  `Stage.ProjectExprs`) — and the join's keys and its OutputFilter bind to
  what the producer publishes rather than to the source the alias once
  resolved to. Gated in
  `coordinator.TestArcK3ADerivedBlockPublishesItsOwnProjection` — five
  spellings plus seven controls, four arms, every `want` PostgreSQL 17's
  column set and values.

  THE ROUTE THAT STOOD IN FOR IT IS RETRIGGERED, NOT DELETED.
  `ErrLateralProjectionDistributed` and
  `Coordinator.LateralProjectionLocalRoutes` used to fire on a NAME test
  over every lateral whose block left its stream behind; they now fire on
  what the pass DID — the marked blocks no stage could be made to carry.
  Because that route is NOT answer-preserving (the coordinator-local
  pipeline's ORDER BY is wrong for shapes the DAG gets right), it may take
  only what was already wrong or loud, and the residue is TWO shapes,
  listed with their `bb8635a4` disposition in ADR-0026 §7: a BARE AGGREGATE
  alias beside a computed sibling (silently wrong there) and a WINDOW inside
  the block (loud there). Both answer PostgreSQL on the local pipeline,
  asserted by counter in
  `coordinator.TestArcK3ADerivedBlockPublishesItsOwnProjection`. A block's
  item TYPE is not a disposition: the projection is typed by the same
  inference the single path uses to declare that block, so a container
  (over a column, a filtered scan, an aggregate, a lateral or a set-op
  arm), a scalar-subquery item, an all-NULL `CASE` and a bare `NULL` all
  execute — swept over the whole type-matrix corpus in
  `coordinator.TestArcK3NoBlockItemKindRoutesSilently`.

  WHAT REMAINS a divergence is the star's column ORDER, which is older and
  independent: this engine publishes the JOIN OPERATOR's order (probe side
  first) where PostgreSQL publishes the FROM order, and for some shapes the
  two distribution paths choose different probe sides, so `single` and `dag`
  order one relation two ways. Same set, same values. Pinned per arm in that
  gate's `ctl/derived-aggregate-is-its-stream` cell and in the INNER cells of
  `coordinator.TestArcJ1APublishedKeyIsAUserColumn`.

  CORRECTED 2026-09-08 by arc L1 (#997): "same NAMES" was wrong, and the
  order and the names are ONE divergence rather than two. The join qualifies
  the BUILD side's duplicate columns by their owning alias, and which side
  builds is a cost decision — `logical.reorderJoins` expresses it by
  SWAPPING the join node's two children and `physical.buildJoin` reads
  `Children[0]` as probe and `Children[1]` as build — so `SELECT * FROM t a
  JOIN t b ON a.id = b.id` publishes `…, b.id, b.order_id, …` with no
  predicate and `…, a.id, a.order_id, …` under `WHERE a.id < 100`, on all
  four arms. The SAME STATEMENT's RowDescription changes with the data. A
  client that keys on column labels (JDBC by label, DataGrip, Superset) sees
  two schemas for one query. PostgreSQL publishes the FROM clause's arms in
  written order and keeps duplicate names by POSITION, never qualified.

  CLOSED 2026-09-13 by arc O1 (#997, #1012, #993), and NOT by the
  build-side mark this paragraph proposed — `reorderJoins` swaps only a
  TWO-relation chain and `costBasedJoinReorder` REBUILDS a longer one, so
  there is no node whose children a mark could be relative to. The star is
  EXPANDED into the FROM clause's arms in written order at Optimize step 1,
  before any pass that reorders a join, each item a qualified reference
  published under the column's own name (ADR-0026 §9): the order and the
  names are the QUERY's on all five arms and on the wire, duplicates kept
  by position, and `markCoPathingSelfJoinBuilds`'s arm-specific
  qualification is no longer a published name either. The census that
  pinned it is deleted; the rule is gated by
  `coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan` (74 shapes
  × five arms, varying the join kind, the predicate, the FROM order, the
  star spelling, a derived arm's ROOT and its ITEM KIND) and
  `pgwire.TestO1TheWireDeclaresAStarJoinsOwnArms` (names and type OIDs).

  WHAT STILL PUBLISHES THE PLAN'S ORDER is every FROM item whose own names
  do not address its columns, and the list is in ADR-0026 §9's decline
  table: a block whose two items RESOLVE to one name (`s.id` binds the
  first, so the second column would carry the first's values); an arm that
  is a SET OPERATION, whose columns reach the join under the scan's own
  qualifier; a LATERAL arm; and a table function. Each keeps the VALUES it
  had — a wrong name rather than a wrong value — and each has a cell in
  that gate. Closing the resolve-collision class needs a block's column
  addressed by POSITION.

  An UNALIASED item in an arm — an expression, an aggregate, a literal or a
  CAST — was in that list and is not any more: a star item carries ADR-0026
  §9's PAIR, referencing the producer's spelling (`count(*)`) and publishing
  PostgreSQL's name (`count`), so those arms answer PostgreSQL's value under
  PostgreSQL's name and OID on all five arms and on the wire.

  The deferral this paragraph recorded read: the build side has to become a
  PROPERTY of the join node, with the children left in the query's written
  order and
  `repairDecorrelatedSpelling`, `inner_key_spelling.go`,
  `dedupSemiAntiBuildSide`, `physical.buildJoin`, `walkStages`, the worker
  fragment builder and `exec.joinOutputSchemaWithMapping` all reading that
  property. Pinned per predicate — none, selective, zero-row — in
  `coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan`, so the
  arc's proof is deleting the pin.

  EXTENDED 2026-09-08 by arc M1 (#993): there is a SECOND producer of the
  same divergence, and it splits the two DISTRIBUTED arms from each other.
  `physical.markCoPathingSelfJoinBuilds` sets `Stage.QualifyAllBuildCols`
  when two joins in one chain BUILD over the same table (Q07's self-join
  rule), and it reads each join's BUILD dependency out of the ARM's stage
  DAG — so over `SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id =
  o.id JOIN lat_ord o2 ON o2.id = i.order_id` the broadcast arm finds ONE
  `lat_ord` build and the shuffle arm finds TWO, and `dagshuf` publishes
  `o.customer, o.total` where `single`, `spilled` and `dag` publish them
  bare. PostgreSQL publishes every name bare, the three FROM arms in written
  order. Disabling that pass in place makes all four arms agree;
  `WADJET_STAGE_FUSION=0` does not change it, so the fusion passes are not
  the cause. It reproduces with NO derived block in the statement, which is
  what says arc K3's "a block whose body is a JOIN is never marked" boundary
  is not the condition. Pinned per DAG arm, in both spellings, in the same
  gate; it rides #997's arc because it is the same rule — which side builds
  is a cost decision and must not decide a NAME.

### A15

Catalog: [Names, scopes, joins and stars](0012-divergences/names-scopes.md#catalog) (history only; no live row); from entry [E69](0012-divergences/names-scopes.md#e69), ADR lines 2850-2881 at 0da8399a; dated 2026-09-07.

- **CLOSED 2026-09-07 by arc K3 (#978): a zero-row `SELECT *` over a JOIN
  carried no columns at all**, on every arm and on the wire. `SELECT *`
  over a join has no Project for the declaration walk to read and no single
  scan for it to describe, so a result WITH rows was described from the
  first batch and one without rows by nothing: psql printed no header,
  pgJDBC's executeQuery had no column metadata, and the extended protocol
  answered a parameterized Describe with NoData. It is the shape #846
  deferred, and it was NOT a distributed property — the single-process arms
  did it too.

  The declaration calls the join operator's OWN namer
  (`exec.JoinOutputSchema`) rather than reproducing it, which is why it can
  be made at all: probe columns then build columns, duplicates qualified by
  their owning alias, minus the columns the join materialized for itself.
  Bounded to ONE join whose sides hold no join of their own, because
  `declaredJoinSchema` walks a nested join by concatenating its sides, which
  is not the operator's rule; and it declines outright if any name in the
  answer is in the reserved namespace, which is proof the walk stopped below
  an operator that still had work to do. EACH SIDE IS DECLARED BY WHAT IT
  PUBLISHES: a derived block is a real relation on both paths, so reading
  the scan below it invented `s.id`, `product` and `qty` into the
  `RowDescription` of `SELECT * FROM kord o JOIN (SELECT order_id, amount
  FROM kitem WHERE …) s` — ten fields where PostgreSQL and the non-empty
  twin describe seven — and dropped a rename's alias entirely. The same list
  shapes an EMPTY side of a join, where reading the scan padded a LEFT
  join's unmatched rows with eight columns for PostgreSQL's five on the
  single-process arms. Gated by the four join shapes added
  to `pgwire.TestZeroRowSelectDescribesLikeItsNonEmptyTwin`, each paired with
  the SAME statement under a predicate that matches — a join's side order is
  a cost decision, so dropping the predicate would compare two relations
  rather than one relation twice.

### A16

Catalog: [LATERAL and subqueries](0012-divergences/lateral-subqueries.md#catalog) (history only; no live row); from entry [E86](0012-divergences/lateral-subqueries.md#e86), ADR lines 4161-4168 at 0da8399a; dated 2026-09-07.

- **WITHDRAWN the same day (arc J1 round 3): the refusal of `SELECT *` over
  a LATERAL whose ungrouped COUNT can see no rows.** It fired on the SHAPE,
  and a plan-time refusal cannot know the data — it refused queries whose
  outer rows all match, which had answered correctly one commit earlier. The
  empty-input value rides on the lateral's own OUTPUT COLUMN now
  (`exec.LateralEmptyDefault`, #977), so every reader of that column sees the
  0 and there is no divergence to record.

### A10

Catalog: [Names, scopes, joins and stars](0012-divergences/names-scopes.md#catalog) (history only; no live row); from entry [E54](0012-divergences/names-scopes.md#e54), ADR lines 2069-2078 at 0da8399a; dated 2026-09-13.

**The FIRST of the three is CLOSED (2026-09-13, arc O1, #997/#1012): a
star over a join IS expanded now**, into the FROM clause's arms in
written order (ADR-0026 §9), so a positional ORDER BY has a list to count
and `ResolveStarJoinOrdinalSortKeys` answers it in the item's SOURCE
spelling (the sort reads the join's stream, where a qualified reference
names one column and the published name may name two). Gated by
`coordinator.TestOrderByResolvesAPositionAfterTheStarExpands`'s
`boundary_star_over_join` cell, which used to pin the refusal, and by the
O1 gate's positional cells on five arms.

### A17

Catalog: [LATERAL and subqueries](0012-divergences/lateral-subqueries.md#catalog) (history only; no live row); from entry [E87](0012-divergences/lateral-subqueries.md#e87), ADR lines 4183-4201 at 0da8399a; dated 2026-09-13.

**The LATERAL's own star is no longer among them — CLOSED 2026-09-13 by
arc O2.** It published the whole join until #979, was REFUSED from then
until this arc, and publishes the body's own SELECT list now: the
correlation slot the join is about to drop is identified by
`Node.HiddenJoinCols` — the same identity the drop itself uses (ADR-0026
§3c) — so what the star publishes is exactly the columns the query wrote,
which is what PostgreSQL publishes. The divergence this entry recorded is
DELETED rather than pinned, and the same walk closed the neighbouring
refusals: a derived table or CTE carrying its own `ORDER BY`, `LIMIT` or
`DISTINCT` answers `x.*` too, because none of those changes a column or
its position. Gated by
`coordinator.TestArcJ1AQualifiedStarExpandsFromTheRelationsOutput`,
`coordinator.TestArcK1AStarIsItsSourceInItsPosition`,
`coordinator.TestArcO2ADerivedBlockPublishesItsVisibleList` (the `qstar`
column of every block class), `pgwire.TestArcJ1AHiddenSlotIsNotInTheRowDescription`
and — for the policed list, which is the half a star must never widen —
`server.TestPolicyMaskingIsPlanTimeOnEveryDoor`'s three
`a_laterals_own_star_*` cells.

### A02

Catalog: [Aggregates and windows](0012-divergences/aggregates-windows.md#catalog) (history only; no live row); from entry [E18](0012-divergences/aggregates-windows.md#e18), ADR lines 768-779 at 0da8399a; dated 2026-09-14.

**That FLOAT32 cell's VALUE half CLOSED 2026-09-14 (arc NV, #950).**
`sum(real)` is `real` on PostgreSQL — float4pl — and every arm of this
engine accumulates at that width now, grouped, distinct, through a
derived table and windowed, so the three answers are one and it is the
server's: `1.6777224e+07`. `avg(real)` stays double precision, which is
PostgreSQL's (#760). What remains OPEN is the window column's
DECLARATION: `SUM(real) OVER ()` still describes itself float8 where the
server declares real. The digits are the grouped spelling's, widened —
a declaration, not a value — and moving it is the window-output typing
arc ND owns. The census cell `813 SUM(real) OVER ()` now carries
PostgreSQL's VALUE and names the declaration in its `why`.

### A06

Catalog: [Aggregates and windows](0012-divergences/aggregates-windows.md#catalog) rows r5, r6; from entry [E21](0012-divergences/aggregates-windows.md#e21), ADR lines 941-978 at 0da8399a; dated 2026-09-14.

**That gap CLOSED 2026-09-14 (arc NV, #1000), and it was not only a
type.** The mechanism was one layer below the declaration:
`expr.operandIsInt` kept the network types on the FLOAT path and
`physical.intArithAllInt` mirrored that predicate so a declaration could
never promise an integer the kernel would not produce. What it cost
besides the OID was a VALUE — `c_proto / 2` answered 127.5, because
integer division truncates and this was not integer division — and a
NULL: `SUM(-c_port) OVER ()` answered nothing at all.

A port is constrained to 0..65535 at the TYPE boundary only. Arithmetic
over one is plain int4 arithmetic and its RESULT is an integer, exactly
as `smallint + 1` is `integer` in PostgreSQL, so the kernel and the
declaration moved together: `physical.intArithColumnType` is the one set
both read, `expr.NumericDomainResult` answers ABS and MOD in the integer
domain, and `nodeDeclaredType` declares a negated port an integer. The
ADDRESS types and the temporal ones are untouched — their arithmetic
means something else. Eleven pins flipped and were deleted: six in
`coordinator.TestH2TheWindowDeclaredTypeCensus` and five in
`pgwire.TestAComputedIntegerWindowArgumentDeclaresPostgresOID`. Four
cells replaced them with the shapes the filing named, `c_proto % 3` as a
GROUP BY key now declares INT64 rather than FLOAT64, and the wire gate's
`port_bare` and `protocol_bare` stay as the controls they always were.

What was there before was worse than a wrong type. `TypeProtocol` had no
arm in `kernel.ResolveRowSum`, `exec.isFlatSumType` or the SoA scatter's
SUM/AVG dispatch — only in MIN/MAX — so the GROUPED `SUM(c_proto)`
answered **NULL** over the type-matrix table while the WINDOWED spelling
answered 621435. Two spellings of one question, one of them silently
empty. `coordinator.TestH2TheWindowDeclaredTypeCensus` now asserts both
spellings of `SUM`/`AVG` over PORT and PROTOCOL on all four arms.

DATE, TIMESTAMP and DURATION are deliberately outside this rule, and the
census asserts that from the other side. `date` and `timestamp` are their
own wire types with no PostgreSQL `sum` at all, and an interval's sum is
an interval rather than a number, so all three keep the float64 both
spellings already agreed on. DURATION declares `bigint` on the wire and so
has an argument for `numeric`; that is a filing candidate, not a change
made here.

### A03

Catalog: [Aggregates and windows](0012-divergences/aggregates-windows.md#catalog) (history only; no live row); from entry [E18](0012-divergences/aggregates-windows.md#e18), ADR lines 780-795 at 0da8399a; dated 2026-09-15.

**That DECLARATION half CLOSED 2026-09-15 (arc ND, #1118).**
`SUM(real) OVER (…)` declares real (OID 700) on every arm, which is what
the grouped spelling has declared since #950 and what the server declares
for both. Three layers had to agree — `physical.windowSpecOutputType`,
`exec.windowAccOutputType` and the operator's own output-vector guard,
which refused to write into any vector but a float64's — and the writer
the three float SUM/AVG evaluators share is `exec.windowWriteFloat`.
`AVG(real) OVER ()` stays double precision, which is the server's type
for it. The pin in `pgwire.TestAMovingFloatWindowFrameCarriesTheRecomputedValue`
is DELETED, and `coordinator.TestNDDeclarationsMatchPostgres`'s `1118/*`
cells are what hold it.

One consequence is recorded rather than left to be found: a real total
carries 24 bits, so its last digits move with the ORDER the partials fold
in. ADR-0013's nondeterminism class 9, amended 2026-09-14 for that
carrier.

### A04

Catalog: [Numbers and DECIMAL](0012-divergences/numeric-decimal.md#catalog) (history only; no live row); from entry [E19](0012-divergences/numeric-decimal.md#e19), ADR lines 825-848 at 0da8399a; dated 2026-09-15.

**CLOSED 2026-09-15 (arc ND, #1117), and the DECLARATION was the whole
of it.** `real op real` declares real, and the exact sum, difference or
product of two float32s is representable in a float64 — so rounding the
carrier's result once into a float4 output vector IS the correctly
rounded float4 answer. No float32 kernel was needed. All three rows
above now answer PostgreSQL's: `1.6777216e+07`, `22003 value out of
range: overflow`, `2e+38`.

The range rule came with it, because a narrower declaration without one
is a silent ±Infinity. `batch.FloatRangeError` is IntegerRangeError's
float sibling at the same seam — the STORE — carrying PostgreSQL's own
two sentences, `value out of range: overflow` for a FINITE float64 with
no float32 and `underflow` for a non-zero one that narrows to zero, both
measured on 17.11. The operand exemptions are float_range.go's: an
infinity that ARRIVES is a value.

The rule is EXACTLY that pairing, which is PostgreSQL's own: `real + 1.0`
(a numeric literal), `real + 1` (an integer) and `real + float8` are all
double precision there, and three control cells beside the fix keep it
from widening. `-real` is real; `sum(real)` over a real EXPRESSION is
real, which is what `aggOutputFromInputDecl`'s new arm says; `avg(real)`
is double precision. `%` is untouched — PostgreSQL has no float modulo,
so there is no server type to follow.

### A05

Catalog: [Numbers and DECIMAL](0012-divergences/numeric-decimal.md#catalog) rows r6, r7, r8, r9; from entry [E19](0012-divergences/numeric-decimal.md#e19), ADR lines 849-902 at 0da8399a; dated 2026-09-15.

**Amended 2026-09-15 (arc ND round 2): the declaration is the whole of it
for a PROJECTED value, and for nothing else.** The rounding IS the store
into the float4 output vector the declaration names, so it happens
exactly where a projection materializes the expression. Three positions
have no such store. All were measured on 17.11 over the same row:

```
(r + 1.0::real) + 1.0::real             PG 16777216   wadjet 16777218
WHERE r + 1.0::real > 16777216::real    PG no rows    wadjet one row
WHERE r + 1.0::real = 16777216::real    PG one row    wadjet no rows
CASE WHEN r + 1.0::real = 16777216::real  PG 'eq'     wadjet 'ne'
```

A NESTED step never reaches an output vector and a COMPARISON has none at
all, so the float64 carrier's digits reach the next operator; PostgreSQL
rounds at EVERY float4 operator. A predicate's ROW SET is unchanged from
before #1117 — it was wrong there too — but the row it selects now PRINTS
a number the predicate says it does not hold, which is the visible shape
of the same gap. Cells `1117/nested_real_arith`,
`1117/real_arith_in_a_predicate`, `…_in_an_equality_predicate`,
`…_as_the_only_filter` and `…_in_a_case_condition` carry it on five arms,
each with a `ctl_` twin that spells the `CAST` and gets PostgreSQL's
answer.

Closing it means rounding AT the operator, and the measured shape of that
is an AST rewrite wrapping every `real op real` node in `CAST(… AS REAL)`
— which is why spelling the cast by hand already answers correctly on all
five arms. It was built and backed out in round 2 because it lands on the
two LOCAL arms only: a worker compiles its fragment's expressions from
SQL text with no declaration walk, so the rewrite must travel in the
stage's emitted text too (projection specs, `FilterExprs`, aggregate and
window arguments, join keys), and a half-landed version replaces one
wrong answer with two different ones — the class this list exists to
prevent. Filed with the measurements.

The SET-OPERATION arm was the third position and it IS closed
(`setOpCastExpr`, 2026-09-15). A set operation REPLACES an arm's
declaration with the union's common type, so the store that rounds never
happened: `r + 1.0::real UNION ALL <a float8 arm>` answered 1.6777216e+07
on the two local arms and 1.6777217e+07 on the three stage arms — one
expression, two VALUES, which ADR-0013 admits for no total. The arm now
narrows to real before it widens, on the one rung of `setOpWiden`'s
ladder that loses a narrower type's rounding. Gated on six shapes × five
arms: `1117/setop_real_arm_*` in the declaration census and
`1117/real_arm_through_*` in the value gate.

ITEM 1 of the filing is CLOSED, and closed by measurement rather than by
a change: `CAST(SUM(x) OVER () AS BIGINT)` was reported as INT64
single-process and FLOAT64 on the stage DAG, and at 2e386378 both
spellings declare INT64 on all four arms and answer PostgreSQL's digits.
The DAG reaches that answer by REFUSING the plan and routing to the
coordinator's local pipeline, which the census asserts as a routing
counter beside the rows — so what closed is the DIVERGENCE, and the DAG
still never declares this expression itself.

### A09

Catalog: [Network types](0012-divergences/network.md#catalog) rows r1, r3, r8, r9, r10, r11, r12, r13, r14; from entry [E40](0012-divergences/network.md#e40), ADR lines 1488-1657 at 0da8399a; dated 2026-09-15.

- **CLOSED 2026-09-15 (#1092, #627, #986): a CAST to a network type reads
  its text.** The entry this replaces recorded that
  `CAST('abc' AS IPV4|IPV6|CIDR|MACADDR)` returned the text under a STRING
  declaration where PostgreSQL raises 22P02, and it gave the reason the fix
  had to wait: this ADR requires ONE text accept-set per type, in one
  function, and the network types had none — the writer's parsers, the
  comparison kernels' parsers and (for UUID) the CAST's own parser
  disagreed in BOTH directions. `parquet.NetworkTextValue` is that one
  accept-set now: measured against PostgreSQL 17.11's own input functions
  (`inet` for IPv4/IPv6/CIDR, `macaddr`, `uuid`) and read by the writer, the
  CAST, the plan-time literal classifier and the runtime comparison alike.
  `expr.TestCastToANetworkTypeStillPassesThrough` is deleted; its inverse,
  `expr.TestCastToANetworkTypeParsesItsOperand`, is the proof, and
  `wadjet.TestEveryNetworkTypeReadsOneTextGrammarAtEveryBoundary` walks the
  whole type × form × boundary table.

  What the repair CHANGED, and which was PostgreSQL's answer all along: the
  writer now takes `08-00-2b-01-02-03`, `0800.2b01.0203`, `08002b010203`,
  `a:b:c:d:e:f` and `{uuid}`; the comparison kernels now refuse
  `a-0eebc99…`, which no `uuid_in` spelling contains; a CIDR column
  validates its text at all, where `'zzz'` used to be written into one and
  ordered as raw bytes for the rest of its life; and an out-of-range macaddr
  octet is `22003 invalid octet value`, a different answer from a spelling
  the type cannot read.

  **Amended 2026-09-15 (round-2 review).** The first cut of the repair left
  three readers that were not PostgreSQL's still in the path, and the gate
  that was supposed to catch that listed the spellings the issues named
  rather than the grammar, so it passed with all three present. All three
  are closed and the gate is now generated from a live server — every
  documented ACCEPTED form and every documented REJECTED one, nine doors,
  3 133 cells. What was wrong: an IPV6 column read a maskless v4-shaped
  literal with Go's parser, so `'010.1.2.3'` was 22P02 and `'010.1.2.3/32'`
  was a value; a CIDR column validated its v6 mask with Go's, so `'::1/064'`
  was stored where the IPV6 column one function away refuses it; and
  `INSERT … VALUES`, `COPY` and `UPDATE` TRIMMED, so `' 10.0.0.1'` and
  `'<uuid> '` were stored at three doors and refused at every other.

  Four DELIBERATE readings were this family's residual. Two remain (1
  and 2); two are closed and say so:

  1. **`''` is absence at the embedded ingester and 22P02 at every SQL
     door.** An empty CSV or JSON field means NULL, which is what the Go
     map API's `""` has always meant; a SQL literal `''` is a value, and
     `''::inet` is 22P02 on 17.11. Both doors said absence before this arc.
  2. ~~**PORT and PROTOCOL have two domains.**~~ **SETTLED 2026-09-15
     (Derek).** They have one, and it is the TYPE's: a PORT is 0..65535 and
     a PROTOCOL 0..255. **The range is checked when a value ENTERS the type
     — by CAST or by WRITE — and nowhere else.** `CAST(70000 AS PORT)`,
     `CAST('70000' AS PORT)`, `CAST(-1 AS PROTOCOL)` and
     `CREATE TABLE p AS SELECT CAST(70000 AS PORT)` are all 22003 naming the
     value and the type, through the writer's own check
     (`parquet.NetworkIntRangeError`) so the two boundaries cannot drift.
     The Go-BOX door joined them in round 3 — `db.NewIngester().Ingest`
     with `int32(65536)` put that number at REST while the same value as
     text was refused — and the check lives at the leaf every box narrows
     through, asked BEFORE the carrier's width so the refusal names the
     same range at every magnitude.
     ARITHMETIC keeps int4's rules and may leave the range without error —
     `port + 70000` answers — which is PostgreSQL's `smallint + 1` rule and
     leaves #901's position intact: that ADR entry is about the arithmetic
     and the wire declaration, not about what a value may BE.

     What it replaces: the CAST held the int4 CARRIER's range, so
     `CAST(70000 AS PORT)` answered and the CTAS door carried it to REST —
     a PORT column holding 70000, which `INSERT` on the same table refuses
     (round-2 review N2, measured identical at base). Gated per type ×
     {in range, each edge, one past each edge, negative, from text, from
     int, from a wider int, from a float} in
     `expr.TestAValueEnteringPortOrProtocolIsHeldToTheTypesRange`, on five
     arms in `coordinator.TestNetworkTextGrammarAnswersTheSameOnEveryArm`,
     and on the wire in `pgwire.TestANetworkCastOnTheWire`.
  3. **CLOSED 2026-09-18 by arc EX (#1137): PORT and PROTOCOL beside a
     COLUMN read their OWN input function.** What this recorded: a
     comparison resolved an unknown literal against the column's declared
     wire type (OID 23, #834) — int4's whole grammar — so
     `WHERE c_proto = 'udp'` was 22P02 while `CAST('udp' AS PROTOCOL)`
     answered 17, and `WHERE c_port = '0x1bb'` MATCHED port 443. The five
     sites it named all read one function now
     (`kernel.NetworkIntLitText` over `parquet.NetworkTextValue`, the
     reader the CAST and every writer door already used), and the entry
     that states the settled rule is **PORT and PROTOCOL read their OWN
     input function** in §5's literal-resolution list below.
  4. **CLOSED 2026-09-18 by arc EX (#1141): a fractional value whose
     DECLARATION is text reads the destination's input function, whatever
     shape it arrives in.** (Narrowed twice 2026-09-15; closed
     2026-09-18.) What this recorded: the cast chose between the TYPE's
     input function and the numeric→int conversion by the operand's
     SHAPE, so a bare quoted literal and a STRING column took the text
     reader while a STRING-typed EXPRESSION took neither —
     `CAST(CONCAT('2','.5') AS PORT)`, `CAST(TRIM(s) AS PORT)`,
     `CAST(SUBSTRING(…) AS PORT)` and `CAST(CAST(2.5 AS TEXT) AS PORT)`
     answered 3 where PostgreSQL is 22P02, for PORT and PROTOCOL as well
     as INTEGER, and reached REST through `INSERT … SELECT` and CTAS.

     The SHAPE test is gone, and what replaced it is the DECLARATION:
     `expr.castOperandDeclaresText` asks the expression — a quoted
     literal, a STRING column, a cast to a text type, a call whose
     registered return type is fixed STRING, a container element, a MAP
     value, a scalar subquery, or a CASE/COALESCE all of whose arms are
     one of those — so `CAST(CONCAT('2','.5') AS PORT)`,
     `CAST(TRIM(s) AS PORT)` and `CAST(CAST(2.5 AS TEXT) AS PORT)` are
     22P02 while `CAST(d + 1 AS PORT)` over a DECIMAL column still ROUNDS
     to 4, which is PostgreSQL's numeric→int answer and the half a wider
     repair would have broken. The pin
     (`wadjet.TestAStringTypedExpressionCastToPortIsFC7sOpenCell`) is
     deleted, which is the proof; its positive form is
     `wadjet.TestAStringTypedExpressionCastToPortReadsTheTypesGrammar`,
     and the settled rule is **The CAST to an integer type is TWO casts**
     in §5 below.

     The WRITE doors were the first half of this, closed 2026-09-15:
     `CAST('2.5' AS PORT)`, `CAST(string_col AS PORT)`,
     `INSERT INTO t (port_col) SELECT '2.5'` and a CTAS over the cast all
     stored 3 while the VALUES, COPY, UPDATE and ingester doors said
     22P02. The assignment path coerces an unknown-typed literal with the
     TARGET's input function, which is the rule #1088 relies on
     everywhere else.

  **PostgreSQL-valid text a bare-address column has no room for is 0A000,
  ONE class at every door.** (Amended 2026-09-15.) It has two reasons — the
  literal names a NETWORK, or an address of the OTHER FAMILY — and the
  message says which. Before the amendment `'10/8'` at an IPV4 column was
  0A000 while `'::1'` was 22P02 and two v4-mapped spellings were not
  refused at all at the comparison door: one class, three answers. The
  plan-time classifier now reads the WRITER's classification rather than a
  second copy of it.

  **A mask whose text is not a value of the column's type stops a CAST.**
  (Recorded 2026-09-15.) An ABAC `mask_column` obligation replaces the
  column's expression with the mask, so `CAST(ip AS IPV4)` over a column
  masked `'***'` is `CAST('***' AS IPV4)` — 22P02, which is also
  PostgreSQL's answer for that text. No value leaks in either direction and
  a mask whose text IS a value of the type (`'0.0.0.0'`) flows through
  every door, but a report that casts a policed column stopped answering
  when the cast started parsing. The structural fix is at the policy
  boundary — a mask that cannot produce a value of the column's declared
  type is an unenforceable mask, which `auth.plan_enforce` already refuses
  other kinds of — and it is filed rather than patched at the cast, because
  making the cast hand a non-value back is exactly the pass-through #1092
  closed.

  **BOOL, BYTES and the containers refuse an unknown-typed literal in
  `INSERT … SELECT`** (Added 2026-09-15, #1088.) A bare quoted literal is
  SQL's `unknown` and is typed FROM the INSERT's target, so
  `INSERT INTO t (ip) SELECT '10.0.0.1'` is a value here as it is on the
  server. The targets whose TEXT no leaf in this writer reads — BOOL, BYTES,
  ARRAY, ROW, MAP, VECTOR — stay `42804` instead, because admitting one
  would replace a plan-time refusal with a flush-time box error. A literal
  reached through a UNION, a CTE or a derived table also stays 42804: it is
  typed by that construct's own fold before it meets the target, and the
  walk that marks unknown-typed positions answers only for a single SELECT
  block it can prove.

  **PORT and PROTOCOL have LEFT this family.** (Amended 2026-09-06, #901.)
  The paragraph's reason — "the network types have none" — was never true of
  those two: their carrier is a signed 32-bit field and their text is an
  INTEGER, which `kernel.IntLitText` is the engine's one accept-set for and
  which every other integer destination already reads. So `'443'::PORT` is
  443 under a PORT declaration (OID 23, the same OID a PORT column declares
  since #834), `'abc'::PORT` is `22P02 invalid input syntax for type
  integer`, and a value with no int32 is `22003 integer out of range` — all
  measured. `IPV4`, `IPV6`, `CIDR` and `MAC` followed them out of the family
  on 2026-09-15; the entry above records what replaced it.

  Before #901 all four of `INT32`, `PORT`, `PROTOCOL` and `FLOAT32` were
  accepted NAMES with no cast at all — `Cast.Eval`'s switch matched none of
  them and `physical.inferCastType` declared STRING — so they looked like
  this family from the outside while being the #310/#443 shape instead: a
  NUMBER published as text under OID 25.

### A07

Catalog: [Names, scopes, joins and stars](0012-divergences/names-scopes.md#catalog) (history only; no live row); from entry [E27](0012-divergences/names-scopes.md#e27), ADR lines 1087-1099 at 0da8399a; dated 2026-09-18.

- ~~**A `BETWEEN` of any spelling inside a `JOIN … ON` clause is
  refused.**~~ (Added 2026-09-18 by arc PS, #655/#1154; **CLOSED the same
  day by arc JR, #1178**.)

  `logical.splitOnAnd` split a rendered ON clause into conjuncts on the
  literal text `" AND "`, and `BETWEEN low AND high` carries one, so the
  fragments did not parse and the physical planner refused the clause. The
  four join sites split on the AST now (`logical.splitJoinConjuncts`), the
  way `physical.flattenJoinConjuncts` already did one layer down. Every
  spelling that carries the word AND inside ONE condition is a cell of
  `logical.TestSplitJoinConjunctsSplitsOnTheASTNotTheText`, and the
  end-to-end shapes are in `coordinator.TestJRAOuterJoinOnResidualsAgreeOnFiveArms`.

### A11

Catalog: [Names, scopes, joins and stars](0012-divergences/names-scopes.md#catalog) rows r19, r20; from entry [E54](0012-divergences/names-scopes.md#e54), ADR lines 2079-2119 at 0da8399a; dated 2026-09-18.

**The SECOND is CLOSED TOO (2026-09-18, arcs PS and SR).** USING MERGES
the joined column into one output column — a different rule from the
arms' concatenation — and `logical.usingJoinStarColumns` states it: the
USING columns once and first, then each arm's remaining columns, with the
merged VALUE being the side that is never NULL-extended and
`COALESCE(l.c, r.c)` for a FULL join. Arc SR then removed the last
decline that was not about a name being unnameable: a column name the two
arms share OUTSIDE the USING list is published TWICE, which is
PostgreSQL's answer. What is left refused (0A000) is an arm that
publishes one name twice, a CHAIN of USING joins, and an arm whose own
list this expansion cannot state — each one a shape where a reference by
name could not name its own column. Gated by
`coordinator.TestSRAStarPublishesItsArmsOwnColumns`'s `using/*` cells and
`pgwire.TestSRTheWireDeclaresAStarsOwnArms`.

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

**A SET OPERATION is countable and no longer refused** (2026-09-07, arc
K1, #982). `SELECT * FROM (SELECT … UNION ALL SELECT …) u ORDER BY 2` was
refused on every arm and PostgreSQL answers it; a set operation publishes
its LEFTMOST arm's names, which is the rule `plansql.BlockOutputColumns`
and `applyColumnAliases` already read, so `projectOutputNamesBelow`
descends it. A VALUES derived table is covered by the same step, since the
parser desugars `VALUES` into a UNION ALL of SELECTs. The third of
#810's shapes — a `USING` clause following another join on the same FROM
item, where the star must state a CHAIN of merges — is the one still
refused; the other two are closed above, which is the "lift them
together" prediction coming apart in the order the measurements allowed
rather than all at once. Gated by
`coordinator.TestArcK1AStarIsItsSourceInItsPosition`.

### A08

Catalog: [Names, scopes, joins and stars](0012-divergences/names-scopes.md#catalog) (history only; no live row); from entry [E33](0012-divergences/names-scopes.md#e33), ADR lines 1189-1204 at 0da8399a; dated 2026-09-20.

- ~~**`PARTITION BY <bare name>` over two join arms that both publish it is
  answered, not refused.**~~ (Added 2026-09-07, #975; **CLOSED 2026-09-20
  by arc RS, #1161/#1162.**) PostgreSQL raises 42702 `column reference "w"
  is ambiguous` for `SUM(y.w) OVER (PARTITION BY w)` where two FROM items
  publish `w`, verified live; wadjet bound one of them and answered.

  It closed as a consequence rather than as a target. A SELECT-list WINDOW
  item was the one spelling the binder never resolved a name in, so no
  window key reached the scope's ambiguity census at all. Now that a window
  key is resolved like any other reference, a bare name two of the block's
  own sources provide reaches the same `srcCount > 1` rule every other
  clause has used since #367, and the statement is 42702 — PostgreSQL's own
  answer, re-measured on 17.11. `coordinator.TestArcK1AWindowPartitionKeyBindsItsOwnArm`'s
  `975 ctl the BARE contested spelling PostgreSQL refuses` asserts the
  refusal now; the row set it used to record is gone, which is the proof.

## 2026-09-29: reconciled against the tip binary (arc DS)

Where the differences page and a §5 entry disagreed, or a §5 entry's own cell no longer described the engine, the cell was measured on the tip binary (the embedded server, `wadjet serve`, over its own file store) and the measured answer is what the catalog row says. The source entries keep their text as history. `winner` names which text the measurement bore out: `page`, `adr`, `neither` (both were stale), or `agrees-with-pg` (the divergence is gone on the embedded arm).

| family | page | entry | the page said | the entry said | cell | measured | winner |
|---|---|---|---|---|---|---|---|
| temporal | — | E59 | — | date_trunc RETURNS text (OID 25) here; a function result cannot be TIMESTAMP-typed | `SELECT DATE_TRUNC('day', CAST('2023-11-14 05:06:07' AS TIMESTAMP)) AS t \gdesc` | timestamp without time zone, value 2023-11-14 00:00:00 — meas_temporal_cells.log | neither |
| temporal | P057 | E38 | boolean/container temporal casts retain NULL where PostgreSQL raises 42846 | every other box that fails to parse, a boolean or a container, keeps NULL | `SELECT CAST(ARRAY[1] AS DATE)` | ERROR 42846 cannot cast type integer[] to date — meas_temporal_cells.log (CAST(true AS DATE) is still NULL) | agrees-with-pg |
| numeric-decimal | P025 | E78 | CAST(SUM(a) OVER () AS INTEGER) declares OID 20 | single arm declares int4, DAG arms int8 | `SELECT CAST(SUM(a) OVER () AS INTEGER) FROM ni \gdesc` | bigint; MAX(c_i32) + 0 bigint; CAST(a AS SMALLINT) bigint | page |
| numeric-decimal | P064 | E47 | NaN/Infinity into DECIMAL raises 22003 | INSERT's unchecked write path stores 0; CAST('NaN' AS DECIMAL(9,2)) yields the string NaN | `INSERT INTO nd VALUES ('NaN'); SELECT CAST('NaN' AS DECIMAL(9,2))` | ERROR 22003 for INSERT NaN, INSERT Infinity and the CAST | page |
| containers | P056 | E35 | the parenthesised spellings, qualified (x.c_row).b included, answer as PostgreSQL does | (x.c_row).b is REFUSED with 0A000 naming the derived-table workaround | `SELECT (x.c_row).b FROM n x ORDER BY x.id` | accepted, no error (planned and answered over the table; no ROW value could be inserted from SQL) — meas_containers_P056.log, meas_containers_P056b.log | agrees-with-pg |
| dml-assignment | P078 | — | BOOL, BYTES and container targets of a quoted INSERT SELECT raise 42804 | — | `INSERT INTO qb (f) SELECT 'true'; INSERT INTO tt (b) SELECT 'abc'; INSERT INTO qa (a) SELECT '{1,2}'` | BOOL stores t and BYTES stores \x616263 (both as PostgreSQL); ARRAY target ERROR 42804 — meas_dml-assignment_P078b.log, meas_dml-assignment_cells.log, meas_dml-assignment_P078.log | agrees-with-pg |
| names-scopes | P115 | A11 | an arm publishing one name twice raises 42702 naming the column | an arm that publishes one name twice is refused 0A000 | `SELECT * FROM (SELECT id, v AS x, v AS x FROM ua) s JOIN ub USING (id)` | ERROR 0A000 SELECT * over a JOIN ... USING is not supported for this shape | adr |
| lateral-subqueries | P152 | — | ordering quantifiers (x < ALL (SELECT ...)) raise 0A000; only equality forms implemented | (unlocated: no ADR-0012 entry) | `SELECT id FROM lt_o WHERE k < ALL (SELECT k FROM lt_i WHERE k > 1)` | ERROR 0A000 < ALL (subquery) is not supported; only = ANY and <> ALL over a subquery are; > ANY the same — meas_lateral-subqueries_P152.log | page |
| lateral-subqueries | P153 | E76 | derived-table outer references refuse 0A000 | a derived table in the body naming the enclosing row is the standing 0A000 class | `SELECT o.id FROM lt_o o WHERE EXISTS (SELECT 1 FROM (SELECT i.k FROM lt_i i WHERE i.k = o.k) d)` | ERROR 0A000 a derived table in FROM that references "o" from an enclosing query is not supported — meas_lateral-subqueries_P153.log | page |
| lateral-subqueries | P154 | — | containers, non-text BYTES and non-finite numbers lack reconstructable literals: 0A000 | (unlocated: no ADR-0012 entry) | `SELECT c.id, (SELECT c.f + x.v FROM lt_i2 x WHERE x.id = 1) FROM lt_c c` | Infinity FLOAT64 and BYTES \x00ff: 0A000; ARRAY outer values answer (cardinality(c.arr), c.arr = ARRAY[1,2], ORDER BY c.arr); (SELECT c.arr FROM x) is 42000 cannot store []interface {} into STRING vector — meas_lateral-subqueries_P154.log | neither |
| lateral-subqueries | P133 | — | a WITH item inside a correlated subquery shadowing an outer WITH item is 0A000 | (cites ADR-0021 §1r only) | `WITH d AS (SELECT 1 AS k) SELECT o.id FROM lt_o o WHERE EXISTS (WITH d AS (SELECT 2 AS k) SELECT 1 FROM d WHERE d.k = o.k)` | ERROR 0A000 a WITH item inside a correlated subquery that shadows an outer WITH item is not supported — meas_lateral-subqueries_P133.log | page |
| lateral-subqueries | P033 | — | body SELECT * names its columns s.id, s.k | (cites #1126; no entry in this family) | `SELECT * FROM lt_o o JOIN LATERAL (SELECT * FROM lt_i i WHERE i.k = o.k) s ON true` | columns id, k, total, s.id, s.k, v — meas_lateral-subqueries_P033.log | page |
| lateral-subqueries | — | E87 | — | a bare * over a JOIN is refused rather than guessed | `SELECT * FROM lt_o o JOIN lt_i i ON o.k = i.k` | answers id, k, total, id, k, v; SELECT x.* over a derived table whose body is that star is 0A000 — meas_lateral-subqueries_E87.log | neither |
| lateral-subqueries | — | E75 | — | an uncorrelated nested subquery beside an aggregate is declared FLOAT64 where PostgreSQL declares numeric | `SELECT u.id, (SELECT SUM(x.d) + (SELECT 1) FROM lt_d x WHERE x.k < u.k) FROM lt_o u` | declared numeric, values 2.25, 4.75 (also for SUM(x.d + u.k) + (SELECT 1)) — meas_lateral-subqueries_E75b.log | agrees-with-pg |
| lateral-subqueries | — | E28 | — | the refusal names the construct | `SELECT a.id FROM lt_o a LEFT JOIN lt_i b ON a.k = (SELECT max(k) FROM lt_i)` | ERROR 42000 building physical plan: join ON residual ... on a left join is not evaluable at the join — meas_lateral-subqueries_cells.log | neither |
| aggregates-windows | P023 | E16 | grouped MIN(real) declares double precision; integer declares integer | grouped form widens INT32 to INT64 and FLOAT32 to FLOAT64 | `SELECT MIN(c_real) FROM mm GROUP BY g; SELECT MIN(c_i32) FROM mm GROUP BY g` | real; integer (window spelling also real; integer) | neither |
| aggregates-windows | P001 | E21 | AVG declares numeric with no fixed modifier | AVG is numeric(38,4) as for int4 | `SELECT AVG(c_i32), AVG(d) FROM av` | numeric, numeric (no typmod); 1.6667 and 1.850000 | page |
| aggregates-windows | P090 | A06 | SUM/AVG/STDDEV accept DURATION as the int8 the wire declares | DURATION keeps float64 in both spellings | `SELECT SUM(dur), AVG(dur) FROM du \gdesc` | double precision, double precision; window SUM double precision | adr |
| aggregates-windows | P070 | A01 | computed SUM of unknown width stays bigint and raises 22003 past bigint | computed argument width read from the AST; one int8 operand makes it numeric | `SELECT SUM(b * 1), SUM(ABS(b)) FROM sb` | numeric 18000000000000000000 for both | adr |
| aggregates-windows | P151 | — | ORDER BY COUNT(*) * 2 raises 0A000 | unlocated | `SELECT g, COUNT(*) FROM ob GROUP BY g ORDER BY COUNT(*) * 2` | ERROR 0A000 | page |
| text-collation | — | E56 | — | table row: \gdesc of CAST('abcdef' AS VARCHAR(4)) is unconstrained STRING, OID 25 | `SELECT CAST('abcdef' AS VARCHAR(4)) AS v \gdesc` | character varying(4) — meas_text-collation_cells.log | neither |
| network | P084 | E55 | the CAST reads inet's grammar | CAST(<text> AS CIDR) is not implemented and passes its text through | `SELECT CAST('10' AS CIDR)` | ERROR 22P02 invalid input syntax for type inet; CAST('10/8' AS CIDR) answers — meas_network_P084.log | page |
| network | — | E55 | — | the MAC refusal is asserted at eq and IN only; case, is_distinct, greatest, least and an empty scan answer (#627 open) | `SELECT GREATEST(mac, '08:002b:010203') FROM nt` | ERROR 22P02 at eq, IN, CASE, IS DISTINCT FROM, GREATEST and a predicate no row reaches — meas_network_cells.log, meas_network_E55.log | agrees-with-pg |
| network | — | A09 | — | BOOL, BYTES and the containers refuse an unknown-typed literal in INSERT ... SELECT (42804) | `INSERT INTO nt4 (b) SELECT 'true'` | BOOL stores t and BYTES stores \x6162; ARRAY is still 42804 — meas_network_E55.log | agrees-with-pg |
| network | — | A09 | — | the 0A000 message says which reason: a network or an address of the other family | `SELECT id FROM nt WHERE ip = '::1'` | ERROR 0A000 a network prefix is not representable in an IPV4 column: "::1" — meas_network_P126.log | neither |
| network | — | E55 | — | '10/8' is 10.0.0.0/8 under inet's grammar (host bits kept) | `SELECT CAST('10/8' AS CIDR)` | answers 10/8 (as written); INSERT of '10/8' into a CIDR column reads back 10/8 and compares equal to 10.0.0.0/8 — meas_network_E55.log | neither |
| network | P017 | — | the address and UUID functions declare text and are read by a typed column's input function on assignment | (unlocated: #1254-siblings, no entry in this family) | `SELECT INT_TO_IP(167772161), UUID(), IP_ADD(ip, 1) FROM nt` | text, text, text; INSERT ... SELECT INT_TO_IP(...) stores 10.0.0.2 while CONCAT(...) is 42804 — meas_network_P017.log | page |
| extensions | P032 | E77 | (b).open over DECIMAL(9,2) keeps typmod 589830; PostgreSQL has -1 | field types are the server's for the aggregates the fields are (min of the price type) | `SELECT (b).open FROM (SELECT OHLCV(ts, price, size) AS b FROM trades) s` | numeric(9,2) (typmod 589830); vwap numeric(38,6) — meas_extensions_P032.log | page |
| extensions | P009 | E82 | 2147483647 << 2 gives 8589934588, declared bigint | 2147483647 << 2 is 8589934588 here | `SELECT 2147483647 << 2` | ERROR 42601 syntax error at or near "<"; BITWISE_LEFT_SHIFT(2147483647, 2) is 8589934588 — meas_extensions_P009.log | neither |
| extensions | P097 | E77 | only INTERVAL literals work (otherwise 42804) | the stride must be an INTERVAL literal (42804 otherwise); INTERVAL '1 day 6 hours' is not spellable | `SELECT TIME_BUCKET(CAST(s AS INTERVAL), ts) FROM tb2` | answers 2024-01-01 05:00:00; a text stride is 42804; INTERVAL '1 day 6 hours' is 22007 — meas_extensions_P097.log | neither |
| other | P047 | E58 | DURATION, BYTES, VECTOR and container destinations retain the operand | VECTOR(n), the containers and the network types are pass-through destinations that return the operand unchanged | `SELECT CAST('abc' AS VECTOR(3))` | ERROR 22P02 invalid input syntax for type vector; CAST('abc' AS INTEGER[]) 22P02; DURATION and BYTES still return abc | neither |
| other | P028 | E58 | CAST(ARRAY[1,2] AS VECTOR(2)) converts as pgvector's cast does | a VECTOR(n) destination returns its operand unchanged | `SELECT CAST(ARRAY[1,2] AS VECTOR(2))` | [1,2] (converted; declared text); ARRAY[1,2,3] AS VECTOR(2) is 22000 | page |
| other | — | E58 | — | non-address text cast to IPV4/IPV6/CIDR/MACADDR returns its operand unchanged | `SELECT CAST('abc' AS IPV4)` | ERROR 22P02 invalid input syntax for type inet; IPV6 and CIDR the same, MACADDR 22P02 for type macaddr | neither |

Rows retired by the measurement (the divergence is gone on the embedded arm; a retired row id is not reused):

- aggregates-windows r12 (sources E16, P023): grouped and window MIN/MAX over REAL and INTEGER declare real and integer on the tip, as PostgreSQL does; the page entry P023 is removed.
- lateral-subqueries r6 (sources E75): SUM(decimal) + (SELECT 1) in a correlated body declares numeric with PostgreSQL values on the tip; E75 recorded FLOAT64.

## 2026-09-29: a DATE against a TIMESTAMP (arc DT, #1378)

PostgreSQL's `date = timestamp` promotes the DATE to its midnight; this engine's direct comparison did so for column and literal operands, and every carrier that turns the pair into a KEY did not: a membership, NOT IN, an EXISTS, LATERAL or JOIN key compared the TIMESTAMP's milliseconds since 1970 with the DATE's day number, so the issue's 2024 cells answered 0 rows on every arm, NOT IN kept the TIMESTAMPs at a DATE's midnight, and `1970-01-01 00:00:00.001` matched the DATE 1970-01-02 (in `DATE '1970-01-02' IN (SELECT TIMESTAMP '1970-01-01 00:00:00.001' …)` too). A DATE scalar subquery against a TIMESTAMP was read by a magnitude guess (an integer inside ±500 000 taken as a day count), so `ts = (SELECT d …)` for 1969-12-31 also matched 1969-12-31 23:59:59.999 on the single-process arms, and on all five when correlated. The pair now meets at TIMESTAMP through one rule and one conversion (`batch.TemporalCommonType`, `batch.DateMidnightMillis`) in the comparison kernel, the equi-join key ladder, the membership set and the stage DAG's inlined set and scalar, which closes those cells without a catalog row. A TIMESTAMP-typed bind parameter against a DATE is still read at DATE (#1426, a defect, not a divergence). The DATE / TIMESTAMP declaration a derived table, CTE or recursive CTE now publishes for a column a scalar subquery, a window call or the recursive CTE's non-recursive term computes is read by every operator, as a stored column's is: arithmetic, `sum` / `avg` and an integer key over such a column raise 42883 (at v0.25.2 they answered a number, a day count or 0 rows; r24 names the shapes). Two rows record what stays refused:

| family | row | change | gate |
|---|---|---|---|
| temporal | [r24](0012-divergences/temporal.md#catalog) | Added: CASE / COALESCE / GREATEST / LEAST mixing DATE and TIMESTAMP arms is refused 0A000 whatever the arm's shape (a column — of a table, a derived table, a CTE or recursive CTE (inside its own recursive term too), a join, a set operation, VALUES or a LATERAL output, whatever produced it — a literal, expression, scalar subquery, window call or aggregate); at v0.25.2 it answered a day count in a TIMESTAMP column, epoch milliseconds in a DATE one, or raised 22003 — or, for a TIMESTAMP-first fold whose DATE arm is a scalar subquery, an instant a bare projection printed as PostgreSQL does but that CAST, extract and + INTERVAL then read wrongly (`CAST(COALESCE(ts, (SELECT max(d) …)) AS VARCHAR)` answered epoch milliseconds); 13 such folds measured right in a bare projection at v0.25.2 are refused now | `coordinator.TestArcDTDateTimestampEveryArm`, `coordinator.TestArcDTR2ScalarAndFoldArmsEveryArm` |
| set-operations | [r7](0012-divergences/set-operations.md#catalog) | Amended: gated, with INTERSECT, EXCEPT and a mixed membership body named | `coordinator.TestArcDTDateTimestampEveryArm` |

## 2026-09-29: a window function's argument list (arc WA, #1394 #1399)

A bare literal VALUE argument to a window function was never materialized as an input column, so `SUM(2.5) OVER ()`, `FIRST_VALUE(2.5) OVER (…)`, `LAG(5) OVER (…)` and `SUM(2) OVER ()` answered NULL on every row and arm, and an INSERT … SELECT of one stored NULL; it is now materialized like any expression. The INTEGER argument (LAG / LEAD's offset, NTILE's and NTH_VALUE's n) was read by `strconv.Atoi` over its text, and every evaluator read an offset or n <= 0 as 1, so `LAG(x, 0)` answered the previous row, `LAG(x, -1)` the previous instead of the next, `LAG(x, NULL)`, `LAG(x, 1 + 1)` and `LAG(x, o)` answered as `LAG(x)`, and `NTILE(0)` answered 1. It is now typed as PostgreSQL types it, a constant expression is folded at plan time, and a per-row integer argument is refused.

| family | row | change | gate |
|---|---|---|---|
| aggregates-windows | [r18](0012-divergences/aggregates-windows.md#catalog) | Added: a text or NULL literal to FIRST_VALUE / LAST_VALUE / NTH_VALUE / LAG / LEAD answers where PostgreSQL raises 42804 (at v0.25.2 it answered NULL) | `coordinator.TestArcWAWindowArgumentsEveryArm` |
| aggregates-windows | [r19](0012-divergences/aggregates-windows.md#catalog) | Added: a per-row LAG / LEAD offset or NTILE / NTH_VALUE n (a column, an expression over one, a subquery) is refused 0A000 (at v0.25.2 it was read as the default 1) | `coordinator.TestArcWAWindowArgumentsEveryArm` |
| aggregates-windows | [r20](0012-divergences/aggregates-windows.md#catalog) | Added: a bound int8 / text / numeric parameter as the integer argument answers by its value where PostgreSQL raises 42883 (the same at v0.25.2) | `pgwire.TestAWindowArgumentParameterAnswers` |

## 2026-09-30: a scalar subquery's answer is a typed operand (arc SS, #1428 #1431 #1427 #1422)

A scalar subquery's answer reached its consumer as the runner's row box — a DATE as its ISO text, a TIMESTAMP as a bare count of epoch milliseconds — and nothing that reads an instant by its producer read the subquery's declaration. At v0.25.3, on all five arms, `CAST((SELECT ts …) AS VARCHAR)` for 1969-12-31 23:59:59.999 answered `-1`, `extract(year FROM (SELECT min(ts) …))` over 1000-01-01 answered -968030, and `(SELECT max(ts) …) + INTERVAL '1 hour'` and `- INTERVAL '1 day'` answered the subquery's own 2024-03-04 12:00:00 (`coordinator.TestArcSSScalarSubqueryTypedOperandEveryArm` issue/1428/*, issue/1431/*); `d = (SELECT d …)` for 9999-12-31 answered no row on the single-process arms and the row on the DAG arms (issue/1427/eq). The answer is now the box its declared type has on the row path and every consumer reads its unit from the same declaration (`expr.typedScalarAnswer`, `producedTemporal`), and the comparison kernel's temporal pair rule covers the same-type pair (`batch.TemporalPairType`), so no DATE or TIMESTAMP pair reaches the magnitude guess. A correlated subquery was declared from its text with the outer names unresolved: at v0.25.3 `(SELECT c.f + x.v …)` over a DOUBLE `c.f` and an INT `x.v` declared integer and answered 6 for 6.5 on all five arms (issue/1422/outerFirst), and one returning an outer column declared text (`*/corr/proj`); it is now declared with each outer reference typed as the outer column, as the per-row re-run spells it (`expr.OuterTypedSubquerySQL`): the outer value is a COLUMN-TYPED literal (`plansql.CastNode.Column`) that the planner and the evaluator type as the outer column — its integer width, its DECIMAL (p,s), its array element — so `(SELECT coalesce(o.i, x.v) …)` over an int4 `o.i` declares integer and `(SELECT coalesce(o.a, x.a) …)` over an int4[] declares integer[], as at v0.25.3 and on PostgreSQL (`pgwire.TestArcSSScalarSubqueryTypedOnTheWire` coalesceOuterInt, coalesceOuterArray). An integer CAST the user writes keeps its own rule: `CAST(t.i AS INTEGER) / t.n` answers 1.3333333333333333 double precision as at v0.25.3 (`coordinator.TestArcSSOperandClassAndWidthEveryArm` castI/divN). These close cells without a catalog row; r12 and r19 below are the rows that moved.

| family | row | change | gate |
|---|---|---|---|
| lateral-subqueries | [r12](0012-divergences/lateral-subqueries.md#catalog) | Amended: a correlated subquery returning an ARRAY outer column, `(SELECT c.arr …)`, answers the array (at v0.25.3 it raised `cannot store []interface {} into STRING vector` on all five arms, array/corr/proj), declared as the column is: integer[] (OID 1007) for an integer[] column, as on PostgreSQL (wire corrArray) | `coordinator.TestArcSSScalarSubqueryTypedOperandEveryArm`, `pgwire.TestArcSSScalarSubqueryTypedOnTheWire` |
| numeric-decimal | [r1](0012-divergences/numeric-decimal.md#catalog) | Amended: the zero-row spelling `SELECT t.i - t.i FROM t WHERE t.id = 99` (no row) declares bigint as the rows-returning one does (at v0.25.3 it declared integer — the declaration was read off the text's last column name); int4 arithmetic inside a scalar subquery declares integer and raises 22003 past its range, as on PostgreSQL (`WITH o AS (SELECT CAST(2147483647 AS INT) AS i) SELECT (SELECT o.i + x.v …)`: 22003 at v0.25.3 and now) | `coordinator.TestArcSSOperandClassAndWidthEveryArm`, `pgwire.TestArcSSScalarSubqueryTypedOnTheWire` |
| numeric-decimal | [r19](0012-divergences/numeric-decimal.md#catalog) | Added: a DECIMAL quotient keeps max(6, s1 + p2 + 1) fraction digits, each operand's precision its column type's (ADR-0024 §3, already the rule: the column forms plain/mDivI, plain/vDivN answer the same digits at v0.25.3); a correlated subquery's outer value is typed as its column, so `(SELECT x.m / o.i …)` answers `x.m / i`'s 0.4166666666667 (at v0.25.3: 42000 cannot store string into FLOAT64 vector, corr/mDivI) and `(SELECT x.v / o.n …)` answers `x.v / n`'s 2.22222222222 (corr/vDivN) | `coordinator.TestArcSSOperandClassAndWidthEveryArm` |
| temporal | [r17](0012-divergences/temporal.md#catalog) | Amended: an integer beside a day count (`date - date`, `date - DATE '…'`) is integer arithmetic, declared bigint as int4 arithmetic is (numeric-decimal#r1): `(d - DATE '2024-01-01') + 1`, `/ 7`, `(d - d) * 2` declared double precision at v0.25.3 with the same values (shared/plainDiffPlus, plainDiffDiv7; wire census c7b/dmlit_div7); inside a scalar subquery a day count is integer, as on PostgreSQL: `(SELECT (o.d - DATE '2024-01-01') + x.v …)` is declared integer, as at v0.25.3 (shared/dDiffPlusV, wire dDiffPlusV, CTAS ctasDayCount); beside a NUMERIC a day count is an integer operand of numeric arithmetic — `(d - DATE '2024-01-01') * n` is numeric (69030.50 for 1970-01-01 × -3.50), double precision (69030.5) at v0.25.3 (intop/plainTimesN, wire plainDayCountN), and `(SELECT (t.d - DATE '2024-01-01') * t.n …)` numeric as at v0.25.3 (intop/subTimesN, wire subDayCountN, CTAS ctasDayCountN); a day count divided by zero answers NULL where PostgreSQL raises 22012, NULL at v0.25.3 too (intop/dayCountDivZero, dayCountDivLitZero: a recorded filing candidate) | `coordinator.TestArcSSOperandClassAndWidthEveryArm`, `pgwire.TestArcSSDeclaredByTheSelectListWalk` |
| — | — | Closed without a row (PostgreSQL's answer now, with or without a subquery): `ascii(s)` is integer (OID 23) and divides as an integer — `SELECT ascii(s) / 2` over 'abc' answers 48 where v0.25.3 declared double precision and answered 48.5 (shared/plainAscii, plainAsciiDiv2, wire plainAscii); `sum(s.k)` over a derived table's or a CTE's int4 column is bigint — `SELECT sum(s.k) FROM (SELECT t.i * 2 AS k FROM t) s` declared numeric at v0.25.3 (`t.i + t.i` was bigint there only because the declaration read the text's last column), and over `t.b - t.i` it is numeric where v0.25.3 declared bigint (shared/sum*, wire sumDerived*); a subscript of an integer[] is int4 inside a scalar subquery — `(SELECT x.a[1] + 2147483647 …)` is 22003 where v0.25.3 answered 2147483648 (shared/uncIdxOvf, wire uncIdxOvf); an aggregate over a day count of two DATE columns reads the count — `sum(d - d)` answered NULL and `count(d - d)` 0 at v0.25.3 (shared/sumDayCount, countDayCount) | `coordinator.TestArcSSOperandClassAndWidthEveryArm`, `pgwire.TestArcSSDeclaredByTheSelectListWalk` |
| — | — | Closed without a row (PostgreSQL's refusal now, with or without a subquery): `||` whose operands are both among number, boolean, date and timestamp (`i || i` answered 33 at v0.25.3; also `1 || 2`, `f || n`, `o || o`, `d || d`, `d || 5`, `(d - d) || i`) and `*`, `/`, `%` with a DATE or TIMESTAMP beside a number or another date/timestamp (`d * i` answered 59358 at v0.25.3; also `2 * d`, `ts / 2`, `ts * f`, `d % 2`, `d / d`) are 42883 on every arm, the correlated spellings `(SELECT o.d * x.v …)` / `(SELECT o.f || x.v …)` included. The refusal reaches only operand pairs PostgreSQL has no operator for: `||` with a text operand (`1 || name`, `d || 'x'`, `upper(s) || i`) and `(d - d) * 2`, `(d - DATE …) / 7`, `extract(day FROM d) / 2` answer as before (60 non-subquery cells measured on the wire and five arms) | `coordinator.TestArcSSOperandClassAndWidthEveryArm` |
| — | — | Closed without a row (PostgreSQL's answer now, with or without a subquery): an integer-valued operand beside a NUMERIC — `length(s)`, `ascii(s)`, a subscript of an `integer[]`, `COALESCE`/`CASE`/`abs` over integers — is numeric arithmetic, as an integer column's is: `length(s) * n` and `a[1] * n` are numeric -17.50 and -10.50 where v0.25.3 declared double precision -17.5 and -10.5 (intop/plainLenN, plainIdxN), and `CAST(a[1] AS NUMERIC)` answers where v0.25.3 raised `cannot store string into FLOAT64 vector` (intop/plainCastIdxNumeric); an expression is read in its own scope — over a derived table publishing `t.b AS i` and its own unaliased `t.i + t.i`, `(SELECT t.i + t.i …)` is 18000000000 where v0.25.3 raised 22003 (scope/shadowSubQual), `sum(k)` over a column derived from `i + 0` is numeric where v0.25.3 overflowed its accumulator (scope/shadowSumOvfBare), and `(SELECT t.b + t.b …)` over a derived `2147483647 AS b` is 22003 where v0.25.3 answered 4294967294 (scope/shadowRevSub); a zero-row result declares a correlated `(SELECT o.a[1] …)` integer and `(SELECT o.a …)` integer[] over an `integer[]` outer column, text at v0.25.3 (intop/zeroIdx, zeroArr, wire zeroArr) | `coordinator.TestArcSSOperandClassAndWidthEveryArm`, `pgwire.TestArcSSScopeAndIntegerOperandsOnTheWire` |
| — | — | Closed without a row (PostgreSQL's answer, inside a scalar subquery only): an integer CAST or an integral EXTRACT field beside a NUMERIC in a subquery's body is an integer operand of numeric arithmetic under `*`, `+`, `-` and `%`, computed exactly, and so are unary minus, `abs`, `round`, `trunc`, `mod` and a `CASE` / `COALESCE` / `GREATEST` / `NULLIF` arm over the exact value past 2^53 (`-((SELECT x.b * 10000000 …) * t.n - 3) % 1000` -997.00 where 0.00 was answered before this fix, r8/s_negSub, r8/d_deepModNeg, r8/au_*; v0.25.3 declared these double, -0) — `(SELECT CAST(o.b AS INTEGER) * x.m …)` and `(SELECT extract(year FROM o.d) * x.m …)` are numeric 37.50 and 2530.00, as at v0.25.3 (intop/subCastM, subExtractM, wire subCastM, subExtractM, CTAS ctasCastN); `(SELECT x.m / CAST(o.i AS INTEGER) …)` is numeric 0.4166666666666666666667 (numeric-decimal#r19's digits) where v0.25.3 declared double precision 0.4166666666666667 (answer/subMDivCast). The same expressions in a query's own SELECT list keep double precision (filing candidate N-10; ADR-0024 §2c), as at v0.25.3 (answer/plainExtractN, derivedCastM, cteCastM, derivedExtractN); a quotient over a marked EXTRACT is the double it is divided in, on every row: `(SELECT x.m * (extract(year FROM o.d) / 7) …)` is double 361.42857142857144, as v0.25.3 answered it (r6/y_mTimesDiv7, wire mTimesYearDiv7; PostgreSQL numeric 361.428571428571428625). v0.25.3 declared `(SELECT extract(year FROM t.d) / 7 * t.n …)` numeric by the last column name and wrote the double into it, so it answered where the double fit two places and raised 22003 elsewhere: numeric -985.00 over id 2 (y/uncDiv7N2), -3447.50 over `/ 2` (w/uncDiv2N2), 0.00 over id 4 (w/uncDiv7N4), and 22003 over id 1 (w/uncDiv7N1) and on every per-row form (y/div7M); these are double now (-985, -3447.5, 0, 650.5714285714287). A nested scalar subquery answering an integer, or a bare integral EXTRACT field, is an integer operand of the outer body's numeric arithmetic: `(SELECT (SELECT z.v …) * y.m …)` is numeric 6.25 and `(SELECT (SELECT extract(year FROM o.d) …) * y.m …)` 2530.00, as at v0.25.3 (r6/n_subVtimesM, n_subYearTimesM, wire subVtimesM, subYearTimesM, CTAS ctasSubV); in a query's own SELECT list `(SELECT z.v …) * t.n` is numeric too where v0.25.3 declared double precision (r6/n_topVtimesN) | `coordinator.TestArcSSOperandClassAndWidthEveryArm`, `pgwire.TestArcSSScopeAndIntegerOperandsOnTheWire` |
| numeric-decimal | [r19](0012-divergences/numeric-decimal.md#catalog) | Amended: the measured cell `(SELECT x.m / extract(year FROM o.d) …)` (r9/a_mDivYear) is numeric at r19's division scale, 22 fraction digits correctly rounded, where PostgreSQL keeps 20 and v0.25.3 declared double precision; a window function's input over an integer operand beside a NUMERIC is numeric arithmetic too, exact past 2^53 (`sum((SELECT x.b * 10000000 …) * t.n + 3) OVER (ORDER BY t.id)` 202500000000000003.00, r9/wn_subSum; `sum(t.a[1] * t.n) OVER (…)` 2.25 / -8.25 where v0.25.3 declared double precision, r9/wn_idxPlain); inside a scalar subquery's body a window `sum` / `avg` over a computed `numeric` argument is declared `numeric` (`(SELECT sum(o.i * y.m) OVER () …)` 3.75, r10/z_subBodyColWin, where v0.25.3 raised #361 for an integer column and answered double precision for a subscript, `ascii`, a day count or a nested subquery) | `coordinator.TestArcSSOperandClassAndWidthEveryArm`, `wadjet.TestArcSSWindowInputExactUnderSpill` |
| numeric-decimal | [r18](0012-divergences/numeric-decimal.md#catalog) | Amended (2026-10-01): a numeric[] array's elements share one scale, so an integer element beside a scale-2 one renders at it — `ARRAY[t.n, 1]` is {2.25,1.00} where PostgreSQL prints {2.25,1}; v0.25.3 stored that integer element as an unscaled carrier, {2.25,0.01} (r12/b2/arrN1), and so did every exact operand beside an integer element (`ARRAY[t.a[1] * t.n, 1]`, which v0.25.3 answered as double precision {2.25,1}, r12/b2/arrIdxN1) | `coordinator.TestArcSSOperandKindTimesConsumerEveryArm` r12/c29_*, r12/b2/*; `pgwire.TestArcSSAuditConsumersOnTheWire` r12/c29_arrayCtas_* |
| — | — | Closed without a row (2026-10-01; PostgreSQL's answer now, with or without a subquery): a recursive CTE seeded by numeric arithmetic is an unconstrained numeric column, so `WITH RECURSIVE r(k, v) AS (SELECT t.id, t.i * t.n … UNION ALL SELECT k + 1, v + 1 FROM r …)` answers 6.75, 7.75, 8.75 where v0.25.3 raised 42804 (r12/b3/recSmallCol); the bare numeric(10,2) seed stays 42804 as on PostgreSQL (r12/b3/recSmallN) | `coordinator.TestArcSSOperandKindTimesConsumerEveryArm` r12/c30_*, r12/b3/* |
| — | — | Closed without a row (2026-10-01; values now, the declaration is the float8 rung the sql-reference names): a `CASE` / `COALESCE` with a NUMERIC subquery's bare answer as one arm and arithmetic over it as another answers the double precision values — `CASE … THEN (SELECT y.m * t.i …) ELSE (SELECT y.m * t.i …) + 1 END` is 4.75, -8.75, … where v0.25.3 raised `cannot store string into FLOAT64 vector` (r12/b1/caseMSmall; PostgreSQL numeric 4.75, -8.75) | `coordinator.TestArcSSOperandKindTimesConsumerEveryArm` r12/c20b_*, r12/b1/* |
| recursion | [r11](0012-divergences/recursion.md#catalog) | Added (2026-10-01): a recursive CTE seeded by a scalar subquery over a `numeric(p,s)` column answers where PostgreSQL raises 42804 — `SELECT 1, (SELECT x.n …) UNION ALL SELECT k + 1, v + 1 …` is 2.25, 3.25, 4.25 — because this engine declares the subquery's answer without the column's modifier and the seed is read from that declaration; v0.25.3 raised 42804 (r13/seed/subSeed) | `pgwire.TestArcSSAuditConsumersOnTheWire` r13/seed/subSeed* |
| recursion | [r12](0012-divergences/recursion.md#catalog) | Added (2026-10-01): a `numeric(38,s)` seed, a CAST or a column, reads as unconstrained and answers where PostgreSQL raises 42804 (`CAST(1.5 AS NUMERIC(38,2))` 1.50, 2.50, 3.50), as at v0.25.3 (r13/seed/n382cast) | `pgwire.TestArcSSAuditConsumersOnTheWire` r13/seed/n38* |
| — | — | Closed without a row (2026-10-01; PostgreSQL's answer now): `json_build_object` writes a value declared `numeric` as a JSON number with its own digits — a `numeric` column's `json_build_object('v', n)` is `{"v" : 2.25}` where v0.25.3 wrote the JSON string `{"v" : "2.25"}` (r13/b4/colN), `json_build_object('v', t.a[1] * t.n)` is `{"v" : -10.50}` where v0.25.3 wrote the double's `{"v" : -10.5}` (r13/b4/idxSmall), and an array value's elements are joined by a bare comma as PostgreSQL's are: `json_build_object('v', ARRAY[n, 1])` is `{"v" : [2.25,1]}` where v0.25.3 wrote `{"v" : ["2.25", 1]}` (r13/b4/nArr). The object stays declared text where PostgreSQL declares json (a recorded filing candidate) | `coordinator.TestArcSSOperandKindTimesConsumerEveryArm` r13/b4/*, r13/c36_*; `pgwire.TestArcSSAuditConsumersOnTheWire` r13/b4/* |

## 2026-10-02: the scalar renderers write a value as its declared type's text (arc RN, #1474 #1466 #1467 #1481)

Four renderers turned a typed value into text from the Go type of its box rather than from its declared type. At c39858f3, on all five arms: `json_build_object('d', t.d, 'ts', t.ts)` wrote the day count and the epoch milliseconds (`{"d" : 19786, "ts" : 1709553600000}` for 2024-03-04 12:00:00), a nested `json_build_object` was written as an escaped JSON string, `format('%s|', t.f * t.n + 3)` was `%!s(float64=6.375)|` and `format('%s|', NULL)` `%!s(<nil>)|`, and `regexp_replace(CAST(t.a[1] * t.n AS TEXT), '0', 'z')` replaced every match (`-1z.5z` for -10.50) and took no flags argument (42883). Each now renders by the declaration: json_build_object through `batch.FormatPGJSON` (CAST(container AS JSON)'s renderer), format through `batch.FormatPGText` (the pgwire text format's), and regexp_replace reads its pattern through the `~` operators' ARE translation with PostgreSQL's flags. Gate: `coordinator.TestArcRNRendererTableEveryArm` (963 cells × five arms; 616 fail at c39858f3, and no arm-cell where c39858f3 agreed with PostgreSQL disagrees), `wadjet.TestArcRNEmbeddedRenderers`, `pgwire.TestArcRNRenderersOnTheWire`.

| family | row | change | gate |
|---|---|---|---|
| text-collation | [r3](0012-divergences/text-collation.md#catalog) | Amended: `regexp_replace` reads its pattern through the same ARE translation and refuses a back reference 0A000 (`regexp_replace('Hello', '(l)\1', '#')`; at c39858f3 the pattern reached RE2 untranslated and answered NULL, and `\b` was a word boundary: `regexp_replace('abc', '\b', '\|')` answered `\|abc\|` where PostgreSQL answers `abc`) | `coordinator.TestArcRNRendererTableEveryArm` rx/backrefPattern, rx/backspace |
| text-collation | [r4](0012-divergences/text-collation.md#catalog) | Amended: `regexp_replace`'s `i` flag folds ASCII letters only, and `\y` counts ASCII word characters only (rx/icaseNonASCII, rx/wordY) | `coordinator.TestArcRNRendererTableEveryArm` rx/icaseNonASCII, rx/wordY |
| text-collation | [r23](0012-divergences/text-collation.md#catalog) | Added: the `n`, `m`, `p`, `w`, `x`, `b` and `e` flags and an integer start position are refused 0A000 where PostgreSQL answers | `coordinator.TestArcRNRendererTableEveryArm` rx/flag* |
| text-collation | [r24](0012-divergences/text-collation.md#catalog) | Added: an RE holding a non-greedy quantifier is matched leftmost-first, and groups among equal-length matches are RE2's choice (rx/lazyRE, rx/posixCaptures, both wrong at c39858f3 too) | `coordinator.TestArcRNRendererTableEveryArm` rx/lazyRE, rx/posixCaptures |
| other | [r16](0012-divergences/other.md#catalog) | Added: json_build_object declares text (#1470), so json reaching it through a column or a scalar subquery's answer nests as a JSON string (jx/corrSubq, jx/uncorrSubq, jx/cte, jx/coalesceText) | `pgwire.TestArcRNRenderersOnTheWire`, `coordinator.TestArcRNRendererTableEveryArm` jx/* |
| — | — | Closed without a row (PostgreSQL's answer now): json_build_object writes a DATE / TIMESTAMP as the JSON string of its text, `{"d" : "2024-03-04", "ts" : "2024-03-04T12:00:00"}`, and so their array elements; a key as its value's JSON text (`{"2024-03-04" : 1}`); a nested json_build_object or CAST to json as the object it is; a string with `<`, `>`, `&` unescaped (#1466 #1474) | `coordinator.TestArcRNRendererTableEveryArm` json/*, jsonKey/*, jsonNest/*, jx/* |
| — | — | Closed without a row (PostgreSQL's answer now): format is PostgreSQL's `%[n$][-][width]type` grammar over each value's type text (`6.375\|`, `\|` for NULL, `t`, `{1,2}`), `%d` and `%5.2s` are 22023; quote_ident quotes a non-UNRESERVED keyword and quote_literal writes E'…' for a backslash (#1467) | `coordinator.TestArcRNRendererTableEveryArm` fmtS/*, fmtWidth/*, fmtPosL/*, fx/* |
| — | — | Closed without a row (PostgreSQL's answer now): regexp_replace replaces the first match unless `g`, takes `i` / `c` / `q` / `s` / `t`, expands `\&`, keeps the empty match after a non-empty one under `g`, and an all-greedy RE takes the longest match (`regexp_replace('abcd', 'a\|ab', 'X')` is Xcd; Xbcd at c39858f3) (#1481) | `coordinator.TestArcRNRendererTableEveryArm` rx/* |

## 2026-10-02: a bound parameter's type (arc PW, #1426 #1410)

A bound parameter reached the planner as SQL text, and the text was not a value of the parameter's type: a timestamp was a bare quoted literal — SQL's unknown — which a DATE operand read with the DATE input function, so `d = $1` bound as timestamp `'1969-12-31 23:59:59.999'` matched 1969-12-31, `d IN ($1, $2)` with `'2024-03-04 12:00:00'` matched 2024-03-04, `d < $1` with `'1969-12-31 00:00:00.001'` missed 1969-12-31 and `d = ANY (SELECT $1 UNION ALL SELECT $2)` was 42883 (#1426; v0.25.3, `pgwire.TestArcPWParameterTypesMatchPostgres` date/\*/text and /bin); a bigint was a bare integer, so `SELECT $1` declared integer; and a statement's Describe stood an untyped NULL in for every parameter and declared text. The type of an undeclared parameter was read lexically from the identifier beside it, so `$1 = n + d` was typed integer and a pgx client's 14.5 matched no row, `INSERT … VALUES (2, $1)` bound 2.5 into an integer column was 22P02, and `LIMIT $1` was 42601 (#1410; `pgwire.TestArcPWPgxClientsBindAsPostgres` 1410/\*). A parameter is now its declared type, or its position's as PostgreSQL types it, spliced as a literal of that type, and a NULL parameter — the Describe stand-in included — is a NULL of that type.

| family | row | change | gate |
|---|---|---|---|
| parameters-pgwire | [r5–r13](0012-divergences/parameters-pgwire.md#catalog) | Added: a text-family parameter is SQL's unknown (r5); a FROM-less subquery's parameter takes the comparison's type (r6); an undetermined parameter is text or OID 0 where PostgreSQL raises 42P08 / 42P18 (r7); int2, timestamptz, bytea and arrays have no type of their own here (r8–r10); a binary bytea holding a backslash is 22P02 (r11, as at v0.25.3); the expression around a parameter keeps this engine's declaration (r12); a parameter beside an aggregate or in a table function's arguments is undetermined (r13, as at v0.25.3) | `pgwire.TestArcPWParameterTypesMatchPostgres` |
| aggregates-windows | [r20](0012-divergences/aggregates-windows.md#catalog) | NARROWED: a bound int8 or numeric window offset raises 42883 as on PostgreSQL (both answered `LAG(x, 1)` at v0.25.3); a text one still answers by its value | `pgwire.TestAWindowArgumentParameterAnswers`, `pgwire.TestArcPWParameterTypesMatchPostgres` |
| temporal | [r24](0012-divergences/temporal.md#catalog) | Amended: a TIMESTAMP-typed bind parameter against a DATE answers PostgreSQL's rows (the clause that recorded #1426 as a known wrong value is closed) | `pgwire.TestArcPWParameterTypesMatchPostgres`, `coordinator.TestArcPWParameterTypesOnTheCoordinatorDoor` |
| parameters-pgwire | [r9](0012-divergences/parameters-pgwire.md#catalog) | Narrowed: the offset forms a timestamptz parameter applies are named (`Z`, `±hh`, `±hh:mm`, `±hhmm`, `±hh:mm:ss`); a zone name is refused 22007 (temporal r25) | `pgwire.TestArcPWRound2MatchesPostgres` |
| parameters-pgwire | [r12](0012-divergences/parameters-pgwire.md#catalog) | Amended: a parameter beside arithmetic takes PostgreSQL's operator type (`$1 = n * 2` integer, as at v0.25.3 and on PostgreSQL); the binary-row width of a negative integer or smallint parameter under the integer Describe is recorded; an integer parameter past its width raises 22003 | `pgwire.TestArcPWRound2MatchesPostgres` |
| temporal | [r25](0012-divergences/temporal.md#catalog) | Added: DATE and TIMESTAMP text read one grammar (E41 amended — TIMESTAMP now takes `/` and `.` dates, one-digit fields, a missing seconds field, `t`, `±hhmm`, five-digit years; DATE refuses `12:00:00ZZ` and `12:00:00+`; TIMESTAMP refuses year 0000 22008); the PostgreSQL forms outside it are refused 22007 | `pgwire.TestArcPWRound2MatchesPostgres` |
| parameters-pgwire | [r14](0012-divergences/parameters-pgwire.md#catalog) | Superset withdrawn (PostgreSQL's refusal now): `WHERE n = $1 OR $1 IS NULL` with `$1` declared date, timestamp, timestamptz, boolean or uuid and bound NULL over an integer `n` raises 42883 as on PostgreSQL (v0.25.3 answered every row); with `$1` undeclared or integer it answers every row on both | `pgwire.TestArcPWRound2MatchesPostgres` (p1/\*) |
| parameters-pgwire | [r15](0012-divergences/parameters-pgwire.md#catalog) | Superset withdrawn (PostgreSQL's refusal now): pgx `SELECT $1` with a Go integer, in its cache-statement and describe modes, is refused by the client, which encodes for the text parameter the server declares, as against PostgreSQL (at 978cd0e5 it answered `42`, declared text, in those modes; in exec mode all three answer `42`) | `pgwire.TestArcPWPgxClientsBindAsPostgres` |
| temporal | [r25](0012-divergences/temporal.md#catalog) | Amended 2026-10-03: a numeric zone offset is read by PostgreSQL's digit rule (with no `:`, the last two digits the minute and the rest the hour: `+000130` is 01:30, `+001500` 15:00, `+00130` / `+0000130` / `+00000000130` read, `+05:` is +05, `+053000` and `+0530:00` are 22009 as on PostgreSQL — the clause recording `'…+0530:00'` at 22007 is closed); `…Z+05`, PostgreSQL's POSIX zone five hours west, is a zone name and refused 22007 (it read as a DATE at 978cd0e5, which discarded the suffix); `now` and `today` leave the row's list, which names only gated spellings | `coordinator.TestArcPWZoneSpellingsEveryArm`, `parquet.TestTheZoneOffsetReadsPostgresDigitRule`, `pgwire.TestArcPWRound2MatchesPostgres` (b3/\*) |
| temporal | [r25](0012-divergences/temporal.md#catalog) | Amended 2026-10-03: whitespace after a zone's sign is skipped as PostgreSQL's lexer skips it — `'2024-03-04 12:00:00+ 05'` is +05 and `'… - 05'` −05 (DATE 2024-03-04; the clause recording `… - 05` and "a sign set apart by a space" as refused is closed), `+ 16` / `+ 053000` / `+05 - 16` are 22009 — and the grammar's whitespace is PostgreSQL's isspace (`\n`, `\r`, `\v`, `\f` as well as space and tab, between every field); `…z+05`, the lower-case POSIX zone, joins `…Z+05` in the row's list | `parquet.TestTheGrammarSkipsPostgresWhitespace`, `coordinator.TestArcPWZoneSpellingsEveryArm`, `pgwire.TestArcPWRound2MatchesPostgres` (b3/\*) |

## 2026-10-02: the numeric carrier (arc NX, #1386 #1392 #1450)

Three operand kinds PostgreSQL types `numeric` rode this engine's float8 rung. An integer CAST beside a numeric was not an integer operand of exact arithmetic outside a scalar subquery's body: at v0.25.3 (and at c39858f3), `CAST(t.i AS INTEGER) % t.n` over 1 and 0.01 answered 0.00999999999999998, `CAST(t.i AS INTEGER) * 0.1` over 3 answered 0.30000000000000004 and `CAST(t.b AS BIGINT) * 10000000 * t.n - 3` over 9000000000 and 10.00 answered 900000000000000000 (`wadjet.TestArcNXEmbeddedNumericCarrier` 1450/modMulPast2^53). A numeric constant a double cannot carry was boxed as a float64, so `COALESCE(14.0000000000000000001, 0)`, `CASE WHEN true THEN 14.0000000000000000001 END`, `LEAST(14.0000000000000000001, 20)` and `-(-14.0000000000000000001)` compared equal to 14 and `(SELECT COALESCE(14.0000000000000000001, 0))` answered 14 (1386/choiceUnarySubquery, 1386/where: 4 rows for PostgreSQL's 0). The explicit integer CAST of `5 / 2.0`, `SQRT(6.25)` and `POWER(2.5, 1)` answered 2 (negated -2) for PostgreSQL's 3 (1392/six, 1392/negSmallBig). Each is now PostgreSQL's answer on all five arms (`coordinator.TestArcNXNumericCarrierEveryArm`, 1360 cells) and on the wire (`pgwire.TestArcNXNumericCarrierOnTheWire`); a quotient over an operand a cast made exact keeps the double (r19) and a column a previous operator materialized keeps half-to-even rounding under an explicit CAST (dml-assignment r2).

| family | row | change | gate |
|---|---|---|---|
| numeric-decimal | [r1](0012-divergences/numeric-decimal.md#catalog) | Amended: an integer CAST of a float-carried numeric declares bigint (OID 20) with PostgreSQL's value, `CAST(5 / 2.0 AS INTEGER)` 3 (2 at v0.25.3, 1392/six) | `pgwire.TestArcNXNumericCarrierOnTheWire` (kept w/rcSix, w/rcBig) |
| numeric-decimal | [r3](0012-divergences/numeric-decimal.md#catalog) | Amended: an integer CAST beside an integer stays int4 arithmetic, so `CAST(t.i AS INTEGER) + 2147483647` over 3 is 2147483650 where PostgreSQL raises 22003 (measured at the tip and at c39858f3 alike) | — |
| numeric-decimal | [r18](0012-divergences/numeric-decimal.md#catalog) | Amended: a constant a double cannot carry folds at its own scale, so `COALESCE(t.b, 14.0000000000000000001)` prints 30.0000000000000000000 for 30 and keeps 14.0000000000000000001 (c39858f3: the double 14, nx/ncCoalesceCol/proj); `CASE WHEN 14.0000000000000000001 > 14 THEN 14 ELSE 13.25 END` prints 14.00 for 14 (c39858f3: 13.25, nx/ncCaseChoice/proj) | `coordinator.TestArcNXNumericCarrierEveryArm` |
| numeric-decimal | [r19](0012-divergences/numeric-decimal.md#catalog) | Amended: a quotient over an operand a cast made exact — an integer CAST bare or under NULLIF, COALESCE, CASE, GREATEST, LEAST, abs, unary minus, integer or numeric arithmetic; a bare NUMERIC cast; an integer literal past int64 — keeps the double, `CAST(t.i AS INTEGER) / t.n` 1.3333333333333333 (OID 701) as at c39858f3 (issue/1450/div), `CAST(t.n AS NUMERIC) / 3` 0.75, 0.0033333333333333335 as at c39858f3 (bareCast/numOperand), and `t.n / NULLIF(CAST(t.b AS BIGINT), 0) = 0.0000000011111111111111111111`, `(CAST(t.i AS INTEGER) * 1.0) / t.n = 1.3333333333333333` and `CAST(t.i AS NUMERIC) / t.n = 1.3333333333333333` select PostgreSQL's row as at c39858f3 (nx/icBigDiv/cmp, nx/wq*/cmp, nx/wn*/cmp, qn/*/cmp); the wrapped cast's projection is the double 1.111111111111111e-09 where c39858f3 raised 22003 (nx/icBigDiv/proj) | `coordinator.TestArcNXNumericCarrierEveryArm` |
| dml-assignment | [r2](0012-divergences/dml-assignment.md#catalog) | NARROWED: an explicit integer CAST of a float-carried numeric computed in the cast's own operand rounds half away (`CAST(5 / 2.0 AS INTEGER)` 3; 2 at v0.25.3, 1392/six); a column a previous operator materialized keeps half to even (`CAST(s.x AS INTEGER)` over `SELECT DISTINCT 5 / 2.0 + t.id * 0` 2 on every arm, as at c39858f3, roundOrigin/distinct) | `coordinator.TestArcNXNumericCarrierEveryArm` (kept roundOrigin/*) |
| — | — | Closed without a row (PostgreSQL's answer now): a bare `CAST(<exact operand> AS NUMERIC)` is that operand's numeric in the consumers the rows measure — every operator but a quotient, which keeps the double (r19) — `CAST((SELECT x.b * 10000000 …) * t.n AS NUMERIC) + 3` is 202500000000000003.00 (OID 1700) where c39858f3 answered the double 2.025e+17 (arc SS N-19: coordinator r11/c01_project_castBare and 36 more cells, wire r11/*_castBare), and `CAST('14.0000000000000000001' AS NUMERIC)` keeps its digits (nx/ncCastNumText/*); a wide numeric literal in a window's argument is exact, `SUM(99999999999999999999.5) OVER ()` over one row 99999999999999999999.5 (c39858f3: 1e+20) | `coordinator.TestArcSSOperandKindTimesConsumerEveryArm`, `coordinator.TestArcNXNumericCarrierEveryArm` |
| — | — | Closed without a row (PostgreSQL's answer now): a `numeric` the engine carries exactly — a column, a typed or bare NUMERIC cast, a fractional literal a double cannot carry, an integer literal past int64, a numeric expression or scalar subquery — cast to BOOLEAN, DATE, TIMESTAMP, INTERVAL, UUID, an array or a vector refuses the type pair, 42846 — `CAST(t.n AS DATE)` over a numeric(10,2) column (c39858f3: 22007 invalid input syntax for type date: "2.25", castRefusal/numColWhere/DATE), `CAST(CAST(1.5 AS NUMERIC(10,2)) AS BOOLEAN)` (c39858f3: 22P02, castRefusal/numCast/BOOLEAN), `CAST(14.0000000000000000001 AS DATE)` (c39858f3: 1970-01-15, castRefusal/wide/DATE), `9223372036854775808::DATE` (c39858f3: 22003, castRefusal/pastInt64Colon/DATE), `CAST(CAST(1.5 AS NUMERIC(10,2)) AS VECTOR(1))` (c39858f3: 22P02, castRefusal/numCast/VECTOR; pgvector 0.8.2 answers 42846). A narrow fractional literal keeps its double: `CAST(14.5 AS DATE)` is 1970-01-15 (the day count, as at c39858f3, castRefusal/narrow/DATE) and `CAST(14.5 AS UUID)` 22P02 (as at c39858f3, castRefusal/narrow/UUID, candidate NX-C11) | `coordinator.TestArcNXNumericCarrierEveryArm` castRefusal/*, `pgwire.TestArcNXNumericCarrierOnTheWire` w/castRefusal* |
| — | — | Closed without a row (PostgreSQL's answer now): an integer literal past int64 is the numeric its digits name, and a minus folds into the constant before it is typed — `SELECT 9223372036854775808` is 9223372036854775808 (OID 1700), `SELECT -9223372036854775808` the bigint -9223372036854775808 and `SELECT -(-9223372036854775808)` the numeric 9223372036854775808 (c39858f3: the doubles 9.223372036854776e+18, -9.223372036854776e+18, 9.223372036854776e+18; negLit/pastInt64, negLit/int64Min, negLit/doubleNeg), `9223372036854775808 = 9223372036854775807` is false (c39858f3: true, negLit/pastInt64Cmp), and `SELECT - 9223372036854775808 - 1` and `CAST((-9223372036854775809) AS BIGINT)` raise 22003 bigint out of range (c39858f3: -9223372036854775809 and -9223372036854775808, negLit/int64MinMinus1, negLit/pastInt64ToBigint) | `coordinator.TestArcNXNumericCarrierEveryArm` negLit/*, `pgwire.TestArcNXNumericCarrierOnTheWire` w/pastInt64*, `wadjet.TestAnOutOfRangeCastRefusesAtTheDoor` |
| — | — | Closed without a row (PostgreSQL's answer now): an explicit integer CAST over an EXTRACT of a column rounds half away on the stage DAG too — `CAST(extract(year FROM t.d) * 0 + 2.5 AS INTEGER)` is 3 on all five arms (c39858f3: 2 on every arm; roundOrigin/extractDate). A DAG stage re-parses the EXTRACT as `year(t.d)`, which now keeps EXTRACT's category | `coordinator.TestArcNXNumericCarrierEveryArm` roundOrigin/extract* |

## Dated markers inside the entries

Every `Added` / `Amended` / `CLOSED` / `Corrected` / `narrowed` marker still inside an entry's verbatim text, in date order, with the entry that carries it.

| date | marker | entry | family |
|---|---|---|---|
| 2026-08-24 | Added | [E42](0012-divergences/comparison-membership.md#e42) | comparison-membership |
| 2026-08-24 | Added | [E43](0012-divergences/numeric-decimal.md#e43) | numeric-decimal |
| 2026-08-25 | Added | [E16](0012-divergences/aggregates-windows.md#e16) | aggregates-windows |
| 2026-08-25 | Added | [E42](0012-divergences/comparison-membership.md#e42) | comparison-membership |
| 2026-08-25 | Added | [E44](0012-divergences/aggregates-windows.md#e44) | aggregates-windows |
| 2026-08-25 | Added | [E45](0012-divergences/names-scopes.md#e45) | names-scopes |
| 2026-08-25 | Added | [E46](0012-divergences/numeric-decimal.md#e46) | numeric-decimal |
| 2026-08-25 | Amended | [E42](0012-divergences/comparison-membership.md#e42) | comparison-membership |
| 2026-08-25 | Closed | [E42](0012-divergences/comparison-membership.md#e42) | comparison-membership |
| 2026-08-25 | Corrected | [E15](0012-divergences/aggregates-windows.md#e15) | aggregates-windows |
| 2026-08-28 | closed | [E55](0012-divergences/network.md#e55) | network |
| 2026-08-29 | Added | [E47](0012-divergences/numeric-decimal.md#e47) | numeric-decimal |
| 2026-09-02 | Added | [E50](0012-divergences/numeric-decimal.md#e50) | numeric-decimal |
| 2026-09-02 | Added | [E51](0012-divergences/aggregates-windows.md#e51) | aggregates-windows |
| 2026-09-03 | Added | [E11](0012-divergences/parameters-pgwire.md#e11) | parameters-pgwire |
| 2026-09-03 | Added | [E31](0012-divergences/names-scopes.md#e31) | names-scopes |
| 2026-09-03 | Added | [E34](0012-divergences/names-scopes.md#e34) | names-scopes |
| 2026-09-03 | Added | [E36](0012-divergences/temporal.md#e36) | temporal |
| 2026-09-03 | Added | [E38](0012-divergences/temporal.md#e38) | temporal |
| 2026-09-03 | Added | [E41](0012-divergences/temporal.md#e41) | temporal |
| 2026-09-03 | Added | [E42](0012-divergences/comparison-membership.md#e42) | comparison-membership |
| 2026-09-03 | Added | [E49](0012-divergences/lateral-subqueries.md#e49) | lateral-subqueries |
| 2026-09-03 | Added | [E52](0012-divergences/names-scopes.md#e52) | names-scopes |
| 2026-09-03 | Added | [E53](0012-divergences/names-scopes.md#e53) | names-scopes |
| 2026-09-03 | Added | [E54](0012-divergences/names-scopes.md#e54) | names-scopes |
| 2026-09-03 | Added | [E55](0012-divergences/network.md#e55) | network |
| 2026-09-03 | Added | [E56](0012-divergences/text-collation.md#e56) | text-collation |
| 2026-09-04 | Added | [E07](0012-divergences/names-scopes.md#e07) | names-scopes |
| 2026-09-04 | Added | [E10](0012-divergences/names-scopes.md#e10) | names-scopes |
| 2026-09-04 | Added | [E18](0012-divergences/aggregates-windows.md#e18) | aggregates-windows |
| 2026-09-04 | Added | [E22](0012-divergences/names-scopes.md#e22) | names-scopes |
| 2026-09-04 | Added | [E35](0012-divergences/containers.md#e35) | containers |
| 2026-09-04 | Added | [E57](0012-divergences/numeric-decimal.md#e57) | numeric-decimal |
| 2026-09-04 | Added | [E58](0012-divergences/other.md#e58) | other |
| 2026-09-04 | Added | [E59](0012-divergences/temporal.md#e59) | temporal |
| 2026-09-04 | Added | [E60](0012-divergences/temporal.md#e60) | temporal |
| 2026-09-04 | Amended | [E41](0012-divergences/temporal.md#e41) | temporal |
| 2026-09-04 | CLOSED | [E59](0012-divergences/temporal.md#e59) | temporal |
| 2026-09-04 | closed | [E56](0012-divergences/text-collation.md#e56) | text-collation |
| 2026-09-05 | Added | [E06](0012-divergences/set-operations.md#e06) | set-operations |
| 2026-09-05 | Added | [E08](0012-divergences/names-scopes.md#e08) | names-scopes |
| 2026-09-05 | Added | [E09](0012-divergences/set-operations.md#e09) | set-operations |
| 2026-09-05 | Added | [E60](0012-divergences/temporal.md#e60) | temporal |
| 2026-09-05 | Added | [E61](0012-divergences/network.md#e61) | network |
| 2026-09-05 | Added | [E63](0012-divergences/text-collation.md#e63) | text-collation |
| 2026-09-05 | Added | [E64](0012-divergences/numeric-decimal.md#e64) | numeric-decimal |
| 2026-09-05 | CLOSED | [E55](0012-divergences/network.md#e55) | network |
| 2026-09-05 | CORRECTED | [E18](0012-divergences/aggregates-windows.md#e18) | aggregates-windows |
| 2026-09-05 | Corrected | [E55](0012-divergences/network.md#e55) | network |
| 2026-09-05 | added | [E63](0012-divergences/text-collation.md#e63) | text-collation |
| 2026-09-05 | amended | [E10](0012-divergences/names-scopes.md#e10) | names-scopes |
| 2026-09-06 | Added | [E03](0012-divergences/other.md#e03) | other |
| 2026-09-06 | Added | [E04](0012-divergences/other.md#e04) | other |
| 2026-09-06 | Added | [E05](0012-divergences/other.md#e05) | other |
| 2026-09-06 | Added | [E39](0012-divergences/temporal.md#e39) | temporal |
| 2026-09-07 | Added | [E20](0012-divergences/aggregates-windows.md#e20) | aggregates-windows |
| 2026-09-07 | Added | [E21](0012-divergences/aggregates-windows.md#e21) | aggregates-windows |
| 2026-09-07 | Added | [E65](0012-divergences/other.md#e65) | other |
| 2026-09-07 | Added | [E66](0012-divergences/lateral-subqueries.md#e66) | lateral-subqueries |
| 2026-09-07 | Added | [E67](0012-divergences/lateral-subqueries.md#e67) | lateral-subqueries |
| 2026-09-07 | Added | [E70](0012-divergences/lateral-subqueries.md#e70) | lateral-subqueries |
| 2026-09-07 | Added | [E91](0012-divergences/lateral-subqueries.md#e91) | lateral-subqueries |
| 2026-09-07 | Amended | [E87](0012-divergences/lateral-subqueries.md#e87) | lateral-subqueries |
| 2026-09-07 | CLOSED | [E18](0012-divergences/aggregates-windows.md#e18) | aggregates-windows |
| 2026-09-07 | CLOSED | [E22](0012-divergences/names-scopes.md#e22) | names-scopes |
| 2026-09-07 | CLOSED | [E66](0012-divergences/lateral-subqueries.md#e66) | lateral-subqueries |
| 2026-09-07 | CLOSED | [E67](0012-divergences/lateral-subqueries.md#e67) | lateral-subqueries |
| 2026-09-07 | CLOSED | [E87](0012-divergences/lateral-subqueries.md#e87) | lateral-subqueries |
| 2026-09-08 | Added | [E02](0012-divergences/names-scopes.md#e02) | names-scopes |
| 2026-09-08 | Added | [E77](0012-divergences/extensions.md#e77) | extensions |
| 2026-09-08 | Added | [E79](0012-divergences/containers.md#e79) | containers |
| 2026-09-08 | Added | [E80](0012-divergences/aggregates-windows.md#e80) | aggregates-windows |
| 2026-09-08 | Added | [E81](0012-divergences/extensions.md#e81) | extensions |
| 2026-09-08 | Added | [E82](0012-divergences/extensions.md#e82) | extensions |
| 2026-09-08 | Added | [E83](0012-divergences/extensions.md#e83) | extensions |
| 2026-09-08 | Added | [E84](0012-divergences/extensions.md#e84) | extensions |
| 2026-09-08 | added | [E21](0012-divergences/aggregates-windows.md#e21) | aggregates-windows |
| 2026-09-08 | amended | [E20](0012-divergences/aggregates-windows.md#e20) | aggregates-windows |
| 2026-09-11 | Added | [E85](0012-divergences/extensions.md#e85) | extensions |
| 2026-09-11 | added | [E82](0012-divergences/extensions.md#e82) | extensions |
| 2026-09-11 | amended | [E81](0012-divergences/extensions.md#e81) | extensions |
| 2026-09-11 | amended | [E82](0012-divergences/extensions.md#e82) | extensions |
| 2026-09-12 | Added | [E74](0012-divergences/lateral-subqueries.md#e74) | lateral-subqueries |
| 2026-09-12 | Added | [E75](0012-divergences/lateral-subqueries.md#e75) | lateral-subqueries |
| 2026-09-13 | Added | [E32](0012-divergences/names-scopes.md#e32) | names-scopes |
| 2026-09-13 | Added | [E88](0012-divergences/lateral-subqueries.md#e88) | lateral-subqueries |
| 2026-09-13 | Added | [E89](0012-divergences/names-scopes.md#e89) | names-scopes |
| 2026-09-13 | CLOSED | [E02](0012-divergences/names-scopes.md#e02) | names-scopes |
| 2026-09-14 | Added | [E19](0012-divergences/numeric-decimal.md#e19) | numeric-decimal |
| 2026-09-14 | NARROWED | [E88](0012-divergences/lateral-subqueries.md#e88) | lateral-subqueries |
| 2026-09-15 | Added | [E78](0012-divergences/numeric-decimal.md#e78) | numeric-decimal |
| 2026-09-15 | CLOSED | [E82](0012-divergences/extensions.md#e82) | extensions |
| 2026-09-18 | Added | [E23](0012-divergences/names-scopes.md#e23) | names-scopes |
| 2026-09-18 | Added | [E24](0012-divergences/names-scopes.md#e24) | names-scopes |
| 2026-09-18 | Added | [E25](0012-divergences/names-scopes.md#e25) | names-scopes |
| 2026-09-18 | Added | [E26](0012-divergences/names-scopes.md#e26) | names-scopes |
| 2026-09-18 | Added | [E28](0012-divergences/lateral-subqueries.md#e28) | lateral-subqueries |
| 2026-09-18 | Added | [E29](0012-divergences/names-scopes.md#e29) | names-scopes |
| 2026-09-18 | Added | [E30](0012-divergences/numeric-decimal.md#e30) | numeric-decimal |
| 2026-09-18 | Added | [E62](0012-divergences/text-collation.md#e62) | text-collation |
| 2026-09-18 | CLOSED | [E09](0012-divergences/set-operations.md#e09) | set-operations |
| 2026-09-18 | CLOSED | [E32](0012-divergences/names-scopes.md#e32) | names-scopes |
| 2026-09-18 | CLOSED | [E63](0012-divergences/text-collation.md#e63) | text-collation |
| 2026-09-20 | Added | [E76](0012-divergences/lateral-subqueries.md#e76) | lateral-subqueries |
| 2026-09-22 | Added | [E01](0012-divergences/comparison-membership.md#e01) | comparison-membership |
| 2026-09-22 | Added | [E92](0012-divergences/recursion.md#e92) | recursion |
| 2026-09-22 | narrowed | [E76](0012-divergences/lateral-subqueries.md#e76) | lateral-subqueries |
| 2026-09-24 | Added | [E71](0012-divergences/lateral-subqueries.md#e71) | lateral-subqueries |
| 2026-09-24 | Amended | [E02](0012-divergences/names-scopes.md#e02) | names-scopes |
| 2026-09-24 | Amended | [E90](0012-divergences/lateral-subqueries.md#e90) | lateral-subqueries |
| 2026-09-24 | CLOSED | [E83](0012-divergences/extensions.md#e83) | extensions |
| 2026-09-24 | closed | [E85](0012-divergences/extensions.md#e85) | extensions |
| 2026-09-25 | Added | [E72](0012-divergences/lateral-subqueries.md#e72) | lateral-subqueries |
| 2026-09-25 | Added | [E73](0012-divergences/lateral-subqueries.md#e73) | lateral-subqueries |
| 2026-09-25 | Amended | [E02](0012-divergences/names-scopes.md#e02) | names-scopes |
| 2026-09-25 | Amended | [E72](0012-divergences/lateral-subqueries.md#e72) | lateral-subqueries |
| 2026-09-25 | CLOSED | [E88](0012-divergences/lateral-subqueries.md#e88) | lateral-subqueries |
| 2026-09-25 | NARROWED | [E71](0012-divergences/lateral-subqueries.md#e71) | lateral-subqueries |
| 2026-09-26 | Amended | [E01](0012-divergences/comparison-membership.md#e01) | comparison-membership |
| 2026-09-27 | Amended | [E01](0012-divergences/comparison-membership.md#e01) | comparison-membership |
| 2026-09-27 | amended | [E01](0012-divergences/comparison-membership.md#e01) | comparison-membership |
| 2026-09-28 | Added | [E93](0012-divergences/dml-assignment.md#e93) | dml-assignment |
