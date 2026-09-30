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

PostgreSQL's `date = timestamp` promotes the DATE to its midnight; this engine's direct comparison did so for column and literal operands, and every carrier that turns the pair into a KEY did not — a membership answered 0 rows on every arm, NOT IN kept every row, and an EXISTS, LATERAL or JOIN key matched nothing. A DATE scalar subquery against a TIMESTAMP was read by a magnitude guess (an integer inside ±500 000 taken as a day count), so `ts = (SELECT d …)` for 1969-12-31 also matched 1969-12-31 23:59:59.999 on the single-process arms, and on all five when correlated. The pair now meets at TIMESTAMP through one rule and one conversion (`batch.TemporalCommonType`, `batch.DateMidnightMillis`) in the comparison kernel, the equi-join key ladder, the membership set and the stage DAG's inlined set and scalar, which closes those cells without a catalog row. A TIMESTAMP-typed bind parameter against a DATE is still read at DATE (#1426, a defect, not a divergence). Two rows record what stays refused:

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
