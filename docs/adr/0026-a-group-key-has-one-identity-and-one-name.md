# ADR-0026: A GROUP BY key has one identity and one published name

Status: Accepted (2026-08-30, #720 / #723 / #725; amended 2026-09-03 by arc S1 — §4b's deferral is CLOSED, the phantom scan column under it is named at its real site, and a sort or window key over a computed derived alias needs no second name ON THE WIRE because the definition is materialized at plan time; amended three times the same day after review — one identity, one SLOT, one published name, one ALLOCATOR per aggregate, and a NAME never re-read as structure; amended 2026-09-04 by arc E3 — §3a is CLOSED: a HAVING binds its aggregate through the slot that aggregate OWNS, and the gather pairs a lone rename by CLASS (#785); amended again 2026-09-01 for #737 and #759 — a WINDOW above the aggregate is spelled against what it publishes, and the allocator's per-aggregate SCOPE is a boundary with a fixture that attempts it; amended 2026-09-02 with §5 for #792, #775 and #729 — a name re-spelled for dispatch is TYPED where it was re-spelled TO — and with §4a's record that the stage-spelling pass sketched there was built and WITHDRAWN, because a Stage carrying one name per key cannot state a derived alias (#794, #795); amended 2026-09-04 by arc F4 — §3a's fragment-projection residual is CLOSED, and it was TWO defects: an unaliased SELECT item was invisible to the class walk's lookup, and a fragment projection above an aggregate addressed a duplicated name by NAME where it now addresses the SLOT.

§2 REWRITTEN 2026-09-02 from a sketch into the design that closes #794 and
#795: a Stage carries TWO names per GROUP BY key — the PUBLISHED name in
`Stage.GroupByCols` and the RESOLUTION spelling in `Stage.GroupByResolve`,
index-aligned, both through `distributed.OpSpec` and the worker's aggregate
builder. `worker.derivedGroupKeys` is retired to a compatibility fallback,
`refuseUnstageableGroupKey` and all three of its conditions are deleted, and
§4a's refusal is retired with them. What remains refused is a plan that carries
the key's value nowhere — stated by a stream model (`stageStreamColumns`) that
mirrors the join executor's own naming rule.

A key whose expression is an AGGREGATE or WINDOW call was refused there too,
and that refusal is retired: the DISTINCT lowering was RECORDING one, and no
query can write one. §3b.)

## Context

A grouped query says the same expression more than once. `GROUP BY g + 1`
names the key; `SELECT g + 1 AS gk` asks for its value; `HAVING g + 1 > 2`
filters on it; `ORDER BY g + 1` sorts by it; an outer query references it
through a CTE or a derived table. Everything above the aggregate has to
answer one question about each of those: *which key is this?*

Below the aggregate `g + 1` is arithmetic over a column. Above it, the
aggregate has already consumed `g` and emits ONE column carrying the
computed value, so `g + 1` is a NAME. Every consumer that re-reads it as
arithmetic looks for `g`, finds nothing, and answers NULL for every row —
or, in a filter, UNKNOWN for every row, which admits nothing.

Each site had grown its own answer to "is this the same expression":

| site | its rule |
|---|---|
| `physical.buildProject`'s `gbExprToSyn` | exact AST rendering |
| `physical.aggregateOutputName` | `strings.EqualFold` — case-insensitive, paren-sensitive |
| `physical.validate.groupTermKey` | lower-cased, OUTER parentheses stripped only |
| `physical.aggregateProjectionSource` | the projection's own `Expr`/`Column` text |
| `logical.sortTermResolvesOverAggregate` | `strings.EqualFold` on `String()` |
| `sql.resolveGroupByAliasRef` | "contains no `.`, space or parenthesis" |
| HAVING | no rule at all |

So which SPELLING the query used decided which execution path answered.
Against live PostgreSQL 17 over `internal/oracle/typematrix` — one table,
`g = i % 7` with a NULL every thirteenth row, 5000 rows — PostgreSQL
answers eight rows (keys 1–7 and a NULL group) for every one of these, and
wadjet answered:

| SQL | single-process | stage DAG |
|---|---|---|
| `SELECT (g + 1) AS gk … GROUP BY g + 1` | 8 rows, gk **all NULL** | correct |
| `SELECT G + 1 AS gk … GROUP BY g + 1` | 8 rows, gk **all NULL** | correct |
| `SELECT g + 1 AS gk … GROUP BY (g + 1)` | 8 rows, gk **all NULL** | 8 rows, gk **all NULL** |
| `SELECT g AS gk … GROUP BY (g)` | **all NULL** | **all NULL** |
| `SELECT ((g) + 1) … GROUP BY g + 1` | **42803** | **42803** |
| `SELECT (g+1)+2 … GROUP BY g+1+2` | **42803** | **42803** |
| `ORDER BY (g + 1)` over `GROUP BY g + 1` | **refused** | **refused** |
| `HAVING g + 1 > 2` | **0 rows** | **0 rows** |
| `SELECT "g + 1" … GROUP BY "g + 1"` | correct | **loud**: `GROUP BY key "\"g + 1\"" is not a column of its input` |
| `SELECT (g + 1) * 2 … GROUP BY g + 1` | **all NULL** | **all NULL** |
| `SELECT (g + 1) + COUNT(*)` | correct | **all NULL** |

None of these errors except where marked. A grouped query returning the
right number of rows with a NULL key column is the most expensive kind of
wrong answer this repository has: it survives every row-count gate, every
"both paths agree" gate where both paths are wrong together, and it looks
like data.

## Decision

**A GROUP BY key has exactly one IDENTITY and exactly one published NAME.
Every consumer resolves through them; no site compares spellings.**

Three parts.

### 1. `plansql.ExprIdentity` is the identity, and it erases only spelling

`ExprIdentity` (`internal/planner/sql/canonical.go`) renders an expression
with three differences erased and no others:

- **parentheses** — the parse tree already records the grouping;
- **identifier case** — the way PostgreSQL folds an unquoted identifier;
- **whitespace** — which the AST rendering already gave.

The rendering is fully parenthesised at every INFIX node. That is the part
that matters: dropping `ParenNode` and printing `a * b + c` for
`a * (b + c)` would make two DIFFERENT expressions share an identity, which
is wrong in the more dangerous direction than the defect being fixed.
`g - 1 - 2` and `g - (1 - 2)` keep different identities, and stay two group
keys, as PostgreSQL has them. A node kind the canonicaliser does not know
keeps its own `String()` — the behaviour every caller had before.

### 2. A key has a PUBLISHED name and a RESOLUTION spelling, and a Stage carries both

A GROUP BY key answers two different questions, and until 2026-09-02 the stage
DAG had one field for both.

- **What is this key CALLED?** Every consumer above the aggregate — the SELECT
  list, HAVING, a sort key, the next stage's merge, the gather's rename —
  reads the value under one name. That is the PUBLISHED name:
  `plansql.GroupKeyName`, and `Stage.GroupByCols` carries it.
- **Where does the fragment FIND it?** The aggregate looks the key up in its
  own input, which is a different relation with different column names. That
  is the RESOLUTION spelling, and `Stage.GroupByResolve` carries it,
  index-aligned.

The two are the same string for every ordinary `GROUP BY c` and every ordinary
`GROUP BY c + 1`, which is why one field survived so long. They are different
strings whenever the key names a derived table's alias: a join's stream carries
`w` where the query wrote `x.w`, and `y.w` where the join qualified a
duplicate, while the alias's defining expression `a * 3` names a column the
join does not carry at all. `Stage.GroupByCols` was both at once, and the
worker recovered the second by PARSING the first (`worker.derivedGroupKeys`) —
which a text cannot say (§2c: `GROUP BY "g + 1"` names a column, `GROUP BY
g + 1` is arithmetic, and both are recorded as `g + 1`). Every shape whose two
names differ was answered from the wrong one: ONE NULL group over the whole
table, silently, on both DAG arms, where the single-process path answers
PostgreSQL's rows (#736, #777, #781, #794, #795).

A derived key is not a column of the aggregate's input at all, so one of the
two engines has to materialize it. **It is materialized into a hidden slot —
`SlotName(SlotGroupKey, N)`, i.e. `__gb_expr_N` — and PUBLISHED under its
canonical text by a rename at the aggregate's output.** The name a consumer
uses and the name the value is stored under are two different names on
purpose. `GroupKeyResolution.Computed` is the planner's answer to "must this
fragment materialize the value", and nothing downstream re-derives it.

The first version of this ADR materialized the key under its own canonical
text and called the resulting collision "possible and accepted". That was
wrong twice over. It is not rare — any relation carrying a column spelled
like the key produces it, including one a query mints itself with
`SELECT c AS "g + 1"` — and the two engines do not even fail the same way,
so "both behave the same" was false. Measured against PostgreSQL 17 over a
derived table that renames a column to `"g + 1"`:

| | PG 17 | single-process | stage DAG |
|---|---|---|---|
| `SELECT g + 1 AS k, COUNT(*) … GROUP BY g + 1` | 8 rows | **4829 rows**, grouped by the COLUMN | **4829 rows** |
| `… MAX("g + 1")` | 8 rows | wrong | **loud** |
| `COUNT(*) AS "G + 1"` beside a `g + 1` key | 8 rows | correct | **wrong** |

The pre-aggregate projection APPENDS on the single-process path and
`batch.RecordBatch.ColumnIndex` answers with the FIRST exact match, so the
input column won and the query grouped by it; the worker's projection
NARROWS, so the key won and shadowed the column an aggregate needed. One
name, two operators, two different wrong answers.

#### Only ONE class of reader reads the resolution spelling

`Stage.GroupByCols` has about twenty-five non-test readers, and the design is
only safe if each of them reads the name it means. The census, classified:

| class | readers | what they read |
|---|---|---|
| **AUTHOR** — writes both names | `walkStages`' aggregate arm and `stageGroupKeyNames`, `set_op_stages.emitSetOpCountingStage`, `fuse_stage_chains` (carries the pair onto the absorbing join), `agg_over_exchange.rewireAggOverRawExchange` (takes over the dropped scan's list when a merge becomes a raw aggregate), `resolveStageGroupKeys` (settles a derived alias) | both |
| **RESOLUTION** — the fragment that COMPUTES the key | `worker.buildFragmentHashAggregate` and `worker.buildAggInputProjection` via `fragmentGroupKeyPlan`; the three dispatch sites that build a non-merge `OpHashAggregate` (`buildAggregateFragment`, `buildScanAggregateFragment`, the chain-terminal partial); `pruneFusedAggOutputCols`' read-set argument; `agg_over_exchange`'s `aggInputsCovered` | RESOLUTION |
| **PUBLISHED** — everything above the aggregate | `aggregateOutputName` (sort keys), `agg_output_projection`'s `aggregateStageOutputs`/`aggregateStageDecls`, `agg_rename_retarget`, `stageEmittedColumns` and `stageStreamColumns`, `distribution.go`'s `RequiredChildDistribution`/`OutputDistribution`, `exchange_partial_agg`'s payload split and `Exchange.PartialAggGroupBy`, `dispatchFinalAggregateFanout`'s `-interm` stage, `aggregate_shuffle`'s key-coverage test, `shared_subplan_dedup`'s probe-key coverage, `coordinator.aggregate_shuffle`'s pre-computed signature, the worker's merge-mode aggregate and its `mergeByPosition` ordinal | PUBLISHED |
| **PUBLISHED, transitively** — `logical.AggregateSignature.GroupByCols` is not a `Stage` field, but the value it carries IS `Stage.GroupByCols`, through `physical/aggregate_shuffle.go` → `coordinator/aggregate_shuffle.go` → `coordinator.go`'s `PreComputedAggregate` → `worker/executor.go`. Its byte-exact `stringsEqual(node.GroupBy, sig.GroupByCols)` changed meaning with the field, which is why `keyNamesAreTheirSpelling` declines a candidate whose two names differ | the published name |
| **PRESENCE** — asks only whether there IS an aggregate | `native_dag_rewrite`, `fuse_stage_chains`' eligibility tests, `fuse_scan_aggregate_shuffle`, `fuse_scan_shuffle`, `dynamic_filter_attach`, `join_input_projection`, `project_stage_insert`, `filter_carrier`, `carrier_schema`, `eager_feed`, `execute_stage_dag`'s fragment-shape guards | neither |

Two of those moved when the names separated, and both are recorded rather than
inferred. `aggregateOutputName` used to answer the DISPATCH re-spelling,
because a stage published its keys under the spelling the worker computed them
from; it now answers what the fragment EMITS, which is the same name the
single-process aggregate emits for the same query. And `aggregate_shuffle`'s
pre-compute synthesis writes each key twice — once as a select item and once
in the `GROUP BY` — from one list, which is sound only while a key's published
name is also a spelling the base table can evaluate; it now DECLINES a
candidate whose two names differ (`AggShuffleRejectKeyNameIsNotItsSpelling`)
rather than synthesizing SQL over a column the table does not have — and it
asks the stage that COMPUTES the keys, which is never the one
`followToAggregate` hands it. That walk stops at the first aggregate-typed
stage carrying `GroupByCols`, and on the canonical chain (scan-aggregate →
merge → shuffle → join) that is the MERGE, which by this design carries no
resolution list at all: asking IT answered "the two names are the same" for
every plan, so the guard could not fire on the chain it guards.
`followToKeyComputingStage` walks past it, a chain with no key-computing stage
under it answers NO, and
`TestAggregateShuffleDeclinesAKeyWhoseNameIsNotItsSpelling` is the fixture that
reaches the reject — a guard no fixture reaches is untested code on the default
path (method 10, #794 round 2).

The type system carries part of this: `GroupByResolve` is a `[]GroupKeyResolution`
and not a second `[]string`, so a reader that wants a list of NAMES cannot pick
it up by accident, and `resolveExprs` is the one place that turns it back into
text.

#### A RawInputAggregate's input is clustered on the RESOLUTION

`RequiredChildDistribution` demands `ClusteredOn(GroupByCols)` for a grouped
final, and for a MERGE that is right: its input is a partial's output, where
every key is already a column under its published name. A `RawInputAggregate`
final's input is RAW rows, which carry the RESOLUTION spelling — so the demand
is spelled from the resolution list there (`clusteringKeysForAggregate`), and
where the keys are MATERIALIZED by the fragment itself there is no input column
to cluster on at all and the demand is `RequiredAny`. `OutputDistribution`'s
mirror-the-input branch compares against the same list, or it would call an
input that IS mirrored un-mirrored.

No corpus shape reached the mismatch — the exchange's key lookup applies the
runtime's qualified↔bare fallback, and the fixture is too small to splice a
repartition above a raw final — so this is a latent trap closed rather than a
defect fixed, and the shapes that would reach it are in the corpus
(`arm/distinct-aggregate-*`).

#### #794 dissolves by construction

The merge boundary needed no repair at all once the fields separated. A
`final_aggregate` or `merge_aggregate` in merge mode reads a partial's OUTPUT,
where every key is already a column under its published name — so it carries no
resolution list, and `Exchange.PartialAggGroupBy` (minted from the exchange's
PAYLOAD columns) and the `-interm` stage of `dispatchFinalAggregateFanout`
(minted from `stage.GroupByCols`) are reading the only name there is. The
exception is a `RawInputAggregate` final: the distribution pass hash-partitions
RAW rows into disjoint groups and that final aggregates them in one level, so
it computes its keys and carries the list. `stageComputesGroupKeys` states the
rule once and `TestStageCarriesOneGroupKeyList` asserts it in both directions —
a computing stage without a list, and a merge with one, are both failures.

#### The resolution is decided AFTER the projection passes

For a key that names a derived table's COMPUTED alias there are two candidate
spellings — the alias, and the expression that defines it — and which one a
fragment carries is decided by `attachScanSelectProjections` and
`absorbWindowArmProjection`, which run after `walkStages` emits the stage.
`resolveStageGroupKeys` settles it at the end of `PlanDistributed`, exactly
where `resolveFilterAliasSpelling` settles a predicate's spelling and
`resolveDerivedAliasSortKeys` a sort key's (ADR-0025). Its rules, in order:

Every rule asks WHICH ARM first. The key names a derived table, that table is
one arm of the join, and a column of the same name on another arm is a
different value:

1. the stream spells the alias EXACTLY (`y.w`), because the join qualified that
   arm's duplicate column — resolve by that name;
2. the stream carries exactly ONE BARE column of the alias's bare name FROM
   THE KEY'S ARM, some fragment COMPUTED it, and no copy of it was dropped —
   resolve by the bare name;
3. no bare one, and exactly ONE QUALIFIED column of that name from that arm
   whose qualifier is the alias's own (or the key was written bare) — resolve
   by the qualified name;
4. the KEY'S ARM carries every column the DEFINITION reads — resolve by the
   definition RE-SPELLED into the names the stream gives that arm's columns
   (`a * 3` becomes `z.a * 3` where the join qualified z's copy), materialized
   into a slot;
5. none of the above — REFUSED with the ARM named, and the coordinator answers
   the query on its local pipeline.

Skipping the arm is a silent wrong answer and not a missed optimisation, which
is how round 1 shipped it: with the key naming an arm whose own inner ORDER BY
or LIMIT stopped `attachScanSelectProjections`, the only bare column of that
name in the stream is the PROBE's, and the definition's columns are on both
arms. `SELECT z.w, SUM(x.a) FROM decpair x JOIN (SELECT id, a*3 AS w FROM
decpair ORDER BY id) z ON x.id = z.id + 1 GROUP BY z.w` answered `x.a * 3`
where the key is `z.a * 3` — five plausible rows of a different table's value,
`routed=false`, on both DAG arms (#794 round 2).

The RE-SPELLING in rule 4 is half of that fix and not decoration: handing the
fragment the definition's own text lets an ordinary lookup resolve it, and
where two arms carry a column of that name the PROBE's copy wins whichever arm
the key meant. Checking that the arm HAS the column is not enough.

Rule 2's MATERIALIZED test is what keeps it off the shape that killed the
previous attempt. `(SELECT id, SUM(id) OVER () + 0 AS g FROM collslot) x GROUP
BY g` puts a window alias over a table that has its own `g`; the stream carries
that base column under the same name, nothing computed it, so rule 2 declines
and rule 4 answers `__win_0 + 0` — which is what the DAG has always evaluated
there, correctly.

The resolution is decided against what the join's ARMS can SUPPLY, not against
what its `Columns` ships today, and `ensureJoinCarriesEvaluatedColumns` reads
the resolutions and widens the payload to match. The loop has to be closed
rather than either half guessed: resolving against the shipped list refused a
shape the DAG evaluated correctly at base, because the payload used to follow
the key through the GATHER's rename — `aggStageRenames` recorded the DISPATCH
spelling, and the published name IS the query's own alias now, so there is no
rename left to carry it.

#### The model the rules read

`stageStreamColumns` (`planner/physical/stage_stream_model.go`) lists what a
stage's fragment SHIPS, per column, and it mirrors the executor rather than
guessing:

- a JOIN emits the probe's columns and then the build's with every DUPLICATE
  name QUALIFIED by its owning alias, which is `joinOutputSchemaWithMapping`'s
  own rule — so a stream really does carry `w` and `y.w` at once. §4a's claim
  that "a join stream carries `w`, never `y.w`" was a fact about the old MODEL
  and not about the engine (#795);
- every column carries its ORIGIN ARM — the `BuildTableAlias` (or
  `BuildColOrigins` entry) of the build subtree that produced it, "" for the
  probe side — and EVERY rule asks it first. Setting it only where the join
  QUALIFIED a duplicate was the round-1 defect: with the arm unknown for every
  uncontested column, no rule could ask which arm a bare `w` came from, and a
  key naming an arm whose own inner ORDER BY / LIMIT stopped
  `attachScanSelectProjections` bound the OTHER arm's column of that name —
  five plausible rows of a different table's value, `routed=false`, on both DAG
  arms (#794 round 2);
- a duplicate the join cannot qualify is DROPPED, and the model records it as
  dropped so a key naming that arm is refused rather than bound to the other
  arm's column of the same name;
- the PRIMARY build is `RightDepStage` (else `Dependencies[1]`), which is
  `buildTaskInputsForStage`'s own rule. Taking "every dependency that is not
  the probe" gave a CHAINED link's build the primary join's alias as well as
  its own, so one arm's columns appeared twice under two aliases and a bare key
  of that name looked ambiguous where the stream has exactly one;
- a CHAINED link carries its OWN `Columns` as that link's output filter, so a
  fused chain's real output is the LAST link's list — reading the stage's list
  is what refused a CTE shape the DAG was executing correctly (#795);
- an output filter is applied with BOTH halves of the qualified↔bare fallback
  the executor applies;
- a column is MATERIALIZED when some fragment computes it under that exact
  name (a projection output, a window output, an aggregate key or output) and
  merely present when a scan reads it. Only the first can be a derived alias.

`Stage.Columns` on a scan is a READ SET and not an output schema — it carries
names ancestors asked for, including ones no file has — so the model intersects
it with the catalog's declared schema WHERE ONE IS KNOWN. That qualifier is
load-bearing and the earlier draft of this paragraph hid it:
`annotateScanSchemas` runs at the END of `PlanDistributed`, AFTER
`resolveStageGroupKeys`, so `ScanSchema` is empty at the moment the model runs
and the intersection is INERT today. What actually keeps a phantom name from
mattering is the MATERIALIZED test — a read-set entry is never materialized, so
rules 2 and 3 cannot take it, and the worst a phantom can do is let rule 4
accept the DEFINITION, which is the pre-arc behaviour. The intersection is
there for the day the schemas are annotated earlier, and it is written down as
inert rather than described as protection (rule 9).

#### Compatibility is a decision

An `OpSpec` with no `GroupByResolve` is an OLDER coordinator, and the worker
falls back to `derivedGroupKeys` — the text parse this field replaces, which is
exactly the behaviour that worker had before. That is the precedent
`buildAggInputProjection` already set for `GroupByTypes`, and it is asserted
(`TestFragmentFallsBackWhenTheCoordinatorSendsNoResolution`) rather than
described.

An `OpSpec` whose resolution list is PRESENT but not index-aligned with the
published one, or whose Computed entry does not parse, is NOT a version: a
coordinator that sends the field sends it aligned, and
`TestStageCarriesOneGroupKeyList` asserts that at plan time. Falling back there
would answer the query by the pre-arc rule with no signal, so the task FAILS
instead (`TestFragmentRefusesAMisalignedResolutionList`). What the check can see is LENGTH: a list of the wrong length, or an entry that does not parse, fails the task naming both lists. A list of the right length whose entries are PERMUTED is indistinguishable from a correct one on the wire — the entries are positional and carry no key name — so that skew is prevented by upgrade order (workers first), not detected. The fallback is for
the ABSENT list and nothing else.

The other direction is NOT supported, and the measurement is worse than the
first draft of this paragraph claimed. A NEW coordinator against an OLD worker
sends published names in `GroupByCols`, and that worker computes every key from
them. Stubbing `fragmentGroupKeyPlan` to decline — exactly that pairing — turns
**46 assertions in `internal/coordinator` and 4 in `internal/worker` red**, and
the red list includes shapes that are RIGHT on base:
`781/a-computed-decimal-alias-over-a-bare-scan`,
`792/a-decimal-expression-alias-as-a-key`,
`ctl/a-derived-table-with-an-order-by-inside`,
`ctl/a-derived-table-with-a-limit-inside`,
`ctl/a-derived-alias-that-shadows-a-base-column`,
`ctl/unwrapped-window-output`, `ctl/a-non-window-alias-beside-a-window`. So the
skew is not "degrades the same way and no further": the published name a new
coordinator sends is not the spelling the old worker's parse computed from, and
shapes that were right become wrong. Workers upgrade FIRST, that ordering is
the whole of the protection, and nothing on the wire detects a violation of it.
A version marker on `OpSpec` would — it is not in this arc, and the reason it
is not is that a marker only turns a silent wrong answer into a loud one for a
deployment the project does not otherwise support; the honest record is this
paragraph.

### 2a. A slot is ALLOCATED, never merely named

`SlotName(family, n)` is a namer. Naming is not enough, because a slot is
safe only when NOTHING else answers to it, and three different things can:

- a name already **in scope** — a table may legitimately store a column
  called `__gb_expr_0`, and it is never refused at read;
- a name the query **binds** elsewhere — another group key, an aggregate's
  argument or output, a filter column;
- a slot the same query has **already issued**.

A per-key namer sees only the first, and that is a wrong answer, not a
missed optimisation. Over a table carrying `__gb_expr_0`, two derived keys
landed in one column: key 0 stepped off the stored name onto `__gb_expr_1`,
key 1 started at its own index, found `__gb_expr_1` free, and took it — so
the second key silently carried the first's value and a twelve-group query
answered three. The DAG had the mirror image: `SUM(__gb_expr_0)` beside
`GROUP BY g + 1` was answered from the key's slot, because the worker's
projection narrows and the key had claimed the name first — right keys,
right row count, the sum of a group key.

**A slot is therefore ALLOCATED from a per-aggregate (planner) or
per-fragment (worker) allocator that excludes all three sets, and
termination is bounded by their size rather than assumed.** Two authors hit
this same bug independently, which is the evidence that the shared API
needs an allocator and not only a namer.

### The SCOPE is the AGGREGATE, and that is a boundary, not an oversight (2026-09-01, #759)

`__win_N`'s scope had to become the QUERY (ADR-0025 §"A scope is the QUERY,
not the block"), because two sibling subqueries minting one window slot
carried a column of that one name into a join and the projection above it
published one window's value twice. The group-key allocator is per AGGREGATE
and #759 was filed against it as the same defect one slot family over.

It is not, and the difference is what the two slots ARE. A window slot is an
OUTPUT column: `exec.Window` appends it to its input and it travels up through
every consumer, a join included. A group-key slot is a column of its
aggregate's OWN INPUT — the pre-aggregate projection materializes it, the
aggregate groups on it, and the aggregate publishes the value under
`plansql.GroupKeyName`. The slot never leaves the operator that minted it, and
two sibling aggregates have disjoint input streams, so one name in both is two
different columns that never meet.

Method 10 does not accept that as an argument, only as a claim with a fixture
attempting it. `ctl/SiblingAggregatesEachMintingASlot` is the attempt, over
`collslot` — whose two STORED slot-family columns push both aggregates' first
allocation to the same `__gb_expr_2`, so the shapes really do mint one name
twice. Five spellings: two siblings joined, a sibling NESTED in a sibling (the
shape that broke the window family on the single path), each sibling minting
TWO slots, both stored slots read as aggregate ARGUMENTS at once, and the
UNION ALL form. Every one is asserted on the KEY and on BOTH siblings'
aggregate values, which are far apart on purpose — a count of 80 beside a sum
in the thousands — and every one agrees with PostgreSQL 17 on all arms.

The claim this records is therefore narrow: the per-aggregate scope is
sufficient *because the slot is not published*, and the day a group-key slot
becomes visible above its aggregate — a stage that ships `__gb_expr_N` as its
output name would be one — the scope has to widen with it and these fixtures
are what says so.

`__gb_expr_` is in the RESERVED namespace (`planner/physical/reserved_slots.go`):
a user column, derived-table output or SELECT alias spelled inside it is
refused with 42601 naming the family, so no query can put a value there and
the collision is **impossible**, not accepted. That reservation is a
deliberate divergence from PostgreSQL, which has no reserved column
namespace — recorded in ADR-0025 — and it is the right trade, because the
alternative to refusing those queries is not answering them but answering
them wrongly.

Two keys are NOT materialized, and both matter:

- a key whose expression is a bare column of the input — it is already
  there, which is every ordinary `GROUP BY c`;
- a key an aggregate DIRECTLY BELOW already publishes under the same name
  and identity. `SELECT DISTINCT g + 1 AS k … GROUP BY g + 1` lowers to two
  aggregates keyed alike, and the outer one reads the inner one's OUTPUT:
  its recorded expression still says `g + 1` over a `g` that is no longer
  in scope, so materializing it would evaluate that `g` against a schema
  without one and collapse the table into a single NULL group. Only an
  aggregate below counts — a derived table that merely has a column SPELLED
  like the key carries a different value under that name, which is the
  collision above.

`groupKeyByIdentity` therefore indexes a key whenever a consumer cannot
simply NAME it: derived, elided-literal, or published under a text no column
reference can spell.

### 2b. `plansql.GroupKeyName` is the published name, and both engines use it

A bare column reference is published under its own name with any delimiters
stripped: `GROUP BY "g + 1"` names the column `g + 1`, not the four tokens
its quoted spelling lexes into. Anything else is published under its own
rendered text with redundant OUTER parentheses removed, so
`GROUP BY (g + 1)` and `GROUP BY g + 1` publish one name.

Case is PRESERVED here, because a batch column is matched by BYTES
(`batch.RecordBatch.ColumnIndex`). The identity folds case because it is
only ever compared; the name does not, because it has to be found.

The single-process pre-aggregate projection now materializes a derived key
under that same name instead of a synthetic `__gb_expr_N`. The two engines'
aggregate output schemas are therefore IDENTICAL, which is what lets one
logical rewrite — the HAVING respelling below — be evaluable on both. A
LITERAL key keeps the synthetic name: it is elided from the key set and
re-attached as a constant, no consumer resolves a constant by expression,
and `1` or `'x'` makes a poor column name.

`physical.groupKeyOutputs` states the naming rule once
(`internal/planner/physical/group_key_identity.go`), and
`aggregateOutputNames`, the pre-aggregate projection, the projection above
the aggregate and the gather's rename all read it from there.

### 2c. Only a column REFERENCE is resolved as a column name

A key's recorded TEXT cannot say whether the query wrote a name or an
expression: a delimited identifier's quotes are not part of its name, so
`GROUP BY "g + 1"` and `GROUP BY g + 1` are both recorded as `g + 1`.
Asking the text whether some column answers to it bound the ARITHMETIC key
to a delimited column of that spelling — PostgreSQL answers five groups of
the sum, both DAG arms answered nine groups of the column, silently.

**The parsed form decides.** A term that is not a `*ColRef` is never
resolved as a name; only its LEAVES are. PostgreSQL's rule is that unquoted
`g + 1` is arithmetic, full stop.

The same rule settles what a NAME may travel through. A key defined by a
rename Project is re-spelled into SOURCE columns for DISPATCH only
(`aggStageDispatchKey`), because the DAG flattens that Project;
`aggregateOutputName` does not take that path, since it answers what a SORT
KEY names and a sort key is resolved on both engines.

And a key so re-spelled is TYPED below the rename chain, which is #387's
own rule: `inputColDecls` stops at a Project, so a derived key over a
renamed DECIMAL column had no declared type, fell to the float rule, and
handed exact fixed point to a FLOAT64 vector.

### 2d. A name is never re-read as structure — in EITHER direction

§2c settles which way a key BINDS. The same confusion runs the other way, in
the rule that decides whether a query is legal at all.

`GROUP BY "g + 1"` groups by one COLUMN and says nothing about `g`, so
`SELECT g + 1` beside it reads an ungrouped column and PostgreSQL refuses it
with 42803. The grouping-coverage walk recorded each term's *recorded text*
as well as its parsed form — and since #725 that text is the key's published
NAME with the delimiters stripped, so re-parsing it read a column as
arithmetic and marked `g` grouped. The query then answered: 60 rows with a
NULL key on the single-process path, 3 rows on the DAG. Two engines
disagreeing about a query neither should answer.

`"g plus 1"` did the same with no operator in it at all — it parsed to just
`g` and marked THAT grouped — which is why the repair is to stop reading text
as structure rather than to special-case operators.

**The grouped terms are read from their PARSED forms and from nothing else.**

### 2e. A list that PRUNES columns is part of the identity (2026-09-04, #731 round 2)

Every rule above is about how a name RESOLVES. There is a class of list that
never resolves anything and decides something stronger: whether the column
exists downstream at all. A join's `OutputFilter`, an exchange's payload
manifest, a scan's read set — each is built from the names a consumer asked
for and each drops what it does not recognise.

Those lists were written as an optimization ("skip columns nothing needs") and
so they compared BYTES. That is exact until two relations of one join carry
the same column name in different cases, which they may, because an unquoted
reference folds and a delimited one does not: `rvya("MixedCol")` joined to
`rvyb(mixedcol)` publishes `[k mixedcol rvya.k rvya.MixedCol]`, qualifying the
colliding build column by relation — §1's identity, applied to the SCHEMA.
The consumer then asks for `rvya.mixedcol`, no byte-exact test matches
`rvya.MixedCol`, the column is dropped, and the reference above falls back to
the bare name and binds the OTHER relation's column.
`SELECT rvya.MixedCol FROM rvyb, rvya WHERE rvya.k = rvyb.k` answered 900
where PostgreSQL answers 100, on all four arms — and `SELECT rvya."MixedCol"`,
which is the only spelling PostgreSQL itself can resolve, answered NULL.

**A pruning list applies the same identity as a resolution: a column matches
when its name FOLDS to a name the list asks for and the two agree on the
RELATION — byte-exact when both spell one, since a delimited alias is
byte-exact, and either side may leave it off.** A reference cannot resolve
what the join did not ship, so a resolver that holds the identity perfectly is
worth nothing while the list above it holds bytes. Keeping a column the list
did not name exactly costs bytes; dropping one it did name costs an answer,
and the asymmetry is the whole argument for matching permissively.

Where each of the three stands today, because the rule is stated for the class
and only one of them implements it:

| list | how it holds the identity |
|---|---|
| a join's `OutputFilter` | `exec.outputFilterMatcher` — the rule, in code. It covers BOTH paths: a join `Stage.Columns` on the DAG *is* the OutputFilter, and the worker's fragment passes it to the same `joinOutputSchemaWithMapping`. |
| a scan's read set | by its OWN folding, not by that matcher: `logical.sanitizeScanNeeds` looks a needed name up in the scan's schema case-insensitively and attributes a QUALIFIED name to the matching relation only, so `needs=[k mixedcol rvya.k rvyb.k]` keeps `[MixedCol k]` from `rvya` and `[k mixedcol]` from `rvyb`. Correct today, by a second implementation of one rule. |
| an exchange's payload manifest | NOT independently established. It is covered IN FACT for every shape in the colliding corpus — the two DAG arms run all of them, and the `BroadcastBytesOverride=1` arm forces every build through an `exchange-repartition`, so the manifest carries these columns under a shuffle as well as a broadcast — and it is widened from the same `NeededColumns` the join's filter is built from. It is the place to look first if a colliding-name shape ever diverges on a DAG arm alone. |

Two implementations of one rule is a standing hazard, and the reason this
table is here rather than a claim that the class is handled.

The corollary for gates: a corpus that always writes the odd-spelled relation
FIRST cannot see any of this. The first relation of a FROM list is the join's
PROBE side, published unqualified, and the bare byte-exact test matches it.
Every colliding-name shape belongs in the corpus in BOTH FROM orders.

## The impossibilities, and the fixtures that attempt them

Method 10 of the correctness-fix protocol: a claim of the form *X cannot
happen* is exactly where the next regression lives, and it is invisible to
every gate until a fixture contains X. Each claim above is listed here with
the fixture that tries it.

| claim | fixture that attempts it |
|---|---|
| a slot is a name no query can spell | `collslot` STORES `__gb_expr_0` and `__gb_expr_1` — admitted through the catalog, because the DDL door refuses them; `ctl/TheStoredColumnIsStillGroupable` reads and groups by one |
| …including one the query itself mints | `SlotCollidesWithAStoredColumn/MintedByADerivedAliasIsRefused` — `1 AS "__gb_expr_0"` is REFUSED at the alias door (#694's reservation), which is stronger than allocating around it; a STORED column of that name is still admitted and read, and the rows above are that case |
| the slot never shadows an aggregate's argument | `AggregateOverTheStoredColumn` — `SUM`/`MAX` of the stored column, asserted on the VALUE |
| two keys never share a slot | `TwoDerivedKeys`, `ReversedKeyOrder`, `ThreeDerivedKeys`, `WithHaving`, all with the stored slot present |
| an arithmetic key and a delimited column of that text are different things | `ArithmeticKeyBesideADelimitedColumnOfThatText` — both directions, 5 rows against 9 |
| a delimited term does not group what its name spells | `gcov` carries `"g + 1"` and `"g plus 1"`; `GroupingCoverageUnderADelimitedTerm` asserts the 42803 AND the answering direction, plus three controls that must keep refusing and one that must keep answering |
| a pruning list can compare bytes because it only drops what nothing needs | `collide.Corpus()` carries `clt4("MixedCol")`, `clt5(mixedcol)` and `clt6("MIXEDCOL")` and names every two-relation shape in BOTH FROM orders — 21 entries fail on all four arms when the join's output filter is put back to byte-exact |
| two SIBLING aggregates never share a slot | `SiblingAggregatesEachMintingASlot` — five spellings over `collslot`, whose stored slot columns make both aggregates allocate the same `__gb_expr_2`; joined, nested, two keys each, over the stored slots, and UNION ALL, asserted on both siblings' aggregate VALUES (#759) |

### 3. HAVING is spelled against what the aggregate publishes

The predicate is rewritten in the LOGICAL plan
(`plansql.ReplaceGroupKeyRefs`, called from `logical.BuildFromSelect`), so
both engines receive one they can evaluate. The walk is TOP-DOWN and stops
at the first whole-term match, so the LARGEST expression that is a key is
the one replaced rather than descending to a column the aggregate does not
emit. It never enters an aggregate call: inside `SUM(g + 1)` the expression
is evaluated over the aggregate's INPUT rows, where `g` is exactly the
column that does exist.

Bare column keys are deliberately absent from the identity→name map. Their
value is published under the input column's own name and every consumer
already reads it there; a mapping for them would only re-route a resolution
that works.

#### 3a. An AGGREGATE OUTPUT may still take a group key's name, and the HAVING binds it through the slot it OWNS (2026-09-02 #785, CLOSED 2026-09-04 by arc E3)

This ADR gives a group KEY one identity and one name. It says nothing about
an AGGREGATE OUTPUT minting the same name, and one can:

```
SELECT COUNT(*) AS g, g AS x FROM t GROUP BY g HAVING COUNT(*) > 0
  → Aggregate: group_by=[g] aggs=[count() AS g]   -- TWO columns named g
    Filter: [g > 0]                                -- the HAVING
```

Every consumer above the aggregate resolves a name through
`batch.RecordBatch.ColumnIndex`, which returns the FIRST match — the key. So
the HAVING was evaluated against the key's values `{0,1,2}` instead of the
counts `{80,80,80}`: `> 0` dropped one group, `> 1` two, `> 79` all three,
where PostgreSQL 17 keeps all three every time. (The ladder is the
instrument: a row count alone cannot tell "bound to the key" from "bound to
the count".) The computed spelling is the same collision —
`GROUP BY g + 1` emits the key as `g + 1` and `COUNT(*) AS "g + 1"` names the
aggregate that — and it answered 0 rows for PostgreSQL's 8, on all four arms.

**The exact-site fix does not work, and the reason is structural.** Giving
the colliding aggregate a hidden slot and letting the SELECT-list projection
rename it — the nested-aggregate rewrite's existing machinery — fixes the
ladder on the single-process path and BREAKS the DAG. Measured: the
`HAVING g + 1 > 2` control, right on all four arms before, came back with
the COUNT under `k` and NULL under `g + 1`. The projection above the
aggregate becomes `[g + 1 AS k, __agg_0 AS "g + 1"]`, whose output name
`g + 1` is another item's SOURCE name — a PERMUTATION — and
`absorbAggregateOutputProjection` carries the SELECT list onto the aggregate
stage as a rename map, which has no order.

**The fix is the OTHER direction: nothing about the SELECT list changes, and
the HAVING stops asking the batch for a name two columns answer to.**

A HAVING's aggregate is REUSED from the SELECT list when the two normalize to
the same (function, input, distinct) — that is what keeps `SELECT a, COUNT(*)
AS c … HAVING COUNT(*) > 1` from counting twice. The reuse points the
rewritten predicate at `AggExpr.OutputCol`, and that is the whole defect: an
output column's NAME is not a handle when the aggregate's own output batch
answers to it twice.

So the reuse is DECLINED when `AggExpr.OutputCol` is also a GROUP BY key's
published name (or a second aggregate's output name), and the HAVING takes the
branch that already existed for an aggregate the SELECT list does not carry: a
`__having_N` slot, which nothing else in the batch answers to, computed a
second time. `logical.aggOutputNameIsShared` is the test and it is asked of
the aggregate's own output batch — group keys under their published names
(`cleanExpr` of the GROUP BY term, what `NewAggregate` is given, §2b) plus the
aggregate outputs.

Costing one extra aggregate in the collision case is the price, and it is
paid only there. The SELECT list is untouched: a duplicate OUTPUT name is
legal SQL — PostgreSQL accepts `SELECT COUNT(*) AS g, g AS x` and answers it —
and `absorbAggregateOutputProjection` still declines over it, so no
permutation is ever carried as a rename map.

**The gather's half of the same collision.** `renameSourceIndices`
(`coordinator/execute_stage_dag.go`) pairs a group of renames sharing one
source name with the columns of their own CLASS (#575); a group of ONE fell
through to `resolveRenameSource`, which is deterministic in the name and
therefore always answered the FIRST column. With the key NOT in the select
list — `SELECT COUNT(*) AS g, MIN(id) AS m FROM t GROUP BY g` — there is
exactly one rename spelled `g` and the first column of that name is the KEY,
so both DAG arms answered the key's values under the aggregate's alias while
single and spilled answered the count. `classScopedMatch` applies the same
class rule to a singleton group: an aggregate output takes the LAST column of
its name, a key reference the FIRST, and with one column of the name both
answers are that column.

What must not stand is `ColumnIndex`'s first-match rule deciding which of two
columns a query meant.

**It still does, one operator further in, and the boundary is written down
rather than claimed away** (2026-09-04 round 2). The gather's pairing is by
CLASS and the class is now carried THROUGH a wrapper — `renameIsAggregateOutput`
walks the renames to the projection that defines the name and stops where the
two classes are separated, which is the Project whose input is the aggregate's
own output — so a derived table over the collision answers PostgreSQL's rows on
every arm. A CTE does not: there the fragment's OWN projection has already
applied the block's SELECT list, so the gather sees no duplicate at all, and it
is that projection which resolved the name against the aggregate's output and
took the first match. Closing it means a projection addressing an aggregate's
outputs by POSITION (`exec.ProjectColumn.SourceIdx` exists and nothing sets it
for this shape), which is this rule one operator over and its own change.

**The boundary is the FRAGMENT projection, not the CTE.** Calling the residual
"the CTE spelling" reads narrower than it is, and three spellings were pinned
rather than one, at
`internal/coordinator/arc_e3_names_scopes_two_path_test.go`: `785/nested-in-a-cte`,
`785/nested-in-a-derived-table-inside-a-cte`, and
`785/nested-two-derived-tables-deep` — the last with no CTE anywhere in it.

**CLOSED 2026-09-04 (arc F4), and the paragraph above was right about the
boundary and wrong about the mechanism at one of the three.** Measuring the
three apart is what closed them, because they are TWO defects:

- **Two derived tables put NO fragment projection anywhere.** The stage list
  for `SELECT z.g, z.x FROM (SELECT u.g, u.x FROM (…collision…) u) z` is
  `scan → final_aggregate → gather`, measured, so "a SECOND wrapper puts a
  fragment projection between the aggregate and the gather exactly as a CTE
  does" was not true. What was wrong is the CLASS. `renameIsAggregateOutput`
  walks to the projection that defines the name, and the lookup it used —
  `projectionForName` — matches on `Projection.Alias` only. A SELECT item
  written with no alias (`SELECT u.g, u.x`) has none, so the walk found no
  item at all, returned "not an aggregate output", and `classScopedMatch`
  paired both renames with the first column of the name: the group KEY.
  `projectionPublishingName` widens the lookup to an unaliased item's own bare
  column name, for the CLASS walk alone. The name-RESOLVING walks keep the
  narrow lookup deliberately: for them an unaliased qualified item resolves to
  ITSELF (`u.x` → `u.x`), a fixpoint that stops the walk one Project short of
  the answer — widening `projectionForName` itself was measured and made
  `SELECT u.g, u.x FROM (…) u ORDER BY u.x` refuse its whole plan, because the
  sort key stopped resolving to the column the aggregate emits.
- **A CTE really does put a fragment projection there**, and that projection
  resolved `g` by NAME against the aggregate's output — where the key and the
  count both answer to it — and took the first. It addresses the SLOT now:
  `ProjectExprSpec.SourceSlot` carries the position the planner chose,
  `distributed.ProjectSpec.SourceIdx` carries it on the wire, and both fragment
  builders turn it into `exec.ProjectColumn.SourceIdx` — the same addressing
  the single-process projection has applied since #575. The slot is decided
  from the producer's own output order (`[group keys…, aggregate outputs…]`)
  and the CLASS the gather's renames already carry; a producer whose output is
  not that shape gets no slot and keeps the name path, because a slot read off
  a model that does not hold is worse than the name it replaces.

`785/nested-in-a-derived-table` — ONE wrapper, right on all four arms before —
remains the control, and two cells were added to attempt the boundary from both
sides: `785/nested-three-derived-tables-deep` and `785/nested-in-a-cte-key-first`
(the two classes in the other order in the SELECT list). All five assert
`routes=none` beside their rows, so a shape that started ROUTING rather than
answering is not mistaken for a fix.

#### 3b. A lowering records the SLOT the operator below publishes, never the call the query wrote (2026-09-04, #797)

`SELECT DISTINCT g, COUNT(*) + 0 AS w FROM t GROUP BY g` lowers to an outer
aggregate whose keys are the SELECT list's expressions
(`logical.rewriteDistinctAsGroupBy`). One of those expressions holds an
aggregate CALL, and a pre-aggregate projection cannot evaluate one: the value
was computed by the operator BELOW and published under `__agg_0`.
`physical.refuseUnevaluableGroupKey` said so and the coordinator answered on
its local pipeline — right rows, both DAG arms routed, for a query the DAG can
run.

**The two names were already there and the lowering read the wrong one.** The
builder rewrites a projection that WRAPS an aggregate or a window into a
reference to that operator's own slot (`__agg_0 + 0`, `__win_0 + 0`) and
stores the rewrite in `Projection.ASTExpr`; `Projection.Expr` keeps the text
the query wrote. `projectionGroupKey` returned `p.Expr`. That is §2c and §2d
one layer up — a key with two names that disagree, and a NAME re-read as
structure — and the window spelling could not even round-trip through
`ParseExpression`, because `WindowFuncNode.String()` renders `OVER (...)`.

So a lowering that MINTS a group key records the spelling the operator below
publishes. `exprReadsReservedSlot` is the test: a projection whose AST reads a
column in the planner's own hidden-slot namespace was re-spelled by the
builder, and its TEXT is stale. Both spellings run as stages now, and the two
`routed=true` pins that recorded the refusal are deleted.

### 4. A WINDOW above the aggregate is spelled against what it publishes (2026-09-01, #737)

`SELECT g + 1 AS k, ROW_NUMBER() OVER (ORDER BY g + 1) FROM t GROUP BY g + 1`
answered the right eight rows with the key NULL on every one of them, on all
five arms. Two consumers of the same fact, each reading `g + 1` as arithmetic
where it is a NAME:

- **the SELECT item.** The walk that re-points a select item at its key's
  column stops at any node that does not leave the aggregate's own columns
  visible, and a `NodeWindow` was on none of their lists. It belongs there —
  `exec.Window` APPENDS its output and renames nothing, which is the same
  answer `scopePreservingWrapper` gives for the relation-scope question.
  `logical.AggScopePreservingWrapper` states the list once and **all FIVE**
  walks read it — `physical.aggregateUnderOutput` for the gather,
  `physical.findAggregateAncestor` for the single-process projection,
  `physical.groupKeysPublishedBelow`, which decides whether an aggregate
  DIRECTLY BELOW already publishes the key, `logical.AggregateOverGroupRows`,
  which decides whether a Project's INPUT rows are one per GROUP and therefore
  whether a predicate above it may be substituted below, and
  `physical.aggregateOutputNames`, which answers what the aggregate below
  PUBLISHES so each projection can be pinned to the physical slot its
  provenance names.

  The count in this section has been wrong four times, and the sequence is the
  point: "both walks read one list" (two), then three, then four, then five.
  Each miss cost the same kind of answer. The third kept its own hardcoded
  Filter/Sort/Limit list, so `SELECT DISTINCT g + 1 … GROUP BY g + 1` with a
  window between its two aggregates re-materialized a key the inner one had
  already published and collapsed the table into ONE NULL group. The fourth
  asked its question through `logical.AggregateBelowProject`, whose list is
  Filter-ONLY, so a WHERE on the key applied above a window substituted `k`
  away to `(g + 1)`, met a schema with no `g`, and admitted no row at all on any
  arm (#774). The fifth descended NodeFilter alone while the call site that
  guards it (`findAggregateAncestor`) read the full list, so with a WINDOW
  between them #575's duplicate-name slot pinning was skipped and
  `SELECT COUNT(*) AS g, g AS x, ROW_NUMBER() OVER (ORDER BY g) … GROUP BY g`
  published the KEY's value under the aggregate's alias on the single-process
  path.

  The list therefore lives in `logical` and not in `physical`: the fourth
  reader is in that package, `physical` imports it and not the reverse, and a
  COPY is exactly what let the two disagree. `physical.aggScopePreservingWrapper`
  is now a delegation.

  `logical.AggregateBelowProject` keeps a narrower, Filter-only list ON PURPOSE,
  and says so at its definition. Its two callers —
  `physical.aggregateProjectionTarget` and `physical.aggregateGroupKeyName` —
  map a Project's SELECT list onto the aggregate's own STAGE, and a Sort, a
  LIMIT or a WINDOW between the two emits a stage of ITS own that the projection
  would be carried past. "Are these rows one per group" and "which stage does
  this Project sit on" are two questions, and conflating them is how #774 was
  written.

  `TestAggScopePreservingWrapperIsReadByEveryWalk` states exactly what is
  checked: **these five NAMED readers agree with the list, and the list covers
  every node type the logical package declares.** It cannot discover a SIXTH —
  it drives the five by name, and a review proved the point by adding a walk
  with its own list and watching the test pass. Saying otherwise is an
  overclaim this ADR has now made twice.

  A source-level guard was considered and rejected, and the number that
  justified the rejection was wrong. This ADR said "twelve functions in the
  package carry a `NodeFilter, NodeSort, NodeLimit` case". The census is:
  **29 functions in `internal/planner/physical` carry a literal case naming at
  least one of the three** (17 on the strictest reading, a clause containing all
  three), plus **6 more** that walk the same kinds through the shared predicate,
  and **15** in `internal/planner/logical`. So an allowlist would be closer to
  thirty entries than to eleven, and the argument against it is stronger than
  the one originally made — but it is an argument against a GUARD, not against
  a RECORD, so here is the record. Every candidate that is even arguably asking
  this section's question, with its decision:

  | function | list | decision |
  |---|---|---|
  | `physical.aggregateUnderOutput` | shared | reader 1 |
  | `physical.findAggregateAncestor` | shared | reader 2 |
  | `physical.groupKeysPublishedBelow` | shared | reader 3 |
  | `logical.AggregateOverGroupRows` | shared | reader 4 (#774) |
  | `physical.aggregateOutputNames` | shared | reader 5 (#575 under a window) |
  | `physical.wrapsAWindow` | shared | a REFINEMENT of the question — "is one of the wrappers specifically a Window" — used to keep the projection-elision decision from looking through one, because a window ADDS a column and elision needs the node's WHOLE output |
  | `logical.AggregateBelowProject` | Filter only | deliberately narrower; its callers map a SELECT list onto the aggregate's own STAGE and a Sort/LIMIT/window emits a stage of its own. Documented at its definition |
  | `physical.scopePreservingWrapper` | Filter/Sort/Limit/Distinct/**Window** | the RELATION-scope twin of this question, already has Window |
  | `physical.resolveSortKeyColumn` | Filter/Limit/Sort/Distinct, no Window | MEASURED, left alone. `SELECT g AS k, ROW_NUMBER() OVER (…) … GROUP BY g ORDER BY k DESC` and six siblings answer PostgreSQL's order on all four arms — the call site's `producerMaterializesName` reset already covers it, and its mirror walk `derivedAliasSourceColumn` also excludes Window, so changing one alone is the ADR-0025 out-of-step shape |
  | `physical.aggregateUnderWindow` | Filter/Project/Sort/Limit/Distinct, no Window | MEASURED, left alone. Five stacked-window-over-aggregate shapes over a DECIMAL(18,4) column — the type question it exists for — answer PostgreSQL's values on single and on both DAG arms. Adding Window would also need `windowSpecOutputType` layered for the skipped window, so it is a change with its own gap and no defect to justify it |
  | `logical.aggregateBelow` | Filter/Project/Window/Sort/Limit — its own, WIDER | a sixth de-facto reader, filed as **#787**; wider than the shared list by `NodeProject`, so it is a different question or a bug, and either way not settled here |

  The schema walks (`inputColTypes`, `emittedColTypes`, `emittedColDecimal`,
  `inputColDecimal`, `strictIntArithCols`, …) are NOT candidates: they ask what
  a node EMITS, and most already carry their own `NodeWindow` arm that ADDS the
  window's outputs, which is the correct answer to a different question. The
  set-operation walks and the locator walks are likewise their own questions.

  What finds the next reader is a review counting them, which is how the third,
  the fourth and the fifth were each found.
  On the DAG the SELECT list is attached to the WINDOW stage's fragment
  (ADR-0025 shape g), and that projection is respelled over the producer's
  emitted columns by the same `respellSpecsOverProducerOutput` the
  `StageProject` branch uses.

- **the window's OWN spec.** Its argument, its PARTITION BY and its ORDER BY
  keys are evaluated over the aggregate's OUTPUT too. `ORDER BY g + 1` reached
  `resolveWindowKeys` as arithmetic, was materialized by EVALUATING it against
  a schema with no `g`, and ordered by NULL on every row — the right rows in an
  arbitrary sequence, which no row-count or key-set assertion can see. It is
  respelled in the LOGICAL plan, where §3 already respells HAVING for the same
  reason, and rendered as a DELIMITED identifier so the key resolver reads it
  as the NAME it is rather than re-parsing it as structure (§2c, in the
  direction that decides how a key BINDS).

An AGGREGATE inside a window's spec — `SUM(COUNT(*)) OVER ()`, `ORDER BY
COUNT(*)` — is the same rule for the other kind of published column: it names
the aggregate's own output, is REUSED when the SELECT list already computes it
and hoisted into the nested-aggregate slot family otherwise, which is what
HAVING has done since it grew `__having_N`.

### 4a. A key that NAMES a window's output is a STAGE question, and §2's two names answer it (2026-09-01 #777; resolved 2026-09-02 #794/#795)

The other direction of §4: not a key read above a window, but a WINDOW OUTPUT
read as a key. `SELECT x.id, x.w, COUNT(*) FROM (SELECT id, SUM(a) OVER () + 0
AS w FROM decpair) x LEFT JOIN decpair z ON x.id = z.id GROUP BY x.id, x.w`
answered `w = 52.99` on the single-process path and NULL on every row on both
DAG arms.

`aggStageGroupKey` answers a key that names a derived table's COMPUTED alias
with the alias's DEFINING EXPRESSION, and for a wrapped window that expression
is `__win_0 + 0` — a SLOT the join does not carry, because the window arm's own
projection (`absorbWindowArmProjection`, ADR-0025 shape g) already renamed it
away to `w`.

The obvious repair is to answer the ALIAS instead, and it is **not available at
that point in the plan**. `walkStages` emits `GroupByCols` BEFORE
`attachScanSelectProjections` and `absorbWindowArmProjection` run, and those are
the passes that decide whether any fragment publishes the alias at all —
`absorbWindowArmProjection` fires only from the join-input-projection pass, for
a join arm's child with exactly one window stage, empty ProjectExprs and its
expression columns available. Both candidate spellings are therefore right on
some plan shapes and wrong on others, and nothing at stage-emission time knows
which.

Three attempts to infer it from NODE KINDS were each wrong in a different
direction, and this ADR records them because the shape of the mistake is more
useful than the fix:

- the whole subtree below the aggregate ("any join, sort, LIMIT, window or
  DISTINCT") turned two CORRECT DAG answers into `stage scan-0: column "w" does
  not exist`, over a derived table with an ORDER BY or a LIMIT in it;
- the producer DIRECTLY below the defining Project turned another correct
  answer loud for `id % 7 AS k` beside a window — arithmetic over a scan column
  that nothing materializes (`TestWindowPartitionKeyTwoPath` caught it);
- that plus `referencesSyntheticWindow` still over-fired with NO join above:
  nothing attaches the arm projection there, so the key dispatched as the bare
  alias and `hash_aggregate` bound whatever the batch carried. LOUD where the
  alias names nothing — and SILENT where it SHADOWS a base column.
  `(SELECT id, SUM(id) OVER () + 0 AS g FROM collslot) x GROUP BY g` answered
  three groups keyed by the SCAN's `g` where PostgreSQL answers one group of
  240, on both DAG arms, turning a right answer into a wrong one. No fixture in
  the tree contained a window alias that shadows a base column, which is
  method 10 turned against the predicate itself.

The cell was REFUSED — `refuseUnstageableGroupKey` condition (3) — and routed
to the coordinator-local pipeline, which answered PostgreSQL's rows for every
shape in it including the ones the DAG used to get right by luck. That refusal
was a placeholder, and it is **retired** (2026-09-02): `refuseUnstageableGroupKey`
is gone, and so are its other two conditions, because the question all three
were standing in for is answered by §2's two names.

A first attempt at the stage-level answer was built and WITHDRAWN before them.
`respellAggregateGroupKeys` re-spelled `GroupByCols` after the projection
passes, and an adversarial review found two defects that were the same fact
twice: a `Stage` carrying ONE name per key cannot state a derived alias.

- A join fragment's stream was believed to carry `w` and never `y.w`, so a
  qualified alias could only be resolved by its BARE name, and when both arms
  publish that name the batch holds two columns called `w` with
  `RecordBatch.ColumnIndex` answering the first. `SELECT y.w, COUNT(*) FROM
  (SELECT id, a*3 AS w FROM decpair) x JOIN (SELECT id, a*100 AS w FROM decpair)
  y ON x.id = y.id GROUP BY y.w` answered x's values on the broadcast arm.
- `stageEmittedColumns` under-reported a join fragment's real output — a chained
  link carries its own `Columns`, and a stage that declares no list forwards
  everything — so "the producer does not emit this" was evidence about the MODEL
  as much as about the plan. Bounded by that model the pass refused a CTE shape
  the DAG was EXECUTING correctly and routed it local: right to refused-routed,
  a regression in kind (protocol item 8).

Both are answered, and the second one twice over. The first claim was simply
FALSE about the engine: `joinOutputSchemaWithMapping` qualifies a duplicate
build column with its owning alias, so the stream carries `w` AND `y.w`, and
`stageStreamColumns` now models that — the ambiguous pair resolves per ARM, in
both key directions, by the qualified name where the join qualified it and by
the bare one where it did not. The second is #795: the model reports a chained
link's own `Columns`, applies an output filter with both halves of the
qualified↔bare fallback, and records a duplicate the join had to DROP so a key
naming that arm is refused instead of bound to the other arm's column.

What remains refused is not a spelling choice, and it is two classes rather
than one. The first is a plan fact: a derived arm whose inner `ORDER BY …
LIMIT` stopped `attachScanSelectProjections` from materializing its alias, read
through a join whose exchange manifest ships neither the alias nor the
expression's columns. Nothing in that plan carries the value, the model says so
exactly, and the error carries the stream's column list.

The second was found by retiring the old refusal and measuring what came out
from under it, and it belongs to a different pass. A DISTINCT over a SELECT
list makes every item a GROUP BY key, so `COUNT(*) + 0 AS w` and
`SUM(a) OVER () + 0 AS w` become key EXPRESSIONS and the stage carries the CALL
as the key's text — as BOTH names, which agree. There is nothing for the
carrier to separate: what is wrong is that a pre-aggregate PROJECTION evaluates
a scalar over one row and an aggregate call is not one, while the value it
names was computed by the operator below and published under `__agg_0` /
`__win_0`. On base the aggregate spelling answered ONE NULL group silently and
the window spelling was covered by condition (3)'s refusal.
`refuseUnevaluableGroupKey` states it and routes; the repair is for the
DISTINCT lowering to record the slot rather than the call, and that is the next
lead in this family.

`resolveStageGroupKeys` raises both, and the coordinator answers the query on
its local pipeline.

The refusal's worst case is NOT "a slow correct answer", and saying so was too
comfortable. `runRefusedLocal` runs under a budget of 8× `localFastPathBytes`,
so a routed query carrying a JOIN under a small `--local-fastpath-bytes` can
fail LOUDLY where the DAG would have completed — and the join's build check
fires before the aggregate ever reaches its spill path, so the failure is a
budget refusal rather than a degraded run. Every case observed while measuring
this was base-WRONG → routed-right, which is still an improvement in kind; the
residual risk is a shape that is base-RIGHT and routes into that budget, which
nothing in the corpus produces but nothing rules out either.

`TestWindowOutputAsAGroupKeyMatchesPostgres` asserts the disposition beside the
rows for every shape in the cell, so neither half can move in silence: the #777
entries now assert `routed=false` and PostgreSQL's rows on both DAG arms, and
the one shape that still routes asserts `routed=true` with the mechanism named
in the test.

The same one-field problem in a shape with no window in it — a computed alias
over a BARE SCAN, and its aggregate-wrapped spelling — was **#781**, pinned in
that gate and now asserted. The discarded wide predicate would not have fixed
the aggregate-wrapped spelling either: `COUNT(*) + 0 AS w` references no
`__win_N` slot, so no window-shaped condition could see it. That is the
clearest evidence that the question was about what a STAGE emits and not about
what kind of node produced it.

Both halves of §4 were needed. With only the walks widened, `SELECT g + 1,
COUNT(*), SUM(COUNT(*)) OVER ()` stopped failing loudly and started answering
NULL, and `ORDER BY COUNT(*)` started answering in an arbitrary order — a LOUD
failure turned SILENT, which is a regression in kind (protocol item 8) even
though the loud failure was itself an accident of a different column's
projection.

### 4b. A SORT or WINDOW key over a computed derived alias is the SAME question, and it was blocked by a PHANTOM COLUMN, not by the carrier (2026-09-03, #807 / #658 — DEFERRED, then CLOSED the same day)

§2's two names answer a GROUP BY key that names a derived table's computed
alias. A SORT key and a WINDOW key over the same alias are the same question at
two other callers of one function, and they are still open. The residual is
recorded here because the repair was attempted, measured, and stopped one layer
BELOW this record's territory — so the next attempt does not spend the same
evidence.

The shape:

```sql
SELECT x.w FROM (SELECT g * 3 AS w FROM t ORDER BY w LIMIT 5) x ORDER BY x.w
```

right on the single-process pipeline, and on both DAG arms
`sort: key column "w" does not exist in the input schema`. The window spelling
is one caller over — `window: PARTITION BY "gk" is not a column of its input
(input has: id, g)`. `physical.derivedAliasSourceColumn` declines a computed
alias BY DESIGN (its doc says it returns `""` for one), so `SortKeySpec.AliasSource`
stays empty and the stage keys on a name nothing emits.

**What §2's model says to do, and why it is not enough.** Give the key its
alias's DEFINITION as the second name — the field `SourceExpr` already there
for a synthetic `__sortkey_N` — and let `resolveHiddenSortKeys` materialize it
onto the producing fragment under the alias's own name, which
`materializeSortKey` already does. That is the right shape and the carrier
needs only a flag to say which of the two spellings it is holding. It does not
work, for a reason neither issue names and which was found by building it:

> **The scan's REQUESTED COLUMN LIST already contains the alias.**

Column pruning records `w` as a column the scan needs, because the Project that
publishes it sits above that scan. Every "what does this stage emit" model in
the planner — `stageEmittedColumns`, and through it `emittedThroughPassThrough`
and `gatherOutputSources` — reads a scan's emitted set off that list. So:

- ask the stream whether `w` exists, and it says YES, and the pass skips the
  materialization that would have created it;
- ask instead whether some fragment MATERIALIZES `w` (the right question, and
  the one `resolveDerivedAliasSortKeys` already asks), and the materialization
  runs — but it builds its pass-through list from that same column list, so the
  projection carries `w` as a pass-through of a column the table does not have
  and the failure MOVES to the scan:
  `operator execute: column "w" does not exist in the input schema`.

That phantom is **#776's own mechanism one consumer over**: a scan REQUESTS a
column its table lacks, the parquet reader narrows it away silently, and every
reachability model above believes the scan produces it. #776 shows it as wrong
COLUMN NAMES and NULL values out of the gather; this shows it as a
materialization that cannot be placed.

**So the order is fixed, and it is the reverse of the one the issues were filed
in.** The pruner must stop putting a Project's output name into the scan below
it FIRST; then §2's two-name carrier extends to `SortKeySpec` and
`WindowColSpec` mechanically. Repairing the key resolution first is a fix
bounded by a model the same change knows to be incomplete, which protocol rule
11 says is not shipped — so it was not.

**And that first change has a PRECEDENT in this tree, which is where the next
attempt starts.** `logical.pushColumnNeeds` already solves this exact phantom
for a WINDOW: it deletes each `WindowExpr.OutputCol` from the needs set it
pushes down (`optimizer.go`), with a comment naming the identical failure — a
scan asked for a column its table does not have, `#694` round 2 — and it states
the rule generally: a node's own output is skipped when it is PUSHED PAST the
node that computes it, not when it is COLLECTED. One node kind over,
`sanitizeScanNeeds` drops, at the scan itself, every name the scan's schema
lacks. So "a Project's output name is not a need of the scan below it" is not a
new rule to invent; it is the Window arm's rule applied to the Project arm, plus
the sanitize that already runs. What §2's carrier then needs is only a
materialization with somewhere to place the column. Neither #807 nor #658 names
either function, and the first attempt rediscovered the phantom from scratch.

Pinned by `coordinator.TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG`,
eleven cells on three arms: the four loud SORT shapes, the two loud WINDOW
shapes, the plain-rename controls that prove the defect is the COMPUTED alias
and not the derived table, and the CTE and count-above spellings that are RIGHT
today by ROUTING — asserted with `UnreachableOutputLocalRoutes` beside the rows,
because a fix that makes the DAG execute a shape it currently routes is
invisible to a row check and so is the regression back (rule 11). Nothing in the
tree named #807 before it.

#### CLOSED, and the phantom was the whole of it (2026-09-03, arc S1)

The order this section fixed was right and the estimate of what came after it
was wrong. Once the phantom was closed the carrier needed no extension at all,
and that is the correction worth keeping.

**The phantom's site was one branch of `sanitizeScanNeeds`, not `pushColumnNeeds`.**
The paragraph above points at the Window arm's rule "applied to the Project
arm"; the measurement says the bare spelling was already handled — a bare `w`
IS dropped by the sanitize the paragraph names — and what got through was the
QUALIFIED one. A derived table's alias BECOMES the scan's `TableAlias`, so
`x.w` matched the qualifier branch and was kept as the bare `w` whether or not
the schema had it. The CTE spelling of the same query is the control that
proves the site: `c.w` does not match the alias, the name was dropped, and that
plan was REFUSED and answered locally where the derived-table spelling failed
loud. One query, two spellings, two dispositions.

`annotateScanSchemas` also moved from the end of `PlanDistributed` to before
the resolution passes, which makes §2's own intersection in `stageStreamColumns`
— recorded above as INERT — live, and lets `stageEmittedColumns` ask the same
question.

**And then the key needs no second name on the wire.** §2 gives a GROUP BY key
a PUBLISHED name and a RESOLUTION spelling because the fragment has to look the
key up in a relation whose columns are spelled differently. A sort or window key
over a computed derived alias has the same two names — `w`, and `g * 3` — but
the second is consumed at PLAN time: `SortKeySpec.AliasExpr` carries the
definition, `materializeAliasColumns` projects it onto the producing fragment
under the ALIAS'S OWN NAME, and the key, the gather's rename, an outer sort and
the exchange that clusters a PARTITION BY all keep reading the one published
name. Nothing new crosses the wire, and `distributed.OpSpec` is untouched.

The WINDOW half runs at STAGE EMISSION rather than in a late pass, because a
PARTITION BY key is also the stage's DISTRIBUTION and rewriting it after
`EnsureDistribution` would leave the exchange and the operator keyed on
different columns. Both halves share one materializer, which takes the whole
set of aliases at once: `OpProject` narrows to its projections, so a window with
a computed PARTITION BY and a computed ORDER BY needs both columns from one
call.

The BOUNDARY is `derivedAliasDefinition`, and it is stated positively rather
than by enumerating what to avoid — the rule ADR-0025 arrived at for an
aggregate's argument, after enumeration was wrong twice. It looks through
Project, Filter, Sort and Limit, which is exactly where `walkStages` provably
emits no stage for the Project, and stops at everything else. Below a JOIN, an
AGGREGATE, a DISTINCT or a set operation the alias is MATERIALIZED — the arm's
own projection, the aggregate's output name, the DISTINCT's group key — so
substituting the definition there would compute it a second time over columns
that relation no longer carries. The corpus attempts both sides: the
plain-rename controls, the CTE spellings, and #716's two collapsing producers,
which still refuse and still answer on the coordinator-local pipeline.

The eleven cells now assert `UnreachableOutputLocalRoutes` = 0 on both DAG arms
with PostgreSQL's rows, and #716's SCAN producer falls to the same
materialization and is asserted as PLANNED rather than deferred.

### 5. A name re-spelled for dispatch is TYPED where it was re-spelled TO (2026-09-02, #792 / #775 / #729)

A GROUP BY key and an aggregate's ARGUMENT are both RE-SPELLED before dispatch —
the key into its defining expression, the argument into the column a rename
Project binds — and §2c's rule applies to both: a name so re-spelled is typed
where it was re-spelled TO, not where the query wrote it. Two declaration scopes
were consulted, in fixed order, each with its own gate, and neither gate asked
the question that decides it:

- the EMITTED scope was accepted whenever `nodeDeclaredType` answered Decided,
  which arithmetic always does — the FLOAT rule is a rule, not an observation.
  `GROUP BY k` over `(SELECT c_dec + 1 AS k FROM typemx) s` dispatches as
  `c_dec + 1` into a scope carrying `k` and no `c_dec`, was answered FLOAT64
  *with confidence*, and died at the #361 store guard on both DAG arms for a
  query the same SQL over the base table answers (**#792**).
- the SOURCE scope (`sourceColDeclsThroughRenames`) stops at a COMPUTED
  projection item and returns NOTHING, because a rename may rebind a name to a
  different value. True of a NAME; not true of the DEFINING EXPRESSION that item
  was hoisted out of, which is spelled in the Project's own input scope. So
  `a * 3` over `(SELECT id, a * 3 AS w FROM decpair) x` had no scope at all and
  fell to the same float rule (#786, and **#729**'s last DAG residual — a
  fractional literal in a key over a renamed DECIMAL).
- the same walk stopped at the immediate child for an aggregate's ARGUMENT.
  `SUM(w * 2)` hoists its constant out of the aggregate, so the stage carries
  `SUM(__win_0)` plus a POST-BREAKER projection `__agg_0 * 2`; the Project below
  the aggregate emits `id` and `w`, so `__win_0` resolved nowhere, the aggregate
  declared FLOAT64 over a DECIMAL window slot, `aggOutputFromInputDecl`
  inherited it, and that projection met an exact DECIMAL at the store guard
  (**#775**). `aggSpecInputDecimal` also asked the SCAN-only walk where
  `aggSpecOutputType` asked the emitted one — two functions answering one
  question about one column with two different walks, which is ADR-0023 item 5
  one layer over.

`physical.namingScopeDecls` descends the chain below the aggregate until it
reaches the level whose emitted columns can NAME every column the expression
references. Descending is gated on COVERAGE in both directions, and that is what
makes it safe: a name the Project's OUTPUT can name stops at the OUTPUT, so a
rebound name is never read past its rebinding; a name it cannot name is looked
for one level down, where it either resolves or the walk gives up and the caller
keeps the answer it had.

`groupKeyScopeDescends` — Project, Filter, Sort, Limit, Distinct, Window — is
its OWN list and deliberately not `logical.AggScopePreservingWrapper`, for the
reason §4 gives about the fifth reader: "are this node's input columns still
values of the same rows" is not "do this node's rows carry one row per group",
and a shared list that answers both is a list answering neither. A node belongs
here when a name it does not emit may still be a name its INPUT emits, for the
same rows. `NodeAggregate` is excluded, and that is the boundary: an aggregate
REPLACES its input's scope with keys and aggregate outputs.

| claim | fixture that attempts it |
|---|---|
| the descent never types a name from past its rebinding | `ctl/a-derived-alias-that-shadows-a-base-column` — `typemx.g` is `i % 7`, eight groups against the derived three, so binding or typing the wrong `g` is a visible wrong answer |
| a value-preserving wrapper between the Project and the scan is looked through | `ctl/a-derived-table-with-an-order-by-inside` and `…-with-a-limit-inside`, both DECIMAL so the type is what is gated |
| the emitted scope still wins where it names the columns | TPC-H Q07/Q08/Q09's `GROUP BY SUBSTR(l_shipdate, 1, 4)` in `physical.TestTPCHStageDumpGolden` (it lives in `internal/planner/physical/`, NOT in `benchmarks/tpch/` — a `-run` against the wrong package reports ok and runs nothing), and `ctl/an-ordinary-computed-key-still-runs-on-the-dag` |
| an aggregate's argument and a key get ONE answer | `TestNumericArc2ShapesMatchPostgres`'s six `#775` entries beside their FLOAT and bare-argument controls |

The walk answers a window over a SCAN. It does NOT answer a window over a
DERIVED TABLE: put one level of nesting between them and `SUM(w*2)` over
`SUM(a) OVER ()` is still LOUD on both DAG arms, at the same site and with the
same `cannot store string into FLOAT64 vector`, base-identical — and the SINGLE
path keeps the right value under a FLOAT box where PostgreSQL says numeric, so
the nesting loses the declaration on every arm and the DAG is only where losing
it is loud. That is the
rule's BOUNDARY rather than a regression, and rule 11 of the correctness-fix
protocol is why it carries a fixture instead of a sentence: **#796**, pinned in
`TestNumericArc2ShapesMatchPostgres` beside the six shapes it is one nesting
level away from.

**#796 is CLOSED as of 2026-09-04 (arc F1), and it was not this walk.** The
LOUD half described above is already gone on `18f3660e`; what survived was the
DECLARATION, on every arm. The defect was one level up from the walk this
section is about: `windowSpecOutputType` resolved the WINDOW's own input column
through `inputColDecls`, which STOPS at a Project, so a window one nesting level
above its scan resolved nothing and fell to the float64 name-list fallback. The
aggregate above it then inherited a float box for a number PostgreSQL calls
numeric. It now reads `emittedColDecls` — the walk that crosses a derived
table's Project (#529), and the one the aggregate's own argument and
`declaredOutputSchema` already use — so the window's declaration, the
aggregate's above it and the wire's read one map through a nesting level. Gate:
`coordinator.TestF1AWindowDeclaresTheSameTypeThroughADerivedTable`, four arms,
with the one-level spelling and a renaming derived table as the controls.

One residual is left where #796 sat and is a DIFFERENT rule: an integer SUM/AVG
OVER a window is accumulated in FLOAT64 (#813). It declares float8 where
PostgreSQL declares bigint or numeric — and past 2^53 it also answers different
DIGITS from the grouped spelling and from PostgreSQL, which the first statement
of this paragraph asserted otherwise without measuring (ADR-0012's entry carries
the numbers). The integer-exactness rule reached the grouped aggregate in #784
and not the window, and it cannot reach the window's declaration alone:
`exec.windowAccOutputType` gives an integer input a float64 accumulator, so
declaring the exact type without moving the carrier is the #361 silent-write
class. DEFERRED with that mechanism; pinned on the VALUE in the same gate.

## Consequences

- A site that has to ask "is this the same expression as that group key"
  calls `ExprIdentity`. A site that has to name the key's column calls
  `GroupKeyName`, or reads `groupKeyOutputs`. Adding a new consumer without
  one of them is the way this class comes back.
- The identity is a MAP KEY that crosses package boundaries and is
  recovered by re-parsing a stage's published name, so its exact rendering
  is pinned by `TestExprIdentityIsStable` and its idempotence by
  `TestExprIdentityIsIdempotent`. Changing the rendering is changing what
  two planners agree on.
- A COLLISION is IMPOSSIBLE, not accepted. The materialized value lives in
  the reserved namespace, which no query can spell, so nothing the user
  writes can be mistaken for it or hidden by it. The earlier draft of this
  ADR accepted the collision; the review refuted that with a 5-arm matrix
  (single-process, single-process spilled, DAG, DAG broadcast, DAG spilled)
  and the position is corrected above.
- A key a rename Project defines is re-spelled into SOURCE columns for
  DISPATCH only (`aggStageDispatchKey`): the DAG flattens a rename Project,
  so `a_b + 1` over `(SELECT c_i32 AS a_b …)` reached the worker spelled
  over a column the scan does not emit and collapsed the table into ONE
  NULL group. `aggregateOutputName` deliberately does NOT take that path —
  it answers what a SORT KEY names, and a sort key is resolved on both
  engines, of which only one has a dispatch spelling.
- Not decided here, and filed instead:
  - **#731** — an unquoted identifier is never folded to lower case
    anywhere in the engine, so `GROUP BY G + 1` still computes a key from a
    name nothing resolves and collapses the table into one NULL group.
    That is column RESOLUTION, not key matching: `SELECT G FROM t` with no
    GROUP BY is wrong the same way. Pinned in the two-path corpus.
  - **#732** — an unaliased expression is named after its own text where
    PostgreSQL names it `?column?`. Both paths agree with each other; the
    rule is about naming a select ITEM, not resolving a KEY.
  - **#729** — the DAG declared FLOAT64 for arithmetic whose value is an
    exact DECIMAL. CLOSED for a WINDOW's output (2026-09-01): `inputColTypes`
    and `inputColDecimal` stopped at a `NodeWindow`, so the slot had no
    declared type and the float rule stood; both walks now add the window's
    own slots from `windowSpecOutputType`, which `emittedColDecimal` has read
    since #586. The AGGREGATE's output declaration is CLOSED 2026-09-02 with
    **#775**, and the input declaration was the missing half rather than the
    output one. `SUM(w * 2)` hoists its constant out, so the aggregate is
    `SUM(__win_0)`, and `aggInputColumnType`/`aggInputColumnDecimal` looked that
    re-spelled name up in the scope of the Project directly below the aggregate
    — which had renamed `__win_0` to `w`. The declaration fell to FLOAT64,
    `aggOutputFromInputDecl` inherited it, and the POST-BREAKER projection
    `__agg_0 * 2` met an exact DECIMAL at the store guard. §5's
    `namingScopeDecls` answers it, and `aggSpecInputDecimal` — which asked the
    SCAN-only walk where `aggSpecOutputType` asked the emitted one — now asks
    the same one.
  - **#774** — CLOSED 2026-09-01, and it was §4's own defect one reader over
    rather than a third consumer of the HAVING respelling. A WHERE on the key
    applied ABOVE the window admitted no row at all, on every arm: the outer
    predicate is pushed below the derived table's Project and `k` substituted
    away to `(g + 1)`, which above the aggregate is a NAME and not arithmetic,
    so the filter was UNKNOWN on every row and a filter admits only TRUE. The
    substitution is DECLINED for a definition that is not evaluable over group
    rows — `projRefs.overAgg` — and that flag was read off
    `AggregateBelowProject`, the Filter-only walk. It now reads
    `AggregateOverGroupRows`, the fourth reader of §4's list. Nothing about the
    predicate's spelling changed; what changed is that it stays where the query
    wrote it. Gated on nine spellings plus the SUM and COUNT faces in
    `R4/…/WhereOnTheKeyAboveAWindow`, with the four controls that make the cell
    exactly this one.
  - **#749** — `DECIMAL(38,10)` arithmetic keeps too few decimal places
    (`d + 1` is `201.000000013` where PostgreSQL says `201.0000000125`), on
    every arm of every tree. Inherited by the group-key family for the same
    reason: the key reaches the arithmetic now.
  - **#736** — CLOSED 2026-09-01. What remained of the DAG half after the
    reference rule above closed the collision shapes was re-measured over
    `typemx` on four arms (single, single spilled at 512 KiB, DAG, DAG
    `BroadcastBytesOverride=1`) against a live PostgreSQL 17: eight of the
    eleven shapes the issue tabulates already agreed everywhere, and the
    remaining three were three DIFFERENT DAG mechanisms, none of them the
    reference rule.

    One was a naming asymmetry and is fixed outright. An aggregate whose
    ARGUMENT is spelled like the key failed LOUDLY —
    `column "\"g + 1\"" does not exist` — because the parser records a
    delimited argument WITH its quotes (`ColRef.String()` re-delimits), the
    single-process path strips them with `NormalizeIdentRef` and the DAG
    carried the spelling verbatim, so the alias lookup missed and the argument
    was never re-spelled to its source column. A delimited identifier's quotes
    are not part of its NAME (§2b), and that is as true of an aggregate's
    argument as of a key.

    The other two need the SEPARATION §2 is about, and a `Stage` has one field
    where the single-process path has two. `Stage.GroupByCols` is
    simultaneously what the worker computes the key FROM and what it publishes
    it AS; the worker re-derives "is this key derived?" by PARSING that text
    and mints its own slot. So a key an aggregate DIRECTLY BELOW already
    publishes was recomputed against a schema that no longer has its leaves
    (the DISTINCT lowering — ONE NULL group), and a derived key sharing its
    published name with one of the aggregate's own outputs won every by-name
    lookup above it, because a batch resolves a name to its FIRST column and
    the aggregate emits keys before outputs (the KEY's value under the
    aggregate's alias).

    Both were REFUSED and ROUTED (`ErrGroupKeyDistributed` →
    `runGroupKeyLocal`) rather than fixed on the DAG — deliberate and narrow,
    and not the end of the story. **Both refusals are DELETED 2026-09-02** and
    both shapes now run ON the DAG. The key an aggregate DIRECTLY BELOW
    publishes carries `Computed=false` and is looked up as the COLUMN it is,
    instead of being re-parsed as arithmetic over leaves that aggregate no
    longer emits — that is §2's two names.

    The second is not the two names, and finding that out is the useful part.
    A derived key sharing its published name with an aggregate output gives the
    stage TWO output columns of one name, and no carrier makes a name
    unambiguous. What made it wrong was a reader that tried:
    `absorbAggregateOutputProjection` renamed exactly ONE of the two onto the
    query's alias, and every reference spelled before that pass — the sort key
    `aggregateOutputName` had resolved, the gather's rename — then named a
    column whose meaning had changed under it. The query came back ordered by
    the COUNT. The pass DECLINES there now, and the readers that can tell the
    two apart do it by CLASS and by POSITION rather than by name: the aggregate
    emits keys before outputs, the gather's `OutputRename.IsAgg` pairs each
    rename with the column of its own class (#575), and a merge addresses its
    aggregates by ordinal (`mergeByPosition`). "A projection carried wrong is
    worse than one not carried at all" is that pass's own rule, and this is a
    case of it.

    The narrowness is still asserted, and now in the other direction:
    `ctl/an-ordinary-computed-key-still-runs-on-the-dag` fails if anything
    starts swallowing plain `GROUP BY g + 1`, and
    `DAGResolvesAComputedKeyAgainstTheScan/DistinctOverTheKey` asserts
    `GroupKeyLocalRoutes()` did NOT move — the DAG executes it now, and a
    return to routing would be a regression in kind that its rows alone cannot
    see.

    "All on the DAG arms alone" was written here and is FALSE with a WINDOW
    present: `SELECT DISTINCT g + 1 AS k, ROW_NUMBER() OVER (…) … GROUP BY
    g + 1` was one NULL group on the SINGLE-process path too, for §4's third
    walk rather than for #736. Both halves now answer.

    The refusal's own boundary is measured, and it is where the next finding
    lives. Its first cut asked `!Derived && !nameIsPlainColumn(Name)`, which is
    true of `GROUP BY n1.n_name` — a key that is not derived because it IS a
    column, whose qualified name is not "plain" by that predicate. TPC-H Q07
    was refused and routed local, and `TestTPCHStageDumpGolden` is what said
    so. `groupKeyOutputs` now records `PublishedBelow` — WOULD be derived, but
    an aggregate below already publishes it — and the refusal reads that, so
    the two reasons a key needs no materializing cannot be confused again.

    That narrowing also decided **#781**, filed from the same matrix while
    gating #777: a computed alias over a BARE SCAN used as a key through a
    join or a DISTINCT. 2026-09-02 established WHY before closing it: its key
    needs the two names §2 is about, and every attempt to CHOOSE between them
    at stage level fails on a shape the stage cannot describe (§4a's withdrawal
    record). Carrying BOTH closes it — the join arm's projection materializes
    the alias, the stream carries it, and the resolution names it — including
    the AGGREGATE-wrapped spelling and the ambiguous two-arm pair in both key
    directions. Its TYPE half was closed separately as **#792** by §5's scope
    walk: the key was dispatched correctly all along and only its DECLARATION
    was wrong. One shape of the cell still routes and is asserted as routing —
    a derived arm with an inner `ORDER BY … LIMIT` read through a join, where
    no fragment in the plan carries either the alias or its definition.

    CLOSED in the family since: **#785**, an aggregate aliased like the key
    BESIDE a HAVING on it, which answered zero rows on every arm and was
    therefore not the DAG's. §3a has it.

A related naming rule, settled here because two of the four review findings
turned on it: **an ALIAS is a name, and its case is part of it.** A
delimited alias is published as written on every path, and a positional
`ORDER BY` resolves to the select ITEM, never to the alias's text re-parsed
as an expression.

## Gates

- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/SlotCollidesWithAStoredColumn` — the allocator, over a fixture that
  STORES a column named like the slot: two and three derived keys, either
  order, with HAVING, minted by a derived alias with no stored column at
  all, and an aggregate over the stored column asserted on its VALUE.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/ArithmeticKeyBesideADelimitedColumnOfThatText` and
  `R4/…/DelimitedAliasUnderPositionalOrderBy`.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/AGroupKeyIsResolvedByIdentityNotBySpelling` — the whole spelling
  matrix on both arms, asserted as ordered rows with per-key counts
  computed from the fixture generator, plus the two controls
  (`ctl/AssociativityIsNotSpelling`, `DelimitedIdentifierKey`) that fail if
  the identity is made too coarse.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `N3/HavingOnAComputedGroupKey` — #720's five rows, and the refusal
  PostgreSQL gives for a select-list alias in HAVING.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/WindowAboveTheAggregate` and its four siblings — the ordered answer,
  key sequence and window value per row, over the repro, the same query
  ordered BY the window's output, PARTITION BY the key, a HAVING beside it,
  and a BARE key as the control that needs no materialization; plus
  `WindowOverAnAggregateOutput` in the reuse and the hoist spellings (#737).
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/SlotCollidesWithAStoredColumn/SiblingAggregatesEachMintingASlot` —
  the fixture §2a's scope claim needs (#759).
- `tpch.TestPostgresOracle` § `groupKeySpellingCases` — the same matrix
  asked of live PostgreSQL 17 over TPC-H, including the DECIMAL key under
  `TPCH_DECIMAL=1`, where the comparison is digit for digit.
- `sql.TestExprIdentityErasesOnlySpelling` — the identity's contract in
  both directions: spellings that must collapse, and expressions that must
  not.

§2's two names have four gates of their own, and each fails on a different
half of the carrier:

- `physical.TestStageCarriesOneGroupKeyList` — the CARRIER invariant over
  every TPC-H plan: one group-key list per stage, the resolution list index-
  aligned with it, present on exactly the stages that COMPUTE their keys and
  absent from every merge, and no key left DEFERRED when planning returns.
- `physical.TestEveryComputedKeyResolvesAgainstItsProducer` — the resolution
  pass's own claim, over the derived-alias shapes: the spelling a fragment
  resolves a key by names something that fragment's input carries. A COMPUTED
  resolution is checked as an expression and a NAME as a name, which is §2c
  inside the assertion.
- `physical.TestGroupKeyPublishedNameIsTheQuerysOwn` — the published half,
  including the two qualified keys that strip to one name and must keep their
  qualifiers.
- `coordinator.TestStageCarriesFilterAndProjectionTwoPath` §
  `R4/…/DAGResolvesAComputedKeyAgainstTheScan` — the two shapes #736 refused,
  now asserted ON the DAG with `GroupKeyLocalRoutes()` beside the rows, plus
  `ctl/an-ordinary-computed-key-still-runs-on-the-dag` as the width guard.
- `coordinator.TestWindowOutputAsAGroupKeyMatchesPostgres` — the whole #777 and
  #781 cell, every entry asserting the DISPOSITION beside PostgreSQL's ordered
  rows on both DAG arms, including the ambiguous two-arm pair in both key
  directions and the two shapes that still route.
- `worker.TestFragmentResolvesAndPublishesTheTwoNames` and its three
  siblings — the wire's half, driven through the real aggregate builder: the
  derived alias, the qualified alias, the slot, the key an aggregate below
  publishes, and a merge; plus the compatibility fallback when a spec carries
  no resolution list, and the property that what the DAG's aggregate emits is
  what `exec.PublishedGroupKeyNames` gives the single-process one.

Every half was verified able to fail. Stubbing `ExprIdentity` to `n.String()`
fails the paren-nested, identifier-case and ORDER BY entries; stubbing
`GroupKeyName` the same way fails every delimited-identifier entry; dropping
`GroupByResolve` from `walkStages`' partial-aggregate literal fails
`TestStageCarriesOneGroupKeyList`; and making `resolveStageGroupKeys` a no-op
fails `TestEveryComputedKeyResolvesAgainstItsProducer` on the join shapes and
`TestWindowOutputAsAGroupKeyMatchesPostgres` on every #777 and #781 entry.
