# ADR-0026: A GROUP BY key has one identity and one published name

Status: Accepted (2026-08-30, #720 / #723 / #725; amended 2026-09-03 by arc S1 — §4b's deferral is CLOSED, the phantom scan column under it is named at its real site, and a sort or window key over a computed derived alias needs no second name ON THE WIRE because the definition is materialized at plan time; amended three times the same day after review — one identity, one SLOT, one published name, one ALLOCATOR per aggregate, and a NAME never re-read as structure; amended 2026-09-04 by arc E3 — §3a is CLOSED: a HAVING binds its aggregate through the slot that aggregate OWNS, and the gather pairs a lone rename by CLASS (#785); amended again 2026-09-01 for #737 and #759 — a WINDOW above the aggregate is spelled against what it publishes, and the allocator's per-aggregate SCOPE is a boundary with a fixture that attempts it; amended 2026-09-02 with §5 for #792, #775 and #729 — a name re-spelled for dispatch is TYPED where it was re-spelled TO — and with §4a's record that the stage-spelling pass sketched there was built and WITHDRAWN, because a Stage carrying one name per key cannot state a derived alias (#794, #795); amended 2026-09-04 by arc F4 — §3a's fragment-projection residual is closed for the THREE WRAPPED spellings it pinned, and it was two defects: an unaliased SELECT item was invisible to the class walk's lookup, and a fragment projection above an aggregate addressed a duplicated name by NAME where it now addresses the SLOT. Two sibling spellings — under a SET-OP wrapper and under a DISTINCT — are NOT closed and stay pinned (2026-09-05). Amended 2026-09-07 by arc J2 with §6 — the two names are not a property of GROUP BY keys: a UNION arm's projection, an aggregate argument, a window argument, an ORDER BY term and a projection's DECLARED TYPE each have a second spelling, and every one of them binds through the identity its producer published (#770, #947, #949; the mechanism is in ADR-0025); amended 2026-09-07 by arc J1 with §3c — a key the PLANNER MINTED is published under a hidden slot and RESOLVED by the column it reads, which is §2's pair of names in the opposite direction, and the stage's published list says what `exec.PublishedGroupKeyNames` will emit (#956, #767); amended the same day after review — the minted column is DROPPED BY THE JOIN that made it rather than trimmed at the statement's output, because a star-only query has no output projection to trim, and the collision is closed in the spelling where the SELECT list carries the key too (#956, #767); amended a fourth time after review — the empty-input default is the ITEM's folded value and lands only where the correlation key is NULL, the reference rewrite is deleted, and a qualified star expands from its relation's OUTPUT list or is refused; amended a third time after review — the drop's identity is a POSITION on the side the lowering BUILT (a name, and a name that is a join key, both dropped a user's stored `__key_0`), the re-spell walks the whole block, and an ungrouped aggregate's empty-input value rides on the lateral's OUTPUT COLUMN rather than on the references to it (#977); amended again after the second review — the drop is by IDENTITY (the slot the join KEYS ON, on the side it minted it for) and never by a name a table could also own, the colliding spelling takes the FULL mint with its own references re-spelled to the slot, and the distributed path's materialized lateral projection is what may ask for the slot back (#956, #767); amended a fifth time after review — the empty-input default is a COMPILED PROJECTION EXPRESSION and not a stamped value (a text carrier could not write a varlen or a container vector and emptied a MATCHED string row), a published correlation key is a USER column under whatever name and however many times the query published it, and a written ON over an unrepaired lateral is folded over the defaults and REFUSED unless it provably rejects the padded row — never NULL where PostgreSQL answers a value (#977, #956); amended a sixth time after review — a star over a lateral whose block projection is not its stage's column list is ROUTED to the local pipeline rather than answered short (the INNER spelling lost the column silently, the LEFT one failed loudly), and a constant `ON` folds through the compiler rather than through its text (#984); amended 2026-09-07 by arc K3 with §7 — a DERIVED BLOCK A STAR READS IS A RELATION AND A STAGE PUBLISHES IT, so the route's trigger shrinks to the blocks no stage could carry (#984, #980, #981); amended 2026-09-08 by arc L1 with §6a — the ORDER BY consumer keeps the QUALIFIER on the single-process path too, because a `SELECT *` has no select list to take a position from and the qualifier is the only thing telling two references of one relation apart (#989); §6a also records the residual it does NOT settle — the join's published name list is a plan artifact because `reorderJoins` expresses the build side by SWAPPING the node's children (#997, deferred with its mechanism). Amended 2026-09-13 by arc O2 with §9 — a DERIVED BLOCK PUBLISHES ITS VISIBLE LIST and a QUALIFIED STAR READS IT: a key the block materialized for its own ORDER BY dies where every relation-COMBINING operator composes its output (#991, #1075 — a JOIN was not the only one, and a set operation put `__sortkey_0` on the wire, refused the DAG and lost every INTERSECT row), a minted correlation slot's ordinal is read through the block's own Sort (#1020), the star binds the RESOLUTION spelling and publishes the PUBLISHED one (#1077), an AGGREGATE OUTPUT occupies its name before a key's qualifier is stripped (#1078), and two published columns of one name are not enumerable by name at all (#1076, refused). Amended the same day after review — the block-publication walk reaches a sort key one projection deeper, and a correlated LATERAL whose body carries its own bound is marked rather than answering a plausible row count (#1079); amended a second time after the closure review — the class is every WIRING of a join or set-op node and not its four constructors, because the decorrelation of IN / NOT IN / EXISTS and a correlated scalar subquery each builds a NodeJoin literally (#1080), and the LATERAL bound's refusal is narrowed to the QUALIFIED STAR, because a refusal on the bound's EXISTENCE replaced right answers with errors (#1019); amended 2026-09-18 by arc SR with §9a — a name TWO relations publish is publishable twice and a name ONE relation publishes twice is not addressable, which closes the USING star's shared-tail-name decline (#1177), the duplicate-published-name reference (#1094) and the set operation's published names on both engines (#1079).

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

Amended 2026-09-18 by arc WK — **§8j is SETTLED**: a window key, a sort key, a
lifted predicate column and a join-arm reference bind by the OCCURRENCE that
produced the column, and a NAME is derived from that identity for publication,
never the reverse (#1028). `exec.ColumnIndexFallback` is NOT deleted — its
qualifier strip is the join's own publication convention read back, measured —
and what became unreachable is the PLAN-TIME erasure in front of it.

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
| **PUBLISHED, transitively** — `logical.AggregateSignature.GroupByCols` is not a `Stage` field, but the value it carries IS `Stage.GroupByCols`, through `coordinator/dagplan/aggregate_shuffle.go` → `coordinator/aggregate_shuffle.go` → `coordinator.go`'s `PreComputedAggregate` → `worker/executor.go`. Its byte-exact `stringsEqual(node.GroupBy, sig.GroupByCols)` changed meaning with the field, which is why `keyNamesAreTheirSpelling` declines a candidate whose two names differ | the published name |
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

`stageStreamColumns` (`planner/dagplan/stage_stream_model.go`) lists what a
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
(`coordinator/dag_merge.go`) pairs a group of renames sharing one
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

**TWO SPELLINGS ARE NOT CLOSED, and the residual stays open for them**
(2026-09-05, round-1 review B3). The collision under a SET-OP wrapper
(`SELECT u.g, u.x FROM (…collision…) u UNION ALL SELECT 99, 99 …`) and under a
DISTINCT over the same derived wrapper both answer the group KEY under both
names on `dag` and `dagshuf`, at base and at tip. One mechanism explains them
and the naming exception ADR-0012 now records:
`physical.findOutputProjectionNode` answers nil for any root that is not
Project / Sort / Limit / Filter / Distinct, so a SET-OP root emits no gather
`OutputRenames` at all — the class walk never runs and there is nothing for
`pinProjectSpecSlots` to pin — and `aggregateEmittedSlots` independently
declines any producer carrying `UnionArms`. Closing them is the union stage
publishing its leftmost arm's output identity, not a widening of the two sites
above. Pinned fail-on-agree at `coordinator.TestArcF4BoundariesArePinned`
(`785/under-a-set-op-wrapper`, `785/under-a-distinct`).

`785/nested-in-a-derived-table` — ONE wrapper, right on all four arms before —
remains the control, and two cells were added to attempt the boundary from both
sides: `785/nested-three-derived-tables-deep` and `785/nested-in-a-cte-key-first`
(the two classes in the other order in the SELECT list). All five assert
`routes=none` beside their rows, so a shape that started ROUTING rather than
answering is not mistaken for a fix.

#### 3c. A key the PLANNER MINTED is published under a hidden slot (2026-09-07, #956, #767)

§3a is about an aggregate OUTPUT taking a group key's name. This is the same
collision from the other side: a group KEY the query never wrote, taking an
aggregate output's name.

A LATERAL subquery is decorrelated by promoting its correlated equality into
the join condition, so the join can key on the inner value only if the
subquery's output PUBLISHES it — `logical.buildLateralSubquery` injects a
select item for the key and, for an aggregated lateral, a GROUP BY term. Both
were spelled with the SOURCE COLUMN's own name, and a name is not a handle:

```
SELECT d.k, s.g, s.c FROM typemx_dim d
JOIN LATERAL (SELECT MAX(t.id) AS g, COUNT(*) AS c FROM typemx t WHERE t.g = d.k) s ON true
  → Aggregate: group_by=[t.g] aggs=[max(t.id) AS g, count() AS c]
    output [g(key), g(max), c] — TWO columns named g
```

`batch.RecordBatch.ColumnIndex` answers with the first match, so `s.g` read
the KEY: `0,1,2,…` on the single-process arm where PostgreSQL 17 answers
`4998,4999,4993,…`, and the same for `SUM` and `COUNT` (the DAG answered the
key too for `COUNT`, and failed loudly for the rest). Its mirror is worse:
`SELECT amount AS order_id … WHERE order_id = o.id` already answers to the
key's name while holding another column's value, so `lateralSelectsColumn`
decided the key was published, injected nothing, and the join keyed on a column
its build side does not carry — ZERO rows for PostgreSQL's four, on all four
arms (#767's mirror, pinned in the D5 census since 2026-09-03).

**The key is published under `SlotName(SlotCorrKey, N)` — `__key_N` — and
resolved by the source column.** The slot is in the reserved namespace
(`plansql/reserved_slots.go`), so no query can spell it and no alias can shadow
it: the collision is impossible rather than unlikely, which is the same
argument §2 makes for `__gb_expr_N`.

The direction is the reverse of a DERIVED key and the type system says so.
A derived key RESOLVES by a slot and PUBLISHES under its canonical text; a
minted key RESOLVES by an ordinary input column and PUBLISHES under the slot.
`logical.Node.GroupByPublish` is where the logical plan records it — parallel
to `GroupBy`, empty for every key the query wrote — and `physical.groupKeyOut`
carries it as `Minted`, so `publishedGroupKeyNames` hands it to
`exec.HashAggregate.GroupByOutNames` and `stageGroupKeyNames` puts it in
`Stage.GroupByCols` with the source column in `GroupByResolve`. Nothing else
in the planner learns a new rule: every consumer already reads one of those
two lists.

Three readers had to be told which of the two names they were reading, and
each of them was a wrong answer rather than a missed optimisation:

- `pruneFusedAggOutputCols` deletes an aggregate's OUTPUT names from a fused
  scan's read set, keeping anything the fragment READS. It deleted the group
  key's text (`t.g`) and not its BASE name, so the read set lost the scan's own
  `g` and the fragment failed with `GROUP BY key "t.g" is not a column of its
  input (input has: id)`.
- `resolveShuffleKey` chases a join key down through a Project to the source
  column the shuffle reads, because the DAG emits no stage for a Project. A
  minted key exists on the DAG only under its slot — exactly as a COMPUTED
  output exists only under its own alias, the case beside it — so the walk
  stops there. Chasing it handed the shuffle `g`, which the aggregate stage
  does not emit: `partitioned shuffle: key "g" not in schema`, and where the
  join still built, zero matched rows.
- `logical.sanitizeScanNeeds` keeps every `__`-prefixed name in a scan's
  required columns, because a fused scan-aggregate fragment really does emit
  `__having_N` and `__gb_expr_N`. A correlation slot is minted by a Project or
  an Aggregate and never by a scan, and it arrives on BOTH of a join's scans
  because the promoted equality names it, so keeping it there trips the
  worker's all-or-nothing projection guard and reverts the scan — and every
  shuffle fed by it — to full width.

**The stage's PUBLISHED list says what the operator will emit.** A key whose
two names are the same string gets no `GroupByOutNames`, so the fragment's
aggregate names it by `exec.PublishedGroupKeyNames`' own rule: the relation
qualifier is stripped, and kept only where stripping would make two keys
collide. `stageGroupKeyNames` recorded the GROUP BY TEXT instead, so the two
disagreed for every QUALIFIED plain key — which is what a decorrelated LATERAL
always produces. A join above one carried `t.g` in its output set while the
build stream published `g`: a fragment whose build partition was EMPTY wrote a
`.wshf` file without that column and one with rows wrote it, and the shuffle
read refused the pair (`declares 3 columns [k g c] where an earlier file …
declared 4 [k g c t.g]`, ADR-0010). Only keys whose two names are already the
same take exec's rule; a derived, literal, delimited or minted key has a name
the planner decided and hands over explicitly.

**THE PAD IS MARKED BY THE KEY, AND THE DEFAULT IS THE ITEM'S OWN VALUE.** An
ungrouped-aggregate lateral's outer row with no match is a row PostgreSQL
returns — `COUNT(*)` is 0 over an empty input — and this lowering can only
manufacture it as a LEFT pad, which writes NULL. Two questions follow, and the
first cut of the repair answered both wrongly.

WHICH ROWS: not "the rows whose value is NULL". A matched row may hold a NULL
of its own — `NULLIF(COUNT(*), 2)` over a row that counted 2 — and stamping the
column's own nulls turned right rows into wrong ones. The pad is marked by the
CORRELATION KEY: the join keys on it and a NULL key matches nothing, so the key
column is null exactly on the manufactured rows. The default operator reads
that column, above the join, and the join therefore leaves the slot in place
for it and lets the operator drop it — the marker is qualified by the lateral's
alias so a user's stored column of that name on the probe side is never
mistaken for it (ADR-0012).

WHAT VALUE: not a literal 0 on a COUNT column. It is the SELECT ITEM's value
over an empty input — the item with each aggregate replaced by its own
empty-input value, COUNT-family 0 and everything else NULL. `COUNT(*)+1` is 1,
`COUNT(*)=0` is true, `COALESCE(SUM(x),0)` is 0, `CASE WHEN COUNT(*)>5 THEN 1
END` is NULL, and a NULL default is not carried at all because the pad already
wrote it.

**AND IT IS AN EXPRESSION, NOT A VALUE.** The first cut folded the item to a
CONSTANT, carried it per column as TEXT, and wrote it into the padded row's
vector IN PLACE through a hand-written type switch. A value carrier needs one
writer per type, and that switch could not write a varlen or a container
vector: `CAST(COUNT(*) AS VARCHAR)` answered `2 | "" | "20"` — a MATCHED row
emptied and the padded one holding two values run together, because a string
vector's offsets are not a slot you can overwrite — and the star spellings
crashed in `slice bounds out of range`.

Both rules now live in ONE compiled projection expression, built as an AST at
plan time and rendered as
`CASE WHEN <marker> IS NULL THEN <item over an empty input> ELSE <column> END`:
`internal/planner/logical.lateralEmptyDefaults` builds it, the physical planner
and the worker each compile it through `internal/engine/expr`, and
`exec.LateralEmptyDefault` IS a `Project` over the join's output — every other
column copied by POSITION, the defaulted ones computed, the marker omitted.
There is no per-type writer left to get wrong: the default takes the engine's
own typed kernel for all 22 types and writes a NEW vector, which is what every
computed column in the engine does. Twenty type families are gated with all
three row kinds in one cell — a matched NULL, a matched value and the pad —
across five arms (`TestArcJ1TheEmptyInputDefaultIsRightForEveryTypeFamily`).

**A STAR OVER A BLOCK NO STAGE PUBLISHES IS ROUTED, NOT ANSWERED SHORT.** A
Project emits no stage, so on the distributed path a decorrelated lateral's
SELECT list is not a relation: the Aggregate or Scan beneath it materializes,
and a `SELECT *` above the join publishes THAT stream. Where the two agree the
star is right and runs distributed — `SELECT COUNT(*) AS n` over a stream of
`order_id, n`, with the minted slot's rename invisible below the join's drop.
Where they differ the client was handed a relation the query did not write, and
the two join kinds failed differently, which is what hid it for two rounds:

```
SELECT * FROM lat_ord o {LEFT,} JOIN LATERAL (
  SELECT order_id, order_id AS oid, COUNT(*) AS n … GROUP BY order_id) s ON true
PostgreSQL   id, customer, total, order_id, oid, n
INNER, DAG   order_id,           n, id, customer, total   ← `oid` silently gone
LEFT,  DAG   LOUD — ADR-0010, `one stage's files describe one relation`
```

Three ways a projection leaves its stream behind and one test for all three —
is every published name a column of the stream, once: a source column published
TWICE, a RENAME the stream does not carry (`order_id AS oid`, or `COUNT(*)+1 AS
n` over an aggregate publishing `__agg_0`), and a duplicate published NAME. The
minted correlation slot is excluded, because the join drops it.

The disposition is `ErrLateralProjectionDistributed` and a ROUTE to the
coordinator-local pipeline (`Coordinator.LateralProjectionLocalRoutes`), the
same handoff `ErrGroupKeyDistributed` takes. It is scoped to a STAR because
that is the consumer with no column list of its own; a named SELECT list asks
the gather for its columns by name and was always right. **K3 removes it**: the
structural fix is #984 — a stage declares the block's PROJECTION rather than
its stream — and the day it lands every shape here runs distributed and the
refusal, its counter and its gate go with it.

**A WRITTEN `ON` IS PART OF THE SEMANTICS, AND WHERE THIS ORDER CANNOT ANSWER
IT, IT REFUSES.** PostgreSQL evaluates the lateral per outer row and applies
the ON AFTER it, so for an outer row whose lateral input is empty there IS a
lateral row — the item over an empty input — and the ON decides that pair. On
the unrepaired path (an OUTER join with a written ON) this lowering has the two
reversed: the join pads because the correlation found nothing, and the ON never
sees the defaulted row, so the answer is NULL whatever the ON says. The two
agree exactly where the ON REJECTS the pair, so the ON is FOLDED over the
defaults and a definite FALSE keeps the plan — `ON s.n > 1` folds to `0 > 1`
and answers as it always did. Everything else is 0A000 in one sentence:
`ON s.n = 0` folds TRUE (PostgreSQL's `Carol, 0` against this engine's
`Carol, NULL`), and `ON o.id > 1` cannot be folded at all because it reads an
OUTER column — that one is the reason the refusal is not scoped to ONs naming
the lateral, since it was silently `Carol, NULL` with no complaint. The INNER
spelling is untouched: `lateralPadThenFilter` MOVES its ON into the enclosing
WHERE, which is evaluated ABOVE the default, so `JOIN LATERAL … ON s.n = 0`
answers PostgreSQL's row.

A condition that folds to a constant TRUE is not a residual at all — it rejects
nothing, so the repair that makes the join LEFT on the correlation alone IS its
semantics. That test folds through the expression compiler, not through the
text: matching the literal `true` made `ON 1 = 1` a residual and REFUSED a query
`ON true` answered, and one constant cannot have two dispositions.

ONE mechanism for every consumer, so the REFERENCE rewrite that used to serve
the named spelling — wrapping each reference in `COALESCE(ref, 0)` — is DELETED:
it reached no star, and it had the same matched-NULL defect. Its removal makes
one more rule explicit: a predicate over a defaulted column is neither pushed
below the join nor allowed to demote it to an inner join, because the pad
carries a VALUE and `WHERE s.n = 0` keeps the row PostgreSQL keeps.

**A minted column is dropped by the operator that made it, not at the
statement's output.** A slot the query never wrote is not a result column, and
a `SELECT *` over the join used to publish it: five columns where PostgreSQL
sends four, five `FieldDescription`s in the wire's `RowDescription`, and the
name readable through a derived table's or a CTE's star (`SELECT x.__key_0
FROM (SELECT * FROM …) x` answered the key's value).

Trimming it where `__sortkey_N` is trimmed does not reach any of that. That
trim reads the statement's OUTPUT PROJECTION, and a star-only query has none —
`hiddenSortTrimOp` returns nil for it by construction, because an unexpanded
star has no column list to narrow to. Below the star there is exactly one
place the column exists: the JOIN that needed it. So `logical.Node.HiddenJoinCols`
carries the minted slots to `exec.HashJoinProbe.OutputExclude` (and the
sort-merge join's, and through `OpSpec.HiddenColumns` to the worker), and the
join drops them from its OUTPUT SCHEMA while still keying on them. One drop,
below every door: the outer star, the qualified star, a derived table's star, a
CTE's star, the wire, and any reference by name from above.

`OutputExclude` is deliberately not the inverse of `OutputFilter`. A filter is
an optimisation — "nothing above needs these, do not gather them" — and its
absence means "emit everything", which is exactly the star case; the exclusion
is a correctness rule that has to hold when there is no filter at all.

**THE IDENTITY IS A POSITION.** A name is not one — reading is not minting, so
a table may already store `__key_0` (ADR-0012) — and neither is "a name that is
a join KEY of its own side": a query that CORRELATES ON the stored column makes
it a key, which is exactly when that rule admits the user's column to the
exclusion and drops it. `SELECT * FROM o JOIN LATERAL (… WHERE __key_0 =
o.__key_0) s` lost `o.__key_0` on all four arms and on the wire.

The join drops the column at the ORDINAL its own lowering put it at, on the
side that lowering BUILT — `logical.Node.LateralSubtree` marks that side, so a
join-order swap cannot move the rule to the other one, and the name at the
ordinal is a SAFETY CHECK: when the plan's model of a side's emitted order
disagrees with the runtime the column is KEPT, because an extra column is a
divergence a gate sees and a dropped one is a user's data gone. The two paths
compute the ordinal against the model in force for each — the logical subtree's
emitted order on the single-process path, the STAGE's stream on the distributed
one, where a Project emits no stage.

**A NAME IS NOT THE IDENTITY, and the drop is by identity.** Reading is not
minting, so a table may already STORE a column called `__key_0` (ADR-0012), and
excluding by bare name over the join's whole output dropped the USER's column:
`SELECT * FROM o JOIN LATERAL (…) s` lost `o.__key_0` entirely, and
`SELECT o.__key_0` read NULL where PostgreSQL reads its values — right →
silently wrong, on a population that is every table written before the
reservation existed.

The column this join minted is the one it KEYS ON, on the side it minted it
for. So the exclusion holds only where the name is a JOIN KEY OF THE COLUMN'S
OWN SIDE: a stored column of that name on the other side is not a key there and
survives, and on the same side the allocator steps around every name this layer
can see — the lateral's own text and now the OUTER side's names as well
(`logical.outerScopeNames`). The one it cannot see is a stored column of the
INNER relation that the lateral's list does not name; that is pinned LOUD
rather than answered wrongly, and closing it is `physical.renameCollidingSlots`
gaining a correlation-family arm.

**Two spellings of the same collision, and the colliding one takes the FULL
mint.** Where the SELECT list does not carry the key, the slot is INJECTED as
an output item and the join keys on it. Where the list DOES carry it under
another name (`SELECT t.g AS gk, MAX(t.id) AS g … GROUP BY t.g`), the AGGREGATE
below still publishes its key under the source column's stripped text — the
same `g` the list aliased its MAX to — and the projection resolves by name:
`0,0,0…` for PostgreSQL's `0,0,4998…`, on the single-process arm and on both
DAG arms.

Stamping the aggregate's key as a slot while the join kept keying on the list's
own name (`gk`) closed it on the single-process path and answered ZERO ROWS on
both DAG arms in a second spelling: a Project emits no stage, so the build
stream is the AGGREGATE's `[__key_0, order_id]` and `gk` is not in it —
`exec.HashJoin` resolves the build key to −1, the degenerate all-rows-equal
key. So the colliding shape injects the slot AND keys on it, which is the one
name that survives every path: the aggregate publishes it, the projection
carries it, the shuffle can spell it, and the join drops it again on the way
out.

The subquery's OWN references to the key are re-spelled to the slot with it
(`respellKeyRefsToSlot`, planting `plansql.ColRef.Slot` for provenance). An
item still reading the source column bound whatever answered to that name in
the stage's stream, which in the colliding shape is the AGGREGATE:
`SELECT order_id AS oid, MAX(amount) AS order_id …` answered `Alice,100,100`
for PostgreSQL's `Alice,1,100` on both DAG arms.

It walks the BLOCK, not the select list. HAVING and the subquery's own ORDER BY
read what the aggregate PUBLISHES and take the slot with the list; the WHERE and
the GROUP BY are resolved against its INPUT and keep the source column. A
HAVING left behind named a column the aggregate no longer publishes and turned
`GROUP BY order_id HAVING order_id > 1` from PostgreSQL's row into a refusal on
all four arms. And the walk runs only where an AGGREGATE republishes the key:
without one the injected item is a SIBLING in the same projection, nothing has
computed it yet, and a re-spelled `CASE WHEN order_id > 1 …` read NULL and took
the ELSE arm on every row.

**Who may ask for the slot back is decided by the CALLER.** On the distributed
path the lateral's projection is materialized ABOVE the join, and that
projection is exactly the operator that reads the slot — so the fragment
builder removes the slot from the exclusion when the stage's own column list
names it (`worker.hiddenColumnSet`). Making the OPERATOR yield to its
`OutputFilter` instead applied that rule to both paths and re-opened the read
direction: `SELECT x.__key_0 FROM (SELECT * … lateral …) x` answered the key's
values again on the single-process arms.

The rename is made ONLY where the names really collide. `SELECT t.g, COUNT(*)
AS c` has its projection ELIDED over the aggregate (the shapes match), so what
the lateral emits IS the aggregate's output and the join keys on that; moving
the key to a slot there left the shuffle with `key "s.g" not in schema`. A
rename that breaks no collision buys nothing.

**What is NOT closed.**

- `SELECT t.g, MAX(t.id) AS g` — the key under its OWN name beside an
  aggregate of that name — publishes two columns called `g`. PostgreSQL
  refuses the outer `s.g` as ambiguous (42702) and this engine answers one of
  them, which is a superset either way; since the collision takes the full
  mint the key leaves under the slot and the `g` that survives is the
  AGGREGATE's. Pinned as
  `956/pinned-ambiguous-own-name-answers-the-aggregate`.
- A reference to a `__`-prefixed name THROUGH A DERIVED STAR over a lateral is
  loud on both DAG arms — `sanitizeScanNeeds` keeps every `__`-prefixed
  reference in a scan's required columns, the reference reaches the lateral's
  scan, and the worker's all-or-nothing projection guard takes the list down
  with it. Pre-existing, measured with this arc's own scan-needs rule disabled,
  and pinned with an ordinary-column control beside it.
- On the DISTRIBUTED arms a star over a NON-aggregated lateral still shows the
  key's SOURCE column: that lateral's projection emits no stage, so the stream
  carries the scan's names and the slot's alias never lands, which
  `OutputExclude` cannot match by name. The single-process arms and PostgreSQL
  publish four columns and the DAG publishes five. Recorded in ADR-0012 and
  pinned per-arm in `coordinator.TestArcJ1AStarOverALateralPublishesPostgres-
  Columns`; closing it needs the lateral's own projection materialized onto
  its stage.
- A QUALIFIED star (`o.*`, `s.*`) publishes the whole join rather than the
  named relation. Older than this arc and independent of laterals; ADR-0012.

**A star's declared side schemas are the whole relation, not the keys.**
`joinSideSchemas` built its `want` list from `NeededColumns` plus the join
keys, and no NeededColumns is not "needs nothing" — it is a star, which needs
every column. Narrowing to the keys made a task with an EMPTY build partition
write a file two columns wide beside files carrying the whole relation, and the
shuffle read refused the pair (ADR-0010). An empty `want` keeps every column,
which is what the star asks for.

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

§8j is this section's other direction, SETTLED 2026-09-18: which OCCURRENCE a
window key, a sort key, a lifted predicate column or a join-arm reference binds,
and why a name is derived from that identity rather than the reverse.

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
  walks read it — `dagplan.aggregateUnderOutput` for the gather,
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
  `dagplan.aggregateProjectionTarget` and `physical.aggregateGroupKeyName` —
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
  | `dagplan.aggregateUnderOutput` | shared | reader 1 |
  | `physical.findAggregateAncestor` | shared | reader 2 |
  | `physical.groupKeysPublishedBelow` | shared | reader 3 |
  | `logical.AggregateOverGroupRows` | shared | reader 4 (#774) |
  | `physical.aggregateOutputNames` | shared | reader 5 (#575 under a window) |
  | `physical.wrapsAWindow` | shared | a REFINEMENT of the question — "is one of the wrappers specifically a Window" — used to keep the projection-elision decision from looking through one, because a window ADDS a column and elision needs the node's WHOLE output |
  | `logical.AggregateBelowProject` | Filter only | deliberately narrower; its callers map a SELECT list onto the aggregate's own STAGE and a Sort/LIMIT/window emits a stage of its own. Documented at its definition |
  | `physical.scopePreservingWrapper` | Filter/Sort/Limit/Distinct/**Window** | the RELATION-scope twin of this question, already has Window |
  | `dagplan.resolveSortKeyColumn` | Filter/Limit/Sort/Distinct, no Window | MEASURED, left alone. `SELECT g AS k, ROW_NUMBER() OVER (…) … GROUP BY g ORDER BY k DESC` and six siblings answer PostgreSQL's order on all four arms — the call site's `producerMaterializesName` reset already covers it, and its mirror walk `derivedAliasSourceColumn` also excludes Window, so changing one alone is the ADR-0025 out-of-step shape |
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

## §6. The two names are not a property of GROUP BY keys (2026-09-07, arc J2)

§2 gave a GROUP BY key a PUBLISHED name and a RESOLUTION spelling and carried
both on the Stage. The two names stopped at the aggregate, and every consumer
below or beside one was left guessing a spelling and hoping the payload
carried it under exactly that text. Five more consumers have the same two
names, and #770, #947 and #949 are what that cost:

| consumer | its second spelling |
|---|---|
| a UNION arm's projection | the derived alias rewritten into the expression that DEFINES it (#554) |
| an aggregate ARGUMENT | the alias re-spelled to its source, or left as the alias |
| a WINDOW argument | the same, and the DECLARATION goes with the value |
| an ORDER BY term | the OUTPUT column it names, versus the producer's SOURCE names the fold leaves it addressing |
| a projection's DECLARED TYPE | the producer's own declaration, versus the expression re-read as arithmetic |
| a window's PARTITION BY key (2026-09-07, #975) | the arm its qualifier NAMES publishes it as an alias on one engine and as a source column on the other |

The rule is the same one, stated once: **a consumer binds through the identity
its PRODUCER published**, and where the producer publishes nothing for the
value, the payload carries it. The mechanism, its two phases, its plan-time
mirror of the runtime resolver and its measured payload cost (zero — the TPC-H
stage-dump golden is byte-identical) are in ADR-0025's SETTLED section, which
is where a payload question belongs; this section records only that §2's
carrier is the pattern the others follow.

The SIXTH is the PARTITION BY key, and it is the one that cannot wait for the
end of planning: the key is also the stage's DISTRIBUTION, so it settles at
EMISSION and the arm-aware choice has to be made there. ADR-0025's "SETTLED:
the sixth consumer" section carries it — a qualified key keeps its qualifier
where the input's column set cannot bind it, and the DAG resolves it inside the
arm its qualifier names. Same rule, different site, because the deadline is
different.

Each consumer records its candidates at emission and settles them at the end
of planning, exactly as `GroupKeyResolution.Alias`/`Def` does:
`AggSpec.InputRefs` and `WindowColSpec.InputRefs` (the written spelling, the
source column, the defining expression) and `SortKeySpec.WrittenTerm` (the
ORDER BY term as the query wrote it). None of the three reaches the wire.

§2c holds one node further out than it was written. "A name is never re-read
as structure" was about a GROUP BY key's text; #949 is the same sentence about
a DECLARATION. The DISTINCT lowering makes every SELECT item a group key, so
`SELECT DISTINCT a * 2 AS v` publishes `a * 2` as a COLUMN and emits no `a` at
all — and the projection above it, read as arithmetic, looked for `a`, found
nothing and fell to the float rule. An exact DECIMAL declared FLOAT64 on every
arm, and on the DAG a DECIMAL value written into a float vector, which is what
#361's guard is for.

### §6a. The ORDER BY consumer keeps the QUALIFIER (2026-09-08, arc L1, #989)

§6's fourth row — an ORDER BY term's two names — was settled for the STAGE
consumer (`SortKeySpec.WrittenTerm`, `resolveSortKeyColumn`) and left unstated
for the single-process one, which built each key as `cleanExpr(ob.Column)` and
threw the table qualifier away. That is §2c exactly: the qualifier is the only
thing that distinguishes one reference's column from another's, and dropping it
turns two keys into one.

A `SELECT *` over a join reads the join's stream, and
`exec.joinOutputSchemaWithMapping` publishes the probe's columns bare with
every DUPLICATE build column qualified by its owning alias. So `SELECT * FROM q
a JOIN q b ON … ORDER BY a.order_id, a.amount, b.amount` — a TOTAL order, no
ADR-0013 class — had a third key spelled `amount`, identical to the second, and
`exec.columnIndexFallback` bound both to the first column carrying it. The
trailing key was never applied and the residual sequence was the join's
emission order: right rows, wrong order, on the DEFAULT path, while both DAG
arms answered PostgreSQL's.

`physical.sortKeyLocalColumn` is the rule stated once: the key carries the
spelling the query wrote (`plansql.NormalizeIdentRef`, which strips delimiters
and keeps the qualifier). It needs NO model of which side of the join built,
which is what makes it the identity rule rather than a compensation:
`columnIndexFallback` tries the qualified spelling first and falls back to the
bare one, so `b.amount` binds the qualified column when the join qualified `b`
and the bare one when a predicate made the optimizer qualify `a` instead. Every
term that resolved before resolves to the same column, because the by-name
resolution IS the resolver's second step.

The address hierarchy for a single-process ORDER BY term is now, in order: the
ORDINAL (#557), the SELECT-list POSITION of the item the term names
(`sortKeyLocalSlotPos`, #905 — available only where the Sort's child is a
Project), and the WRITTEN SPELLING (#989 — what a star-only query has left).

SETTLED 2026-09-13 by arc O1 (#997, #1012, #993) — §9. The lead this
paragraph recorded was that the join's PUBLISHED name list is itself a plan
artifact: `logical.reorderJoins` expresses "which side builds" by SWAPPING the
join node's children and `physical.buildJoin` reads `Children[0]` as probe, so
the star's column ORDER and WHICH side gets qualified both moved with a cost
estimate — `SELECT * FROM t a JOIN t b ON a.id = b.id` published `b.*`
qualified with no predicate and `a.*` qualified under `WHERE a.id < 100`, on
all four arms. The fix is NOT the build-side mark this paragraph proposed:
`reorderJoins` only swaps a TWO-relation chain, and `costBasedJoinReorder`
REBUILDS a longer one, so there is no node whose children a mark could be
relative to. §9 records the star's list where it is still written down — the
FROM clause — and leaves this section's own rule untouched: the ORDER BY term
still carries the spelling the query wrote, and it still reads the join's
stream, because the projection §9 mints sits ABOVE the sort. The census is in
`internal/coordinator/arc_l1_order_by_qualifier_two_path_test.go`.

## §7 A derived block a STAR reads is a relation, and a stage publishes it

Added 2026-09-07 by arc K3 (#984, #980, #981).

§2 gave a GROUP BY key two names, and §6 generalized that: a consumer binds
through the identity its producer published. §7 is the same rule for the
consumer that has NO name of its own.

A `Project` emits no stage. On the distributed path a derived table's SELECT
list is therefore not a relation: the `Aggregate` or the `Scan` below it is
what materializes, and every consumer above compensates PER CONSUMER —
`resolveShuffleKey`, `resolveAggInputName`, `resolveSortKeyColumn` and the
gather's `OutputRenames` each map the name the query wrote back to the name the
stream carries. A **star** has nothing to map. It reads the stream by POSITION,
so it published whatever the stage emitted, and four ways a projection leaves
its stream behind became four wrong answers on both DAG arms:

| the block writes | the stream carries | the client got |
|---|---|---|
| `order_id, order_id AS oid` | `order_id` | `oid` GONE |
| `order_id AS k` | `order_id` | the source name |
| `CAST(COUNT(*) AS VARCHAR) AS n` | `order_id, __agg_0` | `n` **and** `__agg_0` |
| `amount * 2 AS d` | `order_id, amount, d` | `d` **and** `amount` |

**The rule.** A derived block a star reads emits its PROJECTION as the stage's
declared column set — by position, under the block's own names. It becomes a
real `OpProject` through `Stage.ProjectExprs`, which is the machinery
`attachScanSelectProjections` already uses for the statement's own SELECT list,
placed either on the producing stage or in a `StageProject` of its own. No
second namer is introduced: once the stage publishes the block's relation, the
join operator's own naming rule produces the star's columns from a relation
that is already right.

**Scoped to a star**, and the scope is the whole argument that this is not a
wider change: a named SELECT list over every one of those blocks answers
PostgreSQL on all four arms and always did.

Three things follow, and each was a separate defect until the block became a
relation:

1. **The join binds to what the producer publishes.** `resolveShuffleKey` and
   `resolveJoinNeededColumns` stop at a materialized block and take the
   PUBLISHED name — the same rule they already apply to a minted group key
   (§3a). `ON d.k = o.id` over a materialized rename otherwise fails with
   `partitioned shuffle: key "order_id" not in schema`.
2. **One column set per stage, defaulted columns included (#980).**
   `declaredJoinSchema` — the declaration a side that delivers NO BATCH is
   shaped by — reads the block's projection too, from the SAME walk that builds
   the specs. Two rules for one relation is exactly how an empty build task
   wrote a file its siblings could not be read beside: `declares 3 columns
   where an earlier file of the same stage input declared 4`, and where the
   widths agreed, `names column 3 "n" where an earlier file … named it
   "__agg_0"`. The known-ness of a declared type travels BESIDE the type,
   because `parquet.TypeBool` is the zero `TypeID` and a bool read as "no
   declaration" is shipped as a string.
3. **The hidden slot's ordinal follows the materialization.** A minted
   correlation key is dropped BY POSITION on the side the lowering built (§3c),
   and the two models — the stage's stream and the subtree's emitted list —
   are ONE model wherever the block is materialized, so `stageHiddenPositions`
   reads the projection there.

**What is MARKED.** A block whose projection is not the column list of the
stream beneath it — a name the stream does not carry, a name published twice, or
a stream that carries more than the block publishes. One class, one question,
asked of the STREAM. Two things are excluded, and neither is an allowlist: a
block whose stream this pass cannot state at all (there is no known divergence
to act on), and an ARM of a set operation, because `UNION`, `INTERSECT` and
`EXCEPT` name their arms — publishing an arm's own list onto the arm's stage
made the operation above read columns that were no longer there (`[<nil>]` for
`[50]`).

**ONE TYPE INFERENCE.** The block's published projection is typed by the call
`declaredJoinSchema`'s own computed-column arm and `attachScanSelectProjections`
already make for the statement's own SELECT list — same walk, same `strictInt`
hint, same STRING fallback, and the same SCALAR-SUBQUERY RESOLVER. One
inference means one set of ARGUMENTS too: without `subqueryDecl`, `(SELECT
MAX(amount) …) AS sq` fell to the STRING fallback and a zero-row `SELECT *`
over a block holding one declared it `text` where the same statement under a
matching predicate declared `int8` and PostgreSQL declares integer — a WRONG
declaration, which is worse than the none #978 replaced. There is no such thing as an item this engine
cannot declare: `ARRAY[c0]`, an all-NULL `CASE`, a bare `NULL` and
`COALESCE(NULL, NULL)` reach a client with an OID today, and so does a
scalar-subquery item. A WEAKER SECOND INFERENCE HERE WAS THE DEFECT that three
rounds of this arc kept re-discovering: it declined on `expr.Undecided`, and
every decline became a DISPOSITION — a query the single path answers with a
declared type was routed off the DAG, or left to read the stream.

**THE FALLBACK IS THE ROUTE.** A marked block that still cannot be published is
refused with one sentence and routed to the coordinator-local pipeline, which
is answer-preserving. There is no second disposition: "left alone" is gone,
because it is the door a star walks through onto the stream. Every routed shape
is measured at `bb8635a4` to have been wrong or loud there; a base-right shape
that routes is a defect in the inference, to be fixed, never a pin.

**The residue is TWO shapes**, each measured at `bb8635a4`, each answering
PostgreSQL on the coordinator-local pipeline, each gated by counter in
`coordinator.TestArcK3ADerivedBlockPublishesItsOwnProjection`:

| shape | at bb8635a4 |
|---|---|
| a BARE AGGREGATE alias beside a computed sibling (`SUM(x) AS sa, SUM(x)*1 AS sb`) — an aggregate item is computed by the aggregate stage, not by a projection above it | silent wrong (`__agg_1` published) |
| a WINDOW inside the block (`SUM(x) OVER () AS w`) | loud |

Everything else the earlier rounds listed now EXECUTES: a container over a
plain column, over a FILTERED scan, over an aggregate, inside an INNER and a
LEFT lateral, and in a set-op arm; a scalar-subquery item; an all-NULL `CASE`;
a bare `NULL`; one name published twice; a twice-referenced CTE with and
without a rename. The container spellings no longer publish their SOURCE column
beside the container either — the stage emits exactly what the block wrote.

**The rule's boundary, measured and recorded.** A block this pass cannot state
the STREAM of is never marked, and the one that matters is a block whose body
is itself a JOIN: `(SELECT i.amount AS id, i.order_id FROM lat_item i JOIN
lat_ord o2 ON o2.id = i.order_id)` under a star publishes NINE columns on `dag`
and seven on `dagshuf`, with `id` carrying the join's own `id` rather than the
block's aliased `amount`, where PostgreSQL and the single-process arms publish
five. Identical at `bb8635a4` and at `a0539069`, so this arc neither creates
nor closes it. Closing it needs a join's emitted list stated at plan time —
`exec.JoinOutputSchema` is that list, and reaching it here needs the side
schemas the star declaration already assembles, one level deeper.

RE-MEASURED 2026-09-08 by arc M1 (#993): the column SET half of that boundary
no longer reproduces — v0.18.62's `Stage.ProjectExprs` closed it and all four
arms publish PostgreSQL's ten for the bare-star spelling. What survives is a
NAME divergence between the two DAG arms, and it is NOT the block's: the SAME
three relations written as a plain three-way join, with no derived block at
all, diverge identically. Its producer is `markCoPathingSelfJoinBuilds`, which
sets `Stage.QualifyAllBuildCols` when two joins in one chain BUILD over the
same table (Q07's rule) — disabling that pass in place makes all four arms
agree on the bare spelling, and `WADJET_STAGE_FUSION=0` does not change it. The
two DAG arms differ because the walk reads each join's BUILD dependency and the
arms' stage DAGs put different sides on the build: over this statement the
broadcast arm finds ONE `lat_ord` build and the shuffle arm finds TWO, so only
the shuffle arm marks. That is §6a's rule broken by a second producer — which
side builds is a cost decision and must not decide a NAME — so it rides #997's
arc rather than a second pass here. Pinned per DAG arm, in both spellings, in
`coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan`.

A block whose own `ORDER BY` was materialized publishes its `__sortkey_N` — the
sort below still reads that key, so the projection cannot drop it — and what
happens next is decided by THE SORT KEY, not by the class:

  - keying on a column the block PUBLISHES executes distributed with every
    routing counter at zero (`… ORDER BY product LIMIT 3` over a block
    publishing `order_id, product`);
  - keying on a column it does NOT publish is refused by the SELECT-list
    reachability check (`UnreachableOutputLocalRoutes`, #656) and answered on
    the coordinator-local pipeline (`… ORDER BY amount …`), on BOTH DAG arms,
    with and without a LIMIT, and whether or not the block also introduces a
    column.

Measured at this tip in both spellings and gated as a pair. An earlier
statement of this in §7 and in `docs/sql-reference.md` said such a block
executes in the narrowing spelling; that was wrong, and it was wrong because
the measurement that produced it had truncated the routing counter out of the
probe's own output.

**Two PRE-EXISTING leaks were pinned, not fixed; ONE is closed.** A block whose
own `ORDER BY` was MATERIALIZED published its `__sortkey_N` (the sort below
still reads that key), on every arm and on the wire — **closed 2026-09-13 by
§9**: a star over a join publishes each arm's VISIBLE projection, and the
planner's materialized term is not one of them, so the reserved name stops
reaching the client for that shape (the two `sortkey/` cells in
`coordinator.TestArcK3ADerivedBlockPublishesItsOwnProjection` now assert
PostgreSQL's list). Two independent LATERALs over one table still publish the
second lowering's `__key_1`; it is in a `want` string, so the day it stops
leaking its cell fails.

**A build side that collapses its input is not a repeated scan of its table
(#981).** `markCoPathingSelfJoinBuilds` walks a join's build dependency chain
to the underlying scan and force-qualifies every build column when two joins
read one table in one chain — Q07's self-join rule. It walked THROUGH an
aggregate, so two laterals over one table looked like a self-join and the
client was handed `s.mx`, `s2.mn` where PostgreSQL and the single-process path
publish `mx`, `mn`. An aggregate's output is its group keys and its aggregates;
a stage carrying a materialized block projection publishes the block's names.
Neither is the table's columns, so the walk stops there. Names that really do
collide are still qualified, by the `isDup` + `BuildColOrigins` rule in
`joinOutputSchemaWithMapping`, which is the rule for a collision the plan
cannot see coming.

**Not settled here; CLOSED 2026-09-08 by arc M1 (#988) — see §8.** Two
independent LATERALs over one table publish the SECOND lowering's minted slot
to the client (`__key_1` beside `mx` and `mn`) on both DAG arms. The slot is
left in place by the join for the empty-input default operator above it to drop
(`Stage.LateralPadMarker` / `LateralDropMarker`), and with two such joins only
one drop runs. Pre-existing and measured identical at `bb8635a4` — where it was
worse, spelled `s2.__key_1` — and untouched here: it is the pad-marker drop's
own mechanism, not the stage's column set. §8 names the mechanism exactly: the
second join is ABSORBED by `fuseStageChains` into the first's stage, and
`ChainedJoinSpec` carried neither the marker nor the defaults.

## §8 The DAG publishes the PLAN's column set and ORDER, never a stage's stream

Added 2026-09-08 by arc M1 (#1002, #961, #988, and the gather-rename defect the
#1002 gate found).

§2 gave a group key two names, §6 generalized it to every consumer, and §7 made
a derived block a relation. §8 is the same rule stated for the two things a
consumer can lose besides a name: the ORDER of the rows and the SET of the
columns. Four producers broke it, each in its own way, and each fix is the same
sentence — *ask the producer, do not re-derive*.

### 8a. A merge that cannot apply the query's ordering says so (#1002)

Two coordinator merges re-apply a top-level `ORDER BY` over rows the DAG has
already produced: `mergeProbePartials`, after a probe-split re-aggregate or
dedup, and `dedupGatherResult`, after the post-gather `DISTINCT` `walkStages`
emits no stage for (#163). Both bound each key by an EXACT lookup in
`mergeColIdx` and `continue`d past a key that missed.

A dropped key is a silent wrong ORDER, and ADR-0013 lists no nondeterminism
class that covers a TOTAL one. A join publishes the probe's columns bare and
every duplicate build column qualified by its owning alias, so
`SELECT DISTINCT * FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id
ORDER BY a.order_id, a.amount, b.amount` had NEITHER `a.` key in that map: both
DAG arms returned the rows sorted by `b.amount` alone — the LEADING key not
applied at all — where PostgreSQL 17 and the two single-process arms answer the
written sequence. Under an `OFFSET` it is a wrong ROW SET as well, because the
truncation is applied after the ordering.

**The rule.** `mergeSortKeyIndices` resolves the keys ONCE, through
`exec.ColumnIndexFallback` — the engine's one resolver, which
`physical.sortKeyLocalColumn` (§6a) and the DAG's own sort stage already bind
through — preferring `SlotPos` where the planner recorded one (§2, #557). A key
that still does not resolve, and a set of partials that do not describe one
relation, are `0A000` REFUSALS — PostgreSQL answers the statement and what
wadjet is saying is that its own merge cannot. The alternative is rows in an
order the client did not ask for and cannot detect; `reAggregatePartials`
already refuses an unresolvable GROUP BY name one screen up for exactly that
reason. An EMPTY result is neither refusal: it has no order to get wrong.

**And a NULL row is still a WRITE (#1007).** Above one batch the merge has to
COALESCE — a selection vector reorders within one batch and cannot express an
order across two — and `coalesceForOrdering` set the null bit and skipped
`copyVectorValue`. A variable-length column's value at row i is
`Data[Offsets[i]:Offsets[i+1]]`, so a row that writes nothing leaves the closing
offset at zero and the NEXT non-null value is read from the arena's origin:
`SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id ORDER BY a.id
DESC` returned 116 of its 5000 `c_str` values as the concatenation of every
value above them, on both DAG arms, with the rows and the ORDER right.

The rule this settles is narrower than "fix the offsets": **the null decision
belongs INSIDE the copier, and the copier delegates it to the batch package's
own writer.** `batch.Vector.WriteNullAt` knows what each carrier owes — the bit,
an empty span for a `BytesColumn`, a ROW's children recursively. The engine's
other copier (`exec.copyVectorValue`) already decided it inside, which is why no
single-process path was ever wrong; the coordinator's left it to its two callers
and one of them got it wrong. A copier that a caller can get wrong is the defect,
not the caller.

The gate for it is per CARRIER and it is a UNIT gate, because the SQL that
reaches it needs more than 2048 rows and every shape cell in this arc's own
census is eight: `coordinator.TestCoalescingAMergeAdvancesAVarlenNullsOffset`
(five `BytesColumn` types, ARRAY, MAP, ROW, three fixed-width controls) with
`TestM1AMergedOrderIsTheQuerysOrderAtScale` as the SQL half. ARRAY and MAP write
BOTH ends of their span and self-heal — they are coverage, not reproductions,
and saying which is which is what keeps the gate honest.

**Why only this shape reached it.** `rewriteDistinctAsGroupBy` gives every other
`DISTINCT` a stage — a projection's items become GROUP BY keys, and a star over
ONE relation takes `rewriteStarDistinct` — but a star over a SELF-JOIN declines
there, because `starDistinctGroupKeys` refuses a name two scans publish (one
group key cannot stand for two columns, #277). That decline is what routes the
statement to the coordinator's dedup, and the re-sort after it.

### 8b. A set operation's arms supply the operation's result columns (#961)

`UNION`, `INTERSECT` and `EXCEPT` match their arms BY POSITION over the whole
result row: the result column list is the first arm's, every arm is projected
onto it (§7's exclusion), and for every spelling but `UNION ALL` that whole row
is also the DEDUP KEY. Nothing above the operation can therefore say that an arm
may stop producing a column.

`logical.pushColumnNeeds` had no set-op case at all, so an outer need fell
through the generic recursion straight into both arms. Two STAR arms then read
ONE column each while the union stage's arm projection — built from the arms'
DECLARED output lists, which is what the operation publishes — still asked for
the table's whole list, and both DAG arms failed with `column "g" does not exist
in the input schema`. On the single-process path there is no name-based arm
projection to fail and the narrowing landed on the DEDUP KEY instead, silently:
a distinct `UNION`, an `INTERSECT` and an `EXCEPT` over two star arms each
answered 0 on all four arms where PostgreSQL answers 30, 20 and 10.

A set-op node pushes `nil` — this walk's "all columns" — into every arm now. An
arm with an explicit SELECT list is a `Project`, which builds its own needs set
from its own items and is narrowed exactly as before, so only the STAR arm,
which has no `Project` at all, is widened. This is the rule `rewriteStarDistinct`
already states one operator over ("the group keys are required columns, so the
pruner keeps them", #277).

### 8c. A join's own rules travel with it when a stage absorbs it (#988)

A decorrelated LATERAL's join owns two rules: the drop of the `__key_N` slot the
lowering minted (§3c) and the per-column empty-input defaults
`exec.LateralEmptyDefault` applies above it. `fuseStageChains` absorbs a 1:1
downstream join into its upstream stage as a `ChainedJoinSpec`, which carried
that join's `HiddenJoinCols` but neither its `LateralPadMarker` nor its
`LateralEmptyDefaults` — so with two independent LATERALs over one table exactly
one drop and one default ran per QUERY. `__key_1` reached the client on both DAG
arms beside `mx` and `mn`, and the absorbed lateral's `COUNT(*) + 1` came back
NULL for an outer row it matched nothing for where PostgreSQL answers 1: a wrong
VALUE, not only a leaked name. `WADJET_STAGE_FUSION=0` answered PostgreSQL on
both DAG arms at base, which is what localizes it to the fusion.

**The rule.** A spec that replaces a join is the whole record of that join, or
the join is not absorbed. `ChainedJoinSpec` carries all three fields and the
dispatcher puts them on the chained `OpSpec`, so the worker builds one
`exec.LateralEmptyDefault` per absorbed lateral join. `FusedJoinSpec` has a
field for none of it, so `fuseJoinStages` DECLINES a candidate carrying a pad
marker or a hidden slot — the same call its `NullAwareAnti` and `ProjectExprs`
guards already make, and the honest alternative to carrying a rule nowhere.

### 8d. A gather rename binds the column its source NAMES, exact spelling first

A SELECT list of plain group-key references emits no stage: the aggregate
materializes and the gather's `OutputRename{From, To}` maps each item back onto
what that stage publishes. An aggregate publishes a key under BOTH spellings
(§2), and the join's own names ride the stream too, so the gather resolved
against `[order_id amount amount a.order_id a.amount b.amount]`.

`classScopedMatch` — the §2 class rule for an item whose name several columns
answer to — rescanned for the BARE name whenever the EXACT spelling matched
fewer than TWO columns, which is every uniquely-resolving qualified reference.
`a.amount` and `b.amount` each matched exactly ONE column and both were re-bound
to the first bare `amount`, so

    SELECT a.order_id, a.amount, b.amount FROM lat_item a
    JOIN lat_item b ON b.order_id = a.order_id
    GROUP BY a.order_id, a.amount, b.amount ORDER BY a.order_id, a.amount, b.amount

returned eight rows whose THIRD column carried the SECOND's value on both DAG
arms — the right groups under wrong values — where PostgreSQL 17 and the
single-process arms answer eight distinct triples. `SELECT DISTINCT` over the
same self-join lowers to the same tree and had it too.

The rescan runs only when the exact spelling matched NOTHING now, which is
`exec.ColumnIndexFallback`'s order and §2c's rule: a column REFERENCE is
resolved as a column name, and a name that resolves is not re-read as a shape.
The case the rescan exists for (#785 — an item spelled through a derived table's
alias, over a stream carrying the bare name twice) has ZERO exact matches and is
unchanged.

### 8e. A DEPENDENT join is not reorderable (#1008)

A decorrelated LATERAL is a join the PLANNER manufactured: its inner side is a
plan OF the outer side's rows, and the node carries the rules that make it
correct — the `__key_N` slot it minted and drops (§3c), the pad marker, the
empty-input defaults. `reorderJoins` treated it as an ordinary inner join, and
reordering it is not a cost decision:

* `flattenJoinChain` walked THROUGH it, so two LATERALs over one outer
  flattened to THREE relations and `costBasedJoinReorder` rebuilt the chain
  with `NewJoin` — which carries none of those rules. `SELECT *` over two
  grouped laterals published `__key_0` and `__key_1` in the client's relation,
  and the re-hung conditions keyed a STRING against the integer correlation
  column: `cols=[] rows=0` on the single-process arms where PostgreSQL 17
  answers eight rows, and the loud `join key "s.__key_0" is STRING on the
  probe side` (#615) on both DAG arms.
* The two-way swap exchanged the SIDES, and for a manufactured join the side
  order IS the answer — PostgreSQL publishes the outer relation's columns and
  then the lateral's. One grouped lateral came back as `p, id, customer,
  total`, and the same swap is what made
  `pgwire.TestArcJ1AHiddenSlotIsNotInTheRowDescription`'s
  `star_over_a_non_aggregated_lateral` cell record a divergence from
  PostgreSQL that was never the join operator's doing. That pin is spent.

**The rule.** A join the planner manufactured is one relation to the cost
model: `isDependentJoin` reads the lowering's own marks
(`Node.LateralSubtree` on the side it built, plus the rules it hangs on the
join), `flattenJoinChain` stops there, and the two-way swap declines. An
ordinary inner join is reordered exactly as before — which is §6a's "not
settled" paragraph, unchanged: an ORDINARY join's sides are still ordered by
estimated rows, so `SELECT *` over one still publishes the cost model's order
rather than the query's.

### 8f. An ORDINAL sort key binds the slot the producer PUBLISHED (#1003)

An output slot's identity is its POSITION (#557), so `ORDER BY 1, 2` addresses
slots, not names — and two output columns may legally carry one name.
`sortKeySlotPosStage` dropped the position for any sort whose subtree contains
a join, because on the DAG a Project emits no stage and a select-list position
need not address the producing stage's stream. The key then resolved by NAME,
and `ColumnIndexFallback` answers a duplicate with the FIRST match:

    SELECT DISTINCT a.order_id AS amount, b.amount FROM lat_item a
    JOIN lat_item b ON b.order_id = a.order_id ORDER BY 1, 2 DESC

publishes `amount` twice, both keys bound column one, and the two DAG arms
returned `1,50 | 1,100 | …` where PostgreSQL 17 and both single-process arms
return `1,100 | 1,50 | …`. The rows are right and the SEQUENCE is not, which
no unordered comparison can see and which ADR-0013 lists no nondeterminism
class for.

**The rule is §8's own, one consumer over: what the producer PUBLISHES decides,
and it is MEASURED.** The position is used when the stage producing the sort's
input publishes the select list as the ordered prefix of its own output —
which the `final_aggregate` stage under that query does, materializing
`[a.order_id→amount, b.amount→amount, a.order_id, b.amount]`. The whole
visible list is compared, source expression and name (a projection has more
than one legitimate name — §2's resolution spelling and the `PublishedName`
the client is told — and either matches), so a producer that publishes the
same names in another order, or narrows the list, does not qualify. Where the
projection is not materialized — `SELECT clt1.c2, clt2.c1 FROM clt1, clt2
ORDER BY 2`, whose join stage carries no `ProjectExprs` — the key resolves by
name exactly as before, which is the bound this replaces the guess with.

A WRITTEN qualified term beside a duplicate output name was NOT closed by
this, and is now — see §8g.

### 8g. A WRITTEN term binds the same slot, under the same measurement (#1014)

An ordinal and a written qualified term address the SAME list, so §8f's rule
covers both; only the ordinal half was implemented. `resolveSortKeyColumn`
rewrites a written term onto the select-list item that carries it, so
`ORDER BY 1, b.amount DESC` over an output list that publishes `amount` twice
reached the stage spelled `amount` and bound the first of them — the very
answer §8f exists to prevent, one spelling over, and at 5000 rows under a
LIMIT a wrong ROW SET as well as a wrong sequence.

**Both engines resolve a written term's slot through ONE function**
(`sortKeyWrittenSlotPos`, extracted unchanged from `sortKeyLocalSlotPos`):
exactly one visible item must answer to the term, by its alias or by the
expression it was written as; two is the ambiguity ADR-0012 records as a
superset, and a position there would change WHICH one on one arm and not the
others. **The PROOF is not shared.** On the DAG a written term takes a
position ONLY under §8f's measurement (`producerPublishesSelectList`), never
under the subtree-shape bound an ordinal may also use: a written term is
resolvable on far more queries than an ordinal is, and a position handed out
where this layer has not looked at the producer is the defect rather than the
fix.

### 8h. A SET OPERATION's result columns are addressed by POSITION (#1022)

A set operation's result columns are named by its LEFTMOST arm, and two of
them may be the SAME string: `SELECT order_id AS amount, amount FROM lat_item
UNION SELECT id, total FROM lat_ord` publishes `amount` twice. The name is
therefore not an address, and it was the only one in use in three places:

1. **The DEDUP KEY.** A distinct `UNION`'s dedup is a `GroupByAll` hash
   aggregate, which resolves its key set from the live schema and then looked
   each key back up BY NAME, so both keys bound column one: the operation
   deduplicated `(order_id, order_id)` and the two DAG arms answered THREE
   rows whose second column carried the first's values under the first's
   declared type, for PostgreSQL's seven. `INTERSECT` and `EXCEPT` take the
   same shape through `emitSetOpCountingStage`'s key list and answered ZERO
   rows for PostgreSQL's four. The key is now addressed by POSITION —
   `exec.HashAggregate.GroupByColIdx`, the group-key twin of
   `AggColumn.InputColIdx` (#575), carried on `Stage.GroupByColIdx` and
   `distributed.OpSpec.GroupByColIdx` — which is what the positions ARE by
   construction: every arm is projected onto the result column list, in order,
   so that the arms are one schema (§8b). `GroupByAll` needs no wire field at
   all: "group by every input column" means key i IS column i.

2. **The ORDINAL sort key, twice on one path.** `resolveSetOpOrderBy` rewrites
   `ORDER BY <n>` to the leftmost arm's name for item n but, unlike
   `resolveOrderBy` beside it, recorded no `OrderByItem.Ordinal`; and
   `buildSetOpPlan` built its Sort's keys by hand rather than through
   `orderExprFor`, so even a recorded position would not have reached
   `OrderExpr.SlotPos`. Both keys of `ORDER BY 1, 2 DESC` therefore reached
   the sort spelled `amount` and bound the first — seven right rows with key 2
   never applied, on EVERY arm. A position over a set operation needs no
   further proof at the stage layer (`sortInputSetOpWidth`): the operation's
   output IS its result column list, which is exactly what §8b makes true.

3. **A position at or past a STAR in the leftmost arm** was refused outright,
   `ORDER BY position 3 is out of range (1-1)` — the star counted as one
   column — for a query PostgreSQL answers. It is now DEFERRED to
   `logical.ResolveOrdinalSortKeys` exactly as the non-set-op spelling is
   (#810, #982), and that pass now records `SlotPos` when it resolves, so a
   deferred position is a position rather than a name.

4. **A NESTED operation's arm projection read its result columns by name.**
   `A UNION B UNION C` parses left-deep, so arm 1 of the outer operation IS a
   set operation, and `setOpArmProjection`'s nested branch spelled each outer
   spec `ProjectExprSpec{Expr: innerNames[i]}`. Where the inner result list
   carries one name twice both specs read the FIRST column, so the union
   stage's file declared column two FLOAT64 and carried column one's INT64 —
   which the next stage refuses loudly with the ADR-0010 type-disagreement
   message rather than answering. The specs carry `SourceSlot` now, the field
   that already exists for exactly this ("a name is not a handle when two
   columns answer to it", §3a), and the positions are the inner operation's
   own by the same construction the rest of this item rests on.

### 8j. A WINDOW key, a SORT key and a join-arm reference bind by OCCURRENCE (2026-09-14, arc L1: #1028; SETTLED 2026-09-18, arc WK)

A window key is a key, and §4's rule holds for it: the identity of a column is
the relation that produced it, never the bare name two relations share. Three
mechanisms used to answer "which arm owns this key" and they did not agree.
This section recorded the seam; arc WK closes it with ONE resolution.

**THE RULE.** A window key, a sort key, a lifted predicate column and a
join-arm reference bind by IDENTITY — the OCCURRENCE that produced the column,
and the column within that occurrence — carried from binding through every
rewrite. **A NAME is derived from the identity for publication; an identity is
never derived from a name.** It is §4 stated for the consumers this section
left open, and it is §9's `StarColumn{Resolve, Publish}` pair one consumer
over: a key is a (resolve, publish) pair whose resolve half names an
occurrence. "Occurrence" and not "arm" deliberately — two references to one
table are two occurrences, a written alias is a SPELLING of one rather than the
occurrence itself, and an occurrence with no written alias still produces
columns.

Two corollaries are what the code obeys.

1. **No pass may narrow a reference to a spelling that names less than the
   identity.** The planner never widens a qualified reference to a bare one
   because some column answers to the bare name.
2. **A resolver may bind a slot by name only where the planner has established
   that the slot IS this occurrence's carrier in THIS stream.** Where it has
   not, the reference is TRANSLATED through the producer mapping, the carrier
   is SUPPLIED, or the plan REFUSES — never bound to another occurrence's
   column.

**The defect corollary 1 closes.** `resolveWindowKeys` dropped a qualified
reference to its bare spelling whenever `bindWindowColRef` found the bare name
in the input's type map — and `inputColTypes` over a join MERGES both arms into
one map keyed by the bare name (it drops only a name the two sides type
differently), so `o.id` was not a key of it and `id` was. The occurrence was
erased at plan time, after which the runtime can only answer whichever arm the
join publishes bare: `joinOutputSchemaWithMapping` emits the probe's columns
bare and qualifies every duplicate build column by its owning alias, and which
side probes is a cost decision. `PARTITION BY o.id` over `lat_ord o JOIN
lat_item i` therefore put every row in its own partition and answered 1 where
PostgreSQL 17.11 answers 2, on all five arms, in silence (#1028); `SUM(o.total)
OVER (PARTITION BY o.id)` and `ORDER BY o.id` were the same fact through the
window's other two positions, and `QUALIFY` was a fourth spelling of it.

An earlier version of this section said `inputColTypes` "declines a JOIN
outright, so the bind below never even runs there". That is true of a join of
DERIVED arms — the walk has no `NodeProject` case — and FALSE of a join of base
scans. **The fold is the merge**, and it is why one written key took two
different mechanisms depending on whether a Project stood between the window
and the scans.

The repair is `windowArgKeepsItsQualifier`, which is the rule the window's own
ARGUMENT has followed since #742 round 4, applied to the PARTITION BY and ORDER
BY terms thirty lines away in the same file: a qualified term keeps the
spelling the query wrote wherever more than one occurrence of the input
publishes its bare name. The carried spelling is then an ADDRESS rather than a
guess — `exec.ColumnIndexFallback` hits `o.id` exactly when the join qualified
o's side and hits through its qualifier strip when it qualified i's instead, so
it needs no model of which side built, which is the composition §6a's sort-key
rule already relies on.

**It is NOT the materialization route, and that is why it holds.** Three prior
repairs routed a qualified reference the input cannot settle into a
`__winkey_N` slot — `PARTITION BY o.id + 0`, one character away and an
EXPRESSION, is right — and each fixed those cells and broke three green gates:
`coordinator.TestArcK1AWindowPartitionKeyBindsItsOwnArm` (#975), where
`PARTITION BY x.w` over two derived arms is right AS A NAME and materializing
it gives every row its own partition; and
`TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG` (#658) and
`TestJ2AJoinConsumerBindsThePublishedIdentity` (#770), which failed even after
the route was narrowed to a base-scan arm. Nothing is recomputed under the
rule, so all three stay green. The direction claim an earlier version added —
that materializing an ORDER BY term inverts the window's numbering — was FALSE
and is withdrawn; `orderDescExprOverJoin` and `orderDescSingleRel` are gate
cells so it cannot drift back.

**`exec.ColumnIndexFallback` is NOT deleted.** Measured: removing only its
qualifier-strip step fails nine top-level tests across `internal/engine/exec`
and `internal/planner/physical` — the hash join's key assignment, the
sort-merge join's keys, the hash aggregate's group keys, a projection over a
self-join and the sort's own keys. Over a join's own output its first two steps
ARE that convention read back.

**What became unreachable is ONE thing, and the second is owed.** The PLAN-TIME
erasure is gone: no pass widens a qualified reference to a bare one because the
folded type map answers the bare name. Corollary 2's precondition — that a
reference reaches the resolver only in a stream whose carrier the planner has
ESTABLISHED — is the contract phase 2 states, not yet a mechanical guarantee.
It holds wherever a producer contract exists (a base scan's own columns, a
derived arm's rename, a set operation's own list since §8i item 1) and it does
NOT hold for a decorrelated LATERAL arm on the three DAG arms, where the strip
still adjudicates and binds the outer occurrence — measured in the seam's own
table below, retained and pinned per arm. Nothing enforces the precondition
structurally today; what enforces it is the gate.

**What corollary 2 still owes, measured.** The seam's own table enumerates
every consumer against every producer on five arms
(`coordinator.TestWKASeamConsumerBindsItsOwnOccurrence`, 50 cells, 250 (cell,
arm) results against live PostgreSQL 17.11; the wire half is
`pgwire.TestWKTheWireDeclaresTheSeamsOwnColumns`). 216 agree. The 34 that do
not are one COLUMN of it, the LATERAL producer, and one KEY SHAPE, an
expression whose two leaves name two occurrences — both DAG-only, both
`distributed`. The LATERAL column:

- **five consumers × three DAG arms.** A decorrelated body's Project emits no
  stage, so the DAG's join publishes the body's INNER SCAN spelling where the
  single-process join publishes the arm's own alias. `p.id` misses exactly and
  the qualifier strip binds the OUTER `id` — corollary 2's precondition
  failing, not its lookup working. Right on the engine's arm and wrong only on
  the distributed ones, so it is pinned per arm, labelled `distributed`, and
  left to the distributed campaign (engine-first). Closing it is the rule's DAG
  half: translate the reference to the body's carrier INSIDE the occurrence the
  qualifier names, before the consumer binds it — `windowArgSourceInScope`
  composed with `derivedAliasSourceColumn`, which is how a window key already
  reaches a derived arm. #1126 is the same fact seen as two published
  SPELLINGS, and both halves — the resolution and the published name — have to
  land together: renaming the output to the bare `id` leaves the DAG's sort
  still bound to the outer column.
- **a star over a LATERAL**, refused on all five arms: the arm's list carries
  the correlation slot the join drops (§3c), so it is not expanded and a
  positional `ORDER BY` has no position to count. §9's decline list.
- **the contested lifted predicate on the two single arms**, which is #1130;
  the three DAG arms answer PostgreSQL's rows. Not a key-binding question —
  ADR-0021 §1q measured both available routes out.
- **an EXPRESSION key whose two LEAVES name two occurrences**, on the three DAG
  arms. The mint gives the RESULT a name nothing else owns and says nothing
  about the leaves, which the ordinary reference rules bind: over two derived
  arms publishing `w`, `PARTITION BY x.w + y.w` computes `y.w + y.w` where the
  answer is `x.w + y.w`, and the plain `x.w + y.w AS k` — no window at all —
  computes `x.w + x.w`, which localises it to the join-arm REFERENCE consumer
  rather than to the window. Corollary 1 reaches a key that IS a reference; a
  key that CONTAINS one is the same question one layer down. Right on the two
  single arms, `distributed`, pinned per arm (measured by the round-1 review).

*(2026-09-23, arc LT: the rewrite is back and the minted partition BINDS on
the three DAG arms — measured over the arc's seam table and over
self-correlated bodies, every equality-keyed bound cell agreeing with
PostgreSQL 17.11. The four-door disclosure below was not the minted key's
binding at all: it is the distributed path's own, for ANY window over a
policed scan feeding a join — user-written included — and is guarded by
`dagplan.CheckPolicedWindowUnderJoin` (routed single-process) until it is
localised; ADR-0021 §1s records it. What follows is the record of the
withdrawal; §1s is the current rule.)*

**A PLANNER-MINTED window inherited all three, and that is why #1019's rewrite
was withdrawn.** Arc L1 built the per-outer-row LATERAL bound on a minted
`ROW_NUMBER() OVER (PARTITION BY <the inner correlation column>)`: on the three
DAG arms that partition did not bind the body's own column, so over a
SELF-correlated body every row became its own partition — and over a POLICED
column that is a per-row disclosure of the stored value's equivalence classes
on four of the nine doors (ADR-0021 §1q, ADR-0033). A window the PLANNER writes
is not safer than one the user writes: it is the same key resolution, reached
where no user can see it. Corollary 1 removes that fault for a base-scan
occurrence; the rewrite itself stays ADR-0021 §1q's, with its own two measured
faults. Because this arc rewrites BINDING,
`server.TestArcWKAReboundKeyOverAPolicedColumnReadsTheMask` holds the class on
all nine doors — a window keyed on a masked column has ONE partition under the
mask and eight singletons under the stored values, and the gate asserts the
mask's answer, the absence of every stored policed value, and a non-vacuous
(cell, door) answered count.

### 8i. A join ARM that is not a base scan is named by the query (2026-09-14, arc R2: #1102, #1099, #1095)

A join arm that is a SET OPERATION, a join-bodied derived block, a grouped
block or a nested renamed block is keyed, shuffled, merged and NAMED on the
three DAG arms exactly as on the single-process arms. Four defects said
otherwise, each right on the single arms and wrong on the distributed ones, and
each is one sentence of the same rule: **the identity of a column is the
relation that produced it, and a reference into an arm has to reach the
spelling the arm's stream really carries.**

1. **A SET OPERATION's arm is its own relation (#1102, a wrong VALUE).** The
   stage projects every arm onto the operation's own column list, so the stream
   the join receives carries the operation's columns and nothing of any scan
   below it. `stageBuildTableAlias` named it after the first scan it found, so
   `(SELECT id FROM lat_ord UNION SELECT id FROM lat_ord) a JOIN lat_item b ON
   b.order_id = a.id` qualified the arm's duplicate `id` as `lat_ord.id`, and
   `SELECT a.id` — matching neither spelling exactly — fell through the
   qualifier strip onto lat_item's `id`: 1|2|3|4 for PostgreSQL's 1|1|2|2.
   Which side BUILDS is a cost decision, so the same statement was right
   whenever the plan chose the other relation. Such an arm is a MATERIALIZED
   arm now (`setOpArmPublishesItsOwnList`), qualified by `joinArmAlias`; with
   its columns addressable, arc O1's decline of the STAR over one comes out
   (§9's table) and `blockOwnProjection` descends a set operation to its
   leftmost arm, which is the list PostgreSQL publishes.

2. **A key into a block names the column the stream carries (#1099, wrong
   ROWS).** `resolveShuffleKey` chased `Projection.Column`, the BARE name, so a
   block's item written `o2.id AS k` resolved to `id` — and where the block is
   itself a JOIN, both of its relations answer to `id`. The outer join keyed on
   `lat_item`'s id instead of `lat_ord`'s and paired rows violating its own
   condition. It keeps the qualifier the query wrote EXACTLY WHERE THE PRODUCER
   PUBLISHES THE BARE NAME TWICE, which is exactly where the stream carries the
   qualified spelling: a JOIN inside the block whose two relations both answer
   to the name, or an AGGREGATE publishing its key qualified because one of its
   own outputs took the stripped name (§2b's rule, #1078). Not everywhere: a
   qualifier the stream does not carry is resolved by every binding
   (`exec.ColumnIndexFallback` strips one on a miss) EXCEPT
   `exec.HashJoin.FixKeyAssignment`, which asks whether a key is "in the build
   schema" with an exact-name map and swaps a correctly assigned pair when the
   answer is no — after which a null-aware anti join keys on the probe's
   column and loses NOT IN's NULL (25 rows for PostgreSQL's 0, measured). The
   two-name rule is about which column a name IS, and the spelling that reaches
   the stream is a fact about the producer.

3. **An ordering above a join addresses the SLOT its class names (#1095, a
   wrong ORDER).** §8d's class rule for a duplicate name had one consumer
   missing: the ordering FUSED onto a join. Both halves of the slot are
   recorded where each is known (§6's pattern) — the CLASS at emission,
   through the block and not off the wrapper; the POSITION at the end of
   planning, from the PROBE's model, which is the join's leading prefix
   MEASURED against the stage's own output stream rather than assumed.

4. **The gather's rename keeps the BUILD arm's name.** Two copies of one block
   resolve their items to one bare source name, which binds the probe's copy,
   so every row came back paired with itself. `buildArmQualified` puts the
   arm's name back for the gather's rename, wherever the arm holds exactly one
   relation and computes no relation of its own.

5. **An OUTER join's DECLARED side schema describes the stream its siblings
   write (round 2).** The task whose build partition is EMPTY shapes its
   NULL-extended rows from the side's declaration, so a declaration narrower
   than the stream is a file of the wrong WIDTH beside its siblings —
   `declares 2 columns where an earlier file of the same stage input declared
   3` (ADR-0010) — and where it merely LOST the NULL-extended column the row
   read the other relation's value: `3,3` for PostgreSQL's `3,NULL`. Three
   narrowings did it: a duplicate bare name DROPPED where a join under that
   side emits it qualified; a want list spelled in the CONSUMER's names against
   a walk that enumerates the producer's; and `markCoPathingSelfJoinBuilds`
   qualifying every build column of a co-pathing join AFTER every declaration
   was written. The first two are the declaration's own; the third is settled
   over the finished graph, qualifier only, for the sides that flag reaches.

   **Item 5 has two residues of its own, both measured, both refusal-only.**
   The declaration describes the stream where this layer can derive it from
   the logical plan, and two producers put a relation there that no walk of
   that plan states. A SET OPERATION whose arms are FILTERED empties one
   shuffle partition of the operation's own output and not another, and the
   stage's files then disagree about a column's NAME (`names column 1 "s.id"
   where an earlier file … named it "k"`), about the WIDTH of a star, or the
   DISTINCT spelling's GROUP BY key resolves against an input that no longer
   carries it — nine cells, every distributed arm. And a block whose body is a
   CO-PATHING SELF-JOIN under an outer join is qualified on BOTH of its joins
   by that same late pass, so the declaration is one column narrower than the
   file its siblings write — two cells, on `dag` and `dag-morsel4`, where
   `dag-shuffled` now agrees. Every one is loud at the shuffle, never a value,
   and identical at base: they are recorded per arm in
   `coordinator.arc_r2_pins_test.go`'s `r2Refuse`, and a cell that starts
   ANSWERING fails the gate.

**The residue is the NESTED GROUPED arm, and it is two cells.** An aggregate
publishes a relation of its own — its keys and outputs, under the names IT
decided — so an arm whose SELECT list the aggregate absorption MATERIALIZED is
named by the query (item 1's move, one producer over), and the four cells
deferred in round 1 are two now. Round 1's stated reason for that deferral was
not the measured one: the TPC-H aliases do not move — the stage-dump golden,
the distribution snapshot and both invariance arms are byte-identical. What the
measurement does say is a BOUNDARY: a DEPENDENT join's arm is excluded, because
a decorrelated LATERAL's arm is a plan OF the outer side's rows rather than a
relation the query wrote (§3c), and naming it by the enclosing alias moved
`MAX` over a CTE inside a LATERAL from PostgreSQL's declared scale on the DAG
to the single-process arm's narrower one — a wrong declaration traded for a
right row set, which is not a trade. The two cells that remain are the block
that renames TWO blocks above the aggregate, with the inner block's Sort
between them: no list is materialized there, and marking such an arm anyway
trades one wrong answer for another. They are pinned per arm in
`coordinator.arc_r2_pins_test.go` with that mechanism.

Gate: `coordinator.TestR2AJoinArmIsKeyedAndNamedTheSameOnEveryArm` — 515 cells,
{a plain derived block, the four set operations, join-bodied, grouped, grouped
with an aggregate aliased like the key's source, nested renamed, nested renamed
over a grouped inner block, a block carrying its own sort key, the same with a
LIMIT} × {names nothing else spells, names the other side spells too} × {left,
right, both sides} × {an explicit list, a qualified reference alone, a star,
DISTINCT, an ORDER BY above, a GROUP BY above}, on five arms against live
PostgreSQL 17.11, plus {LEFT, RIGHT, FULL} × {join-bodied, set-op, grouped,
nested renamed} × {list, DISTINCT, star} with the arm on the NULL-supplying
side. The name-collision dimension is load-bearing: with names nothing else
spells, three of the five classes answer correctly by luck. So is the OUTER
one: an inner join never asks a task to shape a row its data did not produce.

### 8k. A join conjunct hangs where every relation its QUALIFIERS name is joined (2026-09-24, arc JP: #1299)

`reorderJoins` takes an inner-join chain apart into relations and edges and
rebuilds it in the cheapest order, hanging each ON conjunct on the first join
that holds its edge's relations. `flattenJoinChain` resolved an edge's two
relations by BARE COLUMN NAME: the right endpoint was a relation owning one of
the conjunct's column names, the left one found by excluding the names the
right one owned. Where two relations carry the same names, that elimination
excluded every name and fell back to "the last relation of the left side" —
which, with a table joined three times, is a SIBLING copy:

```
FROM lat_ord o JOIN lat_item s ON s.order_id = o.id AND s.id IN (2,4)
               JOIN lat_item t ON t.order_id = o.id AND t.id IN (1,3)
               JOIN lat_item u ON u.order_id = o.id AND u.id IN (1,3)
-- t's edge recorded as s–t; the DP joined s and t on `t.order_id = o.id`,
-- the executor stripped the qualifier it could not find and bound `o.id` to
-- s.id: one row `1,2,3,3` where PostgreSQL 17.11 answers `1,2,1,1` and
-- `2,4,3,3`
```

The seam is not "the same table three times": the arc's seam table reaches it
with three DIFFERENT tables that share column names, with no per-arm filter at
all, and in a chain as well as a star — every INNER cell whose key names a
column the other relations also carry under another meaning (`a.oid = o.v`,
the issue's `s.order_id = o.id`). Where the shared name holds the SAME value in
every relation (`x.oid = o.oid` in a star) the mis-hung key happens to be
transitively equal and the answer is right by luck, which is why the issue
read as specific to per-arm filters.

**The rule.** A conjunct's edge is the SET of relations its column references
name: a qualified reference belongs to the one relation that publishes its
qualifier (`joinSidesScanInfo`'s scope names, byte-exact), a bare one to the
one relation that carries the column; the edge applies only at a join that
holds every one of them (`joinEdge.extra` carries a third and later relation,
so `c.x = a.y + b.z` is never a pair that leaves one to chance). Where a
reference does not resolve to exactly one relation, the column-name endpoints
stand, WIDENED by every relation that did resolve — a wider edge only delays a
conjunct to a join that certainly holds what it reads
(`logical/join_edge_members.go`).

**The net.** The single-process planner refuses a join whose condition
qualifies a column by a relation NEITHER of its sides answers to — any scan,
CTE reference or derived table below it, compared without case, so it can
refuse only a name that is nowhere
(`physical.refuseStrandedJoinQualifier`). With the edge rule in place no plan
reaches it; with the edge rule reverted, the issue's query refuses instead of
answering the wrong pairing.

The 22 TPC-H plans are byte-identical (their conjuncts name distinct columns,
so the column-name endpoints were already the qualifier's).

Gate: `coordinator.TestArcJPASelfJoinArmPairsTheRowsItsQualifierNamesOnEveryArm`
— 1 008 cells, {the same table ×2, ×3, ×4; three different tables sharing every
column name; mixed ×3, ×4} × {a per-arm ON filter on every arm, on some, on
none} × {IN, equality, range} × {INNER, LEFT, RIGHT, INNER/LEFT alternating} ×
{the key under a shared name with one meaning, under a shared name with a
different value per relation, under a name only the outer relation has} ×
{star, chain}, on five arms against live PostgreSQL 17.11: 40 wrong on the
single arm at base, 0 at the tip. The masking gate
`server.TestArcJPAReorderedAndReKeyedJoinsReadThePublishedValueOnEveryDoor`
reorders copies of e7bal and e7emp on all nine doors.

**Not settled (distributed).** An OUTER-join chain whose second arm is keyed
on the first, where the outer relation's key has a name no item relation has,
comes back on the stage DAG with the first arm's columns NULL on a row the
second arm pads (and dropped when every build is shuffled) — identical at base,
the logical plan right; pinned per arm in the gate, filed `distributed`.

### 8l. A block's name belongs to the block, and a minted slot is its own name (2026-09-24, arc JP round 2: #1302, #1299)

Round 1 made one key rule for bounded and unbounded LATERAL bodies and read
the key back by NAME wherever the body already published one. The review
found the rows that name then bound: the OUTER relation's column of the same
name. Four sites carried a name where an identity belonged, and each is now
guarded where it happens, not per spelling:

- **The lifted key owns a slot** (`logical.buildLateralSubquery`). A
  correlated equality whose outer side is an expression is evaluated above
  the join, over both sides' columns; its inner key is ALWAYS minted into a
  `__key_N` slot (§3a), never handed over under the body's own `k` or alias.
  A select item §3c respells to that slot keeps the name the query gave it.
- **A DISTINCT keeps its block's name** (`logical.rewriteDistinctAsGroupBy`).
  The rewrite returned the Project below the Distinct and dropped the
  derived / LATERAL alias, CTE name and LateralSubtree stamped on the root, so
  the join qualified the arm's duplicates by the scan's alias and `s.k` fell
  to the other arm's `k` — on every path, a plain derived table included.
- **An aliased table answers to its alias alone** (`Node.ScopeNames`), as in
  PostgreSQL: `FROM jp_i jp_j JOIN jp_j jp_i` made `jp_i` name both scans, and
  §8k's edge rule, finding two, fell back.
- **The stage DAG reads what a LATERAL's stream carries**
  (`physical.resolveRenameSource`, `logical.ResolveFilterThroughProjects`). A
  LATERAL arm is never materialized on the DAG, so its stream is the body's
  raw columns qualified by the scan's alias: a reference by the lateral's name
  resolves to the unaliased item publishing it (qualified by the body's one
  relation when written bare), and a filter over a slot an aggregate
  publishes (`GroupByPublish`) keeps the slot rather than its source column.
  Scoped to LATERAL roots: the generic form moved CTE and derived arms onto a
  wrong spelling (measured).

Gate: `coordinator.TestArcJPBLateralBodyNamesNeverBindTheOuterRelationOnEveryArm`
(954 cells, every body unaliased, five arms, PostgreSQL 17.11; 2 696
(cell, arm) fail at 6cbe2041 and 1 737 at round 1), the round-2 cells of the
nine-door masking gate, and the embedded cells of
`wadjet.TestArcJPAJoinArmKeyIsTheColumnTheQueryWrote`.

**Not settled (distributed).** A derived table (not a LATERAL) whose
colliding column the DAG reads through a filter over a cross join
(`(SELECT i.id, i.k FROM lt_i i) s … WHERE s.k = o.k + 1`) or through a window
(`row_number() OVER …` in the block), and two copies of one block whose scans
share an inner alias, still read the other arm's column on the DAG — the
generic resolution is the fix and it needs the arm's materialization state,
which the resolver does not see. It is a filing candidate with its cells.

**Round 3 (2026-09-24): the DAG half is a routing property, not a resolver
rule.** The review of round 2 found the DAG re-spell reading the outer
column through spellings the four sites above did not reach (a body naming
its relation by table or CTE name, `SELECT DISTINCT *`, a derived table or a
CTE inside the body). A correlated LATERAL now runs as stages only when no
non-minted name its arm carries across its join is carried by another
relation of the query and its join does not pad a grouped arm; every other one runs on the
coordinator's single-process pipeline (`dagplan.refuseCollidingLateral`,
ADR-0021 §1s). The resolver rules above stay for the plans the DAG carries.
The bare-key `LEFT JOIN LATERAL` over a `DISTINCT` body that failed ADR-0010's
schema check is one of the routed shapes and answers.

## §9 A derived block publishes its VISIBLE list, and a qualified star reads it

Added 2026-09-13 by arc O2 (#1077, #991, #1020).

§7 made a derived block a relation the DAG can publish. §9 says what that
relation IS on every path — its VISIBLE projection, under the names the block
wrote — and closes the three ways it was not.

**A slot the planner minted is dropped where the operator that minted it ends,
and a derived block had no such place.** An `ORDER BY` term the SELECT list
does not carry is materialized as a hidden projection and sorted on, so the
projection sits BELOW the sort. At the top of a statement the hidden tail is
trimmed where the rows leave the engine (`physical.hiddenSortTrimOp`, the
gather's output renames). A derived block has neither, and with the block as
the ONLY relation the statement's own output projection IS the block's — which
is why this was invisible until a JOIN stood above it:

```
SELECT * FROM lat_ord o JOIN (SELECT order_id, product FROM lat_item
                              ORDER BY amount LIMIT 3) s ON s.order_id = o.id
PostgreSQL   id, customer, total, order_id, product
wadjet       … , __sortkey_0          ← a name no query can spell, on the wire
```

`logical.dropBlockHiddenSlots` re-projects the block to its visible list ABOVE
its own Sort and LIMIT — the block's own projection re-applied, same names,
same order, one reference per item.

**WHERE IT RUNS IS THE RULE, and the first statement of it was wrong.** It said
"in `NewJoin`, because a JOIN is the one consumer that reads a side's STREAM",
and a SET-OPERATION arm reads the same stream: `SELECT * FROM (<sorted block>)
x UNION ALL SELECT …` published `__sortkey_0` beside the block's own columns on
the single-process arms and in `RowDescription`; the two arms then disagreed on
column COUNT, so all three DAG arms REFUSED a query PostgreSQL answers; and
`INTERSECT` answered ZERO rows for PostgreSQL's three, silently (#1075). A
`CREATE TABLE AS` over the same statement refused naming an internal slot,
which is the DDL door's reserved-name guard standing between the leak and a
stored column no query can spell.

The rule that is true: **a relation-COMBINING operator builds its output from
its sides' STREAMS, so each side publishes its visible list there.** Every
UNARY operator — Filter, Project, Aggregate, Sort, Limit, Distinct, Window —
passes a relation through or names its own columns, and the materialized tail
then survives only as far as the statement's own output trim. A BINARY one
composes a new relation out of what its sides emit, and there the tail becomes
a column of the answer.

**AND THE CLASS IS EVERY WIRING OF SUCH A NODE, NOT ITS FOUR CONSTRUCTORS.**
The first statement of this said `NewJoin`, `NewUnion`, `NewIntersect` and
`NewExcept` were the whole class because there is no fifth binary node TYPE.
That is true of the type and false of the wiring: the decorrelation of `IN`,
`NOT IN`, `EXISTS` and a correlated scalar subquery each builds a `NodeJoin`
LITERALLY, with a nil probe child the caller fills in afterwards, so the
constructor never saw the side — and a sorted derived block under any of them
still published `__sortkey_0` on five arms and in `RowDescription` (#1080). A
type is not a door.

`setCombinedChild` is the door: the constructors call it, the four literal
constructions call it, and
`logical.TestEveryRelationCombiningSideIsWiredThroughOneDoor` walks BUILT AND
OPTIMIZED plans asserting the property rather than the plumbing — for every
join and set-operation node, re-applying the rule to a side must be a no-op.
Three of the four sites are optimizer rules, so a gate over the builder's
output alone would have passed with the defect in place; the gate runs
`Optimize` for exactly that reason, and each of the three reverts fails it
(1, 4 and 2 subtests).

The BLOCK-level placement was measured and is not better: applying it at the
block's own construction puts a Project between the SELECT list and the
producer that would materialize it, and the DAG then computes no SELECT list
for the shape at all (`physical.TestDerivedTableAliasSortKeyNamesAnEmittedColumn`
said so). A Project above a block already drops the tail by naming its own
columns, so the re-projection there is redundant as well as harmful.

**DEPTH IS NOT A BOUNDARY THE RULE HAS.** `blockSortCarriesAMaterializedKey`
walked from the block's projection through Sort, Limit and Distinct and stopped
at the first Project, so a block whose SORTED block is one level deeper was not
marked, the DAG's publication took the narrowing exemption, and the star read
the stream: six columns where the single-process arms and PostgreSQL publish
five (#1076). A projection that carries no hidden column of its own
re-publishes what is under it, so the walk continues through it; one that does
IS the block the question is about.

Two consumers had to be told, and each was a wrong answer:

- A Project emits no stage, so on the DAG the re-projection alone left the star
  reading the stream and the sort key's SOURCE column rode out in the slot's
  place. `blockProjectionLeavesItsStream` exempts a block that publishes FEWER
  columns than its stream because the column pruning answers that — and pruning
  cannot remove a column the sort BELOW reads. That narrowing is marked now.
- `resolveShuffleKey` stops at a projection that publishes a MINTED group key,
  because such a key exists on the DAG only under its slot (§3c). The stop was
  reachable only from a QUALIFIED spelling and only from the Project directly
  over the aggregate; the re-projection makes the key bare one operator higher
  and puts a Project in between, after which the walk chased the key to its
  SOURCE column — `partitioned shuffle: key "order_id" not in schema`. It reads
  a bare spelling as itself now and sees through a Project that republishes the
  name unchanged.

**The MINTED slot's ordinal is read through the block's own sort (#1020).** A
join drops the slot its lowering minted BY POSITION, and on the DAG that
position comes from the list the STAGE publishes (§7 item 3).
`materializedBlockUnder` stopped at a Sort, so a grouped LATERAL carrying its
own `ORDER BY` fell through to `declaredJoinSchema`, which descends to the
AGGREGATE and answers its order (`product, __key_0`) where the stage publishes
the projection's (`__key_0, p`): the slot was looked for at the wrong ordinal,
nothing was dropped, and `__key_0` reached the client on both DAG arms. A Sort
changes neither the columns nor their order, so the block beneath it is still
the relation that side publishes, and the walk passes it exactly as it already
passes a Filter, a Limit and a Distinct.

**AN AGGREGATE OUTPUT OCCUPIES ITS NAME BEFORE A KEY'S QUALIFIER IS STRIPPED
(#1078).** §3a is an aggregate output taking a group key's name and §3c is the
reverse; this is the third face of the same collision, and it is inside
`exec.PublishedGroupKeyNames` itself. That function keeps a key's table
qualifier when stripping it would make two columns of the operator's own output
share one name — and it counted only KEYS. An aggregate emits its keys and its
aggregates into ONE batch, so `SELECT COUNT(*) AS product … GROUP BY i.product`
published TWO columns called `product`, and every consumer above read the FIRST
through `batch.RecordBatch.ColumnIndex`: a derived or lateral block over that
aggregate answered product NAMES on the three DAG arms where PostgreSQL and the
single-process arms answer a count. An aggregate output's name is one the
planner DECIDED, so it occupies the name and the key's qualifier is what there
is to give up. ONE rule, read by both engines: the operator asks it of its own
`Aggs`, the worker's fragment plan of `spec.Aggregates`, and the planner's
model of the aggregate node's `AggExprs` or the stage's `AggSpecs` — threaded
through every `stageEmittedKeyNames` call site, because a copy of the rule in
the planner is how the two would drift (§2b).

**A QUALIFIED star binds the block's RESOLUTION spelling and publishes its
PUBLISHED one (#1077).** §2's pair, in the direction a star needs. An unaliased
select item publishes `?column?` — a RENDERING, which no stream carries — and
resolves by the spelling `buildProject` emits it under, its expression text.
`logical.projectionOutputNames` answered the published name alone, so
`SELECT x.* FROM (SELECT id, g + 1 FROM shp) x` referenced a column nothing
carries: NULL under a STRING declaration on every arm, where the bare star
answered the value. `StarSourceColumns` answers PAIRS now, and the expansion
carries both.

The same walk decides what a star can REACH, and it reached almost nothing: it
stopped at the block's root, so a block carrying its own `ORDER BY`, `LIMIT` or
`DISTINCT` answered nil and the query was REFUSED where PostgreSQL answers it,
and a decorrelated LATERAL was excluded outright. It descends through the
operators that change neither a column nor its position, and a LATERAL body
publishes its list minus the slots the join above OWES — `Node.HiddenJoinCols`,
the same identity the drop itself uses. The ADR-0012 divergence that recorded
the lateral-star refusal is deleted with it.

**TWO PUBLISHED COLUMNS OF ONE NAME ARE NOT ENUMERABLE BY NAME (#1076).** A
duplicate output name is legal SQL and PostgreSQL answers it positionally. This
expansion emits one column REFERENCE per published column, and two references
spelled alike both bind the first column of that name: `SELECT x.* FROM (SELECT
a.id, b.id …) x` published the first `id` twice, and `(SELECT order_id AS k,
amount AS k …)` published a wrong TYPE with it. The list is answered nil and
the star is REFUSED — the same direction this pass already takes for an item
with no name at all — while the BARE star over the same block, which reads the
relation by POSITION, is untouched and right. Closing it means the block's
published list travelling by position, `ProjectExprSpec.SourceSlot` one
relation out, and it is recorded in ADR-0012 until it does.

*(2026-09-23, arc LT: superseded by ADR-0021 §1s — the bound is applied per
outer row, `Node.LateralBoundNotPerRow` and the qualified star's decline on it
are deleted, and the two `qstar` refusals assert PostgreSQL's rows.)*

**A BOUND INSIDE A CORRELATED LATERAL IS NOT APPLIED PER OUTER ROW (#1019), AND
THE ONLY CONSUMER THAT DECLINES IS THE ONE THAT CANNOT STATE THE RELATION.**
PostgreSQL evaluates a LATERAL body once per OUTER ROW, so its `LIMIT` bounds
each evaluation; the decorrelation makes the body one relation joined once and
the bound applies to the whole of it — three rows where PostgreSQL answers
four. Honouring it means the bound travelling WITH the correlation key as a
per-key top-N (ADR-0021's territory).

**A REFUSAL ON THE BOUND'S EXISTENCE WAS WRONG, and it was measured wrong.**
Whether a bound BINDS is a property of the DATA: `… ORDER BY p LIMIT 10` over a
body that never yields ten rows for one outer key answers PostgreSQL's rows
either way, and so does `OFFSET 0` — both were RIGHT on five arms and a
plan-time refusal on "the body has a bound" turned them into errors. A new
refusal on a shape that answered correctly is a regression, whatever it stands
in for.

The mark is on the BODY (`Node.LateralBoundNotPerRow`) and only the QUALIFIED
STAR declines on it, because a star publishes a RELATION and this body's ROW
COUNT is not the one the query wrote — the same "a list this pass cannot
state" rule the duplicate published name takes, one property over. `SELECT *`
and an explicit list keep the disposition they had, with the row count PINNED
and PostgreSQL's answer recorded beside it. `OFFSET 0` and `LIMIT ALL` are not
marked at all: they cannot remove a row, so the two forms agree by
construction. An UNCORRELATED lateral is untouched.

**What a star must never widen is the POLICED list**, and that is unchanged:
what the expansion publishes is the block's own SELECT list, which the column
policy already vetted, so a masked column arrives masked and a denied one is
still `42703`. `server.TestPolicyMaskingIsPlanTimeOnEveryDoor`'s three
`a_laterals_own_star_*` cells assert exactly that, on every door.

### Gates

| gate | what it holds |
|---|---|
| `logical.TestEveryRelationCombiningSideIsWiredThroughOneDoor` | §9's class — every wiring of a join or set-op node, over built AND optimized plans |
| `coordinator.TestArcO2ADerivedBlockPublishesItsVisibleList` | §9 — 409 cells on five arms against PostgreSQL 17.11: five block classes × twelve projection shapes × five consumers, the four SET OPERATIONS and the five DECORRELATED semi/anti spellings over a sorted block, nesting depth 2 and 3, the duplicate published name, and the five issues in the spelling each was reported in; the reserved-name property tolerates nothing, and a REFUSAL is a recorded disposition rather than a pin |
| `pgwire.TestArcO2TheWireUnderACombiningOperator` | §9 on the wire door — the four set operations, the five decorrelated spellings, the CTE spelling, both arms sorted, and two controls |
| `worker.TestFragmentResolvesAndPublishesTheTwoNames` | §9's aggregate-output rule, asserted where the two engines have to agree |
| `pgwire.TestArcK3TheWireDeclaresTheBlocksProjection` | §9 on the wire door — the two `__sortkey_0` pins are deleted |
| `server.TestPolicyMaskingIsPlanTimeOnEveryDoor` | §9's policed half — a lateral's own star publishes the body's list, masked stays masked, denied stays 42703 |
| `coordinator.TestM1AMergedOrderIsTheQuerysOrder` | 8a — ten shapes, four arms, incl. LIMIT (top-K heap), OFFSET, a zero-row result, the arm-swapping predicate, and two controls that never reach the merge |
| `coordinator.TestM1AMergedOrderIsTheQuerysOrderAtScale` | 8a past ONE BATCH — seven 5000-row shapes, the two DAG arms asserted row for row against the single-process one |
| `coordinator.TestCoalescingAMergeAdvancesAVarlenNullsOffset` + `TestCoalescingAdvancesEveryNullsOffset` | 8a's null-write rule, per carrier |
| `coordinator.TestTheMergeRefusesAnOrderingItCannotApply` | 8a's two refusals through both comparators, their SQLSTATE, and the empty boundary |
| `physical.TestFuseJoinStagesDeclinesAJoinWhoseRulesTheSpecCannotCarry` | 8c's other half — a spec that cannot carry a rule does not absorb the join |
| `coordinator.TestM1ASetOperationsArmsSupplyItsResultColumns` | 8b — twelve shapes, all four set-op spellings, three controls holding the explicit-list boundary |
| `coordinator.TestM1AEveryLateralJoinDropsItsOwnSlot` | 8c — ten shapes incl. three and nested laterals, same and different tables, derived and CTE stars |
| `pgwire.TestArcJ1AHiddenSlotIsNotInTheRowDescription` | 8c on the wire door |
| `coordinator.TestM1AGatherRenameBindsWhatItsSourceNames` | 8d — seven shapes, two controls |
| `coordinator.TestArcE3` (#785 cells) | 8d's boundary: the rescan's own case is unchanged |
| `coordinator.TestN1AGroupedLateralAnswersItsRows` | 8e's VALUES — a lateral whose inner GROUPS, seven shapes incl. LEFT, nested and different tables |
| `coordinator.TestN1ATwoGroupedLateralsPublishTheirOwnColumns` | 8e's COLUMN LIST and its order, with three controls (an ordinary two- and three-way join, #988's ungrouped laterals) |
| `coordinator.TestN1AnOrdinalSortKeyBindsItsSlot` | 8f — eight cells, both keys DESC in turn, the ordinals swapped, 5000 rows, two controls |
| `coordinator.TestC3AWrittenSortKeyBindsItsOwnColumn` | 8g — thirteen cells on five arms: the written term leading, alone, DESC, beside an ordinal, both keys written, the CTE spelling, the GROUP BY twin in both spellings, 5000 rows under a LIMIT, ADR-0012's ambiguous-name divergence, and the bare cross join the measurement must decline |
| `coordinator.TestC3ASetOperationOrdinalsBindTheirOwnSlots` | 8h — twenty-five cells on five arms: UNION / UNION ALL / INTERSECT / EXCEPT × four orderings × a duplicated-name list, a star, the ALL spellings, a THREE-ARM chain and a 5000-row pair, plus the row COUNTS, which is the half with no sort key in it |
| `coordinator.TestC3ANullGroupKeyIsItsOwnGroupOnEveryArm` | a NULL group key is its own group on every arm — the NULL-arm UNION over every flat type, eight repetitions per arm, with the morsel-parallel engagement counter asserted (#1058) |
| `coordinator.TestN1AResultWithNoColumnsIsRefused` | an empty column list is never an answer (ADR-0012's divergence list) |

### Not settled

CLOSED 2026-09-13 by arc O1 (§9): the star's column ORDER and its qualified
side were the PLAN's, from THREE producers of one class — `reorderJoins`
swapping an ordinary inner join's sides, `costBasedJoinReorder` rebuilding a
longer chain, and `markCoPathingSelfJoinBuilds` deciding
`Stage.QualifyAllBuildCols` from the ARM's stage DAG (so the two DAG arms
published different names for one statement). None of them decides a published
name any more, because none of them is asked: the star's list is read off the
FROM clause before any of them runs.

What is still open in this family: two shapes declare NO columns when they
return no rows (a star over a bushy join, a star over a query carrying a
decorrelated LATERAL); ADR-0012's divergence list records that they are
refused rather than answered until the declaration reaches them.

§8f left one of its own — a WRITTEN qualified ORDER BY term beside a duplicate
output name bound the first column of the name it was rewritten onto — and
**§8g settles it** (2026-09-12, #1014): both engines resolve a written term's
slot through `sortKeyWrittenSlotPos`, the DAG under §8f's own measurement. The
pin this paragraph used to name in
`coordinator.TestN1AnOrdinalSortKeyBindsItsSlot` is deleted; the family's gate
is `coordinator.TestC3AWrittenSortKeyBindsItsOwnColumn`.

What is still open in that family is the DERIVED-TABLE spelling, and it is a
different bound: `SELECT * FROM (SELECT a.order_id AS amount, b.amount FROM
lat_item a JOIN lat_item b ON b.order_id = a.order_id) x ORDER BY 1, 2 DESC`
answers PostgreSQL's sequence on the single-process arms and not on the three
distributed ones. `producerPublishesSelectList` compares each visible item's
SOURCE EXPRESSION against the producing stage's `ProjectExprs`, and through a
derived table's RENAME those differ by construction — the outer item reads
`x.amount` where the stage computes it from `a.order_id` — so the measurement
declines and the key falls back to a name the block publishes twice. Teaching
it to follow a derived block's rename is the star-identity territory the
paragraph above names, not a bound §8g can move at its seam.


### Fixed ROW consumers and computed aliases (A3b)

The complete column declaration, including fixed ROW fields, crosses every
projection and materialization boundary (`declTypeParts`). A projected stream
publishes a new column set: a further projection materializes above it through
`StageProject`, instead of overwriting its producer. Derived ROW field parents
bind through the aggregate rename resolver; set-operation outputs publish their
resolved child fields to that same declaration walk.

This also makes the two-computed-alias #807 control execute without the former
local route and repairs #851's computed alias values when an outer SELECT reads
an alias that shadows a stored column. The old value pins now assert PostgreSQL's
values. An outer SELECT without ORDER BY compares a multiset (ADR-0013 class 1);
an explicitly ordered twin checks the sequence. The gather-owned wrapped-window
refusal remains unchanged. See the A3b position corpus and alias-key controls.

An expression already published as a GROUP BY key remains a reference:
`aggregateGroupKeyName` checks its identity before alias materialization.
Recomputing it above an aggregate would read arguments that the stream no longer
contains. ROW parent rewrites similarly stop at a join's published identities;
they may resolve a rename owned by the current unary scope, not another arm's.

## §9 A star over a join is the FROM clause's arms, in written order

Added 2026-09-13 by arc O1 (#997, #1012, #993).

§2 gave a key two names; §6 generalized it to every consumer; §7 gave the
consumer with no name of its own — a star — a relation to read. §9 is the same
rule for the relation a star reads when that relation is a JOIN.

**The rule.** `SELECT *` over a join publishes every FROM arm's own output
list, LEFT ARM FIRST in the clause's written order, duplicate names kept BY
POSITION and never qualified. It is a property of the QUERY. Nothing in the
plan decides it: not which side builds, not which arm the cost model puts
first, not which arm a particular stage DAG happens to build over.

Three producers used to decide it, and all three are the same defect — a cost
decision changing what a statement MEANS:

| producer | what it decided | measured |
|---|---|---|
| `logical.reorderJoins` | swapped a two-relation chain's children by estimated rows, so the probe side's columns came first and the build side's were qualified | `SELECT * FROM lat_ord o JOIN lat_item i …` published `i`'s four columns then `o`'s three (#1012); `SELECT * FROM t a JOIN t b ON a.id = b.id` qualified `b` with no predicate and `a` under `WHERE a.id < 100` (#997) |
| `logical.costBasedJoinReorder` | REBUILT a three-or-more chain, so there is no swap to record | the three-relation shapes, on every arm |
| `physical.markCoPathingSelfJoinBuilds` | set `Stage.QualifyAllBuildCols` from the ARM's stage DAG | `dagshuf` published `o.customer, o.total` where `single`, `spilled` and `dag` published them bare (#993) |

**Where the list is recorded.** In the star's own expansion, at Optimize step
1, before any of the three runs. `logical.joinStarColumns` walks the join chain
in written order and asks each arm what it publishes — the SAME two list
functions a qualified star already asks (`projectionOutputNames` for a named
block, `StarSourceColumns` for a base relation, so an ABAC security projection
is what publishes where one stands). Each column becomes a QUALIFIED reference
published under the column's own name, which is what makes the item
plan-independent: `expr.ResolveColumnRef` binds `b.id` exactly when the join
qualified b's side and through the qualifier-stripping fallback when it
qualified a's instead. The join operator's qualification is thereby demoted to
what §2 calls a RESOLUTION spelling, and the published name is the column's
own.

**Why not a build-side mark on the join node.** Both filings proposed it. It
cannot express the multi-way case: `reorderJoins` swaps only a two-relation
chain, and `costBasedJoinReorder` rebuilds the tree for three or more, so the
written FROM order is not a permutation of one node's children but a SHAPE that
no longer exists in the plan. A mark has nothing to be relative to. The order
has to be recorded where it is still written down.

**The projection is minted on a HYPOTHESIS.** `isStarOnly` builds no Project
for a bare star, on the claim that the star selects its input unchanged — true
for a scan, false for a join. `BuildFromSelect` mints one when the FROM is a
join, on SHAPE alone (no scan is annotated yet), and `ElideUnstatedJoinStar`
takes it back out when the expansion could not state the arms, restoring the
tree the builder would have built — including the DerivedAlias / CTEName /
CTERefAlias and the WITH list the enclosing query stamped on a block's root.
Without the elision a shape whose arms cannot be enumerated would be REFUSED
where it used to answer.

**The projection sits ABOVE the ORDER BY and the LIMIT**, because it is an
OUTPUT permutation. The Sort therefore reads the FROM clause's own namespace,
where `i.id` names one column, and every sort-key resolver below it sees the
tree it saw before: §6a's rule (the term keeps the qualifier the query wrote)
and §8f/§8g's (an ordinal and a written term bind the slot the producer
published) are untouched. Placed BELOW the sort instead — measured — a written
`i.id` resolved against the star's own published list through
`derivedAliasSourceColumn`, bound the FIRST `id`, and the broadcast arms tied
every row. The one term that addresses the OUTPUT list is a POSITIONAL one, and
`ResolveStarJoinOrdinalSortKeys` answers it from the expanded list in the
item's SOURCE spelling; `ORDER BY 4` over a star join was refused with 42P10
before this arc, for a statement PostgreSQL answers.

**A STAR ITEM IS A (RESOLVE, PUBLISH) PAIR, and that is the invariant.** §8j
generalizes exactly this pair to the window key, the sort key, the lifted
predicate column and the join-arm reference, whose resolve half names an
OCCURRENCE (SETTLED 2026-09-18, arc WK). The
expanded list carries NAMES — `(FROM-clause relation, column name)` — resolved
later against whatever tree the optimizer ends up with, not positions in the
step-1 tree and not stable column handles. Measured (round-2 review), the pair
survives every rule that restructures the tree: the two-way swap, the
`costBasedJoinReorder` rebuild, predicate pushdown, forced estimates, a forced
build side, a decorrelation that ADDS a join, CTE inlining, one CTE referenced
twice, and a renaming block arm. It is stable because the resolve spelling is
the PRODUCER's: `expr.ResolveColumnRef` binds `b.id` exactly when the join
qualified b's side and through the qualifier-stripping fallback when it
qualified a's, so no later rule can permute what the item means.

It broke in exactly ONE direction — where an arm's PUBLISHED name is not the
name its producer EMITS, which is §2's pair of names arriving at a star:
`(SELECT order_id, COUNT(*) …)` publishes `count` (PostgreSQL's FigureColname)
and emits `count(*)`, so an item spelled `s.count` bound nothing and read NULL
under the STRING default. **CLOSED by carrying the pair**: a star item is a
`StarColumn{Resolve, Publish}` — it references the producer's spelling and
publishes PostgreSQL's name — which is the same seam a QUALIFIED star uses
(#1077, arc O2), so one model serves both spellings. An unaliased aggregate,
expression, literal or CAST in an arm now answers PostgreSQL's value under
PostgreSQL's name and OID on all five arms and on the wire.

**What declines, and why the boundary is there.** Each of these keeps the
answer it had — the plan's order under the producer's names — and a PARTIAL
expansion is never returned:

| declines | why |
|---|---|
| an arm that RESOLVES two items to one name | the reference `s.id` binds the first, so the second column would carry the first's VALUES. Two items that merely PUBLISH one name are fine — each still references its own producer spelling, and PostgreSQL publishes duplicates too |
| two arms of one name | both expand to the same qualified reference |
| a LATERAL arm, or a manufactured lateral's join | the subtree carries the correlation slot the join drops (§3c) |
| a SEMI/ANTI join | it publishes its probe alone; no star spells one |
| a table function | no catalog annotation to publish from |
| an Aggregate, Window or set operation BETWEEN the star and the join | the emitted columns are that operator's |
| an arm whose own list this pass cannot state | an aggregate or a window whose projection was elided |

A SET OPERATION is no longer one of them: it publishes its leftmost arm's list
and its own stage emits exactly that list, so once the arm is named by the
query (§8i item 1) the expansion's reference is an address on every arm
(#1102, arc R2).

A block's ROOT is not one of them. `(SELECT … ORDER BY … LIMIT 2) a`,
`(SELECT DISTINCT …) a` and a `GROUP BY` block all publish their own
projection's list, reached through the nodes that pass their input's columns
through unchanged (`blockOwnProjection`) — stopping at the root instead was
#997's divergence surviving one node above where the first pass looked for it
(round-2 review, P1).

Nor is a block's own RE-PROJECTION. A block that materialized an ORDER BY term
of its own is wrapped in a Project of its visible list above its Sort and
LIMIT, so the minted key dies with the sort (§9's `dropBlockHiddenSlots`,
#991), and that wrapper carries NO alias — the name is the block's, one node
down. The list comes from the wrapper, because it is what the block publishes;
the NAME comes from the block under it (`blockRelationName`). Reading only the
root made every such arm unstatable, which put the plan's order back on exactly
the shapes #991 had just repaired — the two rules compose or neither holds, and
`coordinator.TestArcK3ADerivedBlockPublishesItsOwnProjection`'s
`sortkey/introducing-block-keeps-its-column` is where that is measured.

The design, with every measurement, is
`docs/internals/bare-star-over-a-join-arms.md`.

### §9a. A star publishes a name both arms carry, and a name one arm carries TWICE is not addressable (2026-09-18, arc SR: #1177, #1094, #1079)

§9 above gives a bare star over a join its arms' own lists and §9 (arc O2)
gives a derived block its visible list. Three of the boundaries those two arcs
drew were drawn on the wrong property, and each is one sentence of the same
rule: **a name two RELATIONS publish is publishable twice; a name ONE relation
publishes twice is not addressable at all.**

1. **A `JOIN … USING` star publishes a shared tail name TWICE (#1177).**
   `usingJoinStarColumns` refused the statement (0A000) whenever the two arms
   shared a column name outside the USING list, on the claim that a qualified
   reference to such a name "binds one of them wherever the plan put it" —
   #706 read through a star. Measured at `563aa517` over zzp/zzj, whose two
   arms share BOTH names and declare `d92` at DECIMAL(9,2) and DECIMAL(18,4)
   with values that differ per row, the claim is FALSE: the same pair spelled
   with `ON` answers PostgreSQL 17.11's values AND both declarations on five
   arms, in either FROM order, under a filter, through a LEFT join and in a
   three-way chain. The expansion emits `a.d92` and `b.d92` and
   `expr.ResolveColumnRef` binds each exactly where the join qualified that
   side and through the qualifier strip where it qualified the other — §9's
   own composition, which is why it holds. The decline refused a statement
   PostgreSQL answers over a premise its own `ON` spelling disproves. **A
   deferral is a claim and it is measured like one.**

   What is still declined is the property that really is unaddressable: an arm
   that publishes ONE name twice (below), a CHAIN of USING merges, and an arm
   whose own list this pass cannot state.

2. **A REFERENCE into a block that publishes one name twice is 42702 (#1094).**
   O2 made the qualified STAR over such a block decline (c8d94fe3) and left
   the explicit list answering the FIRST column on all five arms, which is a
   value chosen by the block's item order and never disclosed. That was the
   WRONG-VALUE half. `colScope.srcCount` cannot say it — it counts SOURCES,
   and this is one source counted twice — and neither can a per-(qualifier,
   column) COUNT, because `quals` is keyed on the FOLDED qualifier and two
   DIFFERENT relations may fold to one key. So the duplicate is decided WITHIN
   one source's own list (`noteSourceDuplicates`, once per source) and only the
   VERDICT is recorded, in `colScope.dupQualified`; `resolveRef`'s qualified
   branch refuses on it. The message names the COLUMN, because the qualifier names
   exactly one relation. The BARE star over the same block is untouched: it
   reads the relation by POSITION and answers both columns, which is
   PostgreSQL's answer.

   The qualified star's own decline is NOT closed and the boundary is the same
   one O1 and O2 each measured: every expanded star item is a qualified
   reference, so publishing the second column means the block's list
   travelling by POSITION (`ProjectExprSpec.SourceSlot` one relation out).
   That is a slot-identity change, not a star one. Loud beats plausible, and
   the refusal is recorded in ADR-0012 with PostgreSQL's answer beside it.

3. **A SET OPERATION publishes its LEFTMOST arm's names (#1079).** §8b said
   the arms supply the operation's result COLUMNS; the PUBLISHED half of those
   names was applied by nobody. Both places a query's values leave the engine
   — the collecting sink and the gather's rename — are reached through the
   statement's OUTPUT PROJECTION, and a set-op root has none, so the operation
   went out under the arm's RESOLUTION spelling: `total + 1` for PostgreSQL's
   `?column?`, `count(*)` for `count`, `cast(total as varchar)` for `total`,
   on five arms and in `RowDescription`. The derived-table and CTE spellings of
   the same statement were already right, which is how it survived twelve
   releases: only a set operation reaches that node.

   `publishedOutputProjectionNode` answers "whose names does the CLIENT read"
   and passes a set operation to its leftmost arm.
   `findOutputProjectionNode` keeps its own answer for every consumer that
   asks where the pipeline's output projection IS — the gather's rename
   target, the distinct dedup, the stage projection — because a set operation
   has none and its arms each have one. The DAG is told through a gather
   rename built from the SAME node (`dagplan.setOpPublishedRenames`): names
   only, one per visible item, emitted only where the two names differ, since
   a copy of the rule in one engine is how the two would drift (§2b).

   The QUALIFIED star over such a block was 42703 for the same fact one layer
   over — `relationOutputColumns` read the block through
   `blockOutputProjection`, which does not descend a set operation, while the
   BARE star's own walk (`blockOwnProjection`) does. One walk answers both
   spellings now.

**What this section did NOT move, measured.** A star over a LATERAL arm is
still not expanded (§9's decline list: the subtree carries the correlation
slot the join drops), so it reads the join operator's stream and publishes the
duplicate `id` under whichever alias that operator qualified — `l.id` on the
two single-process arms, `i.id` on the three DAG ones, where PostgreSQL
publishes `id` (#1126's two published spellings, ADR-0012's list, pinned per
arm). A star over a GROUP BY of a join publishes the keys' own qualified
spellings (`a.id`) where PostgreSQL publishes `id`, which is §2b's
`PublishedGroupKeyNames` rule and not a star's. A positional `ORDER BY` over a
FULL `JOIN … USING`'s merged key is refused because the merged value is MINTED
by the projection the star expands into, which sits above the Sort. Each is
loud or names-only, and each is recorded in ADR-0012.

### Gates

| gate | what it holds |
|---|---|
| `coordinator.TestSRAStarPublishesItsArmsOwnColumns` | §9a — 48 shapes on five arms against PostgreSQL 17.11: the star forms × the join and block classes × arms sharing 0/1/2 names × value, name, declared (p,s), order and the zero-row declaration. zzp/zzj is the discriminating pair; psa/psb the control |
| `pgwire.TestSRTheWireDeclaresAStarsOwnArms` | §9a on the wire — RowDescription NAMES and type OIDs, in BOTH result formats |
| `server.TestArcSRAStarOverAPolicedArmNeverPublishesTheOtherArmsValue` | §9a's masking class on all nine doors: a star that publishes one name twice where one of the two is POLICED, the set operation's gather rename over a policed relation, and the 42702 refusal as a read |
| `coordinator.TestWKASeamConsumerBindsItsOwnOccurrence` | §8j's rule: the name-ownership seam enumerated ONCE — {window PARTITION BY, window ORDER BY, window ARGUMENT, sort key, join-arm reference, star} × {base scan, derived block, LATERAL, set operation, grouped block, nested block} + {lifted predicate} × {LATERAL} × three spellings, plus the MIRROR spelling that keys the window on the OUTER occurrence and the expression key whose two leaves name two occurrences, on five arms against live PostgreSQL 17.11. 50 cells, 250 (cell, arm) results; the mirror is what makes the table FAIL at `aed447e3` — a corpus keyed only on the arm the plan publishes bare answers correctly by luck there |
| `pgwire.TestWKTheWireDeclaresTheSeamsOwnColumns` | §8j on the wire: RowDescription NAMES and type OIDs for the same consumers, the window's own declaration, and the reserved-name property |
| `server.TestArcWKAReboundKeyOverAPolicedColumnReadsTheMask` | §8j's masking class on all nine doors: a window or sort key over a MASKED column has one partition under the mask and eight singletons under the stored values; the mask's answer, no stored policed value anywhere, and a non-vacuous (cell, door) count |
| `coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan` | the seam: 74 shapes — inner / left / right / full / cross / comma / self / three-way / derived block / CTE × no, selective and zero-row predicates × both FROM orders × `*`, `t.*`, `*` beside an item × no sort, a written key, a positional key, DISTINCT, LIMIT × a derived arm's ROOT (Sort, LIMIT, DISTINCT, GROUP BY, set operation) × its ITEM KIND (aliased, unaliased expression, aggregate, literal, CAST) — on FIVE arms against PostgreSQL 17.11 |
| `pgwire.TestO1TheWireDeclaresAStarJoinsOwnArms` | the same rule on the wire: RowDescription names AND type OIDs, including the three #997 predicates and the zero-row declaration |
| `logical.TestABareStarOverAJoinExpandsToTheFromClausesArms` | the list per shape, and that every item keeps its qualifier |
| `logical.TestABareStarOverAJoinDeclinesWhatItCannotState` | the declines above |
| `logical.TestAnUnstatedStarProjectionIsTakenBackOut` | the hypothesis, and the naming that travels back with it |
| `logical.TestAPositionalSortKeyOverAStarJoinBindsItsItemsSource` | the ordinal, in the input's spelling |
