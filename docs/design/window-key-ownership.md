# Window key ownership: one identity, one published name

Design memo for arc WK. It settles the seam ADR-0026 §8j records as NOT
SETTLED — *which arm owns a window key* — and states the one rule the next
change implements.

ADR-0026 §8j exists because three bounded repairs were written on this seam and
measured back out. Each fixed one of the three mechanisms that answer the
question and broke another. This memo enumerates the three by measurement,
states the rule that subsumes them, gives the mechanism on both engines, names
the cells the change must move and the gates it must keep, and records what is
deferred with the measurement that says so.

Every number below names the log it came from, under
`tooling/arcs/wk_window_key_seam/wk_author/`, measured at
`aed447e31adfa4e6f116c3016022a77d0a3401e9`. The five execution arms are
`single`, `spilled512k`, `dag`, `dag-shuffled`, `dag-morsel4`.

The engine's answer is the deliverable. A divergence that is right on the
single arm and wrong only on a DAG arm is recorded per arm with its mechanism,
not chased here.

---

## (a) The three mechanisms, by measurement

`physical.resolveWindowKeys` (`internal/planner/physical/window_keys.go`)
decides, at plan time, the name each PARTITION BY term, window ORDER BY term
and function argument reaches the operator under. It decides it ONCE for both
engines — `windowExecColumn` is called by `physical.buildWindow` for the local
operator and by `dagplan`'s stage emission for the spec the DAG ships — so the
two paths cannot come to different conclusions about what a key *names*. They
routinely come to different conclusions about what that name *binds*, which is
the seam.

### Which route a key takes, and what chooses it

Measured with a throwaway probe over hand-built plans
(`probe_mechanism.log`): the route is chosen by whether `inputColTypes` can see
through the window's input.

| the window's input | `inputColTypes` answers | `PARTITION BY o.id` / `x.w` resolves to | route |
|---|---|---|---|
| two BASE-SCAN arms (`lat_ord o JOIN lat_item i`) | `[amount customer id order_id product total]` — both arms merged into ONE map keyed by the BARE name | `id` | **M1, the bare-name bind** |
| two DERIVED arms (`(…) x JOIN (…) y`, both publishing `w`) | `[]` — the walk has no `NodeProject` case | `x.w` | **M3, the qualified name** |
| an EXPRESSION term (`o.id + 0`), any input | irrelevant | `__winkey_0` | **M2, the materialized slot** |
| ONE relation (`lat_item p`) | `[amount id order_id]` | `order_id` | M1, and right — there is one arm |

**A correction this memo makes to ADR-0026 §8j.** §8j and the header of
`coordinator.TestArcK1AWindowPartitionKeyBindsItsOwnArm` both say the bind
"reads `inputColTypes`, which declines a JOIN outright — so over the one input
shape where two columns DO answer to a bare name, nothing ran." That is true of
a join of DERIVED arms and **false** of a join of BASE SCANS.
`declared_output.go`'s `NodeJoin` arm merges both sides into one map and keeps a
duplicate whose two sides agree on TYPE; it drops only a name the two sides
type differently. `o.id` is not a key of that map, the bare `id` is, and
`bindWindowColRef` therefore answers `id`. **The fold is the merge**, and the
merge is why one written key takes two different mechanisms depending on
whether a Project stands between the window and the scans.

### M1 — the bare-name bind: the identity ERASED

Reached whenever the key is a QUALIFIED reference and the window's input types
resolve — every arm a scan, or a scan under Filter/Sort/Limit/Distinct/Window.
`bindWindowColRef` fails the qualified lookup, succeeds on the bare one, and
returns the bare name: `(o, id)` becomes `id`.

The runtime then answers whatever the stream publishes bare.
`exec.joinOutputSchemaWithMapping` publishes the PROBE arm's columns bare and
qualifies every duplicate BUILD column by its owning alias, so a bare key binds
the PROBE arm — and which arm probes is a cost decision. The key means one
thing or the other depending on `reorderJoins`.

Wrong on ALL FIVE arms, silently — the seven cells of
`coordinator.TestArcL1AWindowKeyBindsItsOwnJoinArm` pinned with PostgreSQL
17.11's answer beside them:

| cell | the window | wadjet, five arms | PostgreSQL 17.11 |
|---|---|---|---|
| `partOuterArm` | `COUNT(*) OVER (PARTITION BY o.id)` | every row its own partition — `1,1,1 \| 1,2,1 \| 2,3,1 \| 2,4,1` | `…,2` on every row |
| `partArmsSwapped` | the same, FROM written `lat_item i JOIN lat_ord o` | identical | identical to `partOuterArm` |
| `argOverArm` | `SUM(o.total) OVER (PARTITION BY o.id)` | `150,150,200,200` — each row's own total | `300,300,400,400` |
| `orderOverArm` | `COUNT(*) OVER (ORDER BY o.id)` | `1,2,3,4` — a distinct rank per row | `2,2,4,4` |
| `threeWay` | `PARTITION BY o.id` over a three-relation join | `2` per row | `4` per row |
| `orderDescOverJoin` | `ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY i.amount DESC)` | `1` on every row | `2,1,2,1` |
| `orderAscOverJoin` | the ASC twin | `1` on every row | `1,2,1,2` |

`coordinator.TestArcL1QualifyAnswersDuckDBOnEveryArm`'s `overJoin` is the same
fact reached through the `QUALIFY` clause, pinned the same way.

M1 is right in five measured places, and each is right for a reason that is not
the mechanism working:

- `partInnerArm`, `singleRelation`, `partUncontested` — one arm answers to the
  bare name, so erasing the arm loses nothing;
- `partBothArms` (`PARTITION BY o.id, i.id`) — both arms are named, so the
  partition is the pair either way;
- `leftJoinArm` — right **by luck**: a LEFT join emits the probe arm bare and
  the probe is the arm the key names. `partArmsSwapped` is the control that
  shows the luck: writing the same INNER join's FROM the other way round does
  not change the answer, because the reorderer, not the text, picks the probe.

### M2 — the materialized slot: the identity MINTED

Reached when the term is not a bare column reference: an expression, a ROW
field path, or a window argument that is an expression. The key has no identity
in the input at all, so one is created — `plansql.NewSlotAllocator` seeded with
the input's own column names, producing `__winkey_N` in the reserved namespace
(`plansql/reserved_slots.go`) that no query can spell and no alias can shadow.
The projection below the window computes it; nothing above the window reads it.

Right on all five arms everywhere it is reached, and that is the localisation
that made this seam legible: `partExpression` (`PARTITION BY o.id + 0`) is one
character away from `partOuterArm` and answers PostgreSQL's rows, which says
the loss is in the NAME and not in the carrier. `orderDescExprOverJoin`,
`derivedAliasArg` and `derivedAliasNested` are the same fact through the ORDER
BY and the argument.

M2 has no measured wrong cell. Its bound is that it cannot be reached FROM a
name — and routing a name down it is precisely the repair that was measured
back out (below).

### M3 — the qualified name: the identity carried as TEXT

Reached when `inputColTypes` answers nothing, i.e. a derived block, a CTE, an
aggregate or a set operation stands between the window and the scans. The key
keeps the spelling the query wrote and each engine resolves it against its own
stream — which are two different streams:

- **single**: the arm's own Project is a real operator, so the join receives
  the arm's OUTPUT and publishes the arm's ALIAS, qualifying the duplicate by
  the name the query wrote. `x.w` is an exact hit.
- **DAG**: that Project emits no stage, so the join publishes the arm's SOURCE
  column. The key is resolved INSIDE the arm its qualifier names
  (`dagplan.windowArgSourceInScope` over `relationScopeSubtree`), and
  `WindowColSpec.InputRefs` carries the candidate spellings from stage emission
  to the end of planning (ADR-0026 §6).

Right on the cells of `TestArcK1AWindowPartitionKeyBindsItsOwnArm` (#975): the
contested alias in both key directions, PARTITION BY and ORDER BY in one
window, two windows over the two contested aliases, and the DISTINCT-alias
control.

Bounded in one measured place, per arm: `975 PARTITION BY the arm whose alias
is COMPUTED` refuses on `dag-shuffled` (`key "y.w" not in schema`) because a
COMPUTED alias publishes no source column to resolve to, and the exchange ahead
of the window is keyed on a name the join's payload does not carry.
Pre-existing, pinned fail-on-agree.

### The repair that was written and measured back out

Routing a qualified reference the input cannot settle down M2 — the
materialization route — fixes the seven M1 cells and breaks three gates that
were green:

- `coordinator.TestArcK1AWindowPartitionKeyBindsItsOwnArm` (#975): over two
  derived arms that both publish `w`, `PARTITION BY x.w` is right AS A NAME,
  and materializing it gives every row its own partition;
- `coordinator.TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG`
  (#658) and `coordinator.TestJ2AJoinConsumerBindsThePublishedIdentity` (#770)
  still fail after the route is narrowed to base-scan arms.

An earlier version of §8j gave a second reason — that materializing an ORDER BY
term inverts the window's direction — and it is FALSE, measured twice;
`orderDescExprOverJoin` and `orderDescSingleRel` are gate cells so the claim
cannot drift back. The three gates are the whole of the deferral's reason.

### The seam is inside ONE window

The decisive observation for (b): **the same window's three positions are on
two different rules today.** `physical.windowInputCol` already keeps the
ARGUMENT's qualifier exactly where dropping it would leave a name more than one
arm publishes (`windowArgKeepsItsQualifier`, `docs/internals/window-argument-qualification.md`,
#742 round 4). The PARTITION BY and ORDER BY terms, thirty lines away in the
same file, do not. `SUM(y.w) OVER ()` over two arms that both publish `w` binds
y's copy on both engines; `PARTITION BY y.w` over the same input does not.

---

## (b) The rule

> **A window key, a sort key, a lifted predicate column and a join-arm
> reference bind by IDENTITY — the ARM that produced the column, and the column
> within that arm — carried from binding through every rewrite. A NAME is
> derived from the identity for publication; an identity is never derived from
> a name.**

This is ADR-0026 §4 ("the identity of a column is the relation that produced
it, never a bare name two relations share") stated for the consumers §8j left
open, and it is ADR-0026 §9's `StarColumn{Resolve, Publish}` pair one consumer
over: a key is a `(resolve, publish)` pair whose resolve half names an ARM.

Two corollaries are what the code has to obey.

1. **No pass may narrow a reference to a spelling that names less than the
   identity.** The planner never widens a qualified reference to a bare one
   because some column answers to the bare name. `bindWindowColRef`'s fold is
   exactly that move, and it is the whole of M1.

2. **A resolver finds the identity in the stream it is given, by that stream's
   own publication convention — it never guesses an arm.**
   `exec.columnIndexFallback`'s first two steps (exact spelling, then the
   qualifier stripped) ARE the join's publication convention read back: the
   join publishes the probe bare and qualifies a duplicate build column by its
   owning alias, so a written `b.amount` hits exactly when the join qualified
   b's side and hits through the strip when it qualified a's instead. That is
   why the rule needs NO model of which side built — ADR-0026 §6a's sort-key
   rule is the same composition, and says so. The fallback's third step, the
   unique `.bare` suffix scan, is the only step that guesses, and it already
   declines on more than one match (#762, #656, #742).

**Why this subsumes the three mechanisms instead of adding a fourth.** They are
not three answers to one question; they are one answer and one erasure.

- M2 is the identity MINTED: an expression has no identity in the input, so one
  is created in a namespace no query can reach, and the key names it. Right by
  construction.
- M3 is the identity CARRIED: `(arm, column)` travels as the text the query
  wrote, and each engine resolves it against its own stream's convention.
- M1 is the identity ERASED: `(arm, column)` collapses to `column`, after which
  no resolver can do better than "the arm the stream happens to publish bare".

Close the erasure and M1 becomes M3; M2 is already the same rule at a different
carrier. There is no fourth mechanism to write.

### The rule's engine half, measured

The corollary-1 change is one branch: a qualified PARTITION BY / ORDER BY term
keeps the spelling the query wrote whenever more than one arm of the window's
input publishes its bare name and the qualifier names one of those arms — that
is, the terms take the rule the ARGUMENT already takes. Applied as a throwaway
experiment and measured (`probe_rule_experiment.log`, `probe_rule_wide.log`),
over `./internal/planner/...`, `./internal/engine/exec/`, `./wadjet/` and
`./internal/coordinator/`:

| | measured |
|---|---|
| `TestArcL1AWindowKeyBindsItsOwnJoinArm` — pinned cells that move to PostgreSQL's answer | **35 (cell, arm) pairs** = all 7 pins × all 5 arms |
| `TestArcL1QualifyAnswersDuckDBOnEveryArm/overJoin` | **5 (cell, arm) pairs**, moves to DuckDB 1.1.3's answer |
| cells that were right and become wrong | **0** — no `got/want` line anywhere in the four packages |
| `TestArcK1AWindowPartitionKeyBindsItsOwnArm` (#975) | PASS |
| `TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG` (#658) | PASS |
| `TestJ2AJoinConsumerBindsThePublishedIdentity` (#770) | PASS |
| `./internal/planner/...`, `./internal/engine/exec/`, `./wadjet/` | ok |

That is the measurement §8j asked for. The three gates that broke every prior
repair are green because the rule does not route a NAME down the
MATERIALIZATION route; it keeps the name and stops erasing the arm from it.
The experiment is not the change — it is one branch of it, sufficient to say
the rule is sound against the corpus — and it was reverted before this memo was
committed.

---

## (c) The mechanism

### On the single arm

The key is decided at binding, where the FROM clause is still written down, and
carried as the pair the query wrote. `resolveWindowKeys`' qualified-reference
branch stops asking the folded type map whether the bare name exists and asks
instead whether the qualifier names an arm and whether another arm publishes
the bare name — `physical.windowArgKeepsItsQualifier` and
`armsPublishingBareName`, the helpers the argument already uses, plus
`ownedJoinArm` where the arm itself has to be named. `bindWindowColRef`'s bare
answer survives only where the name is uncontested, where it is a no-op.

`exec.Window.bindKeyNames` then resolves the carried spelling against the batch
through `columnIndexFallback`, which is the convention above, and REFUSES a key
it cannot place rather than dropping it — a dropped key is one partition over
the whole input, which is #585's silence.

### Across a stage boundary

The DAG's join publishes the arm's SOURCE column where the single arm publishes
the arm's own alias, because the arm's Project emits no stage. The rule's DAG
half is therefore not "carry the same name" but "carry the same identity and
let each side spell it": the key is resolved inside the arm its qualifier names
(`windowArgSourceInScope`), and `WindowColSpec.InputRefs` holds the candidates
from emission until the end of planning. The deadline is real — a PARTITION BY
key is also the stage's DISTRIBUTION, so the arm-aware choice has to be made at
EMISSION or the exchange and the operator end up keyed on different columns
(ADR-0026 §4b, §6).

ADR-0026 §8i states the same rule from the stage side — *a reference into an
arm has to reach the spelling the arm's stream really carries* — and the R2/ND
finding behind #1135 is why an identity may not be represented by its published
name alone: **a stage publishes its CARRIER, not its declared identity.** #1135
is that fact about a DECLARATION (int4 declared on the single arms, int8 on the
three DAG arms, for an expression over a published slot); this seam is the same
fact about a VALUE. The pair is what survives both.

### Where the two published spellings of a lateral collapse (#1126)

`TestO1AStarOverAJoinPublishesTheQueryNotThePlan`'s `lateral-arm-star` is
pinned on all five arms: `single` and `spilled512k` publish `l.id`, the three
DAG arms publish `i.id`, PostgreSQL publishes the bare `id`. Under the rule the
arm's identity is the one the query named — `l`, which ADR-0021 §1q's alias
stamp now puts on the lateral's subtree root — and the PUBLISHED name is
DERIVED from the identity, which §9 has already decided for a star over a join:
the column's OWN name, unqualified. `l.id` and `i.id` are then the two engines'
RESOLUTION spellings, neither of which reaches a client, and the join's
qualification is demoted to what §2 calls a resolution spelling. The pin is
deleted on all five arms rather than re-pinned per arm.

### What `columnIndexFallback` becomes

Neither deleted nor exact-only. **Measured** (`probe_fallback_experiment.log`):
removing only its qualifier-strip step — leaving exact, then the unique `.bare`
suffix scan — fails nine top-level tests across two packages:

```
exec:      TestColumnIndexFallback_Bidirectional, TestBuildFromRowsResolvesQualifiedBuildKey,
           TestFixKeyAssignmentKeepsAKeyOnlyBuildsRowsAndNullFlag (semi / plain_anti / null_aware_anti),
           TestFixKeyAssignmentRebuildResolvesQualifiedBuildKey, TestHashAggregateWithTableQualifiedColumn,
           TestProject_SelfJoinQualifiedSource, TestSortQualifiedKeyResolvesBare,
           TestSortMergeJoin_QualifiedAndSwappedKeyNames
physical:  TestUncorrelatedSubqueryPlannedUncorrelated
```

The strip is load-bearing for the hash join's key assignment, the sort-merge
join's keys, the hash aggregate's group keys, a projection over a self-join and
the sort's own keys — every operator resolves a name this way. It is not a
guess; it is corollary 2. What becomes unreachable is the PLAN-TIME erasure in
front of it. The suffix scan stays the resolver's one guessing step, bounded by
its decline, and is where a phase-2 review should look next.

---

## (d) The cells the change must move, and the gates it must keep

**Must move** — every pin below is deleted as the proof, on the arms named:

| pin | where | arms |
|---|---|---|
| `partOuterArm`, `partArmsSwapped`, `argOverArm`, `orderOverArm`, `threeWay`, `orderDescOverJoin`, `orderAscOverJoin` | `coordinator.TestArcL1AWindowKeyBindsItsOwnJoinArm` (`l1WindowKeyPins`) | all five |
| `overJoin` | `coordinator.TestArcL1QualifyAnswersDuckDBOnEveryArm` | all five |
| `lateral-arm-star` (#1126) | `coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan` | all five |
| #1130's `R4/outerContestsName` and #1131's `R4/distinctBody` | `coordinator.TestArcL1LateralAndWindowScopeAnswersPostgresOnEveryArm` (`l1ArmPins`, `l1ValuePins`) | measured, then moved or restated in their own terms — see (e) |

**Must keep green** — the three that broke every prior repair, by name, plus
the tables this seam is threaded through:

| gate | what it holds |
|---|---|
| `coordinator.TestArcK1AWindowPartitionKeyBindsItsOwnArm` | **#975** — a PARTITION BY over two derived arms binds its own arm AS A NAME |
| `coordinator.TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG` | **#658** — a derived table's computed alias as a sort/window key on the DAG |
| `coordinator.TestJ2AJoinConsumerBindsThePublishedIdentity` | **#770** — a join consumer binds the identity its producer published |
| K1 | `TestArcK1AnOutputSlotHasOneIdentity`, `TestArcK1AStarIsItsSourceInItsPosition`, `TestArcK1AColumnAliasListRenamesPositionally` |
| K3 | `TestArcK3ADerivedBlockPublishesItsOwnProjection`, `TestArcK3NoBlockItemKindRoutesSilently`, `pgwire.TestArcK3TheWireDeclaresTheBlocksProjection` (the reserved-name property) |
| J1 | `TestArcJ1ALateralKeyIsPublishedUnderAHiddenSlot`, `TestArcJ1AQualifiedStarBesideAnotherItemExpands`, `pgwire.TestArcJ1AHiddenSlotIsNotInTheRowDescription`, `TestArcJ1TheReservedNamespaceRefusesOnlyMinting` |
| N1 | `TestN1AnOrdinalSortKeyBindsItsSlot`, `TestN1AGroupedLateralAnswersItsRows`, `TestN1ATwoGroupedLateralsPublishTheirOwnColumns` |
| O1 | `TestO1AStarOverAJoinPublishesTheQueryNotThePlan`, `pgwire.TestO1TheWireDeclaresAStarJoinsOwnArms`, `logical.TestABareStarOverAJoinExpandsToTheFromClausesArms` and its three siblings |
| O2 | `TestArcO2ADerivedBlockPublishesItsVisibleList`, `pgwire.TestArcO2TheWireUnderACombiningOperator`, `logical.TestEveryRelationCombiningSideIsWiredThroughOneDoor` |
| L1 | `TestArcL1LateralAndWindowScopeAnswersPostgresOnEveryArm`, `TestArcL1QualifyAnswersDuckDBOnEveryArm` |
| R1 | `TestArcR1ACorrelatedBodyAnswersPostgresRowSetOnEveryArm` |
| R2 | `TestR2AJoinArmIsKeyedAndNamedTheSameOnEveryArm`, `TestR2TheSingleArmAnswersPostgreSQLForEveryJoinArm`, `arc_r2_pins_test.go`'s `r2Refuse` (a refusal that starts answering fails) |
| MASKING | `server.TestPolicyMaskingIsPlanTimeOnEveryDoor`, `server.TestArcR1AStarOverAStarBodiedBlockNeverPublishesAPolicedValue`, `server.TestArcL1ABoundedLateralReadsThePublishedValue` — required, because this arc rewrites BINDING: a re-bound key over a policed column, on all nine doors, with a non-vacuous (cell, door) count |
| LICENSE | `go run ./tools/licensecheck .` and the DAG-vocabulary gate — the single-arm half is MIT `internal/planner/physical`, the stage half is AGPL `internal/coordinator/dagplan`, and neither may declare the other's vocabulary |
| DOCS | `go run ./tools/docscheck .` |

The oracle arms (`task pg-oracle:test`, `task pg-oracle:test-decimal`),
`TestTPCHQueries`, the two-path suite and the `dagplan` golden/snapshot gates
run at the literal tip, per COMMON.md.

---

## (e) What is deferred, and why — measured

A deferral is a claim and carries its measurement.

1. **#1135 — a stage declares its CARRIER's width, not the declared width.**
   `CAST(SUM(a) OVER () AS INTEGER)` and `MAX(c_i32) + 0` declare int4 on the
   two single-process arms (PostgreSQL: integer) and int8 on the three DAG
   arms. It is this seam's shape applied to a DECLARATION rather than to a
   VALUE, and arc ND narrowed, measured and put back two repairs for it. It is
   `distributed`-labelled and right on the engine's arm, so under engine-first
   it is recorded per arm with its mechanism and not chased here. The fix it
   names — the stage publishing the declared width beside the carrier — is a
   channel, not a binding rule.

2. **#1113 — an AGGREGATE-terminated join arm named after the scan below it,
   two cells, DAG-only.** Pinned per arm in
   `internal/coordinator/arc_r2_pins_test.go` with its measured mechanism: the
   remaining two are a rename two blocks above the aggregate with the inner
   block's Sort between them, where no list is materialized, and marking such
   an arm anyway trades one wrong answer for another (measured by arc R2).
   Right on the single arm; recorded, not chased.

3. **The COMPUTED-alias key through a shuffle.** K1's `975 PARTITION BY the arm
   whose alias is COMPUTED` refuses on `dag-shuffled`: the arm publishes no
   source column, so the exchange ahead of the window is keyed on a name the
   join's payload does not carry. Pre-existing (the same stage refused at
   `bb8635a4` with a different spelling of the missing name), pinned
   fail-on-agree, DAG-only. The rule reaches the BINDING; what is missing there
   is the arm's materialization crossing the exchange, which is ADR-0026 §4b's
   carrier and a different seam.

4. **#1019's per-outer-row bound — the BASE half only.** The rule closes the
   key binding that blocked arc L1's rewrite: a planner-MINTED
   `ROW_NUMBER() OVER (PARTITION BY <correlation key>)` inherits M1 today, which
   is why that rewrite put every row in its own partition on the three DAG arms
   and, over a policed column, disclosed the stored value's equivalence classes
   on four of the nine doors (ADR-0021 §1q, ADR-0033). Closing the binding
   removes that fault; it does not deliver the rewrite, which has two more
   measured faults of its own — a `__win_N` allocated per BLOCK rather than per
   STATEMENT, and a false decline list — and belongs to ADR-0021 §1q. What this
   arc owes #1019 is the base half: the binding, and the qualified star's
   decline.

5. **#1131's DISTINCT body, and #1130's contested name.** Neither is a
   key-binding question. #1131 needs the lifted predicate evaluated without
   publishing a column into the DISTINCT body; #1130 needs the same column
   published where the outer relation already carries the name. Arc L1 measured
   both available routes out: routing the materialized slot into
   `HiddenJoinCols` (dropped by position) answers `rows=0` wherever an equality
   beside the predicate keys the join, and minting a `__key_N` name takes the
   three DAG arms from PostgreSQL's nine rows to three NULL pads. They are
   measured against the rule in phase 2 and closed only if it reaches them;
   otherwise their mechanism is restated in their own terms.

6. **The BARE contested spelling.** `PARTITION BY w` where two arms publish `w`
   is `42702 column reference "w" is ambiguous` in PostgreSQL 17.11 and an
   answer here — an ADR-0012 superset, with K1's control recording WHICH column
   is bound. The rule does not change it and must not: a key with no qualifier
   names no arm, so there is no identity to carry. Unchanged, deliberately.

7. **`columnIndexFallback`'s suffix scan.** The one step that guesses, bounded
   today by declining on more than one match. Left alone with the measurement
   above: removing the strip step in front of it fails nine tests across two
   packages, so narrowing this resolver is its own arc, not a rider on this one.

### Dimensions deliberately left out of (a)

- **Set-operation and grouped arms** as the window's input: reached by M3 (no
  `inputColTypes`), and their arm naming is ADR-0026 §8i / arc R2's territory,
  measured there.
- **The frame** (`ROWS`/`RANGE`/`GROUPS` bounds): a frame names no relation, so
  it has no arm to own.
- **The wire declaration** of a window output: ADR-0024/§5's question, and
  #1135's above.
- **Spill**: a spilled window re-reads its keys by the same names
  (`window_external.go`), so the spilled arm is a replication of the single
  arm's binding, not a sixth mechanism — which is what `spilled512k` answering
  identically in every cell above already shows.
