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

Round 2 revision. Memo review round 1 blocked on B1: the first draft declared
`exec.columnIndexFallback`'s exact-then-strip lookup sound unconditionally,
which contradicts the ownership rule itself on a measured shape. Corollary 2
now carries an ownership PRECONDITION and a disposition for when it fails;
§(c) states how `l.id` becomes `i.id` before the sort; §(a) and §(c) carry the
narrowed claims P1–P3 asked for.

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
fact reached through the `QUALIFY` clause, pinned the same way (DuckDB 1.1.3 is
that clause's oracle; PostgreSQL has no `QUALIFY`).

M1's five measured right cells are each right for a reason that is **not the
mechanism working**, and the first draft over-stated two of them (P3):

- `partUncontested` and `singleRelation` — exactly one column in the input
  answers to the bare name, so erasing the arm loses nothing;
- `partInnerArm` — right because the arm the plan selected as PROBE **is** `i`,
  the arm the key names, not because only one arm publishes `id`. Its mirror
  `partOuterArm` is the same query with the other arm named and is wrong;
- `partBothArms` (`PARTITION BY o.id, i.id`) — a **weak control**. The fold
  makes BOTH references `id`, so the operator partitions on `(i.id, i.id)`, and
  it agrees with PostgreSQL only because `i.id` is unique in this fixture and
  both give singleton groups. It is not "the pair either way";
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

**What "right by construction" does and does not claim (P3).** The mint gives
the expression's RESULT a name nothing else can own, which is why a materialized
key never collides. It says nothing about the expression's INPUT LEAVES: those
are resolved by the ordinary reference rules, so `PARTITION BY x.w + y.w` over
two arms that both publish `w` can still bind one leaf to the wrong occurrence
and then compute the wrong value into a correctly-named slot. `o.id + 0` over
one contested pair and J2's `control: an expression over TWO window arms` are
the nearest corpus shapes and neither is that spelling; the leaf-binding premise
is corollary 1's, not the mint's, and phase 2 adds the cell.

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
- **DAG**: for a PLAIN-RENAME arm that Project emits no stage, so the join
  publishes the arm's SOURCE column, and the key is resolved INSIDE the arm its
  qualifier names (`dagplan.windowArgSourceInScope` over
  `relationScopeSubtree`). "The DAG publishes source" is true of a plain rename
  and **not** in general (P1): a COMPUTED alias is materialized on the
  producing stage and published under its own name
  (`derivedAliasColumnFor` → `materializeWindowAliasKeys`), and a SET-OPERATION
  arm publishes the operation's own column list (ADR-0026 §8i item 1,
  `setOpArmPublishesItsOwnList`).

Right on the cells of `TestArcK1AWindowPartitionKeyBindsItsOwnArm` (#975): the
contested alias in both key directions, PARTITION BY and ORDER BY in one
window, two windows over the two contested aliases, and the DISTINCT-alias
control.

Bounded in one measured place, per arm: `975 PARTITION BY the arm whose alias
is COMPUTED` refuses on `dag-shuffled` (`key "y.w" not in schema`) because a
COMPUTED alias publishes no source column to resolve to, and the exchange ahead
of the window is keyed on a name the join's payload does not carry.
Pre-existing, pinned fail-on-agree. That refusal is the right disposition under
the rule — it is corollary 2's third option — and is preferable to the strip
that would bind x's `w`.

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
> reference bind by IDENTITY — the OCCURRENCE that produced the column, and the
> column within that occurrence — carried from binding through every rewrite. A
> NAME is derived from the identity for publication; an identity is never
> derived from a name.**

"Occurrence" and not "arm" deliberately: two references to one table are two
occurrences, a written alias is a *spelling* of an occurrence rather than the
occurrence itself, and an occurrence with no written alias still produces
columns. A qualifier is how an occurrence is addressed in TEXT, and text is a
carrier, not the identity.

**What the implementation settled about an UNNAMED occurrence, measured.** The
first draft said the plan "already has a handle" and pointed at the node
`physical.ownedJoinArm` returns and at `Node.HiddenJoinCols`. That was too
compressed and the round-2 review was right to say so: `ownedJoinArm` requires
a qualified name and answers nil without one, and `HiddenJoinCols` is a string
list on a join rather than a map keyed by that node. The rule does not need a
new handle, and the reason is a disposition rather than a representation —
measured at `78a0a671` on five arms (`probe_unnamed_occurrence.log`,
`probe_unnamed_occurrence_pg.log`):

| shape | wadjet, five arms | PostgreSQL 17.11 |
|---|---|---|
| `FROM lat_ord o, LATERAL (SELECT i.id AS w …)` — no alias — with `PARTITION BY w` | REFUSED at plan time: `join ON "o.id = (SELECT …).__key_0"` | 4 rows |
| the same, its column named BARE and contested (`SELECT i.id`) with `PARTITION BY id` | the same refusal | `42702 column reference "id" is ambiguous` |
| the same, read as a plain select item (`SELECT w AS b`) | the same refusal | 4 rows |

An alias-less LATERAL is refused BEFORE any consumer binds anything: the
decorrelation has no name for the arm, so the join key it mints cannot be
resolved. So there is no shape in which a MISSING textual qualifier is used as
an identity — corollary 1 fires only for a reference the query wrote WITH a
qualifier, and an unnamed occurrence's columns are reachable only bare, where
either exactly one column answers (SQL has resolved it) or two do and
PostgreSQL raises 42702 (the ADR-0012 superset in §(e) item 6). The refusal
itself is a gap — PostgreSQL answers two of those three — and it is recorded as
a filing candidate, not closed here.

Where an occurrence IS named, the handle is the name the query wrote, and the
walks that resolve it into a producer's subtree (`relationScopeSubtree`,
`ownedJoinArm`, `namedArmScope`) are the ones already in the tree.

This is ADR-0026 §4 ("the identity of a column is the relation that produced
it, never a bare name two relations share") stated for the consumers §8j left
open, and it is ADR-0026 §9's `StarColumn{Resolve, Publish}` pair one consumer
over: a key is a `(resolve, publish)` pair whose resolve half names an
occurrence.

Two corollaries are what the code has to obey.

**1. No pass may narrow a reference to a spelling that names less than the
identity.** The planner never widens a qualified reference to a bare one
because some column answers to the bare name. `bindWindowColRef`'s fold is
exactly that move, and it is the whole of M1.

**2. A resolver may bind a slot by name only where the planner has established
that the slot IS this occurrence's carrier in THIS stream; where it has not,
the reference is translated, supplied, or refused — never bound to another
occurrence's column.**

The first draft of this corollary said the exact-then-strip lookup is the
join's publication convention read back and "never guesses". That is true of
one stream and false of another, and the difference is measured.

*Where it holds.* Over a join's OWN output the convention is
`exec.joinOutputSchemaWithMapping`: the probe bare, every duplicate build
column qualified by its owning alias. A written `b.amount` hits exactly when
the join qualified b's side and hits through the strip when it qualified a's
instead, so the composition needs no model of which side built — ADR-0026 §6a's
sort-key rule is that composition and says so, and `TestL1AQualifiedOrderByTermBindsTheReferenceItNames`
gates it. Removing the strip is not an option: measured
(`probe_fallback_experiment.log`), deleting only that step fails nine top-level
tests across `internal/engine/exec` and `internal/planner/physical` (listed in
§(c)). The strip has legitimate consumers and stays.

*Where it fails — the decisive cell, measured on five arms.* A stream that
carries the occurrence's value under a spelling the planner never mapped is not
that convention, and there the strip silently binds another occurrence. Measured
at base with an order-preserving harness (`probe_b1_lateral_sort.log`, command
echoed in the log, HEAD `aed447e3`):

```sql
-- 1. the O1 gate's own cell
SELECT * FROM lat_ord o, LATERAL (SELECT i.id, i.amount FROM lat_item i
                                  WHERE i.order_id = o.id) l ORDER BY o.id, l.id
   single, spilled512k     l.id | 1,50 · 2,100 · 3,75 · 4,125     (order right, name wrong)
   dag, dag-shuffled,
   dag-morsel4             i.id | 2,100 · 1,50 · 4,125 · 3,75     (inner ids 2,1,4,3)
   PostgreSQL 17.11          id | 1,50 · 2,100 · 3,75 · 4,125

-- 2. the mirror, ordering by the CARRIER's spelling instead
SELECT * FROM … l ORDER BY o.id, i.id
   single, spilled512k     inner ids 2,1,4,3          ← l.id binds, i.id does not
   dag ×3                  inner ids 1,2,3,4          ← i.id binds, l.id does not

-- 3. the same ownership question with NO star and NO sort key in question
SELECT o.id AS a, l.id AS b FROM … l ORDER BY a, b
   single, spilled512k     1,1 | 1,2 | 2,3 | 2,4      = PostgreSQL 17.11
   dag, dag-shuffled,
   dag-morsel4             1,1 | 1,1 | 2,2 | 2,2      ← l.id reads the OUTER id
```

Cell 3 is the sharpest form: the DAG stream carries `[id (o's), i.id (l's
carrier)]`, `l.id` misses exactly, the strip returns the OUTER `id`
immediately — the suffix scan is never reached — and the query answers the
outer key in the inner column, silently, on three arms. It is a wrong VALUE and
not an ordering question: the multiset itself differs, so no ADR-0013 class
explains it. Cells 1 and 2 are exact mirrors of each other, which is the seam
stated as compactly as it can be: **one column, two spellings, and each engine
answers to exactly one of them.**

ADR-0026 §8i's set-operation arm (lines ~2492–2506) is the same failure form
one producer over: `stageBuildTableAlias` named the arm after the first scan it
found, `SELECT a.id` matched neither spelling exactly, the strip bound
`lat_item`'s `id`, and the query answered `1|2|3|4` for PostgreSQL's `1|1|2|2`.
The repair there was to give the arm a real producer contract
(`setOpArmPublishesItsOwnList`), after which the by-name lookup is an address.
That is the order this corollary states in general: **the contract first, the
lookup second.**

*The disposition when the precondition fails*, in order, for a window key, a
sort key, a lifted predicate reference and a join-arm reference alike:

1. **TRANSLATE** to the occurrence's spelling in this stream, through the
   producer mapping the tree already has — `dagplan.windowArgSourceInScope` /
   `physical.derivedAliasSourceColumn` for a block, `ownedJoinArm` +
   `resolveRenameSource` for a join arm, `dagplan.resolveSortKeyColumn` for a
   sort key.
2. **SUPPLY** the carrier where no spelling reaches it: materialize the
   occurrence's value on the producing fragment under a name that stream will
   carry (`materializeAliasColumns`, and `bindConsumersToPublishedIdentity`'s
   phase-1 carry, which is #770's own mechanism).
3. **REFUSE**, loudly, with the existing sentences —
   `exec.Window.bindKeyNames`'s refusal, `unresolvedAggColumn`'s `0A000`, the
   shuffle's `key %q not in schema`. K1's `dag-shuffled` computed-alias refusal
   is this option already taken, and it is correct.

Never a fourth: bind a slot whose owner is a different occurrence. That is the
one outcome the rule forbids, and it is what every cell above measures.

**Why this subsumes the three mechanisms instead of adding a fourth.** They are
not three answers to one question; they are one answer and one erasure.

- M2 is the identity MINTED: an expression has no identity in the input, so one
  is created in a namespace no query can reach, and the key names it — with the
  leaf-binding caveat above.
- M3 is the identity CARRIED: `(occurrence, column)` travels as the text the
  query wrote, and each engine translates it to its own stream's spelling.
- M1 is the identity ERASED: `(occurrence, column)` collapses to `column`,
  after which no resolver can do better than "the occurrence the stream happens
  to publish bare".

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
| cells that were right and become wrong | **none found** — every failure in the four packages is a pin-agrees failure. The 40 `got` lines in the logs are those pins reporting their corrected values (P3: the first draft said "no got/want line", which was wrong about the log's text and right about its verdict) |
| `TestArcK1AWindowPartitionKeyBindsItsOwnArm` (#975) | PASS |
| `TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG` (#658) | PASS |
| `TestJ2AJoinConsumerBindsThePublishedIdentity` (#770) | PASS |
| `./internal/planner/...`, `./internal/engine/exec/`, `./wadjet/` | ok |

That is the measurement §8j asked for. The three gates that broke every prior
repair are green because the rule does not route a NAME down the
MATERIALIZATION route; it keeps the name and stops erasing the occurrence from
it. The experiment is not the change — it is corollary 1's branch only, and it
does NOT address corollary 2's precondition, which is why the decisive cell
above is still wrong under it. It was reverted before this memo was committed.

**A passing gate is weaker than every cell agreeing with PostgreSQL** (N3). The
gates above pass WITH their pins and refusals standing: K1's computed-alias
shuffle refusal, J2's three outer-join schema refusals, R2's `r2Refuse` entries
and the nested-grouped-list pins. Those stay visible in §(d) and §(e); none of
them makes binding another occurrence's column an acceptable resolution.

---

## (c) The mechanism

### On the single arm

The key is decided at binding, where the FROM clause is still written down, and
carried as the pair the query wrote. `resolveWindowKeys`' qualified-reference
branch stops asking the folded type map whether the bare name exists and asks
instead whether the qualifier names an occurrence and whether another
occurrence publishes the bare name — `physical.windowArgKeepsItsQualifier` and
`armsPublishingBareName`, the helpers the argument already uses, plus
`ownedJoinArm` where the occurrence itself has to be named.
`bindWindowColRef`'s bare answer survives only where the name is uncontested,
where it is a no-op.

`exec.Window.bindKeyNames` then resolves the carried spelling against the batch
through `columnIndexFallback`, under corollary 2's precondition: over the
join's own output that composition is an address, and it REFUSES a key it
cannot place rather than dropping it — a dropped key is one partition over the
whole input, which is #585's silence.

### Across a stage boundary — the carrier per key position, and the deadline

The three window positions do NOT share one carrier, and the first draft
conflated them (P1). Measured in `dagplan/stage_emission.go` and
`dagplan/published_identity.go`:

| position | how the owner reaches a carrier | deadline |
|---|---|---|
| `PartitionBy[i]` | a three-step ladder AT EMISSION: (1) `windowArgSourceInScope` — resolve inside the occurrence the qualifier names, then `CleanExpr`; (2) `DerivedAliasSourceColumn` unscoped; (3) `derivedAliasColumnFor` → `materializeWindowAliasKeys`, which computes the value on the producing stage under its own name | **EMISSION, final.** No late correction exists |
| `OrderBy[i].Column` | the same ladder, same loop | **EMISSION, final** |
| `InputCol` (the ARGUMENT) | a DIFFERENT ladder. It STOPS as soon as the qualifier names an arm — `scoped` true with an empty `src`, which is a COMPUTED join-arm alias, does NOT fall through to the unscoped lookup or the materialization the keys take. The argument then travels as the alias and `WindowColSpec.InputRefs` carries its candidates, settled at the END of planning by `bindConsumersToPublishedIdentity`, which rewrites `InputCol` alone | emission, with a late correction |

`WindowColSpec.InputRefs` is therefore the ARGUMENT's channel and nothing else;
the first draft's claim that it carries the keys is withdrawn. And the two
ladders' DIFFERENCE is load-bearing rather than incidental: collapsing them —
making the argument fall through the way the keys do — would delete the late
carrier path a computed join-arm argument depends on. That is recorded at the
branch itself in `dagplan`'s window emission, so the next change cannot erase
it by tidying. **The deadline is
the contract**: a PARTITION BY key is also the stage's DISTRIBUTION, so
`EnsureDistribution` consumes it and the exchange and the operator must already
agree on one name — which is why the arm-aware choice is made at emission
(ADR-0026 §4b, §6) and why a late pass cannot be the fix for a key the way it is
for an argument. Phase 2 owes the key positions the same *decision*, not the
same *timing*: either the ladder's step 1 returns the occurrence's carrier
spelling directly, or the key takes step 3 and the exchange keys on the
materialized column.

Steps 1 and 2 each apply `CleanExpr` to the resolved source, which strips the
qualifier a second time — after the occurrence has been chosen. Under corollary
2 that strip is sound only while the resulting bare name is unique in the
stage's stream; where it is not, the translation must keep the producer's own
spelling. That is the same erasure as M1, one layer down, and phase 2 measures
it.

ADR-0026 §8i states the rule from the stage side — *a reference into an arm has
to reach the spelling the arm's stream really carries* — and the R2/ND finding
behind #1135 is why an identity may not be represented by its published name
alone: **a stage publishes its CARRIER, not its declared identity.** #1135 is
that fact about a DECLARATION (int4 declared on the single arms, int8 on the
three DAG arms, for an expression over a published slot); this seam is the same
fact about a VALUE. The pair is what survives both.

### How `l.id` becomes `i.id` before the sort (#1126)

The decisive cell's translation, stated explicitly as B1 requires. The
occurrence is the LATERAL body, which ADR-0021 §1q's alias stamp puts on the
body's subtree ROOT as `l` — the stamp is on the root only, so the body's own
`i` scope is untouched and this memo invents no user-visible alias. Given the
reference `l.id`:

1. `relationScopeSubtree(child, "l")` finds that occurrence's subtree — the
   stamp is what makes it findable, and it is already in the tree;
2. `derivedAliasSourceColumn("id", scope)` answers the spelling the body's own
   projection publishes `id` from, which on the DAG's stream is `i.id`;
3. the SORT KEY (and the window key, and the lifted predicate reference) binds
   **that** spelling, not `l.id` and not the bare `id`;
4. the PUBLISHED name is derived separately, from the identity, by §9's rule for
   a star over a join: the column's OWN name, unqualified — `id`, which is what
   PostgreSQL publishes. `l.id` and `i.id` are then both RESOLUTION spellings
   and neither reaches a client.

Step 3 is the half the first draft omitted, and it is why the pin cannot go on a
published-name change alone: renaming the output column to `id` leaves the
DAG's sort still bound to the outer `id`, which is measured cell 1's inner ids
`2,1,4,3` and cell 3's values `1,1|2,2`. **Both halves land together or the pin
stays.** Where step 2 answers nothing — a COMPUTED body item — the disposition
is corollary 2's step 2 (supply the carrier) or step 3 (refuse); never the
strip.

### The lifetime of a lifted column: the LAST READER, not the join (P2)

Ownership says which column a reference means. It does not keep that column
alive, and the two are separately necessary. The tree has two different
mechanisms and they are not interchangeable:

- `Node.HiddenJoinCols` **removes** a slot at the join, by position. A value
  removed there is unavailable to any reader above the join.
- `Node.StarLiftedRefCols` only **hides** a column from qualified-star
  expansion (`relationOutputColumns`); the column stays on the stream, which is
  what lets a lifted non-equality predicate evaluated ABOVE the join read it.

Arc L1 measured both failures of getting this wrong, and they are the boundary
on #1130/#1131: routing the materialized column into `HiddenJoinCols` answers
`rows=0` wherever an equality beside the predicate keys the join (its
counterfactual `cf_drop`), and minting a `__key_N` name takes the three DAG
arms from PostgreSQL's nine rows to three NULL pads (`cf_mint2`). So the rule
phase 2 implements is: **the operator that mints a column owns it until its
LAST READER, and the final visible list is decided separately from the
stream's width.** Publishing a column into a DISTINCT's key or a star's
published list is not a naming change — it changes the rows, which is #1131's
own measurement (arc L1's repair would have made it 9 rows, not PostgreSQL's
7). Identity does not settle evaluation order, and this memo does not claim it
does.

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
the sort's own keys — every operator resolves a name this way, and M2's
consumers need it. It is not deleted and not narrowed globally.

What changes is the PRECONDITION in front of it. Two things become unreachable
rather than one:

1. the PLAN-TIME erasure — `bindWindowColRef` widening a qualified reference to
   a bare one because the folded map answers the bare name (corollary 1);
2. a reference reaching the resolver in a stream where the planner has NOT
   established the carrier — the decisive cell. There the planner translates,
   supplies or refuses BEFORE the lookup, so the strip never adjudicates an
   ownership question it cannot see.

The suffix scan stays the resolver's one guessing step, bounded by its decline
on more than one match, and is where a phase-2 review should look next.

---

## What phase 2 landed

Corollary 1, and the gates that hold the rule. Measured at the arc's tip:

| | |
|---|---|
| the change | `physical.resolveWindowKeys` keeps a qualified PARTITION BY / ORDER BY term's spelling wherever more than one occurrence of the window's input publishes its bare name (`windowArgKeepsItsQualifier`, the window ARGUMENT's own rule since #742 round 4) |
| pins DELETED | the seven of `TestArcL1AWindowKeyBindsItsOwnJoinArm` and `TestArcL1QualifyAnswersDuckDBOnEveryArm`'s `overJoin`, on all five arms — 40 (cell, arm) pairs |
| the three that broke every prior repair | #975, #658, #770 green |
| the seam, enumerated once | `coordinator.TestWKASeamConsumerBindsItsOwnOccurrence` — 39 cells × 5 arms, 173 of 195 agree with PostgreSQL 17.11 |
| the wire half | `pgwire.TestWKTheWireDeclaresTheSeamsOwnColumns` |
| the masking class | `server.TestArcWKAReboundKeyOverAPolicedColumnReadsTheMask` — 72 (cell, door) pairs answer the mask, 9 refuse the denied column, over nine doors |
| ADR-0026 §8j | rewritten from NOT SETTLED to the rule, §4 and §9 cross-referenced |

Corollary 2's precondition is STATED and its disposition ladder is named to the
functions that implement each step, but the one family that needs the TRANSLATE
step — a reference into a decorrelated LATERAL arm on the three DAG arms — is
not closed here. It is right on the engine's own arm and wrong only on the
distributed ones, so under engine-first it is pinned per arm with its mechanism
in the seam's table and carried as a `distributed` filing candidate. §(e) item 9
records it.

## (d) The cells the change must move, and the gates it must keep

**Must move** — every pin below is deleted as the proof, on the arms named:

| pin | where | arms |
|---|---|---|
| `partOuterArm`, `partArmsSwapped`, `argOverArm`, `orderOverArm`, `threeWay`, `orderDescOverJoin`, `orderAscOverJoin` | `coordinator.TestArcL1AWindowKeyBindsItsOwnJoinArm` (`l1WindowKeyPins`) | all five |
| `overJoin` | `coordinator.TestArcL1QualifyAnswersDuckDBOnEveryArm` | all five |
| `lateral-arm-star` (#1126) | `coordinator.TestO1AStarOverAJoinPublishesTheQueryNotThePlan` | all five — and BOTH halves (the published name on single/spilled, the resolution on the three DAG arms) or the pin stays |
| the explicit-list spelling of the same shape — NOT gated today, a new cell | `SELECT o.id AS a, l.id AS b FROM lat_ord o, LATERAL (…) l ORDER BY a, b` answers `1,1\|1,1\|2,2\|2,2` on the three DAG arms for PostgreSQL's `1,1\|1,2\|2,3\|2,4` | three DAG arms |
| #1130's `R4/outerContestsName` and #1131's `R4/distinctBody` | `coordinator.TestArcL1LateralAndWindowScopeAnswersPostgresOnEveryArm` (`l1ArmPins`, `l1ValuePins`) | measured, then moved or restated in their own terms — see (e) |

**Must keep green** — the three that broke every prior repair, by name, plus
the tables this seam is threaded through:

| gate | what it holds |
|---|---|
| `coordinator.TestArcK1AWindowPartitionKeyBindsItsOwnArm` | **#975** — a PARTITION BY over two derived arms binds its own arm AS A NAME; its computed-alias `dag-shuffled` refusal is a retained exception |
| `coordinator.TestADerivedTablesComputedAliasIsNotASortOrWindowKeyOnTheDAG` | **#658** — computed aliases, plain renames, CTEs and shadowing aliases as sort and window keys, with a per-cell `UnreachableOutputLocalRoutes` delta: replacing DAG execution with a local answer FAILS it |
| `coordinator.TestJ2AJoinConsumerBindsThePublishedIdentity` | **#770** — respelling AND carrying, through three, four and five relations; its three outer-join shuffled-schema refusals are retained exceptions |
| `coordinator.TestL1AQualifiedOrderByTermBindsTheReferenceItNames` | §6a — the self-join sort, direction, swapped arms and top-N; the composition corollary 2 depends on |
| K1 | `TestArcK1AnOutputSlotHasOneIdentity`, `TestArcK1AStarIsItsSourceInItsPosition`, `TestArcK1AColumnAliasListRenamesPositionally` |
| K3 | `TestArcK3ADerivedBlockPublishesItsOwnProjection`, `TestArcK3NoBlockItemKindRoutesSilently`, `pgwire.TestArcK3TheWireDeclaresTheBlocksProjection` (the reserved-name property) |
| J1 | `TestArcJ1ALateralKeyIsPublishedUnderAHiddenSlot`, `TestArcJ1AQualifiedStarBesideAnotherItemExpands`, `pgwire.TestArcJ1AHiddenSlotIsNotInTheRowDescription`, `TestArcJ1TheReservedNamespaceRefusesOnlyMinting` |
| N1 | `TestN1AnOrdinalSortKeyBindsItsSlot`, `TestN1AGroupedLateralAnswersItsRows`, `TestN1ATwoGroupedLateralsPublishTheirOwnColumns` |
| O1 | `TestO1AStarOverAJoinPublishesTheQueryNotThePlan`, `pgwire.TestO1TheWireDeclaresAStarJoinsOwnArms`, `logical.TestABareStarOverAJoinExpandsToTheFromClausesArms` and its three siblings |
| O2 | `TestArcO2ADerivedBlockPublishesItsVisibleList`, `pgwire.TestArcO2TheWireUnderACombiningOperator`, `logical.TestEveryRelationCombiningSideIsWiredThroughOneDoor` |
| L1 | `TestArcL1LateralAndWindowScopeAnswersPostgresOnEveryArm`, `TestArcL1QualifyAnswersDuckDBOnEveryArm` |
| R1 | `TestArcR1ACorrelatedBodyAnswersPostgresRowSetOnEveryArm` |
| R2 | `TestR2AJoinArmIsKeyedAndNamedTheSameOnEveryArm`, `TestR2TheSingleArmAnswersPostgreSQLForEveryJoinArm`, `arc_r2_pins_test.go`'s `r2Refuse` (a refusal that starts answering fails) and its nested-grouped-list pins |
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
   channel, not a binding rule. The producer/carrier distinction still
   constrains its VALUE binding (N1).

2. **#1113 — an AGGREGATE-terminated join arm named after the scan below it,
   two cells, DAG-only.** Pinned per arm in
   `internal/coordinator/arc_r2_pins_test.go` with its measured mechanism: the
   remaining two are a rename two blocks above the aggregate with the inner
   block's Sort between them, where no list is materialized, and marking such
   an arm anyway trades one wrong answer for another (measured by arc R2).
   Right on the single arm; recorded, not chased. Deferring the producer does
   not make the other copy's payload a valid identity resolution — under
   corollary 2 those cells are refusals-in-waiting, not licensed guesses.

3. **The COMPUTED-alias key through a shuffle.** K1's `975 PARTITION BY the arm
   whose alias is COMPUTED` refuses on `dag-shuffled`: the arm publishes no
   source column, so the exchange ahead of the window is keyed on a name the
   join's payload does not carry. Pre-existing (the same stage refused at
   `bb8635a4` with a different spelling of the missing name), pinned
   fail-on-agree, DAG-only. It is corollary 2's option 3 correctly taken. The
   rule reaches the BINDING; what is missing is the arm's materialization
   crossing the exchange, which is ADR-0026 §4b's carrier and a different seam.

4. **#1019's per-outer-row bound — the BASE half only.** The rule closes the
   key binding that blocked arc L1's rewrite: a planner-MINTED
   `ROW_NUMBER() OVER (PARTITION BY <correlation key>)` inherits M1 today, which
   is why that rewrite put every row in its own partition on the three DAG arms
   and, over a policed column, disclosed the stored value's equivalence classes
   on four of the nine doors (ADR-0021 §1q, ADR-0033). Closing the binding
   removes that fault; it does not deliver the rewrite, which has two more
   measured faults of its own — a `__win_N` allocated per BLOCK rather than per
   STATEMENT, and a false decline list — and belongs to ADR-0021 §1q (N2). What
   this arc owes #1019 is the base half: the binding, and the qualified star's
   decline.

5. **#1131's DISTINCT body, and #1130's contested name.** Neither is a
   key-binding question and they are two different mechanisms, not one name fix
   (N2). #1131 needs the lifted predicate evaluated without publishing a column
   into the DISTINCT body — arc L1's repair would have answered 9 rows where
   PostgreSQL answers 7. #1130 needs the same column published where the outer
   relation already carries the name; its single/spilled arms answer three NULL
   pads for PostgreSQL's `(1,NULL),(2,Widget),(3,Gadget),(3,Widget)` while the
   DAG arms agree. Arc L1 measured both available routes out (the `cf_drop` and
   `cf_mint2` counterfactuals in §(c)). They are measured against the rule in
   phase 2 and closed only if it reaches them; otherwise their mechanism is
   restated in their own terms and their pins and recorded PostgreSQL rows stay
   visible. Neither is closed by changing an output label.

6. **The BARE contested spelling.** `PARTITION BY w` where two arms publish `w`
   is `42702 column reference "w" is ambiguous` in PostgreSQL 17.11 and an
   answer here — an ADR-0012 superset, with K1's control recording WHICH column
   is bound. The rule does not change it and must not: a key with no qualifier
   names no occurrence, so there is no identity to carry. Unchanged,
   deliberately.

7. **`columnIndexFallback`'s suffix scan.** The one step that guesses, bounded
   today by declining on more than one match. Left alone with the measurement
   in §(c): removing the strip step in front of it fails nine tests across two
   packages, so narrowing this resolver is its own arc, not a rider on this one.

8. **An out-of-scope qualifier answered under a star and refused under a list.**
   Measured beside the decisive cell (`probe_b1_lateral_sort.log`):
   `SELECT * FROM … l ORDER BY o.id, i.id` ANSWERS on all five arms, while
   `SELECT o.id AS a, l.id AS b FROM … l ORDER BY a, i.id` refuses with
   `missing FROM-clause entry for table "i"` on all five. One out-of-scope
   reference, two dispositions decided by the enclosing SELECT list.
   Pre-existing, not a binding question, recorded as a filing candidate.

9. **The rule's DAG half for a LATERAL arm — the seam's remaining column.**
   Five consumers × three DAG arms, measured in
   `TestWKASeamConsumerBindsItsOwnOccurrence`: a decorrelated body's Project
   emits no stage, so the DAG's join publishes the body's INNER SCAN spelling
   where the single-process join publishes the arm's own alias; the reference
   misses exactly and the qualifier strip binds the OUTER column. Right on
   `single` and `spilled512k`, wrong on `dag`, `dag-shuffled` and
   `dag-morsel4`, so it is `distributed` by the arm rule and pinned per arm
   rather than chased (engine-first, Derek 2026-09-16). #1126 is the same fact
   seen as two published SPELLINGS, and its pin stays for the reason §(c)
   gives: the published-name half alone cannot retire it.

10. **An alias-less LATERAL is refused on all five arms**, where PostgreSQL
    answers — the decorrelation has no name for the arm, so the join key it
    mints cannot be resolved. Measured beside the unnamed-occurrence
    disposition in §(b). Pre-existing, not a binding question, filing
    candidate.

11. **A `*` standing BESIDE another item over a join is refused**, where
    PostgreSQL answers eight fields. The expansion mints its projection on
    SHAPE alone and takes it back out when the arms cannot be stated, and a
    star beside an item is left for the physical planner, which has no relation
    to expand it from. Pinned with PostgreSQL's own list in
    `pgwire.TestWKTheWireDeclaresTheSeamsOwnColumns`; filing candidate.

### Dimensions deliberately left out of (a)

- **Set-operation and grouped arms** as the window's input: reached by M3 (no
  `inputColTypes`), and their arm naming is ADR-0026 §8i / arc R2's territory,
  measured there. §8i's set-op arm is cited in (b) as the same failure FORM,
  not re-measured here.
- **The frame** (`ROWS`/`RANGE`/`GROUPS` bounds): a frame names no relation, so
  it has no occurrence to own (N1).
- **The wire declaration** of a window output: ADR-0024/§5's question, and
  #1135's above.
- **Spill**: a spilled window re-reads its keys by the same names through the
  same resolver (`exec/window_external.go` calls `columnIndexFallback`), so the
  spilled arm replicates the single arm's binding rather than being a sixth
  mechanism — which is what `spilled512k` answering identically in every cell
  above already shows.
