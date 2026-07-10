# Bushy join enumeration for the CBO

Status (2026-07-10): Phase A MERGED (#203) · Layer B MERGED (#204) ·
distributed fixes MERGED (#205: composite broadcast eligibility +
probe-side-only fusion; #206: exchange-aware costing in the bushy regime).
Phase C SF10 A/B: THREE same-window pairs run — rows identical in all,
BushyJoinsPlanned=21 each bushy arm. Final pair (bin 71cab6a):
suite −2.1%; consistent wins Q18 −32%, Q16 −31%, Q13 −23%, Q07 −19%,
Q05 −16%; consistent loss Q08 +88% (was +123/+135% pre-#206).

**Flag stays DEFAULT OFF.** Before any default-flip campaign:
1. WIDTH-AWARE EXCHANGE COST — distributedExchangeCost prices rows, not
   bytes; a composite build shuffles WIDE join-output rows where left-deep
   shuffles narrow scan outputs. Q08's bushy pick survives row-based
   pricing but loses in reality. Extend the DP cost inputs with per-column
   width (RelStats has the pieces; estimateJoinSubtreeBytes already
   computes composite widths physical-side).
2. SF100 same-window pair after (1), targeting the roadmap band
   (Q05/Q07/Q09) vs baseline results/20260709-175846 (48.5m).
3. Decorrelator-emitted side-assigned keys (kills the Q02 runtime-repair
   dependency; see §5).

Layer B findings (2026-07-09):
- STRICT-WIN SHAPES are broader than predicted: linear fact-fact FK chains
  tie (left-deep kept), but SNOWFLAKE DIMENSION CHAINS (fact→d1→d2,
  supplier→nation→region) strictly win bushy — the fact stream passes one
  join instead of two. Pure star schemas tie and stay left-deep. Forced-on
  SF0.01: 22/22 queries chose at least one bushy order, rows identical.
- DPsub CONNECTIVITY INVARIANT is load-bearing: without it the bushy pass
  laundered the DP's cross-join escape-hatch entries (penalty 10× ≈ free on
  tiny dims) into plans with key-less joins — distributed executor refuses,
  Q07/Q08 0 rows. dpEntry.connected gates partitions.
- markCoPathingSelfJoinBuilds stays reaches-based: intersection
  generalization force-qualified Q02's parallel-branch partsupp pair and
  broke flag-OFF Q02. Bushy self-join collisions resolve at the colliding
  join via isDup + BuildColOrigins (Phase A) — proven by distributed
  forced-on 22/22.
- SF0.01 harness walls show bushy arm slower at toy scale (extra shuffle
  stages replace fused broadcast chains). Not a wall signal (local-repro
  lies); the SF10 pair decides.
Prior art: left-deep DP reorder (`internal/planner/logical/optimizer.go:2863`),
bushy attempt deferred 2026-05-26 (commit 80c2f46, wrong rows on
Q02/Q07/Q08/Q09/Q21 at SF0.01).

## 1. Problem

`dpJoinReorder` enumerates only left-deep trees: every join's build side is a
single base relation, so plans are one spine
`((((lineitem ⋈ orders) ⋈ customer) ⋈ nation) ⋈ region)`. A bushy plan may
join two intermediates — e.g. `(customer ⋈ orders ⋈ lineitem) ⋈
(supplier ⋈ nation ⋈ region)` — pre-collapsing dimension chains into one
small build instead of streaming the fact spine through several separate
probes. Trino enumerates bushy shapes; the roadmap names bushy CBO as the
residual join-order gap (Q05/Q07/Q09 class).

SF100 anchors (baseline results/20260709-175846, 48.5m suite): Q05 2m45,
Q07 2m42, Q09 3m01, Q02 1m18, Q08 1m39.

The May attempt proved the enumeration itself is not the hard part. Bushy
plans produced **wrong rows** because the physical layer's column resolution
assumes left-deep nesting. This memo is therefore two designs in one:

- **Layer A (the enabler, most of the work): structure-independent column
  resolution** — make join-key and alias resolution correct for arbitrary
  tree shapes, as a zero-behavior-change refactor gated on existing plans.
- **Layer B (small): bushy DP enumeration** behind a dormant flag, activated
  only after Layer A holds.

## 2. Why the May attempt broke — verified failure map (grounded 2026-07-09 vs main 93e137f)

A logical `NodeJoin` carries `JoinCond` as a raw SQL string. Conversion to
physical build/probe keys is a 4-step guessing pipeline, and every step
assumes the build side (`Children[1]`) is a single base scan:

| Step | Location | Left-deep assumption | Bushy failure |
|---|---|---|---|
| `parseJoinKeys` | physical/plan.go:4810 | positional split on `=`; side assignment deferred entirely to later repair | none by itself, but all repairs downstream inherit its guesses |
| `fixJoinKeyOrder` | physical/plan.go:4842 | each equality has exactly one column resident in the build subtree; swap fires only on `leftInBuild && !rightInBuild` | multi-table build → both sides resident → no/false swap |
| `collectPlanColumns` | physical/plan.go:4864 | unions ALL scans under `Children[1]` (special-cases only semi/anti) | inner-join build leaks several tables' columns into the membership set |
| `findScanAlias` → `BuildTableAlias` | physical/plan.go:6978, 3481, 3928 | one scan under the build | returns the first scan's alias in DFS order; all other build tables' aliases are lost |
| `joinOutputSchemaWithMapping` | exec/join.go:3190 | one `buildAlias` valid for every build column | stamps the wrong alias onto other tables' columns (e.g. region cols shipped as `n2.r_regionkey`) |
| `markCoPathingSelfJoinBuilds` | physical/plan.go:2678 | build dep chain reaches a `StageScan` within 3 Exchange hops | build dep = join stage → walk bails → Q07-class self-join qualification silently disabled |
| `columnIndexFallback` | exec/aggregate.go:693 | unqualified suffix match unique in schema | multi-table build → ambiguous → −1 → zero/wrong rows |
| `FixKeyAssignment` / SMJ counterpart | exec/join.go:1707, sort_merge_join.go:230 | runtime swap on the same asymmetric-membership rule | same blind spot as `fixJoinKeyOrder`, now against the real schema |
| `resolveShuffleKey` | physical/plan.go:3073 | walks single-child chains only | breaks at a 2-child build; Project aliases inside a bushy build unresolved for shuffle keys |
| `findScanRowEstimate` | physical/plan.go:6870 | first scan represents the build | mis-sized arena; wrong semi/anti swap threshold |

Two facts make Layer A tractable rather than a rewrite:

1. **The dataflow layer already runs bushy trees.** `buildJoin` recursively
   builds `Children[1]` pipelines, `walkStages` records per-child leaf stages
   and wires a join-stage build dependency generically (plan.go:3322-3345 —
   nested-join support is explicitly claimed there), and
   `ValidateNativeDAGShape` checks dependency counts only. Semi/anti
   subtrees-as-leaves ALREADY put join subtrees on the build side in
   production. Only the *naming* layer is wrong.
2. **The logical optimizer already knows the truth.** At reorder time it has
   per-relation column sets (`collectSubtreeColumns`) and the edge list — it
   knows exactly which relation owns each key. The information is discarded
   into a flat SQL string and then re-guessed downstream. The fix is to stop
   discarding it.

The strict-columns binder (PRs #151-#153, physical/validate.go) was assessed
as a resolution source and rejected: it runs pre-plan on the SQL AST, keeps
nothing (returns only pass/fail), and deliberately goes lossy on ambiguous
scopes. Its per-alias column-set idea is the right shape, but the resolver
must live where the tree is, in the planner.

## 3. Design

### 3.1 The invariant (Layer A)

> **Every column in a join-output schema has exactly one stable, unique name,
> derived from its ORIGIN scan (alias-qualified where needed), independent of
> join-tree shape. Plan-time join/shuffle keys are emitted in exactly that
> naming, with sides already assigned. Runtime name repair becomes a safety
> net, not a resolution mechanism.**

Three changes carry the invariant:

**(a) Origin-aware output naming in the executor.**
`joinOutputSchemaWithMapping` stops stamping the single `BuildTableAlias`
onto every build column. Instead: a build column that already carries a
qualifier (because the build subtree is itself a join that qualified it)
keeps its name verbatim; only bare columns from a scan-shaped build get the
build alias. Qualification remains conditional exactly as today (`isDup ||
qualifyAllBuildCols`) so left-deep output schemas are byte-identical —
this is a strict generalization, not a rename.

**(b) A subtree output-naming resolver in the physical planner.**
New helper `subtreeOutputName(node, col) → qualified name` (and its
companion `subtreeOwnsColumn`) that computes, for any logical subtree, the
name a column will carry in that subtree's OUTPUT schema — recursing through
joins with the same qualification rule as (a), through Projects/CTE aliases
(subsuming `resolveShuffleKey`'s single-child walk), and through semi/anti
probe-only visibility (subsuming the special case in
`collectPlanColumnsRec`). This is the one place tree-shape knowledge lives.

**(c) Plan-time key resolution replaces guessing.**
`buildJoin` and `walkStages` resolve each parsed equality against
`subtreeOwnsColumn(Children[0])` / `(Children[1])`:
side assignment is decided by ownership (error loudly on
both-sides/neither-side instead of silently keeping positional order), and
the emitted key strings are the exact `subtreeOutputName` of each side.
`fixJoinKeyOrder` is deleted from these paths. Runtime `FixKeyAssignment`,
`columnIndexFallback` fallback tiers, and SMJ counterpart adoption stay as
safety nets, with a WARN log when they actually fire (they should not, on
any plan — the log is the regression tripwire).

**(d) Structure-independent co-pathing qualification.**
`markCoPathingSelfJoinBuilds`'s build-chain walk continues through join
stages (collecting ALL underlying scan tables of the build subtree, not
bailing at the first non-scan), so self-join qualification engages for bushy
builds. With (a), qualifying "all build cols" is per-origin correct.

**(e) Structure-independent build estimate.**
The `findScanRowEstimate(Children[1])` uses (arena sizing, semi/anti swap
threshold plan.go:3946) switch to `estimateSubtreeStats` — which already
exists and handles join subtrees.

Layer A changes NO plan shapes. Gate: golden snapshots
(`testdata/ensure_distribution/qNN.golden`), `TestTPCHSelfJoinAliases`,
TPC-H SF0.01 22/22, harness `--mode=local` — all must pass unchanged.
New tests: a plan-shape-independent resolution suite that constructs bushy
logical trees directly (bypassing the reorderer) and asserts key/alias
resolution + row correctness on self-join and multi-dimension shapes — this
is the test bed that would have caught the May failure at unit level.

### 3.2 Bushy enumeration (Layer B)

Extend `dpJoinReorder` with the standard subset-partition transition:

```
for each connected subset S (by increasing popcount):
  dp[S] = min over ( existing left-deep extension,          — unchanged
                     partitions (S1, S2) of S, S1 ∪ S2 = S, both dp-reachable,
                     edges(S1, S2) non-empty:
                       dp[S1].cost + dp[S2].cost + hashJoinCost(probe, build) )
```

- Probe/build orientation per pair chosen by estimated rows (larger side
  probes), matching the existing 2-way rule.
- Join condition = conjunction of all edges crossing the (S1, S2) cut, each
  equality emitted side-assigned via the ownership map (Layer A c) — the
  optimizer writes truth, not a string to be re-guessed.
- **Tie-break prefers left-deep**: with Selinger floored at max(L,R), FK→PK
  chains cost identically bushy or left-deep (observed in the May attempt);
  requiring strict cost improvement for a bushy partition minimizes plan
  churn and keeps goldens stable except where bushy genuinely wins.
- Complexity: subset-partition DP is O(3^N). Bushy enumeration only for
  N ≤ 10 (3^10 ≈ 59K transitions, negligible); 11-16 relations keep
  left-deep DP; >16 keep greedy. TPC-H maxes at N=8 (Q08).
- Cross-join penalty, `hasEmptyJoinCond` greedy fallback, and CTE-ref bail
  are unchanged.

Flag: `--bushy-join-reorder` (bool, **default off** v1) plumbed like
`--late-materialization`: wadjet.Config + coordinator.Config + server.Config,
env `WADJET_BUSHY_JOIN_REORDER` for the bench harness. Off = today's
behavior bit-for-bit. Observability: `BushyJoinsPlanned` counter + one Info
log per query naming the partition chosen (mirrors SortMergeJoinsPlanned /
DynamicFiltersPlanned — the same-window A/B discipline requires a mechanism
marker proving the treatment engaged).

### 3.3 Scope exclusions (v1)

- Semi/anti joins stay leaf relations (current `flattenJoinChain` handling).
  Reordering them into bushy positions is a follow-on.
- Outer joins: never reordered (unchanged).
- No cost-model changes. Bushy inherits Selinger + HLL/histogram inputs
  as-is; if estimates are wrong, left-deep is equally exposed today.

## 4. Phases and gates

1. **Phase A — resolution refactor** (no flag; zero behavior change).
   Gates: all existing plan snapshots byte-identical, TPC-H SF0.01 22/22,
   full planner/exec/coordinator/worker suites, harness `--mode=local`,
   new bushy-tree resolution unit suite green, FixKeyAssignment WARN
   tripwire silent across the whole TPC-H run.
2. **Phase B — enumeration behind dormant flag.**
   Gates: flag-off = plans byte-identical (dormancy assert on the counter);
   flag-on forced SF0.01 22/22 row-identical vs flag-off; plan dumps for
   Q05/Q07/Q08/Q09 inspected — expected shapes: dimension-chain pre-joins
   (supplier⋈nation⋈region class); harness `--mode=local` both arms.
3. **Phase C — distributed validation + perf.** SF10 pair, then SF100
   same-window pair (deploy approval per run). Success = target-band wins
   (Q05/Q07/Q09) with no per-query regression beyond the ±15% noise
   envelope, counter proving bushy plans actually fired. Default flip is a
   separate decision after the pair, per the morsel/late-mat precedent.

## 5. Risks

- **Plan-space risk**: bushy opens shapes where cardinality-estimate error
  does more damage (a bad bushy pick materializes a large intermediate as a
  build). Mitigations: strict-improvement tie-break (§3.2), HLL/histogram
  stats already landed, dormant flag + A/B before flip.
- **Golden churn**: Phase B flag-on will change snapshots for queries where
  bushy wins — deliberate, reviewed per query in plan dumps, not bulk
  `-update`.
- **Hidden name-repair dependencies**: some correct left-deep plan may today
  be *rescued* by FixKeyAssignment/counterpart adoption firing. Phase A's
  WARN tripwire across TPC-H + harness will surface any such case before
  the safety nets are demoted.
  **CONFIRMED (Phase A, 2026-07-09): Q02's decorrelated scalar-subquery join
  is exactly such a case** — after decorrelation, BOTH children contain
  partsupp and supplier scans, so bare `s_suppkey`/`ps_suppkey` are owned by
  both sides and plan-time assignment (old membership test AND new ownership
  test) correctly stays conservative; the runtime repair fixes it, as the
  long-standing comment in FixKeyAssignment records. Root cause: the flat
  JoinCond string loses the query-block scope of each column reference. The
  architectural fix is the decorrelator emitting side-assigned keys
  (LeftKeys/RightKeys on the Node instead of a re-parsed string) — deferred,
  tracked as a Phase B+ follow-on. Until then the tripwire gate reads: NO
  repair other than the known Q02 pair, and the bushy unit suite asserts
  ZERO repairs on bushy shapes.
- **Memory**: a bushy build is a join output, so build size = intermediate
  cardinality, not base-table size. Spill machinery (grace partition-on-
  arrival) already covers oversized builds; `estimateSubtreeStats` (Layer A
  e) keeps admission estimates honest.
