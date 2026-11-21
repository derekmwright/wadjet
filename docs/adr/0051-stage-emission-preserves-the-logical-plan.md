# ADR-0051: The stage DAG refines the logical plan; it does not rewrite it

- Status: **Direction accepted** by the maintainer (2026-11-21); **stage R0 (test seams only) authorized and landed**; **NOT build-ready beyond R0**. What is and is not decided:
  - Accepted (2026-11-21), the contract as the maintainer worded it: "Stage emission preserves the logical plan's observable semantics: producer output bindings, expression evaluation boundaries, NULL-extension behaviour, row multiplicity and required projections. It does not independently inline, duplicate, omit or relocate expressions across relational boundaries unless an explicitly validated semantics-preserving transformation authorizes it." (§1.)
  - Authorized (ruling D1, 2026-11-21): "R0 approved. Proceed with the test-only foundation: PostgreSQL counterexample corpus, the existing distributed-plan-on-single-process diagnostic seam, differential reporting, and an exact checker if it can be established within the bounded stage. No production execution or routing changes are authorized by R0." What R0 landed is in "Stage R0, as landed" below.
  - Decided (ruling D2): "Binding strategy deferred. Do not choose between reusing the existing binder and a second binding pass yet." T1 (§2.6, §7) stays open; R0's evidence for it is in §7.
  - Decided (ruling D3): "Checker remains test-only. Do not use it as a production routing or refusal mechanism before the repair stages." This decides T4.
  - Decided (ruling D4, plan-dependent errors): "PostgreSQL remains the semantic reference. We do not need to reproduce every PostgreSQL optimization choice solely to match whether a particular expression is evaluated. Where Wadjet returns rows, the values and observable semantics must be correct. Where safe execution cannot be established, an explicitly documented refusal may be an acceptable support boundary. ADR-0013 may record narrowly justified per-statement dispositions, but the PostgreSQL reference result must remain visible. Do not weaken the oracle, silently rebaseline tests, or classify ordinary implementation defects as accepted compatibility differences. Proposed permanent refusals of mainstream analytical SQL still require my approval." This record proposes no ADR-0013 disposition: the R0 corpus keeps PostgreSQL's exact result and SQLSTATE for every error-domain cell and pins Wadjet's base verdict beside it.
  - Not decided, and not approved by R0: every stage after R0 (§6). The independent judge's remaining findings bind them and are listed, by owing stage, in "Remaining findings" below.
- Deciders: Derek Wright
- Related: [ADR-0047](0047-a-column-reference-has-one-resolved-identity.md); [ADR-0050](0050-a-coordinator-statement-plans-under-one-authorization-state.md) §1.3–1.5 and "What is out"; [ADR-0026](0026-a-group-key-has-one-identity-and-one-name.md) §5, §8j; [ADR-0013](0013-correctness-gates-and-their-boundaries.md) classes 1–4, 9–11; [ADR-0012](0012-sql-semantics-authority.md) item 7; [ADR-0024](0024-decimal-is-finite-fixed-point-with-postgres-result-types.md) §11; [ADR-0033](0033-a-column-policy-is-a-plan-time-projection-at-the-scan.md); [ADR-0035](0035-mergeable-aggregate-states.md); [ADR-0037](0037-one-module-two-licenses.md) decision 6; `docs/internals/sql-hidden-slot-namespace.md`; `docs/internals/native-dag-execution.md`. Epic #1678. The column-identity PLAN stages 4–9.
- Evidence: the crew notes of epic #1678 (r1, the judge's two records, r2, and R0's result note `1678-r0_r1.md` with `1678-r0_evidence/`); in the repository, R0's kept tests and their testdata (`internal/coordinator/r0_*_test.go`, `internal/coordinator/testdata/r0_*`).
- Labels: **[obs]** observed by a command in the cited evidence (R0's: a kept test); **[src]** verified in source at file:line on 33560b12; **[inf]** inferred; **[unres]** unresolved; **[open]** a tradeoff the maintainer decides. No checker exists (§4.3 says why R0 could not build one), so every verdict about one is **[inf]**.

## Stage R0, as landed (test-only; no production file changed)

All of it is `_test.go` code and testdata in `internal/coordinator`; `git diff --stat 33560b12` lists no non-test Go file.

- **The corpus** (`TestR0Corpus{Own,Uniq,Dup}`, `testdata/r0_corpus_cells.tsv`, `_pg17.tsv`, `_pins.tsv`). 172 distinct statements (r7's verdict table, the first challenge's 54, r1's 17, the judge's 20, r2's 18 error-domain cells, the judge's formal pairs; every alias kept), 230 statement-fixture rows, six arms (single, spilled512k, dag, dag-shuffled, dag-morsel4, localD), all over pgwire. PostgreSQL 17.11 (`postgres:17-alpine`, C/C, UTF8) measured by the kept test itself from the committed fixtures, with column names, declared OIDs and typmods, every value's text, and the SQLSTATE and message of every error. The comparator compares names, declared types, value text, order where the statement's ADR-0013 mode orders it, and SQLSTATE; `TestR0ComparatorRejectsEachDifference` shows each can fail. Every one of the 1,380 cells is pinned with its base verdict and a class (emission, logical, executor, error-domain, capability); a pin of "wrong" is a recorded defect on #1678, not an acceptance, and a cell moving in either direction fails the test. Two cells flap (spilled512k, `h2_base2_control`, `memory budget exceeded`: the #1706 class) and are reported, not pinned.
- **The localD seam** (`r0_locald_test.go`, `TestR0LocalDSeamRunsD`). The statement is planned with ExecuteSQL's preamble and `planStatement(stamp=false)` (the D the stage planner receives) and executed with `runRefusedLocal`'s mechanics. D is deep-copied before any physical planner runs (a reflection deep copy; the copy is checked to share no pointer, map or slice with the executed plan, and a shallow copy is shown to fail that check); options, the manifest snapshot's pinned tables and revisions, and the policy state are recorded. A statement whose D carries a subquery, or whose stage graph has a deferred scalar producer (D'), is reported "late-subplan", never established; a statement the DAG would route is reported apart. Guards against drift of the copied preamble: D's text equals the plan text the dag arm's own ExecuteSQL reports, on every unrouted corpus cell, and the plan localD executed carries no binder stamp (with the test-only fault that plans L instead, that guard fires on 78 statements).
- **The differential, report mode.** Corpus (in the pins and the report), TPC-H SF0.01 (`TestR0TPCHDifferential`, `testdata/r0_tpch_pins.tsv`: 19 agree, 3 localD not established — Q11, Q15, Q22 carry a deferred scalar producer), and 18 existing coordinator arm tables run by name through a test-only hook (`r0_armdiff_test.go`; 37,705 DAG-arm cells: 12,352 agree with localD, 19,319 routed, 1,530 refused alike, 2,511 not planned, 1,398 localD not established, 595 differ, of which the volatile-CTE and TABLESAMPLE tables' 135 are run-to-run nondeterminism, not evidence; one table, WhereCompareScalar, is partial: its own 10-minute timeout fired with the hook doubling its work).
- **The binding census and switch** (`TestR0BindingCensus`, `TestR0BindingSwitch*`): §7, T1.
- **Cost baselines** (`testdata/r0_cost_baseline.tsv`) and the judge's partition probe as a test (`TestR0PartitionPruningThroughDerivedAlias`: 2 files / 888 B to 1 file / 452 B through a derived alias).
- **The exact checker: not built.** §4.3 gives the measurement that stopped it.

## Context

### What goes wrong, measured

The stage DAG answers differently from the optimized logical plan `D` it lowers. r1 and the judge measured four forms on PostgreSQL 17.11 (`--locale=C`): a reference re-spelled into another relation's column (S1, C2L, `cascade`); a non-strict definition recomputed above an outer join's padding (`outer_nonstrict_rows`, `join2_outer_coalesce`, the judge's `gk_outer_expression` and `gk_outer_having`); an expression evaluated twice (`group_expr`); an expression omitted (`union_cast_values`). The judge added `window_nested_order` (a window ORDER BY over a computed item binds the wrong value) **[obs, judge probe 3]**.

r2 adds a fifth form, the **evaluation domain**: the DAG evaluates an expression on rows other than those its logical node receives. Over 18 statements on five arms against PostgreSQL 17.11 (`1678-re_evidence/r2/error_domain_matrix.tsv`) **[obs]**:

| Class | Cells | What happens |
|---|---|---|
| DAG differs from `D` run locally, which is PostgreSQL's answer | `e_derived_limit`, `l_error_over_limit3`, `e_conj_order_a` | A derived Project below a LIMIT (or ORDER BY … LIMIT) is emitted **above** the Limit and the outer Filter (`project-3` over `limit-1`; `sort-1` with `FilterExprs` and `ProjectExprs` after `Limit`), so `10/(id-1)` never meets `id=1` and the DAG answers where PostgreSQL, single and localD raise 22012. In `e_conj_order_a` the DAG applies the conjuncts in written order and raises where the others answer. |
| DAG is PostgreSQL's answer, `D` run locally is not | `e_cte_once` (the judge's `cte_error_filter`) | The same mechanism as the row above (Project emitted after the Filter on `scan-0`) happens to agree with PostgreSQL, which inlines a single-reference CTE and filters first. Single and localD raise. Filed on #1678 (P2, engine). |
| `D` and the DAG both differ from PostgreSQL | `e_derived_offset0`, `e_derived_distinct`, `e_inner_join_arm`, `e_left_join_strict`, `e_union_arm` | Logical or single-process: every arm agrees with every other arm. |
| All agree | 8 cells, including `e_cte_twice` and `e_left_join_nonstrict` (all raise, as PostgreSQL) | — |
| Routed | `l_filter_over_limit_computed` | The DAG arms route to `L` (`UnreachableOutputLocalRoutes`) and answer; not DAG evidence |

PostgreSQL's own outcome follows its plan, not the text: it answers `e_left_join_strict` (`EXPLAIN VERBOSE` evaluates the strict `10/(tb_bd.id-1)` at the Hash Left Join's output) and raises on `e_left_join_nonstrict` (the non-strict `COALESCE(...)` is computed at the `tb_bd` scan); it applies `id > 1` before `10/(id-1) > 0` whichever is written first (`pg_explain.txt`) **[obs]**. So the emitter cannot tell which domain change is safe: the same move is PostgreSQL's answer in `e_cte_once` and not in `e_derived_limit`. Only the logical plan can carry that decision.

### Why the emitter does it

A logical `Project` emits no stage (`dagplan/stage_emission.go:1568`) **[src]**. Consumer sites re-spell their references into the Project's input spelling with the logical optimizer's adjacent Filter–Project substitution (`projRefs`, `substituteColRefs`), across joins one arm at a time (r1 §2.1) **[src]**; and six opportunistic passes decide alone where a Project's output is materialized, which is how a Project lands above a Limit, a Sort or a Filter (r1 §2.2 P1, P2, A1, A2, W2, S1) **[src-d; r2 obs for the Limit and Sort cases]**. Of r1's five candidate reasons only the structural one holds for the instrumented families. Narrowed per the judge (probe 5): the DAG worker does no static row-group pruning from a stage's `FilterExprs` (`dag_scan.go:476-485`, `executor_fragment.go:2740-2763`) **[src]**, but the coordinator **does** prune partitions statically before dispatch (`stage_emission.go:113-121`; 2 files / 888 bytes to 1 file / 452 bytes through a derived alias) **[obs, judge]**. No pushdown benefit of the crossing inlining was observed for F1, F2/F2b and F3 on r1's fixture; A1, A2, K1, S1, W1, W2 and U1 were not censused and SF1 was not run **[obs / unres]**.

### Why column identity alone does not repair it

`outer_nonstrict_rows` has one `v` and is still wrong: the definition was moved, not mis-bound. `window_arm` (r1 §4.5) shows a kept spelling binding the wrong slot. And the domain cells above involve no name at all.

## Decision

### 1. The contract

**As the maintainer worded it.** Stage emission preserves the logical plan's observable semantics: producer output bindings, expression evaluation boundaries, NULL-extension behaviour, row multiplicity and required projections. It does not independently inline, duplicate, omit or relocate expressions across relational boundaries unless an explicitly validated semantics-preserving transformation authorizes it.

**Made precise.** Five definitions and one rule.

**1.1 The specification is a frozen plan family.** When the stage planner starts, `D` is deep-copied and frozen with its options, its policy state and its snapshot; the physical planners mutate plans (`statement_plan.go:63-66`, `:168-170`) **[src]**, so no later run may share its nodes. Each deferred scalar subplan built by `emitScalarProducerStagesTyped` (`dagplan/subquery_resolution.go:607-687`, policy at `:667` and `:677`) **[src]** is its own frozen plan `D'`, frozen after its own policy application and optimization, with a link to the root slot it substitutes. The family `{D, D'1, …}` is the specification; each member is checked on its own.

**1.2 Reference semantics of a frozen plan.** Each node is evaluated bottom-up over the **whole output** of its child. A node's **domain** is the set of rows (for a join, candidate pairs) its child delivers to it. Inside a node:
- A Project evaluates every item on every row of its domain.
- A Filter applies its conjuncts **in the order `D` states**, each on the rows that passed the earlier ones. A join applies its key conjuncts, then its residual on key-matching pairs. A scan applies `D`'s scan-level conjuncts before the Filter above it.
- An aggregate evaluates an argument on the rows of its group that pass its FILTER; CASE evaluates a branch only on rows that select it.
- A volatile expression is evaluated once per row of its domain; a WITH item that `CTEDef.EvaluatedOnce` marks is evaluated once per statement (`sql/volatile_cte.go:165-175`) **[src]**.

`D` today does **not** state conjunct order: the local physical planner applies scan-pushable `col <op> literal` conjuncts in the scan first (`physical/scan_filter_pushdown.go:7-12`, `:84`) and the DAG applies `FilterExprs` in written order **[src]**, which is `e_conj_order_a` **[obs]**. Stating it is obligation **E-ORD** on the logical plan (§5), a prerequisite of stage R-dom.

**1.3 Observables.** The client-observable result of a statement:

| | Observable | Protected at the stage-emission boundary? | If not, whose |
|---|---|---|---|
| O1 | Published column names, in order, duplicates allowed | Yes: the DAG publishes `D`'s root names | — |
| O2 | Declared types and typmods; per-value decimal display scale where the engine carries one; the zero-row description | Yes: the DAG declares what `D`'s root declares and carries each value's scale unchanged | Whether `D` declares PostgreSQL's types and scales: ADR-0024 and the numeric gates |
| O3 | Row multiplicity (the bag) | Yes | — |
| O4 | Row sequence where `D` orders it | Yes, through every exchange and the final merge | Which permutations are legal: ADR-0013 classes 2, 3, 4 per statement |
| O5 | Values | Yes, except ADR-0013 classes 9 and 10 recorded per statement | ADR-0013 |
| O6 | Error outcome: whether the statement raises, and its SQLSTATE | Yes: every expression is evaluated on exactly its domain under 1.2 | Whether `D`'s domains give PostgreSQL's outcome: the logical plan (§5, E-DOM) |
| O7 | Volatility and effects: evaluations per domain row; one evaluation of an `EvaluatedOnce` item | Yes | — |

The contract protects O1–O7 **relative to `D`**. It does not make `D` right: where `D` is not PostgreSQL's answer the defect is logical, and §4.2 says what the gate does then.

**1.4 The rule.** The executable graph `G` is everything that runs for the statement: `PlanDistributed`'s stages, the dispatch-time rewrites (AVG decomposition `coordinator/avg_decompose.go:23-37`, ordered-gather selection `dag_gather.go:41-49`, broadcast, probe and skew splits) and the coordinator's merge (`mergeProbePartials`, `coordinator.go:2045-2074`; `applyOutputRenames`, `dag_merge.go:16-27`) **[src]**. `G` preserves `D` when, for every database state, `G`'s O1–O7 equal those of `D` under 1.2, up to the ADR-0013 classes recorded for the statement; and every difference between the shape of `G` and the shape of `D` is an instance of a refinement rule in §1.5. Anything not in §1.5 is prohibited. Three consequences, stated so a checker can test them:
- **E1, domain.** Each evaluation of a definition reads the definition's own input slots and runs on a subset-or-equal **and** superset-or-equal of its domain, that is, on its domain exactly. Moving a Project above a Filter, Limit, Sort, Join, Aggregate, Window, Distinct or set operation changes its domain and is prohibited in emission.
- **E2, repetition.** A non-volatile definition may be evaluated more than once, each time on its exact domain and from its own input slots (cost only: the error outcome and the value are those of one evaluation). A volatile definition is evaluated once per domain row. This retires r1's T2: the adjacent composition is legal only when it leaves the domain unchanged, and the judge's composed form (`t2_composed`, which evaluates `10/(id-1)` only on `id > 1` rows) is a domain change and prohibited **[obs: t2_oracle.json]**.
- **E3, order.** Conjuncts are applied in `D`'s order (1.2); a stream `D` orders stays ordered through every exchange the order must cross.

A logical optimizer transformation may change domains, move, inline or eliminate expressions, on the logical plan, under its own legality rule, visible in `D`. It is authorized exactly when it is in `D`. Whether it matches PostgreSQL is that rule's own obligation, not this record's.

**1.5 The refinement rules (closed list).** Each rule names what it may change, its proof obligation, and its test. The checker (§4.3) accepts a difference between `G` and `D` only as an instance of one of these.

| Rule | May | Proof obligation | Test: existing / required |
|---|---|---|---|
| RR1 Partial/final aggregation and re-aggregation | Split an aggregate into partial, exchange and final, and re-aggregate at the coordinator. AVG becomes state slots `__avg_sum#out` and `__avg_count#out` at dispatch (`avg_decompose.go:23-37`), folded by name (`worker/avg_fold.go:34-65`) **[src]** | State law per function: `final(merge(partial(X1)…partial(Xn))) = agg(X1 ∪ … ∪ Xn)` with the declared result type and scale (ADR-0035). DISTINCT and whole-input aggregates do not split: they keep `RawInputAggregate` (`stage_emission.go:474-505`) **[src]**. State slots are refinement slots (§2.3), never visible above the aggregate | Required: a per-function state-law table over a split corpus, including decimal AVG scale; the judge's `avg_distinct_filter` control **[obs]** |
| RR2 Set-operation counting | Lower INTERSECT/EXCEPT [ALL] to arm tags `__setop_lcnt`/`__setop_rcnt` and a grouped SUM over the full result row (`set_op_stages.go:14-32`, `:123-144`) **[src]** | The count rule of each operator over multisets, NULLs grouped as equal, at the common type (§2.3) | Required: all six operators × duplicates × NULLs; the judge's `intersect_all_computed` and `except_all_project` agree **[obs]** |
| RR3 Exchange insertion and key choice | Insert repartition, replicate and gather exchanges; choose shuffle keys | Every row reaches exactly one consumer partition (all of them for replicate); equal key values hash equal under the key's declared type (int widths, decimal scales, NULL). Key slots are the consumer's key slots, compared by slot, not by key string (`distribution.go:58`, `:154` today) | Required: typed hash-equivalence table per key type pair |
| RR4 Broadcast, probe split and skew split | Replicate a build; divide probe files; split a skewed group | Every probe row in exactly one task; every build row in every task that needs it; a volatile or effectful expression is never evaluated on a replicated input (materialize first). A non-volatile one may be (E2), and the repetition is counted (§4.5) | Required: a volatile expression on a broadcast build; skew split on and off agree |
| RR5 Fusion | Fuse stages and exchanges | The composed slot maps; no expression text rewritten (r1 X1 **[src-d]**) | Required: checker over fused and unfused plans |
| RR6 Dynamic filters | Prune a probe leaf by a build's key set | The pruned side's dropped rows are rows the join drops anyway: INNER and SEMI only. A LEFT join's probe is its preserved side, so admitting `"left"` (`dagplan/dynamic_filter.go:68`, filter consumed by the probe leaf at `:141-146`) **[src]** violates this rule by reading **[inf]**. The key slot must have storage lineage to the probe scan through exchanges only (`findLeafScanStage`, `:156-182`) **[src]** | Owed by R0: an engaged LEFT measurement (r1's `df_left` did not engage **[obs]**). Then the LEFT admission is a line on #1678 |
| RR7 Ordered gather and final merge | Gather with a merge-sort and apply the limit worker-side; re-sort, dedup and re-aggregate at the coordinator | O4: an exchange whose input `D` orders and whose consumer needs the order is an order-preserving merge, and LIMIT follows the merge. The judge's permuted-exchange pair (`formal_oracle.json`) is the counterexample this excludes **[obs]** | Required: ORDER BY … LIMIT through broadcast probe split (the #288 shape) on every arm |
| RR8 Security barrier absorption | Move a barrier Project into the scan's `SecurityProjectExprs` (`stage_emission.go:1645-1656`) | The policy order of ADR-0033: no user predicate sees an unmasked value (`CheckPolicyPlanOrder`) | Existing: the nine-door masking gate (PLAN:244, :331), reassigned to R2 and R6 |
| RR9 Scan projection and partition pruning | Project files to the columns read; prune partitions from `PartitionFilter` | Pruning metadata derives from `D`'s scan conjuncts through storage lineage (§2.4), so a slot change never drops it | Required: the judge's partition probe kept as a test (2 files / 888 B to 1 file / 452 B) |
| RR10 Late subplan substitution | Substitute a `D'` scalar result as a literal | `D'` checked on its own; the literal reads back at the root slot's type (`subquery_resolution.go:603-606`) **[src]** | Required: one corpus row per policy door through a scalar subquery |
| RR11 Repetition (E2) | Evaluate a non-volatile definition again on its exact domain | E1 holds for every evaluation | The repeated-evaluation counter (§4.5) |

**1.6 "An explicitly validated semantics-preserving transformation".** It is a named logical optimizer rule in MIT `internal/planner/logical`, applied before `D` is frozen, with its legality conditions written in the rule (strictness under outer joins, volatility, evaluation domain, the relation each reference reads), covered by the kill-switch invariance oracle (`docs/testing/GATES.md`), and recorded in the optimizer's per-rule trace so the checker can tell a move present in `D` from one introduced after it. There is no stage-level escape beyond §1.5. An emitter habit worth keeping becomes such a rule or goes.

### 2. Slots, occurrences and name spaces

**2.1 Slots.** When `D` is frozen, every node instance in the copy is an **occurrence** with an id; every output position is a slot `(occurrence, ordinal)`. Every node has its own slots, including Filter, Sort, Limit and Distinct, whose slots are **pass-through** (same value, defined as "input slot i"). A reference is resolved to a slot of the node **directly below** its consumer; pass-through chains are never collapsed across a node boundary. A CTE referenced twice is two occurrences with distinct slots in `D`, even when emission later shares one stage for both (`stage_emission.go:34-102`) **[src]**.

**2.2 Per boundary.**
- **Join.** `J.out` is a NEW slot list with an explicit map per edge: `mL: L.out[i] → J.out[i]`, `mR: R.out[j] → J.out[|L|+j]`. A join slot is never its arm's slot: on an outer join's padded side it is the NULL-extended copy; on an inner join its domain is matched rows. A consumer above the join reads `J.out`, never `L.out` or `R.out`.
- **Semi/anti.** `J.out = mL(L.out)`. The join's own key and residual read `R.out`, inside the join's evaluation; `R.out` is not published.
- **USING/NATURAL.** A merged column is a join slot with a definition at a common type: the left copy for INNER and LEFT, the right copy for RIGHT, `COALESCE(l, r)` for FULL. The qualified copies stay join slots at their own types: measured on PostgreSQL, merged `numeric`, left `bigint`, right `numeric` (`formal_oracle.json` `using_types`) **[obs]**. Today Wadjet refuses that statement with 42702 on every arm (judge probe 3), a capability gap outside this record.
- **Aggregate.** Group-key **result** slots, then aggregate **result** slots. State slots exist only inside RR1.
- **Window.** Pass-through slots, then window result slots. PARTITION BY, ORDER BY and the frame read the window's input slots; the frame specification is part of the definition.
- **Set operation.** `S.out[i]` is defined per arm `k` as `coerce(A_k.out[i], T_i)` with `T_i` the common type (`physical/set_op_types.go`). For a deduplicating operation the printed value is the surviving representative's own display scale: measured, `UNION` of `2.00` and `2` prints whichever arm comes first (`r2/pg_setop_types.txt`) **[obs]**, which is ADR-0013 class 11 and ADR-0024 §11, not this record's.
- **Exchanges and fusion.** Carry the input's slot list; fused stages carry the composed map (RR5).
- **Root.** `D`'s root slots map to published names and declared types (O1, O2).

**2.3 Refinement slots.** RR1's AVG state, RR2's tags and any RR4 replica identity are slots of the rule, not of `D`. They keep their reserved protocol names (`__avg_sum`, `__avg_count`, `__setop_` are already reserved, `sql/reserved_slots.go:93-116`) **[src]**, and the checker verifies them by the rule's law, not by `φ`.

**S1, worked.** `WITH s AS (SELECT id, v AS r FROM tb_d) SELECT count(*) FROM s a JOIN s b ON a.id = b.id WHERE a.r <> b.r`. `D` has occurrences `Pa`, `Pb` (the two references) and `J`; `J.out = [mL(Pa.id), mL(Pa.r), mR(Pb.id), mR(Pb.r)] = [J0, J1, J2, J3]`; the Filter's leaves denote `J1` and `J3`. Emission shares `scan-0` for both arms and writes `v != v` on `join-2` **[obs, r7 §A]**. Resolving each leaf as the worker will (exact match on the join stream, whose probe columns are bare, `exec/join.go:4850`) gives the probe's `v`, which is `J1`, for **both** leaves. The second leaf denotes `J3` and reads `J1`, so R-b (§4.3) rejects the plan **[inf, no checker]**. The shared stage does not hide it, because the comparison is made at `J`'s edge maps, not at the producer's origin.

**2.4 Three name spaces, kept apart.**

| Space | What lives there | Consumers that read it today **[src]** | Map |
|---|---|---|---|
| STORAGE | Parquet and catalog column names | File projection (`dag_task_inputs.go:274-302`, `build_cache.go:145`); statistics and NDV (`dagplan/dynamic_filter.go:228-242`), integer key type (`:315-334`); partition pruning (`stage_emission.go:113-121`); dynamic-filter `KeyColumn`/`TargetColumn` (`dynamic_filter.go:131-146`, `worker/executor_fragment.go:778-784`) | **Lineage**: `slot → (table, storage column) | none`, computed on `D`; defined for a scan slot and for a pass-through chain to one with no domain-changing node between. A storage consumer asks lineage, never a spelling |
| CARRIER | Column names on intermediate streams: stage outputs, `.wshf` schemas, S3 materializations | `SchemaGuard` compares name, type, precision and scale per input (`wshf/schema_guard.go:70-82`); `.wshf` accepts any name (`wshf/decode.go:45-48`); AVG fold by prefix (`avg_fold.go:34-65`) | Each logical slot has one minted carrier name per stage output; refinement slots keep their protocol names (2.3) |
| PUBLISHED | Client names, declared types, display scale, zero-row schema | `applyOutputRenames` (`dag_merge.go:16-27`), the native path's `OutputSchema`; a route's `Schema` from its sink (`refused_local.go:117-127`) | Root slot `i` → `D`'s published name `i` and declaration `i` (L's on a route, ADR-0050 §1.3 item 5) |

The minted carrier family (`__s<occ>_<ord>`) lives in CARRIER only. It is **not** reserved today (`reserved_slots.go:93-116`) **[src]**: R2 adds it through the one existing mechanism (reserve the prefix, mint through `SlotAllocator`, refuse a user-minted collision with 42939, renumber against a stored collision via `renameCollidingSlots`; `reserved_slots.go:13-21`, `:136-161`) **[src]**. What each name-reading consumer does after R2: storage consumers read lineage (unchanged results where lineage exists, no optimization where it does not, never a guess); `SchemaGuard` compares carrier names as today; the AVG fold keeps its protocol; publication reads the root map. Carrier names never reach a client.

**2.5 The representation.** (A) slot ids from frozen `D` are the planner's semantic carrier; (B) minted carrier names rendered from (A) are the transitional wire encoding; ordinals (PLAN stage 8) replace (B) at R6. **Chosen** (r1's T5 is closed: it was listed both chosen and open). (C), numbering after emission, is rejected: the mapping would follow the mistaken rewrite.

**2.6 Where the reference-to-slot map comes from [open T1; deferred by ruling D2].** Option 1: bind `D` (with WHERE and ON, stage W). Option 2: a second binding pass over unbound `D`, which needs alias, USING, outer-scope and ambiguity resolution or a fail-closed result; it cannot come from tree positions alone when leaves are unbound text (judge, item 4). R0 measured what each option still owes (§7, T1); it does not choose.

### 3. Reconciliation (PENDING amendments)

None of the amendments below is made by this record: each describes a stage not yet built, and lands with the stage that changes the behaviour it describes. ADR-0047, ADR-0050, ADR-0026 and `docs/internals/native-dag-execution.md` are unchanged by R0.

**3.1 ADR-0050.**
- §1.3 item 3 (`D` unbound): Option 1 amends to "`D` is built bound … the veto's validation and `D`'s share one parse and binding, never one mutable plan"; Option 2 keeps it. Either way add: "`D` is deep-copied and frozen before physical planning (ADR-0051 §1.1)".
- §1.4: add "From ADR-0051 stage R5 on, a stage-DAG answer on a statement whose localD is PostgreSQL's answer equals localD's; elsewhere the difference is a logical or single-process defect and is tracked there." Not "from R3" (judge item 5): R4 still owes key paths then.
- §1.5 (routed cells): unchanged. T4 (§7) must not claim more than it says: a route gives the single-process answer, which is not PostgreSQL's on 24 group-key arm-cells (§1.5) and on 6 of r2's 18 error cells **[obs]**.
- "What is out", first bullet: per T1.

**3.2 ADR-0047.** "The DAG arms keep their base answers until stage 5" becomes "until ADR-0051 stages R2–R5 land; each moves DAG cells only to PostgreSQL's answer or to a counted route". Stage 2's `resolveAggInputName` row: "deleted where it crosses a relational boundary (R2), not converted to bindings".

**3.3 ADR-0026.**
- Line 141, "The DAG rows keep their base answers until ADR-0047 stage 5": "until ADR-0051 R2–R5".
- §5 (lines 1554-1559), "A name re-spelled for dispatch is TYPED where it was re-spelled TO": add "Historical from ADR-0051 R2 for the boundary-crossing sites: a reference is no longer re-spelled into its definition, so its type is its slot's declaration".
- §8j corollary 2: "TRANSLATED to the producer's **slot**, never to its defining expression across a relational boundary".
- Lines 2578-2587, "translate the reference to the body's carrier INSIDE the occurrence … `windowArgSourceInScope` composed with `derivedAliasSourceColumn`": "superseded by ADR-0051 §2.2: the reference resolves to the occurrence's slot; the alias-to-source walk is not composed further".

**3.4 `docs/internals/native-dag-execution.md`** (each sentence becomes historical as the stage that changes it lands):
- `:185`, Project "nothing — passthrough … every other consumer resolves them back to source names": "a Project is evaluated once, at a position satisfying E1 (ADR-0051 §1.4, R2)".
- `:431-456`, "eight resolvers, one convention … every stream carries SOURCE column names": "streams carry slot carrier names (R2); the resolvers that crossed a boundary are deleted".
- `:536-548`, "The resolver re-spells the predicate in place (it never MOVES it …)": "it does move it when it crosses a join (r1 F1), and changes the Project's domain when it lands a Project after the Filter (r2 `e_cte_once`, `e_derived_limit`); removed at R2 and R-dom".
- `:574-576`, "it STOPS at a Sort or a LIMIT": "the filter re-spelling stops there; a materialized Project does not (r2 `l_error_over_limit3`)".
- `:733-748`, "The DEFINITION is re-spelled into the arm's own stream spellings": "replaced at R4 by the occurrence's slot".

**3.5 The remaining column-identity stages.**

| Stage | Disposition |
|---|---|
| 4 "Join conditions travel as bound ASTs" (PLAN:255) | **Split.** The binder does not stamp ON today (`stampBlock` scope: SELECT items, GROUP BY, HAVING, QUALIFY, ORDER BY, `physical/validate_binding.go:296-301`) **[src]**, so stage 4 transports a binding nobody produces. **W** stamps WHERE and ON references in both validations; stage 4 then carries ON bindings through decorrelation, LATERAL and USING. W precedes stage 4's ON half |
| 5a | Landed, unchanged |
| 5b "the DAG plans the bound statement: NOT DECIDED" | One input to T1, not its decider (§7) |
| 5 "ordered output list … (dependency, ordinal)" | Subsumed by R1–R2: entries are occurrence slots. Its +2 % task-byte budget (PLAN:295) moves to R2 |
| 6 "follow a binding instead" | Replaced: a boundary-crossing walk is deleted, not converted. Its nine-door masking gate (PLAN:331) moves to R2. Its interim "keep the base resolution where consumers still read names" is struck |
| 6c "Exchange and distribution keys are identities" | RR3, in R2 |
| 7 per-column properties by position | Unchanged, after R2; its +2 % budget (PLAN:350) stays |
| 8 ordinal leaves, policy enforcement, text replanning | R6; unchanged in content |
| 9 retire and enforce | Adds the checker over `G` on every planned statement in the suites |

### 4. Acceptance

**4.1 The corpus and comparator.** A kept PostgreSQL-oracle table, arms `single`, `spilled512k`, `dag`, `dag-shuffled`, `dag-morsel4`, plus the diagnostic arm `localD`. PostgreSQL 17.11 answers stored with version, locale and collation. Each statement carries its comparison mode, chosen from ADR-0013: bag (class 1); ordered sequence with tie permutations (2); LIMIT compared by count or stripped (3); float digits (4, 9); window permutation property (10); representative display scale (11); a property for volatile statements (equality within one run, as the judge's `cte_volatile_shared`); otherwise exact. Every statement also compares published names, declared types and typmods, each value's text (display scale), and the error outcome by SQLSTATE. A comparator that compares numeric bags alone is not this gate (judge probe 7).

Mandatory rows by name: `outer_nonstrict_rows`, `join2_outer_coalesce`, `group_expr`, `group_negate`, `union_cast_values`, `cascade`, `cascade_reverse`, `ctes_limit_set`, `S1`, `C2L`, `pd_two_arm`, `window_arm` (right at base: the guard against a kept spelling), the judge's `gk_outer_expression`, `gk_outer_having`, `window_nested_order`, `cte_error_filter` (= `e_cte_once`) and its formal pairs (`t2_logical`, `t2_composed`, `error_project_before_filter`, `error_project_after_filter`: PostgreSQL-only references, since Wadjet refuses `AS MATERIALIZED` at parse, `sql/parser.go:1859-1862` **[src]**), the judge's controls, and r2's 18 error-domain cells. Base pins: r1 `verdicts.tsv`, the judge's `out.tsv`, r2's `error_domain_matrix.tsv`.

**4.2 What the gate compares.** PostgreSQL is the oracle. `localD` is a diagnostic that localizes a difference; it cannot override the oracle.

| DAG vs PostgreSQL | localD vs PostgreSQL | Meaning | Disposition |
|---|---|---|---|
| right | right | agree | Must stay right |
| wrong | right | emission defect | In this record's stages |
| right | wrong | logical or single-process defect; the DAG is right by an unauthorized move (`e_cte_once`) | Protected: no stage may move it to wrong. The checker flags the move; the cell sits on an exception list naming its logical repair, and the stage that would enforce the rule there depends on that repair (order of repair below) |
| wrong | wrong, same answer | logical defect | Not this record; must not move except to PostgreSQL's answer |
| wrong | wrong, different | both | Listed separately |

**Order of repair.** Where strict preservation of `D` would turn a PostgreSQL-right DAG cell wrong, the logical repair that makes `D` PostgreSQL's answer lands first, and only then does the emission rule apply there. For `e_cte_once`: the logical swap declines at every CTE tag because the single-process planner replays one cached result for all references (`optimizer.go:2261-2267`) **[src]**; a single-reference CTE needs no replay, and PostgreSQL filters first there and raises when the CTE is read twice (`e_cte_once` rows, `e_cte_twice` 22012) **[obs]**. That repair is obligation E-DOM-1 (§5), the #1678 P2 engine line. The single-process engine's other error-domain cells (`e_derived_offset0`, `e_derived_distinct`, `e_inner_join_arm`, `e_left_join_strict`, `e_union_arm`) block no stage here: the DAG is wrong on them before and after.

**4.3 The checker.** `CheckEmission(family, G, trace)` **[inf, not built]**. Inputs: the frozen family with occurrence slots (§2.1); `G` captured at dispatch, including the dispatch rewrites and the merge plan (§1.4), with each emitted expression node carrying the slot it evaluates and each leaf the slot it reads (provenance fields); the optimizer trace. It rejects:

| Rule | Rejects |
|---|---|
| R-a | A non-pass-through slot evaluated nowhere; a volatile one evaluated more than once per domain row (E2) |
| R-b | A leaf whose slot is not the slot its logical reference denotes, resolved at the consumer's own input (no origin collapse) |
| R-c | An evaluation whose domain differs from the slot's (E1): any Filter, Limit, Sort, Join, Aggregate, Window, Distinct or set operation between the evaluation and the slot's node that is not between them in `D` |
| R-d | A required slot absent from its consumer's stream |
| R-e | Conjuncts applied in another order than `D`'s (E3); an ordered stream crossing an exchange that is not order-preserving |
| R-f | A shape difference that is no instance of §1.5 |

**What R0 established about building it (observed; replaces the r2 draft's predicted table).** An exact checker needs, per emitted leaf, the slot the worker will read AND the slot the logical reference in `D` denotes. R0 measured the second half's raw material (`TestR0CheckerFeasibility`, over the corpus and TPC-H SF0.01, 188 planned statements) **[obs]**: `D` carries 975 column-reference nodes and **0** of them carry a binding (`D` is planned unbound, `auth.ValidateStatementColumns`); references `D` holds only as text (`Projection.Column`, `Predicate.Column`) carry none either. What each of them denotes is therefore not established from `D` without a resolver, and choosing one is T1, deferred by ruling D2. The bound plan `L` is a separate parse; its stamped references (SELECT list, GROUP BY, HAVING, ORDER BY; WHERE and ON never, §7) could be matched to `D`'s only by spelling or position, which is the guess this record forbids. The emitted side has 1,690 column leaves in the stage graphs' expression strings (39 strings not parsable as expressions); the emitted-reference resolver (deliverable 4(a): which stream column the worker will bind for each of them) was **not attempted** in R0. Candidate seams exist whose exactness against the worker is unmeasured **[inf]**: `expr.ResolveColumnRef` (`internal/engine/expr/expr_leaf.go:42-52`), exported so a caller can ask whether a reference resolves without evaluating it; `exec.JoinOutputSchema` (`internal/engine/exec/join.go:4835-4844`), an exported pure function of the two side schemas; and `dagplan.carrierInputColumns` / `unresolvableColumnRefs` (`internal/coordinator/dagplan/carrier_schema.go:11-14`, `:93-130`), which already model the input schemas of scan, aggregate, union and sort/limit/window/project carrier stages and mirror the runtime lookup. Stopping the checker does not rest on that half: the logical half alone leaves 975 of 975 references unbound, so the "intent not established" class covers every leaf of every named cell, and **no checker was built**: shipping a resolver over text under the checker's name is what the judge's item 6 excluded. Per named cell, R0's observed facts instead (arms: dag / dag-shuffled / dag-morsel4; "localD" is `D` run single-process; all against PostgreSQL 17.11):

| Cell | DAG arms | localD | `D` references (bound) | Checker |
|---|---|---|---|---|
| `outer_nonstrict_rows` | wrong (emission) | right | 4 (0) | not established: `D` unbound |
| `join2_outer_coalesce` | wrong; dag-shuffled refused 42000 (emission) | right | 5 (0) | not established |
| `group_expr` | wrong (emission) | right | 2 (0) | not established |
| `group_negate` | wrong (emission) | right | 2 (0) | not established |
| `union_cast_values` | wrong (emission; the wire declares float8 where PostgreSQL declares text: the values match, the type does not) | right | 3 (0) | not established |
| `cascade` | wrong (emission) | right | 6 (0) | not established |
| `S1` (dup fixture; on uniq every arm is right, vacuously) | wrong (emission) | right | 4 (0) | not established |
| `C2L` | wrong (emission) | right | 5 (0) | not established |
| `gk_outer_expression` | wrong (emission) | right | 4 (0) | not established |
| `window_nested_order` | wrong (emission) | right | 9 (0) | not established |
| `cte_error_filter` (= `e_cte_once`) | right | refused 22012 (error-domain; single refuses too) | 4 (0) | not established |
| `one_arm_respelling` | right | right | 3 (0) | not established |
| TPC-H Q01–Q22 | 19 agree with localD; Q11, Q15, Q22 localD not established (deferred scalar producer) | — | 0 bound of every query's | not established |

A checker becomes buildable only when `D`, or the frozen family of §1.1, carries an identity for each reference: R1 after T1. Its rules R-a to R-f and the negative controls of §4.4 are unchanged as a specification. **[inf]** for every acceptance prediction.

**4.4 Negative controls.** A test-only fault per rule that the checker must reject on an otherwise accepted statement: inline a definition across a LEFT join (R-c); apply A1 and A2 both (R-b); drop a Project above a UNION (R-a); swap two same-named join slots (R-b); read a window's input instead of its slot (R-b, `window_arm`); emit a Project above a Limit (R-c); reverse two conjuncts (R-e); plain gather after a fused Sort (R-e); evaluate `random()` on a broadcast build (R-a); admit a dynamic filter on a LEFT probe (R-f).

**4.5 Cost gates**, base recorded at R0, compared per stage, same window for wall time (house rule):
- selected files and bytes per scan (the partition-pruning probe must stay 1 file / 452 B);
- dispatched tasks, including broadcast replicas and retries;
- S3 PUT and GET counts and bytes; shuffle bytes;
- repeated expression evaluations per definition (E2, RR4, RR11);
- coordinator peak memory (the final merge drains aggregation input, `coordinator.go:2045-2074`);
- planning time per statement (ADR-0050 §3's method);
- task bytes on the 500-column scan, at most +2 % (PLAN:295, :350);
- stage and exchange counts, secondary (stage count can fall while bytes rise);
- TPC-H SF0.01 per query and SF1 wall when the host allows.

r1's corpus delta corrected: 531 compared plans, of which 165 gain one or two dispatched scans with no stage change (`stagecount.py`: 321 + 123 + 42 + 18 + 21 + 4 + 2) **[obs, rerun]**. That is the prototype's cost, not the contract's.

### 5. Obligations that live elsewhere

Named here, owned by their homes, not designed here:
- **E-ORD**, `D` states each Filter's and join's conjunct order (§1.2). A logical-plan line on #1678; its content (which order) must match PostgreSQL's measured outcome, which orders by cost (`e_conj_order_a`/`_b` both apply `id > 1` first) **[obs]**. Prerequisite of R-dom.
- **E-DOM**, `D`'s domains give PostgreSQL's error outcome: E-DOM-1 single-reference CTE filter placement (#1678 P2 line, prerequisite of R-dom's CTE site); the five other single-process cells of r2 (lines for the architect to file in that family; not filed by this work). Whether a PostgreSQL error outcome that depends on its plan is gated exactly or carries a disposition (ADR-0013 class 11 is the precedent) is ADR-0013's (§7 note).
- **M2**, the pushdown arm chosen by name: the #1678 P1 planner line (present on the live issue). Its containment invariant needs occurrence-aware, edge-mapped slots (§2.2): a predicate goes below a join only into the arm whose `mL`/`mR` image contains every slot it reads; missing or zero bindings, and unknown reference kinds, keep it above. Needs W.
- **M3**, `IS [NOT] DISTINCT FROM` as NULL-rejecting: the #1207 line "the every-arm outer-join NULL-rejection defects" (fix on the unlanded branch, 0a90b98c, not resumed here).
- **Semi/anti residual NULL**: the #1681 engine line. An **executor** defect: the residual closure calls `evalFilterTyped` on raw typed data with no NULL test (`physical/join_sources.go:572-581`, `:599`) **[src]**, not a logical-optimizer one.
- **#1207's guard branch** (tip 93f117ba): preserved, not resumed.

### 6. Staging

A stage may not move a PostgreSQL-right cell (corpus, arm tables, TPC-H) to wrong or refused, and stops on such a move, on a checker false positive on TPC-H, or on a §4.5 regression. Each stage's checker assertions cover **only the sites it migrates**, identified per statement by the sites the statement's plan touches; no stage promises what a later stage delivers.

| # | Changes | Acceptance (scoped) | Depends on | Still wrong after it |
|---|---|---|---|---|
| R0 (landed) | Test seams only: the corpus and comparator (§4.1); the frozen-copy `localD` seam (deep copy before any physical planner, late subplans recorded; routed rows reported apart, since routes run `L`); capture of `G` as `PlanDistributed` plans it (the dispatch-time rewrites and the merge are not captured: owed by R5); cost-counter baselines; binding census and switch. Not done: the engaged dynamic-filter LEFT measurement (owed, RR6) | Corpus base pins recorded (1,380 cells); every `localD` ≠ DAG statement listed with its §4.2 class (the R0 report); no production path changes | — | Everything |
| T1 | The maintainer picks the slot-map source (§7) | — | R0 | — |
| W | Binder stamps WHERE and ON in both validations | No mixed block on TPC-H; ADR-0050's veto outcome unchanged over its corpus except listed cells | — | M2 |
| 4-ON | PLAN stage 4's ON transport | PLAN stage 4's gates | W | — |
| R1 | Frozen family with occurrence slots; set-op `OutputColumns`; storage lineage | Every reference maps to a slot, or is counted unmappable (0 on TPC-H) | T1, (W, 4-ON) | — |
| R2 | Streams carry slots; F1, F2/F2b, F3 stop crossing; Project placement per §2.2 and the placement rule below; minted family reserved; (B) on the wire for migrated sites; RR3, RR5 by slot | F-site cells to PostgreSQL; checker R-b, R-c, R-d clean **on statements touching only migrated sites**; +2 % task bytes; nine-door gate; §4.5 | R1 | A1/A2, U1/U2, keys, domain sites |
| R-dom | No Project emitted above a Filter, Limit or Sort (P1/P2/W2/S1 materialization sites); conjuncts in `D`'s order; the CTE Filter site | `e_derived_limit`, `l_error_over_limit3`, `e_conj_order_a` to PostgreSQL; `e_cte_once` stays right; R-c, R-e clean on migrated sites | R2, E-ORD, E-DOM-1 | — |
| R3 | A Project above an aggregate or a set operation evaluated once (A1/A2, U2); set-op arms by §2.2 (U1) | `group_expr`, `group_negate`, `union_cast_values`, `ctes_limit_set`, `limit_set_cast` to PostgreSQL; R-a, R-d clean on these sites | R2 | Keys |
| R4 | Window, sort and group key paths read slots (K1, K2, S1, W1, W2) | `kv_group`, `sj_window`, `sj_group`, `k_window`, `k_group`, `gk_outer_expression`, `gk_outer_having`, `window_nested_order` to PostgreSQL; §8j seam pins become agreements | R2 | — |
| R5 | The checker over `G` in every suite (all R-rules, RR1–RR11 validated, dispatch and merge included); the differential equality DAG = localD promised on statements where localD is PostgreSQL's | Zero violations outside the §4.2 exception list; RR6's LEFT admission resolved by its measurement | R2–R4, R-dom | — |
| R6 | Ordinal leaves (PLAN stage 8); the worker's strip/suffix counter 0 | PLAN stage 8's gates; +2 % task bytes; nine-door gate | R5 | — |

**Placement rule (r1's T3, now a rule with a measurement owed by R2).** A Project's slots are evaluated (1) on the producing fragment when that fragment is dispatched anyway; else (2) as an input projection of a consuming fragment that is **not replicated**, or of a replicated one only when every item is non-volatile, its repetition counted (RR4); else (3) as a StageProject. Each under E1. R2 measures (1) against (2), which needs a per-input Stage field on the `BuildFilterExprs` precedent.

### 7. Tradeoffs: open and decided

- **T1, slot-map source — OPEN, deferred by ruling D2.** Bind `D` (Option 1, with W) or a second binding pass over unbound `D` (Option 2). R0 measured, without choosing **[obs]** (R0 note §5):
  - *What the binder provides today* (`TestR0BindingCensus`, the corpus, nine probes and TPC-H): every reference in a SELECT list, GROUP BY, HAVING and ORDER BY of a block the binder completes is stamped (SELECT list 533 of 546; the unstamped ones sit inside EXISTS / scalar-subquery bodies and a LATERAL body); no reference under WHERE (193 corpus, 119 TPC-H) or a join condition (259 corpus, 105 TPC-H) is. Per-occurrence identity: two reads of one CTE get two relation instances (`a.r` rel 3, `b.r` rel 4); a CTE read at two depths gets three instances with one shared body binding; a set-operation's output is one relation instance whose arms bind separately, with no common-type slot; an outer join's padded column binds to the derived relation's output (`s.r` → rel 4, ord 1) with no join-edge or NULL-extension component, so the padded copy and the arm's column are one identity; a USING-merged column is refused 42702 (a capability gap PostgreSQL answers); an EXISTS / scalar subquery body is not stamped; an ORDER BY output name is an output binding; aggregate state and policy-added references are not binder concepts (the policy is applied after binding, `statement_plan.go` `completePlan`) **[src]**.
  - *What binding `D` moves* (`TestR0BindingSwitch*`, a test-only switch that plans the statement bound and executes it on the stage DAG through a copy of ExecuteSQL's tail, whose unbound mode answers every corpus cell as ExecuteSQL does): on the corpus, nothing moves on any DAG arm. On the group-key table (1,182 cells), per DAG arm: refused→right 139, refused→wrong 9, refused→routed 9, wrong→right 6, wrong→refused 1, refused→refused 144 (another refusal), and no right cell moves. The 9 refused→wrong are cells the unbound validation refuses (42803) that the bound validation admits and the stage DAG then answers wrongly; the 6 wrong→right come from planner passes reading the stamps (the stage fields that change name them). This is not the stage-5a spike's "21 transitions" (measured at an earlier base); it is today's count, by cause.
  - *What stamping WHERE and ON too would move* (scratch patch, not committed): on the corpus, 0 of 1,380 cells on any arm, and the group-key DAG transitions are unchanged; the census shows 418 of 452 corpus and 191 of 224 TPC-H WHERE/ON references stamped by it (the rest are subquery-body references).
  - *Sharing one parse without sharing mutable nodes*: R0's seam deep-copies `D` and checks that the copy shares no pointer, map or slice with the executed plan (and that a shallow copy would); the same mechanism would let `L`'s and `D`'s runs share a parse **[inf]**; whether the veto's bound SelectInfo can be shared is not measured.
  - What each option still owes: Option 1, the WHERE/ON stamping (W) and its veto census, the join-edge / NULL-extension identity binding does not give (above), and the late-subplan family; Option 2, a resolver for alias, USING, outer scope and ambiguity with a fail-closed result, and agreement with the binder wherever both bind. Neither has evidence that settles it; the judge's evidence-order finding (5) stands.
- **T4, the checker in production — DECIDED by ruling D3: test-only** until the repair stages; not a route or refusal trigger.
- **Plan-dependent error outcomes — DECIDED by ruling D4** (quoted in the status). The corpus compares SQLSTATE exactly and keeps PostgreSQL's result for every error-domain cell; no ADR-0013 disposition is proposed.

Closed since r1: T2 (retired by E2), T3 (a rule with a measurement), T5 (chosen, §2.5). T4 decided (D3).

## Remaining findings (the independent judge's second record), by owing stage

Not resolved by R0, and not approved by it:

- **Before R2 / R-dom enforcement, complete by R5:** a checkable evaluation domain and conjunct order — duplicate-row occurrences, partition cover, demand and truncation, join key/residual order, error precedence between independent failures; E-ORD and E-DOM keep their owners; replace R-dom's blanket "no Project above a Filter, Limit or Sort" with movement relative to `D`; name every R2 site that waits for E-DOM-1 (judge B1, B6).
- **Before the R1 sharing representation, validate by R5:** the CTE-sharing / materialization law and cross-depth CTE identity across `D` and `D'` (Q15's shared terminal); an explicit rule for bare-LIMIT demand truncation (`stage_emission.go:649-717`); scalar-subplan demand, cardinality and error composition for RR10, with its existing owner (`docs/internals/boolean-subquery-hoisting-boundary.md`) (B2, B4, B5).
- **Before R2 replicated or input placement:** "non-volatile" is insufficient for repetition — `embed()` is a regular function with external effects and a NULL-on-error result (`embedding/func.go`); one fail-closed repeatability predicate shared by E2/RR11, RR4 and the placement rule (B3).
- **Before R5 certification:** refinement certificates — RR1–RR11 validated against O1–O7 (typed, NULL and error aggregate state laws; unmatched-row ownership for replicas; dynamic-filter evaluation domain and engagement; gather multiplicity, offset and ties) (B4).
- **Before the final T1 / R1 commitment:** the evidence order — option-neutral and W-dependent evidence before the choice, or T1 provisional with a revisit gate; non-vacuous partial-stage coverage; site-specific checker waivers (B6, C).
- SHOULDs: storage-origin lineage kept apart from relocation legality (R1–R2); noisy-cost tolerances (R0 recorded baselines, not tolerances); label native versus legacy capture (R0 captured `PlanDistributed`'s stages only).

## Alternatives beaten

- **Column identity alone (ADR-0047 stage 6 as written).** It follows a binding into the definition: `outer_nonstrict_rows` needs no collision.
- **Keep the spelling, materialize by name** (r1's prototype). `window_arm` goes right to wrong.
- **Ordinals or unique names after emission.** The map follows the mistake.
- **A spelling-based guard** (#1207's guard C). Excluded by the ruling.
- **"Deterministic is enough" for composing a definition into a filter.** `t2_logical` raises 22012 and `t2_composed` answers on PostgreSQL **[obs]**.
- **A Filter is not a boundary.** `e_derived_limit` and `l_error_over_limit3` are wrong because a Project was emitted after a Filter (and a Limit) **[obs]**.
- **`localD` as the oracle.** `e_cte_once`: the DAG is PostgreSQL's answer and `localD` is not **[obs]**.
- **A blanket exemption for physical refinements.** It would accept an invalid state merge or a LEFT dynamic filter; the closed list (§1.5) does not.
- **Make every Project a stage.** A write and a read per Project on the materialize-every-stage model; the placement rule reaches E1 without it.

## Consequences

- The DAG is a refinement of `D`: every shape difference is a named rule instance, and a path difference on a statement whose `localD` is PostgreSQL's becomes an emission defect by definition.
- Error outcomes and volatile evaluations become gated observables, not accidents of where a pass materialized a column.
- Logical obligations surface that this work did not create: conjunct order (E-ORD) and error domains (E-DOM) are stated as `D`'s, and the single-process engine's measured divergences are lines in their own family.
- The resolvers and materialization passes that crossed boundaries retire as R2–R4 land; ADR-0026 §5, §8j, line 141 and the native-DAG internals doc are amended as listed.

## Costs

- **Recorded at R0** (`testdata/r0_cost_baseline.tsv`): per corpus statement and TPC-H SF0.01 query, stages, exchanges, scan stages, selected files and bytes, planned tasks, planning times; not exposed yet (owed by R2): dispatched tasks with replicas and retries, S3 PUT/GET, shuffle bytes, repeated evaluations, coordinator peak memory, 500-column task bytes; SF1 owed.
- **Measured:** the prototype's dispatched-scan growth (165 of 531 plans, +1 or +2, no stage change); TPC-H SF0.01: one crossing rewrite in 22 queries; zero filter-pushdown loss for the instrumented families on r1's fixture; coordinator partition pruning present and to be kept (judge probe 5).
- **Not measured:** SF1 wall; the input-projection field (unbuilt); W's cost; the checker's planning time; the cost of stating conjunct order; A1/A2/K1/S1/W1/W2/U1's share.

## What this record does not cover

- Whether `D`'s types, display scales and error domains are PostgreSQL's (ADR-0024; E-DOM; ADR-0013): named in §5, owned there.
- M2, M3, semi/anti and #1207's branch (§5).
- Policy enforcement itself (ADR-0033/0034): this record only requires RR8 to keep its order and the nine-door gate to run.
- The async door (ADR-0050 §1.3 item 6).
- Capability gaps every arm refuses alike (`full_using_cast` 42702, `ordered_string_agg` 42601, `AS MATERIALIZED` at parse): adding them is not a preservation question.
- LATERAL decorrelation's own repairs (ADR-0021, PLAN stage 4), except that their lowered output obeys this contract.
- Recursive CTE fixed points and grouping-set lowering (the DAG refuses or routes today; not examined).
- Whether `exec.ColumnIndexFallback` is deleted.
