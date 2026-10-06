# ADR-0047: A column reference has one resolved identity, from the binder to the last consumer

- Status: Accepted (2026-10-06). The POSITION below is the column-identity program's; its migration lands in nine stages (§ Migration). Stage 1 — the binder's stamp, and the group-key match reading it on the single-process engine — is landed (arc CI1, #1524). Stages 2–9 are the plan.
- Deciders: Derek Wright
- Extends ADR-0026 §1 and §8j from GROUP BY keys and window / sort keys to every column reference; extends ADR-0032 (a block is parsed once) to expression subqueries. Supersedes, as the migration completes, ADR-0021 §1's "settle the text after `reorderJoins`", ADR-0026 §2's text recovery of a key and the worker's `derivedGroupKeys` compatibility path.
- Related: ADR-0012 (PostgreSQL decides semantics), ADR-0021 (subquery name resolution), ADR-0025 (a consumer binds through the identity its producer published), ADR-0026 (a group key has one identity and one name), ADR-0032, ADR-0033 (a column policy is a plan-time projection at the scan), ADR-0037 (one module, two licenses).

## Context

A column travels from the parser to the wire through layers that do not share what they decided: the binder (`physical.validateColumns` → `binder.validateBlock`, `internal/planner/physical/validate.go`), the logical builder, the physical local planner and — on the distributed path — the stage planner (`internal/coordinator/dagplan`) and the worker that compiles a serialized `distributed.OpSpec`. The only thing every layer receives is the column's SPELLING: a name, sometimes qualified, often a printed expression the next layer parses back.

The binder was the only layer that knew which relation a reference names, and it recorded none of it: `colScope.resolveRef` returned only an error, and an enclosing query's names were merged into the scope flat, so not even the binder could say afterwards which level a name came from. Every later layer re-derived the answer from text. At 33e2fb92 the census counted 152 re-parses of plan-carried text (96 `ParseExpression` calls: logical 31, physical 26, dagplan 30, worker 9), about 750 lines comparing or keying by a column name, and a worker that re-plans a whole statement from `Task.SQLText`.

Two things spelled alike are therefore confused, and one thing spelled two ways is not recognized. One root, measured in five consumers by five arcs:

| arc | the consumer | measured (PostgreSQL 17.11 first) |
|---|---|---|
| GK (#1524) | a select item matched to a GROUP BY key | `SELECT 2 * t.n, COUNT(*) FROM ss_t t GROUP BY 2 * n ORDER BY 1` was declared text and sorted `20.00` before `4.50` on every arm at 33e2fb92; over a join every bare-versus-qualified pair was 42803 where PostgreSQL answers; ten sites compared a term with a key and one had the relations' column sets. GK-F4: `SELECT -i AS i FROM ss_t GROUP BY i` answers the key `i`, not `-i`, on the three DAG arms. |
| WD (#1435 / #1436) | a consumer above a window over a derived table computing a column under the name of a column it reads | eight review rounds; one predicate taught to seven walks; #1549 (the same shadow made by an aggregate) is the eighth spelling |
| UN (#1541) round 4 | a per-column "unconstrained numeric" mark | the exchange stamp took the union of names over every file an operator reads; `2.5` where PostgreSQL prints `2.50` on six DAG arms |
| RE review 1 | a per-column PostgreSQL type category for `round()` | `physical.PlanPGCategories` folded by bare name; `round(f)` rounded half to even on the DAG |
| MO N2 / #1326 | a derived join arm's column on the DAG | `… CROSS JOIN (SELECT i.id, i.k FROM lt_i i) s WHERE s.k = o.k + 1`: 0 rows for PostgreSQL's 6 |

#1393 is the same root in the declaration walk (`lookupColType` strips the TEXT `x + a.k` at its last dot and declares the integer `k`).

ADR-0026 already stated the rule for one family — §1 "every consumer resolves through [one identity]; no site compares spellings" and §8j "a NAME is derived from the identity for publication; an identity is never derived from a name" — and could not honour it, because the identity those sections name, `plansql.ExprIdentity`, is a canonical RENDERING of the spelling. It erases parentheses, case and whitespace and nothing else; it cannot say that `t.n` and `n` in a block over `t` are one column.

## Decision

**A column reference is resolved ONCE, by the binder, to an identity that is not a spelling. The identity travels with the reference until the last consumer reads it. Nothing after the binder decides which column a reference names by comparing names or printed expressions. A name is derived from an identity for publication; an identity is never derived from a name.**

### 1. What an identity is

- **A relation instance.** Every occurrence of a relation in a statement's FROM — a base table, each CTE reference, a derived table, a table function — is a distinct instance with a statement-scoped id the binder assigns as it registers the source (`plansql.RelID`). Two references to one table are two instances; an alias is a spelling of an instance, not the instance.
- **A column of an instance** is `(RelID, ordinal)`: the column's position in the list that instance publishes, in its order. Position and not name, because one relation may publish two columns of one name.
- **A SELECT output item** a bare ORDER BY name resolves to (PostgreSQL resolves an ORDER BY name against the output first) is `(the block's own id, item ordinal)`, marked as an output binding.
- **An outer (correlated) reference** carries the identity of the enclosing instance it binds and the number of query levels it crosses.

The Go type is `plansql.Binding{Rel, Ord, Level, Output}` on `plansql.ColRef.Bound` (`internal/planner/sql/ast.go`). Like `ColRef.Slot` it is provenance: the parser never sets it, nothing a query can contain sets it, and a re-parse of rendered text loses it by design. `Binding.BindingKey()` renders it as a key that starts with a NUL byte, which no identifier and no alias can spell — so an alias resolver comparing names can never capture it (the defect of GK round 1's B1: a term re-spelled into the bare key name was read as the output alias of the same name).

### 2. Who assigns it — one resolver

The binder is the resolver. `colScope` keeps, beside its name sets, each FROM source as an instance with its ordered column list (`relInst`), and the scope of the query level it sits under (`colScope.up`) instead of a flat merge; `bindRef` (`internal/planner/physical/validate_binding.go`) resolves a reference by PostgreSQL's order — a QUALIFIED reference names the relation of the innermost level that has one under that name, and only when no level has one is the qualifier read as a ROW column; a BARE reference is the column of the innermost level that provides the name, ambiguous when two of that level's relations do. The rules that already lived in the binder (delimited versus folded spelling #731, 42P01 / 42703 / 42702, PostgreSQL's GROUP BY input-first precedence #739) stay where they are; the binding is recorded where the check already decided.

### 3. Who may compare spellings afterwards — nobody, with three named exceptions

After the binder no site decides WHICH column a reference names by comparing names, folded names, qualifier-stripped names or printed expressions. The places a name is legitimately read are not resolutions: the scan leaf (a stored column's name, once, from the catalog), publication (wire names, star expansion text, EXPLAIN, errors — rendered FROM an identity) and the binder itself.

Two expressions are one GROUP BY key when their trees are equal after `ExprIdentity`'s three erasures AND their leaves carry equal bindings. In stage 1 that comparison is `plansql.GroupTermIdentity`, which renders a bound leaf as its `BindingKey` and an unbound one exactly as `ExprIdentity` does; it is the ONE function every group-key match site reads. `ExprIdentity` itself keeps its spelling meaning until every caller's counterpart is bound: a caller that compares a bound tree with a re-parse of its own text would stop matching (RISKS R1) — and on the DAG every such counterpart is a re-parse today. At stage 9 the two are one function and `ExprIdentityUnqualified` is deleted.

### 4. How PostgreSQL's scope rules map

- **ORDER BY.** A bare name that matches exactly one OUTPUT column binds that output item; otherwise it binds an input column. An expression binds input columns. `ORDER BY t.i` beside `SELECT -t.i AS i … GROUP BY i` binds the input column `t.i`, which IS the key; there is no spelling in between for the alias `i` to capture.
- **GROUP BY.** A bare name binds an INPUT column first and an output alias second (#739: the parser's provisional substitution and the binder's revert stay; the binder records the outcome).
- **HAVING** reads input columns and keys. This engine's concession of an output alias in HAVING (a bare name no input provides) binds that output item; an INPUT reference is never licensed by an alias that spells its name (`count(*) AS n … HAVING t.n …` is PostgreSQL's 42803).
- **Group-key match.** A select item, HAVING, ORDER BY, window or GROUPING term IS key k when `GroupTermIdentity` says so. Over a join `t.x` and a bare `x` that both bind `(t, x)` are one key; `zzj.d92` and `zzp.d92` are two. Commuted operands and constant-folded twins stay two identities — PostgreSQL's 42803.
- **Ambiguity (42702)** is raised by the resolver at the reference, as before.

### 5. Output names are attributes of identities

An output name is used to publish and to let an enclosing block's binder resolve a written reference once. It is never an address. Hidden slots (`__gb_expr_N`, `__win_N`, …) keep their reservation (ADR-0025); resolution never depends on it.

### 6. Where identity is unknown, a block is matched by spelling IN FULL — never a mixture

A block is bound in full or not at all. A reference the binder cannot bind with certainty — an unenumerable FROM source, a name a SELECT star may publish, a ROW field path, a USING-merged bare name, a name no level provides — leaves every reference of its block unbound, and the block's match is the pre-migration spelling comparison, unchanged. So does a block whose terms reach the planner through a carrier that cannot hold a binding:

- an **expression subquery's** or a **LATERAL** body: the planner builds it from a re-parse of its text (`SubqueryNode.SQL`; the LATERAL decorrelation records its GROUP BY as text), so the binder validates it unstamped (`binder.unstamped`) and checks it by the spelling rules its plan is matched by — stages 3 and 4 bind them;
- a **FROM-less scalar subquery the parser unfolded** (`SelectColumn.UnfoldedFrom`): PostgreSQL judges it as written, and the written form is text — stage 3;
- a **name holding a dot** (`"a.b".n`, `t."x.y"`): the plan carries the key and the term as text past the binder, and a dotted name re-read from text is a qualified one (GK review B2 / P3); binding such a block routed a spelling the base refused into that pre-existing wrong value.

A consumer that compares a bound term with an unbound one is #738's mechanism — the 42803 check admits the term, a text site misses it, and the item is published as arithmetic — and is not shippable. The census gate (§ Gates) counts it: none on the corpora.

A DOOR records bindings only when the plan it executes is built from that same AST by consumers that read them (`auth.BindStatementColumns` → `physical.BindColumnsUnderPolicy`): the embedded engine's `Query`, its CTAS / INSERT … SELECT door and the pgwire server over it. The coordinator validates as before and plans an unbound AST, so every stage-DAG arm — and the coordinator's in-process fast path — runs the base code by construction until stage 5 (§ Alternatives 7: no route). A binder that does not stamp leaves the AST exactly as it finds it: under a column policy the embedded door validates a statement twice — at the door, which binds, and again under the policy's schema after the plan is built (`auth.EnforcePlanPolicies`) — and the second pass reads the first pass's bindings rather than clearing them, so the logical and the physical planner never see one block bound and unbound.

### 7. Identity across a stage boundary and in the task message — by position, never by name

A `dagplan.Stage` publishes its output as an ordered list; each entry carries the identity it materializes and its published name. A consumer stage addresses an input column as (input dependency, ordinal). The serialized `OpSpec` carries ordinals for every key, argument, sort key, partition key, join key and projection leaf; names travel beside them for publication only (the precedents: `OpSpec.GroupByColIdx`, `AggColumn.InputColIdx`, `SortKeySpec.SlotPos`, `ProjectExprSpec.SourceSlot`, UN's in-band mark). A per-column property rides with the column's position or inside its own schema entry, never in a name-keyed side map. `RelID` never crosses the wire as a meaning. Stages 5–8.

## Stage 1 as landed (arc CI1, 2026-10-06)

- **The stamp.** `plansql.Binding` / `ColRef.Bound`; the binder's instances, levels and `bindRef`; `binder.stampBlock` binds the SELECT items (window terms ride inside them), GROUP BY, HAVING, QUALIFY and ORDER BY of every block it validates and records the bindings when every one binds.
- **One carrier converted.** A window's PARTITION BY / ORDER BY / argument terms reached the over-aggregate re-spelling as text re-parsed from `WindowSpec` (`logical.respellOverAggregate`); `WindowExpr` now carries the parsed terms (`ArgExprs`, `PartitionByExprs`, `OrderByExprs`) and the re-spelling matches a bound block's tree. Reverting it re-introduces 81 bound-term-against-re-parse comparisons and 38 wrong answers over the group-key table (the census's own self-check).
- **The match.** `GroupTermIdentity` at every single-process match site: `plansql.groupKeyRefLookup` (HAVING, QUALIFY, window terms), `logical.computedGroupKeyRefs`, `logical.sortTermResolvesOverAggregate`, the GROUPING argument check, `physical.groupKeyOutputs` and its readers (`buildProject`, `aggregateGroupKeyName`), and the binder's own 42803 check (`groupCheck`, which also reads ORDER BY from its parsed tree rather than re-parsing `OrderByItem.Column`). A key of a bound block that the aggregate directly below already publishes under ANOTHER spelling (`SELECT DISTINCT i + 1 … GROUP BY t.i + 1`) is grouped by that column under that column's name. The qualifier-erasing fallbacks (`ExprIdentityUnqualified` in `groupKeyRefLookup` and `groupCheck`) now serve unbound blocks only, and are deleted at stage 9. No text is rewritten anywhere.
- **Measured before any consumer switched** (RISKS M1–M5; arc notes `tooling/arcs/ci1_binder_stamp/ci1_landing_notes.md`): bindability — every block of the group-key table (1,223) and of TPC-H SF0.01 (53) bound in full, 313 of 73,480 blocks of the coordinator two-path corpus mixed (ROW field paths, a star's outputs, USING merges), all left unbound; stamp survival — the only carrier losing bindings at a stage-1 match site was the window text above; ordinal invariant — the binder's list equals the plan's for every compared instance (TPC-H 91, group-key table 1,160 at the tip); timing — no change past noise (depth 16: 3.3 / 3.1 ms; 200 items over 50 keys: 49.0 / 49.0 ms; the pre-push hook 345 / 363 s with the new gates in the second run).
- **What it answers.** Over the 1,063-cell group-key table the single-process arm moves from 578 cells answering PostgreSQL to 974; none moves away. The remainder are base-identical or the ALIKE spelling's own catalogued answer (testdata/arc_gk_group_key_spelling_kept.tsv). The three DAG arms answer byte for byte what they answered at the base on every cell but two whose answer the statement leaves open (a correlated `LIMIT 1` without ORDER BY; a refusal message quoting a per-row outer value).

## Alternatives rejected, each with its measured failure

1. **Compare spellings, better.** ADR-0026's seven "is this the same expression" rules became one rendering (`ExprIdentity`), which fixed case, parentheses and whitespace and could not fix qualification: GK's table at 33e2fb92 had 344 cells refused or wrong that respelling fixed and 48 join cells no spelling rule can fix, because over a join `t.x = x` is true or false depending on the OTHER relation's columns.
2. **Respell the text (GK round 1, `plansql.RespellGroupKeyTerms`, never landed).** Rewriting a term into the key's spelling hands it to every resolver that reads spellings, including the one that reads output aliases first: `SELECT -t.i AS i … GROUP BY i ORDER BY t.i` sorted by the alias (11 cells wrong ORDER on every arm, base right), and a key over a dotted name moved four cells from 42803 into a wrong value.
3. **Teach each walk where to stop (WD).** Seven walks share one predicate after eight rounds, and the predicate does not cover the aggregate shadow (#1549). A walk that follows a name has to know where the name stops meaning the same column; a walk that follows an identity does not.
4. **Name-keyed side channels.** UN round 4's marks keyed by name over the union of an operator's files and RE's categories folded by bare name each carried a correct fact to the wrong column of the same name.
5. **A second resolver beside the binder (GK option B).** Two implementations of one rule is a standing hazard (ADR-0026 §2e records the measured pairs).
6. **Settle the text late (ADR-0021 §1).** Recording what a reference means is right; spelling it afterwards is re-read by `exec.ColumnIndexFallback`'s qualifier strip.
7. **Route a mixed-spelling block to the single-process pipeline (the plan's stage-1 DAG interim).** Declined by the coordinator (2026-10-06): a DAG arm that keeps exactly the base's answer is not a regression; a route changes which engine answers a statement on a production path, for a class stage 5 removes at its root. The DAG keeps its answers, pinned cell by cell, and a pin that starts agreeing with PostgreSQL fails and is deleted as the proof.

## Migration

| stage | the one change | status |
|---|---|---|
| 1 | the binder's stamp; the group-key match reads it on the single-process engine | landed (arc CI1) |
| 2 | a node's declared output is an ordered list keyed by identity (closes #1393) | planned |
| 3 | an expression subquery's body is parsed once and bound with its outer scope (the binder runs on subquery bodies: the nine-door masking gate ships with it) | planned |
| 4 | decorrelation, LATERAL and USING carry bindings | planned |
| 5 | the Stage publishes an ordered identity list; DAG keys and outputs bind by position (closes #1524 on the DAG arms and GK-F4) | planned |
| 6 | the DAG stream model binds by identity (#1549, #1326) | planned |
| 7 | per-column properties ride by position on the DAG | planned |
| 8 | wire expressions carry ordinal leaves; the worker stops planning from text | planned |
| 9 | retire `ExprIdentityUnqualified`, the qualifier fallbacks and the compatibility paths; a census test whose counts of re-parses and name comparisons may only go down | planned |

## What this does NOT decide

- The concrete encoding of a wire expression — only that its leaves are ordinals (stage 8, with its byte measurement).
- Whether `exec.ColumnIndexFallback` is deleted (a later, separately measured question).
- Identifier case folding (#731), output-name rules (#732), numeric typing (ADR-0024), container elements (ADR-0045): unchanged.
- ADR-0033's policy semantics: where a mask or a denial is applied becomes identity-keyed at stage 3; what it means does not change. Stage 1 runs the binder on no path it did not run on before.
- Mixed-version clusters: no deployment exists; the task message may change shape without a fallback (stage 5 adds a version a worker refuses on mismatch).

## License boundary

The identity type (`RelID`, `Binding`, `GroupTermIdentity`) lives in `internal/planner/sql` and the resolver in `internal/planner/physical` — both MIT, both already imported by `dagplan`. Stage 1 adds no physical member `dagplan` reaches and no name the DAG vocabulary gate reserves; `dagplan` is untouched. Later stages keep the question "the identity of node N's output k" on `logical.Node` (MIT, not budgeted) so that `TestAGPLPhysicalMemberBudget` does not rise.

## Consequences

- A consumer that needs to know which column a reference names reads its binding; adding a consumer that compares names is how this class comes back. The end-state gate is a census whose counts may only go down (stage 9).
- During the migration two comparisons coexist — by binding in a bound block, by spelling in an unbound one — and the every-or-none rule is what keeps them from meeting in one block. The census gate holds it.
- The worker's compatibility fallbacks for an older coordinator are deleted, not extended (stage 5).

## Gates

- `coordinator.TestArcGKGroupKeySpellingEveryArm` — 1,063 cells × five arms against PostgreSQL 17.11: the single-process arms answer PostgreSQL or a kept line (base-identical or the alike spelling's catalogued row); the DAG arms are pinned to the base. 418 cells fail with the file on the base.
- `wadjet.TestArcCI1BindingCensusOverTheGroupKeyTable` and `tpch.TestCI1BindingCensusTPCH` — RISKS M1 / M3 / M5 as counts: no mixed block, no bound term compared with an unbound one, no ordinal disagreement.
- `server.TestArcCI1GroupKeyByBindingNeverPublishesAPolicedValue` — the nine-door masking census over e7emp (masked ssn / acct, denied salary) and e7bal (row filter): no policed value and no denied column reaches a client, a masked key answers the mask, and the mixed-spelling cells answer on the three embedded doors and keep 42803 on the six coordinator and http doors.
- `pgwire.TestArcGKGroupKeySpellingOnTheWire`, `wadjet.TestArcGKEmbeddedGroupKeySpelling` (declared OIDs, CTAS, INSERT … SELECT), `wadjet.TestArcGKGroupKeyMatchPlanningBound` (depth 16, 200 items over 50 keys, a twelve-way join, each under 2 s).
- `coordinator.TestTheIdentityErasesAQualifierAndATypeSynonym` (#738's mirror spelling now answers on the single-process arms and keeps its 42803 on the DAG) and `coordinator.TestArcNXNumericCarrierEveryArm` (#1524's issue cells' pins hold on the DAG arms only).
