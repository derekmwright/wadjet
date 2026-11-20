# ADR-0050: A coordinator statement plans under one authorization state, and its local paths plan the bound statement

- Status: Accepted (2026-11-19) for implementation. This record is stage 5a of the column-identity program and nothing else. Amended 2026-11-19 (section 3: the check covers every coordinator statement; the incarnation's uniqueness is stated with its restore exception).
- Revised 2026-11-20 (maintainer's ruling after an independent adversarial review): section 1 rewritten as a veto and two plans; section 2's audit sentence and section 3's "no other plan is tried" corrected to what the code can deliver; the cell-for-cell equality promise withdrawn; acceptance replaced. The first build of section 1 stopped on a wrong premise, stated in Context. Sections 2 and 3 are landed (eabfa539, 9ade851f, f6f62320); section 1 is not on main until its build lands.
- Deciders: Derek Wright
- Related: [ADR-0047](0047-a-column-reference-has-one-resolved-identity.md) (stages 1 to 3: the binding this record makes the coordinator use), [ADR-0034](0034-every-door-authorizes-before-it-acts.md), [ADR-0033](0033-a-column-policy-is-a-plan-time-projection-at-the-scan.md), [ADR-0030](0030-a-dml-statement-commits-against-the-manifest-it-read.md) (the table incarnation), [ADR-0012](0012-sql-semantics-authority.md) item 7. Epic #1678.
- Not accepted by this record: the positional stage model (the draft numbered 0048) and the per-statement catalog view (the draft numbered 0049). Neither is policy. Nothing here depends on them.

## Context

The coordinator plans a `SELECT` once, unbound (`auth.ValidateStatementColumns`), and hands that one plan to its local fast path, to the stage planner, and to any route that runs a refused plan locally. The embedded engine plans the same statement bound (`auth.BindStatementColumns`). So the coordinator's fast path, which is on by default for every statement under 64 MiB of scan, answers differently from the embedded engine.

**The spike this record was first accepted on (2026-11-18), kept as measured.** On the group-key table's 1,182 cells with the coordinator arms forced onto the fast path:

| | Unbound (today) | Bound |
|---|---|---|
| Equal to the single-process arm | 688 | 1,181 |
| PostgreSQL's exact text, per arm | 554 | 1,032 |

493 cells moved, none regressed, and one cell remained (the same `0A000` refusal with a different literal). Planning cost a median 4.9 ms per TPC-H statement (worst 15.3 ms) and 0.8 ms per short statement (p99 5.5 ms). These are true measurements of a spike that gave ONE bound plan to every consumer. That is not the design this record decides: the stage planner cannot take a bound plan (binding it moves cells the wrong way; that is the unaccepted work), so a statement needs two plans. The record as first accepted cited the spike's numbers for the two-plan design, and its first build measured 1,159, not 1,181, and stopped.

**Measured for the two-plan design (2026-11-20).** Fast-path arm against the single arm run beside it, 1,182 cells:

| | Equal to single | PostgreSQL: same SQLSTATE, or same declared classes and rows | Stored-oracle text, trimmed |
|---|---|---|---|
| main, unbound | 687 | 613 | 556 |
| The record as first accepted (two plans, fall-through) | 1,159 | 1,074 | 1,017 |
| This record (two plans and the veto of section 1) | 1,175 | 1,084 | 1,031 |

Why the rows differ from the spike: 687 against 688 and 556 against 554 are a different single-arm run and a trimmed comparison (two cells vary by run); 1,159 against 1,181 is 22 cells where the bound plan refused and the first build fell through to an unbound plan that answered or refused differently; the veto recovers 16 of them; 7 cells stay apart and are named in section 1. The single arm itself scores 1,086 or 1,087 by the middle column's rule.

Agreement with the single-process engine is not agreement with PostgreSQL. Of the 224 group-key cells the single arm refuses, PostgreSQL refuses with the same code on 150, answers 69 and refuses with another code on 5, all recorded. This record moves the coordinator's local paths toward the single-process engine, and no further.

**The two validations.** Over 49,653 distinct statements (every coordinator arm-table test), unbound against bound validation: both accept 44,148; both refuse identically 5,154; only the unbound refuses 281 (PostgreSQL answers all 281); only the bound refuses 66, and they refuse differently on 4 (PostgreSQL refuses all 70 with the bound side's code). All 351 disagreements come from one fixture. The bound validation is nevertheless not PostgreSQL's verdict: measured on PostgreSQL 17.11 (`--locale=C`), `SELECT count(*), (SELECT max((SELECT t.i FROM ss_i y LIMIT 1)) FROM ss_i x LIMIT 1) FROM ss_t t GROUP BY t.i + 1` answers 3 rows and the bound validation refuses `42803` (a false refusal, a line on #1678).

Two plans raise the questions this record answers: which plan answers, what a refusal by one plan means for the other, which authorization decision governs, and what happens if the catalog changes between them. Its scope is `Coordinator.ExecuteSQL`, the synchronous door.

## Decision

### 1. A veto, then two plans

#### 1.1 The bound validation's refusal is a veto, not an authority

For a statement on `ExecuteSQL`, the bound validation runs first, under the statement's pinned authorization state. **If it refuses, the statement is refused: the coordinator does not go on to answer it from a plan built without bindings.** This holds on every synchronous path, whether or not the fast path is enabled and whatever the statement's size (maintainer's ruling, 2026-11-20).

This is a conservative safeguard, not a claim that the refusal is right. The validator has false refusals (the one in Context; and refusals both validations share that PostgreSQL answers); each is a compatibility defect to investigate. The justification is the standing rule that a loud refusal is acceptable and a silent wrong answer is not, and what the alternative does. Measured population: the 70 corpus statements on which the two validations disagree with the bound one refusing, plus the 2 false-refusal statements: 72. On them the unbound path gave PostgreSQL's answer 0 times, a refusal 71 times and silently wrong rows once (`collide/havingQual/sel`). No measured case loses a correct answer to the veto; that absence is a corpus observation.

It is not "validity decided once". A statement the bound validation accepts can still be refused by the stage planner's own unbound validation (281 corpus statements, all of which PostgreSQL answers). Those stay as they are: false refusals of the stage planner, tracked on #1678; a small statement of that kind is answered by the fast path, a large one is refused.

The veto is the validation step of the statement's first planning run, not a second attempt. Its outcome has three values, kept apart:

- **Not applicable**: parse or extract fails, or the statement is not one `planStatement` plans. The ordinary run reports it.
- **Refused**: terminal, whatever the refusal is (validity, table-function capability, authorization).
- **Accepted**: its parse, its bindings and its table-function context are `L`'s, reused if `L` is built.

The table-function context (reader schemas, which may open files) is meant to be taken once per statement and shared by every plan. Whether the code lets `L` and `D` share it is not verified; it is an acceptance item, and if it cannot be shared, each plan may observe a reader input separately and this record says so.

#### 1.2 Which refusals end the statement, once observed

"Terminal" means: once a plan of the statement observes it, no other plan is built or run. It does not mean every path would observe it: which relations a plan reaches, and so which authorization decisions and which cost totals it meets, depend on the plan.

Terminal once observed, by provenance, never by SQLSTATE class:

- the veto of 1.1;
- an authorization refusal (`42501` from a decision under the pinned state);
- the typed incarnation refusal (`40001`, section 3);
- the query-limit refusal (`53400`, from the cost walk over the plan being planned);
- cancellation of the statement's context, at planning and at execution.

Everything else keeps today's disposition, unchanged by this record. A failure of `L` after validation (build, optimization, physical planning, coded or not, including checks that are about SQL validity such as `GROUPING` arguments) skips the fast path and the stage DAG plans `D`. The fast path declining on size or saturation goes to `D`. A local execution failure follows the existing #308 rule. This record does not sort those failures into capability and validity, and does not claim that falling through preserves a correct refusal; the measured fact is only that on the group-key table no post-validation failure of `L` falls through to rows. Finality is not generalized beyond the list above.

#### 1.3 Two plans

1. One planning function, `planStatement`, re-parsing per run (except `L`, which continues from the accepted validation of 1.1). Planner options are read once per statement.
2. `L` (bound) is completed when a local path will consume it: the fast path when it is enabled, a route when one fires.
3. `D` (unbound, today's plan) is built when the statement goes to the stage DAG, and for distributed `EXPLAIN`, which is subject to the veto like any statement.
4. A route runs `L`. If `L` cannot be built, the route reports `L`'s error.
5. An answer, its declared types and its zero-row description come from the plan that answers. A route's error may name the stage planner's refusal that caused the route, as today: diagnostic text, not a second answer.
6. The async door (`SubmitSQL`) is outside this record: it keeps the live provider, plans unbound, and its workers re-plan the text. Its risks are tracked on #1705 and #1676.

#### 1.4 What the paths promise

A statement answered on a local path is planned bound and executed single-process: it answers as the embedded engine does for that statement and catalog. A statement answered by the stage DAG is planned unbound as today. Which path answers depends on the fast-path switch, the size estimate, saturation and routing, so the answer of a statement both paths accept can differ between paths wherever the stage planner has a defect the bound plan does not (the arm tables' kept pins). This record does not remove that. It removes the case where the bound validation refuses and an unbound plan answers.

"The fast path equals the single arm cell for cell" is not promised. Seven group-key cells stay apart: 4 `UNION`-types cells that both sides refuse with different codes while PostgreSQL answers; 2 statements the size estimate declines although only `L` can answer them (a routing gap, a line on #1678); 1 legal nondeterminism.

#### 1.5 Routed cells

Routes run `L`, so routed stage-DAG-arm cells move to the single arm's answer. Equality with the single arm is not correctness: of the routed group-key arm-cells that move, 420 move to PostgreSQL's answer and 24 are wrong before and after. No kept pin changes until a complete base-and-tip census of every routed family, with PostgreSQL's answer beside each cell, shows no routed cell that was PostgreSQL's at base and is not at tip (nondeterministic cells compared by their own rule).

### 2. One authorization state per statement

- **A1. One state.** When the coordinator accepts a statement it takes one authorization state: the provider's policy state and the bind verdict that belongs to it, as a consistent pair. Every authorization decision of the statement, in every planning run and during execution, is made against it.
- **A2. One time.** The decision time is stamped once, with the state.
- **A3. A refusal is final once observed.** A statement refused on authorization by any run, at logical or physical planning, reports that refusal, and no other plan is built or run after it. Which decisions a run meets depends on its plan (section 1.2).
- **A4. The accepted state governs.** A policy change after acceptance applies from the next statement.

A4 is the reference's behaviour: on PostgreSQL 17.11 a running statement is not refused mid-execution by a concurrent policy or grant change, and `ALTER POLICY` waits for it (five cases measured, 2026-11-17).

The pair is read state, verdict, state again, and retried when the state moved. Measured on a model of the provider's store order: the plain two-load read can return an older state with a newer "no refusal", which held at no moment of the read and fails open; the re-checked read cannot, and its other interleavings fail closed. The implementation is verified against the provider's real install and reload paths, not only the model.

**Audit** (corrected 2026-11-20; the earlier sentence "one audit record is written per statement" is withdrawn). A column-policy audit emission is not repeated within a statement: for one identity, one table and one pair of masked list and denied list, compared as ordered lists as enforcement produces them, at most one record is written however many plans reach enforcement. Completeness is not promised and is not changed: which plans reach enforcement depends on section 1.3, and the lookup for scans the optimizer mints may return obligations without an audit call today (a line on #1677, not measured).

### 3. A statement reads one relation per name

While a catalog is serving, a name changes meaning in one way: `DROP` followed by `CREATE`. Each `CREATE` mints a fresh incarnation on the table record and on its manifest ([ADR-0030](0030-a-dml-statement-commits-against-the-manifest-it-read.md), #919). A statement already pins each table's manifest on first read.

Measured: after a drop and re-create with an added column, the statement's pinned manifest names the old incarnation while the metadata read that enforcement uses for the column list returns the new one.

- **C1. Pin first.** Before the statement decides anything about a table from its record (an authorization decision, the scan's declared schema, a star expansion), the statement's manifest pin for it exists.
- **C2. Verify after.** Before a plan takes effect, the table record's incarnation equals the pinned manifest's, for every table the plan reads.
- **C3. A detected mismatch refuses the statement** with `40001`, and no plan is built or run after the check that detects it.
- **C4. One pin per statement,** shared by every planning run.

**Where the check must run** (corrected 2026-11-20; "no other plan is tried" was stronger than the landed code). Measured on main: after a table is re-created, a refused-plan route can plan and run to completion before the refusal; the client gets `40001` and no row, and the route's result is discarded. The validation of section 1.1 can also observe a re-created table and record the mismatch without returning it. So with section 1 the check runs immediately after the veto, whatever comes next, and a mismatch refuses before each of:

- `L`'s logical build after the accepted validation;
- `D`'s planning directly after an accepted validation when the fast path is disabled;
- distributed `EXPLAIN`'s planning of `D` after an accepted validation;
- `D`'s planning after `L` failed to build, after the fast path declined (size, saturation), and after a fast-path physical-planning failure or a retryable execution bail-out;
- each route's entry, before it obtains or physically plans `L` (the refused-plan routes and the unreachable-output route);
- the input-lost re-execution, before it re-enters;
- any result being returned.

**What holds under a concurrent `DROP` and `CREATE`.** A table's incarnation is pinned at the statement's first read of it. With section 1 the first reads move earlier: the veto makes them for the relations it reaches (FROM items, CTE bodies, expression subqueries it traverses), later planning for the rest (`TABLESAMPLE`-argument subqueries, derived bodies the binder skips, scans the optimizer mints). That changes which interleavings refuse and which answer. In every interleaving: any rows returned come from one incarnation per table, and a detected mismatch refuses `40001`. Other refusals remain possible: a table dropped before the statement's first read of it and created after is `42P01`, since nothing was pinned.

**Every coordinator statement, with or without an authorization provider** (amended 2026-11-19, maintainer's ruling). The relation a statement reads is a correctness invariant, not an authorization feature. Measured at slice 2 with the check off: a statement racing a drop and re-create answers the old incarnation's rows under the new record's column list (`SELECT id, owner` answers `owner = NULL`), with a provider or without one. Slice 2 refused it only under an enabled provider; the check now runs for every coordinator statement. Tracked on #1676.

**What the check is.** Protection against ORDINARY catalog changes while a catalog is serving: a `DROP` and re-`CREATE` under a running statement. It is not a universal guarantee of one relation identity per statement: it does not cover a catalog restore (below), the async door, or the embedded doors.

**What equality proves, and where it stops.** `CREATE` mints a random incarnation, so between two catalog restores no two creations share one, and equality at C2 means every read between the pin and that moment was of the same relation.

A catalog restore is outside it, and is a catalog-consistency hazard in its own right, tracked on #1676, not merely an exception to uniqueness. `Catalog.Restore` writes every key of a snapshot back as it was, old incarnations included, one key at a time, without a compare-and-swap and without removing keys the snapshot lacks (`internal/storage/catalog/snapshot.go`). It runs when a server starts: always when `--force-restore-catalog` is given, otherwise only when the catalog holds no tables. It is NOT confined to a moment with no readers:

- In coordinator mode the catalog store is shared. Another coordinator can be serving statements, and workers can be running tasks, while a starting coordinator restores.
- In standalone mode the restore runs before the listeners open but after the in-process worker, the query reaper and (when enabled) the alert scheduler have started.

So an incarnation is unique among the creations made between two restores of one catalog store, and is not unique across a restore. A table can go from incarnation X to Y (a drop and re-create) and back to X (a restore of a snapshot taken at X); a statement that pinned X, decided from Y's record, and verified after the restore passes C2. That needs a drop, a re-create and a restore inside one statement's planning. It has not been reproduced.

From that source, and not reproduced: a statement running during a restore can read some keys restored and others not; restored manifests name files reclaimed since the snapshot (a loud read failure) and omit files written since; an ingester bound to incarnation X is accepted again once X is back ([ADR-0030](0030-a-dml-statement-commits-against-the-manifest-it-read.md)'s flush check has the same gap). Workers hold no incarnation and read what the coordinator's plan names. "Restore only a catalog nobody is reading" is a convention nothing enforces, so it is not a correctness guarantee.

The repair is not this record's: an enforceable restore boundary that excludes concurrent readers, or an atomic catalog-generation transition. Comparing a store revision beside the incarnation would catch X, Y, X on one store and would not solve partial restoration.

**Doors not covered (measured or read at the extension's landing, f6f62320).** The embedded doors and the HTTP door's own local path show the same race, measured (`SELECT id, owner` answers `owner = NULL`). The async door's workers re-plan the statement's text at task time against the live catalog, so a coordinator-side check cannot prove the rows there; covering it needs each task to carry the pinned incarnation. Both are lines on #1676.

**Where the check sits.** At the catalog's table-record read, under the statement's context: a record read pins the manifest and compares that exact record's incarnation with the pin's, so any record the statement uses was of the pinned incarnation, whichever of the two reads came first. Measured cost of this mechanism alone, at its landing (f6f62320): no added catalog read per statement, and no measurable planning time on the 22 TPC-H statements.

**Cost of the statement as section 1 builds it** (measured on the build, 2026-11-20; the sentence above is not true of a whole statement any more). The veto's bound validation reads each table's record and metadata before `D`'s own validation reads them again. Catalog reads per statement, base to build (table-record reads / metadata reads; manifest reads unchanged):

| | Single table | Three-table join |
|---|---|---|
| Fast path, no provider or provider disabled | 6 / 5, unchanged | 18 / 15, unchanged |
| Fast path, provider enabled | 11 / 13 to 12 / 13 | 29 / 37 to 32 / 37 |
| Stage DAG, no provider or provider disabled | 6 / 6 to 7 / 8 | 21 / 18 to 24 / 24 |
| Stage DAG, provider enabled | 11 / 14 to 13 / 18 | 31 / 40 to 37 / 52 |

With the veto's bind removed, the stage-DAG rows return exactly to the base figures, so those reads are the veto's. The one added record read per table on the fast path under an enabled provider is not explained (a hypothesis only: the bind now sits outside `L`'s run and no longer shares a read). Planning time on TPC-H SF0.01, base to build: fast path off, median of per-query medians 3.93 to 5.52 ms, worst 10.6 to 21.6 ms; fast path on, 4.17 to 4.77 ms, worst 12.4 to 9.7 ms. These were measured against an in-process catalog store; against a remote store each read is a network round trip, so the read counts, not the milliseconds, are the figure to carry.

At a route's entry, at `D` after a local run and at the input-lost re-execution, the check re-reads the record of every pinned table: a table read only by the veto is otherwise not read again before the route's pipeline reads it, and the route would run. That is one record read per pinned table on routed and fall-through statements only; the figure is read from the source and not measured. The other transitions directly follow a validation's or a planning run's reads and add none.

## What is out

- The stage DAG's plan stays unbound. A non-routed DAG-arm cell changes only where the veto of section 1.1 refuses it: measured on the group-key table with the fast path off, 3,546 arm-cells: 3,047 identical; 420 routed, wrong to PostgreSQL's answer; 24 routed, wrong before and after; 33 direct, wrong to PostgreSQL's refusal; 15 direct, PostgreSQL's code before and after with the bound validation's message; 6 run-to-run noise; 1 legal nondeterminism. Each stage-DAG statement pays one bound validation: a median 2.0 ms on TPC-H SF0.01 (largest per-query median 5.3 ms).
- The async door (`SubmitSQL`).
- No catalog view: two runs may resolve a name against different catalogs. C1 to C4 refuse the statement when a table it reads was dropped and re-created under it; they do not make the runs see one catalog, and they do not cover the system relations, which have no manifest and no incarnation.
- A door's own checks before it calls the coordinator (the HTTP preflight) decide earlier, against the live provider. A refusal there is final; an allow is decided again under the accepted state, which can refuse and cannot widen.
- The embedded doors, the DML and listing doors, the alert statements and the query-ownership check: unchanged.

## Acceptance

Sections 2 and 3 landed against the earlier list (the static provider-read test, the reload grid, the re-create tests with a provider enabled, disabled and absent, the catalog-read overhead). For section 1, none of the items below is claimed until its build measures it:

1. Group-key table: the fast-path arm equals the single arm except named cells of the three classes of section 1.4. Nondeterministic cells are compared by the table's skip rule, never by "must differ".
2. PostgreSQL agreement is reported separately at base and tip, under the stated rule and as exact text, with the list of cells PostgreSQL-right at base and not at tip. That list is empty apart from named nondeterministic cells.
3. Fast path off: a complete base-and-tip dump of every arm table. Every moved stage-DAG arm-cell is either a veto (listed, with PostgreSQL's answer) or routed (listed, with the single arm's and PostgreSQL's). Section 1.5's condition holds before any pin changes.
4. The veto is identical with the fast path on and off over the corpus; its three outcomes are distinguished; an authorization refusal at the veto builds no plan.
5. A static test names the terminal provenances of section 1.2 and fails on a new one; no refusal is terminal by SQLSTATE class; cancellation is terminal at planning and at execution.
6. Each transition of section 3's list, by name, including the two directly after the veto, with the route or dispatch counter not moving.
7. The audit contract across `L`, `D`, a rebuilt `L`, and a failure before enforcement.
8. Section 2's reload grid and section 3's re-create tests with the event between the veto and `L`, and between `L` and `D`. No fail-open pair.
9. Routes under a mask, a row filter and a denial.
10. Whether `L` and `D` share the table-function context, measured with a reader input changed between plans.
11. Planning time per statement at base and tip, fast path on and off, re-measured on the build; the TPC-H route counter stays 0.

## Alternatives beaten

- **Each run decides against the live provider.** Smaller, but a statement refused under one state could answer under another, and which policy governed would depend on which plan answered. Rejected by the maintainer, 2026-11-18: an existing window is not a reason to widen it.
- **Wait for the per-statement catalog and authorization views.** Larger, unaccepted, and not needed for the measured gain.
- **Bind the one plan and give it to the stage planner too.** Measured: it moves stage-DAG cells the wrong way.
- **Clone the bound plan before the stage planner mutates it.** No clone of a plan with its shared AST pointers exists, and it would not give the stage planner an unbound plan.
- **Two plans with fall-through and no veto** (this record as first accepted). Measured 1,159 of 1,182, and one statement the bound validation refuses is answered with wrong rows by the unbound plan.
- **The veto only when `L` is built.** Keeps every non-routed DAG cell unchanged, but whether such a statement is refused or answered then depends on the fast-path switch and the statement's size, and the measured wrong-row statement stays answered when the fast path is off. Rejected by the maintainer, 2026-11-20.
- **The bound validation as the authority on validity.** It agrees with PostgreSQL on all 351 corpus disagreements and is wrong on a measured statement outside the corpus.

## Consequences

- A statement that goes to the stage DAG is validated bound, then planned unbound; one that tried the fast path first is planned twice; a route can plan a third time.
- A statement the bound validation refuses is refused on every synchronous path, including where the stage DAG answered it before, and including the validator's false refusals.
- A statement that today falls through to the DAG after a fast-path authorization refusal at physical planning is refused.
- A statement racing a drop and re-create of a table it reads is refused with `40001` where before it could answer one incarnation's rows under the other's columns.
