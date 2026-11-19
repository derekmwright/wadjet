# ADR-0050: A coordinator statement plans under one authorization state, and its local paths plan the bound statement

- Status: Accepted (2026-11-19) for implementation. This record is stage 5a of the column-identity program and nothing else. Amended 2026-11-19 (section 3: the check covers every coordinator statement; the incarnation's uniqueness is stated with its restore exception).
- Deciders: Derek Wright
- Related: [ADR-0047](0047-a-column-reference-has-one-resolved-identity.md) (stages 1 to 3: the binding this record makes the coordinator use), [ADR-0034](0034-every-door-authorizes-before-it-acts.md), [ADR-0033](0033-a-column-policy-is-a-plan-time-projection-at-the-scan.md), [ADR-0030](0030-a-dml-statement-commits-against-the-manifest-it-read.md) (the table incarnation), [ADR-0012](0012-sql-semantics-authority.md) item 7. Epic #1678.
- Not accepted by this record: the positional stage model (the draft numbered 0048) and the per-statement catalog view (the draft numbered 0049). Neither is policy. Nothing here depends on them.

## Context

The coordinator plans a `SELECT` once, unbound (`auth.ValidateStatementColumns`), and hands that one plan to its local fast path, to the stage planner, and to any route that runs a refused plan locally. The embedded engine plans the same statement bound (`auth.BindStatementColumns`). So the coordinator's fast path, which is on by default for every statement under 64 MiB of scan, answers differently from the embedded engine.

Measured at main, 2026-11-18, on the group-key table's 1,182 cells with the coordinator arms forced onto the fast path:

| | Unbound (today) | Bound |
|---|---|---|
| Equal to the single-process arm | 688 | 1,181 |
| PostgreSQL's exact text, per arm | 554 | 1,032 |

493 cells move, none regresses. The one remaining cell is the same `0A000` refusal with a different literal in its message. Planning costs a median 4.9 ms per TPC-H statement (worst 15.3 ms) and 0.8 ms per short statement (p99 5.5 ms); binding itself adds nothing measurable.

Agreement with the single-process engine is not agreement with PostgreSQL. The single-process arm differs from PostgreSQL's text on 150 of the 1,182 cells: 53 in message wording only, 97 in substance (69 of them loud refusals), all recorded. This record moves the coordinator's local paths to where the single-process engine is, and no further.

The stage planner cannot take a bound plan yet (binding it moves cells the wrong way; that is the unaccepted work). So a statement needs two plans: a bound one for the local paths and an unbound one for the stage DAG. Two plans raise three questions this record answers: which plan answers, which authorization decision governs, and what happens if the catalog changes between them.

## Decision

### 1. Every answer comes from one plan

Every answer, error and result description a statement returns is produced by exactly one plan, built and enforced by one complete planning run. Nothing from one plan is combined with another.

1. The coordinator's sequence from `ExtractSelect` to `EnforceOptimizedPlan` is one function, `planStatement(ctx, sql, stamp)`. It returns the plan, the context its enforcement rebound and its plan text as one value, and re-parses on each call. Planner options are read once per statement.
2. `ExecuteSQL` builds `L` (bound) first. The fast path consumes `L`.
3. A statement that goes on to the stage DAG builds `D` (unbound). The stage planner and distributed `EXPLAIN` consume `D`.
4. A route runs `L`. A plan value records its consumption; a route whose `L` was consumed by physical planning builds a fresh `L`. A second use of one value is a coding error the suite asserts.
5. Result columns, declared types, plan text and zero-row descriptions come from the plan that answers.
6. If `L` fails to build for a reason other than authorization, the fast path is skipped and the DAG runs `D`, as today. A route that then needs `L` reports `L`'s error.
7. The async door (`SubmitSQL`) is unchanged: it plans unbound and the worker re-plans the text.

### 2. One authorization state per statement

- **A1. One state.** When the coordinator accepts a statement it takes one authorization state: the provider's policy state and the bind verdict that belongs to it, as a consistent pair. Every authorization decision of the statement, in every planning run and during execution, is made against it.
- **A2. One time.** The decision time is stamped once, with the state.
- **A3. A refusal is final.** A statement refused on authorization by any run, at logical or physical planning, reports that refusal. No other plan is built or run in its place.
- **A4. The accepted state governs.** A policy change after acceptance applies from the next statement.

A4 is the reference's behaviour: on PostgreSQL 17.11 a running statement is not refused mid-execution by a concurrent policy or grant change, and `ALTER POLICY` waits for it (five cases measured, 2026-11-17).

The pair is read state, verdict, state again, and retried when the state moved. Measured on a model of the provider's store order: the plain two-load read can return an older state with a newer "no refusal", which held at no moment of the read and fails open; the re-checked read cannot, and its other interleavings fail closed. The implementation is verified against the provider's real install and reload paths, not only the model.

One audit record is written per statement.

### 3. A statement reads one relation per name

While a catalog is serving, a name changes meaning in one way: `DROP` followed by `CREATE`. Each `CREATE` mints a fresh incarnation on the table record and on its manifest ([ADR-0030](0030-a-dml-statement-commits-against-the-manifest-it-read.md), #919). A statement already pins each table's manifest on first read.

Measured: after a drop and re-create with an added column, the statement's pinned manifest names the old incarnation while the metadata read that enforcement uses for the column list returns the new one.

- **C1. Pin first.** Before the statement decides anything about a table from its record (an authorization decision, the scan's declared schema, a star expansion), the statement's manifest pin for it exists.
- **C2. Verify after.** Before a plan takes effect, the table record's incarnation equals the pinned manifest's, for every table the plan reads.
- **C3. A mismatch refuses the statement.** No other plan is tried.
- **C4. One pin per statement,** shared by every planning run.

**Every coordinator statement, with or without an authorization provider** (amended 2026-11-19, maintainer's ruling). The relation a statement reads is a correctness invariant, not an authorization feature. Measured at slice 2 with the check off: a statement racing a drop and re-create answers the old incarnation's rows under the new record's column list (`SELECT id, owner` answers `owner = NULL`), with a provider or without one. Slice 2 refused it only under an enabled provider; the check now runs for every coordinator statement. Tracked on #1676.

**What equality proves, and where it stops.** `CREATE` mints a random incarnation, so between two catalog restores no two creations share one, and equality at C2 means every read between the pin and that moment was of the same relation.

A catalog restore is the exception. `Catalog.Restore` writes every key of a snapshot back as it was, old incarnations included, one key at a time, without a compare-and-swap and without removing keys the snapshot lacks (`internal/storage/catalog/snapshot.go`). It runs when a server starts: always when `--force-restore-catalog` is given, otherwise only when the catalog holds no tables. It is NOT confined to a moment with no readers:

- In coordinator mode the catalog store is shared. Another coordinator can be serving statements, and workers can be running tasks, while a starting coordinator restores.
- In standalone mode the restore runs before the listeners open but after the in-process worker, the query reaper and (when enabled) the alert scheduler have started.

So an incarnation is unique among the creations made between two restores of one catalog store, and is not unique across a restore. A table can go from incarnation X to Y (a drop and re-create) and back to X (a restore of a snapshot taken at X); a statement that pinned X, decided from Y's record, and verified after the restore passes C2. That needs a drop, a re-create and a restore inside one statement's planning. It has not been reproduced.

What a restore under live readers means beyond this check, none of it changed by this record: restored manifests name the files of the snapshot's moment, so files written since are unreferenced and files reclaimed since are named and missing (a loud read failure); a reader mid-restore can see some keys restored and others not; an ingester bound to incarnation X is accepted again once X is back ([ADR-0030](0030-a-dml-statement-commits-against-the-manifest-it-read.md)'s flush check has the same exception); workers hold no incarnation and read what the coordinator's plan names. Every restored key is a new write, so caches keyed by a key's store revision or by the catalog generation are invalidated by it. A forced restore is therefore an operation for a catalog nobody is reading, which nothing enforces today (#1676).

Not taken, and not authorized by this record: comparing the table record's store revision as well as its incarnation (the revision only moves forward, so it would also catch X, Y, X on one store), or refusing a forced restore while another coordinator holds leadership.

The refusal is `40001`, the code this engine already gives a concurrent catalog change a client should retry (`wadjet/dml.go`, `catalog/dml_commit.go`). PostgreSQL does not reach this state: its `DROP` waits for the running statement. That is a mode this engine lacks; the bounded answer is the refusal.

## What is out

- The stage DAG's plan stays unbound. No DAG-arm answer, error or declared type changes on a non-routed cell.
- No catalog view: two runs may resolve a name against different catalogs. C1 to C4 refuse the statement when a table it reads was dropped and re-created under it; they do not make the runs see one catalog, and they do not cover the system relations, which have no manifest and no incarnation.
- A door's own checks before it calls the coordinator (the HTTP preflight) decide earlier, against the live provider. A refusal there is final; an allow is decided again under the accepted state, which can refuse and cannot widen.
- The embedded doors, the DML and listing doors, the alert statements and the query-ownership check: unchanged.

## Acceptance

1. A `coord-fastpath` arm equals the `single` arm cell for cell on the group-key table. The one remaining cell is explained and fixed or listed by name.
2. PostgreSQL agreement is reported separately and falls on no cell.
3. No cell that was right becomes wrong.
4. With the fast path off, every DAG-arm cell of every arm table is identical at base and tip, except routed cells, which equal the single arm.
5. A static test: on the `ExecuteSQL` path the live provider is read at one site; `logical.BuildFromSelect`, `auth.EnforcePlanPolicies` and `auth.EnforceOptimizedPlan` are called only inside `planStatement`.
6. The authorization contract under a real reload, between the two plans and inside a fast-path attempt that bails out: a policy tightened, loosened, a mask removed, a refusal followed by a loosening, a reload that fails to bind. No fail-open pair.
7. A table dropped and re-created between the pin and the point a plan takes effect, in one run and between runs, refuses with `40001`, with an authorization provider enabled, disabled and absent. The overhead in catalog reads per statement is measured and reported.
8. The existing policy gates on every door, and one table whose cells each reach a route under a mask, a row filter and a denial.
9. Planning time per statement at base and tip on TPC-H SF0.01 and the group-key table; the TPC-H route counter stays 0.

## Alternatives beaten

- **Each run decides against the live provider.** Smaller, but a statement refused under one state could answer under another, and which policy governed would depend on which plan answered. Rejected by the maintainer, 2026-11-18: an existing window is not a reason to widen it.
- **Wait for the per-statement catalog and authorization views.** Larger, unaccepted, and not needed for the measured gain.
- **Bind the one plan and give it to the stage planner too.** Measured: it moves stage-DAG cells the wrong way.
- **Clone the bound plan before the stage planner mutates it.** No clone of a plan with its shared AST pointers exists, and it would not give the stage planner an unbound plan.

## Consequences

- A statement that goes to the stage DAG is planned twice; a route after a consumed plan, three times.
- A statement that today falls through to the DAG after a fast-path authorization refusal at physical planning is refused.
- A statement racing a drop and re-create of a table it reads is refused with `40001` where before it could answer one incarnation's rows under the other's columns.
