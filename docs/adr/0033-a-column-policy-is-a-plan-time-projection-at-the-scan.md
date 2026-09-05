# ADR-0033: A column policy is a plan-time projection at the scan

Status: Accepted
Date: 2026-09-04
Issue: #859

## Context

Wadjet's ABAC obligations include `mask_column` (replace a column's value) and
`deny_column` (remove it). Until v0.18.31 they were enforced in two different
places, and neither of them was where a value can actually be caught.

`logical.InjectColumnPolicies` wrapped a policed table's Scan in a security
projection — the right design — but it built that projection from the scan's
own column annotation and **returned the plan unchanged when that annotation
was empty**. It is empty for exactly the queries a mask matters most for. And
the relations it was told to police came from `plansql.SelectInfo.Tables`,
which is not a list of tables: a derived table appears there under its own
subquery TEXT, a CTE reference under the CTE's name, and the arms of a UNION
appear not at all.

Where the projection was skipped, a second mechanism was supposed to catch it:
`Server.applyColumnPolicies` rewrote RESULT ROWS after execution. It existed on
one door — HTTP — and could only ever rewrite an output column that still
carried the policed column's name. The census at v0.18.30 (three doors ×
four arms × eighteen shapes) measured what that produced:

| shape | what the analyst got at base |
|---|---|
| `SELECT ssn`, `SELECT *`, `MIN/MAX(ssn)`, `GROUP BY ssn`, `ORDER BY ssn` on the standalone HTTP door | the TRUE values |
| `SUM(acct)`, `COUNT(DISTINCT ssn)`, a self-join on `ssn`, `WHERE ssn = '<true>'`, a window over `ssn` on the same door | computed on the TRUE values |
| `UNION ALL` over a masked column, every door and every arm | the TRUE values |
| `(SELECT MAX(ssn) FROM t)`, every door and every arm | the TRUE value |
| a derived table or a CTE over the table, every door | `access denied to table "(SELECT ssn FROM e7emp)"` |
| `SELECT salary` (denied) | a phantom all-NULL column on the single-process path; the whole `SELECT *` row set on the DAG |
| `WHERE salary > 0` (denied) | answered from the raw column — a working oracle for the denied value |
| `SUM(salary)` (denied) on the standalone HTTP door | the TRUE sum |

## Decision

**1. Masking and denial are plan-time, at the scan, unconditionally.**

The security projection is built from the TABLE's declared catalog schema,
which is always known at enforcement time. It replaces the column for every
consumer above the scan: WHERE, GROUP BY, aggregates, DISTINCT, join keys,
windows, derived tables, CTEs, set-operation arms, `SELECT *`.

**2. The relations to police come from the PLAN.**

`logical.PolicedScanTables` walks the plan for base-table Scans, which is the
one place a derived table, a CTE reference and a UNION arm all resolve to the
relation actually read. The statement's FROM list is still consulted, filtered
to the names the catalog recognises as tables, so a relation the plan carries
no Scan for is still access-checked and a subquery's TEXT is never handed to a
default-deny evaluator.

**3. A denied column does not exist.**

A reference to one is 42703 — the same refusal, byte for byte, that a column
the table really does not have produces, hint list included. This is
implemented by binding names against the schema the identity can see
(`physical.ValidateColumnsUnderPolicy`), not by a second rule about denial, so
every position a name can appear in is covered by the binder that already
knows them all. An identity that may not read a column may not learn that the
column exists.

**4. A policy that cannot be applied refuses.**

`logical.ErrColumnPolicyUnenforceable`. A scan whose columns nobody can name,
or a table with every column denied, is not "no policy": it is a control that
could not be applied, and the query is refused rather than answered unmasked.
This is #802's rule ("a security control never degrades to a grant") applied to
enforcement rather than to configuration.

**5. One enforcement path; the door-specific one is deleted.**

`Server.applyColumnPolicies` and `auth.AccessPolicy.ApplyToRow(s)` are gone. A
legacy YAML `policies:` block keeps working exactly as documented, through
`MigrateRBACToABAC` into the same obligations the plan-time path enforces —
and now reaches the aggregate, the group key and the join key, which a
result-row pass never could.

**6. A row filter is BELOW the security projection; a user predicate is above
it — on every arm, including the DAG's fragment.**

This is PostgreSQL's RLS ordering. The POLICY's predicate reads the row as
stored, so a row filter written against a masked column compares the TRUE
value. A predicate the USER writes sits above the projection, so
`WHERE ssn = '<true value>'` matches nothing and `WHERE ssn = '<mask>'` matches
every visible row. `auth.EnforcePlanPolicies` injects the projection first and
the row filter second for exactly this reason; `InjectRowFilter` lands directly
above the Scan and marks its Filter `PolicyFilter`.

The DAG's scan fragment carries the same two-slot order —
`OpScan → OpFilter(policy) → SecurityProject → OpFilter(user)` — because ONE
slot for both is a disclosure, not a detail. With one slot, a user predicate
the plan left above the barrier (one substitution could not push down, which in
practice means one carrying a subquery) was lowered into it and read the
STORED column: `WHERE bal > (SELECT MIN(bal) FROM t)` over a `bal` masked to 0
returned exactly the rows whose hidden value was positive, while the
in-process pipeline returned none. The other operand is the policy's own mask,
a constant the client knows, so each row's membership in that answer IS the
hidden value.

`physical.CheckSecurityFilterOrder` is the invariant, checked after every
rewriting pass: **no predicate below a security projection may name a column
that projection hides**, the policy's own filter excepted. It refuses `0A000`
rather than trusting the routing, because a pass that copies `FilterExprs`
without its `PostSecurityFilterExprs` companion fails as a disclosure, not as
a wrong count. A pin is never the disposition for a leak.

A row filter naming a column the table does not have refuses (42703): it would
restrict no rows, which is the mask spellings' failure class said for the other
half of a cell policy. It binds against the UNFILTERED schema, because a policy
predicate is allowed to read a column the same policy denies.

**7. An expression subquery is planned under the same policy.**

`(SELECT MAX(ssn) FROM t)`, an IN set and an EXISTS are whole second queries
that `physical.buildSubqueryPipeline` parses, builds and optimizes on its own;
they never passed through the enforcement path. The resolved policies travel on
the CONTEXT (`logical.ContextWithColumnPolicies`), which is the only carrier
both planning paths already share, and that path applies the same projection
and the same 42703 binding.

**8. A DML statement is a write, and its reads see what a SELECT sees.**

An identity whose policies grant no `ActionWrite` on the table is refused with
42501 before anything is read or written. When the write IS allowed, a DENIED
column does not exist inside the statement — in a predicate, as a SET or
INSERT target, or inside a SET expression — and a MASKED column reads as its
mask, so `WHERE ssn = '<stored>'` matches nothing and `SET dept = ssn` writes
the mask. It is a SUBSTITUTION into the statement's own expressions rather
than a projection because a DML predicate is compiled and never planned
(ADR-0031); where the rewrite cannot be done soundly the statement is refused.

**9. An unpoliced identity is unchanged.**

An admin identity, and an in-process caller with no identity at all, see the
raw table. `EnforcePlanPolicies` still no-ops with no provider, no identity or
no evaluator. A network door with a provider wired refuses an unauthenticated
caller before planning, so "no identity" is a state only the embedded API and
the coordinator's `ExecuteSQL` can present.

## Consequences

- The mask's default value is chosen from the column's DECLARED TYPE (`0` for
  numerics, `false` for booleans, `'***'` otherwise) rather than from the Go
  type of a result value. A plan-time projection has to type-check, so a bare
  `'***'` over a BIGINT column would make `SUM(col)` an error. This is the
  same table the deleted row-side `defaultMaskValue` used.
- **A masked column declares the MASK EXPRESSION's type on the wire**, which is
  what PostgreSQL declares for any expression in a SELECT list. Where the mask
  is written in the column's own type — `'***'` over `text`, `0` over
  `bigint` — the declaration does not move, and those are the two the
  type-derived default produces. Where it is not, it moves and the client sees
  the mask's type: a `TIMESTAMP` masked with the default `'***'` arrives as
  `text`, `MAX(ts)` is `'***'`, and `WHERE ts > '2000-01-01'` returns no rows.
  That is the honest consequence of a mask being an expression, and the
  remedy is to write a mask of the column's own type
  (`value: "TIMESTAMP '1970-01-01'"`); a typed placeholder for every one of the
  22 types is NOT settled here. `SELECT *` under a deny policy omits the
  column from the RowDescription on every door.
- The projection is one plan node per POLICED scan. A query by an identity with
  no column obligations gets no node and no cost.
- A policy binds to a RELATION — the catalog table a scan reads — and never to
  an alias. `FROM other AS policed_name` is a scan of `other` and is untouched.
- The optimizer MINTS scans: the decorrelation passes re-parse a subquery from
  its text and build a fresh Scan after enforcement ran. The projection is
  re-applied to any uncovered policed scan immediately after every
  `logical.Optimize`. That pass takes its column list from the SCAN, because
  after pruning the authority is what the scan produces; before the optimizer
  the catalog is the authority, which is decision 1. It adds every MASKED
  column to that list whether the scan's pruned list names it or not — a mask
  is computed from a literal, so publishing it costs nothing, and leaving it
  out turns a predicate above the projection into a read of a column the
  projection does not carry.
- **The relation may not exist yet when enforcement runs.** A table named only
  inside an `IN (SELECT … )` is SQL TEXT at that moment — no Scan, no FROM
  entry — so it was never policed at all, and the semi-join the optimizer
  later built from it had no projection and a predicate over the STORED
  column. `logical.PolicyLookup` rides the context so any pass that meets a
  scan can ask what the policy does to ITS table, taking the same decision
  path (table access, column obligations, row filter) as the first pass.
- **The invariant is asserted, not assumed, over EVERY plan the query builds.**
  `logical.CheckPolicyPlanOrder` runs on the statement's final logical plan AND
  on each plan the physical planner builds for itself — every subquery pipeline
  (which every subquery runner and IN-set materializer reaches) and the DAG's
  scalar producer. A scan of a policed relation with NO projection is `0A000`,
  and so is a non-policy filter between a projection and its scan that reads a
  policed column. Asking only about the STATEMENT's plan is unfalsifiable for a
  relation named only inside subquery TEXT, and what that text contains — a
  derived table, a set operation, a correlation — is the client's choice, so no
  per-shape enumeration can cover it. The line is drawn by the INNER PLAN: a
  subquery the planner folds into the outer plan is ordered with it and
  ANSWERS; a subquery that keeps a plan of its own — a set operation inside an
  `IN`/`EXISTS` list, a correlated scalar, `LATERAL` — REFUSES, whether or not
  the outer statement reads the same relation. Which shapes fold is the
  PLANNER's business and moves with it: a derived table inside an `IN` list
  refused until v0.18.36's set-operation work and answers now, correctly, on
  every arm.
- **Scan-level filter pushdown stops at a security projection.** It evaluates
  against the FILE, so pushing a predicate that sits above the projection makes
  it read the stored column — the in-process twin of a single filter slot on
  the DAG.
- **A policed scan carries no predicate but the policy's own.** Node order
  above a scan says nothing about what is ATTACHED to it: `attachScanPredicates`
  copies a filter's `col <op> literal` conjuncts onto the scan directly beneath
  it for row-group pruning, and the scans the post-optimize pass covers are
  minted by that same optimizer — the inner of a decorrelated `IN`/`EXISTS` is a
  Filter over a bare Scan when the copy happens, and the projection arrives
  afterwards. The scanner then prunes by the STORED column's statistics: `… IN
  (SELECT id FROM emp WHERE ssn = '<the mask>')` skipped every row group whose
  stored range excluded the mask and answered NO ROWS in process, where the DAG,
  which attaches nothing there, answered every row. That was this arc's one arm
  split, and it was a DISCLOSURE — the row set is arithmetic on statistics of
  the hidden column, so a client moving the constant reads its range off the
  answer — not the neutral path difference an earlier revision of this ADR
  recorded on a mis-localized diagnosis. The pass that injects the projection
  now strips the policed attachments (scan predicates, node predicates,
  partition filters) from the scan it covers; `Predicate.FromPolicy` exempts
  the row filter, which reads the row as stored by design (decision 6); and
  `CheckPolicyPlanOrder` asserts it structurally, so an attachment made by a
  future pass refuses rather than prunes.
- A mask expression is evaluated BELOW the barrier, against the row as stored.
  One that reads a column the same rule masks or denies is refused at load and
  at enforcement, because it would publish exactly what the rule takes away. An
  expression over an unrestricted column is allowed and sees the stored row.

### A `query_limit` obligation is a cost ceiling, enforced where the config's is

The obligation was read by the loader and dropped by the evaluator, so
docs/security.md's obligation table said "Not enforced" beside a security
control. It now narrows the SAME cost guard `query_limits:` uses:
`target` names the ceiling (`max_scan_rows` — the default when empty and the
only reading the docs ever gave it — `max_scan_bytes` or `max_scan_files`),
`value` is a positive integer, and anything else refuses at config load and at
hot reload, keeping the previous policy set (decision 4's doctrine).

The two ceilings arrive by different routes because they are decided at
different times: the deployment's is on `Planner.QueryLimits`, set at each
door's one planner-construction site; the identity's is decided while the
policies are evaluated, which is after every planner has been built, so
`EnforcePlanPolicies` puts it on the context and
`Planner.enforceQueryLimits` — the one place the guard runs — takes the
tighter of the two. A policy can only NARROW: an obligation naming a larger
number than the deployment allows does not widen the deployment's guard, and a
statement reading two policed relations is held to the tighter of theirs.

A DML statement is not planned (ADR-0031), so it has no scan-cost estimate and
no ceiling to compare it against; `query_limit` is a read control.

### Not settled

- **A task that carries a statement's TEXT is re-planned where no policy is.**
  `TaskTypePipeline` carries `SQLText`, and `worker/executor.go`'s
  `executePipeline` parses, builds and optimizes it again. Every dispatch site
  that puts such a task on the wire is guarded at the one choke point they all
  go through — `Scheduler.PublishTasks` refuses `0A000` when a policy shaped
  the query and the task carries text with no operator fragment and no inputs —
  so the consequence today is that the async door refuses rather than answers
  unmasked. The fix is a pipeline task that carries the enforced PLAN rather
  than its text; a worker that reconstructs the projection from
  `PolicyDecisionJSON` is a SECOND enforcement path and decision 5 forbids it.
  Its own arc.
- **MERGE under a column policy is refused.** Its WHEN clauses carry raw
  SET/VALUES text that the DML rewriter does not decompose, so it cannot be
  shown to honour the policy. Refusing beats running those reads against the
  stored row.
- **A typed mask placeholder per type** (see the wire-type consequence above).
- **`LATERAL` over a policed relation refuses.** Its decorrelated inner keeps
  its predicate in a shape the planner cannot reorder above the projection, so
  the invariant refuses `0A000` uniformly on every arm rather than answer.
- Per-row / per-cell labels (a visibility column, a `has_access` function
  family, dictionary-level evaluation) are a 0.19 arc, not this one.

## Gate

`internal/server/policy_masking_matrix_test.go`:

- `TestPolicyMaskingIsPlanTimeOnEveryDoor` — three doors (embedded, pgwire via
  pgx, HTTP `POST /v1/queries`) × the arms each can present (single, spilled,
  DAG, DAG-shuffled; pgwire and HTTP in both their single-process and
  coordinator wiring) × twenty-one shapes. Every cell asserts the masked value
  or the refusal, and a blunt textual check that no true value appears anywhere
  in the answer. `WADJET_E7_CENSUS=1` prints the per-cell census instead of
  asserting — that is how the base state above was recorded.
- `TestPolicyMaskingLeavesUnpolicedIdentitiesAlone` — decision 8.
- `TestLegacyYAMLCellPoliciesAreEnforcedAtThePlanOnEveryDoor` — decision 5.
- `TestARowFilterSeesTheTrueValueAndTheUserPredicateSeesTheMask` — decision 6.
- `internal/planner/logical/plan_coverage_test.go` —
  `TestInjectColumnPolicies_SchemaColumnsBeatAnEmptyScan`,
  `_NoColumnsAvailable`, `_AllColumnsDenied` for decisions 1 and 4.
