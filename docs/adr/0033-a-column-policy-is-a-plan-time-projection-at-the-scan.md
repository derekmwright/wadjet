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
it.**

This is PostgreSQL's RLS ordering. The POLICY's predicate reads the row as
stored, so a row filter written against a masked column compares the TRUE
value. A predicate the USER writes sits above the projection, so
`WHERE ssn = '<true value>'` matches nothing and `WHERE ssn = '<mask>'` matches
every visible row. `auth.EnforcePlanPolicies` injects the projection first and
the row filter second for exactly this reason; `InjectRowFilter` lands directly
above the Scan.

**7. An expression subquery is planned under the same policy.**

`(SELECT MAX(ssn) FROM t)`, an IN set and an EXISTS are whole second queries
that `physical.buildSubqueryPipeline` parses, builds and optimizes on its own;
they never passed through the enforcement path. The resolved policies travel on
the CONTEXT (`logical.ContextWithColumnPolicies`), which is the only carrier
both planning paths already share, and that path applies the same projection
and the same 42703 binding.

**8. An unpoliced identity is unchanged.**

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
- The declared wire type of a masked column does not move: `ssn` stays `text`,
  `acct` stays `bigint`, and `SELECT *` under a deny policy omits the column
  from the RowDescription.
- The projection is one plan node per POLICED scan. A query by an identity with
  no column obligations gets no node and no cost.
- What is NOT settled here: per-row / per-cell labels (a visibility column, a
  `has_access` function family, dictionary-level evaluation) are a 0.19 arc,
  not this one.

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
