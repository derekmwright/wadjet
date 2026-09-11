# Auth dml policy boundary

Source: internal/auth/dml_enforce.go — func EnforceDMLPolicies(ctx context.Context, provider *Provider, cat *catalog.Catalog,, moved 2026-09-11 (#1026)
Superseded: Enabled auth with no identity now refuses, and a provider without an evaluator still enforces the shared role/table-access decision; neither is a no-op.

EnforceDMLPolicies applies ABAC to an INSERT / UPDATE / DELETE / MERGE
before any row is read or written. It is called from the ONE DML entry point
(wadjet.DB.ExecuteParsed), so the embedded door, the pgwire door and the
HTTP API server all carry it.

Two rules, and they are the SELECT door's rules said for a statement that
writes (ADR-0033):

 1. **A DML statement is an ActionWrite.** An identity whose policies grant
    it no write on the table is refused with 42501 before anything happens.
    A role allowed only `read` used to be able to run
    `DELETE FROM t WHERE ssn = '<stored value>'` and destroy the row.

 2. **A read inside the statement sees what a SELECT would see.** A column
    the policy DENIES does not exist, so naming it in a predicate, a SET
    target or a SET expression is 42703 — before #859 `UPDATE t SET dept='z'
    WHERE salary = 700009` matched exactly the row with that salary, a
    working oracle for a column the identity may not read. A MASKED column
    reads as its mask, so `WHERE ssn = '<stored value>'` matches nothing and
    `SET dept = ssn` writes '***' instead of copying the stored value into a
    column the identity may read.

Rule 2 is a SUBSTITUTION rather than a projection because a DML predicate is
compiled, not planned (ADR-0031): there is no Scan for a security projection
to sit on. Where the substitution cannot be done soundly the statement is
REFUSED, never run against the stored row.

No-ops when the provider is nil/disabled, no identity is attached, or the
provider has no evaluator — the same contract EnforcePlanPolicies keeps.
