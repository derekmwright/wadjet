# Auth policy aware name diagnostics

Source: internal/auth/plan_enforce.go — func ValidateStatementColumns(ctx context.Context, provider *Provider, cat *catalog.Catalog, info *plansql.SelectInfo, protocol string) error {, moved 2026-09-11 (#1026)

ValidateStatementColumns is the plan-time name binding every query entry
point runs before it builds a logical plan, done over the schema the CALLING
IDENTITY can see.

It replaces a bare physical.Planner.ValidateColumns at those entry points.
The unfiltered binder answers `SELECT nosuchcol FROM t` with a hint that
lists the table's columns, and a column the policy DENIES has no business in
that list: an identity that may not read `salary` may not learn that
`salary` exists either. The policy is resolved LAZILY, per table, as the
binder resolves relations — so it covers CTE bodies, derived tables,
subquery blocks and set-operation arms without needing a plan.

TABLE-LEVEL DENIAL IS DECIDED HERE TOO, since #946, and it has to be.

The position this comment used to state — that table denial stays in
EnforcePlanPolicies so the order of the two refusals does not change — was
answerable only while the binder's diagnostic said nothing about a relation.
It says a great deal: `SELECT nocol FROM secret` answered `unknown column
"nocol" (available: id, note)` for an identity that may not read `secret`,
on every door and under BOTH provider shapes, and the hint POOLS relations —
`SELECT id FROM emp WHERE id = (SELECT MAX(nocol) FROM secret)` published
emp's columns unioned with secret's. `docs/security.md` and ADR-0034 say an
identity that may not read a table may not read its schema either, and an
error's hint is schema.

So the decision is asked PER RELATION as the binder resolves it
(policedColumnSource.GetTable), which is the only seam that sees CTE bodies,
derived tables, subquery blocks and set-operation arms as well as the FROM
list. It is the SHARED rule — `TableAccess`, ADR-0034 item 5 — so the answer
and its sentence are the ones every other door gives, and it is installed in
BOTH provider shapes: the legacy `roles:` shape denies relations too, and it
was publishing their columns just as loudly.

What changes for a denied relation is the CLASS of an existing refusal:
42703 with a column list becomes 42501 with none. What does not change is a
relation the identity MAY read (42703 with the list, exactly as before), a
relation that does not exist (42P01 — the decision is asked only after the
catalog finds the table), or a statement with no provider.
