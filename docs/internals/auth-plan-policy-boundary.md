# Auth plan policy boundary

Source: internal/auth/plan_enforce.go — func EnforcePlanPolicies(ctx context.Context, provider *Provider, cat *catalog.Catalog, selectInfo *plansql.SelectInfo, plan *logical.Node, protocol string) (context.Context, *logical.Node, error) {, moved 2026-09-11 (#1026)
Superseded: Enabled auth with no identity now refuses, and legacy providers without an evaluator still enforce role/table access. The old no-op description is stale.

EnforcePlanPolicies applies ABAC to a query at plan level: table-access
denial, column deny/mask injection and row-filter injection for every table
the plan READS. It is THE shared enforcement path — the embedded engine
(wadjet.DB.Query), the HTTP door and the coordinator's native-DAG executor
all call it with the same inputs, so an identity sees identical policy
behavior regardless of which door and which execution path answers.

Masking and denial are PLAN-TIME, at the scan, unconditionally (#859):

  - The security projection is built from the TABLE's catalog schema, which
    is always known here, and never from the scan's pruned column list.
    `SELECT *`, an aggregate-only SELECT list and a derived table all leave
    that list empty, and those are exactly the queries a mask matters most
    for.
  - The relations to police come from the PLAN, not from the statement's
    FROM list. `plansql.SelectInfo.Tables` carries a derived table under its
    own subquery TEXT, a CTE reference under the CTE's name, and NOTHING at
    all for the arms of a UNION — so a `UNION ALL` over a masked column was
    unmasked on every door, and a query with a derived table or a CTE was
    default-DENIED under the name `"(SELECT ...)"`.
  - A column policy that cannot be applied REFUSES. A security control never
    degrades to a grant (#802).

The returned context carries the resolved policies so the physical planner
applies the same projection to an expression subquery, which it plans on its
own (physical.buildSubqueryPipeline).

No-ops (returns the plan unchanged) when the provider is nil/disabled, no
identity is attached to ctx, or the provider has no evaluator — matching the
embedded engine's historical behavior. protocol labels the evaluation
environment for policy conditions and audit.
