# Subquery context policy enforcement

Source: internal/planner/physical/validate_policy.go — applyContextColumnPolicies, moved 2026-09-11 (#1026)

applyContextColumnPolicies enforces the query's policy on a plan this
planner built for ITSELF — the expression-subquery path and the DAG's
scalar-producer path, neither of which passes through
auth.EnforcePlanPolicies.

EVERY RELATION THIS PLAN READS ASKS THE CONTEXT LOOKUP (#945). That is the
same decision `EnforcePlanPolicies` asks for the relations the statement's
own plan named and `EnforceOptimizedPlan` asks for a scan the optimizer
minted — access first, then the obligations that follow from it. A scalar
subquery in the SELECT list is SQL TEXT when enforcement runs, so
`policedRelations` cannot see it and this is the FIRST place its relation is
known; before this pass asked, `SELECT (SELECT MAX(id) FROM other)` answered
the value on every door, under both provider shapes, for an identity whose
role does not list `other` and for one an ABAC policy denies it to. The
spelling does not matter — SELECT list, WHERE, CASE, CTE body — because the
refusal is at the site that turns the text into a plan, not at a walk of the
text.

The pass used to return early whenever the RESOLVED policy set was empty,
which is exactly the case a subquery-only relation produces: the outer
statement carries no policed relation, so there is nothing in the set and
the lookup — the one carrier that knows about the relation — went unasked.
The obligations follow the same seam: a row filter bound to a relation
reached only from a subquery restricted nothing, and a masked relation
reached only from a subquery refused (no projection could be found above its
scan) instead of answering the mask.

It returns an error when the identity may not read a relation this plan
reads, and when a policed scan could not be covered — the alternative to
both is answering that subquery from the raw column.
