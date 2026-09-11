package auth

import (
	"context"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TableAccess is the shared decision before catalog metadata reads or actions.
// Nil/disabled allows; BindError or enabled auth without identity refuses
// (#882, ADR-0033 rule 3). Require role HasPermission in BOTH provider shapes;
// admin grants permissions, but policies only narrow the role's coarse grant.
// Then use ABAC deny-overrides/default-deny, or legacy CanAccessTable without it.
// Supply catalog-resolved table spelling (#731) and DecisionEnvironment's
// trusted context environment with time stamped at decision, not connection.
// Return 42501 (HTTP403/gRPC PermissionDenied); client text must disclose no
// rule/reason. Operator details belong only in audit logs.
// See docs/internals/auth-shared-table-access.md for the design.
func TableAccess(ctx context.Context, provider *Provider, table string, action Action) error {
	return tableAccess(ctx, provider, table, action, "")
}

// tableAccess is THE rule, and the only implementation of it. TableAccess is
// its exported form for a caller that has no protocol label of its own; the
// shared plan and DML paths call it with theirs, so `env.protocol` means the
// same thing to the metadata decision and to the data decision about the same
// relation.
//
// It exists as a separate function only because the exported one cannot take
// the label: the point of ADR-0034 item 5 is that there is ONE rule in ONE
// place, and every door — metadata, plan, DML — reaches this body.
func tableAccess(ctx context.Context, provider *Provider, table string, action Action,
	protocol string) error {
	if provider == nil || !provider.Enabled() {
		return nil
	}
	if err := provider.BindError(); err != nil {
		return sqlerr.Wrap("42501", err)
	}
	id := IdentityFromContext(ctx)
	if id == nil {
		return sqlerr.New("42501", "permission denied for table %q: authentication required", table)
	}
	// The role's `allow` list is a COARSE GATE, and it is applied in BOTH
	// provider shapes, before anything else looks at the relation. A policy
	// NARROWS what a role may do; it never widens it.
	//
	// It did not hold under an explicit `abac_policies:` block: the evaluator
	// answered alone, so a role written `allow: [read]` that a policy
	// permitted to write could write — while DDL on the same door, which asks
	// `RequirePermission`, demanded the permission. Two doors disagreeing about
	// the same identity is the shape this ADR exists to remove.
	//
	// `admin` still grants everything, because `HasPermission` says so.
	authz := provider.Authorizer()
	if authz == nil || !authz.HasPermission(id, permissionForAction(action)) {
		return sqlerr.New("42501", "permission denied for table %q", table)
	}
	env := DecisionEnvironment(ctx, protocol)
	if ev := provider.Evaluator(); ev != nil {
		if td := ev.EvaluateTableAccess(id.ToSubject(), table, action, env); td != nil && td.Allowed {
			return nil
		}
		return sqlerr.New("42501", "permission denied for table %q", table)
	}
	if authz.CanAccessTable(id, table) {
		return nil
	}
	return sqlerr.New("42501", "permission denied for table %q", table)
}

// VisibleTables filters tables to the ones TableAccess allows this identity to
// READ, preserving order.
//
// It is what a listing door (`SHOW TABLES`, the HTTP table list, the gRPC
// ListTables) hands back, so a name an identity may not read is not published
// by the listing either. That is a deliberate divergence from PostgreSQL,
// which shows `\d` to anyone: the product's position is that metadata follows
// the effective table-access decision (ADR-0034), and the HTTP door already
// behaved this way before the other doors did.
//
// With the provider nil or auth disabled the input is returned as it is —
// same slice, same nil-ness — so a no-auth deployment allocates nothing and
// sees no change.
func VisibleTables(ctx context.Context, provider *Provider, tables []string) []string {
	if provider == nil || !provider.Enabled() {
		return tables
	}
	visible := make([]string, 0, len(tables))
	for _, t := range tables {
		if TableAccess(ctx, provider, t, ActionRead) == nil {
			visible = append(visible, t)
		}
	}
	return visible
}

// permissionForAction maps an ABAC Action onto the legacy permission
// vocabulary the Authorizer holds — `read`, `write`, `admin`, and nothing else
// (no new permission words, ADR-0034).
//
// The grouping is the one `MigrateRBACToABAC` already emits, so the legacy
// path and the migrated-ABAC path answer alike for the same role: `read`
// covers reading and describing, `write` covers writing, creating and
// dropping, `admin` covers admin. An action this does not know maps to
// `admin`, which is the narrowest grant that exists — an unrecognized action
// is answered by the fewest identities, never by the most.
func permissionForAction(action Action) string {
	switch action {
	case ActionRead, ActionDescribe:
		return "read"
	case ActionWrite, ActionCreate, ActionDrop:
		return "write"
	default:
		return "admin"
	}
}
