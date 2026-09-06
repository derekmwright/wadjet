package auth

import (
	"context"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TableAccess is the ONE effective table-access decision every door asks for a
// catalog table it is about to read metadata of or act on.
//
// It exists because the doors did not agree. `SHOW TABLES` filtered on the
// HTTP door and on no other; `DESCRIBE` refused on one door and answered on
// the rest; a DDL statement asked `HasPermission` and never asked whether the
// identity may touch THAT relation. Each of those is a separate reading of the
// same question, and a security decision with several readings has the weakest
// one for its answer. The decision lives here now, and a door that
// re-implements it has forked it.
//
// The rule, in order:
//
//   - Provider nil, or auth disabled: nil. There is nothing to enforce
//     (dev, embedded without SetAuthProvider), and nothing changes.
//   - A policy set that could not be BOUND to the catalog: refused. An
//     unbindable set enforces nothing, and a rule that matches nothing is a
//     grant beside a broad allow (#882, ADR-0033 rule 3).
//   - Auth enabled with NO identity in the context: refused. Authentication
//     proves who; a door that reaches here with nobody has no one to
//     authorize.
//   - The role's `allow` list, in BOTH provider shapes:
//     `HasPermission(id, perm(action))`. It is a COARSE GATE — a policy
//     narrows what a role may do and never widens it — and `admin` grants
//     everything, as it always has.
//   - Then, with an ABAC evaluator installed: the EVALUATOR decides
//     (`EvaluateTableAccess`), which is deny-overrides with a default deny —
//     explicit denies win, an unmatched request is refused.
//   - With no evaluator: `CanAccessTable(id, table)`, the other half of the
//     legacy rule. The permission alone is not access to a relation, and the
//     relation alone is not permission to write it.
//
// `table` must be the CATALOG-RESOLVED spelling (`catalog.ResolveTableName`):
// an unquoted identifier folds at the lexer (#731), and a policy bound to
// `Users` must police a statement that spelled it `users`.
//
// The environment comes from the context — attached at the protocol boundary,
// never from a caller below it — through the one builder every enforcement
// path uses (`DecisionEnvironment`), so `Time` is stamped at DECISION time. A
// pgwire connection lives for hours; an `env.hour` condition means the hour
// the statement ran.
//
// The refusal is a `sqlerr` 42501, which every door renders in its own class:
// pgwire SQLSTATE 42501, HTTP 403, gRPC codes.PermissionDenied, and the same
// error verbatim on the embedded API.
func TableAccess(ctx context.Context, provider *Provider, table string, action Action) error {
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
	env := DecisionEnvironment(ctx, "")
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
