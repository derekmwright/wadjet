package auth

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// RequirePermission enforces that the caller in ctx holds perm, but only when
// the provider is present and auth is enabled. It is the gate for privileged
// DDL (e.g. CREATE/DROP/ALTER ALERT, CREATE/DROP TABLE, ANALYZE, CREATE/DROP
// FUNCTION) that has no per-row ABAC surface of its own. Fail-closed contract:
// with auth enabled, a missing identity or an identity lacking perm is
// rejected; with auth absent/disabled it returns nil (dev/embedded, nothing to
// enforce).
//
// The refusal carries PostgreSQL's 42501 (insufficient_privilege), because a
// client branches on the CLASS and this is the only thing it can branch on: an
// authorization refusal that crossed pgwire without a code arrived as the
// blanket 42000, indistinguishable from a syntax error, and through the HTTP
// door as a bare message. `sqlerr.Wrap` keeps the chain, so
// `errors.Is(err, ErrUnauthorized)` still holds for the in-process callers.
func RequirePermission(provider *Provider, ctx context.Context, perm string) error {
	if provider == nil || !provider.Enabled() {
		return nil
	}
	id := IdentityFromContext(ctx)
	if id == nil {
		return sqlerr.Wrap("42501", fmt.Errorf(
			"%w: authentication required for this operation", ErrUnauthorized))
	}
	if authz := provider.Authorizer(); authz != nil && authz.HasPermission(id, perm) {
		return nil
	}
	return sqlerr.Wrap("42501", fmt.Errorf(
		"%w: permission denied: %q permission required (identity %q, role %q)",
		ErrUnauthorized, perm, id.Name, id.Role))
}

// IdentitySnapshot is the persistable subset of an Identity sufficient to
// re-establish its ABAC subject later (see Identity.ToSubject, which keys on
// role/name/method plus attributes). It is stored with definer's-rights
// resources — an alert runs under its creator's identity on every scheduled
// tick, so the creator's role and attributes must survive in the catalog.
// Tables/Perms are intentionally omitted, and that is now load-bearing rather
// than an economy: they are pure configuration, resolved from the ROLE, so
// persisting them would freeze a grant at creation time. `StampDefiner`
// re-resolves them from the Authorizer's current roles on every tick
// (`Authorizer.ResolveRole`), which is what makes a narrowed or deleted role
// take effect on the alerts its holder created.
type IdentitySnapshot struct {
	Name       string            `json:"name,omitempty"`
	Role       string            `json:"role,omitempty"`
	Method     string            `json:"method,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// SnapshotIdentity captures the identity in ctx as an IdentitySnapshot. The
// zero snapshot (all fields empty) means no identity was present — callers
// persisting it record "no definer", which the scheduler treats fail-closed.
func SnapshotIdentity(ctx context.Context) IdentitySnapshot {
	id := IdentityFromContext(ctx)
	if id == nil {
		return IdentitySnapshot{}
	}
	var attrs map[string]string
	if len(id.Attributes) > 0 {
		attrs = make(map[string]string, len(id.Attributes))
		for k, v := range id.Attributes {
			if s, ok := v.(string); ok {
				attrs[k] = s
			} else {
				attrs[k] = fmt.Sprintf("%v", v)
			}
		}
	}
	return IdentitySnapshot{Name: id.Name, Role: id.Role, Method: id.Method, Attributes: attrs}
}

// Empty reports whether the snapshot carries no usable identity — either no
// definer was recorded (pre-definer-rights alert) or it was created with no
// authenticated identity. Enforcement treats an empty snapshot fail-closed.
func (s IdentitySnapshot) Empty() bool {
	return s.Name == "" && s.Role == "" && s.Method == "" && len(s.Attributes) == 0
}

// StampDefiner stamps snap's identity onto ctx for definer's-rights execution
// (e.g. a scheduled alert query running as its creator) and reports whether
// the definer is attributed — i.e. whether a real identity was recorded.
//
//   - provider nil / auth disabled: ctx is returned unchanged with true —
//     there is no policy to enforce (dev/embedded).
//   - auth enabled: snap.ToIdentity() is ALWAYS stamped, even for an empty
//     (legacy) snapshot. A nil identity is refused outright now (ADR-0034
//     item 7), and an unattributed alert should say WHY rather than fail as
//     "authentication required": stamping a role-less identity routes it into
//     the same default-deny with attributed=false, so the caller can warn that
//     the alert needs recreating under an identity.
//
// The stamped identity's GRANTS are re-resolved from the Authorizer's current
// role definitions (`Authorizer.ResolveRole`). A snapshot records who the
// definer was and not what they could do, so the grants cannot be stale: an
// alert whose creator's role has since lost `write`, or been deleted, is
// refused on its next tick. Without this the definer carried an empty `Perms`
// and `Tables`, which the coarse gate and `CanAccessTable` read — so on a
// `roles:`-only deployment every scheduled alert stopped running.
func StampDefiner(ctx context.Context, provider *Provider, snap IdentitySnapshot) (context.Context, bool) {
	if provider == nil || !provider.Enabled() {
		return ctx, true
	}
	id := snap.ToIdentity()
	provider.Authorizer().ResolveRole(id)
	return ContextWithIdentity(ctx, id), !snap.Empty()
}

// ToIdentity reconstructs an *Identity for context stamping. Attributes are
// widened back to the Attributes (map[string]any) shape ToSubject expects.
func (s IdentitySnapshot) ToIdentity() *Identity {
	var attrs Attributes
	if len(s.Attributes) > 0 {
		attrs = make(Attributes, len(s.Attributes))
		for k, v := range s.Attributes {
			attrs[k] = v
		}
	}
	return &Identity{Name: s.Name, Role: s.Role, Method: s.Method, Attributes: attrs}
}
