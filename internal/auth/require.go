package auth

import (
	"context"
	"fmt"
)

// RequirePermission enforces that the caller in ctx holds perm, but only when
// the provider is present and auth is enabled. It is the gate for privileged
// DDL (e.g. CREATE/DROP/ALTER ALERT) that has no per-row ABAC surface of its
// own. Fail-closed contract: with auth enabled, a missing identity or an
// identity lacking perm is rejected; with auth absent/disabled it returns nil
// (dev/embedded, nothing to enforce).
func RequirePermission(provider *Provider, ctx context.Context, perm string) error {
	if provider == nil || !provider.Enabled() {
		return nil
	}
	id := IdentityFromContext(ctx)
	if id == nil {
		return fmt.Errorf("%w: authentication required for this operation", ErrUnauthorized)
	}
	if authz := provider.Authorizer(); authz != nil && authz.HasPermission(id, perm) {
		return nil
	}
	return fmt.Errorf("%w: %q permission required (identity %q, role %q)",
		ErrUnauthorized, perm, id.Name, id.Role)
}

// IdentitySnapshot is the persistable subset of an Identity sufficient to
// re-establish its ABAC subject later (see Identity.ToSubject, which keys on
// role/name/method plus attributes). It is stored with definer's-rights
// resources — an alert runs under its creator's identity on every scheduled
// tick, so the creator's role and attributes must survive in the catalog.
// Tables/Perms are intentionally omitted: they gate RBAC operations (table
// access, DDL) that the scheduled-query path does not perform — that path
// applies only ABAC plan enforcement, which reads role/attributes.
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
//     (legacy) snapshot. This is deliberate: EnforcePlanPolicies fail-OPENS on
//     a nil identity, so stamping a non-nil role-less identity instead routes
//     an unattributed alert into ABAC default-deny (fail closed) rather than
//     unfiltered execution. attributed is false in that case so the caller can
//     warn that the alert needs recreating under an identity.
func StampDefiner(ctx context.Context, provider *Provider, snap IdentitySnapshot) (context.Context, bool) {
	if provider == nil || !provider.Enabled() {
		return ctx, true
	}
	return ContextWithIdentity(ctx, snap.ToIdentity()), !snap.Empty()
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
