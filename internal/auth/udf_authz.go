package auth

import (
	"context"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// UDFMutation is everything a CREATE / DROP FUNCTION needs from the
// authorizer, decided in ONE place because the two doors that ran those
// statements had each decided it for themselves and each got it wrong in a
// different direction (#940, #942):
//
//   - `wadjet.DB.Query` — the boundary the embedded caller and pgwire both
//     reach — asked for no permission at all, recorded an EMPTY owner, and
//     passed the literal `isAdmin = true` into `UDFStore.Register` /
//     `Unregister`. That bit is the only thing the `WITH LOCK` ownership check
//     consults, so a role holding only `read` installed process-global
//     functions, replaced another user's locked one and dropped it.
//   - The HTTP handlers asked for no permission either, and computed
//     `isAdmin` as `identity.Role == "admin"` — the role's NAME rather than
//     its permissions. Both directions were wrong at once: a role literally
//     named `admin` whose `allow:` list is `[read]` overrode another owner's
//     lock, while a role named `ops` that actually HOLDS `admin` was refused.
//
// `expr.DefaultUDFs` and `expr.DefaultRegistry` are process-global, so every
// one of those crossed connection, role and tenant boundaries: replacing a
// widely used UDF changes other identities' query RESULTS.
type UDFMutation struct {
	// Owner is the name recorded on the definition, and the name the lock is
	// later checked against. Empty only when there is no provider — with one,
	// a mutation without an identity does not get this far.
	Owner string
	// IsAdmin says whether this caller may override ANOTHER owner's
	// `WITH LOCK`. It comes from `Authorizer.HasPermission(id, "admin")` —
	// the configured permission set — never from the role's name.
	IsAdmin bool
}

// AuthorizeUDFMutation decides a CREATE / DROP FUNCTION.
//
// Mutating the registry needs `write`, the same permission every other
// mutation on this engine needs; overriding another owner's locked function
// needs `admin`. No new permission words (ADR-0034).
//
// Fail-closed contract, the one `RequirePermission` already keeps: provider
// nil or auth disabled → the mutation is ALLOWED (there is no permission to
// require), but the caller is NOT an administrator and records no owner. Auth
// enabled and no identity → refused 42501.
func AuthorizeUDFMutation(ctx context.Context, provider *Provider) (UDFMutation, error) {
	if provider == nil || !provider.Enabled() {
		// `IsAdmin: false`, deliberately. A caller with no identity is not an
		// administrator, and this is not the "auth disabled enforces nothing"
		// case it looks like: the UDF registry is PERSISTED (cmd/wadjet wires
		// `SetPersister` / `LoadDefs`), so it can hold a definition whose
		// owner was recorded while auth was ON. Returning true here let an
		// unauthenticated caller replace and drop THAT — the HTTP door refused
		// it before this batch, and unifying the two doors on `DB.Query`'s old
		// literal `true` would have unified them on half of #940.
		//
		// Nothing an unauthenticated deployment does breaks: a definition
		// created in this state carries the empty owner, and `UDFStore`'s lock
		// check only fires on a NON-EMPTY owner (`existing.def.Owner != ""`),
		// so the same caller can still replace and drop everything it made.
		return UDFMutation{}, nil
	}
	if err := RequirePermission(provider, ctx, "write"); err != nil {
		return UDFMutation{}, err
	}
	// Non-nil: RequirePermission refuses a missing identity above.
	id := IdentityFromContext(ctx)
	m := UDFMutation{Owner: id.Name}
	if authz := provider.Authorizer(); authz != nil {
		m.IsAdmin = authz.HasPermission(id, "admin")
	}
	return m, nil
}

// AuthorizeUDFRead decides a SHOW FUNCTIONS.
//
// It requires an identity and no particular permission. That is PostgreSQL's
// rule for the same information: a role with no privileges on a function reads
// its body and its owner out of `pg_proc.prosrc`, and `\sf` prints the whole
// definition. A function body is not data; it is part of the schema the
// server publishes to the sessions that may call it.
//
// Under auth ENABLED a caller with no identity is still refused, because a
// door that reaches here with nobody has no one to answer.
func AuthorizeUDFRead(ctx context.Context, provider *Provider) error {
	if provider == nil || !provider.Enabled() {
		return nil
	}
	if IdentityFromContext(ctx) == nil {
		return sqlerr.New("42501",
			"permission denied: authentication required for this operation")
	}
	return nil
}
