# Tracked query owner authorization

Source: internal/coordinator/coordinator.go — authorizeQueryAccess, moved 2026-09-11 (#1026)

authorizeQueryAccess is the ONE owner-or-admin decision for a tracked
query, and every door asks it here rather than at its own handler: the
lifecycle methods below take a context so a door cannot reach a query
without the question being asked (#936, ADR-0034).

The rule:

  - No provider, or auth disabled: nil. Owners are empty and every caller
    may act, which is what the embedded and dev paths have always done.
  - No identity in the context under auth enabled: refused. Every door
    that reaches here has authenticated its caller.
  - The identity's NAME equals the recorded owner's: allowed. The name is
    the principal the authenticator resolved; how they proved it (an API
    key on one call, a JWT on the next) is not an ownership property, and
    PostgreSQL's own rule for cancelling a backend is the same one.
  - Otherwise the `admin` permission: allowed.
  - An UNOWNED entry (the zero snapshot under auth enabled — a query
    registered before this arc, or an internal entry reached by its id) is
    therefore administrator-only. That is the fail-closed reading: nobody
    can claim what nobody owns.

The refusal is a `sqlerr` 42501 so each door renders it in its own class:
HTTP 403, gRPC PermissionDenied, pgwire SQLSTATE 42501 — never 404, which
would answer a different question than the one that was asked.
