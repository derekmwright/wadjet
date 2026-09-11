# Auth configuration fail closed

Source: internal/auth/auth.go — func Build(cfg Config) (*Authenticator, *Authorizer, error) {, moved 2026-09-11 (#1026)

Build creates an Authenticator and Authorizer from configuration, reporting
a configuration it cannot honour.

A configuration error is an ERROR, never "authentication disabled" (#931):

  - `jwt.enabled` with no secret and no readable public key, an unparseable
    key file, a typo in the path — `NewJWTVerifier`'s error used to be
    DISCARDED and the verifier left nil.
  - `enabled: true` with no usable mechanism at all: no API keys, no JWT, no
    mTLS. An operator who wrote `enabled: true` did not ask for an open
    server.
  - A credential naming a role the configuration does not define — an
    `api_keys` entry, an mTLS `role_map` value, an mTLS `default_role` — and
    `enabled: true` with credentials but no `roles:` at all. Such a
    credential authenticates and then holds NO permission, so it can do
    nothing; before this batch it could read everything, because the data
    path did not consult the role at all. Either way the configuration
    cannot mean what the operator wrote, and the doctrine is that such a
    configuration refuses at LOAD rather than behaving surprisingly at
    runtime.

On error the returned Authenticator is not a disabled one. It reports
Enabled() and refuses EVERY credential with the configuration error, so a
caller that ignores the error — and every door reads `Provider.Enabled()`,
not this — is closed rather than open. Startup and hot reload both report
it: `cmd/wadjet` refuses to start, and `Provider.UpdateFromConfig` refuses
the swap and keeps the running state.
