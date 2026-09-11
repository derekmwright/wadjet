# Auth policy binding cas

Source: internal/auth/provider.go — func (p *Provider) BindToCatalog(ctx context.Context, cat *catalog.Catalog) error {, moved 2026-09-11 (#1026)
Superseded: A failed bind keeps the installed policy state but records BindError, causing enforcement to refuse; it does not keep serving requests under the previous unbound state.

BindToCatalog attaches the catalog a policy's names are resolved against and
binds the CURRENT policy set to it.

It is separate from NewProvider because of startup order: the provider is
built from the config file, and the catalog does not exist yet at that
point. Calling this is what turns "a policy that names no relation" from a
rule that silently never matches into a startup refusal.

A failure returns the error and swaps NOTHING — the caller decides whether
that is fatal (it is, at startup) — and the provider keeps running on the
policy set it already had.

It is IDEMPOTENT: attaching a set that is already bound to this catalog
writes nothing at all. The HTTP DML door re-attaches per statement, so that
is a request-path property, not an optimization.

The swap is a CAS, not a store. BindToCatalog reads the running set, binds a
COPY of it, and installs the copy; a set installed between the read and the
install would otherwise be OVERWRITTEN by the older snapshot — a retired
policy set coming back, which is a security control silently reverting. On a
lost CAS the newly installed set is bound instead.
