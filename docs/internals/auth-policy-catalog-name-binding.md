# Auth policy catalog name binding

Source: internal/auth/policy_bind.go — func BindPoliciesToCatalog(ctx context.Context, cat *catalog.Catalog,, moved 2026-09-11 (#1026)

Binding a policy's NAMES to the catalog, once, at load.

A policy names relations and columns, and so does a query — but they arrive
spelled differently. A query's unquoted identifier is folded to lower case
by the lexer (#731); a catalog keeps the spelling the parquet file or the
Iceberg import gave it, where CamelCase is ordinary. The engine reconciles
the two with ONE rule (`catalog.ResolveTableName` for a relation,
`batch.ResolveSchemaIndex` for a column): byte-exact first, then a unique
ASCII-case-insensitive match for a name that is itself folded, with a
delimited name staying byte-exact.

Before #882 the policy layer did not use that rule at all. It compared the
operator's YAML bytes against whatever spelling the statement happened to
carry, and the two enforcement paths disagreed about which spelling that
was — so on a CamelCase relation there was no single `table:` an operator
could write that bound on both. A policy that failed to bind did not refuse:
beside the broad allow a `roles:` migration emits, it granted.

Two things follow, and this file is the second:

 1. the COMPARISON is fold-aware wherever a relation name is matched
    (`relationEq`, `policyKey`) — the floor, so no spelling mismatch can
    ever yield "no policy applies";
 2. the BINDING happens ONCE, here, against the catalog, and a policy naming
    a relation or column that does not resolve is REFUSED — the policy is
    rewritten to the catalog's own spelling, so evaluation compares two
    names that came from the same place.

(2) is what makes a typo loud. Without it, `resource.name eq "hitz"` is
indistinguishable from a relation that does not exist yet, and the rule
carrying the obligations silently never matches — which is the same
disclosure #882 was, reached by a different road. ADR-0033's rule stands: a
policy that cannot be enforced does not load.

The consequence an operator must know: a policy may not name a relation the
catalog does not hold. Startup refuses, and a hot reload refuses and KEEPS
THE PREVIOUS SET rather than installing a policy set weaker than the one
running (#802's contract, applied to names).
