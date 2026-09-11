# Pgwire folded nested schema aliases

Source: internal/server/pgwire/paraminfer.go — aliases := make(map[string]parquet.Column), moved 2026-09-11 (#1026)
Superseded: The claim that ordered is always nil on the legacy path predates result-meta structure (#965); it remains nil for this catalog-guess fallback only.

This map is keyed by the CATALOG's spelling and looked up by an OUTPUT
column name — a reference, which an unquoted identifier folds to lower
case at the lexer (#731). CamelCase column names are ordinary in a
catalog, so `SELECT Attrs FROM t` over a column declared `Attrs` missed
here; `ordered` is deliberately nil on this path, so the positional
fallback could not save it either. The consequence is wire-visible and
silent: `formatPgValueTyped(val, nil)` renders a ROW in SORTED-KEY
order rather than declared field order, and loses the ARRAY/MAP
distinction — `(9,A)` came back as `(A,9)`. Publish the folded spelling
as an alias so a folded reference finds its declaration; a byte-exact
entry is never shadowed, and the `conflicting` rule above has already
removed the names two tables spell differently.

TWO catalog names that fold to ONE key are AMBIGUOUS and must resolve to
nothing — batch/schema.go item 3's rule, which the hand-rolled map here
has to carry itself. The `conflicting` pass above cannot see this case:
it keys by the CATALOG spelling, so `Attrs` and `ATTRS` are two distinct
entries that are never compared, and both are TypeRow anyway. Without
the guard both are "untaken" and the winner is whichever Go's map
iteration wrote last: over `nsa.Attrs ROW(zeta,alpha)` joined to
`nsb.ATTRS`, 25 identical runs of
`SELECT a.Attrs FROM nsa a JOIN nsb b ON a.id = b.id` rendered `(9,A)`
twice and `(A,9)` 23 times — the WIRE BYTES changing run to run for one
query over one fixture, which is not one of ADR-0013's eight legal
classes of nondeterminism. Dropping the ambiguous alias renders the
declaration-less way, which is the miss it is, and the same way every
time.
