# Scope resolution delimited names

Source: internal/planner/physical/validate.go — resolveRef / refuseDelimitedMiss, moved 2026-09-11 (#1026)

resolveRef resolves ref against this scope, returning nil when it resolves
(or when the scope is too uncertain to judge) and a SQLSTATE-carrying error
for the three refusals PostgreSQL makes at analysis time: an unknown column
(42703), a qualifier no FROM entry provides (42P01, #380), and a bare name
two sources both provide (42702, #367). Every uncertain case resolves —
a false positive breaks a working query; a false negative merely lets a
typo through to the existing runtime check.
refuseDelimitedMiss is the byte-exactness a DELIMITED identifier is owed.

Every other map on this scope is keyed on the FOLDED name, which is the
right key for an unquoted reference because the lexer folded it already
(#731). A reference that still carries an ASCII upper-case letter can only
have been written between double quotes, and PostgreSQL matches such a name
byte for byte: over a column `g`, `SELECT "G"` is 42703 there, and answering
it with `g`'s values — or, as this engine did, with a column of NULLs — is a
silent wrong answer either way.

It fires ONLY where the scope KNOWS the spelling, which is a BASE TABLE's
columns — the ones whose declared type this binder carries, so colTypes is
the test. Every other name here has passed through a planner pass that may
have lowercased it before registering (agg_output_projection emits
`strings.ToLower(alias)`), and refusing on a spelling the scope no longer
has would break `SELECT id AS "Kk" … ORDER BY "Kk"`, which works.
`SELECT "WatchID"` over a base table publishing `WatchID` finds those bytes
in exact and passes.
